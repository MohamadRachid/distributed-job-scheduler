"""W5 reaper logic tests — failure detection + re-dispatch (THE PROJECT).

These pin the reaper's *transitions* on SQLite by driving the real `sweep_once`
against seeded rows (the SKIP-LOCKED *concurrency* skip is Postgres-specific and
lives in test_w5_postgres.py; the full end-to-end fence loop lives in
scripts/chaos_test.py).

The reaper's contract:
  * an expired ASSIGNED/RUNNING run with retries left -> PENDING, retries -1,
    node_id + lease cleared, `attempt` UNCHANGED (the next claim bumps it);
  * an expired run with no retries left -> FAILED (terminal), not requeued;
  * a non-expired lease, a PENDING run, and a terminal run are all left untouched;
  * `attempt` is never touched by the reaper — the claim owns the bump, and that
    single bump is what fences a zombie's late result.

`now` is injected so "the lease expired" is deterministic (no sleeping).
"""

from datetime import datetime, timedelta, timezone

from app.models import Job, JobStatus, Node, NodeStatus, Run, RunStatus
from app.reaper import sweep_once
from app.scheduler import assign_runs

NOW = datetime(2026, 7, 15, 12, 0, 0, tzinfo=timezone.utc)
PAST = NOW - timedelta(seconds=30)   # a lease that lapsed 30s ago
FUTURE = NOW + timedelta(seconds=30)  # a lease still good for 30s


# --- helpers ----------------------------------------------------------------


async def _seed(
    factory,
    *,
    status=RunStatus.ASSIGNED,
    lease=PAST,
    attempt=1,
    retries=1,
    node=True,
):
    """Create one node + one job + one run in the given state; return their ids."""
    async with factory() as s:
        n = Node(
            name="w5-node", status=NodeStatus.idle, cpu_cores=4, has_gpu=False,
            ram_mb=8192, capacity=4, token_hash="h" * 64, last_heartbeat=NOW,
        )
        job = Job(name="j", image="fyp-dummy:latest", status=JobStatus.RUNNING, replicas=1)
        s.add_all([n, job])
        await s.flush()
        run = Run(
            job_id=job.id,
            node_id=n.id if node else None,
            status=status,
            attempt=attempt,
            lease_expires_at=lease,
            retries_remaining=retries,
        )
        s.add(run)
        await s.commit()
        return n.id, job.id, run.id


async def _get_run(factory, run_id):
    async with factory() as s:
        return await s.get(Run, run_id)


async def _get_job(factory, job_id):
    async with factory() as s:
        return await s.get(Job, job_id)


# --- requeue path (retries remaining) ---------------------------------------


async def test_expired_assigned_run_is_requeued(session_factory):
    node_id, job_id, run_id = await _seed(
        session_factory, status=RunStatus.ASSIGNED, lease=PAST, attempt=1, retries=1
    )

    decisions = await sweep_once(session_factory=session_factory, now=NOW)

    assert len(decisions) == 1
    assert decisions[0]["decision"] == "requeued"
    assert decisions[0]["node_id"] == node_id
    assert decisions[0]["attempt"] == 1

    run = await _get_run(session_factory, run_id)
    assert run.status == RunStatus.PENDING          # back in the queue
    assert run.retries_remaining == 0               # one retry consumed
    assert run.node_id is None                      # detached from the dead node
    assert run.lease_expires_at is None             # lease cleared
    assert run.attempt == 1                          # UNCHANGED — the claim bumps it
    # the job drops back to PENDING (its only run is waiting again)
    job = await _get_job(session_factory, job_id)
    assert job.status == JobStatus.PENDING


async def test_expired_running_run_is_requeued(session_factory):
    _, _, run_id = await _seed(
        session_factory, status=RunStatus.RUNNING, lease=PAST, attempt=2, retries=1
    )

    await sweep_once(session_factory=session_factory, now=NOW)

    run = await _get_run(session_factory, run_id)
    assert run.status == RunStatus.PENDING
    assert run.retries_remaining == 0
    assert run.node_id is None
    assert run.attempt == 2                          # still unchanged from RUNNING


# --- exhausted path (no retries) --------------------------------------------


async def test_expired_run_out_of_retries_fails_terminally(session_factory):
    _, job_id, run_id = await _seed(
        session_factory, status=RunStatus.RUNNING, lease=PAST, attempt=3, retries=0
    )

    decisions = await sweep_once(session_factory=session_factory, now=NOW)

    assert decisions[0]["decision"] == "failed_exhausted"
    run = await _get_run(session_factory, run_id)
    assert run.status == RunStatus.FAILED            # terminal, not requeued
    assert run.attempt == 3                           # still untouched
    assert run.exit_code is None                      # lost, not a bad exit code
    assert run.finished_at is not None
    job = await _get_job(session_factory, job_id)
    assert job.status == JobStatus.FAILED


# --- runs the reaper must NOT touch -----------------------------------------


async def test_unexpired_lease_is_untouched(session_factory):
    _, _, run_id = await _seed(
        session_factory, status=RunStatus.ASSIGNED, lease=FUTURE, attempt=1, retries=1
    )

    decisions = await sweep_once(session_factory=session_factory, now=NOW)

    assert decisions == []                            # nothing lapsed
    run = await _get_run(session_factory, run_id)
    assert run.status == RunStatus.ASSIGNED           # left exactly as it was
    assert run.retries_remaining == 1
    assert run.lease_expires_at is not None


async def test_pending_run_is_untouched(session_factory):
    # A PENDING run carries no lease; it is never the reaper's business.
    _, _, run_id = await _seed(
        session_factory, status=RunStatus.PENDING, lease=None, attempt=0, retries=1, node=False
    )

    decisions = await sweep_once(session_factory=session_factory, now=NOW)

    assert decisions == []
    run = await _get_run(session_factory, run_id)
    assert run.status == RunStatus.PENDING


async def test_terminal_runs_are_untouched(session_factory):
    # Even a terminal run whose (stale) lease is in the past must be left alone —
    # a result, once accepted, is final. The reaper only eyes ASSIGNED/RUNNING.
    for terminal in (RunStatus.SUCCEEDED, RunStatus.FAILED):
        _, _, run_id = await _seed(
            session_factory, status=terminal, lease=PAST, attempt=1, retries=1
        )
        await sweep_once(session_factory=session_factory, now=NOW)
        run = await _get_run(session_factory, run_id)
        assert run.status == terminal


# --- the fencing story at logic level ---------------------------------------


async def test_requeue_keeps_attempt_so_next_claim_bumps_it(session_factory):
    """The reaper leaves attempt=N; the NEXT claim bumps N->N+1. That one bump is
    the fence — a late report from the dead node carries N and is rejected."""
    node_a_id, job_id, run_id = await _seed(
        session_factory, status=RunStatus.RUNNING, lease=PAST, attempt=1, retries=1
    )

    await sweep_once(session_factory=session_factory, now=NOW)
    run = await _get_run(session_factory, run_id)
    assert run.status == RunStatus.PENDING and run.attempt == 1  # requeued, still 1

    # A fresh node B claims the requeued run -> attempt bumps 1 -> 2.
    async with session_factory() as s:
        node_b = Node(
            name="w5-node-b", status=NodeStatus.idle, cpu_cores=4, has_gpu=False,
            ram_mb=8192, capacity=4, token_hash="b" * 64, last_heartbeat=NOW,
        )
        s.add(node_b)
        await s.flush()
        claimed = await assign_runs(s, node_b, NOW)
        await s.commit()
        assert [a.run_id for a in claimed] == [run_id]

    run = await _get_run(session_factory, run_id)
    assert run.attempt == 2                            # the fence: N -> N+1
    assert run.node_id == node_b.id                    # now owned by B, not the dead A
    assert node_b.id != node_a_id
