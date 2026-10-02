"""W5c failure-aware rescheduling tests (FR-12).

The whole feature: a run killed by a PROVEN RAM shortage is retried, but only on a
node with strictly MORE RAM than the one that failed; if the machine itself ran out
and no bigger node exists, it fails with an explicit "insufficient pool" reason.

Two groups:
  * PURE decision (scheduler.decide_oom_outcome): the honesty rules as code —
    user-limit vs node-capacity, the retry cap, the give-up rule, the ordering.
  * END-TO-END through the real status endpoint + claim query: a node-capacity OOM
    requeues to a strictly-stronger node (and the weak node can no longer claim it);
    a user-limit OOM stays FAILED; no stronger node -> INSUFFICIENT_POOL; a non-OOM
    failure never escalates; the escalation is visible in the /jobs/{id}/runs read.

Escalation fires ONLY on the kernel-proven OOM_KILLED fact — never on a guess.
"""

from app.models import Run, RunStatus
from app.scheduler import INSUFFICIENT_POOL, OOM_KILLED, decide_oom_outcome


# ===========================================================================
# 1) Pure decision — no database
# ===========================================================================


def test_decide_user_limit_hit_does_not_escalate():
    # The user set a cap smaller than the node's RAM -> their cap was too small,
    # not the machine. Retrying elsewhere would fail the same way, so: FAIL.
    o = decide_oom_outcome(
        user_mem_limit_mb=128, node_ram_mb=8192, escalation_count=0, stronger_node_exists=True
    )
    assert o.action == "fail"
    assert o.reason == OOM_KILLED
    assert "memory limit (128 MB)" in o.detail
    assert o.learned_min_ram_mb is None


def test_decide_node_capacity_escalates_to_learned_ram():
    # No user cap -> the container was capped at the node's RAM and the kernel killed
    # it there. The machine was too weak: escalate, demanding strictly more RAM.
    o = decide_oom_outcome(
        user_mem_limit_mb=None, node_ram_mb=8192, escalation_count=0, stronger_node_exists=True
    )
    assert o.action == "requeue"
    assert o.learned_min_ram_mb == 8192   # "strictly more than 8192" excludes this node
    assert o.reason is None


def test_decide_user_limit_equal_to_node_ram_is_node_capacity():
    # A cap EQUAL to the node's RAM is not "smaller than the node" -> node-capacity.
    o = decide_oom_outcome(
        user_mem_limit_mb=8192, node_ram_mb=8192, escalation_count=0, stronger_node_exists=True
    )
    assert o.action == "requeue"
    assert o.learned_min_ram_mb == 8192


def test_decide_no_stronger_node_is_insufficient_pool():
    o = decide_oom_outcome(
        user_mem_limit_mb=None, node_ram_mb=8192, escalation_count=0, stronger_node_exists=False
    )
    assert o.action == "fail"
    assert o.reason == INSUFFICIENT_POOL
    assert "more than 8192 MB" in o.detail
    assert o.learned_min_ram_mb is None


def test_decide_cap_exhausted_gives_up():
    o = decide_oom_outcome(
        user_mem_limit_mb=None, node_ram_mb=8192, escalation_count=3, stronger_node_exists=True
    )
    assert o.action == "fail"
    assert o.reason == OOM_KILLED     # gave up after escalating — not "insufficient pool"
    assert "giving up" in o.detail


def test_decide_cap_check_precedes_pool_check():
    # With the cap reached AND no stronger node, we still report "giving up" (we
    # already tried enough), not INSUFFICIENT_POOL — the cap is checked first.
    o = decide_oom_outcome(
        user_mem_limit_mb=None, node_ram_mb=8192, escalation_count=3, stronger_node_exists=False
    )
    assert o.reason == OOM_KILLED
    assert "giving up" in o.detail


# ===========================================================================
# 2) End-to-end — status endpoint + claim query
# ===========================================================================


def _auth(node):
    return {"Authorization": f"Bearer {node['token']}"}


async def _register(client, name, ram_mb):
    specs = {"cpu_cores": 4, "has_gpu": False, "ram_mb": ram_mb, "capacity": 4, "agent_version": "0.12.0"}
    resp = await client.post("/agent/register", json={"name": name, "specs": specs})
    assert resp.status_code == 200, resp.text
    return resp.json()


async def _create_job(client, **overrides):
    body = {
        "name": "j", "image": "fyp-dummy:latest", "entrypoint": ["python", "train.py", "--oom"],
        "env": {}, "resource_reqs": {"needs_gpu": False}, "replicas": 1,
    }
    body.update(overrides)
    resp = await client.post("/jobs", json=body)
    assert resp.status_code == 200, resp.text
    return resp.json()


async def _heartbeat(client, node, running=None):
    resp = await client.post(
        "/agent/heartbeat",
        headers=_auth(node),
        json={"node_id": node["node_id"], "status": "idle", "running": running or []},
    )
    assert resp.status_code == 200, resp.text
    return resp.json()


async def _status(client, node, run_id, attempt, state, exit_code=None, reason=None, detail=None):
    body = {"attempt": attempt, "state": state, "exit_code": exit_code}
    if reason is not None:
        body["failure_reason"] = reason
        body["failure_detail"] = detail
    return await client.post(f"/agent/runs/{run_id}/status", headers=_auth(node), json=body)


async def _oom(client, node, run_id, attempt):
    """Report a kernel OOM kill for a run (the agent's hard-fact classification)."""
    return await _status(
        client, node, run_id, attempt, "FAILED", exit_code=137,
        reason="OOM_KILLED",
        detail="RAM overload: the kernel killed the container after it exceeded its memory limit.",
    )


async def _run_row(session_factory, run_id):
    async with session_factory() as s:
        return await s.get(Run, run_id)


async def _claim_one(client, node):
    """Heartbeat and run the first assignment to RUNNING; return the assignment."""
    a = (await _heartbeat(client, node))["assignments"][0]
    await _status(client, node, a["run_id"], a["attempt"], "RUNNING")
    return a


async def test_node_capacity_oom_escalates_to_a_stronger_node(client, session_factory):
    weak = await _register(client, "weak", 8192)
    strong = await _register(client, "strong", 16384)
    job = await _create_job(client)  # no mem_limit -> a node-capacity kill

    a = await _claim_one(client, weak)
    r = await _oom(client, weak, a["run_id"], a["attempt"])
    assert r.status_code == 200

    run = await _run_row(session_factory, a["run_id"])
    assert run.status == RunStatus.PENDING            # requeued, not dead
    assert run.learned_min_ram_mb == 8192             # needs strictly more than the weak node
    assert run.escalation_count == 1
    assert run.node_id is None                        # released
    assert run.failure_reason is None                 # it is being retried, not a dead run
    # The job followed the run back off "terminal".
    assert (await client.get(f"/jobs/{job['job_id']}")).json()["status"] in ("PENDING", "RUNNING")

    # The weak node cannot reclaim it — its RAM is not strictly greater than learned.
    assert (await _heartbeat(client, weak))["assignments"] == []

    # The stronger node can, and the claim bumps the attempt (fences the dead one).
    work = await _heartbeat(client, strong)
    assert len(work["assignments"]) == 1
    b = work["assignments"][0]
    assert b["run_id"] == a["run_id"]
    assert b["attempt"] == a["attempt"] + 1

    # It finishes on the strong node; the escalation history persists (drives the badge).
    await _status(client, strong, b["run_id"], b["attempt"], "RUNNING")
    done = await _status(client, strong, b["run_id"], b["attempt"], "SUCCEEDED", exit_code=0)
    assert done.status_code == 200
    run = await _run_row(session_factory, a["run_id"])
    assert run.status == RunStatus.SUCCEEDED
    assert run.escalation_count == 1
    assert run.learned_min_ram_mb == 8192


async def test_user_limit_oom_does_not_escalate(client, session_factory):
    node = await _register(client, "big", 16384)
    # The user's own 128 MB cap is smaller than the node -> their cap, not the machine.
    await _create_job(client, resource_reqs={"needs_gpu": False, "mem_limit_mb": 128})

    a = await _claim_one(client, node)
    await _oom(client, node, a["run_id"], a["attempt"])

    run = await _run_row(session_factory, a["run_id"])
    assert run.status == RunStatus.FAILED
    assert run.failure_reason == "OOM_KILLED"         # stays OOM — not escalated
    assert run.escalation_count == 0
    assert run.learned_min_ram_mb is None
    assert "memory limit (128 MB)" in run.failure_detail


async def test_node_capacity_oom_with_no_stronger_node_is_insufficient_pool(client, session_factory):
    weak = await _register(client, "weak", 8192)  # the only registered node
    await _create_job(client)

    a = await _claim_one(client, weak)
    await _oom(client, weak, a["run_id"], a["attempt"])

    run = await _run_row(session_factory, a["run_id"])
    assert run.status == RunStatus.FAILED
    assert run.failure_reason == INSUFFICIENT_POOL
    assert "more than 8192 MB" in run.failure_detail
    assert run.learned_min_ram_mb is None             # not requeued
    assert run.escalation_count == 0


async def test_non_oom_failure_never_escalates(client, session_factory):
    weak = await _register(client, "weak", 8192)
    await _register(client, "strong", 16384)          # a stronger node exists…
    await _create_job(client)

    a = await _claim_one(client, weak)
    # …but a plain app error (exit 1, no reason -> APP_ERROR) is not a RAM proof.
    await _status(client, weak, a["run_id"], a["attempt"], "FAILED", exit_code=1)

    run = await _run_row(session_factory, a["run_id"])
    assert run.status == RunStatus.FAILED
    assert run.failure_reason == "APP_ERROR"
    assert run.escalation_count == 0
    assert run.learned_min_ram_mb is None


async def test_escalation_cap_exhausted_fails_terminally(client, session_factory):
    weak = await _register(client, "weak", 8192)
    await _register(client, "strong", 16384)          # stronger node available…
    await _create_job(client)

    a = await _claim_one(client, weak)
    # This run has already been escalated the maximum number of times.
    async with session_factory() as s:
        run = await s.get(Run, a["run_id"])
        run.escalation_count = 3
        await s.commit()
    await _oom(client, weak, a["run_id"], a["attempt"])

    run = await _run_row(session_factory, a["run_id"])
    assert run.status == RunStatus.FAILED
    assert run.failure_reason == "OOM_KILLED"         # gave up, not "insufficient pool"
    assert "giving up" in run.failure_detail
    assert run.learned_min_ram_mb is None


async def test_escalation_surfaces_in_job_runs_read(client, session_factory):
    weak = await _register(client, "weak", 8192)
    await _register(client, "strong", 16384)
    job = await _create_job(client)

    a = await _claim_one(client, weak)
    await _oom(client, weak, a["run_id"], a["attempt"])

    runs = (await client.get(f"/jobs/{job['job_id']}/runs")).json()
    assert runs[0]["learned_min_ram_mb"] == 8192
    assert runs[0]["escalation_count"] == 1
