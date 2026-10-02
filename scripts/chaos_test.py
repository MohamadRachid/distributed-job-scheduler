"""The chaos test — the deterministic full-loop proof of failure recovery.

This is the single most important artifact of the project: the ISO 25010 §4.2.5
(Reliability) evidence and the heart of the defense. It proves, end to end, that:

    a worker can die mid-run, the job STILL finishes on another node, and the dead
    node's late result is NEVER accepted — exactly one result is accepted.

It is deterministic (not a lucky live run): it drives the REAL control-plane
functions — create_job, heartbeat (pull-time claim), sweep_once (the reaper),
run_status (the fence) — against a real Postgres, and forces the lease to expire
by advancing the reaper's clock rather than sleeping. Same code the agents hit
over HTTP; only the ASGI/auth layer is bypassed (we pass node objects directly).

The loop (W5 brief §4):
  1. node A claims the run           -> attempt 1, ASSIGNED/RUNNING
  2. node A goes silent (no heartbeat -> no lease renewal)
  3. the lease expires; the reaper marks it LOST and requeues -> PENDING, attempt STILL 1
  4. node B claims the requeued run   -> attempt bumps 1 -> 2, RUNNING
  5. zombie A posts its old result under attempt 1 -> 409, REJECTED (the fence)
  6. node B finishes                  -> SUCCEEDED under attempt 2; job SUCCEEDED
  7. assert exactly ONE accepted result, and it is node B's

Run it inside the canonical 3.12 container (pinned asyncpg), stack up:

    docker compose up -d
    docker compose run --rm -v "${PWD}/scripts:/scripts" control-plane python /scripts/chaos_test.py

WARNING: it wipes the runs/jobs/nodes/logs tables (clean slate, like the demo
staging). Point it at a throwaway/demo DB, not precious data.
"""

import asyncio
import os
import sys
import traceback
from datetime import datetime, timedelta, timezone

# Make `app` importable whether run from the repo host (script at scripts/) or
# inside the control-plane container (source mounted at /app).
_HERE = os.path.dirname(os.path.abspath(__file__))
for _cand in (os.path.join(_HERE, "..", "control-plane"), "/app", os.getcwd()):
    if os.path.isdir(os.path.join(_cand, "app")):
        sys.path.insert(0, os.path.abspath(_cand))
        break

from fastapi import BackgroundTasks, HTTPException  # noqa: E402
from sqlalchemy import delete  # noqa: E402
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine  # noqa: E402

import app.models  # noqa: E402,F401 -- register all tables on Base.metadata
from app.api.agent import heartbeat, run_status  # noqa: E402  the real handlers
from app.api.jobs import create_job  # noqa: E402
from app.config import get_settings  # noqa: E402
from app.db import Base  # noqa: E402
from app.models import (  # noqa: E402
    Artifact, Job, JobKey, JobStatus, KeyTicket, Node, NodeEvent, NodeStatus,
    Run, RunLog, RunLogArchive, RunSample, RunStatus,
)
from app.reaper import sweep_once  # noqa: E402  the real reaper
from app.schemas import (  # noqa: E402
    HeartbeatRequest, JobCreate, ResourceReqs, RunStatusUpdate,
)

DB_URL = (
    os.environ.get("CHAOS_DATABASE_URL")
    or os.environ.get("TEST_DATABASE_URL")
    or get_settings().database_url  # inside the container this is the compose DB
)


def step(msg: str) -> None:
    print(msg, flush=True)


def ok(msg: str) -> None:
    print(f"   [OK] {msg}", flush=True)


async def _wipe(factory) -> None:
    async with factory() as s:
        # Children before parents. Every table that carries a run_id, job_id or
        # node_id belongs here: a missing one makes this DELETE fail with a foreign
        # key violation on any database that has ever recorded a resource sample, a
        # node event or a private job key -- i.e. every database a live demo touched.
        for model in (
            KeyTicket, RunLog, RunLogArchive, RunSample, Artifact, Run,
            JobKey, Job, NodeEvent, Node,
        ):
            await s.execute(delete(model))
        await s.commit()


async def _seed_node(factory, name: str, token_seed: str) -> str:
    async with factory() as s:
        node = Node(
            name=name, status=NodeStatus.idle, cpu_cores=4, has_gpu=False,
            ram_mb=8192, capacity=4, token_hash=token_seed * 64,
            last_heartbeat=datetime.now(timezone.utc),
            # 2026-09-06: a machine has to say how new it is, because a SEALED job is
            # invisible to an agent that could not stage one — and every job created
            # from that date is sealed. A seeded node with no version reads as too old
            # (the guard fails closed) and would be offered nothing, so the test would
            # stop at step 2 for a reason that has nothing to do with what it proves.
            agent_version="0.12.0",
        )
        s.add(node)
        await s.commit()
        return node.id


async def run_chaos(factory) -> None:
    settings = get_settings()
    step(f"Chaos test — DB={DB_URL}")
    step(f"lease_ttl={settings.lease_ttl_s}s (we advance the reaper's clock past it "
         f"to force a deterministic expiry)\n")

    await _wipe(factory)
    a_id = await _seed_node(factory, "node-A", "a")
    b_id = await _seed_node(factory, "node-B", "b")
    step(f"Seeded node A ({a_id[:8]}) and node B ({b_id[:8]}); pool is clean.\n")

    # --- submit a job (untargeted, one run) ---------------------------------
    step("1. Submit a job (1 run, any eligible node).")
    async with factory() as s:
        created = await create_job(
            JobCreate(
                name="chaos", image="fyp-dummy:latest",
                entrypoint=["python", "train.py"], env={"EPOCHS": "3"},
                resource_reqs=ResourceReqs(), target_node_ids=None, replicas=1,
            ),
            # The handler gained a BackgroundTasks parameter on 2026-09-05 (the
            # checkpoint advisor), and this call had not been updated: the session was
            # landing in that slot and the session parameter was left as its Depends
            # sentinel, so the whole test stopped at step 1. Calling the REAL handler
            # is the point of this file — it is what makes the proof about the
            # platform rather than about a re-implementation — and the price is that a
            # signature change reaches it. An empty task list is right here: nothing
            # runs the queued advisor task in a script, and the advice is not what is
            # being proven.
            BackgroundTasks(),
            s,
        )
    job_id, run_id = created.job_id, created.run_ids[0]
    ok(f"job {job_id[:8]} -> run {run_id[:8]} (PENDING)")

    # --- node A claims + starts ---------------------------------------------
    step("\n2. Node A heartbeats and claims the run (pull-time assignment).")
    async with factory() as s:
        node_a = await s.get(Node, a_id)
        resp = await heartbeat(HeartbeatRequest(node_id=a_id, status="idle", running=[]), node_a, s)
    assert len(resp.assignments) == 1, "node A should have been assigned the run"
    assert resp.assignments[0].run_id == run_id
    assert resp.assignments[0].attempt == 1, "first claim bumps attempt 0 -> 1"
    ok(f"node A claimed run {run_id[:8]} at attempt=1 (ASSIGNED, lease started)")

    async with factory() as s:
        node_a = await s.get(Node, a_id)
        r = await run_status(run_id, RunStatusUpdate(attempt=1, state="RUNNING"), node_a, s)
    assert r.run_status == "RUNNING"
    ok("node A reported RUNNING at attempt=1")

    # --- node A dies; the reaper reclaims the expired lease -----------------
    step("\n3. Node A goes SILENT (stops heart-beating). Its lease is no longer renewed.")
    reaper_now = datetime.now(timezone.utc) + timedelta(seconds=settings.lease_ttl_s + 10)
    step(f"   The reaper sweeps at a time past the lease (now + {settings.lease_ttl_s + 10}s).")
    decisions = await sweep_once(session_factory=factory, now=reaper_now)
    assert len(decisions) == 1, f"reaper should reclaim exactly one run, got {decisions}"
    assert decisions[0]["decision"] == "requeued"
    assert decisions[0]["run_id"] == run_id
    ok(f"reaper marked run {run_id[:8]} LOST (dead node A, attempt 1) and REQUEUED it")
    ok(f"reaper audit record: {decisions[0]}")

    async with factory() as s:
        run = await s.get(Run, run_id)
    assert run.status == RunStatus.PENDING, "requeued run must be PENDING"
    assert run.attempt == 1, "the reaper must NOT bump attempt — the claim does"
    assert run.node_id is None and run.lease_expires_at is None
    ok("run is PENDING again, node cleared, attempt STILL 1 (the reaper never bumps it)")

    # --- node B takes over; the claim is what fences the zombie -------------
    step("\n4. Node B heartbeats and claims the requeued run.")
    async with factory() as s:
        node_b = await s.get(Node, b_id)
        resp = await heartbeat(HeartbeatRequest(node_id=b_id, status="idle", running=[]), node_b, s)
    assert len(resp.assignments) == 1 and resp.assignments[0].run_id == run_id
    assert resp.assignments[0].attempt == 2, "the claim bumps attempt 1 -> 2 (this is the fence)"
    ok(f"node B claimed run {run_id[:8]}; attempt bumped 1 -> 2 (the fence is now set)")

    async with factory() as s:
        node_b = await s.get(Node, b_id)
        await run_status(run_id, RunStatusUpdate(attempt=2, state="RUNNING"), node_b, s)
    ok("node B reported RUNNING at attempt=2")

    # --- the zombie wakes up: its stale result is rejected ------------------
    step("\n5. Zombie node A wakes up and posts its OLD result (attempt 1).")
    fenced = False
    async with factory() as s:
        node_a = await s.get(Node, a_id)
        try:
            await run_status(
                run_id, RunStatusUpdate(attempt=1, state="SUCCEEDED", exit_code=0), node_a, s
            )
        except HTTPException as exc:
            assert exc.status_code == 409, f"expected 409, got {exc.status_code}"
            fenced = True
    assert fenced, "the zombie's stale result MUST be rejected 409 — the whole point"
    ok("node A's attempt-1 result was REJECTED with 409 (the run has moved on: "
       "owner=B, attempt=2). A is told to abort. NOT accepted.")

    # --- node B finishes: exactly one accepted result -----------------------
    step("\n6. Node B finishes the run.")
    async with factory() as s:
        node_b = await s.get(Node, b_id)
        r = await run_status(
            run_id, RunStatusUpdate(attempt=2, state="SUCCEEDED", exit_code=0), node_b, s
        )
    assert r.accepted and r.run_status == "SUCCEEDED"
    ok("node B's attempt-2 result was ACCEPTED -> run SUCCEEDED")

    # --- final assertions ---------------------------------------------------
    step("\n7. Final state — exactly one accepted result, and it is node B's:")
    async with factory() as s:
        run = await s.get(Run, run_id)
        job = await s.get(Job, job_id)
    assert run.status == RunStatus.SUCCEEDED
    assert run.node_id == b_id, "the accepted result is node B's, not the dead A's"
    assert run.attempt == 2
    assert run.exit_code == 0
    assert job.status == JobStatus.SUCCEEDED
    ok(f"run {run_id[:8]}: SUCCEEDED, node=B ({b_id[:8]}), attempt=2, exit=0")
    ok(f"job {job_id[:8]}: SUCCEEDED")

    step(
        "\nRESULT: PASS — the run executed under two attempts (at-least-once "
        "execution),\n        but exactly ONE result was accepted (at-most-once "
        "accepted result).\n        Recovery works; no duplicate accepted. "
        "[FR-7, FR-8, NFR-3, ISO 25010 §4.2.5]"
    )


async def main() -> int:
    engine = create_async_engine(DB_URL)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        await run_chaos(factory)
        return 0
    except Exception:  # noqa: BLE001 - report and fail loudly for the evidence log
        print("\nRESULT: FAIL", flush=True)
        traceback.print_exc()
        return 1
    finally:
        await _wipe(factory)  # leave no trace, even against a shared DB
        await engine.dispose()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
