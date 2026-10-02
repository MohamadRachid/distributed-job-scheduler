"""The reaper's SKIP LOCKED concurrency proof — MUST run on Postgres.

The reaper transitions each expired run under `SELECT … FOR UPDATE SKIP LOCKED`
(reaper._reap_one). The point of SKIP LOCKED: if a run's row is busy in another
transaction *right now* (e.g. a status post for that run is committing this
instant), the reaper must SKIP it this pass — never block, never fight the other
writer, never double-transition a row someone else is mid-change on. It catches
the run on the next pass once the lock is released.

SQLite locks the whole database and ignores the clause, so this can only be
proven on Postgres (on SQLite it would pass trivially = false green). Skips unless
a real Postgres URL is provided:

    set TEST_DATABASE_URL=postgresql+asyncpg://fyp:fyp@localhost:5432/fyp   # PowerShell: $env:TEST_DATABASE_URL=...
    pytest control-plane/tests/test_w5_postgres.py -v
"""

import os
from datetime import datetime, timedelta, timezone

import pytest
import pytest_asyncio
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

import app.models  # noqa: F401 -- register all tables on Base.metadata
from app.db import Base
from app.models import Job, JobStatus, Node, NodeStatus, Run, RunStatus
from app.reaper import sweep_once

PG_URL = os.environ.get("TEST_DATABASE_URL", "")

pytestmark = pytest.mark.skipif(
    "postgresql" not in PG_URL,
    reason="set TEST_DATABASE_URL=postgresql+asyncpg://... to run the reaper SKIP LOCKED proof",
)


@pytest_asyncio.fixture
async def sessionmaker_pg():
    engine = create_async_engine(PG_URL)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async def _wipe() -> None:
        # Delete children before parents, in an order DERIVED from the schema rather
        # than hand-listed. A hand-list is what broke here: this fixture named five
        # tables, and `run_samples`, `node_events` and `job_keys` were added after it
        # was written -- so `DELETE FROM runs` hit a foreign key violation on any
        # database that had ever recorded a resource sample, a node event or a private
        # job key, which is every database a live run has touched. It stayed invisible
        # because CI starts from an empty Postgres, so the failing condition and the
        # passing condition never met. `sorted_tables` is dependency order, so reversed
        # is children-first, and it grows by itself the next time a migration adds a
        # table. `users` is spared on purpose: it holds the bootstrap admin, and these
        # tests drive the scheduler directly rather than logging in.
        #
        # `tiers` is spared too, since 2026-09-06, and for the third time it is the
        # same defect: `users.tier_id` is a foreign key onto `tiers` (2026-09-04) and
        # the spared bootstrap admin carries `standard`, so `DELETE FROM tiers` raises
        # a foreign key violation and this file's reaper-versus-lock proof ERRORS
        # before it runs. Invisible in CI for the usual reason -- an empty Postgres
        # has no admin -- and certain on any database the control plane has started
        # against. Both spared tables are seeded reference data no test here touches.
        spared = {"users", "tiers"}
        async with factory() as s:
            for table in reversed(Base.metadata.sorted_tables):
                if table.name not in spared:
                    await s.execute(delete(table))
            await s.commit()

    await _wipe()
    try:
        yield factory
    finally:
        await _wipe()
        await engine.dispose()


async def _seed_expired_run(factory, now: datetime) -> str:
    """One node + job + an ASSIGNED run whose lease lapsed 30s ago. Returns run id."""
    async with factory() as s:
        node = Node(
            name="A", status=NodeStatus.idle, cpu_cores=4, has_gpu=False,
            ram_mb=8192, capacity=4, token_hash="a" * 64, last_heartbeat=now,
        )
        job = Job(name="j", image="fyp-dummy:latest", status=JobStatus.RUNNING, replicas=1)
        s.add_all([node, job])
        await s.flush()
        run = Run(
            job_id=job.id, node_id=node.id, status=RunStatus.ASSIGNED,
            attempt=1, retries_remaining=1,
            lease_expires_at=now - timedelta(seconds=30),
        )
        s.add(run)
        await s.commit()
        return run.id


async def test_reaper_skips_a_locked_row_then_catches_it(sessionmaker_pg):
    now = datetime.now(timezone.utc)
    run_id = await _seed_expired_run(sessionmaker_pg, now)

    # Hold the run's row locked in an open transaction — as if a status post for
    # this exact run were committing at the instant the reaper sweeps.
    async with sessionmaker_pg() as locker:
        locked = (
            await locker.execute(
                select(Run).where(Run.id == run_id).with_for_update()
            )
        ).scalar_one()
        assert locked.status == RunStatus.ASSIGNED

        # Reaper sweeps WHILE the row is locked -> SKIP LOCKED hides it -> no-op.
        decisions = await sweep_once(session_factory=sessionmaker_pg, now=now)
        assert decisions == [], "reaper must skip a row locked by another txn"

        async with sessionmaker_pg() as s:
            still = await s.get(Run, run_id)
            assert still.status == RunStatus.ASSIGNED  # NOT double-transitioned

        # release the lock (nothing changed in the locker's transaction)
        await locker.rollback()

    # Next pass, with the lock gone, the reaper reclaims it exactly once.
    decisions = await sweep_once(session_factory=sessionmaker_pg, now=now)
    assert len(decisions) == 1 and decisions[0]["decision"] == "requeued"

    async with sessionmaker_pg() as s:
        final = await s.get(Run, run_id)
        assert final.status == RunStatus.PENDING
        assert final.node_id is None
        assert final.retries_remaining == 0
        assert final.attempt == 1  # reaper never bumps attempt
