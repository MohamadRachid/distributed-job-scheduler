"""Cancellation/renewal ordering, only against a disposable PostgreSQL database."""

import asyncio
import os
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from sqlalchemy import select, text

from app.models import Run
from app.scheduler import renew_leases
from tests.test_w2_postgres import sessionmaker_pg as postgres_fixture
from tests.test_w5 import _seed

sessionmaker_pg = postgres_fixture
pytestmark = pytest.mark.skipif(
    "postgresql" not in os.environ.get("TEST_DATABASE_URL", ""),
    reason="requires disposable PostgreSQL TEST_DATABASE_URL",
)


async def test_renewal_waits_for_cancel_and_refreshes_preloaded_run(sessionmaker_pg):
    now = datetime.now(timezone.utc)
    expiry = now + timedelta(seconds=30)
    node_id, _, run_id = await _seed(sessionmaker_pg, lease=expiry)
    async with sessionmaker_pg() as canceller, sessionmaker_pg() as worker:
        # Simulate a session that saw the run before cancellation committed.
        cached = await worker.get(Run, run_id)
        assert cached.cancel_requested_at is None
        pid = await worker.scalar(text("select pg_backend_pid()"))
        run = await canceller.scalar(select(Run).where(Run.id == run_id).with_for_update())
        run.cancel_requested_at = now
        await canceller.flush()

        async def renew():
            await renew_leases(worker, SimpleNamespace(id=node_id),
                               [SimpleNamespace(run_id=run_id, attempt=1)], now + timedelta(seconds=20))
            await worker.commit()

        task = asyncio.create_task(renew())
        try:
            async with asyncio.timeout(5):
                async with sessionmaker_pg() as observer:
                    while not task.done():
                        waiting = await observer.scalar(text(
                            "select wait_event_type = 'Lock' from pg_stat_activity where pid = :pid"
                        ), {"pid": pid})
                        await observer.commit()
                        if waiting:
                            break
                        await asyncio.sleep(0.01)
            assert not task.done(), "renewal must serialize with cancellation"
            await canceller.commit()
            await asyncio.wait_for(task, 5)
        finally:
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
    async with sessionmaker_pg() as s:
        run = await s.get(Run, run_id)
        assert run.cancel_requested_at == now
        assert run.lease_expires_at == expiry
