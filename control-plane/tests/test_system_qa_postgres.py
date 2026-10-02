"""Concurrent completion proof; only a disposable TEST_DATABASE_URL is safe."""

import asyncio
import os
from datetime import datetime, timedelta, timezone

import pytest
from fastapi import HTTPException
from sqlalchemy import select, text

from app.api.keys import redeem_key
from app.models import Job, JobKey, JobStatus, KeyTicket, Run, RunStatus
from app.scheduler import recompute_job_status
from app.schemas import KeyRedeemRequest
from app.sealing import key_to_b64, new_key
from tests.test_w2_postgres import sessionmaker_pg as postgres_fixture

sessionmaker_pg = postgres_fixture

pytestmark = pytest.mark.skipif(
    "postgresql" not in os.environ.get("TEST_DATABASE_URL", ""),
    reason="requires disposable PostgreSQL TEST_DATABASE_URL",
)


@pytest.mark.parametrize("last_status", [RunStatus.SUCCEEDED, RunStatus.FAILED])
async def test_concurrent_last_completions_set_terminal_job(sessionmaker_pg, last_status):
    async with sessionmaker_pg() as session:
        job = Job(name="qa-rollup", image="fyp-dummy:latest", replicas=2,
                  status=JobStatus.RUNNING)
        session.add(job)
        await session.flush()
        runs = [Run(job_id=job.id, status=RunStatus.RUNNING) for _ in range(2)]
        session.add_all(runs)
        await session.commit()
        job_id, run_ids = job.id, [run.id for run in runs]

    async with sessionmaker_pg() as first, sessionmaker_pg() as second:
        second_pid = await second.scalar(text("select pg_backend_pid()"))
        r1 = await first.get(Run, run_ids[0])
        r2 = await second.get(Run, run_ids[1])
        r1.status = RunStatus.SUCCEEDED
        r2.status = last_status
        await first.flush()
        await second.flush()
        await recompute_job_status(first, job_id)

        async def finish_second():
            await recompute_job_status(second, job_id)
            await second.commit()

        task = asyncio.create_task(finish_second())
        try:
            # Release the first writer only once the second has either finished
            # (the buggy path) or is waiting on its lock (the serialized path).
            async with asyncio.timeout(5):
                async with sessionmaker_pg() as observer:
                    while not task.done():
                        waiting = await observer.scalar(text(
                            "select wait_event_type = 'Lock' from pg_stat_activity "
                            "where pid = :pid"
                        ), {"pid": second_pid})
                        await observer.commit()  # refresh PostgreSQL's stats snapshot
                        if waiting:
                            break
                        await asyncio.sleep(0.01)
            await first.commit()
            await asyncio.wait_for(task, 5)
        finally:
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    async with sessionmaker_pg() as session:
        statuses = (await session.scalars(select(Run.status).where(Run.job_id == job_id))).all()
        assert all(status in (RunStatus.SUCCEEDED, RunStatus.FAILED) for status in statuses)
        job = await session.get(Job, job_id)
        expected = JobStatus.SUCCEEDED if last_status == RunStatus.SUCCEEDED else JobStatus.FAILED
        assert job.status == expected


async def test_concurrent_key_redemption_releases_key_once(sessionmaker_pg):
    async with sessionmaker_pg() as session:
        job = Job(name="qa-key", image="fyp-dummy:latest", status=JobStatus.RUNNING)
        session.add(job)
        await session.flush()
        run = Run(job_id=job.id, status=RunStatus.RUNNING, attempt=1)
        session.add(run)
        await session.flush()
        session.add(JobKey(job_id=job.id, key_b64=key_to_b64(new_key())))
        session.add(KeyTicket(ticket="qa-single-use", run_id=run.id, attempt=1,
                              expires_at=datetime.now(timezone.utc) + timedelta(minutes=1)))
        await session.commit()

    async def redeem():
        async with sessionmaker_pg() as session:
            try:
                await redeem_key(KeyRedeemRequest(ticket="qa-single-use"), session=session)
                return 200
            except HTTPException as exc:
                return exc.status_code

    outcomes = await asyncio.wait_for(asyncio.gather(redeem(), redeem()), 5)
    assert sorted(outcomes) == [200, 410]


async def test_key_ticket_that_expires_while_waiting_for_run_lock_is_rejected(
    sessionmaker_pg,
):
    """Expiry is checked after serialization, at the instant the key is released."""
    expires_at = datetime.now(timezone.utc) + timedelta(seconds=1)
    async with sessionmaker_pg() as session:
        job = Job(name="qa-expiry", image="fyp-dummy:latest", status=JobStatus.RUNNING)
        session.add(job)
        await session.flush()
        run = Run(job_id=job.id, status=RunStatus.RUNNING, attempt=1)
        session.add(run)
        await session.flush()
        session.add(JobKey(job_id=job.id, key_b64=key_to_b64(new_key())))
        session.add(KeyTicket(
            ticket="qa-expires-while-waiting",
            run_id=run.id,
            attempt=1,
            expires_at=expires_at,
        ))
        await session.commit()
        run_id = run.id

    async with sessionmaker_pg() as blocker:
        await blocker.execute(select(Run).where(Run.id == run_id).with_for_update())
        pid_ready = asyncio.get_running_loop().create_future()

        async def redeem():
            async with sessionmaker_pg() as session:
                pid = await session.scalar(text("select pg_backend_pid()"))
                pid_ready.set_result(pid)
                try:
                    await redeem_key(
                        KeyRedeemRequest(ticket="qa-expires-while-waiting"),
                        session=session,
                    )
                    return pid, 200
                except HTTPException as exc:
                    return pid, exc.status_code

        task = asyncio.create_task(redeem())
        try:
            async with asyncio.timeout(5):
                pid = await pid_ready
                async with sessionmaker_pg() as observer:
                    while not task.done():
                        waiting = await observer.scalar(text(
                            "select wait_event_type = 'Lock' from pg_stat_activity "
                            "where pid = :pid"
                        ), {"pid": pid})
                        await observer.commit()
                        if waiting:
                            break
                        await asyncio.sleep(0.01)
                assert waiting, "redemption did not wait on the run lock"
                remaining = (expires_at - datetime.now(timezone.utc)).total_seconds()
                if remaining > 0:
                    await asyncio.sleep(remaining + 0.05)
            await blocker.commit()
            _pid, outcome = await asyncio.wait_for(task, 5)
        finally:
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    assert outcome == 410
