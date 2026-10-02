"""Regressions from the preservation-focused review, using isolated stores."""

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from urllib.parse import quote

import pytest

from app import reaper, storage
from app.config import get_settings
from app.models import Node, Run, RunSample, RunStatus
from app.reaper import sweep_once
from app.scheduler import renew_leases
from tests.test_checkpoint_cleanup import (
    KIND_CHECKPOINT, _checkpoint_objects, _checkpoint_rows,
    _claim, _register, _upload,
)
from tests.test_w5 import _seed


async def test_cancel_expires_even_when_worker_keeps_reporting(client, session_factory):
    node = await _register(client)
    assignment = await _claim(client, node)
    run_id = assignment["run_id"]
    async with session_factory() as s:
        run = await s.get(Run, run_id)
        job_id = run.job_id
        original_expiry = run.lease_expires_at.replace(tzinfo=timezone.utc)
    assert (await client.post(f"/jobs/{job_id}/cancel")).status_code == 200
    for seconds in (10, 30, 90):
        async with session_factory() as s:
            n = await s.get(Node, node["node_id"])
            await renew_leases(s, n, [SimpleNamespace(run_id=run_id, attempt=1)],
                               original_expiry + timedelta(seconds=seconds))
            await s.commit()
    decisions = await sweep_once(session_factory, original_expiry + timedelta(seconds=91))
    assert [d["decision"] for d in decisions] == ["cancelled"]
    async with session_factory() as s:
        run = await s.get(Run, run_id)
        assert run.status == RunStatus.FAILED
        assert run.failure_reason == "CANCELLED"
        assert run.attempt == 1


async def test_normal_renewal_keeps_progress_and_fence(client, session_factory):
    node = await _register(client)
    assignment = await _claim(client, node)
    now = datetime.now(timezone.utc) + timedelta(seconds=20)
    async with session_factory() as s:
        n = await s.get(Node, node["node_id"])
        report = SimpleNamespace(run_id=assignment["run_id"], attempt=1,
                                 progress=0.5, metrics={"epoch": 5})
        await renew_leases(s, n, [report], now)
        await s.commit()
        run = await s.get(Run, assignment["run_id"])
        expiry = now + timedelta(seconds=get_settings().lease_ttl_s)
        assert run.lease_expires_at.replace(tzinfo=timezone.utc) == expiry
        assert run.progress == 0.5 and run.metrics_last == {"epoch": 5}
        report.attempt = 0
        await renew_leases(s, n, [report], now + timedelta(seconds=40))
        await s.commit()
        assert run.lease_expires_at.replace(tzinfo=timezone.utc) == expiry


@pytest.mark.parametrize("ending", ["exhausted", "cancelled", "pending_cancel", "retry"])
async def test_recovery_checkpoint_lifetime(ending, client, session_factory, mem_store, monkeypatch):
    monkeypatch.setattr(storage, "_minio_store", lambda: mem_store)
    node = await _register(client)
    a = await _claim(client, node)
    rid = a["run_id"]
    assert (await _upload(client, node, rid, 1, "checkpoint", b"saved", KIND_CHECKPOINT)).status_code == 200
    assert (await _upload(client, node, rid, 1, "result.bin", b"result")).status_code == 200
    now = datetime.now(timezone.utc)
    async with session_factory() as s:
        run = await s.get(Run, rid)
        job_id = run.job_id
        run.lease_expires_at = now - timedelta(seconds=1)
        run.retries_remaining = 0 if ending == "exhausted" else 1
        if ending == "cancelled":
            run.cancel_requested_at = now - timedelta(seconds=10)
        await s.commit()
    await sweep_once(session_factory, now)
    if ending == "pending_cancel":
        assert (await client.post(f"/jobs/{job_id}/cancel")).status_code == 200
    rows = await _checkpoint_rows(session_factory, rid)
    objects = _checkpoint_objects(mem_store, rid)
    assert bool(rows) == (ending == "retry")
    assert bool(objects) == (ending == "retry")
    assert mem_store.get_object(f"runs/{rid}/1/result.bin").startswith(b"FYPSEAL2")


@pytest.mark.parametrize("filename", ["模型.bin", 'result"final.bin', "metrics.json"])
async def test_sealed_result_download_filename(filename, client):
    node = await _register(client)
    a = await _claim(client, node)
    uploaded = await _upload(client, node, a["run_id"], 1, filename, b"plaintext result")
    assert uploaded.status_code == 200, uploaded.text
    response = await client.get(f"/artifacts/{uploaded.json()['artifact_id']}/download")
    assert response.status_code == 200
    assert response.content == b"plaintext result"
    disposition = response.headers["content-disposition"]
    if filename == "metrics.json":
        assert disposition == 'attachment; filename="metrics.json"'
    else:
        assert f"filename*=UTF-8''{quote(filename, safe='')}" in disposition


async def test_samples_return_latest_window_in_attempt_order(client, session_factory):
    node = await _register(client)
    a = await _claim(client, node)
    rid = a["run_id"]
    async with session_factory() as s:
        s.add_all([RunSample(run_id=rid, attempt=1, ts=float(i), mem_used_mb=float(i))
                   for i in range(1, 66)])
        # A replacement worker's clock may be behind the first worker's.
        s.add_all([RunSample(run_id=rid, attempt=2, ts=float(i), mem_used_mb=100+i)
                   for i in range(1, 4)])
        await s.commit()
    response = await client.get(f"/runs/{rid}/samples?limit=4")
    assert response.status_code == 200
    assert [r["mem_used_mb"] for r in response.json()] == [65, 101, 102, 103]
    response = await client.get(f"/runs/{rid}/samples")
    assert len(response.json()) == 60
    assert response.json()[-1]["mem_used_mb"] == 103


@pytest.mark.parametrize("timeout", [False, True])
async def test_slow_checkpoint_cleanup_does_not_block_other_recovery(
    timeout, client, session_factory, mem_store, monkeypatch,
):
    monkeypatch.setattr(storage, "_minio_store", lambda: mem_store)
    node = await _register(client)
    a = await _claim(client, node)
    await _upload(client, node, a["run_id"], 1, "checkpoint", b"saved", KIND_CHECKPOINT)
    now = datetime.now(timezone.utc)
    async with session_factory() as s:
        run = await s.get(Run, a["run_id"])
        run.retries_remaining = 0
        run.lease_expires_at = now - timedelta(seconds=1)
        await s.commit()
    _, _, retry_id = await _seed(session_factory, lease=now - timedelta(seconds=1))
    entered, release = asyncio.Event(), asyncio.Event()

    async def slow_delete(*args):
        entered.set()
        await release.wait()

    monkeypatch.setattr(reaper, "drop_objects", slow_delete)
    if timeout:
        monkeypatch.setattr(reaper, "_CLEANUP_TIMEOUT_S", 0.01)
    task = asyncio.create_task(sweep_once(session_factory, now))
    try:
        await asyncio.wait_for(entered.wait(), 2)
        async with session_factory() as s:
            assert (await s.get(Run, retry_id)).status == RunStatus.PENDING
        if timeout:
            decisions = await asyncio.wait_for(task, 1)
            assert len(decisions) == 2
    finally:
        release.set()
        await asyncio.wait_for(task, 2)
