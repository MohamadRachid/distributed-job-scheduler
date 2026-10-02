"""A failed object deletion must keep its reference and quota charge retryable."""

import pytest
from sqlalchemy import select

from app.models import Artifact, Job, Run, RunLogArchive, RunStatus
from app.quota import retained_used_bytes


@pytest.mark.parametrize("failed_kind", ["result", "checkpoint", "archive", "input", "all"])
async def test_release_keeps_failed_objects_charged_and_can_retry(
    client, session_factory, mem_store, monkeypatch, failed_kind
):
    created = await client.post("/jobs", json={
        "name": "release-failure", "image": "dummy", "entrypoint": ["python", "train.py"],
    })
    assert created.status_code == 200, created.text
    job_id = created.json()["job_id"]
    run_id = created.json()["run_ids"][0]
    sizes = {"result": 1024, "checkpoint": 2048, "archive": 3072, "input": 4096}
    keys = {kind: f"test-release/{job_id}/{kind}" for kind in sizes}
    async with session_factory() as session:
        job = await session.get(Job, job_id)
        owner_id = job.user_id
        job.input_object_key = keys["input"]
        job.input_size_bytes = sizes["input"]
        run = await session.get(Run, run_id)
        run.status = RunStatus.SUCCEEDED
        for kind in ("result", "checkpoint"):
            session.add(Artifact(run_id=run_id, attempt=0, kind=kind,
                                 object_key=keys[kind], size=sizes[kind]))
        session.add(RunLogArchive(run_id=run_id, attempt=0, object_key=keys["archive"],
                                  size_bytes=sizes["archive"], chunk_count=1,
                                  max_seq=0, sha256="0" * 64))
        await session.commit()
    for kind, key in keys.items():
        mem_store.put_object(key, b"x" * sizes[kind], None)

    failed_keys = set(keys.values()) if failed_kind == "all" else {keys[failed_kind]}
    original_delete = mem_store.delete_object

    def fail_selected(key):
        if key in failed_keys:
            raise OSError("object store temporarily unavailable")
        original_delete(key)

    monkeypatch.setattr(mem_store, "delete_object", fail_selected)
    response = await client.delete(f"/jobs/{job_id}/storage")
    assert response.status_code == 503, response.text
    assert "retry" in response.json()["detail"].lower()
    assert set(mem_store._objs) == failed_keys
    async with session_factory() as session:
        artifacts = (await session.execute(select(Artifact))).scalars().all()
        archives = (await session.execute(select(RunLogArchive))).scalars().all()
        job = await session.get(Job, job_id)
        referenced = {row.object_key for row in artifacts + archives}
        if job.input_object_key:
            referenced.add(job.input_object_key)
        assert referenced == failed_keys
        assert await retained_used_bytes(session, owner_id) == sum(
            size for kind, size in sizes.items() if keys[kind] in failed_keys
        )

    monkeypatch.setattr(mem_store, "delete_object", original_delete)
    retry = await client.delete(f"/jobs/{job_id}/storage")
    assert retry.status_code == 200, retry.text
    assert retry.json()["objects_deleted"] == len(failed_keys)
    assert not mem_store._objs
    async with session_factory() as session:
        assert await retained_used_bytes(session, owner_id) == 0
    # An already released job is safe to retry.
    repeated = await client.delete(f"/jobs/{job_id}/storage")
    assert repeated.status_code == 200
    assert repeated.json()["objects_deleted"] == 0


async def test_release_clears_reference_when_object_is_already_absent(client, session_factory):
    created = await client.post("/jobs", json={
        "name": "already-absent", "image": "dummy", "entrypoint": ["python", "train.py"],
    })
    assert created.status_code == 200, created.text
    job_id = created.json()["job_id"]
    async with session_factory() as session:
        job = await session.get(Job, job_id)
        job.input_object_key = f"inputs/{job_id}/already-gone"
        job.input_size_bytes = 4096
        run = await session.get(Run, created.json()["run_ids"][0])
        run.status = RunStatus.FAILED
        await session.commit()
    # Mirrors deletion succeeding but the process dying before committing its rows.
    response = await client.delete(f"/jobs/{job_id}/storage")
    assert response.status_code == 200, response.text
    assert response.json()["objects_deleted"] == 1
    async with session_factory() as session:
        job = await session.get(Job, job_id)
        assert job.input_object_key is None
        assert job.input_size_bytes is None
