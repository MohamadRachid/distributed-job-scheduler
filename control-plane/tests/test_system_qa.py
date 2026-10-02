"""Regression probes from the full-system QA on 2026-09-07."""

import pytest
from conftest import seal_for_run

from app.models import Run, RunStatus
from tests.test_w6b import _claimed_private, _node_auth


@pytest.mark.parametrize("status", ["SUCCEEDED", "FAILED", "LOST", "PENDING"])
async def test_finished_or_requeued_run_cannot_issue_key_ticket(client, session_factory, status):
    node, assignment = await _claimed_private(client)
    async with session_factory() as session:
        run = await session.get(Run, assignment["run_id"])
        run.status = RunStatus(status)
        await session.commit()
    response = await client.post(
        f"/agent/runs/{assignment['run_id']}/key-ticket?attempt={assignment['attempt']}",
        headers=_node_auth(node),
    )
    assert response.status_code == 409, response.text


@pytest.mark.parametrize("status", ["SUCCEEDED", "FAILED", "LOST", "PENDING"])
async def test_ticket_cannot_be_redeemed_after_run_stops(client, session_factory, status):
    node, assignment = await _claimed_private(client)
    issued = await client.post(
        f"/agent/runs/{assignment['run_id']}/key-ticket?attempt={assignment['attempt']}",
        headers=_node_auth(node),
    )
    assert issued.status_code == 200
    async with session_factory() as session:
        run = await session.get(Run, assignment["run_id"])
        run.status = RunStatus(status)
        await session.commit()
    response = await client.post("/container/key", json={"ticket": issued.json()["ticket"]})
    # Do not print the response: a failure would expose the test job's key.
    assert response.status_code == 409


@pytest.mark.parametrize("kind", ["result", "checkpoint"])
async def test_finished_run_cannot_replace_stored_output(client, session_factory, mem_store, kind):
    node, assignment = await _claimed_private(client)
    path = f"/agent/runs/{assignment['run_id']}/artifacts"
    fields = {"attempt": assignment["attempt"], "filename": "output.bin", "kind": kind}
    original_bytes = await seal_for_run(session_factory, assignment["run_id"], b"original")
    replacement_bytes = await seal_for_run(session_factory, assignment["run_id"], b"replacement")
    first = await client.post(path, headers=_node_auth(node), data=fields,
                              files={"file": ("output.bin", original_bytes)})
    assert first.status_code == 200, first.text
    key = first.json()["object_key"]
    original = mem_store.get_object(key)
    async with session_factory() as session:
        run = await session.get(Run, assignment["run_id"])
        run.status = RunStatus.SUCCEEDED
        await session.commit()
    changed = await client.post(path, headers=_node_auth(node), data=fields,
                                files={"file": ("output.bin", replacement_bytes)})
    assert changed.status_code == 409
    assert mem_store.get_object(key) == original
