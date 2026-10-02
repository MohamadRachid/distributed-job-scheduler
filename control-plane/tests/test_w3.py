"""W3 smoke tests — the log path (ingest + catch-up read), minus the container and
the WebSocket (both exercised live; the WS is the same rows over a socket).

Covers the control-plane half of "submit from the browser + live logs":
  - POST /agent/runs/{id}/logs stores a chunk and it reads back;
  - a resent (run_id, attempt, seq) is an idempotent 200, not a duplicate row
    (the DB UNIQUE constraint is the guarantee);
  - the same fencing as status: stale attempt -> 409, wrong node -> 409, unknown -> 404;
  - GET /runs/{id}/logs?since_seq=N returns chunks with seq > N, ordered, regardless
    of the order they were posted in (the W3 reliability point, brief §4).

Reuses the W2 flow (register -> create job -> heartbeat assigns) to get an owned,
attempt-1 run to attach logs to.
"""

SPECS = {"cpu_cores": 4, "has_gpu": False, "ram_mb": 8192, "capacity": 4, "agent_version": "0.12.0"}


# --- helpers ----------------------------------------------------------------


async def _register(client, name="lab-pc-01", specs=None):
    resp = await client.post("/agent/register", json={"name": name, "specs": specs or SPECS})
    assert resp.status_code == 200, resp.text
    return resp.json()


async def _create_job(client, **overrides):
    body = {
        "name": "j",
        "image": "fyp-dummy:latest",
        "entrypoint": ["python", "train.py"],
        "env": {"EPOCHS": "1"},
        "resource_reqs": {"needs_gpu": False},
        "replicas": 1,
    }
    body.update(overrides)
    resp = await client.post("/jobs", json=body)
    assert resp.status_code == 200, resp.text
    return resp.json()


async def _assigned_run(client, node):
    """Register-independent: submit a job and heartbeat `node` to claim its run.
    Returns the assigned run_id (attempt 1, owned by `node`)."""
    await _create_job(client)
    resp = await client.post(
        "/agent/heartbeat",
        headers={"Authorization": f"Bearer {node['token']}"},
        json={"node_id": node["node_id"], "status": "idle", "running": []},
    )
    assert resp.status_code == 200, resp.text
    return resp.json()["assignments"][0]["run_id"]


async def _post_log(client, node, run_id, attempt, seq, chunk):
    return await client.post(
        f"/agent/runs/{run_id}/logs",
        headers={"Authorization": f"Bearer {node['token']}"},
        json={"attempt": attempt, "seq": seq, "chunk": chunk},
    )


async def _get_logs(client, run_id, since_seq=-1):
    resp = await client.get(f"/runs/{run_id}/logs", params={"since_seq": since_seq})
    assert resp.status_code == 200, resp.text
    return resp.json()


# --- ingest + read ----------------------------------------------------------


async def test_log_accepted_and_readable(client):
    node = await _register(client)
    rid = await _assigned_run(client, node)

    r = await _post_log(client, node, rid, 1, 0, "epoch 1/5 loss=1.0\n")
    assert r.status_code == 200, r.text
    assert r.json() == {"accepted": True, "deduped": False}

    rows = await _get_logs(client, rid)
    assert len(rows) == 1
    assert rows[0]["seq"] == 0
    assert rows[0]["attempt"] == 1
    assert rows[0]["chunk"] == "epoch 1/5 loss=1.0\n"
    assert rows[0]["run_id"] == rid


async def test_duplicate_chunk_is_idempotent_no_new_row(client):
    node = await _register(client)
    rid = await _assigned_run(client, node)

    first = await _post_log(client, node, rid, 1, 0, "line a\n")
    assert first.status_code == 200 and first.json()["deduped"] is False

    # Same (run_id, attempt, seq) resent (an agent retry). The DB UNIQUE constraint
    # blocks a second row; the endpoint reports an idempotent, deduped 200.
    again = await _post_log(client, node, rid, 1, 0, "line a\n")
    assert again.status_code == 200, again.text
    assert again.json() == {"accepted": True, "deduped": True}

    rows = await _get_logs(client, rid)
    assert len(rows) == 1  # still exactly one row


# --- fencing (same rules as the status endpoint, protocol.md §9/§11) ---------


async def test_stale_attempt_log_rejected_409(client):
    node = await _register(client)
    rid = await _assigned_run(client, node)  # current attempt is 1
    r = await _post_log(client, node, rid, 2, 0, "from a stale execution\n")
    assert r.status_code == 409, r.text


async def test_log_from_wrong_node_rejected_409(client):
    owner = await _register(client, name="owner")
    other = await _register(client, name="other")
    rid = await _assigned_run(client, owner)
    # 'other' never owned this run -> fencing rejects its log post.
    r = await _post_log(client, other, rid, 1, 0, "not my run\n")
    assert r.status_code == 409, r.text


async def test_log_for_unknown_run_404(client):
    node = await _register(client)
    r = await _post_log(client, node, "does-not-exist", 1, 0, "x\n")
    assert r.status_code == 404, r.text


async def test_log_without_token_401(client):
    node = await _register(client)
    rid = await _assigned_run(client, node)
    r = await client.post(
        f"/agent/runs/{rid}/logs", json={"attempt": 1, "seq": 0, "chunk": "x\n"}
    )
    assert r.status_code == 401, r.text


# --- since_seq read ordering (brief §4) -------------------------------------


async def test_since_seq_returns_ordered_new_chunks(client):
    node = await _register(client)
    rid = await _assigned_run(client, node)

    # Post OUT OF ORDER on purpose — the reader must still return them by seq.
    await _post_log(client, node, rid, 1, 2, "third\n")
    await _post_log(client, node, rid, 1, 0, "first\n")
    await _post_log(client, node, rid, 1, 1, "second\n")

    all_rows = await _get_logs(client, rid, since_seq=-1)
    assert [r["seq"] for r in all_rows] == [0, 1, 2]
    assert [r["chunk"] for r in all_rows] == ["first\n", "second\n", "third\n"]

    # A poller that has seen up to seq 0 asks for the rest.
    tail = await _get_logs(client, rid, since_seq=0)
    assert [r["seq"] for r in tail] == [1, 2]


async def test_get_logs_unknown_run_404(client):
    r = await client.get("/runs/nope/logs", params={"since_seq": -1})
    assert r.status_code == 404, r.text
