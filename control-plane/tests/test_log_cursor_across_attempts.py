"""The log read cursor across a re-dispatch — a KNOWN LIMITATION, pinned.

This file exists because nothing tested it. `seq` is a per-(run, attempt) counter,
so a recovered run's second attempt restarts at seq 0, while
`GET /runs/{id}/logs?since_seq=N` filters on `seq > N` and never looks at the
attempt. A poller that has already reached seq 4 on attempt 1 therefore asks for
`seq > 4` and gets nothing back from an attempt 2 that is sitting at seq 0-2.

Nothing here is a fix. These tests describe the behaviour that exists today, so
that it cannot drift silently and so the limitation the final report publishes has
a test behind it rather than a sentence.

  * The stored rows are complete and correctly ordered - the defect is in the read
    cursor, not in the data. A reader with no cursor sees everything.
  * The user-visible fix shipped on 2026-08-12 is client-side: the browser watches
    the run's attempt number and re-reads the log from the start when it rises
    (`web/src/components/RunLogs.jsx`). The server-side cursor is unchanged, and
    the reason is published in FINAL_REPORT_MASTER.md's limitations list.

Scenario driven through the real handlers: node A claims the run and posts five
chunks, the lease lapses, the reaper requeues, node B claims (attempt 1 -> 2) and
posts three chunks at seq 0-2.
"""

from datetime import datetime, timedelta, timezone

from app.models import Run
from app.reaper import sweep_once

NOW = datetime(2026, 8, 12, 12, 0, 0, tzinfo=timezone.utc)
PAST = NOW - timedelta(seconds=30)

SPECS = {"cpu_cores": 4, "has_gpu": False, "ram_mb": 8192, "capacity": 4, "agent_version": "0.12.0"}


async def _register(client, name):
    resp = await client.post("/agent/register", json={"name": name, "specs": SPECS})
    assert resp.status_code == 200, resp.text
    return resp.json()


async def _heartbeat(client, node):
    resp = await client.post(
        "/agent/heartbeat",
        headers={"Authorization": f"Bearer {node['token']}"},
        json={"node_id": node["node_id"], "status": "idle", "running": []},
    )
    assert resp.status_code == 200, resp.text
    return resp.json()["assignments"]


async def _post_log(client, node, run_id, attempt, seq, chunk):
    resp = await client.post(
        f"/agent/runs/{run_id}/logs",
        headers={"Authorization": f"Bearer {node['token']}"},
        json={"attempt": attempt, "seq": seq, "chunk": chunk},
    )
    assert resp.status_code == 200, resp.text
    return resp.json()


async def _two_attempt_run(client, session_factory):
    """A run that ran twice: attempt 1 wrote seq 0-4, attempt 2 wrote seq 0-2.

    Returns (run_id, node_b). The re-dispatch goes through the real reaper and the
    real claim, so `attempt` is bumped by the claim exactly as it is in production.
    """
    node_a = await _register(client, "log-cursor-node-a")
    node_b = await _register(client, "log-cursor-node-b")

    # Untargeted on purpose: a targeted job pins one run per selected node (W4), so
    # node B could never pick up node A's run and there would be no second attempt.
    resp = await client.post(
        "/jobs",
        json={
            "name": "two-attempt",
            "image": "fyp-dummy:latest",
            "entrypoint": ["python", "train.py"],
            "env": {"EPOCHS": "1"},
            "resource_reqs": {"needs_gpu": False},
            "replicas": 1,
        },
    )
    assert resp.status_code == 200, resp.text

    run_id = (await _heartbeat(client, node_a))[0]["run_id"]
    for seq in range(5):
        await _post_log(client, node_a, run_id, 1, seq, f"a-{seq}\n")

    # Node A goes silent: expire its lease, then let the REAL reaper requeue it.
    async with session_factory() as s:
        run = await s.get(Run, run_id)
        run.lease_expires_at = PAST
        await s.commit()

    decisions = await sweep_once(session_factory=session_factory, now=NOW)
    assert decisions and decisions[0]["decision"] == "requeued", decisions

    assignments = await _heartbeat(client, node_b)
    assert [a["run_id"] for a in assignments] == [run_id]
    async with session_factory() as s:
        assert (await s.get(Run, run_id)).attempt == 2

    for seq in range(3):
        await _post_log(client, node_b, run_id, 2, seq, f"b-{seq}\n")

    return run_id, node_b


async def test_log_cursor_is_not_attempt_scoped(client, session_factory):
    """THE DEFECT, pinned. A poller whose cursor reached seq 4 on attempt 1 asks
    for `seq > 4` and receives ZERO of attempt 2's three chunks, because the read
    filters on seq alone. This is why the live log view went blank during the
    recovery demo before the browser-side fix."""
    run_id, _ = await _two_attempt_run(client, session_factory)

    resp = await client.get(f"/runs/{run_id}/logs", params={"since_seq": 4})
    assert resp.status_code == 200, resp.text
    assert resp.json() == [], (
        "the server cursor is attempt-blind by design today; if this now returns "
        "attempt 2's chunks the server was changed and the report's limitation "
        "entry plus the browser-side re-read must be revisited together"
    )


async def test_cursorless_read_returns_every_chunk_of_both_attempts(
    client, session_factory
):
    """The storage is intact. A reader with no cursor - which is what a page reload
    and the browser-side fix both do - gets all eight chunks."""
    run_id, _ = await _two_attempt_run(client, session_factory)

    resp = await client.get(f"/runs/{run_id}/logs", params={"since_seq": -1})
    assert resp.status_code == 200, resp.text
    rows = resp.json()
    assert len(rows) == 8, rows


async def test_cursorless_read_is_ordered_by_attempt_then_seq(
    client, session_factory
):
    """Ordering is `(attempt, seq)` and NOT `seq` alone. If it were seq alone the
    two attempts would interleave - b-0 between a-0 and a-1 - and a reload would
    show a scrambled log. Checked rather than assumed."""
    run_id, _ = await _two_attempt_run(client, session_factory)

    rows = (
        await client.get(f"/runs/{run_id}/logs", params={"since_seq": -1})
    ).json()
    assert [(r["attempt"], r["seq"]) for r in rows] == [
        (1, 0), (1, 1), (1, 2), (1, 3), (1, 4), (2, 0), (2, 1), (2, 2),
    ]
    assert [r["chunk"] for r in rows] == [
        "a-0\n", "a-1\n", "a-2\n", "a-3\n", "a-4\n", "b-0\n", "b-1\n", "b-2\n",
    ]
