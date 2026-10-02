"""Control-plane fixes for what the stranger found (walk 1, 2026-09-07).

Each test names the row of `docs/evidence/walkthrough_1_2026-09-07.txt` it closes,
and each fails on the tree the walk was taken on.

  row 35  the job page says what input the job carried and what it was charged
  row 58  a sealed job whose container never opened its dataset is told to use the
          reader, by name, and nothing about "private"
"""

import json

from app.diagnostics import (
    INPUT_NOT_OPENED,
    PRIVATE_INPUT_NOT_OPENED,
    decide_private_input_outcome,
)

SPECS = {"cpu_cores": 4, "has_gpu": False, "ram_mb": 8192, "capacity": 4,
         "agent_version": "0.12.0"}
PLAIN = b"id,x\n" + b"".join(b"%d,%d\n" % (i, i * 2) for i in range(2000))


def _spec(**over):
    spec = {"name": "walk", "image": "fyp-dummy:latest",
            "entrypoint": ["python", "train.py"], "replicas": 1}
    spec.update(over)
    return spec


async def _register(client, name="lab-pc-01"):
    r = await client.post("/agent/register", json={"name": name, "specs": SPECS})
    assert r.status_code == 200, r.text
    return r.json()


async def _heartbeat(client, node):
    r = await client.post(
        "/agent/heartbeat",
        headers={"Authorization": f"Bearer {node['token']}"},
        json={"node_id": node["node_id"], "status": "idle", "running": []},
    )
    assert r.status_code == 200, r.text
    return r.json()["assignments"]


async def _submit_with_file(client, filename="data.csv", data=PLAIN, **over):
    r = await client.post(
        "/jobs/with-input",
        data={"spec": json.dumps(_spec(**over))},
        files={"file": (filename, data, "text/csv")},
    )
    assert r.status_code == 200, r.text
    return r.json()


# --- row 35 -----------------------------------------------------------------


async def test_the_job_page_says_what_input_it_carried_and_what_it_was_charged(client):
    """The stranger could not tell which jobs carried a file: six later jobs carried
    one nobody had attached, and the only place that said so was the API's
    `input_filename`. The read shape now carries the sealed size too — the number the
    storage bar counts — so the page can say both."""
    created = await _submit_with_file(client)
    job = (await client.get(f"/jobs/{created['job_id']}")).json()
    assert job["input_filename"] == "data.csv"
    assert job["input_size_bytes"] is not None
    # Sealed bytes are a little larger than the plaintext: header plus one tag.
    assert job["input_size_bytes"] > len(PLAIN)

    plain = (await client.post("/jobs", json=_spec())).json()
    job2 = (await client.get(f"/jobs/{plain['job_id']}")).json()
    assert job2["input_filename"] is None
    assert job2["input_size_bytes"] is None


# --- row 58 -----------------------------------------------------------------


def test_a_sealed_job_that_never_opened_its_dataset_is_told_to_use_the_reader():
    reason, detail = decide_private_input_outcome(
        is_private=True, input_opened=False, sealed=True
    )
    assert reason == INPUT_NOT_OPENED
    assert "fyp_data.open_input()" in detail
    assert "private" not in detail.lower()
    assert "fyp_open" not in detail
    # The old shape keeps the message that names its own opener.
    old_reason, old_detail = decide_private_input_outcome(
        is_private=True, input_opened=False, sealed=False
    )
    assert old_reason == PRIVATE_INPUT_NOT_OPENED
    assert "fyp_open.py" in old_detail
    # And a job that did open its file, or carried none, still says nothing.
    assert decide_private_input_outcome(is_private=True, input_opened=True, sealed=True) == (None, None)
    assert decide_private_input_outcome(is_private=False, input_opened=False, sealed=True) == (None, None)


async def test_the_status_route_labels_a_sealed_run_that_read_its_file_with_open(client):
    """End to end on the door itself: a sealed job with a dataset, run to SUCCEEDED
    without its container ever redeeming the key ticket, carries INPUT_NOT_OPENED
    and the reader's name — never the old private label."""
    created = await _submit_with_file(client)
    node = await _register(client)
    assignments = await _heartbeat(client, node)
    assert len(assignments) == 1 and assignments[0]["sealed"] is True
    run_id, attempt = assignments[0]["run_id"], assignments[0]["attempt"]
    auth = {"Authorization": f"Bearer {node['token']}"}
    # The agent takes a ticket, as it always does before the container starts...
    r = await client.post(f"/agent/runs/{run_id}/key-ticket?attempt={attempt}", headers=auth)
    assert r.status_code == 200, r.text
    # ...and the container never spends it.
    for state in ("RUNNING", "SUCCEEDED"):
        r = await client.post(
            f"/agent/runs/{run_id}/status", headers=auth,
            json={"attempt": attempt, "state": state, "exit_code": 0 if state != "RUNNING" else None},
        )
        assert r.status_code == 200, r.text
    runs = (await client.get(f"/jobs/{created['job_id']}/runs")).json()
    assert runs[0]["status"] == "SUCCEEDED"
    assert runs[0]["failure_reason"] == INPUT_NOT_OPENED
    assert "fyp_data.open_input()" in runs[0]["failure_detail"]
    assert "private" not in runs[0]["failure_detail"].lower()
