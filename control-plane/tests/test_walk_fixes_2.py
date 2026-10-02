"""Control-plane fixes for what the stranger found, second batch (walk 1, 2026-09-07).

  row 11  the server's own log never prints a login token
  row 68  the advisor's evidence reads as words, not as unbalanced brackets
"""

import logging

from app.checkpoint_advisor import scan_text
from app.main import RedactToken


def _record(msg: str, *args) -> logging.LogRecord:
    return logging.LogRecord("uvicorn.error", logging.INFO, __file__, 1, msg, args, None)


def test_a_token_in_a_websocket_line_is_masked_before_it_is_written():
    line = _record(
        '%s - "WebSocket /runs/abc/logs?token=eyJhbGciOiJIUzI1NiJ9.payload.sig" [accepted]',
        "('172.19.0.1', 37658)",
    )
    assert RedactToken().filter(line) is True
    rendered = line.getMessage()
    assert "eyJhbGciOiJIUzI1NiJ9" not in rendered
    assert "token=<redacted>" in rendered
    assert "[accepted]" in rendered, "only the value is masked; the line keeps its shape"


def test_a_line_without_a_token_is_left_exactly_as_it_was():
    line = _record('%s - "POST /agent/heartbeat HTTP/1.1" 200 OK', "172.19.0.1:41002")
    before = line.getMessage()
    RedactToken().filter(line)
    assert line.getMessage() == before


def test_the_filter_is_attached_to_uvicorns_loggers():
    for name in ("uvicorn.access", "uvicorn.error"):
        assert any(isinstance(f, RedactToken) for f in logging.getLogger(name).filters), name


SPECS = {"cpu_cores": 4, "has_gpu": False, "ram_mb": 8192, "capacity": 4,
         "agent_version": "0.12.0"}
OLD_AGENT = dict(SPECS, agent_version="0.11.0")


async def _register(client, name, specs=None):
    r = await client.post("/agent/register", json={"name": name, "specs": specs or SPECS})
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


def _spec(**over):
    spec = {"name": "walk", "image": "fyp-dummy:latest",
            "entrypoint": ["python", "train.py"], "replicas": 1}
    spec.update(over)
    return spec


# --- rows 65 and 66: a worker whose agent is too old ---------------------------


async def test_the_node_list_says_when_an_agent_is_too_old_for_todays_jobs(client):
    old = await _register(client, "node-old", OLD_AGENT)
    new = await _register(client, "node-new")
    rows = {n["name"]: n for n in (await client.get("/nodes")).json()}
    assert rows["node-old"]["agent_outdated"] is True
    assert rows["node-old"]["min_agent_version"] == "0.12.0"
    assert rows["node-new"]["agent_outdated"] is False
    assert old["node_id"] != new["node_id"]


async def test_a_run_aimed_at_an_online_but_too_old_worker_says_what_it_is_waiting_for(client):
    """The machine is here — heart-beating, idle — and the run waited for ever with
    waiting_for null, because 'waiting for' only knew about silence. Row 66."""
    old = await _register(client, "node-old", OLD_AGENT)
    created = (await client.post("/jobs", json=_spec(target_node_ids=[old["node_id"]]))).json()
    assert await _heartbeat(client, old) == [], "a sealed run is never offered to it"
    rows = (await client.get(f"/jobs/{created['job_id']}/runs")).json()
    line = rows[0]["waiting_for"]
    assert line is not None, rows
    assert "node-old" in line and "0.11.0" in line and "0.12.0" in line
    assert "upgrade the agent" in line


async def test_a_run_aimed_at_a_current_worker_still_says_nothing(client):
    node = await _register(client, "node-new")
    created = (await client.post("/jobs", json=_spec(target_node_ids=[node["node_id"]]))).json()
    rows = (await client.get(f"/jobs/{created['job_id']}/runs")).json()
    assert rows[0]["waiting_for"] is None


# --- row 25: the finish line's own progress --------------------------------------


async def test_a_terminal_post_may_carry_the_last_progress_marker(client):
    node = await _register(client, "node-new")
    created = (await client.post("/jobs", json=_spec())).json()
    a = (await _heartbeat(client, node))[0]
    auth = {"Authorization": f"Bearer {node['token']}"}
    # The heartbeat's last picture: epoch 4 of 5.
    r = await client.post(
        "/agent/heartbeat", headers=auth,
        json={"node_id": node["node_id"], "status": "busy",
              "running": [{"run_id": a["run_id"], "attempt": a["attempt"], "state": "RUNNING",
                           "progress": 0.8, "metrics": {"epoch": 4, "total": 5, "loss": 0.25}}]},
    )
    assert r.status_code == 200
    # The terminal post carries the finish.
    r = await client.post(
        f"/agent/runs/{a['run_id']}/status", headers=auth,
        json={"attempt": a["attempt"], "state": "SUCCEEDED", "exit_code": 0,
              "progress": 1.0, "metrics": {"epoch": 5, "total": 5, "loss": 0.2}},
    )
    assert r.status_code == 200, r.text
    run = (await client.get(f"/jobs/{created['job_id']}/runs")).json()[0]
    assert run["progress"] == 1.0
    assert run["metrics_last"]["loss"] == 0.2


async def test_an_older_agents_terminal_post_leaves_the_heartbeats_values_standing(client):
    node = await _register(client, "node-new")
    created = (await client.post("/jobs", json=_spec())).json()
    a = (await _heartbeat(client, node))[0]
    auth = {"Authorization": f"Bearer {node['token']}"}
    await client.post(
        "/agent/heartbeat", headers=auth,
        json={"node_id": node["node_id"], "status": "busy",
              "running": [{"run_id": a["run_id"], "attempt": a["attempt"], "state": "RUNNING",
                           "progress": 0.8, "metrics": {"epoch": 4, "total": 5, "loss": 0.25}}]},
    )
    r = await client.post(
        f"/agent/runs/{a['run_id']}/status", headers=auth,
        json={"attempt": a["attempt"], "state": "SUCCEEDED", "exit_code": 0},
    )
    assert r.status_code == 200
    run = (await client.get(f"/jobs/{created['job_id']}/runs")).json()[0]
    assert run["progress"] == 0.8 and run["metrics_last"]["loss"] == 0.25


# --- row 52: a refusal in words ----------------------------------------------------


async def test_an_over_cap_temporary_disk_ask_is_refused_in_plain_words(client):
    r = await client.post("/jobs", json=_spec(resource_reqs={"scratch_mb": 10 ** 9}))
    assert r.status_code == 422, r.text
    detail = r.json()["detail"]
    assert detail.startswith("Temporary disk:")
    assert "scratch_mb" not in detail
    assert "Lower the number" in detail


def test_the_advisors_evidence_reads_as_words():
    """`.save(` and `.load(` are matched with their bracket so the scan finds a CALL;
    printed inside the reason's own brackets they read as `(.save()`. Row 68."""
    r = scan_text("import fyp_checkpoint\nstate = fyp_checkpoint.load()\n"
                  "if os.path.exists(p): pass\nfyp_checkpoint.save({'epoch': 1})\n")
    assert r["verdict"] == "resumes"
    assert "(.save()" not in r["reason"] and "(.load()" not in r["reason"]
    assert "saves (.save) and loads (os.path.exists, .load) at the checkpoint location (fyp_checkpoint)" == r["reason"]
    # The hit lists keep the raw tokens: the evidence is unchanged, only its display.
    assert ".save(" in r["save_hits"]
