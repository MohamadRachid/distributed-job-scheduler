"""W2 smoke tests — the Definition of Done for "one container, end to end", minus
the container itself (Docker is exercised live; the suite keeps locking/fencing
*logic* on SQLite and the SKIP-LOCKED *concurrency* proof on Postgres — see
test_w2_postgres.py).

Covers the control-plane half of the loop:
  - POST /jobs fans a job out into runs (per-node and per-replica, protocol.md §4);
  - heartbeat assigns a PENDING run at pull-time, setting ASSIGNED + attempt 0->1;
  - status reports drive PENDING->ASSIGNED->RUNNING->SUCCEEDED/FAILED + job rollup;
  - the fencing token: stale attempt, wrong node, unknown run, bad state all reject;
  - eligibility (GPU / RAM / target node) and per-node capacity gate assignment.
"""

SPECS = {"cpu_cores": 4, "has_gpu": False, "ram_mb": 8192, "capacity": 4, "agent_version": "0.12.0"}


# --- helpers ----------------------------------------------------------------


async def _register(client, name="lab-pc-01", specs=None):
    resp = await client.post("/agent/register", json={"name": name, "specs": specs or SPECS})
    assert resp.status_code == 200, resp.text
    return resp.json()


async def _heartbeat(client, node, status="idle", running=None):
    resp = await client.post(
        "/agent/heartbeat",
        headers={"Authorization": f"Bearer {node['token']}"},
        json={"node_id": node["node_id"], "status": status, "running": running or []},
    )
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


async def _runs(client, job_id):
    resp = await client.get(f"/jobs/{job_id}/runs")
    assert resp.status_code == 200, resp.text
    return resp.json()


async def _status(client, node, run_id, attempt, state, exit_code=None):
    return await client.post(
        f"/agent/runs/{run_id}/status",
        headers={"Authorization": f"Bearer {node['token']}"},
        json={"attempt": attempt, "state": state, "exit_code": exit_code},
    )


# --- job fan-out (protocol.md §4) -------------------------------------------


async def test_job_fans_out_to_replicas(client):
    job = await _create_job(client, replicas=3, target_node_ids=None)
    assert len(job["run_ids"]) == 3
    runs = await _runs(client, job["job_id"])
    assert {r["status"] for r in runs} == {"PENDING"}


async def test_job_one_run_per_target_node(client):
    job = await _create_job(client, target_node_ids=["n1", "n2"], replicas=9)
    # target_node_ids set -> one run per node, replicas ignored.
    assert len(job["run_ids"]) == 2


# --- pull-time assignment + the happy path ----------------------------------


async def test_heartbeat_assigns_pending_run(client):
    node = await _register(client)
    job = await _create_job(client)

    work = await _heartbeat(client, node)
    assert len(work["assignments"]) == 1
    a = work["assignments"][0]
    assert a["image"] == "fyp-dummy:latest"
    assert a["entrypoint"] == ["python", "train.py"]
    assert a["env"] == {"EPOCHS": "1"}
    assert a["attempt"] == 1                      # fencing token 0 -> 1 on assign

    runs = await _runs(client, job["job_id"])
    assert runs[0]["status"] == "ASSIGNED"
    assert runs[0]["node_id"] == node["node_id"]
    assert runs[0]["attempt"] == 1


async def test_run_reaches_succeeded_and_rolls_job_up(client):
    node = await _register(client)
    job = await _create_job(client)
    a = (await _heartbeat(client, node))["assignments"][0]
    rid = a["run_id"]

    r = await _status(client, node, rid, 1, "RUNNING")
    assert r.status_code == 200, r.text
    assert (await _runs(client, job["job_id"]))[0]["status"] == "RUNNING"
    assert (await client.get(f"/jobs/{job['job_id']}")).json()["status"] == "RUNNING"

    r = await _status(client, node, rid, 1, "SUCCEEDED", exit_code=0)
    assert r.status_code == 200, r.text
    run = (await _runs(client, job["job_id"]))[0]
    assert run["status"] == "SUCCEEDED"
    assert run["exit_code"] == 0
    assert run["finished_at"] is not None
    assert (await client.get(f"/jobs/{job['job_id']}")).json()["status"] == "SUCCEEDED"


async def test_nonzero_exit_marks_run_and_job_failed(client):
    node = await _register(client)
    job = await _create_job(client)
    rid = (await _heartbeat(client, node))["assignments"][0]["run_id"]

    await _status(client, node, rid, 1, "RUNNING")
    r = await _status(client, node, rid, 1, "FAILED", exit_code=1)
    assert r.status_code == 200, r.text
    run = (await _runs(client, job["job_id"]))[0]
    assert run["status"] == "FAILED"
    assert run["exit_code"] == 1
    assert (await client.get(f"/jobs/{job['job_id']}")).json()["status"] == "FAILED"


async def test_terminal_status_is_idempotent(client):
    node = await _register(client)
    await _create_job(client)
    rid = (await _heartbeat(client, node))["assignments"][0]["run_id"]
    await _status(client, node, rid, 1, "SUCCEEDED", exit_code=0)
    # repeating the same terminal report (same attempt) is accepted, not an error.
    r = await _status(client, node, rid, 1, "SUCCEEDED", exit_code=0)
    assert r.status_code == 200, r.text


# --- fencing + error conventions (protocol.md §6, §11) ----------------------


async def test_stale_attempt_rejected_409(client):
    node = await _register(client)
    await _create_job(client)
    rid = (await _heartbeat(client, node))["assignments"][0]["run_id"]
    # current attempt is 1; a report for attempt 2 is from a stale/other execution.
    r = await _status(client, node, rid, 2, "SUCCEEDED", exit_code=0)
    assert r.status_code == 409, r.text


async def test_status_for_unknown_run_404(client):
    node = await _register(client)
    r = await _status(client, node, "does-not-exist", 1, "RUNNING")
    assert r.status_code == 404


async def test_status_from_wrong_node_409(client):
    owner = await _register(client, name="owner")
    other = await _register(client, name="other")
    await _create_job(client)
    rid = (await _heartbeat(client, owner))["assignments"][0]["run_id"]
    # 'other' never owned this run -> fencing rejects it.
    r = await _status(client, other, rid, 1, "RUNNING")
    assert r.status_code == 409


async def test_invalid_state_value_422(client):
    node = await _register(client)
    await _create_job(client)
    rid = (await _heartbeat(client, node))["assignments"][0]["run_id"]
    r = await _status(client, node, rid, 1, "PAUSED")  # not a valid state
    assert r.status_code == 422


# --- eligibility + capacity (protocol.md §4) --------------------------------


async def test_gpu_requirement_excludes_cpu_only_node(client):
    node = await _register(client)  # has_gpu False
    job = await _create_job(client, resource_reqs={"needs_gpu": True})
    work = await _heartbeat(client, node)
    assert work["assignments"] == []
    assert (await _runs(client, job["job_id"]))[0]["status"] == "PENDING"


async def test_ram_requirement_excludes_small_node(client):
    node = await _register(client)  # 8192 MB
    await _create_job(client, resource_reqs={"min_ram_mb": 999999})
    work = await _heartbeat(client, node)
    assert work["assignments"] == []


async def test_target_node_mismatch_excludes_node(client):
    node = await _register(client)
    job = await _create_job(client, target_node_ids=["some-other-node"])
    work = await _heartbeat(client, node)
    assert work["assignments"] == []
    # the run targeting another node stays PENDING for that node to claim.
    assert (await _runs(client, job["job_id"]))[0]["status"] == "PENDING"


async def test_capacity_limits_assignments_per_node(client):
    node = await _register(
        client, name="cap1",
        specs={"cpu_cores": 1, "has_gpu": False, "ram_mb": 8192, "capacity": 1, "agent_version": "0.12.0"},
    )
    job = await _create_job(client, replicas=2, target_node_ids=None)

    first = await _heartbeat(client, node)
    assert len(first["assignments"]) == 1          # capacity 1 -> at most one

    # the assigned run still occupies the node (ASSIGNED), so a second heartbeat
    # gets nothing until it finishes.
    second = await _heartbeat(client, node, running=[
        {"run_id": first["assignments"][0]["run_id"], "attempt": 1, "state": "RUNNING"}
    ])
    assert second["assignments"] == []
    statuses = sorted(r["status"] for r in await _runs(client, job["job_id"]))
    assert statuses == ["ASSIGNED", "PENDING"]
