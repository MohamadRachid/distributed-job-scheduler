"""W4 smoke tests — parallel dispatch + matching, minus the containers (the
three-agents-live proof runs against real Docker; these tests pin the *logic*).

The W4 DoD is "one job targeting three nodes runs in parallel". The scheduler
piece that makes that true is the pinning rule (scheduler._holds_sibling_run):
for a targeted job, each selected node gets exactly ONE of the job's runs —
even if one node has the spare capacity to take them all.

Covers:
  - a targeted job spreads one-run-per-node across heartbeats (never two to one
    node, whichever order nodes pull in);
  - a single greedy heartbeat (capacity >> runs) still takes at most one;
  - a node that already ran its share of a targeted job gets no sibling later;
  - untargeted replicas PREFER a machine that holds no sibling;
  - ...and share one machine anyway when no other eligible machine is free;
  - hardware matching in a mixed pool: a GPU job skips the CPU node that pulls
    first and lands on the GPU node;
  - the DoD end-to-end at logic level: 3 nodes, 3 runs in flight in parallel,
    all SUCCEEDED -> job SUCCEEDED.
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


async def _three_nodes(client):
    return [await _register(client, name=f"w4-node-{c}") for c in "abc"]


# --- the pinning rule (one run per selected node) ----------------------------


async def test_targeted_job_spreads_one_run_per_node(client):
    a, b, c = await _three_nodes(client)
    job = await _create_job(
        client, target_node_ids=[a["node_id"], b["node_id"], c["node_id"]]
    )
    assert len(job["run_ids"]) == 3

    # Every node has capacity 4 — each still gets exactly one run.
    for node in (a, b, c):
        work = await _heartbeat(client, node)
        assert len(work["assignments"]) == 1

    runs = await _runs(client, job["job_id"])
    assert {r["status"] for r in runs} == {"ASSIGNED"}
    assert sorted(r["node_id"] for r in runs) == sorted(
        n["node_id"] for n in (a, b, c)
    )  # one run per selected node, no node doubled


async def test_greedy_heartbeat_takes_at_most_one_targeted_run(client):
    a = await _register(client, name="w4-greedy")  # capacity 4, spare 4
    b = await _register(client, name="w4-later")
    job = await _create_job(client, target_node_ids=[a["node_id"], b["node_id"]])

    # One pull with spare capacity for both runs — must still claim only one.
    work = await _heartbeat(client, a)
    assert len(work["assignments"]) == 1
    statuses = sorted(r["status"] for r in await _runs(client, job["job_id"]))
    assert statuses == ["ASSIGNED", "PENDING"]  # the other waits for node b


async def test_finished_node_gets_no_sibling_run(client):
    a = await _register(client, name="w4-done")
    b = await _register(client, name="w4-absent")
    job = await _create_job(client, target_node_ids=[a["node_id"], b["node_id"]])

    rid = (await _heartbeat(client, a))["assignments"][0]["run_id"]
    await _status(client, a, rid, 1, "RUNNING")
    r = await _status(client, a, rid, 1, "SUCCEEDED", exit_code=0)
    assert r.status_code == 200, r.text

    # a is idle again with full capacity, but it already ran its share of this
    # job — the remaining run stays PENDING for b, it never doubles up on a.
    work = await _heartbeat(client, a)
    assert work["assignments"] == []
    statuses = sorted(r["status"] for r in await _runs(client, job["job_id"]))
    assert statuses == ["PENDING", "SUCCEEDED"]


async def test_untargeted_replicas_prefer_a_machine_without_a_sibling(client):
    """Three idle eligible machines, replicas=3 -> one run each, never three on
    the first machine to ask.

    This supersedes W4's position that untargeted replicas may share: three
    replicas stacked on one machine while two sit idle defeats FR-5 (dispatch in
    parallel) and NFR-2 (use the pool). The preference is all that is added —
    nothing scores one machine against another, so which machine gets which run
    is still whoever asks first."""
    a = await _register(client, name="w4-spread-a")  # capacity 4 — could take all 3
    b = await _register(client, name="w4-spread-b")
    c = await _register(client, name="w4-spread-c")
    job = await _create_job(client, replicas=3, target_node_ids=None)

    # a has the spare capacity for all three and still takes exactly one,
    # because b and c are online, eligible and free.
    assert len((await _heartbeat(client, a))["assignments"]) == 1
    assert len((await _heartbeat(client, b))["assignments"]) == 1
    assert len((await _heartbeat(client, c))["assignments"]) == 1

    runs = await _runs(client, job["job_id"])
    assert len(runs) == 3
    assert {r["node_id"] for r in runs} == {
        a["node_id"], b["node_id"], c["node_id"]
    }


async def test_untargeted_replicas_share_one_machine_when_nobody_else_is_free(client):
    """The other half of the same rule: the preference must never leave work
    unstarted. One eligible machine, replicas=3 -> all three land on it, in one
    heartbeat, exactly as before the change."""
    node = await _register(client, name="w4-solo")  # capacity 4
    job = await _create_job(client, replicas=3, target_node_ids=None)

    work = await _heartbeat(client, node)
    assert len(work["assignments"]) == 3
    runs = await _runs(client, job["job_id"])
    assert {r["node_id"] for r in runs} == {node["node_id"]}


# --- hardware matching in a mixed pool ---------------------------------------


async def test_gpu_job_skips_cpu_node_and_lands_on_gpu_node(client):
    cpu = await _register(client, name="w4-cpu")  # has_gpu False
    gpu = await _register(
        client, name="w4-gpu",
        specs={"cpu_cores": 8, "has_gpu": True, "ram_mb": 16384, "capacity": 4, "agent_version": "0.12.0"},
    )
    job = await _create_job(client, resource_reqs={"needs_gpu": True})

    # The CPU node pulls first — it must not receive the GPU job.
    assert (await _heartbeat(client, cpu))["assignments"] == []
    work = await _heartbeat(client, gpu)
    assert len(work["assignments"]) == 1
    assert (await _runs(client, job["job_id"]))[0]["node_id"] == gpu["node_id"]


# --- the W4 DoD at logic level ------------------------------------------------


async def test_three_targeted_runs_in_parallel_to_succeeded(client):
    nodes = await _three_nodes(client)
    job = await _create_job(
        client, target_node_ids=[n["node_id"] for n in nodes]
    )

    # Each node claims its run and reports RUNNING -> all three in flight at once.
    claimed = []
    for node in nodes:
        a = (await _heartbeat(client, node))["assignments"][0]
        await _status(client, node, a["run_id"], a["attempt"], "RUNNING")
        claimed.append((node, a))
    running = await _runs(client, job["job_id"])
    assert [r["status"] for r in running] == ["RUNNING"] * 3   # parallel, no serialization
    assert (await client.get(f"/jobs/{job['job_id']}")).json()["status"] == "RUNNING"

    # All finish -> the rollup closes the job as SUCCEEDED.
    for node, a in claimed:
        r = await _status(client, node, a["run_id"], a["attempt"], "SUCCEEDED", exit_code=0)
        assert r.status_code == 200, r.text
    assert (await client.get(f"/jobs/{job['job_id']}")).json()["status"] == "SUCCEEDED"
