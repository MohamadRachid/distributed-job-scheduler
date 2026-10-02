"""W4 dashboard tests — rich specs + live usage (supervisor-requested 2026-07-06).

The contract change is ADDITIVE (two optional fields, two nullable columns), so
the load-bearing assertions are:
  - a rich register stores `hw_specs` and GET /nodes returns it;
  - heartbeat `usage` is stored and returned; a heartbeat WITHOUT usage keeps
    the previous sample (older agents must not wipe the dashboard);
  - the OLD payload shapes (no hw_specs, no usage) still work unchanged —
    that is what makes this a safe edit to the frozen wall.
"""

SPECS = {"cpu_cores": 8, "has_gpu": False, "ram_mb": 16384, "capacity": 8, "agent_version": "0.4.0"}

HW = {
    "cpu_name": "AMD Ryzen 7 5800H with Radeon Graphics",
    "cpu_cores_physical": 8,
    "cpu_threads": 16,
    "gpu_name": "NVIDIA GeForce RTX 3060 Laptop GPU",
    "ram_mhz": 3200,
    "machine_model": "LENOVO 82JU",
    "os": "Windows-11-10.0.26200-SP0",
    "python_version": "3.12.4",
    "docker_version": "27.0.3",
    "disk_total_gb": 476.9,
}


async def _register(client, name="rich-node", specs=None):
    resp = await client.post("/agent/register", json={"name": name, "specs": specs or SPECS})
    assert resp.status_code == 200, resp.text
    return resp.json()


async def _heartbeat(client, node, usage=None):
    body = {"node_id": node["node_id"], "status": "idle", "running": []}
    if usage is not None:
        body["usage"] = usage
    resp = await client.post(
        "/agent/heartbeat",
        headers={"Authorization": f"Bearer {node['token']}"},
        json=body,
    )
    assert resp.status_code == 200, resp.text
    return resp.json()


async def _node(client, node_id):
    nodes = (await client.get("/nodes")).json()
    return next(n for n in nodes if n["node_id"] == node_id)


async def test_rich_register_exposes_hw_specs(client):
    node = await _register(client, specs={**SPECS, "hw_specs": HW})
    out = await _node(client, node["node_id"])
    assert out["hw_specs"] == HW
    assert out["agent_version"] == "0.4.0"


async def test_heartbeat_usage_stored_and_returned(client):
    node = await _register(client, specs={**SPECS, "hw_specs": HW})
    await _heartbeat(client, node, usage={"cpu_pct": 37.5, "ram_pct": 61.2, "disk_pct": 80.0})
    out = await _node(client, node["node_id"])
    assert out["usage"]["cpu_pct"] == 37.5
    assert out["usage"]["ram_pct"] == 61.2


async def test_heartbeat_without_usage_keeps_last_sample(client):
    node = await _register(client)
    await _heartbeat(client, node, usage={"cpu_pct": 50.0, "ram_pct": 40.0})
    # An old-style heartbeat (no usage key) must not wipe the stored sample.
    await _heartbeat(client, node)
    out = await _node(client, node["node_id"])
    assert out["usage"] == {"cpu_pct": 50.0, "ram_pct": 40.0}


async def test_old_register_shape_still_works(client):
    # The exact pre-W4 payload — no hw_specs anywhere. Must register fine and
    # read back with nulls, not errors (backward compatibility on the wall).
    node = await _register(client, name="old-agent", specs=SPECS)
    out = await _node(client, node["node_id"])
    assert out["hw_specs"] is None
    assert out["usage"] is None
    assert out["cpu_cores"] == 8
