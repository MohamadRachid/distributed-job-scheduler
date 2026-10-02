"""W1 smoke test — encodes the Definition of Done (brief §9).

Covers:
  - register -> node appears -> GET /nodes shows it online;
  - heartbeat updates reported status, returns empty assignments/commands;
  - bad / missing node token -> 401;
  - backdated last_heartbeat (> NODE_TIMEOUT_S) -> liveness offline (derived).
"""

from datetime import datetime, timedelta, timezone

from sqlalchemy import select

from app.models import Node

SPECS = {"cpu_cores": 4, "has_gpu": False, "ram_mb": 8192, "capacity": 4, "agent_version": "0.12.0"}


async def _register(client, name="lab-pc-01", specs=None):
    resp = await client.post(
        "/agent/register", json={"name": name, "specs": specs or SPECS}
    )
    assert resp.status_code == 200, resp.text
    return resp.json()


async def test_health_ok(client):
    resp = await client.get("/health")
    assert resp.status_code == 200
    # W7a added `experiment_mode` to the payload (the harness asserts on it before
    # every measurement), so this is no longer an exact-dict check. The W1 contract
    # — a 200 saying "ok" — is unchanged, and test_w7a pins the new field.
    assert resp.json()["status"] == "ok"


async def test_register_then_node_shows_online(client):
    body = await _register(client)
    assert body["node_id"]
    assert body["token"]

    nodes = (await client.get("/nodes")).json()
    assert len(nodes) == 1
    node = nodes[0]
    assert node["name"] == "lab-pc-01"
    assert node["online"] is True          # derived from fresh last_heartbeat
    assert node["reported_status"] == "idle"
    assert node["cpu_cores"] == 4
    assert node["capacity"] == 4


async def test_register_defaults_capacity_to_cpu_cores(client):
    body = await _register(
        client, name="no-cap", specs={"cpu_cores": 6, "has_gpu": False, "ram_mb": 2048, "agent_version": "0.12.0"}
    )
    nodes = (await client.get("/nodes")).json()
    n = next(x for x in nodes if x["node_id"] == body["node_id"])
    assert n["capacity"] == 6


async def test_heartbeat_updates_status_and_returns_empty_work(client):
    body = await _register(client, name="hb")
    resp = await client.post(
        "/agent/heartbeat",
        headers={"Authorization": f"Bearer {body['token']}"},
        json={"node_id": body["node_id"], "status": "busy", "running": []},
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()
    # W1: scheduler not implemented — both arrays always empty.
    assert data["assignments"] == []
    assert data["commands"] == []

    nodes = (await client.get("/nodes")).json()
    n = next(x for x in nodes if x["node_id"] == body["node_id"])
    assert n["reported_status"] == "busy"
    assert n["online"] is True


async def test_bad_node_token_401(client):
    body = await _register(client, name="sec")
    resp = await client.post(
        "/agent/heartbeat",
        headers={"Authorization": "Bearer not-a-real-token"},
        json={"node_id": body["node_id"], "status": "idle", "running": []},
    )
    assert resp.status_code == 401


async def test_missing_node_token_401(client):
    body = await _register(client, name="sec2")
    resp = await client.post(
        "/agent/heartbeat",
        json={"node_id": body["node_id"], "status": "idle", "running": []},
    )
    assert resp.status_code == 401


async def test_invalid_status_value_422(client):
    body = await _register(client, name="bad-status")
    resp = await client.post(
        "/agent/heartbeat",
        headers={"Authorization": f"Bearer {body['token']}"},
        json={"node_id": body["node_id"], "status": "offline", "running": []},
    )
    # 'offline' is not a valid reported status -> 422 (a node never reports offline)
    assert resp.status_code == 422


async def test_liveness_flips_offline_when_heartbeat_stale(client, session_factory):
    body = await _register(client, name="goes-dark")

    # Backdate last_heartbeat well past NODE_TIMEOUT_S. No code ever writes
    # "offline" — liveness is derived purely from this timestamp on read.
    async with session_factory() as session:
        node = (
            await session.execute(select(Node).where(Node.id == body["node_id"]))
        ).scalar_one()
        node.last_heartbeat = datetime.now(timezone.utc) - timedelta(seconds=3600)
        await session.commit()

    nodes = (await client.get("/nodes")).json()
    n = next(x for x in nodes if x["node_id"] == body["node_id"])
    assert n["online"] is False
    # ...but the stored self-reported status is untouched.
    assert n["reported_status"] == "idle"
