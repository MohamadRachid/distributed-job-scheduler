"""W6 tests — results, artifacts, auth (FR-9/10/11; NFR-1 Security).

Three groups:
  * AUTH: login ok / wrong password / unknown user (same 401 body); garbage +
    expired JWT -> 401; EVERY user endpoint without a token -> 401 (this test IS the
    ISO 27001 A.5.15 access-control evidence).
  * ARTIFACTS: happy path (row + object exist, download returns the exact bytes);
    a resent upload is idempotent (one row, one object); fencing (stale attempt 409,
    wrong node 409, unknown run 404); over the size cap -> 413; the list shows the
    CURRENT attempt only.
  * COMPAT: a run that uploads no artifacts still completes and lists no files (old
    agents that never call the artifact endpoint stay fully valid).

`client` is authenticated by default (conftest overrides require_user) + backed by an
in-memory object store (`mem_store`). `anon_client` keeps auth REAL, for the 401s.
"""

from datetime import datetime, timedelta, timezone

import jwt as pyjwt

from app.api.artifacts import artifact_size_cap
from app.config import get_settings
from app.main import app
from app.models import Run

from conftest import TEST_PASSWORD, TEST_USERNAME, seal_for_run

SPECS = {"cpu_cores": 4, "has_gpu": False, "ram_mb": 8192, "capacity": 4, "agent_version": "0.12.0"}


# --- helpers ----------------------------------------------------------------


def _node_auth(node):
    return {"Authorization": f"Bearer {node['token']}"}


async def _register(client, name="lab-pc-01"):
    resp = await client.post("/agent/register", json={"name": name, "specs": SPECS})
    assert resp.status_code == 200, resp.text
    return resp.json()


async def _create_job(client):
    body = {
        "name": "j", "image": "fyp-dummy:latest", "entrypoint": ["python", "train.py"],
        "env": {}, "resource_reqs": {"needs_gpu": False}, "replicas": 1,
    }
    resp = await client.post("/jobs", json=body)
    assert resp.status_code == 200, resp.text
    return resp.json()


async def _claim(client, node):
    """Submit a job and heartbeat `node` to own its (attempt-1) run."""
    await _create_job(client)
    hb = await client.post(
        "/agent/heartbeat",
        headers=_node_auth(node),
        json={"node_id": node["node_id"], "status": "idle", "running": []},
    )
    assert hb.status_code == 200, hb.text
    return hb.json()["assignments"][0]  # {run_id, attempt, ...}


async def _upload(client, node, run_id, attempt, filename, content, ctype="application/json"):
    # 2026-09-06: a sealed job's outputs are sealed inside the container, and the
    # control plane refuses anything else. This helper stands in for a container, so
    # it seals with the run's real key (conftest.seal_for_run) -- which is also what
    # keeps the download assertions honest below: they get plaintext back because the
    # seal opens, not because nothing was sealed.
    content = await seal_for_run(client.session_factory, run_id, content)
    return await client.post(
        f"/agent/runs/{run_id}/artifacts",
        headers=_node_auth(node),
        data={"attempt": str(attempt), "filename": filename},
        files={"file": (filename, content, ctype)},
    )


# ===========================================================================
# AUTH
# ===========================================================================


async def test_login_success_and_token_works(anon_client):
    r = await anon_client.post(
        "/auth/login", json={"username": TEST_USERNAME, "password": TEST_PASSWORD}
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["token"] and body["expires_at"]

    # The minted token authenticates a gated endpoint.
    ok = await anon_client.get("/nodes", headers={"Authorization": f"Bearer {body['token']}"})
    assert ok.status_code == 200, ok.text


async def test_wrong_password_and_unknown_user_are_same_401(anon_client):
    wrong = await anon_client.post(
        "/auth/login", json={"username": TEST_USERNAME, "password": "nope"}
    )
    unknown = await anon_client.post(
        "/auth/login", json={"username": "ghost", "password": "whatever"}
    )
    assert wrong.status_code == 401 and unknown.status_code == 401
    # Same body — never leak which of username/password was wrong.
    assert wrong.json() == unknown.json()


async def test_garbage_and_expired_tokens_are_401(anon_client):
    garbage = await anon_client.get("/nodes", headers={"Authorization": "Bearer not-a-jwt"})
    assert garbage.status_code == 401

    expired = pyjwt.encode(
        {
            "sub": "x", "username": "x",
            "exp": datetime.now(timezone.utc) - timedelta(hours=1),
        },
        get_settings().jwt_secret,
        algorithm="HS256",
    )
    r = await anon_client.get("/nodes", headers={"Authorization": f"Bearer {expired}"})
    assert r.status_code == 401


async def test_every_user_endpoint_requires_login(anon_client):
    """The ISO 27001 A.5.15 evidence: with NO token, every user-facing endpoint is
    refused 401 — reads, writes, logs, samples, node events, artifacts, download."""
    calls = [
        ("get", "/nodes"),
        ("get", "/nodes/some-id/events"),
        ("get", "/jobs"),
        ("get", "/jobs/some-id"),
        ("get", "/jobs/some-id/runs"),
        ("get", "/runs/some-id/logs"),
        ("get", "/runs/some-id/samples"),
        ("get", "/runs/some-id/artifacts"),
        ("get", "/artifacts/some-id/download"),
    ]
    for method, path in calls:
        r = await getattr(anon_client, method)(path)
        assert r.status_code == 401, f"{method.upper()} {path} -> {r.status_code} (expected 401)"

    # POST /jobs is also gated (require_user runs before the body).
    r = await anon_client.post("/jobs", json={"name": "x", "image": "y"})
    assert r.status_code == 401, r.text


# ===========================================================================
# ARTIFACTS
# ===========================================================================


async def test_artifact_upload_lists_and_downloads(client, mem_store):
    node = await _register(client)
    a = await _claim(client, node)
    payload = b'{"accuracy": 0.9, "node": "lab-pc-01"}'

    up = await _upload(client, node, a["run_id"], a["attempt"], "metrics.json", payload)
    assert up.status_code == 200, up.text
    art = up.json()
    assert art["object_key"] == f"runs/{a['run_id']}/{a['attempt']}/metrics.json"

    # The object really landed in storage -- as SEALED bytes (2026-09-06). What
    # storage holds is ciphertext for every job; the plaintext comes back only
    # through the download door below, which opens it with the job's key.
    stored = mem_store.get_object(art["object_key"])
    assert stored.startswith(b"FYPSEAL2") and stored != payload

    # The list shows it (current attempt).
    listed = await client.get(f"/runs/{a['run_id']}/artifacts")
    assert listed.status_code == 200
    rows = listed.json()
    assert len(rows) == 1
    assert rows[0]["filename"] == "metrics.json"
    # The size on the row is the size of the bytes STORED, which since 2026-09-06 are
    # the sealed ones: a little larger than the plaintext, and the number the retained
    # cap has to charge, because sealed bytes are what the platform actually keeps.
    assert rows[0]["size"] == len(stored) > len(payload)

    # Brokered download returns the EXACT bytes.
    dl = await client.get(f"/artifacts/{art['artifact_id']}/download")
    assert dl.status_code == 200
    assert dl.content == payload


async def test_artifact_upload_is_idempotent(client, mem_store):
    node = await _register(client)
    a = await _claim(client, node)
    payload = b"same-bytes"

    first = await _upload(client, node, a["run_id"], a["attempt"], "out.txt", payload)
    again = await _upload(client, node, a["run_id"], a["attempt"], "out.txt", payload)
    assert first.status_code == 200 and again.status_code == 200
    # Same row (idempotent) — one artifact, one object.
    assert again.json()["artifact_id"] == first.json()["artifact_id"]

    listed = (await client.get(f"/runs/{a['run_id']}/artifacts")).json()
    assert len(listed) == 1
    assert list(mem_store._objs.keys()) == [first.json()["object_key"]]


async def test_artifact_stale_attempt_409(client):
    node = await _register(client)
    a = await _claim(client, node)  # current attempt is 1
    r = await _upload(client, node, a["run_id"], a["attempt"] + 1, "x.txt", b"stale")
    assert r.status_code == 409, r.text


async def test_artifact_wrong_node_409(client):
    owner = await _register(client, name="owner")
    other = await _register(client, name="other")
    a = await _claim(client, owner)
    r = await _upload(client, other, a["run_id"], a["attempt"], "x.txt", b"not mine")
    assert r.status_code == 409, r.text


async def test_artifact_unknown_run_404(client):
    node = await _register(client)
    r = await _upload(client, node, "does-not-exist", 1, "x.txt", b"x")
    assert r.status_code == 404, r.text


async def test_artifact_over_size_cap_413(client):
    node = await _register(client)
    a = await _claim(client, node)
    # Shrink the cap to a few bytes via the dependency (cleared at fixture teardown).
    app.dependency_overrides[artifact_size_cap] = lambda: 8
    r = await _upload(client, node, a["run_id"], a["attempt"], "big.bin", b"x" * 100)
    assert r.status_code == 413, r.text


async def test_artifact_list_is_current_attempt_only(client, session_factory):
    node = await _register(client)
    a = await _claim(client, node)
    await _upload(client, node, a["run_id"], a["attempt"], "metrics.json", b"{}")

    # Simulate a re-dispatch: the run's current attempt moves on. The attempt-1
    # artifact is now a stale execution's leftover and must not show as the result.
    async with session_factory() as s:
        run = await s.get(Run, a["run_id"])
        run.attempt = a["attempt"] + 1
        await s.commit()

    listed = (await client.get(f"/runs/{a['run_id']}/artifacts")).json()
    assert listed == []


async def test_download_unknown_artifact_404(client):
    r = await client.get("/artifacts/nope/download")
    assert r.status_code == 404, r.text


async def test_download_is_current_attempt_only(client, mem_store, session_factory):
    """Both directions in one test. The list already hid a stale attempt's files;
    the download served them to anyone holding the id, so the two disagreed about
    the same claim - that a presumed-dead machine's leftovers never surface as the
    accepted result. Now they agree."""
    node = await _register(client)
    a = await _claim(client, node)
    up = await _upload(client, node, a["run_id"], a["attempt"], "metrics.json", b"{}")
    art_id = up.json()["artifact_id"]

    # Direction 1: the current attempt's file downloads.
    ok = await client.get(f"/artifacts/{art_id}/download")
    assert ok.status_code == 200, ok.text
    assert ok.content == b"{}"

    # A re-dispatch moves the run on; that file is now a stale execution's leftover.
    async with session_factory() as s:
        run = await s.get(Run, a["run_id"])
        run.attempt = a["attempt"] + 1
        await s.commit()

    # Direction 2: the same id is now refused, with the fence's own status code.
    stale = await client.get(f"/artifacts/{art_id}/download")
    assert stale.status_code == 409, stale.text
    assert "stale attempt" in stale.json()["detail"]

    # And the listing still agrees with it.
    assert (await client.get(f"/runs/{a['run_id']}/artifacts")).json() == []


# ===========================================================================
# COMPAT — a run that never uploads an artifact still works
# ===========================================================================


async def test_run_without_artifacts_completes_and_lists_none(client):
    node = await _register(client)
    a = await _claim(client, node)
    # Straight to SUCCEEDED, no artifact upload (an old agent never calls it).
    await client.post(
        f"/agent/runs/{a['run_id']}/status",
        headers=_node_auth(node),
        json={"attempt": a["attempt"], "state": "RUNNING"},
    )
    done = await client.post(
        f"/agent/runs/{a['run_id']}/status",
        headers=_node_auth(node),
        json={"attempt": a["attempt"], "state": "SUCCEEDED", "exit_code": 0},
    )
    assert done.status_code == 200
    listed = await client.get(f"/runs/{a['run_id']}/artifacts")
    assert listed.status_code == 200 and listed.json() == []
