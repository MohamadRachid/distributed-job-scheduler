"""A user reads only their own work (2026-09-07).

Until this date any logged-in user could list every job on the platform and open
any run's log, resource history, result listing and result bytes. Authentication
landed in W6 and answers "who are you"; nothing anywhere answered "is this yours",
which was survivable while a deployment had one account and stopped being
survivable on 2026-09-04, when an admin gained `POST /users`.

**These tests run against the REAL login path, not the fixture that stands in for
it.** `conftest.client` overrides `require_user` with the seeded admin so that the
three hundred tests written before authentication keep working; an override like
that would make every assertion here vacuous — an admin sees everything by design,
so it would pass whether or not the scoping exists. So this module uses
`anon_client`, creates two ordinary users through the admin route, logs each of
them in, and sends their real tokens. What is proven is what a person holding a
password can and cannot reach.

Both directions, on every route that carries job data: the owner gets their work,
a stranger gets the same answer a stranger gets for a job that was never created.
"""

import asyncio

import pytest
from sqlalchemy import select

from app.models import Job, JobKey, Run, User
from app.ownership import owns
from app.userauth import create_access_token
from conftest import TEST_PASSWORD, TEST_USERNAME, seal_for_run

SPECS = {"cpu_cores": 4, "has_gpu": False, "ram_mb": 8192, "capacity": 4,
         "agent_version": "0.12.0"}


def _auth(token):
    return {"Authorization": f"Bearer {token}"}


async def _login(client, username, password):
    r = await client.post("/auth/login", json={"username": username, "password": password})
    assert r.status_code == 200, r.text
    return r.json()["token"]


async def _make_user(client, admin, username):
    r = await client.post(
        "/users",
        headers=_auth(admin),
        json={"username": username, "password": f"{username}-pass-123", "tier": "standard"},
    )
    assert r.status_code == 200, r.text
    token = await _login(client, username, f"{username}-pass-123")
    accepted = await client.post("/me/accept-limits", headers=_auth(token))
    assert accepted.status_code == 200, accepted.text
    return token


async def _register(client, name):
    r = await client.post("/agent/register", json={"name": name, "specs": SPECS})
    assert r.status_code == 200, r.text
    return r.json()


async def _claim(client, node):
    r = await client.post(
        "/agent/heartbeat",
        headers=_auth(node["token"]),
        json={"node_id": node["node_id"], "status": "idle", "running": []},
    )
    assert r.status_code == 200, r.text
    return r.json()["assignments"]


async def _finished_job(client, session_factory, node, token, who):
    """Submit one job as `who`, let the node run it to SUCCEEDED, and give it a log
    line, a resource sample and a result file — so every scoped route has something
    real to refuse."""
    spec = (
        '{"name": "' + who + '-job", "image": "fyp-dummy:latest", '
        '"entrypoint": ["python", "train.py"], "replicas": 1}'
    )
    r = await client.post(
        "/jobs/with-input",
        headers=_auth(token),
        data={"spec": spec},
        files={"file": (who + "-data.csv", b"secret rows for " + who.encode(), "text/csv")},
    )
    assert r.status_code == 200, r.text
    job_id = r.json()["job_id"]

    a = (await _claim(client, node))[0]
    run_id, attempt = a["run_id"], a["attempt"]
    hdr = _auth(node["token"])
    assert (await client.post(
        f"/agent/runs/{run_id}/logs", headers=hdr,
        json={"attempt": attempt, "seq": 0, "chunk": f"{who}: loss=0.01\n"},
    )).status_code == 200
    assert (await client.post(
        f"/agent/runs/{run_id}/samples", headers=hdr,
        json={"attempt": attempt, "samples": [
            {"ts": 1.0, "cpu_pct": 10.0, "mem_used_mb": 64.0, "mem_limit_mb": 128.0}
        ]},
    )).status_code == 200
    body = await seal_for_run(session_factory, run_id, f"{who}'s result".encode())
    up = await client.post(
        f"/agent/runs/{run_id}/artifacts", headers=hdr,
        data={"attempt": str(attempt), "filename": "out.txt"},
        files={"file": ("out.txt", body, "text/plain")},
    )
    assert up.status_code == 200, up.text
    artifact_id = up.json()["artifact_id"]
    assert (await client.post(
        f"/agent/runs/{run_id}/status", headers=hdr,
        json={"attempt": attempt, "state": "SUCCEEDED", "exit_code": 0},
    )).status_code == 200
    return {"job_id": job_id, "run_id": run_id, "artifact_id": artifact_id}


@pytest.fixture
async def world(anon_client, session_factory):
    """Two ordinary users and one administrator, each holding finished work.

    `anon_client.session_factory` is set here for the same reason `conftest.client`
    sets it: `seal_for_run` needs the run's real key and gets no fixtures."""
    anon_client.session_factory = session_factory
    admin = await _login(anon_client, TEST_USERNAME, TEST_PASSWORD)
    alice = await _make_user(anon_client, admin, "alice")
    bob = await _make_user(anon_client, admin, "bob")
    node = await _register(anon_client, "lab-pc-01")
    a = await _finished_job(anon_client, session_factory, node, alice, "alice")
    b = await _finished_job(anon_client, session_factory, node, bob, "bob")
    return {
        "c": anon_client, "session_factory": session_factory, "node": node,
        "admin": admin, "alice": alice, "bob": bob, "a": a, "b": b,
    }


def _reads(work):
    """Every user-facing route that carries this job's data, as (method, path)."""
    return [
        ("get", f"/jobs/{work['job_id']}"),
        ("get", f"/jobs/{work['job_id']}/runs"),
        ("get", f"/runs/{work['run_id']}/logs"),
        ("get", f"/runs/{work['run_id']}/samples"),
        ("get", f"/runs/{work['run_id']}/artifacts"),
        ("get", f"/artifacts/{work['artifact_id']}/download"),
    ]


# --- the list ---------------------------------------------------------------------


async def test_the_job_list_shows_only_your_own(world):
    c = world["c"]
    mine = (await c.get("/jobs", headers=_auth(world["alice"]))).json()
    theirs = (await c.get("/jobs", headers=_auth(world["bob"]))).json()
    assert [j["job_id"] for j in mine] == [world["a"]["job_id"]]
    assert [j["job_id"] for j in theirs] == [world["b"]["job_id"]]
    assert [j["name"] for j in mine] == ["alice-job"]
    # And the input filename — the one piece of a job row that names somebody's own
    # data — travels with the row it belongs to and nowhere else.
    assert mine[0]["input_filename"] == "alice-data.csv"


async def test_an_administrator_sees_every_job(world):
    seen = (await world["c"].get("/jobs", headers=_auth(world["admin"]))).json()
    assert {j["job_id"] for j in seen} == {world["a"]["job_id"], world["b"]["job_id"]}


# --- the refusals -----------------------------------------------------------------


async def test_every_route_carrying_another_users_job_data_is_refused(world):
    c, alice = world["c"], _auth(world["alice"])
    for method, path in _reads(world["b"]):
        r = await getattr(c, method)(path, headers=alice)
        assert r.status_code == 404, f"{method.upper()} {path} -> {r.status_code}"


async def test_the_refusal_does_not_say_whether_the_identifier_exists(world):
    """A `403` on a job that exists beside a `404` on one that does not is a lookup
    service for other people's identifiers. Same code AND same words, both ways."""
    c, alice = world["c"], _auth(world["alice"])
    pairs = [
        (f"/jobs/{world['b']['job_id']}", "/jobs/no-such-job"),
        (f"/jobs/{world['b']['job_id']}/runs", "/jobs/no-such-job/runs"),
        (f"/runs/{world['b']['run_id']}/logs", "/runs/no-such-run/logs"),
        (f"/runs/{world['b']['run_id']}/samples", "/runs/no-such-run/samples"),
        (f"/runs/{world['b']['run_id']}/artifacts", "/runs/no-such-run/artifacts"),
        (f"/artifacts/{world['b']['artifact_id']}/download", "/artifacts/no-such-art/download"),
    ]
    for real, invented in pairs:
        theirs = await c.get(real, headers=alice)
        nothing = await c.get(invented, headers=alice)
        assert theirs.status_code == nothing.status_code == 404
        assert theirs.json()["detail"] == nothing.json()["detail"], real


async def test_the_destructive_routes_are_refused_on_another_users_job(world):
    """Cancel, crypto-shred and the storage release valve. Refused, and — the part
    that matters — the other user's job is untouched afterwards."""
    c, alice = world["c"], _auth(world["alice"])
    job_id = world["b"]["job_id"]
    assert (await c.post(f"/jobs/{job_id}/cancel", headers=alice)).status_code == 404
    assert (await c.delete(f"/jobs/{job_id}/key", headers=alice)).status_code == 404
    assert (await c.delete(f"/jobs/{job_id}/storage", headers=alice)).status_code == 404

    async with world["session_factory"]() as s:
        assert await s.get(JobKey, job_id) is not None      # not shredded
        job = await s.get(Job, job_id)
        assert job.input_object_key is not None             # bytes not released
        run = (await s.execute(select(Run).where(Run.job_id == job_id))).scalars().one()
        assert run.cancel_requested_at is None              # not cancelled
    # Bob's own result still opens, which is the end-to-end form of the same claim.
    dl = await c.get(f"/artifacts/{world['b']['artifact_id']}/download",
                     headers=_auth(world["bob"]))
    assert dl.status_code == 200 and dl.content == b"bob's result"


# --- the other direction ----------------------------------------------------------


async def test_the_owner_still_reaches_all_of_their_own_work(world):
    c, alice = world["c"], _auth(world["alice"])
    for method, path in _reads(world["a"]):
        r = await getattr(c, method)(path, headers=alice)
        assert r.status_code == 200, f"{method.upper()} {path} -> {r.text}"
    logs = (await c.get(f"/runs/{world['a']['run_id']}/logs", headers=alice)).json()
    assert [x["chunk"] for x in logs] == ["alice: loss=0.01\n"]
    samples = (await c.get(f"/runs/{world['a']['run_id']}/samples", headers=alice)).json()
    assert len(samples) == 1 and samples[0]["mem_used_mb"] == 64.0
    dl = await c.get(f"/artifacts/{world['a']['artifact_id']}/download", headers=alice)
    assert dl.content == b"alice's result"


async def test_an_administrator_reaches_both_users_work(world):
    c, admin = world["c"], _auth(world["admin"])
    for work in (world["a"], world["b"]):
        for method, path in _reads(work):
            r = await getattr(c, method)(path, headers=admin)
            assert r.status_code == 200, f"{method.upper()} {path} -> {r.text}"


async def test_seeing_everything_is_a_role_and_not_a_name(world):
    """Bob is refused Alice's work; promote the ROLE on his row and the same token
    reaches it. Nothing anywhere compares a username."""
    c = world["c"]
    path = f"/runs/{world['a']['run_id']}/logs"
    assert (await c.get(path, headers=_auth(world["bob"]))).status_code == 404
    async with world["session_factory"]() as s:
        bob = (await s.execute(select(User).where(User.username == "bob"))).scalar_one()
        bob.is_admin = True
        await s.commit()
    assert (await c.get(path, headers=_auth(world["bob"]))).status_code == 200


# --- the machine and pool views stay open -----------------------------------------


async def test_the_pool_stays_visible_to_every_user(world):
    """Nodes, their specifications and their status carry no job data, and an
    ordinary user needs them to choose where to send work."""
    c = world["c"]
    node_id = world["node"]["node_id"]
    for who in ("alice", "bob", "admin"):
        pool = await c.get("/nodes", headers=_auth(world[who]))
        assert pool.status_code == 200 and len(pool.json()) == 1
        events = await c.get(f"/nodes/{node_id}/events", headers=_auth(world[who]))
        assert events.status_code == 200


# --- a job nobody owns ------------------------------------------------------------


async def test_a_job_with_no_owner_stays_visible_to_everyone(world):
    """`jobs.user_id` is nullable and no HTTP door can leave it null, so a null owner
    means a row written straight into the database — the chaos test's jobs and the
    fixtures of the suites that predate authentication. Refusing those would break
    the project's centrepiece proof to protect data belonging to nobody."""
    c = world["c"]
    async with world["session_factory"]() as s:
        job = await s.get(Job, world["a"]["job_id"])
        job.user_id = None
        await s.commit()
    orphan = await c.get(f"/jobs/{world['a']['job_id']}", headers=_auth(world["bob"]))
    assert orphan.status_code == 200
    listed = (await c.get("/jobs", headers=_auth(world["bob"]))).json()
    assert {j["job_id"] for j in listed} == {world["a"]["job_id"], world["b"]["job_id"]}


# --- the socket -------------------------------------------------------------------


class _FakeSocket:
    """The log socket's transport, so the real handler can be driven directly."""

    def __init__(self, token):
        self.query_params = {"token": token} if token is not None else {}
        self.sent, self.accepted, self.closed, self.close_code = [], False, False, None

    async def accept(self):
        self.accepted = True

    async def send_json(self, msg):
        self.sent.append(msg)

    async def close(self, code=1000):
        self.closed, self.close_code = True, code


async def _drive_socket(monkeypatch, session_factory, mem_store, token, run_id):
    from app.api import runs as runs_api

    monkeypatch.setattr(runs_api, "SessionLocal", session_factory)
    sock = _FakeSocket(token)
    await asyncio.wait_for(runs_api.ws_run_logs(sock, run_id, store=mem_store), timeout=10)
    return sock


async def test_the_log_socket_refuses_another_users_run(world, mem_store, monkeypatch):
    """A valid login belonging to somebody else is exactly what the token check
    could not see: it answered "is this a real token", never "whose"."""
    sock = await _drive_socket(
        monkeypatch, world["session_factory"], mem_store,
        world["alice"], world["b"]["run_id"],
    )
    assert sock.accepted is False and sock.closed and sock.close_code == 1008
    assert sock.sent == []


async def test_the_log_socket_refuses_an_unknown_run_the_same_way(world, mem_store, monkeypatch):
    """Refused in the handshake, like somebody else's run — where an unknown run used
    to be an accepted socket carrying an error message, which said out loud that the
    other identifier did exist."""
    sock = await _drive_socket(
        monkeypatch, world["session_factory"], mem_store, world["alice"], "no-such-run",
    )
    assert sock.accepted is False and sock.closed and sock.close_code == 1008


async def test_the_log_socket_still_streams_the_owners_run(world, mem_store, monkeypatch):
    sock = await _drive_socket(
        monkeypatch, world["session_factory"], mem_store,
        world["alice"], world["a"]["run_id"],
    )
    assert sock.accepted and sock.closed
    assert [m["chunk"] for m in sock.sent if "chunk" in m] == ["alice: loss=0.01\n"]
    assert sock.sent[-1] == {"end": True, "run_status": "SUCCEEDED"}


async def test_the_log_socket_still_refuses_a_bad_token(world, mem_store, monkeypatch):
    for token in (None, "", "not-a-jwt"):
        sock = await _drive_socket(
            monkeypatch, world["session_factory"], mem_store, token, world["a"]["run_id"],
        )
        assert sock.accepted is False and sock.close_code == 1008


async def test_the_log_socket_refuses_a_token_whose_user_is_gone(world, mem_store, monkeypatch):
    """A signed token outlives the row it names, so the socket loads the user the way
    the HTTP gate does rather than trusting the claim."""
    async with world["session_factory"]() as s:
        ghost = User(username="ghost", password_hash="x", is_admin=True, tier_id="standard")
        s.add(ghost)
        await s.commit()
        token, _ = create_access_token(ghost)
        await s.delete(ghost)
        await s.commit()
    sock = await _drive_socket(
        monkeypatch, world["session_factory"], mem_store, token, world["a"]["run_id"],
    )
    assert sock.accepted is False and sock.close_code == 1008


# --- the agent doors are untouched ------------------------------------------------


async def test_a_user_token_is_not_a_node_token(world):
    """Nothing about the agent routes changed. They authenticate against a different
    column with a different kind of key, and are scoped by the run they hold at the
    attempt they hold it — which is the fence, a stricter check than this one. A
    user's JWT is not a node token, so those doors refuse it outright."""
    c = world["c"]
    for path in (
        f"/agent/jobs/{world['b']['job_id']}/input",
        f"/agent/runs/{world['b']['run_id']}/checkpoint?attempt=1",
    ):
        r = await c.get(path, headers=_auth(world["alice"]))
        assert r.status_code == 401, f"{path} -> {r.status_code}"


# --- the rule itself --------------------------------------------------------------


def test_who_may_read_a_job():
    """`ownership.owns` without a database. The same three-part answer the cancel
    route has given since it landed, which is why `may_cancel` now delegates to it
    rather than keeping a second copy that can drift."""
    owner = User(id="u1", username="alice", password_hash="x", is_admin=False)
    other = User(id="u2", username="bob", password_hash="x", is_admin=False)
    admin = User(id="u3", username="root", password_hash="x", is_admin=True)

    class J:
        user_id = "u1"

    class Unowned:
        user_id = None

    assert owns(owner, J()) is True
    assert owns(other, J()) is False
    assert owns(admin, J()) is True
    assert owns(other, Unowned()) is True
    assert owns(None, J()) is False
    assert owns(object(), J()) is False
