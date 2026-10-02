"""W6b tests — private job inputs (NFR-7 Data privacy).

Five groups, each proving one sentence of the feature:

  * SEALING     — round trip works; ONE flipped byte fails; a wrong key fails.
                  (This is the tamper-detection guarantee, tested directly.)
  * SUBMIT      — a submit through this door stores ONLY ciphertext (the test reads
                  the stored object and asserts the plaintext is not in it); the
                  `private` flag on the JSON route is refused 422; an empty file is
                  422.
  * TRUST TIER  — a private run is INVISIBLE to an untrusted node, invisible to a
                  too-old agent, and claimable the moment an admin trusts the node.
                  Ordinary jobs are unaffected.
  * KEY RELEASE — the ticket is fenced (stale attempt / wrong node / unknown run),
                  single-use (second redeem 410), expires, and dies when the run's
                  attempt moves on. The sealed-input download is fenced too.
  * SHRED       — deleting the key makes the ticket path answer 410 "permanently
                  unreadable", and the stored bytes stay unopenable.

`client` is authenticated by default (conftest overrides require_user) and backed
by an in-memory object store (`mem_store`), so no MinIO server is needed.

**Updated 2026-09-06 (sealed by default).** Three of the five groups now describe
every job rather than a private one: sealing, the key release and the shred. What is
left that is particular to this door is the trust tier — the one protection of the
four that is a genuine choice — and it is expressed as `jobs.trusted_only` rather than
`jobs.private`, which from that date names only the older submission shape. The tests
that read `job.private` were asserting the bundle; they read the two columns that
replaced it.
"""

from datetime import datetime, timedelta, timezone

import pytest

from app.models import Job, JobKey, KeyTicket, Node, Run
from app.scheduler import _version_tuple, may_run_private
from app.sealing import SealError, key_from_b64, new_key, open_any, open_sealed, seal

SPECS = {"cpu_cores": 4, "has_gpu": False, "ram_mb": 8192, "capacity": 4, "agent_version": "0.12.0"}
# The claim query only offers a SEALED run to an agent at/after this version, and
# every job submitted since 2026-09-06 is sealed — so this is what a machine has to be
# to receive any of today's work. 0.8.0 was the bar when this file was written, when
# the guard that mattered was the older private one.
NEW_AGENT = dict(SPECS, agent_version="0.12.0")
OLD_AGENT = dict(SPECS, agent_version="0.11.0")

SECRET = b"patient-id,diagnosis\n1,confidential-value-XYZ\n"


# --- helpers ----------------------------------------------------------------


def _node_auth(node):
    return {"Authorization": f"Bearer {node['token']}"}


async def _register(client, name="lab-pc-01", specs=None):
    resp = await client.post(
        "/agent/register", json={"name": name, "specs": specs or NEW_AGENT}
    )
    assert resp.status_code == 200, resp.text
    return resp.json()


async def _trust(client, node, trusted=True):
    r = await client.patch(f"/nodes/{node['node_id']}/trusted", json={"trusted": trusted})
    assert r.status_code == 200, r.text
    return r.json()


def _spec(**over):
    body = {
        "name": "private-job",
        "image": "fyp-dummy:latest",
        "entrypoint": ["python", "fyp_open.py", "python", "train.py", "--private-input"],
        "env": {},
        "resource_reqs": {"needs_gpu": False},
        "replicas": 1,
    }
    body.update(over)
    return body


async def _submit_private(client, content=SECRET, filename="secret.csv", **over):
    import json as _json

    return await client.post(
        "/jobs/private",
        data={"spec": _json.dumps(_spec(**over))},
        files={"file": (filename, content, "text/csv")},
    )


async def _heartbeat(client, node):
    r = await client.post(
        "/agent/heartbeat",
        headers=_node_auth(node),
        json={"node_id": node["node_id"], "status": "idle", "running": []},
    )
    assert r.status_code == 200, r.text
    return r.json()["assignments"]


# ===========================================================================
# SEALING — the guarantee, tested directly
# ===========================================================================


def test_seal_round_trip():
    key = new_key()
    blob = seal(SECRET, key)
    assert SECRET not in blob          # the plaintext is simply not in there
    assert open_sealed(blob, key) == SECRET


def test_seal_is_different_every_time():
    """A fresh nonce per seal, so the same file sealed twice looks unrelated — an
    observer learns nothing from comparing two stored blobs."""
    key = new_key()
    assert seal(SECRET, key) != seal(SECRET, key)


@pytest.mark.parametrize("index", [0, 12, 20, -1])
def test_one_flipped_byte_breaks_the_seal(index):
    """THE tamper guarantee. Flip a single bit anywhere — in the nonce, in the
    ciphertext, in the tag — and opening raises instead of returning wrong data."""
    key = new_key()
    blob = bytearray(seal(SECRET, key))
    blob[index] ^= 0x01
    with pytest.raises(SealError):
        open_sealed(bytes(blob), key)


def test_wrong_key_cannot_open():
    blob = seal(SECRET, new_key())
    with pytest.raises(SealError):
        open_sealed(blob, new_key())


def test_truncated_blob_is_rejected():
    key = new_key()
    with pytest.raises(SealError):
        open_sealed(seal(SECRET, key)[:8], key)


# ===========================================================================
# SUBMIT — only ciphertext is ever stored
# ===========================================================================


async def test_private_submit_stores_only_ciphertext(client, mem_store, session_factory):
    r = await _submit_private(client)
    assert r.status_code == 200, r.text
    job_id = r.json()["job_id"]

    async with session_factory() as s:
        job = await s.get(Job, job_id)
        key_row = await s.get(JobKey, job_id)
    # 2026-09-06: this door no longer sets `private` — that column names the OLDER
    # submission shape, whose container opened its input into a RAM folder. What this
    # door does is said by the two columns that replaced it.
    assert job.private is False
    assert job.sealed is True and job.trusted_only is True
    assert job.input_filename == "secret.csv"
    assert job.input_object_key == f"inputs/{job_id}/input.bin"

    stored = mem_store.get_object(job.input_object_key)
    # The whole claim, asserted on the actual stored bytes.
    assert SECRET not in stored
    assert b"confidential-value-XYZ" not in stored
    # …and the key that opens it lives somewhere else entirely.
    assert open_any(stored, key_from_b64(key_row.key_b64)) == SECRET


async def test_private_flag_on_the_json_route_is_422(client):
    """Privacy cannot be half-done: asking for it without a file is refused, never
    silently downgraded to an ordinary job."""
    r = await client.post("/jobs", json=_spec(private=True))
    assert r.status_code == 422, r.text
    assert "private" in r.text


async def test_private_submit_without_content_is_422(client):
    r = await _submit_private(client, content=b"")
    assert r.status_code == 422, r.text


async def test_private_submit_with_bad_spec_is_422(client):
    import json as _json

    r = await client.post(
        "/jobs/private",
        data={"spec": _json.dumps({"name": "no-image"})},
        files={"file": ("x.csv", b"data", "text/csv")},
    )
    assert r.status_code == 422, r.text


async def test_job_read_never_exposes_the_key_or_object(client):
    r = await _submit_private(client)
    job_id = r.json()["job_id"]
    body = (await client.get(f"/jobs/{job_id}")).json()
    assert body["sealed"] is True and body["trusted_only"] is True
    assert body["input_filename"] == "secret.csv"
    assert "key" not in str(body).lower().replace("key_url", "")
    assert "input_object_key" not in body


# ===========================================================================
# TRUST TIER — who may open private data
# ===========================================================================


def test_version_gate_fails_closed():
    assert _version_tuple("0.8.0") == (0, 8, 0)
    assert _version_tuple(None) == (0,)          # unknown version -> too old
    assert _version_tuple("dev") == (0,)         # unparseable -> too old

    class N:
        def __init__(self, trusted, agent_version):
            self.trusted, self.agent_version = trusted, agent_version

    assert may_run_private(N(True, "0.8.0"))
    assert may_run_private(N(True, "0.9.1"))
    assert not may_run_private(N(False, "0.8.0"))   # untrusted
    assert not may_run_private(N(True, "0.7.0"))    # too old to stage one
    assert not may_run_private(N(True, None))


async def test_private_run_is_invisible_to_an_untrusted_node(client):
    node = await _register(client)              # registered, NOT trusted
    await _submit_private(client)
    assert await _heartbeat(client, node) == []  # waits, rather than landing here


async def test_private_run_is_invisible_to_an_old_agent(client):
    node = await _register(client, name="old", specs=OLD_AGENT)
    await _trust(client, node)                  # trusted, but cannot stage one
    await _submit_private(client)
    assert await _heartbeat(client, node) == []


async def test_trusting_a_node_releases_the_waiting_private_run(client):
    """Waiting (not failing) is the honest behaviour: unlike W5c's insufficient
    pool, trust can change at any moment — and one admin click proves it."""
    node = await _register(client)
    r = await _submit_private(client)
    assert await _heartbeat(client, node) == []

    await _trust(client, node)

    assignments = await _heartbeat(client, node)
    assert len(assignments) == 1
    a = assignments[0]
    # The agent is told the run is SEALED, which is what it stages on. `private` names
    # the older path and is not what this door builds any more.
    assert a["sealed"] is True and a["private"] is False
    assert a["job_id"] == r.json()["job_id"]
    assert a["input_filename"] == "secret.csv"
    # The KEY is never in an assignment — the agent gets a ticket later, or nothing.
    assert "key" not in str(a).lower()


async def test_untrusting_stops_future_private_placements(client):
    node = await _register(client)
    await _trust(client, node)
    await _trust(client, node, trusted=False)
    await _submit_private(client)
    assert await _heartbeat(client, node) == []


async def test_ordinary_jobs_are_unaffected_by_the_trust_tier(client):
    """The tier must not quietly change normal scheduling — an untrusted node keeps
    running ordinary work exactly as it did in W6."""
    node = await _register(client)              # untrusted
    ok = await client.post("/jobs", json=_spec(name="ordinary"))
    assert ok.status_code == 200
    assert len(await _heartbeat(client, node)) == 1


async def test_trust_unknown_node_404(client):
    r = await client.patch("/nodes/nope/trusted", json={"trusted": True})
    assert r.status_code == 404


async def test_nodes_list_reports_trust(client):
    node = await _register(client)
    rows = (await client.get("/nodes")).json()
    assert rows[0]["trusted"] is False
    await _trust(client, node)
    rows = (await client.get("/nodes")).json()
    assert rows[0]["trusted"] is True


# ===========================================================================
# KEY RELEASE — the fence guards the input, not just the result
# ===========================================================================


async def _claimed_private(client, name="trusted-pc"):
    """A trusted node holding a private run at its current attempt."""
    node = await _register(client, name=name)
    await _trust(client, node)
    await _submit_private(client)
    a = (await _heartbeat(client, node))[0]
    return node, a


async def test_sealed_input_download_is_ciphertext(client, mem_store):
    node, a = await _claimed_private(client)
    r = await client.get(f"/agent/jobs/{a['job_id']}/input", headers=_node_auth(node))
    assert r.status_code == 200
    assert SECRET not in r.content          # the agent is a courier, not a reader
    assert r.content == mem_store.get_object(f"inputs/{a['job_id']}/input.bin")


async def test_sealed_input_refused_to_a_node_with_no_live_run(client):
    node, a = await _claimed_private(client)
    other = await _register(client, name="stranger")
    await _trust(client, other)
    r = await client.get(f"/agent/jobs/{a['job_id']}/input", headers=_node_auth(other))
    assert r.status_code == 409, r.text


async def test_sealed_input_unknown_job_404(client):
    node, _ = await _claimed_private(client)
    r = await client.get("/agent/jobs/nope/input", headers=_node_auth(node))
    assert r.status_code == 404


async def test_ticket_then_key_works_once(client, session_factory, mem_store):
    node, a = await _claimed_private(client)
    t = await client.post(
        f"/agent/runs/{a['run_id']}/key-ticket?attempt={a['attempt']}",
        headers=_node_auth(node),
    )
    assert t.status_code == 200, t.text
    ticket = t.json()["ticket"]
    assert t.json()["key_url"].endswith("/container/key")

    first = await client.post("/container/key", json={"ticket": ticket})
    assert first.status_code == 200, first.text

    # The key really opens this job's blob — asserted by opening it, rather than by
    # reading a flag off the job row.
    async with session_factory() as s:
        job = await s.get(Job, a["job_id"])
    key = key_from_b64(first.json()["key_b64"])
    assert open_any(mem_store.get_object(job.input_object_key), key) == SECRET

    # …and the ticket is dead the moment it was used.
    again = await client.post("/container/key", json={"ticket": ticket})
    assert again.status_code == 410, again.text
    assert key  # (used above; keeps the intent explicit)


async def test_ticket_issue_is_fenced(client):
    node, a = await _claimed_private(client)
    stale = await client.post(
        f"/agent/runs/{a['run_id']}/key-ticket?attempt={a['attempt'] + 1}",
        headers=_node_auth(node),
    )
    assert stale.status_code == 409, stale.text

    other = await _register(client, name="stranger")
    wrong_node = await client.post(
        f"/agent/runs/{a['run_id']}/key-ticket?attempt={a['attempt']}",
        headers=_node_auth(other),
    )
    assert wrong_node.status_code == 409, wrong_node.text

    unknown = await client.post(
        "/agent/runs/nope/key-ticket?attempt=1", headers=_node_auth(node)
    )
    assert unknown.status_code == 404, unknown.text


async def test_redeem_is_refused_after_the_run_moves_on(client, session_factory):
    """The fence is re-checked at REDEEM time, not only at issue time: a ticket
    minted a moment before a re-dispatch is already worthless when it is used."""
    node, a = await _claimed_private(client)
    t = await client.post(
        f"/agent/runs/{a['run_id']}/key-ticket?attempt={a['attempt']}",
        headers=_node_auth(node),
    )
    ticket = t.json()["ticket"]

    async with session_factory() as s:      # simulate the re-dispatch
        run = await s.get(Run, a["run_id"])
        run.attempt += 1
        await s.commit()

    r = await client.post("/container/key", json={"ticket": ticket})
    assert r.status_code == 409, r.text


async def test_expired_ticket_is_410(client, session_factory):
    node, a = await _claimed_private(client)
    t = await client.post(
        f"/agent/runs/{a['run_id']}/key-ticket?attempt={a['attempt']}",
        headers=_node_auth(node),
    )
    ticket = t.json()["ticket"]

    async with session_factory() as s:
        row = await s.get(KeyTicket, ticket)
        row.expires_at = datetime.now(timezone.utc) - timedelta(seconds=1)
        await s.commit()

    r = await client.post("/container/key", json={"ticket": ticket})
    assert r.status_code == 410, r.text


async def test_unknown_ticket_is_404(client):
    r = await client.post("/container/key", json={"ticket": "made-up"})
    assert r.status_code == 404


async def test_reissuing_a_ticket_kills_the_previous_one(client):
    """A retry must not leave a second live pass lying around."""
    node, a = await _claimed_private(client)
    url = f"/agent/runs/{a['run_id']}/key-ticket?attempt={a['attempt']}"
    first = (await client.post(url, headers=_node_auth(node))).json()["ticket"]
    second = (await client.post(url, headers=_node_auth(node))).json()["ticket"]
    assert first != second
    assert (await client.post("/container/key", json={"ticket": first})).status_code == 404
    assert (await client.post("/container/key", json={"ticket": second})).status_code == 200


async def test_a_job_with_no_input_file_still_gets_a_ticket(client):
    """The reversal of what stood here before, and the reason for it.

    A ticket used to be refused to any job that was not private, because the key
    existed only to OPEN a sealed input. Since 2026-09-06 the container also seals
    what it WRITES — its results and its checkpoints — so a job with nothing to open
    still has something to seal, and refusing it a ticket would leave its outputs the
    only unsealed bytes in the system."""
    node = await _register(client)
    await _trust(client, node)
    await client.post("/jobs", json=_spec(name="ordinary"))
    a = (await _heartbeat(client, node))[0]
    r = await client.post(
        f"/agent/runs/{a['run_id']}/key-ticket?attempt={a['attempt']}",
        headers=_node_auth(node),
    )
    assert r.status_code == 200, r.text
    assert r.json()["ticket"]


# ===========================================================================
# CRYPTO-SHRED — deleting one row makes every copy unreadable
# ===========================================================================


async def test_shred_makes_the_input_permanently_unreadable(client, mem_store, session_factory):
    node, a = await _claimed_private(client)
    job_id = a["job_id"]

    async with session_factory() as s:
        stored_key = (await s.get(JobKey, job_id)).key_b64

    r = await client.delete(f"/jobs/{job_id}/key")
    assert r.status_code == 200 and r.json()["shredded"] is True

    # The sealed bytes are STILL THERE — and that is the point: shredding did not
    # depend on reaching them, and they are noise now regardless.
    blob = mem_store.get_object(f"inputs/{job_id}/input.bin")
    assert blob and SECRET not in blob
    with pytest.raises(SealError):
        open_any(blob, new_key())

    # A container that still holds a ticket is told plainly why it gets nothing.
    t = await client.post(
        f"/agent/runs/{a['run_id']}/key-ticket?attempt={a['attempt']}",
        headers=_node_auth(node),
    )
    redeem = await client.post("/container/key", json={"ticket": t.json()["ticket"]})
    assert redeem.status_code == 410
    assert "unreadable" in redeem.text
    assert stored_key  # the key existed before the shred; nothing can rebuild it now


async def test_shred_is_idempotent_and_guarded(client):
    node, a = await _claimed_private(client)
    await client.delete(f"/jobs/{a['job_id']}/key")
    again = await client.delete(f"/jobs/{a['job_id']}/key")
    assert again.status_code == 200 and again.json()["shredded"] is False

    assert (await client.delete("/jobs/nope/key")).status_code == 404

    # 2026-09-06: a job submitted with no file HAS a key, because its results and its
    # checkpoints are sealed with it — so shredding one is a real act and answers 200.
    # This used to be a 422 ("no sealed input to shred"), which would now be a false
    # statement about the caller's own job.
    ok = await client.post("/jobs", json=_spec(name="ordinary"))
    plain = await client.delete(f"/jobs/{ok.json()['job_id']}/key")
    assert plain.status_code == 200 and plain.json()["shredded"] is True

    # What is still refused is a job that never had a key: one of the rows from before
    # sealing. 404 rather than 422, because the honest answer is that there is no key
    # of that name.
    async with client.session_factory() as s:
        job = await s.get(Job, ok.json()["job_id"])
        job.sealed = False
        await s.commit()
    legacy = await client.delete(f"/jobs/{ok.json()['job_id']}/key")
    assert legacy.status_code == 404


# ===========================================================================
# AUTH — the W6b endpoints join the A.5.15 evidence
# ===========================================================================


async def test_w6b_user_endpoints_require_login(anon_client):
    calls = [
        ("patch", "/nodes/some-id/trusted"),
        ("delete", "/jobs/some-id/key"),
    ]
    for method, path in calls:
        r = await getattr(anon_client, method)(path, **({"json": {"trusted": True}} if method == "patch" else {}))
        assert r.status_code == 401, f"{method.upper()} {path} -> {r.status_code}"
    r = await anon_client.post("/jobs/private", data={"spec": "{}"}, files={"file": ("x", b"y")})
    assert r.status_code == 401, r.text


async def test_agent_private_endpoints_require_a_node_token(client):
    """No node token -> 401, exactly like every other /agent route. The container
    key endpoint is deliberately NOT in this list: its credential is the ticket."""
    assert (await client.get("/agent/jobs/x/input")).status_code == 401
    assert (await client.post("/agent/runs/x/key-ticket?attempt=1")).status_code == 401


async def test_key_is_never_reachable_through_a_user_read(client, session_factory):
    """A logged-in user can read jobs, runs, nodes — but there is no route anywhere
    that returns a key. The only way out is the fenced one-shot ticket."""
    r = await _submit_private(client)
    job_id = r.json()["job_id"]
    async with session_factory() as s:
        assert await s.get(JobKey, job_id) is not None   # it exists…
    for path in (f"/jobs/{job_id}", "/jobs", f"/jobs/{job_id}/runs", "/nodes"):
        body = (await client.get(path)).text
        assert "key_b64" not in body


async def test_registration_cannot_self_declare_trust(client, session_factory):
    """A node claiming "trust me" must count for nothing — the whole reason the tier
    is set from outside the machine."""
    node = await _register(client, name="liar", specs=dict(NEW_AGENT, trusted=True))
    async with session_factory() as s:
        assert (await s.get(Node, node["node_id"])).trusted is False
