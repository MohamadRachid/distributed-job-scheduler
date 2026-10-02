"""Sealed by default, resume anywhere (2026-09-06).

Four groups, each pinning one sentence of the change:

  * FORMAT       — the framed seal round-trips, and every way of moving a piece
                   breaks it: a changed byte, a swapped pair, a truncation, an
                   appended piece, a piece spliced in from another file sealed with
                   the SAME key, and the wrong key. The one-piece format written
                   before today still opens.
  * EVERY JOB    — all three submit doors mint a key and seal; storage holds
                   ciphertext; the owner downloads plaintext; a shredded key makes
                   the results unreadable to us as much as to anyone.
  * OUTPUTS      — an artefact that is not sealed is REFUSED on a sealed job, so
                   "storage holds only sealed bytes" is a property of the door
                   rather than a claim about how a workload behaves.
  * PLACEMENT    — `trusted_only` is available on any job and does nothing but
                   filter machines; a sealed job is invisible to an agent too old to
                   stage one, and visible to one that is new enough.

The reader that opens a sealed file a piece at a time lives in the workload image, so
its tests are host-side in `agent/tests/test_fyp_data.py` — including the one that
proves the two implementations of this format agree.
"""

import json

import pytest

from app.models import Artifact, Job, JobKey
from app.scheduler import MIN_SEALED_AGENT_VERSION, _version_tuple
from app.sealing import (
    FRAME_OVERHEAD,
    HEADER_BYTES,
    SealError,
    is_sealed,
    key_from_b64,
    new_key,
    open_any,
    open_frame,
    open_stream,
    plain_size,
    seal,
    seal_stream,
)

# No `pytestmark = pytest.mark.asyncio` here: `control-plane/pytest.ini` sets
# `asyncio_mode = auto`, so async tests are collected without it — and marking the
# module would put an asyncio mark on this file's SYNC tests (the format ones), which
# pytest-asyncio warns about, once per test.

SPECS = {"cpu_cores": 4, "has_gpu": False, "ram_mb": 8192, "capacity": 4,
         "agent_version": "0.12.0"}
OLD_AGENT = dict(SPECS, agent_version="0.11.0")

PLAIN = b"row,value\n" + b"".join(b"%d,marker-XYZ\n" % i for i in range(500))


# --- helpers ----------------------------------------------------------------


def _node_auth(node):
    return {"Authorization": f"Bearer {node['token']}"}


def _spec(**over):
    spec = {
        "name": "sealed-demo",
        "image": "fyp-dummy:latest",
        "entrypoint": ["python", "train.py"],
        "replicas": 1,
    }
    spec.update(over)
    return spec


async def _register(client, name="lab-pc-01", specs=None):
    r = await client.post("/agent/register", json={"name": name, "specs": specs or SPECS})
    assert r.status_code == 200, r.text
    return r.json()


async def _trust(client, node):
    r = await client.patch(f"/nodes/{node['node_id']}/trusted", json={"trusted": True})
    assert r.status_code == 200, r.text


async def _heartbeat(client, node):
    r = await client.post(
        "/agent/heartbeat",
        headers=_node_auth(node),
        json={"node_id": node["node_id"], "status": "idle", "running": []},
    )
    assert r.status_code == 200, r.text
    return r.json()["assignments"]


async def _claim(client, node=None, **over):
    """Submit a job through the JSON door and let a node claim its run."""
    node = node or await _register(client)
    r = await client.post("/jobs", json=_spec(**over))
    assert r.status_code == 200, r.text
    return node, (await _heartbeat(client, node))[0]


async def _key_of(session_factory, job_id) -> bytes:
    async with session_factory() as s:
        row = await s.get(JobKey, job_id)
    assert row is not None, "every job has its own key"
    return key_from_b64(row.key_b64)


async def _upload(client, node, run_id, attempt, filename, content, kind="result"):
    return await client.post(
        f"/agent/runs/{run_id}/artifacts",
        headers=_node_auth(node),
        data={"attempt": str(attempt), "filename": filename, "kind": kind},
        files={"file": (filename, content, "application/octet-stream")},
    )


# ===========================================================================
# FORMAT — a piece cannot be changed, moved, dropped or borrowed
# ===========================================================================


def test_the_frames_round_trip_at_every_awkward_length():
    """Including the lengths that fall exactly on a piece boundary, and zero.

    Zero matters more than it looks: an empty file still gets one piece, because
    otherwise "empty" and "truncated to nothing" would be the same bytes and the
    truncation check below would have nothing to catch."""
    key = new_key()
    for length in (0, 1, 1023, 1024, 1025, 2048, 5000):
        data = bytes((i * 7) % 251 for i in range(length))
        blob = seal_stream(data, key, chunk_size=1024)
        assert is_sealed(blob)
        assert open_stream(blob, key) == data
        assert plain_size(len(blob), 1024) == length


def test_a_changed_byte_fails_the_piece_it_is_in_and_only_when_touched():
    """The tamper check fires AT the piece, which is what lets a workload be told the
    moment its data stops being trustworthy rather than after the whole file."""
    key = new_key()
    data = bytes(range(256)) * 40  # 10,240 bytes -> ten pieces of 1024
    blob = bytearray(seal_stream(bytes(data), key, chunk_size=1024))
    # a byte inside the SIXTH piece (index 5 — the arithmetic is zero-based, the
    # sentence the platform prints is not)
    blob[HEADER_BYTES + 5 * (1024 + FRAME_OVERHEAD) + 20] ^= 0x01

    assert open_frame(bytes(blob), key, 4) == bytes(data)[4096:5120]  # untouched
    with pytest.raises(SealError) as exc:
        open_frame(bytes(blob), key, 5)
    assert "piece 6 of 10" in str(exc.value), str(exc.value)


def test_two_pieces_cannot_be_swapped():
    key = new_key()
    data = b"".join(b"%04d" % i for i in range(2560))  # 10,240 bytes
    blob = seal_stream(data, key, chunk_size=1024)
    size = 1024 + FRAME_OVERHEAD
    a = blob[HEADER_BYTES:HEADER_BYTES + size]
    b = blob[HEADER_BYTES + size:HEADER_BYTES + 2 * size]
    swapped = blob[:HEADER_BYTES] + b + a + blob[HEADER_BYTES + 2 * size:]
    with pytest.raises(SealError):
        open_stream(swapped, key)


def test_truncation_is_caught_even_though_every_remaining_piece_is_intact():
    """The one a careless per-piece format gets wrong. Cutting a file short leaves
    only whole, valid pieces behind — so without `is_last` in each piece's AAD the
    truncated file would open cleanly and hand a workload half a dataset with nothing
    anywhere to say so."""
    key = new_key()
    data = b"x" * 10_000
    blob = seal_stream(data, key, chunk_size=1024)
    cut = blob[:HEADER_BYTES + 3 * (1024 + FRAME_OVERHEAD)]
    with pytest.raises(SealError):
        open_stream(cut, key)


def test_a_piece_cannot_be_appended_to_the_end():
    key = new_key()
    blob = seal_stream(b"y" * 3000, key, chunk_size=1024)
    extra = seal_stream(b"z" * 1024, key, chunk_size=1024)
    grown = blob + extra[HEADER_BYTES:]
    with pytest.raises(SealError):
        open_stream(grown, key)


def test_a_piece_from_another_file_cannot_be_spliced_in_even_with_the_same_key():
    """What `stream_id` is for. Both files are this user's, sealed with this job's
    key, and the pieces are the same size — so nothing but the identity carried in
    each piece's AAD tells them apart."""
    key = new_key()
    size = 1024 + FRAME_OVERHEAD
    mine = seal_stream(b"a" * 4096, key, chunk_size=1024)
    theirs = seal_stream(b"b" * 4096, key, chunk_size=1024)
    spliced = (
        mine[:HEADER_BYTES + size]
        + theirs[HEADER_BYTES + size:HEADER_BYTES + 2 * size]
        + mine[HEADER_BYTES + 2 * size:]
    )
    with pytest.raises(SealError):
        open_stream(spliced, key)


def test_the_wrong_key_opens_nothing():
    blob = seal_stream(b"secret", new_key())
    with pytest.raises(SealError):
        open_stream(blob, new_key())


def test_the_one_piece_format_is_still_read():
    """Never written again, read for ever: the inputs sealed before today are still
    in the store, and a checkpoint written by an earlier attempt is still opened by a
    later one."""
    key = new_key()
    old = seal(b"written before 2026-09-06", key)
    assert not is_sealed(old)
    assert open_any(old, key) == b"written before 2026-09-06"
    assert open_any(seal_stream(b"written today", key), key) == b"written today"


# ===========================================================================
# EVERY JOB — a key, sealed storage, plaintext only for the owner
# ===========================================================================


async def test_every_door_mints_a_key_and_seals(client, session_factory, mem_store):
    """All three, including the one that carries no file at all — that job still has
    something to seal on the way OUT."""
    plain_job = await client.post("/jobs", json=_spec(name="no-file"))
    with_input = await client.post(
        "/jobs/with-input",
        data={"spec": json.dumps(_spec(name="with-file"))},
        files={"file": ("data.csv", PLAIN, "text/csv")},
    )
    private = await client.post(
        "/jobs/private",
        data={"spec": json.dumps(_spec(name="private"))},
        files={"file": ("data.csv", PLAIN, "text/csv")},
    )
    for r in (plain_job, with_input, private):
        assert r.status_code == 200, r.text
        job_id = r.json()["job_id"]
        key = await _key_of(session_factory, job_id)
        async with session_factory() as s:
            job = await s.get(Job, job_id)
        assert job.sealed is True
        if job.input_object_key:
            stored = mem_store.get_object(job.input_object_key)
            assert is_sealed(stored) and PLAIN not in stored
            assert open_any(stored, key) == PLAIN
            assert job.input_size_bytes == len(stored)


async def test_the_owner_downloads_plaintext_and_storage_holds_ciphertext(
    client, session_factory, mem_store
):
    """The two halves of the same sentence, asserted together so neither can drift:
    what is KEPT is sealed, what is HANDED BACK is not."""
    node, a = await _claim(client)
    key = await _key_of(session_factory, a["job_id"])
    body = b'{"accuracy": 0.91}'
    up = await _upload(
        client, node, a["run_id"], a["attempt"], "metrics.json", seal_stream(body, key)
    )
    assert up.status_code == 200, up.text
    art_id = up.json()["artifact_id"]

    async with session_factory() as s:
        art = await s.get(Artifact, art_id)
    assert art.sealed is True
    stored = mem_store.get_object(art.object_key)
    assert is_sealed(stored) and body not in stored

    got = await client.get(f"/artifacts/{art_id}/download")
    assert got.status_code == 200, got.text
    assert got.content == body


async def test_a_shredded_key_makes_the_results_unreadable_to_us_too(
    client, session_factory
):
    """Crypto-shred reaches the OUTPUTS now, not only the input — they are sealed with
    the same key. `410 Gone` rather than 404: the bytes are right there, and they are
    noise. The user asked for that and got it."""
    node, a = await _claim(client)
    key = await _key_of(session_factory, a["job_id"])
    up = await _upload(
        client, node, a["run_id"], a["attempt"], "out.bin", seal_stream(b"result", key)
    )
    art_id = up.json()["artifact_id"]
    assert (await client.get(f"/artifacts/{art_id}/download")).status_code == 200

    shred = await client.delete(f"/jobs/{a['job_id']}/key")
    assert shred.status_code == 200 and shred.json()["shredded"] is True

    gone = await client.get(f"/artifacts/{art_id}/download")
    assert gone.status_code == 410, gone.text
    assert "permanently unreadable" in gone.text


# ===========================================================================
# OUTPUTS — nothing unsealed is ever stored
# ===========================================================================


async def test_an_unsealed_output_is_refused_and_nothing_is_stored(
    client, session_factory, mem_store
):
    """The door where the guarantee stops being a claim about the workload.

    The check reads the FORMAT of the bytes in front of it, so a container that wrote
    its results with plain `open` cannot get them into storage however it was built —
    and no amount of trusting the agent is involved."""
    node, a = await _claim(client)
    before = set(mem_store._objs)

    refused = await _upload(
        client, node, a["run_id"], a["attempt"], "metrics.json", b'{"plain": true}'
    )
    assert refused.status_code == 422, refused.text
    detail = refused.json()["detail"]
    assert detail["reason"] == "UNSEALED_OUTPUT"
    assert detail["filename"] == "metrics.json"
    assert set(mem_store._objs) == before, "nothing may be stored on a refusal"

    async with session_factory() as s:
        from sqlalchemy import func, select

        rows = (
            await s.execute(select(func.count()).select_from(Artifact))
        ).scalar_one()
    assert rows == 0


async def test_an_unsealed_output_is_accepted_on_a_job_that_predates_sealing(
    client, session_factory
):
    """The refusal is scoped to jobs that ARE sealed. A run of an older job still
    stores its results exactly as it always did — the change does not reach backwards
    and break work already in flight."""
    node, a = await _claim(client)
    async with session_factory() as s:
        job = await s.get(Job, a["job_id"])
        job.sealed = False
        await s.commit()

    ok = await _upload(
        client, node, a["run_id"], a["attempt"], "metrics.json", b'{"plain": true}'
    )
    assert ok.status_code == 200, ok.text
    async with session_factory() as s:
        art = await s.get(Artifact, ok.json()["artifact_id"])
    assert art.sealed is False
    # And it downloads exactly as it was stored, with nothing attempted on it.
    got = await client.get(f"/artifacts/{ok.json()['artifact_id']}/download")
    assert got.content == b'{"plain": true}'


async def test_the_refusal_comes_after_the_fence(client, session_factory):
    """Order matters here for the same reason it does on the quota check: a stale
    attempt must be told it is stale, not told its bytes were the wrong shape."""
    node, a = await _claim(client)
    from app.models import Run

    async with session_factory() as s:
        run = await s.get(Run, a["run_id"])
        run.attempt += 1
        await s.commit()

    stale = await _upload(
        client, node, a["run_id"], a["attempt"], "metrics.json", b"plain"
    )
    assert stale.status_code == 409, stale.text


# ===========================================================================
# PLACEMENT — the one choice left, and the compatibility guard
# ===========================================================================


async def test_trusted_only_is_available_on_an_ordinary_job(client, session_factory):
    """The unbundling. What used to need the private door — and with it a RAM folder,
    no artefacts and no resume — is a field on the ordinary door that changes where
    the run goes and nothing else."""
    node = await _register(client, "untrusted-pc")
    r = await client.post("/jobs", json=_spec(trusted_only=True))
    assert r.status_code == 200, r.text
    async with session_factory() as s:
        job = await s.get(Job, r.json()["job_id"])
    assert job.trusted_only is True and job.sealed is True and job.private is False

    assert await _heartbeat(client, node) == [], "an untrusted machine sees nothing"
    await _trust(client, node)
    assert len(await _heartbeat(client, node)) == 1


async def test_an_ordinary_job_is_not_restricted(client):
    """Off by default, and the default is what almost every job uses."""
    node = await _register(client, "untrusted-pc")
    r = await client.post("/jobs", json=_spec())
    assert r.status_code == 200, r.text
    assert len(await _heartbeat(client, node)) == 1


async def test_an_agent_too_old_to_seal_is_offered_zero_sealed_runs(client):
    """The compatibility guard, and the answer the brief asked for in these words: an
    agent from before this change is never offered a sealed run.

    Both machines are identical in every other respect, so the version is the only
    thing that can explain the difference in what they are offered."""
    old = await _register(client, "old-pc", specs=OLD_AGENT)
    new = await _register(client, "new-pc", specs=SPECS)
    assert (await client.post("/jobs", json=_spec())).status_code == 200

    assert await _heartbeat(client, old) == []
    assert len(await _heartbeat(client, new)) == 1


def test_the_version_guard_fails_closed():
    assert _version_tuple("0.12.0") >= MIN_SEALED_AGENT_VERSION
    assert _version_tuple("0.11.9") < MIN_SEALED_AGENT_VERSION
    assert _version_tuple(None) < MIN_SEALED_AGENT_VERSION      # unknown -> too old
    assert _version_tuple("dev") < MIN_SEALED_AGENT_VERSION     # unparseable -> old


async def test_the_assignment_says_sealed_and_never_carries_a_key(client):
    node, a = await _claim(client)
    assert a["sealed"] is True
    assert "key" not in json.dumps(a).lower()
