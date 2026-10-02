"""Run-log tiering — the archive, the two-phase purge, and the read that cannot tell.

Layer 2 of the log-retention design locked on 2026-07-24, built on
2026-08-22. `run_logs` grows for ever and nothing reclaimed it; now a terminal run's
chunks move, once they are older than the retention window, into ONE compressed
object per (run, attempt), and the rows are purged.

The thing this file has to prove is NOT that archiving works. It is that archiving
cannot lose a line. Our standing guard is *no log line is silently lost*, and a
purge is the only operation in this system that deletes user-visible data, so the
tests are weighted accordingly:

  * the purge is a TWO-PHASE COMMIT and refuses to complete when the read-back does
    not match (`test_verify_failure_keeps_every_row`),
  * a crash between the phases leaves every row where it was
    (`test_crash_between_write_and_purge_keeps_rows`),
  * and re-archiving after such a crash is an overwrite, not a duplicate
    (`test_re_archiving_is_idempotent`).

The read tests do not check that an archived read is *plausible*. They capture the
exact response BEFORE archiving and assert the exact same response afterwards, with
and without a cursor — because "the caller cannot tell" is the actual contract, and
anything weaker would let the shape drift while the tests stayed green.
"""

import asyncio
import gzip
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import func, select

from app import logarchive
from app.config import Settings, get_settings, shipped_lease_ttl_s
from app.logarchive import archive_key, archive_one, decode_body, sweep_once
from app.models import Run, RunLog, RunLogArchive, RunStatus
from app.userauth import create_access_token

NOW = datetime(2026, 8, 22, 12, 0, 0, tzinfo=timezone.utc)
LONG_AGO = NOW - timedelta(days=30)
PAST = NOW - timedelta(seconds=30)

SPECS = {"cpu_cores": 4, "has_gpu": False, "ram_mb": 8192, "capacity": 4, "agent_version": "0.12.0"}


# --------------------------------------------------------------------------
# helpers — everything goes through the real handlers, never straight to SQL
# --------------------------------------------------------------------------


async def _register(client, name):
    resp = await client.post("/agent/register", json={"name": name, "specs": SPECS})
    assert resp.status_code == 200, resp.text
    return resp.json()


async def _heartbeat(client, node):
    resp = await client.post(
        "/agent/heartbeat",
        headers={"Authorization": f"Bearer {node['token']}"},
        json={"node_id": node["node_id"], "status": "idle", "running": []},
    )
    assert resp.status_code == 200, resp.text
    return resp.json()["assignments"]


async def _post_log(client, node, run_id, attempt, seq, chunk):
    return await client.post(
        f"/agent/runs/{run_id}/logs",
        headers={"Authorization": f"Bearer {node['token']}"},
        json={"attempt": attempt, "seq": seq, "chunk": chunk},
    )


async def _finish(client, node, run_id, attempt=1):
    resp = await client.post(
        f"/agent/runs/{run_id}/status",
        headers={"Authorization": f"Bearer {node['token']}"},
        json={"attempt": attempt, "state": "SUCCEEDED", "exit_code": 0},
    )
    assert resp.status_code == 200, resp.text


async def _finished_run(client, session_factory, *, chunks=5, name="tier"):
    """One run that ran, printed `chunks` chunks and finished. Nothing is archived
    yet. Returns (run_id, node)."""
    node = await _register(client, f"{name}-node")
    resp = await client.post(
        "/jobs",
        json={
            "name": name,
            "image": "fyp-dummy:latest",
            "entrypoint": ["python", "train.py"],
            "env": {"EPOCHS": "1"},
            "resource_reqs": {"needs_gpu": False},
            "replicas": 1,
        },
    )
    assert resp.status_code == 200, resp.text
    run_id = (await _heartbeat(client, node))[0]["run_id"]
    for seq in range(chunks):
        r = await _post_log(client, node, run_id, 1, seq, f"line {seq}\n")
        assert r.status_code == 200, r.text
    await _finish(client, node, run_id)
    return run_id, node


async def _age(session_factory, run_id, when=LONG_AGO):
    """Make a finished run old, deterministically. The alternative is waiting a
    week, which is the reason `now` is injectable at all."""
    async with session_factory() as s:
        run = await s.get(Run, run_id)
        run.finished_at = when
        await s.commit()


async def _row_count(session_factory, run_id):
    async with session_factory() as s:
        return (
            await s.execute(
                select(func.count()).select_from(RunLog).where(RunLog.run_id == run_id)
            )
        ).scalar_one()


async def _pointer(session_factory, run_id, attempt=1):
    async with session_factory() as s:
        return await s.get(RunLogArchive, (run_id, attempt))


# --------------------------------------------------------------------------
# 1. The default that protects the demonstration
# --------------------------------------------------------------------------


def test_retention_default_is_seven_days_and_archiving_is_on():
    """A demonstration run is minutes old. Seven days is what guarantees nothing
    the room is looking at can be archived while they look at it, so the default is
    asserted rather than trusted — it is the only thing standing between the
    archiver and the one demonstration that carries a grade."""
    s = Settings()
    assert s.log_retention_days == 7.0
    assert s.log_archive_enabled is True
    assert s.log_archive_interval_s == 3600


async def test_a_fresh_finished_run_is_not_archived(client, session_factory, mem_store):
    """The same guarantee, exercised rather than read off the settings: a run that
    finished seconds ago survives a sweep untouched."""
    run_id, _ = await _finished_run(client, session_factory, name="fresh")

    done = await sweep_once(session_factory=session_factory, store=mem_store, now=NOW)

    assert done == []
    assert await _row_count(session_factory, run_id) == 5
    assert await _pointer(session_factory, run_id) is None


# --------------------------------------------------------------------------
# 2. What the sweep will and will not touch
# --------------------------------------------------------------------------


@pytest.mark.parametrize("state", [RunStatus.RUNNING, RunStatus.ASSIGNED, RunStatus.PENDING])
async def test_a_live_run_is_never_archived_however_old(
    client, session_factory, mem_store, state
):
    """Age is not the gate on its own. A run that is still in flight is a file
    somebody is still writing to, so no amount of age makes it eligible — and this
    is what keeps archival off the accepted-result path entirely."""
    run_id, _ = await _finished_run(client, session_factory, name=f"live-{state.value}")
    async with session_factory() as s:
        run = await s.get(Run, run_id)
        run.status = state
        run.finished_at = LONG_AGO
        run.created_at = LONG_AGO
        await s.commit()

    done = await sweep_once(session_factory=session_factory, store=mem_store, now=NOW)

    assert done == []
    assert await _row_count(session_factory, run_id) == 5


async def test_an_old_terminal_run_is_archived_and_its_rows_are_purged(
    client, session_factory, mem_store
):
    run_id, _ = await _finished_run(client, session_factory, name="old")
    await _age(session_factory, run_id)

    done = await sweep_once(session_factory=session_factory, store=mem_store, now=NOW)

    assert [d["result"] for d in done] == ["archived"]
    assert done[0]["chunks"] == 5
    assert await _row_count(session_factory, run_id) == 0

    ptr = await _pointer(session_factory, run_id)
    assert ptr is not None
    assert ptr.object_key == archive_key(run_id, 1) == f"logs/{run_id}/1.log.gz"
    assert ptr.chunk_count == 5
    assert ptr.max_seq == 4
    assert len(ptr.sha256) == 64

    # The object really holds the lines, and holds them intact.
    body = gzip.decompress(mem_store.get_object(ptr.object_key))
    recs = decode_body(body)
    assert [r["seq"] for r in recs] == [0, 1, 2, 3, 4]
    assert [r["chunk"] for r in recs] == [f"line {i}\n" for i in range(5)]


# --------------------------------------------------------------------------
# 3. THE SAFETY RULE. These three are why this feature is allowed to delete.
# --------------------------------------------------------------------------


class _CorruptingStore:
    """An object store that accepts a write and returns something else. Stands in
    for every way a write can look successful and not be: a truncated upload, a
    silent encoding change, a wrong key served back."""

    def __init__(self, inner, corrupt):
        self._inner = inner
        self._corrupt = corrupt
        self.deleted = []

    def ensure_bucket(self):
        self._inner.ensure_bucket()

    def put_object(self, key, data, content_type):
        self._inner.put_object(key, data, content_type)

    def get_object(self, key):
        return self._corrupt(self._inner.get_object(key))

    def delete_object(self, key):
        self.deleted.append(key)
        self._inner.delete_object(key)


def _drop_last_line(raw: bytes) -> bytes:
    body = gzip.decompress(raw)
    return gzip.compress(b"".join(body.splitlines(keepends=True)[:-1]))


def _flip_a_character(raw: bytes) -> bytes:
    return gzip.compress(gzip.decompress(raw).replace(b"line 2", b"line X"))


@pytest.mark.parametrize(
    "corrupt, what",
    [(_drop_last_line, "a chunk went missing"), (_flip_a_character, "a byte changed")],
)
async def test_verify_failure_keeps_every_row(
    client, session_factory, mem_store, corrupt, what
):
    """THE ONE THAT MATTERS. The archive is written and read back; the read-back
    does not match; NOTHING is deleted.

    Both directions of the check are exercised: a body that is short by one record
    fails the count, and a body of the right length whose content changed fails the
    digest. A verification that only counted would pass the second, which is
    exactly the corruption a user would notice and we would not."""
    run_id, _ = await _finished_run(client, session_factory, name="corrupt")
    await _age(session_factory, run_id)
    store = _CorruptingStore(mem_store, corrupt)

    done = await sweep_once(session_factory=session_factory, store=store, now=NOW)

    assert [d["result"] for d in done] == ["verify_failed"], what
    assert await _row_count(session_factory, run_id) == 5, "rows were purged anyway"
    assert await _pointer(session_factory, run_id) is None, "a pointer was written"
    # The unusable object is removed, so no later pointer could ever reach it.
    assert store.deleted == [archive_key(run_id, 1)]

    # And the run still reads exactly as it did.
    resp = await client.get(f"/runs/{run_id}/logs")
    assert [r["seq"] for r in resp.json()] == [0, 1, 2, 3, 4]


async def test_crash_between_write_and_purge_keeps_rows(
    client, session_factory, mem_store, monkeypatch
):
    """A crash after the object is written and before the rows are deleted must
    leave every row where it was. Simulated by making the process die at exactly
    that point — the object exists, no pointer exists, and the log is complete."""
    run_id, _ = await _finished_run(client, session_factory, name="crash")
    await _age(session_factory, run_id)

    real_decode = logarchive.decode_body

    def die_after_the_write(body):
        # decode_body is called on the read-back, i.e. after put_object and before
        # any delete. Raising here is the crash.
        raise RuntimeError("process died between the phases")

    monkeypatch.setattr(logarchive, "decode_body", die_after_the_write)
    done = await sweep_once(session_factory=session_factory, store=mem_store, now=NOW)
    assert [d["result"] for d in done] == ["verify_failed"]

    assert await _row_count(session_factory, run_id) == 5
    assert await _pointer(session_factory, run_id) is None

    # Recovery: the next sweep, with the process healthy again, finishes the job.
    monkeypatch.setattr(logarchive, "decode_body", real_decode)
    done = await sweep_once(session_factory=session_factory, store=mem_store, now=NOW)
    assert [d["result"] for d in done] == ["archived"]
    assert await _row_count(session_factory, run_id) == 0
    resp = await client.get(f"/runs/{run_id}/logs")
    assert [r["seq"] for r in resp.json()] == [0, 1, 2, 3, 4]


async def test_re_archiving_is_idempotent(client, session_factory, mem_store):
    """Re-archiving the same attempt overwrites one object and refreshes one
    pointer. It cannot make a second of either, because the object key and the
    pointer's primary key are both (run_id, attempt) — the schema makes that
    promise, not a convention.

    The bytes are asserted identical, which is why the archive body is gzipped with
    a fixed timestamp: without that, two archives of the same records would differ
    and 'an overwrite of identical content' would be an argument instead of a
    measurement."""
    run_id, _ = await _finished_run(client, session_factory, name="idem")
    await _age(session_factory, run_id)

    # Keep the rows EXACTLY as they are, timestamps included, so the crash below is
    # a faithful one: in a real crash between the phases the rows never left, so
    # their timestamps are still the originals. Restoring them with fresh ones is a
    # different scenario — and it is the one the first draft of this test wrote by
    # accident, which is how we learned by measurement that the digest really does
    # cover `ts` and not only the text.
    async with session_factory() as s:
        original = [
            (r.id, r.attempt, r.seq, r.chunk, r.ts)
            for r in (
                await s.execute(select(RunLog).where(RunLog.run_id == run_id))
            ).scalars().all()
        ]
    assert len(original) == 5

    first = await archive_one(session_factory, mem_store, run_id, 1, NOW)
    assert first["result"] == "archived"
    bytes_first = mem_store.get_object(archive_key(run_id, 1))

    # The rows are gone, so a second pass over the same attempt has nothing to do.
    second = await archive_one(session_factory, mem_store, run_id, 1, NOW)
    assert second["result"] == "no_rows"
    assert mem_store.get_object(archive_key(run_id, 1)) == bytes_first

    # The crash case: rows present AND a pointer already there. Put the rows back
    # byte for byte and archive again — one pointer, one object, same bytes.
    async with session_factory() as s:
        for rid, attempt, seq, chunk, ts in original:
            s.add(RunLog(id=rid, run_id=run_id, attempt=attempt, seq=seq,
                         chunk=chunk, ts=ts))
        await s.commit()
    third = await archive_one(session_factory, mem_store, run_id, 1, NOW)
    assert third["result"] == "archived"
    assert third["sha256"] == first["sha256"]
    assert mem_store.get_object(archive_key(run_id, 1)) == bytes_first

    async with session_factory() as s:
        pointers = (
            await s.execute(
                select(RunLogArchive).where(RunLogArchive.run_id == run_id)
            )
        ).scalars().all()
    assert len(pointers) == 1, "a second pointer was created for one attempt"
    assert await _row_count(session_factory, run_id) == 0


async def test_a_chunk_that_arrives_after_the_archive_is_not_lost(
    client, session_factory, mem_store
):
    """A row written after the body was built is not in the archive, so it must
    survive in the database — which is why the purge deletes by the exact row ids
    it verified and never by a sequence range. The read then shows both sources as
    one log."""
    run_id, node = await _finished_run(client, session_factory, name="late")
    await _age(session_factory, run_id)
    assert (await archive_one(session_factory, mem_store, run_id, 1, NOW))["result"] == "archived"

    late = await _post_log(client, node, run_id, 1, 5, "line 5\n")
    assert late.status_code == 200, late.text

    resp = await client.get(f"/runs/{run_id}/logs")
    assert [r["seq"] for r in resp.json()] == [0, 1, 2, 3, 4, 5]
    assert await _row_count(session_factory, run_id) == 1


async def test_a_second_sweep_after_a_straggler_keeps_every_archived_line(
    client, session_factory, mem_store
):
    """The straggler above survives in the database. The NEXT sweep finds that one
    row and archives the attempt again. The object key is (run_id, attempt), so
    that write lands on top of the complete archive — and if it were built from the
    rows alone it would replace five verified lines with one. The re-archive has to
    be a merge: the prior object is read, its records joined with the fresh rows,
    and the merged body is what is written, verified and pointed at."""
    run_id, node = await _finished_run(client, session_factory, name="merge")
    await _age(session_factory, run_id)
    assert (await archive_one(session_factory, mem_store, run_id, 1, NOW))["result"] == "archived"

    late = await _post_log(client, node, run_id, 1, 5, "line 5\n")
    assert late.status_code == 200, late.text

    second = await archive_one(session_factory, mem_store, run_id, 1, NOW)
    assert second["result"] == "archived", second
    assert second["chunks"] == 6

    resp = await client.get(f"/runs/{run_id}/logs")
    assert resp.status_code == 200
    assert [r["seq"] for r in resp.json()] == [0, 1, 2, 3, 4, 5]
    assert [r["chunk"] for r in resp.json()] == [f"line {i}\n" for i in range(6)]
    assert await _row_count(session_factory, run_id) == 0

    pointer = await _pointer(session_factory, run_id)
    assert pointer.chunk_count == 6
    assert pointer.max_seq == 5
    obj = mem_store.get_object(archive_key(run_id, 1))
    assert len(decode_body(gzip.decompress(obj))) == 6
    assert pointer.size_bytes == len(obj)

    # Nothing left to move, so a third pass is a no-op and the object is untouched.
    third = await archive_one(session_factory, mem_store, run_id, 1, NOW)
    assert third["result"] == "no_rows"
    assert mem_store.get_object(archive_key(run_id, 1)) == obj


async def test_a_prior_archive_that_cannot_be_read_stops_the_purge(
    client, session_factory, mem_store
):
    """A pointer whose object is missing or unreadable is a hole nobody can see
    into. Writing a one-row body over it and purging the row would turn a hole into
    a permanent loss, so the sweep refuses: no object is written, the row stays,
    and the refusal is named."""
    run_id, node = await _finished_run(client, session_factory, name="dangle")
    await _age(session_factory, run_id)
    assert (await archive_one(session_factory, mem_store, run_id, 1, NOW))["result"] == "archived"
    late = await _post_log(client, node, run_id, 1, 5, "line 5\n")
    assert late.status_code == 200, late.text

    mem_store.delete_object(archive_key(run_id, 1))  # the pointer now dangles

    outcome = await archive_one(session_factory, mem_store, run_id, 1, NOW)
    assert outcome["result"] == "prior_unreadable", outcome
    assert await _row_count(session_factory, run_id) == 1
    with pytest.raises(KeyError):
        mem_store.get_object(archive_key(run_id, 1))
    # The pointer is left as it was: it is the evidence that something is missing.
    assert (await _pointer(session_factory, run_id)) is not None


async def test_the_merge_prefers_the_archived_record_on_a_conflict(
    client, session_factory, mem_store
):
    """Same (attempt, seq) in both places with different text. The read already
    prefers the verified archive over the database row (see `fetch_logs_since`);
    the re-archive has to make the same choice, or the read would change its answer
    the moment the row was folded in."""
    run_id, _ = await _finished_run(client, session_factory, name="conflict")
    await _age(session_factory, run_id)
    assert (await archive_one(session_factory, mem_store, run_id, 1, NOW))["result"] == "archived"

    # UNIQUE(run_id, attempt, seq) no longer sees the archived rows, so a second
    # seq 2 can be inserted. Done straight to SQL because no handler would do it.
    async with session_factory() as s:
        s.add(RunLog(run_id=run_id, attempt=1, seq=2, chunk="IMPOSTOR\n", ts=PAST))
        await s.commit()

    before = (await client.get(f"/runs/{run_id}/logs")).json()
    assert [r["chunk"] for r in before] == [f"line {i}\n" for i in range(5)]

    second = await archive_one(session_factory, mem_store, run_id, 1, NOW)
    assert second["result"] == "archived", second
    assert second["chunks"] == 5
    assert await _row_count(session_factory, run_id) == 0

    after = (await client.get(f"/runs/{run_id}/logs")).json()
    assert after == before
    assert [r["chunk"] for r in after] == [f"line {i}\n" for i in range(5)]
    assert (await _pointer(session_factory, run_id)).chunk_count == 5

# --------------------------------------------------------------------------
# 4. The read cannot tell. Captured before, compared after.
# --------------------------------------------------------------------------


@pytest.mark.parametrize("since_seq", [-1, 0, 2, 4, 99])
async def test_the_read_is_identical_before_and_after_archiving(
    client, session_factory, mem_store, since_seq
):
    """The contract, stated as an assertion: the same request returns the same
    response, byte for byte, whether the chunks are rows or an object. Captured
    first, then archived, then compared — not eyeballed for plausibility.

    The cursor values span the interesting ones: no cursor, mid-log, exactly the
    last chunk, and past the end."""
    run_id, _ = await _finished_run(client, session_factory, name=f"read{since_seq}")
    before = await client.get(f"/runs/{run_id}/logs", params={"since_seq": since_seq})
    assert before.status_code == 200

    await _age(session_factory, run_id)
    assert (await sweep_once(
        session_factory=session_factory, store=mem_store, now=NOW
    ))[0]["result"] == "archived"
    assert await _row_count(session_factory, run_id) == 0

    after = await client.get(f"/runs/{run_id}/logs", params={"since_seq": since_seq})
    assert after.status_code == 200
    assert after.json() == before.json()


async def test_a_run_with_one_attempt_archived_and_one_live_reads_as_one_log(
    client, session_factory, mem_store
):
    """The recovered-run case, which is the one this platform is built around. A
    run that ran twice, with attempt 1 archived and attempt 2 still in the
    database, reads as a single log ordered by (attempt, seq) — the same ordering
    the pure-database read used."""
    node_a = await _register(client, "merge-a")
    node_b = await _register(client, "merge-b")
    resp = await client.post(
        "/jobs",
        json={
            "name": "merge",
            "image": "fyp-dummy:latest",
            "entrypoint": ["python", "train.py"],
            "resource_reqs": {"needs_gpu": False},
            "replicas": 1,
        },
    )
    assert resp.status_code == 200, resp.text
    run_id = (await _heartbeat(client, node_a))[0]["run_id"]
    for seq in range(5):
        await _post_log(client, node_a, run_id, 1, seq, f"a-{seq}\n")

    from app.reaper import sweep_once as reap
    async with session_factory() as s:
        run = await s.get(Run, run_id)
        run.lease_expires_at = PAST
        await s.commit()
    assert (await reap(session_factory=session_factory, now=NOW))[0]["decision"] == "requeued"

    assert [a["run_id"] for a in await _heartbeat(client, node_b)] == [run_id]
    for seq in range(3):
        await _post_log(client, node_b, run_id, 2, seq, f"b-{seq}\n")

    before = (await client.get(f"/runs/{run_id}/logs")).json()
    assert len(before) == 8

    # Archive attempt 1 only, which is what a crash mid-sweep would also leave.
    assert (await archive_one(session_factory, mem_store, run_id, 1, NOW))["result"] == "archived"

    after = (await client.get(f"/runs/{run_id}/logs")).json()
    assert after == before
    assert [(r["attempt"], r["seq"]) for r in after] == [
        (1, 0), (1, 1), (1, 2), (1, 3), (1, 4), (2, 0), (2, 1), (2, 2)
    ]


async def test_a_cursor_past_the_archive_never_fetches_the_object(
    client, session_factory, mem_store
):
    """`max_seq` on the pointer is an index, and this proves it is used: a poller
    already past the archived attempt's last chunk gets its answer without the
    object store being touched at all."""
    run_id, _ = await _finished_run(client, session_factory, name="skip")
    await _age(session_factory, run_id)
    await sweep_once(session_factory=session_factory, store=mem_store, now=NOW)

    class _Refuses:
        def ensure_bucket(self):
            pass

        def put_object(self, key, data, content_type):
            raise AssertionError("wrote to the store during a read")

        def get_object(self, key):
            raise AssertionError("fetched the archive for a cursor past max_seq")

        def delete_object(self, key):
            raise AssertionError("deleted during a read")

    from app.storage import get_storage
    from app.main import app

    app.dependency_overrides[get_storage] = lambda: _Refuses()
    try:
        resp = await client.get(f"/runs/{run_id}/logs", params={"since_seq": 4})
        assert resp.status_code == 200, resp.text
        assert resp.json() == []
    finally:
        app.dependency_overrides[get_storage] = lambda: mem_store


# --------------------------------------------------------------------------
# 5. The socket path, and the fence
# --------------------------------------------------------------------------


class _FakeSocket:
    """A stand-in for one WebSocket. It records what the handler sent.

    What this covers: the handler's own body — the replay loop, the merge through
    the archive, the terminal check and the end marker. What it does NOT cover: the
    ASGI handshake and the token-before-accept refusal, which this build does not
    touch and which test_w6 already exercises. Stated so the coverage is not read
    as wider than it is."""

    def __init__(self, token):
        self.query_params = {"token": token}
        self.sent = []
        self.accepted = False
        self.closed = False

    async def accept(self):
        self.accepted = True

    async def send_json(self, payload):
        self.sent.append(payload)

    async def close(self, code=1000):
        self.closed = True


async def test_the_socket_replays_an_archived_run_and_closes(
    client, session_factory, mem_store, monkeypatch
):
    """A terminal run whose logs live in the object store still streams over the
    socket and still ends with the end marker. The handler is the real one; only
    the transport and the session factory are substituted."""
    run_id, _ = await _finished_run(client, session_factory, name="ws")
    await _age(session_factory, run_id)
    assert (await sweep_once(
        session_factory=session_factory, store=mem_store, now=NOW
    ))[0]["result"] == "archived"
    assert await _row_count(session_factory, run_id) == 0

    from app.api import runs as runs_api
    from app.models import User

    async with session_factory() as s:
        user = (await s.execute(select(User))).scalars().first()
        token, _ = create_access_token(user)

    monkeypatch.setattr(runs_api, "SessionLocal", session_factory)
    sock = _FakeSocket(token)
    await asyncio.wait_for(
        runs_api.ws_run_logs(sock, run_id, store=mem_store), timeout=10
    )

    assert sock.accepted and sock.closed
    chunks = [m for m in sock.sent if "seq" in m]
    assert [m["seq"] for m in chunks] == [0, 1, 2, 3, 4]
    assert [m["chunk"] for m in chunks] == [f"line {i}\n" for i in range(5)]
    assert sock.sent[-1] == {"end": True, "run_status": "SUCCEEDED"}


async def test_a_stale_attempt_log_post_on_an_archived_run_is_still_409(
    client, session_factory, mem_store
):
    """The fence is untouched by archival, and this is the proof rather than the
    claim. The attempt check reads `runs.attempt`; it has never read the log rows,
    so it does not care whether they are in the database, in the object store, or
    half in each."""
    node_a = await _register(client, "fence-a")
    node_b = await _register(client, "fence-b")
    resp = await client.post(
        "/jobs",
        json={
            "name": "fence",
            "image": "fyp-dummy:latest",
            "entrypoint": ["python", "train.py"],
            "resource_reqs": {"needs_gpu": False},
            "replicas": 1,
        },
    )
    assert resp.status_code == 200, resp.text
    run_id = (await _heartbeat(client, node_a))[0]["run_id"]
    for seq in range(3):
        await _post_log(client, node_a, run_id, 1, seq, f"a-{seq}\n")

    from app.reaper import sweep_once as reap
    async with session_factory() as s:
        run = await s.get(Run, run_id)
        run.lease_expires_at = PAST
        await s.commit()
    await reap(session_factory=session_factory, now=NOW)
    await _heartbeat(client, node_b)          # attempt 1 -> 2, node B now owns it

    # Attempt 1's chunks move to the object store.
    assert (await archive_one(session_factory, mem_store, run_id, 1, NOW))["result"] == "archived"

    # The zombie wakes up and posts against the attempt that has been archived.
    stale = await _post_log(client, node_a, run_id, 1, 3, "zombie\n")
    assert stale.status_code == 409, stale.text
    assert "stale attempt" in stale.json()["detail"] or "not owned" in stale.json()["detail"]

    # And nothing of the zombie's reached the log, from either source.
    body = (await client.get(f"/runs/{run_id}/logs")).json()
    assert [r["chunk"] for r in body] == ["a-0\n", "a-1\n", "a-2\n"]


# --------------------------------------------------------------------------
# 6. The encoding, checked directly
# --------------------------------------------------------------------------


def test_the_body_round_trips_awkward_content():
    """Log lines are arbitrary container output. A chunk holding a newline, a quote,
    a tab or a non-ASCII character has to come back exactly, or the archive is a
    lossy copy of the thing it replaced."""
    records = [
        {"run_id": "r", "attempt": 1, "seq": 0, "chunk": 'a "quoted" line\n', "ts": None},
        {"run_id": "r", "attempt": 1, "seq": 1, "chunk": "tab\there\nand a second line\n",
         "ts": "2026-08-22T12:00:00+00:00"},
        {"run_id": "r", "attempt": 1, "seq": 2, "chunk": "accents: éàü — ok\n",
         "ts": "2026-08-22T12:00:01+00:00"},
        {"run_id": "r", "attempt": 1, "seq": 3, "chunk": "", "ts": None},
    ]
    body = logarchive.encode_body(records)
    assert logarchive.decode_body(body) == records
    # Canonical: the same records always produce the same bytes, whatever order
    # they arrive in. That is what lets the digest be the receipt.
    assert logarchive.encode_body(list(reversed(records))) == body
    assert logarchive.compress(body) == logarchive.compress(body)


def test_a_malformed_body_raises_rather_than_returning_an_empty_log():
    """A body we cannot read is a failed verification, never an empty archive.
    Returning [] here would turn a corrupted object into a run that silently has
    no output."""
    with pytest.raises(Exception):
        decode_body(b'{"run_id": "r", "attempt": 1, "seq": 0}\n')   # no chunk, no ts
    with pytest.raises(Exception):
        decode_body(b"not json at all\n")


def test_settings_are_the_ones_the_app_actually_runs_with():
    """`get_settings` is cached, so a test that read `Settings()` directly would be
    checking a different object from the one the archiver uses. This checks the one
    it uses.

    The lease line joined this test on 2026-08-29 rather than becoming a test of
    its own, because a new test moves the suite count into four report sites and a
    Table 23 row, and this assertion is the same question the test already asks: is
    the value the app is running with the value we declared?

    `shipped_lease_ttl_s()` reads the field declaration, so asserting 60 here pins
    the number one layer above it and a silent edit fails loudly. The OTHER half of
    the agreement -- that docker-compose.yml's `${LEASE_TTL_S:-60}` fallback carries
    the same 60 -- cannot be checked from here: the canonical container mounts only
    ./control-plane, so the compose file does not exist inside it. That half lives
    in scripts/tests/test_lease_default_agreement.py, which runs on the host and is
    in neither counted suite."""
    assert get_settings().log_retention_days == 7.0
    assert shipped_lease_ttl_s() == 60
    assert get_settings().lease_ttl_s == shipped_lease_ttl_s()
