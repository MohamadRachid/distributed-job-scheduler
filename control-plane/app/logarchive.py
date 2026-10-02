"""Run-log tiering — moving a finished run's output out of the database.

This is layer 2 of the log-retention design locked on 2026-07-24,
and the answer to the supervisor's log-rotation remark of 2026-07-22. Layer 1, the
container log caps in `docker-compose.yml`, shipped on 2026-07-31.

THE PROBLEM. `run_logs` only ever grows. Every chunk any container has ever printed
stays in PostgreSQL forever, and nothing reclaims it. On a long-lived deployment the
largest table would eventually be the output of runs nobody will read again, and it
would sit in the same database the scheduler claims work from.

WHAT WE REFUSED. Deleting old lines was the obvious fix and it breaks our own
standing guard: *no log line is silently lost*. Capping the lines per run breaks it
in a quieter way — the run that most needs its output read is the one that failed
after printing the most. So nothing is deleted. The lines MOVE.

THE SHAPE. Logs stay HOT in PostgreSQL while a run is alive: the live stream, the
catch-up cursor and the `UNIQUE(run_id, attempt, seq)` de-duplication all need them
there, and archival never touches a run that is not terminal, whatever its age. Once
a run is terminal AND older than the retention window (default seven days), its
chunks are written to ONE compressed object per (run, attempt) and the rows are
purged, leaving a pointer row behind. Reads then serve the database first and the
archive behind it, transparently, in the same shape.

WHY IT CANNOT TOUCH THE FENCE. Archival happens only after a run reaches a terminal
state, so it can never race the accepted-result path. A late report from a presumed
dead machine is refused by exactly the same attempt check as before — the fence
reads `runs.attempt`, not the log rows, so it does not care whether those rows sit
in PostgreSQL, in the object store, or half in each.

THE PURGE IS A TWO-PHASE COMMIT, AND THAT IS THE WHOLE SAFETY ARGUMENT.
A delete that trusts a write it never read back is a delete that will one day lose
data quietly. So:

  Phase 1  Build the canonical body from the rows, write it to the object store,
           then READ IT BACK OUT of the store, decompress it, parse it, re-encode
           what came back, and compare that digest against the digest of what went
           in — plus the chunk count. Nothing is deleted in this phase.
  Phase 2  Only on an exact match: in ONE transaction, re-read those exact rows by
           id, confirm they still produce the same digest, write the pointer, and
           delete them. Both land or neither does.

Any mismatch keeps the rows, removes the bad object and says so loudly. A crash
between the phases leaves the rows intact and the object unreferenced, and the next
sweep simply archives the same attempt again — the object key and the pointer's
primary key are both (run_id, attempt), so re-archiving lands on the same object and
the same pointer and never makes a duplicate of either.

RE-ARCHIVING IS A MERGE, NOT AN OVERWRITE. The purge deletes rows by id, so a chunk
that arrives after an attempt was archived survives in the database and the read
shows both sources as one log. The next sweep then finds that one row, and if it
built the body from the rows alone it would write one line over an object holding
every line that came before it — and purge the row, so the loss would be for good.
So when a pointer already exists, the prior object is fetched, decoded and merged
with the fresh rows before anything is written; on the same (attempt, seq) in both
the archived record wins, the same preference the read applies. The merged body is
what is written, verified and pointed at, while phase 2 still re-verifies and purges
the DATABASE ROWS by their own digest. If the prior object cannot be read at all,
nothing is written and nothing is purged: a hole is a retry, and writing over it
would turn the hole into a loss.

WHAT WE GAVE UP, STATED HERE BECAUSE IT IS REAL. An archived run's logs read more
slowly than a live one's: the read fetches and decompresses an object instead of
scanning an index. That is the trade — the database stays bounded and historical
reads get slower — and it is the right way round, because the reads that must be
fast are the live ones.
"""

from __future__ import annotations

import asyncio
import gzip
import hashlib
import json
import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from .config import get_settings
from .db import SessionLocal
from .models import Run, RunLog, RunLogArchive, RunStatus
from .storage import ObjectStore, get_storage

log = logging.getLogger("logarchive")

# Only a run in one of these states can be archived. A PENDING run has not started,
# and an ASSIGNED or RUNNING one is still producing output — archiving either would
# be archiving a file somebody is still writing to.
TERMINAL = (RunStatus.SUCCEEDED, RunStatus.FAILED, RunStatus.LOST)

# The object key, one per (run, attempt) — the key layout chosen on 2026-07-24.
# The body is JSON Lines (see `encode_body`), because a chunk carries a sequence
# number and a timestamp as well as its text, and an archive that dropped those
# could not reproduce the read it replaces.
KEY_TEMPLATE = "logs/{run_id}/{attempt}.log.gz"

_CONTENT_TYPE = "application/gzip"


def archive_key(run_id: str, attempt: int) -> str:
    return KEY_TEMPLATE.format(run_id=run_id, attempt=attempt)


# --------------------------------------------------------------------------
# The record shape — ONE definition, used by the archive and by both reads.
# --------------------------------------------------------------------------
# `api/runs.py` renders every chunk through this function, for the HTTP read and
# for the socket alike, and the archive stores exactly what it produces. That is
# deliberate: it is what makes an archived read byte-identical to a live one
# instead of merely similar. Two functions producing "the same" shape in two
# modules is the defect that cost us a silent resume failure on 2026-08-13 —
# staging wrote one constant and the runner read another, each half green against
# its own. One definition, imported by both.


def log_record(r: RunLog) -> dict:
    """The wire shape of one log chunk. Also the archive's storage shape."""
    return {
        "run_id": r.run_id,
        "attempt": r.attempt,
        "seq": r.seq,
        "chunk": r.chunk,
        "ts": r.ts.isoformat() if r.ts is not None else None,
    }


def _sort_key(rec: dict) -> tuple[int, int]:
    return (rec["attempt"], rec["seq"])


def encode_body(records: list[dict]) -> bytes:
    """The canonical archive body: one JSON object per line, keys sorted, UTF-8.

    Canonical on purpose. The verification below compares a digest of what we
    wrote against a digest of what we read back and re-encoded, so the encoding
    has to be a function of the records alone — if key order or spacing could
    drift, the digest would drift with it and the check would report a corruption
    that never happened.
    """
    out = bytearray()
    for rec in sorted(records, key=_sort_key):
        out += json.dumps(rec, sort_keys=True, ensure_ascii=False).encode("utf-8")
        out += b"\n"
    return bytes(out)


def decode_body(body: bytes) -> list[dict]:
    """Parse an archive body back into records. Raises on anything malformed — a
    body we cannot read is a failed verification, never an empty archive."""
    records = []
    for line in body.decode("utf-8").splitlines():
        if not line.strip():
            continue
        rec = json.loads(line)
        # Guard the shape rather than trusting it: a body missing a field would
        # otherwise surface later as a half-rendered log line to the user.
        for field in ("run_id", "attempt", "seq", "chunk", "ts"):
            if field not in rec:
                raise ValueError(f"archived record missing {field!r}")
        records.append(rec)
    return records


def body_digest(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def merge_records(archived: list[dict], fresh: list[dict]) -> list[dict]:
    """Join what is already archived with what is still in the database, keyed on
    (attempt, seq). On a conflict the archived record wins — it has been written,
    read back and verified, and `fetch_logs_since` already prefers it, so the
    re-archive must make the same choice or the read would change its answer the
    moment the row was folded in. Identical inputs produce identical output, which
    is what keeps re-archiving byte-for-byte idempotent."""
    merged: dict[tuple[int, int], dict] = {_sort_key(rec): rec for rec in archived}
    for rec in fresh:
        merged.setdefault(_sort_key(rec), rec)
    return [merged[k] for k in sorted(merged)]


def compress(body: bytes) -> bytes:
    """gzip with a fixed timestamp, so the same records always compress to the same
    bytes. Determinism is not cosmetic here: it is what makes re-archiving after a
    crash provably an overwrite of identical content rather than a new object whose
    equivalence we would have to argue."""
    return gzip.compress(body, compresslevel=9, mtime=0)


# --------------------------------------------------------------------------
# The read path — database first, archive behind it.
# --------------------------------------------------------------------------


async def _archives_for(session: AsyncSession, run_id: str) -> list[RunLogArchive]:
    return list(
        (
            await session.execute(
                select(RunLogArchive)
                .where(RunLogArchive.run_id == run_id)
                .order_by(RunLogArchive.attempt)
            )
        )
        .scalars()
        .all()
    )


async def fetch_logs_since(
    session: AsyncSession,
    store: ObjectStore,
    run_id: str,
    since_seq: int,
    cache: dict | None = None,
) -> list[dict]:
    """Every stored chunk of this run with `seq > since_seq`, ordered by
    (attempt, seq) — from the database, from the archive, or from both.

    The filter and the ordering are exactly what the pure-database read used
    before there was an archive, applied uniformly to both sources. That is the
    contract this function has to keep: the caller cannot tell where a chunk came
    from, and neither can the response.

    `cache` is an optional per-connection dict so the socket, which re-reads on a
    ~0.6 s tick, fetches a given archived attempt from the object store once
    rather than on every poll.
    """
    rows = list(
        (
            await session.execute(
                select(RunLog)
                .where(RunLog.run_id == run_id, RunLog.seq > since_seq)
                .order_by(RunLog.attempt, RunLog.seq)
            )
        )
        .scalars()
        .all()
    )

    archives = await _archives_for(session, run_id)
    if not archives:
        # The overwhelmingly common case, and the one every existing run is in:
        # nothing has been archived, so this is the original query and the
        # original result, with no object store involved at all.
        return [log_record(r) for r in rows]

    merged: dict[tuple[int, int], dict] = {}
    for arc in archives:
        if arc.max_seq <= since_seq:
            # A cursor already past this attempt's last chunk. Nothing in the
            # object could satisfy the filter, so we do not fetch it.
            continue
        recs = None if cache is None else cache.get((run_id, arc.attempt))
        if recs is None:
            raw = await asyncio.to_thread(store.get_object, arc.object_key)
            recs = decode_body(gzip.decompress(raw))
            if cache is not None:
                cache[(run_id, arc.attempt)] = recs
        for rec in recs:
            if rec["seq"] > since_seq:
                merged[(rec["attempt"], rec["seq"])] = rec

    # The database rows go in after the archived ones and do not overwrite them.
    # In normal operation the two sets are disjoint — a chunk is in one place or
    # the other. They can only overlap if a machine posted a chunk for an attempt
    # that was already archived, which is what `UNIQUE(run_id, attempt, seq)` used
    # to prevent while the rows were still there. Preferring the verified archive
    # extends that same one-chunk-per-(attempt, seq) guarantee across the storage
    # boundary instead of dropping it at the boundary.
    for r in rows:
        merged.setdefault((r.attempt, r.seq), log_record(r))

    return [merged[k] for k in sorted(merged)]


# --------------------------------------------------------------------------
# The sweep.
# --------------------------------------------------------------------------


def _as_utc(dt: datetime | None) -> datetime | None:
    """Postgres returns aware datetimes, SQLite (tests) returns naive. Same helper
    and same reason as `reaper._as_utc`."""
    if dt is None:
        return None
    return dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)


async def _candidates(session: AsyncSession, cutoff: datetime) -> list[tuple[str, int]]:
    """(run_id, attempt) pairs that are eligible to be archived right now.

    Eligible means: the run is terminal, it ended before the cutoff, and it still
    has log rows for that attempt. A run with no rows left is skipped rather than
    given an empty archive.
    """
    rows = (
        await session.execute(
            select(Run.id, Run.finished_at, Run.created_at).where(
                Run.status.in_(TERMINAL)
            )
        )
    ).all()

    old_enough = []
    for run_id, finished_at, created_at in rows:
        # `finished_at` is the honest clock for "how long ago did this run end".
        # A terminal run without one can exist — the reaper's exhausted path sets
        # it, but a run terminal before W5b may not carry one — so `created_at` is
        # the fallback, and it is the conservative one: a run is never younger
        # than its own creation.
        ended = _as_utc(finished_at) or _as_utc(created_at)
        if ended is not None and ended < cutoff:
            old_enough.append(run_id)
    if not old_enough:
        return []

    pairs = (
        await session.execute(
            select(RunLog.run_id, RunLog.attempt)
            .where(RunLog.run_id.in_(old_enough))
            .distinct()
            .order_by(RunLog.run_id, RunLog.attempt)
        )
    ).all()
    return [(run_id, attempt) for run_id, attempt in pairs]


async def archive_one(
    factory,
    store: ObjectStore,
    run_id: str,
    attempt: int,
    now: datetime | None = None,
) -> dict:
    """Archive ONE (run, attempt): write, verify, then purge. Returns a record of
    what happened, including on refusal — a sweep that quietly does nothing is
    indistinguishable from one that quietly loses something."""
    now = now or datetime.now(timezone.utc)
    key = archive_key(run_id, attempt)

    # ---- Phase 1a: read the rows and build the body -------------------------
    async with factory() as session:
        rows = list(
            (
                await session.execute(
                    select(RunLog)
                    .where(RunLog.run_id == run_id, RunLog.attempt == attempt)
                    .order_by(RunLog.seq)
                )
            )
            .scalars()
            .all()
        )
        if not rows:
            return {"run_id": run_id, "attempt": attempt, "result": "no_rows"}
        row_ids = [r.id for r in rows]
        fresh = [log_record(r) for r in rows]
        prior = await session.get(RunLogArchive, (run_id, attempt))
        prior_key = prior.object_key if prior is not None else None

    # The digest of the ROWS ALONE. Phase 2 re-reads those rows by id and checks
    # them against this, not against the merged body: what phase 2 has to prove
    # is that the rows it is about to delete are the rows that went into the
    # archive, and the archived records it merged with were never in the database.
    rows_digest = body_digest(encode_body(fresh))

    # ---- Phase 1a': fold in what is already archived ------------------------
    # A pointer means an earlier sweep archived this attempt and these rows came
    # after it (or a crash left both). The object under that pointer holds
    # verified lines that exist nowhere else, so it is read and merged before the
    # key is written to. If it cannot be read, nothing happens to anything: the
    # rows stay, the pointer stays, and the sweep says so.
    prior_raw: bytes | None = None
    records = fresh
    if prior is not None:
        try:
            prior_raw = await asyncio.to_thread(store.get_object, prior_key)
            archived = decode_body(gzip.decompress(prior_raw))
        except Exception as exc:  # noqa: BLE001 — any failure means "touch nothing"
            log.error(
                "log archive run=%s attempt=%s: the prior object %s cannot be read "
                "(%s) — nothing written, %d rows kept in the database",
                run_id, attempt, prior_key, exc, len(rows),
            )
            return {
                "run_id": run_id,
                "attempt": attempt,
                "result": "prior_unreadable",
                "object_key": prior_key,
                "rows_kept": len(rows),
            }
        records = merge_records(archived, fresh)

    body = encode_body(records)
    digest = body_digest(body)

    # ---- Phase 1b: write it, then read it back and check --------------------
    # The COMPRESSED bytes are what storage holds, so they are what the retained
    # quota counts (2026-09-04) — not the raw body, and not the size of the rows
    # that were purged. Measured here, from the object actually written, so the
    # number recorded is a fact about storage rather than an estimate of it.
    stored_bytes = compress(body)
    await asyncio.to_thread(store.put_object, key, stored_bytes, _CONTENT_TYPE)
    try:
        raw = await asyncio.to_thread(store.get_object, key)
        readback = decode_body(gzip.decompress(raw))
        # Re-encoding what came back, rather than comparing raw bytes, is what
        # makes this a proof about the RECORDS. Byte equality would also pass for
        # an object that happened to match and could not be parsed.
        ok = (
            len(readback) == len(records)
            and body_digest(encode_body(readback)) == digest
        )
        why = "" if ok else f"{len(readback)} records back, {len(records)} sent"
    except Exception as exc:  # noqa: BLE001 — any failure means "do not purge"
        ok, why = False, str(exc)

    if not ok:
        # The object is unusable, so it must not be left where a read could find
        # it if a pointer ever appeared. The rows are untouched, which is the
        # whole point: a failed archive costs a retry, never a line of output.
        # When a prior archive was merged in, its original bytes go back under the
        # key rather than the key being removed — a pointer already reaches it,
        # and those bytes were verified when they were first written.
        log.error(
            "log archive VERIFY FAILED run=%s attempt=%s (%s) — %s, "
            "%d rows kept in the database",
            run_id, attempt, why,
            "prior object restored" if prior_raw is not None else "object removed",
            len(rows),
        )
        try:
            if prior_raw is not None:
                await asyncio.to_thread(store.put_object, key, prior_raw, _CONTENT_TYPE)
            else:
                await asyncio.to_thread(store.delete_object, key)
        except Exception as exc:  # noqa: BLE001 — best effort; next sweep retries
            log.warning("could not put back the object under %s (%s)", key, exc)
        return {"run_id": run_id, "attempt": attempt, "result": "verify_failed"}

    # ---- Phase 2: pointer + purge, in ONE transaction -----------------------
    async with factory() as session:
        again = list(
            (
                await session.execute(
                    select(RunLog).where(RunLog.id.in_(row_ids)).order_by(RunLog.seq)
                )
            )
            .scalars()
            .all()
        )
        # Re-read and re-verify against what is in the database NOW, not against
        # the list built a moment ago. If anything moved underneath us the honest
        # answer is to keep the rows and try again next sweep.
        again_digest = body_digest(encode_body([log_record(r) for r in again]))
        if len(again) != len(rows) or again_digest != rows_digest:
            log.warning(
                "log archive run=%s attempt=%s: rows changed between write and "
                "purge — rows kept, will retry",
                run_id, attempt,
            )
            return {"run_id": run_id, "attempt": attempt, "result": "rows_changed"}

        existing = await session.get(RunLogArchive, (run_id, attempt))
        max_seq = max(r["seq"] for r in records)
        if existing is None:
            session.add(
                RunLogArchive(
                    run_id=run_id,
                    attempt=attempt,
                    object_key=key,
                    chunk_count=len(records),
                    max_seq=max_seq,
                    sha256=digest,
                    archived_at=now,
                    size_bytes=len(stored_bytes),
                )
            )
        else:
            # Re-archiving — after a crash between the phases, or because a chunk
            # arrived after the first archive: the pointer already describes this
            # attempt, so it is refreshed to describe the merged body rather than
            # duplicated.
            existing.object_key = key
            existing.chunk_count = len(records)
            existing.max_seq = max_seq
            existing.sha256 = digest
            existing.archived_at = now
            existing.size_bytes = len(stored_bytes)

        # Delete by the exact ids we archived and verified — never by a range. A
        # chunk that arrived after the body was built is not in the archive, so it
        # must survive in the database, where the read path will still find it.
        await session.execute(delete(RunLog).where(RunLog.id.in_(row_ids)))
        await session.commit()

    log.info(
        "archived run=%s attempt=%s -> %s (%d chunks)",
        run_id, attempt, key, len(records),
    )
    return {
        "run_id": run_id,
        "attempt": attempt,
        "result": "archived",
        "chunks": len(records),
        "object_key": key,
        "sha256": digest,
    }


async def sweep_once(
    session_factory=None,
    store: ObjectStore | None = None,
    now: datetime | None = None,
    retention_days: float | None = None,
) -> list[dict]:
    """One archival pass. Returns one record per (run, attempt) it touched.

    `now` and `retention_days` are injectable for exactly the reason the reaper's
    `now` is: a test must be able to make a run 'old' deterministically instead of
    waiting a week for it."""
    settings = get_settings()
    factory = session_factory or SessionLocal
    store = store or get_storage()
    now = now or datetime.now(timezone.utc)
    days = settings.log_retention_days if retention_days is None else retention_days
    cutoff = now - timedelta(days=days)

    async with factory() as session:
        pairs = await _candidates(session, cutoff)

    # A pair only appears above while rows still exist for it, so an attempt that
    # was fully archived is already gone from this list. One that carries BOTH a
    # pointer and rows is a crash between the two phases or a chunk that arrived
    # after the archive, and archive_one finishes the job by merging the rows into
    # the object and purging them.
    return [await archive_one(factory, store, run_id, attempt, now)
            for run_id, attempt in pairs]


async def archive_loop() -> None:
    """The background task, started and stopped with the app beside the reaper.

    Deliberately slow: the retention window is measured in days, so sweeping more
    than once an hour would only add load to the database the scheduler claims
    work from. Like the reaper it survives a bad pass and keeps going — a failed
    archive is a retry, and a loop that dies on one is a table that grows for
    ever."""
    settings = get_settings()
    if not settings.log_archive_enabled:
        log.info("log archiving is DISABLED by configuration; no sweep will run")
        return
    interval = settings.log_archive_interval_s
    log.info(
        "log archiver started (sweep every %ss, retention %s days)",
        interval, settings.log_retention_days,
    )
    try:
        while True:
            try:
                done = [d for d in await sweep_once() if d["result"] == "archived"]
                if done:
                    log.info(
                        "archived %d run/attempt log set(s) this pass, %d chunks",
                        len(done), sum(d["chunks"] for d in done),
                    )
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 — a bad sweep must not kill the loop
                log.exception("log archive sweep failed; continuing")
            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        log.info("log archiver stopped")
        raise
