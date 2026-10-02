"""Artifacts (protocol.md §9–§10) — W6. FR-9 (outputs collected centrally) +
part of FR-10 (unified results view reads these).

Four endpoints, one feature:
  * ``POST /agent/runs/{id}/artifacts`` (node token) — the agent uploads one file.
    Fenced exactly like a log/status post; stored to MinIO, then indexed.
  * ``GET /runs/{id}/artifacts`` (JWT) — list a run's RESULTS (current attempt only).
  * ``GET /artifacts/{id}/download`` (JWT) — stream a RESULT's bytes back THROUGH the
    control plane. The browser never talks to MinIO (brokered download).
  * ``GET /agent/runs/{id}/checkpoint`` (node token) — the newest CHECKPOINT for this
    run, across attempts, so a re-dispatched run resumes instead of restarting
    (2026-08-13).

**Results are fenced; checkpoints are not — and that is not a contradiction.** A
result is a claim about a finished run, and accepting two of them would break
at-most-once, the guarantee the whole project rests on. A checkpoint is intermediate
training state; reading a stale one costs repeated work and nothing else. Sharper:
**the request is fenced, the object is not** — the agent asking for a checkpoint must
still own the run at its current attempt (the ordinary fence, applied to the reader).
What is relaxed is only which attempt the returned *object* may have come from.

That rule lives in ``_artifact_query`` — in the query, not in which route asked — so
a caller that asks for a result from another attempt gets nothing back whichever door
it came through.

Storage is brokered: only the control plane holds MinIO credentials (A.8.31), and
the deterministic object key makes a re-sent upload idempotent — the artifact twin
of the log-dedup argument.

**Checkpoints are cleaned up; results are kept (2026-08-29).** The supervisor asked
on 28 August what a checkpoint costs at scale, and the honest answer was worse than
it needed to be: a stable name bounded a checkpoint *within* an attempt, but a
re-dispatched run left one per attempt and a finished run left its last one for ever.
So two deletes, both narrow by `kind`: a new attempt's checkpoint supersedes the
older attempts' once its bytes have been read back and matched, and a run reaching a
terminal state loses its checkpoint altogether. Storage is now "latest, while
running" instead of "latest per attempt, for ever". None of this touches
`_artifact_query`: the cleanup decides what is worth STORING, never what may be READ,
and the fence lives entirely in the second of those.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
from datetime import datetime, timezone
from urllib.parse import quote

from fastapi import (
    APIRouter,
    Depends,
    File,
    Form,
    HTTPException,
    Response,
    UploadFile,
)
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from ..auth import require_node
from ..config import get_settings
from ..db import get_session
from ..models import Artifact, Job, JobKey, Node, Run, RunStatus, User
from ..quota import check_retained, get_tier, lock_user, mb
from ..schemas import ArtifactOut
from ..sealing import SealError, is_sealed, key_from_b64, open_any
from ..storage import ObjectStore, get_storage
from ..ownership import run_for_user
from ..userauth import require_user

log = logging.getLogger("artifacts")

router = APIRouter(tags=["artifacts"])

# What a stored file IS. A finished output (the default, and what every row written
# before 2026-08-13 is) versus intermediate training state a later attempt may resume
# from. Values, not free text: an unknown `kind` on upload is a 422.
KIND_RESULT = "result"
KIND_CHECKPOINT = "checkpoint"
KINDS = (KIND_RESULT, KIND_CHECKPOINT)


def _artifact_query(run_id: str, kind: str, current_attempt: int):
    """THE cross-attempt rule, in one place.

      * ``result``     -> the run's CURRENT attempt only. A presumed-dead machine's
                          leftovers can never surface as the accepted result. This is
                          at-most-once, and it does not move.
      * ``checkpoint`` -> any attempt. This is what lets a re-dispatched run pick up
                          where the dead one stopped, and the worst it can cost is
                          repeated training — never a wrong result.

    Written as a query rather than as a rule about routes on purpose: both user reads
    and the agent's checkpoint read go through it, so neither door can drift from the
    other, and asking for a cross-attempt *result* returns nothing no matter who asks.
    """
    q = select(Artifact).where(Artifact.run_id == run_id, Artifact.kind == kind)
    if kind != KIND_CHECKPOINT:
        q = q.where(Artifact.attempt == current_attempt)
    return q


async def collect_checkpoints(
    session: AsyncSession, run_id: str, *, older_than: int | None = None
) -> list[str]:
    """Delete a run's checkpoint ROWS and return the object keys they pointed at.

    Rows only — the caller commits, and the caller drops the objects afterwards with
    `drop_objects`. Splitting it that way is what puts the two failures on the safe
    side of the line (see `drop_objects`).

    `older_than=None` removes every checkpoint the run has. `older_than=N` removes
    only those written by an attempt strictly before N, which is the supersede case.
    Results are never matched: the filter is on `kind`, and the result is the thing
    the run existed to produce."""
    q = select(Artifact).where(
        Artifact.run_id == run_id, Artifact.kind == KIND_CHECKPOINT
    )
    if older_than is not None:
        q = q.where(Artifact.attempt < older_than)
    rows = (await session.execute(q)).scalars().all()
    keys = [r.object_key for r in rows]
    for row in rows:
        await session.delete(row)
    return keys


async def drop_objects(storage: ObjectStore, keys: list[str]) -> None:
    """Remove the bytes, one key at a time, and never raise.

    This runs AFTER the rows are committed, and that ordering is the whole guarantee.
    The two partial failures are deliberately not symmetric:

      * the row goes and the object does not -> bytes nothing can reach any more.
        A leak, invisible, costing storage and nothing else.
      * the object goes and the row does not -> a row promising bytes that are gone.
        That one would matter, and it is why the rows go first: it cannot happen
        without the surrounding transaction also failing, which means the result was
        not recorded either. And even if it somehow did, `fetch_checkpoint` already
        treats unreadable bytes as ABSENT rather than as an error, so the worst case
        degrades to "start from the top".

    So a failure here is logged and swallowed. The run's result is already accepted
    at this point, and nothing may take that back to tidy up a file.

    This is deliberately weaker than the log archiver's verify-then-purge two-phase
    commit, and the difference is in what is being deleted rather than in how much
    care we felt like taking. A log line is the only copy of something a person may
    still want to read, so it is written, read back, digest-matched and only then
    removed. A checkpoint is disposable by construction: it is working state on the
    way to a result, and the worst a wrongly deleted one can ever cost is repeated
    training — never a wrong answer."""
    for key in keys:
        try:
            await asyncio.to_thread(storage.delete_object, key)
        except Exception:  # noqa: BLE001 - a leaked object may not fail a run
            log.warning("could not delete checkpoint object %s; leaving it", key)


async def _supersede_older_checkpoints(
    session: AsyncSession,
    storage: ObjectStore,
    run_id: str,
    attempt: int,
    key: str,
    digest: str,
) -> None:
    """One checkpoint per RUN, not one per attempt.

    A re-dispatched run used to leave a checkpoint behind on every attempt it had
    lived through, and only the newest was ever readable — the read takes the highest
    attempt — so the rest were storage nobody could reach.

    The delete is authorised by reading the new checkpoint back out of the store and
    matching the digest the control plane computed from the bytes it stored. If that
    read cannot be trusted, nothing older is removed: an unverifiable new checkpoint
    must never be the reason a verified old one is lost.

    The read-back is paid ONLY when there is something older to delete, so the common
    case — a first attempt saving every thirty seconds — costs nothing extra. A
    re-dispatched run pays one extra fetch per save, which is the price of not
    keeping every dead attempt's copy for ever."""
    doomed = (
        await session.execute(
            select(Artifact.id).where(
                Artifact.run_id == run_id,
                Artifact.kind == KIND_CHECKPOINT,
                Artifact.attempt < attempt,
            )
        )
    ).first()
    if doomed is None:
        return  # nothing older — no delete to authorise, so no read-back to pay for

    try:
        stored = await asyncio.to_thread(storage.get_object, key)
    except Exception:  # noqa: BLE001 - unreadable new bytes authorise nothing
        log.warning(
            "checkpoint %s could not be read back; keeping older checkpoints", key
        )
        return
    if hashlib.sha256(stored).hexdigest() != digest:
        log.warning(
            "checkpoint %s read back with a different digest; keeping older "
            "checkpoints", key,
        )
        return

    keys = await collect_checkpoints(session, run_id, older_than=attempt)
    await session.commit()
    await drop_objects(storage, keys)


# The label a refused unsealed output carries, in the body of the 422 and in the
# run's `failure_reason` afterwards. One string, defined where the refusal is made;
# the agent carries its own copy for the status it posts, the same way it already
# carries OOM_KILLED.
UNSEALED_OUTPUT = "UNSEALED_OUTPUT"


def artifact_size_cap() -> int:
    """Per-file cap in bytes. A dependency (not a bare constant) so a test can
    override it to a tiny value and exercise the 413 path without a 50 MB upload."""
    return get_settings().max_artifact_mb * 1024 * 1024


def _safe_name(filename: str) -> str:
    """Reduce a claimed filename to a bare basename — no directories, no traversal.
    The object key is built from this, so it must never contain a path separator."""
    name = os.path.basename(filename.replace("\\", "/")).strip()
    if not name or name in (".", ".."):
        raise HTTPException(status_code=422, detail="invalid artifact filename")
    return name


def _object_key(run_id: str, attempt: int, filename: str) -> str:
    # Deterministic on purpose: a re-sent upload overwrites the SAME object and
    # upserts the SAME row, so a lost-reply retry stores exactly one copy.
    return f"runs/{run_id}/{attempt}/{filename}"


async def _refuse_over_quota(
    session: AsyncSession,
    run: Run,
    attempt: int,
    name: str,
    key: str,
    incoming: int,
) -> None:
    """The retained cap, applied to one upload (R3, 2026-09-04).

    Runs AFTER the whole fence and after the existing per-file `413`, and that
    ordering is load-bearing in both directions:

      * after the fence, because **a stale attempt must never consume quota**. A
        zombie execution that the platform has already given up on is refused with
        `409` before this code is reached, so a machine nobody is waiting for cannot
        spend the storage of a user whose work has already moved on;
      * after the per-file cap, and reusing its `413`, because an agent that predates
        this change already treats `413` from this call as fatal-for-this-file. It
        needs no update to behave correctly against the new refusal — it skips the
        file, exactly as it would for an over-size one. Only the `reason` in the body
        differs, and the CONTROL PLANE is what turns the refusal into a failed run,
        so nothing depends on the agent understanding it.

    **A repeat under a stable name is charged only its growth.** A checkpoint is
    written to the same object key every time (protocol.md §9, 2026-08-13), so a
    repeat refreshes one row with new bytes rather than adding a second. Charging the
    whole file each time would mean a run that saves every thirty seconds appearing
    to consume its checkpoint over and over until it was refused for space it was
    never using. The test is therefore `used - old_size + new_size <= cap`: an
    unchanged checkpoint costs nothing more, and a growing one costs its growth.

    A refusal STAMPS THE RUN and commits that stamp before raising, which is what
    makes R4 possible: whatever the agent posts afterwards, the control plane already
    knows this attempt was refused storage and records the outcome itself. It does not
    ask the agent to be honest about it, exactly as W5c decides an escalation from its
    own facts rather than from the agent's opinion.

    A job with no owner is not checked at all — the chaos test creates jobs with no
    user, and no user means no tier and nothing to enforce."""
    job = await session.get(Job, run.job_id)
    user_id = job.user_id if job is not None else None
    owner = await lock_user(session, user_id)
    if owner is None:
        return
    tier = await get_tier(session, owner.tier_id)
    if tier is None:
        return
    existing_size = (
        await session.execute(
            select(func.coalesce(Artifact.size, 0)).where(
                Artifact.run_id == run.id,
                Artifact.attempt == attempt,
                Artifact.object_key == key,
            )
        )
    ).scalar_one_or_none() or 0
    state = await check_retained(
        session, owner, tier, incoming=incoming, replacing=int(existing_size)
    )
    if state.fits:
        return

    detail = (
        f"{name} ({mb(incoming)} MB) was refused: storing it would take "
        f"{owner.username} to {mb(state.would_be)} MB against a "
        f"{mb(state.cap)} MB retained-storage cap."
    )
    run.quota_refused_at = datetime.now(timezone.utc)
    run.quota_refused_detail = detail
    await session.commit()
    raise HTTPException(
        status_code=413,
        detail=state.payload(file_mb=mb(incoming), detail=detail),
    )


def _out(a: Artifact) -> ArtifactOut:
    return ArtifactOut(
        artifact_id=a.id,
        run_id=a.run_id,
        attempt=a.attempt,
        filename=a.object_key.rsplit("/", 1)[-1],
        object_key=a.object_key,
        size=a.size,
        content_type=a.content_type,
        kind=a.kind,
    )


@router.post("/agent/runs/{run_id}/artifacts", response_model=ArtifactOut)
async def upload_artifact(
    run_id: str,
    attempt: int = Form(...),
    filename: str = Form(...),
    file: UploadFile = File(...),
    # Additive and defaulted, so an agent that predates this field still uploads
    # results exactly as before. An unknown value is a 422 — never silently a result.
    kind: str = Form(KIND_RESULT),
    node: Node = Depends(require_node),
    session: AsyncSession = Depends(get_session),
    storage: ObjectStore = Depends(get_storage),
    cap: int = Depends(artifact_size_cap),
) -> ArtifactOut:
    """Store one of a run's output files (node-token auth). Fenced like every other
    agent post:

      * unknown run          -> 404
      * not this node's run  -> 409 (abort)
      * stale attempt        -> 409 (a zombie's file can't become the result; abort)
      * over the size cap    -> 413
      * unknown `kind`       -> 422
      * over the owner's retained storage cap -> 413 `STORAGE_QUOTA_EXCEEDED`
        (2026-09-04), nothing stored, and the run's attempt marked refused so the
        control plane — not the agent — decides the outcome

    Idempotent: the object key is deterministic, so a re-sent upload overwrites the
    same object, and `UNIQUE(run_id, attempt, object_key)` collapses the retry to the
    same row — a lost-200 retry stores exactly one copy (the log-dedup twin).

    2026-08-13: `kind` (defaulted to `result`) says whether this is a finished output
    or a checkpoint a later attempt may resume from. A checkpoint is written to a
    stable name and therefore to the SAME object key every time, so a run keeps one
    checkpoint per attempt no matter how often it saves — bounded storage with no
    cleanup job. Because those repeats carry NEW bytes, the row is refreshed to
    describe the object actually stored; a row whose digest described older bytes
    would fail every verification that followed.

    The digest is computed HERE, from the bytes the control plane stored, rather than
    accepted from the uploader — so it is a fact about what is in storage. It cannot
    catch an agent that lies about the bytes themselves, which is not the threat: it
    catches corruption and truncation between here and a later resume."""
    run = (
        await session.execute(select(Run).where(Run.id == run_id).with_for_update())
    ).scalar_one_or_none()
    if run is None:
        raise HTTPException(status_code=404, detail="unknown run_id")
    if run.node_id != node.id:
        raise HTTPException(status_code=409, detail="run not owned by this node")
    if attempt != run.attempt:
        raise HTTPException(status_code=409, detail="stale attempt — abort run")
    if run.status not in (RunStatus.ASSIGNED, RunStatus.RUNNING):
        raise HTTPException(status_code=409, detail="run is no longer live — abort run")

    if kind not in KINDS:
        raise HTTPException(status_code=422, detail=f"unknown artifact kind: {kind}")

    data = await file.read()
    if len(data) > cap:
        raise HTTPException(status_code=413, detail="artifact exceeds size cap")

    # Sealed by default (2026-09-06). On a sealed job, bytes that are not sealed are
    # REFUSED, and nothing is stored.
    #
    # This is the door where "storage holds only sealed bytes" stops being a claim
    # about how the workload behaves and becomes a property of the system: the check
    # reads the FORMAT of the bytes in front of it, so a container that wrote its
    # results with plain `open` cannot get them into storage however it was built,
    # and no amount of trusting the agent is involved. The agent turns this refusal
    # into a failed run with `UNSEALED_OUTPUT`, which is the honest outcome — a run
    # whose results cannot be kept has not succeeded.
    #
    # `422` rather than `413`: the file is not too big, it is the wrong thing. And
    # unlike the quota refusal this needs no stamp on the run, because the guarantee
    # does not depend on what the agent does next. An agent that ignores the refusal
    # simply stores nothing, which is the same outcome for the data either way.
    job = await session.get(Job, run.job_id)
    if job is not None and job.sealed and not is_sealed(data):
        raise HTTPException(
            status_code=422,
            detail={
                "reason": UNSEALED_OUTPUT,
                "filename": _safe_name(filename),
                "detail": (
                    "this job is sealed, so its outputs must be sealed inside the "
                    "container before they leave it — write them through the "
                    "reader/writer in your image (fyp_data), not through open()"
                ),
            },
        )

    digest = hashlib.sha256(data).hexdigest()
    name = _safe_name(filename)
    key = _object_key(run_id, attempt, name)
    # R3: the owner's retained cap, checked BEFORE a byte reaches storage. Nothing is
    # written when this refuses — which is the difference between a cap the control
    # plane enforces and one the storage layer would enforce after the transfer.
    await _refuse_over_quota(session, run, attempt, name, key, len(data))
    # Write the bytes first (idempotent overwrite), then index the row. If the row
    # already exists (a retry), the object write was a harmless overwrite.
    await asyncio.to_thread(storage.put_object, key, data, file.content_type)

    # The run lock serializes uploads and completion. Update retries in this
    # transaction; rolling back a duplicate insert would release that lock
    # between the object write and its metadata update.
    art = (
        await session.execute(select(Artifact).where(
            Artifact.run_id == run_id, Artifact.attempt == attempt,
            Artifact.object_key == key,
        ))
    ).scalar_one_or_none()
    if art is None:
        art = Artifact(run_id=run_id, attempt=attempt, object_key=key, kind=kind)
        session.add(art)
    # Preserve the existing kind: a retry cannot turn a checkpoint into a result.
    art.size = len(data)
    art.content_type = file.content_type
    art.sha256 = digest
    art.sealed = is_sealed(data)
    await session.commit()
    out = _out(art)
    if kind == KIND_CHECKPOINT:
        await _supersede_older_checkpoints(
            session, storage, run_id, attempt, key, digest
        )
    return out


@router.get("/runs/{run_id}/artifacts", response_model=list[ArtifactOut])
async def list_run_artifacts(
    run_id: str,
    user: User = Depends(require_user),
    session: AsyncSession = Depends(get_session),
) -> list[ArtifactOut]:
    """A run's RESULT files — the CURRENT attempt only, so a stale attempt's
    leftovers are never shown as the accepted result.

    Checkpoints are excluded by the same query rule: they are working state, not a
    finished output, and this is the view the results screen reads. A checkpoint can
    therefore never appear to a user as something the run produced.

    Owner-scoped from 2026-09-07: somebody else's run answers the same `404` as a run
    that does not exist. Two filters now, answering two different questions — the
    attempt filter says WHICH files of this run are the accepted result, and this one
    says whether the run is yours to ask about at all."""
    run = await run_for_user(session, run_id, user)
    rows = (
        await session.execute(
            _artifact_query(run_id, KIND_RESULT, run.attempt).order_by(
                Artifact.object_key
            )
        )
    ).scalars().all()
    return [_out(a) for a in rows]


@router.get("/artifacts/{artifact_id}/download")
async def download_artifact(
    artifact_id: str,
    user: User = Depends(require_user),
    session: AsyncSession = Depends(get_session),
    storage: ObjectStore = Depends(get_storage),
) -> Response:
    """Stream an artifact's bytes back through the control plane (brokered — the
    browser never touches MinIO). JWT-gated like every user read.

    Scoped to the run's CURRENT attempt, exactly like the listing above. Without
    that filter the list and the download disagreed: the list hid a stale attempt's
    files and the download served them to anyone holding the id, which contradicts
    the claim that a presumed-dead machine's leftovers never surface as the accepted
    result. `409` rather than `404` because the row exists and the reason is the
    fence — the same code, and the same word, every other stale-attempt rejection
    uses.

    2026-08-13: the check is now made by re-reading the row THROUGH `_artifact_query`
    with `kind=result`, so this door and the listing above enforce one rule written
    once. A checkpoint is refused here for the same reason and with the same code —
    it exists, and it is not this run's accepted result.

    **Owner-scoped from 2026-09-07, and this door mattered most of the three.** The
    listing and the log read hand out a description of somebody's work; this one hands
    out the work — and since 2026-09-06 it also UNSEALS it on the way past, so the one
    place the platform opens the seal on the owner's behalf was the one place that did
    not check who the owner was. An artefact belonging to somebody else's job is now
    `404`, the same as an artefact id that was never minted; the ownership check is
    made before the bytes are fetched, so a refused caller never causes a read from
    the object store either."""
    art = await session.get(Artifact, artifact_id)
    if art is None:
        raise HTTPException(status_code=404, detail="unknown artifact_id")
    try:
        run = await run_for_user(session, art.run_id, user)
    except HTTPException:
        # Same words as an artefact id that does not exist: which of the two it is
        # would otherwise say whether this id names one of somebody else's results.
        raise HTTPException(status_code=404, detail="unknown artifact_id")
    art = (
        await session.execute(
            _artifact_query(art.run_id, KIND_RESULT, run.attempt).where(
                Artifact.id == artifact_id
            )
        )
    ).scalar_one_or_none()
    if art is None:
        raise HTTPException(
            status_code=409, detail="stale attempt — not this run's accepted result"
        )
    try:
        data = await asyncio.to_thread(storage.get_object, art.object_key)
    except Exception:  # noqa: BLE001 - object missing/unreachable -> a clean 404
        raise HTTPException(status_code=404, detail="artifact bytes not found")

    # Sealed by default (2026-09-06): the owner gets their result IN THE CLEAR, and
    # this is the one place the seal is opened on their behalf.
    #
    # It has to be here and not in the browser: a browser that could unseal would
    # need the key, and the key would then live in a page anyone can read. Opening it
    # here keeps the same shape W6 already chose for artefacts — the control plane
    # brokers, the browser never touches storage — and adds one step to it.
    #
    # A shredded key is `410 Gone`, matching the ticket door. Not `404`, because the
    # bytes are right there and are not missing; not `500`, because nothing failed.
    # The user asked for their data to become unreadable and it did, to us as much as
    # to anyone, and that is what the answer says.
    if art.sealed:
        key_row = await session.get(JobKey, run.job_id)
        if key_row is None:
            raise HTTPException(
                status_code=410,
                detail=(
                    "this job's key was deleted — its results are permanently "
                    "unreadable, by us as much as by anyone"
                ),
            )
        try:
            data = open_any(data, key_from_b64(key_row.key_b64))
        except SealError as exc:
            # The tamper check, on the way OUT. Bytes that no longer open are not
            # served half-opened or served as-is: the download fails and says why.
            raise HTTPException(
                status_code=409, detail=f"this result no longer opens: {exc}"
            )
    filename = art.object_key.rsplit("/", 1)[-1]
    # Keep ordinary filenames unchanged. Unusual names need an ASCII fallback
    # plus RFC 6266's encoded filename*, never raw Unicode or controls in a header.
    fallback = "".join(c if 32 <= ord(c) < 127 and c not in '\\"' else "_"
                       for c in filename)
    disposition = f'attachment; filename="{fallback}"'
    if fallback != filename:
        disposition += f"; filename*=UTF-8''{quote(filename, safe='')}"
    return Response(
        content=data,
        media_type=art.content_type or "application/octet-stream",
        headers={"Content-Disposition": disposition},
    )


@router.get("/agent/runs/{run_id}/checkpoint")
async def fetch_checkpoint(
    run_id: str,
    attempt: int,
    node: Node = Depends(require_node),
    session: AsyncSession = Depends(get_session),
    storage: ObjectStore = Depends(get_storage),
) -> Response:
    """The newest checkpoint for this run, so a re-dispatched run resumes instead of
    starting the training over (node-token auth).

    **The request is fenced; the object is not.** The caller must be the node that
    owns this run at its current attempt — the ordinary fence, unchanged, applied to
    the reader:

      * unknown run          -> 404
      * not this node's run  -> 409 (abort)
      * stale attempt        -> 409 (abort)

    What is relaxed is only which attempt the returned *object* came from: the query
    asks for `kind=checkpoint`, and that is the condition that permits crossing
    attempts. Ask it for a result and the attempt filter comes back, whichever route
    is calling.

    **Absent is not an error.** No checkpoint, or bytes that storage cannot produce,
    is `204` — nothing to resume from, so start at the beginning, which is exactly
    what every run did before this feature existed. The digest travels in
    `X-Checkpoint-Sha256` so the agent can verify what it received; a mismatch is
    treated by the agent as absent too, because a run that crashes on resume is worse
    than one that starts over."""
    run = await session.get(Run, run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="unknown run_id")
    if run.node_id != node.id:
        raise HTTPException(status_code=409, detail="run not owned by this node")
    if attempt != run.attempt:
        raise HTTPException(status_code=409, detail="stale attempt — abort run")

    art = (
        await session.execute(
            _artifact_query(run_id, KIND_CHECKPOINT, run.attempt)
            .order_by(Artifact.attempt.desc(), Artifact.object_key)
            .limit(1)
        )
    ).scalars().first()
    if art is None:
        return Response(status_code=204)  # nothing saved yet — start from the top

    try:
        data = await asyncio.to_thread(storage.get_object, art.object_key)
    except Exception:  # noqa: BLE001 - unreachable bytes are ABSENT, never an error
        log.warning(
            "checkpoint object %s for run %s could not be read; reporting none",
            art.object_key,
            run_id,
        )
        return Response(status_code=204)

    return Response(
        content=data,
        media_type=art.content_type or "application/octet-stream",
        headers={
            "X-Checkpoint-Sha256": art.sha256 or "",
            "X-Checkpoint-Attempt": str(art.attempt),
        },
    )
