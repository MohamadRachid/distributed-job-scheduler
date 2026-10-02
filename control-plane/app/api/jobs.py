"""User-facing job endpoints (protocol.md §10).

A user submits a *job* (an image + entrypoint + where/what it may run on); the
control plane fans it out into one or more *runs* (protocol.md §4), which the
scheduler later hands to agents at pull-time.

Auth: JWT-gated from W6 (protocol.md §1). Every route requires `require_user`; on
submit the job is attributed to the caller (`jobs.user_id`).

**That attribution is read from 2026-09-07.** It used to be honest wiring with
nothing looking at it, which was defensible while a deployment had one account and
no way to make a second, and stopped being defensible the moment `POST /users`
landed. Every read below is now scoped to the caller through `app.ownership`: a
user sees their own jobs and an administrator sees all, and a job that is not
yours answers exactly what a job that does not exist answers.

There are three doors onto the same act, and the reason is always the same one:
JSON cannot carry a file. `POST /jobs` is the JSON shape it has been since W2.
`POST /jobs/with-input` (multipart) carries a dataset. `POST /jobs/private`
(multipart) is that door with `trusted_only` forced on. All three build the same Job
row through `_new_job`, and the two file doors share one implementation
(`_submit_with_file`) so they cannot drift apart.

**Sealed by default (2026-09-06).** Every job minted here gets its own 256-bit key,
whichever door it came through and whether or not it carries a file. An input file is
sealed before a byte of it is stored; results and checkpoints are sealed inside the
container before they leave it; the owner gets plaintext back through the download
door, which unseals for them. There is no flag for any of this, because there is
nothing for a user to decide — sealing costs them one line in their loader, and a
protection that must be chosen is one most people will not have.
"""

import asyncio
import logging
import os
from datetime import datetime, timedelta, timezone

from fastapi import (
    APIRouter,
    BackgroundTasks,
    Depends,
    File,
    Form,
    HTTPException,
    UploadFile,
)
from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..config import get_settings
from ..checkpoint_advisor import advise
from ..db import get_session, get_session_factory
from ..models import (
    Artifact,
    Job,
    JobKey,
    JobStatus,
    Node,
    Run,
    RunLogArchive,
    RunStatus,
    Tier,
    User,
)
from ..quota import (
    LIMITS_NOT_ACCEPTED,
    check_retained,
    get_tier,
    limits_accepted,
    lock_user,
    mb,
    retained_used_bytes,
    scratch_cap_mb,
)
from ..scheduler import MIN_SEALED_AGENT_VERSION, _version_tuple
from ..schemas import CancelResponse, JobCreate, JobCreateResponse, JobOut, RunOut
from ..ownership import job_for_user, owns, visible_jobs
from ..scheduler import recompute_job_status
from ..sealing import key_to_b64, new_key, seal_stream
from ..storage import ObjectStore, get_storage
from ..userauth import require_user
from .artifacts import collect_checkpoints, drop_objects

log = logging.getLogger("jobs")

_TERMINAL_RUN = (RunStatus.SUCCEEDED, RunStatus.FAILED)

router = APIRouter(tags=["jobs"])


async def waiting_for(session: AsyncSession, job: Job, run: Run) -> str | None:
    """Why this PENDING run is still waiting, when the reason is that the machines it
    names are not here (2026-09-06).

    **Waiting, not failing — and the choice is the point.** A run aimed at a machine
    that is offline is not hopeless: laptops come back, and the platform has said so
    since W6b, where a private job with no trusted node waits rather than failing
    because trust is one admin action away. The same reasoning holds here, and the
    contrast is W5c's `INSUFFICIENT_POOL`, which fails fast precisely because no
    amount of waiting can conjure a bigger machine. So this run waits.

    What it must not do is wait SILENTLY. Before today a run aimed at a machine that
    never came back sat PENDING for ever with nothing anywhere to say what it was
    waiting for — and, worse, it filled other machines' claim windows while it did
    (fixed the same day in `scheduler._candidates`). This is the visible half: one
    sentence naming the machines and how long they have been quiet.

    Returns None for every run that is not waiting on an absent machine, which is
    almost all of them: a run that is running, finished, untargeted, or targeted at a
    machine that is here."""
    if run.status is not RunStatus.PENDING or not job.target_node_ids:
        return None
    rows = (
        await session.execute(
            select(Node).where(Node.id.in_(list(job.target_node_ids)))
        )
    ).scalars().all()
    if not rows:
        return "the machines this run was aimed at are no longer registered"

    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(seconds=get_settings().node_timeout_s)
    absent = []
    too_old = []
    for node in rows:
        last = node.last_heartbeat
        if last is not None and last.tzinfo is None:
            last = last.replace(tzinfo=timezone.utc)   # SQLite hands back naive
        if last is not None and last >= cutoff:
            # 2026-09-07 (walk 1, row 66): a machine that is HERE but whose agent is
            # too old to be offered a sealed run is not "here" for this job. It used
            # to wait with nothing said anywhere, because the machine was online.
            if job.sealed and _version_tuple(node.agent_version) < MIN_SEALED_AGENT_VERSION:
                too_old.append((node.name, node.agent_version))
                continue
            return None          # one of its machines is here; it is not waiting on us
        absent.append((node.name, last))

    def _silent_for(last) -> str:
        if last is None:
            return "never heart-beat"
        seconds = int((now - last).total_seconds())
        if seconds < 120:
            return f"silent for {seconds}s"
        if seconds < 7200:
            return f"silent for {seconds // 60}m"
        return f"silent for {seconds // 3600}h"

    parts = []
    if absent:
        named = ", ".join(f"{name} ({_silent_for(last)})" for name, last in absent)
        parts.append(f"waiting for {named}")
    if too_old:
        need = ".".join(str(p) for p in MIN_SEALED_AGENT_VERSION)
        for name, version in too_old:
            parts.append(
                f"{name} is online but its agent is {version or 'of unknown version'} "
                f"and this job needs agent {need} or newer — upgrade the agent on that "
                "machine and this run starts there"
            )
    return "; ".join(parts) if parts else None


def _run_out(r: Run, waiting: str | None = None) -> RunOut:
    return RunOut(
        waiting_for=waiting,
        run_id=r.id,
        job_id=r.job_id,
        node_id=r.node_id,
        status=r.status.value,
        attempt=r.attempt,
        exit_code=r.exit_code,
        started_at=r.started_at,
        finished_at=r.finished_at,
        # W5b diagnostics (nullable): why it died + live progress.
        failure_reason=r.failure_reason,
        failure_detail=r.failure_detail,
        progress=r.progress,
        metrics_last=r.metrics_last,
        # W5c: the learned RAM requirement + how many times this run was escalated.
        learned_min_ram_mb=r.learned_min_ram_mb,
        escalation_count=r.escalation_count,
        cancel_requested_at=r.cancel_requested_at,
    )


def _job_out(j: Job) -> JobOut:
    return JobOut(
        job_id=j.id,
        name=j.name,
        image=j.image,
        status=j.status.value,
        replicas=j.replicas,
        target_node_ids=j.target_node_ids,
        created_at=j.created_at,
        # W6b: the lock flag + the original filename. Never the key, never the
        # object key — a read shape must not help anyone find the sealed bytes.
        private=j.private,
        input_filename=j.input_filename,
        # 2026-09-07: what the job carries and what it was charged for it.
        input_size_bytes=j.input_size_bytes,
        # 2026-09-06: what used to be one flag, told apart. `sealed` says the data is
        # sealed with this job's own key; `trusted_only` says where it may run.
        sealed=j.sealed,
        trusted_only=j.trusted_only,
        # The verdict and the evidence behind it — never `source_text` itself.
        checkpoint_advice=j.checkpoint_advice,
    )


async def _gate_submission(
    session: AsyncSession, user: User | None, req: JobCreate, private: bool = False
) -> Tier | None:
    """The two policy questions asked at BOTH submission doors, in one place.

    1. **Have you accepted your limits?** (R1) No -> `403 LIMITS_NOT_ACCEPTED`.
    2. **Are you already full?** (R2) At or above the retained cap -> `403
       STORAGE_QUOTA_EXCEEDED`, naming what is used and what the cap is.

    Refusing at SUBMISSION as well as at upload is not belt-and-braces; the two catch
    different things. Submission catches the user who is already full, before a
    worker spends hours on a job whose result could not be stored. Upload catches the
    run that fills them, because bytes are produced DURING a run and are not knowable
    before it. Both doors belong to the control plane, so both are free to ask.

    **The submission check on the JSON door reads the sum without holding the user's
    row, and that is deliberate.** It is a coarse "are you already full" question
    whose answer cannot be made stale in a way that matters: two jobs submitted at the
    same instant by a user who is under their cap should both be accepted, and a user
    who is over it will be refused at the upload door where the lock actually is. The
    private door DOES lock, because it stores bytes itself and so has to answer the
    precise question.

    It also validates the scratch ask, which is the one number the user may choose
    here: at most their tier's scratch cap. Over it is `422`, never a silent trim — a
    job quietly given less disk than it asked for fails later for a reason nobody can
    see.

    **`private` is a dead parameter as of 2026-09-06 and is kept for one release.**
    It used to switch the ceiling to `PRIVATE_TMPFS_MB`, because a private run's
    scratch WAS a RAM folder of that size. Sealed runs have the ordinary three
    folders on disk, so there is no second ceiling to switch to, and no door passes
    it any more.

    Returns the caller's tier (or None when the job has no owner — the chaos test
    submits through this handler with no user, and no user means no tier and nothing
    to enforce)."""
    if not isinstance(user, User):
        return None
    tier = await get_tier(session, user.tier_id)
    if not limits_accepted(user, tier):
        raise HTTPException(
            status_code=403,
            detail={
                "reason": LIMITS_NOT_ACCEPTED,
                "detail": (
                    "accept your storage limits before submitting — "
                    "see GET /me and POST /me/accept-limits"
                ),
            },
        )
    used = await retained_used_bytes(session, user.id)
    cap = tier.retained_cap_bytes if tier is not None else 0
    if used >= cap:
        raise HTTPException(
            status_code=403,
            detail={
                "reason": "STORAGE_QUOTA_EXCEEDED",
                "used_mb": mb(used),
                "cap_mb": mb(cap),
                "detail": (
                    "you are at your retained-storage cap; free space with "
                    "DELETE /jobs/{job_id}/storage and submit again"
                ),
            },
        )
    ask = req.resource_reqs.scratch_mb
    if ask is not None:
        ceiling = scratch_cap_mb(tier)
        if ask <= 0:
            raise HTTPException(
                status_code=422, detail="scratch_mb must be greater than zero"
            )
        if ceiling is not None and ask > ceiling:
            # In words a person reads on the form (walk 1, row 52): the number, the
            # limit, whose limit, and what to do.
            raise HTTPException(
                status_code=422,
                detail=(
                    f"Temporary disk: {ask} MB is more than your tier ({user.tier_id}) "
                    f"allows, which is {ceiling} MB per run. Lower the number, or ask "
                    "an admin for a bigger tier."
                ),
            )
    return tier


def _fan_out(job: Job, req: JobCreate) -> list[Run]:
    """A job becomes N PENDING runs (protocol.md §4): one per targeted node, or
    `replicas` if no nodes were named. Runs are created node-agnostic — the actual
    node is chosen at pull-time by the scheduler, which increments `attempt` then."""
    n_runs = len(req.target_node_ids) if req.target_node_ids else req.replicas
    return [Run(job_id=job.id, status=RunStatus.PENDING) for _ in range(n_runs)]


def _new_job(req: JobCreate, user: User | None, tier: Tier | None = None) -> Job:
    # Over HTTP, `user` is the JWT-resolved User (require_user) and the job is
    # attributed to them. The chaos test (W5) calls create_job DIRECTLY,
    # bypassing the auth layer by design, so `user` is left as the Depends sentinel
    # — no attribution there (user_id stays null). Same handler, both paths.
    reqs = req.resource_reqs.model_dump()
    # FREEZE the tier's scratch ceiling onto the job at submit time (2026-09-04).
    #
    # Two numbers live in `resource_reqs` and they mean different things, which is
    # the whole reason they are two:
    #
    #   `scratch_mb`      what the USER asked for. Absent unless they said a number.
    #                     An explicit ask is a REQUIREMENT: the scheduler will not
    #                     place the run on a machine with less free disk, and will
    #                     not offer it to an agent too old to enforce it.
    #   `scratch_cap_mb`  the CEILING their tier carried when they submitted. Always
    #                     present for an owned job. The agent stops the run at it.
    #
    # Collapsing them into one field — the brief's first shape, where an omitted ask
    # defaulted to the tier cap — makes the system deadlock, and it is worth saying
    # exactly how rather than only that it does. When this was decided the default
    # tier's scratch cap was 500 GB (it is 50 GB since the re-sizing of 2026-09-05,
    # migration 20260905_a2b3c4d5e6f7). As a ceiling either number is harmless. As a
    # placement REQUIREMENT the 500 GB one meant every ordinary job demanded a machine
    # with 500 GB free, no machine in a lab pool has that, and every untargeted run
    # waits PENDING for ever. The re-sizing shrinks how badly that fails without
    # changing that it fails, which is the point: a ceiling
    # and a requirement are not the same statement about a number, and the brief's own
    # R12 already draws that line for the agent-version guard ("explicitly set is a
    # requirement ... left empty is a policy"); this carries the same line into
    # placement, where it is load-bearing.
    #
    # Frozen at submit rather than read at assignment for the same reason acceptance
    # stores numbers: the user was shown a figure and agreed to it, and a later
    # administrative change should not silently re-scope a job already running.
    if tier is not None:
        reqs["scratch_cap_mb"] = scratch_cap_mb(tier)
    return Job(
        user_id=user.id if isinstance(user, User) else None,
        name=req.name,
        # The pasted training script (2026-09-05), on BOTH doors because a private job
        # is the one that most wants this advice: its outputs come back through the
        # logs, so losing an attempt's work to a restart costs the most there.
        source_text=req.source_text or None,
        image=req.image,
        entrypoint=req.entrypoint or None,
        env=req.env or None,
        resource_reqs=reqs,
        target_node_ids=req.target_node_ids,
        replicas=req.replicas,
        status=JobStatus.PENDING,
        # Sealed by default (2026-09-06). Every job created from today carries its
        # own key and has its data sealed, on every door, with no field to set and no
        # box to tick. It is here — in the one function all three doors build a job
        # through — rather than at each door, so a fourth door cannot be added that
        # forgets.
        sealed=True,
        # The one thing the old private route bundled that is genuinely a choice.
        trusted_only=bool(req.trusted_only),
    )


def _spec(spec: str) -> JobCreate:
    """Parse the JSON spec that rides beside an uploaded file, or 422."""
    try:
        return JobCreate.model_validate_json(spec)
    except ValidationError as exc:
        raise HTTPException(status_code=422, detail=f"invalid job spec: {exc.errors()}")


@router.post("/jobs", response_model=JobCreateResponse)
async def create_job(
    req: JobCreate,
    background_tasks: BackgroundTasks,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(require_user),
    session_factory=Depends(get_session_factory),
) -> JobCreateResponse:
    """Create a job and fan it out into PENDING runs (protocol.md §4):

      target_node_ids set  -> one run per listed node (replicas ignored);
      target_node_ids null -> `replicas` runs, scheduled to any eligible node.

    For targeted jobs the scheduler enforces one run per selected node (W4, see
    scheduler._holds_sibling_run).

    W6b: `private: true` is REFUSED here (422). A private job is defined by having a
    sealed input file, and JSON cannot carry a file — so accepting the flag on this
    route could only produce a job that says "private" while nothing is sealed. When
    privacy is asked for, the only honest answers are "sealed" or "refused"."""
    if req.private:
        raise HTTPException(
            status_code=422,
            detail=(
                "a private job must be submitted with its input file — "
                "use POST /jobs/private (multipart)"
            ),
        )
    tier = await _gate_submission(session, user, req)
    job = _new_job(req, user, tier)
    session.add(job)
    await session.flush()  # assign job.id before creating its runs
    # Sealed by default (2026-09-06): a key for EVERY job, including one submitted
    # with no input file. A job with nothing to seal on the way in still has
    # something to seal on the way out — its results and its checkpoints are sealed
    # inside the container with this key before they leave it. Minting it here, on
    # the door that carries no file, is what makes "every job has its own key" true
    # without an exception anybody has to remember.
    session.add(JobKey(job_id=job.id, key_b64=key_to_b64(new_key())))
    runs = _fan_out(job, req)
    session.add_all(runs)
    await session.commit()

    # Checkpoint-use advice (2026-09-05), AFTER the commit and outside the request.
    # Two properties this ordering buys, both deliberate: the job exists before
    # anything is said about it, so the task can never advise on a row that was rolled
    # back; and a slow, broken or unreachable advisor cannot delay or fail a
    # submission, because by the time it runs the caller already has their job id.
    background_tasks.add_task(advise, job.id, session_factory)

    return JobCreateResponse(
        job_id=job.id, run_ids=[r.id for r in runs], status=job.status.value
    )


async def _submit_with_file(
    req: JobCreate,
    filename: str | None,
    data: bytes,
    session: AsyncSession,
    user: User,
    storage: ObjectStore,
) -> tuple[Job, list[Run]]:
    """The one implementation behind both file-carrying doors (2026-09-06).

    `POST /jobs/with-input` and `POST /jobs/private` used to differ in three ways:
    one sealed the file and the other did not, one restricted placement to trusted
    machines and the other did not, and one gave the container a RAM folder and no
    writable disk while the other gave it the ordinary three folders. Only the middle
    difference was ever a decision a user should be asked to make. Sealing is now the
    floor for both, the container shape is the same for both, and what is left is one
    boolean on the spec — so the two doors are one act with one flag different, and
    they are built here rather than in two places that could drift.

    In order, and the order is what makes a refusal leave nothing behind:

      1. hold the submitter's row, so two submissions cannot both see the same
         "used" figure and both decide their file fits (quota.lock_user);
      2. refuse an empty file (422), an over-size one (413), or one that would cross
         the owner's retained cap (413) — all BEFORE a key is minted or a byte
         stored;
      3. mint a fresh 256-bit key for this job, seal the file in the framed format,
         and store ONLY the sealed bytes;
      4. keep the key in `job_keys` — a different table from the data it opens, so
         crypto-shred stays one DELETE.

    From step 3 the plaintext exists nowhere we control: not in the database, not in
    object storage, not on any worker's disk. It comes back only inside the running
    container, one piece at a time, through the reader in the image."""
    locked = await lock_user(session, user.id if isinstance(user, User) else None)
    tier = await _gate_submission(session, locked or user, req)

    if not data:
        raise HTTPException(status_code=422, detail="input file is empty")
    cap = get_settings().max_input_mb * 1024 * 1024
    # The cap is applied to the PLAINTEXT the user handed over, which is the number
    # they can see and control. Sealing then adds its own small overhead (a 32-byte
    # header and 28 bytes per 4 MiB frame), and charging a user's per-file cap for
    # our framing would make an accepted file size depend on a format detail.
    if len(data) > cap:
        raise HTTPException(status_code=413, detail="input file exceeds size cap")

    key = new_key()
    blob = seal_stream(data, key)

    # Checked AFTER the per-file cap and BEFORE anything is stored, against the SEALED
    # length, because sealed bytes are what storage will actually hold and the
    # retained cap is a statement about what we keep.
    if locked is not None and tier is not None:
        state = await check_retained(session, locked, tier, incoming=len(blob))
        if not state.fits:
            raise HTTPException(
                status_code=413,
                detail=state.payload(
                    file_mb=mb(len(blob)),
                    detail=(
                        "this input file would take you over your retained-storage "
                        "cap; nothing was stored"
                    ),
                ),
            )

    job = _new_job(req, locked or user, tier)
    job.input_filename = os.path.basename((filename or "input.bin").replace("\\", "/"))
    session.add(job)
    await session.flush()  # need job.id for the object key and the key row

    # The stored name is fixed, and the user's own name is kept only as a label on
    # the job. A name that reached the object key would let a submitted filename
    # decide where bytes land, the same class of mistake `_safe_name` closes on the
    # artefact route.
    job.input_object_key = f"inputs/{job.id}/input.bin"
    # The SEALED size, which is what storage holds and therefore what the retained sum
    # must count. Recorded here rather than derived later, so the sum never has to ask
    # the object store what it is holding.
    job.input_size_bytes = len(blob)
    # Only the sealed bytes ever leave this function. `data` is dropped with the
    # request; nothing writes it anywhere.
    await asyncio.to_thread(
        storage.put_object, job.input_object_key, blob, "application/octet-stream"
    )
    session.add(JobKey(job_id=job.id, key_b64=key_to_b64(key)))

    runs = _fan_out(job, req)
    session.add_all(runs)
    await session.commit()
    return job, runs


@router.post("/jobs/with-input", response_model=JobCreateResponse)
async def create_job_with_input(
    background_tasks: BackgroundTasks,
    spec: str = Form(...),
    file: UploadFile = File(...),
    session: AsyncSession = Depends(get_session),
    user: User = Depends(require_user),
    storage: ObjectStore = Depends(get_storage),
    session_factory=Depends(get_session_factory),
) -> JobCreateResponse:
    """Submit a job that carries a dataset file (2026-09-05).

    Why a route rather than a flag on `POST /jobs`: JSON cannot carry a file. That is
    the whole reason, and it is why `/jobs/private` exists too.

    **The file is sealed (2026-09-06), and there is no way to ask for it not to be.**
    Before today this door stored the file exactly as it was handed over and only the
    private door sealed. Sealing costs the submitter nothing they can feel — the
    container reads through a reader instead of through `open` — so there was nothing
    for a flag to decide, and a protection that has to be chosen is one most people
    will not have.

    **The file is not unpacked here, and that is deliberate.** A zip is stored as a
    zip and the container opens it. Unpacking someone's archive is untrusted-input
    handling — an archive can expand to fill a disk, or carry paths that write outside
    the directory it was extracted into — and doing it in the agent would move that
    risk onto the platform. The container already has its own writable temporary
    space, measured and capped, which is exactly where an archive should be opened.

    Refusals, in the order they are checked, so a rejected submission leaves nothing
    behind: bad spec 422, `private: true` 422 (that is the other route), empty file
    422, over the per-file cap 413, over the owner's retained-storage cap 413."""
    req = _spec(spec)
    if req.private:
        raise HTTPException(
            status_code=422,
            detail=(
                "`private` names the older submission shape — use POST /jobs/private, "
                "or set `trusted_only` on this route"
            ),
        )
    job, runs = await _submit_with_file(
        req, file.filename, await file.read(), session, user, storage
    )
    background_tasks.add_task(advise, job.id, session_factory)
    return JobCreateResponse(
        job_id=job.id, run_ids=[r.id for r in runs], status=job.status.value
    )


@router.post("/jobs/private", response_model=JobCreateResponse)
async def create_private_job(
    background_tasks: BackgroundTasks,
    spec: str = Form(...),
    file: UploadFile = File(...),
    session: AsyncSession = Depends(get_session),
    user: User = Depends(require_user),
    storage: ObjectStore = Depends(get_storage),
    session_factory=Depends(get_session_factory),
) -> JobCreateResponse:
    """Submit a job whose input file is sealed and which runs only on trusted
    machines (W6b; unbundled 2026-09-06).

    **What this door meant before today, and what it means now.** It used to bundle
    four protections behind one flag: a per-job key, sealed storage, plaintext kept
    off every disk, and placement restricted to machines an admin marked trusted. The
    first three cost a user nothing at run time and are now the floor for every job,
    on every door. The fourth costs them the rest of the pool, so it stayed a choice
    — and this route is the shape of that choice that already existed. It is exactly
    `POST /jobs/with-input` with `trusted_only` forced on.

    **What it stops charging.** A job submitted here used to have no writable host
    mount at all, which meant it could collect no results and could not checkpoint,
    so a private job that lost its machine started its training again from nothing.
    That price is gone: this door's runs now keep the ordinary three folders, their
    outputs and checkpoints are sealed inside the container with the job's own key,
    and a lost run resumes on another machine. Privacy and resume no longer trade.

    **`jobs.private` is not set by this route any more.** That column names the OLD
    shape — a one-piece sealed input opened into a RAM folder — and the rows that
    carry it are still run that way. Saying "private" about a job built the new way
    would make one word mean two different container shapes, which is how a flag
    becomes a bug. What this door does is stated in the two columns that say it:
    `sealed` and `trusted_only`."""
    req = _spec(spec)
    # Forced rather than merely defaulted: this door IS the trusted-only choice, so a
    # spec that says otherwise cannot quietly widen where the job runs.
    req.trusted_only = True
    job, runs = await _submit_with_file(
        req, file.filename, await file.read(), session, user, storage
    )
    background_tasks.add_task(advise, job.id, session_factory)
    return JobCreateResponse(
        job_id=job.id, run_ids=[r.id for r in runs], status=job.status.value
    )


@router.delete("/jobs/{job_id}/key")
async def crypto_shred(
    job_id: str,
    user: User = Depends(require_user),
    session: AsyncSession = Depends(get_session),
) -> dict:
    """Crypto-shred: delete a job's key (W6b should-tier; every job since 2026-09-06).

    Deleting one small row makes EVERY sealed copy of that job's data permanently
    unreadable — the input object in MinIO, any blob still staged on a worker, any
    backup of either, and **since 2026-09-06 the job's results and checkpoints too**,
    because those are now sealed with the same key before they leave the container.
    Not because we chased the copies down, but because without the key they are noise
    and nothing can rebuild it. That is a stronger and much cheaper guarantee than
    "we deleted all the copies we know about".

    **It applies to every job now, and that is the change worth naming.** It used to
    422 on a job that was not private, because a job that was not private had nothing
    sealed to shred. Every job has its own key today, so the refusal would now be
    false — and a user who asks us to make their data unreadable and is told "this
    job has no sealed input" would be told something untrue about their own job. What
    is still refused is a job that never had a key at all: the rows from before this
    date that were submitted without the private door. `404` there, not `422`, because
    the honest answer is that there is no key of that name.

    Irreversible by design, and honest about it: the sealed objects are left in place
    (deliberately — it demonstrates that shredding does not depend on reaching them).
    Idempotent: shredding an already-shredded job is a 200 with `shredded: false`.

    **The caller's own job only (2026-09-07).** This is the most destructive route on
    the platform — it makes data unreadable for ever, and nothing can undo it — so it
    was the worst one to leave open. Somebody else's job answers `404`, the same as a
    job that does not exist."""
    job = await job_for_user(session, job_id, user)
    row = await session.get(JobKey, job_id)
    if row is None:
        # Told apart on purpose. A job that HAS been shredded answers 200 with
        # `shredded: false` — the caller's wish is already true. A job that never had
        # a key has nothing of the sort to talk about.
        if not (job.private or job.sealed):
            raise HTTPException(status_code=404, detail="job has no key")
        return {"shredded": False, "detail": "key already deleted — this job's data is unreadable"}
    await session.delete(row)
    await session.commit()
    return {
        "shredded": True,
        "detail": (
            "key deleted — every sealed copy of this job's input, results and "
            "checkpoints is now permanently unreadable"
        ),
    }


CANCELLED = "CANCELLED"


def may_cancel(user, job: Job) -> bool:
    """Who may stop a job: its owner, an admin, or anyone for a job with no owner
    (the chaos test's). Pure, so it is tested without a database.

    2026-09-07: this is `ownership.owns` and is kept as a name rather than as a second
    copy of the rule. Stopping a job and reading its results are the same question
    about the same row, and two implementations of one rule is how they come to
    disagree."""
    return owns(user, job)


@router.post("/jobs/{job_id}/cancel", response_model=CancelResponse)
async def cancel_job(
    job_id: str,
    user: User = Depends(require_user),
    session: AsyncSession = Depends(get_session),
    storage: ObjectStore = Depends(get_storage),
) -> CancelResponse:
    """Stop a job (2026-09-07, walk 1 row 64 — there was no way to).

    Three kinds of run, three answers, all in one request:

      * `PENDING` — nothing holds it, so it ends here: `FAILED`, reason `CANCELLED`;
      * `ASSIGNED` / `RUNNING` — a worker holds it. The run is STAMPED
        (`cancel_requested_at`) and that worker is told at its next heartbeat
        (`commands: [{type: "cancel", run_id}]`); it stops the container and posts
        `FAILED` with reason `CANCELLED`. If the worker never answers — it died, or
        its agent predates commands — the reaper ends the stamped run as cancelled
        when its lease expires instead of requeueing it, so a cancel is never undone
        by recovery;
      * terminal — untouched. A result, once accepted, is final (protocol.md §7).

    No new state. A cancelled run is a run that ended without a result, and that is
    `FAILED` with a reason, where every other ending already lives. Fencing is
    untouched too: a stale attempt's late post is still 409, whatever the stamp says.

    Idempotent: asking twice stamps nothing new and ends nothing twice.

    **2026-09-07: somebody else's job is `404` here, where it was `403`.** The route
    landed one day earlier with the more descriptive code, and read-scoping makes it
    the wrong one: a `403` on a job that exists beside a `404` on one that does not is
    a way of discovering other people's job identifiers, which is the exact leak the
    read routes close. `403` is still right where the caller already knows the thing
    exists — an admin route refusing a non-admin — and wrong where the existence is
    the secret."""
    await job_for_user(session, job_id, user)
    runs = (
        await session.execute(
            select(Run).where(Run.job_id == job_id).order_by(Run.id).with_for_update()
        )
    ).scalars().all()
    now = datetime.now(timezone.utc)
    who = getattr(user, "username", None) or "the user"
    ended = requested = finished = 0
    stale_checkpoints = []
    for run in runs:
        if run.status in _TERMINAL_RUN:
            finished += 1
        elif run.status is RunStatus.PENDING:
            run.status = RunStatus.FAILED
            run.finished_at = now
            run.node_id = None
            run.lease_expires_at = None
            run.failure_reason = CANCELLED
            run.failure_detail = f"Cancelled by {who} before it started."
            run.cancel_requested_at = now
            stale_checkpoints.extend(await collect_checkpoints(session, run.id))
            ended += 1
        else:  # ASSIGNED | RUNNING — a worker holds it
            if run.cancel_requested_at is None:
                run.cancel_requested_at = now
            requested += 1
    await recompute_job_status(session, job_id)
    await session.commit()
    await drop_objects(storage, stale_checkpoints)
    parts = []
    if ended:
        parts.append(f"{ended} run(s) that had not started were ended")
    if requested:
        parts.append(
            f"{requested} running run(s) will be stopped by their worker at its next "
            "check-in (within a few seconds), or ended when their lease expires"
        )
    if finished:
        parts.append(f"{finished} run(s) had already finished and keep their result")
    return CancelResponse(
        job_id=job_id,
        cancelled_now=ended,
        cancel_requested=requested,
        already_finished=finished,
        detail="; ".join(parts) if parts else "this job has no runs",
    )


@router.delete("/jobs/{job_id}/storage")
async def release_job_storage(
    job_id: str,
    user: User = Depends(require_user),
    session: AsyncSession = Depends(get_session),
    storage: ObjectStore = Depends(get_storage),
) -> dict:
    """Free everything this job is holding in object storage (R6, 2026-09-04).

    **A quota without a release valve is a trap.** A cap that can only ever be
    reached, never stepped back from, stops being a policy and becomes a wall: the
    user's only remedy would be to ask an administrator for a bigger tier. So one
    route removes a finished job's bytes — its results, its checkpoints, its archived
    logs and its sealed input, including the leftovers of attempts that were fenced
    out and can no longer be read but are still occupying space.

    **Only when every run of the job is terminal.** Deleting the results of a job
    that is still executing would race the very upload that is producing them, and
    the run would then finish having stored something this call had already decided
    was gone. `409` while anything is still in flight.

    **This is NOT crypto-shred, and the two are deliberately separate.**
    `DELETE /jobs/{id}/key` removes READABILITY and leaves the sealed object exactly
    where it is, which is the point of it: it demonstrates that the guarantee does not
    depend on reaching the copies. This removes BYTES. Either may follow the other,
    and doing both leaves neither the key nor the ciphertext.

    **Objects first, rows second** — the opposite order from checkpoint cleanup
    (`artifacts.drop_objects`), and for a stated reason rather than by accident. There
    the risk being avoided was a row promising bytes that were gone, so the rows went
    first and a failure left a harmless orphaned object. Here the number that matters
    is the QUOTA, and the two half-failures are not equal: a row left behind after its
    object is deleted over-counts the user, which is visible, correctable by calling
    this again, and safe. A row deleted while its object survives under-counts them
    for ever — space that is occupied but chargeable to nobody, which is exactly the
    hole a cap exists to close. So the deletion that could leave storage untracked is
    the one that goes second. `scripts/quota_audit.py` reports either drift.

    **The caller's own job only (2026-09-07).** It deletes bytes, and the bytes are
    charged against their owner's cap — releasing somebody else's storage would be
    destroying their results and editing their quota in one call."""
    job = await job_for_user(session, job_id, user)
    runs = (
        await session.execute(select(Run).where(Run.job_id == job_id))
    ).scalars().all()
    unfinished = [r.id for r in runs if r.status not in _TERMINAL_RUN]
    if unfinished:
        raise HTTPException(
            status_code=409,
            detail=(
                f"{len(unfinished)} run(s) of this job are still in flight; "
                "storage can only be released once they have all finished"
            ),
        )

    run_ids = [r.id for r in runs]
    artefacts = (
        (await session.execute(select(Artifact).where(Artifact.run_id.in_(run_ids))))
        .scalars()
        .all()
        if run_ids
        else []
    )
    archives = (
        (
            await session.execute(
                select(RunLogArchive).where(RunLogArchive.run_id.in_(run_ids))
            )
        )
        .scalars()
        .all()
        if run_ids
        else []
    )

    keys = [a.object_key for a in artefacts] + [a.object_key for a in archives]
    if job.input_object_key:
        keys.append(job.input_object_key)

    removed = set()
    for key in keys:
        try:
            await asyncio.to_thread(storage.delete_object, key)
            removed.add(key)
        except Exception:  # noqa: BLE001 - retain the reference and charge for retry
            log.warning("could not delete object %s while releasing job %s", key, job_id)

    freed = 0
    for row in artefacts:
        if row.object_key in removed:
            freed += row.size or 0
            await session.delete(row)
    for row in archives:
        if row.object_key in removed:
            freed += row.size_bytes or 0
            await session.delete(row)
    # The sealed input's "row" is the pair of columns on the job that point at it.
    # The `job_keys` row is left alone: that is crypto-shred's to remove, and a key
    # with nothing left to open is harmless where a shredded key is a guarantee.
    if job.input_object_key in removed:
        freed += job.input_size_bytes or 0
        job.input_object_key = None
        job.input_size_bytes = None
    await session.commit()

    failed = len(set(keys) - removed)
    if failed:
        # Successful deletions are committed above. Failed ones remain discoverable
        # and charged, so retrying this endpoint attempts precisely what is left.
        raise HTTPException(
            status_code=503,
            detail=(
                f"{len(removed)} object(s) removed; {failed} could not be deleted. "
                "Their storage is still counted. Please retry releasing storage."
            ),
        )

    return {
        "job_id": job_id,
        "objects_deleted": len(removed),
        "objects_listed": len(keys),
        "freed_mb": mb(freed),
        "detail": (
            "results, checkpoints, archived logs and any sealed input for this job "
            "have been removed from storage"
        ),
    }


@router.get("/jobs", response_model=list[JobOut])
async def list_jobs(
    user: User = Depends(require_user),
    session: AsyncSession = Depends(get_session),
) -> list[JobOut]:
    """The caller's own jobs, newest first — and every job if the caller is an
    administrator (2026-09-07). The filter is in the query, not in a loop over the
    answer: see `ownership.visible_jobs`."""
    rows = (
        await session.execute(visible_jobs(user).order_by(Job.created_at.desc()))
    ).scalars().all()
    return [_job_out(j) for j in rows]


@router.get("/jobs/{job_id}", response_model=JobOut)
async def get_job(
    job_id: str,
    user: User = Depends(require_user),
    session: AsyncSession = Depends(get_session),
) -> JobOut:
    """One job, if it is the caller's (2026-09-07). Somebody else's job answers the
    same `404` as a job that was never created — telling the two apart would turn
    this route into a way of discovering which identifiers exist."""
    job = await job_for_user(session, job_id, user)
    return _job_out(job)


@router.get("/jobs/{job_id}/runs", response_model=list[RunOut])
async def get_job_runs(
    job_id: str,
    user: User = Depends(require_user),
    session: AsyncSession = Depends(get_session),
) -> list[RunOut]:
    """This job's runs, if the job is the caller's (2026-09-07)."""
    job = await job_for_user(session, job_id, user)
    rows = (
        await session.execute(
            select(Run).where(Run.job_id == job_id).order_by(Run.created_at)
        )
    ).scalars().all()
    return [_run_out(r, await waiting_for(session, job, r)) for r in rows]
