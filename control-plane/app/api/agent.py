"""Agent-facing endpoints (protocol.md §9).

Pull model (§5): the agent always initiates. The control plane never
connects out to a worker. `register` is open (it issues the token); everything
else requires the node token.

W1: register + heartbeat (empty work). W2 adds the two things that make a run
actually execute: the heartbeat now HANDS OUT work (pull-time assignment), and a
new endpoint accepts the agent's run-status reports, fenced by `attempt`.
"""

import logging
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from ..auth import hash_token, new_token, require_node
from ..config import get_settings
from ..db import get_session
from ..diagnostics import (
    classify_comeback,
    decide_private_input_outcome,
    reason_from_exit_code,
)
from ..models import (
    Job,
    KeyTicket,
    Node,
    NodeEvent,
    NodeStatus,
    Run,
    RunLog,
    RunSample,
    RunStatus,
)
from .artifacts import collect_checkpoints, drop_objects
from ..quota import STORAGE_QUOTA_EXCEEDED
from ..scheduler import (
    OOM_KILLED,
    apply_oom_escalation,
    assign_runs,
    recompute_job_status,
    renew_leases,
)
from ..schemas import (
    Command,
    GoodbyeRequest,
    HeartbeatRequest,
    HeartbeatResponse,
    LogAck,
    LogChunkUpload,
    RegisterRequest,
    RegisterResponse,
    RunStatusResponse,
    RunStatusUpdate,
    SampleAck,
    SampleBatch,
)
from ..storage import ObjectStore, get_storage

router = APIRouter(prefix="/agent", tags=["agent"])

log = logging.getLogger("agent_api")

# Terminal run states cannot be moved away from (a result, once accepted, is
# final). Reporting a terminal state again with the right attempt is idempotent.
_TERMINAL = (RunStatus.SUCCEEDED, RunStatus.FAILED)


@router.post("/register", response_model=RegisterResponse)
async def register(
    req: RegisterRequest, session: AsyncSession = Depends(get_session)
) -> RegisterResponse:
    """Create a node row, issue an opaque token (returned once), store its hash.

    last_heartbeat is set to now so the node reads as `online` immediately
    (satisfies the DoD: register -> node shows online)."""
    token = new_token()
    specs = req.specs
    node = Node(
        name=req.name,
        status=NodeStatus.idle,
        cpu_cores=specs.cpu_cores,
        has_gpu=specs.has_gpu,
        ram_mb=specs.ram_mb,
        capacity=specs.capacity if specs.capacity is not None else specs.cpu_cores,
        agent_version=specs.agent_version,
        token_hash=hash_token(token),
        last_heartbeat=datetime.now(timezone.utc),
        hw_specs=specs.hw_specs,  # rich identity; None from older agents
    )
    session.add(node)
    await session.commit()
    await session.refresh(node)
    return RegisterResponse(node_id=node.id, token=token)


@router.post("/heartbeat", response_model=HeartbeatResponse)
async def heartbeat(
    req: HeartbeatRequest,
    node: Node = Depends(require_node),
    session: AsyncSession = Depends(get_session),
) -> HeartbeatResponse:
    """Record the heartbeat, renew leases for in-flight runs, and HAND OUT work.

    Everything below runs in one transaction (one shared session per request), so
    a node update, its lease renewals, and its new assignments commit atomically —
    a half-applied heartbeat can never leave a run assigned to a node whose
    last_heartbeat wasn't bumped."""
    if req.node_id != node.id:
        # Token resolved a different node than the body claims — reject.
        raise HTTPException(status_code=404, detail="unknown node_id")

    now = datetime.now(timezone.utc)
    # W5b: read the LAST black-box battery BEFORE we overwrite it — the comeback
    # interview refines a "likely" cause from the picture taken before the outage.
    last_battery_pct, last_battery_charging = node.battery_pct, node.battery_charging

    node.status = req.status
    node.last_heartbeat = now
    if req.usage is not None:
        # Latest sample only — the dashboard shows "now", history is stretch.
        node.usage = req.usage
        # 2026-09-04: lift free disk out of the free-form usage sample into a real
        # column, because this one field is READ BY THE SCHEDULER and a JSON blob is
        # the wrong place to match on. Only if it arrives and is a number: an agent
        # that predates the field leaves the last known value alone rather than
        # wiping it, which is the same rule the whole `usage` block already follows.
        free = req.usage.get("disk_free_mb")
        if isinstance(free, (int, float)) and free >= 0:
            node.disk_free_mb = int(free)
    if req.battery_pct is not None:
        # Fresh black-box picture (desktops/old agents omit it -> keep last).
        node.battery_pct = req.battery_pct
        node.battery_charging = req.battery_charging

    # W5b comeback interview: a returning agent hands over hard facts about the gap
    # it just came back from. Classify to one honest cause and record a node_event.
    # Nodes are still never reaped — this is history, not a liveness write.
    if req.interview:
        cause, detail = classify_comeback(
            req.interview,
            last_battery_pct=last_battery_pct,
            last_battery_charging=last_battery_charging,
        )
        if cause is not None:
            session.add(
                NodeEvent(
                    node_id=node.id,
                    event="COMEBACK",
                    cause=cause,
                    evidence={**req.interview, "detail": detail},
                )
            )

    await renew_leases(session, node, req.running, now)      # keep my runs alive + progress
    assignments = await assign_runs(session, node, now)      # pull-time dispatch

    # 2026-09-07 (walk 1, row 64): the runs this node holds that a user asked to stop.
    # The `commands` array has been in the response since W1 and this is the first
    # thing to fill it. An agent that predates it ignores it, and the reaper finishes
    # the cancel when that run's lease expires.
    stamped = (
        await session.execute(
            select(Run.id).where(
                Run.node_id == node.id,
                Run.status.in_((RunStatus.ASSIGNED, RunStatus.RUNNING)),
                Run.cancel_requested_at.is_not(None),
            )
        )
    ).scalars().all()
    commands = [Command(type="cancel", run_id=rid) for rid in stamped]

    await session.commit()
    return HeartbeatResponse(assignments=assignments, commands=commands)


async def _private_input_check(session: AsyncSession, run: Run):
    """Did this run's container actually open its sealed input? (2026-08-15)

    One query, asked only when a run finishes SUCCEEDED, answering both halves at
    once: is the job private, and has any key ticket for *this* attempt been
    redeemed. The agent takes a ticket for every private run before the container
    starts, and only the container can spend it — so an unspent ticket on a finished
    run is evidence the workload never read the file.

    The attempt matters. A ticket is issued per attempt and re-checked at redeem, so
    a redemption by an earlier, fenced execution must not vouch for this one.

    Returns (reason, detail), or (None, None) for every ordinary run.
    """
    opened = (
        select(KeyTicket.ticket)
        .where(
            KeyTicket.run_id == run.id,
            KeyTicket.attempt == run.attempt,
            KeyTicket.redeemed_at.is_not(None),
        )
        .exists()
    )
    # 2026-09-06: the same question, asked of the population that now carries sealed
    # data. It used to be "is this job private", which named the old shape. A job
    # built today is sealed and carries its dataset the new way, so the condition is
    # "was there a sealed input to open at all" — and a job with NO input file is
    # excluded, because a workload that was given nothing to read cannot be accused
    # of not reading it.
    row = (
        await session.execute(
            select(
                Job.private,
                Job.sealed,
                Job.input_object_key,
                opened,
            ).where(Job.id == run.job_id)
        )
    ).first()
    if row is None:  # no job row: nothing we can honestly say
        return None, None
    had_sealed_input = bool(row[0]) or (bool(row[1]) and bool(row[2]))
    # 2026-09-07: a job of the NEW shape gets the message that names the reader it
    # should have used; the old private shape keeps the message that names its
    # opener wrapper, because that is still what those rows need.
    return decide_private_input_outcome(
        is_private=had_sealed_input,
        input_opened=bool(row[3]),
        sealed=bool(row[1]) and not bool(row[0]),
    )


@router.post("/runs/{run_id}/status", response_model=RunStatusResponse)
async def run_status(
    run_id: str,
    req: RunStatusUpdate,
    node: Node = Depends(require_node),
    session: AsyncSession = Depends(get_session),
    storage: ObjectStore = Depends(get_storage),
) -> RunStatusResponse:
    """Accept an agent's report that a run started / succeeded / failed.

    This is where the no-duplicate-accepted-result guarantee is enforced
    (protocol.md §6, §7). The run is locked for the check-and-update so two
    concurrent reports serialise, then:

      * unknown run            -> 404
      * not this node's run    -> 409 (someone else's lease; abort)
      * stale attempt          -> 409 (presumed-dead node's late result; abort)
      * already terminal        -> idempotent 200, result NOT overwritten

    A 409 tells the agent to stop working on the run immediately.

    W7a: the two 409s are the *fencing-class* rejection (the design widened
    that class to "stale attempt OR wrong node"), and they are exactly what the
    `guarantee` experiment switch turns off, so the cost of not having them can be
    measured rather than asserted. The switch arms them independently:

        full        both halves          (today's system)
        owner_only  wrong-node only      (ownership, but no per-attempt identity)
        lease_only  neither
        none        neither, and the reaper does not sweep either

    `owner_only` exists because without it the measurement proves too little. If
    every zombie in the experiment comes from a node that no longer owns the run,
    the ownership check alone catches it, and the per-attempt token looks like
    unearned complexity. `owner_only` is the arm where the OWNING node reports an
    old execution — and only the token can tell that apart.

    The terminal-state guard below is NOT part of the fencing class — "a result,
    once accepted, is final" is ordinary state-machine hygiene that any competent
    alternative also has, so it stays on in every arm. Keeping it on is what makes
    the weaker arms fair ones."""
    run = (
        await session.execute(
            select(Run).where(Run.id == run_id).with_for_update()
        )
    ).scalar_one_or_none()
    if run is None:
        raise HTTPException(status_code=404, detail="unknown run_id")

    wrong_node = run.node_id != node.id
    stale_attempt = req.attempt != run.attempt
    guarantee = get_settings().experiment_guarantee_mode
    # Which half of the fencing-class rejection is armed in this mode. Splitting it
    # is what lets E1 ask the sharper question: not "is SOME check needed?" (nobody
    # doubts that) but "does the per-attempt TOKEN earn its place on top of an
    # ownership check?" Only `owner_only` can answer that, because it is the one
    # arm where a report from the RIGHT node carrying the WRONG attempt gets in.
    check_owner = guarantee in ("full", "owner_only")
    check_attempt = guarantee == "full"
    if check_owner and wrong_node:
        raise HTTPException(status_code=409, detail="run not owned by this node")
    if check_attempt and stale_attempt:
        # The fencing rejection. Current attempt has moved on (the run was
        # re-dispatched), so this report is from a stale execution — drop it.
        raise HTTPException(status_code=409, detail="stale attempt — abort run")
    if wrong_node or stale_attempt:
        # EXPERIMENT ARM ONLY. We are deliberately accepting a report from an
        # execution the platform had already given up on. Leave an audit line so
        # the harm is traceable in the server's own log, not only in the driver's
        # notes — this line IS the E1 evidence that a stale result got through.
        log.warning(
            "EXPERIMENT accepted-unfenced run=%s posted_attempt=%s current_attempt=%s "
            "posted_node=%s owner_node=%s state=%s",
            run_id, req.attempt, run.attempt, node.id, run.node_id, req.state,
        )

    if run.status in _TERMINAL:
        # Already accepted a terminal result for this attempt — idempotent.
        return RunStatusResponse(accepted=True, run_status=run.status.value)

    now = datetime.now(timezone.utc)
    if req.state == "RUNNING":
        run.status = RunStatus.RUNNING
        if run.started_at is None:
            run.started_at = now
        run.lease_expires_at = now + timedelta(seconds=get_settings().lease_ttl_s)
        # W5b: a fresh attempt is running now — clear any stale loss reason from a
        # previous attempt (the run is no longer lost; it is executing).
        run.failure_reason = None
        run.failure_detail = None
        # 2026-09-04, and for exactly the same reason one line up: a storage refusal
        # belongs to the ATTEMPT it happened on, not to the run for ever.
        #
        # The hole this closes: `quota_refused_at` is a column on `runs`, so without
        # this a run whose upload was refused and whose lease then expired before it
        # could report — reaped LOST, requeued, claimed by somebody else — would carry
        # the old attempt's refusal into the new one and be failed for it. The new
        # attempt might be writing a smaller file, or its owner might have freed space
        # with `DELETE /jobs/{id}/storage` in between, and it would be refused for a
        # decision made about work that no longer exists.
        #
        # Cleared HERE rather than at the two requeue sites (the reaper's, and W5c's
        # escalation) because a RUNNING post is the one universal signal that a fresh
        # attempt has begun, whatever path it arrived by — the same argument that put
        # the loss reason on this line. And it cannot erase the CURRENT attempt's
        # stamp, because within one attempt RUNNING is always posted before the
        # container has produced anything to upload.
        run.quota_refused_at = None
        run.quota_refused_detail = None
    elif run.quota_refused_at is not None:
        # R4 (2026-09-04). An artefact upload for THIS attempt was refused because
        # the owner's retained-storage cap was reached, so the run did not produce a
        # stored result — whatever word the agent posts about it.
        #
        # THE CONTROL PLANE GUARANTEES THE OUTCOME, the agent does not report it. The
        # agent also stops the container the moment it sees the refusal, to stop
        # burning a machine on work that cannot be kept, but that is a courtesy to the
        # worker and not the mechanism: an agent that ignored the 413 entirely, or
        # crashed before acting on it, would still not be able to post SUCCEEDED for
        # this attempt. The same shape as W5c, where the server decides an escalation
        # from its own facts rather than from the agent's opinion.
        #
        # **No re-dispatch.** This is the USER'S cap, not the machine's: another
        # machine would fill the same quota with the same bytes, so retrying could
        # only waste a second worker's time. That is exactly the rule W5c already
        # applies to a kill at the user's own `mem_limit_mb` — a limit the user chose
        # is not evidence that the machine was wrong.
        run.status = RunStatus.FAILED
        run.exit_code = req.exit_code
        run.finished_at = now
        run.failure_reason = STORAGE_QUOTA_EXCEEDED
        run.failure_detail = run.quota_refused_detail or (
            "An output file could not be stored because your retained-storage cap "
            "was reached. Free space and run it again."
        )
    else:  # SUCCEEDED | FAILED — terminal
        run.status = RunStatus[req.state]
        run.exit_code = req.exit_code
        run.finished_at = now
        # 2026-09-07 (walk 1, row 25): the finish line's own progress, when the agent
        # sends it. Fenced by everything above: a stale attempt never got here.
        if req.progress is not None:
            run.progress = req.progress
        if req.metrics is not None:
            run.metrics_last = req.metrics
        if run.status is RunStatus.SUCCEEDED:
            # A SUCCEEDED run normally gets NO reason — silence is only allowed when
            # nothing died. The one exception is a PRIVATE run whose container never
            # opened its sealed input: the program exited zero, so the result stands
            # and we do not rewrite the status, but reporting it silently would tell
            # the user their private file was used when it never reached the
            # workload. Flagged, not failed.
            run.failure_reason, run.failure_detail = await _private_input_check(
                session, run
            )
        else:  # FAILED — record why (agent's hard-fact classification, or a fallback)
            reason, detail = req.failure_reason, req.failure_detail
            if reason is None:
                # Older agent didn't classify — derive the minimal reason the exit
                # code alone proves (a hard fact the server has too).
                reason, detail = reason_from_exit_code(req.exit_code)
            if run.cancel_requested_at is not None:
                # 2026-09-07: a user asked for this. Whatever the container's exit
                # looked like from the worker's side (a SIGKILL, a SIGTERM), the reason
                # the run ended is the request — say so, where every other ending is
                # said. A SUCCEEDED post is left alone above: the work finished before
                # the stop landed, and a result, once accepted, is final.
                reason = "CANCELLED"
                detail = detail if req.failure_reason == "CANCELLED" else (
                    "Cancelled by the user; the worker stopped the container."
                )
            run.failure_reason = reason
            run.failure_detail = detail
            # W5c: a PROVEN RAM-shortage kill (the only signal we act on) may
            # requeue the run to a strictly-stronger node — or, if the node itself
            # ran out and no bigger machine exists, fail with an explicit reason. A
            # requeue flips the run back to PENDING here, and recompute below rolls
            # the job with it. (node == the run's owner; verified above.)
            if reason == OOM_KILLED:
                await apply_oom_escalation(session, run, node)

    # 2026-08-29: the run has ended, so its checkpoint has nothing left to do. A
    # checkpoint is only ever a way of GETTING to a result; once the result is
    # stored, keeping the working state costs storage for ever and buys nothing.
    #
    # Read the run's state HERE, after the escalation above, and never the word the
    # agent posted. The two are not the same thing: a proven RAM kill on a machine
    # too small posts `FAILED` and comes out of `apply_oom_escalation` as PENDING,
    # because it is being retried rather than finished — and its checkpoint is
    # precisely what that retry should resume from. Keying on the posted word would
    # throw away the work the retry exists to save.
    #
    # The rows go inside this transaction, so cleanup and the accepted result stand
    # or fall together. The bytes go after the commit, where a failure is a leak and
    # never a lost result (see `drop_objects`).
    stale_checkpoints: list[str] = []
    if run.status in _TERMINAL:
        stale_checkpoints = await collect_checkpoints(session, run.id)

    # Roll the job's status up from its runs after every change (a run starting
    # makes the job RUNNING; the last run finishing makes it SUCCEEDED/FAILED).
    await recompute_job_status(session, run.job_id)
    await session.commit()
    await drop_objects(storage, stale_checkpoints)
    return RunStatusResponse(accepted=True, run_status=run.status.value)


@router.post("/runs/{run_id}/logs", response_model=LogAck)
async def post_logs(
    run_id: str,
    req: LogChunkUpload,
    node: Node = Depends(require_node),
    session: AsyncSession = Depends(get_session),
) -> LogAck:
    """Store one batch of container output for a run (protocol.md §9).

    Fenced exactly like a status post — a stale `attempt` or a run this node does
    not own is `409` (abort). The no-duplicate guarantee for logs is enforced by
    the database, not app code: `UNIQUE(run_id, attempt, seq)` means a chunk the
    agent re-sends (after a network hiccup) cannot create a second row — we catch
    that and return an idempotent `200`, so the agent's retry is safe.

    We deliberately do NOT gate on run status: the agent flushes its last output
    just before posting the terminal status, so a log can legitimately arrive for a
    run that is about to finish. `attempt` is the only correctness gate.
    """
    run = (
        await session.execute(select(Run).where(Run.id == run_id))
    ).scalar_one_or_none()
    if run is None:
        raise HTTPException(status_code=404, detail="unknown run_id")
    if run.node_id != node.id:
        raise HTTPException(status_code=409, detail="run not owned by this node")
    if req.attempt != run.attempt:
        raise HTTPException(status_code=409, detail="stale attempt — abort run")

    session.add(
        RunLog(run_id=run_id, attempt=req.attempt, seq=req.seq, chunk=req.chunk)
    )
    try:
        await session.commit()
    except IntegrityError:
        # This exact (run_id, attempt, seq) is already stored — a resent chunk.
        # The DB constraint rejected the duplicate; that is the guarantee working.
        await session.rollback()
        return LogAck(accepted=True, deduped=True)
    return LogAck(accepted=True)


@router.post("/runs/{run_id}/samples", response_model=SampleAck)
async def post_samples(
    run_id: str,
    req: SampleBatch,
    node: Node = Depends(require_node),
    session: AsyncSession = Depends(get_session),
) -> SampleAck:
    """Store a batch of the container's own resource samples (W5b). Fenced exactly
    like logs/status — a stale `attempt` or a run this node does not own is `409`.

    Idempotent like logs: `UNIQUE(run_id, attempt, ts)` means a re-sent batch (after
    a lost 200) stores nothing new. We select the ts already stored for this batch
    and insert only the missing ones, so a retry is a harmless no-op."""
    run = (
        await session.execute(select(Run).where(Run.id == run_id))
    ).scalar_one_or_none()
    if run is None:
        raise HTTPException(status_code=404, detail="unknown run_id")
    if run.node_id != node.id:
        raise HTTPException(status_code=409, detail="run not owned by this node")
    if req.attempt != run.attempt:
        raise HTTPException(status_code=409, detail="stale attempt — abort run")

    if not req.samples:
        return SampleAck(accepted=True, stored=0)

    incoming = {s.ts: s for s in req.samples}
    existing = set(
        (
            await session.execute(
                select(RunSample.ts).where(
                    RunSample.run_id == run_id,
                    RunSample.attempt == req.attempt,
                    RunSample.ts.in_(list(incoming.keys())),
                )
            )
        ).scalars().all()
    )
    stored = 0
    for ts, s in incoming.items():
        if ts in existing:
            continue
        session.add(
            RunSample(
                run_id=run_id,
                attempt=req.attempt,
                ts=ts,
                cpu_pct=s.cpu_pct,
                mem_used_mb=s.mem_used_mb,
                mem_limit_mb=s.mem_limit_mb,
                # 2026-09-04: MB on the run's temporary disk at sample time. NULL from
                # an agent that does not report it — "did not say" and "wrote nothing"
                # are different facts and the column keeps them apart.
                scratch_used_mb=s.scratch_used_mb,
            )
        )
        stored += 1
    try:
        await session.commit()
    except IntegrityError:
        # A concurrent identical batch beat us to some rows — the data is there.
        await session.rollback()
        return SampleAck(accepted=True, stored=0)
    return SampleAck(accepted=True, stored=stored)


@router.post("/goodbye")
async def goodbye(
    req: GoodbyeRequest,
    node: Node = Depends(require_node),
    session: AsyncSession = Depends(get_session),
) -> dict:
    """The agent's best-effort "going offline" on a clean stop (W5b). When it lands,
    the outage cause is EXACT — recorded as a CLEAN_SHUTDOWN node_event, so even a
    node that never comes back has explained itself."""
    session.add(
        NodeEvent(
            node_id=node.id,
            event="GOODBYE",
            cause="CLEAN_SHUTDOWN",
            evidence={"reason": req.reason},
        )
    )
    await session.commit()
    return {"accepted": True}
