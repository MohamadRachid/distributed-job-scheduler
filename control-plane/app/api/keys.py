"""Sealed delivery + fenced key release (W6b) — three endpoints, one idea.

The idea: **the sealed data and the key that opens it travel by different roads,
to different holders, at different times.**

  * ``GET  /agent/jobs/{job_id}/input``      (node token) — the AGENT gets the
    sealed blob. Ciphertext only. The agent can carry it, store it, and mount it,
    and still cannot read one byte of it.
  * ``POST /agent/runs/{run_id}/key-ticket`` (node token, FENCED) — the AGENT gets
    a single-use ticket, not a key.
  * ``POST /container/key``                  (the ticket IS the credential) — the
    CONTAINER redeems the ticket, once, for the key. Nothing else in the system
    ever holds it.

**Why the fence guards the key too.** Since W2, the `attempt` number has decided
whose *result* we accept. W6b points the same check at the *input*: a ticket is
issued only to the node holding the run's current attempt, and the redeem checks
the attempt again. So a node the control plane has already given up on — one whose
run was re-dispatched — cannot obtain the key for a job it no longer runs. The
mechanism that stops a zombie from writing a stale result now also stops it from
reading fresh data. One fence, two directions; no second mechanism was invented.

**Why a ticket and not the key.** If the agent held the key, it would have to hand
it to the container somehow, and every route (env var, command line, a file) is
visible in `docker inspect` or on disk for as long as the container exists. The
ticket is deliberately worthless a second after use: one redeem flips it dead, and
it expires in ~2 minutes regardless. `docker inspect` on a live private container
shows a ticket that no longer opens anything.

**Honesty guard (locked):** job data is sealed from submit until it
opens inside the container, never touches the worker's disk in readable form, and
any tampering breaks the seal and fails the run. That protects it from users of the
machine — not from its root administrator, who can read the memory of a running
container (and could, in principle, race the redeem). Root-proof privacy needs TEE
hardware — our named future work. We never claim more than this.
"""

from __future__ import annotations

import asyncio
import secrets
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..auth import require_node
from ..config import get_settings
from ..db import get_session
from ..models import Job, JobKey, KeyTicket, Node, Run, RunStatus
from ..schemas import KeyRedeemRequest, KeyRedeemResponse, KeyTicketResponse
from ..storage import ObjectStore, get_storage

router = APIRouter(tags=["private"])

# A run is "live on this node" while it holds the lease — that is the window in
# which staging its input is legitimate.
_LIVE = (RunStatus.ASSIGNED, RunStatus.RUNNING)


def _as_utc(dt: datetime) -> datetime:
    """Postgres returns aware datetimes, SQLite (tests) naive — assume UTC there."""
    return dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)


async def _fenced_run(session: AsyncSession, run_id: str, node: Node, attempt: int) -> Run:
    """The standard W2 fence, reused verbatim for the input side: unknown run 404;
    a run this node does not own 409; a stale attempt 409 (abort)."""
    run = (
        await session.execute(select(Run).where(Run.id == run_id).with_for_update())
    ).scalar_one_or_none()
    if run is None:
        raise HTTPException(status_code=404, detail="unknown run_id")
    if run.node_id != node.id:
        raise HTTPException(status_code=409, detail="run not owned by this node")
    if attempt != run.attempt:
        raise HTTPException(status_code=409, detail="stale attempt — abort run")
    if run.status not in _LIVE:
        raise HTTPException(status_code=409, detail="run is no longer live — abort run")
    return run


@router.get("/agent/jobs/{job_id}/input")
async def get_sealed_input(
    job_id: str,
    node: Node = Depends(require_node),
    session: AsyncSession = Depends(get_session),
    storage: ObjectStore = Depends(get_storage),
) -> Response:
    """Stream a private job's SEALED input to the agent (node-token auth).

    Brokered through the control plane exactly like an artifact download, so the
    worker still holds no MinIO credentials (ISO 27001 A.8.31 stays literally true —
    the same claim W6 made, unchanged by adding private data to the store).

    Access rule: this node must currently hold a live run of this job. That is the
    input-side twin of the run fence — a machine with no live claim on the job has
    no business fetching even its ciphertext. And what it fetches is ciphertext:
    handing this to the wrong party costs nothing, which is exactly the property
    sealing was chosen for."""
    job = await session.get(Job, job_id)
    # 2026-09-05: opened to an ORDINARY job that carries a dataset file. The access
    # rule below is unchanged and is what matters — a node with no live claim on this
    # job gets nothing either way. What differs is only what the bytes ARE: sealed for
    # a private job, as-uploaded for an ordinary one. Nothing here has to know which,
    # because this route only ever moves bytes it cannot read.
    if job is None or not job.input_object_key:
        raise HTTPException(status_code=404, detail="no input file for this job")
    holds_live_run = (
        await session.execute(
            select(Run.id).where(
                Run.job_id == job_id,
                Run.node_id == node.id,
                Run.status.in_(_LIVE),
            ).limit(1)
        )
    ).first()
    if holds_live_run is None:
        raise HTTPException(status_code=409, detail="node holds no live run of this job")

    try:
        blob = await asyncio.to_thread(storage.get_object, job.input_object_key)
    except Exception:  # noqa: BLE001 - object missing/unreachable -> a clean 404
        raise HTTPException(status_code=404, detail="sealed input bytes not found")
    return Response(content=blob, media_type="application/octet-stream")


def _key_url(request: Request) -> str:
    """Where the CONTAINER should redeem its ticket.

    Default: the address the agent used to reach us, taken from this very request —
    so it is right by construction on a LAN. But a container cannot reach the host's
    `localhost`, so when the agent called us on a loopback address (the single-machine
    demo) we hand back `host.docker.internal`, which Docker resolves to the host from
    inside a container. `CONTAINER_KEY_URL` overrides both if a deployment needs it."""
    override = get_settings().container_key_url
    if override:
        return override.rstrip("/") + "/container/key"
    base = str(request.base_url).rstrip("/")
    for loopback in ("//localhost", "//127.0.0.1"):
        if loopback in base:
            base = base.replace(loopback, "//host.docker.internal")
            break
    return base + "/container/key"


@router.post("/agent/runs/{run_id}/key-ticket", response_model=KeyTicketResponse)
async def issue_key_ticket(
    run_id: str,
    request: Request,
    attempt: int,
    node: Node = Depends(require_node),
    session: AsyncSession = Depends(get_session),
) -> KeyTicketResponse:
    """Issue a single-use ticket for this run's key (node-token auth, FENCED).

    The agent asks for this just before starting the container, and passes the
    TICKET — never a key — into the container's environment. Fencing is the whole
    security story: 404 unknown run, 409 wrong node, 409 stale attempt. A zombie
    node whose run was re-dispatched gets 409 here, so it can never obtain the key
    for work that is no longer its own.

    Old tickets for this run+attempt are dropped as we mint the new one, so a retry
    (or a container restart) never leaves a second live pass lying around."""
    run = await _fenced_run(session, run_id, node, attempt)
    job = await session.get(Job, run.job_id)
    # 2026-09-06: a ticket is issued for any SEALED job, with or without an input
    # file, because the key is no longer only for reading. The container also seals
    # what it writes — its results and its checkpoints — so a job with nothing to
    # open still has something to seal, and refusing it a ticket would leave its
    # outputs the only unsealed bytes in the system.
    if job is None or not (job.sealed or job.private):
        raise HTTPException(status_code=404, detail="run's job has no key")

    now = datetime.now(timezone.utc)
    old = (
        await session.execute(
            select(KeyTicket).where(
                KeyTicket.run_id == run_id, KeyTicket.attempt == attempt
            )
        )
    ).scalars().all()
    for row in old:
        await session.delete(row)

    ticket = KeyTicket(
        ticket=secrets.token_urlsafe(32),
        run_id=run_id,
        attempt=attempt,
        expires_at=now + timedelta(seconds=get_settings().key_ticket_ttl_s),
    )
    session.add(ticket)
    await session.commit()
    return KeyTicketResponse(
        ticket=ticket.ticket, expires_at=ticket.expires_at, key_url=_key_url(request)
    )


@router.post("/container/key", response_model=KeyRedeemResponse)
async def redeem_key(
    req: KeyRedeemRequest,
    session: AsyncSession = Depends(get_session),
) -> KeyRedeemResponse:
    """Exchange a ticket for the job's key — ONCE (W6b).

    Called from inside the running container, by the startup helper, over the
    ordinary network. The ticket is the only credential: the container holds no node
    token and no JWT, because it runs untrusted user code and should be handed the
    smallest possible thing.

    Four checks, and the last two are why this is safe:
      * unknown ticket        -> 404
      * expired               -> 410 (gone)
      * ALREADY REDEEMED      -> 410 (gone) — the one-shot latch
      * attempt has moved on  -> 409 — the fence again, checked at redeem time and
        not only at issue time, so a ticket minted a moment before a re-dispatch is
        already worthless by the time it is used.

    `redeemed_at` is stamped in the SAME transaction that reads the key out, so two
    concurrent redeems cannot both succeed: the row is locked for the check-and-set,
    exactly like the run-status fence."""
    # Lock the run before the ticket, matching issue_key_ticket's lock order.
    # This serializes release with completion/recovery and avoids a deadlock
    # when a concurrent issue deletes the ticket being redeemed.
    snapshot = await session.get(KeyTicket, req.ticket)
    if snapshot is None:
        raise HTTPException(status_code=404, detail="unknown ticket")
    run = (
        await session.execute(
            select(Run).where(Run.id == snapshot.run_id).with_for_update()
        )
    ).scalar_one_or_none()
    row = (
        await session.execute(
            select(KeyTicket).where(KeyTicket.ticket == req.ticket)
            .with_for_update().execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if row is None:
        raise HTTPException(status_code=404, detail="unknown ticket")
    now = datetime.now(timezone.utc)
    if row.redeemed_at is not None:
        raise HTTPException(status_code=410, detail="ticket already used")
    if _as_utc(row.expires_at) < now:
        raise HTTPException(status_code=410, detail="ticket expired")

    if run is None or run.attempt != row.attempt:
        raise HTTPException(status_code=409, detail="stale attempt — abort run")
    if run.status not in _LIVE:
        raise HTTPException(status_code=409, detail="run is no longer live — abort run")

    key_row = await session.get(JobKey, run.job_id)
    if key_row is None:
        # Crypto-shred: the key was deleted, so the sealed input is permanently
        # unreadable — by us as much as by anyone. Say so plainly.
        raise HTTPException(
            status_code=410,
            detail="this job's key was deleted — its input is permanently unreadable",
        )

    row.redeemed_at = now  # the latch: any second redeem now gets 410
    await session.commit()
    return KeyRedeemResponse(key_b64=key_row.key_b64)
