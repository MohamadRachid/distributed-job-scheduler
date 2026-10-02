"""User-facing run log reading (protocol.md §10). JWT-gated from W6, owner-scoped
from 2026-09-07.

A run belongs to its job and a job belongs to the user who submitted it, so every
read here resolves the run through `ownership.run_for_user`: the caller's own runs,
plus everything if the caller is an administrator, and the same `404` for a run
that is somebody else's as for a run that does not exist. The socket asks the same
question in its own form, because a valid login belonging to somebody else is
exactly the case this closes and a token check alone cannot see it.

The two GET reads take `require_user` like every other user endpoint. The
WebSocket is the one exception to header auth: a browser cannot set an
Authorization header on a socket, so `WS /runs/{id}/logs` takes the JWT as a
`?token=` query param, validated before the socket is accepted. (Honesty note:
query strings can appear in server logs — acceptable at demo scale, and we say so.)


Two transports over ONE data model (`run_logs`):

  * ``GET /runs/{id}/logs?since_seq=N`` — return the stored chunks with ``seq > N``,
    ordered. This is the catch-up read AND the pre-approved fallback if the socket
    is fiddly (risk register §21): the browser can just poll it with a rising
    cursor and get the same data.
  * ``WS /runs/{id}/logs`` — replay what is already stored, then push new chunks as
    they land, and close once the run is terminal. This is the live upgrade sitting
    on the exact same rows.

Both order strictly by ``(attempt, seq)``, so every chunk shows **once, in order**,
no matter what order the agent's posts arrived in — that ordering + the DB's
``UNIQUE(run_id, attempt, seq)`` dedup is the W3 reliability point (brief §4).

The WebSocket polls the database (~0.6s) rather than holding an in-process pub/sub.
At demo scale that is simpler, needs no shared broadcaster state, and reuses the
exact query the GET endpoint uses. Each poll opens a short-lived session so it sees
newly-committed rows (a long-held transaction would keep a stale snapshot).
"""

import asyncio

from fastapi import APIRouter, Depends, WebSocket, WebSocketDisconnect
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..db import SessionLocal, get_session
from ..logarchive import fetch_logs_since
from ..models import Run, RunSample, RunStatus, User
from ..ownership import may_read_run, run_for_user
from ..schemas import LogChunkOut, RunSampleOut
from ..storage import ObjectStore, get_storage
from ..userauth import require_user, user_from_ws_token

router = APIRouter(tags=["runs"])

_TERMINAL = (RunStatus.SUCCEEDED, RunStatus.FAILED, RunStatus.LOST)
_POLL_SECONDS = 0.6


async def _fetch_since(
    session: AsyncSession,
    store: ObjectStore,
    run_id: str,
    since_seq: int,
    cache: dict | None = None,
) -> list[dict]:
    """Chunks with seq > since_seq, ordered by (attempt, seq).

    W3 runs have a single attempt, so ordering by seq alone would do; ordering by
    (attempt, seq) is already correct for the W5 re-dispatch case (a second attempt
    restarts seq at 0) without needing a change then.

    2026-08-22: a run whose logs have been archived out of the database keeps
    answering both reads exactly as it did, because the merge happens HERE rather
    than at each caller. `logarchive.fetch_logs_since` applies the same filter and
    the same ordering to the rows and to the archive, and renders both through the
    same one function, so a caller cannot tell the two apart and neither can the
    response. A run that has never been archived — every run in a demonstration,
    and every run in every test that predates this — takes a fast path that never
    touches the object store at all."""
    return await fetch_logs_since(session, store, run_id, since_seq, cache)


@router.get("/runs/{run_id}/logs", response_model=list[LogChunkOut])
async def get_run_logs(
    run_id: str,
    since_seq: int = -1,
    user: User = Depends(require_user),
    session: AsyncSession = Depends(get_session),
    store: ObjectStore = Depends(get_storage),
) -> list[LogChunkOut]:
    """Catch-up read + WebSocket fallback. `since_seq=-1` (default) returns all
    chunks; a poller passes the highest seq it has already seen.

    A run whose job is not the caller's answers `404` (2026-09-07). Log lines are the
    most revealing thing a run produces — they are the training script's own output —
    so this is the route the gap mattered most on."""
    await run_for_user(session, run_id, user)
    rows = await _fetch_since(session, store, run_id, since_seq)
    return [LogChunkOut(**r) for r in rows]


@router.get("/runs/{run_id}/samples", response_model=list[RunSampleOut])
async def get_run_samples(
    run_id: str,
    limit: int = 60,
    user: User = Depends(require_user),
    session: AsyncSession = Depends(get_session),
) -> list[RunSampleOut]:
    """The container's own resource history (W5b) — cpu%/mem-used/mem-limit over
    time, oldest first. The UI shows the last few as "the last resource picture"
    on a failed run (e.g. RAM climbing to the OOM limit).

    Owner-scoped from 2026-09-07 like every other run read. A resource history is
    less obviously private than a log line and is not less private: it says when
    somebody's work ran, for how long, and how big it was."""
    await run_for_user(session, run_id, user)
    rows = (
        await session.execute(
            select(RunSample)
            .where(RunSample.run_id == run_id)
            .order_by(RunSample.attempt.desc(), RunSample.ts.desc())
            .limit(max(1, min(limit, 500)))
        )
    ).scalars().all()
    return [
        RunSampleOut(
            ts=s.ts,
            cpu_pct=s.cpu_pct,
            mem_used_mb=s.mem_used_mb,
            mem_limit_mb=s.mem_limit_mb,
            # 2026-09-04: temporary disk beside memory, so the "last picture before
            # the failure" shows a run filling its scratch the same way it already
            # shows one climbing to a memory limit.
            scratch_used_mb=s.scratch_used_mb,
        )
        for s in reversed(rows)
    ]


@router.websocket("/runs/{run_id}/logs")
async def ws_run_logs(
    websocket: WebSocket,
    run_id: str,
    store: ObjectStore = Depends(get_storage),
) -> None:
    """Live log stream: replay stored chunks, then push new ones until the run is
    terminal. Message shape matches the GET rows: `{run_id, attempt, seq, chunk,
    ts}`; a final `{"end": true, "run_status": "..."}` marks the run finished.

    JWT-gated (W6) via `?token=<jwt>` — validated BEFORE accept, so an unauthorized
    socket is refused with an HTTP 403 handshake, never upgraded.

    **Owner-scoped from 2026-09-07, and the check moved before accept with it.** The
    token check alone answered "is this a valid login", which was the whole question
    while one account existed; it could not see that the login belonged to somebody
    else. The socket now resolves the user from the token and asks whether the run's
    job is theirs, and refuses in the handshake either way — a run that is not yours
    and a run that does not exist are the same closed socket, where the second used
    to be an accepted connection carrying an error message. Refusing before accept is
    also what stops a stranger's socket ever being upgraded at all."""
    async with SessionLocal() as session:
        user = await user_from_ws_token(session, websocket.query_params.get("token"))
        allowed = user is not None and await may_read_run(session, run_id, user)
    if not allowed:
        await websocket.close(code=1008)  # policy violation (before accept -> HTTP 403)
        return
    await websocket.accept()

    last_seq = -1
    # One archive cache for the life of this connection. The loop re-reads every
    # ~0.6s; without this, a run whose logs live in the object store would be
    # fetched and decompressed on every tick. Bounded by construction: at most one
    # entry per attempt of one run, and it dies with the socket.
    cache: dict = {}
    try:
        while True:
            async with SessionLocal() as session:
                rows = await _fetch_since(session, store, run_id, last_seq, cache)
                run = await session.get(Run, run_id)

            for r in rows:
                await websocket.send_json(r)
                last_seq = r["seq"]

            if run is not None and run.status in _TERMINAL:
                # One more read closes the tiny gap where a final chunk committed
                # between the log fetch and the run fetch above.
                async with SessionLocal() as session:
                    tail = await _fetch_since(session, store, run_id, last_seq, cache)
                for r in tail:
                    await websocket.send_json(r)
                    last_seq = r["seq"]
                await websocket.send_json({"end": True, "run_status": run.status.value})
                await websocket.close()
                return

            await asyncio.sleep(_POLL_SECONDS)
    except WebSocketDisconnect:
        return
