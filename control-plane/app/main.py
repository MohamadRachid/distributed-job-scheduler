"""FastAPI app factory + wiring.

W1 mounted: /health, /agent/register, /agent/heartbeat, /nodes.
W2 adds: pull-time assignment (inside /agent/heartbeat), /agent/runs/{id}/status,
and the /jobs routes (submit + read).
W3 adds: POST /agent/runs/{id}/logs (ingest) and the user-facing log reads —
GET /runs/{id}/logs?since_seq=N + WS /runs/{id}/logs.
W5 adds: the reaper — one background task started/stopped with the app that
sweeps for expired leases and requeues lost runs (see reaper.reaper_loop).
W6 adds: artifacts (upload/list/download via MinIO) + user JWT gating (POST
/auth/login; every user endpoint now requires a login). Startup also creates the
one admin user and ensures the MinIO bucket exists.
W6b adds: private job inputs — POST /jobs/private (seal at submit), the sealed
delivery + fenced key release endpoints (api/keys.py), PATCH /nodes/{id}/trusted,
and DELETE /jobs/{id}/key (crypto-shred).
2026-08-22 adds a SECOND background task beside the reaper: the log archiver, which
moves a terminal run's log chunks out of PostgreSQL into one compressed object per
(run, attempt) once they are older than the retention window (see logarchive.py).
"""

import asyncio
import logging
import re
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .api import agent, artifacts, auth, health, jobs, keys, nodes, runs, users
from .config import (
    EXPERIMENT_DEFAULTS,
    experiment_is_default,
    experiment_mode,
    get_settings,
    shipped_lease_ttl_s,
)
from .db import SessionLocal
from .logarchive import archive_loop
from .reaper import reaper_loop
from .quota import ensure_tiers
from .storage import get_storage
from .userauth import ensure_admin_user

# Make this application's own INFO lines visible. Nothing configured logging
# before 2026-08-30, so every `log.info` here was dropped on the floor -- the
# reaper's "reclaimed N run(s) this pass" included, which the record had been
# citing as an audit trail. uvicorn configures its own loggers and clears root's
# handlers before importing this module, so this call is what puts one back.
#
# THE FORMAT PIN IS THE LOAD-BEARING PART. basicConfig's default format prefixes
# every record with "LEVEL:logger:", which would rewrite the reaper's LOST audit
# line -- the shape published in docs/evidence/live_kill_output.txt and matched by
# scripts/experiments/harness.py and scripts/experiments/e1_fencing.py -- into
# something neither the file nor those scripts describe. "%(message)s" is exactly
# what the bare root logger was already emitting, so every existing warning line
# keeps its shape and only the info lines are new.
logging.basicConfig(level=logging.INFO, format="%(message)s")

log = logging.getLogger("main")


class RedactToken(logging.Filter):
    """Keep a login token out of the server's own log (walk 1, row 11).

    A browser cannot set a header on a WebSocket, so the log socket carries the JWT
    as `?token=` — and uvicorn's access line prints the whole URL, token included, on
    every connection. Anyone reading `docker compose logs` could lift a live login
    from it. The socket keeps its shape (protocol.md §10 says why); what changes is
    only that the printed line masks the value. Applied to uvicorn's two loggers, which
    is where those lines are made; the application's own lines never carry one."""

    _PATTERN = re.compile(r"(token=)[^\s&\"'\]]+")

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            message = record.getMessage()
        except Exception:  # noqa: BLE001 - a record we cannot render is not ours to touch
            return True
        if "token=" in message:
            record.msg = self._PATTERN.sub(r"\1<redacted>", message)
            record.args = ()
        return True


for _name in ("uvicorn.access", "uvicorn.error", "uvicorn"):
    logging.getLogger(_name).addFilter(RedactToken())


def _warn_if_experimental() -> None:
    """W7a: shout if this process is NOT the system we ship.

    The experiment switches let us run a deliberately weaker version of our own
    guarantees so the comparison can be measured. That is only safe if it is
    impossible to do by accident, so a non-default mode is announced in three
    unmissable lines at startup — and also published on GET /health. We never ship
    a silent switch that disables our own guarantee."""
    if experiment_is_default():
        return
    mode = experiment_mode()
    weakened = ", ".join(
        f"{k}={v} (normally {EXPERIMENT_DEFAULTS[k]})"
        for k, v in mode.items()
        if v != EXPERIMENT_DEFAULTS[k]
    )
    log.warning("!!! EXPERIMENT MODE — THIS IS NOT THE SHIPPED SYSTEM !!!")
    log.warning("!!! weakened: %s", weakened)
    log.warning("!!! results from this process are measurements, not the product")


def _log_effective_lease() -> None:
    """Print the lease this process is actually running with, once, at startup.

    The lease decides how long a machine may go silent before its run is given to
    someone else, and it is the one tunable that is deliberately different in the
    demonstration (15s) from what we ship (60s). `scripts/stage_demo.ps1` prints it
    in its READY banner, but that only helps someone who staged through that script.
    A stack brought up any other way — `docker compose up` in a shell that happens
    to export LEASE_TTL_S, a laptop with a stale .env — would otherwise give no sign
    of which value it took. One line here means the answer is always in the log of
    the process that is using it.

    PRINTED, NOT LOGGED, AND THAT IS STILL DELIBERATE. A startup fact about which
    mode this process came up in must not depend on anyone's logging configuration
    being right, which is why the adjacent "control plane: TLS ON/OFF" line is a
    shell echo in the Dockerfile and this one is a print.

    It was written as `log.info` first and did not appear in the container log at
    all, found by restarting the stack and reading it rather than by reviewing the
    code. The cause was that nothing in this application configured logging, so
    every `log.info` was dropped on the floor -- the reaper's "reclaimed N run(s)
    this pass" included. That gap is CLOSED as of 2026-08-30: the root logger is
    configured at INFO at the top of this module, with the format pinned so the
    existing warning lines keep their shape. This line stays a print regardless,
    for the reason above.
    """
    s = get_settings()
    shipped = shipped_lease_ttl_s()
    note = "" if s.lease_ttl_s == shipped else f" (NOT the shipped default of {shipped}s)"
    print(
        f"control plane: lease {s.lease_ttl_s}s{note} | heartbeat "
        f"{s.heartbeat_interval_s}s | reaper sweep {s.reaper_interval_s}s | "
        f"node timeout {s.node_timeout_s}s",
        flush=True,
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup: bootstrap the admin user + the artifact bucket (both best-effort so
    a momentary MinIO/DB hiccup never blocks the API), then own the two background
    tasks' lifetimes — spawn them on startup, cancel them cleanly on shutdown."""
    _warn_if_experimental()
    _log_effective_lease()
    try:
        async with SessionLocal() as session:
            # Tiers FIRST: the admin row created below points at one, and records the
            # caps it accepted, so the numbers have to exist before the user does.
            # Idempotent — it does nothing once the table has a row, so a deployment
            # that migrated (the migration seeds them) or that edited its own figures
            # keeps exactly what it has.
            await ensure_tiers(session)
            await ensure_admin_user(session)
    except Exception as exc:  # noqa: BLE001 - never let bootstrap crash the app
        log.warning("admin bootstrap skipped (%s)", exc)
    try:
        await asyncio.to_thread(get_storage().ensure_bucket)
    except Exception as exc:  # noqa: BLE001 - the first upload will retry the bucket
        log.warning("MinIO bucket check skipped (%s)", exc)

    # Two background tasks now, owned the same way: spawned here, cancelled here.
    # They are independent — the archiver only ever touches runs that are already
    # terminal, and the reaper only ever touches runs that are not — so neither can
    # be waiting on the other, and one failing does not stop the other.
    tasks = [asyncio.create_task(reaper_loop()), asyncio.create_task(archive_loop())]
    try:
        yield
    finally:
        for task in tasks:
            task.cancel()
        for task in tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass


def create_app() -> FastAPI:
    settings = get_settings()
    app = FastAPI(title="FYP Control Plane", version="0.1.0", lifespan=lifespan)

    # DEV-ONLY wide-open CORS: lets the static W1 page reach the
    # API cross-origin. Tightened to the real web origin in W6 with JWT.
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_allow_origins,
        allow_credentials=False,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(health.router)
    app.include_router(auth.router)
    app.include_router(agent.router)
    app.include_router(artifacts.router)
    app.include_router(keys.router)   # W6b: sealed input + fenced key release
    app.include_router(nodes.router)
    app.include_router(users.router)  # 2026-09-04: /me, tiers, admin user routes
    app.include_router(jobs.router)
    app.include_router(runs.router)
    return app


app = create_app()
