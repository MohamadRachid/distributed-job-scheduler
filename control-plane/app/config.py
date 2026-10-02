"""Central settings — one place reads the environment (brief §7).

W1 only *uses* NODE_TIMEOUT_S (for read-time node liveness). HEARTBEAT_INTERVAL_S
and LEASE_TTL_S are loaded here from day one so the W5 reaper and the agent read
tunables from a single source, not scattered literals.

W5 adds REAPER_INTERVAL_S — how often the background reaper sweeps for expired
leases. LEASE_TTL_S (the run reaper's deadline) and NODE_TIMEOUT_S (read-time node
liveness) stay independent knobs (§8). Tests and scripts/chaos_test.py do not depend
on this value at all: they set `lease_expires_at` to an absolute past time, or advance
an injected clock past whatever the lease is, so changing the default cannot move them.

THE LEASE DEFAULT IS 60s (ruled 2026-08-29, raised from 15s). The lease is how long a
machine may go silent before the server declares it lost and gives its run to someone
else. At 15s a machine that loses its network for half a minute and comes back has
already been fenced out; at 60s it carries on. The cost is that a truly dead machine is
detected in about a minute rather than about fifteen seconds. The fencing counter is
what makes either value safe: a false alarm costs one duplicate attempt's work, never a
wrong result. With HEARTBEAT_INTERVAL_S at 3s, 60s allows twenty missed heartbeats
before a false alarm where 15s allowed five.

This number is defined in TWO places that must agree, because a compose file cannot
import Python: here, and as the interpolation default in docker-compose.yml. The
DEMONSTRATION deliberately runs at 15s so recovery is visible inside the slot —
scripts/stage_demo.ps1 sets it for the stack it brings up and prints the effective
value in its READY banner. Every published measurement was taken at 15s on the frozen
tree and stays labelled as such; this change does not re-measure or re-word any of them.
"""

from functools import lru_cache
from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict

# --- W7a: the experiment-only switches --------------------------------------
#
# These exist for ONE reason: to run the *weaker alternative* to a design choice
# we made, inside our own platform, so the comparison is measured instead of
# asserted. Every default below is today's shipped behaviour, so
# a normal `docker compose up` is bit-for-bit the system we defend.
#
# Three safety rules, all enforced in code, not by discipline:
#   1. docker-compose.yml sets NONE of them (the experiment harness injects them
#      through a throwaway compose override instead).
#   2. An unknown value is a hard startup failure, not a silent fallback to the
#      default — a mislabelled arm is worse than a missing arm.
#   3. Any non-default value prints a loud startup banner AND shows up on
#      GET /health, so a weakened guarantee can never run unnoticed.
EXPERIMENT_DEFAULTS: dict[str, str] = {
    "guarantee": "full",
    "claim": "skip_locked",
    "reschedule": "learned",
}


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Database. asyncpg in prod (compose); tests override with sqlite+aiosqlite.
    database_url: str = "postgresql+asyncpg://fyp:fyp@postgres:5432/fyp"

    # Tunables (protocol.md §8). Names match the brief (_S suffix).
    heartbeat_interval_s: int = 3
    # 60s since 2026-08-29 (was 15s). See the module docstring for why, and keep
    # docker-compose.yml's `${LEASE_TTL_S:-60}` in step with it.
    lease_ttl_s: int = 60
    node_timeout_s: int = 12
    # W5: how often the reaper sweeps for expired leases (independent of lease_ttl).
    reaper_interval_s: int = 3

    # MinIO — present from W1, WIRED IN W6 (the control plane is the only client).
    minio_endpoint: str = "minio:9000"
    minio_access_key: str = "minioadmin"
    minio_secret_key: str = "minioadmin"
    minio_bucket: str = "artifacts"
    minio_secure: bool = False  # plain HTTP inside the compose network (dev)

    # W6 — auth + artifacts.
    # A JWT is a signed pass card: the server verifies the signature instead of
    # keeping a session table. HS256 with one shared secret is enough at our scale.
    jwt_secret: str = "dev-insecure-change-me"
    jwt_expiry_hours: int = 12
    # Bootstrap: if the users table is empty on startup and BOTH are set, create
    # this one admin user. There is NO registration endpoint (user management is
    # future work) — this is the honest, single-user scope.
    admin_username: str | None = None
    admin_password: str | None = None
    # Per-file artifact size cap (MB) — over this the upload is rejected 413.
    max_artifact_mb: int = 50

    # Run-log tiering (2026-08-22). A terminal run's log chunks move out of
    # PostgreSQL into ONE compressed object per (run, attempt) once the run has
    # been finished for longer than this window, and the rows are purged. Reads
    # are unchanged: the database is served first and the archive behind it.
    #
    # SEVEN DAYS IS A DEMO-SAFETY DEFAULT AS WELL AS AN OPERATIONAL ONE. A run
    # created during a demonstration or a rehearsal is minutes old, so nothing
    # the room is looking at can be archived while they look at it. A test asserts
    # this default rather than trusting it, because it is the only thing standing
    # between the archiver and the one demonstration that carries a grade.
    log_retention_days: float = 7.0
    # How often the archiver sweeps. Deliberately slow — the window is measured in
    # days, so a faster sweep would only add load to the database the scheduler
    # claims work from.
    log_archive_interval_s: int = 3600
    # The off switch. Present so a deployment that wants every log line to stay in
    # the database for ever can have that, and so an operator debugging the
    # archiver can stop it without editing code.
    log_archive_enabled: bool = True

    # W6b — private job inputs.
    # Cap on a submitted private input file (MB); over this the submit is 413.
    max_input_mb: int = 100
    # How long a key ticket stays redeemable. Short on purpose: it only has to
    # survive the gap between "agent asks" and "container starts and redeems".
    key_ticket_ttl_s: int = 120
    # Size of the RAM folder a PRIVATE container opens its data into. Agent-side in
    # protocol.md §8, and mirrored here for one reason: a private run's scratch IS
    # that folder, so this is the ceiling a private job's `scratch_mb` is validated
    # against, and the control plane cannot validate against a number it does not
    # hold. The same two-places-that-must-agree situation as `lease_ttl_s` and
    # docker-compose.yml, and named as such rather than left to be discovered — a
    # deployment that raises PRIVATE_TMPFS_MB on its workers raises it here too.
    private_tmpfs_mb: int = 256

    # Where a CONTAINER should redeem its ticket. Empty = derive it from the agent's
    # own request (swapping a loopback host for host.docker.internal, which is how a
    # container reaches its host). Set it only if a deployment needs a fixed address.
    container_key_url: str = ""

    # --- W7a experiment-only switches (see EXPERIMENT_DEFAULTS above) --------
    # Literal types on purpose: a typo ("lease-only", "skiplocked") makes the app
    # refuse to start with a named error instead of quietly running `full`.
    #
    # guarantee: how much of the no-duplicate-accepted-result machinery is on.
    #   full        today. Lease + reaper + the whole fencing-class rejection
    #               (stale attempt OR wrong node -> 409).
    #   owner_only  lease + reaper + the WRONG-NODE half only. A late report is
    #               refused if it comes from a node that does not own the run, but
    #               there is no per-attempt identity, so a report from the owning
    #               node is accepted however old the execution behind it is.
    #               This is the STRONGEST honest alternative: what a careful
    #               engineer writes who checks ownership but has never needed an
    #               epoch. It is the arm that decides whether the fencing TOKEN
    #               earns its place, as opposed to merely some ownership check.
    #   lease_only  lease + reaper still run, but NO fencing-class rejection at
    #               all: a late result from a presumed-dead execution is ACCEPTED.
    #               The honest "a timeout is enough" system a competent engineer
    #               builds before meeting the duplicate-result problem.
    #   none        neither: the reaper does not sweep and nothing is fenced.
    experiment_guarantee_mode: Literal[
        "full", "owner_only", "lease_only", "none"
    ] = "full"
    # claim: how the scheduler takes a PENDING run out of the queue.
    #   skip_locked  today. SELECT … FOR UPDATE SKIP LOCKED — never blocks, never
    #                double-assigns.
    #   blocking     SELECT … FOR UPDATE — correct, but concurrent claimers queue
    #                behind each other instead of stepping past.
    #   naive        plain SELECT, then the UPDATE on commit, no row lock at all —
    #                the realistic beginner version at READ COMMITTED.
    experiment_claim_mode: Literal["skip_locked", "blocking", "naive"] = "skip_locked"
    # reschedule: what happens after a run dies of a kernel-proven RAM shortage.
    #   learned  today (W5c). Requeue demanding strictly more RAM than the node
    #            that died; fail fast when the pool can never satisfy it.
    #   blind    requeue with no learned requirement — retry anywhere eligible.
    #   none     no retry at all; the run stays FAILED.
    experiment_reschedule_mode: Literal["learned", "blind", "none"] = "learned"

    # DEV-ONLY open CORS. The static W1 page
    # is served from a different origin (or file://). Tightened to the real web
    # origin in W6 when JWT lands. DO NOT ship this wide open.
    cors_allow_origins: list[str] = ["*"]


@lru_cache
def get_settings() -> Settings:
    return Settings()


def shipped_lease_ttl_s() -> int:
    """The lease default as DECLARED here, independent of the environment.

    `get_settings().lease_ttl_s` is the EFFECTIVE value — the environment wins,
    which is the whole point of `${LEASE_TTL_S:-60}` in docker-compose.yml and is
    how the demonstration gets 15. This returns what we ship, so a caller can say
    which of the two it is looking at instead of guessing.

    It is read off the field declaration rather than repeated as a second literal,
    so this file contains exactly one 60 and there is nothing here to drift.
    """
    return int(Settings.model_fields["lease_ttl_s"].default)


def experiment_mode() -> dict[str, str]:
    """The three switches as one small dict — what /health publishes and what the
    harness asserts against before it measures anything (brief §4.1)."""
    s = get_settings()
    return {
        "guarantee": s.experiment_guarantee_mode,
        "claim": s.experiment_claim_mode,
        "reschedule": s.experiment_reschedule_mode,
    }


def experiment_is_default() -> bool:
    """True when the process is running the system we actually ship and defend."""
    return experiment_mode() == EXPERIMENT_DEFAULTS
