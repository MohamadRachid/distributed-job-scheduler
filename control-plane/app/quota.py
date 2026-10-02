"""The storage quota policy, in one place (2026-09-04).

Every user belongs to a **tier**, and a tier is two numbers:

  * **retained** — how many bytes the platform may HOLD for that user in object
    storage across all their jobs: results, checkpoints, archived logs, sealed
    inputs;
  * **scratch** — how many bytes ONE of their runs may write to temporary disk on
    the worker while it runs.

This module owns the retained half and the acceptance rule. The scratch half is
enforced where the disk actually is — on the worker, by the agent — because the
control plane cannot see a worker's disk and would be guessing if it claimed to.

Nothing new was added to the system to do any of this: no second service, no MinIO
configuration, no credential on a worker. The control plane already brokers every
byte that reaches storage, so the two doors it already owns — job submission and
artefact upload — are the two places a cap can be enforced before bytes are stored
rather than after.

---

**The sum is computed under a lock; it is never a cached counter.**

A counter column would be faster and it would drift. It has to be decremented on
every path that removes an object — checkpoint supersede, checkpoint cleanup at a
terminal state, crypto-shred, the R6 release valve, a failed archive verification —
and the first one anybody forgets makes the number wrong for ever, in a direction
nobody notices until a user is refused work they should have been allowed. A sum
recomputed from the rows cannot drift, because there is no second copy of the truth
to disagree with the first.

The lock is what makes the sum safe under concurrency: `SELECT ... FROM users WHERE
id = :id FOR UPDATE` at the top of the transaction serialises that ONE user's
uploads, so two concurrent uploads cannot both read the same "used" figure and both
decide they fit. Other users are untouched — the lock is on their row, not on a
table. What it costs is stated rather than hidden: at the measured 74.127 MB/s a
64 MB artefact holds one user's row for roughly nine tenths of a second.

On SQLite (the test database) `FOR UPDATE` renders as nothing, which is the same
situation the run-status fence has always been in — that is why the locking property
is proven on real PostgreSQL and not in the SQLite suite.

---

**A job with no owner has no cap.** `jobs.user_id` is nullable and the chaos test
creates jobs through the handler with no user on purpose (see `api/jobs.py`). No
user means no tier, and no tier means nothing to enforce — stated here so that
"quota did not apply" is a readable fact rather than a silent hole.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from .models import Artifact, Job, Run, RunLogArchive, Tier, User

# --- the machine labels a refusal travels under -----------------------------
# Short, stable, and shared with the UI and the agent. They name WHICH rule refused,
# never merely that something was refused: "you have not accepted your limits" and
# "you are full" are different problems with different fixes, and a user told only
# "413" cannot tell them apart.
LIMITS_NOT_ACCEPTED = "LIMITS_NOT_ACCEPTED"
STORAGE_QUOTA_EXCEEDED = "STORAGE_QUOTA_EXCEEDED"
SCRATCH_QUOTA_EXCEEDED = "SCRATCH_QUOTA_EXCEEDED"

# The tier every user lands in unless an admin moves them. Matches the column's
# server default, so a user row and this constant cannot disagree.
DEFAULT_TIER_ID = "standard"

_MB = 1024 * 1024


def mb(n: int | float | None) -> float:
    """Bytes -> MB, rounded to three places. Every number this policy shows a human
    is in MB: bytes are unreadable at gigabyte scale and gigabytes hide the megabyte
    the proofs run at."""
    return round((n or 0) / _MB, 3)


@dataclass(frozen=True)
class RetainedState:
    """What the retained check found. `used` excludes the incoming bytes; `fits` has
    already accounted for them and for anything they replace."""

    used: int
    cap: int
    incoming: int
    replacing: int
    fits: bool

    @property
    def would_be(self) -> int:
        return self.used - self.replacing + self.incoming

    def payload(self, **extra) -> dict:
        """The refusal body, in MB, with the reason first. One shape wherever a
        retained refusal is raised, so the UI parses one thing."""
        body = {
            "reason": STORAGE_QUOTA_EXCEEDED,
            "used_mb": mb(self.used),
            "cap_mb": mb(self.cap),
        }
        body.update(extra)
        return body


async def get_tier(session: AsyncSession, tier_id: str | None) -> Tier | None:
    return await session.get(Tier, tier_id or DEFAULT_TIER_ID)


async def lock_user(session: AsyncSession, user_id: str | None) -> User | None:
    """Take the user's row for the rest of this transaction, or return None when the
    thing being checked has no owner.

    This is the serialisation point for one user's uploads. It is deliberately the
    FIRST thing a quota-checked handler does, before any sum and before any byte
    reaches storage, because a lock taken after the read it was meant to protect
    protects nothing."""
    if not user_id:
        return None
    return (
        await session.execute(
            select(User).where(User.id == user_id).with_for_update()
        )
    ).scalar_one_or_none()


def limits_accepted(user: User, tier: Tier | None) -> bool:
    """Has this user agreed to the numbers that apply to them RIGHT NOW?

    Three things must line up: the tier they accepted, and both caps as that tier
    carried them at the moment of acceptance. Comparing the numbers rather than a
    boolean is what makes "the user agrees to numbers, not to a word" true instead of
    merely said — a move to another tier, or an edit to a tier's figures, withdraws
    acceptance by arithmetic, and there is no flag anywhere that could be left stale."""
    if tier is None:
        return False
    return (
        user.limits_accepted_at is not None
        and user.limits_accepted_tier == tier.id
        and user.limits_accepted_retained_cap_bytes == tier.retained_cap_bytes
        and user.limits_accepted_scratch_cap_bytes == tier.scratch_cap_bytes
    )


def accept_limits(user: User, tier: Tier) -> None:
    """Record the agreement as the numbers that were on the screen."""
    user.limits_accepted_tier = tier.id
    user.limits_accepted_at = datetime.now(timezone.utc)
    user.limits_accepted_retained_cap_bytes = tier.retained_cap_bytes
    user.limits_accepted_scratch_cap_bytes = tier.scratch_cap_bytes


async def retained_used_bytes(session: AsyncSession, user_id: str) -> int:
    """Every byte object storage is holding for this user, summed from the rows.

    THREE separate aggregates, deliberately not one joined query. A single query
    joining artefacts, archives and jobs would fan out — a job with two runs and one
    sealed input would count that input twice — and the bug would be invisible
    because the number it produces is still a plausible number. Three sums over three
    disjoint sets cannot do that.

    What is counted:
      * `artifacts.size`  — results AND checkpoints, EVERY attempt. A stale attempt's
        leftovers are unreadable (the fence) but they still occupy storage, and a cap
        that pretended otherwise would be a cap on what you can read rather than on
        what you are using. They stop counting when R6 removes them.
      * `run_log_archives.size_bytes` — a run's logs after they left the database.
      * `jobs.input_size_bytes` — a private job's sealed input.

    What is NOT counted, and is stated as a limit rather than left to be discovered:
    log chunks still in `run_logs`. They live in PostgreSQL, not in object storage,
    and they tier out to an archive after `LOG_RETENTION_DAYS`, at which point they
    start counting.

    A NULL size counts as zero. That undercounts a user whose rows predate this
    change, which is the safe direction to be wrong in for a cap — it can delay a
    refusal, never cause a wrong one — and `scripts/quota_audit.py` is what closes
    the gap by backfilling those rows from the store."""
    artefacts = (
        await session.execute(
            select(func.coalesce(func.sum(Artifact.size), 0))
            .select_from(Artifact)
            .join(Run, Run.id == Artifact.run_id)
            .join(Job, Job.id == Run.job_id)
            .where(Job.user_id == user_id)
        )
    ).scalar_one()
    archives = (
        await session.execute(
            select(func.coalesce(func.sum(RunLogArchive.size_bytes), 0))
            .select_from(RunLogArchive)
            .join(Run, Run.id == RunLogArchive.run_id)
            .join(Job, Job.id == Run.job_id)
            .where(Job.user_id == user_id)
        )
    ).scalar_one()
    inputs = (
        await session.execute(
            select(func.coalesce(func.sum(Job.input_size_bytes), 0)).where(
                Job.user_id == user_id
            )
        )
    ).scalar_one()
    return int(artefacts) + int(archives) + int(inputs)


async def check_retained(
    session: AsyncSession,
    user: User,
    tier: Tier | None,
    incoming: int,
    replacing: int = 0,
) -> RetainedState:
    """Would storing `incoming` bytes for `user` cross their retained cap?

    `replacing` is what those bytes take the place of, and it is why a checkpoint
    saved every thirty seconds does not slowly eat a quota it never grows into. A
    checkpoint is written to a stable object key, so a repeat REFRESHES one row with
    new bytes (protocol.md §9, 2026-08-13) — the storage cost of that save is the
    difference between the two, not the whole file. A checkpoint that stays the same
    size costs nothing more; one that grows is charged only its growth.

    Call this with the user's row already locked (`lock_user`), or the answer is a
    guess about a number another request may be changing."""
    cap = tier.retained_cap_bytes if tier is not None else 0
    used = await retained_used_bytes(session, user.id)
    return RetainedState(
        used=used,
        cap=cap,
        incoming=incoming,
        replacing=replacing,
        fits=(used - replacing + incoming) <= cap,
    )


def scratch_cap_mb(tier: Tier | None) -> int | None:
    """The tier's scratch cap in whole MB, or None when there is no tier (and so
    nothing to enforce). Whole MB because that is the unit the assignment carries and
    the unit the agent compares against — a fractional cap would only ever be a
    rounding difference between two machines."""
    if tier is None:
        return None
    return int(tier.scratch_cap_bytes // _MB)


def reconcile(
    rows: dict[str, int], objects: dict[str, int]
) -> tuple[int, list[str]]:
    """Compare what the ROWS claim storage is holding against what it holds.

    `rows` maps object key -> the size a database row claims. `objects` maps object
    key -> the size the store actually reports. Returns the total the store holds for
    the keys the rows name, and the keys the store could not produce at all.

    A row whose object is missing contributes NOTHING to the object-side total, which
    is what makes it surface as drift instead of quietly balancing out. This lives
    here rather than in `scripts/quota_audit.py` for one reason: it is the arithmetic
    the policy rests on, so it belongs where the policy is and where the test suite
    can reach it without a database or a bucket."""
    present = 0
    missing: list[str] = []
    for key in rows:
        if key in objects:
            present += objects[key]
        else:
            missing.append(key)
    return present, sorted(missing)


_GB = 1024 * 1024 * 1024

# --- The two tiers, sized 2026-09-05 (was: the supervisor's examples) -------------
#
# Derived rather than exemplified. The full derivation, its anchors and the reference
# lab it assumes are written out in the migration that moved an existing deployment to
# these numbers: alembic/versions/20260905_a2b3c4d5e6f7_tier_sizing.py. In one line
# each: scratch is a 10 GB dataset + a 12 GB checkpoint for a billion-parameter model
# (12.044 bytes per parameter, measured in docs/evidence/checkpoint_reshape_2026-09-01
# .txt) + 3 GB of output, doubled; retained is ten kept sweeps of 100 jobs at the 50 MB
# MAX_ARTIFACT_MB + five concurrent 12 GB checkpoints + archived logs, with headroom.
#
# **These figures are duplicated in that migration, and that duplication is
# deliberate.** A migration describes a historical step and must keep saying what it
# did even after these constants move again; importing them there would rewrite
# history every time the policy changes. `tests/test_storage_quota.py` asserts the two
# copies agree, so the pair cannot drift the way the checkpoint filename once did.
STANDARD_RETAINED_BYTES = 200 * _GB     # 214,748,364,800
STANDARD_SCRATCH_BYTES = 50 * _GB       #  53,687,091,200
LIMITED_RETAINED_BYTES = 20 * _GB       #  21,474,836,480
LIMITED_SCRATCH_BYTES = 5 * _GB         #   5,368,709,120

# "plan" is the supervisor's own word for a tier, used once in each description.
STANDARD_DESCRIPTION = (
    "The standard plan: 200 GB retained on the server, 50 GB of temporary disk per "
    "run on a worker. Sized for a reference lab of ten workers (~500 GB free each, "
    "four runs at once), one 4 TB storage volume and twenty users."
)
LIMITED_DESCRIPTION = (
    "The limited plan: 20 GB retained on the server, 5 GB of temporary disk per run "
    "on a worker; one tenth of the standard plan on both numbers."
)


async def ensure_tiers(session: AsyncSession) -> None:
    """Seed the two tiers if the table is empty (startup, idempotent).

    The migration seeds them for a deployment that migrates. This covers the other
    two ways the schema comes into existence — a database created from the models,
    and the test suite — so there is no configuration in which a user's `tier_id`
    points at a row that does not exist. Doing nothing once a row is present means a
    deployment that edited its numbers keeps them."""
    existing = (await session.execute(select(Tier.id).limit(1))).first()
    if existing is not None:
        return
    session.add_all(
        [
            Tier(
                id="standard",
                retained_cap_bytes=STANDARD_RETAINED_BYTES,
                scratch_cap_bytes=STANDARD_SCRATCH_BYTES,
                description=STANDARD_DESCRIPTION,
            ),
            Tier(
                id="limited",
                retained_cap_bytes=LIMITED_RETAINED_BYTES,
                scratch_cap_bytes=LIMITED_SCRATCH_BYTES,
                description=LIMITED_DESCRIPTION,
            ),
        ]
    )
    await session.commit()
