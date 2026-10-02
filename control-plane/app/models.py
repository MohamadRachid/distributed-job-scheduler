"""All six tables, FROZEN in W1.

W1 only exercises `users`/`nodes`, but every table is defined now so later weeks
add *behaviour*, not schema. The contract-critical constraints
(the `run_logs` uniqueness, `runs.attempt` fencing token) are present even though
nothing writes to them yet — they are the wall.

Cross-DB portability note: IDs are stored as 36-char UUID strings and status
columns use `Enum(..., native_enum=False)` (renders as VARCHAR + CHECK on both
Postgres and SQLite), so the same models back the asyncpg prod DB and the SQLite
test DB without divergence.
"""

import enum
import uuid
from datetime import datetime, timezone

from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    Enum as SAEnum,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.types import JSON
from sqlalchemy.orm import Mapped, mapped_column

from .db import Base


def _uuid() -> str:
    return str(uuid.uuid4())


def _now() -> datetime:
    return datetime.now(timezone.utc)


# --- Enumerations (stored as portable VARCHAR + CHECK) ---


class NodeStatus(str, enum.Enum):
    """Node's SELF-REPORTED state. A node never reports itself offline —
    online/offline is derived at read time from last_heartbeat (protocol.md §2)."""

    idle = "idle"
    busy = "busy"


class JobStatus(str, enum.Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


class RunStatus(str, enum.Enum):
    """Run state machine (protocol.md §3). LOST is the recovery path (W5)."""

    PENDING = "PENDING"
    ASSIGNED = "ASSIGNED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    LOST = "LOST"


_node_status = SAEnum(NodeStatus, native_enum=False, length=8, name="node_status")
_job_status = SAEnum(JobStatus, native_enum=False, length=12, name="job_status")
_run_status = SAEnum(RunStatus, native_enum=False, length=12, name="run_status")


# --- Tables ---


class Tier(Base):
    """A storage tier: two numbers a user is held to (2026-09-04).

    `retained_cap_bytes` is how many bytes the platform may HOLD for that user in
    object storage across all their jobs — results, checkpoints, archived logs and
    sealed inputs. `scratch_cap_bytes` is how many bytes ONE of their runs may write
    to temporary disk on the worker while it runs. Two different questions about two
    different disks, which is why they are two columns and not one.

    **The numbers are configuration and are never measured.** The seeded defaults are
    the supervisor's own examples (2026-09-03); every proof of the mechanism runs at
    megabytes so a refusal is visible live, in seconds.

    THE PRIMARY KEY IS THE TIER'S NAME. The brief specified `id` plus a UNIQUE
    `name`, and those would have been two columns saying one thing: a tier has no
    identity apart from what it is called. Collapsing them is what lets
    `users.tier_id` carry `server_default='standard'`, so every user row that existed
    before this migration lands in a tier by the schema's own doing rather than by a
    data-migration step that could be skipped or half-run.

    There is deliberately NO route that edits a tier's numbers — they are set where
    the deployment is configured. That matters for acceptance: `User` records the
    caps it agreed to, so if these numbers ever were edited, acceptance would lapse
    on its own rather than silently covering figures nobody agreed to."""

    __tablename__ = "tiers"

    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    retained_cap_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    scratch_cap_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)


class User(Base):
    __tablename__ = "users"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    username: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    password_hash: Mapped[str] = mapped_column(String(255), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_now, nullable=False
    )
    # --- Storage quota policy (2026-09-04) ---------------------------------
    # Roles are ONE flag. `is_admin` gates the routes that create users, move a
    # user between tiers, and mark a machine trusted — every decision that is made
    # ABOUT someone rather than BY them. The startup user is the admin; the
    # migration marks it, and `ensure_admin_user` keeps it so. There is deliberately
    # no finer permission model: two kinds of caller is the honest scope, and a role
    # table nobody populates would be a claim we could not defend.
    is_admin: Mapped[bool] = mapped_column(
        Boolean, default=False, nullable=False, server_default="0"
    )
    # Which tier's numbers apply to this user. Server default `standard`, so every
    # row that existed before this migration lands in a tier without a data step
    # that could be skipped or half-run.
    tier_id: Mapped[str] = mapped_column(
        String(32), ForeignKey("tiers.id"), nullable=False, server_default="standard"
    )
    # Acceptance, stored as WHAT WAS AGREED rather than as a yes/no flag. A user
    # agrees to two numbers, not to a word: so we record the tier they accepted AND
    # the two caps that tier carried at the moment they accepted. Acceptance holds
    # only while all three still match (see `quota.limits_accepted`). A tier move —
    # or an edit to a tier's numbers — therefore withdraws it by arithmetic, with
    # nothing to remember to reset and nothing that can drift.
    limits_accepted_tier: Mapped[str | None] = mapped_column(
        String(32), ForeignKey("tiers.id"), nullable=True
    )
    limits_accepted_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    limits_accepted_retained_cap_bytes: Mapped[int | None] = mapped_column(
        BigInteger, nullable=True
    )
    limits_accepted_scratch_cap_bytes: Mapped[int | None] = mapped_column(
        BigInteger, nullable=True
    )


class Node(Base):
    __tablename__ = "nodes"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    # Self-reported only (idle|busy). Never stores offline.
    status: Mapped[NodeStatus] = mapped_column(
        _node_status, default=NodeStatus.idle, nullable=False
    )
    cpu_cores: Mapped[int] = mapped_column(Integer, nullable=False)
    has_gpu: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    ram_mb: Mapped[int] = mapped_column(Integer, nullable=False)
    capacity: Mapped[int] = mapped_column(Integer, nullable=False)
    last_heartbeat: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    agent_version: Mapped[str | None] = mapped_column(String(32), nullable=True)
    registered_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_now, nullable=False
    )
    # Node token is stored HASHED, never plaintext (protocol.md §2).
    token_hash: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    # Rich machine identity (CPU/GPU names, RAM speed, model, OS, sw versions),
    # reported once at registration. Free-form JSON — the pool is heterogeneous,
    # so a fixed column set would either bloat or lie. Additive, nullable
    # (supervisor-requested 2026-07-06; an additive, change-controlled edit).
    hw_specs: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    # Latest usage sample (cpu_pct, ram_pct, ...), refreshed each heartbeat.
    # Current value only — time-series history stays with the Prometheus stretch.
    usage: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    # W5b black box: the last battery reading before the node went silent. Nullable
    # because desktops have no battery (psutil returns None) and old agents omit it.
    # Read at outage time to refine a "likely" cause ("battery 4% & discharging").
    battery_pct: Mapped[float | None] = mapped_column(Float, nullable=True)
    battery_charging: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    # W6b trust tier: may this machine receive PRIVATE jobs? This is the control
    # plane's judgment, set by an admin (PATCH /nodes/{id}/trusted) — a node can
    # NEVER declare itself trusted (registration does not accept this field), because
    # a machine claiming "trust me" is exactly what the tier exists to stop.
    trusted: Mapped[bool] = mapped_column(
        Boolean, default=False, nullable=False, server_default="0"
    )
    # Free space on the filesystem holding the agent's working root, refreshed by
    # every heartbeat that carries it (2026-09-04). NULL = an agent old enough not to
    # report it, and the claim query reads NULL as "unknown, do not exclude".
    #
    # This is the ONE usage-derived field that feeds matching, and protocol.md §2's
    # "neither feeds the scheduler" is narrowed by a dated decision rather than
    # quietly broken. The reason it has to: RAM matching can use the DECLARED total
    # because the kernel enforces the container's cap separately, so a machine's
    # declared RAM stays a true statement about what it can be asked for. Disk has no
    # equivalent per-container cap on our machines, so the only truthful placement
    # signal is how much space is free now.
    disk_free_mb: Mapped[int | None] = mapped_column(Integer, nullable=True)


class Job(Base):
    __tablename__ = "jobs"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    user_id: Mapped[str | None] = mapped_column(
        String(36), ForeignKey("users.id"), nullable=True
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    image: Mapped[str] = mapped_column(String(512), nullable=False)
    entrypoint: Mapped[list | None] = mapped_column(JSON, nullable=True)
    env: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    resource_reqs: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    target_node_ids: Mapped[list | None] = mapped_column(JSON, nullable=True)
    replicas: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    status: Mapped[JobStatus] = mapped_column(
        _job_status, default=JobStatus.PENDING, nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_now, nullable=False
    )
    # W6b private jobs (additive). `private` marks a job whose input
    # file is SEALED (AES-GCM) at submit: the ciphertext lives in MinIO under
    # `input_object_key`, the key lives in `job_keys` (a separate table on purpose),
    # and the plaintext exists only inside the running container's RAM.
    # `input_filename` is the original name, shown in the UI. Both null for a normal
    # job, so every pre-W6b row stays exactly as valid as before.
    private: Mapped[bool] = mapped_column(
        Boolean, default=False, nullable=False, server_default="0"
    )
    input_object_key: Mapped[str | None] = mapped_column(String(512), nullable=True)
    input_filename: Mapped[str | None] = mapped_column(String(255), nullable=True)
    # Size of the SEALED object stored under `input_object_key` (2026-09-04). A
    # private job's input is retained storage like any other object, so the quota sum
    # has to be able to see it — and it must not have to ask the object store, because
    # a sum that walks a bucket is a slower and more fragile thing than a sum over
    # rows. Written when the sealed blob is stored; NULL on rows written before this
    # date, which `scripts/quota_audit.py` backfills from the bucket.
    input_size_bytes: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    # Checkpoint-use advisor (2026-09-05, M. Ayli's suggestion of the same day).
    #
    # `source_text` is the training script the user PASTED, optional and at most 64 KB
    # (schemas.JobCreate enforces the size at the door). It is stored rather than
    # scanned-and-discarded for one reason: the advice below is a claim about a
    # specific text, and a claim whose subject was thrown away cannot be checked
    # afterwards by the user, by us, or by a jury asking where the verdict came from.
    #
    # It is NOT the job's code. The container still runs whatever the image contains;
    # nothing here is executed, fetched, or sent to a worker. Pasting a script that
    # differs from the image produces advice about the paste, which is why the page
    # says the image is not read.
    source_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    # What the two arms concluded: {verdict, arm_a, arm_b, checked_at}. Generic JSON
    # rather than the brief's JSONB, matching every other JSON column in this file —
    # the same declaration has to work on Postgres and on the SQLite the tests run on,
    # and nothing here queries INTO the document, so JSONB's indexable operators would
    # buy nothing and cost a dialect split.
    checkpoint_advice: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    # Sealed by default (2026-09-06). TRUE on every job created from that date, and
    # it means one thing said in three places: this job has its own key, its input
    # was sealed at submit in the FRAMED format, and its outputs and checkpoints are
    # sealed inside the container before they leave it. Nothing about placement, and
    # no button — a job is sealed because every job is.
    #
    # FALSE on every row that existed before, and the server default keeps it that
    # way, because a column cannot make a claim about bytes nobody has read. Those
    # older jobs still run: an unsealed job's artefacts are stored and downloaded
    # exactly as they always were.
    #
    # This is what the claim query reads to keep a sealed run away from an agent too
    # old to stage one (scheduler.MIN_SEALED_AGENT_VERSION) — the same shape as the
    # three version guards already there.
    sealed: Mapped[bool] = mapped_column(
        Boolean, default=False, nullable=False, server_default="0"
    )
    # Placement: run only on machines an ADMIN marked `trusted` (2026-09-06).
    #
    # This is the one protection the old private route bundled that is genuinely a
    # CHOICE rather than a floor. Sealing costs a user nothing, so it is on for
    # everyone; restricting a job to trusted machines costs them the rest of the
    # pool, so it is theirs to ask for. Available on any job, off by default, and it
    # does nothing else — it is a filter in `_eligible`, not a different way of
    # running the container.
    trusted_only: Mapped[bool] = mapped_column(
        Boolean, default=False, nullable=False, server_default="0"
    )


class Run(Base):
    __tablename__ = "runs"

    # The claim query's index (2026-09-06). Every heartbeat walks the PENDING queue
    # oldest-first (scheduler._survey), and since the fixed candidate window was
    # removed that walk can reach the end of the queue rather than stopping at four
    # rows. `(status, created_at)` is exactly the shape of that walk: the filter
    # first, then the order, so the database reads the rows it needs in the order it
    # needs them instead of sorting the whole table each time.
    #
    # `id` is not in the index. It is in the ORDER BY as the tie-break that makes the
    # scan's paging exact, but adding it here would only widen the index for rows the
    # planner already has to visit.
    __table_args__ = (Index("ix_runs_status_created_at", "status", "created_at"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    job_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("jobs.id"), nullable=False
    )
    node_id: Mapped[str | None] = mapped_column(
        String(36), ForeignKey("nodes.id"), nullable=True
    )
    status: Mapped[RunStatus] = mapped_column(
        _run_status, default=RunStatus.PENDING, nullable=False
    )
    # Fencing token / epoch (protocol.md §6). Incremented on each (re-)dispatch
    # from W2/W5. Present now so the contract never changes.
    attempt: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    lease_expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    retries_remaining: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    exit_code: Mapped[int | None] = mapped_column(Integer, nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    finished_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_now, nullable=False
    )
    # W5b diagnostics — why a dead run died (nullable, additive). A SUCCEEDED run
    # carries NONE (silence is only allowed when nothing died). `failure_reason` is
    # a short machine label (OOM_KILLED, APP_ERROR, NODE_LOST, ...); `failure_detail`
    # is the human sentence (hard facts, or "likely …" with evidence when inferred).
    failure_reason: Mapped[str | None] = mapped_column(String(32), nullable=True)
    failure_detail: Mapped[str | None] = mapped_column(Text, nullable=True)
    # Live progress (0–1) + the latest ##PROGRESS metrics JSON, updated each heartbeat.
    progress: Mapped[float | None] = mapped_column(Float, nullable=True)
    metrics_last: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    # W5c failure-aware rescheduling (control-plane-only; the agent never sees these,
    # so the frozen agent↔CP message shapes are unchanged). A run that died a
    # *proven* node-capacity OOM is requeued carrying `learned_min_ram_mb` — the
    # failed node's RAM — and the claim query then places it only on a node with
    # strictly more RAM. `escalation_count` bounds this to a few tries.
    learned_min_ram_mb: Mapped[int | None] = mapped_column(Integer, nullable=True)
    escalation_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    # Storage quota (2026-09-04). Stamped when an artefact upload for THIS attempt was
    # refused because the owner's retained cap was reached, and read when the agent
    # later posts a terminal status: whatever word the agent posts, the outcome is
    # FAILED with STORAGE_QUOTA_EXCEEDED. The control plane guarantees that rather
    # than trusting the agent to, exactly as W5c decides an escalation from its own
    # facts. `quota_refused_detail` names the file and the cap, so the sentence the
    # user reads is about their own run and not a generic apology.
    quota_refused_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    quota_refused_detail: Mapped[str | None] = mapped_column(Text, nullable=True)
    # Cancel (2026-09-07, walk 1 row 64). When a user asked this run to stop. Read by
    # the heartbeat, which tells the worker holding the run (`commands`), and by the
    # reaper, which ends a stamped run as cancelled instead of requeueing it when its
    # worker never confirms. Not a state: a cancelled run ends FAILED with the reason
    # CANCELLED, so the state machine the report draws is unchanged.
    cancel_requested_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )


class RunLog(Base):
    __tablename__ = "run_logs"
    # DB enforces no-duplicate log chunks, not app code. This is a
    # load-bearing reliability constraint even though nothing writes logs until W3.
    __table_args__ = (
        UniqueConstraint("run_id", "attempt", "seq", name="uq_run_logs_run_attempt_seq"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    run_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("runs.id"), nullable=False
    )
    attempt: Mapped[int] = mapped_column(Integer, nullable=False)
    seq: Mapped[int] = mapped_column(Integer, nullable=False)
    chunk: Mapped[str] = mapped_column(Text, nullable=False)
    ts: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_now, nullable=False
    )


class RunLogArchive(Base):
    """Where one (run, attempt)'s log chunks went after they left the database.

    Run logs are HOT in PostgreSQL while a run is alive — the live stream, the
    catch-up cursor and the `UNIQUE(run_id, attempt, seq)` de-duplication all need
    them there. But `run_logs` only ever grows, and nothing was reclaiming it: a
    long-lived deployment's largest table would eventually be the output of runs
    nobody will read again. So once a run is terminal AND older than the retention
    window, its chunks are written to ONE compressed object per (run, attempt) and
    the rows are purged, leaving this row as the pointer.

    The primary key is (run_id, attempt), which is the same statement as "one object
    per (run, attempt)" made by the schema rather than by a convention — a second
    archive of the same attempt cannot exist, so re-archiving after a crash is an
    upsert and not a duplicate.

    `sha256` is the digest of the canonical body the object holds, computed from the
    rows BEFORE they were deleted and re-verified against the bytes read back out of
    the store. It is the receipt: it is what lets the purge be a two-phase commit
    rather than a hopeful delete, and it is what a later integrity check would
    compare against.

    `chunk_count` and `max_seq` are the index. `max_seq` lets a read with a cursor
    skip fetching the object at all when no archived chunk could satisfy it, so a
    poller that is already past the archive never pays for it.
    """

    __tablename__ = "run_log_archives"

    run_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("runs.id"), primary_key=True
    )
    attempt: Mapped[int] = mapped_column(Integer, primary_key=True)
    object_key: Mapped[str] = mapped_column(String(512), nullable=False)
    chunk_count: Mapped[int] = mapped_column(Integer, nullable=False)
    max_seq: Mapped[int] = mapped_column(Integer, nullable=False)
    sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    archived_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_now, nullable=False
    )
    # Bytes of the compressed object this row points at (2026-09-04). Written at
    # archive time so the retained-storage sum can count archived logs without listing
    # a bucket. NULL on rows written before this date; the audit script backfills those
    # from the store, and the sum treats NULL as zero rather than guessing —
    # undercounting a user is the safe direction to be wrong in for a cap.
    size_bytes: Mapped[int | None] = mapped_column(BigInteger, nullable=True)


class Artifact(Base):
    """A run's output file, stored in MinIO by the control plane (W6). The row is
    the index; the bytes live under `object_key`.

    `attempt` is the fencing token, exactly like `run_logs`: an artifact belongs to
    the attempt that produced it. `UNIQUE(run_id, attempt, object_key)` is the same
    idempotency idea as the log UNIQUE — a re-sent upload (after a lost 200) upserts
    the one row rather than creating a second, and a stale attempt's file can never
    masquerade as the accepted result. `GET /runs/{id}/artifacts` returns only the
    run's CURRENT attempt, so a zombie's leftovers are never shown as accepted.

    2026-08-13 adds `kind`. A **result** is a claim about a finished run: accepting
    two would break at-most-once, so it is readable only for the run's current
    attempt. A **checkpoint** is intermediate training state: reading a stale one
    costs repeated work and nothing else, so it is readable across attempts. The rule
    lives in the query (`_artifact_query`), never in which route happened to ask.
    """

    __tablename__ = "artifacts"
    __table_args__ = (
        UniqueConstraint(
            "run_id", "attempt", "object_key", name="uq_artifacts_run_attempt_key"
        ),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    run_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("runs.id"), nullable=False
    )
    # Additive W6. server_default 0 backfills a live DB cleanly
    # (there are no artifact rows before W6), while ORM inserts pass it explicitly.
    attempt: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    object_key: Mapped[str] = mapped_column(String(512), nullable=False)
    size: Mapped[int | None] = mapped_column(Integer, nullable=True)
    content_type: Mapped[str | None] = mapped_column(String(255), nullable=True)
    # Additive 2026-08-13 (checkpoint and resume). `kind` says what this file IS, and
    # the reads branch on it: a 'result' is readable only for the run's CURRENT
    # attempt (the fence, unchanged), while a 'checkpoint' is readable across
    # attempts, which is what lets a re-dispatched run pick up where the dead one
    # stopped. A real column rather than a filename convention on purpose — the guard
    # "a checkpoint is never served as a result" is then enforced by the schema and
    # the query instead of by a naming rule anyone can typo. server_default 'result'
    # backfills every pre-existing row correctly: everything written before this date
    # was a result, and an old agent that sends no `kind` still stores one.
    kind: Mapped[str] = mapped_column(
        String(16), nullable=False, server_default="result"
    )
    # SHA-256 of the bytes the control plane actually stored — computed here, not
    # trusted from the uploader. The agent re-hashes a downloaded checkpoint and
    # treats a mismatch as ABSENT (start over) rather than as an error: a run that
    # crashes on resume is worse than one that starts again. NULL on rows written
    # before this date, which is why "no digest" is treated as "cannot verify" and
    # the checkpoint is refused rather than accepted unchecked.
    sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
    # Sealed by default (2026-09-06): these stored bytes are in the framed sealed
    # format, so `GET /artifacts/{id}/download` unseals them before handing them to
    # their owner, and a shredded key makes them permanently unreadable like every
    # other copy.
    #
    # Set from the BYTES at upload time — the control plane sniffs the format's magic
    # — and never from anything the uploader said about them. An uploader that lies
    # about sealed bytes cannot make them plaintext, and one that lies about plaintext
    # cannot get it stored: on a sealed job an artefact that is not sealed is refused
    # at the door, so this column and the bytes it describes cannot drift apart.
    sealed: Mapped[bool] = mapped_column(
        Boolean, default=False, nullable=False, server_default="0"
    )


class RunSample(Base):
    """W5b: the container's OWN resource usage, sampled every few seconds while it
    runs — so a failed run shows its last picture ("RAM climbed to the 128 MB limit
    2s before the kernel killed it"). Extends the PR #5 node-level sampling to
    per-run and STORES it.

    `ts` is the agent's wall clock (unix seconds) at sample time. It is the dedup
    key: `UNIQUE(run_id, attempt, ts)` makes a re-sent batch (after a lost 200) an
    idempotent no-op — the same idea as `run_logs`' UNIQUE, so the agent can retry.
    """

    __tablename__ = "run_samples"
    __table_args__ = (
        UniqueConstraint("run_id", "attempt", "ts", name="uq_run_samples_run_attempt_ts"),
        Index("ix_run_samples_run_id", "run_id"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    run_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("runs.id"), nullable=False
    )
    attempt: Mapped[int] = mapped_column(Integer, nullable=False)
    ts: Mapped[float] = mapped_column(Float, nullable=False)
    cpu_pct: Mapped[float | None] = mapped_column(Float, nullable=True)
    mem_used_mb: Mapped[float | None] = mapped_column(Float, nullable=True)
    mem_limit_mb: Mapped[float | None] = mapped_column(Float, nullable=True)
    # 2026-09-04: how many MB this run had written to its temporary disk on the worker
    # at sample time. The agent measures its own per-run directory, so the same reading
    # that already shows RAM climbing to a limit now shows disk doing it too — and it
    # is the reading the scratch cap is enforced from. NULL from an agent that predates
    # the field.
    scratch_used_mb: Mapped[float | None] = mapped_column(Float, nullable=True)


class JobKey(Base):
    """W6b: the AES-GCM key that opens ONE private job's sealed input.

    A SEPARATE TABLE on purpose, for two reasons a juror can see immediately:

      1. "The key is stored apart from the data it opens" is then literally visible
         in the database tour — the ciphertext is in MinIO, the key is one small
         row here, and neither location holds both halves.
      2. Crypto-shred is one DELETE. Remove this row and every sealed copy of that
         job's input — in MinIO, on any worker disk, in any backup — becomes
         permanently unreadable, because nothing anywhere can reconstruct the key.

    The key is released ONLY through the fenced ticket path (api/keys.py): never in
    a job read, never in an env var, never to an agent.
    """

    __tablename__ = "job_keys"

    job_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("jobs.id"), primary_key=True
    )
    key_b64: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_now, nullable=False
    )


class KeyTicket(Base):
    """W6b: a single-use, short-lived pass that a container redeems ONCE for its
    job's key.

    Why a ticket instead of handing the key to the agent: the key would then sit in
    the container's environment, where `docker inspect` (and anything reading the
    process list) shows it forever. A ticket is issued under the node's *fenced*
    authority (only the node holding the run's current attempt can get one), lives
    ~120 seconds, and dies the instant it is used — so `docker inspect` on a running
    private container shows only a ticket that is already dead.

    `redeemed_at` is the one-shot latch: NULL means unused, and the redeem stamps it
    inside the same transaction that returns the key, so a second call gets 410.
    """

    __tablename__ = "key_tickets"
    __table_args__ = (Index("ix_key_tickets_run_id", "run_id"),)

    ticket: Mapped[str] = mapped_column(String(64), primary_key=True)
    run_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("runs.id"), nullable=False
    )
    # The attempt the ticket was issued for. Checked again at redeem time, so a
    # ticket issued to a node that has since been fenced cannot open anything.
    attempt: Mapped[int] = mapped_column(Integer, nullable=False)
    expires_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    redeemed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_now, nullable=False
    )


class NodeEvent(Base):
    """W5b node postmortem: one row per diagnosed node event — a clean goodbye, or a
    comeback interview that classified an outage (network partition, sleep, agent
    crash, clean shutdown, power loss). `cause` is the taxonomy value; `evidence`
    holds the supporting facts (ranges, failed-delivery timestamps, the human
    "likely …" detail). Nodes are still NEVER reaped — this is history, not liveness.
    """

    __tablename__ = "node_events"
    __table_args__ = (Index("ix_node_events_node_id", "node_id"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    node_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("nodes.id"), nullable=False
    )
    ts: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_now, nullable=False
    )
    event: Mapped[str] = mapped_column(String(32), nullable=False)
    cause: Mapped[str | None] = mapped_column(String(32), nullable=True)
    evidence: Mapped[dict | None] = mapped_column(JSON, nullable=True)
