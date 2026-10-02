"""Pydantic request/response models — shapes FROZEN in protocol.md §9–§10.

W1 implemented register, heartbeat, GET /nodes. W2 fills the frozen-but-empty
`assignments` shape and adds the job-submission + run-status shapes (protocol.md
§9 run status, §10 jobs). No shape here changed — W2 only started *using* them.
"""

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field, field_validator

from .models import NodeStatus

# The advisor's pasted-script cap (2026-09-05), in BYTES. Defined here because the
# 422 is raised here; `checkpoint_advisor.MAX_SOURCE_BYTES` is the same number as a
# backstop deeper in, and the test asserts the door rather than the backstop.
MAX_SOURCE_TEXT_BYTES = 65_536


# --- POST /agent/register ---


class NodeSpecs(BaseModel):
    cpu_cores: int
    has_gpu: bool = False
    ram_mb: int
    # capacity defaults to cpu_cores server-side if the agent omits it.
    capacity: int | None = None
    agent_version: str | None = None
    # Rich machine identity — free-form on purpose: the pool is heterogeneous,
    # so agents report what they can detect and omit what they can't.
    # Recommended keys (protocol.md §2): cpu_name, cpu_cores_physical, gpu_name,
    # ram_mhz, machine_model, os, python_version, docker_version, disk_total_gb.
    # Optional + additive (2026-07-06): old agents that omit it stay valid.
    hw_specs: dict | None = None


class RegisterRequest(BaseModel):
    name: str
    specs: NodeSpecs


class RegisterResponse(BaseModel):
    node_id: str
    token: str  # returned ONCE, in plaintext; server stores only its hash.


# --- POST /agent/heartbeat ---


class RunningItem(BaseModel):
    run_id: str
    attempt: int
    state: str
    # W5b live progress: `progress` = epoch/total (0..1); `metrics` = the latest
    # ##PROGRESS JSON the workload printed. Both optional — old agents omit them.
    progress: float | None = None
    metrics: dict | None = None


class HeartbeatRequest(BaseModel):
    node_id: str
    status: NodeStatus  # idle|busy — invalid value -> 422
    running: list[RunningItem] = Field(default_factory=list)
    # Latest usage sample (cpu_pct, ram_pct, disk_pct, gpu_pct where available).
    # Optional + additive (2026-07-06): omitted -> the stored sample is kept.
    usage: dict | None = None
    # W5b black box (2026-07-15): the last battery reading. Optional — desktops and
    # old agents omit it; present -> the node's last-picture is refreshed.
    battery_pct: float | None = None
    battery_charging: bool | None = None
    # W5b comeback interview: the facts a returning agent hands over after a gap
    # (failed_deliveries / slept_ranges / reboot / new_agent_session / dirty_shutdown).
    # Present only on the first contact after an outage; classified into a node_event.
    interview: dict | None = None


class Assignment(BaseModel):
    run_id: str
    attempt: int
    image: str
    entrypoint: list[str]
    env: dict[str, str] = Field(default_factory=dict)
    # W5b: an optional container memory cap (MB) from the job's resource_reqs. The
    # agent applies it as --memory (+ --memory-swap), so an over-budget run is
    # OOM-killed by the kernel — provable RAM overload. Null -> no cap (old shape).
    mem_limit_mb: int | None = None
    # W6b: this run carries a SEALED input. The agent must stage it (download the
    # blob, mount tmpfs, fetch a ticket) instead of starting the container directly.
    # False for every ordinary job, so an old agent sees the shape it always saw —
    # and the claim query guarantees an old agent is never sent a private run anyway.
    private: bool = False
    job_id: str | None = None      # which job's input file to download
    input_filename: str | None = None  # the original name, for the container's env
    # 2026-09-05: this ORDINARY run carries a dataset file the agent must download and
    # mount before the container starts. Never true at the same time as `private` —
    # a private run stages its input by the sealed path instead. False for every job
    # submitted without a file, so an agent that predates the field sees the shape it
    # always saw, and the claim query is what stops a job WITH a file from reaching
    # one (scheduler.MIN_INPUT_AGENT_VERSION).
    has_input: bool = False
    # 2026-09-06: this run's data is sealed. The agent fetches a one-shot key ticket
    # for the container -- whether or not there is an input file, because outputs and
    # checkpoints are sealed too -- and mounts any sealed input READ-ONLY. Never true
    # at the same time as `private`: that is the older shape, staged the older way.
    # False for every job submitted before this date, so an agent that predates the
    # field sees the shape it always saw, and the claim query is what stops a sealed
    # job reaching such an agent at all (scheduler.MIN_SEALED_AGENT_VERSION).
    sealed: bool = False
    # 2026-09-04: how many MB this run may write to temporary disk on the worker.
    # From the job's `resource_reqs.scratch_mb`, or the owner's tier default. Null ->
    # no cap, which is what an unowned job gets and what every job got before this
    # date; an agent that predates the field ignores it and enforces nothing, and the
    # claim query is what stops an EXPLICIT ask from reaching such an agent.
    scratch_mb: int | None = None


class Command(BaseModel):
    type: str
    run_id: str


class HeartbeatResponse(BaseModel):
    # W1: both ALWAYS empty. The scheduler that fills `assignments` lands W2/W4.
    assignments: list[Assignment] = Field(default_factory=list)
    commands: list[Command] = Field(default_factory=list)


# --- GET /nodes ---


class NodeOut(BaseModel):
    node_id: str
    name: str
    online: bool          # DERIVED at read time (not stored)
    reported_status: str  # stored self-reported idle|busy
    cpu_cores: int
    has_gpu: bool
    ram_mb: int
    capacity: int
    last_heartbeat: datetime | None
    agent_version: str | None = None
    hw_specs: dict | None = None  # rich identity (set at registration)
    usage: dict | None = None     # latest heartbeat usage sample
    battery_pct: float | None = None       # W5b black box — last picture
    battery_charging: bool | None = None
    # W6b: may this machine receive PRIVATE jobs? Admin-set, never self-declared.
    trusted: bool = False
    # 2026-09-04: free space on the filesystem holding this agent's working root, as
    # of its last heartbeat. Null from an agent that does not report it. Shown on the
    # node card, and read by the claim query as the only truthful placement signal for
    # a run's temporary-disk need.
    disk_free_mb: int | None = None
    # 2026-09-07 (walk 1, row 65): is this machine's agent too old for the jobs being
    # submitted today? Every job since 2026-09-06 is sealed, and a sealed run is never
    # offered to an agent below `min_agent_version` — so an out-of-date machine sits
    # online, idle and offered nothing, which the pool used to show as an ordinary
    # healthy node. Additive; older readers ignore both.
    agent_outdated: bool = False
    min_agent_version: str | None = None


# --- POST /agent/runs/{run_id}/status (W2) ---


class RunStatusUpdate(BaseModel):
    """Agent reports a run's progress (protocol.md §9). `attempt` is the fencing
    token: the control plane rejects (409) any post whose attempt is not the run's
    current attempt — that is how a presumed-dead node's late result is dropped."""

    attempt: int
    state: Literal["RUNNING", "SUCCEEDED", "FAILED"]  # invalid value -> 422
    exit_code: int | None = None
    # W5b: why a run FAILED, as classified by the agent from hard facts (OOMKilled,
    # GPU/image errors, exit signals). Optional — a RUNNING/SUCCEEDED post omits it,
    # and an old agent that reports FAILED without it gets a server-side exit fallback.
    failure_reason: str | None = None
    failure_detail: str | None = None
    # 2026-09-07 (walk 1, row 25): the LAST progress marker the container printed,
    # carried on the terminal post so the stored progress reaches the finish rather
    # than stopping one heartbeat short of it. Optional; an older agent omits both
    # and the last heartbeat's values stand, exactly as before.
    progress: float | None = None
    metrics: dict | None = None


class RunStatusResponse(BaseModel):
    accepted: bool
    run_status: str


# --- POST /jobs + reads (W2; JWT gating arrives W6) ---


class ResourceReqs(BaseModel):
    min_ram_mb: int | None = None
    needs_gpu: bool = False
    # W5b: an optional per-run memory cap (MB). Travels to the agent on the
    # assignment (see Assignment.mem_limit_mb) and becomes the container's --memory.
    mem_limit_mb: int | None = None
    # 2026-09-04: how much temporary disk this run needs on a worker, in MB. At most
    # the submitter's tier scratch cap (a bigger ask is a 422, not a silent trim);
    # omitted means "give me the tier default". It does two jobs: the agent stops the
    # run at it, and the scheduler will not place the run on a machine with less free
    # disk than it.
    scratch_mb: int | None = None


class JobCreate(BaseModel):
    name: str
    image: str
    entrypoint: list[str] = Field(default_factory=list)
    env: dict[str, str] = Field(default_factory=dict)
    resource_reqs: ResourceReqs = Field(default_factory=ResourceReqs)
    # null -> `replicas` runs scheduled to any eligible node;
    # set  -> one run per listed node (protocol.md §4).
    target_node_ids: list[str] | None = None
    replicas: int = Field(default=1, ge=1)
    # W6b: a private job carries a SEALED input file, so it cannot be submitted as
    # plain JSON — it must come through POST /jobs/private (multipart). Setting this
    # true on the JSON route is a 422 with that message, never a silently non-private
    # job (failing loudly is the only honest option when privacy was asked for).
    private: bool = False
    # Sealed by default (2026-09-06): the ONE choice the old private route bundled
    # that is genuinely a choice. Sealing is now the floor for every job and has no
    # field, because it costs the user nothing; restricting a job to machines an
    # admin trusts costs them the rest of the pool, so it is theirs to ask for.
    # Accepted on every submit door, off by default, and it changes placement and
    # nothing else.
    trusted_only: bool = False
    # Checkpoint-use advisor (2026-09-05). The training script, pasted by the user so
    # the platform has something it can actually read — it receives an IMAGE, and
    # opening that to guess which file is the training script would be a guess wearing
    # a check's clothes. Optional: absent means the advice records `not_checked`,
    # which is a different statement from "we looked and found no checkpointing".
    #
    # The cap is measured in BYTES, not characters. A 64 K-character limit on a script
    # full of non-ASCII would let through a payload two or three times the size the
    # column and the LLM request were sized for, and the difference only shows up on
    # the input nobody tested with.
    source_text: str | None = None

    @field_validator("source_text")
    @classmethod
    def _source_text_fits(cls, v: str | None) -> str | None:
        if v is not None and len(v.encode("utf-8")) > MAX_SOURCE_TEXT_BYTES:
            raise ValueError(
                f"source_text is larger than {MAX_SOURCE_TEXT_BYTES} bytes"
            )
        return v


class JobCreateResponse(BaseModel):
    job_id: str
    run_ids: list[str]
    status: str


class RunOut(BaseModel):
    run_id: str
    job_id: str
    node_id: str | None
    status: str
    attempt: int
    exit_code: int | None
    started_at: datetime | None
    finished_at: datetime | None
    # W5b diagnostics (all nullable): why a dead run died + live progress.
    failure_reason: str | None = None
    failure_detail: str | None = None
    progress: float | None = None
    metrics_last: dict | None = None
    # W5c: set once a run has been escalated after a proven node-capacity OOM. The UI
    # shows "retried on a stronger node (needs > learned_min_ram_mb MB)".
    learned_min_ram_mb: int | None = None
    escalation_count: int = 0
    # 2026-09-06: why a PENDING run is still waiting, when the reason is that the
    # machines it was aimed at are not here — "waiting for lab-pc-03 (silent for 2h)".
    # Null for every other run, which is almost all of them.
    #
    # It is a SENTENCE and not a code, because it is the whole answer: a run aimed at
    # an absent machine waits rather than failing (the machine may come back, exactly
    # as W6b's trust tier waits), and what was missing was never a decision — it was
    # anyone being able to see what it was waiting for.
    waiting_for: str | None = None
    # 2026-09-07 (walk 1, row 64): when a user asked this run to stop, or null. Shown
    # as "cancel requested" until the worker confirms or the lease expires.
    cancel_requested_at: datetime | None = None


class CancelResponse(BaseModel):
    """What `POST /jobs/{id}/cancel` did, counted, so the caller can say it."""

    job_id: str
    cancelled_now: int        # PENDING runs ended in this request
    cancel_requested: int     # ASSIGNED/RUNNING runs whose worker is told next heartbeat
    already_finished: int     # terminal runs, untouched
    detail: str


class JobOut(BaseModel):
    job_id: str
    name: str
    image: str
    status: str
    replicas: int
    target_node_ids: list[str] | None
    created_at: datetime
    # W6b: is this job's input sealed, and under what original filename. The UI shows
    # a lock + the "outputs via logs" note. The KEY is never in any read shape.
    private: bool = False
    input_filename: str | None = None
    # 2026-09-07 (walk 1, row 35): the SEALED size of the input this job carries, in
    # bytes — the number it is charged against its owner's retained storage. Null
    # when the job carries no file. Additive; an older reader ignores it.
    input_size_bytes: int | None = None
    # Sealed by default (2026-09-06). `sealed` is a statement about this job's DATA —
    # its own key, its input sealed at submit, its outputs and checkpoints sealed
    # inside the container. `trusted_only` is a statement about WHERE it may run.
    # They were one flag until today and are two now, because they answer two
    # different questions. Neither read shape returns a key or an object key.
    sealed: bool = False
    trusted_only: bool = False
    # Checkpoint-use advisor (2026-09-05): {verdict, arm_a, arm_b, checked_at}, or
    # null on every job submitted before this feature existed. The pasted script
    # itself is deliberately NOT in this shape — a read model returns the verdict and
    # the evidence for it, never the user's source code back over the wire.
    checkpoint_advice: dict | None = None


# --- Logs: POST /agent/runs/{id}/logs + reads (W3; protocol.md §9–§10) ---


class LogChunkUpload(BaseModel):
    """The agent posts a batch of container output. `attempt` is the fencing token
    (a stale or wrong-node post is rejected 409, like a status post). `seq` is a
    per-(run, attempt) counter; `UNIQUE(run_id, attempt, seq)` makes a resent chunk
    a harmless no-op, so the agent can retry a failed post with the same seq."""

    attempt: int
    seq: int = Field(ge=0)
    chunk: str


class LogAck(BaseModel):
    accepted: bool
    deduped: bool = False  # true when this (run_id, attempt, seq) was already stored


class LogChunkOut(BaseModel):
    run_id: str
    attempt: int
    seq: int
    chunk: str
    ts: datetime


# --- W5b: per-run resource samples (POST /agent/runs/{id}/samples + reads) ---


class ResourceSample(BaseModel):
    """One reading of the container's own usage. `ts` (agent wall clock, unix
    seconds) is the dedup key: a re-sent batch is idempotent, like a log chunk."""

    ts: float
    cpu_pct: float | None = None
    mem_used_mb: float | None = None
    mem_limit_mb: float | None = None
    # 2026-09-04: MB written to this run's temporary disk on the worker at sample
    # time. Optional and additive — an older agent omits it, and the stored row is
    # then NULL rather than zero, because "did not report" and "wrote nothing" are
    # different facts.
    scratch_used_mb: float | None = None


class SampleBatch(BaseModel):
    """A batch of samples for one run+attempt (fenced like logs/status)."""

    attempt: int
    samples: list[ResourceSample] = Field(default_factory=list)


class SampleAck(BaseModel):
    accepted: bool
    stored: int = 0  # rows newly stored this call (dupes are skipped)


class RunSampleOut(BaseModel):
    ts: float
    cpu_pct: float | None = None
    mem_used_mb: float | None = None
    mem_limit_mb: float | None = None
    scratch_used_mb: float | None = None  # 2026-09-04, nullable like the rest


# --- W6: auth (POST /auth/login) ---


class LoginRequest(BaseModel):
    username: str
    password: str


class LoginResponse(BaseModel):
    token: str
    expires_at: datetime


# --- W6: artifacts (GET /runs/{id}/artifacts, download) ---


class ArtifactOut(BaseModel):
    artifact_id: str
    run_id: str
    attempt: int
    filename: str        # derived from object_key (the last path segment)
    object_key: str
    size: int | None = None
    content_type: str | None = None
    # 2026-08-13: 'result' (a finished output) or 'checkpoint' (state a later attempt
    # may resume from). Defaulted, so nothing that built an ArtifactOut before this
    # date has to change. The results listing only ever returns 'result'.
    kind: str = "result"


# --- W6b: private job inputs (sealed delivery, tickets, trust tier) ---


class TrustUpdate(BaseModel):
    """PATCH /nodes/{id}/trusted — the ADMIN's judgment that this machine may run
    private jobs. There is deliberately no way for a node to send this about itself."""

    trusted: bool


class KeyTicketResponse(BaseModel):
    """What the agent gets from POST /agent/runs/{id}/key-ticket. Note what is NOT
    here: the key. The agent only ever handles a ticket."""

    ticket: str
    expires_at: datetime
    key_url: str  # where the container redeems it (the control plane's own address)


class KeyRedeemRequest(BaseModel):
    """POST /container/key — the ticket IS the credential. The container holds no
    node token and no JWT: it is untrusted code, so it gets the smallest possible
    one-shot pass instead of a durable key of any kind."""

    ticket: str


class KeyRedeemResponse(BaseModel):
    key_b64: str


# --- W5b: node black box + comeback interview (POST /agent/goodbye, reads) ---


class GoodbyeRequest(BaseModel):
    """The agent's best-effort "going offline" message on a clean stop."""

    reason: str = "shutdown"


class NodeEventOut(BaseModel):
    ts: datetime
    event: str
    cause: str | None = None
    evidence: dict | None = None


# --- Storage quota policy (2026-09-04): tiers, roles, acceptance ------------


class TierOut(BaseModel):
    """A tier as the user sees it — the two numbers, in MB, plus the sentence that
    says what they mean. MB rather than bytes because that is the unit every other
    limit in this system is stated in (`mem_limit_mb`, `MAX_ARTIFACT_MB`), and a
    policy stated in a different unit from the limits beside it invites the wrong
    comparison."""

    tier: str
    retained_cap_mb: float
    scratch_cap_mb: float
    description: str | None = None


class MeOut(BaseModel):
    """`GET /me` — where the caller stands, in one shape.

    This exists because a cap the user cannot see is a trap: the platform would
    refuse work for a reason nothing on the screen had ever mentioned. The submit
    form reads this before it lets anything be submitted."""

    username: str
    is_admin: bool
    tier: str
    retained_cap_mb: float
    scratch_cap_mb: float
    retained_used_mb: float
    limits_accepted: bool
    limits_accepted_at: datetime | None = None


class UserCreate(BaseModel):
    """`POST /users` (admin only). A created user starts with their tier's numbers
    and with acceptance NOT given — they agree for themselves, which is the whole
    point of asking."""

    username: str
    password: str
    tier: str = "standard"


class UserOut(BaseModel):
    user_id: str
    username: str
    is_admin: bool
    tier: str
    limits_accepted: bool


class TierAssignment(BaseModel):
    """`PATCH /users/{id}/tier` (admin only) — move one user to another tier."""

    tier: str
