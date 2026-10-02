"""Pull-time assignment — the scheduler (protocol.md §4).

There is no background scheduler loop. Assignment happens INSIDE the heartbeat:
when an agent checks in with spare capacity, the control plane, in one DB
transaction, claims a `PENDING` run for it and hands it back in the response.

Two properties this file is responsible for — both are graded (NFR-3):

1. **No double-assignment under concurrency.** Two agents heart-beating at the
   same instant must not grab the same run. We use
   `SELECT … FOR UPDATE SKIP LOCKED`: the first transaction locks the row; the
   second *skips* it (does not block, does not see it) and moves to the next.
   This is Postgres-specific — it is the reason DB-as-queue works without a
   broker, and the reason the concurrency tests must run on Postgres, not SQLite.

2. **The fencing token starts here.** On assignment we increment `run.attempt`
   (0 → 1 on the first dispatch). Every later message the agent sends about this
   run must carry that same number; a stale number is rejected (409). Threading
   the token from the *first* assignment — not only on re-dispatch — is what
   closes the per-run no-duplicate hole before the W5 reaper exists.
"""

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from sqlalchemy import case, func, or_, select, true
from sqlalchemy.ext.asyncio import AsyncSession

from .config import get_settings
from .models import Job, JobStatus, Node, Run, RunStatus
from .schemas import Assignment

log = logging.getLogger("scheduler")

_TERMINAL = (RunStatus.SUCCEEDED, RunStatus.FAILED)

# W6b: the first agent version that can stage a private run (download the sealed
# blob, mount tmpfs, pass a ticket instead of a key). An older agent is never
# offered a private run — it would receive a job it physically cannot set up.
MIN_PRIVATE_AGENT_VERSION = (0, 8, 0)

# 2026-09-04: the first agent version that enforces a per-run temporary-disk cap and
# reports its own free disk. The guard is the same shape as the private one above and
# it is armed for the same reason — an agent is never handed work it physically
# cannot honour — but it is deliberately NARROWER. It applies only to a job whose
# submitter asked for a scratch size EXPLICITLY, because that ask is a requirement:
# they said how much disk the run needs, and an agent that cannot stop the run at it
# is not running the job they described. A job that left the field empty carries its
# tier's ceiling as a POLICY, and an older agent may take it and enforce nothing —
# which is exactly what every agent did before this date. That case is a stated limit
# rather than a hidden one, and the platform records `agent_version` on every node, so
# which machines were enforcing is a matter of record and not of memory.
MIN_SCRATCH_AGENT_VERSION = (0, 10, 0)

# 2026-09-05: the first agent version that can stage an ORDINARY job's dataset file
# (download it, mount it read-only, point the container at it). An older agent would
# start the container with no file and the workload would fail looking for one — a
# silent wrong behaviour, which is exactly what a version guard exists to prevent. The
# same shape as the two guards above, and it fails CLOSED through `_version_tuple`.
MIN_INPUT_AGENT_VERSION = (0, 11, 0)

# 2026-09-06: the first agent version that can stage a SEALED run — fetch a one-shot
# key ticket for the container whether or not there is an input file, mount a sealed
# input read-only, and hand a workload the reader that opens it a piece at a time. An
# older agent would start the container with no ticket, and the workload would stop on
# its first read with no key to open anything: a run that fails for a reason nobody
# outside this file could name. So a sealed job is INVISIBLE to it.
#
# Every job created from 2026-09-06 is sealed, so the practical effect on a pool with
# an out-of-date machine in it is that the machine is offered nothing new. That is the
# intended answer and not a side effect worth softening — an agent that cannot keep a
# job's data sealed should not be running that job — and it is also the shape the
# proof reads: an agent at the previous version is offered zero sealed runs while one
# at this version is offered one.
MIN_SEALED_AGENT_VERSION = (0, 12, 0)

# W5c failure-aware rescheduling (FR-12). Labels: OOM_KILLED must match the agent's
# hard-fact label (agent/classify.py) — that is the only signal we escalate on.
# INSUFFICIENT_POOL is the server's own give-up reason.
OOM_KILLED = "OOM_KILLED"
INSUFFICIENT_POOL = "INSUFFICIENT_POOL"
# A run may be escalated to a stronger node at most this many times before we stop.
# Bounded because each escalation demands a *strictly* stronger machine, so a job too
# big for the whole pool fails in a few steps, not an endless loop.
MAX_ESCALATIONS = 3

# How many pending runs one page of the survey pulls (2026-09-06).
#
# This replaces `_CANDIDATE_MULTIPLIER = 4`, which was not a page size but the WHOLE
# search: the claim query fetched `spare * 4` of the oldest pending runs, once, and
# whatever was ineligible among them was lost capacity. Its comment named the danger
# exactly — "so that rows ineligible for this node don't starve a node that could
# otherwise be filled" — and a constant cannot deliver that, because nothing bounds
# how many unclaimable runs sit at the head of a queue. Four was enough to starve a
# one-core machine completely.
#
# As a PAGE size the number is only about how many rows a round trip fetches, and
# nothing depends on it being big enough: `_survey` keeps turning pages.
#
# What it costs, measured on 2026-09-06 against a queue holding nothing this node
# could take (docs/evidence/walk_cost_2026-09-06.txt): 70 pending runs -> a median
# heartbeat of 17.6 ms, 170 -> 29.7 ms, 370 -> 30.0 ms, 530 -> 36.8 ms, 730 ->
# 48.5 ms, twenty heartbeats a depth with min and max beside each. The cost is
# dominated by the NUMBER OF PAGES rather than the number of rows, so a bigger page
# would flatten the tail — the number is recorded here rather than tuned on a hunch,
# so whoever needs that has the measurement to act on.
#
# The earlier version of this comment cited PART G of a capture that no longer exists
# under the name it gave (`claim_starvation_2026-09-06.txt`) and quoted medians of
# five, one of which read faster at 164 pending runs than at 64. The file above is
# the same question asked at n=20.
_SCAN_PAGE = 64

# The bound on how much queue one heartbeat will walk before giving up on this node.
# A cost bound, not a correctness one: below it starvation is impossible, and above it
# the remainder waits for the next heartbeat. Deliberately far past anything this
# project runs — a queue here is tens of runs — and the scan says so in the log when
# it is ever reached, because a silent bound is how the defect it replaced survived
# for three months.
MAX_CLAIM_SCAN = 5000


def scratch_ask_mb(job: Job) -> int | None:
    """What the submitter EXPLICITLY asked for, in MB, or None if they said nothing.

    This is the number that constrains PLACEMENT. It is deliberately not the same as
    `scratch_limit_mb` below, and the difference is the whole design:

      * an explicit ask is a REQUIREMENT — "this run needs this much disk" — so a
        machine without that much free space cannot run it, and an agent too old to
        enforce it must not be offered it;
      * a tier ceiling is a LIMIT — "you may not exceed this" — which says nothing
        about what any machine must provide.

    Treating a tier ceiling as a requirement would deadlock the pool. When this was
    decided the default tier allowed 500 GB of scratch, no lab machine has 500 GB
    free, and every ordinary run would have waited PENDING for a machine that does
    not exist. The tiers were re-sized on 2026-09-05 (standard is 50 GB scratch,
    200 GB retained; migration 20260905_a2b3c4d5e6f7), which makes the failure
    smaller and not different in kind."""
    ask = (job.resource_reqs or {}).get("scratch_mb")
    return int(ask) if ask else None


def scratch_limit_mb(job: Job) -> int | None:
    """The cap the AGENT stops this run at: the explicit ask if there was one, else
    the owner's tier ceiling as it stood when the job was submitted. None means no
    cap at all, which is what an unowned job gets and what every job got before
    2026-09-04."""
    reqs = job.resource_reqs or {}
    ask = reqs.get("scratch_mb")
    if ask:
        return int(ask)
    ceiling = reqs.get("scratch_cap_mb")
    return int(ceiling) if ceiling else None


def _eligible(job: Job, node) -> bool:
    """Can `node` run `job`? (protocol.md §4 step 1.)

    Static matching only (target list, GPU, RAM, and from 2026-09-04 free disk). For
    the heart-beating node online is implicit — it is talking to us right now. The one
    other caller, `_other_eligible_node_free`, asks about a machine that is NOT talking
    to us, so it checks liveness itself before calling this. The per-job "one run per
    selected node" pinning is stateful and lives in assign_runs (W4)."""
    targets = job.target_node_ids
    if targets and node.id not in targets:
        return False  # job named specific nodes and this isn't one of them
    reqs = job.resource_reqs or {}
    if reqs.get("needs_gpu") and not node.has_gpu:
        return False
    min_ram = reqs.get("min_ram_mb")
    if min_ram is not None and node.ram_mb < min_ram:
        return False
    # 2026-09-04 — temporary disk. One more condition in the SAME eligibility test
    # that already answers GPU and RAM, which is where protocol.md §4 step 1 says a
    # resource requirement is answered. No new stage, no scoring, no comparison
    # between machines: this is a FILTER, so among eligible machines the first to ask
    # still wins and the untargeted spread preference is untouched.
    ask = scratch_ask_mb(job)
    if ask is not None:
        # NULL free disk is UNKNOWN, and unknown does not exclude. An agent that
        # reports nothing is either older than this feature — in which case the
        # version guard below already excludes it — or briefly unable to read its own
        # disk, and refusing to place work on a machine because one reading failed
        # would be a worse answer than placing it.
        free = getattr(node, "disk_free_mb", None)
        if free is not None and free < ask:
            return False
        if _version_tuple(node.agent_version) < MIN_SCRATCH_AGENT_VERSION:
            return False
    # 2026-09-05 — a dataset file. An ordinary job carrying one needs an agent that
    # knows to fetch and mount it. A PRIVATE job is excluded here because its input
    # travels the sealed path, which `may_run_private` already guards; checking both
    # would refuse a private run on a trusted node for the wrong reason.
    if job.input_object_key and not (job.private or job.sealed):
        if _version_tuple(node.agent_version) < MIN_INPUT_AGENT_VERSION:
            return False
    # 2026-09-06 — a sealed job. Same shape as the three guards above and it fails
    # closed the same way, through `_version_tuple`. The TRUST condition is not here:
    # it is in the claim query, because trust makes a run invisible rather than
    # ineligible (see the note there).
    if job.sealed and _version_tuple(node.agent_version) < MIN_SEALED_AGENT_VERSION:
        return False
    return True


def _version_tuple(version: str | None) -> tuple[int, ...]:
    """"0.8.0" -> (0, 8, 0). Anything unparseable (None, "dev", an old free-form
    string) sorts as (0,) — i.e. too old — so the guard fails CLOSED: a node whose
    version we cannot read never receives private work."""
    if not version:
        return (0,)
    parts: list[int] = []
    for piece in str(version).split("."):
        digits = "".join(c for c in piece if c.isdigit())
        if not digits:
            break
        parts.append(int(digits))
    return tuple(parts) if parts else (0,)


def may_run_private(node) -> bool:
    """May this machine run a job of the OLD private shape (W6b)?

      * `trusted` — an ADMIN marked this machine as one we are willing to open
        private data on. A node can never set this about itself.
      * agent version — the agent is new enough to actually stage such a run.

    Both are plain facts about the node object we already hold, so the claim query
    below needs no join for them: they collapse to one boolean, and that boolean
    switches one WHERE condition on or off.

    Kept as it was for the rows that still carry `jobs.private`. New jobs express the
    same wish through `trusted_only`, which `may_run_trusted_only` answers."""
    return bool(node.trusted) and _version_tuple(node.agent_version) >= MIN_PRIVATE_AGENT_VERSION


def may_run_trusted_only(node) -> bool:
    """May this machine run a job whose owner asked for trusted machines only
    (2026-09-06)?

    One condition, and deliberately only one: an admin marked the machine trusted.
    The version part of `may_run_private` is not repeated here because it is not the
    same question — how new the agent is decides whether it can STAGE the run, and
    `_eligible` already answers that through `MIN_SEALED_AGENT_VERSION`. Trust decides
    whether we are willing to place the work there at all. Two questions, asked in the
    two places that own them."""
    return bool(node.trusted)


async def _holds_sibling_run(session: AsyncSession, run: Run, node_id: str) -> bool:
    """Does this node already hold ANOTHER run of the same job?

    W4 pinning rule for targeted jobs: "one job targeting three nodes" means one
    run PER node — the user picked those machines to run side by side (e.g. to
    compare hardware). Without this check, the first node to heartbeat with
    spare capacity could legally vacuum up every run of the job, deleting the
    parallelism the user asked for.

    Any status counts (including terminal): a node that already ran its share of
    a targeted job must not receive a requeued sibling either — two results from
    the same machine is not what "run on each of these nodes" means."""
    stmt = (
        select(func.count())
        .select_from(Run)
        .where(
            Run.job_id == run.job_id,
            Run.node_id == node_id,
            Run.id != run.id,
        )
    )
    return (await session.execute(stmt)).scalar_one() > 0


async def _other_eligible_node_free(
    session: AsyncSession, job: Job, run: Run, node, now: datetime
) -> bool:
    """Is there ANOTHER machine that is online, eligible for this job and not
    full — i.e. somebody else who could take this replica instead of us?

    This is the escape hatch on the untargeted spread preference. Untargeted replicas *prefer* a machine that does not already
    hold a sibling, but the preference must never leave work unstarted: if
    nobody else can take it, the node in front of us takes it.

    "Online" is the same read-time rule the node list uses (protocol.md §2):
    a heartbeat inside `node_timeout_s`. Nothing here writes anything, and no
    machine is scored against another — the question is only "does an
    alternative exist", never "which alternative is best" (placement stays
    first-to-ask).

    **A machine that already holds a run of this job is not an alternative
    (2026-09-06).** It was counted as one until this date, and that is what made
    the preference able to deadlock: with ten replicas and three machines of
    capacity four, each machine takes one, and then every machine defers the
    fourth replica to the others — each of which is deferring it straight back,
    because each is online, eligible and not full. Nobody is wrong and the replica
    never runs. Handing it to a machine that already holds a sibling is exactly
    what the preference would rather avoid, but the alternative on the table is
    not a cleaner machine: it is no machine at all."""
    cutoff = now - timedelta(seconds=get_settings().node_timeout_s)
    # Machines already holding a run of this job, in one query rather than one per
    # machine. Any status counts, the same rule `_holds_sibling_run` uses.
    holders = set(
        (
            await session.execute(
                select(Run.node_id).where(
                    Run.job_id == job.id, Run.node_id.is_not(None)
                )
            )
        )
        .scalars()
        .all()
    )
    rows = (
        await session.execute(select(Node).where(Node.id != node.id))
    ).scalars().all()
    for other in rows:
        if other.id in holders:
            continue  # a second replica there is the thing we are avoiding here
        last = other.last_heartbeat
        if last is None:
            continue
        if last.tzinfo is None:  # SQLite (tests) hands back naive datetimes
            last = last.replace(tzinfo=timezone.utc)
        if last < cutoff:
            continue  # offline by the same rule GET /nodes uses
        if not _eligible(job, other):
            continue
        if job.private and not may_run_private(other):
            continue
        if job.trusted_only and not may_run_trusted_only(other):
            continue
        if (
            run.learned_min_ram_mb is not None
            and run.learned_min_ram_mb >= other.ram_mb
        ):
            continue  # W5c: this run learned it needs more RAM than that machine
        if other.capacity - await _running_count(session, other.id) > 0:
            return True
    return False


def _hidden_from(node):
    """The claim query's trust condition: which jobs this machine may not even SEE.

    Invisible, not rejected. The run simply stays PENDING and waits, because trust is
    one admin click away — unlike W5c's insufficient pool, where no amount of waiting
    can help, so that one fails fast instead.

    Two flags are named because they are the same wish written in two eras:
    `private` on the rows submitted before 2026-09-06, `trusted_only` on the rows
    submitted since. The migration copied the first into the second, so on old rows
    this is belt and braces and on new ones it is the only rule.

    They are still asked separately because the answers can differ: an untrusted
    machine may see neither, while a TRUSTED machine running an agent too old for the
    old private staging may see `trusted_only` work and not `private` work."""
    blocked = []
    if not may_run_trusted_only(node):
        blocked.append(Job.trusted_only.is_(True))
    if not may_run_private(node):
        blocked.append(Job.private.is_(True))
    if not blocked:
        return true()
    return Run.job_id.notin_(select(Job.id).where(or_(*blocked)))


async def _running_count(session: AsyncSession, node_id: str) -> int:
    """Runs currently held by this node (leased or executing). Authoritative
    capacity comes from DB state, never from the agent's self-report."""
    stmt = (
        select(func.count())
        .select_from(Run)
        .where(
            Run.node_id == node_id,
            Run.status.in_([RunStatus.ASSIGNED, RunStatus.RUNNING]),
        )
    )
    return (await session.execute(stmt)).scalar_one()


async def _candidates(
    session: AsyncSession, node, now: datetime, want: int
) -> tuple[list[tuple[Run, Job]], list[tuple[Run, Job]]]:
    """Walk the PENDING queue oldest-first and collect runs this node may take.

    Reads WITHOUT a lock and claims nothing: `assign_runs` locks what it takes and
    re-checks it under that lock, so a run taken between the walk and the claim is
    simply not there any more — the same answer as never having seen it.

    Three things about the walk:

      * **it keeps turning pages**, which is what makes starvation impossible: a run
        is never hidden by runs ahead of it that this node cannot take, however many
        there are;
      * **it pages by (created_at, id)** rather than by OFFSET, because OFFSET shifts
        under you when a concurrent claim removes a row and the row that slides into
        the gap is never examined. Written as an OR of two comparisons rather than a
        row-value comparison, so it renders on SQLite as well as PostgreSQL;
      * **it joins the job**, so eligibility costs no extra query per row. The old
        code fetched each candidate's job separately, which was fine at four rows and
        would not be at four hundred.

    `want` is BREADTH, and breadth is for concurrency rather than for this node: see
    `assign_runs` on why a claimer needs more candidates than it can use.

    **Returns two lists, and the split is the whole point (2026-09-06).**
    `preferred` is what this node should take. `deferred` is what it may take only
    because something in `preferred` was claimed by somebody faster between this
    unlocked walk and the lock.

    The spread-replicas preference used to STEP PAST a sibling,
    which emptied the candidate list rather than ordering it: on a queue that is one
    job's replicas, every row after the first is a sibling, so the list came back one
    row long — and it was the same row for every claimer. One won it under
    `SKIP LOCKED` and the rest had nothing to fall back on, which is how thirty
    claimers on thirty replicas ended up with seven served
    (`docs/evidence/contention_rerun_2026-09-06.txt`). A preference that empties the
    list is a filter wearing a preference's clothes. It now orders instead: a sibling
    goes to the back, and the back of the queue is still in the queue."""
    preferred: list[tuple[Run, Job]] = []
    deferred: list[tuple[Run, Job]] = []
    # Jobs already represented in `preferred`. Two runs of one job must never both be
    # offered to one node (W4), and nothing is written yet for the DB check in
    # `_holds_sibling_run` to see, so this set is the only thing that knows.
    pinned_here: set[str] = set()
    # `_other_eligible_node_free` is asked once per (job, learned RAM bar) instead of
    # once per row. Nothing is written during the walk, so the answer cannot change
    # underneath us -- and on a queue of one job's replicas the un-memoized version
    # asked it once for every row, each time reading every node.
    alternatives: dict[tuple[str, int | None], bool] = {}
    cursor: tuple[datetime, str] | None = None
    examined = 0

    while len(preferred) + len(deferred) < want and examined < MAX_CLAIM_SCAN:
        stmt = (
            select(Run, Job)
            .join(Job, Job.id == Run.job_id)
            .where(
                Run.status == RunStatus.PENDING,
                # W5c: an escalated run learned it needs strictly MORE RAM than the
                # node it died on. A node that can't clear that bar never even
                # fetches the run. `IS NULL` = the ordinary case (nothing learned).
                (Run.learned_min_ram_mb.is_(None))
                | (Run.learned_min_ram_mb < node.ram_mb),
                # W6b trust tier, widened 2026-09-06 to the `trusted_only` choice:
                # the runs of a job that asked for trusted machines are INVISIBLE to
                # a node that is not one. See `_hidden_from`.
                _hidden_from(node),
            )
            # `id` is the tie-break that makes the paging exact: two runs created in
            # the same microsecond would otherwise be an ambiguous place to resume
            # from, and one of them could be walked twice or not at all.
            .order_by(Run.created_at, Run.id)
            .limit(_SCAN_PAGE)
        )
        if cursor is not None:
            at, last_id = cursor
            stmt = stmt.where(
                (Run.created_at > at)
                | ((Run.created_at == at) & (Run.id > last_id))
            )

        rows = (await session.execute(stmt)).all()
        if not rows:
            break                     # the queue is exhausted, not merely unhelpful

        for run, job in rows:
            cursor = (run.created_at, run.id)
            examined += 1
            if not _eligible(job, node):
                continue              # step past it; it is somebody else's work
            if job.id in pinned_here or await _holds_sibling_run(
                session, run, node.id
            ):
                if job.target_node_ids:
                    continue  # W4: one run per selected node — this node has its share
                # Untargeted: spreading replicas is a PREFERENCE,
                # not pinning. While somebody else could take this sibling it goes to
                # the BACK of our list rather than out of it; if no other eligible
                # machine is online and free, it is an ordinary candidate and shares.
                key = (job.id, run.learned_min_ram_mb)
                if key not in alternatives:
                    alternatives[key] = await _other_eligible_node_free(
                        session, job, run, node, now
                    )
                if alternatives[key]:
                    deferred.append((run, job))
                    if len(preferred) + len(deferred) >= want:
                        break
                    continue
            preferred.append((run, job))
            pinned_here.add(job.id)
            if len(preferred) + len(deferred) >= want:
                break

    if not preferred and not deferred and examined >= MAX_CLAIM_SCAN:
        # Said out loud rather than swallowed. Past this many rows the walk stops and
        # the rest of the queue waits for the next heartbeat — three seconds later,
        # and from the same end of the queue, so a prefix longer than this WOULD hide
        # work again. It is a bound on cost, and at this project's scale (a queue is
        # tens of runs, not thousands) it is never reached. If it ever is, the answer
        # is not a bigger number: it is to stop leaving runs aimed at machines that
        # are never coming back, which §5.5 names as future work.
        log.warning(
            "node %s examined %d pending runs without finding work it can take; "
            "the rest of the queue was not examined this heartbeat",
            node.id, examined,
        )
    return preferred, deferred


async def assign_runs(
    session: AsyncSession, node, now: datetime
) -> list[Assignment]:
    """Claim up to spare-capacity PENDING runs for `node`, mutate them to ASSIGNED
    in the caller's transaction, and return the assignment specs.

    The caller (the heartbeat handler) commits. Mutating-without-committing here
    keeps the whole heartbeat — node update + lease renewal + assignment — in one
    atomic transaction (db.get_session shares one session across the request).

    **What was wrong (2026-09-06).** This fetched a fixed window of the oldest PENDING
    runs and filtered it afterwards — `.order_by(Run.created_at).limit(spare * 4)` —
    so a run this node could not take still occupied a place in the window, and a
    queue whose head was full of such runs hid every run behind them. Four were enough
    to starve a one-core machine completely. The over-fetch existed for exactly that
    danger and its comment said so; the lesson is that a constant is a mitigation and
    not a bound, and that the runs which collect at the head of a queue are precisely
    the ones nothing ever clears (a run targeted at a machine that never comes back
    waits for ever, on purpose, because nothing reaps a node and the machine may
    return).

    **The shape that fixes it, and why it is two steps.** One number was doing two
    jobs, and they pull in opposite directions:

      * how far down the queue this node may LOOK — too small and unclaimable runs
        hide the rest, which was the defect;
      * how many rows this node LOCKS — too many and it holds the queue's head shut
        against every other claimer, because `SKIP LOCKED` makes them step past
        everything it holds. The old `spare * 4` did both at once, and the W7a
        harness had already noticed the second: with thirty claimers on thirty runs,
        "the first seven or eight lock the whole queue and the remaining twenty-two
        come back empty-handed".

    So they are separated. `_candidates` LOOKS as far as it must, without a lock and
    without bound; this function LOCKS at most `spare` rows, `SKIP LOCKED` skipping
    whatever other claimers already hold. A claimer therefore holds the smallest
    footprint it can and still steps past its neighbours, which is why six claimers on
    six runs now take six different runs in one round rather than one
    (tests/test_w2_postgres.py::test_concurrent_claimers_spread_across_the_queue).

    The candidate list is deliberately WIDER than `spare`: it is what a claimer falls
    back on when its first choice is taken by somebody faster. Breadth is for
    concurrency, not for this node's capacity.

    **And breadth has to survive the spread preference (2026-09-06).** Between the
    two fixes above, `_candidates` STEPPED PAST a sibling of a job it had already
    picked, so on a queue that is one job's replicas the list came back one row long
    — the same row for every claimer — and the breadth this paragraph promises did
    not exist where it was needed most. Thirty claimers on thirty replicas served
    seven. The preference now ORDERS the list instead of emptying it: siblings sort
    to the back, `take` keeps this node from actually taking them while another
    machine could, and a claimer whose first choice is gone still has somewhere to
    go. Both cases hold at once — six independent jobs, six machines, six served;
    one job, thirty replicas, thirty machines, thirty served.

    The claim is still one locked query inside the heartbeat's one transaction, which
    is what protocol.md §4 promises. Placement is still decided in one test,
    `_eligible`, where every generation of this project's scheduling has put its
    condition rather than adding a stage: declared (W2/W4) + learned (W5c) + trust
    (W6b) + disk (2026-09-04) + sealed (2026-09-06).

    W7a: the locking strategy — and ONLY the locking strategy — is switchable, so the
    "why not just SELECT and UPDATE?" question can be answered with numbers (E2). All
    three arms walk the same queue and claim with the same WHERE, ordering and limit;
    only the locking clause differs, which is what keeps the comparison fair."""
    spare = node.capacity - await _running_count(session, node.id)
    if spare <= 0:
        return []

    # Breadth is `_SCAN_PAGE` or this node's capacity, whichever is larger. The first
    # is what a claimer falls back on when another takes its first choice; the second
    # stops a machine bigger than one page from being capped at a page's worth of work
    # per heartbeat, which no machine in this pool is, and which would be a silly way
    # to find out otherwise.
    preferred, deferred = await _candidates(session, node, now, max(spare, _SCAN_PAGE))
    if not preferred:
        # Nothing this node should take. `deferred` may hold siblings, but each of
        # them has a machine that would take it without holding a second replica, so
        # taking one here would defeat the spread rather than serve it — and there is
        # no preferred row left for them to stand in for.
        return []
    ordered = preferred + deferred
    by_id = {run.id: job for run, job in ordered}

    # How many rows this node may take: its capacity, but never more than it had
    # PREFERRED candidates. The deferred rows are substitutes, not extra work — a
    # machine with four free slots facing three replicas of one job takes ONE while
    # another machine is free, which is the spread preference doing its job. Without
    # this cap the first machine to ask would take all three.
    take = min(spare, len(preferred))

    # Lock only what we are taking, and re-read the status under that lock. On SQLite
    # (tests) the locking clause is a no-op — which is exactly why locking is tested
    # on Postgres.
    #
    # Ordered by the walk's own preference, not by age: a deferred sibling must sort
    # behind every preferred row, including preferred rows created after it. Inside
    # each group the queue's order is the walk's order, so oldest-first still decides
    # among equals.
    rank = {run.id: i for i, (run, _job) in enumerate(ordered)}
    stmt = (
        select(Run)
        .where(Run.id.in_(list(by_id)), Run.status == RunStatus.PENDING)
        .order_by(case(rank, value=Run.id, else_=len(ordered)), Run.created_at, Run.id)
        .limit(take)
    )
    claim_mode = get_settings().experiment_claim_mode
    if claim_mode == "skip_locked":
        # Ours. Lock what I take; STEP PAST anything another claimer holds — which is
        # both the safety property and the reason claimers spread out.
        stmt = stmt.with_for_update(skip_locked=True)
    elif claim_mode == "blocking":
        # Correct too — but a second claimer WAITS behind the first instead of
        # moving on, so the queue drains one claimer at a time. E2 prices that.
        stmt = stmt.with_for_update()
    else:  # "naive" — no row lock at all; the UPDATE lands at commit.
        # The realistic beginner version: read the PENDING rows, then write them
        # ASSIGNED. At READ COMMITTED two claimers happily read the SAME row and
        # both write it, so both heartbeats walk away believing they own it.
        pass

    lease = timedelta(seconds=get_settings().lease_ttl_s)
    assignments: list[Assignment] = []
    for run in (await session.execute(stmt)).scalars().all():
        job = by_id[run.id]
        run.status = RunStatus.ASSIGNED
        run.node_id = node.id
        run.attempt += 1                       # fencing token: 0 -> 1 first time
        run.lease_expires_at = now + lease
        assignments.append(
            Assignment(
                run_id=run.id,
                attempt=run.attempt,
                image=job.image,
                entrypoint=job.entrypoint or [],
                env=job.env or {},
                # W5b: carry the optional memory cap so the agent can apply it.
                mem_limit_mb=(job.resource_reqs or {}).get("mem_limit_mb"),
                # W6b: tell the agent this run needs sealed staging. The KEY is not
                # here and never will be — the agent gets a one-shot ticket instead,
                # and only the container ever holds the key.
                private=job.private,
                job_id=job.id,
                input_filename=job.input_filename,
                # 2026-09-04: the temporary-disk cap the agent stops this run at. The
                # explicit ask if the user made one, otherwise the tier ceiling frozen
                # onto the job at submit. Null -> no cap, exactly as before.
                scratch_mb=scratch_limit_mb(job),
                # 2026-09-05: an ordinary job's dataset file. `private` and this are
                # never both true — the sealed path and the plain path are the same
                # object route with different bytes, and a run takes exactly one.
                has_input=bool(job.input_object_key) and not job.private,
                # 2026-09-06: seal this run's data. The agent fetches a one-shot
                # ticket for the container and mounts any input READ-ONLY as the
                # ciphertext it is. The KEY is not here and never will be — only the
                # container ever holds it, and only for as long as it runs.
                sealed=job.sealed,
            )
        )
    return assignments


async def renew_leases(session: AsyncSession, node, running, now: datetime) -> None:
    """Extend the lease of each run the agent reports it is still executing —
    but ONLY if the reported attempt matches the run's current attempt (fencing).

    A late report from a node we've already given up on carries a stale attempt,
    so it cannot keep its run alive past the lease. This is a no-op until the W5
    reaper actually reclaims expired leases, but it is threaded now so W5 is
    purely "add the background sweep", not a retrofit into every path."""
    if not running:
        return
    by_id = {item.run_id: item for item in running}
    lease = timedelta(seconds=get_settings().lease_ttl_s)
    rows = (
        await session.execute(
            select(Run).where(Run.id.in_(list(by_id.keys())))
            .order_by(Run.id).with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalars().all()
    for run in rows:
        reported = by_id[run.id]
        if (
            run.node_id == node.id
            and run.attempt == reported.attempt
            and run.status in (RunStatus.ASSIGNED, RunStatus.RUNNING)
            and run.cancel_requested_at is None
        ):
            # Cancellation and renewal use the same row lock. Once cancelled,
            # even a worker that ignores commands cannot extend the stop deadline.
            run.lease_expires_at = now + lease
            # W5b: live progress rides the same fenced path — store it only for the
            # run we still own at the reported attempt (a stale report can't write it).
            if getattr(reported, "progress", None) is not None:
                run.progress = reported.progress
            if getattr(reported, "metrics", None) is not None:
                run.metrics_last = reported.metrics


async def recompute_job_status(session: AsyncSession, job_id: str) -> None:
    """Roll a job's status up from its runs.

    W2 jobs have one run, so this is mostly "the run succeeded -> the job
    SUCCEEDED". Written generally so the W4 fan-out (many runs per job) needs no
    change here. Policy: all runs SUCCEEDED -> SUCCEEDED; all terminal with at
    least one not-succeeded -> FAILED; any still in flight -> RUNNING."""
    # Sibling runs may finish in separate transactions. Serialize their rollups
    # before reading siblings, otherwise both can see the other's old RUNNING
    # state and leave a completed job RUNNING forever. Autoflush persists this
    # transaction's run change before it waits; the subsequent READ COMMITTED
    # query sees the previous rollup writer's committed runs.
    job = (
        await session.execute(
            select(Job).where(Job.id == job_id).with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if job is None:
        return
    runs = (
        await session.execute(
            select(Run).where(Run.job_id == job_id)
            .execution_options(populate_existing=True)
        )
    ).scalars().all()
    if not runs:
        return
    if all(r.status == RunStatus.SUCCEEDED for r in runs):
        job.status = JobStatus.SUCCEEDED
    elif all(r.status in _TERMINAL for r in runs):
        job.status = JobStatus.FAILED
    elif any(r.status in (RunStatus.ASSIGNED, RunStatus.RUNNING) for r in runs):
        job.status = JobStatus.RUNNING
    else:
        job.status = JobStatus.PENDING


# --- W5c: failure-aware rescheduling (FR-12) --------------------------------
#
# Failure evidence feeds placement. A run killed by a *proven* RAM shortage is
# retried — but only on a node with strictly MORE RAM than the one that failed.
# The whole feature is one number: `learned_min_ram_mb`. Because the escalated
# requirement is "strictly greater than the failed node's RAM", the failed node
# (and every equally-weak or weaker node) is automatically excluded — no
# exclusion list, no new mechanism. The retry itself is the SAME recovery path a
# LOST run takes (back to PENDING, attempt untouched; the next claim bumps it and
# fences the dead attempt), so the lease + fencing guarantees are unchanged.
#
# CPU is never escalated: RAM kills, CPU starves. GPU-VRAM escalation is deferred
# (should-tier) — we only act on a hard, kernel/driver-proven fact, and today the
# kernel OOM flag is the one we capture.


@dataclass
class OomOutcome:
    """What to do with a run that reported a proven OOM. `action` is "requeue"
    (retry on a stronger node, carrying `learned_min_ram_mb`) or "fail" (stay
    terminal FAILED with `reason`/`detail`)."""

    action: str  # "requeue" | "fail"
    reason: str | None = None            # failure_reason to set when action == "fail"
    detail: str | None = None            # the human sentence for that failure
    learned_min_ram_mb: int | None = None  # set when action == "requeue"


def decide_oom_outcome(
    *,
    user_mem_limit_mb: int | None,
    node_ram_mb: int,
    escalation_count: int,
    stronger_node_exists: bool,
    max_escalations: int = MAX_ESCALATIONS,
) -> OomOutcome:
    """Pure decision behind an OOM kill — testable without a database.

    Two kinds of OOM, only one escalates:

    1. The USER'S OWN limit was hit (they set a cap smaller than the node's RAM).
       The kill proves the *limit* was too small, not the machine — retrying
       elsewhere would fail the same way. So: FAIL, and say so plainly.
    2. The NODE itself ran out (no user cap, or a cap ≥ the node's RAM, so the
       agent capped at the node's full RAM). The kill proves the *machine* was too
       weak. So: escalate — requeue demanding strictly more RAM than this node —
       unless we've escalated too many times already, or no registered node can
       ever satisfy the raised requirement (then FAIL with an explicit reason).
    """
    # Case 1 — the user's cap, not our weakness.
    if user_mem_limit_mb is not None and user_mem_limit_mb < node_ram_mb:
        return OomOutcome(
            action="fail",
            reason=OOM_KILLED,
            detail=(
                f"Your memory limit ({int(user_mem_limit_mb)} MB) was reached — raise "
                "the limit or the run will fail the same way on any machine."
            ),
        )

    # Case 2 — the node ran out. Escalate to a strictly stronger node…
    if escalation_count >= max_escalations:
        # …but not forever: we already moved it to stronger machines this many times.
        return OomOutcome(
            action="fail",
            reason=OOM_KILLED,
            detail=(
                f"Still out of memory after escalating {escalation_count} time(s) to "
                "stronger machines — giving up."
            ),
        )
    if not stronger_node_exists:
        # …and only if the pool can ever hold it. No stronger machine exists, so
        # waiting would be forever — fail now with the exact number (FR-12).
        return OomOutcome(
            action="fail",
            reason=INSUFFICIENT_POOL,
            detail=(
                f"This run needs a machine with more than {int(node_ram_mb)} MB of RAM; "
                "no registered node has that."
            ),
        )
    return OomOutcome(action="requeue", learned_min_ram_mb=int(node_ram_mb))


async def apply_oom_escalation(session: AsyncSession, run: Run, node: Node) -> str:
    """A run just reported terminal FAILED with reason OOM_KILLED. In the SAME
    transaction, decide (see decide_oom_outcome) whether to requeue it to a
    strictly-stronger node or leave it FAILED with the honest reason. Returns the
    action taken ("requeue" | "fail"). Caller runs recompute_job_status after."""
    job = await session.get(Job, run.job_id)
    user_limit = (job.resource_reqs or {}).get("mem_limit_mb") if job else None
    # The give-up test asks the whole POOL (every registered node), not just online
    # ones: a capable-but-offline node means "wait" (PENDING), while no capable node
    # anywhere means "fail now". Strictly greater -> the failed node is excluded.
    stronger = (
        await session.execute(
            select(func.count()).select_from(Node).where(Node.ram_mb > node.ram_mb)
        )
    ).scalar_one()
    outcome = decide_oom_outcome(
        user_mem_limit_mb=user_limit,
        node_ram_mb=node.ram_mb,
        escalation_count=run.escalation_count,
        stronger_node_exists=stronger > 0,
    )

    # W7a: the two weaker rescheduling arms (E5). Both leave case 1 alone — a kill
    # at the USER'S OWN memory cap proves their limit was too small, not the
    # machine, and no retry policy disagrees about that. Only what happens after a
    # *node-capacity* kill differs, which is the one thing E5 is comparing.
    mode = get_settings().experiment_reschedule_mode
    user_capped = user_limit is not None and user_limit < node.ram_mb
    if mode != "learned" and not user_capped:
        if mode == "none":
            # No retry at all: the run dies where it fell.
            outcome = OomOutcome(
                action="fail",
                reason=OOM_KILLED,
                detail="Out of memory; this system does not retry after a memory kill.",
            )
        elif run.escalation_count >= MAX_ESCALATIONS:
            # "blind", budget spent. Same bound as ours, so the comparison is about
            # WHERE the retries went, not how many were allowed.
            outcome = OomOutcome(
                action="fail",
                reason=OOM_KILLED,
                detail=(
                    f"Out of memory again after {run.escalation_count} blind retry/retries "
                    "— giving up."
                ),
            )
        else:
            # "blind" — retry, but learn nothing from the death. No learned
            # requirement travels with the run, so the claim query's RAM filter is
            # inert and the very node that just died is eligible again. It also
            # cannot detect an impossible pool: giving it our INSUFFICIENT_POOL
            # give-up rule would hand the weaker arm our idea and flatter it.
            outcome = OomOutcome(action="requeue", learned_min_ram_mb=None)
    if outcome.action == "requeue":
        # Same recovery path as a LOST requeue: back to PENDING, node/lease cleared,
        # attempt left as-is (the next claim bumps it and fences the dead attempt).
        # The run is being retried, not dead — so it carries no failure reason, only
        # the learned requirement + a bumped escalation counter.
        run.status = RunStatus.PENDING
        run.node_id = None
        run.lease_expires_at = None
        run.finished_at = None
        run.exit_code = None
        run.failure_reason = None
        run.failure_detail = None
        run.learned_min_ram_mb = outcome.learned_min_ram_mb
        run.escalation_count += 1
    else:  # keep the terminal FAILED, replace the reason/detail with the honest one
        run.failure_reason = outcome.reason
        run.failure_detail = outcome.detail
    return outcome.action
