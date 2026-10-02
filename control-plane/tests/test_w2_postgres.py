"""The SKIP LOCKED concurrency proof — MUST run on Postgres.

`SELECT … FOR UPDATE SKIP LOCKED` is Postgres-specific; SQLite locks the whole DB
and silently ignores the clause, so this guarantee cannot be tested there (it would
pass trivially = false green on the exact property that is the contribution). This
file therefore skips unless a real Postgres URL is provided:

    # one-off throwaway DB is fine; the test wipes runs/jobs/nodes it touches
    set TEST_DATABASE_URL=postgresql+asyncpg://fyp:fyp@localhost:5432/fyp   # PowerShell: $env:TEST_DATABASE_URL=...
    pytest control-plane/tests/test_w2_postgres.py -v

What it proves: with ONE pending run and TWO agents pulling at the same instant
(two overlapping transactions), exactly one agent is assigned the run and the other
is handed nothing — never both.
"""

import os
from datetime import datetime, timezone

import pytest
import pytest_asyncio
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

import app.models  # noqa: F401 -- register all tables on Base.metadata
from app.db import Base
from app.models import Job, JobStatus, Node, NodeStatus, Run, RunStatus
from app.scheduler import assign_runs

PG_URL = os.environ.get("TEST_DATABASE_URL", "")

pytestmark = pytest.mark.skipif(
    "postgresql" not in PG_URL,
    reason="set TEST_DATABASE_URL=postgresql+asyncpg://... to run the SKIP LOCKED proof",
)


def _node(name: str, h: str, now: datetime) -> Node:
    return Node(
        name=name, status=NodeStatus.idle, cpu_cores=4, has_gpu=False,
        ram_mb=8192, capacity=4, token_hash=h * 64, last_heartbeat=now,
    )


@pytest_asyncio.fixture
async def sessionmaker_pg():
    # A pool big enough for the widest test in this file to hold every transaction
    # open AT ONCE, which is the only way a claimer can find its first choice already
    # locked. SQLAlchemy's default is five plus ten of overflow, and the thirty-claimer
    # test simply timed out against it.
    #
    # This is the TEST engine and has nothing to do with the control plane's own pool.
    # That one is still five plus ten -- the fifteen the contention campaign reads off
    # the live engine and publishes as the bound above which a request waits for a
    # connection rather than for a row. Nothing here changes that number or the
    # measurements that rest on it.
    engine = create_async_engine(PG_URL, pool_size=40, max_overflow=10)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async def _wipe() -> None:
        # Delete children before parents, in an order DERIVED from the schema rather
        # than hand-listed. A hand-list is what broke here: this fixture named five
        # tables, and `run_samples`, `node_events` and `job_keys` were added after it
        # was written -- so `DELETE FROM runs` hit a foreign key violation on any
        # database that had ever recorded a resource sample, a node event or a private
        # job key, which is every database a live run has touched. It stayed invisible
        # because CI starts from an empty Postgres, so the failing condition and the
        # passing condition never met. `sorted_tables` is dependency order, so reversed
        # is children-first, and it grows by itself the next time a migration adds a
        # table. `users` is spared on purpose: it holds the bootstrap admin, and these
        # tests drive the scheduler directly rather than logging in.
        #
        # `tiers` is spared too, since 2026-09-06, and the reason is the very defect
        # the paragraph above describes happening a second time. `users.tier_id` is a
        # foreign key onto `tiers` (2026-09-04) and the bootstrap admin carries
        # `standard`, so `DELETE FROM tiers` while that user is spared raises a
        # foreign key violation and every test in this file ERRORS before it runs.
        # It stayed invisible for the same reason as last time: CI starts from an
        # empty Postgres and creates no admin, so the failing condition and the
        # passing condition never met -- but on any database the control plane has
        # ever started against, these two SKIP LOCKED proofs could not run at all.
        # Both spared tables are seeded reference data that no test here touches.
        spared = {"users", "tiers"}
        async with factory() as s:
            for table in reversed(Base.metadata.sorted_tables):
                if table.name not in spared:
                    await s.execute(delete(table))
            await s.commit()

    await _wipe()        # clean slate: one PENDING run is the only candidate
    try:
        yield factory
    finally:
        await _wipe()    # leave no trace, even if pointed at a shared DB
        await engine.dispose()


async def test_concurrent_claimers_spread_across_the_queue(sessionmaker_pg):
    """Six one-core machines pulling at the same instant take six DIFFERENT runs.

    **Why this is a test and not an optimisation note.** `SKIP LOCKED` does two jobs.
    The famous one is safety — two claimers never take the same run — and the test
    below pins it. This is the other one: a claimer that finds a row locked STEPS PAST
    IT and keeps looking, so a queue drains in one round instead of one claimer per
    round. E2c measured exactly this and found zero waiting up to sixteen claimers.

    **What it is really measuring is how much of the queue one claimer holds.** Every
    row a claimer locks is a row the others must step over, so the fewer it holds the
    further they spread. Until 2026-09-06 a claimer locked `spare * 4` rows — the
    over-fetch window — and the W7a harness had already noticed the cost in its own
    words: with thirty claimers on thirty runs "the first seven or eight lock the
    whole queue and the remaining twenty-two come back empty-handed". It now locks at
    most `spare`, because looking and locking were separated: `_candidates` looks
    without a lock and as far as it must, `assign_runs` locks only what it takes.

    **Measured on this test, same scenario, three trees:**

        0c220c2  (the fixed `spare * 4` window)              2 of 6
        b1ddaf5  (first fix: walk, but only `spare` candidates) 1 of 6
        today    (walk for breadth, lock only what is taken)  6 of 6

    The middle row is why this test exists. The first fix for head-of-line blocking
    walked the queue correctly and collected only as many candidates as the node could
    use — so every claimer picked the same first run, and all but one came back with
    nothing. It made starvation impossible and contention worse, and no test then in
    the suite could tell.

    Six jobs rather than one job with six replicas, on purpose: replicas of one job
    are deliberately spread one-per-machine, so a single job could
    never offer a second candidate to the same machine and the test would be about
    that rule instead of about locking. One core each, for the same reason -- a
    machine with capacity 4 asks for four runs and holds four rows, which muddies
    what is being measured."""
    now = datetime.now(timezone.utc)
    count = 6

    async with sessionmaker_pg() as s:
        nodes = [
            Node(name=f"N{i}", status=NodeStatus.idle, cpu_cores=1, has_gpu=False,
                 ram_mb=8192, capacity=1, token_hash=str(i) * 64, last_heartbeat=now)
            for i in range(count)
        ]
        jobs = [
            Job(name=f"j{i}", image="fyp-dummy:latest", status=JobStatus.PENDING,
                replicas=1)
            for i in range(count)
        ]
        s.add_all([*nodes, *jobs])
        await s.flush()
        s.add_all([Run(job_id=j.id, status=RunStatus.PENDING) for j in jobs])
        await s.commit()
        node_ids = [n.id for n in nodes]

    # Every transaction is open at the same time -- the overlap is the point.
    sessions = [sessionmaker_pg() for _ in range(count)]
    opened = [await sess.__aenter__() for sess in sessions]
    try:
        claimed = []
        for sess, node_id in zip(opened, node_ids):
            node = await sess.get(Node, node_id)
            claimed.append(await assign_runs(sess, node, now))
        for sess in opened:
            await sess.commit()
    finally:
        for sess in sessions:
            await sess.__aexit__(None, None, None)

    took = [c[0].run_id for c in claimed if c]
    assert len(took) == count, (
        f"only {len(took)} of {count} claimers got a run in one round -- a claimer "
        "that finds a row locked must step past it, not give up"
    )
    assert len(set(took)) == count, "two claimers took the same run"


async def test_skip_locked_prevents_double_assignment(sessionmaker_pg):
    now = datetime.now(timezone.utc)

    async with sessionmaker_pg() as s:
        a, b = _node("A", "a", now), _node("B", "b", now)
        job = Job(name="j", image="fyp-dummy:latest", status=JobStatus.PENDING, replicas=1)
        s.add_all([a, b, job])
        await s.flush()
        run = Run(job_id=job.id, status=RunStatus.PENDING)
        s.add(run)
        await s.commit()
        a_id, b_id, run_id = a.id, b.id, run.id

    # Two agents pull at the same instant: two transactions overlap in time.
    async with sessionmaker_pg() as s1, sessionmaker_pg() as s2:
        node_a = await s1.get(Node, a_id)
        node_b = await s2.get(Node, b_id)

        # s1 locks + claims the run inside its still-open transaction.
        claimed_a = await assign_runs(s1, node_a, now)
        # s2 races for the same run; the row is locked, SKIP LOCKED hides it.
        claimed_b = await assign_runs(s2, node_b, now)

        await s1.commit()
        await s2.commit()

    # THE property: the run went to exactly one agent, never both.
    assert len(claimed_b) == 0, "concurrent puller must not see a locked run"
    assert [x.run_id for x in claimed_a] == [run_id]

    # ...and it is durably ASSIGNED to A with the fencing token bumped 0 -> 1.
    async with sessionmaker_pg() as s:
        final = await s.get(Run, run_id)
        assert final.status == RunStatus.ASSIGNED
        assert final.node_id == a_id
        assert final.attempt == 1


@pytest.mark.parametrize("count", [10, 30])
async def test_one_jobs_replicas_spread_across_concurrent_claimers(
    sessionmaker_pg, count
):
    """N replicas of ONE job, N free machines claiming at once: every machine is
    handed one replica, and no replica goes to two of them.

    **The test the suite admitted it did not have.** Its neighbour above uses six
    independent jobs on purpose, and says so: replicas of one job are spread
    one-per-machine, so a single job could never offer a second
    candidate to the same machine and that test would be about the spread rule
    instead of about locking. True — and the case it set aside is the case that
    broke. On 2026-09-06 the spread preference STEPPED PAST every sibling, so on a
    queue that is one job's replicas each claimer's candidate list came back one row
    long, and it was the same row for every claimer: one won it under SKIP LOCKED and
    the rest had no second choice. Thirty claimers on thirty replicas served seven
    (`docs/evidence/contention_rerun_2026-09-06.txt`).

    This is FR-5's own sentence -- the scheduler expands a job into runs and
    dispatches them to eligible nodes in parallel -- so it is tested at the size the
    jury asked about rather than at six.

    Thirty transactions are held open at once here for the same reason the six-job
    test holds six: a claimer must find its first choice ALREADY LOCKED, which cannot
    happen if each transaction commits before the next begins."""
    now = datetime.now(timezone.utc)

    async with sessionmaker_pg() as s:
        nodes = [
            Node(name=f"R{i}", status=NodeStatus.idle, cpu_cores=1, has_gpu=False,
                 ram_mb=8192, capacity=1, token_hash=f"{i:02d}" * 32,
                 last_heartbeat=now)
            for i in range(count)
        ]
        # ONE job, `count` replicas, untargeted -- the fan-out FR-5 describes.
        job = Job(name="one-job", image="fyp-dummy:latest",
                  status=JobStatus.PENDING, replicas=count)
        s.add_all([*nodes, job])
        await s.flush()
        s.add_all([Run(job_id=job.id, status=RunStatus.PENDING) for _ in range(count)])
        await s.commit()
        node_ids = [n.id for n in nodes]

    sessions = [sessionmaker_pg() for _ in range(count)]
    opened = [await sess.__aenter__() for sess in sessions]
    try:
        claimed = []
        for sess, node_id in zip(opened, node_ids):
            node = await sess.get(Node, node_id)
            claimed.append(await assign_runs(sess, node, now))
        for sess in opened:
            await sess.commit()
    finally:
        for sess in sessions:
            await sess.__aexit__(None, None, None)

    took = [c[0].run_id for c in claimed if c]
    assert len(took) == count, (
        f"only {len(took)} of {count} claimers were served on one job's replicas -- "
        "the spread preference must ORDER the candidate list, never empty it"
    )
    assert len(set(took)) == count, "two claimers took the same replica"

    # ...and the database agrees: every replica assigned, each to a different machine.
    async with sessionmaker_pg() as s:
        runs = (await s.execute(select(Run))).scalars().all()
        assert all(r.status == RunStatus.ASSIGNED for r in runs)
        assert len({r.node_id for r in runs}) == count


async def test_spread_preference_holds_when_replicas_are_fewer_than_machines(
    sessionmaker_pg,
):
    """Three replicas, three machines with room for four runs each, asking one after
    another: each takes ONE. Nobody hoovers.

    This is the other half of the 2026-09-06 fix and the reason `assign_runs` caps
    what it takes at the number of PREFERRED candidates rather than at spare
    capacity. The candidate list now carries siblings at the back so a claimer whose
    first choice is stolen has somewhere to go -- and without the cap, the first
    machine to ask would simply take all three, which is the spread preference
    deleted rather than kept.

    Sequential and committed between claims, deliberately: nothing is locked when the
    second machine asks, so only the preference can produce this answer."""
    now = datetime.now(timezone.utc)
    machines, replicas = 3, 3

    async with sessionmaker_pg() as s:
        nodes = [
            Node(name=f"B{i}", status=NodeStatus.idle, cpu_cores=4, has_gpu=False,
                 ram_mb=8192, capacity=4, token_hash=f"b{i}" * 32,
                 last_heartbeat=now)
            for i in range(machines)
        ]
        job = Job(name="spread", image="fyp-dummy:latest",
                  status=JobStatus.PENDING, replicas=replicas)
        s.add_all([*nodes, job])
        await s.flush()
        s.add_all([Run(job_id=job.id, status=RunStatus.PENDING)
                   for _ in range(replicas)])
        await s.commit()
        node_ids = [n.id for n in nodes]

    counts = []
    for node_id in node_ids:
        async with sessionmaker_pg() as s:
            node = await s.get(Node, node_id)
            got = await assign_runs(s, node, now)
            await s.commit()
            counts.append(len(got))

    assert counts == [1, 1, 1], (
        f"each machine should take one replica while another is free, got {counts}"
    )

    async with sessionmaker_pg() as s:
        runs = (await s.execute(select(Run))).scalars().all()
        assert len({r.node_id for r in runs}) == replicas, (
            "two replicas landed on one machine while another machine was free"
        )


async def test_a_replica_no_clean_machine_can_take_is_not_left_waiting(
    sessionmaker_pg,
):
    """More replicas than machines: the machines share, rather than all politely
    deferring to each other while the work waits.

    Found while fixing the one-job collapse. `_other_eligible_node_free` counted a
    machine that ALREADY held a run of this job as an alternative, so with four
    replicas and two machines of capacity four, each machine took one and then both
    deferred the third to the other -- each of which was deferring it straight back,
    because each was online, eligible and not full. Nobody was wrong and the replica
    never ran. A machine holding a sibling is no longer an alternative, so when no
    clean machine exists the preference stands aside and the work goes out."""
    now = datetime.now(timezone.utc)
    machines, replicas = 2, 4

    async with sessionmaker_pg() as s:
        nodes = [
            Node(name=f"S{i}", status=NodeStatus.idle, cpu_cores=4, has_gpu=False,
                 ram_mb=8192, capacity=4, token_hash=f"s{i}" * 32,
                 last_heartbeat=now)
            for i in range(machines)
        ]
        job = Job(name="share", image="fyp-dummy:latest",
                  status=JobStatus.PENDING, replicas=replicas)
        s.add_all([*nodes, job])
        await s.flush()
        s.add_all([Run(job_id=job.id, status=RunStatus.PENDING)
                   for _ in range(replicas)])
        await s.commit()
        node_ids = [n.id for n in nodes]

    # Two full rounds: the first gives each machine its one, the second is where the
    # old code deadlocked.
    for _ in range(2):
        for node_id in node_ids:
            async with sessionmaker_pg() as s:
                node = await s.get(Node, node_id)
                await assign_runs(s, node, now)
                await s.commit()

    async with sessionmaker_pg() as s:
        pending = (
            await s.execute(select(Run).where(Run.status == RunStatus.PENDING))
        ).scalars().all()
        assert not pending, (
            f"{len(pending)} replica(s) still PENDING with free capacity in the pool "
            "-- every machine deferred to a machine that was deferring back"
        )
