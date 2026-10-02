"""Head-of-line blocking in the claim query (found 2026-09-06, fixed the same day).

**The defect these tests pin, in one sentence.** A run this node cannot take used to
occupy a place in the candidate window, so a queue whose head was full of such runs
hid every run behind them — and a machine that could have worked was told there was
nothing to do.

**How it happened.** `assign_runs` fetched a fixed window of the oldest PENDING runs
(`LIMIT spare * 4`) and then filtered them in Python. The over-fetch was there for
exactly this reason and its comment said so — "so that rows ineligible for *this* node
don't starve a node that could otherwise be filled" — but a constant is a mitigation,
not a bound. Four ineligible runs ahead of the queue were enough to hide everything
from a machine with capacity 1.

**Why it is not academic.** The runs that pile up at the head are the ones nothing
ever clears: a run targeted at a machine that never comes back waits for ever, by
design, because the machine may return. Two of those from a demonstration a week ago,
plus a couple from a proof, and a lab PC with one core claims nothing at all — while
the dashboard shows it idle and the queue full.

**These tests pin the DEFECT, not the fix**: each one fails against
the code as it stood this morning, and the failure is the behaviour, not an exception.
"""

import pytest

from app.models import Job, Node, Run, RunStatus

SPECS = {"cpu_cores": 4, "has_gpu": False, "ram_mb": 8192, "capacity": 4,
         "agent_version": "0.12.0"}
# One core, one run at a time: the smallest useful machine, and the one the defect
# hurts most, because its window is the narrowest.
SMALL = dict(SPECS, cpu_cores=1, capacity=1)


async def _register(client, name, specs=None):
    r = await client.post("/agent/register", json={"name": name, "specs": specs or SPECS})
    assert r.status_code == 200, r.text
    return r.json()


async def _heartbeat(client, node):
    r = await client.post(
        "/agent/heartbeat",
        headers={"Authorization": f"Bearer {node['token']}"},
        json={"node_id": node["node_id"], "status": "idle", "running": []},
    )
    assert r.status_code == 200, r.text
    return r.json()["assignments"]


async def _submit(client, name="j", **over):
    body = {
        "name": name,
        "image": "fyp-dummy:latest",
        "entrypoint": ["python", "train.py"],
        "resource_reqs": {"needs_gpu": False},
        "replicas": 1,
    }
    body.update(over)
    r = await client.post("/jobs", json=body)
    assert r.status_code == 200, r.text
    return r.json()


async def _ghost_node(session_factory, name="machine-that-never-came-back"):
    """A node row for a machine that registered once and never returned.

    Nothing reaps a node — liveness is derived at read time (protocol.md §2) — so
    this row, and any run targeted at it, outlives the machine itself."""
    async with session_factory() as s:
        node = Node(
            name=name, cpu_cores=4, has_gpu=False, ram_mb=8192, capacity=4,
            token_hash="d" * 64, agent_version="0.12.0",
        )
        s.add(node)
        await s.commit()
        return node.id


async def _stale_targeted_runs(client, session_factory, count, ghost_id):
    """`count` PENDING runs aimed at a machine that is never coming back, all older
    than whatever the caller submits next."""
    for i in range(count):
        await _submit(client, name=f"aimed-at-a-ghost-{i}", target_node_ids=[ghost_id])
    async with session_factory() as s:
        from sqlalchemy import func, select

        pending = (
            await s.execute(
                select(func.count()).select_from(Run).where(Run.status == RunStatus.PENDING)
            )
        ).scalar_one()
    assert pending == count, "the ghost's runs must all still be waiting"


# ===========================================================================
# THE DEFECT
# ===========================================================================


@pytest.mark.parametrize("ahead", [4, 12, 40])
async def test_a_small_node_claims_work_behind_any_number_of_runs_it_cannot_take(
    client, session_factory, ahead
):
    """THE test. However many runs it cannot take are ahead of it, a machine that can
    take the next one takes it.

    Parametrised on purpose. Four was already enough to break a one-core machine, and
    the point of the fix is that the number does not matter — so the test says the
    number does not matter rather than picking one that happens to pass."""
    ghost = await _ghost_node(session_factory)
    await _stale_targeted_runs(client, session_factory, ahead, ghost)

    mine = await _submit(client, name="the-run-that-should-be-taken")
    small = await _register(client, "one-core-pc", SMALL)

    offered = await _heartbeat(client, small)
    assert len(offered) == 1, (
        f"a machine with one core was offered nothing with {ahead} unclaimable runs "
        "ahead of it"
    )
    assert offered[0]["run_id"] == mine["run_ids"][0]


async def test_the_ghost_runs_are_still_waiting_afterwards(client, session_factory):
    """The fix must not tidy the queue by throwing work away.

    A run targeted at a machine that is offline is NOT hopeless — laptops come back,
    and that is why the platform waits rather than failing it (the same reasoning the
    trust tier already uses, protocol.md §4). What changes is that it stops standing
    in anyone else's way."""
    ghost = await _ghost_node(session_factory)
    await _stale_targeted_runs(client, session_factory, 6, ghost)
    await _submit(client, name="ordinary")

    small = await _register(client, "one-core-pc", SMALL)
    assert len(await _heartbeat(client, small)) == 1

    async with session_factory() as s:
        from sqlalchemy import select

        rows = (
            await s.execute(
                select(Run.status).join(Job, Job.id == Run.job_id)
                .where(Job.name.like("aimed-at-a-ghost-%"))
            )
        ).scalars().all()
    assert rows and all(r is RunStatus.PENDING for r in rows), (
        "the ghost's runs must still be waiting for their machine, not swept away"
    )


async def test_the_machine_they_were_aimed_at_gets_them_when_it_asks(
    client, session_factory
):
    """And the waiting has a point: the machine asks, and its work is there.

    Through the real door this time — a machine that registered, went quiet while
    other machines worked around its runs, and then heart-beats again. Nothing had to
    be re-submitted and nothing was lost."""
    quiet = await _register(client, "went-quiet-for-a-while")
    for i in range(3):
        await _submit(client, name=f"for-the-quiet-one-{i}",
                      target_node_ids=[quiet["node_id"]])
    await _submit(client, name="for-anyone")

    # Somebody else works in the meantime, and takes only what is theirs.
    other = await _register(client, "still-here", SMALL)
    offered_other = await _heartbeat(client, other)
    assert len(offered_other) == 1

    offered = await _heartbeat(client, quiet)
    assert len(offered) == 3, "its own three runs were waiting for it"


# ===========================================================================
# THE ORDER, AND THE OTHER WAYS A RUN CAN BE INELIGIBLE
# ===========================================================================


async def test_the_oldest_run_it_can_take_is_the_one_it_takes(client, session_factory):
    """Skipping past what it cannot take must not become skipping past what it can.

    First-come-first-served is the placement rule this project defends (§5.2): among
    the runs a machine is eligible for, the oldest wins."""
    ghost = await _ghost_node(session_factory)
    await _stale_targeted_runs(client, session_factory, 5, ghost)
    first = await _submit(client, name="older")
    second = await _submit(client, name="newer")

    small = await _register(client, "one-core-pc", SMALL)
    offered = await _heartbeat(client, small)
    assert [a["run_id"] for a in offered] == [first["run_ids"][0]]

    # And the newer one is left where it was, for the next machine to ask. Skipping
    # past what a node cannot take must not turn into taking things out of order.
    async with session_factory() as s:
        run = await s.get(Run, second["run_ids"][0])
    assert run.status is RunStatus.PENDING


async def test_a_gpu_queue_does_not_starve_the_cpu_machines(client, session_factory):
    """The same defect through a different door, and the reason the fix is not about
    targeting.

    A queue of GPU work is ineligible for a CPU machine for a reason that has nothing
    to do with which node it names. Before the fix these filled the window exactly as
    the ghost's runs did."""
    for i in range(8):
        await _submit(client, name=f"gpu-{i}", resource_reqs={"needs_gpu": True})
    cpu_work = await _submit(client, name="cpu-work")

    cpu = await _register(client, "cpu-only", SMALL)
    offered = await _heartbeat(client, cpu)
    assert len(offered) == 1
    assert offered[0]["run_id"] == cpu_work["run_ids"][0]


async def test_a_sealed_queue_does_not_starve_an_older_agent(client, session_factory):
    """And a third door, which only exists since 2026-09-06: a sealed job is invisible
    to an agent too old to stage one, so a queue of sealed runs is a queue of
    ineligible runs for that machine. It must still be able to take the work it CAN
    take — one of the jobs submitted before sealing."""
    for i in range(8):
        await _submit(client, name=f"sealed-{i}")
    legacy = await _submit(client, name="from-before-sealing")
    async with session_factory() as s:
        job = await s.get(Job, legacy["job_id"])
        job.sealed = False
        await s.commit()

    old = await _register(client, "old-agent", dict(SMALL, agent_version="0.11.0"))
    offered = await _heartbeat(client, old)
    assert len(offered) == 1
    assert offered[0]["run_id"] == legacy["run_ids"][0]


async def test_a_full_queue_of_nothing_it_can_take_is_answered_with_nothing(
    client, session_factory
):
    """The other side of the same coin: scanning past everything must end, and end
    with an empty answer rather than a wrong one."""
    ghost = await _ghost_node(session_factory)
    await _stale_targeted_runs(client, session_factory, 15, ghost)

    small = await _register(client, "one-core-pc", SMALL)
    assert await _heartbeat(client, small) == []


async def test_capacity_is_still_the_ceiling_on_one_heartbeat(client, session_factory):
    """A machine that can hold four runs takes four, not everything it can see."""
    ghost = await _ghost_node(session_factory)
    await _stale_targeted_runs(client, session_factory, 10, ghost)
    for i in range(9):
        await _submit(client, name=f"mine-{i}")

    node = await _register(client, "four-core-pc", SPECS)   # capacity 4
    assert len(await _heartbeat(client, node)) == 4


# ===========================================================================
# WAITING, VISIBLY
# ===========================================================================


async def test_a_run_aimed_at_an_absent_machine_says_what_it_is_waiting_for(
    client, session_factory
):
    """Waiting is the right answer; waiting SILENTLY was the defect.

    A run aimed at a machine that is offline is not hopeless — the machine may come
    back, which is why the platform waits rather than failing it (the same reasoning
    W6b's trust tier uses, and the opposite of W5c's INSUFFICIENT_POOL, which fails
    fast because no amount of waiting can conjure a bigger machine). What was missing
    was anyone being able to SEE what it was waiting for."""
    ghost = await _ghost_node(session_factory, name="lab-pc-that-went-home")
    created = await _submit(client, name="aimed", target_node_ids=[ghost])

    rows = (await client.get(f"/jobs/{created['job_id']}/runs")).json()
    assert len(rows) == 1
    line = rows[0]["waiting_for"]
    assert line and "lab-pc-that-went-home" in line, line
    assert "waiting for" in line


async def test_an_ordinary_waiting_run_says_nothing(client, session_factory):
    """The line appears only when the machines a run NAMES are absent. An untargeted
    run waiting its turn is waiting for a free machine, not for a particular one, and
    a sentence there would be noise on every queue."""
    created = await _submit(client, name="ordinary")
    rows = (await client.get(f"/jobs/{created['job_id']}/runs")).json()
    assert rows[0]["waiting_for"] is None


async def test_the_line_goes_away_when_the_machine_comes_back(client, session_factory):
    """Whatever the interface says has to stop being true at the moment it stops being
    true — a stale explanation is worse than none."""
    node = await _register(client, "comes-back")
    created = await _submit(client, name="aimed", target_node_ids=[node["node_id"]])

    async with session_factory() as s:
        from datetime import datetime, timedelta, timezone

        row = await s.get(Node, node["node_id"])
        row.last_heartbeat = datetime.now(timezone.utc) - timedelta(hours=3)
        await s.commit()
    rows = (await client.get(f"/jobs/{created['job_id']}/runs")).json()
    assert "silent for 3h" in (rows[0]["waiting_for"] or ""), rows[0]["waiting_for"]

    await _heartbeat(client, node)      # it is back, and it takes its run
    rows = (await client.get(f"/jobs/{created['job_id']}/runs")).json()
    assert rows[0]["waiting_for"] is None
    assert rows[0]["status"] == "ASSIGNED"
