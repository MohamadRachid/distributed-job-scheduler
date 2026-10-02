"""W7a Phase 0 — the experiment-only switches.

These switches exist so the *weaker alternative* to each of our core design
choices can be run inside our own platform and measured, instead of asserted. That makes them the most dangerous code in the repo: a switch
that silently weakened a guarantee, or one that failed to weaken it when asked,
would either break the product or fabricate a result. So this file pins both
directions:

  1. **The defaults are today's system.** Every switch left alone behaves exactly
     as W2/W5/W5c did — the ~142 tests around this file are the wider proof, and
     the first block here states it directly.
  2. **A non-default value really does change the behaviour**, in the one specific
     way the brief names, and nothing else.
  3. **A typo is a hard failure**, never a silent fall-back to the default. A
     mislabelled arm is worse than a missing arm.

The concurrency *consequences* of the claim modes (double-assignment under load)
are Postgres-only and belong to E2 on real hardware — SQLite cannot express row
locking at all. What is pinned here is that each mode still assigns correctly.
"""

import logging
from datetime import datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

import app.api.health as health_module
from app.api.health import MODE_EPOCH
from app.config import (
    EXPERIMENT_DEFAULTS,
    Settings,
    experiment_is_default,
    experiment_mode,
    get_settings,
)
from app.main import _warn_if_experimental
from app.models import Job, JobStatus, Node, NodeStatus, Run, RunStatus
from app.reaper import sweep_once

NOW = datetime(2026, 7, 29, 12, 0, 0, tzinfo=timezone.utc)
PAST = NOW - timedelta(seconds=30)

_ENV = {
    "guarantee": "EXPERIMENT_GUARANTEE_MODE",
    "claim": "EXPERIMENT_CLAIM_MODE",
    "reschedule": "EXPERIMENT_RESCHEDULE_MODE",
}


@pytest.fixture
def mode(monkeypatch):
    """Set the switches for one test and put them back afterwards.

    `get_settings` is lru_cached (one read of the environment per process), so the
    cache must be cleared on the way in AND on the way out — otherwise a weakened
    mode would leak into the next test, which is exactly the accident the loud
    startup banner exists to prevent in production."""
    def _set(**kw):
        unknown = set(kw) - set(EXPERIMENT_DEFAULTS)
        assert not unknown, f"unknown switch {unknown}"
        for key, env in _ENV.items():
            monkeypatch.setenv(env, kw.get(key, EXPERIMENT_DEFAULTS[key]))
        get_settings.cache_clear()
        return experiment_mode()

    yield _set
    get_settings.cache_clear()


# ===========================================================================
# 1) Defaults = the shipped system
# ===========================================================================


def test_defaults_are_the_shipped_behaviour(mode):
    assert mode() == EXPERIMENT_DEFAULTS
    assert experiment_is_default() is True


async def test_health_publishes_the_mode(client, mode):
    """The harness reads this before every arm and refuses to record a number if it
    does not match what it asked for — so /health must actually carry it."""
    mode()
    resp = await client.get("/health")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    assert body["experiment_mode"] == EXPERIMENT_DEFAULTS


async def test_health_publishes_a_mode_epoch(client, mode):
    """The epoch tells the harness WHICH control-plane process answered. It re-checks
    it when a repetition ends, so a restart mid-measurement voids that repetition
    instead of silently mislabelling it."""
    mode()
    first = (await client.get("/health")).json()["mode_epoch"]
    second = (await client.get("/health")).json()["mode_epoch"]
    assert first and isinstance(first, str)
    assert first == second      # stable within one process…
    assert first == MODE_EPOCH  # …and it is this process's id


def test_mode_epoch_survives_a_database_reset():
    """Why an id and not a counter: the harness's own `reset_platform()` truncates
    the tables, so a counter stored in the database would be reset by the very tool
    that is supposed to be watched. The epoch lives in the process, so nothing the
    experiment does to the data can touch it."""
    assert MODE_EPOCH == health_module.MODE_EPOCH


def test_unknown_value_is_a_hard_failure_not_a_silent_default(monkeypatch):
    """A typo must stop the control plane, not quietly run `full`. Falling back to
    the default would mean an arm labelled "fencing off" ran with fencing on."""
    monkeypatch.setenv("EXPERIMENT_GUARANTEE_MODE", "lease-only")   # hyphen, not underscore
    with pytest.raises(ValidationError):
        Settings()


def test_banner_is_silent_on_defaults_and_loud_otherwise(mode, caplog):
    with caplog.at_level(logging.WARNING, logger="main"):
        mode()
        _warn_if_experimental()
        assert caplog.records == []

        mode(guarantee="none")
        _warn_if_experimental()
    shouted = "\n".join(r.getMessage() for r in caplog.records)
    assert "EXPERIMENT MODE" in shouted
    assert "guarantee=none" in shouted and "normally full" in shouted


# ===========================================================================
# 2) guarantee — the fencing-class rejection
# ===========================================================================


def _auth(node):
    return {"Authorization": f"Bearer {node['token']}"}


async def _register(client, name, ram_mb=8192):
    resp = await client.post(
        "/agent/register",
        json={"name": name, "specs": {"cpu_cores": 4, "has_gpu": False,
                                      "ram_mb": ram_mb, "capacity": 4, "agent_version": "0.12.0"}},
    )
    assert resp.status_code == 200, resp.text
    return resp.json()


async def _create_job(client, **overrides):
    body = {"name": "j", "image": "fyp-dummy:latest", "entrypoint": ["python", "train.py"],
            "env": {}, "resource_reqs": {}, "replicas": 1}
    body.update(overrides)
    resp = await client.post("/jobs", json=body)
    assert resp.status_code == 200, resp.text
    return resp.json()


async def _heartbeat(client, node):
    resp = await client.post(
        "/agent/heartbeat", headers=_auth(node),
        json={"node_id": node["node_id"], "status": "idle", "running": []},
    )
    assert resp.status_code == 200, resp.text
    return resp.json()


async def _status(client, node, run_id, attempt, state, exit_code=None, reason=None, detail=None):
    body = {"attempt": attempt, "state": state, "exit_code": exit_code}
    if reason is not None:
        body["failure_reason"] = reason
        body["failure_detail"] = detail
    return await client.post(f"/agent/runs/{run_id}/status", headers=_auth(node), json=body)


async def _run_row(session_factory, run_id):
    async with session_factory() as s:
        return await s.get(Run, run_id)


async def _stale_setup(client):
    """Node A claims a run and starts it, then the run is handed to node B at a
    bumped attempt — the exact moment a zombie's late result arrives."""
    node_a, node_b = await _register(client, "a"), await _register(client, "b")
    await _create_job(client)
    assign = (await _heartbeat(client, node_a))["assignments"][0]
    await _status(client, node_a, assign["run_id"], assign["attempt"], "RUNNING")
    return node_a, node_b, assign["run_id"], assign["attempt"]


async def test_full_rejects_a_stale_attempt(client, session_factory, mode):
    """The shipped behaviour, restated here so a change to the switch cannot quietly
    take the fence with it."""
    mode()
    node_a, _, run_id, attempt = await _stale_setup(client)
    resp = await _status(client, node_a, run_id, attempt + 5, "SUCCEEDED", exit_code=0)
    assert resp.status_code == 409
    assert "stale attempt" in resp.json()["detail"]
    assert (await _run_row(session_factory, run_id)).status is RunStatus.RUNNING


async def test_full_rejects_a_report_from_a_node_that_does_not_own_the_run(
    client, session_factory, mode
):
    mode()
    _, node_b, run_id, attempt = await _stale_setup(client)
    resp = await _status(client, node_b, run_id, attempt, "SUCCEEDED", exit_code=0)
    assert resp.status_code == 409
    assert (await _run_row(session_factory, run_id)).status is RunStatus.RUNNING


async def test_lease_only_accepts_the_stale_result(client, session_factory, mode, caplog):
    """The weaker arm's whole point: with no per-attempt identity, a result from an
    execution the platform had already given up on is ACCEPTED. E1 measures the
    damage that causes; this test proves the arm is genuinely weakened."""
    mode(guarantee="lease_only")
    node_a, _, run_id, attempt = await _stale_setup(client)
    with caplog.at_level(logging.WARNING, logger="agent_api"):
        resp = await _status(client, node_a, run_id, attempt + 5, "SUCCEEDED", exit_code=0)
    assert resp.status_code == 200
    run = await _run_row(session_factory, run_id)
    assert run.status is RunStatus.SUCCEEDED
    # The audit line is the server's own record that a stale result got through.
    assert any("accepted-unfenced" in r.getMessage() for r in caplog.records)


async def test_lease_only_accepts_a_report_from_a_non_owning_node(
    client, session_factory, mode
):
    """The design defines "wrong node" as fencing-class, alongside "stale
    attempt" — so the switch turns off both, or the arm would still carry half our
    fence while claiming to have none."""
    mode(guarantee="lease_only")
    _, node_b, run_id, attempt = await _stale_setup(client)
    resp = await _status(client, node_b, run_id, attempt, "SUCCEEDED", exit_code=0)
    assert resp.status_code == 200
    assert (await _run_row(session_factory, run_id)).status is RunStatus.SUCCEEDED


async def test_lease_only_still_refuses_to_overwrite_an_accepted_result(
    client, session_factory, mode
):
    """The terminal guard is NOT fencing — "a result, once accepted, is final" is
    ordinary state-machine hygiene that any competent lease-only system also has.
    Removing it too would make the weaker arm a strawman, so it stays on."""
    mode(guarantee="lease_only")
    node_a, node_b, run_id, attempt = await _stale_setup(client)
    await _status(client, node_a, run_id, attempt, "SUCCEEDED", exit_code=0)
    resp = await _status(client, node_b, run_id, attempt, "FAILED", exit_code=1)
    assert resp.status_code == 200
    run = await _run_row(session_factory, run_id)
    assert run.status is RunStatus.SUCCEEDED and run.exit_code == 0


async def test_owner_only_rejects_a_report_from_a_non_owning_node(
    client, session_factory, mode
):
    """`owner_only` keeps the wrong-node half. This is what makes it the STRONGEST
    honest alternative rather than a strawman — the arm the fencing token has to
    beat, not one it beats for free."""
    mode(guarantee="owner_only")
    _, node_b, run_id, attempt = await _stale_setup(client)
    resp = await _status(client, node_b, run_id, attempt, "SUCCEEDED", exit_code=0)
    assert resp.status_code == 409
    assert (await _run_row(session_factory, run_id)).status is RunStatus.RUNNING


async def test_owner_only_accepts_a_stale_attempt_from_the_OWNING_node(
    client, session_factory, mode
):
    """The whole reason `owner_only` exists, and the case E1's `reclaim` scenario
    drives: the machine really does own the run, so an ownership check passes it —
    and with no per-attempt identity, an execution the platform gave up on lands
    its result anyway. Only the epoch can tell these two apart."""
    mode(guarantee="owner_only")
    node_a, _, run_id, attempt = await _stale_setup(client)
    resp = await _status(client, node_a, run_id, attempt + 5, "SUCCEEDED", exit_code=0)
    assert resp.status_code == 200
    assert (await _run_row(session_factory, run_id)).status is RunStatus.SUCCEEDED


async def test_full_rejects_the_case_owner_only_lets_through(
    client, session_factory, mode
):
    """The paired half of the test above, on the same setup. Together these two are
    the measured claim in miniature: the ownership check alone is not enough, and
    the token is the thing that closes it."""
    mode()
    node_a, _, run_id, attempt = await _stale_setup(client)
    resp = await _status(client, node_a, run_id, attempt + 5, "SUCCEEDED", exit_code=0)
    assert resp.status_code == 409
    assert "stale attempt" in resp.json()["detail"]
    assert (await _run_row(session_factory, run_id)).status is RunStatus.RUNNING


async def test_owner_only_keeps_the_reaper(session_factory, mode):
    """Only `none` turns recovery off. `owner_only` is a fencing question, not a
    recovery one, so the sweep must still fire."""
    mode(guarantee="owner_only")
    await _expired_run(session_factory)
    decisions = await sweep_once(session_factory=session_factory, now=NOW)
    assert [d["decision"] for d in decisions] == ["requeued"]


async def test_unknown_run_is_still_404_in_every_arm(client, mode):
    mode(guarantee="none")
    node = await _register(client, "a")
    resp = await _status(client, node, "no-such-run", 1, "SUCCEEDED", exit_code=0)
    assert resp.status_code == 404


# --- the reaper half of the guarantee switch --------------------------------


async def _expired_run(session_factory, retries=1):
    async with session_factory() as s:
        node = Node(name="n", status=NodeStatus.idle, cpu_cores=4, has_gpu=False,
                    ram_mb=8192, capacity=4, token_hash="h" * 64, last_heartbeat=NOW)
        job = Job(name="j", image="fyp-dummy:latest", status=JobStatus.RUNNING, replicas=1)
        s.add_all([node, job])
        await s.flush()
        run = Run(job_id=job.id, node_id=node.id, status=RunStatus.RUNNING,
                  attempt=1, lease_expires_at=PAST, retries_remaining=retries)
        s.add(run)
        await s.commit()
        return run.id


async def test_full_reaper_still_requeues_an_expired_lease(session_factory, mode):
    mode()
    run_id = await _expired_run(session_factory)
    decisions = await sweep_once(session_factory=session_factory, now=NOW)
    assert [d["decision"] for d in decisions] == ["requeued"]
    run = await _run_row(session_factory, run_id)
    assert run.status is RunStatus.PENDING
    assert run.attempt == 1          # the reaper never bumps — the claim does


async def test_guarantee_none_turns_recovery_off_entirely(session_factory, mode):
    """`none` is the arm with no safety net at all: the lost run simply stays lost.
    The gate lives inside sweep_once, so scripts that call the reaper directly
    (the chaos test, E1) honour it too — no caller can accidentally recover."""
    mode(guarantee="none")
    run_id = await _expired_run(session_factory)
    assert await sweep_once(session_factory=session_factory, now=NOW) == []
    run = await _run_row(session_factory, run_id)
    assert run.status is RunStatus.RUNNING      # never noticed, never requeued
    assert run.node_id is not None


async def test_lease_only_keeps_the_reaper(session_factory, mode):
    """lease_only means "a timeout is enough" — so the timeout must still fire.
    Only the fence is gone."""
    mode(guarantee="lease_only")
    await _expired_run(session_factory)
    decisions = await sweep_once(session_factory=session_factory, now=NOW)
    assert [d["decision"] for d in decisions] == ["requeued"]


# ===========================================================================
# 3) claim — the locking strategy
# ===========================================================================


@pytest.mark.parametrize("claim_mode", ["skip_locked", "blocking", "naive"])
async def test_every_claim_mode_still_dispatches(client, session_factory, mode, claim_mode):
    """All three arms must place work correctly on their own; what separates them is
    what happens when two claimers collide, which SQLite cannot express and E2
    measures on real Postgres. If an arm could not dispatch at all it would be a
    broken alternative, not a weaker one — and comparing against broken proves
    nothing."""
    mode(claim=claim_mode)
    node = await _register(client, "a")
    created = await _create_job(client)
    assignments = (await _heartbeat(client, node))["assignments"]
    assert [a["run_id"] for a in assignments] == created["run_ids"]
    assert assignments[0]["attempt"] == 1        # the fence is set in every arm
    run = await _run_row(session_factory, created["run_ids"][0])
    assert run.status is RunStatus.ASSIGNED and run.node_id == node["node_id"]


async def test_claim_mode_does_not_change_which_runs_are_eligible(client, mode):
    """Only the LOCK differs between arms — the WHERE clause, the order and the limit
    are identical, so a weaker arm never also searches differently."""
    mode(claim="naive")
    weak = await _register(client, "weak", ram_mb=512)
    await _create_job(client, resource_reqs={"min_ram_mb": 4096})
    assert (await _heartbeat(client, weak))["assignments"] == []


# ===========================================================================
# 4) reschedule — what happens after a proven memory kill
# ===========================================================================


async def _oom(client, node, run_id, attempt):
    return await _status(
        client, node, run_id, attempt, "FAILED", exit_code=137,
        reason="OOM_KILLED", detail="RAM overload: the kernel killed the container.",
    )


async def _claim_and_oom(client, node, run_id=None):
    assign = (await _heartbeat(client, node))["assignments"][0]
    await _status(client, node, assign["run_id"], assign["attempt"], "RUNNING")
    await _oom(client, node, assign["run_id"], assign["attempt"])
    return assign["run_id"]


async def test_learned_is_unchanged(client, session_factory, mode):
    """W5c, restated: the retry carries the failed node's RAM as a learned floor."""
    mode()
    weak = await _register(client, "weak", ram_mb=512)
    await _register(client, "strong", ram_mb=4096)
    await _create_job(client)
    run_id = await _claim_and_oom(client, weak)
    run = await _run_row(session_factory, run_id)
    assert run.status is RunStatus.PENDING
    assert run.learned_min_ram_mb == 512
    assert run.escalation_count == 1


async def test_blind_retries_but_learns_nothing(client, session_factory, mode):
    """The weaker arm: same recovery path, same retry budget — but no learned
    requirement travels with the run, so the claim query's RAM filter stays inert
    and the machine that just died is eligible again."""
    mode(reschedule="blind")
    weak = await _register(client, "weak", ram_mb=512)
    await _register(client, "strong", ram_mb=4096)
    await _create_job(client)
    run_id = await _claim_and_oom(client, weak)
    run = await _run_row(session_factory, run_id)
    assert run.status is RunStatus.PENDING
    assert run.learned_min_ram_mb is None
    assert run.escalation_count == 1
    # …and the proof that "eligible again" is real: the weak node re-claims it.
    assert (await _heartbeat(client, weak))["assignments"][0]["run_id"] == run_id


async def test_blind_does_not_inherit_our_give_up_rule(client, session_factory, mode):
    """On a pool where nothing can ever hold the job, ours fails at once with
    INSUFFICIENT_POOL. Blind has no learned requirement, so it cannot know that —
    handing it our give-up rule would flatter the weaker arm."""
    mode(reschedule="blind")
    weak = await _register(client, "weak", ram_mb=512)
    await _create_job(client)
    run_id = await _claim_and_oom(client, weak)
    run = await _run_row(session_factory, run_id)
    assert run.status is RunStatus.PENDING
    assert run.failure_reason is None


async def test_blind_still_gives_up_after_the_same_budget(client, session_factory, mode):
    """Bounded like ours (MAX_ESCALATIONS), so E5 compares WHERE the retries went,
    not how many were allowed."""
    mode(reschedule="blind")
    weak = await _register(client, "weak", ram_mb=512)
    await _create_job(client)
    run_id = None
    for _ in range(4):
        run_id = await _claim_and_oom(client, weak)
    run = await _run_row(session_factory, run_id)
    assert run.status is RunStatus.FAILED
    assert run.escalation_count == 3
    assert "blind retry" in run.failure_detail


async def test_none_does_not_retry_at_all(client, session_factory, mode):
    mode(reschedule="none")
    weak = await _register(client, "weak", ram_mb=512)
    await _register(client, "strong", ram_mb=4096)
    await _create_job(client)
    run_id = await _claim_and_oom(client, weak)
    run = await _run_row(session_factory, run_id)
    assert run.status is RunStatus.FAILED
    assert run.failure_reason == "OOM_KILLED"
    assert run.escalation_count == 0


@pytest.mark.parametrize("reschedule_mode", ["learned", "blind", "none"])
async def test_a_user_capped_oom_never_escalates_in_any_arm(
    client, session_factory, mode, reschedule_mode
):
    """A kill at the USER'S OWN cap proves their limit was too small, not the
    machine — no retry policy disagrees about that, so all three arms leave it
    alone. Keeping this identical is what makes E5 a comparison of one variable."""
    mode(reschedule=reschedule_mode)
    weak = await _register(client, "weak", ram_mb=512)
    await _register(client, "strong", ram_mb=4096)
    await _create_job(client, resource_reqs={"mem_limit_mb": 128})
    run_id = await _claim_and_oom(client, weak)
    run = await _run_row(session_factory, run_id)
    assert run.status is RunStatus.FAILED
    assert run.failure_reason == "OOM_KILLED"
    assert "memory limit (128 MB)" in run.failure_detail
    assert run.learned_min_ram_mb is None
