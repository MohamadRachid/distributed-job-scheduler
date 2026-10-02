"""The private-input silent no-op gets flagged.

**The defect these tests pin.** A private job is sealed at submit and placed on a
trusted node, and all of that works. But the wrapper that redeems the one-shot
ticket, decrypts into memory and verifies the seal is chosen by the *client* — the
browser hard-codes it, the API requires nothing. Submit the same job with a plain
entrypoint and the container never opens the file: the run finishes **SUCCEEDED,
with no warning**, and the user is told their private input was used when it never
reached the workload.

Nothing leaks. The data stays sealed and the trust gate holds. What is wrong is that
the platform reports a success it cannot support.

**Updated 2026-09-06 (sealed by default).** The check now asks whether the job had a
SEALED INPUT to open, rather than whether it was flagged private — the same question,
asked of the population that carries sealed data today. A job with no input file is
excluded, because a workload that was handed nothing to read cannot be accused of not
reading it.

**Why the ticket is the signal.** The agent requests a single-use ticket for every
sealed run *before* the container starts (`_stage_sealed`), so a ticket row always
exists. The container is the only thing that can redeem it. So a private run that
finishes with its ticket still unredeemed is a run whose input was never opened — an
event we *observe*, rather than a string we guess at by matching the entrypoint.

**Flagged, not failed.** The program exited zero and we do not rewrite that. Turning
a clean exit into a failure would conflate "the workload ran" with "the workload used
its input", and it would change accepted-result semantics three weeks from the
deadline. The run stays SUCCEEDED and carries a classified reason, which is where
every other diagnosis already lives (NFR-8), and which the interface already renders
on the presence of a reason.

These tests pin the DEFECT rather than the fix: the clean case must stay silent, and
the broken case must stop being silent. Written before the code, so the second one
failed first.
"""

import json

import pytest

from app.diagnostics import (
    INPUT_NOT_OPENED,
    PRIVATE_INPUT_NOT_OPENED,
    decide_private_input_outcome,
)
from app.models import Run, RunStatus

SPECS = {"cpu_cores": 4, "has_gpu": False, "ram_mb": 8192, "capacity": 4, "agent_version": "0.12.0"}
# The claim query only offers a sealed run to an agent at/after this version, so a
# node below it — or one that omits the field — is invisible to today's work and would
# never claim any of it. 0.8.0 was enough when this file was written, because the
# guard that mattered then was the older private one.
NEW_AGENT = dict(SPECS, agent_version="0.12.0")
SECRET = b"col_a,col_b\n1,2\n"


# --- helpers (self-contained, per this suite's convention) -------------------


def _node_auth(node):
    return {"Authorization": f"Bearer {node['token']}"}


async def _register(client, name="trusted-pc"):
    r = await client.post("/agent/register", json={"name": name, "specs": NEW_AGENT})
    assert r.status_code == 200, r.text
    return r.json()


async def _trust(client, node):
    r = await client.patch(f"/nodes/{node['node_id']}/trusted", json={"trusted": True})
    assert r.status_code == 200, r.text


def _spec(**over):
    spec = {
        "name": "private-job",
        "image": "fyp-dummy:latest",
        "entrypoint": ["python", "fyp_open.py", "python", "train.py", "--private-input"],
        "replicas": 1,
    }
    spec.update(over)
    return spec


async def _submit_private(client, **over):
    r = await client.post(
        "/jobs/private",
        data={"spec": json.dumps(_spec(**over))},
        files={"file": ("secret.csv", SECRET, "text/csv")},
    )
    assert r.status_code == 200, r.text
    return r.json()


async def _submit_public(client, **over):
    r = await client.post("/jobs", json=_spec(**over))
    assert r.status_code == 200, r.text
    return r.json()


async def _heartbeat(client, node):
    r = await client.post(
        "/agent/heartbeat",
        headers=_node_auth(node),
        json={"node_id": node["node_id"], "status": "idle", "running": []},
    )
    assert r.status_code == 200, r.text
    return r.json()["assignments"]


async def _claim_private(client, name="trusted-pc"):
    """A trusted node holding a private run at its current attempt."""
    node = await _register(client, name=name)
    await _trust(client, node)
    await _submit_private(client)
    return node, (await _heartbeat(client, node))[0]


async def _take_a_ticket(client, node, a):
    """What the agent does for EVERY private run, before the container starts."""
    t = await client.post(
        f"/agent/runs/{a['run_id']}/key-ticket?attempt={a['attempt']}",
        headers=_node_auth(node),
    )
    assert t.status_code == 200, t.text
    return t.json()["ticket"]


async def _open_the_input(client, node, a):
    """What a CORRECT private workload adds on top: the container redeems the ticket
    once. This redemption is the only thing separating the two runs below."""
    ticket = await _take_a_ticket(client, node, a)
    redeem = await client.post("/container/key", json={"ticket": ticket})
    assert redeem.status_code == 200, redeem.text


async def _succeed(client, node, a):
    r = await client.post(
        f"/agent/runs/{a['run_id']}/status",
        headers=_node_auth(node),
        json={"attempt": a["attempt"], "state": "SUCCEEDED", "exit_code": 0},
    )
    assert r.status_code == 200, r.text
    return r.json()


async def _read_run(session_factory, run_id) -> Run:
    async with session_factory() as s:
        return await s.get(Run, run_id)


# --- the pure decision ------------------------------------------------------


def test_only_a_private_run_with_an_unopened_input_is_flagged():
    """Four cases, one table. The decision is pure so it can be read at a glance."""
    assert decide_private_input_outcome(is_private=True, input_opened=True) == (None, None)
    assert decide_private_input_outcome(is_private=False, input_opened=False) == (None, None)
    assert decide_private_input_outcome(is_private=False, input_opened=True) == (None, None)

    reason, detail = decide_private_input_outcome(is_private=True, input_opened=False)
    assert reason == PRIVATE_INPUT_NOT_OPENED
    assert detail and "entrypoint" in detail.lower()


# --- the two runs that differ by one thing ----------------------------------


async def test_a_private_run_that_opens_its_input_finishes_clean(client, session_factory):
    """The correct case must stay completely silent. A warning on a good run would be
    worse than the defect: people learn to ignore a badge that cries wolf."""
    node, a = await _claim_private(client)
    await _open_the_input(client, node, a)

    body = await _succeed(client, node, a)
    assert body["run_status"] == "SUCCEEDED"

    run = await _read_run(session_factory, a["run_id"])
    assert run.status is RunStatus.SUCCEEDED
    assert run.failure_reason is None
    assert run.failure_detail is None


async def test_a_private_run_that_never_opens_its_input_is_flagged(client, session_factory):
    """THE DEFECT. Identical to the test above in every respect except that the
    container never redeems the ticket — which is exactly what happens when the
    entrypoint does not call the opener. Before the fix this finished SUCCEEDED with
    no reason at all, and the user was told it worked."""
    node, a = await _claim_private(client)
    await _take_a_ticket(client, node, a)

    body = await _succeed(client, node, a)

    # Flagged, NOT failed — the program really did exit zero and we do not rewrite that.
    assert body["run_status"] == "SUCCEEDED"
    run = await _read_run(session_factory, a["run_id"])
    assert run.status is RunStatus.SUCCEEDED
    assert run.exit_code == 0

    # …but it no longer finishes silently. Since 2026-09-06 the private door builds
    # the SEALED shape, so the label is the sealed one and the message names the
    # reader the workload should have used (walk 1, row 58); the old label is kept
    # for the rows that still carry `jobs.private`, and its own unit test above
    # pins that.
    assert run.failure_reason == INPUT_NOT_OPENED
    assert run.failure_detail and "fyp_data.open_input()" in run.failure_detail


# --- the boundary -----------------------------------------------------------


async def test_an_ordinary_run_is_never_flagged(client, session_factory):
    """A public job has no sealed input and no ticket, so the absence of a redemption
    means nothing. The check must not reach it."""
    node = await _register(client, name="ordinary-pc")
    await _submit_public(client)
    a = (await _heartbeat(client, node))[0]

    await _succeed(client, node, a)

    run = await _read_run(session_factory, a["run_id"])
    assert run.status is RunStatus.SUCCEEDED
    assert run.failure_reason is None


async def test_a_failed_private_run_keeps_its_own_reason(client, session_factory):
    """A private run that never opened its input AND then crashed must report the
    crash. The real cause outranks the flag — overwriting it would hide the thing the
    user actually has to fix."""
    node, a = await _claim_private(client)
    await _take_a_ticket(client, node, a)

    r = await client.post(
        f"/agent/runs/{a['run_id']}/status",
        headers=_node_auth(node),
        json={
            "attempt": a["attempt"],
            "state": "FAILED",
            "exit_code": 1,
            "failure_reason": "APP_ERROR",
            "failure_detail": "Traceback ...",
        },
    )
    assert r.status_code == 200, r.text

    run = await _read_run(session_factory, a["run_id"])
    assert run.status is RunStatus.FAILED
    assert run.failure_reason == "APP_ERROR"


@pytest.mark.parametrize("state", ["RUNNING"])
async def test_a_running_private_run_is_not_flagged_early(client, session_factory, state):
    """The ticket is redeemed at some point DURING the run, so the question can only be
    asked once the run is over. Asking while it is still running would flag every
    private run for the first few seconds of its life."""
    node, a = await _claim_private(client)

    r = await client.post(
        f"/agent/runs/{a['run_id']}/status",
        headers=_node_auth(node),
        json={"attempt": a["attempt"], "state": state},
    )
    assert r.status_code == 200, r.text

    run = await _read_run(session_factory, a["run_id"])
    assert run.status is RunStatus.RUNNING
    assert run.failure_reason is None
