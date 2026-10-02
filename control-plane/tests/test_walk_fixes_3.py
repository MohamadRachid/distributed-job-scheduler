"""Cancel a job (walk 1, row 64) — the one control a running job was missing.

No new state: a cancelled run ends FAILED with reason CANCELLED, and a request that
a worker holds is carried by a stamp the heartbeat and the reaper both read.
"""

from datetime import datetime, timedelta, timezone

from sqlalchemy import select

from app.api.jobs import may_cancel
from app.models import Run, RunStatus, User
from app.reaper import sweep_once

SPECS = {"cpu_cores": 4, "has_gpu": False, "ram_mb": 8192, "capacity": 4,
         "agent_version": "0.12.0"}


def _spec(**over):
    spec = {"name": "walk", "image": "fyp-dummy:latest",
            "entrypoint": ["python", "train.py"], "replicas": 1}
    spec.update(over)
    return spec


async def _register(client, name="lab-pc-01"):
    r = await client.post("/agent/register", json={"name": name, "specs": SPECS})
    assert r.status_code == 200, r.text
    return r.json()


async def _heartbeat(client, node, running=None):
    r = await client.post(
        "/agent/heartbeat",
        headers={"Authorization": f"Bearer {node['token']}"},
        json={"node_id": node["node_id"], "status": "idle", "running": running or []},
    )
    assert r.status_code == 200, r.text
    return r.json()


async def _status(client, node, run_id, attempt, state, **extra):
    body = {"attempt": attempt, "state": state, "exit_code": extra.pop("exit_code", None)}
    body.update(extra)
    return await client.post(
        f"/agent/runs/{run_id}/status",
        headers={"Authorization": f"Bearer {node['token']}"}, json=body,
    )


async def _runs(client, job_id):
    return (await client.get(f"/jobs/{job_id}/runs")).json()


# --- a run nobody holds ends at once ------------------------------------------------


async def test_cancelling_a_pending_run_ends_it_now_with_the_reason(client):
    created = (await client.post("/jobs", json=_spec(replicas=2))).json()
    r = await client.post(f"/jobs/{created['job_id']}/cancel")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["cancelled_now"] == 2 and body["cancel_requested"] == 0
    assert "had not started were ended" in body["detail"]
    rows = await _runs(client, created["job_id"])
    assert all(x["status"] == "FAILED" and x["failure_reason"] == "CANCELLED" for x in rows)
    assert "before it started" in rows[0]["failure_detail"]
    job = (await client.get(f"/jobs/{created['job_id']}")).json()
    assert job["status"] == "FAILED"
    # And nothing is offered to a worker afterwards.
    node = await _register(client)
    assert (await _heartbeat(client, node))["assignments"] == []


# --- a run a worker holds is told at its next heartbeat ---------------------------


async def test_cancelling_a_running_run_reaches_its_worker_as_a_command(client):
    created = (await client.post("/jobs", json=_spec())).json()
    node = await _register(client)
    a = (await _heartbeat(client, node))["assignments"][0]
    assert (await _status(client, node, a["run_id"], a["attempt"], "RUNNING")).status_code == 200

    r = await client.post(f"/jobs/{created['job_id']}/cancel")
    assert r.status_code == 200, r.text
    assert r.json()["cancel_requested"] == 1
    rows = await _runs(client, created["job_id"])
    assert rows[0]["status"] == "RUNNING"          # still held: the worker ends it
    assert rows[0]["cancel_requested_at"] is not None

    running = [{"run_id": a["run_id"], "attempt": a["attempt"], "state": "RUNNING"}]
    hb = await _heartbeat(client, node, running)
    assert hb["commands"] == [{"type": "cancel", "run_id": a["run_id"]}]

    # The worker stops the container and says why.
    r = await _status(client, node, a["run_id"], a["attempt"], "FAILED", exit_code=137,
                      failure_reason="CANCELLED",
                      failure_detail="Cancelled by the user; the worker stopped the container.")
    assert r.status_code == 200
    rows = await _runs(client, created["job_id"])
    assert rows[0]["status"] == "FAILED" and rows[0]["failure_reason"] == "CANCELLED"
    # Once it is over, the heartbeat has nothing more to say about it.
    assert (await _heartbeat(client, node))["commands"] == []


async def test_a_bare_kill_posted_after_a_cancel_is_still_labelled_cancelled(client):
    """An older agent, or one that classified the stop as the signal it saw."""
    created = (await client.post("/jobs", json=_spec())).json()
    node = await _register(client)
    a = (await _heartbeat(client, node))["assignments"][0]
    await _status(client, node, a["run_id"], a["attempt"], "RUNNING")
    await client.post(f"/jobs/{created['job_id']}/cancel")
    r = await _status(client, node, a["run_id"], a["attempt"], "FAILED", exit_code=137,
                      failure_reason="KILLED", failure_detail="SIGKILL")
    assert r.status_code == 200
    rows = await _runs(client, created["job_id"])
    assert rows[0]["failure_reason"] == "CANCELLED"
    assert "Cancelled by the user" in rows[0]["failure_detail"]


async def test_a_result_that_finished_before_the_stop_landed_stands(client):
    created = (await client.post("/jobs", json=_spec())).json()
    node = await _register(client)
    a = (await _heartbeat(client, node))["assignments"][0]
    await _status(client, node, a["run_id"], a["attempt"], "RUNNING")
    await client.post(f"/jobs/{created['job_id']}/cancel")
    r = await _status(client, node, a["run_id"], a["attempt"], "SUCCEEDED", exit_code=0)
    assert r.status_code == 200
    rows = await _runs(client, created["job_id"])
    assert rows[0]["status"] == "SUCCEEDED" and rows[0]["failure_reason"] is None


# --- a worker that never answers: the reaper finishes the cancel ---------------------


async def test_the_reaper_ends_a_stamped_run_as_cancelled_instead_of_requeueing_it(
    client, session_factory
):
    created = (await client.post("/jobs", json=_spec())).json()
    node = await _register(client)
    a = (await _heartbeat(client, node))["assignments"][0]
    await _status(client, node, a["run_id"], a["attempt"], "RUNNING")
    await client.post(f"/jobs/{created['job_id']}/cancel")

    # The worker dies; its lease expires.
    async with session_factory() as s:
        run = (await s.execute(select(Run).where(Run.id == a["run_id"]))).scalar_one()
        assert run.retries_remaining > 0, "a requeue was on the table"
        run.lease_expires_at = datetime.now(timezone.utc) - timedelta(seconds=5)
        await s.commit()
    decisions = await sweep_once(session_factory)
    assert [d["decision"] for d in decisions] == ["cancelled"]
    rows = await _runs(client, created["job_id"])
    assert rows[0]["status"] == "FAILED"
    assert rows[0]["failure_reason"] == "CANCELLED"
    assert "did not confirm the stop" in rows[0]["failure_detail"]
    assert rows[0]["attempt"] == a["attempt"], "never requeued, never re-claimed"


async def test_an_unstamped_expired_lease_is_still_requeued(client, session_factory):
    """The recovery path the project exists for is untouched by the cancel branch."""
    created = (await client.post("/jobs", json=_spec())).json()
    node = await _register(client)
    a = (await _heartbeat(client, node))["assignments"][0]
    await _status(client, node, a["run_id"], a["attempt"], "RUNNING")
    async with session_factory() as s:
        run = (await s.execute(select(Run).where(Run.id == a["run_id"]))).scalar_one()
        run.lease_expires_at = datetime.now(timezone.utc) - timedelta(seconds=5)
        await s.commit()
    decisions = await sweep_once(session_factory)
    assert [d["decision"] for d in decisions] == ["requeued"]
    assert (await _runs(client, created["job_id"]))[0]["status"] == "PENDING"


# --- who may cancel, and what is left alone --------------------------------------


def test_who_may_cancel():
    owner = User(id="u1", username="alice", password_hash="x", is_admin=False)
    other = User(id="u2", username="bob", password_hash="x", is_admin=False)
    admin = User(id="u3", username="root", password_hash="x", is_admin=True)

    class J:
        user_id = "u1"

    class Unowned:
        user_id = None

    assert may_cancel(owner, J()) is True
    assert may_cancel(other, J()) is False
    assert may_cancel(admin, J()) is True
    assert may_cancel(other, Unowned()) is True
    assert may_cancel(object(), J()) is False


async def test_cancel_is_idempotent_and_leaves_finished_runs_alone(client):
    created = (await client.post("/jobs", json=_spec())).json()
    node = await _register(client)
    a = (await _heartbeat(client, node))["assignments"][0]
    await _status(client, node, a["run_id"], a["attempt"], "RUNNING")
    await _status(client, node, a["run_id"], a["attempt"], "SUCCEEDED", exit_code=0)
    for _ in range(2):
        r = await client.post(f"/jobs/{created['job_id']}/cancel")
        assert r.status_code == 200
        assert r.json()["already_finished"] == 1
        assert r.json()["cancel_requested"] == 0
    assert (await _runs(client, created["job_id"]))[0]["status"] == "SUCCEEDED"


async def test_cancelling_an_unknown_job_is_404(client):
    assert (await client.post("/jobs/no-such-job/cancel")).status_code == 404


async def test_the_run_read_shape_carries_the_stamp(client):
    created = (await client.post("/jobs", json=_spec())).json()
    rows = await _runs(client, created["job_id"])
    assert rows[0]["cancel_requested_at"] is None
    assert RunStatus.FAILED.value == "FAILED"
