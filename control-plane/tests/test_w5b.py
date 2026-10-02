"""W5b diagnostics tests — run reasons, samples, progress, node postmortem.

Three groups:
  * PURE classifiers (app.diagnostics): classify_lost (NODE_LOST vs RUN_LOST),
    reason_from_exit_code, classify_comeback (one case per cause + the priority order
    + the "likely" battery refinement). These are the honesty rules as code.
  * The REAPER's LOST context, driven through the real sweep_once (seeded), proving
    the two loss stories are attached to the run — WITHOUT changing the W5 transition.
  * The ENDPOINTS: samples (fenced + idempotent), progress on the heartbeat (fenced),
    failure reasons on status (+ cleared on re-run/success, + old-agent exit fallback),
    goodbye + comeback -> node_events, battery, and OLD-AGENT COMPATIBILITY (every new
    field omitted still 200s) — what makes this a safe additive edit to the frozen wall.

`now` is injected where "the lease expired" must be deterministic (no sleeping).
"""

from datetime import datetime, timedelta, timezone

from app.diagnostics import classify_comeback, classify_lost, reason_from_exit_code
from app.models import Job, JobStatus, Node, NodeStatus, Run, RunStatus
from app.reaper import sweep_once

NOW = datetime(2026, 7, 16, 12, 0, 0, tzinfo=timezone.utc)
SPECS = {"cpu_cores": 4, "has_gpu": False, "ram_mb": 8192, "capacity": 4, "agent_version": "0.12.0"}


# ===========================================================================
# 1) Pure classifiers
# ===========================================================================


class _FakeNode:
    def __init__(self, name, last_heartbeat):
        self.name = name
        self.last_heartbeat = last_heartbeat


def test_classify_lost_node_went_silent_is_node_lost():
    lease = NOW - timedelta(seconds=5)
    node = _FakeNode("node-a", NOW - timedelta(seconds=20))  # last beat BEFORE the lease
    reason, detail = classify_lost(node, lease)
    assert reason == "NODE_LOST"
    assert "stopped heart-beating" in detail
    assert "15s" in detail  # gap = lease - last_heartbeat


def test_classify_lost_node_alive_but_run_silent_is_run_lost():
    lease = NOW - timedelta(seconds=20)
    node = _FakeNode("node-a", NOW - timedelta(seconds=2))  # still beating AFTER the lease
    reason, detail = classify_lost(node, lease)
    assert reason == "RUN_LOST"
    assert "alive but stopped reporting this run" in detail


def test_classify_lost_exhausted_wording():
    reason, detail = classify_lost(_FakeNode("n", NOW - timedelta(seconds=99)), NOW, exhausted=True)
    assert reason == "NODE_LOST"
    assert detail.startswith("Run lost and no retries left")


def test_reason_from_exit_code_decodes_signals():
    assert reason_from_exit_code(0) == (None, None)
    assert reason_from_exit_code(None) == (None, None)
    assert reason_from_exit_code(137)[0] == "KILLED"
    assert reason_from_exit_code(139)[0] == "APP_CRASH"
    assert reason_from_exit_code(143)[0] == "TERMINATED"
    reason, detail = reason_from_exit_code(1)
    assert reason == "APP_ERROR" and "code 1" in detail


def test_comeback_network_partition():
    cause, detail = classify_comeback(
        {"failed_deliveries": [{"ts": 1000.0, "error": "URLError"}, {"ts": 1060.0, "error": "URLError"}]}
    )
    assert cause == "NETWORK_PARTITION"
    assert "2 heartbeat" in detail


def test_comeback_slept():
    cause, detail = classify_comeback({"slept_ranges": [[1000.0, 1200.0]]})
    assert cause == "SLEPT"
    assert "slept" in detail.lower()


def test_comeback_agent_crash():
    cause, detail = classify_comeback({"new_agent_session": True, "reboot": False, "dirty_shutdown": True})
    assert cause == "AGENT_CRASH"
    assert "never rebooted" in detail


def test_comeback_clean_shutdown():
    cause, _ = classify_comeback({"reboot": True, "dirty_shutdown": False, "new_agent_session": True})
    assert cause == "CLEAN_SHUTDOWN"


def test_comeback_power_loss_generic():
    cause, detail = classify_comeback({"reboot": True, "dirty_shutdown": True, "new_agent_session": True})
    assert cause == "POWER_LOSS_OR_CRASH"
    assert "likely" in detail


def test_comeback_power_loss_refined_to_battery():
    cause, detail = classify_comeback(
        {"reboot": True, "dirty_shutdown": True, "new_agent_session": True},
        last_battery_pct=4.0,
        last_battery_charging=False,
    )
    assert cause == "POWER_LOSS_OR_CRASH"
    assert "likely battery" in detail


def test_comeback_priority_partition_beats_sleep():
    # Both signals present -> partition wins (priority 1), because it PROVES the
    # machine was alive the whole time.
    cause, _ = classify_comeback(
        {"failed_deliveries": [{"ts": 1.0, "error": "URLError"}], "slept_ranges": [[1.0, 2.0]]}
    )
    assert cause == "NETWORK_PARTITION"


def test_comeback_nothing_notable_is_none():
    assert classify_comeback({}) == (None, None)
    assert classify_comeback(None) == (None, None)


# ===========================================================================
# 2) The reaper attaches LOST context (real sweep_once; no transition change)
# ===========================================================================


async def _seed_run(factory, *, node_last_hb, lease, retries=1, status=RunStatus.RUNNING, attempt=1):
    async with factory() as s:
        n = Node(
            name="reaper-node", status=NodeStatus.idle, cpu_cores=4, has_gpu=False,
            ram_mb=8192, capacity=4, token_hash="h" * 64, last_heartbeat=node_last_hb,
        )
        job = Job(name="j", image="fyp-dummy:latest", status=JobStatus.RUNNING, replicas=1)
        s.add_all([n, job])
        await s.flush()
        run = Run(
            job_id=job.id, node_id=n.id, status=status, attempt=attempt,
            lease_expires_at=lease, retries_remaining=retries,
        )
        s.add(run)
        await s.commit()
        return run.id


async def _get_run(factory, run_id):
    async with factory() as s:
        return await s.get(Run, run_id)


async def test_reaper_marks_node_lost_when_node_went_silent(session_factory):
    run_id = await _seed_run(
        session_factory, node_last_hb=NOW - timedelta(seconds=20), lease=NOW - timedelta(seconds=5)
    )
    await sweep_once(session_factory=session_factory, now=NOW)
    run = await _get_run(session_factory, run_id)
    assert run.status == RunStatus.PENDING          # requeued (W5 transition unchanged)
    assert run.failure_reason == "NODE_LOST"        # W5b context attached
    assert "stopped heart-beating" in run.failure_detail


async def test_reaper_marks_run_lost_when_node_alive(session_factory):
    run_id = await _seed_run(
        session_factory, node_last_hb=NOW - timedelta(seconds=2), lease=NOW - timedelta(seconds=20)
    )
    await sweep_once(session_factory=session_factory, now=NOW)
    run = await _get_run(session_factory, run_id)
    assert run.failure_reason == "RUN_LOST"
    assert "alive but stopped reporting" in run.failure_detail


async def test_reaper_exhausted_carries_lost_context(session_factory):
    run_id = await _seed_run(
        session_factory, node_last_hb=NOW - timedelta(seconds=20), lease=NOW - timedelta(seconds=5),
        retries=0,
    )
    await sweep_once(session_factory=session_factory, now=NOW)
    run = await _get_run(session_factory, run_id)
    assert run.status == RunStatus.FAILED           # terminal (W5 transition unchanged)
    assert run.failure_reason == "NODE_LOST"
    assert run.failure_detail.startswith("Run lost and no retries left")
    assert run.exit_code is None                     # infra loss, not a bad exit


# ===========================================================================
# 3) Endpoints — samples, progress, reasons, goodbye/comeback, battery, compat
# ===========================================================================


async def _register(client, name="lab-pc-01", specs=None):
    resp = await client.post("/agent/register", json={"name": name, "specs": specs or SPECS})
    assert resp.status_code == 200, resp.text
    return resp.json()


async def _assigned_run(client, node, **job_overrides):
    body = {
        "name": "j", "image": "fyp-dummy:latest", "entrypoint": ["python", "train.py"],
        "env": {"EPOCHS": "1"}, "resource_reqs": {"needs_gpu": False}, "replicas": 1,
    }
    body.update(job_overrides)
    job = (await client.post("/jobs", json=body)).json()
    resp = await client.post(
        "/agent/heartbeat",
        headers={"Authorization": f"Bearer {node['token']}"},
        json={"node_id": node["node_id"], "status": "idle", "running": []},
    )
    assert resp.status_code == 200, resp.text
    return job["job_id"], resp.json()["assignments"][0]["run_id"]


async def _run_row(session_factory, run_id):
    async with session_factory() as s:
        return await s.get(Run, run_id)


# --- samples: fenced + idempotent -------------------------------------------


async def test_samples_stored_and_read_back(client):
    node = await _register(client)
    _, run_id = await _assigned_run(client, node)
    resp = await client.post(
        f"/agent/runs/{run_id}/samples",
        headers={"Authorization": f"Bearer {node['token']}"},
        json={"attempt": 1, "samples": [
            {"ts": 100.0, "cpu_pct": 20.0, "mem_used_mb": 40.0, "mem_limit_mb": 128.0},
            {"ts": 103.0, "cpu_pct": 25.0, "mem_used_mb": 90.0, "mem_limit_mb": 128.0},
        ]},
    )
    assert resp.status_code == 200 and resp.json()["stored"] == 2
    rows = (await client.get(f"/runs/{run_id}/samples")).json()
    assert [r["mem_used_mb"] for r in rows] == [40.0, 90.0]  # ordered by ts


async def test_samples_resend_is_idempotent(client):
    node = await _register(client)
    _, run_id = await _assigned_run(client, node)
    batch = {"attempt": 1, "samples": [{"ts": 100.0, "cpu_pct": 20.0, "mem_used_mb": 40.0, "mem_limit_mb": 128.0}]}
    hdr = {"Authorization": f"Bearer {node['token']}"}
    first = await client.post(f"/agent/runs/{run_id}/samples", headers=hdr, json=batch)
    second = await client.post(f"/agent/runs/{run_id}/samples", headers=hdr, json=batch)  # resend
    assert first.json()["stored"] == 1
    assert second.json()["stored"] == 0                      # deduped on (run_id, attempt, ts)
    rows = (await client.get(f"/runs/{run_id}/samples")).json()
    assert len(rows) == 1                                    # no duplicate row


async def test_samples_stale_attempt_is_409(client):
    node = await _register(client)
    _, run_id = await _assigned_run(client, node)
    resp = await client.post(
        f"/agent/runs/{run_id}/samples",
        headers={"Authorization": f"Bearer {node['token']}"},
        json={"attempt": 99, "samples": [{"ts": 1.0}]},
    )
    assert resp.status_code == 409


async def test_samples_wrong_node_is_409(client):
    owner = await _register(client, name="owner")
    other = await _register(client, name="other")
    _, run_id = await _assigned_run(client, owner)
    resp = await client.post(
        f"/agent/runs/{run_id}/samples",
        headers={"Authorization": f"Bearer {other['token']}"},
        json={"attempt": 1, "samples": [{"ts": 1.0}]},
    )
    assert resp.status_code == 409


# --- progress rides the (fenced) heartbeat ----------------------------------


async def test_progress_stored_via_heartbeat(client, session_factory):
    node = await _register(client)
    _, run_id = await _assigned_run(client, node)
    resp = await client.post(
        "/agent/heartbeat",
        headers={"Authorization": f"Bearer {node['token']}"},
        json={"node_id": node["node_id"], "status": "busy", "running": [
            {"run_id": run_id, "attempt": 1, "state": "RUNNING", "progress": 0.4,
             "metrics": {"epoch": 2, "total": 5, "loss": 0.5}},
        ]},
    )
    assert resp.status_code == 200
    run = await _run_row(session_factory, run_id)
    assert run.progress == 0.4
    assert run.metrics_last == {"epoch": 2, "total": 5, "loss": 0.5}


async def test_progress_from_stale_attempt_is_ignored(client, session_factory):
    node = await _register(client)
    _, run_id = await _assigned_run(client, node)
    await client.post(
        "/agent/heartbeat",
        headers={"Authorization": f"Bearer {node['token']}"},
        json={"node_id": node["node_id"], "status": "busy", "running": [
            {"run_id": run_id, "attempt": 99, "state": "RUNNING", "progress": 0.9},
        ]},
    )
    run = await _run_row(session_factory, run_id)
    assert run.progress is None                              # fenced — a stale report can't write


# --- failure reasons on status ----------------------------------------------


async def _post_status(client, node, run_id, **body):
    return await client.post(
        f"/agent/runs/{run_id}/status",
        headers={"Authorization": f"Bearer {node['token']}"},
        json={"attempt": 1, **body},
    )


async def test_failed_status_stores_reason(client, session_factory):
    # A FAILED post stores the agent's classified reason verbatim. Uses a non-OOM
    # reason on purpose: OOM_KILLED now triggers W5c failure-aware rescheduling
    # (tested in test_w5c.py), so a plain "the reason is stored" check uses a hard
    # fact that stays put.
    node = await _register(client)
    _, run_id = await _assigned_run(client, node)
    resp = await _post_status(
        client, node, run_id, state="FAILED", exit_code=1,
        failure_reason="GPU_UNAVAILABLE", failure_detail="GPU unavailable: no usable driver.",
    )
    assert resp.status_code == 200
    run = await _run_row(session_factory, run_id)
    assert run.failure_reason == "GPU_UNAVAILABLE"
    assert "GPU unavailable" in run.failure_detail


async def test_failed_status_without_reason_uses_exit_fallback(client, session_factory):
    # An OLDER agent reports FAILED with just an exit code — the server derives the
    # minimal hard-fact reason itself, so the run still explains itself.
    node = await _register(client)
    _, run_id = await _assigned_run(client, node)
    await _post_status(client, node, run_id, state="FAILED", exit_code=1)
    run = await _run_row(session_factory, run_id)
    assert run.failure_reason == "APP_ERROR"
    assert "code 1" in run.failure_detail


async def test_running_clears_a_stale_reason(client, session_factory):
    node = await _register(client)
    _, run_id = await _assigned_run(client, node)
    async with session_factory() as s:  # simulate a prior loss reason on the run
        run = await s.get(Run, run_id)
        run.failure_reason, run.failure_detail = "NODE_LOST", "lost once"
        await s.commit()
    await _post_status(client, node, run_id, state="RUNNING")
    run = await _run_row(session_factory, run_id)
    assert run.failure_reason is None                        # re-running -> no longer lost


async def test_succeeded_has_no_reason(client, session_factory):
    node = await _register(client)
    _, run_id = await _assigned_run(client, node)
    async with session_factory() as s:
        run = await s.get(Run, run_id)
        run.failure_reason = "NODE_LOST"
        await s.commit()
    await _post_status(client, node, run_id, state="SUCCEEDED", exit_code=0)
    run = await _run_row(session_factory, run_id)
    assert run.failure_reason is None                        # success gets no reason


async def test_run_reason_surfaces_in_job_runs_read(client):
    node = await _register(client)
    job_id, run_id = await _assigned_run(client, node)
    await _post_status(
        client, node, run_id, state="FAILED", exit_code=1,
        failure_reason="APP_ERROR", failure_detail="exited with code 1",
    )
    runs = (await client.get(f"/jobs/{job_id}/runs")).json()
    assert runs[0]["failure_reason"] == "APP_ERROR"


# --- node postmortem: goodbye, comeback, battery ----------------------------


async def test_goodbye_records_clean_shutdown_event(client):
    node = await _register(client)
    resp = await client.post(
        "/agent/goodbye",
        headers={"Authorization": f"Bearer {node['token']}"},
        json={"reason": "shutdown"},
    )
    assert resp.status_code == 200
    events = (await client.get(f"/nodes/{node['node_id']}/events")).json()
    assert events[0]["event"] == "GOODBYE"
    assert events[0]["cause"] == "CLEAN_SHUTDOWN"


async def test_comeback_interview_records_a_classified_event(client):
    node = await _register(client)
    await client.post(
        "/agent/heartbeat",
        headers={"Authorization": f"Bearer {node['token']}"},
        json={"node_id": node["node_id"], "status": "idle", "running": [],
              "interview": {"failed_deliveries": [{"ts": 1000.0, "error": "URLError"}]}},
    )
    events = (await client.get(f"/nodes/{node['node_id']}/events")).json()
    assert events[0]["cause"] == "NETWORK_PARTITION"
    assert "detail" in events[0]["evidence"]


async def test_battery_stored_and_returned(client):
    node = await _register(client)
    await client.post(
        "/agent/heartbeat",
        headers={"Authorization": f"Bearer {node['token']}"},
        json={"node_id": node["node_id"], "status": "idle", "running": [],
              "battery_pct": 55.0, "battery_charging": False},
    )
    n = next(x for x in (await client.get("/nodes")).json() if x["node_id"] == node["node_id"])
    assert n["battery_pct"] == 55.0
    assert n["battery_charging"] is False


# --- OLD-AGENT COMPATIBILITY (the safety of an additive wall edit) ----------


async def test_old_heartbeat_shape_still_works(client):
    # The exact pre-W5b heartbeat — no battery, no interview, running items with no
    # progress/metrics. Must 200 and store nothing new.
    node = await _register(client)
    resp = await client.post(
        "/agent/heartbeat",
        headers={"Authorization": f"Bearer {node['token']}"},
        json={"node_id": node["node_id"], "status": "idle", "running": []},
    )
    assert resp.status_code == 200
    n = next(x for x in (await client.get("/nodes")).json() if x["node_id"] == node["node_id"])
    assert n["battery_pct"] is None and n["battery_charging"] is None


async def test_old_status_shape_still_works(client, session_factory):
    # A terminal status with no failure_reason field at all (pre-W5b agent).
    node = await _register(client)
    _, run_id = await _assigned_run(client, node)
    resp = await client.post(
        f"/agent/runs/{run_id}/status",
        headers={"Authorization": f"Bearer {node['token']}"},
        json={"attempt": 1, "state": "SUCCEEDED", "exit_code": 0},
    )
    assert resp.status_code == 200
    run = await _run_row(session_factory, run_id)
    assert run.status == RunStatus.SUCCEEDED
