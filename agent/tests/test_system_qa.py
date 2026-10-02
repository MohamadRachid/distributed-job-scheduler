"""Final delivery must survive transient failures before cleanup."""

import pytest

from agent import agent as agent_mod
from agent.runner import Runner
from agent.tests.test_audit_fixes import (
    RUN_ID, _start_with_one_result, client as docker_client_fixture,
)

client = docker_client_fixture


@pytest.mark.parametrize("failed_delivery", ["artifact", "logs"])
def test_final_delivery_is_retried_before_success(monkeypatch, client, failed_delivery):
    runner = Runner()
    _start_with_one_result(runner)
    monkeypatch.setattr(client.started[0], "logs", lambda **kw: b"last line\n")
    posted = []
    monkeypatch.setattr(agent_mod, "post_run_status",
                        lambda *a, **kw: posted.append(a[4]) or "accepted")
    monkeypatch.setattr(agent_mod, "post_run_artifact",
                        lambda *a, **kw: "retry" if failed_delivery == "artifact" else "accepted")
    monkeypatch.setattr(agent_mod, "post_run_logs",
                        lambda *a, **kw: "retry" if failed_delivery == "logs" else "accepted")
    agent_mod._drain_finished("http://qa", "test", runner)
    assert posted == [], "must not report completion while final data is undelivered"
    assert runner.has(RUN_ID), "keep container and output available for retry"

    delivered = []
    monkeypatch.setattr(agent_mod, "post_run_artifact",
                        lambda *a, **kw: delivered.append("artifact") or "accepted")
    monkeypatch.setattr(agent_mod, "post_run_logs",
                        lambda *a, **kw: delivered.append("logs") or "accepted")
    agent_mod._drain_finished("http://qa", "test", runner)
    assert failed_delivery in delivered
    assert posted == ["SUCCEEDED"]
    assert not runner.has(RUN_ID)


class _ChattyRunner:
    """A run whose container prints faster than one post round-trip: every pass finds
    new lines. Raises instead of hanging, so an unbounded loop FAILS the test."""

    def __init__(self, limit: int = 50) -> None:
        self.calls = 0
        self.confirmed = 0
        self.limit = limit

    def collect_logs(self, run_id, final=False):
        self.calls += 1
        if self.calls > self.limit:
            raise AssertionError("the log drain never gave the tick back to the heartbeat")
        return self.confirmed, f"line {self.confirmed}\n"

    def attempt_of(self, run_id):
        return 1

    def confirm_logs(self, run_id):
        self.confirmed += 1

    def abort(self, run_id, **_kw):
        raise AssertionError("must not abort a healthy run")


def test_chatty_workload_cannot_hold_the_tick(monkeypatch):
    """2026-09-07 system QA: a container printing every 0.5 s against posts taking 2 s
    kept `_stream_logs` looping, so the heartbeat never went out, the node showed
    offline, the checkpoint sweep never ran and the lease reclaimed a healthy run as
    LOST. The drain must stop after a bounded number of batches and leave the rest,
    cursor intact, for the next tick.

    This pins `_stream_logs`'s OWN cap, which is what the two final-flush sites use.
    The heartbeat's streaming pass is bounded worker-wide instead, one batch per run
    visited, and `test_chatty_workloads_share_one_tick_budget` is what pins that."""
    runner = _ChattyRunner()
    monkeypatch.setattr(agent_mod, "post_run_logs", lambda *a, **kw: "accepted")
    pending = agent_mod._stream_logs("http://qa", "test", runner, "run-chatty")
    assert runner.confirmed == agent_mod.MAX_LOG_BATCHES_PER_TICK
    assert pending is False, "lines remain behind the cap; the next tick continues"


def test_chatty_workloads_share_one_tick_budget(monkeypatch):
    """The heartbeat bound belongs to the whole worker tick, not to each run.

    A worker can execute several containers.  Giving every chatty container the full
    allowance multiplies the delay before the heartbeat by worker capacity and
    recreates the starvation this cap is meant to prevent.
    """

    class Many:
        def __init__(self):
            self.confirmed = {f"run-{i}": 0 for i in range(4)}

        def tracked_ids(self):
            return list(self.confirmed)

        def collect_logs(self, run_id, final=False):
            seq = self.confirmed[run_id]
            return seq, f"{run_id} line {seq}\n"

        def attempt_of(self, run_id):
            return 1

        def confirm_logs(self, run_id):
            self.confirmed[run_id] += 1

        def abort(self, run_id, **_kw):
            raise AssertionError("must not abort a healthy run")

    runner = Many()
    posted = []
    monkeypatch.setattr(agent_mod, "_log_pump_offset", 0, raising=False)
    monkeypatch.setattr(
        agent_mod, "post_run_logs",
        lambda _s, _t, run_id, *_a, **_kw: posted.append(run_id) or "accepted",
    )

    agent_mod._pump_all_logs("http://qa", "test", runner)
    assert len(posted) <= agent_mod.MAX_LOG_BATCHES_PER_TICK

    agent_mod._pump_all_logs("http://qa", "test", runner)
    assert set(posted) == set(runner.tracked_ids()), "the shared cap must rotate fairly"


def test_final_flush_of_an_exited_container_still_completes(monkeypatch):
    """The cap must not cost a finished run its last lines: an exited container's log
    is finite, so the first pass takes all of it and the second finds nothing, and the
    terminal status may go out."""

    class Finite(_ChattyRunner):
        def collect_logs(self, run_id, final=False):
            self.calls += 1
            return None if self.confirmed else (0, "done\n")

    runner = Finite()
    monkeypatch.setattr(agent_mod, "post_run_logs", lambda *a, **kw: "accepted")
    assert agent_mod._stream_logs("http://qa", "test", runner, "run-done", final=True) is True
    assert runner.confirmed == 1
