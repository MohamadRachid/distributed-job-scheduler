"""Agent-side fixes for the 2026-09-07 audit (agent 0.12.2).

Each block names the defect it closes, and each test fails on the tree the audit
was taken on (0.12.1):

  A  a same-node re-assignment at a bumped attempt was silently dropped
  B  a FAILED post that came back 'retry' was treated as accepted, so the reason
     was lost; the start-failure path had the same hole
  D  `_stage_private` leaked its sealed temp file when the ticket fetch raised
  E  the identity file (node token) was written with the umask's mode
  F  a 409 on the checkpoint fetch was read as "nothing to resume from" and the
     container was started anyway

Pure-logic tests, no Docker daemon and no control plane, in the pattern of the
sibling files. Run from the repo root:  pytest agent/tests -q
"""

import os
import stat
import urllib.error

import pytest

from agent import agent as agent_mod
from agent import runner as runner_mod
from agent.classify import ARTIFACT_TOO_LARGE
from agent.runner import Runner, prepare_run_dir

RUN_ID = "run-audit"


# --- fakes (the same shape the sibling files use) ----------------------------


class FakeContainer:
    """Exited-0 by default; `remove` and `stop` are recorded so a test can say
    whether the agent tore it down."""

    short_id = "audit000"

    def __init__(self, exit_code: int = 0) -> None:
        self.removed = False
        self.stopped = False
        self.status = "exited"
        self.attrs = {"State": {"ExitCode": exit_code, "OOMKilled": False}}

    def logs(self, **_kw) -> bytes:
        return b""

    def reload(self) -> None:
        pass

    def stop(self, **_kw) -> None:
        self.stopped = True

    def remove(self, **_kw) -> None:
        self.removed = True


class FakeClient:
    """A NEW container per `containers.run`, every one kept, so a test can tell the
    first attempt's container from the second's."""

    def __init__(self) -> None:
        self.started: list[FakeContainer] = []
        self.run_kwargs: list[dict] = []

    def ping(self) -> None:
        pass

    @property
    def images(self):
        class _Images:
            @staticmethod
            def get(_image):
                return object()

        return _Images

    @property
    def containers(self):
        outer = self

        class _Containers:
            @staticmethod
            def run(*_a, **kw):
                outer.run_kwargs.append(kw)
                c = FakeContainer()
                outer.started.append(c)
                return c

        return _Containers


@pytest.fixture
def client(monkeypatch, tmp_path):
    monkeypatch.setenv("AGENT_OUTPUT_ROOT", str(tmp_path))
    c = FakeClient()
    monkeypatch.setattr(runner_mod.docker, "from_env", lambda: c)
    return c


def _record_status(posted: list, answer: str = "accepted"):
    """A `post_run_status` stand-in that records (run_id, attempt, state, reason)
    and answers with one fixed verdict."""
    def fake(server, token, run_id, attempt, state, code=None, reason=None, detail=None,
             progress=None, metrics=None):
        posted.append((run_id, attempt, state, reason))
        return answer

    return fake


def _assignment(attempt: int, **extra) -> dict:
    a = {"run_id": RUN_ID, "attempt": attempt, "image": "img", "entrypoint": [], "env": {}}
    a.update(extra)
    return a


# ===========================================================================
# A — a re-assignment at a higher attempt replaces the stale execution
# ===========================================================================


def test_a_re_assignment_at_a_higher_attempt_replaces_the_stale_execution(
    monkeypatch, client
):
    """The lease expired while attempt 1 was still executing here, the reaper
    requeued the run, and THIS node claimed it again at attempt 2. The old
    execution is stale by the control plane's own decision: it is torn down and
    attempt 2 starts. On 0.12.1 the second assignment was dropped, attempt 2 never
    started, and the run was reaped again and again until its retries ran out."""
    posted = []
    monkeypatch.setattr(agent_mod, "post_run_status", _record_status(posted))
    monkeypatch.setattr(agent_mod, "fetch_checkpoint", lambda *a, **k: None)
    r = Runner()

    agent_mod._act_on_assignments("s", "t", r, [_assignment(1)])
    assert r.attempt_of(RUN_ID) == 1
    first = client.started[0]

    agent_mod._act_on_assignments("s", "t", r, [_assignment(2)])
    assert r.attempt_of(RUN_ID) == 2
    assert first.removed, "the attempt-1 container is stopped and removed"
    assert len(client.started) == 2, "a container was started for attempt 2"
    assert (RUN_ID, 2, "RUNNING", None) in posted


def test_a_re_listed_same_attempt_does_not_restart_the_container(monkeypatch, client):
    monkeypatch.setattr(agent_mod, "post_run_status", _record_status([]))
    r = Runner()
    agent_mod._act_on_assignments("s", "t", r, [_assignment(1)])
    agent_mod._act_on_assignments("s", "t", r, [_assignment(1)])
    assert len(client.started) == 1
    assert not client.started[0].removed


def test_an_assignment_at_a_lower_attempt_than_tracked_is_ignored(monkeypatch, client):
    """Cannot happen -- the control plane never hands out a lower attempt -- and if
    it ever did, starting it would be two executions of one run on one machine."""
    posted = []
    monkeypatch.setattr(agent_mod, "post_run_status", _record_status(posted))
    monkeypatch.setattr(agent_mod, "fetch_checkpoint", lambda *a, **k: None)
    r = Runner()
    agent_mod._act_on_assignments("s", "t", r, [_assignment(3)])
    agent_mod._act_on_assignments("s", "t", r, [_assignment(2)])
    assert r.attempt_of(RUN_ID) == 3
    assert len(client.started) == 1
    assert [p for p in posted if p[1] == 2] == []


# ===========================================================================
# B — a FAILED post that is not accepted is not treated as accepted
# ===========================================================================


def _start_with_one_result(r: Runner, attempt: int = 1) -> None:
    run_dir = prepare_run_dir(RUN_ID, attempt)
    r.start(RUN_ID, attempt, "img", [], {}, run_dir=run_dir)
    with open(os.path.join(r._runs[RUN_ID]["output_dir"], "out.bin"), "wb") as f:
        f.write(b"x" * 16)


def test_a_retried_failure_post_leaves_the_run_tracked_until_it_is_accepted(
    monkeypatch, client
):
    """'retry' means the control plane never heard the reason. The run stays
    tracked so the exited container surfaces again next tick and the whole upload
    and fail path repeats -- late, never lost. On 0.12.1 the run was cleaned up on
    the spot and the reason (here ARTIFACT_TOO_LARGE) was gone for good."""
    r = Runner()
    _start_with_one_result(r)
    monkeypatch.setattr(agent_mod, "post_run_artifact", lambda *a, **k: "too_large")

    posted = []
    monkeypatch.setattr(agent_mod, "post_run_status", _record_status(posted, "retry"))
    decided = agent_mod._upload_artifacts("http://cp", "tok", r, RUN_ID, 1)
    assert decided is True, "the upload decided the run's outcome this tick"
    assert posted == [(RUN_ID, 1, "FAILED", ARTIFACT_TOO_LARGE)]
    assert r.has(RUN_ID), "not accepted, so not cleaned up"
    assert not client.started[0].removed

    posted.clear()
    monkeypatch.setattr(agent_mod, "post_run_status", _record_status(posted, "accepted"))
    assert agent_mod._upload_artifacts("http://cp", "tok", r, RUN_ID, 1) is True
    assert posted == [(RUN_ID, 1, "FAILED", ARTIFACT_TOO_LARGE)]
    assert not r.has(RUN_ID), "accepted, so cleaned up"
    assert client.started[0].removed


def test_a_fenced_failure_post_aborts_the_run(monkeypatch, client):
    r = Runner()
    _start_with_one_result(r)
    monkeypatch.setattr(agent_mod, "post_run_artifact", lambda *a, **k: "too_large")
    monkeypatch.setattr(agent_mod, "post_run_status", _record_status([], "abort"))
    assert agent_mod._upload_artifacts("http://cp", "tok", r, RUN_ID, 1) is True
    assert not r.has(RUN_ID)
    assert client.started[0].removed


def test_a_clean_upload_returns_false_so_the_caller_posts_the_terminal_status(
    monkeypatch, client
):
    r = Runner()
    _start_with_one_result(r)
    monkeypatch.setattr(agent_mod, "post_run_artifact", lambda *a, **k: "accepted")
    assert agent_mod._upload_artifacts("http://cp", "tok", r, RUN_ID, 1) is False
    assert r.has(RUN_ID)


def test_drain_never_posts_succeeded_for_a_run_whose_result_was_refused(
    monkeypatch, client
):
    """Exit 0, a result the control plane refuses for size, and a FAILED post that
    comes back 'retry'. SUCCEEDED must not be posted -- the result was not kept --
    and the run stays tracked so the FAILED post is made again next tick."""
    r = Runner()
    _start_with_one_result(r)
    monkeypatch.setattr(agent_mod, "post_run_artifact", lambda *a, **k: "too_large")
    posted = []
    monkeypatch.setattr(agent_mod, "post_run_status", _record_status(posted, "retry"))

    agent_mod._drain_finished("http://cp", "tok", r)
    states = [p[2] for p in posted]
    assert "SUCCEEDED" not in states
    assert states == ["FAILED"]
    assert r.has(RUN_ID), "left tracked: poll() surfaces the exited container again"

    # Next tick, the control plane is back: the same FAILED post lands and the run
    # is cleaned up. Late, never lost.
    posted.clear()
    monkeypatch.setattr(agent_mod, "post_run_status", _record_status(posted, "accepted"))
    agent_mod._drain_finished("http://cp", "tok", r)
    assert [p[2] for p in posted] == ["FAILED"]
    assert not r.has(RUN_ID)


def test_a_start_failure_whose_failed_post_is_retried_is_queued_and_flushed(
    monkeypatch, client
):
    """The second site of the same defect: a start failure posts FAILED and used to
    ignore the answer. 'retry' now queues the post; the next heartbeat tick flushes
    it, and the flushed post carries the same run, attempt and reason."""
    agent_mod._deferred_terminal.clear()
    monkeypatch.setattr(
        agent_mod, "_stage_private",
        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("control plane unreachable")),
    )
    posted = []
    monkeypatch.setattr(agent_mod, "post_run_status", _record_status(posted, "retry"))
    r = Runner()
    agent_mod._act_on_assignments(
        "s", "t", r, [_assignment(4, private=True, job_id="j")]
    )
    assert not r.has(RUN_ID)
    assert len(posted) == 1 and posted[0][:3] == (RUN_ID, 4, "FAILED")
    queued_reason = posted[0][3]
    assert len(agent_mod._deferred_terminal) == 1

    posted.clear()
    monkeypatch.setattr(agent_mod, "post_run_status", _record_status(posted, "accepted"))
    agent_mod._flush_deferred("s", "t")
    assert agent_mod._deferred_terminal == []
    assert posted == [(RUN_ID, 4, "FAILED", queued_reason)]


def test_a_deferred_post_still_refused_stays_queued_and_an_old_one_is_dropped(
    monkeypatch
):
    agent_mod._deferred_terminal.clear()
    agent_mod._deferred_terminal.append(
        {"run_id": "fresh", "attempt": 1, "state": "FAILED", "exit_code": None,
         "reason": "X", "detail": "d", "queued_at": 1_000_000.0}
    )
    agent_mod._deferred_terminal.append(
        {"run_id": "stale", "attempt": 1, "state": "FAILED", "exit_code": None,
         "reason": "X", "detail": "d",
         "queued_at": 1_000_000.0 - agent_mod.DEFERRED_TERMINAL_MAX_AGE_S - 1}
    )
    monkeypatch.setattr(agent_mod.time, "time", lambda: 1_000_000.0)
    posted = []
    monkeypatch.setattr(agent_mod, "post_run_status", _record_status(posted, "retry"))
    agent_mod._flush_deferred("s", "t")
    assert [e["run_id"] for e in agent_mod._deferred_terminal] == ["fresh"]
    assert [p[0] for p in posted] == ["fresh"], "the stale entry is dropped, not posted"
    agent_mod._deferred_terminal.clear()


# ===========================================================================
# D — the sealed temp file does not outlive a failed ticket fetch
# ===========================================================================


def test_stage_private_removes_its_temp_file_when_the_ticket_fetch_raises(
    monkeypatch, tmp_path
):
    monkeypatch.setenv("AGENT_OUTPUT_ROOT", str(tmp_path))
    monkeypatch.setattr(agent_mod, "fetch_sealed_input", lambda *a, **k: b"sealed")
    monkeypatch.setattr(
        agent_mod, "fetch_key_ticket",
        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("409 or a dead link")),
    )
    with pytest.raises(RuntimeError):
        agent_mod._stage_private(
            "http://cp", "tok", {"run_id": RUN_ID, "attempt": 1, "job_id": "j"}
        )
    leftovers = [p for p in os.listdir(tmp_path) if p.startswith("fyp-sealed-")]
    assert leftovers == []


# ===========================================================================
# E — the identity file is owner-only
# ===========================================================================


def test_the_state_file_is_written_owner_only(monkeypatch, tmp_path):
    """The file holds the node token. On POSIX the mode is asserted; on Windows
    `os.chmod` only toggles the read-only bit, so there the test asserts only that
    the file round-trips through `_load_state` -- the mode is not checkable here."""
    path = tmp_path / "agent_state-node-a.json"
    monkeypatch.setattr(agent_mod, "STATE_FILE", str(path))
    state = {"node_id": "n1", "token": "secret", "name": "node-a"}
    agent_mod._save_state(state)
    assert agent_mod._load_state() == state
    if os.name != "nt":
        assert stat.S_IMODE(os.stat(path).st_mode) == 0o600


def test_the_state_file_is_created_and_chmodded_to_0600_explicitly(monkeypatch, tmp_path):
    """Platform-independent half of the same check: the file is CREATED at 0600 and
    then chmodded to 0600, so the mode never depends on the umask. Spied because a
    Windows machine cannot observe the resulting mode."""
    path = tmp_path / "agent_state-node-a.json"
    monkeypatch.setattr(agent_mod, "STATE_FILE", str(path))
    opened, chmodded = [], []
    real_open = os.open

    def spy_open(p, flags, mode=0o777, *a, **k):
        if str(p) == str(path):
            opened.append(mode)
        return real_open(p, flags, mode, *a, **k)

    monkeypatch.setattr(agent_mod.os, "open", spy_open)
    monkeypatch.setattr(agent_mod.os, "chmod", lambda p, m: chmodded.append((str(p), m)))
    agent_mod._save_state({"node_id": "n1", "token": "secret", "name": "node-a"})
    assert opened == [0o600]
    assert chmodded == [(str(path), 0o600)]


# ===========================================================================
# F — a 409 on the checkpoint fetch means the run has moved on
# ===========================================================================


def test_a_409_on_the_checkpoint_fetch_starts_nothing_and_posts_nothing(
    monkeypatch, client
):
    """A 409 means the platform has given this run to a newer attempt. Everywhere
    else the agent aborts on a 409 at once; here 0.12.1 read it as 'no checkpoint'
    and started the container anyway, then posted RUNNING for a fenced attempt."""
    def _urlopen(req, timeout=None, **_k):
        raise urllib.error.HTTPError(req.full_url, 409, "stale attempt", None, None)

    monkeypatch.setattr(agent_mod.urllib.request, "urlopen", _urlopen)
    posted = []
    monkeypatch.setattr(agent_mod, "post_run_status", _record_status(posted))
    r = Runner()
    # A real-looking server URL: `Request` rejects "s/..." before the faked
    # urlopen is reached, and that would be a start failure, not a 409.
    agent_mod._act_on_assignments("http://cp", "t", r, [_assignment(2)])
    assert client.started == [], "no container was started"
    assert posted == [], "nothing was posted -- a FAILED post would itself be refused"
    assert not r.has(RUN_ID)
