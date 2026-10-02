"""Agent-side fixes for what the stranger found (walk 1, 2026-09-07).

Each test names the row of `docs/evidence/walkthrough_1_2026-09-07.txt` it closes,
and each one fails on the tree the walk was taken on.

  row 32  the key is obtained once per CONTAINER, not once per process, so the
          form's own "several steps in order" command works on a sealed job; and a
          key that cannot be obtained says why, without leaving a plain file behind
  row 33  a sealed input that is already gone is not a warning
  row 55  a result refused for its size fails the run with a named reason

Pure-logic tests, no Docker daemon and no control plane, in the pattern of the
sibling files. Run from the repo root:  pytest agent/tests -q
"""

import io
import logging
import os
import sys
import urllib.error

import pytest

from agent import agent as agent_mod
from agent import runner as runner_mod
from agent.classify import ARTIFACT_TOO_LARGE, STORAGE_QUOTA_EXCEEDED
from agent.runner import Runner, prepare_run_dir

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "workloads", "dummy"))

import fyp_data  # noqa: E402

RUN_ID = "run-walk"
ATTEMPT = 1


# --- fakes (the same shape the sibling files use) ----------------------------


class FakeContainer:
    short_id = "walk0000"

    def logs(self, **_kw) -> bytes:
        return b""

    def remove(self, **_kw) -> None:
        pass


class FakeClient:
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
        class _Containers:
            @staticmethod
            def run(*_args, **_kwargs):
                return FakeContainer()

        return _Containers


@pytest.fixture
def client(monkeypatch):
    c = FakeClient()
    monkeypatch.setattr(runner_mod.docker, "from_env", lambda: c)
    return c


@pytest.fixture
def ticketed(monkeypatch, tmp_path):
    """A container that was handed a ticket, with the key cache pointed at a file
    under tmp_path instead of /dev/shm, and no key in memory yet."""
    monkeypatch.setattr(fyp_data, "_KEY", None)
    monkeypatch.setenv("FYP_TICKET", "tkt-1")
    monkeypatch.setenv("FYP_KEY_URL", "http://cp/container/key")
    monkeypatch.setenv(fyp_data.KEY_CACHE_ENV, str(tmp_path / "shm" / ".fyp_key"))
    (tmp_path / "shm").mkdir()
    monkeypatch.setenv("OUTPUT_DIR", str(tmp_path / "out"))
    return tmp_path


def _http_error(code: int, body: bytes = b""):
    return urllib.error.HTTPError("http://cp/container/key", code, "x", {}, io.BytesIO(body))


# --- row 32: the key, once per container --------------------------------------


def test_a_second_step_in_the_same_container_reuses_the_key(ticketed, monkeypatch):
    """The first process redeems the ticket; a second process — a new Python with an
    empty memory, as every step of a shell chain is — finds the key where the first
    one left it and never asks the control plane again. The ticket stays single-use
    against the outside: the fake below refuses a second redemption exactly as the
    control plane does (410)."""
    import base64

    key = os.urandom(32)
    calls = []

    def redeem(**_kw):
        calls.append(1)
        if len(calls) > 1:
            raise fyp_data.KeyUnavailable("already used (HTTP 410)")
        return base64.b64encode(key).decode()

    monkeypatch.setattr(fyp_data, "_redeem", redeem)
    assert fyp_data.key() == key
    assert calls == [1]

    # The next process: nothing in memory, the same container.
    monkeypatch.setattr(fyp_data, "_KEY", None)
    assert fyp_data.key() == key
    assert calls == [1], "the second step never touched the ticket"


def test_the_cached_key_is_readable_by_its_owner_only(ticketed, monkeypatch):
    import base64
    import stat

    key = os.urandom(32)
    monkeypatch.setattr(fyp_data, "_redeem", lambda **_kw: base64.b64encode(key).decode())
    fyp_data.key()
    path = fyp_data._key_cache_path()
    assert os.path.isfile(path)
    if os.name != "nt":  # Windows has no POSIX mode bits to assert on
        assert stat.S_IMODE(os.stat(path).st_mode) == 0o600


def test_a_spent_ticket_is_named_as_the_cause(ticketed, monkeypatch, capsys):
    """What the second step used to print was a traceback ending in 'HTTP Error 410:
    Gone'. It now says what a 410 means here."""
    def redeem(**_kw):
        raise fyp_data.KeyUnavailable(fyp_data._explain_http(410, "http://cp/container/key"))

    monkeypatch.setattr(fyp_data, "_redeem", redeem)
    with pytest.raises(fyp_data.IntegrityError) as err:
        fyp_data.key()
    message = str(err.value)
    assert "already been used or has expired" in message
    assert "HTTP 410" in message
    # And the platform's marker line carries the same words, so the run's reason does.
    assert "##INTEGRITY_ERROR: could not obtain the key: the one-time key ticket" in (
        capsys.readouterr().err
    )


def test_redeem_turns_the_control_planes_codes_into_sentences(monkeypatch):
    seen = {}

    def fake_urlopen(req, timeout=None, context=None):  # noqa: ARG001
        raise _http_error(seen.pop("code"))

    monkeypatch.setattr(fyp_data.urllib.request, "urlopen", fake_urlopen)
    for code, words in ((410, "already been used"), (409, "re-dispatched"),
                        (404, "does not know"), (500, "HTTP 500")):
        seen["code"] = code
        with pytest.raises(fyp_data.KeyUnavailable) as err:
            fyp_data._redeem("http://cp/container/key", "tkt")
        assert words in str(err.value), (code, str(err.value))


def test_create_output_leaves_no_file_when_the_key_cannot_be_obtained(ticketed, monkeypatch):
    """The file used to be opened BEFORE the key was asked for, so a failed
    redemption left an empty unsealed file that the agent then uploaded and the
    control plane refused as UNSEALED_OUTPUT — a cause that was not the cause."""
    def redeem(**_kw):
        raise fyp_data.KeyUnavailable("already used (HTTP 410)")

    monkeypatch.setattr(fyp_data, "_redeem", redeem)
    with pytest.raises(fyp_data.IntegrityError):
        fyp_data.create_output("metrics.json")
    assert not os.path.exists(os.path.join(os.environ["OUTPUT_DIR"], "metrics.json"))


def test_a_missing_cache_folder_costs_nothing_but_the_cache(ticketed, monkeypatch):
    """A container whose RAM folder is missing still gets its key — it simply pays a
    ticket per process, as it did before the cache existed."""
    import base64

    key = os.urandom(32)
    monkeypatch.setattr(fyp_data, "_redeem", lambda **_kw: base64.b64encode(key).decode())
    monkeypatch.setenv(fyp_data.KEY_CACHE_ENV, str(ticketed / "no-such-dir" / "key"))
    assert fyp_data.key() == key


# --- row 33: a gone input is not a warning -----------------------------------


def test_a_sealed_input_already_removed_with_its_run_directory_is_not_a_warning(
    tmp_path, caplog
):
    run_dir = tmp_path / "run"
    (run_dir / "input").mkdir(parents=True)
    sealed = run_dir / "input" / "data.csv"
    sealed.write_bytes(b"FYPSEAL2" + b"\0" * 40)
    with caplog.at_level(logging.WARNING, logger="agent.runner"):
        Runner._wipe_staged({"run_dir": str(run_dir), "sealed_path": str(sealed)})
    assert not run_dir.exists()
    assert "could not remove staged sealed input" not in caplog.text


# --- row 55: a result refused for size fails the run --------------------------


def _fake_status(posted):
    def fake(server, token, run_id, attempt, state, code=None, reason=None, detail=None,
             progress=None, metrics=None):
        posted.update(state=state, reason=reason, detail=detail, progress=progress,
                      metrics=metrics)
        return "accepted"

    return fake


def test_an_over_cap_result_fails_the_run_naming_the_file_size_and_cap(
    monkeypatch, tmp_path, client
):
    """The exit code is ZERO: the workload finished cleanly and wrote a result the
    agent would not send. The run used to read SUCCEEDED with 'no output files'."""
    monkeypatch.setenv("AGENT_OUTPUT_ROOT", str(tmp_path))
    monkeypatch.setattr(agent_mod, "MAX_ARTIFACT_MB", 1)
    run_dir = prepare_run_dir(RUN_ID, ATTEMPT)
    r = Runner()
    r.start(RUN_ID, ATTEMPT, "img", [], {}, run_dir=run_dir)
    with open(os.path.join(r._runs[RUN_ID]["output_dir"], "out.bin"), "wb") as f:
        f.write(b"x" * (1024 * 1024 + 512 * 1024))  # 1.5 MB against a 1 MB cap

    posted = {}
    sent = []
    monkeypatch.setattr(agent_mod, "post_run_artifact", lambda *a, **k: sent.append(a) or "accepted")
    monkeypatch.setattr(agent_mod, "post_run_status", _fake_status(posted))
    agent_mod._upload_artifacts("http://cp", "tok", r, RUN_ID, ATTEMPT)

    assert sent == [], "nothing over the cap is sent"
    assert posted["state"] == "FAILED"
    assert posted["reason"] == ARTIFACT_TOO_LARGE
    assert "out.bin" in posted["detail"]
    assert "1.5 MB" in posted["detail"]
    assert "1 MB" in posted["detail"]
    assert not r.has(RUN_ID), "the run is finished, not left tracked"


def test_a_413_from_the_control_plane_is_read_by_its_reason(monkeypatch):
    """Two refusals share the 413 code and mean different things; the body's `reason`
    tells them apart. Neither is 'skip' any more."""
    def raise_413(body):
        def _raise(*_a, **_k):
            raise _http_error(413, body)
        return _raise

    monkeypatch.setattr(agent_mod, "_post_multipart", raise_413(b'{"detail":"artifact exceeds size cap"}'))
    assert agent_mod.post_run_artifact("http://cp", "tok", RUN_ID, 1, "out.bin", b"x") == "too_large"

    monkeypatch.setattr(
        agent_mod, "_post_multipart",
        raise_413(b'{"detail":{"reason":"STORAGE_QUOTA_EXCEEDED","used_mb":1,"cap_mb":1}}'),
    )
    assert agent_mod.post_run_artifact("http://cp", "tok", RUN_ID, 1, "out.bin", b"x") == "quota"


def test_a_result_the_control_plane_refuses_for_size_fails_the_run(
    monkeypatch, tmp_path, client
):
    monkeypatch.setenv("AGENT_OUTPUT_ROOT", str(tmp_path))
    run_dir = prepare_run_dir(RUN_ID, ATTEMPT)
    r = Runner()
    r.start(RUN_ID, ATTEMPT, "img", [], {}, run_dir=run_dir)
    with open(os.path.join(r._runs[RUN_ID]["output_dir"], "big.bin"), "wb") as f:
        f.write(b"x" * 4096)

    posted = {}
    monkeypatch.setattr(agent_mod, "post_run_artifact", lambda *a, **k: "too_large")
    monkeypatch.setattr(agent_mod, "post_run_status", _fake_status(posted))
    agent_mod._upload_artifacts("http://cp", "tok", r, RUN_ID, ATTEMPT)
    assert posted["reason"] == ARTIFACT_TOO_LARGE
    assert "big.bin" in posted["detail"]


def test_a_quota_refusal_is_posted_under_the_control_planes_own_label(
    monkeypatch, tmp_path, client
):
    monkeypatch.setenv("AGENT_OUTPUT_ROOT", str(tmp_path))
    run_dir = prepare_run_dir(RUN_ID, ATTEMPT)
    r = Runner()
    r.start(RUN_ID, ATTEMPT, "img", [], {}, run_dir=run_dir)
    with open(os.path.join(r._runs[RUN_ID]["output_dir"], "out.bin"), "wb") as f:
        f.write(b"x" * 10)

    posted = {}
    monkeypatch.setattr(agent_mod, "post_run_artifact", lambda *a, **k: "quota")
    monkeypatch.setattr(agent_mod, "post_run_status", _fake_status(posted))
    agent_mod._upload_artifacts("http://cp", "tok", r, RUN_ID, ATTEMPT)
    assert posted["state"] == "FAILED"
    assert posted["reason"] == STORAGE_QUOTA_EXCEEDED


# --- rows 8, 17, 51: one identity file per worker name ----------------------------


def test_the_state_file_is_named_after_the_worker(monkeypatch):
    monkeypatch.delenv("AGENT_STATE_FILE", raising=False)
    assert agent_mod.state_file_for("node-b") == "agent_state-node-b.json"
    assert agent_mod.state_file_for("lab pc/01") == "agent_state-lab_pc_01.json"


def test_an_explicit_state_file_still_wins(monkeypatch):
    monkeypatch.setenv("AGENT_STATE_FILE", r"C:\tmp\fyp-node-a.json")
    assert agent_mod.state_file_for("node-a") == r"C:\tmp\fyp-node-a.json"


def test_two_workers_in_one_directory_keep_two_identities(monkeypatch, tmp_path):
    """The walk's row 8: node-b's start overwrote node-a's file. Now each name has its
    own file, and the black box's session file sits beside it, so neither reads the
    other's session and reports a crash that never happened (row 17)."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("AGENT_STATE_FILE", raising=False)
    for name in ("node-a", "node-b"):
        monkeypatch.setattr(agent_mod, "STATE_FILE", agent_mod.state_file_for(name))
        agent_mod._save_state({"node_id": f"id-{name}", "token": "t", "name": name})
    monkeypatch.setattr(agent_mod, "STATE_FILE", agent_mod.state_file_for("node-a"))
    assert agent_mod._load_state()["node_id"] == "id-node-a"
    monkeypatch.setattr(agent_mod, "STATE_FILE", agent_mod.state_file_for("node-b"))
    assert agent_mod._load_state()["node_id"] == "id-node-b"
    assert sorted(p.name for p in tmp_path.iterdir()) == [
        "agent_state-node-a.json", "agent_state-node-b.json",
    ]


def test_a_worker_from_before_the_per_name_files_keeps_its_node(monkeypatch, tmp_path):
    """A worker that registered under the old shared file is adopted, not
    re-registered, on its first restart after this change."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("AGENT_STATE_FILE", raising=False)
    (tmp_path / agent_mod.LEGACY_STATE_FILE).write_text(
        '{"node_id": "old-id", "token": "t", "name": "node-a"}', encoding="utf-8"
    )
    monkeypatch.setattr(agent_mod, "STATE_FILE", agent_mod.state_file_for("node-a"))
    assert agent_mod._load_state()["node_id"] == "old-id"


# --- row 12: the help says where the state lives and what it needs ---------------


def test_the_help_names_the_state_file_docker_and_the_restart_rule():
    text = agent_mod.build_parser().format_help()
    assert "agent_state-<name>.json" in text
    assert "Docker" in text
    assert "same --name" in text
    assert "(W2)" not in text


# --- row 25: the terminal post carries the last progress marker -------------------


def test_a_terminal_post_carries_progress_and_metrics_when_known(monkeypatch):
    sent = {}
    monkeypatch.setattr(agent_mod, "_post", lambda url, payload, token=None, timeout=10.0: sent.update(payload) or {})
    agent_mod.post_run_status("http://cp", "t", RUN_ID, 1, "SUCCEEDED", 0,
                              progress=1.0, metrics={"epoch": 5, "total": 5})
    assert sent["progress"] == 1.0 and sent["metrics"] == {"epoch": 5, "total": 5}
    sent.clear()
    agent_mod.post_run_status("http://cp", "t", RUN_ID, 1, "SUCCEEDED", 0)
    assert "progress" not in sent and "metrics" not in sent


# --- row 54: the worker's own lines are plain ASCII ------------------------------


def test_the_scratch_warning_carries_no_character_a_windows_console_mangles():
    import inspect

    source = inspect.getsource(agent_mod._enforce_scratch)
    assert "over its %d MB cap - stopping it" in source
    # The em dash that printed as a broken character is gone from the LOGGED line;
    # prose in the docstring is not printed anywhere and is not what this checks.
    assert "cap \u2014 stopping it" not in source


# --- row 64: a cancel command stops the container and names the reason ----------


class StoppableContainer(FakeContainer):
    def __init__(self):
        self.stopped = False
        self.status = "running"
        self.attrs = {"State": {}}

    def stop(self, timeout=None):  # noqa: ARG002
        self.stopped = True
        self.status = "exited"
        self.attrs = {"State": {"ExitCode": 137, "OOMKilled": False}}

    def reload(self):
        pass


def test_a_cancel_command_stops_the_run_and_the_exit_is_posted_as_cancelled(
    monkeypatch, tmp_path
):
    from agent.classify import CANCELLED

    container = StoppableContainer()

    class Client(FakeClient):
        @property
        def containers(self):
            class _C:
                @staticmethod
                def run(*_a, **_k):
                    return container
            return _C

    monkeypatch.setattr(runner_mod.docker, "from_env", lambda: Client())
    monkeypatch.setenv("AGENT_OUTPUT_ROOT", str(tmp_path))
    r = Runner()
    r.start(RUN_ID, ATTEMPT, "img", [], {}, run_dir=prepare_run_dir(RUN_ID, ATTEMPT))

    agent_mod._act_on_commands(r, [{"type": "cancel", "run_id": RUN_ID}])
    assert container.stopped is True
    assert r.has(RUN_ID), "still tracked: the ordinary exit path reports it"

    posted = {}
    monkeypatch.setattr(agent_mod, "_stream_logs", lambda *a, **k: None)
    monkeypatch.setattr(agent_mod, "_upload_artifacts", lambda *a, **k: None)
    monkeypatch.setattr(agent_mod, "post_run_status", _fake_status(posted))
    agent_mod._drain_finished("http://cp", "tok", r)
    assert posted["state"] == "FAILED"
    assert posted["reason"] == CANCELLED
    assert "Cancelled by the user" in posted["detail"]
    assert not r.has(RUN_ID)
    assert RUN_ID not in agent_mod._cancelled


def test_a_command_for_a_run_this_worker_does_not_hold_is_ignored(monkeypatch, client):
    r = Runner()
    agent_mod._act_on_commands(r, [{"type": "cancel", "run_id": "not-mine"},
                                   {"type": "unknown", "run_id": RUN_ID}, "junk"])
    assert agent_mod._cancelled == {}


# --- row 38: a saved checkpoint says so in the run's own log --------------------


def test_a_saved_checkpoint_prints_one_line_with_its_epoch(tmp_path, monkeypatch, capsys):
    """Nothing on screen used to say when a checkpoint existed, so a person waiting
    to kill a worker 'after a checkpoint' had nothing to wait for."""
    import fyp_checkpoint

    monkeypatch.setenv("CHECKPOINT_PATH", str(tmp_path / "ckpt" / "state"))
    monkeypatch.setattr(fyp_data, "_KEY", None)
    monkeypatch.delenv("FYP_TICKET", raising=False)
    assert fyp_checkpoint.save({"epoch": 7, "loss": 0.1}) is True
    out = capsys.readouterr().out
    assert "checkpoint saved (epoch 7)" in out
    assert fyp_checkpoint.save({"note": "no epoch"}) is True
    assert "checkpoint saved\n" in capsys.readouterr().out
