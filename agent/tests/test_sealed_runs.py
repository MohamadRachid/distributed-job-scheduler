"""Agent-side sealed runs (2026-09-06) — staging, the container's shape, and the
refusal that follows an unsealed result.

Pure-logic tests, no Docker daemon and no control plane: a fake docker client backs
the REAL Runner, so the container arguments asserted here are the arguments the real
agent would pass.

**What these pin that the control-plane tests cannot.** A sealed run is an ORDINARY
run with two additions — a read-only ciphertext mount and a one-shot ticket — and the
whole point of the change is what it does NOT do: it takes no writable mount away, so
the run still collects results and can still be resumed. A regression that quietly
brought the old private lockdown back would keep every control-plane test green and
break the feature, and these are what stops that.

Run from the repo root:  pytest agent/tests -q
"""

import json
import os

import pytest

from agent import agent as agent_mod
from agent import runner as runner_mod
from agent.classify import UNSEALED_OUTPUT
from agent.runner import CHECKPOINT_DIR_NAME, Runner, prepare_run_dir

RUN_ID = "run-s"
ATTEMPT = 2
SEALED_INPUT = b"FYPSEAL2\x01\x00\x00\x00\x00\x10\x00" + b"\x00" * 200


class FakeContainer:
    short_id = "fake0000"

    def logs(self, **_kw) -> bytes:
        return b""

    def remove(self, **_kw) -> None:
        pass


class FakeClient:
    def __init__(self) -> None:
        self.run_kwargs: dict = {}

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
            def run(*_args, **kwargs):
                outer.run_kwargs = kwargs
                return FakeContainer()

        return _Containers


@pytest.fixture
def client(monkeypatch):
    c = FakeClient()
    monkeypatch.setattr(runner_mod.docker, "from_env", lambda: c)
    return c


# --- the shape of a sealed container ----------------------------------------


def test_a_sealed_run_keeps_every_writable_folder(client, tmp_path, monkeypatch):
    """THE test of this change. A private run used to have no writable host mount at
    all, which is why it could collect nothing and resume from nothing. A sealed run
    keeps all three folders, so its results are collected and its checkpoint is
    staged — privacy and resume stopped trading against each other here."""
    monkeypatch.setenv("AGENT_OUTPUT_ROOT", str(tmp_path))
    run_dir = prepare_run_dir(RUN_ID, ATTEMPT)
    sealed = os.path.join(run_dir, "input", "data.csv")
    os.makedirs(os.path.dirname(sealed), exist_ok=True)
    with open(sealed, "wb") as f:
        f.write(SEALED_INPUT)

    r = Runner()
    r.start(
        RUN_ID, ATTEMPT, "fyp-dummy:latest", ["python", "train.py"], {},
        run_dir=run_dir,
        checkpoint_dir=os.path.join(run_dir, CHECKPOINT_DIR_NAME),
        sealed=True, sealed_path=sealed, input_name="data.csv",
        ticket="tkt-9", key_url="https://host.docker.internal:8000/container/key",
        ca_pem="-----BEGIN CERTIFICATE-----\nnot-a-real-one\n",
    )
    kw = client.run_kwargs
    modes = {path: v["mode"] for path, v in kw["volumes"].items()}

    # Three writable folders, exactly as an ordinary run has.
    assert sorted(m for m in modes.values() if m == "rw") == ["rw", "rw", "rw"]
    # The sealed input is the one read-only mount, and it is a FILE.
    assert modes[sealed] == "ro"
    # The run collects results, and has somewhere to resume from.
    assert r._runs[RUN_ID]["output_dir"] is not None
    assert r._runs[RUN_ID]["checkpoint_dir"] is not None
    # Nothing of the old private lockdown: no RAM folder, no forced user.
    assert "tmpfs" not in kw
    assert "user" not in kw
    # Still hardened the way every run has been since 2026-09-04.
    assert kw["read_only"] is True


def test_the_container_is_given_a_ticket_and_never_a_key(client, tmp_path, monkeypatch):
    monkeypatch.setenv("AGENT_OUTPUT_ROOT", str(tmp_path))
    run_dir = prepare_run_dir(RUN_ID, ATTEMPT)
    sealed = os.path.join(run_dir, "input", "data.csv")
    os.makedirs(os.path.dirname(sealed), exist_ok=True)
    with open(sealed, "wb") as f:
        f.write(SEALED_INPUT)

    r = Runner()
    r.start(
        RUN_ID, ATTEMPT, "img", [], {}, run_dir=run_dir,
        sealed=True, sealed_path=sealed, input_name="data.csv",
        ticket="tkt-9", key_url="https://cp:8000/container/key", ca_pem="PEM",
    )
    env = client.run_kwargs["environment"]
    assert env["FYP_TICKET"] == "tkt-9"
    assert env["FYP_KEY_URL"].endswith("/container/key")
    assert env["FYP_CA_PEM"] == "PEM"
    # The dataset arrives under the SAME variable an unsealed one would, so a workload
    # never branches on whether its input is sealed -- the reader sniffs the bytes.
    assert env["INPUT_PATH"] == "/input/data.csv"
    assert not any("key_b64" in str(v) for v in env.values())


def test_a_sealed_run_with_no_input_still_gets_its_ticket(client, tmp_path, monkeypatch):
    """A job with nothing to open still has something to seal: its results and its
    checkpoints. Without a ticket those would be the only unsealed bytes anywhere."""
    monkeypatch.setenv("AGENT_OUTPUT_ROOT", str(tmp_path))
    run_dir = prepare_run_dir(RUN_ID, ATTEMPT)
    r = Runner()
    r.start(
        RUN_ID, ATTEMPT, "img", [], {}, run_dir=run_dir,
        sealed=True, ticket="tkt-only", key_url="http://cp:8000/container/key",
    )
    env = client.run_kwargs["environment"]
    assert env["FYP_TICKET"] == "tkt-only"
    assert "INPUT_PATH" not in env
    assert client.run_kwargs["volumes"], "the writable folders are still mounted"


def test_an_ordinary_run_is_untouched(client, tmp_path, monkeypatch):
    """The guard against this change leaking into every other run: no ticket, no key
    URL, no certificate, nothing new in the environment at all."""
    monkeypatch.setenv("AGENT_OUTPUT_ROOT", str(tmp_path))
    r = Runner()
    r.start(RUN_ID, ATTEMPT, "img", [], {}, run_dir=prepare_run_dir(RUN_ID, ATTEMPT))
    env = client.run_kwargs["environment"]
    assert "FYP_TICKET" not in env and "FYP_KEY_URL" not in env
    assert "extra_hosts" not in client.run_kwargs


# --- staging ----------------------------------------------------------------


def test_staging_writes_ciphertext_and_takes_a_ticket(monkeypatch, tmp_path):
    """The agent downloads the sealed bytes, puts them on this worker's disk, and
    asks for a ticket. It never asks for a key, and it could not use one."""
    calls = {}

    def fake_input(server, token, job_id):
        calls["input"] = (server, token, job_id)
        return SEALED_INPUT

    def fake_ticket(server, token, run_id, attempt):
        calls["ticket"] = (run_id, attempt)
        return {"ticket": "tkt-1", "key_url": "http://cp/container/key"}

    monkeypatch.setattr(agent_mod, "fetch_job_input", fake_input)
    monkeypatch.setattr(agent_mod, "fetch_key_ticket", fake_ticket)
    monkeypatch.setattr(agent_mod, "ca_pem", lambda: None)

    run_dir = str(tmp_path / "run")
    os.makedirs(run_dir, exist_ok=True)
    a = {"run_id": RUN_ID, "attempt": ATTEMPT, "job_id": "job-1",
         "has_input": True, "input_filename": "data.csv"}
    extra = agent_mod._stage_sealed("http://cp", "node-token", a, run_dir)

    assert extra["sealed"] is True
    assert extra["ticket"] == "tkt-1"
    assert extra["input_name"] == "data.csv"
    with open(extra["sealed_path"], "rb") as f:
        assert f.read() == SEALED_INPUT
    assert calls["ticket"] == (RUN_ID, ATTEMPT)


def test_a_submitted_filename_can_never_escape_the_run_directory(monkeypatch, tmp_path):
    """A name is a label, never a path — the same rule the plain input path already
    keeps, and worth its own test on a second staging function."""
    monkeypatch.setattr(agent_mod, "fetch_job_input", lambda *_a: SEALED_INPUT)
    monkeypatch.setattr(
        agent_mod, "fetch_key_ticket",
        lambda *_a: {"ticket": "t", "key_url": "u"},
    )
    monkeypatch.setattr(agent_mod, "ca_pem", lambda: None)

    run_dir = str(tmp_path / "run")
    os.makedirs(run_dir, exist_ok=True)
    extra = agent_mod._stage_sealed(
        "http://cp", "tok",
        {"run_id": RUN_ID, "attempt": 1, "job_id": "j", "has_input": True,
         "input_filename": "../../etc/passwd"},
        run_dir,
    )
    assert extra["input_name"] == "passwd"
    assert os.path.realpath(extra["sealed_path"]).startswith(os.path.realpath(run_dir))


# --- an unsealed result fails the run ---------------------------------------


def test_a_refused_unsealed_output_fails_the_run_with_its_own_reason(
    monkeypatch, tmp_path, client
):
    """The exit code is ZERO here: the workload finished cleanly and wrote plaintext.
    That is a failure of the run and not of the program, and naming it is the
    difference between a user fixing one line in their loader and a user staring at a
    successful run with no results."""
    monkeypatch.setenv("AGENT_OUTPUT_ROOT", str(tmp_path))
    run_dir = prepare_run_dir(RUN_ID, ATTEMPT)
    r = Runner()
    r.start(RUN_ID, ATTEMPT, "img", [], {}, run_dir=run_dir, sealed=True, ticket="t")
    with open(os.path.join(r._runs[RUN_ID]["output_dir"], "metrics.json"), "wb") as f:
        f.write(b'{"plain": true}')

    posted = {}
    monkeypatch.setattr(agent_mod, "post_run_artifact", lambda *_a, **_k: "refused")

    def fake_status(server, token, run_id, attempt, state, code=None,
                    reason=None, detail=None):
        posted.update(state=state, reason=reason, detail=detail)
        return "accepted"

    monkeypatch.setattr(agent_mod, "post_run_status", fake_status)
    agent_mod._upload_artifacts("http://cp", "tok", r, RUN_ID, ATTEMPT)

    assert posted["state"] == "FAILED"
    assert posted["reason"] == UNSEALED_OUTPUT
    assert "metrics.json" in posted["detail"]
    assert not r.has(RUN_ID), "the run is finished, not left tracked"


def test_a_422_from_the_artifact_door_is_read_as_a_refusal(monkeypatch):
    """The one line that turns the control plane's answer into the agent's decision.
    A 413 is still 'skip this file' and a 409 is still 'abort'; only the new code is
    new."""
    import urllib.error

    def raise_422(*_a, **_k):
        raise urllib.error.HTTPError("u", 422, "Unprocessable", {}, None)

    monkeypatch.setattr(agent_mod, "_post_multipart", raise_422)
    out = agent_mod.post_run_artifact("http://cp", "tok", RUN_ID, 1, "out.bin", b"x")
    assert out == "refused"


def test_the_label_is_not_invented_twice():
    """One string, defined once on each side of the wire and asserted equal here, so a
    rename on one side cannot quietly stop matching the other."""
    assert UNSEALED_OUTPUT == "UNSEALED_OUTPUT"
    assert json.dumps({"reason": UNSEALED_OUTPUT})
