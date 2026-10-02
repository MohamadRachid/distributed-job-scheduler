"""Agent-side artifact logic (W6) — collect the run's output files + upload them.

The working mount is `/scratch` since 2026-09-04 and was `/output` before it; what
this file tests is unchanged either way, because it asks about the directory the
runner reports rather than about the path inside the container.

Pure-logic tests, no Docker daemon and no control plane: a fake docker client backs
the REAL Runner (so the tracked-state shape can't drift), and a fake urlopen captures
the multipart request the REAL post_run_artifact builds.

Run from the repo root:  pytest agent/tests -q
(Not part of the control-plane container suite — the agent is host software.)
"""

import os
import stat
import urllib.error

import pytest

from agent import agent as agent_mod
from agent import runner as runner_mod
from agent.runner import Runner

RUN_ID = "run-1"
ATTEMPT = 1


# --- fakes -------------------------------------------------------------------


class FakeContainer:
    short_id = "fake0000"

    def logs(self, **_kw) -> bytes:
        return b""

    def remove(self, **_kw) -> None:
        pass


class FakeClient:
    def __init__(self, container: FakeContainer) -> None:
        self._container = container

    def ping(self) -> None:
        pass

    @property
    def images(self):
        class _Images:
            @staticmethod
            def get(_image):
                return object()  # local; no pull

        return _Images

    @property
    def containers(self):
        container = self._container

        class _Containers:
            @staticmethod
            def run(*_a, **_kw):
                return container

        return _Containers


def make_runner(monkeypatch) -> Runner:
    monkeypatch.setattr(runner_mod.docker, "from_env", lambda: FakeClient(FakeContainer()))
    r = Runner()
    r.node_name = "node-x"
    r.start(RUN_ID, ATTEMPT, "fyp-dummy:latest", ["python", "train.py"], {})
    return r


class FakeResp:
    def __init__(self, body: bytes = b'{"artifact_id":"a","object_key":"k"}') -> None:
        self._b = body

    def read(self) -> bytes:
        return self._b

    def __enter__(self):
        return self

    def __exit__(self, *_a):
        return False


# --- collection -------------------------------------------------------------


def test_artifact_paths_lists_output_files(monkeypatch):
    r = make_runner(monkeypatch)
    out_dir = r._runs[RUN_ID]["output_dir"]
    assert os.path.isdir(out_dir)  # start() created the working mount dir

    with open(os.path.join(out_dir, "metrics.json"), "wb") as f:
        f.write(b'{"accuracy": 0.9}')
    with open(os.path.join(out_dir, "model.bin"), "wb") as f:
        f.write(b"\x00\x01\x02\x03")

    found = {name: size for name, _path, size in r.artifact_paths(RUN_ID)}
    assert found == {"metrics.json": 17, "model.bin": 4}

    # cleanup removes the container AND the output dir.
    r.cleanup(RUN_ID)
    assert not os.path.isdir(out_dir)


def test_artifact_paths_empty_when_no_output(monkeypatch):
    r = make_runner(monkeypatch)
    assert r.artifact_paths(RUN_ID) == []
    assert r.artifact_paths("no-such-run") == []


# --- the multipart upload ----------------------------------------------------


def test_post_artifact_builds_multipart_and_accepts(monkeypatch):
    captured = {}

    def fake_urlopen(req, timeout=None):
        captured["url"] = req.full_url
        captured["ctype"] = req.get_header("Content-type")
        captured["auth"] = req.get_header("Authorization")
        captured["body"] = req.data
        return FakeResp()

    monkeypatch.setattr(agent_mod.urllib.request, "urlopen", fake_urlopen)

    result = agent_mod.post_run_artifact(
        "http://cp:8000", "tok", RUN_ID, ATTEMPT, "metrics.json", b'{"x":1}', "application/json"
    )
    assert result == "accepted"
    assert captured["url"] == f"http://cp:8000/agent/runs/{RUN_ID}/artifacts"
    assert captured["ctype"].startswith("multipart/form-data; boundary=")
    assert captured["auth"] == "Bearer tok"
    body = captured["body"]
    # the form fields + the file part + the raw bytes are all present
    assert b'name="attempt"' in body and b"\r\n1\r\n" in body
    assert b'name="filename"' in body and b"metrics.json" in body
    assert b'name="file"; filename="metrics.json"' in body
    assert b'{"x":1}' in body


def test_post_artifact_return_codes(monkeypatch):
    def raise_http(code):
        def _f(req, timeout=None):
            raise urllib.error.HTTPError(req.full_url, code, "err", {}, None)

        return _f

    monkeypatch.setattr(agent_mod.urllib.request, "urlopen", raise_http(409))
    assert agent_mod.post_run_artifact("http://cp", "t", RUN_ID, 1, "f", b"x") == "abort"

    # 2026-09-07 (walk 1, row 55): a 413 with no `reason` in its body is the
    # per-file cap, and it is named rather than skipped — the caller fails the run.
    monkeypatch.setattr(agent_mod.urllib.request, "urlopen", raise_http(413))
    assert agent_mod.post_run_artifact("http://cp", "t", RUN_ID, 1, "f", b"x") == "too_large"

    monkeypatch.setattr(agent_mod.urllib.request, "urlopen", raise_http(500))
    assert agent_mod.post_run_artifact("http://cp", "t", RUN_ID, 1, "f", b"x") == "retry"


# --- upload orchestration ----------------------------------------------------


def test_upload_artifacts_sends_each_file(monkeypatch):
    r = make_runner(monkeypatch)
    out_dir = r._runs[RUN_ID]["output_dir"]
    for name in ("a.json", "b.txt"):
        with open(os.path.join(out_dir, name), "wb") as f:
            f.write(b"data")

    sent = []
    monkeypatch.setattr(
        agent_mod, "post_run_artifact",
        lambda server, token, run_id, attempt, filename, data, *a, **k: sent.append(filename) or "accepted",
    )
    agent_mod._upload_artifacts("s", "t", r, RUN_ID, ATTEMPT)
    assert sorted(sent) == ["a.json", "b.txt"]
    assert r.has(RUN_ID)  # not aborted


def test_upload_artifacts_aborts_on_409(monkeypatch):
    r = make_runner(monkeypatch)
    out_dir = r._runs[RUN_ID]["output_dir"]
    with open(os.path.join(out_dir, "a.json"), "wb") as f:
        f.write(b"data")

    monkeypatch.setattr(
        agent_mod, "post_run_artifact",
        lambda *a, **k: "abort",
    )
    agent_mod._upload_artifacts("s", "t", r, RUN_ID, ATTEMPT)
    assert not r.has(RUN_ID)  # a 409 fenced the run -> aborted + cleaned up


# --- the working mount must be writable by a NON-ROOT container (2026-08-13) ---
# `tempfile.mkdtemp` gives 0700 owned by the agent's user. The container runs as
# whatever uid its IMAGE declares, so on a Linux worker anything but root could not
# write its own results. It was invisible for eleven weeks because the dummy runs as
# root: every test, every DoD and every experiment ran as root, so the failing
# condition never met the passing one. These tests are what make the two meet.


def test_output_dir_is_chmodded_to_run_dir_mode(monkeypatch):
    """Platform-independent: start() must chmod the working mount it just created.

    Asserted as a recorded CALL rather than a resulting mode, so this half also runs
    on Windows, where os.chmod cannot express POSIX bits at all -- which is precisely
    why the defect could hide on the development machine.
    """
    calls: list[tuple[str, int]] = []
    real_chmod = runner_mod.os.chmod
    monkeypatch.setattr(
        runner_mod.os, "chmod", lambda p, m: (calls.append((p, m)), real_chmod(p, m))[1]
    )

    r = make_runner(monkeypatch)
    out_dir = r._runs[RUN_ID]["output_dir"]

    assert (out_dir, runner_mod._RUN_DIR_MODE) in calls, (
        "start() did not chmod the working mount; a non-root container cannot write"
    )
    # sticky + world-writable, and the sticky bit is load-bearing: the dir is
    # world-writable for the run, so only a file's owner -- or the agent, which owns
    # the directory -- may remove it.
    assert runner_mod._RUN_DIR_MODE == 0o1777
    r.cleanup(RUN_ID)


@pytest.mark.skipif(os.name != "posix", reason="POSIX modes; Windows cannot express them")
def test_output_dir_mode_on_posix_admits_a_non_root_uid(monkeypatch):
    """On Linux -- where a worker actually runs -- check the real resulting mode.

    This is the assertion that would have caught the defect. It runs in CI, which is
    ubuntu-latest, so it executes on the platform the bug bites rather than the one
    that hides it.
    """
    r = make_runner(monkeypatch)
    out_dir = r._runs[RUN_ID]["output_dir"]

    mode = stat.S_IMODE(os.stat(out_dir).st_mode)
    assert mode & stat.S_IWOTH, f"mode {oct(mode)}: a non-root container cannot write"
    assert mode & stat.S_ISVTX, f"mode {oct(mode)}: world-writable without the sticky bit"
    assert mode == 0o1777, oct(mode)
    r.cleanup(RUN_ID)
