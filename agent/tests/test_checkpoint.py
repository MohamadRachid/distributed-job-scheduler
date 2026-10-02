"""Agent-side checkpoint & resume (2026-08-13) — fetch, verify, stage, sweep.

Pure-logic tests, no Docker daemon and no control plane: a fake docker client backs
the REAL Runner (so the tracked-state shape cannot drift), and a fake urlopen stands
in for the control plane.

The agent's half of the feature is mostly about what it does when things are NOT
fine, so that is what most of these cover. A checkpoint the agent cannot verify is
treated as ABSENT and the run starts from the beginning — a run that crashes on
resume is worse than one that starts over.

Run from the repo root:  pytest agent/tests -q
(Not part of the control-plane container suite — the agent is host software.)
"""

import hashlib
import os
import stat
import urllib.error

import pytest

from agent import agent as agent_mod
from agent import runner as runner_mod
from agent.runner import Runner

RUN_ID = "run-1"
ATTEMPT = 2  # a re-dispatched run: attempt 1 died somewhere else
BODY = b'{"epoch": 3}'
DIGEST = hashlib.sha256(BODY).hexdigest()


# --- fakes -------------------------------------------------------------------


class FakeContainer:
    short_id = "fake0000"

    def logs(self, **_kw) -> bytes:
        return b""

    def remove(self, **_kw) -> None:
        pass


class FakeClient:
    """Records the kwargs `containers.run` was called with, so the mount and the
    environment the container really receives can be asserted."""

    def __init__(self, container: FakeContainer) -> None:
        self._container = container
        self.run_kwargs: dict = {}

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
        outer = self

        class _Containers:
            @staticmethod
            def run(*_a, **kw):
                outer.run_kwargs = kw
                return outer._container

        return _Containers


class FakeResp:
    """Enough of an http.client.HTTPResponse for the agent's stdlib calls."""

    def __init__(self, status=200, body=b"", headers=None) -> None:
        self.status = status
        self._body = body
        self.headers = headers or {}

    def read(self) -> bytes:
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *_a):
        return False


def make_runner(monkeypatch, checkpoint_dir=None) -> Runner:
    client = FakeClient(FakeContainer())
    monkeypatch.setattr(runner_mod.docker, "from_env", lambda: client)
    r = Runner()
    r.node_name = "node-x"
    r.start(
        RUN_ID, ATTEMPT, "fyp-dummy:latest", ["python", "train.py"], {},
        checkpoint_dir=checkpoint_dir,
    )
    r._fake_client = client  # for asserting the container kwargs
    return r


def fake_get(monkeypatch, resp_or_exc):
    """Point the agent's urlopen at one canned response (or make it raise)."""

    def _urlopen(req, timeout=None):
        if isinstance(resp_or_exc, Exception):
            raise resp_or_exc
        return resp_or_exc

    monkeypatch.setattr(agent_mod.urllib.request, "urlopen", _urlopen)


# ===========================================================================
# FETCH + VERIFY — absent is the safe answer, and it is reached from every side
# ===========================================================================


def test_good_checkpoint_is_returned(monkeypatch):
    fake_get(monkeypatch, FakeResp(200, BODY, {"X-Checkpoint-Sha256": DIGEST}))
    assert agent_mod.fetch_checkpoint("http://cp:8000", "tok", RUN_ID, ATTEMPT) == BODY


def test_204_means_start_from_the_beginning(monkeypatch):
    """The ordinary case, and it must not look like a failure."""
    fake_get(monkeypatch, FakeResp(204, b""))
    assert agent_mod.fetch_checkpoint("http://cp:8000", "tok", RUN_ID, ATTEMPT) is None


def test_bad_digest_is_treated_as_absent(monkeypatch):
    """THE guard. The bytes do not match the digest the control plane recorded when
    it stored them, so the file is corrupt or truncated. We throw it away and train
    from the beginning rather than hand a workload state it may crash on."""
    fake_get(
        monkeypatch,
        FakeResp(200, b"corrupted", {"X-Checkpoint-Sha256": DIGEST}),
    )
    assert agent_mod.fetch_checkpoint("http://cp:8000", "tok", RUN_ID, ATTEMPT) is None


def test_missing_digest_is_refused(monkeypatch):
    """Unverifiable is refused rather than accepted on trust — a row written before
    digests existed can still be read, and must not be resumed from blindly."""
    fake_get(monkeypatch, FakeResp(200, BODY, {}))
    assert agent_mod.fetch_checkpoint("http://cp:8000", "tok", RUN_ID, ATTEMPT) is None


def test_fenced_409_raises_stale_attempt(monkeypatch):
    """2026-09-07 audit (defect F): a 409 is the fence, not 'nothing saved'. It
    means the control plane has given this run to a newer attempt, and the caller
    must start nothing rather than train from the beginning under a dead attempt.
    Every other HTTP error still means absent (the test below)."""
    fake_get(
        monkeypatch,
        urllib.error.HTTPError("u", 409, "stale attempt", None, None),
    )
    with pytest.raises(agent_mod.StaleAttempt):
        agent_mod.fetch_checkpoint("http://cp:8000", "tok", RUN_ID, ATTEMPT)


def test_other_http_errors_are_still_absent(monkeypatch):
    fake_get(
        monkeypatch,
        urllib.error.HTTPError("u", 500, "boom", None, None),
    )
    assert agent_mod.fetch_checkpoint("http://cp:8000", "tok", RUN_ID, ATTEMPT) is None


def test_unreachable_control_plane_is_absent(monkeypatch):
    fake_get(monkeypatch, urllib.error.URLError("no route"))
    assert agent_mod.fetch_checkpoint("http://cp:8000", "tok", RUN_ID, ATTEMPT) is None


def _stage(server, token, a):
    """Stage a checkpoint the way `_act_on_assignments` does: build the run's ONE host
    directory first, then stage into its `checkpoint/` folder.

    From 2026-09-04 the checkpoint folder lives INSIDE that directory, so it is
    counted as the temporary disk it is. The two steps stayed separate rather than
    being folded together because the caller has to build the directory before the
    container starts AND before the previous attempt's state is fetched into it, and a
    function that did both would have to be called from a place that cannot know
    whether the run is private (a private run gets no host directory at all)."""
    run_dir = runner_mod.prepare_run_dir(a["run_id"], a["attempt"])
    return agent_mod._stage_checkpoint(server, token, a, run_dir)


# ===========================================================================
# STAGING — the mount always exists; the fetch only happens when it can help
# ===========================================================================


def test_stage_writes_the_checkpoint_and_makes_the_dir(monkeypatch, tmp_path):
    monkeypatch.setenv("AGENT_OUTPUT_ROOT", str(tmp_path))
    fake_get(monkeypatch, FakeResp(200, BODY, {"X-Checkpoint-Sha256": DIGEST}))

    extra = _stage(
        "http://cp:8000", "tok", {"run_id": RUN_ID, "attempt": 2}
    )
    d = extra["checkpoint_dir"]
    assert os.path.isdir(d)
    with open(os.path.join(d, agent_mod.CHECKPOINT_FILENAME), "rb") as f:
        assert f.read() == BODY


def test_first_attempt_makes_the_dir_without_asking(monkeypatch, tmp_path):
    """Attempt 1 cannot have a checkpoint — they are written by earlier attempts of
    the same run, and there is no earlier attempt. So the ordinary case costs no
    extra request at all, while the directory still exists to write the FIRST one."""
    monkeypatch.setenv("AGENT_OUTPUT_ROOT", str(tmp_path))
    called = {"n": 0}

    def _boom(*_a, **_kw):
        called["n"] += 1
        raise AssertionError("attempt 1 must not ask for a checkpoint")

    monkeypatch.setattr(agent_mod, "fetch_checkpoint", _boom)

    extra = _stage(
        "http://cp:8000", "tok", {"run_id": RUN_ID, "attempt": 1}
    )
    assert called["n"] == 0
    assert os.path.isdir(extra["checkpoint_dir"])
    assert os.listdir(extra["checkpoint_dir"]) == []  # nothing staged


def test_absent_checkpoint_still_leaves_a_writable_dir(monkeypatch, tmp_path):
    monkeypatch.setenv("AGENT_OUTPUT_ROOT", str(tmp_path))
    fake_get(monkeypatch, FakeResp(204, b""))
    extra = _stage(
        "http://cp:8000", "tok", {"run_id": RUN_ID, "attempt": 3}
    )
    assert os.path.isdir(extra["checkpoint_dir"])
    assert os.listdir(extra["checkpoint_dir"]) == []


# ===========================================================================
# THE MOUNT — separate from /output on purpose
# ===========================================================================


def test_start_mounts_checkpoint_dir_and_sets_the_path(monkeypatch, tmp_path):
    d = tmp_path / "ckpt"
    d.mkdir()
    r = make_runner(monkeypatch, checkpoint_dir=str(d))
    kw = r._fake_client.run_kwargs
    assert kw["volumes"][str(d)] == {"bind": "/checkpoint", "mode": "rw"}
    assert kw["environment"]["CHECKPOINT_PATH"] == "/checkpoint/state"
    # ...and the run's working mount is still there beside it. It is `/scratch` since
    # 2026-09-04, where it used to be `/output`; the point this test makes is
    # unchanged and is the reason the two are separate mounts at all — a checkpoint
    # is working state, and `artifact_paths` walks only the working mount, so a
    # checkpoint can never be swept up and uploaded as this run's result.
    assert any(v["bind"] == "/scratch" for v in kw["volumes"].values())


def test_a_checkpoint_is_never_swept_up_as_a_result(monkeypatch, tmp_path):
    """The isolation guard, agent-side. `artifact_paths` walks the working mount only
    (`/scratch` since 2026-09-04, `/output` before it), so a checkpoint written to its
    own mount cannot be uploaded as this run's result — the separation does the work,
    not a rule anyone has to remember."""
    d = tmp_path / "ckpt"
    d.mkdir()
    r = make_runner(monkeypatch, checkpoint_dir=str(d))
    with open(d / "state", "wb") as f:
        f.write(BODY)
    out_dir = r._runs[RUN_ID]["output_dir"]
    with open(os.path.join(out_dir, "metrics.json"), "wb") as f:
        f.write(b"{}")

    assert [name for name, _p, _s in r.artifact_paths(RUN_ID)] == ["metrics.json"]
    assert r.checkpoint_path(RUN_ID) == os.path.join(str(d), "state")


def test_staged_file_is_exactly_where_the_container_and_the_sweep_look(
    monkeypatch, tmp_path
):
    """THE round trip, and the test this feature was missing.

    Staging writes a file; the container reads one; the sweep uploads one. All three
    must mean the SAME file. They did not: the runner mounted `<dir>/state` while the
    agent staged `<dir>/checkpoint`, so the container found nothing and every resume
    silently started over — while both halves passed their own tests, because each
    used its own name and agreed with itself. A live run caught it.

    So this asserts the join rather than either side: stage, then start a run against
    that same directory, and check that what the runner hands the container and what
    the sweep picks up are the file staging actually wrote."""
    monkeypatch.setenv("AGENT_OUTPUT_ROOT", str(tmp_path))
    fake_get(monkeypatch, FakeResp(200, BODY, {"X-Checkpoint-Sha256": DIGEST}))

    extra = _stage(
        "http://cp:8000", "tok", {"run_id": RUN_ID, "attempt": ATTEMPT}
    )
    staged = os.path.join(extra["checkpoint_dir"], agent_mod.CHECKPOINT_FILENAME)
    assert os.path.isfile(staged)

    r = make_runner(monkeypatch, checkpoint_dir=extra["checkpoint_dir"])

    # 1. what the sweep would upload is the file staging wrote
    assert r.checkpoint_path(RUN_ID) == staged

    # 2. what the container is told to read maps to that same file
    container_path = r._fake_client.run_kwargs["environment"]["CHECKPOINT_PATH"]
    mount = [
        host for host, m in r._fake_client.run_kwargs["volumes"].items()
        if m["bind"] == "/checkpoint"
    ][0]
    assert container_path == "/checkpoint/" + os.path.basename(staged)
    assert os.path.join(mount, os.path.basename(container_path)) == staged

    # 3. and its contents are the bytes that were fetched
    with open(staged, "rb") as f:
        assert f.read() == BODY


def test_no_checkpoint_dir_means_no_path_and_no_env(monkeypatch):
    """A run started without one — an old assignment, or a private run — behaves
    exactly as it did before this feature existed."""
    r = make_runner(monkeypatch)
    assert r.checkpoint_path(RUN_ID) is None
    assert "CHECKPOINT_PATH" not in r._fake_client.run_kwargs["environment"]


def test_cleanup_wipes_the_checkpoint_dir(monkeypatch, tmp_path):
    d = tmp_path / "ckpt"
    d.mkdir()
    r = make_runner(monkeypatch, checkpoint_dir=str(d))
    with open(d / "state", "wb") as f:
        f.write(BODY)
    r.cleanup(RUN_ID)
    assert not os.path.isdir(d)  # the copy that matters is in object storage


# ===========================================================================
# THE SWEEP — save the work, cheaply
# ===========================================================================


def _capture_uploads(monkeypatch):
    sent = []

    def _post(server, token, run_id, attempt, filename, data, ctype="x", kind="result"):
        sent.append({"filename": filename, "data": data, "kind": kind, "attempt": attempt})
        return "accepted"

    monkeypatch.setattr(agent_mod, "post_run_artifact", _post)
    return sent


def test_sweep_uploads_the_checkpoint_with_its_kind(monkeypatch, tmp_path):
    d = tmp_path / "ckpt"
    d.mkdir()
    r = make_runner(monkeypatch, checkpoint_dir=str(d))
    with open(d / "state", "wb") as f:
        f.write(BODY)
    sent = _capture_uploads(monkeypatch)
    agent_mod._checkpoint_state.clear()

    agent_mod._upload_checkpoints("http://cp:8000", "tok", r)
    assert len(sent) == 1
    assert sent[0]["kind"] == "checkpoint"
    assert sent[0]["data"] == BODY
    assert sent[0]["attempt"] == ATTEMPT
    # The stable name is what keeps one checkpoint per attempt in storage.
    assert sent[0]["filename"] == agent_mod.CHECKPOINT_FILENAME


def test_unchanged_file_is_not_uploaded_twice(monkeypatch, tmp_path):
    """Same bytes, so re-sending them buys nothing. The interval is forced to zero
    so this tests the change check and not the clock."""
    monkeypatch.setattr(agent_mod, "CHECKPOINT_UPLOAD_INTERVAL_S", 0.0)
    d = tmp_path / "ckpt"
    d.mkdir()
    r = make_runner(monkeypatch, checkpoint_dir=str(d))
    with open(d / "state", "wb") as f:
        f.write(BODY)
    sent = _capture_uploads(monkeypatch)
    agent_mod._checkpoint_state.clear()

    agent_mod._upload_checkpoints("http://cp:8000", "tok", r)
    agent_mod._upload_checkpoints("http://cp:8000", "tok", r)
    assert len(sent) == 1

    # ...but a NEW save is picked up.
    with open(d / "state", "wb") as f:
        f.write(b'{"epoch": 9}')
    os.utime(d / "state", (0, 0))  # force a different mtime on a fast filesystem
    agent_mod._upload_checkpoints("http://cp:8000", "tok", r)
    assert len(sent) == 2
    assert sent[1]["data"] == b'{"epoch": 9}'


def test_sweep_is_quiet_when_there_is_nothing_saved(monkeypatch, tmp_path):
    d = tmp_path / "ckpt"
    d.mkdir()
    r = make_runner(monkeypatch, checkpoint_dir=str(d))
    sent = _capture_uploads(monkeypatch)
    agent_mod._checkpoint_state.clear()
    agent_mod._upload_checkpoints("http://cp:8000", "tok", r)
    assert sent == []


def test_sweep_aborts_the_run_on_a_fence(monkeypatch, tmp_path):
    """A 409 means the control plane has re-dispatched this run elsewhere. The same
    thing happens here as everywhere else in the agent: stop at once."""
    d = tmp_path / "ckpt"
    d.mkdir()
    r = make_runner(monkeypatch, checkpoint_dir=str(d))
    with open(d / "state", "wb") as f:
        f.write(BODY)
    monkeypatch.setattr(agent_mod, "post_run_artifact", lambda *a, **k: "abort")
    agent_mod._checkpoint_state.clear()

    agent_mod._upload_checkpoints("http://cp:8000", "tok", r)
    assert not r.has(RUN_ID)


def test_sweep_state_does_not_grow(monkeypatch, tmp_path):
    """The per-run bookkeeping is pruned against the tracked runs, so a long-lived
    agent does not accumulate an entry per run it has ever executed."""
    d = tmp_path / "ckpt"
    d.mkdir()
    r = make_runner(monkeypatch, checkpoint_dir=str(d))
    agent_mod._checkpoint_state.clear()
    agent_mod._checkpoint_state["a-finished-run"] = {"at": 0.0, "stamp": (1, 1)}
    _capture_uploads(monkeypatch)

    agent_mod._upload_checkpoints("http://cp:8000", "tok", r)
    assert "a-finished-run" not in agent_mod._checkpoint_state


# ===========================================================================
# PERMISSIONS — the mount has to admit a container that is not root
# (2026-08-13; the sibling of the /output fix in test_runner_artifacts.py)
# ===========================================================================
#
# The same `tempfile.mkdtemp` defect one directory over, and worse here: every step of
# checkpointing is best-effort by design, so a permission error is swallowed by code
# that is individually right to swallow it. `save()` returns False into a training loop
# told to ignore it, and the sweep logs and moves on — so the feature degrades to
# SILENCE rather than failing. Absent-is-not-an-error is correct, and it is exactly
# what hid this, which is why the check has to come from outside.


def test_stage_chmods_the_directory_and_the_staged_file(monkeypatch, tmp_path):
    """Platform-independent: recorded CALLS, not resulting modes.

    Asserted this way so this half runs on Windows too, where os.chmod cannot express
    POSIX bits at all — which is precisely why the defect could hide on the machine
    that wrote the code.
    """
    monkeypatch.setenv("AGENT_OUTPUT_ROOT", str(tmp_path))
    fake_get(monkeypatch, FakeResp(200, BODY, {"X-Checkpoint-Sha256": DIGEST}))

    calls: list[tuple[str, int]] = []
    real_chmod = agent_mod.os.chmod
    monkeypatch.setattr(
        agent_mod.os, "chmod", lambda p, m: (calls.append((p, m)), real_chmod(p, m))[1]
    )

    extra = _stage(
        "http://cp:8000", "tok", {"run_id": RUN_ID, "attempt": ATTEMPT}
    )
    d = extra["checkpoint_dir"]
    staged = os.path.join(d, agent_mod.CHECKPOINT_FILENAME)

    assert (d, agent_mod.CHECKPOINT_DIR_MODE) in calls, (
        "the checkpoint mount was not chmodded; a non-root container cannot use it"
    )
    assert (staged, agent_mod.CHECKPOINT_FILE_MODE) in calls, (
        "the staged file was left at the agent's umask; the container may not read it"
    )


def test_checkpoint_dir_mode_is_deliberately_not_the_output_mode():
    """0777, NOT /output's 1777 — and the difference is the point.

    Under the sticky bit a process may only remove or rename-over a file it owns, or
    one in a directory it owns. The agent stages a file into THIS directory before the
    container starts, and the container's uid owns neither it nor the directory — so
    `save()`'s temp-file-then-rename is refused and the resumed run reads its state and
    can never save a new one. Measured both ways in
    `docs/evidence/real_workload_2026-08-13/checkpoint_permissions.txt`.

    This test exists so that copying `_RUN_DIR_MODE` across — which is what the obvious
    fix looks like — fails loudly instead of shipping a half-working resume.
    """
    assert runner_mod.CHECKPOINT_DIR_MODE == 0o777
    assert not runner_mod.CHECKPOINT_DIR_MODE & 0o1000, (
        "the sticky bit stops the container replacing the file the agent staged"
    )
    assert runner_mod._RUN_DIR_MODE == 0o1777  # /output is different, on purpose


@pytest.mark.skipif(os.name != "posix", reason="POSIX modes; Windows cannot express them")
def test_staged_mount_on_posix_admits_a_non_root_uid(monkeypatch, tmp_path):
    """On Linux — where a worker actually runs — check the real resulting modes.

    This is the assertion that would have caught the defect. It runs in CI, which is
    ubuntu-latest, so it executes on the platform the bug bites rather than the one
    that hides it."""
    monkeypatch.setenv("AGENT_OUTPUT_ROOT", str(tmp_path))
    fake_get(monkeypatch, FakeResp(200, BODY, {"X-Checkpoint-Sha256": DIGEST}))

    extra = _stage(
        "http://cp:8000", "tok", {"run_id": RUN_ID, "attempt": ATTEMPT}
    )
    d = extra["checkpoint_dir"]
    staged = os.path.join(d, agent_mod.CHECKPOINT_FILENAME)

    dmode = stat.S_IMODE(os.stat(d).st_mode)
    assert dmode & stat.S_IWOTH, f"dir {oct(dmode)}: a non-root container cannot write"
    assert dmode & stat.S_IXOTH, f"dir {oct(dmode)}: a non-root container cannot enter"
    assert not dmode & stat.S_ISVTX, (
        f"dir {oct(dmode)}: sticky — the container could not replace the staged file"
    )

    fmode = stat.S_IMODE(os.stat(staged).st_mode)
    assert fmode & stat.S_IROTH, f"file {oct(fmode)}: the container cannot read it"
