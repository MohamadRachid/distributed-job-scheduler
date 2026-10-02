"""Temporary disk on the worker — the scratch half of the storage quota policy.

The retained half lives in the control plane and is tested in
`control-plane/tests/test_storage_quota.py`; this is the half that runs where the
disk actually is.

What is pinned here (brief §3 R7–R9, §5):

  R7  a public run may write ONLY to /scratch and /tmp (and /checkpoint), all three
      folders of ONE host directory, with a read-only container root
  R8  the agent measures that directory every sample tick, and when it crosses the
      cap it stops the container and reports SCRATCH_QUOTA_EXCEEDED
  R9  the overshoot is bounded by write speed x sample tick — a bound, never zero
  R12 an assignment with no `scratch_mb` carries no cap, so an old control plane and
      an unowned job behave exactly as they did before

plus the two exit hints that turn a confusing crash into a named cause, and the
private-run case, whose scratch is a RAM folder the kernel sizes and which must
therefore never be sampled at all.

Pure-logic tests, no Docker daemon and no control plane: a fake docker client backs
the REAL Runner, so the tracked-state shape cannot drift from production.

Run from the repo root:  pytest agent/tests -q
"""

import os
import stat

import pytest

from agent import agent as agent_mod
from agent import runner as runner_mod
from agent.classify import (
    READ_ONLY_FILESYSTEM,
    SCRATCH_QUOTA_EXCEEDED,
    classify_failure,
)
from agent.runner import Runner

RUN_ID = "run-scratch"
ATTEMPT = 1
_MB = 1024 * 1024


# --- fakes -------------------------------------------------------------------


class FakeContainer:
    short_id = "fake0000"

    def __init__(self) -> None:
        self.stopped = False
        self.removed = False

    def logs(self, **_kw) -> bytes:
        return b""

    def stop(self, **_kw) -> None:
        self.stopped = True

    def remove(self, **_kw) -> None:
        self.removed = True

    def reload(self) -> None:
        pass


class FakeClient:
    def __init__(self) -> None:
        self.container = FakeContainer()
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
            def run(*_a, **kw):
                outer.run_kwargs = kw
                return outer.container

        return _Containers


def make_runner(monkeypatch, tmp_path, **start_kw) -> Runner:
    monkeypatch.setenv("AGENT_OUTPUT_ROOT", str(tmp_path))
    client = FakeClient()
    monkeypatch.setattr(runner_mod.docker, "from_env", lambda: client)
    r = Runner()
    r._fake_client = client
    r.start(RUN_ID, ATTEMPT, "img", ["python", "train.py"], {}, **start_kw)
    return r


def _write(path: str, mb: float) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        f.write(b"x" * int(mb * _MB))


# ===========================================================================
# R7 — one directory, three folders, a read-only root
# ===========================================================================


def test_prepare_builds_the_three_folders(monkeypatch, tmp_path):
    monkeypatch.setenv("AGENT_OUTPUT_ROOT", str(tmp_path))
    run_dir = runner_mod.prepare_run_dir(RUN_ID, ATTEMPT)
    assert os.path.isdir(run_dir)
    for name in (
        runner_mod.SCRATCH_DIR_NAME,
        runner_mod.TMP_DIR_NAME,
        runner_mod.CHECKPOINT_DIR_NAME,
    ):
        assert os.path.isdir(os.path.join(run_dir, name)), name
    # The path is deterministic, so a demonstration can point at the directory a
    # run's own log line names.
    assert run_dir == runner_mod.run_dir_path(RUN_ID, ATTEMPT)
    assert str(tmp_path) in run_dir and RUN_ID in run_dir


def test_prepare_clears_but_ensure_does_not(monkeypatch, tmp_path):
    """The destructive step belongs to the caller that knows an attempt is BEGINNING.

    An attempt must start with an empty scratch or its first measurement would charge
    it for the last one. But by the time the container starts, the previous attempt's
    checkpoint has already been staged into this same tree — so a `start` that cleared
    would destroy the state the resume exists to use. Found by a test, not by reading:
    staging then starting wiped the staged file and `checkpoint_path` came back None."""
    monkeypatch.setenv("AGENT_OUTPUT_ROOT", str(tmp_path))
    run_dir = runner_mod.prepare_run_dir(RUN_ID, ATTEMPT)
    staged = os.path.join(run_dir, runner_mod.CHECKPOINT_DIR_NAME, "state")
    _write(staged, 0.01)

    same = runner_mod.ensure_run_dir(RUN_ID, ATTEMPT)
    assert same == run_dir
    assert os.path.isfile(staged), "ensure must never clear what a caller staged"

    runner_mod.prepare_run_dir(RUN_ID, ATTEMPT)
    assert not os.path.isfile(staged), "prepare must start the attempt empty"


def test_start_mounts_the_two_writable_folders_and_locks_the_root(monkeypatch, tmp_path):
    r = make_runner(monkeypatch, tmp_path)
    kw = r._fake_client.run_kwargs
    binds = {v["bind"] for v in kw["volumes"].values()}
    assert binds == {"/scratch", "/tmp"}
    assert kw["read_only"] is True
    # OUTPUT_DIR points at the working mount, so a workload that honours it writes
    # where the agent both measures and collects from.
    assert kw["environment"]["OUTPUT_DIR"] == "/scratch"
    run_dir = r._runs[RUN_ID]["run_dir"]
    assert r._runs[RUN_ID]["output_dir"] == os.path.join(
        run_dir, runner_mod.SCRATCH_DIR_NAME
    )


@pytest.mark.skipif(os.name != "posix", reason="POSIX modes; Windows cannot express them")
def test_the_writable_folders_admit_a_container_running_as_any_uid(monkeypatch, tmp_path):
    """The container runs as whatever uid its IMAGE declares, which the agent cannot
    know because the job picks it. A 0700 folder would mean a non-root image cannot
    write its own results — the same defect the checkpoint mount had, on the two
    folders that replaced /output."""
    monkeypatch.setenv("AGENT_OUTPUT_ROOT", str(tmp_path))
    run_dir = runner_mod.prepare_run_dir(RUN_ID, ATTEMPT)
    for name in (runner_mod.SCRATCH_DIR_NAME, runner_mod.TMP_DIR_NAME):
        mode = stat.S_IMODE(os.stat(os.path.join(run_dir, name)).st_mode)
        assert mode & stat.S_IWOTH, f"{name} {oct(mode)}: a non-root container cannot write"
        assert mode & stat.S_IXOTH, f"{name} {oct(mode)}: a non-root container cannot enter"
        assert mode & stat.S_ISVTX, f"{name} {oct(mode)}: not sticky — anyone could delete"
    # The checkpoint folder is deliberately NOT sticky; see CHECKPOINT_DIR_MODE.
    ckpt = stat.S_IMODE(
        os.stat(os.path.join(run_dir, runner_mod.CHECKPOINT_DIR_NAME)).st_mode
    )
    assert not ckpt & stat.S_ISVTX


def test_cleanup_removes_the_whole_run_directory(monkeypatch, tmp_path):
    r = make_runner(monkeypatch, tmp_path)
    run_dir = r._runs[RUN_ID]["run_dir"]
    _write(os.path.join(run_dir, runner_mod.SCRATCH_DIR_NAME, "out.bin"), 0.1)
    r.cleanup(RUN_ID)
    assert not os.path.isdir(run_dir)


# ===========================================================================
# The measurement
# ===========================================================================


def test_scratch_used_counts_all_three_folders(monkeypatch, tmp_path):
    """One directory, one walk. A checkpoint is temporary disk on this worker like
    anything else here — what keeps it out of the RESULTS is `artifact_paths` walking
    `scratch/` alone, not where the folder sits."""
    r = make_runner(monkeypatch, tmp_path)
    run_dir = r._runs[RUN_ID]["run_dir"]
    _write(os.path.join(run_dir, runner_mod.SCRATCH_DIR_NAME, "a.bin"), 1)
    _write(os.path.join(run_dir, runner_mod.TMP_DIR_NAME, "b.bin"), 2)
    _write(os.path.join(run_dir, runner_mod.CHECKPOINT_DIR_NAME, "state"), 0.5)
    assert r.scratch_used_mb(RUN_ID) == pytest.approx(3.5, abs=0.01)

    # ...and nested files are counted too — a run that makes directories does not
    # escape the measurement by doing so.
    _write(os.path.join(run_dir, runner_mod.SCRATCH_DIR_NAME, "deep", "c.bin"), 1)
    assert r.scratch_used_mb(RUN_ID) == pytest.approx(4.5, abs=0.01)


def test_scratch_used_is_none_for_an_unknown_or_private_run(monkeypatch, tmp_path):
    """None is a real answer, not a gap. A private run has no writable host mount at
    all, so its scratch is the RAM folder the kernel sizes for it — exact and
    kernel-enforced, so there is nothing to sample and nothing a sample could add."""
    sealed = tmp_path / "sealed.bin"
    sealed.write_bytes(b"ciphertext")
    r = make_runner(
        monkeypatch, tmp_path,
        private=True, sealed_path=str(sealed), ticket="t", key_url="u",
    )
    assert r.scratch_used_mb(RUN_ID) is None
    assert r.scratch_used_mb("no-such-run") is None
    assert r._runs[RUN_ID]["run_dir"] is None
    # And the private container's shape is untouched: RAM folders, not host mounts.
    kw = r._fake_client.run_kwargs
    assert "tmpfs" in kw and kw["read_only"] is True
    assert all(v["mode"] == "ro" for v in kw["volumes"].values())


def test_the_sample_carries_the_scratch_reading(monkeypatch, tmp_path):
    """The number that stops a run is the same number stored for the person who later
    asks why it stopped."""
    r = make_runner(monkeypatch, tmp_path, scratch_mb=100)
    run_dir = r._runs[RUN_ID]["run_dir"]
    _write(os.path.join(run_dir, runner_mod.SCRATCH_DIR_NAME, "a.bin"), 2)
    monkeypatch.setattr(
        Runner, "sample_stats", lambda self, rid: {"cpu_pct": 1.0, "mem_used_mb": 5.0}
    )
    posted = []
    monkeypatch.setattr(
        agent_mod, "post_run_samples",
        lambda s, t, run_id, attempt, samples: posted.append(samples) or "accepted",
    )
    agent_mod._pump_all_samples("s", "t", r)
    assert posted and posted[0][0]["scratch_used_mb"] == pytest.approx(2, abs=0.01)


# ===========================================================================
# R8 — the cap is enforced
# ===========================================================================


def _capture_enforcement(monkeypatch):
    posted: list[tuple] = []
    monkeypatch.setattr(agent_mod, "post_run_samples", lambda *a, **k: "accepted")
    monkeypatch.setattr(
        agent_mod, "post_run_status",
        lambda s, t, run_id, attempt, state, code=None, reason=None, detail=None:
            posted.append((state, reason, detail)) or "accepted",
    )
    monkeypatch.setattr(Runner, "sample_stats", lambda self, rid: {"cpu_pct": 1.0})
    monkeypatch.setattr(
        agent_mod, "_upload_artifacts",
        lambda *a, **k: (_ for _ in ()).throw(
            AssertionError("a run stopped for filling its disk must upload nothing")
        ),
    )
    return posted


def test_over_the_cap_stops_the_container_and_names_the_reason(monkeypatch, tmp_path):
    r = make_runner(monkeypatch, tmp_path, scratch_mb=2)
    posted = _capture_enforcement(monkeypatch)
    run_dir = r._runs[RUN_ID]["run_dir"]
    container = r._fake_client.container
    _write(os.path.join(run_dir, runner_mod.SCRATCH_DIR_NAME, "big.bin"), 3)

    agent_mod._pump_all_samples("s", "t", r)

    assert container.stopped, "the container must be stopped, not left burning the disk"
    assert len(posted) == 1
    state, reason, detail = posted[0]
    assert state == "FAILED" and reason == SCRATCH_QUOTA_EXCEEDED
    assert "3.0 MB" in detail and "2 MB" in detail
    assert not r.has(RUN_ID), "the run is finished and must not be reported again"
    assert not os.path.isdir(run_dir), "its temporary disk is released with it"


def test_under_the_cap_nothing_happens(monkeypatch, tmp_path):
    r = make_runner(monkeypatch, tmp_path, scratch_mb=10)
    posted = _capture_enforcement(monkeypatch)
    run_dir = r._runs[RUN_ID]["run_dir"]
    _write(os.path.join(run_dir, runner_mod.SCRATCH_DIR_NAME, "small.bin"), 1)

    agent_mod._pump_all_samples("s", "t", r)

    assert posted == []
    assert r.has(RUN_ID) and not r._fake_client.container.stopped


def test_no_cap_is_never_enforced(monkeypatch, tmp_path):
    """R12 compatibility. An assignment without `scratch_mb` — an unowned job, or a
    control plane that predates this feature — carries no cap, and a run under it
    behaves exactly as every run did before 2026-09-04."""
    r = make_runner(monkeypatch, tmp_path)  # no scratch_mb
    posted = _capture_enforcement(monkeypatch)
    run_dir = r._runs[RUN_ID]["run_dir"]
    _write(os.path.join(run_dir, runner_mod.SCRATCH_DIR_NAME, "huge.bin"), 50)

    agent_mod._pump_all_samples("s", "t", r)

    assert posted == []
    assert r.scratch_limit_of(RUN_ID) is None
    assert r.has(RUN_ID)


def test_the_cap_is_checked_even_when_the_sample_post_fails(monkeypatch, tmp_path):
    """Posting a sample is best-effort. Stopping a run that is filling a shared
    machine's disk is not, and a network hiccup must not be what lets it carry on."""
    r = make_runner(monkeypatch, tmp_path, scratch_mb=1)
    posted = _capture_enforcement(monkeypatch)
    monkeypatch.setattr(agent_mod, "post_run_samples", lambda *a, **k: "retry")
    _write(
        os.path.join(r._runs[RUN_ID]["run_dir"], runner_mod.SCRATCH_DIR_NAME, "b.bin"), 2
    )

    agent_mod._pump_all_samples("s", "t", r)

    assert [p[1] for p in posted] == [SCRATCH_QUOTA_EXCEEDED]


def test_scratch_failure_waits_for_final_logs(monkeypatch, tmp_path):
    """Stopping an over-cap run must retain it when its final log post is retryable.

    The status endpoint rejects uploads after a terminal transition, and cleanup
    destroys the local log cursor.  Reporting FAILED before the final log is
    acknowledged therefore loses output permanently.
    """
    r = make_runner(monkeypatch, tmp_path, scratch_mb=1)
    monkeypatch.setattr(r._fake_client.container, "logs", lambda **_kw: b"last line\n")
    posted = _capture_enforcement(monkeypatch)
    monkeypatch.setattr(agent_mod, "post_run_logs", lambda *a, **k: "retry")
    _write(
        os.path.join(r._runs[RUN_ID]["run_dir"], runner_mod.SCRATCH_DIR_NAME, "b.bin"), 2
    )

    agent_mod._pump_all_samples("s", "t", r)

    assert posted == [], "terminal status must wait until final logs are acknowledged"
    assert r.has(RUN_ID), "retain the stopped run and its output for the next tick"


def test_a_fenced_run_is_aborted_rather_than_reported(monkeypatch, tmp_path):
    """A 409 while reporting the disk failure means the control plane has already
    given this run to somebody else. Abort, exactly as everywhere else."""
    r = make_runner(monkeypatch, tmp_path, scratch_mb=1)
    _capture_enforcement(monkeypatch)
    monkeypatch.setattr(agent_mod, "post_run_status", lambda *a, **k: "abort")
    _write(
        os.path.join(r._runs[RUN_ID]["run_dir"], runner_mod.SCRATCH_DIR_NAME, "b.bin"), 2
    )

    agent_mod._pump_all_samples("s", "t", r)
    assert not r.has(RUN_ID)


# ===========================================================================
# The assignment carries the cap through
# ===========================================================================


def test_an_assignment_carries_scratch_mb_into_the_run(monkeypatch, tmp_path):
    monkeypatch.setenv("AGENT_OUTPUT_ROOT", str(tmp_path))
    client = FakeClient()
    monkeypatch.setattr(runner_mod.docker, "from_env", lambda: client)
    monkeypatch.setattr(agent_mod, "post_run_status", lambda *a, **k: "accepted")
    r = Runner()

    agent_mod._act_on_assignments(
        "s", "t", r,
        [{"run_id": RUN_ID, "attempt": ATTEMPT, "image": "img",
          "entrypoint": [], "env": {}, "scratch_mb": 64}],
    )
    assert r.scratch_limit_of(RUN_ID) == 64
    assert r._runs[RUN_ID]["run_dir"] == runner_mod.run_dir_path(RUN_ID, ATTEMPT)
    # The checkpoint folder lives inside the same measured directory.
    assert r._runs[RUN_ID]["checkpoint_dir"] == os.path.join(
        r._runs[RUN_ID]["run_dir"], runner_mod.CHECKPOINT_DIR_NAME
    )


def test_an_old_control_planes_assignment_still_starts(monkeypatch, tmp_path):
    """P4's shape, from the other side: an assignment with no `scratch_mb` key at all
    is exactly what an older control plane sends, and the run must start normally."""
    monkeypatch.setenv("AGENT_OUTPUT_ROOT", str(tmp_path))
    client = FakeClient()
    monkeypatch.setattr(runner_mod.docker, "from_env", lambda: client)
    monkeypatch.setattr(agent_mod, "post_run_status", lambda *a, **k: "accepted")
    r = Runner()
    agent_mod._act_on_assignments(
        "s", "t", r,
        [{"run_id": RUN_ID, "attempt": ATTEMPT, "image": "img", "entrypoint": [], "env": {}}],
    )
    assert r.has(RUN_ID) and r.scratch_limit_of(RUN_ID) is None


# ===========================================================================
# The heartbeat's free-disk reading
# ===========================================================================


def test_the_heartbeat_carries_free_disk_for_the_working_root(monkeypatch, tmp_path):
    monkeypatch.setenv("AGENT_OUTPUT_ROOT", str(tmp_path))
    monkeypatch.setattr(agent_mod, "collect_usage", lambda: {"cpu_pct": 3.0})
    usage = agent_mod._usage_with_disk()
    assert usage["cpu_pct"] == 3.0
    assert isinstance(usage["disk_free_mb"], int) and usage["disk_free_mb"] >= 0


def test_an_unreadable_disk_omits_the_field_rather_than_guessing(monkeypatch):
    """The control plane reads a missing value as UNKNOWN and never excludes the
    machine for it. A placement refused because one reading failed would be a worse
    answer than a placement made."""
    monkeypatch.setattr(agent_mod, "collect_usage", lambda: {"cpu_pct": 3.0})
    monkeypatch.setattr(
        agent_mod.shutil, "disk_usage",
        lambda _p: (_ for _ in ()).throw(OSError("no such device")),
    )
    assert "disk_free_mb" not in agent_mod._usage_with_disk()


# ===========================================================================
# The two exit hints
# ===========================================================================


def test_out_of_space_is_named_for_a_public_run():
    reason, detail = classify_failure(
        {"OOMKilled": False, "ExitCode": 1}, 1, None,
        log_tail="Traceback...\nOSError: [Errno 28] No space left on device\n",
        scratch_mb=100,
    )
    assert reason == SCRATCH_QUOTA_EXCEEDED
    assert "100 MB" in detail and "temporary disk" in detail


def test_out_of_space_for_a_private_run_points_at_the_ram_folder():
    """Telling a private job's owner to ask for more disk would send them to fix the
    wrong thing: their working space is RAM, sized by PRIVATE_TMPFS_MB."""
    reason, detail = classify_failure(
        {"OOMKilled": False, "ExitCode": 1}, 1, None,
        log_tail="OSError: [Errno 28] No space left on device",
        private=True,
    )
    assert reason == SCRATCH_QUOTA_EXCEEDED
    assert "PRIVATE_TMPFS_MB" in detail and "RAM" in detail


def test_a_write_outside_the_mounts_is_named_rather_than_left_as_exited_1():
    reason, detail = classify_failure(
        {"OOMKilled": False, "ExitCode": 1}, 1, None,
        log_tail="OSError: [Errno 30] Read-only file system: '/var/out.bin'",
    )
    assert reason == READ_ONLY_FILESYSTEM
    assert "/scratch" in detail and "/tmp" in detail


def test_the_disk_hints_never_fire_on_an_ordinary_failure():
    """A run that failed for its own reasons must keep its own reason. The hints match
    the operating system's own words, so an ordinary traceback cannot trip them."""
    reason, _detail = classify_failure(
        {"OOMKilled": False, "ExitCode": 1}, 1, None,
        log_tail="ValueError: bad hyperparameter",
    )
    assert reason == "APP_ERROR"
    assert classify_failure({"OOMKilled": False, "ExitCode": 0}, 0, None) is None


def test_an_oom_kill_still_wins_over_a_read_only_message():
    """Order matters: a kernel OOM kill is a harder fact than a line in the log tail,
    and a run that was killed for memory must not be reported as a disk problem."""
    reason, _detail = classify_failure(
        {"OOMKilled": True, "ExitCode": 137}, 137, None,
        log_tail="Read-only file system",
        mem_limit_mb=128,
    )
    assert reason == "OOM_KILLED"
