"""Agent-side W5b pure helpers — classify_failure, parse_progress, compute_stats.

Host-side, dependency-free (agent/classify.py imports only stdlib), the same pattern
as test_runner_logs.py. These pin the HONESTY rules as code: reasons come from hard
facts, OOM is told apart from a plain SIGKILL, CPU is never a kill reason, and a
SUCCEEDED run gets no reason.

Run from the repo root:  pytest agent/tests -q
"""

from agent.classify import classify_failure, compute_stats, parse_progress, progress_fraction


# --- classify_failure -------------------------------------------------------


def test_oom_killed_beats_exit_137_and_names_the_limit():
    # An OOM kill often ALSO surfaces as exit 137 — the OOMKilled flag must win, and
    # the detail must name the limit the agent applied.
    reason, detail = classify_failure(
        {"OOMKilled": True, "ExitCode": 137}, 137, None, mem_limit_mb=128
    )
    assert reason == "OOM_KILLED"
    assert "128 MB" in detail


def test_plain_137_without_oom_is_killed_not_oom():
    reason, detail = classify_failure({"OOMKilled": False, "ExitCode": 137}, 137, None)
    assert reason == "KILLED"
    assert "out-of-memory" in detail  # explicitly says it is NOT an OOM kill


def test_segfault_139_is_app_crash():
    reason, _ = classify_failure({"OOMKilled": False, "ExitCode": 139}, 139, None)
    assert reason == "APP_CRASH"


def test_sigterm_143_is_terminated():
    reason, _ = classify_failure({"OOMKilled": False, "ExitCode": 143}, 143, None)
    assert reason == "TERMINATED"


def test_plain_nonzero_is_app_error():
    reason, detail = classify_failure({"OOMKilled": False, "ExitCode": 1}, 1, None)
    assert reason == "APP_ERROR"
    assert "code 1" in detail


def test_exit_zero_is_not_a_failure():
    assert classify_failure({"OOMKilled": False, "ExitCode": 0}, 0, None) is None


def test_gpu_start_error():
    err = RuntimeError("could not select device driver \"\" with capabilities: [[gpu]]")
    reason, _ = classify_failure(None, None, err)
    assert reason == "GPU_UNAVAILABLE"


def test_image_not_found_start_error():
    err = RuntimeError("No such image: nope:latest")
    reason, detail = classify_failure(None, None, err)
    assert reason == "IMAGE_ERROR"
    assert "nope:latest" in detail


def test_generic_start_error_is_image_error():
    err = RuntimeError("failed to create shim: entrypoint not executable")
    reason, _ = classify_failure(None, None, err)
    assert reason == "IMAGE_ERROR"


# --- parse_progress ---------------------------------------------------------


def test_parse_progress_good_line():
    assert parse_progress('##PROGRESS {"epoch": 3, "total": 25, "loss": 0.42}') == {
        "epoch": 3, "total": 25, "loss": 0.42
    }


def test_parse_progress_malformed_json_is_none_not_error():
    assert parse_progress("##PROGRESS not json") is None       # never raises


def test_parse_progress_ignores_normal_lines():
    assert parse_progress("epoch 3/25 loss=0.42") is None


def test_progress_fraction():
    assert progress_fraction({"epoch": 3, "total": 25}) == 3 / 25
    assert progress_fraction({"epoch": 3}) is None             # missing total
    assert progress_fraction({"epoch": 3, "total": 0}) is None  # non-positive total
    assert progress_fraction({"epoch": 30, "total": 25}) == 1.0  # clamped to 1


# --- compute_stats ----------------------------------------------------------


def test_compute_stats_from_a_docker_sample():
    raw = {
        "cpu_stats": {
            "cpu_usage": {"total_usage": 1_100_000_000},
            "system_cpu_usage": 10_000_000_000,
            "online_cpus": 4,
        },
        "precpu_stats": {
            "cpu_usage": {"total_usage": 1_000_000_000},
            "system_cpu_usage": 9_600_000_000,
        },
        "memory_stats": {
            "usage": 100 * 1024 * 1024,
            "limit": 128 * 1024 * 1024,
            "stats": {"cache": 10 * 1024 * 1024},
        },
    }
    out = compute_stats(raw)
    assert out["cpu_pct"] == 100.0        # (100M/400M) * 4 * 100
    assert out["mem_used_mb"] == 90.0     # usage - cache
    assert out["mem_limit_mb"] == 128.0


def test_compute_stats_empty_is_none():
    assert compute_stats(None) is None
    assert compute_stats({}) is None
