"""Pure helpers that interpret what a container DID — for W5b run diagnostics.

Agent-side and dependency-free (stdlib only), so they run on the worker and are
tested host-side (agent/tests), the same pattern as runner.py's log logic.

Three jobs:
  classify_failure  -> why a run died (hard facts: OOMKilled, Docker start errors,
                       decoded exit signals). A run that did not fail -> None.
  parse_progress    -> read a `##PROGRESS {json}` line the workload printed.
  compute_stats     -> turn one docker-stats sample into cpu%/mem-used/mem-limit.

Honesty rule: reasons come from hard facts only. CPU is
NEVER a kill reason (RAM kills, CPU starves). A SUCCEEDED run gets no reason.
"""

import json

# Short machine labels (kept in step with app/diagnostics.py where they overlap).
OOM_KILLED = "OOM_KILLED"
GPU_UNAVAILABLE = "GPU_UNAVAILABLE"
IMAGE_ERROR = "IMAGE_ERROR"
KILLED = "KILLED"
APP_CRASH = "APP_CRASH"
TERMINATED = "TERMINATED"
APP_ERROR = "APP_ERROR"
# W6b: the seal did not verify — the sealed input was changed, or the key was wrong.
INTEGRITY_ERROR = "INTEGRITY_ERROR"
# 2026-09-04 (storage quota policy). Two different disk failures, kept apart because
# the fix is different: one means the run used more temporary disk than it was allowed,
# the other means it tried to write somewhere it is not allowed to write at all.
SCRATCH_QUOTA_EXCEEDED = "SCRATCH_QUOTA_EXCEEDED"
READ_ONLY_FILESYSTEM = "READ_ONLY_FILESYSTEM"
# 2026-09-06 (sealed by default). The container finished cleanly and wrote a result
# that was NOT sealed, so the control plane refused to store it. Kept apart from
# INTEGRITY_ERROR on purpose: that one means bytes we hold did not open, this one
# means bytes we were offered were never sealed. Different fix — one is a corrupted
# or tampered file, the other is one line in the workload's own code.
UNSEALED_OUTPUT = "UNSEALED_OUTPUT"
# 2026-09-07 (walk 1, row 55). A result file was refused for its SIZE — over the
# per-file cap the agent applies before sending, or the one the control plane applies
# on arrival — so the run has no stored result and must not read as a success. The
# detail names the file, its size and the cap.
ARTIFACT_TOO_LARGE = "ARTIFACT_TOO_LARGE"
# The control plane's own label for a refusal at the owner's retained-storage cap.
# Named here so the agent can post it as the reason when it sees that refusal; the
# control plane decides the outcome from its own stamp either way (protocol.md §9).
STORAGE_QUOTA_EXCEEDED = "STORAGE_QUOTA_EXCEEDED"
# 2026-09-07 (walk 1, row 64). A user asked for the run to stop; the control plane
# told this worker at a heartbeat (`commands`), the worker stopped the container, and
# this is the reason it posts — the same label the control plane uses when it ends a
# cancelled run itself.
CANCELLED = "CANCELLED"

# What the kernel says when a filesystem is full, and when a write lands outside the
# two writable mounts. Matching the message the operating system prints is the same
# "one printed line is the contract" idea as ##PROGRESS and the integrity marker: it
# costs nothing, it works for any workload without asking it to cooperate, and it
# turns a confusing "exited 1" into a named cause (NFR-8).
_NO_SPACE = "no space left on device"
_READ_ONLY = "read-only file system"


def _tail_has(log_tail, needle: str) -> bool:
    return bool(log_tail) and needle in str(log_tail).lower()

# The startup helper prints this exact marker before exiting when AES-GCM rejects
# the blob. Matching a printed marker (rather than an exit code) is deliberate: it is
# the same "one printed line is the contract" idea as ##PROGRESS, so any real
# workload that adopts the helper gets the diagnosis for free.
INTEGRITY_MARKER = "##INTEGRITY_ERROR"


def _integrity_detail(log_tail):
    """If the helper reported a broken seal, return its message. Scans from the end,
    so the marker is found even behind a long traceback."""
    if not log_tail:
        return None
    for line in reversed(str(log_tail).splitlines()):
        stripped = line.strip()
        if stripped.startswith(INTEGRITY_MARKER):
            rest = stripped[len(INTEGRITY_MARKER):].lstrip(": ").strip()
            return rest or "the sealed input failed its integrity check"
    return None


def classify_failure(
    inspect_state,
    exit_code,
    start_error,
    mem_limit_mb=None,
    log_tail=None,
    scratch_mb=None,
    private=False,
):
    """Return (reason, detail) for a run that failed, or None if it did not fail.

    inspect_state : the container's Docker `State` dict (has OOMKilled, ExitCode),
                    or None when the container never started.
    exit_code     : the exit code the agent observed (may repeat State.ExitCode).
    start_error   : the exception raised trying to START the container, or None.
    mem_limit_mb  : the memory cap the agent applied, so an OOM detail names it.
    log_tail      : the container's output (W6b), scanned for the integrity marker
                    and (2026-09-04) for the two disk messages the kernel prints.
    scratch_mb    : the temporary-disk cap applied, so a disk detail names it.
    private       : whether this was a private run, whose "disk" is a RAM folder.
    """
    # 0) W6b — a broken seal. Checked FIRST because it is the most specific fact we
    #    can have about a private run: AES-GCM's tag did not match, which the maths
    #    guarantees means the bytes changed (or the key was wrong). Everything else
    #    below would only report the symptom ("exited 1").
    integrity = _integrity_detail(log_tail)
    if integrity is not None:
        return (
            INTEGRITY_ERROR,
            f"Sealed input failed its integrity check — {integrity}. The data was "
            "changed after it was sealed, so the run was stopped rather than trained "
            "on data we cannot vouch for.",
        )

    # 1) Failed to even start — a Docker/engine error, not a program exit.
    if start_error is not None:
        msg = str(start_error).strip()
        low = msg.lower()
        if "could not select device driver" in low or ("nvidia" in low and "driver" in low):
            return (
                GPU_UNAVAILABLE,
                "GPU unavailable: the machine has no usable GPU driver for this container.",
            )
        if any(
            k in low
            for k in ("not found", "no such image", "manifest", "pull access", "repository does not exist")
        ):
            return (IMAGE_ERROR, f"Image error: {msg}")
        return (IMAGE_ERROR, f"The container could not start: {msg}")

    state = inspect_state or {}

    # 2) The kernel OOM-killed it — the ONE provable RAM-overload signal. Checked
    #    before exit codes because an OOM kill often also surfaces as exit 137.
    if state.get("OOMKilled"):
        limit = f" (limit {int(mem_limit_mb)} MB)" if mem_limit_mb else ""
        return (
            OOM_KILLED,
            "RAM overload: the kernel killed the container after it exceeded its "
            f"memory limit{limit}.",
        )

    # 2b) 2026-09-04 — the run filled its temporary disk.
    #
    #     Placed BETWEEN the OOM check and the exit codes, and both sides of that are
    #     deliberate. Behind the OOM kill, because `State.OOMKilled` is a hard fact the
    #     kernel reported about this container, while these are messages found in its
    #     output — and a container killed for memory can easily have printed an
    #     unrelated disk line earlier in its run, so a log tail must never outrank the
    #     kernel. Ahead of the exit codes, because those would report the symptom
    #     ("exited 1") and lose the cause. A test pins the order.
    #
    #     A PUBLIC run normally never reaches here for this reason, because the agent
    #     samples the directory and stops it first (see `_enforce_scratch`). This
    #     catches the case the sampler cannot: a run that fills the disk BETWEEN two
    #     samples and dies of it on its own. A PRIVATE run always reaches here, because
    #     its scratch is a RAM folder the kernel sizes exactly — there is nothing to
    #     sample, the write simply fails, and this is where it gets its name.
    if _tail_has(log_tail, _NO_SPACE):
        if private:
            where = (
                "its private in-memory folder is full (PRIVATE_TMPFS_MB). A private "
                "run has no writable folder on the machine's disk by design, so its "
                "working space is RAM and is fixed in size"
            )
        else:
            limit = f" of {int(scratch_mb)} MB" if scratch_mb else ""
            where = (
                f"it filled its temporary disk{limit}. Ask for more temporary disk "
                "when you submit, or write less"
            )
        return (
            SCRATCH_QUOTA_EXCEEDED,
            f"Out of temporary space: {where}.",
        )

    # 2c) 2026-09-04 — the image wrote somewhere it may not write. The container's
    #     root filesystem is read-only and only /scratch and /tmp are writable, which
    #     is what makes temporary disk a measurable thing rather than a question about
    #     an image layer. An image that writes elsewhere fails, and this is the
    #     difference between a person seeing "exited 1" and seeing what to change.
    if _tail_has(log_tail, _READ_ONLY):
        return (
            READ_ONLY_FILESYSTEM,
            "The program tried to write outside /scratch and /tmp. Everything else in "
            "the container is read-only, so that temporary disk can be measured and "
            "capped — and so a job cannot alter its own image while it runs. Write "
            "your files under /scratch (which is where OUTPUT_DIR points) or /tmp.",
        )

    code = exit_code if exit_code is not None else state.get("ExitCode")
    if code in (0, None):
        return None  # success, or nothing to explain — no reason (honesty rule)

    # 3) Decode the exit signal (hard facts — POSIX exit conventions).
    if code == 137:
        return (
            KILLED,
            "The container was killed (SIGKILL, exit 137) — stopped by the system, "
            "not an out-of-memory kill.",
        )
    if code == 139:
        return (
            APP_CRASH,
            "The program crashed with a segmentation fault — bad memory access "
            "(SIGSEGV, exit 139).",
        )
    if code == 143:
        return (TERMINATED, "The program was told to stop (SIGTERM, exit 143).")
    return (APP_ERROR, f"The program exited with code {code}.")


def parse_progress(line):
    """A `##PROGRESS {json}` line -> the parsed dict, or None for any other line.

    Malformed JSON is NOT an error — it is just a normal log line, so we return None
    and never raise (the raw line still flows to run_logs unchanged)."""
    stripped = line.strip()
    if not stripped.startswith("##PROGRESS"):
        return None
    payload = stripped[len("##PROGRESS"):].strip()
    try:
        data = json.loads(payload)
    except (ValueError, TypeError):
        return None
    return data if isinstance(data, dict) else None


def progress_fraction(metrics):
    """`epoch`/`total` from a progress dict -> a 0..1 fraction, or None if either is
    missing or `total` is not positive."""
    if not isinstance(metrics, dict):
        return None
    epoch, total = metrics.get("epoch"), metrics.get("total")
    if isinstance(epoch, (int, float)) and isinstance(total, (int, float)) and total > 0:
        return max(0.0, min(1.0, epoch / total))
    return None


def compute_stats(raw):
    """One docker `stats(stream=False)` sample -> {cpu_pct, mem_used_mb, mem_limit_mb},
    or None if the sample is too empty to read. cpu_pct is derived from the delta the
    sample carries against its own `precpu_stats` (no second call needed)."""
    if not isinstance(raw, dict):
        return None
    out = {}
    mem = raw.get("memory_stats") or {}
    usage = mem.get("usage")
    limit = mem.get("limit")
    if isinstance(usage, (int, float)):
        # Subtract the page cache when the kernel reports it (matches `docker stats`).
        cache = ((mem.get("stats") or {}).get("cache")) or 0
        out["mem_used_mb"] = round(max(0, usage - cache) / (1024 * 1024), 1)
    if isinstance(limit, (int, float)) and limit > 0:
        out["mem_limit_mb"] = round(limit / (1024 * 1024), 1)

    cpu = raw.get("cpu_stats") or {}
    precpu = raw.get("precpu_stats") or {}
    try:
        cpu_delta = cpu["cpu_usage"]["total_usage"] - precpu["cpu_usage"]["total_usage"]
        system_delta = cpu["system_cpu_usage"] - precpu["system_cpu_usage"]
        ncpu = cpu.get("online_cpus") or len(cpu["cpu_usage"].get("percpu_usage") or []) or 1
        if system_delta > 0 and cpu_delta >= 0:
            out["cpu_pct"] = round((cpu_delta / system_delta) * ncpu * 100.0, 1)
    except (KeyError, TypeError):
        pass
    return out or None
