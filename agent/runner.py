"""W2 container execution via the Docker SDK (docker-py).

Design rule: the agent NEVER blocks the heartbeat loop on a container. It starts
containers *detached* and polls them once per heartbeat tick. Why this matters:

  * the pull loop is single-threaded and must keep heart-beating while runs
    execute (a silent agent looks dead and gets its runs reclaimed in W5);
  * "start, then poll" is the same shape log streaming (W3) needs — read from the
    same tracked containers — so nothing here is throwaway.

Each tracked run remembers its `attempt` (the fencing token). When the control
plane rejects a status post with 409 (a presumed-dead node's late result), the
caller tells us to `abort` that run and we tear the container down.

Entrypoint mapping (read once): a job's `entrypoint` is passed to Docker as the
container ENTRYPOINT *override*, not as a command appended to the image's
entrypoint. So `["python","train.py"]` runs exactly `python train.py`, even
though the dummy image also bakes that same ENTRYPOINT in — no double-run.
"""

import logging
import os
import shutil
import tempfile

import docker
from docker.errors import APIError, DockerException, ImageNotFound, NotFound

from .classify import compute_stats, parse_progress, progress_fraction

log = logging.getLogger("agent.runner")

# Exit code we report when the container vanished or never started — distinct
# from any real program exit code so it's obvious in the runs table.
_LOST_EXIT = -1

# --- temporary disk, 2026-09-04 (storage quota policy) ------
#
# A public run now gets ONE host directory with three folders in it, and the
# container's root filesystem is READ-ONLY. Those three are the only places a run can
# write, and because they are three folders of one directory, "how much temporary
# disk has this run used" is one `os.scandir` walk rather than a question about a
# writable image layer.
#
#   scratch/    -> /scratch    the working area AND where results are collected from
#   tmp/        -> /tmp        what any normal program expects to exist
#   checkpoint/ -> /checkpoint working state a later attempt may resume from
#
# **Why read-only root.** Without it a run can write anywhere in the container's
# writable layer, which the agent cannot measure cheaply — `docker inspect --size`
# walks the whole layer and gets slower exactly when it matters, i.e. when a run is
# filling the disk. The alternative, a per-container disk limit through Docker's
# `--storage-opt`, exists only on the overlay2 driver over an xfs disk with project
# quotas: not on our machines and not portable (NFR-5). Read-only root plus measured
# mounts is portable, cheap and honest. It also means the image cannot be modified
# while it runs, which is a free gain for NFR-1.
#
# **Why THREE folders and not the two the brief named.** Dropping `/checkpoint` would
# delete checkpoint-and-resume, which is a shipped feature with its own proof on
# disk. It stays a separate mount for the reason it was separated in the first place:
# `artifact_paths` walks `scratch/` only, so a checkpoint can never be swept up and
# uploaded as this run's result. The separation is the guard; nothing has to remember
# a rule. All three sit inside the one measured directory, so the count is still one
# walk of one tree and the brief's "one measurement, one mechanism" holds.
#
# **`OUTPUT_DIR` now points at `/scratch`.** It was `/output`, its own fourth mount.
# Keeping it would have meant either a fourth writable place the cap does not see, or
# loosening the read-only root to preserve an old path — and a run's output IS
# temporary disk on the worker until the control plane accepts it, so counting it is
# the truthful thing to do rather than an accounting convenience.
_SCRATCH_MOUNT = "/scratch"
# The output path handed to workloads. Named separately from _SCRATCH_MOUNT even
# though they are the same string today, because they are two different promises: one
# is "where you may write", the other is "where we will look for your results".
_OUTPUT_MOUNT = _SCRATCH_MOUNT

# The mode that per-run writable host dirs are created with (2026-08-13).
#
# `tempfile.mkdtemp` creates 0700 owned by the agent's own user, which is right for a
# private temp dir and wrong for a bind mount: the container writing into it runs as
# whatever uid its IMAGE declares, and the agent cannot know that uid — the job picks
# it. So a container running as anyone other than the agent's user cannot write its
# own results. Sticky + world-writable is the same answer the PRIVATE path already
# reached for the same reason (`mode=1777` on its tmpfs, below); this is the ordinary
# path catching up, not a new idea.
#
# The sticky bit is not decoration. The directory is world-writable for the life of
# the run, so it earns /tmp's rule: only a file's owner — or the directory's owner,
# which is the agent — may remove it. That is also what keeps cleanup working, since
# the agent must delete files created under a uid that is not its own.
#
# What we give up, stated rather than hidden: for the run's lifetime any local user on
# that worker can write into this directory. The alternative is to make the job declare
# the uid it runs as, which is a contract change and a worse trade for a machine that
# is already trusted to execute the container.
_RUN_DIR_MODE = 0o1777

# --- checkpoint & resume (2026-08-13) ---------------------------------------
# A SECOND writable host dir, mounted separately from the working mount on purpose.
# A checkpoint is working state, not a finished output, and `artifact_paths()` walks
# `scratch/` only — so a checkpoint written here can never be swept up and uploaded as
# this run's result. The separation is the guard; nothing has to remember a rule.
_CHECKPOINT_MOUNT = "/checkpoint"

# 2026-09-05 -- where an ordinary job's dataset file appears inside the container.
# A directory rather than a single fixed path like the sealed mount, because the
# user's own filename is kept: a workload that expects `data.zip` should find
# `data.zip`, and INPUT_PATH names the file exactly so nothing has to guess.
_INPUT_MOUNT = "/input"
# The three folder names inside a run's one host directory. Public, because the agent
# imports them to measure the tree and a second copy of these names is exactly the
# kind of drift that made the checkpoint filename one constant instead of two.
SCRATCH_DIR_NAME = "scratch"
TMP_DIR_NAME = "tmp"
CHECKPOINT_DIR_NAME = "checkpoint"
# ONE file, a stable name, overwritten in place. That is what keeps storage bounded
# without a cleanup job: however often a workload saves, it writes the same path, so
# the agent uploads the same object key and the run keeps one checkpoint per attempt.
# PUBLIC on purpose: `agent.py` imports this rather than keeping its own copy. When
# the two had separate constants they disagreed — the runner mounted `<dir>/state`
# while the agent staged `<dir>/checkpoint`, so the container found nothing and every
# resume silently started over. Both halves were self-consistent and the system was
# wrong; one definition is the fix, not a rule to remember.
CHECKPOINT_FILENAME = "state"

# The mode the CHECKPOINT dir is created with — 0777, and DELIBERATELY NOT the 1777
# `_RUN_DIR_MODE` above. Copying that mode here looked obviously right and is wrong,
# which is why the difference is written down rather than left to be re-derived.
#
# The two directories differ in one way that matters: the agent STAGES A FILE into
# this one before the container starts, and the container has to replace that file to
# save. Under the sticky bit a process may only remove or rename-over a file it owns,
# or that sits in a directory it owns — and the container's uid owns neither, since
# the agent wrote the staged file and the agent owns the directory. So the container
# reads the resumed state and then silently cannot save a new one. Measured, both
# ways, in `docs/evidence/real_workload_2026-08-13/checkpoint_permissions.txt`: the
# same non-root uid that fails `replace` at 1777 succeeds at 0777, and it fails at
# 1777 even when the staged file itself is world-writable, because the sticky rule
# asks who OWNS the file and never what its mode is.
#
# What that costs, stated rather than buried: for the life of the run any local user
# on that worker can delete or overwrite this run's checkpoint, where they could only
# add files to /scratch. The bound on the damage is the feature's own design — a
# checkpoint that is absent, stale or unreadable costs repeated training and can never
# cost a wrong result, because a checkpoint is never a claim about a finished run.
# A sticky bit that protects the file by stopping it from ever being written is not a
# protection, and the run it silently breaks is exactly the recovery this project
# exists to demonstrate.
CHECKPOINT_DIR_MODE = 0o777

# The mode the staged checkpoint FILE is written with, so the container — running as
# an image-chosen uid the agent cannot know — can read what it is resuming from.
# `open(..., "wb")` honours the agent's umask, so on a worker whose umask is 077 the
# staged file would be 0600 and unreadable to the container: the resume would fail on
# a machine configuration rather than on anything the code does. Set explicitly, so it
# does not depend on how the agent's shell was configured.
#
# The same 0644 is what `fyp_checkpoint.save()` sets on its side, and it has to be:
# without it the container's saved file is `mkstemp`'s 0600 and the AGENT cannot read
# back the checkpoint it is meant to upload. Three legs — stage, save, sweep — and the
# feature needs all three; two of them were only ever exercised as root.
CHECKPOINT_FILE_MODE = 0o644

# --- W6b private-run mounts -------------------------------------------------
# The SEALED input, bind-mounted read-only. These bytes are ciphertext, so this is
# the one file a private container may see from the host disk.
_SEALED_MOUNT = "/sealed/input.bin"
# tmpfs = a folder that looks like a disk but lives in RAM and vanishes with the
# container. The opened (plaintext) data is written ONLY here, so it never touches
# the worker's disk in readable form — the load-bearing claim of this whole feature.
_PRIVATE_MOUNT = "/private"
# With a read-only root filesystem, Python still needs somewhere scratch to exist
# (temp files, bytecode). A small tmpfs gives it that without opening the disk.
_TMP_MOUNT = "/tmp"
# Where per-run directories are created. `AGENT_OUTPUT_ROOT` kept its name from W6
# rather than being renamed to something about scratch: renaming it would silently
# change where an existing deployment puts its run directories, which is a worse
# thing to do than living with a name that has outgrown itself.
_RUN_ROOT_ENV = "AGENT_OUTPUT_ROOT"
# Size of the plaintext tmpfs; the opened input must fit in it.
PRIVATE_TMPFS_MB = int(os.environ.get("PRIVATE_TMPFS_MB", "256"))
# Unprivileged uid:gid inside the container ("nobody"). User code runs as a normal
# user, not root, so a container escape would land with the fewest possible rights.
_NOBODY = "65534:65534"


def run_root() -> str:
    """Where per-run directories live on this worker."""
    return os.environ.get(_RUN_ROOT_ENV) or tempfile.gettempdir()


def run_dir_path(run_id: str, attempt: int) -> str:
    """Where this run's one host directory is.

    A DETERMINISTIC path rather than `mkdtemp`, so the directory a demonstration
    points at is the directory named in the run's own log line, and so a killed agent
    leaves ONE stale directory to be reused rather than a new one accumulating beside
    it on every restart."""
    return os.path.join(run_root(), "fyp-runs", str(run_id), str(attempt))


def prepare_run_dir(run_id: str, attempt: int) -> str:
    """CLEAR and build this run's host directory. Called once, when an attempt begins.

    The clearing is why this is separate from `ensure_run_dir` below, and the
    separation is not tidiness. An attempt must start with an empty scratch or its
    very first measurement would charge it for the last one — but by the time the
    container starts, the previous attempt's checkpoint has already been STAGED into
    this same tree, and a `start` that cleared would destroy the state the resume
    exists to use. So the destructive step belongs to the one caller that knows a new
    attempt is beginning, and `start` only ever ensures.

    That ordering was found by a test rather than by reading: staging then starting
    wiped the staged file, and `checkpoint_path` came back None."""
    path = run_dir_path(run_id, attempt)
    if os.path.isdir(path):
        shutil.rmtree(path, ignore_errors=True)
    return ensure_run_dir(run_id, attempt)


def ensure_run_dir(run_id: str, attempt: int) -> str:
    """Make sure this run's directory and its three folders exist. Never clears.

    Safe to call over a directory that already holds staged state, which is exactly
    what `start` needs when a caller did not prepare one for it."""
    path = run_dir_path(run_id, attempt)
    os.makedirs(path, exist_ok=True)
    os.chmod(path, _RUN_DIR_MODE)
    for name in (SCRATCH_DIR_NAME, TMP_DIR_NAME):
        folder = os.path.join(path, name)
        os.makedirs(folder, exist_ok=True)
        # 0700 from `makedirs` would be right for a private temp dir and is wrong for
        # a bind mount: the container runs as whatever uid its IMAGE declares, which
        # the agent cannot know because the job picks it. See _RUN_DIR_MODE.
        os.chmod(folder, _RUN_DIR_MODE)
    checkpoint = os.path.join(path, CHECKPOINT_DIR_NAME)
    os.makedirs(checkpoint, exist_ok=True)
    # NOT the sticky 1777 the other two get. The agent STAGES A FILE into this one and
    # the container has to replace it to save; under the sticky bit a process may only
    # replace a file it owns, in a directory it owns, and the container's uid owns
    # neither. See CHECKPOINT_DIR_MODE — the difference is measured, both ways.
    os.chmod(checkpoint, CHECKPOINT_DIR_MODE)
    return path


def _tree_bytes(root: str) -> int:
    """Total bytes of every file under `root`, following no symlinks.

    `os.scandir` rather than `os.walk` because the entry it yields already carries
    the stat the size comes from, so a directory of N files costs one directory read
    instead of N. A file that disappears mid-walk is skipped rather than raising:
    this runs against a directory a container is actively writing to, so a name that
    is gone by the time we look at it is the ordinary case and not an error.

    Symlinks are not followed, and are counted as the link rather than the target.
    A run that symlinks somewhere else on the host has not put bytes on this disk
    through that link, and following it would charge this run for a file it does not
    own — the read-only root and the two bind mounts are what stop it writing there
    in the first place."""
    total = 0
    stack = [root]
    while stack:
        current = stack.pop()
        try:
            with os.scandir(current) as entries:
                for entry in entries:
                    try:
                        if entry.is_dir(follow_symlinks=False):
                            stack.append(entry.path)
                        else:
                            total += entry.stat(follow_symlinks=False).st_size
                    except OSError:
                        continue  # vanished under us — the ordinary case here
        except OSError:
            continue
    return total


class Runner:
    """Owns the local Docker containers for this agent's in-flight runs."""

    def __init__(self) -> None:
        # from_env() reads DOCKER_HOST etc. Raises if the Docker daemon is not
        # reachable — the caller turns that into a clear fatal message.
        self._client = docker.from_env()
        self._client.ping()
        # run_id -> {"container": Container, "attempt": int, "output_dir": str, ...}
        self._runs: dict[str, dict] = {}
        # W6: the node's display name, injected into every container as FYP_NODE_NAME
        # so a run's output (metrics.json) records which machine produced it. Set by
        # the agent after registration; None -> not injected (tests).
        self.node_name: str | None = None
        # W5c: the default container memory cap (MB) when a job sets no mem_limit —
        # the node's own declared RAM, set by the agent after it reads its specs. So
        # a run that outgrows the machine is OOM-killed at the node's capacity (a
        # clean, provable cgroup kill), not left to drive the host into swap. A job's
        # explicit mem_limit always wins over this. None -> no default cap (tests).
        self.default_mem_limit_mb: int | None = None

    # --- queries -----------------------------------------------------------

    def docker_version(self) -> str | None:
        """Engine version string for the node's software identity (dashboard)."""
        try:
            return self._client.version().get("Version")
        except Exception:  # noqa: BLE001 - identity is best-effort, never fatal
            return None

    def has(self, run_id: str) -> bool:
        return run_id in self._runs

    def running_report(self) -> list[dict]:
        """The `running` array sent on every heartbeat (protocol.md §9). Lets the
        control plane renew our leases and is how W5 knows the run is still ours.

        W5b: also carries the latest ##PROGRESS fraction + metrics for each run, so
        live progress rides the existing fenced heartbeat path (the control plane
        stores it only when the reported attempt matches — a stale report can't)."""
        return [
            {
                "run_id": rid,
                "attempt": v["attempt"],
                "state": "RUNNING",
                "progress": v.get("progress"),
                "metrics": v.get("metrics"),
            }
            for rid, v in self._runs.items()
        ]

    def tracked_ids(self) -> list[str]:
        """Run ids we currently own a container for (copy — safe to mutate during)."""
        return list(self._runs.keys())

    def attempt_of(self, run_id: str) -> int | None:
        v = self._runs.get(run_id)
        return v["attempt"] if v is not None else None

    def mem_limit_of(self, run_id: str) -> int | None:
        v = self._runs.get(run_id)
        return v.get("mem_limit_mb") if v is not None else None

    def progress_of(self, run_id: str) -> tuple[float | None, dict | None]:
        """The latest ##PROGRESS fraction and metrics parsed for this run, as
        `scan_progress` left them — (None, None) when nothing was ever printed."""
        v = self._runs.get(run_id)
        if v is None:
            return None, None
        return v.get("progress"), v.get("metrics")

    def is_private(self, run_id: str) -> bool:
        """Does this run open a sealed input? The exit classifier needs it: "out of
        space" means a full disk for an ordinary run and a full RAM folder for a
        private one, and telling a user to ask for more disk when their limit is
        `PRIVATE_TMPFS_MB` would send them to fix the wrong thing."""
        v = self._runs.get(run_id)
        return bool(v.get("private")) if v is not None else False

    # --- temporary disk (2026-09-04) ---------------------------------------

    def scratch_limit_of(self, run_id: str) -> int | None:
        """The cap this run must be stopped at, in MB, or None when it has none.

        Comes straight off the assignment (`assignments[].scratch_mb`), which is the
        submitter's explicit ask if they made one and otherwise their tier's ceiling.
        None means no cap — an unowned job, or an assignment from a control plane
        that predates this feature."""
        v = self._runs.get(run_id)
        return v.get("scratch_mb") if v is not None else None

    def scratch_used_mb(self, run_id: str) -> float | None:
        """How much temporary disk this run has used, in MB, or None when the
        question does not apply.

        Walks the run's ONE host directory — all three folders — with `os.scandir`,
        which is the cheap directory walk rather than a `stat` per name through
        `os.walk`. It is called once per sample tick, on a tree that holds a
        workload's own files, so its cost is proportional to how many files the run
        made and not to anything the platform did.

        None for a PRIVATE run, and that is a real answer rather than a gap: a private
        run has no writable host mount at all, so its scratch is the RAM folder of
        `PRIVATE_TMPFS_MB` that the kernel sizes for it. That limit is exact and
        enforced by the kernel, so sampling it would add nothing — the write simply
        fails with `ENOSPC` and the exit classifier names it."""
        v = self._runs.get(run_id)
        if v is None:
            return None
        root = v.get("run_dir")
        if not root or not os.path.isdir(root):
            return None
        return round(_tree_bytes(root) / (1024 * 1024), 3)

    def stop(self, run_id: str) -> None:
        """Stop a running container without forgetting the run.

        Used by the scratch-cap enforcement, which needs the container dead but the
        tracked state alive: the caller still has to flush the run's last output and
        post its terminal status, and both of those need the entry `cleanup` would
        have removed."""
        v = self._runs.get(run_id)
        if v is None:
            return
        try:
            v["container"].stop(timeout=1)
        except (APIError, DockerException, NotFound):
            pass

    # --- artifacts (W6) ----------------------------------------------------

    def artifact_paths(self, run_id: str) -> list[tuple[str, str, int]]:
        """The run's output files as `(filename, host_path, size_bytes)`, walking the
        per-run `/scratch` mount. Called after the container exits (files are
        flushed). The caller uploads each below the size cap; the object name is the
        basename.

        Walks `scratch/` ONLY, never the whole run directory — which is what keeps a
        checkpoint from being swept up and uploaded as this run's result, and what
        keeps `/tmp` out of the results. Two folders, two meanings, and the walk is
        where that distinction is enforced."""
        v = self._runs.get(run_id)
        if v is None:
            return []
        out = v.get("output_dir")
        if not out or not os.path.isdir(out):
            return []
        found: list[tuple[str, str, int]] = []
        for root, _dirs, files in os.walk(out):
            for name in files:
                path = os.path.join(root, name)
                try:
                    size = os.path.getsize(path)
                except OSError:
                    continue
                found.append((name, path, size))
        return found

    # --- checkpoint & resume (2026-08-13) ----------------------------------

    def checkpoint_path(self, run_id: str) -> str | None:
        """Host path of this run's ONE checkpoint file, or None if the run has no
        checkpoint mount (a private run) or has not saved anything yet.

        Deliberately not part of `artifact_paths`: a checkpoint is working state and
        must never be uploaded as a result. Two directories, two meanings."""
        v = self._runs.get(run_id)
        if v is None:
            return None
        d = v.get("checkpoint_dir")
        if not d:
            return None
        path = os.path.join(d, CHECKPOINT_FILENAME)
        return path if os.path.isfile(path) else None

    # --- resource samples + progress (W5b) ---------------------------------

    def sample_stats(self, run_id: str) -> dict | None:
        """One non-streaming `docker stats` reading for this run's container, turned
        into {cpu_pct, mem_used_mb, mem_limit_mb}. None if the container is gone or
        the reading is unusable. Cheap: a single API call, no streaming."""
        v = self._runs.get(run_id)
        if v is None:
            return None
        try:
            raw = v["container"].stats(stream=False)
        except (APIError, DockerException, NotFound):
            return None
        return compute_stats(raw)

    def scan_progress(self, run_id: str) -> None:
        """Refresh this run's latest ##PROGRESS marker from the container's output.
        Scans all complete lines and keeps the LAST valid marker (cheap at demo
        scale; the ##PROGRESS lines still flow to run_logs as normal lines)."""
        v = self._runs.get(run_id)
        if v is None:
            return
        try:
            raw = v["container"].logs(stdout=True, stderr=True, timestamps=False)
        except (APIError, DockerException, NotFound):
            return
        latest = None
        for line in raw.decode("utf-8", "replace").split("\n"):
            marker = parse_progress(line)
            if marker is not None:
                latest = marker
        if latest is not None:
            v["metrics"] = latest
            v["progress"] = progress_fraction(latest)

    # --- log capture (W3) --------------------------------------------------

    def collect_logs(self, run_id: str, final: bool = False) -> tuple[int, str] | None:
        """Return the next batch of unsent container output as `(seq, chunk)`, or
        None if there's nothing new. Does NOT advance the cursor — the caller calls
        `confirm_logs` only after the control plane accepts it, so a failed post is
        re-sent next tick with the *same* seq (the DB then dedups it).

        A batch in flight is FROZEN: while one is unconfirmed, we re-return that
        exact chunk rather than recomputing it. Why: the control plane may have
        already stored it and only the 200 got lost. The DB keeps the FIRST row
        for a seq, so a retry that grew fatter (same seq, more lines) would have
        its extra lines silently dropped — the retry must be byte-identical.

        While running we only emit COMPLETE lines (those ending in a newline); a
        half-written final line is held back so it isn't sent twice. `final=True`
        (called once after the container exits) flushes that trailing line too."""
        v = self._runs.get(run_id)
        if v is None:
            return None
        if v["pending"]:
            return v["next_seq"], v["pending_chunk"]
        try:
            raw = v["container"].logs(stdout=True, stderr=True, timestamps=False)
        except (APIError, DockerException, NotFound):
            return None
        text = raw.decode("utf-8", "replace")
        parts = text.split("\n")
        # parts[-1] is the text after the last newline: "" if the output ends in a
        # newline, else a not-yet-terminated final line. Hold it back unless final.
        complete = parts if (final and parts[-1] != "") else parts[:-1]
        new = complete[v["sent_lines"]:]
        if not new:
            return None
        chunk = "\n".join(new) + "\n"
        v["pending"] = len(new)
        v["pending_chunk"] = chunk
        return v["next_seq"], chunk

    def log_tail(self, run_id: str, max_lines: int = 40) -> str:
        """The last few lines the container printed — the evidence a failure
        classification is read from (W6b: the helper's integrity marker; generally
        the stderr tail behind a reason). Never raises: no tail is just no evidence."""
        v = self._runs.get(run_id)
        if v is None:
            return ""
        try:
            raw = v["container"].logs(stdout=True, stderr=True, tail=max_lines)
        except (APIError, DockerException, NotFound):
            return ""
        return raw.decode("utf-8", "replace")

    def confirm_logs(self, run_id: str) -> None:
        """Mark the last collected batch as accepted: advance the line cursor and
        bump the seq so the next batch is a new row."""
        v = self._runs.get(run_id)
        if v is None or not v["pending"]:
            return
        v["sent_lines"] += v["pending"]
        v["next_seq"] += 1
        v["pending"] = 0
        v["pending_chunk"] = ""

    # --- lifecycle ---------------------------------------------------------

    def start(
        self,
        run_id: str,
        attempt: int,
        image: str,
        entrypoint,
        env,
        mem_limit_mb=None,
        checkpoint_dir: str | None = None,
        input_path: str | None = None,
        input_name: str | None = None,
        run_dir: str | None = None,
        scratch_mb: int | None = None,
        private: bool = False,
        sealed: bool = False,
        sealed_path: str | None = None,
        ticket: str | None = None,
        key_url: str | None = None,
        input_filename: str | None = None,
        ca_pem: str | None = None,
    ) -> None:
        """Pull the image if it isn't local, then launch the container detached.

        Pull-if-absent (not always-pull) so locally-built images that live in no
        registry — like the dummy workload — just run. Raises on failure; the
        caller reports the run FAILED so it never hangs in ASSIGNED.

        W5b: when the job set `mem_limit_mb`, apply it as --memory AND --memory-swap
        (equal, so there is no swap cushion) — an over-budget run is then OOM-killed
        by the kernel deterministically, and Docker sets State.OOMKilled=true, which
        is our provable RAM-overload signal.

        W5c: when the job set NO cap, fall back to the node's declared RAM
        (`default_mem_limit_mb`). The kill then proves the *node* was too weak (not a
        user limit) — which is what the control plane escalates on. The job's own cap
        always takes precedence.

        W6b (private=True) changes the container's shape, and every change is one
        sentence of the privacy claim:

          * the SEALED file is bind-mounted READ-ONLY — ciphertext is the only thing
            from the host disk the container can see, and it cannot alter it;
          * `/private` is a **tmpfs** (a RAM folder) — the opened data is written only
            there, so plaintext never touches the worker's disk and dies with the
            container, with no cleanup step that could be skipped or fail;
          * there is **no writable host mount at all** — so a private run has no
            `/scratch` and therefore collects no artifacts (a stated consequence, not
            an oversight: see _upload_artifacts and the UI note);
          * the root filesystem is **read-only** and the process runs as an
            **unprivileged user**, so untrusted code gets the least it can be given;
          * the container receives a one-shot **ticket**, never the key.

        2026-08-13 (`checkpoint_dir`, ordinary runs only): a second writable host dir
        mounted at `/checkpoint`, with `CHECKPOINT_PATH` naming one file inside it.
        The agent has already put the previous attempt's saved state there if there
        was any, so a workload that opted in finds it and continues; a workload that
        did not, and every workload before today, sees an unused mount and behaves
        exactly as before. A **private** run gets no checkpoint mount: it has no
        writable host mount at all, by design, so it cannot produce one — private and
        resume do not combine, and that is stated rather than discovered."""
        try:
            self._client.images.get(image)
        except ImageNotFound:
            log.info("image %s not local; pulling", image)
            self._client.images.pull(image)

        # A job's explicit cap wins; otherwise fall back to the node's declared RAM.
        effective_mem_limit_mb = mem_limit_mb or self.default_mem_limit_mb
        limits = {}
        if effective_mem_limit_mb:
            limits["mem_limit"] = f"{int(effective_mem_limit_mb)}m"
            limits["memswap_limit"] = f"{int(effective_mem_limit_mb)}m"  # no swap -> hard OOM

        full_env = dict(env or {})
        if self.node_name:
            full_env["FYP_NODE_NAME"] = self.node_name
        output_dir: str | None = None
        extra: dict = {}

        if private:
            # A private run has NO writable host mount at all, so it can never have a
            # run directory — forced to None here rather than merely left unset, so a
            # caller that passed one by mistake cannot make a private run look as
            # though it has bytes on this worker's disk.
            run_dir = None
            # Plaintext lives in RAM only. OUTPUT_DIR points inside that same tmpfs so
            # a workload that writes results still works — but nothing is collected
            # from there, and it vanishes with the container.
            full_env["OUTPUT_DIR"] = f"{_PRIVATE_MOUNT}/output"
            full_env["INPUT_CIPHER_PATH"] = _SEALED_MOUNT
            full_env["INPUT_PLAIN_PATH"] = f"{_PRIVATE_MOUNT}/{input_filename or 'input.bin'}"
            full_env["FYP_TICKET"] = ticket or ""
            full_env["FYP_KEY_URL"] = key_url or ""
            # The authority certificate as text, so the in-container helper can
            # verify the control plane when it redeems the ticket over HTTPS. Text
            # rather than a path because a worker's file path means nothing inside
            # a container that does not mount it -- and adding a mount would break
            # the single-read-only-mount property this container is built on.
            # Empty on a plain-HTTP deployment.
            if ca_pem:
                full_env["FYP_CA_PEM"] = ca_pem
            extra = {
                "volumes": {sealed_path: {"bind": _SEALED_MOUNT, "mode": "ro"}},
                "tmpfs": {
                    _PRIVATE_MOUNT: f"rw,size={PRIVATE_TMPFS_MB}m,mode=1777",
                    _TMP_MOUNT: "rw,size=64m,mode=1777",
                },
                "read_only": True,
                "user": _NOBODY,
                # Docker resolves this to the host, so the container can reach the
                # control plane to redeem its ticket even in the single-machine demo.
                "extra_hosts": {"host.docker.internal": "host-gateway"},
            }
        else:
            # 2026-09-04: ONE host directory per run, with three folders in it, and a
            # READ-ONLY root filesystem. Those three folders are the only places this
            # run can write, which is what makes "how much temporary disk has it
            # used" answerable by one walk of one tree.
            #
            # `checkpoint_dir` is passed in by the caller (it stages the previous
            # attempt's saved state into it before we are called), and it is created
            # INSIDE this run directory, so it is measured along with everything
            # else. A checkpoint is temporary disk on the worker like anything else
            # here; the thing that keeps it out of the RESULTS is `artifact_paths`
            # walking `scratch/` alone, not where the folder happens to live.
            # `run_dir` is normally prepared by the caller (`_act_on_assignments`),
            # which has to build it first so the previous attempt's checkpoint can be
            # staged inside it before the container starts. A direct caller that did
            # not — the agent tests do this deliberately — gets a fresh one here.
            run_dir = run_dir or ensure_run_dir(run_id, attempt)
            output_dir = os.path.join(run_dir, SCRATCH_DIR_NAME)
            tmp_dir = os.path.join(run_dir, TMP_DIR_NAME)
            full_env["OUTPUT_DIR"] = _OUTPUT_MOUNT
            # Isolation (NFR-1 / ISO 27001 A.8.31): default bridge network, no extra
            # privileges, and from 2026-09-04 a read-only root filesystem.
            # Hardening (network_mode=none) is W7.
            volumes = {
                output_dir: {"bind": _SCRATCH_MOUNT, "mode": "rw"},
                tmp_dir: {"bind": _TMP_MOUNT, "mode": "rw"},
            }
            if checkpoint_dir:
                volumes[checkpoint_dir] = {"bind": _CHECKPOINT_MOUNT, "mode": "rw"}
                # Read AND write the same path: the agent stages the previous
                # attempt's file here before the container starts, and the workload
                # overwrites it as it goes. One env var, one file, no second concept.
                full_env["CHECKPOINT_PATH"] = f"{_CHECKPOINT_MOUNT}/{CHECKPOINT_FILENAME}"
            if input_path:
                # READ-ONLY, and a single file rather than a directory, so the run
                # cannot alter the dataset it was given -- the same shape the sealed
                # mount uses. INPUT_PATH names the file; the container unpacks it into
                # its own temporary space if it is an archive, which is where that work
                # belongs (see agent._stage_input).
                name = input_name or os.path.basename(input_path)
                volumes[input_path] = {
                    "bind": f"{_INPUT_MOUNT}/{name}", "mode": "ro"
                }
                full_env["INPUT_PATH"] = f"{_INPUT_MOUNT}/{name}"
            if sealed:
                # 2026-09-06 -- a SEALED run. Three env vars and, when there is a
                # dataset, one more read-only mount. What is NOT here is the point:
                # no tmpfs, no second container shape, no writable mount taken away.
                # A sealed run is this run with sealed bytes, which is why it keeps
                # its results and can be resumed on another machine.
                full_env["FYP_TICKET"] = ticket or ""
                full_env["FYP_KEY_URL"] = key_url or ""
                if ca_pem:
                    # The authority as TEXT, so the container can verify the control
                    # plane when it redeems its ticket without a fourth mount and
                    # without writing a certificate to disk.
                    full_env["FYP_CA_PEM"] = ca_pem
                if sealed_path:
                    name = input_name or os.path.basename(sealed_path)
                    volumes[sealed_path] = {
                        "bind": f"{_INPUT_MOUNT}/{name}", "mode": "ro"
                    }
                    # The SAME variable an unsealed dataset arrives under. The
                    # workload asks the reader to open INPUT_PATH either way, and the
                    # reader sniffs the bytes -- so adopting a second variable name
                    # would make every workload branch on a distinction it does not
                    # have to care about.
                    full_env["INPUT_PATH"] = f"{_INPUT_MOUNT}/{name}"
                # The container reaches the control plane to redeem its ticket, the
                # same way a private container always has.
                extra_hosts = {"host.docker.internal": "host-gateway"}
                extra = {
                    "volumes": volumes,
                    "read_only": True,
                    "extra_hosts": extra_hosts,
                }
            else:
                extra = {"volumes": volumes, "read_only": True}

        container = self._client.containers.run(
            image,
            entrypoint=entrypoint or None,   # override image ENTRYPOINT; see header
            environment=full_env,
            detach=True,
            name=f"fyp-run-{run_id}-{attempt}",
            # 2026-09-07 (audit, defect C): three labels so that an agent that
            # crashed and came back can find the containers IT started -- and only
            # those, since several agents may share one daemon on one laptop. See
            # `reconcile_orphans`.
            labels={
                "fyp.node": self.node_name or "",
                "fyp.run_id": run_id,
                "fyp.attempt": str(attempt),
            },
            **extra,
            **limits,
        )
        self._runs[run_id] = {
            "container": container,
            "attempt": attempt,
            # The host dir mounted at /scratch — collected after the run, then wiped.
            # None for a private run: there is no writable host mount, by design.
            "output_dir": output_dir,
            # 2026-09-04: the ONE directory holding this run's three writable folders.
            # What `scratch_used_mb` walks and what `_wipe_staged` removes. None for a
            # private run, whose scratch is a RAM folder the kernel sizes instead.
            "run_dir": run_dir,
            # The cap this run is stopped at, in MB, straight off the assignment.
            # None -> no cap, which is what every run had before this date.
            "scratch_mb": scratch_mb,
            # 2026-08-13: the host dir mounted at /checkpoint — read before start,
            # uploaded during the run, wiped with everything else afterwards.
            "checkpoint_dir": checkpoint_dir,
            # W6b: the staged CIPHERTEXT on this worker's disk. Wiped on cleanup/abort,
            # in a finally-path, so a sealed blob never outlives the run that used it.
            "sealed_path": sealed_path,
            "private": private,
            # 2026-09-06: this run's data is sealed. Kept on the run record so the
            # agent can say WHY an output was refused without asking the server again.
            "sealed": sealed,
            # The cap actually applied (job's, or the W5c node-RAM default). Names the
            # limit in an OOM detail; the control plane decides escalation from its own
            # facts (job cap vs node RAM), so this is for the human message only.
            "mem_limit_mb": effective_mem_limit_mb,
            # Log streaming state (W3): sent_lines = complete lines already accepted
            # by the control plane; next_seq = the seq the next batch will use;
            # pending / pending_chunk = the batch currently awaiting a 200 — a
            # retry re-sends that FROZEN chunk under the same seq (see collect_logs).
            "sent_lines": 0,
            "next_seq": 0,
            "pending": 0,
            "pending_chunk": "",
            # W5b progress (##PROGRESS): latest fraction + metrics parsed from stdout.
            "progress": None,
            "metrics": None,
        }
        log.info(
            "started run %s attempt %s -> container %s", run_id, attempt, container.short_id
        )

    def poll(self) -> list[dict]:
        """Check every tracked container once. Returns the runs that have exited:
        [{run_id, attempt, exit_code, state}] — the caller posts the terminal status
        and (W5b) classifies the failure from `state` (the Docker State dict, which
        carries OOMKilled + ExitCode). `state` is None when the container vanished."""
        finished: list[dict] = []
        for run_id, v in list(self._runs.items()):
            container = v["container"]
            try:
                container.reload()
            except NotFound:
                log.warning("container for run %s vanished; reporting failure", run_id)
                finished.append(
                    {"run_id": run_id, "attempt": v["attempt"], "exit_code": _LOST_EXIT, "state": None}
                )
                continue
            except (APIError, DockerException) as exc:
                log.warning("poll of run %s failed (%s); will retry next tick", run_id, exc)
                continue
            if container.status == "exited":
                state = container.attrs.get("State", {}) or {}
                code = state.get("ExitCode", _LOST_EXIT)
                finished.append(
                    {"run_id": run_id, "attempt": v["attempt"], "exit_code": code, "state": state}
                )
        return finished

    def cleanup(self, run_id: str) -> None:
        """Drop tracking, remove the (exited) container, and wipe everything this run
        staged on disk. Called after the terminal status was accepted (200), and after
        artifacts are uploaded, so we don't re-report it or leave anything behind.

        W6b — "nothing lingers", implemented as INSPECT-THEN-REMOVE rather than
        Docker's `auto_remove` flag. `auto_remove` deletes the container the instant
        it exits, which would destroy `State.OOMKilled` before the W5b classifier can
        read it — we would gain tidiness and lose the ability to say why a run died.
        Removing it here, after poll() has read the end-state, gives the same
        guarantee with the diagnostics intact."""
        v = self._runs.pop(run_id, None)
        if v is not None:
            self._remove(v["container"])
            self._wipe_staged(v)

    def abort(self, run_id: str, reason: str = "fenced by control plane") -> None:
        """Fencing 409 received for this run — stop and discard it immediately.

        `reason` is only what the log line says. The one other caller is the
        re-assignment path (2026-09-07, defect A), where the execution is stale by
        the control plane's own decision rather than by a 409."""
        v = self._runs.pop(run_id, None)
        if v is not None:
            log.warning("aborting run %s (%s)", run_id, reason)
            self._remove(v["container"])
            self._wipe_staged(v)

    def reconcile_orphans(self) -> list[dict]:
        """Remove the containers a PREVIOUS agent process on this node left running
        (2026-09-07 audit, defect C). Returns the `{run_id, attempt}` pairs removed.

        `_runs` is in memory. When the agent process dies mid-run its containers do
        not: they keep burning CPU and disk on this machine, unmonitored, for ever,
        while the control plane's reaper has long since handed the run to someone
        else. At startup the agent lists every container labelled `fyp.node=<its own
        name>`, skips the ones it is tracking, and force-removes the rest along with
        their host directories.

        Own name only, and only through the label. Several agents share one Docker
        daemon on one laptop (`scripts/stage_demo.ps1` starts three), so an agent
        must never touch a sibling's containers; the label filter is what keeps
        that true. A container started by an older agent version carries no label,
        is not matched by the filter, and is left alone.

        Why kill rather than adopt: the log-stream cursor (sent_lines, next_seq, the
        frozen pending chunk) died with the process, so a resumed stream would
        either resend lines under a new seq or skip the ones in flight -- both break
        the log dedup contract. Killing the orphan and letting the reaper re-dispatch
        the run is the at-least-once path the platform already guarantees, and it
        costs the run nothing it had not already lost. With no node name yet nothing
        is listed, because the filter would be `fyp.node=` and match nothing useful."""
        if not self.node_name:
            log.info("orphan reconciliation skipped: no node name set")
            return []
        try:
            found = self._client.containers.list(
                all=True, filters={"label": f"fyp.node={self.node_name}"}
            )
        except (APIError, DockerException) as exc:
            log.warning("could not list this node's containers (%s); skipping", exc)
            return []
        tracked = {(rid, str(v["attempt"])) for rid, v in self._runs.items()}
        removed: list[dict] = []
        for container in found:
            labels = getattr(container, "labels", None) or {}
            run_id = labels.get("fyp.run_id")
            attempt = labels.get("fyp.attempt")
            if (run_id, attempt) in tracked:
                continue
            log.warning(
                "orphaned container %s (run %s attempt %s) was left by a previous "
                "agent process; removing it -- the control plane's reaper "
                "re-dispatches the run",
                getattr(container, "short_id", "?"), run_id, attempt,
            )
            self._remove(container)
            if run_id and attempt and str(attempt).isdigit():
                shutil.rmtree(run_dir_path(run_id, int(attempt)), ignore_errors=True)
                removed.append({"run_id": run_id, "attempt": int(attempt)})
        return removed

    @staticmethod
    def _remove(container) -> None:
        try:
            container.remove(force=True)
        except (APIError, DockerException):
            pass

    @staticmethod
    def _wipe_staged(v: dict) -> None:
        """Remove this run's host-disk footprint: its whole run directory (normal
        runs, which is scratch/ + tmp/ + checkpoint/ in one tree) and
        the staged sealed blob (private runs), and the /checkpoint dir. Best-effort
        and logged — the sealed
        file is only ciphertext, so a failure here is untidy, not a data leak, and the
        privacy claim never rested on this wipe succeeding."""
        # One directory holds all three folders since 2026-09-04, so one removal
        # takes the lot. `output_dir` and `checkpoint_dir` are still removed
        # individually afterwards, because a private run has neither a run directory
        # nor an output directory, and a caller that staged a checkpoint directory
        # somewhere else entirely (the old shape, and the shape the tests build) must
        # still be cleaned up rather than leaked.
        run_dir = v.get("run_dir")
        if run_dir:
            shutil.rmtree(run_dir, ignore_errors=True)
        out = v.get("output_dir")
        if out:
            shutil.rmtree(out, ignore_errors=True)
        ckpt = v.get("checkpoint_dir")
        if ckpt:
            # The copy that matters is in object storage; this one is scratch.
            shutil.rmtree(ckpt, ignore_errors=True)
        sealed = v.get("sealed_path")
        if sealed:
            # A sealed run's input lives INSIDE the run directory removed above, so
            # by now it is normally gone — and a file that is already gone is the
            # outcome this wipe wants, not a warning (walk 1, row 33: every input-file
            # run used to end with "could not remove staged sealed input").
            try:
                os.remove(sealed)
            except FileNotFoundError:
                pass
            except OSError as exc:
                log.warning("could not remove staged sealed input %s (%s)", sealed, exc)
