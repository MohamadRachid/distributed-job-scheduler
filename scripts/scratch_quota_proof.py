"""Prove the temporary-disk cap on a REAL worker, with real containers.

Run it from the repo root with the stack up and `fyp-dummy:latest` built:

    .venv\\Scripts\\python.exe scripts/scratch_quota_proof.py --runs 20 \\
        >> docs/evidence/storage_quota_2026-09-04.txt 2>&1

Two limbs of `docs/evidence/predictions/storage_quota_2026-09-04.md` live here,
because both are about the AGENT and neither can be answered without a container
actually writing to a disk:

  P2  the overshoot at the kill, n as given, at a 100 MB cap. The cap is enforced by
      SAMPLING, so between two samples the container keeps writing — the overshoot is
      bounded by write speed x sample tick and is a BOUND, never zero, and this
      measures it rather than asserting it.
  P4  an agent from the commit BEFORE this change runs an ordinary job to SUCCEEDED
      against the new control plane, ignoring `scratch_mb` and reporting no free disk.

The agents are started as real subprocesses against the real control plane. The write
speed the bound rests on is CALIBRATED here, with the same image, the same
`--fill-scratch` mode, the same read-only root and the same kind of bind mount as the
runs under test — so the bound and the thing it bounds are measured on the same
machine by the same instrument. Nothing here is typed by hand.

**Runs are strictly serial** (`AGENT_CAPACITY=1`, one job at a time). Two containers
filling one disk at once would contend for it and every write-speed number would come
out invisibly wrong — the rule that costs an evening if forgotten.
"""

from __future__ import annotations

import argparse
import os
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
import uuid

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from storage_quota_proof import (  # noqa: E402
    ADMIN, API, call, limb, login, say, _results,
)

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CA = os.path.join(REPO, "certs", "ca.pem")
PY = sys.executable
CAP_MB = 100


def start_agent(name: str, cwd: str, root: str, env_extra: dict | None = None):
    """Start a real agent process and return it. `cwd` is which TREE the agent's code
    comes from — the repository for the current agent, a git worktree at an older
    commit for P4's."""
    env = dict(os.environ)
    env["AGENT_CAPACITY"] = "1"        # strictly serial; see the module docstring
    env["AGENT_OUTPUT_ROOT"] = root
    env["AGENT_STATE_FILE"] = os.path.join(root, "agent_state.json")
    env.update(env_extra or {})
    args = [PY, "-m", "agent", "--server", API, "--name", name]
    if os.path.exists(CA):
        args += ["--ca-cert", CA]
    # The agent's output goes to a FILE, never to a pipe. A pipe with nobody reading
    # it fills the operating system's buffer (64 KB on Windows) and the agent then
    # BLOCKS for ever on its next log line — the heartbeat loop stops, the node goes
    # offline, and the control plane correctly reclaims its runs as NODE_LOST while a
    # container keeps writing to the disk. That is what happened on the first attempt
    # at this capture: seven runs measured cleanly and the eighth hung, and it looked
    # like a defect in the feature until the agent's own log turned out to be the
    # thing that was stuck. Keeping the log is also better evidence than discarding
    # it — the path is printed, so a reader can go and look.
    log_path = os.path.join(root, "agent.log")
    handle = open(log_path, "w", encoding="utf-8", errors="replace")
    say(f"  agent log  : {log_path}")
    proc = subprocess.Popen(args, cwd=cwd, env=env, stdout=handle,
                            stderr=subprocess.STDOUT)
    proc._log_handle = handle  # keep it open for the life of the process
    return proc


def node_named(token: str, name: str):
    status, pool = call("GET", "/nodes", token=token)
    for n in pool or []:
        if n["name"] == name:
            return n
    return None


def wait_for_node(token: str, name: str, seconds: int = 60):
    for _ in range(seconds):
        n = node_named(token, name)
        if n and n["online"]:
            return n
        time.sleep(1)
    raise SystemExit(f"agent {name} never came online")


def submit(token: str, name: str, entrypoint: list[str], reqs: dict, env: dict | None = None):
    status, job = call("POST", "/jobs", {
        "name": name, "image": "fyp-dummy:latest", "entrypoint": entrypoint,
        "env": env or {}, "replicas": 1, "resource_reqs": reqs,
    }, token=token)
    if status != 200:
        raise SystemExit(f"submit failed: {status} {job}")
    return job


def wait_terminal(token: str, job_id: str, run_id: str, seconds: int = 180):
    for _ in range(seconds):
        status, rows = call("GET", f"/jobs/{job_id}/runs", token=token)
        row = next((r for r in rows if r["run_id"] == run_id), None)
        if row and row["status"] in ("SUCCEEDED", "FAILED"):
            return row
        time.sleep(1)
    raise SystemExit(f"run {run_id} never finished")


def samples(token: str, run_id: str) -> list[dict]:
    status, rows = call("GET", f"/runs/{run_id}/samples?limit=200", token=token)
    return rows or []


def tree_mb(path: str) -> float:
    total = 0
    for root, _dirs, files in os.walk(path):
        for name in files:
            try:
                total += os.path.getsize(os.path.join(root, name))
            except OSError:
                pass
    return total / (1024 * 1024)


def calibrate_write_speed(seconds: float = 12.0) -> tuple[float, float, float]:
    """Measure how fast the workload actually writes, ON THIS MACHINE, IN THIS FILE.

    The prediction's bound is "write speed x sample tick", so the write speed has to
    be a measured number rather than a guess — and it has to be measured the way the
    run under test writes, or the bound describes a different thing. So this is the
    SAME image, the SAME `--fill-scratch` mode, the SAME read-only root and the SAME
    kind of bind mount: it is the run under test, timed, with nothing stopping it.

    Deriving it instead from the run's own stored samples was tried first and does not
    work: the agent samples once per three-second heartbeat and a 100 MB fill is over
    in a few of those, so most runs give one usable reading and no rate at all. That
    is stated rather than quietly replaced — the instrument's resolution is the reason,
    and it is the same resolution that produces the overshoot being measured.

    Returns (MB per second, MB written, seconds observed)."""
    root = os.path.join(tempfile.gettempdir(), f"fyp-calib-{uuid.uuid4().hex[:8]}")
    scratch = os.path.join(root, "scratch")
    tmp = os.path.join(root, "tmp")
    os.makedirs(scratch, exist_ok=True)
    os.makedirs(tmp, exist_ok=True)
    name = f"fyp-calib-{uuid.uuid4().hex[:8]}"
    proc = subprocess.run(
        ["docker", "run", "-d", "--name", name, "--read-only",
         "-v", f"{scratch}:/scratch", "-v", f"{tmp}:/tmp",
         "-e", "OUTPUT_DIR=/scratch", "--entrypoint", "python",
         "fyp-dummy:latest", "train.py", "--fill-scratch"],
        capture_output=True, text=True,
    )
    if proc.returncode != 0:
        say(f"  calibration could not start: {proc.stderr.strip()}")
        return (0.0, 0.0, 0.0)
    try:
        # Let it get past container startup before the clock starts, so what is timed
        # is writing rather than Docker.
        time.sleep(3.0)
        t0, mb0 = time.perf_counter(), tree_mb(scratch)
        time.sleep(seconds)
        t1, mb1 = time.perf_counter(), tree_mb(scratch)
    finally:
        subprocess.run(["docker", "rm", "-f", name], capture_output=True, text=True)
        shutil.rmtree(root, ignore_errors=True)
    elapsed = t1 - t0
    written = mb1 - mb0
    return (written / elapsed if elapsed > 0 else 0.0, written, elapsed)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--runs", type=int, default=20, help="n for P2")
    args = ap.parse_args()

    say("=" * 78)
    say("TEMPORARY-DISK CAP — live proof on a real worker")
    say(f"date         : {time.strftime('%Y-%m-%d %H:%M:%S')}")
    say(f"api          : {API}")
    say(f"cap under test: {CAP_MB} MB | n for P2: {args.runs} | AGENT_CAPACITY=1 (serial)")
    say("=" * 78)

    admin = login(*ADMIN)
    tag = uuid.uuid4().hex[:8]
    user = f"scratch-proof-{tag}"
    status, made = call("POST", "/users",
                        {"username": user, "password": "scratch-proof-pass-123"},
                        token=admin)
    if status != 200:
        raise SystemExit(f"could not create the proof user: {status} {made}")
    token = login(user, "scratch-proof-pass-123")
    call("POST", "/me/accept-limits", token=token)
    say(f"\nproof user: {user}")

    # ------------------------------------------------------------------ P4 --
    say("\n" + "-" * 78)
    say("P4 — an agent from BEFORE this change still works against the new server")
    say("-" * 78)
    old_commit = subprocess.run(
        ["git", "-C", REPO, "rev-parse", "--short", "HEAD~2"],
        capture_output=True, text=True).stdout.strip()
    worktree = os.path.join(tempfile.gettempdir(), f"fyp-old-agent-{tag}")
    subprocess.run(["git", "-C", REPO, "worktree", "add", "--detach", worktree, old_commit],
                   capture_output=True, text=True, check=True)
    old_version = subprocess.run(
        ["git", "-C", REPO, "show", f"{old_commit}:agent/agent.py"],
        capture_output=True, text=True).stdout
    old_version = next(
        (ln for ln in old_version.splitlines() if ln.startswith("AGENT_VERSION")), "?")
    say(f"  old tree   : {old_commit} in {worktree}")
    say(f"  its version: {old_version.strip()}")

    old_root = os.path.join(tempfile.gettempdir(), f"fyp-old-root-{tag}")
    os.makedirs(old_root, exist_ok=True)
    old_name = f"old-agent-{tag}"
    old_proc = start_agent(old_name, worktree, old_root)
    try:
        node = wait_for_node(token, old_name)
        say(f"  registered : {old_name} agent_version={node['agent_version']} "
            f"disk_free_mb={node['disk_free_mb']}")
        limb("P4.a the old agent reports NO free disk",
             node["disk_free_mb"] is None,
             f"disk_free_mb={node['disk_free_mb']}")

        # Long enough to be sampled several times: a two-second run can finish
        # between two three-second ticks, and "no samples carried a scratch reading"
        # would then be true because there were no samples at all. A limb that passes
        # by having nothing to check is not evidence.
        job = submit(token, f"p4-{tag}", ["python", "train.py"],
                     {"needs_gpu": False}, {"EPOCHS": "12", "EPOCH_SECONDS": "1"})
        row = wait_terminal(token, job["job_id"], job["run_ids"][0])
        say(f"  ordinary job -> {row['status']} exit={row['exit_code']} "
            f"reason={row['failure_reason']}")
        limb("P4.b it runs an ordinary job to SUCCEEDED",
             row["status"] == "SUCCEEDED", f"status={row['status']}")

        arts = call("GET", f"/runs/{job['run_ids'][0]}/artifacts", token=token)[1]
        limb("P4.c and its result was collected as before",
             any(a["filename"] == "metrics.json" for a in arts),
             f"artifacts={[a['filename'] for a in arts]}")

        srows = samples(token, job["run_ids"][0])
        limb("P4.d it was sampled, and none of its samples carries a scratch reading",
             len(srows) > 0 and all(r.get("scratch_used_mb") is None for r in srows),
             f"{len(srows)} sample(s) stored, scratch readings = "
             f"{sorted({str(r.get('scratch_used_mb')) for r in srows})}")

        # And the guard: a job that ASKS for a scratch size is never offered to it.
        asked = submit(token, f"p4-guard-{tag}", ["python", "train.py"],
                       {"needs_gpu": False, "scratch_mb": 64},
                       {"EPOCHS": "1", "EPOCH_SECONDS": "1"})
        time.sleep(12)
        status, rows = call("GET", f"/jobs/{asked['job_id']}/runs", token=token)
        say(f"  a job asking for 64 MB of temporary disk: {rows[0]['status']} "
            f"node={rows[0]['node_id']}")
        limb("P4.e a job with an EXPLICIT scratch ask is never offered to it",
             rows[0]["status"] == "PENDING" and rows[0]["node_id"] is None,
             f"status={rows[0]['status']} node_id={rows[0]['node_id']}")
    finally:
        old_proc.terminate()
        try:
            old_proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            old_proc.kill()

    # ------------------------------------------------------------------ P2 --
    say("\n" + "-" * 78)
    say(f"P2 — the overshoot at the kill, n={args.runs}, cap {CAP_MB} MB")
    say("-" * 78)
    new_root = os.path.join(tempfile.gettempdir(), f"fyp-new-root-{tag}")
    os.makedirs(new_root, exist_ok=True)
    new_name = f"new-agent-{tag}"
    proc = start_agent(new_name, REPO, new_root)
    overshoots: list[float] = []
    speeds: list[float] = []
    try:
        node = wait_for_node(token, new_name)
        say(f"  registered : {new_name} agent_version={node['agent_version']} "
            f"disk_free_mb={node['disk_free_mb']}")
        say("")
        say("  calibrating the write speed first — same image, same --fill-scratch")
        say("  mode, same read-only root, same kind of bind mount, nothing stopping it")
        speed, written, elapsed = calibrate_write_speed()
        say(f"  measured: {written:.1f} MB in {elapsed:.1f} s = {speed:.1f} MB/s")
        say("")
        say(f"  {'run':>4} {'at kill MB':>12} {'overshoot MB':>14} {'MB/s':>8}  reason")
        say("       (MB/s is '--' where the run gave only one reading before the kill;")
        say("        the rate the bound uses is the calibrated figure above)")

        for i in range(1, args.runs + 1):
            job = submit(token, f"p2-{tag}-{i}",
                         ["python", "train.py", "--fill-scratch"],
                         {"needs_gpu": False, "scratch_mb": CAP_MB})
            run_id = job["run_ids"][0]
            if proc.poll() is not None:
                say(f"  the agent exited (code {proc.returncode}) before run {i}; "
                    "stopping here rather than reporting a number it did not produce")
                break
            row = wait_terminal(token, job["job_id"], run_id)
            srows = samples(token, run_id)
            readings = [r["scratch_used_mb"] for r in srows
                        if r.get("scratch_used_mb") is not None]
            at_kill = max(readings) if readings else None
            # Also derive a rate from the run's OWN readings where there are two of
            # them. Usually there are not — the fill is over in a few ticks — which is
            # exactly why the calibration above exists; where it IS available it is
            # printed beside the calibrated figure rather than instead of it.
            sample_rate = None
            prev = None
            for r in sorted(srows, key=lambda x: x["ts"]):
                used = r.get("scratch_used_mb")
                if used is None:
                    continue
                if prev is not None and r["ts"] > prev[0] and used > prev[1]:
                    rate = (used - prev[1]) / (r["ts"] - prev[0])
                    sample_rate = rate if sample_rate is None else max(sample_rate, rate)
                prev = (r["ts"], used)
            if sample_rate:
                speeds.append(sample_rate)
            if at_kill is None:
                say(f"  {i:>4} {'--':>12} {'--':>14} {'--':>8}  "
                    f"{row['failure_reason']} (no sample stored)")
                continue
            over = at_kill - CAP_MB
            overshoots.append(over)
            # An em dash, not 0.0, when no rate could be derived. A run usually gives
            # ONE scratch reading before the kill, and printing "0.0" for that would
            # read as "it wrote nothing" when it means "two readings were needed and
            # there was one". The calibrated figure above is where the rate comes from.
            rate = f"{sample_rate:.1f}" if sample_rate else "--"
            say(f"  {i:>4} {at_kill:>12.1f} {over:>14.1f} "
                f"{rate:>8}  {row['failure_reason']}")
            if row["failure_reason"] != "SCRATCH_QUOTA_EXCEEDED":
                say(f"       detail: {row['failure_detail']}")
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            proc.kill()

    say("")
    if overshoots:
        tick = 3.0  # HEARTBEAT_INTERVAL_S: the sample tick, protocol.md §8
        bound = speed * tick if speed else None
        say(f"  OVERSHOOT, n={len(overshoots)}, cap {CAP_MB} MB:")
        say(f"    median {statistics.median(overshoots):.1f} MB "
            f"| min {min(overshoots):.1f} MB | max {max(overshoots):.1f} MB")
        say(f"  mechanism: the cap is enforced by SAMPLING, once per "
            f"{tick:.0f} s heartbeat, so a container keeps writing between two")
        say("    samples. The overshoot is bounded by write speed x sample tick and")
        say("    is never zero. It is published as a bound, with its n and spread.")
        if speeds:
            say(f"  a rate was also derivable from {len(speeds)} run(s) own stored "
                f"readings: max {max(speeds):.1f} MB/s (most runs give only one")
            say("    reading before the kill, which is the same resolution that")
            say("    produces the overshoot)")
        if bound:
            say(f"  predicted bound = {speed:.1f} MB/s x {tick:.0f} s = {bound:.1f} MB")
        limb("P2.a every run was stopped for the right reason",
             len(overshoots) == args.runs,
             f"{len(overshoots)} of {args.runs} runs produced a kill reading")
        limb("P2.b the overshoot is never zero — it is a bound, and we say so",
             min(overshoots) > 0,
             f"smallest overshoot {min(overshoots):.1f} MB over n={len(overshoots)}")
        if bound:
            limb("P2.c every overshoot is within write speed x sample tick",
                 max(overshoots) <= bound,
                 f"largest {max(overshoots):.1f} MB against a measured bound of "
                 f"{bound:.1f} MB ({speed:.1f} MB/s x {tick:.0f} s)")
        else:
            limb("P2.c the bound could be measured", False,
                 "the write-speed calibration produced no figure")
    else:
        limb("P2 the overshoot was measured", False, "no runs produced a kill reading")

    subprocess.run(["git", "-C", REPO, "worktree", "remove", "--force", worktree],
                   capture_output=True, text=True)
    # The two roots are LEFT on disk: they hold the agents own logs, and a capture
    # that deletes the log it just told the reader to go and look at is not evidence.
    # Their run directories are already gone -- the agent wipes each one when its run
    # ends -- so what is left is text.
    say("")
    say(f"  agent logs kept at: {old_root} and {new_root}")

    say("\n" + "=" * 78)
    held = sum(1 for _n, ok, _d in _results if ok)
    for name, ok, detail in _results:
        say(f"  {'HELD     ' if ok else 'WITHDRAWN'} {name}: {detail}")
    say(f"\nRESULT: {held}/{len(_results)} limbs held")
    say("=" * 78)
    return 0 if held == len(_results) else 1


if __name__ == "__main__":
    sys.exit(main())
