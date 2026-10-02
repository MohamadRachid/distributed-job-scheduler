"""E5 — learned rescheduling vs blind retrying vs no retry at all (W7a brief §5).

THE QUESTION. A run is killed because the machine ran out of memory. Does
*learning* from that death beat simply trying again? W5c's answer is one number:
the run remembers how much RAM the machine that killed it had, and from then on
only a machine with strictly more RAM may take it. We call that number the
**learned floor**. E5 prices it — against the two things a competent engineer
would plausibly build instead.

A word on the death itself, because everything here depends on it: a **kernel OOM
kill** is the operating system stopping a program that tried to use more memory
than its container was allowed. It is not a guess or an error code the program
chose — the kernel did it, Docker records the flag, and that flag is the ONLY
signal our platform escalates on.

THREE ARMS (`harness.set_mode(reschedule=…)`), one variable between them:

  * `none`    — a memory kill is final. No retry.
  * `blind`   — retry, but learn nothing. Any eligible node may take the run
                again, including the machine that just killed it.
  * `learned` — ours. Retry only onto a strictly stronger machine, and fail at
                once with INSUFFICIENT_POOL when no such machine is registered.

TWO POOLS (three worker nodes each, differing only in the third machine's RAM):

  * `mixed`    — 512 MB, 512 MB, 1024 MB. A machine that can hold the job exists.
  * `all-weak` — 512 MB, 512 MB,  512 MB. No machine can ever hold it.

HELD CONSTANT ACROSS EVERY ARM AND POOL: the same image and entrypoint, the same
memory appetite (`OOM_TARGET_MB`), no user memory cap on the job (so the kill is
always the *machine's* wall, never the user's — see `_submit`), the same node
count, the same CPU/GPU declaration, the same retry budget (`MAX_ESCALATIONS` in
`control-plane/app/scheduler.py` bounds `blind` exactly as it bounds `learned`),
and the same staged start-up (the third machine joins only after the first
machine has already claimed the run — see `one_rep`).

FAIRNESS, stated plainly. `blind` is the steelman, not a strawman: it keeps the
whole recovery path, the whole fencing guarantee and the *same* number of
retries. What it does not get is our give-up rule — a blind retrier has no
learned floor, so it has nothing to compare the pool against and cannot know the
pool is too small. Lending it that rule would be lending it our idea and then
congratulating it. The arms are implemented in `scheduler.apply_oom_escalation`;
this file only keeps the conditions around them identical.

READING THE SUMMARY. `success` is recorded 0/1 per repetition, so its median says
whether most repetitions finished and min/max say whether the arm was mixed
(min 0, max 1). `raw.jsonl` carries every outcome verbatim, including the final
`failure_reason`.

    .venv\\Scripts\\python.exe scripts\\experiments\\e5_learned_vs_blind.py
    .venv\\Scripts\\python.exe scripts\\experiments\\e5_learned_vs_blind.py --dry
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import harness as h  # noqa: E402

EXP = "E5"

ARMS = ("none", "blind", "learned")
POOLS = ("mixed", "all-weak")

# The pools the brief locks (§5). Two weak machines plus a third that is either
# strong (mixed) or weak like the others (all-weak) — the ONLY difference.
WEAK_RAM_MB = 512
STRONG_RAM_MB = 1024

# How much RAM the job asks for. It must be comfortably ABOVE the weak machines
# (so they are certainly killed) and comfortably BELOW the strong one (so it
# certainly fits) — otherwise the experiment measures nothing at all. The margin
# also absorbs the Python interpreter's own footprint inside the container, which
# counts against the same cap as the memory the job deliberately allocates.
DEFAULT_TARGET_MB = 700
MARGIN_MB = 128

# The dummy allocates 8 MB every 0.4 s when OOM_TARGET_MB is set, so a repetition
# is tens of seconds long. Nothing here is timed by this process — all timings are
# read out of Postgres — so polling slowly costs no accuracy and keeps the machine
# quiet, which the measurement rules require.
POLL_S = 2.0
CLAIM_TIMEOUT_S = 120
DEFAULT_REP_TIMEOUT_S = 300

TERMINAL = ("SUCCEEDED", "FAILED")
# "Somebody has taken this run" — the moment the third machine is allowed to join.
CLAIMED = ("ASSIGNED", "RUNNING", "SUCCEEDED", "FAILED")

INSUFFICIENT_POOL = "INSUFFICIENT_POOL"
OOM_KILLED = "OOM_KILLED"

# Metrics that reach summary.md and the chart. Everything else stays in raw.jsonl.
SUMMARY_METRICS = [
    "success",
    "attempts_used",
    "escalation_count",
    "time_to_answer_s",
    "time_to_success_s",
    "wasted_node_seconds",
    "insufficient_pool",
    "oom_observed",
    "timeout",
]


def third_node_ram_mb(pool: str) -> int:
    return STRONG_RAM_MB if pool == "mixed" else WEAK_RAM_MB


def agent_env(ram_mb: int) -> dict[str, str]:
    """The environment that makes one worker process behave like a machine of a
    given size.

    `AGENT_RAM_MB` is not something this experiment invented: it is the agent's
    own documented override ("env override still wins, for tests and capacity
    experiments", `agent/_detect_ram_mb`). It is also not cosmetic. The node
    declares that number to the control plane, and the agent then caps every
    uncapped container at exactly that number (W5c Step 1), so the kill really
    does happen at 512 MB, really is performed by the kernel, and really does set
    Docker's OOMKilled flag. The simulation stops at the host: the laptop still
    has all its RAM, so what we model is a machine whose *budget* is 512 MB, not
    a machine under real system-wide memory pressure.

    The other two settings remove variables rather than add any: capacity 1
    because E5 runs one run at a time, and no GPU because the pool must differ in
    RAM and nothing else."""
    return {
        "AGENT_RAM_MB": str(ram_mb),
        "AGENT_CAPACITY": "1",
        "AGENT_HAS_GPU": "false",
    }


def _run_row(run_id: str) -> dict:
    """Where the run is right now, and how big the machine holding it is."""
    rows = h.psql_json(
        "SELECT r.attempt, r.status::text AS status, r.node_id, "
        "n.name AS node_name, n.ram_mb AS node_ram_mb "
        "FROM runs r LEFT JOIN nodes n ON n.id = r.node_id "
        f"WHERE r.id = '{run_id}'"
    )
    return rows[0] if rows else {}


def _attempt_spans(run_id: str) -> list[dict]:
    """How long each attempt of this run actually executed, per attempt.

    The platform stores no per-attempt start/end columns — `started_at` is set
    once and `finished_at` is cleared on every requeue — but every stored log
    chunk carries its attempt and a control-plane timestamp, and the dummy prints
    a line for every 8 MB it allocates. So the span from an attempt's first
    stored chunk to its last is a server-side measurement of that attempt's
    execution.

    It is a LOWER BOUND, deliberately named as one: it excludes the container's
    start-up and whatever happened between the last stored chunk and the kill.
    Under-reporting wasted time is the safe direction — it can only make the
    weaker arms look better than they were."""
    return h.psql_json(
        "SELECT attempt, min(ts) AS first_ts, max(ts) AS last_ts, count(*) AS chunks "
        f"FROM run_logs WHERE run_id = '{run_id}' GROUP BY attempt"
    )


def _wasted_node_seconds(run_id: str, final_status: str, final_attempt: int) -> tuple[float, list[dict]]:
    """Execution time that produced nothing.

    One attempt at most is ever accepted (that is the at-most-once guarantee), so
    every other attempt's execution was spent and thrown away. If the run ended
    FAILED, no attempt was accepted and all of them are waste — which is the
    honest reading: the pool burned that time and the user got no result."""
    total = 0.0
    detail: list[dict] = []
    for row in _attempt_spans(run_id):
        first, last = h.parse_ts(row["first_ts"]), h.parse_ts(row["last_ts"])
        span = (last - first).total_seconds() if first and last else None
        accepted = final_status == "SUCCEEDED" and row["attempt"] == final_attempt
        detail.append({
            "attempt": row["attempt"],
            "log_span_s": round(span, 3) if span is not None else "«MISSING»",
            "chunks": row["chunks"],
            "accepted": accepted,
        })
        if not accepted and span is not None:
            total += span
    return round(total, 3), detail


def _submit(mode: str, pool: str, rep: int, target_mb: int) -> str:
    """One job, one run, no user memory cap.

    The missing cap is the whole point. When the user sets no limit of their own,
    the agent caps the container at the node's declared RAM, so the kill proves
    the MACHINE was too small — the one case any of the three arms treats
    differently. A job carrying its own `mem_limit_mb` would be killed at the
    user's wall instead, and all three arms agree to leave that alone, so it
    would compare nothing.

    Untargeted on purpose too: a targeted run is pinned to the machines the user
    named, so it could never move to a stronger one."""
    return h.submit_job(
        name=f"{EXP}-{mode}-{pool}-{rep}",
        entrypoint=["python", "train.py", "--oom"],
        env={"OOM_TARGET_MB": str(target_mb)},
        resource_reqs={},
        target_node_ids=None,
        replicas=1,
    )


def _watch(job_id: str, run_id: str, deadline: float) -> tuple[dict, list[dict]]:
    """Wait for the job to reach a final answer, noting every machine the run
    passes through on the way.

    The harness decides when the job is done; this loop only adds an observation
    it does not make — which node held which attempt. That history is not
    recoverable afterwards (the run row keeps only its current node), and for
    `blind` it is the evidence that matters: it shows the run being handed back to
    a machine that already killed it.

    The placement note is an observation, never a timing. Every number that
    reaches a result row still comes out of the database."""
    placements: list[dict] = []
    seen: set[tuple] = set()
    result: dict = {"reached": False, "runs": []}
    while True:
        row = _run_row(run_id)
        key = (row.get("attempt"), row.get("node_id"))
        if row.get("node_id") and key not in seen:
            seen.add(key)
            placements.append({
                "attempt": row.get("attempt"),
                "node": row.get("node_name"),
                "node_ram_mb": row.get("node_ram_mb"),
            })
        left = deadline - time.monotonic()
        if left <= 0:
            break
        slice_s = min(POLL_S, left)
        result = h.wait_for(job_id, TERMINAL, timeout_s=slice_s, poll_s=slice_s)
        if result["reached"]:
            break
    return result, placements


def one_rep(
    mode: str,
    pool: str,
    rep: int,
    *,
    target_mb: int,
    rep_timeout_s: int,
    dry: bool = False,
) -> dict:
    """One repetition: a fresh platform, a fresh pool, one job, one recorded row.

    The pool is rebuilt from empty every time. A learned floor lives on the run
    row, so truncating the tables removes it — but the nodes are truncated with
    them, and stale worker processes would keep old tokens, so the workers are
    restarted too. Cheap insurance against the one contamination that would
    invalidate the whole experiment: a requirement learned in one repetition
    still steering placement in the next.

    The third machine joins AFTER the first claim, and it does so in both pools.
    Otherwise the strong machine would sometimes win the first claim, the job
    would simply succeed, no memory kill would happen, and the repetition would
    compare nothing — for every arm equally, but it would still be noise. Waiting
    means every repetition starts where the interesting question starts: on a
    machine too small to hold the job. Identical in both pools, so it cannot
    favour an arm; the pools still differ only in the third machine's size."""
    arm = f"{mode}/{pool}"
    third_ram = third_node_ram_mb(pool)

    h.reset_platform()
    weak = h.start_agents(
        2, names=["e5-weak-1", "e5-weak-2"], env=agent_env(WEAK_RAM_MB)
    )
    third: list = []
    try:
        job_id = _submit(mode, pool, rep, target_mb)
        deadline = time.monotonic() + rep_timeout_s
        run_id = h.api_get(f"/jobs/{job_id}/runs")[0]["run_id"]

        claimed = h.wait_for(
            job_id, CLAIMED, timeout_s=min(CLAIM_TIMEOUT_S, rep_timeout_s), poll_s=1.0
        )
        first_row = _run_row(run_id)
        first_ram = first_row.get("node_ram_mb")

        # The third machine arrives now — while the first attempt is still
        # allocating, so it is registered long before any retry can be placed.
        third = h.start_agents(1, names=["e5-node-3"], env=agent_env(third_ram))
        h.wait_online(3)

        result, placements = _watch(job_id, run_id, deadline)

        # Re-check the mode BEFORE recording. A control plane that restarted
        # under this repetition would leave the row labelled with an arm it
        # cannot be proven to have run under; better to lose it loudly.
        h.assert_stable(where=f"{arm} rep {rep}")

        times = h.server_times(run_id)
        status = times["status"]
        reason = times["failure_reason"]
        success = status == "SUCCEEDED"
        wasted, spans = _wasted_node_seconds(run_id, status, times["attempt"])
        final_row = _run_row(run_id)

        # Did the mechanism under test actually fire? If a repetition finished on
        # its first attempt with no memory kill at all, it measured nothing about
        # rescheduling — the setup slipped (a target that fits the weak machines,
        # or a limit Docker did not enforce). Recorded, never quietly dropped.
        oom_observed = bool(
            times["escalation_count"] > 0 or reason in (OOM_KILLED, INSUFFICIENT_POOL)
        )

        metrics = {
            "reschedule": mode,
            "pool": pool,
            "success": int(success),
            "final_status": status,
            # Executions burned: `attempt` is bumped by every claim, so it counts
            # the number of times this run was actually executed.
            "attempts_used": times["attempt"],
            "escalation_count": times["escalation_count"],
            # created_at -> the accepted terminal result, both stamped by the
            # control plane. On the all-weak pool this IS the "time to a final
            # honest answer" the brief asks for, success or failure alike.
            "time_to_answer_s": times["total_s"],
            "time_to_success_s": times["total_s"] if success else None,
            "wasted_node_seconds": wasted,
            "insufficient_pool": int(reason == INSUFFICIENT_POOL),
            "oom_observed": int(oom_observed),
            "timeout": int(not result["reached"]),
            # Verbatim, so the difference between "we knew and said so" and "we
            # ran out of retries" is visible in the data and not only in a timing.
            "failure_reason": reason,
            "failure_detail": times["failure_detail"],
            "learned_min_ram_mb": times["learned_min_ram_mb"],
            "exit_code": times["exit_code"],
            "first_node_ram_mb": first_ram,
            "final_node_ram_mb": final_row.get("node_ram_mb"),
            "placements": placements,
            "attempt_spans": spans,
            "log_rows": times["log_rows"],
            # The setup, on every row, so no reading of this file depends on
            # remembering how the bench was configured.
            "oom_target_mb": target_mb,
            "weak_ram_mb": WEAK_RAM_MB,
            "third_node_ram_mb": third_ram,
            "rep_timeout_s": rep_timeout_s,
            "claim_reached": int(claimed["reached"]),
        }
        if not oom_observed:
            metrics["note"] = (
                "no memory kill in this repetition — it compares nothing about "
                "rescheduling; check --target-mb against the weak node's RAM"
            )
        return h.record(EXP, arm, rep, metrics, dry=dry)
    finally:
        h.stop_agents([*weak, *third])


def _check_target(target_mb: int) -> None:
    """Stop early rather than measure nothing.

    The target has to be far enough above the weak machines that they are
    certainly killed, and far enough below the strong one that it certainly fits
    once the interpreter's own footprint is counted."""
    if target_mb < WEAK_RAM_MB + MARGIN_MB:
        raise SystemExit(
            f"--target-mb {target_mb} is too close to the weak node's {WEAK_RAM_MB} MB. "
            f"The job must clearly exceed it, so use at least {WEAK_RAM_MB + MARGIN_MB}."
        )
    if target_mb > STRONG_RAM_MB - MARGIN_MB:
        raise SystemExit(
            f"--target-mb {target_mb} is too close to the strong node's {STRONG_RAM_MB} MB. "
            f"It must still fit with the interpreter's own memory, so use at most "
            f"{STRONG_RAM_MB - MARGIN_MB}."
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="E5 — learned vs blind vs no retry")
    parser.add_argument("--arms", default=",".join(ARMS), help="comma-separated: " + ",".join(ARMS))
    parser.add_argument("--pools", default=",".join(POOLS), help="comma-separated: " + ",".join(POOLS))
    parser.add_argument("--reps", type=int, default=10, help="repetitions per arm and pool")
    parser.add_argument("--dry", action="store_true", help="2 repetitions, every row tagged dry")
    parser.add_argument("--target-mb", type=int, default=DEFAULT_TARGET_MB,
                        help="how much RAM the job asks for")
    parser.add_argument("--rep-timeout", type=int, default=DEFAULT_REP_TIMEOUT_S,
                        help="seconds before a repetition is recorded as a timeout")
    # Left as null when not given: the harness records a missing condition rather
    # than a guessed one, and a wrong condition is worse than a missing one.
    parser.add_argument("--machine-idle", action=argparse.BooleanOptionalAction, default=None)
    parser.add_argument("--mains-power", action=argparse.BooleanOptionalAction, default=None)
    parser.add_argument("--fresh", action="store_true",
                        help="archive an existing raw.jsonl and start clean; without "
                             "it a run that would append onto measured rows stops "
                             "instead (the old file is renamed, never deleted)")
    args = parser.parse_args(argv)

    arms = [a.strip() for a in args.arms.split(",") if a.strip()]
    pools = [p.strip() for p in args.pools.split(",") if p.strip()]
    if set(arms) - set(ARMS):
        raise SystemExit(f"unknown arm(s): {sorted(set(arms) - set(ARMS))}; pick from {ARMS}")
    if set(pools) - set(POOLS):
        raise SystemExit(f"unknown pool(s): {sorted(set(pools) - set(POOLS))}; pick from {POOLS}")
    _check_target(args.target_mb)

    reps = 2 if args.dry else args.reps
    h.require_ready()
    if not args.dry:
        h.guard_fresh(EXP, args.fresh)
    h.ensure_image()

    modes_used: list[dict] = []
    try:
        # One control-plane restart per arm, not per repetition: the arm is the
        # only thing set_mode changes, and every restart is a chance for the
        # platform to be caught mid-flight.
        for mode in arms:
            modes_used.append(h.set_mode(reschedule=mode))
            for pool in pools:
                for rep in range(1, reps + 1):
                    print(f"\n--- {mode}/{pool} rep {rep}/{reps} "
                          f"(nodes {WEAK_RAM_MB}/{WEAK_RAM_MB}/{third_node_ram_mb(pool)} MB, "
                          f"job wants {args.target_mb} MB) ---")
                    row = one_rep(
                        mode, pool, rep,
                        target_mb=args.target_mb,
                        rep_timeout_s=args.rep_timeout,
                        dry=args.dry,
                    )
                    print(
                        f"    {row['final_status']} after {row['attempts_used']} attempt(s)"
                        f"{' [TIMEOUT]' if row['timeout'] else ''}"
                        f" — answer in {row['time_to_answer_s']}s,"
                        f" wasted {row['wasted_node_seconds']}s,"
                        f" reason {row['failure_reason']}"
                    )
                    if not row["oom_observed"]:
                        print("    WARNING: no memory kill happened — this repetition "
                              "compares nothing (see the note in raw.jsonl)")
    finally:
        # Never leave the platform on a weakened arm, even after a failure.
        h.set_mode()

    h.summarize(
        EXP,
        metrics=SUMMARY_METRICS,
        title="E5 — learned rescheduling vs blind retry vs no retry",
    )
    h.chart(
        EXP,
        metric="time_to_answer_s",
        title="E5 — time to a final answer (median, min–max over repetitions)",
    )
    h.manifest(
        EXP,
        reps=reps,
        modes=modes_used,
        machine_idle=args.machine_idle,
        mains_power=args.mains_power,
        notes=(
            "Three rescheduling arms x two pools. Nodes are simulated by the agent's "
            "own AGENT_RAM_MB override and the container is really capped at that "
            "number, so every kill is a real kernel OOM kill. The job carries no user "
            "memory cap, so every kill is a node-capacity kill. wasted_node_seconds is "
            "a lower bound measured from per-attempt log-chunk timestamps."
        ),
        extra={
            "e5": {
                "arms": arms,
                "pools": pools,
                "weak_ram_mb": WEAK_RAM_MB,
                "strong_ram_mb": STRONG_RAM_MB,
                "oom_target_mb": args.target_mb,
                "rep_timeout_s": args.rep_timeout,
                "nodes_per_pool": 3,
                "third_node_joins": "after the first claim, in both pools",
                "job": {
                    "image": "fyp-dummy:latest",
                    "entrypoint": ["python", "train.py", "--oom"],
                    "user_mem_limit_mb": None,
                    "target_node_ids": None,
                    "replicas": 1,
                },
            }
        },
    )
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except h.HarnessError as exc:
        print(f"\nE5 STOPPED: {exc}", file=sys.stderr)
        sys.exit(1)
