"""E0 — headline results: how well does the platform actually work? (brief §5)

**E0 is not a comparison.** Every other experiment in this campaign builds the
weaker version of one of our design choices, runs it beside the real one, and
counts the difference. E0 runs ONLY the shipped system, on its shipped defaults,
and simply measures it. It exists because the final-report rubric has a "Results
and analysis" row asking for a quantitative evaluation of the solution's
effectiveness, and we have nothing for it yet. So the `arm` field here names the
PART being measured (`speedup-2node`, `recovery`, `dispatch`, `logs`), never a
weakened alternative — there is no weakened alternative in E0.

Four parts, each runnable on its own (`--part`):

  E0.1 speedup     12 identical ~5 s runs on 1, then 2, then 3 nodes — 5 reps
  E0.2 recovery    kill an agent mid-run, time the repair — 10 reps
  E0.3 dispatch    submit while a node sits idle and waiting — 20 reps
  E0.4 logs        a container prints a marker; when is it stored? — 20 reps

WHERE EVERY NUMBER COMES FROM (brief §3 rule 3: server-side timings only)
-------------------------------------------------------------------------
MEASURED — read out of the database, stamped by the control plane itself:

  runs.created_at    the run row was written; the run became PENDING
  runs.started_at    the control plane accepted the agent's RUNNING report
                     (set ONCE — a re-dispatched run keeps attempt 1's value, so
                     it is never used as "when the second attempt started")
  runs.finished_at   it accepted the terminal report
  run_logs.ts        a log chunk was stored
  reaper "LOST" line the control-plane container's own log line, timestamped by
                     Docker (the transition leaves no timestamp column behind —
                     the row simply becomes PENDING again)

DERIVED — computed, not read, and labelled as such in every row it appears in:

  assigned_at        the platform stores no "assigned at" column. `wait_for`
                     returns `lease_expires_at - lease_ttl_s`, which IS the moment
                     the server claimed the run, computed from the server's own
                     row. It is the only derived timestamp in the harness, it is
                     the weakest link in E0.3, and the red-team pass should attack
                     it. Every row it touches carries `assigned_at_source` and the
                     `lease_ttl_s` the derivation used.

CROSS-CLOCK — exactly one value cannot avoid it, and says so:

  kill_to_lost_s     the instant an agent is killed exists only on THIS machine,
                     so its anchor is the host clock while LOST is stamped inside
                     Docker. `lease_expiry_to_lost_s` is recorded beside it as the
                     pure server-side answer to the same question (how long past
                     the lease deadline before the platform acted), and
                     `detection_worst_case_s` is the honest ceiling built from
                     server-side values only.

Run it on the HOST, with the stack up, on an otherwise idle machine, on mains
power (brief §6 — parallel load makes every timing invisibly wrong):

    .venv\\Scripts\\python.exe scripts\\experiments\\e0_headline.py --part all \\
        --machine-idle yes --mains-power yes

`--dry` runs 2 repetitions per arm and tags every row `dry: true`, which
`summarize` and `chart` then drop. Rows APPEND to `raw.jsonl`: re-running a part
adds rows rather than replacing them. If a repetition is voided (the control
plane restarted underneath it), `assert_stable` stops the script loudly — the
rows already written are still good, and the affected part is re-run.
"""

from __future__ import annotations

import argparse
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import harness as h  # noqa: E402

EXP = "E0"

# Repetition counts are the brief's (§5). `--dry` overrides them all with 2.
REPS = {"speedup": 5, "recovery": 10, "dispatch": 20, "logs": 20}
DRY_REPS = 2

# One run slot per node. See _fresh_pool for why this matters more than it looks.
NODE_CAPACITY = 1

_TERMINAL = ("SUCCEEDED", "FAILED")

# E0.1: 12 identical runs of about five seconds each (the dummy sleeps
# EPOCH_SECONDS per epoch and prints a line each time).
SPEEDUP_RUNS = 12
SPEEDUP_EPOCHS = "5"
EPOCH_SECONDS = "1"

# E0.2: long enough that the run is unmistakably mid-flight when we pull the plug,
# short enough that ten repetitions fit in the measurement window.
RECOVERY_EPOCHS = "20"
# How long to let the run actually work before killing its node. Nothing is
# measured from this value; it only guarantees the container is doing something.
KILL_AFTER_S = 3.0

# E0.3: the shortest honest job — we are timing the platform's reaction, not work.
DISPATCH_EPOCHS = "1"

# E0.4: the container prints ONE line carrying its own clock reading, then stays
# alive for a few seconds. The sleep is deliberate: the agent streams logs once per
# heartbeat, and it also does a final flush just before posting the terminal
# status. A container that exited instantly would be measured through that final
# flush, which is the wrong path — NFR-4 is about output reaching the user WHILE
# the job runs. Sleeping past two heartbeats forces the live streaming path.
_MARKER = "##MARK"
_MARKER_SNIPPET = (
    "import time; "
    "print('##MARK %.6f' % time.time(), flush=True); "
    "time.sleep(8)"
)
LOG_ENTRYPOINT = ["python", "-c", _MARKER_SNIPPET]
_MARKER_RE = re.compile(r"##MARK\s+([0-9]+(?:\.[0-9]+)?)")

# Docker prints log timestamps with nanoseconds; datetime parses at most
# microseconds. Trim, do not round — we are cutting sub-microsecond noise off a
# number whose real error bars are in the hundreds of milliseconds.
_NANOS_RE = re.compile(r"\.(\d{6})\d+")


# --- small local helpers (arithmetic and parsing only — no harness logic) -----


def _gap(start: datetime | None, end: datetime | None) -> float | None:
    """Seconds from start to end, or None if either end is missing. A missing
    timestamp is recorded as None, never filled in with a guess (brief §3 rule 7)."""
    if start is None or end is None:
        return None
    return (end - start).total_seconds()


def _gap_str(start: str | None, end: str | None) -> float | None:
    """Same, for two timestamps still in their database string form."""
    return _gap(h.parse_ts(start), h.parse_ts(end))


def _parse_docker_ts(value: str | None) -> datetime | None:
    """A `docker compose logs --timestamps` stamp -> aware UTC datetime.

    Two shapes to fix before the standard parser will take it: nanosecond
    fractions, and a trailing `Z` that older Pythons reject."""
    if not value or value == "«MISSING»":
        return None
    text = _NANOS_RE.sub(r".\1", value)
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return h.parse_ts(text)
    except ValueError:
        return None


def _fresh_pool(n: int) -> list:
    """Empty the platform, then bring up n identical worker agents.

    Capacity is pinned to ONE run per node on purpose. Left alone, an agent
    declares `capacity = its CPU core count`, so on an eight-core laptop a single
    node would legally claim all twelve runs of the speedup job at once and
    "three nodes" would measure nothing but the host's core count — the experiment
    would quietly measure the wrong thing. One slot per node makes "a node" mean
    "a worker", which is the thing E0.1 adds more of. The value is written into
    every recorded row so nobody has to take this comment's word for it."""
    h.reset_platform()
    return h.start_agents(n, env={"AGENT_CAPACITY": str(NODE_CAPACITY)})


def _job_span(job_id: str) -> dict:
    """The whole job's wall clock, straight from the run rows.

    One aggregate read instead of per-run reads: the question E0.1 asks is when
    the FIRST run appeared and when the LAST one finished, and both are columns
    the control plane stamped. An examiner can paste this SQL into pgAdmin."""
    rows = h.psql_json(
        "SELECT min(created_at) AS first_created, "
        "max(finished_at) AS last_finished, "
        "count(*) AS runs_total, "
        "count(*) FILTER (WHERE status::text = 'SUCCEEDED') AS runs_succeeded, "
        "max(attempt) AS max_attempt "
        f"FROM runs WHERE job_id = '{job_id}'"
    )
    return rows[0] if rows else {}


def _assignment(result: dict, run_id: str) -> tuple[datetime | None, int | None]:
    """Pull the DERIVED claim time for one run out of a `wait_for` result.

    Returns (assigned_at, lease_ttl_s). Missing means the polling loop never saw
    the run in the ASSIGNED state — the window between the claim and the container
    reporting RUNNING can be shorter than one poll. That is recorded as a miss, not
    papered over."""
    entry = (result.get("assigned_at") or {}).get(run_id)
    if not entry:
        return None, None
    return h.parse_ts(entry.get("at")), entry.get("lease_ttl_s")


_ASSIGNED_SOURCE = (
    "DERIVED: lease_expires_at - lease_ttl_s, both from the run's own server row "
    "(the platform stores no assigned_at column)"
)


# --- E0.1 speedup -------------------------------------------------------------


def part_speedup(reps: int, dry: bool) -> None:
    """12 identical runs on 1, then 2, then 3 nodes.

    The three node counts are measured INSIDE each repetition, one after the
    other, and the speedup is taken against that repetition's own single-node
    time. Pairing them this way means a machine that was a little slower for five
    minutes moves all three arms together instead of inventing a speedup — which
    is what comparing arm medians measured hours apart would do.

    The job is untargeted with `replicas=12`, so the scheduler places purely by
    spare capacity. A targeted job would pin one run per named node (the W4 rule)
    and there would be no scheduling to measure."""
    for rep in range(1, reps + 1):
        baseline: float | None = None
        for nodes in (1, 2, 3):
            arm = f"speedup-{nodes}node"
            agents = _fresh_pool(nodes)
            try:
                job_id = h.submit_job(
                    name=f"{EXP}-{arm}-r{rep}",
                    env={"EPOCHS": SPEEDUP_EPOCHS, "EPOCH_SECONDS": EPOCH_SECONDS},
                    replicas=SPEEDUP_RUNS,
                )
                # poll_s is loosened from the default: every poll runs a psql query
                # through `docker exec`, and on a measurement machine that polling
                # is itself load. The numbers come from the database afterwards, so
                # polling slowly costs nothing and disturbs less.
                result = h.wait_for(job_id, _TERMINAL, timeout_s=900, poll_s=1.0)
                span = _job_span(job_id)
            finally:
                h.stop_agents(agents)

            wall = _gap_str(span.get("first_created"), span.get("last_finished"))
            if nodes == 1:
                baseline = wall
            # The 1-node arm's speedup is 1.0 by definition, not by measurement —
            # it is the yardstick the other two are read against. `baseline_wall_s`
            # travels in every row so the ratio can be recomputed from raw.jsonl.
            speedup = (baseline / wall) if (baseline and wall) else None
            efficiency = (speedup / nodes) if speedup is not None else None

            h.assert_stable(where=f"{arm} rep {rep}")
            h.record(EXP, arm, rep, {
                "nodes": nodes,
                "node_capacity": NODE_CAPACITY,
                "runs_requested": SPEEDUP_RUNS,
                "runs_total": span.get("runs_total"),
                "runs_succeeded": span.get("runs_succeeded"),
                "max_attempt": span.get("max_attempt"),
                "reached_terminal": int(bool(result["reached"])),
                # MEASURED: min(runs.created_at) -> max(runs.finished_at).
                "wall_s": wall,
                "baseline_wall_s": baseline,
                "speedup": speedup,
                "efficiency": efficiency,
                "note": None if result["reached"] else "job did not finish in time",
            }, dry=dry)
            print(f"    rep {rep} {arm}: wall {wall}s, speedup {speedup}")


# --- E0.2 recovery ------------------------------------------------------------


def part_recovery(reps: int, dry: bool) -> None:
    """Kill the node running a job and time the platform putting it right.

    The job must be UNTARGETED so the run is free to move: a targeted run is
    pinned to its node by design (W4), so killing that node could never be
    recovered elsewhere and the experiment would measure a rule, not a repair.

    Two agents: one dies, one is left to pick the work up. `kill_agent` is a hard
    kill — no goodbye, no last heartbeat — because a machine that really dies does
    not get to explain itself, and the agent's clean-stop path would otherwise
    hand the control plane information a dead machine never has.

    Honest limit worth naming before a jury does: this kills the AGENT PROCESS, not
    the machine. The container it started keeps running, orphaned, until it exits on
    its own. That is the same signal the control plane sees either way (the
    heartbeats stop), but it does mean the host is still doing the dead node's work
    during the recovery window."""
    for rep in range(1, reps + 1):
        agents = _fresh_pool(2)
        try:
            job_id = h.submit_job(
                name=f"{EXP}-recovery-r{rep}",
                env={"EPOCHS": RECOVERY_EPOCHS, "EPOCH_SECONDS": EPOCH_SECONDS},
                replicas=1,
            )
            pre = h.wait_for(job_id, ("RUNNING",), timeout_s=180, poll_s=0.25)
            if not pre["reached"] or not pre["runs"]:
                h.assert_stable(where=f"recovery rep {rep} (never ran)")
                h.record(EXP, "recovery", rep, {
                    "node_capacity": NODE_CAPACITY,
                    "note": "run never reached RUNNING — nothing to kill",
                }, dry=dry)
                continue

            run = pre["runs"][0]
            run_id, owner_id = run["run_id"], run["node_id"]
            _, lease_ttl = _assignment(pre, run_id)
            names = {n["node_id"]: n["name"] for n in h.api_get("/nodes")}
            victim = next(
                (a for a in agents if a.name == names.get(owner_id)), None
            )
            if victim is None:
                h.assert_stable(where=f"recovery rep {rep} (no victim)")
                h.record(EXP, "recovery", rep, {
                    "node_capacity": NODE_CAPACITY,
                    "note": f"could not match owning node {owner_id} to an agent",
                }, dry=dry)
                continue

            time.sleep(KILL_AFTER_S)  # let it do real work before the plug is pulled
            # A generous floor for the log search. `--since` is read by the Docker
            # daemon against its own clock, so the margin absorbs any drift between
            # this machine and the container's clock rather than risking a miss.
            since = datetime.now(timezone.utc) - timedelta(seconds=60)
            h.kill_agent(victim)
            killed_at_host = datetime.now(timezone.utc)
            # The agent is dead, so nothing can renew this lease any more: reading
            # it now gives the deadline the reaper will eventually act on. Pure
            # server-side, and it is what makes a no-cross-clock detection number
            # possible below.
            lease_rows = h.psql_json(
                f"SELECT lease_expires_at FROM runs WHERE id = '{run_id}'"
            )
            lease_at_kill = h.parse_ts(
                lease_rows[0]["lease_expires_at"] if lease_rows else None
            )

            # A FRESH wait_for: its assigned_at map starts empty, so the claim it
            # captures is the RE-dispatch (attempt 2). The run is RUNNING at the
            # moment of the kill, never ASSIGNED, so attempt 1's claim cannot be
            # picked up by mistake here.
            post = h.wait_for(job_id, _TERMINAL, timeout_s=300, poll_s=0.25)
            reassigned_at, lease_ttl_2 = _assignment(post, run_id)
            times = h.server_times(run_id)

            lost = next(
                (e for e in h.reaper_events(since=since) if e.get("run") == run_id),
                None,
            )
            lost_at = _parse_docker_ts(lost.get("ts")) if lost else None
            finished_at = h.parse_ts(times.get("finished_at"))

            # How late past the deadline the sweep acted. Both ends server-side, no
            # clock crossing at all — this is the number that survives any argument
            # about whose clock is whose.
            lag = _gap(lease_at_kill, lost_at)
            ttl = lease_ttl if lease_ttl is not None else lease_ttl_2
            # The worst case a user can be promised: if the machine had died the
            # instant after a renewal, the whole lease had to run out first.
            worst = (ttl + lag) if (ttl is not None and lag is not None) else None

            h.assert_stable(where=f"recovery rep {rep}")
            h.record(EXP, "recovery", rep, {
                "node_capacity": NODE_CAPACITY,
                "nodes": 2,
                "final_status": times.get("status"),
                "final_attempt": times.get("attempt"),
                "recovered": int(
                    times.get("status") == "SUCCEEDED" and (times.get("attempt") or 0) > 1
                ),
                "lease_ttl_s": ttl,
                # CROSS-CLOCK: host-clock kill anchor vs a Docker-stamped log line.
                "kill_to_lost_s": _gap(killed_at_host, lost_at),
                # MEASURED, server-side both ends: how long past the lease deadline.
                "lease_expiry_to_lost_s": lag,
                "detection_worst_case_s": worst,
                # DERIVED on the assigned side (see _ASSIGNED_SOURCE).
                "lost_to_reassigned_s": _gap(lost_at, reassigned_at),
                "reassigned_to_finished_s": _gap(reassigned_at, finished_at),
                # MEASURED: the LOST log line -> runs.finished_at.
                "lost_to_finished_s": _gap(lost_at, finished_at),
                # MEASURED: runs.created_at -> runs.finished_at, submit to accepted
                # result, the whole story including the wasted first attempt.
                "total_s": times.get("total_s"),
                "kill_anchor": "host wall clock — the kill exists only on this machine",
                "assigned_at_source": _ASSIGNED_SOURCE,
                "note": None if lost else "no reaper LOST line found for this run",
            }, dry=dry)
            print(f"    rep {rep} recovery: {times.get('status')} "
                  f"attempt {times.get('attempt')}, total {times.get('total_s')}s")
        finally:
            h.stop_agents(agents)


# --- E0.3 dispatch delay ------------------------------------------------------


def part_dispatch(reps: int, dry: bool) -> None:
    """How long from "submitted" to "a machine is on it", with a node already idle.

    The pool is started once and kept across repetitions on purpose: the question
    is what a user waits when the platform is warm and a worker is sitting there,
    not what it costs to bring a worker up.

    Two numbers, and the difference between them is the honest part:

      pending_to_running_measured_s   created_at -> started_at. Both real columns.
      pending_to_assigned_derived_s   created_at -> the DERIVED claim time.

    The second is the one a reader should distrust first, and it is named so they
    can. It can also simply be missing: the gap between the claim and the container
    reporting RUNNING is sometimes shorter than one poll, and then the ASSIGNED
    state is never observed. Missing is recorded as missing."""
    agents = _fresh_pool(1)
    try:
        for rep in range(1, reps + 1):
            job_id = h.submit_job(
                name=f"{EXP}-dispatch-r{rep}",
                env={"EPOCHS": DISPATCH_EPOCHS, "EPOCH_SECONDS": EPOCH_SECONDS},
                replicas=1,
            )
            # Tight polling here, unlike E0.1: catching the short ASSIGNED window
            # is the whole point of this part.
            result = h.wait_for(job_id, _TERMINAL, timeout_s=180, poll_s=0.25)
            if not result["runs"]:
                h.assert_stable(where=f"dispatch rep {rep} (no run)")
                h.record(EXP, "dispatch", rep, {
                    "node_capacity": NODE_CAPACITY,
                    "note": "job produced no run row",
                }, dry=dry)
                continue

            run_id = result["runs"][0]["run_id"]
            assigned_at, lease_ttl = _assignment(result, run_id)
            times = h.server_times(run_id)
            created_at = h.parse_ts(times.get("created_at"))

            h.assert_stable(where=f"dispatch rep {rep}")
            h.record(EXP, "dispatch", rep, {
                "node_capacity": NODE_CAPACITY,
                "nodes": 1,
                "final_status": times.get("status"),
                "lease_ttl_s": lease_ttl,
                # DERIVED on the assigned side.
                "pending_to_assigned_derived_s": _gap(created_at, assigned_at),
                # MEASURED: runs.created_at -> runs.started_at.
                "pending_to_running_measured_s": times.get("queue_to_running_s"),
                "assigned_observed": int(assigned_at is not None),
                "assigned_at_source": _ASSIGNED_SOURCE,
                "note": None if assigned_at else
                        "ASSIGNED window not observed by the poll — claim time missing",
            }, dry=dry)
            print(f"    rep {rep} dispatch: to RUNNING "
                  f"{times.get('queue_to_running_s')}s")
    finally:
        h.stop_agents(agents)


# --- E0.4 log latency (NFR-4) -------------------------------------------------


def part_logs(reps: int, dry: bool) -> None:
    """From "the container printed it" to "the platform has it stored" (NFR-4).

    The job's entrypoint is overridden with a one-line program that prints its own
    clock reading and then stays alive. The entrypoint is a Docker ENTRYPOINT
    override, so the job spec fully decides what runs — no change to the image and
    nothing platform-specific involved.

    THE ASSUMPTION THIS NUMBER RESTS ON, stated plainly because it is the first
    thing to attack: the printing container's clock and the clock that stamps
    `run_logs.ts` are the same clock. The workload container and the control-plane
    container both run on the Docker host, and `RunLog.ts` is filled in by the
    control-plane process at insert time — so both readings come from the Docker
    host's kernel. This machine's own clock is not involved anywhere in the
    subtraction. If the two ever were different clocks, the number would be wrong
    by their offset and nothing in the output would say so."""
    agents = _fresh_pool(1)
    try:
        for rep in range(1, reps + 1):
            job_id = h.submit_job(
                name=f"{EXP}-logs-r{rep}",
                entrypoint=LOG_ENTRYPOINT,
                replicas=1,
            )
            result = h.wait_for(job_id, _TERMINAL, timeout_s=180, poll_s=1.0)
            if not result["runs"]:
                h.assert_stable(where=f"logs rep {rep} (no run)")
                h.record(EXP, "logs", rep, {
                    "node_capacity": NODE_CAPACITY,
                    "note": "job produced no run row",
                }, dry=dry)
                continue

            run_id = result["runs"][0]["run_id"]
            times = h.server_times(run_id)
            rows = h.psql_json(
                "SELECT ts, chunk FROM run_logs "
                f"WHERE run_id = '{run_id}' AND chunk LIKE '%{_MARKER}%' "
                "ORDER BY attempt, seq LIMIT 1"
            )
            printed_at = stored_at = None
            if rows:
                stored_at = h.parse_ts(rows[0]["ts"])
                found = _MARKER_RE.search(rows[0]["chunk"] or "")
                if found:
                    printed_at = datetime.fromtimestamp(
                        float(found.group(1)), tz=timezone.utc
                    )

            h.assert_stable(where=f"logs rep {rep}")
            h.record(EXP, "logs", rep, {
                "node_capacity": NODE_CAPACITY,
                "nodes": 1,
                "final_status": times.get("status"),
                "log_rows": times.get("log_rows"),
                "marker_found": int(printed_at is not None),
                # MEASURED: the container's own print time -> run_logs.ts, the
                # instant the control plane stored the chunk.
                "print_to_stored_s": _gap(printed_at, stored_at),
                "clock_note": "container clock and run_logs.ts are both the Docker host's clock",
                "note": None if printed_at else "marker line not found in stored logs",
            }, dry=dry)
            print(f"    rep {rep} logs: print -> stored "
                  f"{_gap(printed_at, stored_at)}s")
    finally:
        h.stop_agents(agents)


# --- driver -------------------------------------------------------------------

PARTS = {
    "speedup": part_speedup,
    "recovery": part_recovery,
    "dispatch": part_dispatch,
    "logs": part_logs,
}

# One chart.png exists per experiment, and E0's four parts do not share a metric —
# so the chart shows one part's headline number and the title says which. Arms
# belonging to the other parts have no value for it and are drawn as zero; the
# title carries that warning so the picture cannot be read as a comparison.
HEADLINE = {
    "speedup": ("speedup", "E0.1 speedup vs 1 node"),
    "recovery": ("total_s", "E0.2 submit to accepted result, through one node death"),
    "dispatch": ("pending_to_running_measured_s", "E0.3 submit to RUNNING"),
    "logs": ("print_to_stored_s", "E0.4 container print to stored log row"),
}


def _tri(value: str | None) -> bool | None:
    """yes/no/absent -> True/False/None. Absent stays None: the manifest records
    an unknown condition as null rather than letting the script guess it."""
    return None if value is None else (value == "yes")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="E0 — headline results for the platform (W7a brief §5). "
                    "Not a comparison: every part runs on the shipped defaults."
    )
    parser.add_argument(
        "--part", choices=["all", *PARTS], default="all",
        help="which part to measure (default: all four, in order)",
    )
    parser.add_argument(
        "--dry", action="store_true",
        help="2 repetitions per arm, every row tagged dry=True so it can never "
             "reach a summary, a chart or the report",
    )
    parser.add_argument(
        "--machine-idle", choices=("yes", "no"), default=None,
        help="was the machine otherwise idle? Recorded in the manifest. Left out, "
             "it is recorded as null — the script never guesses the conditions",
    )
    parser.add_argument(
        "--mains-power", choices=("yes", "no"), default=None,
        help="was the machine on mains power? Battery throttling alone is enough "
             "to ruin the speedup numbers. Left out -> null",
    )
    parser.add_argument(
        "--fresh", action="store_true",
        help="archive an existing raw.jsonl and start clean. Without it, a run "
             "that would append onto measured rows stops instead — two runs "
             "blended into one summary cannot be told apart afterwards. The old "
             "file is renamed, never deleted",
    )
    args = parser.parse_args(argv)

    h.require_ready()
    if not args.dry:
        h.guard_fresh(EXP, args.fresh)
    h.ensure_image()
    # Prove on /health that we are measuring the system we ship, before anything
    # is measured. E0 never weakens a switch — but "never" has to be checked.
    modes_used = [h.set_mode()]

    chosen = list(PARTS) if args.part == "all" else [args.part]
    reps_by_part = {p: (DRY_REPS if args.dry else REPS[p]) for p in chosen}

    try:
        for part in chosen:
            print(f"\n--- E0 {part} ({reps_by_part[part]} repetitions"
                  f"{', DRY' if args.dry else ''}) ---")
            PARTS[part](reps_by_part[part], args.dry)
    finally:
        # Agents are stopped inside each part; this is the platform-level promise:
        # never walk away from a stack left in a non-default mode, even after a
        # failure partway through.
        h.set_mode()

    metric, title = HEADLINE[chosen[0]]
    if len(chosen) > 1:
        title += " — other E0 arms measure different things and read 0 here"
    h.summarize(EXP, title="E0 — headline results (shipped defaults, not a comparison)")
    h.chart(EXP, kind="bar", metric=metric, title=title)
    h.manifest(
        EXP,
        reps=sum(reps_by_part.values()),
        modes=modes_used,
        machine_idle=_tri(args.machine_idle),
        mains_power=_tri(args.mains_power),
        notes=(
            "E0 headline results — the shipped system only, no weakened arm. "
            "`repetitions` is the TOTAL across the parts run in this invocation; "
            "per-part counts are in repetitions_by_part. Node capacity was pinned "
            f"to {NODE_CAPACITY} run slot per node so that adding a node means "
            "adding a worker. assigned_at is DERIVED from lease_expires_at minus "
            "the lease TTL; kill_to_lost_s is the one value with a host-clock "
            "anchor, and lease_expiry_to_lost_s is its server-side counterpart."
        ),
        extra={
            "parts_run": chosen,
            "repetitions_by_part": reps_by_part,
            "node_capacity": NODE_CAPACITY,
            "speedup_runs_per_job": SPEEDUP_RUNS,
            "dry_run": bool(args.dry),
        },
    )
    print(f"\nE0 done — {h.exp_dir(EXP).relative_to(h.REPO)}: "
          "raw.jsonl, summary.md, chart.png, manifest.json")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (h.HarnessError, AssertionError) as exc:
        print(f"\nE0 STOPPED: {exc}", file=sys.stderr)
        sys.exit(1)
