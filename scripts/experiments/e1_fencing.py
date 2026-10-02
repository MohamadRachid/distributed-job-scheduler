"""E1 — is the fencing token really needed? (W7a brief §5)

THE QUESTION
------------
Every run in our platform carries a small number called `attempt`. It goes up by
one each time the run is handed to a machine, and every message an agent sends
about that run has to carry the number it was given. A message carrying an
out-of-date number is refused. That number is the **fencing token**: it is how the
control plane tells "the machine I am relying on right now" from "a machine I gave
up on a minute ago".

A **lease** is the other half: when a machine claims a run it gets a deadline
(`lease_expires_at`). It has to keep checking in to push that deadline forward. If
the deadline passes, the platform assumes the machine is gone and hands the run to
somebody else. A **zombie** is a machine that was assumed dead, is not, and shows
up later with the result of work nobody is waiting for any more.

The obvious question a juror asks is: if you already have a lease and a timeout,
why do you also need the token? E1 answers it by running the *same* failure with
different amounts of the machinery switched on, and counting what breaks.

There is a sharper version of that question, and it is the one that actually
threatens us: *"a check that the reporting machine still owns the run would catch
this too — so why an epoch counter?"* Answering it needs an arm that keeps the
ownership check and drops only the epoch (`owner_only`), AND a scenario where the
owner itself reports a dead execution (`reclaim`). Without both, the numbers would
only show that some check is needed, which nobody disputes, and the token would
look like complexity we never earned.

THE FOUR ARMS (`EXPERIMENT_GUARANTEE_MODE`, flipped by `harness.set_mode`)
-------------------------------------------------------------------------
`none` — no safety net at all.
    DROPS: the reaper's sweep (a run whose machine dies is never given to anyone
    else) AND the fencing-class rejection.
    KEEPS: everything else — the lease is still written, results are still final
    once accepted, unknown runs are still 404.

`lease_only` — "a timeout is enough". This is the honest system a competent
    engineer builds *before* they have met the duplicate-result problem, and it is
    the arm that decides whether this experiment is fair.
    KEEPS: the lease, the reaper's sweep, the requeue to PENDING, the re-claim by
    another node, the retry budget, AND the terminal-result guard ("a result, once
    accepted, is final"). It also keeps the 404 on an unknown run, node-token
    authentication, and the 409 fencing on log and sample uploads (the switch only
    touches the status endpoint) — so if anything this arm is treated slightly
    better than a real lease-only system would be.
    DROPS: exactly one thing — the fencing-class rejection on
    `POST /agent/runs/{id}/status`. That class is both halves of it, "stale
    attempt" AND "wrong node" (the design puts both in that class), because
    dropping only one half would leave the weak arm holding a piece of our fence
    while claiming to have none.

`owner_only` — "check it is still your run". The strongest honest alternative, and
    the arm the token has to beat.
    KEEPS: everything `lease_only` keeps, PLUS the wrong-node half of the
    fencing-class rejection — a report from a machine that does not own the run is
    still refused 409.
    DROPS: only the epoch comparison. So a report from the machine that DOES own
    the run is accepted no matter how old the execution behind it is.

`full` — today's shipped system: both halves.

THE TWO SCENARIOS — the chaos test, generalised
-----------------------------------------------
Not a new way to make a zombie: this is `scripts/chaos_test.py` step for step,
driven over the real HTTP API instead of in-process, so the switch, the auth layer
and the endpoints are all the real ones.

    takeover — a machine that stayed down
    1. node A claims the run and reports RUNNING          (attempt 1)
    2. node A goes silent — it simply never heartbeats again
    3. the lease lapses; the platform does whatever its arm allows
    4. node B claims the requeued run and starts work     (attempt 2)
    5. zombie A wakes up and posts its OLD result         (still attempt 1)
    6. the live attempt B finishes
    7. we read what the platform stored, and who it credited

    reclaim — a machine that came back
    Steps 1–3 identical, but there is only ONE node in the pool. At step 4 node A
    itself re-claims its own run (attempt 2) and starts a fresh execution. At step
    5 its PREVIOUS execution reports, still carrying attempt 1.

The difference is the whole point. In `takeover` the zombie is a machine that no
longer owns the run, so "is this still your run?" rejects it and `owner_only`
looks exactly as safe as `full`. In `reclaim` the zombie IS the owner — a network
partition healed, or a laptop woke, or an agent restarted while its previous
container was still running — so ownership tells you nothing and only the attempt
number can separate the dead execution from the live one.

Step 5 comes before step 6 on purpose — that is the chaos-test ordering, and it is
the window in which the fence is the ONLY thing standing between a superseded
execution and the stored result. See "Limits" below.

HOW A NODE EXISTS HERE WITHOUT AN AGENT PROCESS
-----------------------------------------------
To the control plane a node *is* its token. `POST /agent/register` hands one out
and every later agent call presents it. The agent program is only the thing that
normally holds that token. This experiment does not start one, for two reasons:
"stop heart-beating at this exact moment" is something you do by *not* sending the
next request, and no container needs to run for a status report to be accepted.
Nothing is bypassed — every call below goes through the same doors a real agent
uses, with the same authentication.

FORCING THE LEASE TO EXPIRE
---------------------------
`chaos_test.py` calls the reaper in-process and hands it a fake clock. We cannot:
we are on the host, driving the real server, whose reaper sweeps on its own timer.
So we wait out the real lease — `lease_ttl_s` (read from the running container) plus
a grace margin that has to be larger than `REAPER_INTERVAL_S`. Two rules keep that
honest:

  * the wait is **identical in every arm**, and we always wait the FULL window
    rather than stopping early when the requeue lands, so the zombie's execution is
    the same length everywhere and the arms stay comparable;
  * the wait is **never a measured number**. Everything recorded below comes from
    Postgres — row timestamps, or `SELECT now()` read on the server's own clock.

LIMITS OF THIS EXPERIMENT (name them before a juror does)
---------------------------------------------------------
  * The zombie posts while the live attempt is still running. If it posted *after*
    the live attempt had finished, the terminal-result guard alone would refuse it
    in every arm. E1 measures the window where the fence is the only protection.
  * In the `takeover` scenario the run goes to a DIFFERENT node, so the zombie is
    both wrong-node and stale-attempt, and a plain "is this still your run?" check
    catches those cases too. That is why `owner_only` and the `reclaim` scenario
    exist: `reclaim` is the case only the token catches, and `token_only_case` and
    `zombie_still_owned_run` are recorded per repetition so the report states
    plainly which cases needed the epoch and which did not. Reading the `takeover`
    numbers alone would overstate what the token buys; reading `reclaim` alone
    would overstate how often that situation arises.
  * No container runs. The "execution" is simulated, exactly as in the chaos test;
    that is what makes the scenario deterministic. E1 measures the control plane's
    behaviour, not a container's.

    .venv\\Scripts\\python.exe scripts\\experiments\\e1_fencing.py
    .venv\\Scripts\\python.exe scripts\\experiments\\e1_fencing.py --dry
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import requests  # noqa: E402

import harness as h  # noqa: E402

EXP = "E1"
# Brief order: weakest first, ours last.
#
# `owner_only` is the arm that decides whether the per-attempt TOKEN earns its
# place. It keeps the "is this still your run?" check and drops ONLY the epoch
# comparison — what a careful engineer writes who has never met the
# duplicate-result problem. Without this arm the experiment proves only that SOME
# fencing-class check is needed, which nobody disputes, and the token itself would
# look like unearned complexity.
ARMS = ("none", "lease_only", "owner_only", "full")

# Two scenarios, because one of them cannot separate the last two arms.
#
#   takeover — a DIFFERENT node finishes the run. The zombie is then both
#              wrong-node AND stale-attempt, so an ownership check alone catches
#              it. `owner_only` and `full` should behave identically here, and
#              that identical behaviour is itself a finding, not a null result.
#   reclaim  — the SAME node comes back, re-claims its own run at a higher
#              attempt, and then its OLD execution reports. Ownership PASSES —
#              the node genuinely does own the run — so only the attempt number
#              can tell the dead execution from the live one. This is the case the
#              fencing token exists for, and the only place the two arms part.
#
# Both are real. `takeover` is a machine that stayed down; `reclaim` is a machine
# that came back — a network partition healing, a laptop waking, an agent
# restarting while its previous container is still running and still holds work.
SCENARIOS = ("takeover", "reclaim")

DEFAULT_REPS = 20
DRY_REPS = 2

TERMINAL = ("SUCCEEDED", "FAILED")

# What the LIVE attempt reports. Held constant so the only thing that
# varies between repetitions is the zombie's result — that keeps "was the final
# status wrong?" unambiguous: the state the sanctioned execution actually reached
# is always SUCCEEDED when there was a sanctioned execution.
LIVE_RESULT, LIVE_EXIT = "SUCCEEDED", 0

# Exit code 1, never 137. 137 is the kernel's out-of-memory kill, which the server
# classifies as OOM_KILLED and hands to the W5c escalation path — a second moving
# part that has nothing to do with fencing. Exit 1 is a plain application error, so
# E1 varies one thing only.
FAILED_EXIT = 1


# --- talking to the platform as a worker ------------------------------------


def _register(name: str) -> dict:
    """Create a node and keep its token. Registration is the one open agent
    endpoint — it exists precisely to hand out the credential — so this is the same
    first step a real agent takes on a machine that has just joined the pool."""
    resp = requests.post(
        f"{h.API}/agent/register",
        json={
            "name": name,
            "specs": {"cpu_cores": 4, "has_gpu": False, "ram_mb": 8192, "capacity": 4},
        },
        timeout=15,
    )
    if resp.status_code != 200:
        raise h.HarnessError(f"register {name} failed ({resp.status_code}): {resp.text[:200]}")
    body = resp.json()
    return {"name": name, "node_id": body["node_id"], "token": body["token"]}


def _auth(node: dict) -> dict[str, str]:
    return {"Authorization": f"Bearer {node['token']}"}


def _heartbeat(node: dict) -> list[dict]:
    """One heartbeat. The response carries any work the scheduler just claimed for
    this node — assignment happens inside the heartbeat, there is no separate
    dispatch call."""
    resp = requests.post(
        f"{h.API}/agent/heartbeat",
        json={"node_id": node["node_id"], "status": "idle", "running": []},
        headers=_auth(node),
        timeout=15,
    )
    if resp.status_code != 200:
        raise h.HarnessError(
            f"heartbeat for {node['name']} failed ({resp.status_code}): {resp.text[:200]}"
        )
    return resp.json()["assignments"]


def _post_status(node: dict, run_id: str, attempt: int, state: str,
                 exit_code: int | None = None) -> requests.Response:
    """Report a run's state. Deliberately returns the raw response instead of
    raising: a 409 here is not a bench failure, it is the measurement."""
    return requests.post(
        f"{h.API}/agent/runs/{run_id}/status",
        json={"attempt": attempt, "state": state, "exit_code": exit_code},
        headers=_auth(node),
        timeout=15,
    )


def _claim(node: dict, run_id: str, tries: int = 6) -> dict | None:
    """Heartbeat until this node is handed the run we care about, or give up.

    The run is PENDING the moment the job is submitted, so the first heartbeat
    normally gets it; the retries only cover a slow first request. Assignments for
    any other run are ignored — the platform is reset before every repetition, so
    there should not be any.

    It never sleeps after the LAST attempt. That matters for fairness: the `none`
    arm is the one where this loop always runs out of tries, and a trailing sleep
    would quietly add time to that arm's zombie execution and nobody else's."""
    for attempt_no in range(tries):
        for assignment in _heartbeat(node):
            if assignment["run_id"] == run_id:
                return assignment
        if attempt_no < tries - 1:
            time.sleep(0.5)
    return None


def _db_now() -> datetime:
    """The SERVER's clock, read out of Postgres.

    This is the same clock that stamps `started_at` and `finished_at`, so a gap
    measured between one of those columns and this value is a pure server-side
    interval. The host's own clock never enters a recorded number (brief §3 rule 3).
    """
    return h.parse_ts(h.psql_json("SELECT now() AS t")[0]["t"])


def _gap(start: datetime | None, end: datetime | None) -> float | None:
    """Seconds between two server timestamps, or None if either is missing. Never
    guesses a value for a timestamp that does not exist."""
    if start is None or end is None:
        return None
    return round((end - start).total_seconds(), 3)


def _unfenced_audit_lines(run_id: str) -> int:
    """Count the control plane's OWN record that it let a superseded report through.

    When a fencing-class rejection is skipped, the server logs
    `EXPERIMENT accepted-unfenced run=… posted_attempt=… current_attempt=…`. Our
    driver-side observation (the 200 we got back) is the primary number; this is the
    corroborating source, so the report can point at the server's log as well as at
    our own notes. Uses the harness's shell helper rather than a second copy of it.

    `--since 5m` only bounds how much log is read — that is the one place a wall
    clock is used, and it decides nothing. The run id in the needle is what makes
    the count belong to this repetition."""
    proc = h._run(["docker", "compose", "logs", "--no-color", "--since", "5m", h.CP_SERVICE])
    needle = f"accepted-unfenced run={run_id}"
    return sum(1 for line in proc.stdout.splitlines() if needle in line)


def _lost_lines(run_id: str) -> int:
    """How many times the reaper announced this run LOST. Zero in the `none` arm by
    design (the sweep is off); one in the other two when recovery fires."""
    return sum(1 for event in h.reaper_events() if event.get("run") == run_id)


# --- one repetition ---------------------------------------------------------


def one_rep(arm: str, rep: int, *, scenario: str, silence_s: float, work_s: float,
            lease_ttl_s: int, dry: bool = False) -> dict:
    """Drive one scenario once, on whichever arm is currently loaded, and record
    what the platform did with it.

    The two scenarios share this single code path on purpose. They differ in
    exactly one thing — WHO claims the run after the reaper requeues it — and
    everything else (timings, ordering, job, waits, metrics) is identical. If they
    were two functions they would drift, and any difference in the numbers could
    then be an artefact of the driver rather than of the platform."""
    if scenario not in SCENARIOS:
        raise h.HarnessError(f"unknown scenario {scenario!r}; pick from {SCENARIOS}")
    # A clean slate every repetition. Truncating is cheap here and it removes a
    # whole class of interference: a run left PENDING by an earlier repetition could
    # otherwise be claimed by this repetition's node A alongside its own.
    h.reset_platform()

    node_a = _register(f"e1-{arm}-{scenario}-{rep}-a")
    # `takeover` needs a second machine to hand the run to. `reclaim` deliberately
    # has NO second machine: the only node in the pool is the one that went silent,
    # so when it comes back it can only re-claim its own run — which is precisely
    # the situation an ownership check cannot see through.
    node_b = _register(f"e1-{arm}-{scenario}-{rep}-b") if scenario == "takeover" else None
    live_node = node_b if scenario == "takeover" else node_a

    job_id = h.submit_job(name=f"{EXP}-{arm}-{scenario}-{rep}",
                          env={"EPOCHS": "1", "EPOCH_SECONDS": "1"})
    runs = h.api_get(f"/jobs/{job_id}/runs")
    if len(runs) != 1:
        raise h.HarnessError(f"expected exactly one run for job {job_id[:8]}, got {len(runs)}")
    run_id = runs[0]["run_id"]

    # 1. Node A claims the run and reports it started. The server stamps
    #    runs.started_at on the first accepted RUNNING and never overwrites it, so
    #    that column is exactly the moment node A's execution began.
    assignment_a = _claim(node_a, run_id)
    if assignment_a is None:
        raise h.HarnessError(f"node A was never assigned run {run_id[:8]} — bench problem")
    attempt_a = assignment_a["attempt"]
    started = _post_status(node_a, run_id, attempt_a, "RUNNING")
    if started.status_code != 200:
        raise h.HarnessError(f"node A could not start its own run: {started.status_code}")

    # 2 + 3. Node A goes silent. We stop calling on its behalf and wait out the
    #    real lease. The same full window in every arm — never cut short when the
    #    requeue lands early — so the zombie's execution is the same length
    #    everywhere and `a_work_s` stays comparable between arms.
    time.sleep(silence_s)
    after_silence = h.server_times(run_id)

    # 4. The live claimer offers to take the run. Everywhere except `none` the
    #    reaper has put it back to PENDING by now, so the claim succeeds and BUMPS
    #    the attempt — that bump is the fence. In `none` nothing was requeued, so
    #    nothing is handed out and the run stays stuck on the machine we think is
    #    dead.
    #
    #    In `takeover` the claimer is a different machine (node B). In `reclaim` it
    #    is node A itself, coming back: it now owns the run again, at attempt 2,
    #    while its previous execution is still out there about to report.
    assignment_b = _claim(live_node, run_id, tries=2)
    attempt_b = assignment_b["attempt"] if assignment_b else None
    b_start = None
    if assignment_b is not None:
        b_start = _db_now()
        live_running = _post_status(live_node, run_id, attempt_b, "RUNNING")
        if live_running.status_code != 200:
            raise h.HarnessError(
                f"live claimer could not start attempt {attempt_b}: {live_running.status_code}"
            )

    # The live attempt does some work. We sleep this even when B was handed
    # nothing, because the zombie's own execution keeps running regardless of what
    # the platform decided — otherwise the `none` arm's zombie would look like it
    # had worked for less time than everybody else's.
    time.sleep(work_s)

    # 5. The zombie wakes up. Read the run FIRST: whether this post is superseded
    #    is a fact about the run at this instant, and after the post it is too late
    #    to ask.
    before_zombie = h.server_times(run_id)
    zombie_result = "SUCCEEDED" if rep % 2 else "FAILED"
    zombie_exit = 0 if zombie_result == "SUCCEEDED" else FAILED_EXIT
    a_end = _db_now()
    zombie_resp = _post_status(node_a, run_id, attempt_a, zombie_result, zombie_exit)
    after_zombie = h.server_times(run_id)

    # 6. The live attempt finishes, after the zombie — the chaos-test ordering.
    live_resp = None
    b_end = None
    if assignment_b is not None:
        b_end = _db_now()
        live_resp = _post_status(live_node, run_id, attempt_b, LIVE_RESULT, LIVE_EXIT)
    final = h.server_times(run_id)

    # --- what happened, in facts ------------------------------------------
    #
    # WRONGLY-ACCEPTED RESULT — the headline, and it needs a precise definition.
    # The platform keeps a terminal result final in EVERY arm, so the damage here
    # is never "two rows in the database". It is that the result the platform kept
    # came from an execution it had already given up on. So:
    #
    #   accepted_stale = the report was sent by a node that no longer owned the run,
    #                    or under an attempt number the run had already moved past,
    #                    AND the control plane accepted it (no 409).
    #
    # In the `none` arm the platform never supersedes anything — no sweep, no
    # re-claim — so nothing can be stale there and this counts 0 by construction.
    # `none`'s damage shows up as `never_recovered` instead. Reading one metric
    # without the other would flatter that arm.
    zombie_wrong_node = int(before_zombie["node_id"] != node_a["node_id"])
    zombie_stale_attempt = int(before_zombie["attempt"] != attempt_a)
    zombie_superseded = bool(zombie_wrong_node or zombie_stale_attempt)
    zombie_accepted = zombie_resp.status_code == 200
    accepted_stale = int(zombie_accepted and zombie_superseded)

    # Accepted is not the same as harmful: a stale report that lands on an already
    # terminal run changes nothing. This says the stale report is the one that set
    # the stored result.
    zombie_set_result = (
        before_zombie["status"] not in TERMINAL
        and after_zombie["status"] == zombie_result
    )
    stale_became_final = int(accepted_stale and zombie_set_result)

    # Who actually produced the result the database now holds? "A" is the dead
    # execution, "B" the live one — in `reclaim` both run on the same machine, which
    # is exactly why the node id cannot answer this and the attempt number must.
    if zombie_set_result:
        setter, setter_node_id = "A", node_a["node_id"]
    elif assignment_b is not None and final["status"] in TERMINAL:
        setter, setter_node_id = "B", live_node["node_id"]
    else:
        setter, setter_node_id = None, None

    # FINAL STATUS CORRECTNESS. The state the work actually reached is the outcome
    # of the execution the platform sanctioned — node B's, when there was one. When
    # the platform never re-dispatched, node A's execution is the only one there
    # was, so its outcome is the truth.
    truth = LIVE_RESULT if assignment_b is not None else zombie_result
    final_terminal = int(final["status"] in TERMINAL)
    final_status_wrong = int(bool(final_terminal) and final["status"] != truth)

    # WASTED NODE-SECONDS — execution time spent on attempts whose result was not
    # the one the platform kept. Read it together with `a_work_s` and `b_work_s`,
    # which are reported separately on purpose: node A's execution length is set by
    # the lease-expiry wait (the same in every arm) and node B's by --work-s, so
    # the seconds here come from the shape of the scenario, not from the platform.
    # `wasted_attempts` is the construction-independent version of the same fact.
    a_work_s = _gap(h.parse_ts(before_zombie["started_at"]), a_end)
    b_work_s = _gap(b_start, b_end)
    wasted_attempts, wasted_node_s = 0, 0.0
    for who, seconds in (("A", a_work_s), ("B", b_work_s)):
        if seconds is None or (who == "B" and assignment_b is None):
            continue
        if who == setter:
            continue
        wasted_attempts += 1
        wasted_node_s += seconds

    metrics = {
        # the headline pair
        "accepted_stale": accepted_stale,
        "final_status_wrong": final_status_wrong,
        # the other two damages
        "never_recovered": int(assignment_b is None),
        "wasted_attempts": wasted_attempts,
        "wasted_node_s": round(wasted_node_s, 3),
        # who the stored row credits. In an arm with no fence the accepted result
        # can come from node A while the row still names node B as the owner.
        "result_misattributed": int(
            setter is not None and final["node_id"] != setter_node_id
        ),
        "stale_became_final": stale_became_final,
        # Would a plain "is this still your run?" check have caught it too?
        # 1 = the zombie still owned the run, so ONLY the attempt number could tell
        # the dead execution from the live one. This is the number that decides
        # whether the fencing token earns its place: in `reclaim` it should be 1,
        # and every wrongly-accepted result recorded there is one an ownership
        # check would have let through.
        "zombie_still_owned_run": int(not zombie_wrong_node),
        "zombie_wrong_node": zombie_wrong_node,
        "zombie_stale_attempt": zombie_stale_attempt,
        # 1 = only the epoch could have rejected this report (right node, wrong
        # attempt). The token-earning case, counted directly rather than inferred.
        "token_only_case": int(zombie_stale_attempt and not zombie_wrong_node),
        # the server's own corroboration of accepted_stale
        "server_unfenced_lines": _unfenced_audit_lines(run_id),
        "reaper_lost_lines": _lost_lines(run_id),
        # the scenario as it ran
        "zombie_result": zombie_result,
        "zombie_http": zombie_resp.status_code,
        "live_http": live_resp.status_code if live_resp is not None else None,
        "requeued_after_silence": int(after_silence["status"] == "PENDING"),
        "attempt_after_silence": after_silence["attempt"],
        "attempt_a": attempt_a,
        "attempt_b": attempt_b,
        "result_setter": setter,
        "final_status": final["status"],
        "final_terminal": final_terminal,
        "final_attempt": final["attempt"],
        "final_exit_code": final["exit_code"],
        "a_work_s": a_work_s,
        "b_work_s": b_work_s,
        # the conditions this repetition ran under
        "scenario": scenario,
        "guarantee": arm,
        "silence_s": silence_s,
        "work_s": work_s,
        "lease_ttl_s": lease_ttl_s,
    }

    # Re-check the mode BEFORE recording. This matters more in E1 than anywhere
    # else: `set_mode` restarts the control plane between arms, and a repetition
    # measured under a different arm than the one it is labelled with would not be a
    # weak result, it would be a fabricated one.
    h.assert_stable(where=f"{arm}/{scenario} rep {rep}")
    # `summarize` and `chart` group by `arm` alone, so the label carries both the
    # guarantee and the scenario. Both also travel as their own fields above, so
    # raw.jsonl can be re-grouped either way without re-measuring.
    return h.record(EXP, f"{arm}/{scenario}", rep, metrics, dry=dry)


# --- the run ----------------------------------------------------------------


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="E1 — is the fencing token really needed?")
    parser.add_argument("--arms", nargs="+", choices=list(ARMS), default=list(ARMS),
                        help="which guarantee arms to run, in order (default: all four)")
    parser.add_argument("--scenarios", nargs="+", choices=list(SCENARIOS),
                        default=list(SCENARIOS),
                        help="takeover = a different node finishes the run; reclaim = "
                             "the same node comes back and re-claims its own run, so "
                             "only the attempt number can reject its old execution "
                             "(default: both)")
    parser.add_argument("--reps", type=int, default=DEFAULT_REPS,
                        help=f"repetitions per arm and scenario (default {DEFAULT_REPS})")
    parser.add_argument("--dry", action="store_true",
                        help=f"rehearsal: {DRY_REPS} reps per arm/scenario, rows tagged dry")
    parser.add_argument("--work-s", type=float, default=3.0,
                        help="how long the live attempt executes, seconds")
    parser.add_argument("--grace-s", type=float, default=8.0,
                        help="margin added to the lease TTL before we look. Must be larger "
                             "than REAPER_INTERVAL_S (3s by default) or the sweep may not "
                             "have run yet")
    parser.add_argument("--machine-idle", action=argparse.BooleanOptionalAction, default=None,
                        help="was the machine otherwise idle? recorded in the manifest")
    parser.add_argument("--mains-power", action=argparse.BooleanOptionalAction, default=None,
                        help="was the machine on mains power? recorded in the manifest")
    parser.add_argument("--fresh", action="store_true",
                        help="archive an existing raw.jsonl and start clean; without "
                             "it a run that would append onto measured rows stops "
                             "instead (the old file is renamed, never deleted)")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    reps = DRY_REPS if args.dry else args.reps

    h.require_ready()
    if not args.dry:
        h.guard_fresh(EXP, args.fresh)
    h.ensure_image()

    # The lease the CONTROL PLANE is actually running with, asked of the running
    # container by the harness's own reader — so a stack configured with a short
    # lease is honoured automatically instead of assumed away.
    lease_ttl_s = h._lease_ttl_s()
    silence_s = lease_ttl_s + args.grace_s
    per_rep = silence_s + args.work_s + 5          # + the requests and the resets
    total = len(args.arms) * len(args.scenarios) * reps
    print(f"[e1] lease_ttl={lease_ttl_s}s -> silence window {silence_s:.0f}s per repetition")
    print(f"[e1] {len(args.arms)} arm(s) x {len(args.scenarios)} scenario(s) x {reps} "
          f"rep(s) = {total} repetitions ~= {total * per_rep / 60:.0f} min"
          + ("   (DRY — nothing here reaches a summary)" if args.dry else ""))

    modes_used: list[dict] = []
    try:
        for arm in args.arms:
            # set_mode restarts the control plane and reads /health back to prove
            # the arm really took. Nothing is measured on an unverified mode. The
            # scenario loop sits INSIDE this one so a single restart serves both —
            # the scenario is a property of the driver, not of the server.
            modes_used.append(h.set_mode(guarantee=arm))
            for scenario in args.scenarios:
                print(f"\n--- arm {arm} / {scenario} — {reps} repetition(s) ---")
                for rep in range(1, reps + 1):
                    row = one_rep(
                        arm, rep, scenario=scenario,
                        silence_s=silence_s, work_s=args.work_s,
                        lease_ttl_s=lease_ttl_s, dry=args.dry,
                    )
                    print(
                        f"    rep {rep:>2}: zombie {row['zombie_result']:<9} -> HTTP "
                        f"{row['zombie_http']} | accepted_stale={row['accepted_stale']} "
                        f"token_only={row['token_only_case']} "
                        f"final={row['final_status']} (wrong={row['final_status_wrong']}) "
                        f"never_recovered={row['never_recovered']} "
                        f"wasted={row['wasted_attempts']} attempt(s)"
                    )
    finally:
        # Always, even after a crash: the platform must never be left with one of
        # its own guarantees switched off.
        back = h.set_mode()
        print(f"\n[e1] restored the shipped defaults: {back}")

    h.summarize(
        EXP,
        metrics=[
            "accepted_stale", "final_status_wrong", "never_recovered",
            "wasted_attempts", "wasted_node_s", "result_misattributed",
            "stale_became_final", "token_only_case", "zombie_still_owned_run",
            "zombie_wrong_node", "zombie_stale_attempt", "server_unfenced_lines",
            "reaper_lost_lines", "requeued_after_silence", "a_work_s", "b_work_s",
        ],
        title="E1 — fencing: none vs lease_only vs owner_only vs full, "
              "over two scenarios",
    )
    h.chart(
        EXP, metric="accepted_stale",
        title="E1 — results accepted from a superseded execution (1 = yes)",
    )
    h.manifest(
        EXP,
        reps=reps,
        modes=modes_used,
        machine_idle=args.machine_idle,
        mains_power=args.mains_power,
        notes=(
            "Chaos-test zombie scenario driven over the real HTTP API, four guarantee "
            "arms x two scenarios. Nodes are tokens from POST /agent/register, no agent "
            "process and no container; the execution is simulated exactly as in "
            "scripts/chaos_test.py. The lease is expired by waiting out the real "
            "lease_ttl_s, the same full window in every arm. The zombie posts BEFORE the "
            "live attempt finishes; if it posted after, the terminal-result guard alone "
            "would refuse it in every arm. "
            "SCENARIOS: 'takeover' hands the run to a DIFFERENT node, so the zombie is "
            "both wrong-node and stale-attempt and an ownership check alone catches it — "
            "owner_only and full are expected to agree here. 'reclaim' has ONE node in "
            "the pool: it goes silent, the reaper requeues, the same node re-claims its "
            "own run at a higher attempt, and its old execution then reports. Ownership "
            "passes there, so only the attempt number can reject it — see "
            "token_only_case and zombie_still_owned_run. That scenario is the one that "
            "decides whether the fencing TOKEN earns its place on top of an ownership "
            "check, rather than the experiment proving only that some check is needed."
        ),
        extra={
            "scenarios_run": args.scenarios,
            "arms_run": args.arms,
            "scenario": {
                "silence_window_s": silence_s,
                "live_work_s": args.work_s,
                "grace_s": args.grace_s,
                "live_result": LIVE_RESULT,
                "zombie_results": "alternated per repetition: odd SUCCEEDED, even FAILED",
                "zombie_exit_code_on_failure": FAILED_EXIT,
            },
        },
    )
    measured = len(h.load_raw(EXP))
    total = len(h.load_raw(EXP, include_dry=True))
    print(f"[e1] {measured} measured row(s), {total - measured} dry row(s) excluded")
    print(f"  {h.exp_dir(EXP).relative_to(h.REPO)}: raw.jsonl, summary.md, chart.png, manifest.json")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (h.HarnessError, AssertionError) as exc:
        print(f"\nE1 FAILED: {exc}", file=sys.stderr)
        sys.exit(1)
