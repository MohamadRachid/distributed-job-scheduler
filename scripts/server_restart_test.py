"""What a control plane restart does to a run in flight.

    "You call it a distributed scheduler and it is one process on one laptop;
     what is distributed?"

The honest answer names what we survive (a worker dying) and what we do not: the
control plane is a single point of failure, published as NFR-9 **Not met**. This
script turns that admission into a measured statement of exactly what happens, so the
answer stops being "it would probably recover" and becomes an artefact.

TWO SCENARIOS, and the difference between them is the whole point:

  A  short outage (~20s)  - the lease has NOT expired when the server returns.
                            Expect: no LOST, attempt stays 1, renewals resume.
  B  long outage  (~90s)  - the lease HAS expired, but NOTHING SWEPT while the
                            control plane was down, because the reaper is a
                            background task of that same process. Lease expiry is an
                            absolute timestamp, not a timer, so the outage still
                            consumes it. Expect: LOST on the FIRST SWEEP AFTER THE
                            RESTART -- not 60s after it -- then requeue, attempt 2,
                            and the original agent fenced 409 on its next post.

WHAT THIS IS NOT. One host, one control plane, one observation per scenario. It
measures what happens on a restart, not how long a restart takes in general, and it is
not an availability measurement. NFR-9 stays Not met: nothing here gives the platform a
second control plane, and resuming cleanly is a different claim from surviving the loss
of the machine the control plane runs on.

The prediction was written and dated BEFORE either scenario ran:
docs/evidence/predictions/server_restart_2026-08-30.md

Helpers are IMPORTED from scripts/chaos_partition.py rather than copied. Two copies of
a login, a TLS context or an agent launcher drift, and only one of them gets tested --
the reasoning that fixed scripts/seed_job.py on 25 August (section 18 2026-08-25i).

KNOWN GAP, named rather than left to be rediscovered: this script stops its agents in
its finally block but does NOT sweep leftover job containers, which scripts/
chaos_partition.py does. An agent terminated while a container is still exiting leaves
that container behind (section 18 2026-07-31e -- the platform recovers the RUN, nothing
removes the leftover container). Scenario B's first run left one. Remove them with
`docker ps -aq --filter name=fyp-run- | xargs docker rm` after a run, or add the sweep
and prove it in both directions before relying on it.

Run with the stack up, the workload image built, and no other worker online:

    .venv/Scripts/python.exe scripts/server_restart_test.py --scenario A
    .venv/Scripts/python.exe scripts/server_restart_test.py --scenario B
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
from datetime import datetime, timezone
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "scripts"))

import chaos_partition as cp  # noqa: E402  the proven helpers, imported not copied
from agent.tls import context_for, read_ca_pem  # noqa: E402

TERMINAL = {"SUCCEEDED", "FAILED"}
_T0 = time.monotonic()
_TRANSCRIPT: list[str] = []


def say(msg: str) -> None:
    """Printed and kept. A timeline recorded as it happens is worth more than one
    reconstructed afterwards."""
    line = f"[{time.monotonic() - _T0:7.1f}s] {msg}"
    _TRANSCRIPT.append(line)
    print(line, flush=True)


def die(msg: str) -> str:
    say(f"STOPPED: {msg}")
    raise SystemExit(2)


def server_up(server: str, timeout: float = 3.0) -> bool:
    """Is the control plane answering? Used to time the resume, so it asks the
    cheapest unauthenticated endpoint and treats any error as 'not yet'."""
    try:
        cp._get(f"{server}/health", None, timeout=timeout)
        return True
    except Exception:  # noqa: BLE001 - every failure means the same thing here
        return False


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--scenario", choices=["A", "B"], required=True)
    ap.add_argument("--server", default=os.environ.get("SERVER_URL", "https://localhost:8000"))
    ap.add_argument("--ca-cert", default=str(_ROOT / "certs" / "ca.pem"))
    ap.add_argument("--outage-s", type=float, default=None,
                    help="override the outage length (default: 20 for A, 90 for B)")
    ap.add_argument("--epochs", type=int, default=60)
    ap.add_argument("--epoch-seconds", type=float, default=4.0)
    # Long enough for a FULL second attempt after the outage, not just for the
    # requeue. The first scenario-B run closed its window with the run at epoch
    # 60 of 60 and progress 1.0 -- the workload had finished and only the terminal
    # status post had not landed, so a sound recovery was recorded as a FAIL by the
    # harness rather than by the platform. The window must exceed the outage plus a
    # whole attempt.
    ap.add_argument("--observe-s", type=float, default=420.0)
    ap.add_argument("--python", default=str(_ROOT / ".venv" / "Scripts" / "python.exe"))
    ap.add_argument("--out", default=None)
    ap.add_argument("--username", default=cp.DEFAULT_USERNAME)
    ap.add_argument("--password", default=cp.DEFAULT_PASSWORD)
    args = ap.parse_args(argv)

    outage_s = args.outage_s if args.outage_s is not None else (20.0 if args.scenario == "A" else 90.0)
    out_path = Path(args.out) if args.out else (
        _ROOT / "docs" / "evidence" / f"server_restart_{args.scenario}_2026-08-30.txt")
    started_at = datetime.now(timezone.utc)
    findings: dict = {"scenario": args.scenario, "outage_requested_s": outage_s}

    # One TLS context for this script's own calls. chaos_partition builds it inside
    # its main(), which we are not calling, so it is set here explicitly rather than
    # left as None -- None would mean stdlib's default trust store and every call
    # against our own authority would fail.
    ca = args.ca_cert if args.server.startswith("https") else None
    cp._CTX = context_for(read_ca_pem(ca)) if ca else None

    a1 = a2 = None
    log_dir = _ROOT / "docs" / "evidence" / "server_restart_logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    cp._archive_existing_logs(log_dir)   # rename, never delete (section 18 2026-08-29r)

    try:
        say("=" * 70)
        say(f"SERVER RESTART TEST - scenario {args.scenario}, outage about {outage_s:.0f}s")
        say("=" * 70)

        if not server_up(args.server):
            die(f"the control plane is not answering at {args.server}. Bring the stack up first.")
        token = cp.login(args.server, args.username, args.password)
        say("  logged in")

        for n in cp._get(f"{args.server}/nodes", token):
            if n["online"]:
                die(f"node {n['name']!r} is already online. Run this with no other worker up.")
        code, out = cp.docker("images", "--format", "{{.Repository}}:{{.Tag}}")
        if "fyp-dummy:latest" not in out:
            die("workload image fyp-dummy:latest is missing. Build it first.")

        lease = cp._get(f"{args.server}/health")
        say(f"  control plane health: {json.dumps(lease)[:120]}")

        # --- two agents, so a requeued run has somewhere else to go ----------
        a1 = cp.AgentProc("restart-a", args.server, ca, log_dir / "agent1.log",
                          args.python, log_dir / "state-a.json")
        a2 = cp.AgentProc("restart-b", args.server, ca, log_dir / "agent2.log",
                          args.python, log_dir / "state-b.json")
        n1 = cp.wait_for_node(args.server, token, "restart-a", 60)
        n2 = cp.wait_for_node(args.server, token, "restart-b", 60)
        if not n1 or not n2:
            die("the two agents did not both come online")
        nodes = {n1["node_id"]: "restart-a", n2["node_id"]: "restart-b"}
        say(f"  agents online: restart-a={n1['node_id'][:8]} restart-b={n2['node_id'][:8]}")

        # UNTARGETED on purpose. A targeted run is pinned one-per-selected-node, so
        # the surviving node may never take a requeued sibling -- the trap found by
        # rehearsing the demo runbook (section 18 2026-08-15s).
        job = cp._post(f"{args.server}/jobs", {
            "name": f"server-restart-{args.scenario}",
            "image": "fyp-dummy:latest",
            "entrypoint": ["python", "train.py"],
            "env": {"EPOCHS": str(args.epochs), "EPOCH_SECONDS": str(args.epoch_seconds)},
            "replicas": 1,
        }, token)
        job_id, run_id = job["job_id"], job["run_ids"][0]
        say(f"JOB submitted: run {run_id[:8]}, about {args.epochs * args.epoch_seconds:.0f}s of work")

        end = time.monotonic() + 90
        row: dict = {}
        while time.monotonic() < end:
            row = cp.run_row(args.server, token, job_id)
            if row.get("status") == "RUNNING":
                break
            time.sleep(1)
        if row.get("status") != "RUNNING":
            die(f"the run never reached RUNNING (last: {row.get('status')})")
        owner_before = nodes.get(row.get("node_id"), "?")
        findings["owner_before"] = owner_before
        findings["attempt_before"] = row.get("attempt")
        say(f"  {cp.describe(row, nodes)}")
        say(f"  the run is on {owner_before}; it is the machine whose work is at risk")

        # --- THE OUTAGE ------------------------------------------------------
        say("=" * 70)
        say(f"STOPPING THE CONTROL PLANE for about {outage_s:.0f}s.")
        say("  Both agents stay up. Their containers stay up. Only the server goes.")
        say("=" * 70)
        stop_at = time.monotonic()
        code, _ = cp.docker("compose", "stop", "control-plane", timeout=90)
        if code != 0:
            die("could not stop the control-plane container")
        down_at = time.monotonic()
        findings["seconds_to_stop"] = round(down_at - stop_at, 1)
        say(f"  control plane stopped ({down_at - stop_at:.1f}s to stop)")

        while time.monotonic() - down_at < outage_s:
            time.sleep(2)
        findings["outage_actual_s"] = round(time.monotonic() - down_at, 1)

        say(f"RESTARTING after {findings['outage_actual_s']:.0f}s of outage.")
        start_cmd_at = time.monotonic()
        code, _ = cp.docker("compose", "start", "control-plane", timeout=120)
        if code != 0:
            die("could not start the control-plane container")
        # Seconds to resume: from the start command to the API answering again.
        resumed_at = None
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            if server_up(args.server, timeout=2.0):
                resumed_at = time.monotonic()
                break
            time.sleep(0.5)
        if resumed_at is None:
            die("the control plane did not answer within 120s of being started")
        findings["seconds_to_resume"] = round(resumed_at - start_cmd_at, 1)
        say("=" * 70)
        say(f"CONTROL PLANE IS BACK. {findings['seconds_to_resume']:.1f}s from the start "
            f"command to /health answering.")
        say("  ONE OBSERVATION. Mechanism: alembic upgrade head (a no-op here) then "
            "uvicorn binds. Not a benchmark.")
        say("=" * 70)

        # the token was issued by the process that just died; get a fresh one
        token = cp.login(args.server, args.username, args.password)

        # --- watch what the platform does ------------------------------------
        seen: list[str] = []
        lost_seen = False
        attempt2_at = None
        watch_end = time.monotonic() + args.observe_s
        last_desc = ""
        while time.monotonic() < watch_end:
            try:
                row = cp.run_row(args.server, token, job_id)
            except urllib.error.HTTPError as exc:
                if exc.code == 401:
                    token = cp.login(args.server, args.username, args.password)
                    continue
                raise
            except Exception:  # noqa: BLE001
                time.sleep(1)
                continue
            desc = cp.describe(row, nodes)
            if desc != last_desc:
                say(f"  {desc}")
                last_desc = desc
                seen.append(row.get("status"))
            if row.get("failure_reason") in {"NODE_LOST", "RUN_LOST"}:
                lost_seen = True
            if row.get("attempt") == 2 and attempt2_at is None:
                attempt2_at = time.monotonic()
                findings["seconds_resume_to_attempt2"] = round(attempt2_at - resumed_at, 1)
                say(f"  attempt 2 appeared {attempt2_at - resumed_at:.1f}s after the "
                    f"server answered")
            if row.get("status") in TERMINAL:
                break
            time.sleep(1)

        findings["final_row"] = row
        findings["statuses_seen"] = seen
        findings["owner_after"] = nodes.get(row.get("node_id"), row.get("node_id") or "-")

        # The reaper's own lines, from the server's log rather than inferred.
        code, cp_log = cp.docker("compose", "logs", "--no-color", "--tail", "4000",
                                 "control-plane", timeout=60)
        log_lines = cp_log.splitlines()
        lost_lines = [ln.strip() for ln in log_lines
                      if "LOST run=" in ln and run_id[:8] in ln]
        # SCOPED TO THIS RUN, and that is not fussiness. The reaper's "reclaimed"
        # line names no run, so an unscoped grep matches a line left by an EARLIER
        # test and the check passes for the wrong reason -- which is exactly what
        # scenario A's first capture showed (its one reclaimed line belonged to the
        # partition test run 34d35ba3). Only lines AFTER this run's own LOST line
        # count, because the reaper emits them in that order within one sweep.
        first_lost_at = next((i for i, ln in enumerate(log_lines)
                              if "LOST run=" in ln and run_id[:8] in ln), None)
        reclaimed = ([ln.strip() for ln in log_lines[first_lost_at:] if "reclaimed" in ln]
                     if first_lost_at is not None else [])
        findings["reaper_lost_lines"] = lost_lines
        findings["reaper_reclaimed_lines"] = reclaimed[-5:]
        findings["reaper_lost_count"] = len(lost_lines)
        for ln in lost_lines:
            say("  reaper: " + ln[-140:])
        for ln in reclaimed[-3:]:
            say("  reaper: " + ln[-140:])

        # What each agent did. The fence stops the agent; the server never does.
        t1, t2 = a1.log_text(), a2.log_text()
        findings["agent1_fenced"] = "fenced by control plane" in t1
        findings["agent2_fenced"] = "fenced by control plane" in t2
        findings["agent1_heartbeat_failures"] = t1.count("heartbeat failed")
        findings["agent2_heartbeat_failures"] = t2.count("heartbeat failed")
        findings["agent1_alive"] = a1.alive()
        findings["agent2_alive"] = a2.alive()
        say(f"  agent restart-a: {findings['agent1_heartbeat_failures']} failed heartbeats, "
            f"fenced={findings['agent1_fenced']}, alive={findings['agent1_alive']}")
        say(f"  agent restart-b: {findings['agent2_heartbeat_failures']} failed heartbeats, "
            f"fenced={findings['agent2_fenced']}, alive={findings['agent2_alive']}")

        # --- the assertions ---------------------------------------------------
        if args.scenario == "A":
            checks = [
                ("both agents survived the outage", a1.alive() and a2.alive()),
                ("the agent kept retrying rather than giving up",
                 findings["agent1_heartbeat_failures"] > 0),
                ("NO reaper LOST line for this run", len(lost_lines) == 0),
                ("the run never left attempt 1", row.get("attempt") == 1),
                ("the run never went LOST", not lost_seen),
                ("the run finished SUCCEEDED", row.get("status") == "SUCCEEDED"),
                ("it finished on the machine it started on",
                 findings["owner_after"] == owner_before),
                ("neither agent was fenced",
                 not findings["agent1_fenced"] and not findings["agent2_fenced"]),
            ]
        else:
            checks = [
                ("both agents survived the outage", a1.alive() and a2.alive()),
                ("the agent kept retrying rather than giving up",
                 findings["agent1_heartbeat_failures"] > 0),
                ("the reaper logged exactly one LOST for this run", len(lost_lines) == 1),
                ("the reaper's reclaimed line is visible", len(reclaimed) > 0),
                ("the run was requeued, not failed-exhausted",
                 "requeued" in " ".join(lost_lines)),
                ("the run reached attempt 2", row.get("attempt") == 2),
                ("the run finished SUCCEEDED", row.get("status") == "SUCCEEDED"),
                ("the superseded execution was fenced, not accepted",
                 findings["agent1_fenced"] or findings["agent2_fenced"]),
            ]

        say("-" * 70)
        for label, ok in checks:
            say(f"  {'PASS' if ok else 'FAIL'}  {label}")
        findings["checks"] = {label: ok for label, ok in checks}
        passed = all(ok for _, ok in checks)
        findings["verdict"] = "PASS" if passed else "FAIL"
        say("-" * 70)
        say(f"VERDICT: {findings['verdict']}")
        return 0 if passed else 1

    except SystemExit:
        raise
    except Exception as exc:  # noqa: BLE001
        say(f"ERROR: {type(exc).__name__}: {exc}")
        findings["verdict"] = "ERROR"
        return 3
    finally:
        for a in (a1, a2):
            if a is not None:
                a.stop()
        say("agents stopped")
        # make sure we never leave the stack down for the next session
        cp.docker("compose", "start", "control-plane", timeout=120)
        write_results(out_path, started_at, findings, args, outage_s)


def write_results(path: Path, started_at: datetime, findings: dict, args, outage_s: float) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    scen = findings.get("scenario", "?")
    lines = [
        f"Server restart test - scenario {scen} - {started_at.date().isoformat()}",
        "=" * 70,
        "",
        "The control plane is a single point of",
        "failure (NFR-9, published Not met). This measures exactly what that costs a run",
        "in flight, so the answer is an artefact rather than an assurance.",
        "",
        f"Scenario {scen}: the control plane was stopped for about {outage_s:.0f}s with a",
        "run in flight, then started again. Both agents stayed up throughout.",
        "",
        "PREDICTION, written and dated BEFORE this run:",
        "  docs/evidence/predictions/server_restart_2026-08-30.md",
        "",
        f"VERDICT: {findings.get('verdict')}",
        "",
        "-" * 70,
        "TIMELINE",
        "-" * 70,
    ]
    lines += ["  " + ln for ln in _TRANSCRIPT]
    lines += [
        "",
        "-" * 70,
        "FINDINGS",
        "-" * 70,
        json.dumps({k: v for k, v in findings.items() if k != "checks"},
                   indent=2, default=str),
        "",
        "CHECKS",
        json.dumps(findings.get("checks", {}), indent=2),
        "",
        "-" * 70,
        "WHAT THIS DOES NOT ESTABLISH",
        "-" * 70,
        "One host, one control plane, ONE observation. The seconds-to-resume figure is a",
        "single reading with its mechanism (alembic upgrade head, a no-op here, then",
        "uvicorn binding); it is not a benchmark and has no spread. NFR-9 stays Not met:",
        "nothing here gives the platform a second control plane, and resuming cleanly is a",
        "different claim from surviving the loss of the machine the control plane runs on.",
        "",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8", newline="\n")
    print(f"\nwritten to {path}")


if __name__ == "__main__":
    raise SystemExit(main())
