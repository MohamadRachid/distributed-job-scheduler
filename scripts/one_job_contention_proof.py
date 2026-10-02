"""One job, N replicas, N machines, N served -- before and after, on one stack.

The regression this proves gone, in one line: the look-wide-lock-narrow change of
2026-09-06 took the MANY-JOBS case from 2 of 6 to 6 of 6 and the ONE-JOB case from
30 of 30 to 7 of 30, and FR-5 -- "the scheduler expands a job into runs and
dispatches them to eligible nodes in parallel" -- is the one-job case.

Every part is measured twice on the same stack minutes apart: once with
`control-plane/app/scheduler.py` restored byte for byte from the pre-fix commit
through the compose bind mount, once with today's file, each verified by hash
before and after. That is the only way to say the difference is the code and not
the machine, and it is the method the starvation proof used.

What this file does NOT do is publish a timing. It counts machines served, replicas
handed out and runs handed to more than one claimer -- shape, not speed -- so its
answer does not depend on the machine being fit to measure a millisecond on.
"""

import json
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone

sys.path.insert(0, "scripts")
sys.path.insert(0, "scripts/experiments")

import claim_contention as cc   # noqa: E402  register_claimers/fire_round/seed_queue
import harness as h             # noqa: E402  reset_platform/psql_exec/session
import targeted_starvation_proof as tsp  # noqa: E402  call/psql/register/heartbeat

SCHEDULER = "control-plane/app/scheduler.py"
BEFORE_REF = "HEAD"          # the tree that still has the one-job collapse
OUT = "docs/evidence/one_job_contention_2026-09-06.txt"

_lines: list[str] = []


def say(msg: str = "") -> None:
    print(msg, flush=True)
    _lines.append(msg)


def stamp() -> str:
    return datetime.now(timezone.utc).strftime("%H:%M:%S")


def sh(*args: str) -> str:
    return subprocess.run(args, capture_output=True, text=True).stdout.strip()


def sha(path: str) -> str:
    import hashlib
    with open(path, "rb") as fh:
        return hashlib.sha256(fh.read()).hexdigest()[:16]


def restart_control_plane() -> str:
    subprocess.run(["docker", "compose", "restart", "control-plane"],
                   capture_output=True, text=True)
    for _ in range(30):
        try:
            _st, body = tsp.call("/health", timeout=10)
            if body.get("status") == "ok":
                return body.get("mode_epoch", "?")
        except Exception:
            pass
        time.sleep(2)
    raise SystemExit("control plane did not come back")


def seed_multi_job(runs: int) -> None:
    """`runs` PENDING runs, each its own job -- the case the suite already covers."""
    for g in range(1, runs + 1):
        job_id = str(uuid.uuid4())
        h.psql_exec(
            "INSERT INTO jobs (id, name, image, entrypoint, env, resource_reqs, "
            "target_node_ids, replicas, status, created_at, private) VALUES ("
            f"'{job_id}', 'six jobs', 'fyp-dummy:latest', "
            "json_build_array('python', 'train.py'), '{}'::json, '{}'::json, NULL, "
            "1, 'PENDING', now(), false);"
        )
        h.psql_exec(
            "INSERT INTO runs (id, job_id, node_id, status, attempt, "
            "lease_expires_at, retries_remaining, exit_code, started_at, "
            "finished_at, created_at, escalation_count) VALUES ("
            f"'mj000000-0000-4000-8000-' || lpad('{g}', 12, '0'), '{job_id}', NULL, "
            "'PENDING', 0, NULL, 1, NULL, NULL, NULL, "
            f"now() - interval '1 hour' + ({g} * interval '1 millisecond'), 0);"
        )


def one_round(shape: str, claimers_n: int, runs: int) -> dict:
    """One warm-up against an EMPTY queue, then ONE measured simultaneous release.

    The warm-up is against an empty queue on purpose, exactly as the campaign's
    `run_once` does it: warming up against a full one would spend the claimers'
    capacity before the release being measured."""
    h.reset_platform()
    claimers = cc.register_claimers(claimers_n, capacity=1)
    sessions = [h.session() for _ in claimers]
    cc.fire_round(claimers, sessions)                     # warm-up, discarded
    if shape == "one job":
        cc.seed_queue(runs)
    else:
        seed_multi_job(runs)
    result = cc.fire_round(claimers, sessions)            # the measured release
    for s in sessions:
        s.close()
    ids: list[str] = []
    served = 0
    for r in result:
        ids.extend(r["run_ids"])
        if r["run_ids"]:
            served += 1
    doubles = len(ids) - len(set(ids))
    return {"shape": shape, "claimers": claimers_n, "queue": runs,
            "served": served, "empty_handed": claimers_n - served,
            "distinct": len(set(ids)), "double_claims": doubles}


def spread_case(machines: int, replicas: int) -> list[dict]:
    """Fewer replicas than machines, machines asking one after another. Records
    which machine took which replica, so 'one per machine' is readable rather than
    asserted."""
    h.reset_platform()
    nodes = cc.register_claimers(machines, capacity=4)
    cc.seed_queue(replicas)
    sessions = [h.session() for _ in nodes]
    for claimer, sess in zip(nodes, sessions):
        sess.post(
            f"{h.API}/agent/heartbeat",
            json={"node_id": claimer["node_id"], "status": "idle", "running": []},
            headers={"Authorization": f"Bearer {claimer['token']}"},
            timeout=60,
        )
    for s in sessions:
        s.close()
    rows = json.loads(tsp.psql(
        # The LAST segment of the id, not the first: the seeded ids share the prefix
        # 'cc000000-0000-4000-8000-' by construction, so a leading substring names
        # every replica identically and the table below would say nothing.
        "select json_agg(json_build_object('run', right(r.id, 6), "
        "'node', substring(n.name,1,12), 'status', r.status::text)) "
        "from runs r left join nodes n on n.id = r.node_id"
    ) or "[]")
    return rows or []


def arm(label: str) -> dict:
    say()
    say("-" * 78)
    say(f"  {label}")
    say("-" * 78)
    out = {}
    for shape, n, queue in (("one job", 10, 40), ("one job", 30, 120),
                            ("many jobs", 6, 6)):
        row = one_round(shape, n, queue)
        key = f"{shape}@{n}"
        out[key] = row
        say(f"[{stamp()}] {shape:>10}, {queue:>3} pending, {n:>2} claimers at one "
            f"instant: {row['served']:>2} of {n} served, "
            f"{row['empty_handed']:>2} empty-handed, "
            f"{row['distinct']:>2} distinct runs, "
            f"{row['double_claims']} handed to two claimers")
    rows = spread_case(machines=6, replicas=3)
    out["spread"] = rows
    assigned = [r for r in rows if r["node"]]
    say(f"[{stamp()}] 3 replicas, 6 machines of capacity 4, asking one after "
        f"another:")
    for r in sorted(rows, key=lambda x: x["run"]):
        say(f"             run {r['run']} -> {r['node'] or '(nobody)'}  [{r['status']}]")
    say(f"             machines holding a replica: "
        f"{len({r['node'] for r in assigned})} of {len(assigned)} assigned")
    return out


def main() -> int:
    # Read today's file into memory BEFORE anything overwrites it on disk, so the
    # restore at the end cannot depend on git having the fix committed yet.
    with open(SCHEDULER, encoding="utf-8", newline="") as fh:
        fixed_source = fh.read()
    started = datetime.now(timezone.utc).isoformat(timespec="seconds")
    head = sh("git", "rev-parse", "--short", "HEAD")
    before_sha_git = sh("git", "rev-parse", "--short", BEFORE_REF)
    dirty = sh("git", "status", "--porcelain", "--", "control-plane", "agent",
               "workloads", "scripts", "docker-compose.yml")
    fixed_hash = sha(SCHEDULER)

    say("=" * 78)
    say("  ONE JOB, N REPLICAS, N MACHINES, N SERVED")
    say("  the 2026-09-06 one-job collapse, measured before and after on one stack")
    say(f"  started {started}")
    say("=" * 78)
    say()
    say("WHAT IS BEING ASKED")
    say("  FR-5: the scheduler expands a job into runs and dispatches them to")
    say("  eligible nodes in parallel. The look-wide-lock-narrow change of")
    say("  2026-09-06 fixed head-of-line blocking and, in the same move, took this")
    say("  case from 30 of 30 to 7 of 30 -- because the spread-replicas preference")
    say("  (2026-09-02i) STEPPED PAST every sibling, so on a queue of one job's")
    say("  replicas each claimer's candidate list came back one row long, and it was")
    say("  the same row for every claimer. One won it under SKIP LOCKED; the rest")
    say("  had no second choice. Found in docs/evidence/contention_rerun_2026-09-06.txt.")
    say()
    say("  BOTH cases must hold at once, which is why both are measured here: the")
    say("  many-jobs case is the one the 2026-09-06 change fixed, and trading it")
    say("  back would be a different regression, not a fix.")
    say()
    say("WHAT IS MEASURED")
    say("  How many machines were served in ONE simultaneous release, how many were")
    say("  sent away empty, and how many runs were handed to two claimers. Counts,")
    say("  not milliseconds: this file publishes NO timing, so its answer does not")
    say("  depend on the machine being fit to measure a millisecond on.")
    say()
    say("CONDITIONS")
    say(f"  tree                {head}")
    say(f"  before-ref          {before_sha_git} ({BEFORE_REF}) -- the tree with the collapse")
    say(f"  behaviour dirt      {dirty if dirty else '(clean)'}")
    say(f"  scheduler (after)   sha256 {fixed_hash}, {len(open(SCHEDULER, 'rb').read())} bytes")
    say("  claimers            registered through the ordinary /agent/register route,")
    say("                      capacity 1, released together by a threading barrier")
    say("                      (the contention campaign's own claimers and release)")
    say("  queue               written straight to Postgres, untargeted, no resource")
    say("                      requirements, strictly increasing created_at")
    say()

    # ---- BEFORE ------------------------------------------------------------
    subprocess.run(["git", "show", f"{BEFORE_REF}:{SCHEDULER}"],
                   stdout=open(SCHEDULER, "w", encoding="utf-8", newline="\n"),
                   text=True)
    before_hash = sha(SCHEDULER)
    say(f"[{stamp()}] restored {SCHEDULER} from {before_sha_git} through the compose "
        f"bind mount (sha256 {before_hash}, "
        f"{len(open(SCHEDULER, 'rb').read())} bytes)")
    epoch = restart_control_plane()
    say(f"[{stamp()}] control plane restarted, mode epoch {epoch}")
    before = arm(f"BEFORE -- scheduler from {before_sha_git}")

    # ---- AFTER -------------------------------------------------------------
    with open(SCHEDULER, "w", encoding="utf-8", newline="") as fh:
        fh.write(fixed_source)
    back_hash = sha(SCHEDULER)
    say()
    say(f"[{stamp()}] restored today's {SCHEDULER} (sha256 {back_hash}, expected "
        f"{fixed_hash}) -- {'MATCH' if back_hash == fixed_hash else 'MISMATCH'}")
    epoch = restart_control_plane()
    say(f"[{stamp()}] control plane restarted, mode epoch {epoch}")
    after = arm("AFTER -- today's scheduler")

    # ---- the comparison ----------------------------------------------------
    say()
    say("-" * 78)
    say("  BEFORE AND AFTER, SIDE BY SIDE")
    say("-" * 78)
    say("  case                              before          after")
    for key in ("one job@10", "one job@30", "many jobs@6"):
        b, a = before[key], after[key]
        say(f"  {key:<28}  {b['served']:>2} of {b['claimers']:<2} served   "
            f"{a['served']:>2} of {a['claimers']:<2} served")
    say()
    say("  runs handed to two claimers, every case, both arms: "
        f"{sum(r['double_claims'] for k, r in list(before.items()) if k != 'spread')} "
        "before, "
        f"{sum(r['double_claims'] for k, r in list(after.items()) if k != 'spread')} "
        "after")
    say()
    say("  3 replicas / 6 machines of capacity 4 -- machines holding a replica:")
    for label, res in (("before", before), ("after", after)):
        assigned = [r for r in res["spread"] if r["node"]]
        say(f"    {label:<7} {len({r['node'] for r in assigned})} machine(s) hold "
            f"{len(assigned)} replica(s)")
    say()
    say(f"  finished {datetime.now(timezone.utc).isoformat(timespec='seconds')}")

    with open(OUT, "w", encoding="utf-8") as fh:
        fh.write("\n".join(_lines) + "\n")
    print(f"\n[proof] written to {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
