"""What walking a long queue costs, with a sample behind every number.

PART G of the targeted-starvation proof answered this with FIVE heartbeats per
depth (docs/evidence/targeted_starvation_2026-09-06.txt). Five is enough to see an
order of magnitude and not enough to publish, and that capture's own numbers show
why: it reports median 35 ms at 71 pending runs and median 21 ms at 171 -- the
deeper queue reading faster than the shallower one. That is the sample size
talking, not the queue. Nothing there is withdrawn; the question is re-asked here
at twenty heartbeats a depth so the figure travels with its count and its spread.

The queue is filled with runs aimed at a machine that never heart-beats, so the
probe can take none of them and every heartbeat walks the whole queue -- which is
the cost being measured. The probe's assignment list is checked at every depth,
because a probe that took work would be measuring something else.
"""

import json
import statistics
import subprocess
import sys
import uuid
from datetime import datetime, timezone

sys.path.insert(0, "scripts")
sys.path.insert(0, "scripts/experiments")

import targeted_starvation_proof as tsp   # noqa: E402  register/heartbeat/psql/call
import harness as h                       # noqa: E402  reset_platform/psql_exec

DEPTHS = (70, 170, 370, 530, 730)
BEATS = 20
OUT = "docs/evidence/walk_cost_2026-09-06.txt"

_lines: list[str] = []


def say(msg: str = "") -> None:
    print(msg, flush=True)
    _lines.append(msg)


def stamp() -> str:
    return datetime.now(timezone.utc).strftime("%H:%M:%S")


def seed_targeted(ghost_id: str, count: int, tag: str) -> None:
    """`count` PENDING runs aimed at the silent machine, written straight to
    Postgres. The submission path would send them one at a time, and the depth of
    the queue rather than the cost of submitting is what this measures."""
    job_id = str(uuid.uuid4())
    h.psql_exec(
        "INSERT INTO jobs (id, name, image, entrypoint, env, resource_reqs, "
        "target_node_ids, replicas, status, created_at, private) VALUES ("
        f"'{job_id}', 'walk-cost {tag}', 'fyp-dummy:latest', "
        "json_build_array('python', 'train.py'), '{}'::json, '{}'::json, "
        f"json_build_array('{ghost_id}'), {count}, 'PENDING', now(), false);"
    )
    h.psql_exec(
        "INSERT INTO runs (id, job_id, node_id, status, attempt, lease_expires_at, "
        "retries_remaining, exit_code, started_at, finished_at, created_at, "
        "escalation_count) SELECT "
        # 8 + 1 + 4 + 1 + 4 + 1 + 4 + 1 + 12 = 36, the width of the id column.
        f"'{tag}-0000-4000-8000-' || lpad(g::text, 12, '0'), "
        f"'{job_id}', NULL, 'PENDING', 0, NULL, 1, NULL, NULL, NULL, "
        "now() - interval '1 hour' + (g * interval '1 millisecond'), 0 "
        f"FROM generate_series(1, {count}) g;"
    )


def pending() -> int:
    return int(tsp.psql("select count(*) from runs where status='PENDING'"))


def source_line(needle: str) -> str:
    out = subprocess.run(
        ["grep", "-n", needle, "control-plane/app/scheduler.py"],
        capture_output=True, text=True).stdout.strip().splitlines()
    return out[0].strip() if out else "«MISSING»"


def main() -> int:
    started = datetime.now(timezone.utc).isoformat(timespec="seconds")
    head = subprocess.run(["git", "rev-parse", "--short", "HEAD"],
                          capture_output=True, text=True).stdout.strip()
    dirty = subprocess.run(
        ["git", "status", "--porcelain", "--", "control-plane", "agent",
         "workloads", "scripts", "docker-compose.yml"],
        capture_output=True, text=True).stdout.strip()
    _st, health = tsp.call("/health")

    say("=" * 78)
    say("  WHAT WALKING A LONG QUEUE COSTS -- with a sample behind every number")
    say(f"  started {started}")
    say("=" * 78)
    say()
    say("WHY THIS CAPTURE EXISTS")
    say("  PART G of docs/evidence/targeted_starvation_2026-09-06.txt measured this")
    say("  with n=5 a depth. Its own numbers show why five is not enough: median")
    say("  35 ms at 71 pending runs and median 21 ms at 171 -- the deeper queue")
    say("  reading faster than the shallower one. Nothing in that file is")
    say("  withdrawn. The question is re-asked here at n=20.")
    say()
    say("  A SEPARATE FACT, recorded because it was asked. The figures 28 / 48 / 62 ms")
    say("  at 68 / 532 / 732 pending runs are not in that file, and a search of every")
    say("  .txt, .md and .py in this repository does not find them. The two captures")
    say("  that do exist both print medians of five with their min and max beside")
    say("  them:")
    say("    docs/evidence/targeted_starvation_2026-09-06.txt   (tree 9fe92e3)")
    say("       71 pending -> median  35 ms  (min 16, max  36, n=5)")
    say("      171 pending -> median  21 ms  (min 19, max  43, n=5)")
    say("      371 pending -> median  40 ms  (min 28, max  80, n=5)")
    say("    the same PART G as first captured, inside commit b1ddaf5")
    say("       64 pending -> median  31 ms  (min 15, max  35, n=5)")
    say("      164 pending -> median  37 ms  (min 28, max  46, n=5)")
    say("      364 pending -> median 104 ms  (min 96, max 120, n=5)")
    say("  So the answer to the question as asked is: neither single readings nor")
    say("  medians of many. They are medians of five -- and the 28 / 48 / 62 figures")
    say("  are not any capture's numbers.")
    say()
    say("CONDITIONS")
    say(f"  tree            {head}")
    say(f"  behaviour dirt  {dirty if dirty else '(clean)'}")
    say(f"  /health         {json.dumps(health)}")
    say(f"  heartbeats      {BEATS} a depth, fired one after another")
    say(f"  depths          {', '.join(str(d) for d in DEPTHS)} pending runs (target)")
    say("  the queue       every run is aimed at a machine that never heart-beats,")
    say("                  so the probe can take none of them and each heartbeat")
    say("                  walks the whole queue")
    say("  what is timed   the full round trip of POST /agent/heartbeat, measured at")
    say("                  the client. It contains the network hop and the web")
    say("                  framework as well as the walk, and is not a figure for")
    say("                  the claim query alone")
    say()

    h.reset_platform()
    tag = datetime.now(timezone.utc).strftime("%H%M%S")
    ghost = tsp.register(f"walkcost-ghost-{tag}", cores=64, capacity=64)
    say(f"[{stamp()}] registered {ghost['node_id']} and never heart-beat it again")
    probe = tsp.register(f"walkcost-probe-{tag}", cores=1, capacity=1)
    say(f"[{stamp()}] the probe machine: {probe['node_id']} (1 core, capacity 1)")
    say()
    say("-" * 78)
    say("   pending     median       min       max     n   offered")
    say("-" * 78)

    have = pending()
    rows = []
    for i, want in enumerate(DEPTHS):
        if want > have:
            seed_targeted(ghost["node_id"], want - have, f"w{i}{tag}")
            have = pending()
        times, offered_total = [], 0
        for _ in range(BEATS):
            offered, secs = tsp.heartbeat(probe)
            offered_total += len(offered)
            times.append(secs * 1000)
        med, lo, hi = statistics.median(times), min(times), max(times)
        rows.append({"pending": have, "median_ms": round(med, 1),
                     "min_ms": round(lo, 1), "max_ms": round(hi, 1),
                     "n": len(times), "offered": offered_total})
        say(f"  {have:>8}   {med:>8.1f}  {lo:>8.1f}  {hi:>8.1f}   {len(times):>3}   "
            f"{offered_total:>7}")

    say("-" * 78)
    say("  offered is the number of assignments the probe was handed across all")
    say("  heartbeats at that depth. It is zero at every depth, which is what makes")
    say("  each of these a full walk rather than a claim.")
    say()
    say("READ OUT OF THE SOURCE, so this capture states no bound the code does not")
    say("have:")
    say(f"  control-plane/app/scheduler.py:{source_line('_SCAN_PAGE = ')}")
    say(f"  control-plane/app/scheduler.py:{source_line('MAX_CLAIM_SCAN = ')}")
    say()
    say("THE ROWS AS JSON, for anything that would rather read them than parse a")
    say("table:")
    say(json.dumps(rows, indent=2))
    say()
    say(f"  finished {datetime.now(timezone.utc).isoformat(timespec='seconds')}")

    with open(OUT, "w", encoding="utf-8") as fh:
        fh.write("\n".join(_lines) + "\n")
    print(f"\n[walk-cost] written to {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
