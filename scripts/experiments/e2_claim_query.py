"""E2 — is `SELECT … FOR UPDATE SKIP LOCKED` really needed? (brief §5)

The question a juror asks about our queue: *why not just read the pending rows and
update them?* This experiment answers it with a number instead of an opinion. The
existing Postgres proof (`control-plane/tests/test_w2_postgres.py`) already shows
that two claimers cannot both take one row. E2 is that proof turned into a
measurement: many claimers, a real queue, three locking strategies, and a count of
what each one gets wrong.

**The three arms** (the `claim` switch, `control-plane/app/config.py`; the code
under test is `assign_runs` in `control-plane/app/scheduler.py`):

  * `skip_locked` — ours. Lock the rows I take; step past anything another claimer
    already holds.
  * `blocking` — `SELECT … FOR UPDATE` with no SKIP LOCKED. Also correct, but a
    second claimer WAITS behind the first instead of moving on.
  * `naive` — a plain read of the pending rows, then the update, at the database's
    default isolation level (recorded in `manifest.json`; Postgres ships READ
    COMMITTED). This is the honest version a competent beginner writes, and it is
    already implemented that way in `assign_runs` — nothing here handicaps it.

**What is held constant across every arm and every concurrency level**, because a
comparison is only worth something if exactly one thing changes:

  * the same seed — `--runs` (200) PENDING rows, created directly in the database,
    all belonging to one ordinary job (not private, no target nodes, no resource
    requirements), with strictly increasing `created_at` so every claimer sees the
    same queue in the same order;
  * the same claimers — N nodes registered through the ordinary public
    `POST /agent/register`, with identical specs and identical capacity;
  * the same warm-up — one heartbeat per claimer against an EMPTY queue before the
    seed, so no measured request pays for a cold TCP connection or a cold query plan;
  * the same reset — `harness.reset_platform()` before every repetition, so no
    repetition inherits another one's rows;
  * the same simultaneity — every round is released by a thread barrier, and the
    spread of the release instants is measured and recorded (`release_skew_ms`).

Only the locking clause differs. The WHERE clause, the ordering and the LIMIT are
identical in all three arms (see the comment block in `assign_runs`).

**A claimer here is a concurrent heartbeat, not an agent process.** Sixteen real
agent processes would mostly measure Python start-up and each agent's own polling
timer. Pull-time assignment happens *inside* `POST /agent/heartbeat`, so the honest
way to put N claimers on the queue at one instant is N simultaneous heartbeats.
No container is ever started by this experiment.

Run it (host, repo venv, nothing else touching the stack):

    .venv\\Scripts\\python.exe scripts\\experiments\\e2_claim_query.py --dry
    .venv\\Scripts\\python.exe scripts\\experiments\\e2_claim_query.py --machine-idle --mains-power
"""

from __future__ import annotations

import argparse
import math
import statistics
import sys
import threading
import time
import uuid
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))

import harness as h  # noqa: E402

# Which results directory this invocation writes to. Overridable by `--exp-label`
# for one reason: E2b re-runs the SAME three arms in a different contention regime
# (claimers ~= runs instead of a queue two claimers can drain in one round), and
# The project rule requires the two to be published side by side and NEVER merged.
# Without a label the re-run would write into E2/, and `guard_fresh` would archive
# E2's own measured rows to make room — replacing evidence instead of adding to it.
# Default unchanged, so every existing command behaves exactly as before.
EXP = "E2"
ARMS = ("skip_locked", "blocking", "naive")
CONCURRENCY = (2, 4, 8, 16)
REPS = 5
RUNS = 200

# The lease TTL the control plane runs with for the whole of E2 — ten minutes,
# far longer than any repetition.
#
# A claimer here never executes or finishes anything, so a run it claims sits
# ASSIGNED with a ticking lease. On the shipped 60 s lease the reaper would
# eventually decide that node had died and put the run back in the queue — and a
# second claimer taking that requeued run is a CORRECT re-claim, not a double
# assignment, yet from the outside the two look identical. Raising the lease past
# the length of a repetition removes the confusion at its source rather than
# correcting for it afterwards.
#
# The `reaper_interference` detector stays on regardless: this makes the event
# impossible in practice, and the detector proves it did not happen.
LONG_LEASE_TTL_S = 600

# A round is one simultaneous heartbeat per claimer. We keep firing rounds until a
# round hands out nothing, which is the claimers' own signal that the queue is
# empty. The cap is only a runaway guard — if it is ever hit, `queue_drained`
# records that the queue did not finish draining.
MAX_ROUNDS = 12

# Generous on purpose. The `blocking` arm makes claimers wait on each other inside
# Postgres, and at high concurrency it can also exhaust the control plane's database
# connection pool (SQLAlchemy default: 5 + 10 overflow). A short timeout would turn
# that real, measurable cost into a crash instead of a number.
HTTP_TIMEOUT_S = 180


# --- Seeding ----------------------------------------------------------------


def seed_queue(runs: int) -> str:
    """Write one job and `runs` PENDING run rows straight into Postgres.

    Direct inserts rather than `POST /jobs`, for one reason: E2 measures the CLAIM
    side of the queue, and submitting 200 jobs through the API would put the
    submission path's own cost inside every number. The rows are written exactly as
    the API writes them — same columns, same defaults — so the scheduler cannot
    tell the difference.

    Three properties of the seeded job are deliberate:

      * `target_node_ids` is NULL. A targeted job triggers the W4 pinning rule (one
        run per selected node), which would cap each claimer at one run and hide
        the contention this experiment exists to create.
      * `resource_reqs` is empty, so `_eligible` passes for every claimer and no run
        is filtered out for a reason unrelated to locking.
      * `private` is false, so the W6b trust condition in the claim query is
        satisfied for every node and, again, nothing is hidden for the wrong reason.

    `created_at` increases by one millisecond per row. That is the ORDER BY the claim
    query uses, so a strict order means every claimer looks at the same head of the
    queue — the hardest case for any locking strategy, and identical for all arms.
    """
    job_id = str(uuid.uuid4())
    h.psql_exec(
        "INSERT INTO jobs (id, name, image, entrypoint, env, resource_reqs, "
        "target_node_ids, replicas, status, created_at, private) VALUES ("
        f"'{job_id}', 'E2 claim-query queue', 'fyp-dummy:latest', "
        "json_build_array('python', 'train.py'), '{}'::json, '{}'::json, NULL, "
        f"{runs}, 'PENDING', now(), false);"
    )
    # Ids are generated in SQL so the whole seed is two statements regardless of the
    # queue size: 'e2000000-0000-4000-8000-' + 12 digits = 36 characters, the width
    # of the id column, and obviously ours when read in pgAdmin.
    h.psql_exec(
        "INSERT INTO runs (id, job_id, node_id, status, attempt, lease_expires_at, "
        "retries_remaining, exit_code, started_at, finished_at, created_at, "
        "escalation_count) SELECT "
        "'e2000000-0000-4000-8000-' || lpad(g::text, 12, '0'), "
        f"'{job_id}', NULL, 'PENDING', 0, NULL, 1, NULL, NULL, NULL, "
        "now() - interval '1 hour' + (g * interval '1 millisecond'), 0 "
        f"FROM generate_series(1, {runs}) g;"
    )
    return job_id


def register_claimers(n: int, capacity: int) -> list[dict]:
    """Register n nodes through the ordinary public register endpoint.

    These are not agents — nothing runs a container, and nothing reports a run as
    RUNNING. They exist to hold a node token and a capacity, which is all the claim
    query looks at."""
    claimers: list[dict] = []
    for i in range(n):
        resp = requests.post(
            f"{h.API}/agent/register",
            json={
                "name": f"e2-{i:02d}",
                "specs": {
                    "cpu_cores": 8,
                    "has_gpu": False,
                    "ram_mb": 8192,
                    "capacity": capacity,
                    "agent_version": "e2-bench",
                },
            },
            timeout=30,
        )
        if resp.status_code != 200:
            raise h.HarnessError(
                f"register failed ({resp.status_code}): {resp.text[:200]}"
            )
        body = resp.json()
        claimers.append({"name": f"e2-{i:02d}", **body})
    return claimers


# --- One simultaneous round -------------------------------------------------


def fire_round(claimers: list[dict], sessions: list[requests.Session]) -> list[dict]:
    """Send one heartbeat per claimer, all released at the same instant.

    The barrier is the part that matters. Without it we would start thread 1, it
    would finish its request before thread 16 had even been created, and the number
    we published as "16 concurrent claimers" would really be sixteen polite queued
    ones — no contention, no result. `threading.Barrier(n)` makes every thread block
    until the last one arrives, so all N requests leave within roughly a millisecond
    of each other. How close they actually got is measured, not assumed:
    `release_skew_ms` in every recorded row is max-minus-min of the release instants.

    Threads (not asyncio) because `requests` is blocking; the GIL is released while a
    socket waits, so N threads really do have N requests in flight."""
    n = len(claimers)
    gate = threading.Barrier(n)
    # Pre-filled with a complete "this claimer never answered" shape, so a thread
    # that hangs past its join still leaves a readable row instead of a hole the
    # counting code trips over. A hole would be recorded as zero damage.
    out: list[dict] = [
        {"released": None, "latency_ms": None, "run_ids": [], "error": "no result"}
        for _ in range(n)
    ]

    def fire(i: int) -> None:
        claimer, sess = claimers[i], sessions[i]
        # Everything that can be prepared is prepared BEFORE the barrier, so the only
        # work between "released" and "request sent" is the send itself.
        body = {"node_id": claimer["node_id"], "status": "idle", "running": []}
        headers = {"Authorization": f"Bearer {claimer['token']}"}
        try:
            gate.wait(timeout=60)
        except threading.BrokenBarrierError:
            out[i] = {"released": None, "latency_ms": None, "run_ids": [],
                      "error": "barrier broken"}
            return
        released = time.perf_counter()
        try:
            resp = sess.post(
                f"{h.API}/agent/heartbeat", json=body, headers=headers,
                timeout=HTTP_TIMEOUT_S,
            )
            latency_ms = (time.perf_counter() - released) * 1000.0
            if resp.status_code != 200:
                out[i] = {"released": released, "latency_ms": latency_ms,
                          "run_ids": [], "error": f"HTTP {resp.status_code}"}
                return
            run_ids = [a["run_id"] for a in resp.json().get("assignments", [])]
            out[i] = {"released": released, "latency_ms": latency_ms,
                      "run_ids": run_ids, "error": None}
        except Exception as exc:                     # noqa: BLE001 - record, never crash
            out[i] = {"released": released,
                      "latency_ms": (time.perf_counter() - released) * 1000.0,
                      "run_ids": [], "error": type(exc).__name__}

    threads = [
        threading.Thread(target=fire, args=(i,), name=f"e2-claimer-{i}")
        for i in range(n)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=HTTP_TIMEOUT_S + 30)
    return out


# --- Reading the damage -----------------------------------------------------


_FINAL_STATE_SQL = (
    "SELECT count(*)::int AS runs_total, "
    "count(*) FILTER (WHERE status = 'PENDING')::int AS runs_pending, "
    "count(*) FILTER (WHERE status = 'ASSIGNED')::int AS runs_assigned, "
    "count(DISTINCT node_id)::int AS owner_nodes, "
    "coalesce(sum(attempt), 0)::int AS attempt_sum, "
    "coalesce(max(attempt), 0)::int AS attempt_max, "
    "coalesce(extract(epoch FROM (max(lease_expires_at) - min(lease_expires_at))), 0)"
    "::float8 AS claim_span_s "
    "FROM runs"
)


def _percentile(values: list[float], pct: float) -> float | None:
    """p95 means: the value that 95 out of every 100 samples stay under.

    Nearest-rank, computed from our own samples — sort them, take the one 95% of the
    way up. It is deliberately not interpolated, so the number printed is a value we
    actually observed. With few samples (2 claimers × 2 rounds = 4 requests) a p95 is
    barely more than the maximum; `latency_n` in every row says how many samples it
    came from, so nobody reads more precision into it than it has."""
    if not values:
        return None
    ordered = sorted(values)
    rank = math.ceil(pct * len(ordered)) - 1
    return ordered[max(0, min(rank, len(ordered) - 1))]


def run_once(*, mode: str, n: int, rep: int, runs: int, capacity: int,
             dry: bool) -> dict:
    """One (arm, concurrency, repetition): seed the queue, race N claimers at it,
    count what came out, and record a row."""
    h.reset_platform()
    claimers = register_claimers(n, capacity)
    sessions = [requests.Session() for _ in claimers]
    try:
        # Warm-up: one round against an EMPTY queue. It opens each thread's TCP
        # connection, gets the heartbeat route and the claim query's plan into cache,
        # and writes each node's first `last_heartbeat`. None of that is the thing
        # being compared, so none of it belongs inside a measured round. It cannot
        # claim anything — the seed has not happened yet.
        fire_round(claimers, sessions)

        seed_queue(runs)

        rounds: list[list[dict]] = []
        # The clock starts once the queue is fully PENDING and the claimers are about
        # to be released. It is a CLIENT-side clock, and it has to be: the platform
        # stores no "claim completed" timestamp, so there is no server-side row that
        # says when a claim finished. `db_claim_span_s` below is the nearest
        # server-side check, and its limits are stated there.
        t0 = time.perf_counter()
        t_drained = None
        for _ in range(MAX_ROUNDS):
            result = fire_round(claimers, sessions)
            rounds.append(result)
            if sum(len(r["run_ids"]) for r in result) == 0:
                break               # nobody got anything: the queue is exhausted
            t_drained = time.perf_counter()

        drain_wall_s = (t_drained - t0) if t_drained else 0.0

        # --- who was handed what ------------------------------------------------
        #
        # A "double assignment" is one run handed to more than one claimer: two
        # workers are told to execute the same job. This is the whole question E2
        # asks, and the HTTP responses are the honest place to see it — each response
        # is a promise the control plane made to one claimer.
        #
        # The database alone UNDER-COUNTS it, and always will. A run row has one
        # `node_id` column, so however many claimers were told they own it, the row
        # can only remember the last writer. `attempt` does not help either: every
        # naive writer read the same old value and wrote the same new one, so
        # `db_attempt_sum` still equals the number of claimed runs even when every
        # single one was handed out four times. That is why the headline number below
        # comes from the responses and the database numbers are recorded beside it —
        # the gap between `assignments_handed_out` and `db_runs_assigned` IS the
        # damage the database cannot show.
        owners: dict[str, set[str]] = {}
        latencies: list[float] = []
        releases: list[float] = []
        errors = 0
        handed_out = 0
        for result in rounds:
            for i, item in enumerate(result):
                if item.get("error"):
                    errors += 1
                if item.get("latency_ms") is not None:
                    latencies.append(item["latency_ms"])
                if item.get("released") is not None:
                    releases.append(item["released"])
                for run_id in item["run_ids"]:
                    handed_out += 1
                    owners.setdefault(run_id, set()).add(claimers[i]["name"])

        distinct_handed = len(owners)
        double_assigned = sum(1 for nodes in owners.values() if len(nodes) > 1)
        productive_rounds = sum(
            1 for r in rounds if sum(len(x["run_ids"]) for x in r) > 0
        )

        db = h.psql_json(_FINAL_STATE_SQL)[0]

        # The reaper is running in the control plane the whole time (LEASE_TTL_S was 15
        # when this experiment was measured; the shipped default is 60 since 2026-08-29,
        # sweep every 3s) and this experiment cannot turn it off — only the three
        # experiment switches are settable, and the lease TTL is not one of them. If a
        # repetition ever ran long enough for a lease to lapse, the reaper would push
        # claimed runs back to PENDING and a later round could legitimately re-claim
        # them, which would look exactly like a double assignment. Any run that has
        # been through that carries attempt >= 2, so max(attempt) > 1 is the tell.
        # Flagged, not silently corrected: a flagged repetition is one the red-team
        # pass should throw out.
        interference = 1 if db["attempt_max"] > 1 else 0
        if interference:
            print(f"    !! reaper interference in {mode}@{n} rep {rep} "
                  f"(max attempt {db['attempt_max']}) — treat this row as suspect")

        h.assert_stable(where=f"{mode}@{n} rep {rep}")

        metrics = {
            # --- conditions, on every row so `summarize` can group and a jury can ask
            "claim_mode": mode,
            "concurrency": n,
            "capacity": capacity,
            "runs_seeded": runs,
            # --- the headline: client-side, counted from the HTTP responses
            "double_assigned_runs": double_assigned,
            "duplicate_handouts": handed_out - distinct_handed,
            "assignments_handed_out": handed_out,
            "distinct_runs_handed": distinct_handed,
            # --- what the database ended up holding (server-side truth)
            "db_runs_assigned": db["runs_assigned"],
            "db_runs_pending": db["runs_pending"],
            "db_owner_nodes": db["owner_nodes"],
            "db_attempt_sum": db["attempt_sum"],
            "db_attempt_max": db["attempt_max"],
            # Server-side cross-check: the spread of `lease_expires_at` over the
            # claimed rows. Both ends are stamped by the control plane, and the lease
            # TTL cancels out of the subtraction, so nothing is derived or assumed.
            # What it does NOT capture: the stamp is taken when the heartbeat handler
            # STARTS, not when the claim finishes, so within one simultaneous round it
            # is near zero however long the claim took. It measures when claim
            # activity happened, not how long it lasted — which is precisely why the
            # drain and latency numbers below are client-side.
            "db_claim_span_s": db["claim_span_s"],
            "queue_drained": 1 if db["runs_pending"] == 0 else 0,
            "reaper_interference": interference,
            # --- timings: CLIENT-side, an HTTP round trip, which is what a claimer
            # actually experiences. `drain_wall_s` runs from the release of round 1 to
            # the end of the last round that handed anything out; the rounds are fired
            # back to back with no sleep and no database poll between them, so it is
            # the claiming itself plus a few milliseconds of thread creation.
            "drain_wall_s": drain_wall_s,
            "claims_per_s": (db["runs_assigned"] / drain_wall_s) if drain_wall_s else None,
            "latency_mean_ms": statistics.mean(latencies) if latencies else None,
            "latency_p95_ms": _percentile(latencies, 0.95),
            "latency_max_ms": max(latencies) if latencies else None,
            "latency_n": len(latencies),
            # --- proof that "simultaneous" was really simultaneous
            "release_skew_ms": (
                (max(releases) - min(releases)) * 1000.0 if len(releases) > 1 else 0.0
            ),
            "rounds": productive_rounds,
            "rounds_total": len(rounds),
            # A failed or rejected heartbeat. In the `blocking` arm at high
            # concurrency this is where a database-connection-pool timeout would
            # appear — a real cost of that arm, recorded rather than hidden.
            "errors": errors,
            # Kept raw so the whole latency distribution can be re-aggregated later
            # without re-running the measurement. `summarize` ignores non-numeric
            # values, so this never reaches a published table by itself.
            "latencies_ms": [round(v, 3) for v in latencies],
        }
        return h.record(EXP, f"{mode}@{n}", rep, metrics, dry=dry)
    finally:
        for sess in sessions:
            sess.close()


# --- Driver -----------------------------------------------------------------


SUMMARY_METRICS = [
    "double_assigned_runs",
    "duplicate_handouts",
    "assignments_handed_out",
    "distinct_runs_handed",
    "db_runs_assigned",
    "db_runs_pending",
    "db_owner_nodes",
    "queue_drained",
    "drain_wall_s",
    "claims_per_s",
    "latency_mean_ms",
    "latency_p95_ms",
    "latency_max_ms",
    "rounds",
    "errors",
    "release_skew_ms",
    "reaper_interference",
]


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--arms", nargs="+", choices=list(ARMS), default=list(ARMS),
                   help="which claim strategies to measure")
    p.add_argument("--concurrency", nargs="+", type=int, default=list(CONCURRENCY),
                   help="how many claimers heartbeat at the same instant")
    p.add_argument("--reps", type=int, default=REPS,
                   help="repetitions per (arm, concurrency)")
    p.add_argument("--runs", type=int, default=RUNS,
                   help="PENDING runs seeded per repetition")
    p.add_argument("--capacity", type=int, default=None,
                   help="per-node capacity; default is half the queue (see below)")
    p.add_argument("--dry", action="store_true",
                   help="rehearsal: 2 reps at one concurrency level, every row dry")
    p.add_argument("--machine-idle", action=argparse.BooleanOptionalAction,
                   default=None, help="was the machine otherwise idle (manifest)")
    p.add_argument("--mains-power", action=argparse.BooleanOptionalAction,
                   default=None, help="was it on mains, not battery (manifest)")
    p.add_argument("--fresh", action="store_true",
                   help="archive an existing raw.jsonl and start clean; without it a "
                        "run that would append onto measured rows stops instead (the "
                        "old file is renamed, never deleted)")
    p.add_argument("--exp-label", default=EXP,
                   help="results directory under docs/evidence/experiments/. Use "
                        "E2b for the contention-regime re-run so it is published "
                        "BESIDE E2 rather than archiving it")
    p.add_argument("--lease-ttl-s", type=int, default=LONG_LEASE_TTL_S,
                   help="lease TTL the control plane runs with during E2. Raised far "
                        "above a repetition's length so the reaper cannot requeue a "
                        "held run mid-round and make a legitimate re-claim look like "
                        "a double assignment")
    return p.parse_args()


def main() -> int:
    global EXP
    args = _parse_args()
    # Set before anything writes: `run_once` reads this module global on every
    # `h.record`, so rebinding it here routes the whole invocation's output.
    EXP = args.exp_label
    h.require_ready()
    if not args.dry:
        h.guard_fresh(EXP, args.fresh)

    concurrency = list(args.concurrency)
    reps = args.reps
    if args.dry:
        # A rehearsal proves the script runs end to end; it is not evidence, and rule
        # 7 of the brief keeps it out of every summary and chart.
        concurrency = concurrency[:1]
        reps = 2

    # Capacity: how many runs ONE heartbeat may claim. `assign_runs` computes
    # spare = capacity - (runs this node already holds) and takes at most `spare`,
    # after over-fetching spare × 4 candidate rows.
    #
    # The default is half the queue, and the reason is drainage. These claimers never
    # execute or finish anything, so a run they claim stays on their plate for the
    # whole repetition: the pool can therefore only ever claim N × capacity runs in
    # total. With capacity = runs / 2, two claimers already cover the whole queue, so
    # the queue drains at EVERY concurrency level without this script ever reaching
    # into the database mid-measurement to free capacity — no invented state inside a
    # timed window. It also maximises the overlap: at spare × 4 the candidate fetch
    # covers the entire queue, so all N claimers are reaching for the same rows at
    # once, which is the hardest case a locking strategy can be given. Same value in
    # every arm and at every level, and recorded on every row.
    capacity = args.capacity if args.capacity else max(1, args.runs // 2)

    print(f"[E2] arms={args.arms} concurrency={concurrency} reps={reps} "
          f"runs={args.runs} capacity={capacity} lease_ttl_s={args.lease_ttl_s} "
          f"dry={args.dry}")

    modes_used: list[dict] = []
    try:
        # Repetitions on the OUTSIDE, arms on the inside. A machine drifts over a long
        # run (thermal throttling, page cache, background work), and running all of
        # one arm's repetitions before the next arm's would hand that drift to one arm
        # as if it were a property of the locking strategy. Interleaving spreads it
        # evenly. The cost is one control-plane restart per (rep, arm) instead of per
        # arm, which is seconds and buys a fairer comparison.
        for rep in range(1, reps + 1):
            for mode in args.arms:
                # The long lease rides on every set_mode call, so it is re-asserted
                # in the container after each restart rather than assumed to persist.
                got = h.set_mode(claim=mode,
                                 env={"LEASE_TTL_S": str(args.lease_ttl_s)})
                if got not in modes_used:
                    modes_used.append(got)
                for n in concurrency:
                    row = run_once(mode=mode, n=n, rep=rep, runs=args.runs,
                                   capacity=capacity, dry=args.dry)
                    p95 = row["latency_p95_ms"]
                    print(
                        f"    {mode:>11}@{n:<3} rep {rep}: "
                        f"double_assigned={row['double_assigned_runs']:>4} "
                        f"handed={row['assignments_handed_out']:>5} "
                        f"claimed={row['db_runs_assigned']:>4} "
                        f"pending_left={row['db_runs_pending']:>4} "
                        f"drain={row['drain_wall_s']:.3f}s "
                        f"p95={('%.0f' % p95) if p95 is not None else '«MISSING»'}ms "
                        f"skew={row['release_skew_ms']:.1f}ms "
                        f"err={row['errors']}"
                    )
    finally:
        # Never leave the platform on a weakened claim strategy, even after a crash.
        h.set_mode()

    h.summarize(
        EXP, metrics=SUMMARY_METRICS,
        title=f"{EXP} — claim query: SKIP LOCKED vs blocking vs naive",
    )
    h.chart(
        EXP, metric="double_assigned_runs",
        title=f"{EXP} — runs handed to more than one claimer (median, min–max)",
    )
    h.manifest(
        EXP,
        reps=reps,
        modes=modes_used,
        machine_idle=args.machine_idle,
        mains_power=args.mains_power,
        notes=(
            "Claimers are concurrent heartbeats, not agent processes; no container is "
            "started. Double assignment is counted from the HTTP responses — a run row "
            "holds one node_id, so the database alone under-counts it. Drain and "
            "latency are client-side HTTP round trips (the platform stores no "
            "'claim completed' timestamp); db_claim_span_s is the server-side "
            "cross-check and its limits are stated in the row comments. "
            f"The control plane ran with LEASE_TTL_S={args.lease_ttl_s} for the "
            "whole of E2 — far longer than a repetition — so the reaper cannot "
            "requeue a held run mid-round and make a correct re-claim look like a "
            "double assignment. reaper_interference stays on as the proof that it "
            "did not happen, and is reported in summary.md even when it is zero."
        ),
        extra={
            "arms": args.arms,
            "concurrency_levels": concurrency,
            "runs_seeded_per_rep": args.runs,
            "capacity_per_node": capacity,
            "max_rounds": MAX_ROUNDS,
            "http_timeout_s": HTTP_TIMEOUT_S,
            # Recorded explicitly as well as inside compose_overrides: this is a
            # deliberate departure from the shipped 15s default and a reader must
            # not have to dig through YAML to find it.
            "lease_ttl_s_override": args.lease_ttl_s,
            "lease_ttl_s_shipped_default": h.SHIPPED_LEASE_TTL_S,
        },
    )
    print(f"\n[E2] done — {h.exp_dir(EXP)}: raw.jsonl, summary.md, chart.png, "
          "manifest.json")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (h.HarnessError, AssertionError) as exc:
        print(f"\nE2 FAILED: {exc}", file=sys.stderr)
        sys.exit(1)
