"""Claim contention at thirty claimers — the measured evidence NFR-2 has never had.

The jury attack this answers, in its own words: *three workers on one laptop
prove nothing about thirty*. It is a fair attack. Every number this project
publishes about placement was taken at sixteen claimers or fewer (E2, E2b at
2/4/8/16; E2c at the same ladder), and NFR-2's proof cell in the report's Table 7
rests on the requirement's own wording — machines join and leave without a
restart, and the pool is heterogeneous — with nothing measured about what happens
when thirty of them ask for work at the same instant.

**The two questions, put narrowly.**

  1. **Correctness.** With thirty claimers released at one instant against one
     queue, is any run handed to more than one claimer? This is FR-8 and it is the
     headline. A single non-zero falsifies the guarantee the whole project rests
     on, and it would be the most important thing this experiment could find.

  2. **Cost.** What does a claim call cost when thirty claimers arrive together,
     and how does that compare with ten and twenty? A curve, not a point, so a
     reader can see the shape rather than take one number on trust.

--------------------------------------------------------------------------------
THE BOUND THAT HAS TO BE STATED FIRST, BECAUSE IT IS OURS AND NOT THE DATABASE'S
--------------------------------------------------------------------------------

The control plane builds its engine with no pool arguments, so SQLAlchemy's
default applies: five connections plus ten of overflow — a **ceiling of fifteen**.
That is not inferred from a library's source here; `harness.pool_config()` reads
it off the live engine object inside the running container, and the number it
reads is recorded on every row and in the manifest.

So above fifteen concurrent requests, the sixteenth and later wait **for a
connection**, not for a row lock. Reporting that wait as lock contention is
precisely the error E2 made — it published a latency curve that was the control
plane serving sixteen simultaneous heartbeats and read it as a locking result,
and the correction cost this project its entire locking finding (18, 2026-07-31g).
This experiment separates the two before it starts, with three instruments, and
names what none of them can do.

**Instrument 1 — the empty-queue control round.** Immediately before the measured
claim round, the same thirty claimers are released at one instant against an
**empty** queue. That round pays the identical HTTP path, the identical pool
checkout and the identical claim query — and has no row to lock, no row to update
and no commit of a claim. Whatever the pool costs at this concurrency, it costs
in both rounds. `latency_excess_median_ms` is the claim round's median minus the
empty round's median at the same concurrency in the same repetition: the cost of
actually claiming, with the pool and the HTTP concurrency subtracted out by
construction rather than by argument.

**Instrument 2 — the rung below the ceiling.** The ladder runs 1, 10, 20, 30. At
**ten**, every claimer fits inside a fifteen-connection pool at once, so a pool
wait cannot occur — not "was not observed", *cannot occur*. Growth from 1 to 10 is
therefore not the pool. Growth from 10 to 30 is where the pool can bind, and the
control round of instrument 1 says how much of it did.

**Instrument 3 — the connection count, read off Postgres.** A claimer waiting for
a **row lock** is inside Postgres and visible in `pg_stat_activity`, with the
backend that holds the row in its `pg_blocking_pids()`. A claimer waiting for a
**connection** is not in `pg_stat_activity` at all — it is queued inside the
control-plane process, outside the database. The two waits happen in different
places, and that is what makes them separable rather than merely distinguishable
in principle. `db_conns_max` (how many connections to the database existed) tests
whether the pool reached its ceiling; `blocked_backends_max` tests whether anyone
waited on a lock.

**What none of them can do, stated rather than implied.** No per-request split.
The pool's checkout wait is not instrumented in the control plane, and
instrumenting it is product code, which this script may not touch. So the pool's
contribution is reported as a **bound** from the control round, never as a
per-request millisecond figure. Anyone quoting a pool number out of this
experiment is quoting something it did not measure.

--------------------------------------------------------------------------------
THE ARMS — and why a control is not optional
--------------------------------------------------------------------------------

  * `skip_locked` — ours. `SELECT ... FOR UPDATE SKIP LOCKED` in `assign_runs`.
  * `naive` — **the control.** No row lock on the read at all. It is here to make
    the headline zero mean something: without an arm that double-assigns under the
    same conditions, "no run was handed out twice" and "nothing was ever contended"
    are the same observation. E2b measured naive double-assigning eleven of sixteen
    runs at sixteen claimers; if it does not double-assign at thirty, the control
    failed and this script says so instead of quoting the zero as though the
    control had stood behind it.
  * `blocking` — `SELECT ... FOR UPDATE` with no SKIP LOCKED. Not a control and not
    the headline; it is here because a jury will ask what the rejected alternative
    costs at thirty, and because blocked claimers hold pool connections while they
    wait, which is the one place the pool and the lock genuinely interact.

Only the row lock differs between the arms. The WHERE clause, the ordering and the
limit are identical in all three — that is a property of `assign_runs` itself, not
something this script arranges.

--------------------------------------------------------------------------------
THE SHAPE THIS MIRRORS CHANGED ON 2026-09-06 -- READ THIS BEFORE RE-RUNNING
--------------------------------------------------------------------------------

Everything below describes `assign_runs` AS IT WAS on the frozen measurement tree
`1df69b1`, and the numbers this harness published were measured against it. That is
why the text is left exactly as it was: a mirror that quietly followed the code would
make the published numbers unreadable, because nobody could tell afterwards which
shape they were measured against.

What changed. The fixed candidate window (`LIMIT spare * 4`) was head-of-line
blocking: a run a machine could not take still took up a place in the window, so a
queue whose head was full of such runs hid everything behind them. The scheduler now
walks the pending queue in pages WITHOUT a lock, picks what this node may take, and
locks only those rows (`scheduler._survey`, then the claim query in `assign_runs`).

What that means for this harness if it is ever re-run:

  * the arms still differ ONLY in the locking clause, so the comparison is still
    fair -- but each claimer now locks the rows it is taking rather than four
    candidates, so the contention this measures is not the contention it measured;
  * a re-run therefore produces DIFFERENT numbers, and they would be numbers about
    a different scheduler. They must be published as such, against the new tree,
    never merged into or compared with the 2026-07-30 figures.

--------------------------------------------------------------------------------
ONE PLACE THE BRIEF IS WRONG, REPORTED RATHER THAN ABSORBED
--------------------------------------------------------------------------------

The brief says "against a queue of thirty runs, one claim each". With the
scheduler's four-way over-fetch (`_CANDIDATE_MULTIPLIER = 4` in
`control-plane/app/scheduler.py`) a claimer with capacity 1 locks **four**
candidate rows and takes one, releasing the other three only when its transaction
ends. Thirty claimers against thirty rows therefore means the first seven or eight
lock the whole queue and the remaining twenty-two come back empty-handed in the
first round — and the latency median would then be a blend of two different
operations, most of them not claims at all.

So the default seeds `concurrency x 4` runs, which is the smallest queue at which
every claimer can lock its four candidates and take exactly one. "One claim each"
then holds literally, and every latency in the headline is the latency of a real
claim made under real contention. `--runs 30` still runs the brief's version, and
`runs_seeded` is on every row so no reader has to take this on trust.

--------------------------------------------------------------------------------
PREDICTION — written BEFORE this script had ever been run, and committed before
the campaign, because a prediction written afterwards is not a prediction
(18, 2026-07-29f). It is repeated verbatim in the manifest and in
docs/evidence/experiments/claim_contention_prediction.txt, so the three can be
compared without a checkout.
--------------------------------------------------------------------------------

  P1. **`double_assigned_runs` = 0 in every `skip_locked` repetition at every
      level, ten repetitions, 1 / 10 / 20 / 30 claimers.** This is the headline.
      `distinct_runs_handed` equals `assignments_handed_out` in every one of those
      rows, which is the same statement counted the other way round.

      IF P1 FAILS: a single non-zero double assignment under `skip_locked`
      falsifies FR-8 — no run result is silently lost or accepted in duplicate —
      and with it NFR-3 and the guarantee this whole project is built on. The
      campaign STOPS at the first occurrence. Nothing is re-run to see if it goes
      away, nothing is smoothed, and the lead is told before anything else is
      written. It is published at the top of the results file with its arm, its
      concurrency, its repetition and the run id that was handed out twice.

  P2. **`naive` double-assigns at 10, 20 and 30** — `double_assigned_runs` >= 1
      and `duplicate_handouts` far above zero. E2b measured naive double-assigning
      eleven of sixteen runs at sixteen claimers; nothing about thirty makes that
      better. At concurrency 1 it cannot double-assign and is expected to be
      clean, which is a check on the instrument rather than a result.

      IF P2 FAILS: the control did not do the one job it is here for, and P1's
      zero could not then tell "the lock worked" from "nothing was contended". The
      write-up says exactly that: the correctness claim falls back to
      `assignments_handed_out == distinct_runs_handed` plus E2b's own control at
      sixteen, and this experiment's zero is published as consistent with the
      guarantee rather than as evidence for it.

  P3. **Latency grows with concurrency, and beyond fifteen most of the growth is
      the pool rather than the lock.** Concretely: the empty-queue control round
      grows between 10 and 30 by an amount of the same order as the claim round
      does, so `latency_excess_median_ms` — claim minus empty at the same
      concurrency — stays roughly flat across 10, 20 and 30 rather than climbing
      with them.

      IF P3 FAILS one way — `latency_excess_median_ms` climbs steeply from 10 to
      30 while `blocked_backends_max` stays 0 — then something is making claims
      more expensive under concurrency that is neither the connection pool nor a
      row lock. We would have no mechanism for it. It is published as unexplained,
      with all three instruments' readings printed, and NOT attributed to the pool
      to make the story tidy. IF P3 FAILS the other way — the excess climbs and
      `blocked_backends_max` is non-zero under `skip_locked` — that is a claimer
      waiting behind another claimer under our own lock, it contradicts E2c's
      zero, and D6 publishes both results side by side.

  P4. **`db_conns_max` never exceeds `pool_ceiling`**, reaches it at 20 and 30,
      and stays at or below ten at concurrency 10.

      IF P4 FAILS by exceeding the ceiling: the ceiling `pool_config()` read is
      not the ceiling that binds, and the pool explanation the report gives for
      E2c's plateau at fifteen (18, 2026-08-12b and 2026-08-16c) is wrong. That is
      a correction to an already-published paragraph and it is published as one.
      IF P4 fails by NOT reaching the ceiling at 30, the sampler was too slow to
      catch the peak, `db_conns_max` is reported as the lower bound it is, and the
      pool separation rests on instruments 1 and 2 alone.

  P5. **`blocking` at 30 is markedly slower than `skip_locked` at 30**, and is the
      arm most likely to record `errors` (a heartbeat that failed or timed out),
      because a blocked claimer holds its pool connection while it waits. Not a
      headline. Recorded because a jury will ask and because guessing is worse.

--------------------------------------------------------------------------------
WHAT THIS EXPERIMENT CANNOT SAY
--------------------------------------------------------------------------------

  * **Thirty claimers, one machine.** Thirty processes' worth of claim requests
    against one control plane and one Postgres, all on the demonstration laptop.
    It measures the SERVER's behaviour under thirty simultaneous claimers, which
    is the thing NFR-2's attack is about. It says nothing about thirty physical
    machines on a network, and no sentence written from it may imply otherwise.
  * **No containers run.** A claimer here registers, heartbeats and is handed a
    run; nothing executes and nothing reports back. That is deliberate — Docker
    start-up jitter has no business inside a claim-latency number — and it means
    this experiment measures the claim, not the dispatch.
  * **The sampler is slow.** Snapshots go through `docker exec psql`, so the gap
    between them is hundreds of milliseconds and is recorded per row as
    `observe_gap_max_s`. `db_active_max` and `blocked_backends_max` are therefore
    LOWER BOUNDS: a burst shorter than the gap can be missed entirely.
    `db_conns_max` is the exception and is the reason it carries the pool claim —
    a pool connection stays open (idle) after the burst that opened it, so a late
    sample still sees it.
  * **Latency is client-side**, an HTTP round trip measured on the host, because
    the platform stores no "claim completed" timestamp. It is what a claimer
    actually experiences, which is the right quantity here, but it contains the
    network hop and the web framework as well as the database.
  * **It does not re-answer E2c.** Whether a claimer waits behind another claimer
    was measured by E2c with a purpose-built instrument (an artificial holder that
    keeps a row for twenty seconds). Here a claim lasts milliseconds and the
    sampler is slower than that, so a zero in `blocked_backends_max` corroborates
    E2c rather than repeating it.

Run it (host, repo venv, nothing else touching the stack), ONE experiment at a
time, on an otherwise idle machine, on mains power:

    .venv\\Scripts\\python.exe scripts\\experiments\\claim_contention.py --dry
    .venv\\Scripts\\python.exe scripts\\experiments\\claim_contention.py --machine-idle --mains-power
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

EXP = "claim_contention"

# `skip_locked` is ours and is the headline. `naive` is the CONTROL — see the
# header: without an arm that double-assigns under identical conditions, a zero
# cannot be told apart from "nothing was contended". `blocking` is the rejected
# alternative, measured because a jury asks what it costs.
ARMS = ("skip_locked", "naive", "blocking")

# 1 is the uncontended baseline: one claimer, no lock contention and no pool
# contention of any kind, so it sets the floor every other level is read against.
# 10 is BELOW the observed pool ceiling of fifteen, so a pool wait cannot occur
# there by construction. 20 and 30 are above it. 30 is the level the jury attack
# names.
CONCURRENCY = (1, 10, 20, 30)

REPS = 10

# One run per heartbeat, so `spare` is 1 and every claimer wants exactly one row.
# It is also what makes the over-fetch below four rather than a multiple of it.
CAPACITY = 1

# `_CANDIDATE_MULTIPLIER` in control-plane/app/scheduler.py. Mirrored, never
# imported: this script must not depend on the control plane's Python being
# importable on the host, and a silent divergence is caught by
# `empty_handed_responses` being non-zero when it should be zero.
CANDIDATE_MULTIPLIER = 4

# At most this many simultaneous rounds per repetition. Round 1 is the
# measurement; the rest exist to catch a double assignment only a later round
# could produce, and to confirm capacity accounting stops the claimers. With
# runs = N x 4 and capacity 1, round 2 should hand out nothing under skip_locked,
# because every claimer already holds one run and its spare is zero.
MAX_ROUNDS = 3

# Same reason as E2, E2b and E2c: a claimer here never finishes anything, so a run
# it claims sits ASSIGNED with a ticking lease. On the shipped 60 s lease the
# reaper would requeue it and a later re-claim — which is CORRECT — would be
# indistinguishable from a double assignment. Raising the lease past the length of
# a repetition removes the confusion at its source; `reaper_interference` stays on
# as the proof that it did not happen.
LONG_LEASE_TTL_S = 600

# Generous on purpose. In the `blocking` arm at thirty claimers, claimers are
# SUPPOSED to sit inside Postgres waiting, and the pool can fill with them. A short
# timeout would turn the effect being measured into a crash instead of a number.
HTTP_TIMEOUT_S = 180

# Requested gap between `pg_stat_activity` snapshots. The REAL rate is far slower
# and is set by `docker exec` start-up, which is why nothing is derived from this
# constant: `samples_taken` and `observe_gap_max_s` say what actually happened.
OBSERVE_INTERVAL_S = 0.10

# Every claimer declares the same RAM. Nothing in the claim query filters on it at
# these settings (the seeded runs carry no learned floor), but it is fixed so the
# arms cannot differ by accident.
CLAIMER_RAM_MB = 8192


# --- Seeding ----------------------------------------------------------------


def seed_queue(runs: int) -> str:
    """Write one job and `runs` PENDING run rows straight into Postgres.

    Identical in shape to E2's and E2c's seed, deliberately: the same direct
    inserts (the submission path's cost has no business inside a contention
    measurement), the same three properties on the job — `target_node_ids` NULL so
    W4 pinning cannot cap a claimer, `resource_reqs` empty so `_eligible` passes
    for every claimer, `private` false so the W6b trust condition is satisfied —
    and the same strictly increasing `created_at`, which is the claim query's
    ORDER BY and therefore decides which rows each claimer reaches for."""
    job_id = str(uuid.uuid4())
    h.psql_exec(
        "INSERT INTO jobs (id, name, image, entrypoint, env, resource_reqs, "
        "target_node_ids, replicas, status, created_at, private) VALUES ("
        f"'{job_id}', 'claim-contention queue', 'fyp-dummy:latest', "
        "json_build_array('python', 'train.py'), '{}'::json, '{}'::json, NULL, "
        f"{runs}, 'PENDING', now(), false);"
    )
    # Ids generated in SQL so the seed stays two statements at any queue size.
    # 'cc000000-0000-4000-8000-' + 12 digits = 36 characters, the width of the id
    # column, and obviously ours when read in pgAdmin.
    h.psql_exec(
        "INSERT INTO runs (id, job_id, node_id, status, attempt, lease_expires_at, "
        "retries_remaining, exit_code, started_at, finished_at, created_at, "
        "escalation_count) SELECT "
        "'cc000000-0000-4000-8000-' || lpad(g::text, 12, '0'), "
        f"'{job_id}', NULL, 'PENDING', 0, NULL, 1, NULL, NULL, NULL, "
        "now() - interval '1 hour' + (g * interval '1 millisecond'), 0 "
        f"FROM generate_series(1, {runs}) g;"
    )
    return job_id


def register_claimers(n: int, capacity: int) -> list[dict]:
    """Register n nodes through the ordinary public register endpoint.

    These are not agents. Nothing runs a container, nothing reports a run as
    RUNNING and nothing ever finishes. They exist to hold a node token and a
    capacity, which is all the claim query looks at — and that is the point: the
    thing under test is the claim, not the dispatch."""
    claimers: list[dict] = []
    # The harness's session, so registration verifies against the project's
    # own authority exactly as every other campaign call does. A bare
    # `requests.post` here used requests' default certifi bundle, which does
    # not carry certs/ca.pem, and failed CERTIFICATE_VERIFY_FAILED against
    # the HTTPS-only control plane.
    reg = h.session()
    for i in range(n):
        resp = reg.post(
            f"{h.API}/agent/register",
            json={
                "name": f"cc-{i:02d}",
                "specs": {
                    "cpu_cores": 8,
                    "has_gpu": False,
                    "ram_mb": CLAIMER_RAM_MB,
                    "capacity": capacity,
                    "agent_version": "claim-contention-bench",
                },
            },
            timeout=30,
        )
        if resp.status_code != 200:
            raise h.HarnessError(
                f"register failed for cc-{i:02d} ({resp.status_code}): "
                f"{resp.text[:200]}"
            )
        claimers.append({"name": f"cc-{i:02d}", **resp.json()})
    return claimers


# --- The observer: connections and lock waits, asked of Postgres ------------
#
# One aggregate row per snapshot rather than one row per backend. This experiment
# does not need to know WHICH backend was blocked — E2c answered that question with
# a purpose-built instrument — only how many connections existed and whether anyone
# was waiting on a lock at all.

_ACTIVITY_SQL = (
    "SELECT extract(epoch FROM statement_timestamp())::float8 AS observed_epoch_s, "
    # Every connection to the fyp database. This is the number that carries the
    # pool claim, and the reason it can: a pool connection stays OPEN (state
    # 'idle') after the burst that created it, so even a late snapshot still counts
    # it. It also counts anything else connected to fyp — pgAdmin, a stray psql —
    # which is why the campaign requires the demo tools to be down and why
    # `other_containers` is recorded in the manifest.
    "count(*)::int AS conns_total, "
    # Currently executing a statement: how many requests are inside the database at
    # this instant. A LOWER BOUND, because the sampler is slower than a claim.
    "count(*) FILTER (WHERE a.state = 'active')::int AS conns_active, "
    # Waiting for another backend, by the database's own account. Non-zero means a
    # lock wait happened. Zero means none was SEEN, which at this sampling rate is
    # weaker than "none happened" — stated, not glossed.
    "count(*) FILTER (WHERE cardinality(pg_blocking_pids(a.pid)) > 0)::int "
    "  AS blocked_backends "
    "FROM pg_stat_activity a "
    "WHERE a.datname = 'fyp' AND a.pid <> pg_backend_pid()"
)


class Observer(threading.Thread):
    """Poll `pg_stat_activity` for the whole measured phase and keep every snapshot.

    Started before the empty control round and stopped after the last claim round,
    so it covers both — the connection high-water mark is a property of the
    concurrency rather than of which round was running, and covering both is what
    lets the control round's reading be compared with the claim round's."""

    def __init__(self) -> None:
        super().__init__(name="cc-observer", daemon=True)
        self._stop = threading.Event()
        self.samples: list[dict] = []
        self.failures = 0

    def run(self) -> None:
        while not self._stop.is_set():
            try:
                rows = h.psql_json(_ACTIVITY_SQL)
                if rows:
                    self.samples.append(rows[0])
            except Exception:                        # noqa: BLE001 - a missed sample
                self.failures += 1                   # is a gap, never a result
            self._stop.wait(OBSERVE_INTERVAL_S)

    def stop(self) -> None:
        self._stop.set()
        # `.ident` is None until `start()`; a failure before that point has no
        # thread to join, and its own traceback is the diagnostic that matters.
        if self.ident is not None:
            self.join(timeout=30)


def read_activity(samples: list[dict]) -> dict:
    """Turn the snapshots into the three numbers the pool/lock separation needs."""
    conns = [int(s["conns_total"]) for s in samples if s.get("conns_total") is not None]
    active = [int(s["conns_active"]) for s in samples if s.get("conns_active") is not None]
    blocked = [
        int(s["blocked_backends"]) for s in samples
        if s.get("blocked_backends") is not None
    ]
    stamps = [
        float(s["observed_epoch_s"]) for s in samples
        if s.get("observed_epoch_s") is not None
    ]
    gaps = [b - a for a, b in zip(stamps, stamps[1:])]
    return {
        # Robust to a slow sampler — see the comment on conns_total in the SQL.
        "db_conns_max": max(conns) if conns else None,
        # Lower bounds, both of them.
        "db_active_max": max(active) if active else None,
        "blocked_backends_max": max(blocked) if blocked else None,
        "blocked_samples": sum(1 for b in blocked if b > 0),
        "samples_taken": len(samples),
        # The sampler's own resolution, measured rather than assumed. This
        # experiment can only prove the absence of lock waits LONGER than this.
        "observe_gap_max_s": max(gaps) if gaps else None,
        "observe_span_s": (stamps[-1] - stamps[0]) if len(stamps) > 1 else 0.0,
    }


# --- One simultaneous round -------------------------------------------------


def fire_round(claimers: list[dict], sessions: list[requests.Session]) -> list[dict]:
    """Send one heartbeat per claimer, all released at the same instant.

    Deliberately a copy of E2's and E2c's round rather than an import of one: an
    experiment has to be readable end to end on its own, and a change made to E2
    for E2's reasons must never silently change what this measured.

    The barrier is the load-bearing part. Without it, thread 1 would finish before
    thread N had been created, and "thirty simultaneous claimers" would really be
    thirty polite queued ones — with no contention of any kind to measure. How
    close they actually got is measured rather than assumed: `release_skew_ms` on
    every recorded row is max-minus-min of the release instants.

    Threads rather than asyncio because `requests` blocks; the interpreter releases
    its lock while a socket waits, so N threads really do put N requests in flight,
    which is what makes the pool ceiling reachable at all."""
    n = len(claimers)
    gate = threading.Barrier(n)
    # Pre-filled with a complete "this claimer never answered" shape, so a thread
    # that hangs past its join still leaves a readable row rather than a hole the
    # counting code would silently read as zero damage.
    out: list[dict] = [
        {"released": None, "latency_ms": None, "run_ids": [], "error": "no result"}
        for _ in range(n)
    ]

    def fire(i: int) -> None:
        claimer, sess = claimers[i], sessions[i]
        # Everything that can be prepared is prepared BEFORE the barrier, so the
        # only work between "released" and "request sent" is the send itself.
        body = {"node_id": claimer["node_id"], "status": "idle", "running": []}
        headers = {"Authorization": f"Bearer {claimer['token']}"}
        try:
            gate.wait(timeout=120)
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
        threading.Thread(target=fire, args=(i,), name=f"cc-claimer-{i}")
        for i in range(n)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=HTTP_TIMEOUT_S + 30)
    return out


def _percentile(values: list[float], pct: float) -> float | None:
    """Nearest-rank, deliberately not interpolated, so the number printed is a
    value actually observed. `*_latency_n` on every row says how many samples it
    came from, so nobody reads more precision into it than it has."""
    if not values:
        return None
    ordered = sorted(values)
    rank = math.ceil(pct * len(ordered)) - 1
    return ordered[max(0, min(rank, len(ordered) - 1))]


def _latency_block(prefix: str, result: list[dict]) -> dict:
    """Median, spread, p95 and n for one round, under one metric-name prefix."""
    values = [
        item["latency_ms"] for item in result
        if item.get("latency_ms") is not None and not item.get("error")
    ]
    return {
        f"{prefix}_latency_median_ms": statistics.median(values) if values else None,
        f"{prefix}_latency_min_ms": min(values) if values else None,
        f"{prefix}_latency_max_ms": max(values) if values else None,
        f"{prefix}_latency_p95_ms": _percentile(values, 0.95),
        f"{prefix}_latency_n": len(values),
        # Kept raw so the whole distribution can be re-aggregated later without
        # re-running the campaign. `summarize` ignores non-numeric values, so a
        # list never reaches a published table by itself.
        f"{prefix}_latencies_ms": [round(v, 3) for v in values],
    }


# --- Reading the damage -----------------------------------------------------


_FINAL_STATE_SQL = (
    "SELECT count(*)::int AS runs_total, "
    "count(*) FILTER (WHERE status = 'PENDING')::int AS runs_pending, "
    "count(*) FILTER (WHERE status = 'ASSIGNED')::int AS runs_assigned, "
    "count(DISTINCT node_id)::int AS owner_nodes, "
    "coalesce(max(attempt), 0)::int AS attempt_max "
    "FROM runs"
)


class DoubleAssignment(RuntimeError):
    """`skip_locked` handed one run to two claimers.

    Its own exception type because it is not a bench fault and must never be
    handled like one: it falsifies FR-8, and prediction P1 says the campaign stops
    at the first occurrence rather than carrying on to see whether it repeats."""


def _count_handouts(rounds: list[list[dict]], claimers: list[dict]) -> dict:
    """Who was handed what, counted from the HTTP RESPONSES rather than the database.

    The database alone under-counts a double assignment and always will: a run row
    holds ONE `node_id`, so however many claimers were told they own it, the row can
    only remember the last writer. Each response, by contrast, is a promise the
    control plane made to one claimer, and the gap between `assignments_handed_out`
    and `db_runs_assigned` is exactly the damage the database cannot show."""
    owners: dict[str, set[str]] = {}
    round1_owners: dict[str, set[str]] = {}
    handed_out = 0
    errors = 0
    empty_handed = 0
    for idx, result in enumerate(rounds):
        for i, item in enumerate(result):
            if item.get("error"):
                errors += 1
            elif idx == 0 and not item["run_ids"]:
                empty_handed += 1
            for run_id in item["run_ids"]:
                handed_out += 1
                owners.setdefault(run_id, set()).add(claimers[i]["name"])
                if idx == 0:
                    round1_owners.setdefault(run_id, set()).add(claimers[i]["name"])
    return {
        "assignments_handed_out": handed_out,
        "distinct_runs_handed": len(owners),
        "duplicate_handouts": handed_out - len(owners),
        "double_assigned_runs": sum(1 for v in owners.values() if len(v) > 1),
        # The single simultaneous release, separated out: the round every latency
        # in the headline comes from, and the cleanest statement of the question.
        "round1_double_assigned_runs": sum(
            1 for v in round1_owners.values() if len(v) > 1
        ),
        "round1_distinct_runs_handed": len(round1_owners),
        # Round 1 only. A claimer that got nothing in round 1 either found every
        # candidate locked or found the queue empty; at the default queue size it
        # should be zero, and a non-zero is the tell that the seed and the
        # scheduler's over-fetch have drifted apart.
        "empty_handed_responses": empty_handed,
        "errors": errors,
        "doubled_run_ids": sorted(
            run_id for run_id, v in owners.items() if len(v) > 1
        )[:10],
    }


def run_once(*, mode: str, n: int, rep: int, runs: int, capacity: int,
             pool: dict, dry: bool) -> dict:
    """One (arm, concurrency, repetition).

    The order of the steps is the measurement, all of it:

      1. reset and register, so every repetition starts from an identical platform;
      2. a DISCARDED warm-up round against an empty queue — it opens each thread's
         TCP connection and gets the heartbeat route and the claim query's plan
         into cache, none of which is the thing being compared;
      3. start the observer, then the MEASURED empty-queue control round: the same
         N claimers released at one instant with no row to lock, no row to update
         and no claim to commit. This is instrument 1 of the pool/lock separation;
      4. seed the queue and fire the measured claim round — the same N claimers,
         the same simultaneous release, now with work to take;
      5. further rounds until one hands out nothing, to catch a double assignment
         a later round could produce and to confirm capacity stops the claimers;
      6. read the database's own final state and stop the observer."""
    h.reset_platform()
    claimers = register_claimers(n, capacity)
    # h.session(), not requests.Session(): same trust path as every other
    # call, so the claimers verify against certs/ca.pem like the harness does.
    sessions = [h.session() for _ in claimers]
    observer = Observer()
    try:
        fire_round(claimers, sessions)               # warm-up, discarded

        observer.start()
        empty = fire_round(claimers, sessions)       # instrument 1: the control

        seed_queue(runs)

        rounds: list[list[dict]] = []
        for _ in range(MAX_ROUNDS):
            result = fire_round(claimers, sessions)
            rounds.append(result)
            if sum(len(item["run_ids"]) for item in result) == 0:
                break                                # nobody got anything: done
        observer.stop()

        activity = read_activity(observer.samples)
        handouts = _count_handouts(rounds, claimers)
        db = h.psql_json(_FINAL_STATE_SQL)[0]

        # Same tell as E2, E2b and E2c: a run that went through a lease lapse and a
        # requeue carries attempt >= 2, and a later re-claim of it would be CORRECT
        # while looking exactly like a double assignment. Flagged, never silently
        # corrected — a flagged repetition is one a red-team pass should throw out.
        interference = 1 if db["attempt_max"] > 1 else 0
        if interference:
            print(f"    !! reaper interference in {mode}@{n} rep {rep} "
                  f"(max attempt {db['attempt_max']}) - treat this row as suspect")

        h.assert_stable(where=f"{mode}@{n} rep {rep}")

        claim_block = _latency_block("claim", rounds[0])
        empty_block = _latency_block("empty", empty)
        claim_med = claim_block["claim_latency_median_ms"]
        empty_med = empty_block["empty_latency_median_ms"]
        releases = [
            item["released"] for item in rounds[0] if item.get("released") is not None
        ]
        ceiling = pool.get("ceiling")

        metrics = {
            # --- conditions, on every row so `summarize` can group and a jury can
            #     ask what a number was taken under
            "claim_mode": mode,
            "concurrency": n,
            "capacity": capacity,
            "runs_seeded": runs,
            # --- THE HEADLINE: correctness, counted from the HTTP responses
            **handouts,
            # --- cost: the measured claim round, one simultaneous release
            **claim_block,
            # --- instrument 1: the same release against an EMPTY queue. Same HTTP
            #     path, same pool checkout, same claim query, nothing to lock.
            **empty_block,
            # The separation number. Claim minus empty at the SAME concurrency in
            # the SAME repetition, so the pool's cost and the web framework's cost
            # are subtracted out by construction rather than by argument. It is a
            # difference of medians and not a median of differences: the claimers
            # are not paired across rounds, so no per-claimer subtraction would
            # mean anything.
            "latency_excess_median_ms": (
                claim_med - empty_med
                if claim_med is not None and empty_med is not None else None
            ),
            # --- instrument 3: what Postgres itself saw
            **activity,
            "sample_failures": observer.failures,
            # --- the pool ceiling, READ off the live engine (not inferred from a
            #     library's source), carried on every row so the bound travels with
            #     the number it bounds
            "pool_size": pool.get("pool_size"),
            "pool_max_overflow": pool.get("max_overflow"),
            "pool_ceiling": ceiling,
            # Instrument 2, as a flag: below the ceiling a pool wait cannot occur,
            # so any growth at that level is not the pool.
            "above_pool_ceiling": (
                1 if (ceiling is not None and n > ceiling) else 0
            ),
            # Did the pool actually reach its ceiling? A 1 here is an observation;
            # a 0 is only "not seen", because db_conns_max is a maximum over
            # snapshots taken by a sampler slower than the burst.
            "pool_saturated": (
                1 if (activity["db_conns_max"] is not None and ceiling is not None
                      and activity["db_conns_max"] >= ceiling) else 0
            ),
            # --- what the database ended up holding (server-side truth)
            "db_runs_total": db["runs_total"],
            "db_runs_assigned": db["runs_assigned"],
            "db_runs_pending": db["runs_pending"],
            "db_owner_nodes": db["owner_nodes"],
            "db_attempt_max": db["attempt_max"],
            "reaper_interference": interference,
            "rounds_fired": len(rounds),
            # --- proof that "simultaneous" was really simultaneous. Host clock,
            #     and it has to be: it measures when THIS PROCESS released its own
            #     threads. A control on the setup, never a result.
            "release_skew_ms": (
                (max(releases) - min(releases)) * 1000.0 if len(releases) > 1 else 0.0
            ),
        }
        row = h.record(EXP, f"{mode}@{n}", rep, metrics, dry=dry)

        # P1's stop rule, enforced in code rather than left to whoever reads the
        # summary afterwards. The row is written FIRST, so the evidence of the
        # failure is on disk before anything raises.
        if mode == "skip_locked" and handouts["double_assigned_runs"] > 0 and not dry:
            raise DoubleAssignment(
                f"{mode}@{n} rep {rep}: {handouts['double_assigned_runs']} run(s) "
                f"handed to more than one claimer - "
                f"{handouts['doubled_run_ids']}. This falsifies FR-8. The row is "
                "in raw.jsonl. Per the written prediction the campaign stops here "
                "and the lead is told before anything else is written."
            )
        return row
    finally:
        observer.stop()
        for sess in sessions:
            sess.close()


# --- Driver -----------------------------------------------------------------


SUMMARY_METRICS = [
    # Correctness first: it is the headline and it belongs at the top of the file
    # a jury reads.
    "double_assigned_runs",
    "round1_double_assigned_runs",
    "duplicate_handouts",
    "assignments_handed_out",
    "distinct_runs_handed",
    "empty_handed_responses",
    # Cost.
    "claim_latency_median_ms",
    "claim_latency_p95_ms",
    "claim_latency_max_ms",
    "empty_latency_median_ms",
    "latency_excess_median_ms",
    # The pool/lock separation.
    "db_conns_max",
    "db_active_max",
    "blocked_backends_max",
    "pool_saturated",
    "observe_gap_max_s",
    "samples_taken",
    # Server-side truth and the bench's own health.
    "db_runs_assigned",
    "db_runs_pending",
    "db_owner_nodes",
    "rounds_fired",
    "release_skew_ms",
    "errors",
    "reaper_interference",
]

PREDICTION = {
    "P1_headline": (
        "double_assigned_runs = 0 in every skip_locked repetition at every "
        "concurrency (1, 10, 20, 30 x 10 reps); distinct_runs_handed equals "
        "assignments_handed_out in each. IF IT FAILS: FR-8 is falsified, the "
        "campaign stops at the first occurrence, nothing is re-run to see whether "
        "it goes away, and the lead is told before anything else is written."
    ),
    "P2_control": (
        "naive double-assigns at 10, 20 and 30 (double_assigned_runs >= 1). IF IT "
        "FAILS: the control did not do its job, P1's zero cannot be told apart "
        "from 'nothing was contended', and the write-up says so instead of "
        "quoting the zero as evidence."
    ),
    "P3_pool_not_lock": (
        "latency grows with concurrency, and beyond the pool ceiling most of the "
        "growth is the pool rather than the lock: latency_excess_median_ms (claim "
        "minus the empty-queue control at the same concurrency) stays roughly flat "
        "across 10, 20 and 30 rather than climbing with them. IF IT FAILS with "
        "blocked_backends_max = 0, the extra cost has no mechanism we can name and "
        "is published as unexplained, NOT attributed to the pool. IF IT FAILS with "
        "blocked_backends_max > 0 under skip_locked, a claimer waited behind "
        "another claimer under our own lock, which contradicts E2c, and D6 "
        "publishes both results side by side."
    ),
    "P4_ceiling": (
        "db_conns_max never exceeds pool_ceiling, reaches it at 20 and 30, and "
        "stays at or below 10 at concurrency 10. IF IT EXCEEDS the ceiling, the "
        "pool explanation the report gives for E2c's plateau at fifteen is wrong "
        "and that published paragraph is corrected. IF IT DOES NOT REACH the "
        "ceiling at 30, the sampler missed the peak, db_conns_max is reported as "
        "the lower bound it is, and the separation rests on the empty-queue "
        "control and the below-ceiling rung alone."
    ),
    "P5_blocking": (
        "blocking at 30 is markedly slower than skip_locked at 30 and is the arm "
        "most likely to record errors, because a blocked claimer holds its pool "
        "connection while it waits. Not a headline; recorded because a jury asks."
    ),
}


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--arms", nargs="+", choices=list(ARMS), default=list(ARMS),
                   help="which claim strategies to measure. skip_locked is ours; "
                        "naive is the control that must double-assign for the "
                        "headline zero to mean anything")
    p.add_argument("--concurrency", nargs="+", type=int, default=list(CONCURRENCY),
                   help="how many claimers heartbeat at the same instant")
    p.add_argument("--reps", type=int, default=REPS,
                   help="repetitions per (arm, concurrency)")
    p.add_argument("--runs", type=int, default=None,
                   help="PENDING runs seeded per repetition. Default is "
                        "concurrency x 4, the smallest queue at which every "
                        "claimer can lock its four candidates and take exactly "
                        "one - read 'ONE PLACE THE BRIEF IS WRONG' in the header "
                        "before setting this to 30")
    p.add_argument("--capacity", type=int, default=CAPACITY,
                   help="per-node capacity; one run per heartbeat by default")
    p.add_argument("--dry", action="store_true",
                   help="rehearsal: 2 reps at one concurrency level, every row dry")
    p.add_argument("--machine-idle", action=argparse.BooleanOptionalAction,
                   default=None, help="was the machine otherwise idle (manifest)")
    p.add_argument("--mains-power", action=argparse.BooleanOptionalAction,
                   default=None, help="was it on mains, not battery (manifest)")
    p.add_argument("--fresh", action="store_true",
                   help="archive an existing raw.jsonl and start clean; without it "
                        "a run that would append onto measured rows stops instead "
                        "(the old file is renamed, never deleted)")
    p.add_argument("--exp-label", default=EXP,
                   help="results directory under docs/evidence/experiments/")
    p.add_argument("--lease-ttl-s", type=int, default=LONG_LEASE_TTL_S,
                   help="lease TTL the control plane runs with. Raised far above a "
                        "repetition's length so the reaper cannot requeue a claimed "
                        "run mid-repetition and make a correct re-claim look like a "
                        "double assignment")
    return p.parse_args()


def main() -> int:
    global EXP
    args = _parse_args()
    # Set before anything writes: `run_once` reads this module global on every
    # `h.record`, so rebinding it here routes the whole invocation's output.
    EXP = args.exp_label
    h.require_ready()
    if not args.dry:
        # A dirty machine is gated BEFORE the contamination guard, because a
        # campaign refused for its conditions should never have touched the
        # evidence directory at all. Dry rehearsals are exempt: they prove the
        # script runs end to end and are excluded from every summary, so their
        # conditions never travel with a published number.
        h.require_clean_machine()
        # Contamination guard plus clean-repository stamp: refuses to append onto
        # measured rows, and warns out loud, before any measuring, if the tree that
        # produced these numbers is uncommitted.
        h.guard_fresh(EXP, args.fresh)

    # Read ONCE, before any measuring, off the live engine inside the running
    # container - and carried onto every row. The whole pool/lock separation rests
    # on this number, so it is an observation with a timestamp rather than a
    # constant somebody remembered.
    pool = h.pool_config()
    ceiling = pool.get("ceiling")
    print(f"[cc] connection pool read off the live engine: {pool['impl']} "
          f"size={pool['pool_size']} overflow={pool['max_overflow']} "
          f"ceiling={ceiling}")
    if ceiling is None:
        print("[cc] WARNING: the pool could not be read. The campaign will still "
              "run, but pool_ceiling is MISSING on every row and the "
              "pool-versus-lock separation rests on the empty-queue control round "
              "and the below-ceiling rung alone.", file=sys.stderr)

    concurrency = list(args.concurrency)
    reps = args.reps
    if args.dry:
        # A rehearsal proves the script runs end to end; it is not evidence, and
        # every row it writes is excluded from every summary and chart.
        concurrency = concurrency[:1]
        reps = 2

    print(f"[cc] arms={args.arms} concurrency={concurrency} reps={reps} "
          f"runs={args.runs if args.runs else 'concurrency x 4'} "
          f"capacity={args.capacity} lease_ttl_s={args.lease_ttl_s} dry={args.dry}")

    started = time.monotonic()
    modes_used: list[dict] = []
    # Set only if P1's stop rule fires. The campaign then still writes its
    # summary and manifest before returning non-zero: a falsification of
    # FR-8 is the most important result this experiment can produce, and a
    # result without the conditions it was taken under is an anecdote.
    falsified: str | None = None
    try:
        # Repetitions on the OUTSIDE, arms on the inside - same as E2, E2b and E2c,
        # and for the same reason: a machine drifts over a long campaign, and
        # running all of one arm's repetitions before the next arm's would hand
        # that drift to one arm as though it were a property of the locking
        # strategy.
        for rep in range(1, reps + 1):
            for mode in args.arms:
                # The long lease rides on every set_mode call, so it is re-asserted
                # inside the container after each restart rather than assumed to
                # have persisted across one.
                got = h.set_mode(claim=mode,
                                 env={"LEASE_TTL_S": str(args.lease_ttl_s)})
                if got not in modes_used:
                    modes_used.append(got)
                for n in concurrency:
                    runs = args.runs if args.runs else n * CANDIDATE_MULTIPLIER
                    row = run_once(mode=mode, n=n, rep=rep, runs=runs,
                                   capacity=args.capacity, pool=pool, dry=args.dry)
                    med = row["claim_latency_median_ms"]
                    emed = row["empty_latency_median_ms"]
                    exc = row["latency_excess_median_ms"]
                    gap = row["observe_gap_max_s"]
                    print(
                        f"    {mode:>11}@{n:<3} rep {rep}: "
                        f"double={row['double_assigned_runs']:>3} "
                        f"handed={row['assignments_handed_out']:>4} "
                        f"distinct={row['distinct_runs_handed']:>4} "
                        f"empty_handed={row['empty_handed_responses']:>3} "
                        f"claim_med={'%.1f' % med if med is not None else 'na':>8}ms "
                        f"ctrl_med={'%.1f' % emed if emed is not None else 'na':>8}ms "
                        f"excess={'%.1f' % exc if exc is not None else 'na':>8}ms "
                        f"conns_max={row['db_conns_max']}/{ceiling} "
                        f"blocked_max={row['blocked_backends_max']} "
                        f"gap={'%.2f' % gap if gap is not None else 'na'}s "
                        f"rounds={row['rounds_fired']} err={row['errors']}"
                    )
    except DoubleAssignment as exc:
        # Not a bench fault. Printed at the top of the operator's screen, in the
        # words the prediction uses, so nobody has to go looking for what it means.
        print("\n" + "=" * 78, file=sys.stderr)
        print("STOP - PREDICTION P1 FALSIFIED. FR-8 SAYS NO RUN RESULT IS ACCEPTED "
              "IN DUPLICATE.", file=sys.stderr)
        print(str(exc), file=sys.stderr)
        print("=" * 78 + "\n", file=sys.stderr)
        falsified = str(exc)
    finally:
        # Never leave the platform on a weakened claim strategy, even after a crash.
        h.set_mode()
        print(f"[cc] elapsed {(time.monotonic() - started) / 60.0:.1f} min")

    h.summarize(
        EXP, metrics=SUMMARY_METRICS,
        title=f"{EXP} - thirty claimers at one instant: does any run go out "
              "twice, and what does a claim cost?",
    )
    h.chart(
        EXP, metric="claim_latency_median_ms",
        title=f"{EXP} - claim latency under simultaneous claimers (median, min-max)",
    )
    h.manifest(
        EXP,
        reps=reps,
        modes=modes_used,
        machine_idle=args.machine_idle,
        mains_power=args.mains_power,
        notes=(
            "Thirty claimers released at one instant against one queue - the "
            "measured evidence for NFR-2 and the answer to the jury attack "
            "'three workers on one laptop prove nothing about thirty'. Two "
            "questions: does any run go out to more than one claimer (FR-8), and "
            "what does a claim cost at 1, 10, 20 and 30. Correctness is counted "
            "from the HTTP RESPONSES, not the database: a run row holds one "
            "node_id, so however many claimers were promised a run, the row can "
            "only remember the last writer. THE POOL BOUND, STATED FIRST: the "
            "control plane's connection pool is five plus ten of overflow, read "
            "off the live engine inside the running container and carried on "
            "every row, so above fifteen concurrent requests the sixteenth and "
            "later wait for a CONNECTION, not for a row lock. Reporting that as "
            "lock contention is the error E2 made and it cost this project its "
            "whole locking finding. Three instruments separate them: (1) an "
            "empty-queue control round fired at the same concurrency in the same "
            "repetition, which pays the identical HTTP path and pool checkout "
            "with no row to lock - latency_excess_median_ms is the claim round "
            "minus that control; (2) the rung at ten claimers, below the ceiling, "
            "where a pool wait CANNOT occur; (3) pg_stat_activity, where a lock "
            "wait is visible (blocked_backends_max, via pg_blocking_pids) and a "
            "pool wait is not, because it happens outside the database. What none "
            "of them does, stated rather than implied: no per-request split of "
            "pool against lock - the pool's checkout wait is not instrumented in "
            "the control plane and instrumenting it is product code - so the "
            "pool's contribution is a bound from the control round and never a "
            "per-request millisecond figure. Limits: thirty claim requests "
            "against one control plane on one laptop, not thirty machines on a "
            "network; no containers run, so this measures the claim and not the "
            "dispatch; the sampler goes through docker exec, so db_active_max and "
            "blocked_backends_max are LOWER BOUNDS and observe_gap_max_s says by "
            "how much, while db_conns_max is robust because a pool connection "
            "stays open after the burst that created it. The naive arm is a "
            "control, not a candidate: it shows the headline zero means 'the lock "
            "worked' and not 'nothing was contended'. The queue is seeded at "
            "concurrency x 4 rather than the brief's flat thirty because the "
            "scheduler over-fetches four candidates per claimer, and a thirty-row "
            "queue would starve two thirds of the claimers in the first round and "
            "make the latency median a blend of real claims and empty-handed "
            "responses; runs_seeded is on every row. The control plane ran with "
            f"LEASE_TTL_S={args.lease_ttl_s} throughout so the reaper cannot "
            "requeue a claimed run mid-repetition and make a correct re-claim "
            "look like a double assignment; reaper_interference stays on as the "
            "proof that it did not, and is reported even when it is zero."
        ),
        extra={
            "arms": args.arms,
            "concurrency_levels": concurrency,
            "runs_seeded_per_rep": (
                args.runs if args.runs else "concurrency x 4 (over-fetch factor)"
            ),
            "capacity_per_node": args.capacity,
            "candidate_multiplier_mirrored": CANDIDATE_MULTIPLIER,
            "max_rounds": MAX_ROUNDS,
            "observe_interval_s_requested": OBSERVE_INTERVAL_S,
            "http_timeout_s": HTTP_TIMEOUT_S,
            "claimer_ram_mb": CLAIMER_RAM_MB,
            # Read before any measuring, off the live engine object. Recorded here
            # as well as on every row: the bound must be readable without opening
            # raw.jsonl.
            "pool_config_read_before_campaign": pool,
            # Recorded explicitly as well as inside compose_overrides: a deliberate
            # departure from the shipped default, and a reader must not have to dig
            # through YAML to find it.
            "lease_ttl_s_override": args.lease_ttl_s,
            "lease_ttl_s_shipped_default": h.SHIPPED_LEASE_TTL_S,
            # The prediction travels in the results tree as well as in the script's
            # header and the evidence file, so the three can be compared without a
            # checkout.
            "prediction_written_before_first_run": PREDICTION,
            # Null on a completed campaign. A string here means P1 was
            # falsified and the campaign stopped at that repetition, so the
            # rows below it were never taken - a reader must not average
            # across a truncated run without knowing it was truncated.
            "stopped_early_on_double_assignment": falsified,
        },
    )
    print(f"\n[cc] results in {h.exp_dir(EXP)}: raw.jsonl, summary.md, chart.png, "
          "manifest.json")
    if falsified:
        print("[cc] STOPPED EARLY - prediction P1 falsified. The summary "
              "and manifest above describe a TRUNCATED campaign.",
              file=sys.stderr)
        return 2
    print("[cc] done - campaign complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
