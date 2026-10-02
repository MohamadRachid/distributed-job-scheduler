"""E2c — does a claimer ever WAIT behind another claimer? (brief §5; decision D6)

D6 chose `SELECT … FOR UPDATE SKIP LOCKED` over the plain blocking `FOR UPDATE`
for one stated property: **no claimer ever waits behind another claimer.** That
property has never been measured. E2/E2b measured a different thing — how fast
each lock drains a queue — and it came out the other way: from eight claimers
upward the BLOCKING arm drains faster. So the one number we have argues against
D6, and D6's own reason has so far been an argument rather than a measurement.
The report says exactly that in §5.5 limitation 4 and §5.3.4. E2c is the
measurement that closes it.

**The question, put as narrowly as it can be put:** while one claimer is holding
a row inside an open transaction, do the other claimers wait for it, or do they
step past it and come back empty-handed?

**How the wait is created.** A real claimer holds a row for the few milliseconds
its own claim takes — far too short to catch reliably. So E2c reproduces the
same database state deliberately: a holder session opens a transaction, runs
`SELECT … FOR UPDATE` with the SAME WHERE clause, the SAME ordering and the SAME
limit as `assign_runs`, and then sleeps inside that transaction for `--hold-s`
seconds. From every other session's point of view this is indistinguishable from
a claimer that is simply slow: the row carries the holder's transaction id in its
tuple header, which is the only thing a competing lock request can see. Nothing
about the claim query is modified to produce it.

**How the wait is detected — server side, and only server side.** Postgres is
asked directly, through `pg_blocking_pids()`: for every backend on the database,
which other backends is it waiting for? A claimer that has the holder's pid in
that array is, by the database's own account, waiting behind another claimer.
That is the whole measurement. Every timestamp comes from `statement_timestamp()`
inside Postgres, and every age is computed in SQL against the server's own
`state_change`; the host clock never touches a reported wait. (README rule:
"Timings come from the server." E2 departed from it with a stated reason; E2c
does not need to.)

**The three arms** (the `claim` switch, `control-plane/app/config.py`; the code
under test is `assign_runs` in `control-plane/app/scheduler.py`):

  * `skip_locked` — ours. Step past anything another claimer holds.
  * `blocking` — `SELECT … FOR UPDATE` with no SKIP LOCKED.
  * `naive` — no row lock on the read at all. A CONTROL, not a candidate: it is
    here to prove the measurement can tell "did not wait" apart from "did not
    lock", which is a distinction the headline number would otherwise hide.

Only the row lock differs. The WHERE clause, the ordering and the limit are
identical in all three arms — that is a property of `assign_runs` itself, not
something this script arranges — and the holder's own SELECT is written to match
it, so the row it holds is genuinely the row the claimers are reaching for.

--------------------------------------------------------------------------------
PREDICTION — written here BEFORE the script has ever been run, because a
prediction written after the result is not a prediction. It is recorded in the
commit that first adds this file, so the claim can be checked against the clock.
--------------------------------------------------------------------------------

  1. `blocking` — every claimer waits. At concurrency N,
     `blocked_claimers_max` = N and `blocked_wait_max_s` approaches `--hold-s`
     (20 s by default). When the holder finally lets go, ONE waiter takes the
     row and the rest find it already ASSIGNED, so `assignments_handed_out` = 1
     and `empty_handed_responses` = N-1.

  2. `skip_locked` — nobody waits. At every concurrency,
     `blocked_claimers_max` = 0 and `blocked_wait_max_s` = 0. Every claimer
     returns while the holder is still holding, and returns empty-handed:
     `assignments_handed_out` = 0, `empty_handed_responses` = N. The queue is
     NOT drained — `db_runs_pending` stays 1.

  3. `naive` — waits too, but later and for a different reason.
     `blocked_claimers_max` > 0, because although the read takes no lock, the
     UPDATE at commit must, and it lands on the same held row. That wait happens
     AFTER the response's assignment set has already been decided, so the run is
     promised to every claimer at once: `double_assigned_runs` = 1. Naive is
     evidence for nothing about D6; it is the control that shows a zero in
     prediction 2 means "stepped past", not "never locked".

  IF PREDICTION 2 FAILS. A non-zero `blocked_claimers_max` in `skip_locked`, at
  any concurrency level, falsifies the property D6 rests on. D6 then publishes
  that, in the block itself and not in a footnote: the reason we gave for
  choosing SKIP LOCKED is not a reason this measurement supports, and it sits
  beside E2b's finding that our arm drains slower from eight claimers upward.
  No re-run until it agrees, no softening of the wording, and the number gets
  the same prominence a confirming number would have had.

  IF PREDICTION 3 FAILS. `naive` returning `blocked_claimers_max` = 0 means the
  control did not do the one job it was added for: it would leave "nobody
  waited" and "nothing ever locked" indistinguishable, so prediction 2's zero
  would prove nothing on its own. The separation then rests entirely on
  `double_assigned_runs`, and the write-up says exactly that rather than quoting
  the zero as though the control had stood behind it.

  Prediction 2 is the one that matters. Note what 1 and 2 together say about
  cost: `skip_locked` buys its zero wait by leaving the row on the queue, while
  `blocking` pays the wait and gets the work. That is the same trade E2/E2b
  priced from the other side, and it is why both results can be true at once.

--------------------------------------------------------------------------------
WHAT E2c CANNOT SAY
--------------------------------------------------------------------------------

  * The holder is artificial and holds for seconds. A real claimer holds for
    milliseconds. E2c shows that the wait EXISTS and that its length is set by
    the holder's transaction — not that waits of this size occur in production.
    Read beside E2/E2b, whose blocking arm drains a real queue FASTER at eight
    claimers and up, the honest reading is that the waits are real but short,
    and that a short wait can be cheaper than the extra rounds SKIP LOCKED
    forces. E2c settles whether D6's property holds, not whether D6 was the
    better trade.
  * The sampler has a floor. Blocking is detected by polling
    `pg_stat_activity` through `docker exec`, so a wait shorter than the gap
    between two samples can be missed entirely. `observe_gap_max_s` and
    `samples_taken` are recorded on every row so that floor is a number rather
    than an assumption: E2c can only prove the absence of waits LONGER than
    `observe_gap_max_s`.
  * `client_rtt_max_ms` closes that gap from the other side, and is the only
    reason a host-clock number appears in this script at all. It is not the
    measurement and no claim rests on it: a claimer that had waited behind a
    20-second holder could not possibly have answered in milliseconds, so a
    small RTT corroborates a zero from the sampler even where the sampler is
    too coarse to see it. It is named `client_` throughout for that reason.

Run it (host, repo venv, nothing else touching the stack), one experiment at a
time, on an otherwise idle machine, on mains power:

    .venv\\Scripts\\python.exe scripts\\experiments\\e2c_claim_wait.py --dry
    .venv\\Scripts\\python.exe scripts\\experiments\\e2c_claim_wait.py --machine-idle --mains-power
"""

from __future__ import annotations

import argparse
import statistics
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))

import harness as h  # noqa: E402

EXP = "E2c"
ARMS = ("skip_locked", "blocking", "naive")
# The same ladder as E2/E2b, so the two results can be laid side by side without
# anyone having to interpolate. The claim under test is a per-claimer property,
# so it should hold at every level or fail at every level; a ladder makes that
# visible instead of assumed.
CONCURRENCY = (2, 4, 8, 16)
REPS = 5

# ONE pending run, and it is the row the holder takes. That is the whole point:
# with a single eligible row, a claimer that steps past the lock has nothing else
# to step onto, so "stepped past" and "came back empty-handed" are the same
# observable event and neither can be confused with "found other work". A wider
# queue asks a different question — how much work a claimer gets DESPITE the lock
# — and that question is not E2c's. `--runs` exists so a follow-up can ask it,
# not so this experiment's claim can quietly widen.
RUNS = 1

# One run per heartbeat. `assign_runs` computes spare = capacity - (runs held)
# and over-fetches spare x 4 candidates, so capacity 1 means each claimer wants
# exactly one row and looks at up to four. With one row seeded the limit never
# binds and every claimer is reaching for the same row — the only case in which
# the question "does it wait?" is even well posed.
CAPACITY = 1

# The over-fetch factor in `assign_runs` (`_CANDIDATE_MULTIPLIER`). Mirrored here
# only so the holder locks exactly what one real claimer would lock. At the
# default RUNS=1 that is one row: the single row the brief names.
CANDIDATE_MULTIPLIER = 4

# How long the holder keeps its transaction open. Long enough that the sampler
# gets tens of snapshots inside the window and that a blocked claimer is
# unmistakably blocked rather than merely slow; short enough that a full campaign
# is an evening rather than a night. Recorded on every row and in the manifest,
# because every wait this experiment can report is bounded by it.
HOLD_S = 20.0

# How long to wait for the holder to actually own the lock before giving up. A
# bench-health timeout, not a measurement.
HOLDER_READY_TIMEOUT_S = 30.0

# Minimum gap between two `pg_stat_activity` snapshots. The REAL rate is slower
# and is bounded by `docker exec` start-up, which is why nothing is derived from
# this constant: `samples_taken` and `observe_gap_max_s` say what actually
# happened, and they are what a reader should use.
OBSERVE_INTERVAL_S = 0.20

# Same as E2, same reason: a claimer here never finishes anything, so a run it
# claims sits ASSIGNED with a ticking lease. On the shipped 60 s lease the reaper
# would eventually requeue it, and a later re-claim — which is CORRECT — would be
# indistinguishable from a fault. Raising the lease past the length of a
# repetition removes the confusion at its source. `reaper_interference` stays on
# regardless, as the proof that it did not happen.
LONG_LEASE_TTL_S = 600

# Generous on purpose. In the `blocking` arm a claimer is SUPPOSED to sit inside
# Postgres for the whole hold window, and at high concurrency the control plane's
# connection pool (SQLAlchemy default: 5 + 10 overflow) can fill with blocked
# claimers. A short timeout would turn the very effect being measured into a
# crash instead of a number.
HTTP_TIMEOUT_S = 180

# Marked so the observer can pick the holder out of `pg_stat_activity` by a
# column rather than by guessing at query text.
HOLDER_APPNAME = "e2c-holder"

# Every claimer declares the same RAM, and the holder's SELECT is written against
# this same number, so the holder's WHERE clause and the claimers' WHERE clause
# select the same rows. If these ever drift apart, the holder would be locking a
# row the claimers were not asking for and every zero in the results would be an
# artefact of the instrument.
CLAIMER_RAM_MB = 8192


# --- Seeding ----------------------------------------------------------------


def seed_queue(runs: int) -> str:
    """Write one job and `runs` PENDING run rows straight into Postgres.

    Identical in shape to E2's seed and deliberately so: the same direct inserts
    (the submission path's cost has no business inside a locking measurement),
    the same three properties on the job — `target_node_ids` NULL so W4 pinning
    cannot cap a claimer, `resource_reqs` empty so `_eligible` passes for
    everyone, `private` false so the W6b trust condition is satisfied — and the
    same strictly increasing `created_at`, which is the claim query's ORDER BY.

    At the default RUNS=1 the ordering has nothing to order, and that is fine: it
    is kept identical to E2 so that widening the queue later changes one number
    and not the seed's meaning."""
    job_id = str(uuid.uuid4())
    h.psql_exec(
        "INSERT INTO jobs (id, name, image, entrypoint, env, resource_reqs, "
        "target_node_ids, replicas, status, created_at, private) VALUES ("
        f"'{job_id}', 'E2c held-row queue', 'fyp-dummy:latest', "
        "json_build_array('python', 'train.py'), '{}'::json, '{}'::json, NULL, "
        f"{runs}, 'PENDING', now(), false);"
    )
    # Ids generated in SQL so the seed is two statements regardless of queue size:
    # 'e2c00000-0000-4000-8000-' + 12 digits = 36 characters, the width of the id
    # column, and obviously ours when read in pgAdmin.
    h.psql_exec(
        "INSERT INTO runs (id, job_id, node_id, status, attempt, lease_expires_at, "
        "retries_remaining, exit_code, started_at, finished_at, created_at, "
        "escalation_count) SELECT "
        "'e2c00000-0000-4000-8000-' || lpad(g::text, 12, '0'), "
        f"'{job_id}', NULL, 'PENDING', 0, NULL, 1, NULL, NULL, NULL, "
        "now() - interval '1 hour' + (g * interval '1 millisecond'), 0 "
        f"FROM generate_series(1, {runs}) g;"
    )
    return job_id


def register_claimers(n: int, capacity: int) -> list[dict]:
    """Register n nodes through the ordinary public register endpoint.

    These are not agents — nothing runs a container and nothing reports a run as
    RUNNING. They exist to hold a node token and a capacity, which is all the
    claim query looks at. Nodes registered this way are UNTRUSTED, which is what
    makes the holder's WHERE clause below the correct branch to copy."""
    claimers: list[dict] = []
    for i in range(n):
        resp = requests.post(
            f"{h.API}/agent/register",
            json={
                "name": f"e2c-{i:02d}",
                "specs": {
                    "cpu_cores": 8,
                    "has_gpu": False,
                    "ram_mb": CLAIMER_RAM_MB,
                    "capacity": capacity,
                    "agent_version": "e2c-bench",
                },
            },
            timeout=30,
        )
        if resp.status_code != 200:
            raise h.HarnessError(
                f"register failed ({resp.status_code}): {resp.text[:200]}"
            )
        claimers.append({"name": f"e2c-{i:02d}", **resp.json()})
    return claimers


# --- The holder: one claimer, frozen mid-claim ------------------------------


def _holder_sql(hold_s: float, limit: int) -> str:
    """The holder's transaction, written to BE the claim query's own SELECT.

    Line for line against `assign_runs` for an UNTRUSTED node (which every node
    registered by `register_claimers` is):

      * `status = 'PENDING'`                     -> Run.status == PENDING
      * the `learned_min_ram_mb` pair            -> the W5c escalation condition
      * `job_id NOT IN (private jobs)`           -> the W6b trust condition, on
        the `may_run_private(node) is False` branch
      * `ORDER BY created_at`                    -> .order_by(Run.created_at)
      * `LIMIT spare * 4`                        -> .limit(spare * _CANDIDATE_MULTIPLIER)
        **as of the frozen measurement tree `1df69b1`. That window was removed on
        2026-09-06** -- it was head-of-line blocking, and the scheduler now walks the
        queue unlocked and locks only the rows it takes. This mirror is deliberately
        NOT updated: it describes the shape the published E2c numbers were measured
        against, and a mirror that silently followed the code would make those numbers
        impossible to read afterwards. A re-run measures a different scheduler and its
        numbers belong to a new tree.
      * `FOR UPDATE`                             -> .with_for_update()

    It has to match. A holder that locked some OTHER row would leave the claimers
    nothing to wait for, and every arm would then report zero waits — a clean
    null result manufactured entirely by the instrument.

    `pg_sleep` rather than a session held open from Python: the sleep runs INSIDE
    Postgres, so the hold window is timed by the server's clock and not the
    host's, and there is no socket to keep alive across a measurement. psql runs
    a multi-statement `-c` string as a single implicit transaction, so the row
    lock taken by the SELECT is held for the whole sleep and released when the
    string ends — no explicit BEGIN/COMMIT to leak if this process dies."""
    return (
        f"SET application_name = '{HOLDER_APPNAME}'; "
        "SELECT id FROM runs "
        "WHERE status = 'PENDING' "
        f"  AND (learned_min_ram_mb IS NULL OR learned_min_ram_mb < {CLAIMER_RAM_MB}) "
        "  AND job_id NOT IN (SELECT id FROM jobs WHERE private IS TRUE) "
        "ORDER BY created_at "
        f"LIMIT {limit} "
        "FOR UPDATE; "
        f"SELECT pg_sleep({hold_s:.3f});"
    )


class Holder(threading.Thread):
    """Runs the holding transaction for the length of the hold window.

    Shells out to psql the same way `harness.psql_exec` does — same container,
    same user, same database — but captures stdout, which `psql_exec` discards.
    E2c needs that output: the ids the SELECT returned are the server's own
    statement of WHICH rows were held, and that is evidence, not logging. It is
    read after the transaction has ended, so it costs nothing inside the window."""

    def __init__(self, hold_s: float, limit: int) -> None:
        super().__init__(name="e2c-holder", daemon=True)
        self.hold_s = hold_s
        self.limit = limit
        self.held_ids: list[str] = []
        self.error: str | None = None

    def run(self) -> None:
        try:
            proc = subprocess.run(
                [
                    "docker", "exec", "-i", h.PG_CONTAINER,
                    "psql", "-U", "fyp", "-d", "fyp", "-t", "-A",
                    "-c", _holder_sql(self.hold_s, self.limit),
                ],
                capture_output=True, text=True,
                timeout=self.hold_s + 120,
            )
            if proc.returncode != 0:
                self.error = proc.stderr.strip()[:300] or f"psql exit {proc.returncode}"
                return
            self.held_ids = [
                line.strip() for line in proc.stdout.splitlines()
                if line.strip() and line.strip() not in ("SET", "SELECT 1")
            ]
        except Exception as exc:                     # noqa: BLE001 - record, never crash
            self.error = f"{type(exc).__name__}: {exc}"[:300]


_HOLDER_STATE_SQL = (
    "SELECT a.pid AS pid, "
    "a.state AS state, "
    "a.wait_event_type AS wait_event_type, "
    "a.wait_event AS wait_event, "
    "extract(epoch FROM (statement_timestamp() - a.xact_start))::float8 AS xact_age_s, "
    "(SELECT count(*) FROM pg_locks l WHERE l.pid = a.pid AND l.granted "
    "  AND l.locktype = 'transactionid')::int AS xid_locks "
    "FROM pg_stat_activity a "
    f"WHERE a.datname = 'fyp' AND a.application_name = '{HOLDER_APPNAME}'"
)


def holder_state() -> list[dict]:
    return h.psql_json(_HOLDER_STATE_SQL)


def wait_for_holder(timeout_s: float) -> int:
    """Block until Postgres itself says the holder owns the lock; return its pid.

    Two conditions, both read off the server, and both required:

      * `wait_event = 'PgSleep'` — the holder has finished its SELECT and is
        inside the sleep. A holder still executing the SELECT has not necessarily
        taken anything yet.
      * at least one GRANTED `transactionid` lock — `SELECT … FOR UPDATE` writes
        the locking transaction's id into the tuple header, which forces a real
        xid to be assigned. Row locks themselves never appear in `pg_locks`, so
        this is the closest thing to a receipt the database will give — and it is
        a receipt: a session that locked nothing would not have one.

    This gate exists so that a page of zeros can never be published as "nobody
    waited" when the truth was "there was nothing to wait for". On timeout the
    repetition is stopped, not recorded."""
    deadline = time.monotonic() + timeout_s
    last = "no holder session"
    while time.monotonic() < deadline:
        rows = holder_state()
        if len(rows) > 1:
            raise h.HarnessError(
                f"{len(rows)} sessions are using the holder's application_name — "
                "a previous repetition did not clean up. Restart the stack and "
                "start this experiment again."
            )
        if len(rows) == 1:
            row = rows[0]
            if row["wait_event"] == "PgSleep" and row["xid_locks"] >= 1:
                return int(row["pid"])
            last = (f"state={row['state']} wait_event={row['wait_event']} "
                    f"xid_locks={row['xid_locks']}")
        time.sleep(0.1)
    raise h.HarnessError(
        f"the holder never took the row within {timeout_s:.0f}s ({last}). "
        "Refusing to measure: with no lock held, every arm would truthfully "
        "report zero waits and the result would mean nothing."
    )


def assert_no_stale_holder() -> None:
    """Contamination guard, E2c's own.

    A holder left alive by a crashed repetition would keep a row lock open into
    the next one — and `reset_platform`'s TRUNCATE would HANG behind it, which
    reads as a frozen bench rather than as contaminated data. Checked BEFORE the
    reset for exactly that reason."""
    rows = holder_state()
    if rows:
        raise h.HarnessError(
            f"{len(rows)} leftover holder session(s) are still open on Postgres "
            f"(pids {[r['pid'] for r in rows]}). A previous repetition did not "
            "finish. Restart the stack before measuring anything."
        )


# --- The observer: who is waiting for whom, asked of Postgres ---------------


_ACTIVITY_SQL = (
    "SELECT extract(epoch FROM statement_timestamp())::float8 AS observed_epoch_s, "
    "a.pid AS pid, "
    "a.application_name AS application_name, "
    "a.state AS state, "
    "a.wait_event_type AS wait_event_type, "
    "a.wait_event AS wait_event, "
    "pg_blocking_pids(a.pid) AS blocking_pids, "
    "extract(epoch FROM (statement_timestamp() - a.state_change))::float8 AS state_age_s, "
    "extract(epoch FROM (statement_timestamp() - a.query_start))::float8 AS query_age_s "
    "FROM pg_stat_activity a "
    "WHERE a.datname = 'fyp' AND a.pid <> pg_backend_pid()"
)


class Observer(threading.Thread):
    """Poll `pg_stat_activity` for the whole hold window and keep every snapshot.

    `pg_blocking_pids(pid)` is the entire measurement: it returns the backends
    that `pid` is waiting for. A claimer with the holder's pid in that array is
    waiting behind another claimer, by the database's own account — no inference
    from response times, no threshold, and no host clock.

    `statement_timestamp()` rather than `clock_timestamp()`: it is fixed for the
    whole statement, so every row in one snapshot shares a single server-side
    instant and the ages are all measured against the same reference.

    Snapshots are kept whole and aggregated afterwards, so a question nobody
    thought to ask during the run can still be put to the same data."""

    def __init__(self) -> None:
        super().__init__(name="e2c-observer", daemon=True)
        self._stop = threading.Event()
        self.samples: list[list[dict]] = []
        self.failures = 0

    def run(self) -> None:
        while not self._stop.is_set():
            try:
                self.samples.append(h.psql_json(_ACTIVITY_SQL))
            except Exception:                        # noqa: BLE001 - a missed sample
                self.failures += 1                   # is a gap, never a result
            self._stop.wait(OBSERVE_INTERVAL_S)

    def stop(self) -> None:
        self._stop.set()
        # `.ident` is None until `start()`. A failure BEFORE the observer was
        # started must surface its own traceback - `wait_for_holder` refusing to
        # measure is the diagnostic that matters most here, and a RuntimeError
        # from this cleanup path would stand in front of it.
        if self.ident is not None:
            self.join(timeout=30)


def read_blocking(samples: list[list[dict]], holder_pid: int) -> dict:
    """Turn the snapshots into the numbers a jury reads.

    `blocked_*` counts only backends blocked BY THE HOLDER, which is the D6
    property exactly. `waiters_any_max` counts every blocked backend whoever is
    blocking it — kept separate so that waiting caused by something else (two
    claimers queued behind each other once the holder lets go, say) can never be
    quietly folded into the headline number."""
    per_sample: list[int] = []
    waiters_any: list[int] = []
    waits: list[float] = []
    ever: set[int] = set()
    stamps: list[float] = []
    for sample in samples:
        blocked_here = 0
        any_here = 0
        for row in sample:
            blockers = row.get("blocking_pids") or []
            if blockers:
                any_here += 1
            if holder_pid in blockers:
                blocked_here += 1
                ever.add(int(row["pid"]))
                if row.get("state_age_s") is not None:
                    waits.append(float(row["state_age_s"]))
        per_sample.append(blocked_here)
        waiters_any.append(any_here)
        if sample and sample[0].get("observed_epoch_s") is not None:
            stamps.append(float(sample[0]["observed_epoch_s"]))

    # The sampler's own resolution, measured rather than assumed. E2c can only
    # prove the ABSENCE of waits longer than this.
    gaps = [b - a for a, b in zip(stamps, stamps[1:])]
    return {
        "blocked_claimers_max": max(per_sample) if per_sample else 0,
        "blocked_claimers_ever": len(ever),
        "blocked_wait_max_s": max(waits) if waits else 0.0,
        "blocked_samples": sum(1 for c in per_sample if c > 0),
        "waiters_any_max": max(waiters_any) if waiters_any else 0,
        "samples_taken": len(samples),
        "observe_gap_max_s": max(gaps) if gaps else None,
        "observe_span_s": (stamps[-1] - stamps[0]) if len(stamps) > 1 else 0.0,
        "blocked_counts": per_sample,
    }


# --- One simultaneous round -------------------------------------------------


def fire_round(claimers: list[dict], sessions: list[requests.Session]) -> list[dict]:
    """Send one heartbeat per claimer, all released at the same instant.

    Deliberately a copy of E2's round rather than an import of it. An experiment
    has to be readable end to end on its own, and — more to the point — a change
    made to E2 for E2's reasons must never silently change what E2c measured.

    The barrier is the part that matters. Without it, thread 1 would finish
    before thread N had been created, and "N simultaneous claimers" would really
    be N polite queued ones, with nothing for anyone to wait behind.
    `threading.Barrier(n)` makes every thread block until the last one arrives.
    How close they actually got is measured, not assumed: `release_skew_ms` on
    every recorded row is max-minus-min of the release instants.

    Threads rather than asyncio because `requests` is blocking; the GIL is
    released while a socket waits, so N threads really do have N requests in
    flight — which in the `blocking` arm means N backends really do sit inside
    Postgres at once."""
    n = len(claimers)
    gate = threading.Barrier(n)
    # Pre-filled with a complete "this claimer never answered" shape, so a thread
    # that hangs past its join still leaves a readable row instead of a hole the
    # counting code trips over. A hole would be recorded as zero damage.
    out: list[dict] = [
        {"released": None, "rtt_ms": None, "run_ids": [], "error": "no result"}
        for _ in range(n)
    ]

    def fire(i: int) -> None:
        claimer, sess = claimers[i], sessions[i]
        # Everything that can be prepared is prepared BEFORE the barrier, so the
        # only work between "released" and "request sent" is the send itself.
        body = {"node_id": claimer["node_id"], "status": "idle", "running": []}
        headers = {"Authorization": f"Bearer {claimer['token']}"}
        try:
            gate.wait(timeout=60)
        except threading.BrokenBarrierError:
            out[i] = {"released": None, "rtt_ms": None, "run_ids": [],
                      "error": "barrier broken"}
            return
        released = time.perf_counter()
        try:
            resp = sess.post(
                f"{h.API}/agent/heartbeat", json=body, headers=headers,
                timeout=HTTP_TIMEOUT_S,
            )
            rtt_ms = (time.perf_counter() - released) * 1000.0
            if resp.status_code != 200:
                out[i] = {"released": released, "rtt_ms": rtt_ms, "run_ids": [],
                          "error": f"HTTP {resp.status_code}"}
                return
            run_ids = [a["run_id"] for a in resp.json().get("assignments", [])]
            out[i] = {"released": released, "rtt_ms": rtt_ms,
                      "run_ids": run_ids, "error": None}
        except Exception as exc:                     # noqa: BLE001 - record, never crash
            out[i] = {"released": released,
                      "rtt_ms": (time.perf_counter() - released) * 1000.0,
                      "run_ids": [], "error": type(exc).__name__}

    threads = [
        threading.Thread(target=fire, args=(i,), name=f"e2c-claimer-{i}")
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
    "coalesce(max(attempt), 0)::int AS attempt_max "
    "FROM runs"
)


def run_once(*, mode: str, n: int, rep: int, runs: int, capacity: int,
             hold_s: float, dry: bool) -> dict:
    """One (arm, concurrency, repetition).

    The order of the steps is load-bearing, all of it:

      1. check for a leftover holder BEFORE the reset — a stale row lock would
         hang the TRUNCATE rather than fail it;
      2. reset, register, and warm up against an EMPTY queue, so that no measured
         request pays for a cold TCP connection or a cold query plan;
      3. seed, start the holder, and WAIT until Postgres confirms it owns the
         lock — the premise of the whole experiment, proven before it is used;
      4. start the observer, then release all N claimers at one instant;
      5. keep observing until the holder lets go, so the entire window in which a
         wait could occur is covered — not only the part the claimers were
         visibly inside."""
    assert_no_stale_holder()
    h.reset_platform()
    claimers = register_claimers(n, capacity)
    sessions = [requests.Session() for _ in claimers]
    holder = Holder(hold_s, capacity * CANDIDATE_MULTIPLIER)
    observer = Observer()
    try:
        fire_round(claimers, sessions)               # warm-up, empty queue
        seed_queue(runs)

        holder.start()
        holder_pid = wait_for_holder(HOLDER_READY_TIMEOUT_S)
        observer.start()

        result = fire_round(claimers, sessions)

        # The holder is joined BEFORE the observer is stopped, so the sampler
        # covers the whole hold window. In `blocking` the round above has already
        # spent most of it; in `skip_locked` the round returned in milliseconds
        # and this is where the remaining seconds of "did anyone quietly wait?"
        # actually get sampled.
        holder.join(timeout=hold_s + 90)
        if holder.is_alive():
            raise h.HarnessError(
                "the holder transaction did not end — its row lock is still open, "
                "and the next repetition's reset would hang behind it. Restart "
                "the stack."
            )
        observer.stop()

        if holder.error:
            raise h.HarnessError(f"the holder failed: {holder.error}")

        blocking = read_blocking(observer.samples, holder_pid)

        # --- what came back over HTTP -------------------------------------
        #
        # Counted from the responses, not the database, for the reason E2 sets
        # out: a run row holds ONE node_id, so however many claimers were told
        # they own it, the row can only remember the last writer. In the `naive`
        # arm that gap is the entire finding.
        owners: dict[str, set[str]] = {}
        rtts: list[float] = []
        releases: list[float] = []
        errors = 0
        handed_out = 0
        empty_handed = 0
        for i, item in enumerate(result):
            if item.get("error"):
                errors += 1
            elif not item["run_ids"]:
                empty_handed += 1
            if item.get("rtt_ms") is not None:
                rtts.append(item["rtt_ms"])
            if item.get("released") is not None:
                releases.append(item["released"])
            for run_id in item["run_ids"]:
                handed_out += 1
                owners.setdefault(run_id, set()).add(claimers[i]["name"])

        db = h.psql_json(_FINAL_STATE_SQL)[0]

        # Same tell as E2: a run that went through a lease lapse and a requeue
        # carries attempt >= 2, and a later re-claim of it would be CORRECT while
        # looking exactly like a fault. Flagged, never silently corrected — a
        # flagged repetition is one the red-team pass should throw out.
        interference = 1 if db["attempt_max"] > 1 else 0
        if interference:
            print(f"    !! reaper interference in {mode}@{n} rep {rep} "
                  f"(max attempt {db['attempt_max']}) — treat this row as suspect")

        h.assert_stable(where=f"{mode}@{n} rep {rep}")

        metrics = {
            # --- conditions, on every row so `summarize` can group and a jury
            #     can ask
            "claim_mode": mode,
            "concurrency": n,
            "capacity": capacity,
            "runs_seeded": runs,
            "hold_s": hold_s,
            # --- the premise, proven rather than assumed
            "holder_lock_confirmed": 1,
            "holder_pid": holder_pid,
            "holder_rows_held": len(holder.held_ids),
            # --- THE HEADLINE, all of it server-side: how many claimers were
            #     waiting behind the holder, and for how long, according to
            #     Postgres itself
            "blocked_claimers_max": blocking["blocked_claimers_max"],
            "blocked_claimers_ever": blocking["blocked_claimers_ever"],
            "blocked_wait_max_s": blocking["blocked_wait_max_s"],
            "any_claimer_blocked": 1 if blocking["blocked_claimers_ever"] else 0,
            "blocked_samples": blocking["blocked_samples"],
            "waiters_any_max": blocking["waiters_any_max"],
            # --- the sampler's own resolution, so the zeros above can be bounded
            "samples_taken": blocking["samples_taken"],
            "sample_failures": observer.failures,
            "observe_gap_max_s": blocking["observe_gap_max_s"],
            "observe_span_s": blocking["observe_span_s"],
            # --- what the claimers walked away with
            "assignments_handed_out": handed_out,
            "distinct_runs_handed": len(owners),
            "double_assigned_runs": sum(1 for v in owners.values() if len(v) > 1),
            "empty_handed_responses": empty_handed,
            # --- what the database ended up holding (server-side truth)
            "db_runs_assigned": db["runs_assigned"],
            "db_runs_pending": db["runs_pending"],
            "db_owner_nodes": db["owner_nodes"],
            "db_attempt_max": db["attempt_max"],
            "queue_drained": 1 if db["runs_pending"] == 0 else 0,
            "reaper_interference": interference,
            # A failed or rejected heartbeat. In `blocking` at high concurrency
            # this is where a connection-pool timeout would surface — a real cost
            # of that arm, recorded rather than hidden.
            "errors": errors,
            # --- proof that "simultaneous" was really simultaneous. Host clock,
            #     and it has to be: it measures when THIS PROCESS released its own
            #     threads. A control on the setup, not a result.
            "release_skew_ms": (
                (max(releases) - min(releases)) * 1000.0 if len(releases) > 1 else 0.0
            ),
            # --- corroboration only, named `client_` so it can never be mistaken
            #     for the measurement. See "WHAT E2c CANNOT SAY" in the header: a
            #     claimer that had waited behind a hold_s-second holder could not
            #     have answered in milliseconds, so these bound the sampler's
            #     blind spot from the other side.
            "client_rtt_max_ms": max(rtts) if rtts else None,
            "client_rtt_mean_ms": statistics.mean(rtts) if rtts else None,
            "client_rtt_n": len(rtts),
            # Kept raw so the shape of the wait can be re-read later without
            # re-running the measurement. `summarize` ignores non-numeric values,
            # so this never reaches a published table by itself.
            "blocked_counts": blocking["blocked_counts"],
        }
        return h.record(EXP, f"{mode}@{n}", rep, metrics, dry=dry)
    finally:
        observer.stop()
        # Never leave a row lock open behind us, whatever went wrong above: the
        # next repetition's TRUNCATE would hang on it. `.ident` is None until
        # `start()`, and a failure before that point (`seed_queue`, or
        # `wait_for_holder` refusing to measure) has no thread to join.
        if holder.ident is not None:
            holder.join(timeout=hold_s + 90)
        for sess in sessions:
            sess.close()


# --- Driver -----------------------------------------------------------------


SUMMARY_METRICS = [
    "blocked_claimers_max",
    "blocked_claimers_ever",
    "blocked_wait_max_s",
    "any_claimer_blocked",
    "waiters_any_max",
    "assignments_handed_out",
    "double_assigned_runs",
    "empty_handed_responses",
    "db_runs_assigned",
    "db_runs_pending",
    "queue_drained",
    "client_rtt_max_ms",
    "samples_taken",
    "observe_gap_max_s",
    "release_skew_ms",
    "errors",
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
                   help="PENDING runs seeded per repetition. The default of 1 is "
                        "the regime E2c's claim is about — read the comment on "
                        "RUNS before changing it")
    p.add_argument("--capacity", type=int, default=CAPACITY,
                   help="per-node capacity; one run per heartbeat by default")
    p.add_argument("--hold-s", type=float, default=HOLD_S,
                   help="how long the holder keeps its transaction open. Every "
                        "wait this experiment can report is bounded by it")
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
                   help="lease TTL the control plane runs with during E2c. Raised "
                        "far above a repetition's length so the reaper cannot "
                        "requeue a held run mid-round and make a correct re-claim "
                        "look like a fault")
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
        # conditions do not travel with any published number.
        h.require_clean_machine()
    if not args.dry:
        # Contamination guard + clean-repository stamp. `guard_fresh` refuses to
        # append onto measured rows — which would blend two runs into one summary
        # with nothing downstream able to tell them apart — and warns out loud,
        # before any measuring, if the tree that produced these numbers is
        # uncommitted. Every recorded row carries its own `git_dirty` besides.
        h.guard_fresh(EXP, args.fresh)

    concurrency = list(args.concurrency)
    reps = args.reps
    if args.dry:
        # A rehearsal proves the script runs end to end; it is not evidence, and
        # rule 7 of the brief keeps it out of every summary and chart.
        concurrency = concurrency[:1]
        reps = 2

    print(f"[E2c] arms={args.arms} concurrency={concurrency} reps={reps} "
          f"runs={args.runs} capacity={args.capacity} hold_s={args.hold_s} "
          f"lease_ttl_s={args.lease_ttl_s} dry={args.dry}")

    modes_used: list[dict] = []
    try:
        # Repetitions on the OUTSIDE, arms on the inside — same as E2, same
        # reason: a machine drifts over a long run, and running all of one arm's
        # repetitions before the next arm's would hand that drift to one arm as
        # if it were a property of the locking strategy.
        for rep in range(1, reps + 1):
            for mode in args.arms:
                # The long lease rides on every set_mode call, so it is
                # re-asserted in the container after each restart rather than
                # assumed to have persisted.
                got = h.set_mode(claim=mode,
                                 env={"LEASE_TTL_S": str(args.lease_ttl_s)})
                if got not in modes_used:
                    modes_used.append(got)
                for n in concurrency:
                    row = run_once(mode=mode, n=n, rep=rep, runs=args.runs,
                                   capacity=args.capacity, hold_s=args.hold_s,
                                   dry=args.dry)
                    rtt, gap = row["client_rtt_max_ms"], row["observe_gap_max_s"]
                    print(
                        f"    {mode:>11}@{n:<3} rep {rep}: "
                        f"blocked_max={row['blocked_claimers_max']:>3}/{n:<3} "
                        f"wait_max={row['blocked_wait_max_s']:>6.2f}s "
                        f"handed={row['assignments_handed_out']:>3} "
                        f"double={row['double_assigned_runs']:>3} "
                        f"empty={row['empty_handed_responses']:>3} "
                        f"pending_left={row['db_runs_pending']:>3} "
                        f"samples={row['samples_taken']:>4} "
                        f"gap={('%.2f' % gap) if gap is not None else '«MISSING»'}s "
                        f"rtt_max={('%.0f' % rtt) if rtt is not None else '«MISSING»'}ms "
                        f"err={row['errors']}"
                    )
    finally:
        # Never leave the platform on a weakened claim strategy, even after a
        # crash.
        h.set_mode()

    h.summarize(
        EXP, metrics=SUMMARY_METRICS,
        title=f"{EXP} — does a claimer wait behind a held row? "
              "SKIP LOCKED vs blocking vs naive",
    )
    h.chart(
        EXP, metric="blocked_claimers_max",
        title=f"{EXP} — claimers waiting behind one held row (median, min–max)",
    )
    h.manifest(
        EXP,
        reps=reps,
        modes=modes_used,
        machine_idle=args.machine_idle,
        mains_power=args.mains_power,
        notes=(
            "E2c measures the ONE property D6 chose SKIP LOCKED for and that "
            "E2/E2b never tested: whether a claimer waits behind another "
            "claimer. A holder session locks the head of the queue with the same "
            "WHERE clause, the same ordering and the same limit as assign_runs, "
            f"and sleeps inside that transaction for {args.hold_s}s; the other "
            "claimers then heartbeat at one instant. Waiting is read from "
            "Postgres via pg_blocking_pids() and timed with "
            "statement_timestamp() — no reported wait touches the host clock. "
            "The holder's lock is CONFIRMED server-side (PgSleep plus a granted "
            "transactionid lock) before any claimer is released, so a page of "
            "zeros cannot mean 'there was nothing to wait for'. Limits, stated "
            "rather than implied: the holder is artificial and holds for seconds "
            "where a real claimer holds for milliseconds, so E2c shows the wait "
            "EXISTS and is bounded by the holder's transaction, NOT that waits "
            "of this size occur in production; and the sampler polls, so a wait "
            "shorter than observe_gap_max_s can be missed — client_rtt_max_ms is "
            "recorded solely to bound that blind spot from the other side and is "
            "not the measurement. The naive arm is a control: it shows that 'did "
            "not wait' and 'did not lock' are different things. The control "
            f"plane ran with LEASE_TTL_S={args.lease_ttl_s} throughout so the "
            "reaper cannot requeue a held run mid-round; reaper_interference "
            "stays on as the proof that it did not, and is reported in "
            "summary.md even when it is zero."
        ),
        extra={
            "arms": args.arms,
            "concurrency_levels": concurrency,
            "runs_seeded_per_rep": args.runs,
            "capacity_per_node": args.capacity,
            "hold_s": args.hold_s,
            "holder_application_name": HOLDER_APPNAME,
            "holder_candidate_limit": args.capacity * CANDIDATE_MULTIPLIER,
            "observe_interval_s_requested": OBSERVE_INTERVAL_S,
            "http_timeout_s": HTTP_TIMEOUT_S,
            "claimer_ram_mb": CLAIMER_RAM_MB,
            # Recorded explicitly as well as inside compose_overrides: a
            # deliberate departure from the shipped default, and a reader must
            # not have to dig through YAML to find it.
            "lease_ttl_s_override": args.lease_ttl_s,
            "lease_ttl_s_shipped_default": h.SHIPPED_LEASE_TTL_S,
            # The prediction travels in the results tree as well as in the
            # script's header, so the two can be compared without a checkout.
            "prediction_written_before_first_run": {
                "skip_locked": "blocked_claimers_max = 0 at every concurrency; "
                               "every claimer returns empty-handed while the "
                               "holder still holds; the queue is NOT drained",
                "blocking": "blocked_claimers_max = N at every concurrency; "
                            "blocked_wait_max_s approaches hold_s; one "
                            "assignment once the holder lets go",
                "naive": "blocked_claimers_max > 0 (the UPDATE at commit needs "
                         "the same row lock) AND double_assigned_runs = 1 — a "
                         "control, not evidence about D6",
            },
        },
    )
    print(f"\n[E2c] done — {h.exp_dir(EXP)}: raw.jsonl, summary.md, chart.png, "
          "manifest.json")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (h.HarnessError, AssertionError) as exc:
        print(f"\nE2c FAILED: {exc}", file=sys.stderr)
        sys.exit(1)
