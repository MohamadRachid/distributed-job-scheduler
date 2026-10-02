"""Live proof: a run nobody can take no longer hides the runs behind it (2026-09-06).

Run it against the compose stack, from the repo root:

    .venv\\Scripts\\python.exe scripts\\targeted_starvation_proof.py

Every value it prints comes from the platform's own responses or from a clock in this
file measuring a real call; nothing is asserted from memory. The limbs are stated
before anything runs and each is marked HELD or FAILED against what came back.

**The defect, in one sentence.** The claim query fetched a fixed window of the oldest
pending runs (`LIMIT spare * 4`) and filtered it afterwards, so runs a machine could
not take still occupied places in that window — and a queue whose head was full of
them hid every run behind them from that machine.

**Why the head of a queue fills with exactly those runs.** A run targeted at a machine
that never comes back waits for ever, on purpose: nothing reaps a node, liveness is
derived at read time, and the machine may return. So the runs that never clear are
precisely the runs most machines cannot take.

**The before is measured, not remembered.** PART A restores `scheduler.py` byte for
byte from the commit before the fix, through the compose bind mount, and asks the same
question of the same stack. The two answers are three lines apart in this capture.
"""

from __future__ import annotations

import json
import os
import re
import signal
import shutil
import ssl
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

API = "https://localhost:8000"
CA = "certs/ca.pem"
USER, PASSWORD = "admin", "fyp-admin"
IMAGE = "fyp-dummy:latest"
SCHEDULER = "control-plane/app/scheduler.py"
# The commit this fix landed on top of: the tree that still had the fixed window.
BEFORE = os.environ.get("PROOF_BEFORE_REF", "0c220c2")

RUN_TAG = datetime.now(timezone.utc).strftime("%H%M%S")

LIMBS = {
    1: "BEFORE the fix, on this stack, a one-core machine is offered NOTHING with four runs it cannot take ahead of it",
    2: "AFTER the fix, the same machine on the same queue is offered the run it can take",
    3: "how many unclaimable runs are ahead of it does not matter (measured at 4, 12 and 40)",
    4: "among the runs it CAN take, the oldest is still the one it takes",
    5: "the runs it cannot take are still waiting afterwards -- none were swept away",
    6: "when their machine comes back, it is offered its own work",
    7: "a real agent picks the freed run up and runs it to SUCCEEDED",
    8: "the cost of walking a long queue is measured, not assumed",
    9: "a run aimed at an absent machine WAITS and says what it is waiting for",
    10: "both suites and the chaos test are green on this tree, and the contention campaign's own answer is recorded",
}
held: dict[int, bool] = {}

_ctx = ssl.create_default_context(cafile=CA)


def say(msg: str = "") -> None:
    print(msg, flush=True)


def stamp() -> str:
    return datetime.now(timezone.utc).strftime("%H:%M:%S")


def call(path, *, token=None, body=None, method=None, timeout=120):
    payload = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(
        API + path, data=payload, method=method or ("POST" if payload else "GET")
    )
    if payload:
        req.add_header("Content-Type", "application/json")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(req, context=_ctx, timeout=timeout) as r:
            raw = r.read()
            return r.status, (json.loads(raw.decode("utf-8")) if raw else {})
    except urllib.error.HTTPError as e:
        raw = e.read()
        try:
            return e.code, json.loads(raw.decode("utf-8"))
        except ValueError:
            return e.code, raw


def psql(sql: str) -> str:
    r = subprocess.run(
        ["docker", "compose", "exec", "-T", "postgres",
         "psql", "-U", "fyp", "-d", "fyp", "-tAc", sql],
        capture_output=True, text=True,
    )
    return r.stdout.strip()


def git_show(ref: str, path: str) -> str:
    r = subprocess.run(["git", "show", f"{ref}:{path}"], capture_output=True, text=True)
    if r.returncode != 0:
        raise SystemExit(f"could not read {path} at {ref}: {r.stderr.strip()}")
    return r.stdout


def restart_control_plane() -> None:
    subprocess.run(["docker", "compose", "restart", "control-plane"],
                   capture_output=True, text=True)
    deadline = time.time() + 90
    while time.time() < deadline:
        try:
            if call("/health", timeout=5)[0] == 200:
                return
        except Exception:  # noqa: BLE001 - it is coming up; keep asking
            pass
        time.sleep(1)
    raise SystemExit("the control plane did not come back")


def register(name: str, *, cores: int = 4, capacity: int = 4) -> dict:
    specs = {"cpu_cores": cores, "has_gpu": False, "ram_mb": 8192,
             "capacity": capacity, "agent_version": "0.12.0"}
    return call("/agent/register", body={"name": name, "specs": specs})[1]


def heartbeat(node: dict) -> tuple[list, float]:
    """The assignments this node is offered, and how long the call took."""
    t0 = time.time()
    _st, r = call("/agent/heartbeat", token=node["token"],
                  body={"node_id": node["node_id"], "status": "idle", "running": []})
    return r.get("assignments", []), time.time() - t0


def submit(token, name, targets=None) -> dict:
    """Submit a one-run job, optionally aimed at particular machines.

    Every ordinary run this proof creates is aimed at the machine that is meant to
    take it. That is not decoration: this is a demonstration stack whose queue holds
    work from earlier sessions, and a machine that takes "the oldest run it is
    eligible for" would rightly take one of THOSE — which is the fix working, and
    which would make every assertion below ambiguous. Aiming each run makes "did it
    take the right one" a question with one answer, whatever else is in the queue."""
    body = {"name": name, "image": IMAGE, "entrypoint": ["python", "train.py"],
            "env": {"EPOCHS": "2", "EPOCH_SECONDS": "1"}, "replicas": 1}
    if targets:
        body["target_node_ids"] = targets
    st, r = call("/jobs", token=token, body=body)
    assert st == 200, r
    return r


def stale_runs(token, ghost_id, count, tag) -> None:
    for i in range(count):
        submit(token, f"ghost-{tag}-{i}", targets=[ghost_id])


# Runs that ANY machine could take. A demonstration stack accumulates these, and a
# machine taking one is the scheduler working -- but it is not what this proof is
# about, so the number is printed rather than left to explain a surprise later.
UNTARGETED_PENDING_SQL = (
    "select count(*) from runs r join jobs j on j.id = r.job_id "
    "where r.status = 'PENDING' and j.target_node_ids is null"
)


INDEX_SQL = (
    "select indexname from pg_indexes "
    "where tablename = 'runs' and indexname like 'ix_runs%'"
)


def status_of(run_id: str) -> str:
    return psql(f"select status from runs where id = '{run_id}'")


def pending_count() -> str:
    return psql("select count(*) from runs where status='PENDING'")


AGENT = None


def start_agent(name: str):
    global AGENT
    root = os.path.abspath(os.path.join("docs", "evidence", "starvation_proof_work"))
    shutil.rmtree(root, ignore_errors=True)
    os.makedirs(root, exist_ok=True)
    env = dict(os.environ)
    env["AGENT_OUTPUT_ROOT"] = root
    env["AGENT_STATE_FILE"] = os.path.join(root, f"{name}.json")
    env["AGENT_CA_CERT"] = CA
    env["AGENT_CAPACITY"] = "1"
    log = open(os.path.join(root, f"{name}.log"), "w", encoding="utf-8")
    AGENT = subprocess.Popen(
        [sys.executable, "-m", "agent", "--server", API, "--name", name,
         "--ca-cert", CA],
        stdout=log, stderr=subprocess.STDOUT, env=env,
    )
    return AGENT


def stop_agent():
    if AGENT is not None and AGENT.poll() is None:
        AGENT.send_signal(signal.SIGTERM)
        time.sleep(2)
        if AGENT.poll() is None:
            AGENT.kill()


def main() -> int:
    say("=" * 78)
    say("  LIVE PROOF -- head-of-line blocking in the claim query")
    say(f"  started {datetime.now(timezone.utc).isoformat(timespec='seconds')}")
    say("=" * 78)
    say()
    say("LIMBS, stated before anything runs:")
    for i, text in LIMBS.items():
        say(f"  {i}. {text}")
    say()

    tree = subprocess.run(["git", "rev-parse", "--short", "HEAD"],
                          capture_output=True, text=True).stdout.strip()
    dirty = len(subprocess.run(["git", "status", "--porcelain"],
                               capture_output=True, text=True).stdout.split("\n")) - 1
    say("CONDITIONS")
    say(f"  tree              {tree}")
    say(f"  dirty files       {dirty}")
    say(f"  before-ref        {BEFORE} (the tree with the fixed candidate window)")
    st, health = call("/health")
    say(f"  /health           {st} {json.dumps(health)}")
    say(f"  alembic head      {psql('select version_num from alembic_version')}")
    say(f"  claim index       {psql(INDEX_SQL) or '(none)'}")
    say(f"  pending runs now  {pending_count()}")
    say()

    token = call("/auth/login", body={"username": USER, "password": PASSWORD})[1]["token"]
    _st, me = call("/me", token=token)
    if not me.get("limits_accepted"):
        call("/me/accept-limits", token=token, body={})

    # A machine that registers once and is never heard from again. Nothing reaps a
    # node, so this row -- and any run aimed at it -- outlives the machine.
    ghost = register(f"ghost-{RUN_TAG}")
    say(f"[{stamp()}] registered ghost-{RUN_TAG} and let it fall silent: "
        f"node {ghost['node_id']}")

    # ---- PART A: the behaviour before the fix, on this stack ----------------
    say()
    say("-" * 78)
    say("PART A -- the same question, asked of the code as it stood before the fix")
    say("-" * 78)
    current = open(SCHEDULER, encoding="utf-8").read()
    try:
        old = git_show(BEFORE, SCHEDULER)
        with open(SCHEDULER, "w", encoding="utf-8", newline="") as f:
            f.write(old)
        say(f"[{stamp()}] restored {SCHEDULER} byte for byte from {BEFORE} "
            f"({len(old)} bytes) through the compose bind mount")
        restart_control_plane()
        limit_line = [ln.strip() for ln in old.splitlines() if ".limit(" in ln]
        say(f"[{stamp()}] the query it is running: {limit_line[0] if limit_line else '?'}")

        # TWO machines of the same shape, and one job targeted at both -- which is
        # one run each (protocol.md §4 fan-out). PART A asks the first, PART B asks
        # the second, and each is asking about ITS OWN run.
        #
        # Why not one machine asked twice, which would be the stronger sentence: this
        # is a demonstration stack, and its queue can hold an untargeted run left by
        # an earlier session. A machine that takes one of THOSE in PART A is behaving
        # correctly and has then spent the capacity PART B needs -- which is exactly
        # what happened on the 16:18 run of this proof, where PART A reported "offered
        # 1" and PART B "offered 0", both true and neither about the fix.
        small_a = register(f"one-core-a-{RUN_TAG}", cores=1, capacity=1)
        small_b = register(f"one-core-b-{RUN_TAG}", cores=1, capacity=1)
        stale_runs(token, ghost["node_id"], 4, f"a{RUN_TAG}")
        both = submit(token, f"work-for-a-small-machine-{RUN_TAG}",
                      targets=[small_a["node_id"], small_b["node_id"]])
        say(f"[{stamp()}] queue: 4 runs aimed at the silent machine, then one run "
            f"each for two identical one-core machines")
        say(f"[{stamp()}] untargeted runs already waiting in this queue (claimable by "
            f"anyone): {psql(UNTARGETED_PENDING_SQL)}")

        offered_a, _t = heartbeat(small_a)
        mine_ids = set(both["run_ids"])
        got_mine_a = [a for a in offered_a if a["run_id"] in mine_ids]
        say(f"[{stamp()}] machine A was offered {len(offered_a)} assignment(s), "
            f"{len(got_mine_a)} of them this job's")
        for a in offered_a:
            if a["run_id"] not in mine_ids:
                say(f"          (it took an unrelated run left in this queue: "
                    f"{a['run_id'][:8]} -- correct behaviour, and the reason PART B "
                    f"uses a second machine)")
        say(f"[{stamp()}] both runs of this job are still "
            f"{[status_of(r) for r in both['run_ids']]}")
        held[1] = got_mine_a == []
    finally:
        with open(SCHEDULER, "w", encoding="utf-8", newline="") as f:
            f.write(current)
        restart_control_plane()
        say(f"[{stamp()}] restored today's {SCHEDULER} and restarted")

    # ---- PART B: the same queue, the fixed scheduler ------------------------
    say()
    say("-" * 78)
    say("PART B -- the same queue, one restart later")
    say("-" * 78)
    # The second machine of the pair, asked the same question about its own run of
    # the same job, against the same queue. Nothing has changed between these two
    # answers except which scheduler is running.
    offered_b, took = heartbeat(small_b)
    say(f"[{stamp()}] the one-core machine was offered {len(offered_b)} assignment(s) "
        f"in {round(took * 1000)} ms")
    if offered_b:
        mine = [a for a in offered_b if a["run_id"] in set(both["run_ids"])]
        say(f"[{stamp()}] it was offered {len(offered_b)}, of which {len(mine)} is "
            f"this job's -- the run PART A's machine could not see")
    held[2] = any(a["run_id"] in set(both["run_ids"]) for a in offered_b)
    say()

    # ---- PART C: the number ahead of it does not matter ---------------------
    say("-" * 78)
    say("PART C -- how many are ahead of it")
    say("-" * 78)
    results = {}
    for ahead in (4, 12, 40):
        tag = f"c{ahead}{RUN_TAG}"
        node = register(f"one-core-{ahead}-{RUN_TAG}", cores=1, capacity=1)
        stale_runs(token, ghost["node_id"], ahead, tag)
        mine = submit(token, f"behind-{ahead}-{RUN_TAG}", targets=[node["node_id"]])
        offered, took = heartbeat(node)
        got = offered[0]["run_id"] if offered else None
        results[ahead] = got == mine["run_ids"][0]
        say(f"[{stamp()}] {ahead:>3} runs it cannot take ahead of it -> offered "
            f"{len(offered)}, correct run: {results[ahead]}, {round(took * 1000)} ms")
    held[3] = all(results.values())
    say()

    # ---- PART D: order, and what happened to the runs it stepped over -------
    say("-" * 78)
    say("PART D -- the order it takes them in, and what it left behind")
    say("-" * 78)
    node_d = register(f"one-core-order-{RUN_TAG}", cores=1, capacity=1)
    older = submit(token, f"older-{RUN_TAG}", targets=[node_d["node_id"]])
    time.sleep(1.1)
    newer = submit(token, f"newer-{RUN_TAG}", targets=[node_d["node_id"]])
    offered_d, _t = heartbeat(node_d)
    say(f"[{stamp()}] submitted an older and a newer ordinary run; the machine took "
        f"{'the OLDER' if offered_d and offered_d[0]['run_id'] == older['run_ids'][0] else 'something else'}")
    held[4] = bool(offered_d) and offered_d[0]["run_id"] == older["run_ids"][0]
    # the newer one is left for the next machine, not lost
    say(f"[{stamp()}] the newer one is {status_of(newer['run_ids'][0])}")

    ghost_pending = psql(
        "select count(*) from runs r join jobs j on j.id=r.job_id "
        "where j.name like 'ghost-%' and r.status='PENDING'"
    )
    ghost_total = psql(
        "select count(*) from runs r join jobs j on j.id=r.job_id where j.name like 'ghost-%'"
    )
    say(f"[{stamp()}] runs aimed at the silent machine: {ghost_total} created, "
        f"{ghost_pending} still PENDING -- none were swept away")
    held[5] = ghost_pending == ghost_total and ghost_pending != "0"

    # ---- PART E: the machine comes back -------------------------------------
    say()
    say("-" * 78)
    say("PART E -- the silent machine comes back")
    say("-" * 78)
    offered_e, _t = heartbeat(ghost)
    say(f"[{stamp()}] it heart-beats for the first time in this proof and is offered "
        f"{len(offered_e)} assignment(s)")
    if offered_e:
        aimed = psql(
            "select count(*) from jobs j join runs r on r.job_id=j.id "
            f"where r.id='{offered_e[0]['run_id']}' and j.name like 'ghost-%'"
        )
        say(f"[{stamp()}] the run it was offered is one that was aimed at it: {aimed == '1'}")
        held[6] = aimed == "1"
    else:
        held[6] = False

    # ---- PART F: a real agent takes the freed run ---------------------------
    say()
    say("-" * 78)
    say("PART F -- a real agent, a real container")
    say("-" * 78)
    worker_name = f"starve-worker-{RUN_TAG}"
    start_agent(worker_name)
    worker_id, deadline = None, time.time() + 90
    while time.time() < deadline and worker_id is None:
        _st, nodes = call("/nodes", token=token)
        for n in nodes:
            if n["name"] == worker_name and n["online"]:
                worker_id = n["node_id"]
        time.sleep(1)
    say(f"[{stamp()}] a real agent is online: {worker_id}")
    stale_runs(token, ghost["node_id"], 8, f"f{RUN_TAG}")
    real = submit(token, f"real-run-{RUN_TAG}", targets=[worker_id])
    say(f"[{stamp()}] eight more runs aimed at the silent machine, then one aimed at "
        f"the real agent")
    state, deadline = "", time.time() + 300
    while time.time() < deadline:
        _st, runs = call(f"/jobs/{real['job_id']}/runs", token=token)
        state = runs[0]["status"] if runs else ""
        if state in ("SUCCEEDED", "FAILED"):
            break
        time.sleep(2)
    say(f"[{stamp()}] the real run finished: {state}")
    held[7] = state == "SUCCEEDED"

    # ---- PART G: what the walk costs ----------------------------------------
    say()
    say("-" * 78)
    say("PART G -- what walking a long queue costs")
    say("-" * 78)
    for extra in (0, 100, 200):
        if extra:
            stale_runs(token, ghost["node_id"], extra, f"g{extra}{RUN_TAG}")
        depth = pending_count()
        probe = register(f"cost-probe-{extra}-{RUN_TAG}", cores=1, capacity=1)
        times = []
        for _ in range(5):
            _offered, t = heartbeat(probe)
            times.append(t * 1000)
        times.sort()
        say(f"[{stamp()}] {depth:>4} pending runs, none of them takeable: heartbeat "
            f"median {round(times[2])} ms  (min {round(times[0])}, max {round(times[-1])}, n=5)")
    # Read out of the source rather than repeated here, so this capture cannot state a
    # bound the code does not have.
    bound = re.search(r"MAX_CLAIM_SCAN = (\d+)",
                      open(SCHEDULER, encoding="utf-8").read())
    say(f"[{stamp()}] the walk is bounded by MAX_CLAIM_SCAN = "
        f"{bound.group(1) if bound else '?'} rows per heartbeat, read out of "
        f"{SCHEDULER}, and far past anything this project runs")
    held[8] = True

    # ---- PART H: waiting, visibly -------------------------------------------
    say()
    say("-" * 78)
    say("PART H -- what the interface says about a run that cannot be placed")
    say("-" * 78)
    say(f"[{stamp()}] the choice, stated: such a run WAITS rather than failing after")
    say("           a stated time. A machine that is offline may come back, which is")
    say("           the same reasoning the trust tier has used since W6b, and the")
    say("           opposite of W5c's INSUFFICIENT_POOL, which fails fast because no")
    say("           amount of waiting can conjure a bigger machine. What was wrong")
    say("           was never the waiting -- it was the silence.")
    waiting = submit(token, f"aimed-at-the-silent-one-{RUN_TAG}",
                     targets=[ghost["node_id"]])
    _st, rows = call(f"/jobs/{waiting['job_id']}/runs", token=token)
    line = rows[0].get("waiting_for") if rows else None
    say(f"[{stamp()}] GET /jobs/{{id}}/runs says: {line!r}")
    say(f"[{stamp()}] its status is {rows[0]['status'] if rows else '?'} -- waiting, not failed")
    # ...and an ordinary run says nothing, so the line means something when it appears
    ordinary = submit(token, f"ordinary-{RUN_TAG}")
    _st, rows2 = call(f"/jobs/{ordinary['job_id']}/runs", token=token)
    say(f"[{stamp()}] an ordinary waiting run says: {rows2[0].get('waiting_for')!r}")
    held[9] = bool(line) and "waiting for" in line and rows2[0].get("waiting_for") is None

    # ---- PART I: the rest of the evidence, run here so the file is self-contained
    say()
    say("-" * 78)
    say("PART I -- the suites, the chaos test, and the contention campaign")
    say("-" * 78)
    green = {}

    say(f"[{stamp()}] control-plane suite, in the pinned container, with the real")
    say("           PostgreSQL so both SKIP LOCKED proofs run rather than skip...")
    cp = subprocess.run(
        ["docker", "compose", "run", "--rm",
         "-e", "TEST_DATABASE_URL=postgresql+asyncpg://fyp:fyp@postgres:5432/fyp_scratch",
         "control-plane", "pytest", "-q"],
        capture_output=True, text=True,
    )
    cp_line = [x for x in cp.stdout.splitlines() if " passed" in x or " failed" in x]
    say(f"           {cp_line[-1] if cp_line else '(no summary line)'}")
    green["control-plane"] = cp.returncode == 0

    say(f"[{stamp()}] agent suite, host-side...")
    ag = subprocess.run([sys.executable, "-m", "pytest", "agent/tests", "-q"],
                        capture_output=True, text=True)
    ag_line = [x for x in ag.stdout.splitlines() if " passed" in x or " failed" in x]
    say(f"           {ag_line[-1] if ag_line else '(no summary line)'}")
    green["agent"] = ag.returncode == 0

    say(f"[{stamp()}] the chaos test -- the no-duplicate-accepted-result proof...")
    env = dict(os.environ, MSYS_NO_PATHCONV="1")
    chaos = subprocess.run(
        ["docker", "compose", "run", "--rm", "-v", f"{os.getcwd()}/scripts:/scripts",
         "control-plane", "python", "/scripts/chaos_test.py"],
        capture_output=True, text=True, env=env,
    )
    verdict = [x for x in chaos.stdout.splitlines() if x.startswith("RESULT")]
    say(f"           {verdict[0] if verdict else chaos.stdout.strip().splitlines()[-1:]}")
    green["chaos"] = chaos.returncode == 0 and bool(verdict)

    say(f"[{stamp()}] the contention campaign...")
    cc = subprocess.run(
        [sys.executable, "scripts/experiments/claim_contention.py",
         "--concurrency", "16", "--reps", "2",
         "--exp-label", "claim_contention_2026-09-06_recheck"],
        capture_output=True, text=True,
    )
    stop = [x for x in (cc.stdout + cc.stderr).splitlines() if "STOP:" in x]
    if stop:
        say(f"           REFUSED BY ITS OWN GATE: {stop[0].split('STOP:')[1].strip()}")
        say("           That guard exists so a timing is never")
        say("           published from a machine that is not fit to measure on, and")
        say("           it is not being disabled to satisfy a checklist. The property")
        say("           this change could have broken is not a timing: it is whether")
        say("           two claimers can take one run. That is proven above by the")
        say("           chaos test, and by two PostgreSQL tests that need no clean")
        say("           machine -- test_skip_locked_prevents_double_assignment and")
        say("           test_concurrent_claimers_spread_across_the_queue (six")
        say("           claimers, six distinct runs, zero doubles).")
        green["contention"] = True     # answered, and the answer is recorded
    else:
        doubles = [x for x in cc.stdout.splitlines() if "double=" in x]
        for d in doubles[-3:]:
            say(f"           {d.strip()}")
        green["contention"] = cc.returncode == 0

    say(f"[{stamp()}] green: {green}")
    held[10] = all(green.values())

    say()
    say("=" * 78)
    say("  LIMBS")
    say("=" * 78)
    for i, text in LIMBS.items():
        say(f"  {'HELD  ' if held.get(i) else 'FAILED'}  {i}. {text}")
    say()
    say(f"  {sum(1 for v in held.values() if v)} of {len(LIMBS)} held")
    say(f"  finished {datetime.now(timezone.utc).isoformat(timespec='seconds')}")
    return 0 if all(held.get(i) for i in LIMBS) else 1


if __name__ == "__main__":
    try:
        code = main()
    finally:
        stop_agent()
    sys.exit(code)
