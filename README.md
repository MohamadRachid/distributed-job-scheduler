# Distributed Training Job Scheduler

*A Container-Based Platform for Distributed Training Job Scheduling Across Heterogeneous
Machines* — final year project, Computer and Communications Engineering, USJ ESIB (2026).

A lightweight, self-hosted platform that pools ordinary machines — lab PCs,
laptops, an idle server — and runs containerized training jobs across them. A
central scheduler hands work out, notices when a machine dies, and recovers the
lost work **without ever accepting a duplicate result**.

> The core of the project is the **scheduler and the failure-recovery layer**.
> Everything else — the web UI, storage, login — exists to demonstrate it.
> `protocol.md` is the frozen contract between the control plane and the agent.

**The reliability guarantee, stated exactly, because the precise version is the
stronger claim:** *at-least-once execution, at-most-once accepted result.* Under
failure a run may physically execute more than once — but only one result is ever
accepted and stored. We never claim "exactly-once"; it is not achievable here, and
saying so is the point.

---

## What it does today

Everything below is built, merged and proven live on real Docker. Nothing here is
a plan.

| Capability | What it means | Where it lives |
|---|---|---|
| **Node pool** | Machines register themselves and heartbeat every 3 s, reporting real specs (CPU/GPU names, RAM, machine model) and live usage. Online/offline is **derived at read time**, never stored. | `agent/`, `app/api/nodes.py` |
| **Pull-time scheduling** | There is no separate scheduler process. A machine asking for work is what causes work to be handed out, inside one database transaction using `SELECT … FOR UPDATE SKIP LOCKED` — so two machines can never claim the same run. | `app/scheduler.py` |
| **Parallel dispatch + matching** | One job fans out into runs across selected machines, filtered by GPU, RAM and capacity. | `app/scheduler.py`, `app/api/jobs.py` |
| **Failure recovery (the centrepiece)** | Every run holds a lease. A background **reaper** finds runs whose lease expired, marks them `LOST`, and requeues them. The re-dispatch bumps a **fencing token** so the presumed-dead machine's late result is refused. | `app/reaper.py` |
| **Run + node diagnostics** | A dead run explains itself from hard facts — the kernel's own OOM flag, GPU and image errors, decoded exit signals — with the container's own memory samples and last log lines as evidence. A silent machine gets a postmortem: a black box before death, a goodbye when there is one, and an interview when it returns. | `agent/classify.py`, `app/diagnostics.py` |
| **Failure-aware rescheduling** | A run killed by a *kernel-proven* RAM shortage is re-dispatched only to a machine with strictly more RAM, carrying what it learned. If no registered machine can ever satisfy it, it fails at once with the exact numbers instead of retrying into the same wall. | `app/scheduler.py` |
| **Checkpoint and resume** | Recovery used to save the *run* and throw the *work* away. A re-dispatched run now continues from what the dead attempt saved, instead of restarting from epoch zero. | `app/api/artifacts.py`, `agent/runner.py` |
| **Results and artifacts** | Output files land in MinIO — written and read back **only through the control plane**, so a worker never holds storage credentials. | `app/storage.py`, `app/api/artifacts.py` |
| **Login** | A JWT gates every user endpoint. Agents use separate node tokens. Two kinds of caller, two kinds of key. | `app/userauth.py` |
| **Sealed by default** | **Every** job has its own key. Its input file is sealed at submit, its results and checkpoints are sealed inside the container before they leave it, and storage never holds anything else — the owner downloads plaintext from the control plane. The key reaches a container through a fenced, single-use ticket, and the data is read **a piece at a time**, so what a container needs to read a file does not grow with the file. One flipped byte fails the run at the piece it is in. Deleting the key makes every sealed copy of the input, the results and the checkpoints unreadable for ever. Running only on machines an admin trusts is a separate choice, off by default. | `app/sealing.py`, `app/api/keys.py`, `workloads/dummy/fyp_data.py` |
| **Run-log tiering** | `run_logs` no longer grows for ever. A finished run's chunks move, after a retention window, into one compressed object — behind a two-phase commit that reads the object back and compares its digest **before** deleting a single row. | `app/logarchive.py` |
| **Encrypted transport** | Browser and agent traffic runs over HTTPS/WSS with the certificate **verified**. There is no switch to skip verification. | `agent/tls.py` |

**Two limits stated up front, because they are the honest half.**
*Privacy* protects data from **users** of a worker machine — **not from its root
administrator**, who can read a running container's memory. Root-proof privacy needs
TEE hardware, which is named future work. And *node tokens* are permanent: they are
never rotated and cannot be revoked, so encrypted transport removes the wire as a way
to steal one but does not make the token itself stronger.

---

## Architecture

```
   browser                                                    workers
  ┌──────────┐                ┌────────────────────┐        ┌──────────────┐
  │ React UI │ ──HTTPS/WSS──▶ │   CONTROL PLANE    │ ◀──────│ agent + Docker│
  │  (web/)  │ ◀────────────  │  (FastAPI, :8000)  │  the agent always
  └──────────┘                │                    │  dials out; nothing
                              │ scheduler · reaper │  ever dials a worker
                              │ fencing · storage  │        └──────────────┘
                              └─────────┬──────────┘
                                        │
                        ┌───────────────┴───────────────┐
                        ▼                               ▼
                 ┌────────────┐                  ┌────────────┐
                 │  Postgres  │                  │   MinIO    │
                 │ state +    │                  │ artifacts, │
                 │ the queue  │                  │ sealed     │
                 └────────────┘                  │ inputs     │
                                                 └────────────┘
```

**Pull model.** The agent opens every connection — register, "I am alive", "give me
work", "here are my logs and results". The control plane never connects out to a
worker. That needs no open ports on a worker and makes one outbound channel carry
everything.

**Hub and spoke, not a chain.** Workers talk only to the control plane; the browser
talks only to the control plane; workers and the browser never talk to each other.
That is load-bearing rather than tidy: the fencing check that refuses a duplicate
result lives **only** in the control plane, so a direct worker-to-browser path would
route around the guarantee this project exists to make.

**The database is the queue.** No broker. Dispatch is a row claimed inside a
transaction, which keeps the orchestration logic in this codebase and visible
rather than hidden inside an external broker.

---

## Layout

```
docker-compose.yml      postgres + minio + control-plane  (+ pgadmin under `tools`)
control-plane/          FastAPI app — app/, alembic/, tests/
  app/scheduler.py        pull-time assignment, matching, the claim query
  app/reaper.py           lease expiry -> LOST -> requeue  (the recovery core)
  app/api/                agent/, jobs, runs, artifacts, keys, nodes, auth, health
agent/                  the worker agent (host software: Docker SDK + stdlib)
web/                    React + Vite UI — pool, submit, live logs, results
workloads/dummy/        the test container + fyp_data.py + fyp_checkpoint.py
                        (fyp_open.py is the pre-2026-09-06 wrapper, kept for old job specs)
scripts/                chaos_test.py, stage_demo.ps1, make_certs.py, seed_job.py …
protocol.md             the FROZEN contract (the wall)
standards.md            ISO/IEC standards mapping
docs/evidence/          proof captures — never edited after capture
```

---

## Run it

Prereqs: Docker and Docker Compose. For an agent, Python on the worker — the
project is built and tested on **3.12** (both images and CI), so match that. If
your default `python` is another version, make a 3.12 environment first and use it
for every `python` below: `py -3.12 -m venv .venv` on Windows (then
`.venv\Scripts\activate`), `python3.12 -m venv .venv` elsewhere (then
`source .venv/bin/activate`).

```bash
# 1. Bring up the world (postgres + minio + control-plane on :8000).
#    The control-plane container runs `alembic upgrade head`, then serves.
docker compose up -d --build

# 2. Check it is alive.
curl localhost:8000/health            # -> {"status":"ok", ...}

# 3. Build the workload image ONCE, on each machine that will run an agent.
#    The agent runs it locally — there is no registry in the demo setup.
docker build -t fyp-dummy:latest workloads/dummy

# 4. Start an agent (install its deps first: pip install -r agent/requirements.txt).
python -m agent --server http://localhost:8000 --name node-a
#    It registers, prints its node_id, and heartbeats every HEARTBEAT_INTERVAL_S.
#    Repeat on other machines (or with other --name values) to grow the pool: one
#    --name per worker. A worker keeps its identity in agent_state-<name>.json in
#    the directory you started it from, so to RESTART a worker start it again with
#    the same --name from the same directory and the pool shows the same machine.

# 5. Open the UI.
cd web && npm install && npm run dev        # -> http://localhost:5173
#    Log in with the demo admin below, submit a job, watch its logs stream live.

# 6. Or submit from the command line and watch it to completion.
python scripts/seed_job.py --epochs 3       # talks plain http unless certs/ca.pem exists
python scripts/seed_job.py --fail           # the failure path
```

**Log in with `admin` / `fyp-admin`.** That admin is created at startup from
`ADMIN_USERNAME` / `ADMIN_PASSWORD` in `docker-compose.yml`. There is no
self-registration: the admin creates every other account from the **Admin** panel on
the dashboard (or `POST /users`) and puts it in a storage tier — `standard` or
`limited`, two numbers each: how much the server keeps for that user, and how much
temporary disk one of their runs may use on a worker. A new user accepts those two
numbers on the submit form before their first job. Change the admin password, and
`JWT_SECRET`, before this ever leaves a lab network.

**The template training script** is `workloads/dummy/train.py`, the image's own
entrypoint. `docker run --rm fyp-dummy:latest python train.py --help` lists its
modes: `--resume` saves a checkpoint every epoch and continues from one after a
re-dispatch, `--read-input` reads the dataset you attached through the sealed reader,
`--write-mb N` and `--fill-scratch` exercise the storage caps, `--oom` and `--crash`
the failure paths. A real script adopts three lines from it: `fyp_data.open_input()`
to read its dataset, `fyp_data.create_output()` to write a result, and
`fyp_checkpoint.save()` / `load()` to survive its machine dying — see
`workloads/dummy/fyp_data.py` and `fyp_checkpoint.py`, which are the whole contract.

**Optional — the database GUI:**

```bash
docker compose --profile tools up -d        # adds pgAdmin at http://localhost:5050
# Only the control plane (:8000) is published to the network. PostgreSQL (:5432),
# MinIO (:9000/:9001) and pgAdmin (:5050) are bound to 127.0.0.1: every access rule
# lives in the control plane, and a database reachable from the LAN would hand out
# the sealed jobs' keys to anyone holding the development password. Change the
# passwords through .env (see .env.example) rather than by editing compose.
```

It opens already connected to the database, with no login page.

**On Windows, one command stages the whole demo** — stack, three worker windows, the
UI and the database GUI, validating everything *before* it touches the database:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\stage_demo.ps1
powershell -ExecutionPolicy Bypass -File scripts\stage_demo.ps1 -CheckSchemaOnly   # look, touch nothing
powershell -ExecutionPolicy Bypass -File scripts\kill_worker.ps1 node-b            # kill a worker properly
powershell -ExecutionPolicy Bypass -File scripts\stop_demo.ps1
```

Kill a worker with `kill_worker.ps1`, never by closing its window: an agent is two
Python processes with identical command lines, so closing the window can leave the
real one still heart-beating — and then the recovery you are trying to demonstrate
never happens.

### Turning on encrypted transport

A fresh clone serves **plain HTTP**: the certificates are gitignored, so nothing is
in the repo to serve. Turning TLS on is one command and no file edit — the control
plane serves HTTPS whenever `certs/server.pem` exists and says which mode it is in on
its first log line.

```bash
python scripts/make_certs.py                # writes certs/{ca,server}.{pem,key}
docker compose up -d --force-recreate control-plane
curl --cacert certs/ca.pem https://localhost:8000/health

# The agent must then be given the authority, and refuses to start without it.
python -m agent --server https://localhost:8000 --name node-a --ca-cert certs/ca.pem
```

The agent trusts **that authority alone**, not the machine's trust store, and there
is deliberately no "skip verification" option. Two mismatches are refused rather than
warned about: an `https` server with no certificate configured, and a certificate
configured against a plain-`http` server. The second matters more — it would
otherwise *succeed*, unencrypted, while the operator believed the certificate was
doing something.

### Tunables (env — the full table is `protocol.md` §8)

| Knob | Default | Governs |
|---|---|---|
| `HEARTBEAT_INTERVAL_S` | 3 | how often an agent checks in |
| `NODE_TIMEOUT_S` | 12 | a node reads offline if its last heartbeat is older than this |
| `LEASE_TTL_S` | 60 | a run is reclaimed as `LOST` if its lease is not renewed in time. 60s since 2026-08-29 (was 15s). The demo runs at 15s, set by `scripts/stage_demo.ps1`, which prints the effective value in its READY banner |
| `REAPER_INTERVAL_S` | 3 | how often the reaper looks (independent of the deadline above) |
| `MAX_ARTIFACT_MB` | 50 | per-file cap on an uploaded result |
| `LOG_RETENTION_DAYS` | 7 | how long a finished run's logs stay in the database before tiering |
| `AGENT_CA_CERT` | *(empty)* | the authority certificate; required when the agent's server is `https` |

`NODE_TIMEOUT_S` and `LEASE_TTL_S` are independent on purpose: one decides when a
*machine* is shown offline, the other when a *run* is taken back.

---

## Tests

```bash
# Control plane — the canonical suite, in the pinned 3.12 container.
# TEST_DATABASE_URL turns on the two Postgres proofs; point it at a THROWAWAY db.
docker compose run --rm \
  -e TEST_DATABASE_URL=postgresql+asyncpg://fyp:fyp@postgres:5432/fyp_test \
  control-plane pytest -q

# Agent — host-side, because the agent is host software.
python -m pytest agent/tests -q

# The web build.
cd web && npm run build
```

At the final measured build, continuous integration reported **control plane 413
passed, 2 skipped**; **agent 190 passed**; **web 25 passed**.

**The skip count is part of the number**, not a footnote. Two tests guard
`SELECT … FOR UPDATE SKIP LOCKED` — the property the whole contribution rests on —
and SQLite cannot express it, so without `TEST_DATABASE_URL` the suite skips them and
**still shows green**. The two skips in the figure above are a different pair: the
checkpoint-advisor tests, which need `workloads/` mounted into the container. Mount it
and the control plane reads 415.

### The chaos test — the most important artefact in the project

```bash
docker compose up -d
docker compose run --rm -v "${PWD}/scripts:/scripts" control-plane python /scripts/chaos_test.py
```

It drives the whole recovery loop deterministically, by advancing the reaper's clock
rather than waiting on a timer: lease expiry → `LOST` → requeue with `attempt`
untouched → the new claim bumps 1→2 → the zombie's stale attempt-1 report refused
`409` → `SUCCEEDED` under attempt 2, with **exactly one accepted result**.

Captured output on the final tree: `docs/evidence/chaos_test_2026-09-06.txt`.

---

## Design questions, answered

**1. Why is node liveness *derived*, not stored?**
A dead machine cannot report its own death. Storing `offline` would need *something*
to write it — a background sweep over nodes. We avoid that: a node is online iff
`now - last_heartbeat <= NODE_TIMEOUT_S`, computed **on read** in `GET /nodes`. Runs
*do* get a reaper, because a lost run must be actively **requeued** — that is an
action. Liveness is a label; recovery is an action. (`app/api/nodes.py`.)

**2. Why did the heartbeat response carry `assignments` from week one, when nothing
filled it until week two?**
Because the wire contract was frozen first (`protocol.md` §9). The agent's loop was
written once; the server then started filling `assignments` at pull-time with no
change to the agent at all. Freezing the shape early is exactly what let three people
build against the same wall in parallel. (`app/schemas.py` → `HeartbeatResponse`.)

**3. How do two machines not run the same job twice?**
Two layers. **Assignment** is one transaction using `SELECT … FOR UPDATE SKIP
LOCKED`: the first claimer locks the row, a second *steps past* it and takes the
next — so no run is handed to two machines at once. **If a machine is wrongly
presumed dead** and its run is re-dispatched, the new lease bumps the run's `attempt`
— the *fencing token*. The old machine's late result still carries the old `attempt`,
the control plane sees it no longer matches, and refuses it with `409`. So a run may
*execute* twice, but **at most one result is ever accepted**. (`app/scheduler.py`,
`app/api/agent.py`.)

**4. What stops a stale result being accepted after recovery — and how do you know?**
The fencing check — and it is worth saying exactly where each half of it lives, because
the short answer used to overclaim. For **log chunks and artefacts** the guard is a
database constraint: `UNIQUE(run_id, attempt, seq)` on `run_logs` and
`UNIQUE(run_id, attempt, object_key)` on `artifacts`. For the **terminal status** it is
a conditional update in application code, taken under a row lock
(`SELECT … FOR UPDATE` in `app/api/agent.py`), with no CHECK constraint behind it: the
lock is what makes the compare-and-set atomic, and the database does not know the rule
on its own. `protocol.md` §2 says the same sentence.
We know it holds because we removed the fence and measured what happened: with the
lease alone and no fencing token, the dead machine's late report became the stored
final answer in **every** repetition. The evidence is in
`docs/evidence/experiments/E1/`.

**5. Is the data on a worker machine safe?**
**Not from the machine's root administrator** — that comes first, every time it is
asked. Whoever owns a worker has root on it by definition and can
read a running container's memory. Nothing in software prevents that; it needs
confidential-computing hardware and is named as future work. We never claim
root-proof privacy.

What sealing does give, with that limit already stated: the set of people who can read
a job's data shrinks from everyone who touches storage, the network, the agent or the
worker's disk down to the owner of the one machine an administrator chose. Every job
is sealed, not just the ones somebody remembered to tick (2026-09-06). The demo proves
each step: the file is gibberish in storage and on the worker's disk, it opens only
inside the container, `docker inspect` shows no key — only a one-shot ticket that is
already spent — one flipped byte fails the run at the piece it is in, and deleting the
key makes every copy unreadable for ever.

---

## How this repository was worked

Trunk-based Git with short-lived branches, and continuous integration (both test
suites, `ruff`, the web build and a dependency audit) on every push. Schema changes
go through Alembic migrations — fourteen, `3816ff8401ff` → `d5e6f7a8b9c0` (the
control plane prints the whole chain when it starts) — each proven up, down and up
again on real PostgreSQL before it merged.

Nothing was "done" until it ran under `docker compose` and had a test. A bug fix
came with a test shown to fail on the code before the fix. Proof captures go in
`docs/evidence/` and are **never edited after capture**; a correction is appended
beside the original.

`protocol.md` is frozen. Every change to it was additive, dated and agreed by the
team first, because moving the contract breaks work done against it in parallel.

---

## Team

Final year project at Université Saint-Joseph de Beyrouth (USJ), École Supérieure
d'Ingénieurs de Beyrouth (ESIB), June–September 2026, supervised by Dr Maroun Ayli.

- **Mohamad Rachid** (lead) — control plane, scheduler, failure recovery, web UI
- **Jaafar El Khatib** — worker agent, container execution, log streaming, authentication
- **Robin Yaghi** — infrastructure: Docker, PostgreSQL, MinIO, CI
