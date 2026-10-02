# `protocol.md` — Frozen Week-1 Contract

> **This is the wall.** It turns the settled design decisions (architecture, schema, API) into one concrete contract that the control plane and the agent are built against **in parallel**.
>
> **FREEZE DATE: 2026-06-02** (all three students read & agreed.)
> Once frozen, change **only** via a dated, recorded decision **and** notifying the partners. No silent edits — moving the wall breaks parallel work.

---

## 1. Scope & auth model

Two trust domains, kept separate on purpose:

- **Agents** authenticate with an opaque **node token** issued at registration. Sent on every agent call as `Authorization: Bearer <node_token>`. (Built in W1.)
- **Users** authenticate with a **JWT** from `/auth/login`, sent as `Authorization: Bearer <jwt>`. **Gating is ON as of W6**: every user endpoint requires a valid login (missing/expired/garbage → `401`). One admin user is created at startup from `ADMIN_USERNAME`/`ADMIN_PASSWORD`; there is no registration endpoint. *(Additive 2026-09-04: "single-user scope" is narrowed rather than reversed — an admin may now create further users with `POST /users`, and each carries a storage tier. There is still **no self-registration**: an account is made FOR someone by an admin, which is a different thing from a stranger signing themselves up. `users.is_admin` is the one role flag; admin-only routes answer `403` to a valid non-admin login, never `401`.)* JWT is authentication only — it does **not** make job data private (that is W6b).
- **`POST /agent/register` is the bootstrap exception** — it *issues* the token, so it cannot require one. In W1 it is open on the LAN. *Open question: add a shared enrollment secret for register. Deferred, not built yet.*

All timestamps are **UTC ISO-8601**. All IDs are **UUID strings** unless stated.

---

## 2. Data model (six tables at the freeze; **twelve** today, every addition additive)

**Six tables were frozen in W1. There are twelve now**, counted off
`control-plane/app/models.py`: `users`, `nodes`, `jobs`, `runs`, `run_logs`,
`artifacts` (the six), plus `run_samples` and `node_events` (W5b), `job_keys` and
`key_tickets` (W6b), `run_log_archives` (2026-08-22) and `tiers` (2026-09-04).
**The wall did not move:**
every one of those six arrived through a dated, recorded decision with its own Alembic
migration, every added column is nullable or carries a server default, and an agent
built against the W1 shapes stays valid — which is the property the freeze existed to
buy, and which the compatibility tests assert rather than assume.

Full column lists live in `control-plane/app/models.py`. The contract-critical constraints:

| Table | Contract-critical fields / constraints |
|---|---|
| `users` | `id, username (unique), password_hash` |
| `nodes` | `id, name, status (reported: idle\|busy), cpu_cores, has_gpu, ram_mb, capacity, last_heartbeat, agent_version, hw_specs (JSON, nullable), usage (JSON, nullable)` — token stored **hashed**, never plaintext. *Additive 2026-07-06: `hw_specs` = rich identity reported once at register (CPU/GPU names, RAM MHz, machine model, OS, sw versions — free-form, best-effort); `usage` = latest heartbeat sample (`cpu_pct`, `ram_pct`, …), current value only. Neither feeds the scheduler — matching stays on the declared columns.* |
| `jobs` | `id, user_id, name, image, entrypoint, env (JSON), resource_reqs (JSON), target_node_ids (JSON, nullable), replicas, status` — *W5b: `resource_reqs` gains an optional `mem_limit_mb` key → the container's memory cap.* — *W6b additive: `private` (bool, default false), `input_object_key` (text, nullable — where the SEALED input lives), `input_filename` (text, nullable — the original name, shown in the UI). A private job's input is AES-GCM-sealed at submit; only ciphertext is ever stored.* |
| `runs` | `id, job_id, node_id (nullable), status, attempt, lease_expires_at, retries_remaining, exit_code` — *W5b additive: `failure_reason` (short label), `failure_detail` (text), `progress` (float 0–1, nullable), `metrics_last` (JSON, nullable). Why a dead run died + live progress. A SUCCEEDED run carries no reason.* — *W5c additive: `learned_min_ram_mb` (int, nullable), `escalation_count` (int, default 0). Control-plane-only (the agent never sees them); set when a proven node-capacity OOM re-dispatches the run to a strictly-stronger node.* |
| `run_logs` | `id, run_id, attempt, seq, chunk, ts` — **`UNIQUE(run_id, attempt, seq)`** (DB enforces no-duplicate log lines, not app code) |
| `run_log_archives` | *Additive 2026-08-22:* `run_id, attempt` (**composite PRIMARY KEY**), `object_key, chunk_count, max_seq, sha256, archived_at` — where ONE (run, attempt)'s log chunks went after they left `run_logs`. A terminal run older than `LOG_RETENTION_DAYS` has its chunks written to one compressed object per (run, attempt) and its rows purged; this row is the pointer. The key being the pair is the schema stating "one object per (run, attempt)", so re-archiving after a crash is an upsert and a second pointer cannot exist. `sha256` is the digest of the canonical body, computed from the rows **before** they were deleted and re-verified against the bytes read back out of the store — it is what makes the purge a two-phase commit rather than a hopeful delete. **No READ SHAPE changes:** `GET /runs/{id}/logs` and `WS /runs/{id}/logs` return exactly the fields they returned before, in the same order, whether a chunk comes from a row or from the archive. |
| `run_samples` | *W5b:* `id, run_id, attempt, ts, cpu_pct, mem_used_mb, mem_limit_mb` — **`UNIQUE(run_id, attempt, ts)`** (a resent sample batch is idempotent, same idea as `run_logs`). The container's own resource history. |
| `node_events` | *W5b:* `id, node_id, ts, event, cause, evidence (JSON)` — the node postmortem history (goodbyes + classified comeback causes). Nodes are still never reaped. |
| `nodes` (W5b additive) | *`battery_pct` (float, nullable), `battery_charging` (bool, nullable) — the last black-box picture before a node goes silent.* |
| `nodes` (W6b additive) | *`trusted` (bool, default false) — may this machine run PRIVATE jobs? Set only by an admin via `PATCH /nodes/{id}/trusted`. **A node can never declare itself trusted**: neither `register` nor `heartbeat` accepts the field, because "trust me" from an untrusted machine is worth nothing.* |
| `tiers` | *Additive 2026-09-04:* `id` (**PRIMARY KEY — the tier's name**), `retained_cap_bytes`, `scratch_cap_bytes`, `description`. Two numbers a user is held to: how many bytes the platform may HOLD for them in object storage across all their jobs, and how many bytes ONE of their runs may write to temporary disk on a worker. Seeded `standard` and `limited`. The figures are **configuration and were never measured** — they are the supervisor's own examples; every proof of the mechanism runs at megabytes. There is deliberately **no route that edits them**. |
| `users` (2026-09-04 additive) | `is_admin` (bool, default false — the startup user is true); `tier_id` (FK → `tiers`, server default `standard`, so every pre-existing row lands in a tier by the schema's own doing); `limits_accepted_tier` (FK, nullable), `limits_accepted_at`, `limits_accepted_retained_cap_bytes`, `limits_accepted_scratch_cap_bytes`. **Acceptance is stored as WHAT WAS AGREED, not as a boolean:** the tier plus both caps as they stood at the moment of agreement. A tier move — or an edit to a tier's figures — withdraws acceptance by arithmetic, so there is no flag to reset and none that can go stale. |
| `runs` (2026-09-04 additive) | `quota_refused_at`, `quota_refused_detail`. Stamped when an artefact upload for **this attempt** was refused over the owner's retained cap; read when the agent later posts a terminal status. This is what lets the **control plane** decide the outcome instead of trusting the agent to report it. |
| `jobs` (2026-09-04 additive) | `input_size_bytes` — the size of the SEALED object, so a private job's input can be counted by the retained sum without asking the object store. |
| `run_log_archives` (2026-09-04 additive) | `size_bytes` — the COMPRESSED bytes of the archived object, written at archive time, so archived logs count towards retained storage. NULL on rows written earlier; `scripts/quota_audit.py --backfill` fills those from the store, and the sum treats NULL as zero (undercounting is the safe direction for a cap). |
| `run_samples` (2026-09-04 additive) | `scratch_used_mb` — MB on the run's temporary disk at sample time, so the "last picture before the failure" shows disk filling the way it already shows RAM climbing. NULL from an older agent. |
| `nodes` (2026-09-04 additive) | `disk_free_mb` — free space on the filesystem holding the agent's working root, refreshed by each heartbeat that carries it. **This is the ONE usage-derived field that feeds matching**, and it narrows the "neither feeds the scheduler" note above by a dated decision rather than quietly breaking it. Why it has to: RAM matching can use the DECLARED total because the kernel enforces the container's cap separately, so declared RAM stays a true statement about what a machine can be asked for; disk has no equivalent per-container cap on our machines, so free space now is the only truthful placement signal. NULL = unknown, and unknown never excludes a machine. |
| `job_keys` | *W6b:* `job_id (PK/FK), key_b64, created_at` — the AES-GCM key for ONE private job. A separate table on purpose: the key is stored apart from the data it opens, and crypto-shred is one DELETE. Released only through the fenced ticket path — never in any read shape. |
| `key_tickets` | *W6b:* `ticket (PK), run_id, attempt, expires_at, redeemed_at, created_at` — the single-use pass a container redeems ONCE for the key, so the key never sits in an env var. `redeemed_at` is the one-shot latch (stamped in the same transaction that returns the key); `attempt` lets the redeem re-check the fence. |
| `artifacts` | `id, run_id, object_key, size, content_type` — *W6 additive: `attempt` (int) + **`UNIQUE(run_id, attempt, object_key)`** — the fencing token + a log-style idempotency key, so a re-sent upload upserts one row and a stale attempt's file can't pose as the accepted result. Object key = `runs/{run_id}/{attempt}/{filename}`.* **Additive 2026-08-13: `kind` (`result`\|`checkpoint`, NOT NULL, server_default `result`) + `sha256` (nullable, the digest of the stored bytes, computed by the control plane).** A **result** is readable only for the run's CURRENT attempt (the fence, unchanged); a **checkpoint** is readable ACROSS attempts, which is what lets a re-dispatched run resume. The rule lives in one query, not in which route asks. |

| `jobs` (2026-09-06 additive) | `sealed` (bool, server default false — true on every job created from that date) and `trusted_only` (bool, server default false). What used to be the single flag `private`, told apart into the two different things it meant. `sealed` is about the DATA: this job has its own key in `job_keys`, its input was sealed at submit, and its results and checkpoints are sealed inside the container before they leave it. `trusted_only` is about PLACEMENT: run only on machines an admin marked `trusted`. `private` is neither dropped nor renamed — it names the OLD submission shape, whose input is sealed in the one-piece format and opened into a RAM folder, and the rows that carry it are still run that way. The migration copies `private → trusted_only` so an old private job keeps landing on exactly the machines it always did, and deliberately does NOT set `sealed`, because those inputs are in the older format and their containers stage the older way. |
| `artifacts` (2026-09-06 additive) | *Same row:* `sealed` (bool, server default false) — these stored bytes are in the framed sealed format, so `GET /artifacts/{id}/download` opens them for their owner. Set from the BYTES at upload time (the format's magic), never from anything the uploader said about them: on a sealed job an artefact that is not sealed is refused at the door, so the column and the bytes it describes cannot drift apart. |

**Sealed blob format (2026-09-06 — the second format, and the first is read for ever).**
Two formats exist and a reader tells them apart by sniffing the first eight bytes, never by a flag anyone passes in:

- **one piece** (W6b, never written again): `nonce(12) || ciphertext+tag`. To read any of it you must hold all of it, which is why the old private path opened a whole dataset into a RAM folder before the workload started, and why the size of the file was the size of the memory needed to read it.
- **framed** (`FYPSEAL2`, written by everything since): a 32-byte header — magic(8), version(1), reserved(3), `chunk_size`(4, big-endian, the PLAINTEXT bytes per piece), `stream_id`(16, random per seal) — followed by pieces of `nonce(12) || ciphertext+tag`. Every piece but the last carries exactly `chunk_size` plaintext bytes, so a piece's position is arithmetic and a reader can seek without an index. Each piece is sealed with `header || piece_index || is_last` as its **AAD**, which is what stops a piece being changed (its own tag), reordered (wrong index), spliced in from another file even under the same key (wrong `stream_id`), truncated off the end (the piece that is now last was sealed as not-last), or read under an edited header (the header is in every AAD). The default piece is 4 MiB.

Implemented twice on purpose — `control-plane/app/sealing.py` and `workloads/dummy/fyp_data.py` — because the second has to be copyable into any workload image with nothing but `cryptography` available, and importing the control plane into a container would drag a web framework and a database driver with it. `agent/tests/test_fyp_data.py::test_the_two_implementations_agree` seals with one and opens with the other, in both directions.

**Result-acceptance guard:** a terminal result (`SUCCEEDED`/`FAILED` + artifacts) is accepted **only** when the reported `attempt` equals the run's current `attempt`. This is the teeth behind at-most-once accepted result. *Where it is enforced, said exactly (corrected 2026-09-07; the sentence used to read "enforced at the DB layer" for all three):* for log chunks and artefacts the guard is a database constraint — `UNIQUE(run_id, attempt, seq)` on `run_logs` and `UNIQUE(run_id, attempt, object_key)` on `artifacts`; for the terminal status it is a conditional update in application code, taken under a row lock (`SELECT … FOR UPDATE` on the run in `control-plane/app/api/agent.py`), with no CHECK constraint behind it. The lock is what makes the compare-and-set atomic; the database does not know the rule on its own.

**`nodes.status` vs displayed liveness — read this once so it never confuses you:**
- `nodes.status` stores the node's **self-reported** state from its last heartbeat: `idle` or `busy`. A node never reports itself `offline`.
- **Online/offline is derived at read time**, not stored: `online = (now - last_heartbeat) <= NODE_TIMEOUT_S`. `GET /nodes` returns both `online` (derived) and `reported_status` (stored). No background sweep touches nodes — only runs get the reaper (W5).

---

## 3. Run state machine

```
PENDING ──assign──▶ ASSIGNED ──container started──▶ RUNNING ──exit 0──▶ SUCCEEDED (terminal)
   ▲                   │                                │
   │                   │                                └──non-zero / unrecoverable──▶ FAILED (terminal)
   │                   │
   └──requeue if       └──── lease expires (ASSIGNED or RUNNING) ────▶ LOST
      retries remain                                                    │
   ◀────────────────────────────────────────────────────────────────────┤
                                                                         └──no retries left──▶ FAILED
```

| State | Meaning |
|---|---|
| `PENDING` | created or requeued; awaiting assignment |
| `ASSIGNED` | leased to a node; container not yet confirmed running |
| `RUNNING` | agent confirmed container started |
| `SUCCEEDED` | exit 0, artifacts uploaded (terminal) |
| `FAILED` | non-zero exit / unrecoverable / retries exhausted (terminal) |
| `LOST` | lease expired while `ASSIGNED`/`RUNNING` → `PENDING` if retries remain, else `FAILED` |

Each run carries: `attempt` (the fencing token / epoch), `node_id`, `lease_expires_at`, `retries_remaining`.

---

## 4. Scheduling & matching — assignment happens AT PULL-TIME

There is no separate scheduler loop. When agent **X** heartbeats with spare capacity, the control plane, **inside one DB transaction**:

1. Finds a `PENDING` run eligible for X: target-node match (if the job specified `target_node_ids`) **AND** `resource_reqs` satisfied (GPU/RAM) **AND** X currently online.
2. Claims it with `SELECT … FOR UPDATE SKIP LOCKED` — two concurrent pulls cannot grab the same run.
3. Sets `status=ASSIGNED`, `node_id=X`, **increments `attempt`**, sets `lease_expires_at = now + LEASE_TTL_S`; returns the run spec in the heartbeat response.

**Capacity** = max concurrent runs per node (configurable; default derived from `cpu_cores`).

**W6b trust filter (one more condition in the SAME claim query, no new stage):** a `private` job's runs are only offered to a node that is `trusted` **and** whose `agent_version` is ≥ `0.8.0` (an older agent cannot stage a sealed run, so it is never sent one — the compat guard). Step 1 therefore matches on **declared specs + learned requirements (W5c) + trust**, in one query. A private run with no eligible trusted node **stays `PENDING` and waits** (the UI says "waiting for a trusted node") — deliberately unlike W5c's `INSUFFICIENT_POOL` fail-fast, because trust can be granted at any moment with one admin action, so waiting is honest rather than hopeless.

**Temporary-disk filter (one more condition in the SAME eligibility test that already answers GPU and RAM, no new stage):** a job whose submitter asked for `resource_reqs.scratch_mb` **explicitly** is only offered to a node whose reported `disk_free_mb` is at least that (NULL = unknown, which never excludes) **and** whose `agent_version` is ≥ `0.10.0` (an older agent cannot stop a run at a disk cap, so it is never sent a run that requires one — the same compat guard shape as W6b's). A run no online machine can satisfy stays `PENDING`, exactly as a GPU requirement does today.

**A ceiling is not a requirement, and the difference is load-bearing.** A job that left `scratch_mb` empty carries its owner's tier ceiling instead, and that ceiling filters **nothing** — it is only the number the agent stops the run at. Treating it as a requirement would filter every untargeted run on a number nobody asked for. *(This sentence used to argue the point from a 500 GB default tier; the tier re-sizing of 2026-09-05, migration `a2b3c4d5e6f7`, took the standard scratch cap to **50 GB** and the retained cap to 200 GB, so the old arithmetic no longer holds even though the rule does — 50 GB is a cap a lab machine may well have free, which is exactly why it must not double as an eligibility test.)* This is a **filter, not a score**: among eligible machines the first to ask still wins, and the untargeted spread preference is unchanged.

**How the candidate is found (2026-09-06 — an implementation clarification, no shape change).** Step 1 above says "finds a `PENDING` run eligible for X", and until this date the code looked in a fixed window of the queue's head (`LIMIT spare * 4`) and filtered it afterwards. That was **head-of-line blocking**: a run X could not take still occupied a place in the window, so a queue whose head was full of such runs hid every run behind them from X — and the runs that collect at the head are exactly those, because a run targeted at a machine that never comes back waits for ever by design (nothing reaps a node; the machine may return). Four of them were enough to starve a one-core machine completely (measured before and after on the same stack: `docs/evidence/targeted_starvation_2026-09-06.txt`).

The scheduler now **walks the pending queue in pages, without a lock, oldest first**, applies the same eligibility test to each row, and then **locks exactly the rows it is taking** and re-checks that they are still `PENDING`. The re-check under that lock is what makes the unlocked walk safe: a run another claimer took in between is simply not returned. So **the claim is still one locked query inside the heartbeat's one transaction**, and what changed is only which rows it locks — the ones being taken, rather than a window of the queue's head. Consequences worth stating: a node no longer makes every other claimer step over rows it was only going to reject, and a run is never hidden by runs ahead of it that this node cannot take, however many there are. The walk stops after `MAX_CLAIM_SCAN` rows (5000) and says so in the log; that is a bound on cost, not on correctness, and at this project's scale it is never reached.

**What that one transaction contains, counted (2026-09-06 — additive; the sentence above is unchanged and remains exact).** "One locked query" says one query takes a lock, and that is still true to the letter: exactly one statement in the claim path carries `FOR UPDATE`, the `SELECT ... FOR UPDATE SKIP LOCKED` built and executed in `assign_runs` (`control-plane/app/scheduler.py`). What the paragraph above does not say is what sits around it, so it is written down here rather than left to be rediscovered. In one heartbeat's transaction the claim path issues, in order: one unlocked count of the runs this node already holds, which is where `spare` comes from; one unlocked `SELECT` per page of the walk, sixty-four rows a page (`_SCAN_PAGE`); one unlocked count per distinct job the walk reaches, asking whether this node already holds a sibling of it (`_holds_sibling_run`); and, for an untargeted job whose replica this node has already picked or already holds, one unlocked read of the machines that already hold a run of that job, one unlocked read of the node list, and one unlocked running count per machine examined until a free one is found — asked once per job and reused for the rest of its replicas (`:485`, `:306`, `:316`, `:339`). Then the one locked statement, then the rows it returned are mutated to `ASSIGNED` and the heartbeat handler commits. So the claim is a read-then-lock inside one transaction, and it always has been since the walk replaced the fixed window on this date; what the 2026-09-06 spread-preference fix added is the two reads named third and fourth. None of this is visible to an agent: no request or response shape changes, no column changes, and an old agent is unaffected.

**A run aimed at a machine that is not here WAITS, and says so (2026-09-06).** The choice was between waiting and failing after a stated time, and waiting is the honest one for the same reason W6b's trust tier waits: the condition can change without anyone re-submitting anything — a laptop comes back. It is the opposite of W5c's `INSUFFICIENT_POOL`, which fails fast precisely because no amount of waiting can conjure a bigger machine. What was wrong before this date was not the waiting but the silence: such a run sat `PENDING` for ever with nothing anywhere saying what it was waiting for, and it filled other machines' claim windows while it did. `GET /jobs/{id}/runs` therefore gains one optional field, `waiting_for` — a sentence naming the machines and how long they have been quiet ("waiting for lab-pc-03 (silent for 2h)"), null for every run that is not in that position, which is almost all of them. Additive: an older caller ignores it. *(2026-09-07, walk 1 row 66: the same sentence also covers a targeted machine that is online but whose agent is too old to be offered a sealed run — "lab-pc-03 is online but its agent is 0.11.0 and this job needs agent 0.12.0 or newer" — because from the run's side that machine is not here either, and it used to wait with nothing said.)*

**Job → runs fan-out (at submission):**
- `target_node_ids` set → **one run per listed node** (`replicas` ignored).
- `target_node_ids` null → **`replicas` runs** created `PENDING`, scheduled to any eligible node at pull-time.

---

## 5. Heartbeat, failure detection, the reaper

- Agents heartbeat every `HEARTBEAT_INTERVAL_S`.
- **Background reaper** (sweep every few seconds), W5:
  - Run in `ASSIGNED`/`RUNNING` with `lease_expires_at < now` → `LOST` → `PENDING` if `retries_remaining > 0`, else `FAILED`.
- Node liveness is **not** reaped — it's computed at read time (§2).

---

## 6. Lease + fencing — the no-duplicate-accepted-result guarantee

- Every agent message about a run carries the `attempt` it is working on.
- Re-dispatch **increments** `attempt` (N → N+1).
- A late message from a presumed-dead node arrives with **stale `attempt=N`** while current is `N+1` → **rejected with `409`**; the agent is told to abort that run.
- Result: a run may **execute** more than once under failure, but **at most one result is ever accepted.**

---

## 7. Reliability guarantees (state these *exactly* — never "exactly-once")

1. **At-least-once execution** — under failure a run may run more than once.
2. **At-most-once accepted result** — lease + fencing + the result-acceptance guard ensure only one result is ever committed. *(Where that guard lives is said exactly in §2, corrected 2026-09-07: a database constraint for log chunks and artefacts, and a conditional update under a row lock in application code for the terminal status. This line used to read "the DB result-acceptance guard" and was the last site still saying it.)*
3. **No silent loss, no duplicate accepted** — the reaper detects `LOST` runs and requeues them; `UNIQUE(run_id, attempt, seq)` and `UNIQUE(run_id, attempt, object_key)` block duplicate chunks and artefacts **at the database**, and the terminal status is blocked by the fenced compare-and-set **under a row lock in application code** (§2).

**W6b data-privacy guarantee (state it *exactly* — never "root-proof", never "fully secure"):** job data is sealed from submit until it opens inside the container, never touches the worker's disk in readable form, and any tampering breaks the seal and fails the run. That protects it from **users** of the machine — not from its **root administrator**, who can read a running container's memory. Root-proof privacy needs TEE hardware (our named future work). Two parts of this are absolute and may be claimed as such: **tamper detection** (guaranteed by AES-GCM's tag, so it holds even against an attacker with full control of the machine) and **crypto-shred** (delete the key and every sealed copy is unreadable, everywhere, forever). Everything else is scoped by the sentence above.

---

**Checkpoints do not change any of the above (2026-08-13).** A *result* is a claim
about a finished run, and accepting two would break at-most-once — so the result
reads stay attempt-scoped and in fact gained a second filter. A *checkpoint* is
intermediate working state; a later attempt may read one written by an earlier
attempt, and the worst that can cost is repeated training. **The request is fenced,
the object is not:** the node asking must still own the run at its current attempt.
A checkpoint is never listed or downloaded as a result.

---

## 8. Tunables (env vars, with defaults)

| Knob | Default | Governs |
|---|---|---|
| `HEARTBEAT_INTERVAL_S` | `3` | how often agents check in |
| `NODE_TIMEOUT_S` | `12` | node shown offline if `last_heartbeat` older than this (read-time) |
| `LEASE_TTL_S` | `60` | run reclaimed as `LOST` if lease not renewed in time (reaper). **60s since 2026-08-29** (was 15s): a machine that drops for under a minute should carry on rather than be fenced out, and the fencing token is what makes either value safe, because a false alarm costs one duplicate attempt's work and never a wrong result. The price is that a truly dead machine is detected in about a minute rather than about fifteen seconds. The **demonstration** deliberately runs at `15` so recovery is visible inside the slot; `scripts/stage_demo.ps1` sets it and prints the effective value in its READY banner |
| `REAPER_INTERVAL_S` | `3` | *(W5)* how often the reaper sweeps for expired leases. **Independent of `LEASE_TTL_S`**, which is the deadline: this is only how often we look. Neither the tests nor `scripts/chaos_test.py` depend on this value: a test that needs an expired lease sets `lease_expires_at` to an absolute past time, and the chaos test advances the reaper's clock past whatever the lease is. So the default can move without moving them |
| `MAX_ARTIFACT_MB` | `50` | *(W6)* per-file cap on an uploaded artifact; over it the upload is `413` (see §9). The agent carries its own matching `AGENT_MAX_ARTIFACT_MB` so an over-size file is refused before it is sent as well as when it arrives |
| `KEY_TICKET_TTL_S` | `120` | *(W6b)* how long a key ticket stays redeemable — it only has to survive the gap between "agent asks" and "container starts" |
| `PRIVATE_TMPFS_MB` | `256` | *(W6b, agent-side; **also read by the control plane since 2026-09-04**)* size of the RAM folder a private container opens its data into. A private run has no writable host mount, so that folder **is** its scratch — kernel-enforced and exact, never sampled — which is why the control plane needs the value too: it is the ceiling a private job's `scratch_mb` is validated against. Two places that must agree, the same situation as `LEASE_TTL_S` and `docker-compose.yml`, and named as such rather than left to be discovered. **2026-09-06: read only by the OLD private path now.** A sealed run has the ordinary three writable folders on disk and no RAM folder, so nothing validates a scratch ask against this any more — the owner's tier ceiling is the one ceiling, on every door |
| `MAX_INPUT_MB` | `100` | *(W6b)* per-file cap on a submitted private input; over it the submit is `413` |
| `AGENT_CHECKPOINT_INTERVAL_S` | `30` | *(2026-08-13, agent-side)* how often a running container's checkpoint is swept up. Not every heartbeat: a checkpoint can be large and saving it again three seconds later buys little. An unchanged file is skipped entirely |
| `CONTAINER_KEY_URL` | *(empty)* | *(W6b)* fixed address a container redeems its ticket at. Empty = derived from the agent's own request (a loopback host becomes `host.docker.internal`, which is how a container reaches its host) |
| `LOG_RETENTION_DAYS` | `7` | *(2026-08-22)* how long a run's log chunks stay in `run_logs` after the run reaches a terminal state. Past this they move to one compressed object per (run, attempt) and the rows are purged. Seven days is an operational default **and** a demonstration guard: a run created in the room is minutes old, so nothing anyone is watching can move while they watch it |
| `LOG_ARCHIVE_INTERVAL_S` | `3600` | *(2026-08-22)* how often the archiver sweeps. Deliberately slow — the window is measured in days, so a faster sweep would only add load to the database the scheduler claims work from |
| `LOG_ARCHIVE_ENABLED` | `true` | *(2026-08-22)* the off switch, for a deployment that wants every log line to stay in the database for ever |
| `AGENT_CA_CERT` | *(empty)* | *(2026-08-22, agent-side)* PEM certificate of the authority that signed the control plane's certificate. **Required** when the agent's `--server` is `https`, and the agent refuses to start without it rather than trusting whoever answers. Empty means plain HTTP, unchanged. The agent trusts this authority **alone** — not the machine's system trust store — and passes the same certificate to a private job's container so it can verify the key endpoint too. Generate one with `scripts/make_certs.py` |

`NODE_TIMEOUT_S` and `LEASE_TTL_S` are **independent** knobs: one decides when a *node* is displayed offline, the other when a *run* is reclaimed. Trade detection speed vs false positives; document the chosen values in the report.

---

## 9. Agent ↔ control-plane API (node-token auth; `register` excepted)

### `POST /agent/register`
Request:
```json
{
  "name": "lab-pc-01",
  "specs": {
    "cpu_cores": 8, "has_gpu": true, "ram_mb": 16384, "capacity": 8, "agent_version": "0.1.0",
    "hw_specs": { "cpu_name": "…", "gpu_name": "…", "ram_mhz": 3200, "machine_model": "…", "os": "…", "python_version": "…", "docker_version": "…", "disk_total_gb": 476.9 }
  }
}
```
`specs.hw_specs` is **optional + free-form** (added 2026-07-06): best-effort machine identity for the dashboard. Agents omit anything they can't detect; older agents omit it entirely and stay valid.
Response `200`:
```json
{ "node_id": "uuid", "token": "opaque-node-token-returned-once" }
```

### `POST /agent/heartbeat`
Request:
```json
{
  "node_id": "uuid",
  "status": "idle",
  "running": [ { "run_id": "uuid", "attempt": 3, "state": "RUNNING", "progress": 0.4, "metrics": {"epoch":2,"total":5,"loss":0.5} } ],
  "usage": { "cpu_pct": 12.5, "ram_pct": 61.0, "disk_pct": 49.1, "gpu_pct": 0.0 },
  "battery_pct": 78.0, "battery_charging": false,
  "interview": { "failed_deliveries": [{"ts":1000.0,"error":"URLError"}], "slept_ranges": [], "reboot": false, "new_agent_session": false, "dirty_shutdown": false }
}
```
`usage` is **optional** (added 2026-07-06): the latest usage sample for the dashboard. Omitted → the stored sample is kept (an old agent never wipes it). Current value only — time-series history is out of scope (Prometheus stretch).
**2026-09-04 additive:** `usage.disk_free_mb` — free space on the filesystem holding the agent's working root. Optional; an agent that omits it leaves the stored value alone rather than wiping it. It is lifted out of the free-form sample into the `nodes.disk_free_mb` column because, unlike everything else in `usage`, the scheduler reads it (§4).
**W5b additions (all optional — old agents omit them and stay valid):** each `running` item may carry `progress` (0–1) + `metrics` (latest `##PROGRESS` JSON), stored fenced (only when the reported attempt matches); `battery_pct` + `battery_charging` refresh the node black box; `interview` (present only on the first contact after an outage) is classified into a `node_events` row (network partition / sleep / agent crash / clean shutdown / power loss).
Response `200` — **assignment happens here** (W1: both arrays always empty):
```json
{
  "assignments": [
    { "run_id": "uuid", "attempt": 1, "image": "repo/img:tag", "entrypoint": ["python","train.py"], "env": {"LR":"0.01"}, "mem_limit_mb": 128 }
  ],
  "commands": [ { "type": "cancel", "run_id": "uuid" } ]
}
```
`assignments[].mem_limit_mb` is **optional** (W5b): the container memory cap from the job's `resource_reqs.mem_limit_mb`. Null → no cap. The agent applies it as `--memory` (+ `--memory-swap`), so an over-budget run is OOM-killed by the kernel.
`assignments[].scratch_mb` is **optional** (2026-09-04): how many MB this run may write to temporary disk on the worker — the submitter's explicit ask if they made one, otherwise their tier's ceiling. Null → no cap, which is what an unowned job gets and what every job got before this date. The agent measures its own per-run directory each sample tick and stops the container when it crosses this; an agent that predates the field ignores it and enforces nothing, and the claim query's version guard is what stops an EXPLICIT ask from ever reaching such an agent.
**W6b additions (all optional):** `assignments[].private` (bool, default false), `job_id`, `input_filename` — tell the agent this run needs sealed staging. **No key is ever in an assignment**; the agent fetches a one-shot ticket instead (below). Old agents never receive `private: true` (the claim query's compat guard), so their shapes are unchanged.

**Assignment fields (additive, 2026-09-06):** `sealed` (bool, default false) — this run's data is sealed, so the agent fetches a one-shot key ticket for the container (whether or not there is an input file, because outputs and checkpoints are sealed too) and mounts any sealed input read-only. Never true at the same time as `private`, which names the older staging path. A sealed run keeps the ordinary three writable folders, so — unlike a private one — it collects results and can be resumed on another machine. An agent older than `scheduler.MIN_SEALED_AGENT_VERSION` (0.12.0) is never offered a sealed run at all, the same guard shape as the private and scratch ones; since every job created from 2026-09-06 is sealed, the practical effect is that an out-of-date machine is offered nothing new, which is the intended answer rather than a side effect to soften.

### `POST /agent/runs/{run_id}/logs`
```json
{ "attempt": 1, "seq": 42, "chunk": "epoch 1 loss=0.5\n" }
```
`200` accepted · `409` stale attempt (abort) · duplicate `(run_id, attempt, seq)` rejected by DB constraint.

### `POST /agent/runs/{run_id}/status`
```json
{ "attempt": 1, "state": "FAILED", "exit_code": 137, "failure_reason": "OOM_KILLED", "failure_detail": "RAM overload: the kernel killed the container after it exceeded its memory limit (128 MB)." }
```
`state ∈ {RUNNING, SUCCEEDED, FAILED}`. `200` accepted · `409` fencing rejection — stale `attempt`, or a run not owned by the calling node (abort). Terminal states only committed when `attempt` matches current (result-acceptance guard).
`failure_reason` + `failure_detail` are **optional** (W5b): the agent's hard-fact classification on a FAILED post (OOM / GPU / image / exit signal). Omitted on RUNNING/SUCCEEDED; a FAILED post without them gets a server-side exit-code fallback. A SUCCEEDED/RUNNING post clears any prior reason.
*(2026-09-07, additive — two more values, from the first stranger's walk. `ARTIFACT_TOO_LARGE`: the agent posts FAILED with it when a result file is over the per-file cap, whether the agent refused to send it or the control plane answered `413` without a `reason`; the detail names the file, its size and the cap. Before this a run whose only result was dropped for size read SUCCEEDED with no output. `INPUT_NOT_OPENED`: set by the control plane on a SUCCEEDED run of a **sealed** job that carried a dataset whose key ticket was never redeemed — the sealed-shape twin of `PRIVATE_INPUT_NOT_OPENED`, whose message named the old opener wrapper; this one names `fyp_data.open_input()`. Old agents post neither and are unaffected. Also additive on this post, same date (row 25): optional `progress` and `metrics`, the last `##PROGRESS` marker the container printed, so a terminal post carries the finish rather than the heartbeat before it; omitted by an older agent, in which case the last heartbeat's values stand.)*
**W5c behaviour (no message change):** when a FAILED post carries `failure_reason=OOM_KILLED` and the kill was at the *node's* capacity (the job set no smaller `mem_limit_mb`), the server may requeue the run to a strictly-stronger node (back to `PENDING`, `attempt` untouched — the next claim bumps it) instead of leaving it FAILED; if no registered node has more RAM it fails with `failure_reason=INSUFFICIENT_POOL`. A kill at the user's own `mem_limit_mb` stays FAILED (their cap, not the machine).

### `POST /agent/runs/{run_id}/samples`  *(W5b)*
```json
{ "attempt": 1, "samples": [ { "ts": 1721131200.0, "cpu_pct": 22.5, "mem_used_mb": 90.0, "mem_limit_mb": 128.0 } ] }
```
The container's own resource samples. **2026-09-04 additive:** each sample may carry `scratch_used_mb`, the MB in the run's temporary directory on the worker at that moment — optional, so an older agent omits it and the stored row is NULL rather than zero ("did not report" and "wrote nothing" are different facts). `200` accepted (`{accepted, stored}`) · `409` stale attempt / wrong node (abort). `ts` (agent wall clock) is the dedup key: `UNIQUE(run_id, attempt, ts)` makes a resent batch idempotent (same philosophy as logs).

### `POST /agent/goodbye`  *(W5b)*
```json
{ "reason": "shutdown" }
```
Best-effort "going offline" on a clean stop → a `CLEAN_SHUTDOWN` `node_events` row (an exact cause). Node-token auth.

### `POST /agent/runs/{run_id}/artifacts`  (multipart)
Multipart form fields: `attempt`, `filename`, `file` (one file per call), and — additive 2026-08-13 — `kind` (`result` default \| `checkpoint`; an unknown value is `422`). An agent that predates `kind` sends nothing and stores a result exactly as before. Node-token auth. The control plane brokers the write to MinIO (agents never hold storage creds) under the deterministic key `runs/{run_id}/{attempt}/{filename}`, then indexes the row. `200` `{artifact_id, run_id, attempt, filename, object_key, size, content_type}` · `404` unknown run · `409` stale attempt / wrong node (abort) · `413` over the per-file cap `MAX_ARTIFACT_MB` (default 50) · **`413` `{"reason": "STORAGE_QUOTA_EXCEEDED", "used_mb", "cap_mb", "file_mb"}` (2026-09-04) when storing it would take the job's owner over their tier's retained cap.** Nothing is stored. Checked **after** the whole fence and after the per-file cap, and both halves of that ordering matter: after the fence, because a stale attempt must never consume quota; and reusing `413`, because an agent that predates this already treats a `413` from this call as fatal-for-this-file and so needs no change. A repeat under a stable object key (a checkpoint) is charged only its **growth** — the test is `used - old_size + new_size <= cap` — so a checkpoint that stays the same size costs nothing more. **The refusal decides the run, and the control plane decides it:** that attempt is recorded `FAILED` with `failure_reason=STORAGE_QUOTA_EXCEEDED` whatever the agent posts next, and it is **not** re-dispatched, because it is the user's cap and not the machine's (the rule W5c already applies to a kill at the user's own `mem_limit_mb`). Idempotent (W6): a re-sent upload overwrites the same object and `UNIQUE(run_id, attempt, object_key)` collapses it to one row — a lost-200 retry stores exactly one copy. 2026-08-13: because a checkpoint is written to a stable name, repeats carry NEW bytes, so that one row is refreshed (`size`, `content_type`, `sha256`) to describe the object now in storage; `kind` is fixed by the write that created the row, so a later upload cannot turn a checkpoint into a result. The digest is computed by the control plane from the bytes it stored, never accepted from the uploader.

**`422 {"reason": "UNSEALED_OUTPUT", "filename", "detail"}` (2026-09-06)** when the run's job is `sealed` and the uploaded bytes are not. Nothing is stored. Checked after the whole fence — a stale attempt is told it is stale, not told its bytes were the wrong shape — and after `kind` and the per-file cap. This is the door where "storage holds only sealed bytes" stops being a claim about how a workload behaves and becomes a property of the system: the check reads the FORMAT of the bytes in front of it, so a container that wrote its results with plain `open` cannot get them into storage however it was built. `422` rather than `413` because the file is not too big, it is the wrong thing; and unlike the quota refusal it needs no stamp on the run, because an agent that ignores it simply stores nothing, which is the same outcome for the data either way. The agent turns it into a `FAILED` status with `failure_reason=UNSEALED_OUTPUT` — a run whose results cannot be kept has not succeeded, and the exit code is zero, so nothing else would have said so.

### `GET /agent/runs/{run_id}/checkpoint?attempt=N`  *(2026-08-13)*
Streams the **newest checkpoint for this run, across attempts** (`application/octet-stream`), so a re-dispatched run resumes instead of restarting its training. Node-token auth, and **fenced on the request exactly like a status post**: `404` unknown run · `409` run not owned by this node · `409` stale attempt (abort). What is relaxed is only which attempt the returned *object* came from, and only because the query asks for `kind=checkpoint`.
`200` bytes, with `X-Checkpoint-Sha256` (the digest recorded when the bytes were stored) and `X-Checkpoint-Attempt` (which attempt wrote it) · **`204` nothing to resume from — absent is NOT an error**, and covers both "no checkpoint saved" and "storage cannot produce the bytes". The agent re-hashes what it receives and treats a mismatch as absent too: a run that crashes on resume is worse than one that starts over. A **private** run has no writable host mount, so it can neither keep a checkpoint nor be resumed.

### `GET /agent/jobs/{job_id}/input`  *(W6b)*
Streams the job's **SEALED** input (`application/octet-stream`). Node-token auth. Brokered through the control plane, so workers still hold no MinIO credentials (A.8.31 unchanged). Access rule: the calling node must currently hold a **live run** (`ASSIGNED`/`RUNNING`) of that job — the input-side twin of the run fence. `200` ciphertext · `404` no sealed input for this job — **and the same `404` when the sealed bytes cannot be read back out of the store**, because from the agent's side those are one condition: there is nothing to fetch · `409` the node holds no live run of it. What comes back is ciphertext the agent has no way to open: the agent is a courier, not a reader.

### `POST /agent/runs/{run_id}/key-ticket?attempt=N`  *(W6b)*
→ `{ "ticket": "opaque", "expires_at": "2026-…", "key_url": "https://…/container/key" }`
The **scheme follows the address the agent called us on**, because the control plane
builds this URL from that request rather than from a setting. An agent that reached
us over `https` is handed an `https` key URL and its container verifies the
certificate before spending the ticket; a plain-`http` deployment gets `http` and
behaves exactly as it did before. *(2026-08-22, encrypted transport — the field, its
shape and its meaning are unchanged.)*
*(2026-09-06: issued for any **sealed** job, with or without an input file, because the key is no longer only for reading — the container also seals its results and its checkpoints with it. `404` only when the job has no key at all, which is a row from before that date submitted without the private door.)*
Node-token auth, **fenced exactly like a status post**: `404` unknown run · `409` run not owned by this node · `409` stale attempt (abort). So the fencing token now guards the data going **in** as well as the result coming **out** — a node whose run was re-dispatched cannot obtain the key for work that is no longer its own. Re-issuing drops any previous ticket for the same `(run, attempt)`. TTL `KEY_TICKET_TTL_S` (default 120 s). **The agent receives a ticket, never a key.**

### `POST /container/key`  *(W6b)*
```json
{ "ticket": "opaque" }
```
→ `{ "key_b64": "…" }`, **once**. Called from **inside the running container** by the startup helper. **The ticket is the credential** — the container holds no node token and no JWT, because it runs untrusted user code and is handed the smallest possible one-shot pass. `404` unknown ticket · `410` already redeemed (the one-shot latch, stamped in the same transaction that returns the key) · `410` expired · `410` the job's key was crypto-shredded (permanently unreadable) · `409` the run's `attempt` has moved on (the fence, re-checked at redeem time, not only at issue time). Consequence: `docker inspect` on a live private container shows only a ticket that is already dead.

---

## 10. User ↔ control-plane API (JWT auth; gating switches on W6)

### `POST /auth/login`
```json
{ "username": "alice", "password": "..." }
```
→ `{ "token": "jwt", "expires_at": "2026-..." }` *(W6 shape; HS256, 12 h expiry, `sub` = user id.)* A wrong username and a wrong password return the same `401` (no disclosure of which existed). Not gated (this is where you get the token). **Every other user endpoint below requires `Authorization: Bearer <jwt>`.**

### Read scoping — a user reads only their own work  *(2026-09-07)*
Until this date a valid login could read **every** job on the platform. Authentication (W6) answers "who are you"; nothing answered "is this yours", which was survivable while a deployment had one account and stopped being survivable on 2026-09-04, when an admin gained `POST /users`. The rule now, written once in `control-plane/app/ownership.py` and applied by every route below that carries job data:

- **A job belongs to the user who submitted it** (`jobs.user_id`, carried since W1 and read from this date). `GET /jobs` returns the caller's own rows; `GET /jobs/{id}`, `GET /jobs/{id}/runs`, `GET /runs/{id}/logs`, `WS /runs/{id}/logs`, `GET /runs/{id}/samples`, `GET /runs/{id}/artifacts`, `GET /artifacts/{id}/download`, `POST /jobs/{id}/cancel`, `DELETE /jobs/{id}/key` and `DELETE /jobs/{id}/storage` all resolve the job through that rule first.
- **An administrator sees everything** — the role `users.is_admin`, never a name and never a username comparison.
- **A job with no owner is visible to everyone.** `jobs.user_id` is nullable and no submission door leaves it null, so a null owner means a row written straight into the database: the chaos test's jobs and the fixtures that predate authentication. Refusing those would break the centrepiece proof to protect data belonging to nobody.
- **A refusal never says whether the identifier exists.** Somebody else's job and a job that was never created get the same code and the same words — `404`, not `403`. `403` is right where the caller already knows the resource exists (an admin route refusing a non-admin); it is wrong where the existence is the secret, because a `403` beside a `404` is a lookup service for other people's identifiers. **This changes one shape published one day earlier:** `POST /jobs/{id}/cancel` answered `403` for another user's job on 2026-09-06 and answers `404` from this date.
- **The pool stays open.** `GET /nodes` and `GET /nodes/{id}/events` are unchanged and visible to every logged-in user: they carry machine data, not job data, and a user needs them to choose where to send work.
- **The agent routes are untouched.** They authenticate with a node token against a different column and are already scoped by the run they hold at the attempt they hold it — the fence, which is a stricter check than this one.

Additive in behaviour and unchanged for a single-user installation: the one account a deployment bootstraps is an administrator, so it sees exactly what it saw before. Tests `control-plane/tests/test_read_scoping.py` assert both directions, and eight of them fail on the code before the fix.

### `GET /nodes`
```json
[
  { "node_id":"uuid","name":"lab-pc-01","online":true,"reported_status":"idle",
    "cpu_cores":8,"has_gpu":true,"ram_mb":16384,"capacity":8,"last_heartbeat":"2026-06-02T17:30:00Z",
    "battery_pct":78.0,"battery_charging":false }
]
```
`battery_pct` + `battery_charging` are **W5b additions** (nullable — desktops/old agents omit them): the last black-box picture, so a silent node can show a "likely …" cause at read time.
`trusted` (bool) is a **W6b addition**: may this machine run private jobs.
`agent_outdated` (bool) + `min_agent_version` (string) are a **2026-09-07 addition** (walk 1, row 65): true when this machine's agent is below the version a sealed run needs (`scheduler.MIN_SEALED_AGENT_VERSION`, 0.12.0), which since 2026-09-06 means it is offered nothing new; the pool says so on the row instead of showing an ordinary healthy node.

### `GET /me` · `POST /me/accept-limits` · `GET /tiers`  *(2026-09-04)*
`GET /me` → `{username, is_admin, tier, retained_cap_mb, scratch_cap_mb, retained_used_mb, limits_accepted, limits_accepted_at}`. `retained_used_mb` is the SAME sum that refuses an upload, so the bar on the screen and the number that refuses cannot disagree. `POST /me/accept-limits` records agreement to the two figures **as they stand now**, and returns the same shape. `GET /tiers` lists what the deployment offers. There is deliberately no route that EDITS a tier: a cap anyone can raise is not a cap.
**Why `GET /me` exists at all:** a cap the user cannot see is a trap — the platform would refuse work for a reason nothing on the screen had ever mentioned.

### `POST /users` · `GET /users` · `PATCH /users/{user_id}/tier`  *(2026-09-04 — admin only)*
`POST /users {username, password, tier}` → `UserOut`. **A created user starts with acceptance NOT given**: an admin decides which numbers apply to someone and cannot agree to them on their behalf, which is the whole point of asking. `422` unknown tier · `409` username already exists. `PATCH /users/{id}/tier {tier}` moves one user; nothing resets acceptance because nothing needs to — acceptance compares the tier and both caps that were agreed against the ones that now apply. `403` for a valid non-admin login, never `401`.
The **bootstrap admin** is the one exception to "acceptance is given by the user": it is created from the deployment's own environment by the operator who configured the tiers, so it is recorded as accepting at creation. Every user created through `POST /users` starts un-accepted.

### `POST /jobs/{job_id}/cancel`  *(2026-09-07, walk 1 row 64)*
→ `{ job_id, cancelled_now, cancel_requested, already_finished, detail }`. JWT-gated: the job's owner or an admin. `404` for an unknown job **and, since 2026-09-07, for another user's job — where this answered `403` on the day it landed** (see *Read scoping* above: a `403` on a job that exists beside a `404` on one that does not is a way of discovering other people's identifiers). Stops a job **with no new state**: a `PENDING` run ends in this request as `FAILED` with `failure_reason=CANCELLED`; an `ASSIGNED`/`RUNNING` run is stamped (`runs.cancel_requested_at`, migration `d5e6f7a8b9c0`) and the worker holding it is told at its next heartbeat through the `commands` array (`[{"type": "cancel", "run_id": …}]` — the field the response has carried since W1, filled for the first time), stops the container and posts `FAILED` with reason `CANCELLED`; if that worker never confirms — it died, or its agent predates commands — the **reaper ends the stamped run as cancelled when its lease expires instead of requeueing it**, so recovery never undoes a cancel. A terminal run is untouched: a result, once accepted, is final (§7), and a `SUCCEEDED` posted after the stamp stands because the work finished before the stop landed. Fencing is unchanged. `GET /jobs/{id}/runs` gains `cancel_requested_at` (nullable). Idempotent. Old agents: they ignore `commands` and finish the run; the stamp then labels its ending `CANCELLED`, or the reaper does.

### `DELETE /jobs/{job_id}/storage`  *(2026-09-04)*
**The release valve.** Removes everything this job is holding in object storage — results, checkpoints, archived logs and any sealed input, including the leftovers of attempts that were fenced out and can no longer be read but still occupy space — and the rows that point at them. JWT-gated. `404` unknown job · `409` while any of the job's runs is still in flight (deleting the results of a job that is still executing would race the very upload producing them). → `{job_id, objects_deleted, objects_listed, freed_mb, detail}`.
**A quota without a release valve is a trap**, so this is part of the policy and not an extra. It is **not** crypto-shred and the two are deliberately separate: `DELETE /jobs/{id}/key` removes READABILITY and leaves the sealed object exactly where it is, which is what demonstrates that the guarantee does not depend on reaching the copies; this removes BYTES. Either may follow the other.
Objects go first and rows second — the opposite order from checkpoint cleanup, for a stated reason. There, the risk was a row promising bytes that were gone. Here the number that matters is the QUOTA, and the two half-failures are not equal: a row left after its object is deleted over-counts the user, which is visible and fixed by calling this again; a row deleted while its object survives under-counts them for ever, which is exactly the hole a cap exists to close.

### `PATCH /nodes/{node_id}/trusted`  *(W6b; **admin-only since 2026-09-04**)*
```json
{ "trusted": true }
```
→ the updated `NodeOut`. JWT-gated — the **admin's** judgment, made from outside the machine, and the only way the flag can ever be set. `404` unknown node. Withdrawing trust stops **future** private placements only; it does not touch a run already executing there (the data is already open in that container) — stated plainly rather than over-claimed.

### `GET /nodes/{node_id}/events`  *(W5b)*
The node postmortem history, newest first: `[{ ts, event, cause, evidence }]` — goodbyes + classified comeback causes.

### `GET /runs/{run_id}/samples`  *(W5b)*
The run's own resource history, oldest first: `[{ ts, cpu_pct, mem_used_mb, mem_limit_mb }]` — the "last picture" before a failure (e.g. RAM climbing to an OOM limit).

### `POST /jobs`
```json
{
  "name": "sweep-lr-001",
  "image": "repo/dummy:latest",
  "entrypoint": ["python","train.py"],
  "env": { "LR": "0.01" },
  "resource_reqs": { "min_ram_mb": 4096, "needs_gpu": false },
  "target_node_ids": ["uuid1","uuid2"],
  "replicas": 1
}
```
→ `{ "job_id":"uuid", "run_ids":["uuid", "..."], "status":"PENDING" }` (runs fan out per §4).
**2026-09-04 additive, on BOTH submission doors:** `resource_reqs` gains an optional `scratch_mb` — at most the submitter's tier scratch cap on **both** routes since 2026-09-06 — the multipart door validated against `PRIVATE_TMPFS_MB` until then, and `private` is a dead parameter now, so the tier cap is the one ceiling on every door (§8) — and over that ceiling is a `422` rather than a silent trim (a job quietly given less disk than it asked for fails later for a reason nobody can see). Two new refusals: `403 {"reason": "LIMITS_NOT_ACCEPTED"}` when the caller has not accepted their tier's figures, and `403 {"reason": "STORAGE_QUOTA_EXCEEDED", "used_mb", "cap_mb"}` when they are already at their retained cap. **Refusing at submission as well as at upload is not belt-and-braces:** submission catches the user who is already full, before a worker spends hours on a job whose result could not be stored, and upload catches the run that fills them, because bytes are produced DURING a run and are not knowable before it.
**W6b:** the body gains an optional `private` (bool). Sending `private: true` **here** is a `422` — a private job is defined by having a sealed input file, and JSON cannot carry one, so accepting the flag on this route could only produce a job that claims privacy while nothing is sealed. Use the multipart route below. When privacy is asked for, the only honest answers are "sealed" or "refused".

**`trusted_only` (bool, default false, 2026-09-06)** is accepted on every submit door: run only on machines an admin marked `trusted`. It is the one protection the private route used to bundle that is genuinely a choice — sealing costs the submitter nothing, so it has no field and is on for everyone; restricting a job to trusted machines costs them the rest of the pool, so it is theirs to ask for. It changes placement and nothing else.

### `POST /jobs/private`  (multipart)  *(W6b)*
Form fields: `spec` (the same `POST /jobs` JSON body, as a string) + `file` (the input). JWT-gated. **Since 2026-09-06 this is exactly `POST /jobs/with-input` with `trusted_only` forced on**, and both doors run one implementation so they cannot drift apart. The control plane, in order: mints a fresh 256-bit key **per job**; seals the file in the **framed** format; writes **only** the sealed blob to object storage at `inputs/{job_id}/input.bin`; stores the key in `job_keys`; creates the job and fans it out. **It no longer sets `jobs.private`** — that column names the older container shape, and saying "private" about a job built the new way would make one word mean two different shapes. What this door does is said by the two columns that say it: `sealed` and `trusted_only`. What it stops charging: its runs keep the ordinary three writable folders, so they collect results and resume from a checkpoint after a machine dies, where a private job could do neither. → the same `{job_id, run_ids, status}` shape. `422` bad spec / empty file · `413` over `MAX_INPUT_MB` (default 100) · **`413 {"reason": "STORAGE_QUOTA_EXCEEDED", …}` (2026-09-04) when the sealed input would take the submitter over their retained cap** — checked after the per-file cap and **before** the key is minted and the file sealed, so a refusal leaves no `job_keys` row and no object rather than a half-created private job. From this moment the plaintext exists nowhere we control.

### `DELETE /jobs/{job_id}/key`  *(W6b should-tier)*
**Crypto-shred.** *(2026-09-06: every job has a key, so this works on every job — and it reaches the job's RESULTS and CHECKPOINTS too, because they are sealed with the same key. `404`, not `422`, for a job that never had one: a row from before that date submitted without the private door.)* Deletes the `job_keys` row → every sealed copy of that job's data becomes permanently unreadable (the object in storage, anything staged on a worker, any backup) — not because the copies were chased down, but because without the key they are noise and nothing can rebuild it. JWT-gated, irreversible, idempotent (`{shredded: false}` when already gone). `404` for an unknown job, somebody else's job, or a job that never had a key — **not `422`**, which this route stopped returning on 2026-09-06 and which the note above already says. The sealed object is deliberately left in place, to show the guarantee does not depend on reaching it.

### `GET /jobs` · `GET /jobs/{id}` · `GET /jobs/{id}/runs`
Return job(s) and their runs with current `status`, `node_id`, `attempt`, `exit_code`. *(2026-09-07: **the caller's own jobs**, or every job for an administrator — see *Read scoping* above. The response SHAPE is unchanged; which rows it holds is not.)* *(W5b: also `failure_reason`, `failure_detail`, `progress`, `metrics_last`. W5c: also `learned_min_ram_mb`, `escalation_count` — the learned RAM requirement + how many times the run was escalated. W6b: jobs also carry `private` + `input_filename`; **no read shape anywhere returns a key or the input object key**. 2026-09-07: jobs also carry `input_size_bytes`, the SEALED size of the input they hold — the number charged against the owner's retained storage — null when there is no file.)*

### `WS /runs/{id}/logs`
Live log subscription. Server streams `{run_id, attempt, seq, chunk, ts}` ordered by `seq`.
**Auth (W6):** a browser cannot set an `Authorization` header on a socket, so the JWT rides the query string — `WS /runs/{id}/logs?token=<jwt>` — validated **before** accept (invalid → HTTP 403 handshake). *(Query strings can appear in server logs — acceptable at demo scale.)* **2026-09-07:** the socket now resolves the USER from that token and refuses a run whose job is not theirs, in the handshake, with the same 403 — a valid login belonging to somebody else was exactly what a token check could not see. An unknown `run_id` is refused there too, where it used to be an accepted socket carrying an error message, which said out loud that the other identifiers did exist.
**Fallback if WS is fiddly:** `GET /runs/{id}/logs?since_seq=N` — same data model, simpler transport.
**Log tiering (additive, 2026-08-22) does NOT change either shape.** A terminal run's chunks may have moved out of `run_logs` into the object store (see `run_log_archives` in §2). Both reads still return `{run_id, attempt, seq, chunk, ts}` filtered by `seq > since_seq` and ordered by `(attempt, seq)` — the same filter and the same ordering applied to both sources, rendered by the same one function, so a caller cannot tell where a chunk came from. Archival only ever touches a run that is already terminal, so it can never race the accepted-result path, and the fencing check reads `runs.attempt` rather than the log rows, so a stale-attempt log post is still `409` on an archived run.

### `GET /runs/{id}/artifacts`
→ list of `{ artifact_id, run_id, attempt, filename, object_key, size, content_type, kind }` — the run's **current attempt only**, and **results only** (a stale attempt's leftovers are never shown as the accepted result, and a checkpoint is working state rather than something the run produced).

### `GET /artifacts/{artifact_id}/download`  *(W6)*
Streams the artifact's bytes back **through the control plane** (brokered — the browser never talks to MinIO), with a `Content-Disposition` attachment header. JWT-gated, and **owner-scoped since 2026-09-07**: an artefact belonging to somebody else's job answers the same `404` as an artefact id that was never minted, checked before any byte is fetched from storage. This door mattered most of the three, because since 2026-09-06 it also UNSEALS on the way past — the one place the seal is opened on the owner's behalf was the one place that did not check who the owner was. `404` unknown artifact · `409` not this run's accepted result — a stale attempt's file, or a checkpoint. **2026-09-06: a sealed artefact is OPENED here, and this is the one place the seal is opened on the owner's behalf.** It has to be here rather than in the browser, because a browser that could open it would need the key and the key would then live in a page anyone can read. `410 Gone` when the job's key was crypto-shredded — not `404`, because the bytes are right there and are not missing, and not `500`, because nothing failed: the user asked for their data to become unreadable and it did, to us as much as to anyone. `409` when sealed bytes no longer open, which is the tamper check applied on the way out. Same query rule as the listing above, written once.

---

## 11. Error / fencing conventions

| Code | Meaning |
|---|---|
| `200` | accepted |
| `401` | bad/missing token (node or JWT) |
| `404` | unknown `node_id` / `run_id` / `job_id` — **and, since 2026-09-07, a job that exists but is not the caller's**, deliberately indistinguishable from one that does not exist (see *Read scoping* in §10) |
| `409` | **fencing rejection** — stale `attempt`, *or* a status post for a run not owned by the calling node; agent must abort the run |
| `410` | *(W6b)* **gone for good** — a key ticket that was already redeemed, has expired, or whose job's key was crypto-shredded. Distinct from `409` on purpose: `409` means "you are the wrong caller", `410` means "this pass is spent". Neither is retryable |
| `403` | *(2026-09-04)* **policy refusal** — the caller has not accepted their storage limits, is already at their retained cap, or is a non-admin calling an admin route. Distinct from `401` on purpose: `401` means "I do not know who you are", `403` means "I know exactly who you are, and this is not yours to do" |
| `413` | payload over a configured cap (an artifact, or a private input file), **or (2026-09-04) storing it would cross the owner's retained-storage cap** — the body's `reason` says which |
| `422` | malformed body (FastAPI validation), `private: true` on a JSON or `with-input` submit, or **(2026-09-06) an artefact that is not sealed on a job that is** — the body's `reason` says which |

`409` is the load-bearing one: it is how the fencing token protects the no-duplicate guarantee. Any agent receiving `409` for a run stops working on it immediately. **W6b points the same check at the input side** — a ticket is issued only to the node holding the run's current attempt, and the attempt is checked again at redeem — so the mechanism that stops a zombie writing a stale result also stops it reading fresh data.

---

## 12. Change control

This file is frozen on the date at the top. To change anything here: record a dated decision stating what changed and why, tell your partners, then edit. The contract moving without both of those is the single fastest way to break the parallel split.
