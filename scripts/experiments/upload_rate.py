"""How fast does the control plane accept an artifact upload from one agent?

WHY THIS EXISTS
---------------
M. Ayli asked, on 2026-08-28, what happens to checkpoint storage at scale. Half of
that question is answered by the design and needs no measurement: a run keeps ONE
checkpoint per attempt, written to a stable name and overwritten in place, so
checkpoints do not stack up (`control-plane/app/api/artifacts.py`, the
`IntegrityError` branch of `upload_artifact`). The other half is a real cost and had
no number behind it: **every checkpoint a worker saves travels through one control
plane**, because workers hold no storage credentials (ISO/IEC 27001 A.8.31, and the
reason downloads are brokered at all). One server is therefore a shared pipe, and a
shared pipe has a width.

This script measures that width, so the report can state the bound in the form an
engineer can act on:

    at R MB per second, a checkpoint of S MB takes S/R seconds, and the
    workload's checkpoint interval must exceed that or saves queue behind
    each other.

WHAT IS MEASURED, EXACTLY
-------------------------
One agent uploading one file of `--size-mb` megabytes through the real endpoint,
`POST /agent/runs/{id}/artifacts`, `--reps` times, with the duration of each call
recorded and reported as a median and a full spread. Nothing is averaged into a
single number without its spread.

**It is the real path, not a replica of it.** This script imports the agent's own
`_post_multipart` and its own `register` out of `agent/agent.py` and calls them. The
multipart framing, the TLS context, the header shapes and the timeout are the
agent's, because they are literally the agent's code. The only thing this file adds
is a clock around the call and a loop.

**End to end from the agent's side, and that is deliberate.** The measured interval
starts before the agent builds its multipart buffer and stops when the control plane's
response has been read. It therefore includes the agent's own in-memory copy of the
payload, the TLS record layer, the ASGI receive, FastAPI's read of the upload into
memory, the SHA-256 the control plane computes over the bytes it stored, the MinIO
write, and the Postgres insert. That whole chain is what a checkpoint save actually
costs a worker, and the checkpoint interval has to clear the whole chain — not the
server's share of it. Two component costs are probed separately and recorded in the
manifest so a reader can see roughly how the total divides; they are named as probes,
not as a decomposition.

**High-entropy payload.** The bytes are `os.urandom`, not zeros. A real checkpoint
is base64 of float tensors, which is close to incompressible, and a zero-filled
payload would flatter any layer that happens to compress.

**The bytes are proved to have landed.** After the last repetition the artifact row
is read out of Postgres and its `size` and `sha256` are compared against the payload
that was sent. A fast number produced by an upload that stored nothing is the failure
this check exists to catch, and a mismatch voids the campaign rather than publishing
it.

THE SIZE CAP — READ THIS BEFORE CHANGING `--size-mb`
----------------------------------------------------
The shipped cap is `MAX_ARTIFACT_MB = 50` (`control-plane/app/config.py` line 70,
set to "50" in `docker-compose.yml`), and `upload_artifact` answers 413 above it. So
a 64 MB upload is REFUSED by the platform as shipped.

This script raises the cap for the duration of the campaign, through the same
override mechanism the harness already uses for tunables (`set_mode(env=...)`, the
mechanism E2 used for `LEASE_TTL_S`), and the override is asserted inside the running
container before any repetition and recorded in `manifest.json`. Raising it is honest
here because the cap is a single length comparison on an in-memory buffer
(`if len(data) > cap`) and changes no part of the path being timed. It is recorded
rather than quietly done, and `--keep-cap` measures at the shipped 50 MB instead for
anyone who would rather not move it.

`LEASE_TTL_S` is raised for the campaign too, for the same reason E2 raised it: a
run held across twenty uploads would otherwise have its lease lapse, be requeued by
the reaper, and come back at a higher attempt — at which point every later upload is
a legitimate 409 and the campaign measures nothing. Both overrides travel in the
manifest.

WHICH TREE THIS DESCRIBES — AND WHY IT IS NOT `1df69b1`
-------------------------------------------------------
Every published scheduler number in this project came from tree `1df69b1` running the
dummy container, and keeps describing that tree. **This number cannot carry that
label and must never be given it.** The artifact `kind` column, the checkpoint route
and the whole checkpoint-and-resume feature landed on 2026-08-13, months after
`1df69b1` was frozen; the code being timed here did not exist on that tree. So this
measurement is labelled with the tree it actually ran on and the machine it ran on,
and it stands beside the frozen campaign rather than inside it.

--------------------------------------------------------------------------------
PREDICTION — written before this script had ever been executed, and committed in
the same commit that first adds the file, so the claim can be checked against the
clock rather than taken on trust.
--------------------------------------------------------------------------------

  * **Order of magnitude: tens of MB per second, most likely between 50 and 200.**
    Everything is on one host, so no physical network is crossed and the number is
    not a network number.

  * **The mechanism expected to dominate is repeated copying of the payload, plus
    the second local hop into MinIO** — not TLS and not the digest. The 64 MB is
    built into the agent's buffer, pushed through the TLS record layer, reassembled
    by the ASGI server, read whole into memory by FastAPI, hashed, and then sent
    over a second loopback HTTP call to MinIO before being written to disk through
    Docker Desktop's filesystem bridge. SHA-256 runs at roughly 1-2 GB/s on this
    class of machine, so the digest should be a small share (tens of milliseconds
    for 64 MB) and should NOT be the answer.

  * **What would falsify this reasoning, stated in advance:**
      - a median **above about 1 GB/s** (an upload finishing in under ~64 ms) would
        mean the bytes are not making the trip they are assumed to make — the first
        thing to check would be the stored-object verification at the end of the run,
        because the honest reading is then that something short-circuited;
      - a median **below about 5 MB/s** (an upload taking over ~12 s) would mean
        copying is not the dominant cost and something else is — most likely a
        per-object flush in MinIO or the Windows Docker filesystem bridge — and the
        mechanism named in the report changes to whichever it is.
    In both cases the measured number is published and the *mechanism* is corrected.
    The prediction is a claim about why, not a claim about what we will allow the
    result to be.

RUN IT (host, repo venv, machine otherwise idle, mains power)
------------------------------------------------------------
    .venv\\Scripts\\python.exe scripts\\experiments\\upload_rate.py --fresh

Strictly one measurement at a time. The harness
takes the stack lock, refuses a machine that is not fit to measure on, and refuses to
append onto measured rows.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import os
import statistics
import sys
import tempfile
import time
import urllib.error
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import harness as h  # noqa: E402

EXP = "UPLOAD_RATE"

# The shipped cap, read here so a drift between this file and the platform shows up
# as a mismatch in the results rather than as a silent 413 halfway through a run.
SHIPPED_CAP_MB = 50
# One definition, in harness, so a moved default cannot go stale in five places.
SHIPPED_LEASE_TTL_S = h.SHIPPED_LEASE_TTL_S

# The real trainer's checkpoint, for the worked example in the report sentence.
# Recorded in workloads/dummy/fyp_checkpoint.py (lines 50-55) and in the deck source:
# 206,922 parameters cost 2.38 MB of raw bytes and 3.17 MB once base64'd.
#
# 3.17 is the number the transfer bound must use. The checkpoint contract carries
# JSON, so what crosses the wire is the ENCODED form; 2.38 MB is what the tensors
# weigh in memory and is not what gets uploaded.
TRAINER_CKPT_RAW_MB = 2.38
TRAINER_CKPT_WIRE_MB = 3.17
TRAINER_PARAMS = 206_922

# A billion-parameter model, so the answer scales to the question that was actually
# asked.
#
# THE TRAP, NAMED BECAUSE THIS FILE FELL INTO IT (2026-09-01). The figure quoted
# everywhere as "about 16 bytes per parameter" is the BASE64 number, not the raw one.
# Measured on the trainer above: 2.38 MiB of tensors over 206,922 parameters is
# 12.06 bytes each, and 3.17 MiB once encoded is 16.06 -- and 12 is exactly what the
# format predicts, 4 bytes of float32 weight plus Adam's two float32 moments. The
# published ratio 3.17/2.38 = 1.3319 against base64's 1.3333 is the confirmation.
# So 16 = 12 * 4/3 ALREADY. Using 16 as the raw figure and multiplying by 4/3 again
# double-counts the encoding and overstates both columns by a third, which is what
# the two lines below used to do.
_RAW_BYTES_PER_PARAM = 12          # 4 weight + 4 exp_avg + 4 exp_avg_sq, all float32
BILLION_PARAM_RAW_MB = _RAW_BYTES_PER_PARAM * 1_000_000_000 / (1024 * 1024)  # ~11444 MB
BILLION_PARAM_WIRE_MB = BILLION_PARAM_RAW_MB * (4 / 3)             # base64 overhead
# Sanity, and it is a real check rather than decoration: the corrected WIRE figure
# equals the number this file previously printed as RAW, to the megabyte.

HTTP_TIMEOUT_S = 600.0  # generous: a slow upload must be measured, never truncated


def _agent_module(state_file: Path):
    """Import the real agent, with its state file pointed somewhere disposable.

    `agent.agent` reads `AGENT_STATE_FILE` at import time, so the environment has to
    be set first. Imported late, and inside a function, so that `--help` works on a
    machine without docker-py installed."""
    os.environ["AGENT_STATE_FILE"] = str(state_file)
    try:
        from agent import agent as agent_mod
    except ImportError as exc:  # noqa: BLE001 - docker-py missing is the usual cause
        raise h.HarnessError(
            f"cannot import the agent ({exc}). Run this with the repo venv: "
            r".venv\Scripts\python.exe scripts\experiments\upload_rate.py"
        ) from exc
    return agent_mod


def _claim_one_run(agent_mod, server: str, state: dict, timeout_s: int = 60) -> dict:
    """Heartbeat until the control plane hands this node a run, and return it.

    The claim happens at pull time, on the heartbeat, exactly as it does for a real
    worker — there is no back channel that assigns a run any other way. The run is
    left ASSIGNED and no container is ever started: the upload endpoint fences on
    ownership and attempt, not on the run being RUNNING, so an ASSIGNED run is a
    faithful and much cheaper carrier for the payload."""
    url = f"{server}/agent/heartbeat"
    body = {"node_id": state["node_id"], "status": "idle", "running": []}
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        resp = agent_mod._post(url, body, token=state["token"])
        assignments = resp.get("assignments") or []
        if assignments:
            return assignments[0]
        time.sleep(1)
    raise h.HarnessError(
        f"no run was assigned within {timeout_s}s. The job was submitted targeting "
        "this node, so check that the node registered and is online."
    )


def _probe_components(payload: bytes) -> dict:
    """Two component costs, measured on this machine, on these bytes.

    Named as probes rather than as a decomposition of the headline: they are the
    same work the chain does, measured in isolation, so a reader can see roughly how
    much of the total is the digest and how much is one in-memory copy. They do not
    sum to the measured time and are not claimed to."""
    t0 = time.perf_counter()
    hashlib.sha256(payload).hexdigest()
    sha_s = time.perf_counter() - t0

    t0 = time.perf_counter()
    buf = io.BytesIO()
    buf.write(payload)
    buf.getvalue()
    copy_s = time.perf_counter() - t0

    return {"sha256_s": round(sha_s, 4), "one_memory_copy_s": round(copy_s, 4)}


def _verify_stored(run_id: str, attempt: int, filename: str, payload: bytes) -> dict:
    """Prove the bytes really landed, out of the control plane's own row.

    A high rate produced by an upload that stored nothing is the one way this
    measurement could be fast and worthless, so the check is not optional and a
    mismatch voids the campaign."""
    expect_sha = hashlib.sha256(payload).hexdigest()
    key = f"runs/{run_id}/{attempt}/{filename}"
    rows = h.psql_json(
        "SELECT object_key, size, sha256, kind, attempt FROM artifacts "
        f"WHERE run_id = '{run_id}' AND object_key = '{key}'"
    )
    if not rows:
        raise h.HarnessError(
            f"VERIFICATION FAILED: no artifact row for {key}. The uploads reported "
            "success but nothing was stored — the timings are void."
        )
    row = rows[0]
    if int(row["size"]) != len(payload):
        raise h.HarnessError(
            f"VERIFICATION FAILED: stored size {row['size']} != sent {len(payload)}."
        )
    if row["sha256"] != expect_sha:
        raise h.HarnessError(
            "VERIFICATION FAILED: the stored digest does not match the bytes sent. "
            f"stored={row['sha256']} sent={expect_sha}"
        )
    return row


def _worked_examples(rate_mb_s: float) -> list[str]:
    """The report's sentence, worked at both scales, from the measured rate."""
    lines = []
    for label, mb in (
        (f"the real trainer ({TRAINER_PARAMS:,} parameters), base64'd for the wire",
         TRAINER_CKPT_WIRE_MB),
        ("the same checkpoint's raw tensors, for comparison only",
         TRAINER_CKPT_RAW_MB),
        ("a 1-billion-parameter model at ~12 bytes per parameter, raw -- and since"
         " the 2026-09-01 reshape this is also what the contract sends",
         BILLION_PARAM_RAW_MB),
        ("the same model packed as text, which is what the contract sent BEFORE the"
         " 2026-09-01 reshape -- kept for comparison, no longer what is sent",
         BILLION_PARAM_WIRE_MB),
    ):
        secs = mb / rate_mb_s
        lines.append(f"  - {label}: {mb:,.2f} MB / {rate_mb_s:.1f} MB/s = {secs:,.2f} s")
    return lines


def _write_results_file(
    path: Path, args, rows: list[dict], stored: dict, probes: dict,
    payload_mb: float, cap_used_mb: int, sha: str, tree_note: str,
    transport: str, trust: str,
) -> None:
    durations = [r["upload_s"] for r in rows]
    rates = [r["upload_mb_per_s"] for r in rows]
    med_rate = statistics.median(rates)
    text = [
        "Server upload rate - how fast one agent's artifact upload is accepted",
        "=" * 72,
        "",
        f"Written {datetime.now(timezone.utc).isoformat(timespec='seconds')}.",
        "",
        "WHAT THIS IS",
        "-" * 72,
        "One agent uploading one file through the real endpoint",
        "POST /agent/runs/{id}/artifacts, using the agent's own upload code",
        "(agent/agent.py, _post_multipart), timed end to end from the agent's side.",
        "It answers the transfer half of the supervisor's 2026-08-28 question about",
        "checkpoint storage at scale: every checkpoint a worker saves crosses one",
        "control plane, so that server is a shared pipe and this is its width.",
        "",
        "THE TREE, STATED HONESTLY",
        "-" * 72,
        tree_note,
        "",
        "This number is NOT labelled 1df69b1 and must never be. Every published",
        "scheduler figure in this project came from that frozen tree running the",
        "dummy container. The artifact kind column, the checkpoint route and the",
        "whole checkpoint-and-resume feature landed on 2026-08-13, long after that",
        "freeze - the code timed here did not exist on 1df69b1, so it cannot carry",
        "its label. This measurement stands beside the frozen campaign, not inside",
        "it.",
        "",
        "CONDITIONS",
        "-" * 72,
        f"  payload             : {payload_mb:.0f} MB of os.urandom bytes (incompressible,",
        "                        like base64'd float tensors; zeros would flatter any",
        "                        layer that compresses)",
        f"  repetitions         : {len(rows)}",
        f"  MAX_ARTIFACT_MB     : {cap_used_mb} for the campaign "
        f"(shipped default {SHIPPED_CAP_MB})",
        f"  LEASE_TTL_S         : {args.lease_ttl_s} for the campaign "
        f"(shipped default {SHIPPED_LEASE_TTL_S})",
        "  storage             : MinIO, local, same host",
        # Observed, not asserted. These two lines used to be hard-coded and
        # claimed the project CA whatever the run actually did.
        f"  transport           : {transport}",
        f"  trust               : {trust}",
        "",
        "RESULT",
        "-" * 72,
        f"  rate   median {med_rate:.1f} MB/s   "
        f"spread {min(rates):.1f} to {max(rates):.1f} MB/s   n={len(rates)}",
        f"  time   median {statistics.median(durations):.3f} s  "
        f"spread {min(durations):.3f} to {max(durations):.3f} s  n={len(durations)}",
        "",
        "  Mechanism: the measured interval covers the agent's own multipart buffer",
        "  build, the TLS record layer, the ASGI receive, FastAPI reading the upload",
        "  whole into memory, the SHA-256 the control plane computes over the bytes",
        "  it stored, the MinIO write, and the Postgres insert. That whole chain is",
        "  what a checkpoint save costs a worker, which is the thing a checkpoint",
        "  interval has to clear.",
        "",
        "  Component probes on the same bytes, same machine (not a decomposition):",
        f"    SHA-256 over the payload : {probes['sha256_s']:.4f} s",
        f"    one in-memory copy       : {probes['one_memory_copy_s']:.4f} s",
        "",
        "THE BYTES REALLY LANDED",
        "-" * 72,
        "  Checked against the control plane's own artifact row, not against the",
        "  HTTP status. A fast number produced by an upload that stored nothing is",
        "  the one way this could be quick and worthless.",
        f"    object_key : {stored['object_key']}",
        f"    size       : {stored['size']} bytes (sent {int(payload_mb * 1024 * 1024)})",
        f"    sha256     : {stored['sha256']}",
        f"    sent sha256: {sha}",
        f"    match      : {'YES' if stored['sha256'] == sha else 'NO'}",
        "",
        "WHAT IT MEANS FOR CHECKPOINTS",
        "-" * 72,
        f"  At {med_rate:.1f} MB per second, a checkpoint of S MB takes S/{med_rate:.1f}",
        "  seconds to reach the server, and the workload's checkpoint interval must",
        "  exceed that or saves queue behind each other:",
        "",
        *_worked_examples(med_rate),
        "",
        "  The last line is the supervisor's question answered with a number. The",
        "  platform's own trainer is comfortable; a billion-parameter model is not,",
        "  and the honest reading is that this contract is shaped for the workloads",
        "  we demonstrate rather than for large models. Two costs are separate and",
        "  are not blurred: this is the TRANSFER bound. How much is KEPT is bounded",
        "  separately by one checkpoint per attempt, overwritten in place.",
        "",
        "LIMITS OF THIS NUMBER, STATED RATHER THAN LEFT TO BE FOUND",
        "-" * 72,
        "  - One machine, one agent, MinIO on the same host. It bounds the pipe on",
        "    the demonstration hardware; it is not a claim about a deployment where",
        "    the store is on another machine, which would be slower.",
        "  - One agent, not many. Several workers saving at once share this pipe,",
        "    and that contention is not measured here.",
        f"  - Measured at {payload_mb:.0f} MB. A much smaller file pays a larger share",
        "    of fixed per-request cost, so the rate at small sizes will be lower.",
        "  - The cap and the lease were raised for the campaign, both recorded above",
        "    and in manifest.json. Neither changes the path being timed: the cap is a",
        "    single length comparison, and the lease only stops the reaper requeuing",
        "    the carrier run mid-campaign.",
        "",
    ]
    path.write_text("\n".join(text) + "\n", encoding="utf-8")
    h.log(f"wrote {path.relative_to(h.REPO)}")


def main() -> int:
    # Declared here, before add_argument reads EXP for its default:
    # a `global` after the first use of the name is a SyntaxError.
    global EXP
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--size-mb", type=int, default=64,
                    help="payload size in MB (default 64)")
    ap.add_argument("--reps", type=int, default=20,
                    help="repetitions (default 20)")
    ap.add_argument("--cap-mb", type=int, default=None,
                    help="MAX_ARTIFACT_MB for the campaign (default: payload + 64)")
    ap.add_argument("--keep-cap", action="store_true",
                    help="do NOT raise the cap; measure at the shipped %d MB instead"
                         % SHIPPED_CAP_MB)
    ap.add_argument("--lease-ttl-s", type=int, default=600,
                    help="LEASE_TTL_S for the campaign (default 600), so the reaper "
                         "cannot requeue the carrier run mid-campaign")
    ap.add_argument("--fresh", action="store_true",
                    help="archive existing measured rows and start clean")
    ap.add_argument("--dry", action="store_true",
                    help="rehearsal: rows are quarantined and never summarised")
    ap.add_argument("--exp-label", default=EXP,
                    help="write into this experiment directory instead of "
                         f"{EXP}. A rehearsal MUST use it: guard_fresh "
                         "refuses to append onto published measured rows, so "
                         "without a label a --dry run either cannot start or "
                         "would archive real evidence to prove a script runs "
                         "(the reason --exp-label exists at all, 2026-07-31d)")
    args = ap.parse_args()
    # Rebound before anything reads it: every h.record/summarize/chart call
    # below routes through this module global.
    EXP = args.exp_label

    h.require_ready()
    if not args.dry:
        h.require_clean_machine()

    size_mb = args.size_mb
    if args.keep_cap:
        cap_mb = SHIPPED_CAP_MB
        if size_mb > SHIPPED_CAP_MB:
            size_mb = SHIPPED_CAP_MB
            h.log(f"--keep-cap: measuring at {size_mb} MB, the shipped cap, "
                  f"instead of the requested {args.size_mb} MB")
    else:
        cap_mb = args.cap_mb if args.cap_mb is not None else size_mb + 64
        if size_mb > SHIPPED_CAP_MB:
            h.log(f"the shipped cap is {SHIPPED_CAP_MB} MB and the payload is "
                  f"{size_mb} MB, so MAX_ARTIFACT_MB is raised to {cap_mb} for this "
                  "campaign; it is asserted in the container and recorded in the "
                  "manifest")

    h.guard_fresh(EXP, fresh=args.fresh)

    payload = os.urandom(size_mb * 1024 * 1024)
    payload_sha = hashlib.sha256(payload).hexdigest()
    payload_mb = len(payload) / (1024 * 1024)
    probes = _probe_components(payload)
    h.log(f"payload {payload_mb:.0f} MB, sha256 {payload_sha[:16]}…; "
          f"probes: sha {probes['sha256_s']:.3f}s, copy "
          f"{probes['one_memory_copy_s']:.3f}s")

    state_file = Path(tempfile.gettempdir()) / "fyp-exp-uploadrate.json"
    state_file.unlink(missing_ok=True)
    agent_mod = _agent_module(state_file)

    filename = "checkpoint_probe.bin"
    rows: list[dict] = []
    stored: dict = {}
    try:
        # Both overrides asserted inside the running container by set_mode before a
        # single byte is uploaded, and both recorded in the manifest.
        h.set_mode(env={
            "MAX_ARTIFACT_MB": str(cap_mb),
            "LEASE_TTL_S": str(args.lease_ttl_s),
        })
        h.reset_platform()

        # TLS exactly as the real agent configures it: the project's own
        # authority, verified, no insecure fallback (agent/tls.py).
        #
        # CORRECTED 2026-08-30. This read AGENT_CA_CERT and nothing else, so on
        # a shell where that variable is unset -- the ordinary case -- it
        # configured no context at all and then printed "not configured (plain
        # HTTP)". That message was FALSE about the transport, and the campaign
        # of 2026-08-29 was published under it. h.API is https, the control
        # plane serves HTTPS only and refuses plain HTTP outright, and the
        # campaign completed: the bytes went over TLS. What actually happened is
        # that stdlib urllib on Windows falls back to the OS root store, where
        # the project CA was installed by `certutil -addstore -user Root` in the
        # re-trust step, so the connection verified against the machine's store
        # instead of against certs/ca.pem.
        #
        # It now defaults to the harness's CA path, so the trust root is this
        # repository's rather than whatever the operator's machine happens to
        # hold, and the line reports the SCHEME it is about to use rather than
        # inferring the transport from a variable that does not determine it.
        ca_pem = agent_mod.read_ca_pem(
            os.environ.get(agent_mod.CA_CERT_ENV) or h.VERIFY
        )
        agent_mod.configure_tls(ca_pem)
        scheme = h.API.split("://", 1)[0]
        transport_note = (
            f"{scheme.upper()} to {h.API} (the control plane serves HTTPS "
            "only and refuses plain HTTP)" if scheme == "https"
            else f"{scheme.upper()} to {h.API}"
        )
        trust_note = (
            f"the project CA at {h.VERIFY}, loaded by agent/tls.py"
            if ca_pem else
            "this machine's system certificate store - NO CA was configured, "
            "so stdlib's default context was used"
        )
        h.log(f"transport: {scheme}; trust: " + (
            f"the project CA at {h.VERIFY}" if ca_pem
            else "this machine's system store (no CA configured)"))

        node_state = agent_mod.register(
            h.API, "upload-rate-probe", agent_mod.detect_specs()
        )
        h.wait_online(1)
        job_id = h.submit_job(
            name="upload-rate-carrier",
            entrypoint=["python", "train.py"],
            target_node_ids=[node_state["node_id"]],
        )
        assignment = _claim_one_run(agent_mod, h.API, node_state)
        run_id, attempt = assignment["run_id"], assignment["attempt"]
        h.log(f"carrier run {run_id} at attempt {attempt} (job {job_id}); "
              "no container is started — the upload fences on ownership and "
              "attempt, not on the run being RUNNING")

        url = f"{h.API}/agent/runs/{run_id}/artifacts"
        fields = {"attempt": attempt, "filename": filename, "kind": "checkpoint"}
        for rep in range(1, args.reps + 1):
            t0 = time.perf_counter()
            agent_mod._post_multipart(
                url, fields, "file", filename, payload,
                "application/octet-stream", token=node_state["token"],
                timeout=HTTP_TIMEOUT_S,
            )
            elapsed = time.perf_counter() - t0
            h.assert_stable(where=f"rep {rep}")
            row = h.record(EXP, arm=f"{int(payload_mb)}MB", rep=rep, dry=args.dry,
                           metrics={
                               "upload_s": round(elapsed, 4),
                               "upload_mb_per_s": round(payload_mb / elapsed, 3),
                               "payload_mb": round(payload_mb, 3),
                           })
            rows.append(row)
            h.log(f"rep {rep}/{args.reps}: {elapsed:.3f} s "
                  f"= {payload_mb / elapsed:.1f} MB/s")

        stored = _verify_stored(run_id, attempt, filename, payload)
        h.log(f"verified: {stored['size']} bytes stored, digest matches")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", "replace")[:300]
        if exc.code == 413:
            raise h.HarnessError(
                f"HTTP 413: the control plane refused a {payload_mb:.0f} MB upload. "
                f"MAX_ARTIFACT_MB is {cap_mb} for this campaign — the override did "
                f"not take. Detail: {detail}"
            ) from exc
        raise h.HarnessError(f"upload failed HTTP {exc.code}: {detail}") from exc
    finally:
        h.set_mode()  # back to the shipped defaults, always
        state_file.unlink(missing_ok=True)

    if args.dry:
        print("\n[upload_rate] DRY pass complete — rows quarantined, nothing "
              "summarised. Re-run without --dry to measure.")
        return 0

    h.summarize(EXP, title="Server upload rate — one agent, one artifact upload")
    h.chart(EXP, metric="upload_mb_per_s",
            title="Upload rate (median, min–max over repetitions)")

    sha = h._git("rev-parse", "HEAD")
    tree_note = (
        f"Measured on tree {sha[:7]} ({h._git('rev-parse', '--abbrev-ref', 'HEAD')}), "
        f"on {h.machine_state().get('total_mb')} MB host "
        f"{os.environ.get('COMPUTERNAME') or 'the demonstration laptop'}."
    )
    _write_results_file(
        h.EVIDENCE / "upload_rate_results.txt", args, rows, stored, probes,
        payload_mb, cap_mb, payload_sha, tree_note,
        transport=transport_note, trust=trust_note,
    )

    rates = [r["upload_mb_per_s"] for r in rows]
    h.manifest(
        EXP,
        reps=args.reps,
        machine_idle=True,
        mains_power=True,
        notes=(
            "How fast the control plane accepts one artifact upload from one agent, "
            "measured through the real endpoint with the agent's own upload code. "
            "It answers the transfer half of the 2026-08-28 checkpoint-at-scale "
            "question: every checkpoint crosses one server, so the server is a "
            "shared pipe and this is its width. NOT part of the frozen 1df69b1 "
            "campaign and must never be labelled with it — the checkpoint route did "
            "not exist on that tree. The size cap and the lease TTL were raised for "
            "the campaign, both asserted in the container and recorded in "
            "compose_overrides; neither changes the path being timed."
        ),
        extra={
            "payload_mb": round(payload_mb, 3),
            "payload_bytes": len(payload),
            "payload_source": "os.urandom (incompressible, like base64'd tensors)",
            "payload_sha256": payload_sha,
            "stored_artifact": stored,
            "component_probes_s": probes,
            "max_artifact_mb_used": cap_mb,
            "max_artifact_mb_shipped_default": SHIPPED_CAP_MB,
            "lease_ttl_s_override": args.lease_ttl_s,
            "lease_ttl_s_shipped_default": SHIPPED_LEASE_TTL_S,
            "endpoint": "POST /agent/runs/{run_id}/artifacts",
            "upload_code_under_test": "agent/agent.py::_post_multipart (imported, "
                                      "not reimplemented)",
            "measured_interval": (
                "agent-side end to end: multipart buffer build, TLS, ASGI receive, "
                "FastAPI read, control-plane SHA-256, MinIO write, Postgres insert"
            ),
            "frozen_tree_label_refused": (
                "1df69b1 — the checkpoint route and artifacts.kind did not exist on "
                "that tree, so this number cannot describe it"
            ),
            "rate_mb_per_s": {
                "median": round(statistics.median(rates), 3),
                "min": round(min(rates), 3),
                "max": round(max(rates), 3),
                "n": len(rates),
            },
            "prediction_written_before_first_run": {
                "order_of_magnitude": "tens of MB/s, most likely 50-200",
                "dominant_mechanism_expected": (
                    "repeated copying of the payload plus the second local hop into "
                    "MinIO — not TLS and not the digest"
                ),
                "digest_expected_share": "small; SHA-256 at ~1-2 GB/s is tens of ms "
                                         "for 64 MB",
                "falsified_if_above": "~1 GB/s — the bytes are not making the trip; "
                                      "check the stored-object verification",
                "falsified_if_below": "~5 MB/s — copying is not dominant; name the "
                                      "real cost (MinIO flush, or the Windows Docker "
                                      "filesystem bridge)",
            },
            "worked_examples": {
                "trainer_params": TRAINER_PARAMS,
                "trainer_ckpt_raw_mb": TRAINER_CKPT_RAW_MB,
                "trainer_ckpt_wire_mb": TRAINER_CKPT_WIRE_MB,
                "billion_param_raw_mb": round(BILLION_PARAM_RAW_MB, 1),
                "billion_param_wire_mb": round(BILLION_PARAM_WIRE_MB, 1),
                "note": "3.17 MB, not 2.38 MB, is the transfer figure: the checkpoint "
                        "contract carries JSON, so tensors travel base64'd. 2.38 MB "
                        "is what they weigh in memory and is not what is uploaded.",
            },
        },
    )
    med = statistics.median(rates)
    print(f"\n[upload_rate] done — median {med:.1f} MB/s "
          f"(spread {min(rates):.1f}-{max(rates):.1f}, n={len(rates)})")
    print(f"[upload_rate] {h.exp_dir(EXP)}: raw.jsonl, summary.md, chart.png, "
          "manifest.json")
    print(f"[upload_rate] {h.EVIDENCE / 'upload_rate_results.txt'}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (h.HarnessError, AssertionError) as exc:
        print(f"\nupload_rate FAILED: {exc}", file=sys.stderr)
        sys.exit(1)
