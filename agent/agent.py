"""Pull-based worker agent.

W1: register + heartbeat (read empty work). W2: actually EXECUTE runs. The loop is
still single-threaded and pull-only — the control plane never connects to us. Each
tick:

  1. poll our containers; post a terminal status for any that exited;
  2. heartbeat, carrying the runs we're still executing (so our leases renew);
  3. start a container for each new assignment and confirm it RUNNING.

Container management lives in `runner.py` (Docker SDK). HTTP stays stdlib-only so
the agent has one dependency (docker-py) and is trivial to run on a worker.

The loop survives transient network errors: catch, log, keep going. A 409 from a
status post means the control plane has fenced that run (a re-dispatch happened) —
we abort it at once. A 401 from heartbeat means our token is stale — re-register.
"""

import argparse
import atexit
import hashlib
import http.client
import io
import json
import logging
import os
import secrets
import shutil
import signal
import socket
import ssl
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request

from docker.errors import DockerException

from .blackbox import BlackBox
from .classify import (
    ARTIFACT_TOO_LARGE,
    CANCELLED,
    SCRATCH_QUOTA_EXCEEDED,
    STORAGE_QUOTA_EXCEEDED,
    UNSEALED_OUTPUT,
    classify_failure,
)
from .runner import CHECKPOINT_DIR_NAME as _CHECKPOINT_DIR_NAME
from .runner import CHECKPOINT_FILENAME as _CHECKPOINT_FILENAME
from .runner import CHECKPOINT_DIR_MODE as _CHECKPOINT_DIR_MODE
from .runner import CHECKPOINT_FILE_MODE as _CHECKPOINT_FILE_MODE
from .runner import Runner, ensure_run_dir, prepare_run_dir, run_dir_path, run_root  # noqa: F401
from .sysinfo import collect_battery, collect_hw_specs, collect_usage
from .tls import CA_CERT_ENV, CaCertError, context_for, read_ca_pem

log = logging.getLogger("agent")

# 0.8.0 is the version the control plane's claim query requires before it will hand
# this agent a PRIVATE run (scheduler.MIN_PRIVATE_AGENT_VERSION) — an older agent
# cannot stage one, so it is never offered one.
# 0.10.0 is the version the control plane's claim query requires before it will hand
# this agent a run whose submitter asked for a temporary-disk size EXPLICITLY
# (scheduler.MIN_SCRATCH_AGENT_VERSION) — an older agent cannot stop a run at a disk
# cap, so it is never sent one that requires it.
AGENT_VERSION = "0.12.4"  # 2026-09-07 QA follow-up: worker-wide log budget, safe quota drain
# Where this worker keeps its identity (node_id + token) between restarts.
#
# 2026-09-07 (walk 1, rows 8, 17 and 51): ONE file per worker NAME, not one per
# directory. Two workers started from the same directory used to share
# `agent_state.json`: the second overwrote the first's identity, read the first's
# session file and reported itself as an agent that had crashed and restarted, and a
# worker restarted the README way found somebody else's identity in the file and
# registered as a brand-new node — so the pool showed it twice and a run aimed at the
# old row waited for ever. The name is in the command line already, so it decides
# the file: `agent_state-<name>.json`. `AGENT_STATE_FILE` still wins when set, which
# is how scripts/stage_demo.ps1 has always placed them.
LEGACY_STATE_FILE = "agent_state.json"
STATE_FILE = os.environ.get("AGENT_STATE_FILE", LEGACY_STATE_FILE)


def state_file_for(name: str) -> str:
    """The state file this worker name uses: the explicit `AGENT_STATE_FILE` if one
    is set, else `agent_state-<name>.json` beside wherever the agent was started."""
    explicit = os.environ.get("AGENT_STATE_FILE")
    if explicit:
        return explicit
    safe = "".join(c if (c.isalnum() or c in "-_.") else "_" for c in name) or "node"
    return f"agent_state-{safe}.json"
# Per-file artifact cap (MB): skip anything larger before uploading. The control
# plane enforces its own cap (413); this just avoids sending a doomed request.
MAX_ARTIFACT_MB = int(os.environ.get("AGENT_MAX_ARTIFACT_MB", "50"))
# How often a running container's checkpoint is swept up to the control plane. Not
# every heartbeat: a checkpoint can be large, and the value of saving it again three
# seconds later is small. An unchanged file is skipped entirely (same size + mtime),
# so a workload that saves rarely costs nothing between saves.
CHECKPOINT_UPLOAD_INTERVAL_S = float(os.environ.get("AGENT_CHECKPOINT_INTERVAL_S", "30"))
# The one name a checkpoint is ever stored under — on the worker's disk, inside the
# container, and as the object key's last segment. Imported from `runner`, which owns
# the mount, so there is exactly ONE definition: staging and reading must agree, and
# when they were two constants they did not (the container looked for a file the
# agent had written under a different name, and every resume silently started over).
# Stable on purpose too: the object key is built from it, so every save of a given
# attempt overwrites the SAME object and upserts the SAME row — one checkpoint per
# attempt, bounded storage, no cleanup job.
CHECKPOINT_FILENAME = _CHECKPOINT_FILENAME
CHECKPOINT_DIR_NAME = _CHECKPOINT_DIR_NAME
# Modes, from the same one place, for the same reason (see `runner`).
CHECKPOINT_DIR_MODE = _CHECKPOINT_DIR_MODE
CHECKPOINT_FILE_MODE = _CHECKPOINT_FILE_MODE
# run_id -> {"at": last sweep time, "stamp": (size, mtime) last uploaded}. Purely an
# optimisation: losing it costs one redundant upload, never correctness. Pruned
# against the tracked runs each sweep, so it cannot grow.
_checkpoint_state: dict[str, dict] = {}

# Clean-stop goodbye (W5b): on a graceful stop (Ctrl+C / SIGTERM / normal exit) the
# agent marks a clean shutdown and sends a best-effort goodbye, so the outage cause
# is EXACT. A hard kill (SIGKILL / power loss) skips this -> a dirty marker -> the
# comeback interview reports AGENT_CRASH / POWER_LOSS instead. Guarded so it fires once.
_shutdown = {"server": None, "token": None, "blackbox": None, "said": False}


def _say_goodbye() -> None:
    if _shutdown["said"] or not _shutdown["token"]:
        return
    _shutdown["said"] = True
    if _shutdown["blackbox"] is not None:
        _shutdown["blackbox"].mark_clean_shutdown()  # the twin of the goodbye message
    post_goodbye(_shutdown["server"], _shutdown["token"])


def _on_signal(signum, _frame) -> None:  # noqa: ANN001
    _say_goodbye()
    raise SystemExit(0)


# --- spec detection ---------------------------------------------------------


def _detect_ram_mb() -> int:
    """Total RAM in MB — real value via psutil now (was a 4096 fallback on
    Windows). Env override still wins, for tests and capacity experiments."""
    override = os.environ.get("AGENT_RAM_MB")
    if override:
        return int(override)
    import psutil

    return int(psutil.virtual_memory().total / (1024 * 1024))


def detect_specs(hw_specs: dict | None = None) -> dict:
    """The scheduler-facing declared specs (unchanged shape) + the free-form
    rich identity dict the dashboard shows (additive to the protocol)."""
    cpu = os.cpu_count() or 1
    has_gpu = os.environ.get("AGENT_HAS_GPU", "false").strip().lower() in (
        "1", "true", "yes",
    )
    # A GPU detected by name counts as having one, unless explicitly overridden.
    if "AGENT_HAS_GPU" not in os.environ and hw_specs and hw_specs.get("gpu_name"):
        has_gpu = "nvidia" in hw_specs["gpu_name"].lower()
    capacity = int(os.environ.get("AGENT_CAPACITY", cpu))
    specs = {
        "cpu_cores": cpu,
        "has_gpu": has_gpu,
        "ram_mb": _detect_ram_mb(),
        "capacity": capacity,
        "agent_version": AGENT_VERSION,
    }
    if hw_specs:
        specs["hw_specs"] = hw_specs
    return specs


# --- HTTP (stdlib) ----------------------------------------------------------

# The TLS context every outbound call uses, or None when the control plane is
# plain HTTP. Set once by main() from --ca-cert / AGENT_CA_CERT.
#
# It lives at module level and is applied inside _urlopen() on purpose: the agent
# talks to the control plane from four places, and a context passed by hand at
# each of them is a context that a fifth call site will forget. One door, one
# rule. `None` means stdlib's ordinary behaviour, which is what a plain-HTTP
# deployment gets -- there is no third state where TLS is on but unverified.
_SSL_CONTEXT = None
# The authority as text, kept so a private job's container can be handed it
# through the environment (see runner.start): a path on the worker's disk means
# nothing inside a container that does not mount it.
_CA_PEM: str | None = None


def configure_tls(ca_pem: str | None) -> None:
    """Install the verified context for every later call. Called once, at start."""
    global _SSL_CONTEXT, _CA_PEM
    _CA_PEM = ca_pem
    _SSL_CONTEXT = context_for(ca_pem)


def ca_pem() -> str | None:
    """The authority certificate as text, for handing to a container."""
    return _CA_PEM


def _urlopen(req, timeout: float):
    """urlopen with this agent's TLS context applied.

    When no TLS is configured the call is made WITHOUT a context argument rather
    than with `context=None`. The two are identical to stdlib, and the difference
    is deliberate: it keeps the plain-HTTP call exactly the shape it has always
    been, so a deployment that has not turned TLS on is running the same code path
    it ran yesterday -- which is what the report claims, and claims are cheaper to
    keep true than to re-establish."""
    if _SSL_CONTEXT is None:
        return urllib.request.urlopen(req, timeout=timeout)
    return urllib.request.urlopen(req, timeout=timeout, context=_SSL_CONTEXT)


def _post(url: str, payload: dict, token: str | None = None, timeout: float = 10.0) -> dict:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST")
    req.add_header("Content-Type", "application/json")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    with _urlopen(req, timeout=timeout) as resp:
        body = resp.read().decode("utf-8")
        return json.loads(body) if body else {}


def _post_multipart(
    url: str, fields: dict, file_field: str, filename: str, file_bytes: bytes,
    content_type: str = "application/octet-stream", token: str | None = None,
    timeout: float = 60.0,
) -> dict:
    """Post `multipart/form-data` with stdlib only (the agent's one dep stays
    docker-py). Text fields first, then one file part. Built into a bytes buffer so
    the raw file bytes are never text-encoded."""
    boundary = "----fyp" + secrets.token_hex(16)
    crlf = b"\r\n"
    buf = io.BytesIO()
    b = boundary.encode("ascii")
    for key, value in fields.items():
        buf.write(b"--" + b + crlf)
        buf.write(f'Content-Disposition: form-data; name="{key}"'.encode() + crlf + crlf)
        buf.write(str(value).encode("utf-8") + crlf)
    buf.write(b"--" + b + crlf)
    buf.write(
        f'Content-Disposition: form-data; name="{file_field}"; filename="{filename}"'.encode()
        + crlf
    )
    buf.write(f"Content-Type: {content_type}".encode() + crlf + crlf)
    buf.write(file_bytes + crlf)
    buf.write(b"--" + b + b"--" + crlf)

    req = urllib.request.Request(url, data=buf.getvalue(), method="POST")
    req.add_header("Content-Type", f"multipart/form-data; boundary={boundary}")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    with _urlopen(req, timeout=timeout) as resp:
        body = resp.read().decode("utf-8")
        return json.loads(body) if body else {}


def post_run_artifact(
    server: str, token: str, run_id: str, attempt: int, filename: str, file_bytes: bytes,
    content_type: str = "application/octet-stream", kind: str = "result",
) -> str:
    """Upload one file for a run (W6). Returns:
        'accepted' (200) · 'abort' (409, fenced) · 'too_large' (413, over the
        per-file cap) · 'quota' (413, the owner's retained-storage cap) · 'refused'
        (422, unsealed on a sealed job) · 'retry' (transient error). Idempotent
        server-side (deterministic object key + UNIQUE(run_id, attempt, object_key)),
        so a retry is safe.

    2026-09-07 (walk 1, row 55): a 413 used to come back as 'skip', and the caller
    dropped the file and went on to post SUCCEEDED — a green run with no result and
    nothing on screen saying why. Both 413s now name themselves, and the caller fails
    the run with the reason.

    `kind` (2026-08-13) is 'result' — a finished output, the default and everything
    this call ever sent before — or 'checkpoint', intermediate state a later attempt
    may resume from. It decides which reads can see the file: a checkpoint is never
    listed or downloaded as a result."""
    url = f"{server}/agent/runs/{run_id}/artifacts"
    fields = {"attempt": attempt, "filename": filename, "kind": kind}
    try:
        _post_multipart(url, fields, "file", filename, file_bytes, content_type, token=token)
        return "accepted"
    except urllib.error.HTTPError as exc:
        if exc.code == 409:
            return "abort"
        if exc.code == 413:
            if _refusal_reason(exc) == STORAGE_QUOTA_EXCEEDED:
                log.error(
                    "artifact %s was refused: the job owner's retained-storage cap "
                    "is reached", filename,
                )
                return "quota"
            log.error(
                "artifact %s was refused: over the control plane's per-file cap",
                filename,
            )
            return "too_large"
        if exc.code == 422:
            # 2026-09-06: this job is sealed and these bytes are not. The control
            # plane stored nothing, and it never will for this file, so retrying is
            # pointless and skipping would be dishonest -- a run whose results
            # cannot be kept has not succeeded. 'refused' travels up to the caller,
            # which fails the run with UNSEALED_OUTPUT.
            log.error(
                "artifact %s was refused: the job is sealed and this file is not",
                filename,
            )
            return "refused"
        log.warning("artifact post for run %s -> HTTP %s; will retry", run_id, exc.code)
        return "retry"
    except (urllib.error.URLError, OSError) as exc:
        log.warning("artifact post for run %s failed (%s); will retry", run_id, exc)
        return "retry"


def _refusal_reason(exc: urllib.error.HTTPError) -> str | None:
    """The `reason` the control plane put in a refusal body, or None. Read once,
    because an HTTPError's body can only be read once."""
    try:
        body = json.loads(exc.read().decode("utf-8"))
    except Exception:  # noqa: BLE001 - a body that is not JSON is simply not a reason
        return None
    detail = body.get("detail") if isinstance(body, dict) else None
    return detail.get("reason") if isinstance(detail, dict) else None


def _get_bytes(url: str, token: str, timeout: float = 60.0) -> bytes:
    """GET raw bytes with the node token (used for the sealed input download)."""
    req = urllib.request.Request(url, method="GET")
    req.add_header("Authorization", f"Bearer {token}")
    with _urlopen(req, timeout=timeout) as resp:
        return resp.read()


def fetch_job_input(server: str, token: str, job_id: str) -> bytes:
    """Download a job's input file through the control plane.

    The same route serves both kinds of input, because the difference is in the BYTES
    and not in the transfer: a private job's are sealed and this agent cannot open
    them, an ordinary job's are the file the user uploaded. Brokered either way, so
    the worker still holds no storage credentials."""
    return _get_bytes(f"{server}/agent/jobs/{job_id}/input", token)


def fetch_sealed_input(server: str, token: str, job_id: str) -> bytes:
    """Download a private job's SEALED input through the control plane (W6b).

    What comes back is ciphertext, and the agent has no way to open it — it has no
    key and the key endpoint does not accept node tokens. The agent is a courier
    here, which is the point: a machine can carry private data it cannot read.
    Brokered through the control plane, so the worker still holds no MinIO
    credentials (A.8.31 unchanged from W6)."""
    return _get_bytes(f"{server}/agent/jobs/{job_id}/input", token)


class StaleAttempt(Exception):
    """The control plane answered 409 to a request about a run: it has given the run
    to a newer attempt, and this one is dead. Raised rather than returned so a caller
    cannot mistake it for an ordinary 'nothing to resume from' (2026-09-07, defect F)."""


def fetch_checkpoint(server: str, token: str, run_id: str, attempt: int) -> bytes | None:
    """The newest saved state for this run, or **None meaning start from the top**.

    None is the ordinary answer, not a failure. It covers every case where we cannot
    hand the workload something we trust:

      * `204` — nothing saved yet (attempt 1 of anything, or a workload that never
        checkpoints). This is what every run did before the feature existed;
      * the digest does not match the bytes — the file is corrupt or truncated, so we
        throw it away and train from the beginning. **A run that crashes on resume is
        worse than one that starts over**, so a bad checkpoint is treated as no
        checkpoint. It is logged loudly, because it should never happen;
      * no digest at all (a row written before digests existed) — unverifiable, and
        unverifiable is refused rather than accepted on trust;
      * the request failed. Losing the resume costs repeated training; getting it
        wrong could cost a wrong answer.

    A `409` is NOT one of those: it is the fence, and it raises `StaleAttempt` so
    the caller starts nothing (2026-09-07 audit, defect F). Before that it read as
    absent, and the agent started a container -- and posted RUNNING -- for an
    attempt the control plane had already moved past.

    The digest is the control plane's own record of the bytes it stored, so this
    check catches corruption and truncation between storage and here."""
    url = f"{server}/agent/runs/{run_id}/checkpoint?attempt={attempt}"
    req = urllib.request.Request(url, method="GET")
    req.add_header("Authorization", f"Bearer {token}")
    try:
        with _urlopen(req, timeout=120.0) as resp:
            if resp.status == 204:
                return None  # nothing to resume from — the normal case
            data = resp.read()
            claimed = (resp.headers.get("X-Checkpoint-Sha256") or "").strip()
            from_attempt = resp.headers.get("X-Checkpoint-Attempt")
    except urllib.error.HTTPError as exc:
        if exc.code == 409:
            raise StaleAttempt(
                f"run {run_id} attempt {attempt}: the control plane has moved on"
            ) from exc
        log.warning(
            "checkpoint fetch for run %s -> HTTP %s; starting from the beginning",
            run_id, exc.code,
        )
        return None
    except (urllib.error.URLError, OSError) as exc:
        log.warning(
            "checkpoint fetch for run %s failed (%s); starting from the beginning",
            run_id, exc,
        )
        return None

    if not claimed:
        log.warning("checkpoint for run %s carries no digest; not using it", run_id)
        return None
    actual = hashlib.sha256(data).hexdigest()
    if actual != claimed:
        log.error(
            "checkpoint for run %s FAILED its digest (%s != %s); treating it as absent "
            "and starting from the beginning",
            run_id, actual, claimed,
        )
        return None
    log.info(
        "resuming run %s from a %d-byte checkpoint saved by attempt %s",
        run_id, len(data), from_attempt,
    )
    return data


def fetch_key_ticket(server: str, token: str, run_id: str, attempt: int) -> dict:
    """Get a single-use ticket for this run's key (W6b). Fenced server-side: a stale
    attempt or a run this node doesn't own is 409, so a node the control plane has
    given up on cannot obtain a pass. Returns {ticket, expires_at, key_url}.

    The agent puts the TICKET into the container's environment and never sees a key."""
    url = f"{server}/agent/runs/{run_id}/key-ticket?attempt={attempt}"
    return _post(url, {}, token=token)


def post_run_status(
    server: str, token: str, run_id: str, attempt: int, state: str,
    exit_code: int | None = None,
    failure_reason: str | None = None, failure_detail: str | None = None,
    progress: float | None = None, metrics: dict | None = None,
) -> str:
    """Report a run's state. Returns one of:
        'accepted' (200) · 'abort' (409, fenced) · 'retry' (transient error).

    W5b: a FAILED post carries the agent's hard-fact classification (why it died).
    2026-09-07 (walk 1, row 25): a terminal post may carry the LAST progress marker
    the container printed, so the stored progress does not stop one heartbeat short
    of the finish (the run used to read "100% · loss 0.25" beside a log whose last
    epoch said 0.2). Optional; an older control plane ignores the two fields."""
    url = f"{server}/agent/runs/{run_id}/status"
    payload = {"attempt": attempt, "state": state, "exit_code": exit_code}
    if failure_reason is not None:
        payload["failure_reason"] = failure_reason
        payload["failure_detail"] = failure_detail
    if progress is not None:
        payload["progress"] = progress
    if metrics is not None:
        payload["metrics"] = metrics
    try:
        _post(url, payload, token=token)
        return "accepted"
    except urllib.error.HTTPError as exc:
        if exc.code == 409:
            return "abort"
        log.warning("status post for run %s -> HTTP %s; will retry", run_id, exc.code)
        return "retry"
    except (urllib.error.URLError, OSError) as exc:
        log.warning("status post for run %s failed (%s); will retry", run_id, exc)
        return "retry"


def post_run_samples(
    server: str, token: str, run_id: str, attempt: int, samples: list[dict]
) -> str:
    """Post a batch of the container's resource samples (W5b). Returns:
        'accepted' (200) · 'abort' (409, fenced) · 'retry' (transient error).
    Idempotent server-side (UNIQUE(run_id, attempt, ts)), so a retry is safe."""
    url = f"{server}/agent/runs/{run_id}/samples"
    try:
        _post(url, {"attempt": attempt, "samples": samples}, token=token)
        return "accepted"
    except urllib.error.HTTPError as exc:
        if exc.code == 409:
            return "abort"
        return "retry"
    except (urllib.error.URLError, OSError):
        return "retry"


def post_goodbye(server: str, token: str, reason: str = "shutdown") -> None:
    """Best-effort 'going offline' on a clean stop (W5b). Failure is ignored — a
    lost goodbye is fine, the clean marker doubles it on the comeback interview."""
    try:
        _post(f"{server}/agent/goodbye", {"reason": reason}, token=token, timeout=3.0)
    except (urllib.error.HTTPError, urllib.error.URLError, OSError):
        pass


def post_run_logs(
    server: str, token: str, run_id: str, attempt: int, seq: int, chunk: str
) -> str:
    """Post one batch of container output. Returns:
        'accepted' (200) · 'abort' (409, fenced) · 'retry' (transient error)."""
    url = f"{server}/agent/runs/{run_id}/logs"
    payload = {"attempt": attempt, "seq": seq, "chunk": chunk}
    try:
        _post(url, payload, token=token)
        return "accepted"
    except urllib.error.HTTPError as exc:
        if exc.code == 409:
            return "abort"
        log.warning("log post for run %s -> HTTP %s; will retry", run_id, exc.code)
        return "retry"
    except (urllib.error.URLError, OSError) as exc:
        log.warning("log post for run %s failed (%s); will retry", run_id, exc)
        return "retry"


# --- state ------------------------------------------------------------------


def _load_state() -> dict | None:
    """This worker's saved identity, or None.

    Falls back to the pre-2026-09-07 shared file when the per-name one does not exist
    yet, so a worker that registered before the per-name files existed keeps its
    node rather than appearing twice after its first restart. The caller still checks
    that the name matches, so somebody else's identity is never adopted."""
    for path in (STATE_FILE, LEGACY_STATE_FILE):
        try:
            with open(path, encoding="utf-8") as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            continue
    return None


def _save_state(state: dict) -> None:
    """Write this worker's identity, owner-readable only (2026-09-07 audit, defect E).

    The file holds the node token. Created at 0600 through `os.open` and then
    chmodded to 0600 explicitly, so the mode does not depend on the umask the agent
    happened to be started under -- the same reasoning the checkpoint files carry.
    On Windows `os.chmod` only toggles the read-only bit, and the call is harmless."""
    fd = os.open(STATE_FILE, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        json.dump(state, f)
    os.chmod(STATE_FILE, 0o600)


# --- lifecycle --------------------------------------------------------------


def _speaks_tls(host: str, port: int, timeout: float = 3.0) -> bool:
    """Does the peer complete a TLS handshake? Observed, not assumed.

    Verification is off here ON PURPOSE and it is not a hole: nothing is sent and
    nothing is trusted. The only question asked is whether the port speaks TLS at
    all, so that the sentence below can be a measured fact rather than a guess.
    The answer is used to tell the operator to switch to the VERIFIED https path;
    no request, token or credential crosses this socket.
    """
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    try:
        with socket.create_connection((host, port), timeout=timeout) as sock:
            with ctx.wrap_socket(sock, server_hostname=host):
                return True
    except OSError:
        return False


def scheme_mismatch(server: str, exc: BaseException) -> str | None:
    """One plain sentence when a failed connection is really the wrong scheme.

    The control plane has served HTTPS only since 2026-08-22. Point a plain-http
    client at it and the socket closes with no reply -- as `RemoteDisconnected`
    from uvicorn, as a bare connection reset from others -- and point an https
    client at a plain-http port and TLS reports a wrong version number. Neither
    error says "wrong scheme" anywhere in its text, so `register()` retried both
    for ever, three seconds apart, printing the raw exception each time.

    This is the fourth sibling of the defect fixed earlier in
    `seed_job.py`; the 13 August live checkpoint script and the W7a harness were
    the second and third.

    A CLOSED CONNECTION IS NOT BY ITSELF EVIDENCE OF TLS -- a correctly configured
    http server can reset one too -- so that half is confirmed by handshaking with
    the port before the sentence is said. The https half needs no probe: a wrong
    version number means the peer already answered in something that was not TLS.

    Returns None for anything else. A control plane that is merely not up yet must
    still be waited for, which is what the staging script depends on.
    """
    reason = getattr(exc, "reason", exc)
    low = server.lower()
    parts = urllib.parse.urlsplit(server)

    if low.startswith("http://") and isinstance(
        reason, (http.client.RemoteDisconnected, http.client.BadStatusLine,
                 ConnectionResetError)
    ):
        host, port = parts.hostname, parts.port or 80
        if host and _speaks_tls(host, port):
            return (
                "the control plane is speaking HTTPS but --server says http:// -- "
                "point it at %s instead, and pass --ca-cert."
                % ("https://" + server[len("http://"):])
            )
        return None

    if low.startswith("https://") and isinstance(reason, ssl.SSLError):
        return (
            "the control plane is speaking plain HTTP but --server says https:// -- "
            "point it at %s instead, and unset --ca-cert."
            % ("http://" + server[len("https://"):])
        )
    return None


def register(server: str, name: str, specs: dict) -> dict:
    """Register, retrying until the control plane answers. Returns {node_id, token}."""
    payload = {"name": name, "specs": specs}
    attempt = 0
    said_hint = False
    while True:
        attempt += 1
        try:
            resp = _post(f"{server}/agent/register", payload)
            state = {"node_id": resp["node_id"], "token": resp["token"], "name": name}
            _save_state(state)
            return state
        except (urllib.error.URLError, OSError) as exc:
            hint = scheme_mismatch(server, exc)
            if hint and not said_hint:
                log.error("%s", hint)
                said_hint = True
            log.warning("register attempt %d failed (%s); retrying in 3s", attempt, exc)
            time.sleep(3)


def _usage_with_disk() -> dict:
    """The usual usage sample plus `disk_free_mb` for the filesystem holding this
    agent's working root.

    Measured on the working root rather than on the system drive, because that is the
    disk a run's temporary files will actually land on — on a worker whose
    `AGENT_OUTPUT_ROOT` points at a second disk, the system drive's free space is a
    true number about the wrong thing.

    Best-effort: an unreadable disk simply omits the field, and the control plane
    treats a missing value as UNKNOWN and never excludes the machine for it. A
    placement refused because one reading failed would be a worse answer than a
    placement made."""
    usage = collect_usage()
    try:
        usage["disk_free_mb"] = int(shutil.disk_usage(run_root()).free / (1024 * 1024))
    except OSError as exc:
        log.warning("could not read free disk (%s); omitting it from the heartbeat", exc)
    return usage


def _node_status(runner: Runner) -> str:
    """Self-reported idle|busy (never offline — that's derived server-side)."""
    return "busy" if runner.running_report() else "idle"


# The log-batch budget for ONE TICK OF THE WHOLE WORKER (2026-09-07 system QA;
# widened from per-run to worker-wide in the QA follow-up of the same day). One
# batch already carries EVERY complete line printed since the cursor, so a second
# pass only picks up what arrived during the first post. The loop used to run until
# a pass found nothing new -- which is the workload's decision, not ours: a container
# printing faster than one post round-trip never gave it that pass, the tick never
# reached the heartbeat, the node showed offline after 12 s, the lease expired and a
# healthy run was reclaimed as LOST, its checkpoint never uploaded and its cancel
# never delivered. Found with the dummy printing two lines every 0.5 s against posts
# taking 2 s (docs/evidence/system_qa_2026-09-07/heartbeat_starvation_*.txt).
#
# A cap on EACH RUN was not enough, and that is the QA follow-up's finding: a worker
# executing N chatty runs still spent N times the cap before its heartbeat, which is
# the same starvation with more steps. `_pump_all_logs` therefore spends this budget
# ACROSS runs -- one batch each, rotating which runs it visits -- so a tick's log cost
# is bounded by this number however many runs the worker holds. The bound is bought
# with latency, and the price is written down rather than implied: with N runs a
# given run is visited once every ceil(N / MAX_LOG_BATCHES_PER_TICK) ticks, so six
# concurrent runs means one delivery per run every other tick. What is left goes next
# tick, cursor intact.
#
# The two FINAL-flush sites -- an exited container, and a run stopped at its disk cap
# -- still take up to all three passes: there the output is finite, the terminal
# status waits on it, and there is no next tick to leave it to.
MAX_LOG_BATCHES_PER_TICK = 3

# Where the next tick's rotation starts, so no run is starved by list order alone.
_log_pump_offset = 0


def _stream_logs(
    server: str,
    token: str,
    runner: Runner,
    run_id: str,
    final: bool = False,
    max_batches: int = MAX_LOG_BATCHES_PER_TICK,
) -> bool:
    """Flush new container output for one run, at most `max_batches` posts, so a
    chatty workload cannot hold the tick and starve the heartbeat. The heartbeat's
    streaming pass passes 1 and spends the tick's budget across runs; the two
    final-flush sites take the full `MAX_LOG_BATCHES_PER_TICK`.
    Retry-stable: `collect_logs` reuses the same seq until we `confirm_logs`, so a
    dropped post is re-sent unchanged (the control plane's DB then dedups it).
    Return True when this run has nothing more to send; False when delivery remains
    pending (a retry, or lines still behind the cap) or the run was fenced out."""
    for _ in range(max_batches):
        batch = runner.collect_logs(run_id, final=final)
        if batch is None:
            return True
        seq, chunk = batch
        attempt = runner.attempt_of(run_id)
        if attempt is None:
            return False
        result = post_run_logs(server, token, run_id, attempt, seq, chunk)
        if result == "accepted":
            runner.confirm_logs(run_id)
        elif result == "abort":
            runner.abort(run_id)  # fenced by the control plane — stop this run
            return False
        else:  # 'retry' — stop this tick; the same lines re-send next tick
            return False
    # Cap reached with the last batch accepted and more output possibly behind it.
    # Nothing is lost: the cursor sits on the first unsent line and the next tick
    # continues from it. A final flush that lands here says "pending", so the
    # terminal status waits for the rest instead of closing the run over unsent lines.
    return False


def _pump_all_logs(server: str, token: str, runner: Runner) -> None:
    """Stream output within one worker-wide budget, rotating between active runs."""
    global _log_pump_offset
    tracked = runner.tracked_ids()
    if not tracked:
        _log_pump_offset = 0
        return
    start = _log_pump_offset % len(tracked)
    ordered = tracked[start:] + tracked[:start]
    visit_count = min(MAX_LOG_BATCHES_PER_TICK, len(ordered))
    for run_id in ordered[:visit_count]:
        _stream_logs(server, token, runner, run_id, final=False, max_batches=1)
    _log_pump_offset = (start + visit_count) % len(tracked)


def _enforce_scratch(
    server: str, token: str, runner: Runner, run_id: str, used_mb: float, cap_mb: int
) -> None:
    """This run has written more temporary disk than it was allowed. Stop it (R8).

    Everything happens HERE rather than being left for the ordinary exit path,
    because a killed container would otherwise surface next tick as a plain non-zero
    exit and be reported as "exit 137" — the symptom, not the cause. The run deserves
    to be told why it died.

    The order is: stop the container, flush its last output (so the log a person reads
    ends with what it was doing when it crossed the line), then post FAILED. The
    artefact upload that normally sits between those two is DELIBERATELY SKIPPED, and
    that is a decision rather than an omission: the files in `/scratch` are the ones
    that blew the cap, so uploading them would take a run that used too much temporary
    disk and move those same bytes into the user's retained storage — punishing the
    same cap twice, from both ends. A run stopped for filling its disk keeps no
    results.

    Terminal, and not re-dispatched, for the reason the control plane does not
    re-dispatch a refused upload either: it is the user's own limit, and another
    machine would hit it in exactly the same place."""
    attempt = runner.attempt_of(run_id)
    if attempt is None:
        return
    log.warning(
        "run %s wrote %.1f MB into its temporary disk, over its %d MB cap - stopping it",
        run_id, used_mb, cap_mb,
    )
    runner.stop(run_id)
    if _stream_logs(server, token, runner, run_id, final=True) is False:
        return  # retain the stopped run and its output for the next tick
    if not runner.has(run_id):
        return  # a 409 during the flush already aborted it
    detail = (
        f"Temporary disk full: the run wrote {used_mb:.1f} MB into /scratch and /tmp "
        f"on this machine, over its {cap_mb} MB limit, so it was stopped. Ask for more "
        "temporary disk when you submit, or write less."
    )
    result = post_run_status(
        server, token, run_id, attempt, "FAILED", None,
        SCRATCH_QUOTA_EXCEEDED, detail,
    )
    if result == "abort":
        runner.abort(run_id)
    elif result == "accepted":
        runner.cleanup(run_id)
    # 'retry': leave it tracked. The container is already stopped, so poll() will
    # surface it as exited next tick and it is reported then — late, but never lost.


def _pump_all_samples(server: str, token: str, runner: Runner) -> None:
    """Sample each running container's OWN usage and post it (W5b). One reading per
    tick with a stable wall-clock ts; a transient failure just drops that sample
    (samples are a best-effort picture, not the log guarantee). A 409 aborts the run.

    2026-09-04: the same reading now carries `scratch_used_mb`, and it is the reading
    the temporary-disk cap is enforced from — so the number that stops a run is the
    same number stored for the person who later asks why it stopped.

    **The cap is checked even when the sample post fails.** Posting is best-effort;
    stopping a run that is filling a shared machine's disk is not, and a network
    hiccup must not be the thing that lets it carry on."""
    for run_id in runner.tracked_ids():
        sample = runner.sample_stats(run_id)
        attempt = runner.attempt_of(run_id)
        if attempt is None:
            continue
        used_mb = runner.scratch_used_mb(run_id)  # None for a private run
        if sample is not None:
            if used_mb is not None:
                sample["scratch_used_mb"] = used_mb
            sample["ts"] = time.time()
            if post_run_samples(server, token, run_id, attempt, [sample]) == "abort":
                runner.abort(run_id)
                continue
        cap_mb = runner.scratch_limit_of(run_id)
        if used_mb is not None and cap_mb and used_mb > cap_mb:
            _enforce_scratch(server, token, runner, run_id, used_mb, cap_mb)


def _scan_all_progress(runner: Runner) -> None:
    """Refresh every running container's latest ##PROGRESS marker (W5b), so the next
    heartbeat carries live progress."""
    for run_id in runner.tracked_ids():
        runner.scan_progress(run_id)


def _upload_artifacts(server: str, token: str, runner: Runner, run_id: str, attempt: int) -> bool:
    """Upload every output file a run wrote to /scratch (W6), before its terminal
    status — a crash dump is a result too, so failures upload as well. A transient
    failure retains the run and its files for retry; a 409 aborts the whole run.

    Returns True when finalization must stop this tick -- delivery is pending, it posted
    FAILED for a refused result (whether that post was accepted or must be retried),
    or aborted on a 409 -- and False when the caller should go on to post the
    ordinary terminal status (2026-09-07 audit, defect B)."""
    def _fail(reason: str, detail: str) -> None:
        # Reported here rather than left to the exit code, because the exit code is
        # 0: the workload finished cleanly, and what failed is the RUN — its result
        # cannot be kept. Naming it is the difference between a user fixing one line
        # and a user staring at a green run with no results (walk 1, rows 32 and 55).
        #
        # The verdict is honoured the way `_enforce_scratch` and `_drain_finished`
        # honour it. 'retry' leaves the run TRACKED: the exited container surfaces
        # again from poll() next tick and this whole path repeats, so the reason
        # reaches the control plane late rather than never. Before 2026-09-07 the
        # run was cleaned up whatever the answer, and a transient failure lost the
        # diagnostic for good -- the run then timed out with no reason at all.
        result = post_run_status(
            server, token, run_id, attempt, "FAILED", None, reason, detail
        )
        if result == "accepted":
            runner.cleanup(run_id)
        elif result == "abort":
            runner.abort(run_id)

    for filename, path, size in runner.artifact_paths(run_id):
        name = os.path.basename(filename)
        size_mb = size / (1024 * 1024)
        if size > MAX_ARTIFACT_MB * 1024 * 1024:
            log.error("artifact %s (%.1f MB) is over the %d MB per-file cap; failing "
                      "the run", name, size_mb, MAX_ARTIFACT_MB)
            _fail(
                ARTIFACT_TOO_LARGE,
                f"{name} is {size_mb:.1f} MB, over the {MAX_ARTIFACT_MB} MB per-file "
                "cap on a result file, so nothing was stored. Write a smaller file, "
                "or split it into several under the cap.",
            )
            return True
        try:
            with open(path, "rb") as f:
                data = f.read()
        except OSError as exc:
            log.warning("could not read artifact %s (%s); retrying next tick", filename, exc)
            return True
        result = post_run_artifact(server, token, run_id, attempt, name, data)
        if result == "retry":
            return True
        if result == "abort":
            runner.abort(run_id)
            return True
        if result == "refused":
            _fail(
                UNSEALED_OUTPUT,
                f"{name} was written unsealed; a sealed job's outputs must be written "
                "through the writer in the image (fyp_data.create_output), not "
                "through open()",
            )
            return True
        if result == "too_large":
            _fail(
                ARTIFACT_TOO_LARGE,
                f"the control plane refused {name} ({size_mb:.1f} MB) as over its "
                "per-file cap on a result file, so nothing was stored. Write a "
                "smaller file, or split it into several under the cap.",
            )
            return True
        if result == "quota":
            # The control plane stamped the refusal on the run and decides the
            # outcome itself whatever is posted here; this post only stops the
            # container's machine being spent on work that cannot be kept.
            _fail(
                STORAGE_QUOTA_EXCEEDED,
                f"{name} could not be stored: the job owner's retained-storage cap "
                "is reached. Free space by releasing a finished job's storage, then "
                "run it again.",
            )
            return True
    return False


def _upload_checkpoints(server: str, token: str, runner: Runner) -> None:
    """Sweep each running container's checkpoint file up to the control plane, so the
    work survives the machine (2026-08-13).

    Three cheap rules keep this from costing anything it should not:

      * **not every tick** — `CHECKPOINT_UPLOAD_INTERVAL_S` between sweeps per run;
      * **skip an unchanged file** — same size and mtime means the same bytes, and
        re-sending them buys nothing;
      * **skip a file being written** — the size or mtime moving while we read it
        means we caught a half-written save, so we leave it for the next sweep. The
        contract also asks workloads to write to a temporary name and rename, which
        makes a torn read impossible rather than merely unlikely.

    Uploaded with `kind=checkpoint`, to the same stable name every time, so a run
    keeps ONE checkpoint per attempt however often it saves. Best-effort throughout:
    a failed sweep costs repeated training if the machine then dies, and nothing
    else. A `409` is the fence and aborts the run, exactly as it does everywhere."""
    now = time.time()
    tracked = runner.tracked_ids()
    for gone in set(_checkpoint_state) - set(tracked):
        _checkpoint_state.pop(gone, None)  # finished/aborted runs leave nothing behind
    for run_id in tracked:
        path = runner.checkpoint_path(run_id)
        if path is None:
            continue  # private run, or nothing saved yet
        attempt = runner.attempt_of(run_id)
        if attempt is None:
            continue
        last = _checkpoint_state.get(run_id, {})
        if now - last.get("at", 0.0) < CHECKPOINT_UPLOAD_INTERVAL_S:
            continue
        try:
            before = os.stat(path)
            if (before.st_size, before.st_mtime) == last.get("stamp"):
                _checkpoint_state.setdefault(run_id, {})["at"] = now
                continue  # unchanged since the last successful upload
            if before.st_size > MAX_ARTIFACT_MB * 1024 * 1024:
                log.warning(
                    "checkpoint for run %s is %d bytes, over the cap; not uploading",
                    run_id, before.st_size,
                )
                _checkpoint_state[run_id] = {"at": now, "stamp": last.get("stamp")}
                continue
            with open(path, "rb") as f:
                data = f.read()
            after = os.stat(path)
        except OSError as exc:
            log.warning("could not read checkpoint for run %s (%s)", run_id, exc)
            continue
        if (after.st_size, after.st_mtime) != (before.st_size, before.st_mtime):
            continue  # it moved under us — mid-save; take it next sweep

        result = post_run_artifact(
            server, token, run_id, attempt, CHECKPOINT_FILENAME, data,
            "application/octet-stream", kind="checkpoint",
        )
        if result == "abort":
            runner.abort(run_id)
            _checkpoint_state.pop(run_id, None)
            return
        if result == "accepted":
            _checkpoint_state[run_id] = {
                "at": now, "stamp": (after.st_size, after.st_mtime)
            }
            # Said out loud (walk 1, row 38): a person waiting to kill this worker
            # "after a checkpoint" had nothing on the terminal to wait for.
            log.info("checkpoint for run %s stored (%d bytes)", run_id, len(data))
        else:
            # 'retry'/'skip': try again next sweep rather than recording it as sent.
            _checkpoint_state[run_id] = {"at": now, "stamp": last.get("stamp")}


def _drain_finished(server: str, token: str, runner: Runner) -> None:
    """For every container that has exited: flush its final output, upload its output
    files, then post the terminal status. The log flush goes FIRST so the last lines
    (including a final line with no trailing newline) are never lost; the artifact
    upload goes BEFORE the terminal status so a completed run's results are already
    stored when it is marked done.

    W5b: a non-zero exit is classified from HARD FACTS (the container's State dict —
    OOMKilled + exit code) and the reason travels with the FAILED status."""
    for done in runner.poll():
        run_id, attempt, code = done["run_id"], done["attempt"], done["exit_code"]
        if _stream_logs(server, token, runner, run_id, final=True) is False:
            continue  # retain final output until it is acknowledged, or abort
        if not runner.has(run_id):
            continue  # a 409 during the flush already aborted the run
        if _upload_artifacts(server, token, runner, run_id, attempt):  # W6 — before terminal
            # Delivery is pending, or a refused result was posted as FAILED
            # (accepted, or left tracked to be re-posted next tick), or a 409 aborted
            # the run. Either way the exit code has nothing more to say -- an exit-0
            # run whose result was refused must never be posted SUCCEEDED.
            continue
        if not runner.has(run_id):
            continue  # a 409 during upload aborted the run
        # The last ##PROGRESS line is in the container's output now that it has
        # exited; read it so the terminal post carries the finish, not the step
        # before it (walk 1, row 25).
        runner.scan_progress(run_id)
        progress, metrics = runner.progress_of(run_id)
        if _cancelled.pop(run_id, False):
            # Stopped on the user's request (walk 1, row 64): the reason is the
            # request, not the signal the stop happened to use.
            result = post_run_status(
                server, token, run_id, attempt, "FAILED", code, CANCELLED,
                "Cancelled by the user; the worker stopped the container.",
                progress=progress, metrics=metrics,
            )
        elif code == 0:
            result = post_run_status(
                server, token, run_id, attempt, "SUCCEEDED", code,
                progress=progress, metrics=metrics,
            )
        else:
            # W6b: the output tail is read while the run is still tracked, so a
            # broken seal (the helper's ##INTEGRITY_ERROR marker) is classified as
            # exactly that instead of the bare "exited 1" it would look like.
            classified = classify_failure(
                done.get("state"), code, None, runner.mem_limit_of(run_id),
                log_tail=runner.log_tail(run_id),
                # 2026-09-04: so "out of space" can name the right limit. A public
                # run has a disk cap; a private one has a RAM folder of a fixed size,
                # and telling its owner to ask for more disk would send them to fix
                # the wrong thing.
                scratch_mb=runner.scratch_limit_of(run_id),
                private=runner.is_private(run_id),
            )
            reason, detail = classified if classified else (None, None)
            result = post_run_status(
                server, token, run_id, attempt, "FAILED", code, reason, detail,
                progress=progress, metrics=metrics,
            )
        if result == "accepted":
            log.info(
                "run %s -> %s (exit=%s)", run_id, "SUCCEEDED" if code == 0 else "FAILED", code
            )
            runner.cleanup(run_id)
        elif result == "abort":
            runner.abort(run_id)
        # 'retry': leave it tracked; poll() surfaces it again next tick.


def _stage_private(server: str, token: str, a: dict) -> dict:
    """Prepare everything a PRIVATE run needs before its container starts (W6b).

    Two fetches, in this order:
      1. the SEALED input -> a file on this worker's disk. Ciphertext, so leaving it
         on disk costs nothing; the agent could not read it if it tried.
      2. a single-use TICKET for the key. Not the key — the agent never holds one.

    Returns the kwargs `Runner.start` needs. Raises on failure, and the caller turns
    that into a reported FAILED, so a run that cannot be staged never hangs."""
    blob = fetch_sealed_input(server, token, a["job_id"])
    fd, sealed_path = tempfile.mkstemp(
        prefix=f"fyp-sealed-{a['run_id']}-{a['attempt']}-",
        suffix=".bin",
        dir=os.environ.get("AGENT_OUTPUT_ROOT") or None,
    )
    with os.fdopen(fd, "wb") as f:
        f.write(blob)
    try:
        granted = fetch_key_ticket(server, token, a["run_id"], a["attempt"])
    except Exception:
        # 2026-09-07 audit (defect D): the file written just above belongs to a run
        # that will now never start, and nothing else knows its name. Best-effort.
        try:
            os.remove(sealed_path)
        except OSError:
            pass
        raise
    return {
        "private": True,
        "sealed_path": sealed_path,
        "ticket": granted.get("ticket"),
        "key_url": granted.get("key_url"),
        "input_filename": a.get("input_filename"),
        # The authority this agent verifies against, passed on so the container
        # can verify the same one when it redeems the ticket. None on a plain-HTTP
        # deployment, where the helper skips TLS setup entirely.
        "ca_pem": ca_pem(),
    }


def _stage_input(server: str, token: str, a: dict, run_dir: str) -> dict:
    """Download an ORDINARY job's dataset file and put it where the container will
    find it (2026-09-05).

    Written into the run's own directory, the same tree the scratch, temporary and
    checkpoint folders live in, so one attempt's file is measured with everything else
    that attempt writes and is cleared with it when the next attempt begins.

    The file is NOT unpacked. A zip stays a zip and the container opens it, because
    unpacking an archive we were handed is untrusted-input work and the container
    already owns a measured, capped place to do it.

    Raises on failure, and the caller turns that into a reported FAILED — a run whose
    input never arrived must not start and silently produce a wrong answer."""
    data = fetch_job_input(server, token, a["job_id"])
    # Reduced to a bare name again even though the control plane already did it. The
    # name decides a path on THIS machine, and a filename that arrived over the
    # network is not a thing to trust once.
    raw = (a.get("input_filename") or "input.bin").replace("\\", "/")
    name = os.path.basename(raw).strip() or "input.bin"
    if name in (".", ".."):
        name = "input.bin"
    input_dir = os.path.join(run_dir, "input")
    os.makedirs(input_dir, exist_ok=True)
    path = os.path.join(input_dir, name)
    with open(path, "wb") as f:
        f.write(data)
    # The container runs as a uid we do not choose, so the mode is explicit rather
    # than left to the umask -- the same lesson the checkpoint file already carries.
    os.chmod(path, 0o644)
    log.info("staged %d-byte input %s for run %s", len(data), name, a["run_id"])
    return {"input_path": path, "input_name": name}


def _stage_checkpoint(server: str, token: str, a: dict, run_dir: str) -> dict:
    """Put the previous attempt's saved state into this run's `/checkpoint` folder if
    there is any (2026-08-13). Returns the kwarg `Runner.start` needs.

    The folder is created for EVERY ordinary run by `prepare_run_dir`, whether or not
    anything is resumed, because a first attempt needs somewhere to write its first
    checkpoint. A workload that never saves simply never touches it.

    2026-09-04: it lives INSIDE the run's one host directory rather than in a
    directory of its own, so a checkpoint counts as the temporary disk it is. What
    keeps it out of the RESULTS is unchanged and is not about where the folder sits:
    `artifact_paths` walks `scratch/` alone.

    The fetch is skipped on attempt 1: checkpoints are written by earlier attempts of
    the same run, and a first attempt has none by definition — so the ordinary case
    costs no extra request at all.

    Failure here is not fatal by design. If the fetch fails, or the file cannot be
    written, the run starts from the beginning — which is what it would have done
    before this feature existed."""
    checkpoint_dir = os.path.join(run_dir, CHECKPOINT_DIR_NAME)
    os.makedirs(checkpoint_dir, exist_ok=True)
    # The container runs as its image's uid — so without this the resumed run can
    # neither read what we staged nor write a new checkpoint, and `save()` is
    # best-effort, so it says nothing when it cannot. NOT the sticky 1777 that
    # /scratch uses; see CHECKPOINT_DIR_MODE for why the two genuinely differ.
    os.chmod(checkpoint_dir, CHECKPOINT_DIR_MODE)
    if a["attempt"] > 1:
        data = fetch_checkpoint(server, token, a["run_id"], a["attempt"])
        if data is not None:
            try:
                staged = os.path.join(checkpoint_dir, CHECKPOINT_FILENAME)
                with open(staged, "wb") as f:
                    f.write(data)
                # Explicit, not umask-dependent: the container reads this as a uid we
                # do not choose and cannot know (CHECKPOINT_FILE_MODE).
                os.chmod(staged, CHECKPOINT_FILE_MODE)
            except OSError as exc:
                log.warning(
                    "could not stage checkpoint for run %s (%s); starting fresh",
                    a["run_id"], exc,
                )
    return {"checkpoint_dir": checkpoint_dir}


def _stage_sealed(server: str, token: str, a: dict, run_dir: str) -> dict:
    """Prepare what a SEALED run needs before its container starts (2026-09-06).

    Two things, and only the second is always needed:

      1. if the job carries an input file, the SEALED bytes -> a file inside this
         run's own directory. Ciphertext, so leaving it on this disk costs nothing:
         the agent could not read it if it tried, and it is mounted READ-ONLY so the
         container cannot alter the dataset it was handed;
      2. a single-use TICKET for the job's key. Not the key — the agent never holds
         one — and taken for every sealed run whether or not there is an input,
         because the container also seals what it WRITES.

    Everything else about the run is unchanged: the ordinary three writable folders,
    the checkpoint staged into one of them, the disk cap measured over one tree. That
    sameness is the point of the change. A sealed run is an ordinary run whose bytes
    happen to be sealed, which is why privacy no longer costs a job its results or
    its ability to resume.

    Raises on failure; the caller turns that into a reported FAILED, so a run that
    cannot be staged never hangs in ASSIGNED."""
    extra: dict = {"sealed": True}
    if a.get("has_input"):
        blob = fetch_job_input(server, token, a["job_id"])
        raw = (a.get("input_filename") or "input.bin").replace("\\", "/")
        name = os.path.basename(raw).strip() or "input.bin"
        if name in (".", ".."):
            name = "input.bin"
        input_dir = os.path.join(run_dir, "input")
        os.makedirs(input_dir, exist_ok=True)
        path = os.path.join(input_dir, name)
        with open(path, "wb") as f:
            f.write(blob)
        # 0644 explicitly: the container runs as a different user, and a file it
        # cannot read would fail the run for a reason nobody would look for. Set
        # rather than left to the umask -- the same lesson the checkpoint file and
        # the plain input file already carry.
        os.chmod(path, 0o644)
        extra["sealed_path"] = path
        extra["input_name"] = name
        log.info(
            "staged %d sealed bytes as %s for run %s", len(blob), name, a["run_id"]
        )

    granted = fetch_key_ticket(server, token, a["run_id"], a["attempt"])
    extra["ticket"] = granted.get("ticket")
    extra["key_url"] = granted.get("key_url")
    # The authority as PEM TEXT, so the container can verify the control plane when
    # it redeems the ticket over HTTPS. Text rather than a path, because a worker's
    # file path means nothing inside a container that does not mount it. None on a
    # plain-HTTP deployment.
    extra["ca_pem"] = ca_pem()
    return extra


def _act_on_assignments(server: str, token: str, runner: Runner, assignments: list) -> None:
    """Start a container per new assignment and confirm it RUNNING.

    2026-09-07 audit (defect A): an assignment for a run this node is ALREADY
    executing is compared by attempt, not just by run id. The same attempt is a
    later heartbeat re-listing it, and is ignored as before. A HIGHER attempt means
    the lease on the tracked execution expired while its container was still
    running, the reaper requeued the run, and this same node claimed it again: the
    tracked execution is stale by the control plane's own decision, so it is torn
    down and the new attempt starts. Before this, `runner.has` alone dropped the
    new assignment, attempt N+1 never started, never renewed, and was reaped again
    -- the job stalled, burning retries. A LOWER attempt cannot happen and is not
    started."""
    for a in assignments:
        run_id, attempt = a["run_id"], a["attempt"]
        tracked = runner.attempt_of(run_id)
        if tracked is not None:
            if attempt == tracked:
                continue  # already executing it (a later heartbeat re-listed it)
            if attempt < tracked:
                log.warning(
                    "run %s: assignment at attempt %s is OLDER than the attempt %s "
                    "executing here; ignoring it", run_id, attempt, tracked,
                )
                continue
            log.warning(
                "run %s: re-assigned at attempt %s while attempt %s is still executing "
                "here; the old execution is stale by the control plane's decision, "
                "stopping it and starting attempt %s", run_id, attempt, tracked, attempt,
            )
            runner.abort(run_id, reason=f"superseded by attempt {attempt}")
            _cancelled.pop(run_id, None)
        try:
            # W6b: a private run is staged first (sealed blob + one-shot ticket). Any
            # failure here lands in the same handler below and is reported, never silent.
            # A private run has no writable host mount at all, so it can neither
            # keep a checkpoint nor be resumed — private and resume do not combine.
            if a.get("private"):
                extra = _stage_private(server, token, a)
            else:
                # 2026-09-06: a SEALED run takes the ordinary path and adds to it,
                # rather than replacing it the way the old private path did. Same
                # run directory, same three writable folders, same checkpoint
                # staging -- which is exactly why a sealed run can now be resumed on
                # another machine, and a private one never could.
                # Build the run's ONE host directory first, so the checkpoint is
                # staged INSIDE it and the container's three writable folders are
                # ready before it starts. One directory, prepared once, measured as
                # one tree.
                run_dir = prepare_run_dir(run_id, attempt)
                extra = _stage_checkpoint(server, token, a, run_dir)
                extra["run_dir"] = run_dir
                extra["scratch_mb"] = a.get("scratch_mb")
                # 2026-09-05: staged AFTER the run directory is built and cleared, for
                # the same reason the checkpoint is -- `prepare_run_dir` wipes the
                # tree, so anything staged before it would be destroyed.
                if a.get("sealed"):
                    extra.update(_stage_sealed(server, token, a, run_dir))
                elif a.get("has_input"):
                    extra.update(_stage_input(server, token, a, run_dir))
            runner.start(
                run_id, attempt, a["image"], a.get("entrypoint") or [], a.get("env") or {},
                a.get("mem_limit_mb"), **extra,
            )
        except StaleAttempt:
            # 2026-09-07 audit (defect F). A 409 while staging means the control
            # plane has already given this run to a newer attempt. No container is
            # started, and NOTHING is posted: a FAILED post for this attempt would
            # itself be refused with the same 409. The directory prepared above is
            # released; the next attempt builds its own.
            log.warning(
                "run %s attempt %s: the control plane has already moved this run on "
                "(409 while staging); not starting it", run_id, attempt,
            )
            shutil.rmtree(run_dir_path(run_id, attempt), ignore_errors=True)
            continue
        except Exception as exc:  # noqa: BLE001 - any start failure becomes a reported FAILED, never a crash
            log.error("failed to start run %s (%s); reporting FAILED", run_id, exc)
            # W5b: a start failure is a hard fact too (GPU driver / image / entrypoint).
            classified = classify_failure(None, None, exc)
            reason, detail = classified if classified else (None, None)
            result = post_run_status(
                server, token, run_id, attempt, "FAILED", None, reason, detail
            )
            if result == "retry":
                # 2026-09-07 audit (defect B, second site): nothing is tracked for
                # this run, so nothing would ever re-post it. Queued, and flushed at
                # the top of every heartbeat tick until it lands or is fenced.
                _defer_terminal(run_id, attempt, "FAILED", None, reason, detail)
            continue
        if post_run_status(server, token, run_id, attempt, "RUNNING") == "abort":
            runner.abort(run_id)


# Terminal posts that came back 'retry' for runs the runner no longer tracks (a
# start failure has no container to surface again), waiting to be re-posted
# (2026-09-07 audit, defect B). Bounded by age: an entry older than this is dropped
# with a log line, because by then the control plane's reaper has decided the run
# itself and the queue must not grow for ever on a worker cut off for a day.
_deferred_terminal: list[dict] = []
DEFERRED_TERMINAL_MAX_AGE_S = 600.0


def _defer_terminal(
    run_id: str, attempt: int, state: str, exit_code: int | None,
    reason: str | None, detail: str | None,
) -> None:
    log.warning(
        "run %s attempt %s: the %s post was not accepted; queued to retry", run_id,
        attempt, state,
    )
    _deferred_terminal.append({
        "run_id": run_id, "attempt": attempt, "state": state, "exit_code": exit_code,
        "reason": reason, "detail": detail, "queued_at": time.time(),
    })


def _flush_deferred(server: str, token: str) -> None:
    """Re-post every queued terminal status. Dropped on 'accepted' (it landed) and on
    'abort' (fenced: the control plane has moved the run on and will refuse it for
    ever); kept on 'retry'. Called at the top of every heartbeat tick."""
    if not _deferred_terminal:
        return
    now = time.time()
    keep: list[dict] = []
    for entry in _deferred_terminal:
        if now - entry["queued_at"] > DEFERRED_TERMINAL_MAX_AGE_S:
            log.warning(
                "run %s attempt %s: dropping the deferred %s post after %d s without "
                "an answer; the control plane's reaper decides the run from here",
                entry["run_id"], entry["attempt"], entry["state"],
                DEFERRED_TERMINAL_MAX_AGE_S,
            )
            continue
        result = post_run_status(
            server, token, entry["run_id"], entry["attempt"], entry["state"],
            entry["exit_code"], entry["reason"], entry["detail"],
        )
        if result == "retry":
            keep.append(entry)
    _deferred_terminal[:] = keep


def heartbeat_loop(server: str, state: dict, interval: float, runner: Runner, blackbox: BlackBox) -> None:
    url = f"{server}/agent/heartbeat"
    node_id, token = state["node_id"], state["token"]
    while True:
        blackbox.touch(interval=interval)       # black box: detect a sleep gap
        _flush_deferred(server, token)          # terminal posts that were not accepted
        _pump_all_logs(server, token, runner)   # stream output of running containers
        _pump_all_samples(server, token, runner)  # W5b: per-run resource samples
        _scan_all_progress(runner)              # W5b: refresh ##PROGRESS markers
        _upload_checkpoints(server, token, runner)  # save the work, not just the run
        _drain_finished(server, token, runner)  # exited: final flush + terminal status

        body = {
            "node_id": node_id,
            "status": _node_status(runner),
            "running": runner.running_report(),  # carries live progress (W5b)
            # Fresh usage sample each beat — the dashboard's live numbers, plus
            # (2026-09-04) free disk on the filesystem this agent works on. That one
            # field is lifted into a real column server-side because, unlike the rest
            # of the sample, the scheduler reads it: a run that asked for a given
            # amount of temporary disk is not placed on a machine with less free.
            "usage": _usage_with_disk(),
        }
        battery = collect_battery()  # W5b black box — omitted on desktops
        if battery is not None:
            body.update(battery)
        interview = blackbox.pending_interview()  # W5b — facts about a gap we came back from
        if interview is not None:
            body["interview"] = interview

        try:
            resp = _post(url, body, token=token)
        except urllib.error.HTTPError as exc:
            if exc.code == 401:
                log.error("heartbeat rejected 401 (stale token?); re-registering")
                return  # caller re-registers (server reachable — not a partition)
            log.warning("heartbeat HTTP %s; continuing", exc.code)
            time.sleep(interval)
            continue
        except (urllib.error.URLError, OSError) as exc:
            # Server unreachable — buffer this as evidence of a live-but-unreachable gap.
            log.warning("heartbeat failed (%s); continuing", exc)
            blackbox.record_failed_delivery(exc)
            time.sleep(interval)
            continue

        if interview is not None:
            blackbox.clear_interview()  # delivered — don't report the same gap twice
        _act_on_commands(runner, resp.get("commands") or [])
        _act_on_assignments(server, token, runner, resp.get("assignments") or [])
        time.sleep(interval)


# run_id -> True for a run this worker was told to stop. Read by `_drain_finished`,
# so the container's exit is reported as the cancel it was and not as a bare kill.
_cancelled: dict[str, bool] = {}


def _act_on_commands(runner: Runner, commands: list) -> None:
    """Carry out what the control plane asked at this heartbeat (2026-09-07, walk 1
    row 64). One command exists: `cancel` — stop the container of a run this worker
    holds. The run stays tracked so the ordinary exit path flushes its last output
    and posts the terminal status; `_cancelled` is what makes that status say
    CANCELLED rather than KILLED. A run this worker does not hold is ignored."""
    for cmd in commands:
        if not isinstance(cmd, dict) or cmd.get("type") != "cancel":
            continue
        run_id = cmd.get("run_id")
        if not run_id or not runner.has(run_id) or _cancelled.get(run_id):
            continue
        log.info("run %s: cancel requested by the user; stopping its container", run_id)
        _cancelled[run_id] = True
        runner.stop(run_id)


def _sweep_stale_staging(max_age_s: float = 3600.0) -> int:
    """Remove `fyp-sealed-*` staging files older than `max_age_s` from the directory
    `_stage_private` writes them to (2026-09-07 audit, defect C). Returns the count.

    A staging file outlives its run only when the agent process died between writing
    it and the run's cleanup, so anything an hour old belongs to no live run. Same
    directory rule as `_stage_private`: `AGENT_OUTPUT_ROOT` when set, else the
    system temporary directory. Best-effort; a file that cannot be removed is left."""
    where = os.environ.get("AGENT_OUTPUT_ROOT") or tempfile.gettempdir()
    cutoff = time.time() - max_age_s
    swept = 0
    try:
        entries = list(os.scandir(where))
    except OSError as exc:
        log.warning("could not scan %s for stale staging files (%s)", where, exc)
        return 0
    for entry in entries:
        if not entry.name.startswith("fyp-sealed-"):
            continue
        try:
            if not entry.is_file(follow_symlinks=False):
                continue
            if entry.stat(follow_symlinks=False).st_mtime > cutoff:
                continue
            os.remove(entry.path)
            swept += 1
        except OSError:
            continue
    return swept


def build_parser() -> argparse.ArgumentParser:
    """The agent's command line, built here so its defaults can be read by a test
    rather than retyped into one."""
    parser = argparse.ArgumentParser(
        description=(
            "The worker agent: registers this machine with the control plane, "
            "heartbeats, and runs the jobs it is handed in Docker containers."
        ),
        epilog=(
            "Needs Docker running on this machine. Keeps its identity (node id and "
            "token) in agent_state-<name>.json in the current directory, so the same "
            "--name started again from the same directory is the same worker; set "
            "AGENT_STATE_FILE to put it elsewhere. Several workers on one machine: "
            "give each its own --name. Heartbeat every HEARTBEAT_INTERVAL_S seconds "
            "(default 3). Stop it with Ctrl+C; it says goodbye to the control plane."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--server",
        default=os.environ.get("SERVER_URL", "https://localhost:8000"),
        help="control-plane base URL",
    )
    parser.add_argument(
        "--name",
        default=os.environ.get("AGENT_NAME") or socket.gethostname(),
        help="node display name (defaults to hostname)",
    )
    parser.add_argument(
        "--ca-cert",
        default=os.environ.get(CA_CERT_ENV, ""),
        help=(
            "PEM certificate of the authority that signed the control plane's "
            "certificate. Required when --server is https. Generate one with "
            "scripts/make_certs.py."
        ),
    )
    return parser


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )

    # One identity file per worker name (walk 1, rows 8, 17, 51). Set before anything
    # reads or writes state, including the black box's session file beside it.
    global STATE_FILE
    STATE_FILE = state_file_for(args.name)
    log.info("identity file: %s", STATE_FILE)

    # TLS, and the two mismatches that are worth refusing rather than warning about.
    #
    # An https server with no authority configured cannot be verified, and an agent
    # that carried on regardless would be trusting whoever answered. Refusing is the
    # whole point of the no-insecure-switch rule in agent/tls.py.
    #
    # The reverse pairing -- an authority configured against a plain http server --
    # is refused too, and for a sharper reason: it does not fail. The connection
    # succeeds, unencrypted, while the operator believes the certificate they
    # configured is doing something. A misconfiguration that looks like success is
    # worse than one that stops.
    is_https = args.server.lower().startswith("https://")
    if is_https and not args.ca_cert:
        log.error(
            "--server is https but no CA certificate is set. Pass --ca-cert "
            "(or %s) so the control plane can be verified. Generate one with "
            "scripts/make_certs.py.",
            CA_CERT_ENV,
        )
        raise SystemExit(2)
    if args.ca_cert and not is_https:
        log.error(
            "a CA certificate is set but --server is %s, which is not encrypted. "
            "Point --server at https://, or unset --ca-cert.",
            args.server,
        )
        raise SystemExit(2)
    try:
        configure_tls(read_ca_pem(args.ca_cert or None))
    except CaCertError as exc:
        log.error("%s", exc)
        raise SystemExit(2) from exc
    if is_https:
        log.info("TLS on: verifying the control plane against %s", args.ca_cert)

    interval = float(os.environ.get("HEARTBEAT_INTERVAL_S", "3"))

    # The agent's whole job in W2 is to run containers — fail fast and clearly if
    # the Docker daemon isn't reachable, rather than silently registering a node
    # that can never execute anything.
    try:
        runner = Runner()
    except DockerException as exc:
        log.error(
            "Docker is not reachable (%s). Start Docker Desktop / the daemon and retry.",
            exc,
        )
        raise SystemExit(2) from exc

    hw = collect_hw_specs(docker_version=runner.docker_version())
    specs = detect_specs(hw_specs=hw)
    # W5c: a job with no memory cap is capped at THIS node's declared RAM, so an
    # over-budget run dies a clean, provable OOM at the node's capacity (which the
    # control plane escalates on) instead of thrashing the host.
    runner.default_mem_limit_mb = specs["ram_mb"]
    # W6: the display name is injected into every container as FYP_NODE_NAME, so a
    # run's metrics.json records which machine produced it (visible in the results view).
    runner.node_name = args.name
    # 2026-09-07 audit (defect C): before registering, remove whatever a previous
    # agent process on this node left behind -- containers still running under this
    # node's label, and sealed staging files nobody will ever clean up.
    orphans = runner.reconcile_orphans()
    swept = _sweep_stale_staging()
    log.info(
        "startup reconciliation: %d orphaned container(s) removed, %d stale staging "
        "file(s) removed", len(orphans), swept,
    )
    log.info("agent %s starting; server=%s specs=%s", AGENT_VERSION, args.server, specs)

    # W5b black box: reads the previous session file (if any) to build the comeback
    # interview, then records this session. One instance for the whole agent life.
    blackbox = BlackBox(STATE_FILE)
    _shutdown["blackbox"] = blackbox
    _shutdown["server"] = args.server
    atexit.register(_say_goodbye)
    # SIGINT (Ctrl+C) + SIGTERM everywhere; SIGBREAK (Ctrl+Break / console close)
    # on Windows — the common "clean stop" gestures for a worker window.
    _sigs = [signal.SIGINT, signal.SIGTERM]
    if hasattr(signal, "SIGBREAK"):
        _sigs.append(signal.SIGBREAK)
    for sig in _sigs:
        try:
            signal.signal(sig, _on_signal)
        except (ValueError, OSError, AttributeError):
            pass  # not the main thread / not supported on this OS — atexit still covers it

    # Re-register loop: register, heartbeat until token rejected, repeat.
    while True:
        state = _load_state()
        if not state or state.get("name") != args.name:
            state = register(args.server, args.name, specs)
        _shutdown["token"] = state["token"]  # so a clean stop can say goodbye
        log.info("registered as node_id=%s (name=%s)", state["node_id"], state["name"])
        heartbeat_loop(args.server, state, interval, runner, blackbox)
        # heartbeat_loop returns only on 401 -> drop cached state and re-register.
        try:
            os.remove(STATE_FILE)
        except OSError:
            pass


if __name__ == "__main__":
    main()
