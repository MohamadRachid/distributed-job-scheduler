"""The shared floor under every W7a experiment (brief §4.2).

Why this file exists at all: four people measuring four things will otherwise
invent four ways to reset the database, four ways to time a run, and four output
formats — and the results will not be comparable, which defeats the point. So the
floor is built once, frozen, and every experiment sits on it.

Five rules are enforced HERE rather than left to each script's discipline. The
project's whole safety argument is "in code, not in discipline", and a bench is
not exempt from it: telling four agents "please do not run two experiments at once"
is discipline; a lock that fails is code.

  1. **A mode is asserted, never assumed.** `set_mode` restarts the control plane
     and then reads `GET /health` back. If the mode it finds is not the mode it
     asked for, it raises. A result labelled "fencing off" that was quietly
     measured with fencing on is not a weak result — it is a fabricated one.

  2. **The mode is re-checked when the repetition ends, not only when it starts.**
     `/health` carries a `mode_epoch` that changes whenever the control plane
     restarts. `assert_stable()` compares it against the epoch recorded at assert
     time, so a restart underneath a running measurement voids that repetition
     loudly instead of silently mislabelling it. Same reasoning as the reaper's
     re-check under the lock: verify the thing at the moment you rely on it.

  3. **Only one experiment may touch the stack at a time.** `reset_platform` and
     `set_mode` take an exclusive lock. They truncate tables and restart the
     control plane — two experiments sharing one machine do not merely slow each
     other down, they destroy each other's data. A second holder fails hard and
     names the process holding it.

  4. **Timings come from the server.** `server_times` reads timestamps out of
     Postgres. Nothing here times a run with the host's own clock, because that
     clock also measures our polling loop, the HTTP round trip and Docker's
     start-up jitter, none of which belong in the number.

  5. **Dry runs are quarantined.** Every row written by `record` carries `dry`.
     `summarize` and `chart` drop dry rows before they count anything, so a
     rehearsal can never leak into a published table.

Run it on the HOST (it starts agent processes and drives docker compose), with
the repo venv:

    .venv\\Scripts\\python.exe scripts\\experiments\\e00_smoke.py

Requires Docker Desktop running and the stack up.
"""

from __future__ import annotations

import atexit
import json
import os
import platform
import shutil
import signal
import statistics
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

import requests

# --- Where everything lives -------------------------------------------------

REPO = Path(__file__).resolve().parents[2]

# A campaign must be runnable with ONE command and no environment variable.
# Appendix 1 promises exactly that, and it was not true: launching
# `python scripts/experiments/upload_rate.py` puts the SCRIPT's directory on
# sys.path[0] -- scripts/experiments -- and never the repository root, so
# `from agent import agent` raised ImportError unless the operator had set
# PYTHONPATH by hand. Fixed here rather than in each campaign because every
# campaign imports this module, so the next one written gets it for free; a
# rule that lives in one script does not travel to the next (2026-08-15u).
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

EVIDENCE = REPO / "docs" / "evidence" / "experiments"
# CORRECTED 2026-08-30. This defaulted to http://, and had therefore been unable to
# reach the control plane since the encrypted-transport build of 2026-08-22 — the
# whole W7a harness with it, unnoticed because the last campaign (E2c) ran on
# 11 August, before TLS. Same defect as scripts/seed_job.py had, and the third
# sibling of a correction that was first made in only one place.
#
# VERIFY is a path rather than False: an unverified TLS client hides traffic from a
# listener but not from someone answering in the control plane's place, and that is
# the attack the first invites. Same rule the agent applies.
API = os.environ.get("FYP_API", "https://localhost:8000")
_CA_DEFAULT = str(Path(__file__).resolve().parents[2] / "certs" / "ca.pem")
VERIFY = os.environ.get("AGENT_CA_CERT") or _CA_DEFAULT if API.startswith("https://") else None
PG_CONTAINER = os.environ.get("FYP_PG_CONTAINER", "fyp-postgres-1")
CP_SERVICE = "control-plane"
ADMIN_USER = os.environ.get("FYP_ADMIN_USER", "admin")
ADMIN_PASS = os.environ.get("FYP_ADMIN_PASS", "fyp-admin")

# The tables a reset clears. `users` is deliberately NOT here — wiping it would
# delete the admin the control plane bootstrapped at startup and every later login
# would fail. Same list as scripts/stage_demo.ps1 plus the W6b tables.
_DEMO_TABLES = (
    "nodes", "jobs", "runs", "run_logs", "artifacts",
    "run_samples", "node_events", "job_keys", "key_tickets",
)

EXPERIMENT_DEFAULTS = {"guarantee": "full", "claim": "skip_locked", "reschedule": "learned"}
_ENV_NAMES = {
    "guarantee": "EXPERIMENT_GUARANTEE_MODE",
    "claim": "EXPERIMENT_CLAIM_MODE",
    "reschedule": "EXPERIMENT_RESCHEDULE_MODE",
}
_OVERRIDE = REPO / "docker-compose.experiment.yml"  # written by set_mode, gitignored
_LOCKFILE = REPO / ".experiment.lock"               # gitignored; see take_stack_lock


class HarnessError(RuntimeError):
    """Something is wrong with the bench itself — stop, do not record a number."""


def log(msg: str) -> None:
    print(f"[harness] {msg}", flush=True)


# --- The stack lock ---------------------------------------------------------
#
# Everything below this line assumes it is the only thing driving the platform.
# reset_platform() truncates the tables; set_mode() destroys and recreates the
# control-plane container. Two experiments running at once do not produce two
# slightly noisy results — they produce two ruined ones, and nothing in the output
# would say so. So the bench refuses rather than trusting anyone to remember.

_lock_held = False


def take_stack_lock() -> None:
    """Claim exclusive use of the stack for this process, or fail naming the holder.

    O_CREAT|O_EXCL is the atomic part: creating the file and failing if it already
    exists is a single operation, so two processes racing cannot both win. A lock
    left behind by a crashed run is detected (its pid is gone) and taken over with a
    note, because a stale lockfile that blocks the bench forever would just teach
    everyone to delete it without reading it."""
    global _lock_held
    if _lock_held:
        return
    payload = json.dumps({
        "pid": os.getpid(),
        "script": Path(sys.argv[0]).name if sys.argv and sys.argv[0] else "?",
        "host": platform.node(),
        "acquired_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    })
    try:
        fd = os.open(_LOCKFILE, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(payload)
    except FileExistsError:
        holder = _read_lock()
        if holder and _pid_alive(holder.get("pid")):
            raise HarnessError(
                "the stack is already in use by "
                f"{holder.get('script')} (pid {holder.get('pid')} on "
                f"{holder.get('host')}, since {holder.get('acquired_at')}). "
                "Measurements are strictly one at a time — wait for it to finish, or "
                f"if you are sure it is dead, delete {_LOCKFILE.name}."
            )
        log(f"taking over a stale lock left by pid {holder.get('pid') if holder else '?'}")
        _LOCKFILE.write_text(payload, encoding="utf-8")
    _lock_held = True
    atexit.register(release_stack_lock)


def release_stack_lock() -> None:
    global _lock_held
    if _lock_held:
        _LOCKFILE.unlink(missing_ok=True)
        _lock_held = False


def _read_lock() -> dict | None:
    try:
        return json.loads(_LOCKFILE.read_text(encoding="utf-8"))
    except Exception:                                # noqa: BLE001 - half-written
        return None


def _pid_alive(pid: int | None) -> bool:
    if not pid:
        return False
    if os.name == "nt":
        proc = _run(["tasklist", "/FI", f"PID eq {int(pid)}", "/NH"])
        return str(pid) in proc.stdout
    try:
        os.kill(int(pid), 0)
        return True
    except (ProcessLookupError, ValueError):
        return False
    except PermissionError:
        return True


# --- Shell helpers ----------------------------------------------------------


def _run(cmd: list[str], *, cwd: Path | None = None, timeout: int = 300) -> subprocess.CompletedProcess:
    return subprocess.run(
        cmd, cwd=str(cwd or REPO), capture_output=True, text=True, timeout=timeout
    )


def _venv_python() -> str:
    """The interpreter the agents run under. Prefer the repo venv (it has docker +
    psutil installed); fall back to whatever is running this script."""
    cand = REPO / ".venv" / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    return str(cand) if cand.exists() else sys.executable


# --- Database reads (server-side truth) -------------------------------------


def psql_json(sql: str) -> list[dict]:
    """Run a SELECT inside the Postgres container and return its rows as dicts.

    Going through `docker exec … psql` rather than a Python driver keeps the
    harness dependency-free on the host and makes every query reproducible by hand
    — an examiner can paste the same SQL into pgAdmin and get the same rows.

    The subquery alias is deliberately ugly. `json_agg(x)` means "aggregate the
    ROW x", but if the caller's query happens to have a COLUMN of the same name,
    the column wins and the result comes back as a list of bare values instead of a
    list of objects — so `rows[0]["col"]` explodes with a type error a long way
    from the cause. `SELECT now() AS t` hit exactly that against the old alias `t`.
    A name no caller would write removes the trap for every experiment at once."""
    wrapped = f"SELECT coalesce(json_agg(_hrow), '[]'::json) FROM ({sql}) _hrow"
    proc = _run([
        "docker", "exec", "-i", PG_CONTAINER,
        "psql", "-U", "fyp", "-d", "fyp", "-t", "-A", "-c", wrapped,
    ])
    if proc.returncode != 0:
        raise HarnessError(f"psql failed: {proc.stderr.strip()}")
    return json.loads(proc.stdout.strip() or "[]")


def psql_exec(sql: str) -> None:
    proc = _run([
        "docker", "exec", "-i", PG_CONTAINER, "psql", "-U", "fyp", "-d", "fyp", "-c", sql,
    ])
    if proc.returncode != 0:
        raise HarnessError(f"psql failed: {proc.stderr.strip()}")


def parse_ts(value: str | None) -> datetime | None:
    """Postgres timestamptz -> aware UTC datetime. Naive values (SQLite-shaped) are
    assumed UTC, the same assumption the reaper makes."""
    if not value:
        return None
    dt = datetime.fromisoformat(value)
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _delta_s(a: datetime | None, b: datetime | None) -> float | None:
    """Seconds from a to b, or None if either end is missing. Never guesses."""
    if a is None or b is None:
        return None
    return (b - a).total_seconds()


# --- Auth + HTTP ------------------------------------------------------------

_token: str | None = None


def login(force: bool = False) -> str:
    global _token
    if _token and not force:
        return _token
    resp = requests.post(
        f"{API}/auth/login",
        json={"username": ADMIN_USER, "password": ADMIN_PASS},
        timeout=10,
        verify=VERIFY,
    )
    if resp.status_code != 200:
        raise HarnessError(f"login failed ({resp.status_code}): {resp.text[:200]}")
    _token = resp.json()["token"]
    return _token


def _auth() -> dict[str, str]:
    return {"Authorization": f"Bearer {login()}"}


def session() -> requests.Session:
    """A requests.Session carrying this harness's trust path.

    THE ONE WAY A CAMPAIGN MAKES ITS OWN HTTP CALLS. Every helper above
    passes `verify=VERIFY` explicitly; a campaign that builds a bare
    `requests.Session()` gets requests' own default instead, which is the
    certifi bundle and knows nothing about the authority in certs/ca.pem.
    That is not a warning -- it is a hard CERTIFICATE_VERIFY_FAILED, and it
    is what claim_contention.py hit on 2026-08-30.

    `verify` is a path, never False. An unverified TLS client hides traffic
    from a listener but not from someone answering in the control plane's
    place, and the second is the attack the first invites (2026-08-22f)."""
    s = requests.Session()
    s.verify = VERIFY
    return s


def api_get(path: str, **kw) -> Any:
    resp = requests.get(f"{API}{path}", headers=_auth(), timeout=15, verify=VERIFY, **kw)
    if resp.status_code == 401:                     # token aged out mid-experiment
        login(force=True)
        resp = requests.get(f"{API}{path}", headers=_auth(), timeout=15, verify=VERIFY, **kw)
    resp.raise_for_status()
    return resp.json()


def health(timeout_s: int = 60) -> dict:
    """Poll /health until the control plane answers. Returns the payload, which
    carries `experiment_mode` — the thing set_mode checks."""
    deadline = time.monotonic() + timeout_s
    last = ""
    while time.monotonic() < deadline:
        try:
            resp = requests.get(f"{API}/health", timeout=3, verify=VERIFY)
            if resp.status_code == 200 and resp.json().get("status") == "ok":
                return resp.json()
        except Exception as exc:                     # noqa: BLE001 - it is coming up
            last = str(exc)
        time.sleep(1)
    raise HarnessError(f"control plane never became healthy ({last})")


# --- The three switches -----------------------------------------------------


def current_mode() -> dict[str, str]:
    mode = health().get("experiment_mode")
    if not mode:
        raise HarnessError(
            "/health has no experiment_mode — the control plane is older than W7a. "
            "Rebuild it: docker compose up -d --build control-plane"
        )
    return mode


def mode_epoch() -> str:
    """Which control-plane PROCESS is answering right now. Changes on every restart."""
    epoch = health().get("mode_epoch")
    if not epoch:
        raise HarnessError(
            "/health has no mode_epoch — the control plane predates the Phase 0 "
            "follow-ups. Rebuild it: docker compose up -d --build control-plane"
        )
    return epoch


# What `set_mode` last confirmed. `assert_stable` measures drift against these.
_expected_mode: dict[str, str] | None = None
_expected_epoch: str | None = None
# EVERY override written during this process, not just the current one. An
# experiment always restores the defaults before it writes its manifest, so
# recording only the live override would record `null` every single time — and the
# whole point is that the repo alone can reproduce the weakened arms.
_overrides_used: list[dict] = []


def assert_stable(where: str = "") -> None:
    """Re-check, at the END of a repetition, that we measured what we thought.

    `set_mode` proves the configuration was right when it was read. This proves
    nothing restarted the control plane in between — a manual `docker compose up`,
    a stray `stage_demo.ps1`, a crash and restart — any of which would silently
    re-label the repetition that was in flight. Call it before recording a row; a
    changed epoch means throw the repetition away, and say so out loud."""
    if _expected_epoch is None:
        raise HarnessError("assert_stable() before set_mode() — nothing to compare")
    payload = health()
    got_epoch, got_mode = payload.get("mode_epoch"), payload.get("experiment_mode")
    tail = f" (at {where})" if where else ""
    if got_epoch != _expected_epoch:
        raise HarnessError(
            f"THE CONTROL PLANE RESTARTED MID-REPETITION{tail} — epoch "
            f"{_expected_epoch} -> {got_epoch}. This repetition is VOID: it cannot be "
            "proven to have run under the mode it is labelled with. Discard it and "
            "find out what restarted the stack."
        )
    if got_mode != _expected_mode:
        raise HarnessError(
            f"MODE DRIFTED MID-REPETITION{tail} — expected {_expected_mode}, "
            f"/health now reports {got_mode}. This repetition is VOID."
        )


def set_mode(env: dict[str, str] | None = None, **settings: str) -> dict[str, str]:
    """Restart the control plane under a given experiment mode and PROVE it took.

    Anything not named falls back to the shipped default, so `set_mode()` with no
    arguments always returns the platform to the system we defend.

    `env` carries ordinary TUNABLES that are not experiment switches — today only
    `LEASE_TTL_S`, which E2 raises so the reaper cannot requeue a run mid-round and
    make a legitimate re-claim look like a double assignment, and which E4 will
    sweep on purpose. They are kept out of `**settings` deliberately: the three
    switches weaken a guarantee and are validated as a closed set, while a tunable
    only changes a timing the platform already exposes. Both travel in the same
    generated override, so both land in the manifest and both are asserted after
    the restart.

    The modes are injected through a throwaway compose override rather than being
    written into docker-compose.yml, because the committed compose file must never
    be able to disable one of our own guarantees. The override is
    gitignored — but its exact text is copied into `manifest.json`, or the repo
    alone could not reproduce the run, and reproducibility is this chapter's whole
    claim."""
    global _expected_mode, _expected_epoch
    unknown = set(settings) - set(EXPERIMENT_DEFAULTS)
    if unknown:
        raise HarnessError(f"unknown experiment switch(es): {sorted(unknown)}")
    wanted = {**EXPERIMENT_DEFAULTS, **settings}
    tunables = {k: str(v) for k, v in (env or {}).items()}
    take_stack_lock()

    # A tunable is only "already right" if the container really carries it, so the
    # short-circuit has to check those too — otherwise asking for LEASE_TTL_S=600
    # while the mode happens to match would silently keep the 15s default.
    if wanted == current_mode_or_none() and _tunables_match(tunables):
        log(f"mode already {wanted}" + (f" with {tunables}" if tunables else ""))
        _expected_mode, _expected_epoch = wanted, mode_epoch()
        return wanted

    if wanted == EXPERIMENT_DEFAULTS and not tunables:
        _OVERRIDE.unlink(missing_ok=True)
        files = ["-f", "docker-compose.yml"]
    else:
        lines = [f'      {_ENV_NAMES[k]}: "{v}"' for k, v in sorted(wanted.items())]
        lines += [f'      {k}: "{v}"' for k, v in sorted(tunables.items())]
        text = (
            "# GENERATED by scripts/experiments/harness.py — do not commit, do not\n"
            "# hand-edit. It exists so docker-compose.yml itself never carries a\n"
            "# switch that can weaken our guarantees (W7a brief §4.1).\n"
            "services:\n"
            f"  {CP_SERVICE}:\n"
            "    environment:\n"
            + "\n".join(lines) + "\n"
        )
        _OVERRIDE.write_text(text, encoding="utf-8")
        stamp = {"mode": wanted, "tunables": tunables, "override_yaml": text}
        if not any(o["override_yaml"] == text for o in _overrides_used):
            _overrides_used.append(stamp)
        files = ["-f", "docker-compose.yml", "-f", _OVERRIDE.name]

    log(f"restarting the control plane in mode {wanted}"
        + (f" with {tunables}" if tunables else "") + " …")
    proc = _run(["docker", "compose", *files, "up", "-d", "--force-recreate", CP_SERVICE])
    if proc.returncode != 0:
        raise HarnessError(f"compose up failed: {proc.stderr.strip()[:400]}")

    got = current_mode()
    if got != wanted:
        raise HarnessError(
            f"MODE MISMATCH — asked for {wanted}, /health reports {got}. "
            "Refusing to measure a mislabelled arm."
        )
    # Assert the tunables the same way as the switches: read them back off the
    # running container. An unasserted tunable is exactly the silent mislabelling
    # the mode check exists to stop.
    for key, value in tunables.items():
        actual = _container_env(key)
        if actual != value:
            raise HarnessError(
                f"TUNABLE MISMATCH — asked for {key}={value}, the control plane "
                f"reports {key}={actual!r}. Refusing to measure."
            )
    _expected_mode, _expected_epoch = got, mode_epoch()
    log(f"mode confirmed on /health: {got} (epoch {_expected_epoch})"
        + (f"; tunables confirmed in-container: {tunables}" if tunables else ""))
    return got


def _container_env(key: str) -> str | None:
    """Read one environment variable out of the running control-plane container."""
    proc = _run(["docker", "exec", "fyp-control-plane-1", "printenv", key])
    return proc.stdout.strip() if proc.returncode == 0 else None


def _tunables_match(tunables: dict[str, str]) -> bool:
    return all(_container_env(k) == v for k, v in tunables.items())


def current_mode_or_none() -> dict[str, str] | None:
    try:
        return current_mode()
    except Exception:                                # noqa: BLE001 - it may be down
        return None


# --- Platform reset ---------------------------------------------------------


def reset_platform() -> None:
    """Clean slate: empty the demo tables and drop every agent's saved token, so the
    next `start_agents` registers fresh nodes instead of resurrecting old ones.

    Same reset scripts/stage_demo.ps1 performs before a demo — lifted rather than
    reinvented, so the bench and the demo start from an identical state.

    Takes the stack lock first: this is the single most destructive call in the
    harness, and running it while another experiment is mid-repetition would delete
    that experiment's data with no trace in either result file."""
    take_stack_lock()
    psql_exec(f"TRUNCATE {', '.join(_DEMO_TABLES)} RESTART IDENTITY CASCADE;")
    for path in _agent_state_dir().glob("fyp-exp-*.json*"):
        path.unlink(missing_ok=True)
    log("platform reset: demo tables empty, agent state cleared")


def _agent_state_dir() -> Path:
    return Path(os.environ.get("TEMP") or os.environ.get("TMPDIR") or "/tmp")


# --- Agents -----------------------------------------------------------------


@dataclass
class AgentHandle:
    name: str
    proc: subprocess.Popen
    state_file: Path
    log_path: Path
    _log_fh: Any = None

    @property
    def alive(self) -> bool:
        return self.proc.poll() is None


def start_agents(
    n: int,
    names: list[str] | None = None,
    env: dict[str, str] | None = None,
    log_dir: Path | None = None,
) -> list[AgentHandle]:
    """Start n worker agents as background processes and wait until they register.

    Headless on purpose (the demo script opens visible windows; a measurement must
    not depend on a human closing them). Each agent gets its own state file so they
    register as distinct nodes, and its own log file so a failed run can be
    explained afterwards instead of guessed at."""
    names = names or [f"exp-{chr(ord('a') + i)}" for i in range(n)]
    if len(names) != n:
        raise HarnessError("names must have exactly n entries")
    out_dir = log_dir or (EVIDENCE / "_agent_logs")
    out_dir.mkdir(parents=True, exist_ok=True)

    handles: list[AgentHandle] = []
    for name in names:
        state = _agent_state_dir() / f"fyp-exp-{name}.json"
        state.unlink(missing_ok=True)
        Path(str(state) + ".session").unlink(missing_ok=True)
        proc_env = {**os.environ, "AGENT_STATE_FILE": str(state), **(env or {})}
        log_path = out_dir / f"{name}.log"
        fh = log_path.open("w", encoding="utf-8", errors="replace")
        proc = subprocess.Popen(
            [_venv_python(), "-m", "agent", "--server", API, "--name", name],
            cwd=str(REPO), env=proc_env, stdout=fh, stderr=subprocess.STDOUT,
        )
        handles.append(AgentHandle(name, proc, state, log_path, fh))
    wait_online(len(handles))
    log(f"{len(handles)} agent(s) online: {', '.join(h.name for h in handles)}")
    return handles


def wait_online(count: int, timeout_s: int = 60) -> list[dict]:
    """Block until at least `count` nodes read as online in GET /nodes."""
    deadline = time.monotonic() + timeout_s
    nodes: list[dict] = []
    while time.monotonic() < deadline:
        try:
            nodes = [n for n in api_get("/nodes") if n.get("online")]
            if len(nodes) >= count:
                return nodes
        except Exception:                            # noqa: BLE001 - agents booting
            pass
        time.sleep(1)
    raise HarnessError(f"only {len(nodes)}/{count} agents came online in {timeout_s}s")


def stop_agents(handles: Iterable[AgentHandle]) -> None:
    """Ask each agent to stop, then make sure it did. Graceful first, so the W5b
    goodbye can land; hard kill only if it ignores the ask."""
    for h in handles:
        if h.alive:
            h.proc.terminate()
    for h in handles:
        try:
            h.proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            h.proc.kill()
        if h._log_fh:
            h._log_fh.close()


def kill_agent(handle: AgentHandle) -> None:
    """Pull the plug on one agent — no goodbye, no lease renewal. This is the
    'the machine died' event the recovery experiments are built on, so it must be
    a hard kill: a graceful stop would let the agent explain itself, which is
    precisely the information a dead machine does not get to send."""
    if handle.alive:
        if os.name == "nt":
            handle.proc.kill()
        else:
            handle.proc.send_signal(signal.SIGKILL)
        try:
            handle.proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            pass


# --- Jobs -------------------------------------------------------------------


def ensure_image(tag: str = "fyp-dummy:latest") -> None:
    """Build the dummy workload if it is missing, so a first run on a clean machine
    does not silently measure an image pull."""
    proc = _run(["docker", "images", "--format", "{{.Repository}}:{{.Tag}}"])
    if tag in proc.stdout.split():
        return
    log(f"building {tag} (first time only)")
    built = _run(["docker", "build", "-t", tag, str(REPO / "workloads" / "dummy")], timeout=600)
    if built.returncode != 0:
        raise HarnessError(f"image build failed: {built.stderr.strip()[:400]}")


def submit_job(
    name: str = "exp",
    image: str = "fyp-dummy:latest",
    entrypoint: list[str] | None = None,
    env: dict[str, str] | None = None,
    resource_reqs: dict | None = None,
    target_node_ids: list[str] | None = None,
    replicas: int = 1,
) -> str:
    """POST /jobs and return the job id. Same door the browser uses — no back
    channel — so what we measure is what a user would actually get."""
    body = {
        "name": name,
        "image": image,
        "entrypoint": entrypoint if entrypoint is not None else ["python", "train.py"],
        "env": env or {},
        "resource_reqs": resource_reqs or {},
        "target_node_ids": target_node_ids,
        "replicas": replicas,
    }
    resp = requests.post(f"{API}/jobs", json=body, headers=_auth(), timeout=20, verify=VERIFY)
    if resp.status_code != 200:
        raise HarnessError(f"submit failed ({resp.status_code}): {resp.text[:300]}")
    return resp.json()["job_id"]


_TERMINAL_STATES = ("SUCCEEDED", "FAILED")


def wait_for(
    job_id: str,
    states: Iterable[str] = _TERMINAL_STATES,
    timeout_s: int = 180,
    poll_s: float = 0.25,
) -> dict:
    """Poll until every run of the job sits in one of `states`, or time out.

    Returns `{"reached": bool, "runs": [...], "assigned_at": {run_id: {...}}}`.

    `assigned_at` is collected here because the platform does not store an
    "assigned at" column, and adding one would be a schema change outside this
    increment. While a run is ASSIGNED, the server-set `lease_expires_at` minus the
    lease TTL in force IS the moment the server claimed it — so the value is still
    the server's, computed from the server's own row.

    It is the ONLY timestamp in this harness that is derived rather than read, so it
    is labelled as such in every row it appears in: each entry is
    `{"at": iso, "lease_ttl_s": N, "derived": True}`. The TTL is captured per call
    rather than from a module constant, because E4 sweeps the lease TTL — a global
    constant would silently shift every dispatch number in that whole experiment. Expect the red-team pass to attack this derivation; it is the
    weakest link in E0.3 and it should be named as such."""
    want = set(states)
    lease_ttl = _lease_ttl_s()
    assigned: dict[str, dict] = {}
    deadline = time.monotonic() + timeout_s
    runs: list[dict] = []
    while time.monotonic() < deadline:
        runs = api_get(f"/jobs/{job_id}/runs")
        for row in psql_json(
            f"SELECT id, status::text AS status, lease_expires_at FROM runs "
            f"WHERE job_id = '{job_id}'"
        ):
            if row["id"] in assigned or row["status"] != "ASSIGNED":
                continue
            lease_end = parse_ts(row["lease_expires_at"])
            if lease_end is not None:
                assigned[row["id"]] = {
                    "at": (lease_end - timedelta(seconds=lease_ttl)).isoformat(),
                    "lease_ttl_s": lease_ttl,
                    "derived": True,
                }
        if runs and all(r["status"] in want for r in runs):
            return {"reached": True, "runs": runs, "assigned_at": assigned}
        time.sleep(poll_s)
    return {"reached": False, "runs": runs, "assigned_at": assigned}


# The lease TTL the platform SHIPS with, as opposed to whatever a campaign
# overrides it to. Defined once, here, because four experiment scripts record it in
# their manifests and five hard-coded copies of a number that has now moved once is
# five chances to publish a stale one. The authority is
# `control-plane/app/config.py::Settings.lease_ttl_s`; this is a mirror of it for
# scripts that run OUTSIDE the container and cannot import the app's settings.
# Raised 15 -> 60 on 2026-08-29.
#
# Manifests already written on disk record 15 and are CORRECT for their date: every
# published campaign ran when the shipped default was 15. They are evidence and are
# never edited.
SHIPPED_LEASE_TTL_S = 60


def _lease_ttl_s() -> int:
    """The lease TTL the CONTROL PLANE is actually running with — read out of the
    container, not assumed from the compose file, because `assigned_at` is derived
    from it and a stale assumption would silently shift every dispatch number.

    Read fresh on every call, never cached: E4 changes this value between arms."""
    proc = _run(["docker", "exec", "fyp-control-plane-1", "printenv", "LEASE_TTL_S"])
    if proc.returncode == 0 and proc.stdout.strip().isdigit():
        return int(proc.stdout.strip())
    return int(os.environ.get("LEASE_TTL_S", str(SHIPPED_LEASE_TTL_S)))


def _lease_ttl_s_used() -> int | list[int]:
    """The lease TTL this RUN measured under.

    Prefers the value carried by the compose overrides `set_mode` actually applied,
    because that one was asserted in the running container before any repetition
    started. Falls back to a live read when no override touched the TTL, which is
    the ordinary case (E0, E1, E5 all run on the shipped default).

    Returns a sorted list if a single run genuinely used more than one TTL — E4
    sweeps it on purpose, and collapsing that to one number would be a quiet lie."""
    seen = {
        int(o["tunables"]["LEASE_TTL_S"])
        for o in _overrides_used
        if "LEASE_TTL_S" in o.get("tunables", {})
    }
    if not seen:
        return _lease_ttl_s()
    return seen.pop() if len(seen) == 1 else sorted(seen)


def server_times(run_id: str) -> dict:
    """Every server-side fact about one run, straight out of Postgres.

    Nothing in here was measured by this process. `created_at` is stamped when the
    run row is written, `started_at` when the control plane accepts the agent's
    RUNNING report, `finished_at` when it accepts the terminal one, and the log
    timestamps when each chunk is stored. That is what makes these numbers
    defensible: an examiner can read the same rows in pgAdmin."""
    rows = psql_json(
        "SELECT r.id, r.job_id, r.node_id, r.status::text AS status, r.attempt, "
        "r.exit_code, r.failure_reason, r.failure_detail, "
        "r.learned_min_ram_mb, r.escalation_count, "
        "r.retries_remaining, r.created_at, r.started_at, r.finished_at, "
        "r.lease_expires_at, "
        "(SELECT min(ts) FROM run_logs l WHERE l.run_id = r.id) AS first_log_ts, "
        "(SELECT max(ts) FROM run_logs l WHERE l.run_id = r.id) AS last_log_ts, "
        "(SELECT count(*) FROM run_logs l WHERE l.run_id = r.id) AS log_rows "
        f"FROM runs r WHERE r.id = '{run_id}'"
    )
    if not rows:
        raise HarnessError(f"unknown run {run_id}")
    row = rows[0]
    created, started = parse_ts(row["created_at"]), parse_ts(row["started_at"])
    finished = parse_ts(row["finished_at"])
    row["queue_to_running_s"] = _delta_s(created, started)
    row["running_to_finish_s"] = _delta_s(started, finished)
    row["total_s"] = _delta_s(created, finished)
    return row


def reaper_events(since: datetime | None = None) -> list[dict]:
    """The reaper's own LOST lines, with Docker's server-side timestamps.

    The transition itself leaves no timestamp column behind (the row simply becomes
    PENDING again), so the honest source for "when did the platform notice" is the
    control plane's own log, stamped by the container it was written in — not by
    this script."""
    cmd = ["docker", "compose", "logs", "--no-color", "--timestamps", CP_SERVICE]
    if since:
        cmd += ["--since", since.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")]
    proc = _run(cmd)
    events: list[dict] = []
    for line in proc.stdout.splitlines():
        if "LOST run=" not in line:
            continue
        # compose prints "<service> | <RFC3339 stamp> <the log line>"; take the
        # first token that parses as a timestamp rather than trusting the column.
        stamp = None
        for token in line.replace("|", " ").split():
            if token[:4].isdigit() and "T" in token:
                stamp = token
                break
        fields = dict(
            part.split("=", 1)
            for part in line.split("LOST ", 1)[1].split()
            if "=" in part
        )
        events.append({"ts": stamp or "«MISSING»", **fields})
    return events


# --- Results ----------------------------------------------------------------


def exp_dir(exp: str) -> Path:
    path = EVIDENCE / exp
    path.mkdir(parents=True, exist_ok=True)
    return path


def record(exp: str, arm: str, rep: int, metrics: dict, dry: bool = False) -> dict:
    """Append one repetition to `<exp>/raw.jsonl`. This is the ONLY way a number
    enters the campaign: no figure may appear in a table, a chart or the report
    without a row here to point at (brief §3 rule 5)."""
    row = {
        "exp": exp,
        "arm": arm,
        "rep": rep,
        "dry": bool(dry),
        "ts": datetime.now(timezone.utc).isoformat(),
        # Whether the code that produced THIS row is the code the manifest's
        # git_sha points at. A row taken on a modified tree cannot be reproduced
        # from the commit, and that has to travel with the row itself — a manifest
        # describes a whole run, but rows get filtered, merged and quoted one at a
        # time, and the caveat must survive that.
        "git_dirty": bool(git_dirty_files()),
        **metrics,
    }
    with (exp_dir(exp) / "raw.jsonl").open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(row, default=str) + "\n")
    return row


def guard_fresh(exp: str, fresh: bool = False) -> Path | None:
    """Refuse to append measured rows onto an existing `raw.jsonl`, unless asked.

    `record` appends, which is right during one run and wrong across two: a second
    run silently mixes its rows in with the first, repeating `rep` numbers, and the
    summary then averages two different machine states into one table that looks
    perfectly ordinary. Nothing downstream can detect it afterwards.

    So an experiment stops if measured rows are already there. `--fresh` ARCHIVES
    the old file under a timestamped name and starts clean — it never deletes it,
    because a measurement someone spent an hour taking is evidence, and evidence
    is not ours to throw away just because it is inconvenient. Dry rows do not
    trigger the guard: rehearsals are meant to be disposable.

    Returns the archived path, or None if there was nothing to archive."""
    warn_if_dirty()
    raw = exp_dir(exp) / "raw.jsonl"
    if not raw.exists():
        return None
    measured = len(load_raw(exp))
    if measured == 0:
        return None
    if not fresh:
        raise HarnessError(
            f"{raw.relative_to(REPO)} already holds {measured} measured row(s). "
            "Appending would blend two runs into one summary and nothing later "
            "could tell them apart. Re-run with --fresh to archive the old file "
            "and start clean (it is renamed, never deleted)."
        )
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    archived = raw.with_name(f"raw.superseded-{stamp}.jsonl")
    raw.rename(archived)
    log(f"archived {measured} measured row(s) -> {archived.name} (not deleted)")
    return archived


# The paths whose contents can change a measured number: the platform, the
# worker, the container under test, the harness itself, and the compose file that
# wires them together. Docs, the web UI and the results tree are deliberately
# absent — see git_dirty_files.
_BEHAVIOUR_PATHS = (
    "control-plane",
    "agent",
    "workloads",
    "scripts",
    "docker-compose.yml",
)

_dirty_cache: list[str] | None = None


def git_dirty_files() -> list[str]:
    """Which files differ from HEAD — computed ONCE per process, then cached.

    Only the paths that can CHANGE A NUMBER are watched (`_BEHAVIOUR_PATHS`), and
    the narrowness is the whole point rather than a tidiness preference. Two wider
    definitions were tried and both are useless:

      - everything: an experiment writes its own `raw.jsonl` into the results
        tree, so the flag would be true on every run by its own doing;
      - everything except the results: the project notes are edited every session,
        so the flag would be true during the whole
        measurement window and teach the operator to ignore it.

    A warning that always fires is worse than no warning. So the question asked
    here is the narrow one that actually matters: was the code that PRODUCED
    these numbers committed? Editing a report while a benchmark runs does not
    make the benchmark unreproducible; editing the scheduler does.

    The cost of narrowing, stated plainly: a behaviour-affecting file added
    outside these paths would go unwatched. That is a real gap, and the honest
    mitigation is that this list lives next to the paths it names.

    Cached on purpose. `record` stamps every row with this answer, and shelling
    out to git inside a timed repetition would put a subprocess launch in the
    middle of the thing being measured. The tree is not expected to change during
    a run; if someone does edit mid-run, the state the run STARTED under is the
    honest label anyway, because that is the code it began executing."""
    global _dirty_cache
    if _dirty_cache is None:
        out = _git("status", "--porcelain", "--", *_BEHAVIOUR_PATHS)
        _dirty_cache = [] if out in ("", "«MISSING»") else out.splitlines()
    return _dirty_cache


def warn_if_dirty() -> bool:
    """Say out loud, BEFORE the measuring starts, that this run will not be
    reproducible from its commit.

    The flag already travelled in the manifest, but a manifest is written when the
    run is over — an hour too late to act on, and in a file nobody opens while
    working. A measurement whose code is not committed is not evidence, so the
    warning belongs where it can still change what the operator does."""
    dirty = git_dirty_files()
    if not dirty:
        return False
    log(f"!!! WORKING TREE HAS {len(dirty)} UNCOMMITTED CHANGE(S)")
    log("!!! the manifest's git_sha will NOT reproduce this run — commit first")
    for line in dirty[:10]:
        log(f"      {line}")
    if len(dirty) > 10:
        log(f"      ... and {len(dirty) - 10} more")
    return True


def load_raw(exp: str, include_dry: bool = False) -> list[dict]:
    path = exp_dir(exp) / "raw.jsonl"
    if not path.exists():
        return []
    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    return rows if include_dry else [r for r in rows if not r.get("dry")]


# `git_dirty` is a flag, not a measurement. Python makes bool a subclass of int,
# so without this it is auto-detected as a metric and every summary grows a
# "## git_dirty | median 1" table that means nothing. It belongs in the Integrity
# checks block and nowhere else.
_META_KEYS = {"exp", "arm", "rep", "dry", "ts", "note", "git_dirty"}


def summarize(exp: str, metrics: list[str] | None = None, title: str | None = None) -> Path:
    """Write `<exp>/summary.md`: one block per metric, one row per arm, always
    median + min + max + n.

    Never a lone number. A single figure hides whether the machine was steady or
    the result was luck, and the first thing a jury asks about a benchmark is how
    much it moved between runs — so the spread is not an optional extra column,
    it is the point."""
    rows = load_raw(exp)
    dropped = len(load_raw(exp, include_dry=True)) - len(rows)
    out = exp_dir(exp) / "summary.md"
    if not rows:
        out.write_text(f"# {exp}\n\n«MISSING» — no measured (non-dry) rows yet.\n", encoding="utf-8")
        return out

    arms: list[str] = []
    for r in rows:
        if r["arm"] not in arms:
            arms.append(r["arm"])
    if metrics is None:
        metrics = []
        for r in rows:
            for k, v in r.items():
                if k not in _META_KEYS and k not in metrics and isinstance(v, (int, float)):
                    metrics.append(k)

    lines = [f"# {title or exp}", ""]
    lines.append(f"Generated {datetime.now(timezone.utc).isoformat(timespec='seconds')} "
                 f"from `raw.jsonl` ({len(rows)} measured rows"
                 + (f"; {dropped} dry rows excluded" if dropped else "") + ").")
    lines += ["", *_integrity_block(rows), ""]
    for metric in metrics:
        lines += [f"## {metric}", "",
                  "| arm | n | median | min | max |", "|---|---|---|---|---|"]
        for arm in arms:
            vals = [
                r[metric] for r in rows
                if r["arm"] == arm and isinstance(r.get(metric), (int, float))
            ]
            if not vals:
                lines.append(f"| {arm} | 0 | «MISSING» | «MISSING» | «MISSING» |")
                continue
            lines.append(
                f"| {arm} | {len(vals)} | {_fmt(statistics.median(vals))} | "
                f"{_fmt(min(vals))} | {_fmt(max(vals))} |"
            )
        lines.append("")
    out.write_text("\n".join(lines), encoding="utf-8")
    log(f"wrote {out.relative_to(REPO)}")
    return out


def _integrity_block(rows: list[dict]) -> list[str]:
    """The checks that ran, ALWAYS printed — including when they found nothing.

    A check that found nothing and a check that never ran look identical if the
    only evidence is an absent line. `reaper_interference` is the case that matters:
    it flags a repetition where a lease lapsed mid-round, so a legitimate re-claim
    could be miscounted as a double assignment. Reporting it only when non-zero
    would mean the healthy summaries — the ones we actually publish — are the ones
    that never mention it. So a clean run says "0 of N", out loud."""
    out = ["## Integrity checks", ""]
    flagged = [r for r in rows if r.get("reaper_interference")]
    if any("reaper_interference" in r for r in rows):
        out.append(
            f"- `reaper_interference`: **{len(flagged)} of {len(rows)}** repetition(s) "
            "had a lease lapse mid-repetition."
            + ("" if flagged else " None — no repetition was affected.")
        )
        if flagged:
            out.append(
                "  - Affected rows must NOT be read as double assignments without "
                "checking each one: a requeued run may have been legitimately "
                "re-claimed. Investigate before publishing this table."
            )
    else:
        out.append("- `reaper_interference`: not recorded by this experiment.")

    # Same discipline as above: report it when it is clean, not only when it is
    # broken, or the published tables are exactly the ones that never mention it.
    dirty_rows = [r for r in rows if r.get("git_dirty")]
    if any("git_dirty" in r for r in rows):
        out.append(
            f"- `git_dirty`: **{len(dirty_rows)} of {len(rows)}** repetition(s) ran on a "
            "working tree with uncommitted changes."
            + ("" if dirty_rows else " None — every row is reproducible from its commit.")
        )
        if dirty_rows:
            out.append(
                "  - The manifest's `git_sha` does NOT describe the code that produced "
                "those rows. Commit and re-measure before publishing them."
            )
    else:
        out.append("- `git_dirty`: not recorded by these rows (predates the flag).")
    return out


def _fmt(v: float) -> str:
    if isinstance(v, int) or float(v).is_integer():
        return str(int(v))
    return f"{v:.3f}"


def chart(
    exp: str,
    kind: str = "bar",
    metric: str | None = None,
    title: str | None = None,
    arms: list[str] | None = None,
) -> Path:
    """Write `<exp>/chart.png` — one metric, per arm, with the spread drawn as an
    error bar from min to max so the picture cannot claim more precision than the
    data has. Dry rows are excluded, exactly as in `summarize`.

    An arm that recorded NO value for `metric` is left off the chart entirely. It
    used to be drawn as a zero bar, which is a lie of exactly the kind this whole
    campaign exists to avoid: the picture said "this arm measured zero" when the
    truth was "this arm does not measure this at all". It matters whenever one
    experiment holds arms that measure different things — E0's four parts, for
    instance. Pass `arms` to narrow the chart further by hand."""
    out = exp_dir(exp) / "chart.png"
    rows = load_raw(exp)
    if not rows:
        log(f"chart skipped for {exp}: no measured rows")
        return out
    if metric is None:
        for k, v in rows[0].items():
            if k not in _META_KEYS and isinstance(v, (int, float)):
                metric = k
                break
    if metric is None:
        log(f"chart skipped for {exp}: no numeric metric")
        return out
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        (exp_dir(exp) / "chart.MISSING.txt").write_text(
            "«MISSING» matplotlib is not installed on this host.\n"
            "  .venv\\Scripts\\python.exe -m pip install -r scripts/experiments/requirements.txt\n",
            encoding="utf-8",
        )
        log("chart skipped: matplotlib not installed (see chart.MISSING.txt)")
        return out

    candidates: list[str] = []
    for r in rows:
        if r["arm"] not in candidates:
            candidates.append(r["arm"])
    if arms is not None:
        candidates = [a for a in candidates if a in arms]

    plotted, meds, lows, highs = [], [], [], []
    skipped: list[str] = []
    for arm in candidates:
        vals = [
            r[metric] for r in rows
            if r["arm"] == arm and isinstance(r.get(metric), (int, float))
        ]
        if not vals:
            # No data for this metric — leave the arm OFF the chart. Drawing it at
            # zero would assert a measurement that was never taken.
            skipped.append(arm)
            continue
        med = statistics.median(vals)
        plotted.append(arm)
        meds.append(med)
        lows.append(med - min(vals))
        highs.append(max(vals) - med)
    if skipped:
        log(f"chart({exp}, {metric}): omitted arm(s) with no data — {', '.join(skipped)}")
    if not plotted:
        log(f"chart skipped for {exp}: no arm recorded {metric}")
        return out

    fig, ax = plt.subplots(figsize=(6, 3.6), dpi=150)
    if kind == "line":
        ax.errorbar(plotted, meds, yerr=[lows, highs], marker="o", capsize=4)
    else:
        ax.bar(plotted, meds, yerr=[lows, highs], capsize=4, color="#3b6ea5")
    ax.set_ylabel(metric)
    ax.set_title(title or f"{exp} — {metric} (median, min–max over repetitions)", fontsize=9)
    ax.grid(axis="y", alpha=0.3)
    fig.tight_layout()
    fig.savefig(out)
    plt.close(fig)
    log(f"wrote {out.relative_to(REPO)}")
    return out


# --- Manifest ---------------------------------------------------------------


def manifest(
    exp: str,
    *,
    reps: int | None = None,
    modes: list[dict] | None = None,
    machine_idle: bool | None = None,
    mains_power: bool | None = None,
    notes: str = "",
    job_image: str | None = None,
    extra: dict | None = None,
) -> Path:
    """Write `<exp>/manifest.json` — the conditions the numbers were taken under.

    A benchmark without its conditions is an anecdote. Anything the harness cannot
    determine by itself (was the machine idle, was it on mains) is recorded as
    `null` rather than guessed, because a wrong condition is worse than a missing
    one — see the brief's rule about never inventing a value."""
    data = {
        "experiment": exp,
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "git_sha": _git("rev-parse", "HEAD"),
        "git_branch": _git("rev-parse", "--abbrev-ref", "HEAD"),
        "git_dirty": bool(git_dirty_files()),
        # Not just "was it dirty" but WHICH files, so the caveat is actionable a
        # month later when nobody remembers what was in flight.
        "git_dirty_files": git_dirty_files(),
        # Beside the commit SHA on purpose: the SHA says which source was
        # committed, these say which images actually ran. Each carries the
        # reference and the digest. See the note above _image_digest.
        "control_plane_image": _image_identity(_cp_image_ref()),
        "job_image": _image_identity(job_image or JOB_IMAGE),
        "host": {
            "os": f"{platform.system()} {platform.release()}",
            "machine": platform.machine(),
            "python": platform.python_version(),
            "cpu_count": os.cpu_count(),
            "total_ram_mb": _total_ram_mb(),
        },
        "docker_version": _docker_version(),
        "postgres_version": _pg_version(),
        "postgres_isolation": _pg_isolation(),
        # The TTL the RUN used, not the one the container happens to hold now.
        #
        # This header used to be a live read, and E2's manifest therefore says 15
        # while every one of its repetitions ran at 600: an experiment restores the
        # shipped defaults in its `finally` before it writes the manifest, so the
        # live read always samples the wrong moment. An override is authoritative
        # when one exists — it is the value that was asserted inside the container
        # before any measuring happened (see set_mode).
        "lease_ttl_s": _lease_ttl_s_used(),
        "modes_used": modes if modes is not None else [current_mode_or_none()],
        "mode_epoch_at_write": health().get("mode_epoch"),
        # The generated compose override is gitignored — it must never be committed
        # and picked up by accident — but then the repo alone could not reproduce
        # this run. So the exact text of EVERY override used travels in the manifest
        # instead, and anyone can recreate any arm from this file.
        # Empty list = the whole run was on the shipped defaults.
        "compose_overrides": _overrides_used,
        "repetitions": reps,
        "conditions": {
            "machine_idle": machine_idle,
            "mains_power": mains_power,
            "other_containers": _other_containers(),
            # Measured, not asserted. `machine_idle` above is a human's claim and
            # stays null when nobody made one; these are read off the machine.
            "machine_state": machine_state(),
        },
        # The pool the control plane actually built, read off the live engine.
        # E2c's top level is bounded by this number, so a reader can see the bound
        # beside the result instead of having to take it from the prose.
        "pool_config": pool_config(),
        "rows": {
            "measured": len(load_raw(exp)),
            "dry_excluded": len(load_raw(exp, include_dry=True)) - len(load_raw(exp)),
        },
        "notes": notes,
        **(extra or {}),
    }
    out = exp_dir(exp) / "manifest.json"
    out.write_text(json.dumps(data, indent=2), encoding="utf-8")
    log(f"wrote {out.relative_to(REPO)}")
    return out


def _git(*args: str) -> str:
    proc = _run(["git", *args])
    return proc.stdout.strip() if proc.returncode == 0 else "«MISSING»"


def _docker_version() -> str:
    proc = _run(["docker", "version", "--format", "{{.Server.Version}}"])
    return proc.stdout.strip() or "«MISSING»"


# --- Image identity ---------------------------------------------------------
#
# `git_sha` answers "which source was committed". It cannot answer "was the
# running container built from that source" — the control plane runs from an image
# that was built at some earlier moment, and nothing in a manifest written before
# today tied the two together. A rebuild between two campaigns moves every number
# and leaves the same git_sha behind it. That is the reproducibility gap Appendix 1
# §A1.4 states, and these two fields are what closes it going forward.
#
# Going FORWARD is the whole scope. The 405 rows already published were measured
# without these fields, so §A1.4's limitation is true of this report's data and is
# deliberately left standing. It changes only after a campaign that actually
# carries a digest has run.
#
# A missing value is «MISSING», never a guess — the same rule as every other
# header here. A digest invented because the daemon was down would be worse than
# no digest at all, because it would look like evidence.

JOB_IMAGE = "fyp-dummy:latest"          # the container under test (see submit_job)
_CP_CONTAINER = os.environ.get("FYP_CP_CONTAINER", "fyp-control-plane-1")


def _image_digest(ref: str) -> str:
    """The image's own content ID, e.g. `sha256:1a2b…`, or «MISSING»."""
    if not ref or ref == "«MISSING»":
        return "«MISSING»"
    proc = _run(["docker", "image", "inspect", "--format", "{{.Id}}", ref])
    return proc.stdout.strip() if proc.returncode == 0 else "«MISSING»"


def _cp_image_ref() -> str:
    """The image the RUNNING control plane was started from.

    Asked of the container rather than assumed from the compose project name: the
    project name comes from the directory, so hardcoding `fyp-control-plane` would
    quietly record the wrong reference on a clone in a differently named folder.
    """
    proc = _run(["docker", "inspect", "--format", "{{.Config.Image}}", _CP_CONTAINER])
    return proc.stdout.strip() if proc.returncode == 0 and proc.stdout.strip() else "«MISSING»"


def _image_identity(ref: str) -> dict[str, str]:
    return {"ref": ref, "digest": _image_digest(ref)}


def _pg_version() -> str:
    try:
        return psql_json("SELECT version() AS v")[0]["v"]
    except Exception:                                # noqa: BLE001
        return "«MISSING»"


def _pg_isolation() -> str:
    """The default isolation level, recorded because the `naive` claim arm's
    behaviour depends on it and an examiner will ask (brief §5, E2)."""
    try:
        # current_setting(), not SHOW: psql_json wraps the query as a subquery and
        # SHOW cannot be used there.
        return psql_json(
            "SELECT current_setting('default_transaction_isolation') AS iso"
        )[0]["iso"]
    except Exception:                                # noqa: BLE001
        return "«MISSING»"


def _other_containers() -> int | None:
    proc = _run(["docker", "ps", "--format", "{{.Names}}"])
    if proc.returncode != 0:
        return None
    ours = {"fyp-postgres-1", "fyp-minio-1", "fyp-control-plane-1", "fyp-pgadmin-1"}
    return len([n for n in proc.stdout.split() if n not in ours])


def _total_ram_mb() -> int | None:
    try:
        import psutil
        return int(psutil.virtual_memory().total / (1024 * 1024))
    except Exception:                                # noqa: BLE001
        return None


# --- Machine state ----------------------------------------------------------

# The window a measurement campaign is allowed to run in.
#
# MIN_FREE_MB was set to 4096 on 2026-08-30, by ruling, against the CAMPAIGN
# FOOTPRINT - what upload_rate.py and claim_contention.py actually need in front
# of them: the compose stack, the dummy container each claimer runs, and the
# harness process itself - and above the 3.7 GB (about 3789 MB) refusal that set
# the old value, so the machine that refusal describes is still refused today.
#
# The old value was 8192 and it was NEVER CALIBRATED. It was written from a
# refusal rather than from a passing run, and no passing run could have informed
# it: machine_state() landed on 2026-08-16, the last campaign before it (E2c) ran
# on 2026-08-11, and so NO manifest in this repository records the memory its
# campaign actually had. 8192 was a round number placed above one bad reading. On
# 2026-08-30 it refused a rebooted machine running nothing but Docker at 6407 MB
# available, which is the shape of a gate that refuses every campaign forever -
# and a gate that can never pass is a stop, not a gate.
#
# What did NOT change is why the numbers live here at all. Discipline that
# depends on a human remembering is not discipline, so the shell enforces them
# rather than the operator.
#
# The same lesson as scripts/stage_demo.ps1: validate before you act. There it
# stopped a wipe running against a dead path; here it stops a timing campaign
# running on a machine whose numbers would be quietly wrong rather than loudly
# absent — which is the worse failure, because it looks like evidence.
MIN_FREE_MB = 4096
MAX_UPTIME_H = 3.0


def machine_state() -> dict:
    """What the machine looked like when the campaign started.

    Recorded into the manifest so the conditions travel with the numbers, the way
    the demo re-stage's two conditions travel with its 24.4 s median. Anything that
    cannot be read is `None` rather than guessed."""
    # The key is named for the psutil field it holds. It was `free_mb` until
    # 2026-08-30 and that was a small lie with a real cost: the reading is
    # `.available`, an operator who read "free" went to Task Manager's Free
    # figure, and the two are different numbers. This name travels into every
    # manifest, so it is the one a reader meets long after anyone can ask.
    state: dict[str, Any] = {
        "available_mb": None, "total_mb": None, "uptime_h": None,
        "cpu_idle_pct": None,
    }
    try:
        import psutil
        vm = psutil.virtual_memory()
        state["available_mb"] = int(vm.available / (1024 * 1024))
        state["total_mb"] = int(vm.total / (1024 * 1024))
        state["uptime_h"] = round((time.time() - psutil.boot_time()) / 3600, 2)
        # Sampled over a real interval. cpu_percent() with no interval returns the
        # average since the process started, which on a fresh process is 0.0 and
        # would read as a perfectly idle machine — a wrong value dressed as a good
        # one, which is the class of defect this project keeps writing down.
        state["cpu_idle_pct"] = round(100.0 - psutil.cpu_percent(interval=1.0), 1)
    except Exception:                                # noqa: BLE001
        pass
    return state


def pool_config() -> dict:
    """The connection pool the CONTROL PLANE actually built, read off the live
    engine object inside the running container.

    Read rather than assumed on purpose. E2c's plateau at fifteen waiters was
    attributed to this pool by reading the pinned library's source, and the report
    says in as many words that this is "the mechanism the numbers fit, not one we
    measured separately". Reading the live engine closes half of that gap: the pool
    size stops being an inference from source and becomes an observation. It does
    NOT close the other half — that the plateau is caused by the pool — which still
    needs a run above the ceiling."""
    code = (
        "from app.db import engine;"
        "p=engine.pool;"
        "print(type(p).__name__, p.size(), p._max_overflow)"
    )
    proc = _run(["docker", "compose", "exec", "-T", CP_SERVICE, "python", "-c", code],
                cwd=REPO, timeout=60)
    if proc.returncode != 0:
        return {"impl": "«MISSING»", "pool_size": None, "max_overflow": None, "ceiling": None}
    parts = proc.stdout.strip().split()
    try:
        impl, size, overflow = parts[-3], int(parts[-2]), int(parts[-1])
    except (IndexError, ValueError):
        return {"impl": "«MISSING»", "pool_size": None, "max_overflow": None, "ceiling": None}
    return {
        "impl": impl,
        "pool_size": size,
        "max_overflow": overflow,
        # What actually caps concurrent requests at the database.
        "ceiling": size + overflow,
    }


def require_clean_machine(
    *, min_free_mb: int = MIN_FREE_MB, max_uptime_h: float = MAX_UPTIME_H
) -> dict:
    """Refuse to measure on a machine that is not fit to measure on, and say why.

    Gates on free memory and uptime only. CPU idle is recorded but not gated,
    because a single sample is too noisy to refuse a campaign on and a check that
    fires on honest work teaches the operator to disable it."""
    state = machine_state()
    problems: list[str] = []

    available, uptime = state["available_mb"], state["uptime_h"]
    if available is None or uptime is None:
        problems.append(
            "cannot read machine state (psutil unavailable) - refusing rather than "
            "recording a campaign whose conditions are unknown"
        )
    else:
        if available < min_free_mb:
            problems.append(
                f"available memory {available} MB of {state['total_mb']} MB is below the "
                f"{min_free_mb} MB this campaign requires - reboot, start Docker "
                f"Desktop only, and close the dashboard, editor and browser"
            )
        if uptime > max_uptime_h:
            problems.append(
                f"uptime {uptime} h is above the {max_uptime_h} h ceiling - a long "
                f"session leaves cached and fragmented memory that moves timings "
                f"without showing up as a running process; reboot first"
            )

    if problems:
        for problem in problems:
            print(f"[harness] STOP: {problem}", file=sys.stderr)
        raise SystemExit(1)

    log(f"machine ready: {available} MB available of {state['total_mb']}, "
        f"uptime {uptime} h, cpu idle {state['cpu_idle_pct']}%")
    return state


# --- Preflight --------------------------------------------------------------


@dataclass
class Preflight:
    ok: bool
    problems: list[str] = field(default_factory=list)


def preflight() -> Preflight:
    """Refuse to start on a bench that is not ready. Every check here has already
    cost someone an experiment run at some point in this project."""
    problems: list[str] = []
    if shutil.which("docker") is None:
        problems.append("docker CLI not found on PATH")
    elif _run(["docker", "info"]).returncode != 0:
        problems.append("Docker engine is not running — open Docker Desktop first")
    else:
        try:
            health(timeout_s=10)
        except HarnessError as exc:
            problems.append(f"{exc}; try: docker compose up -d --build")
    return Preflight(ok=not problems, problems=problems)


def require_ready() -> None:
    result = preflight()
    if not result.ok:
        for problem in result.problems:
            print(f"[harness] STOP: {problem}", file=sys.stderr)
        raise SystemExit(1)
