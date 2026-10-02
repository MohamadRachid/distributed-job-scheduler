"""The network-partition test — the case the fencing counter exists for.

WHY THIS SCRIPT EXISTS, AND WHAT IT PROVES THAT THE OTHERS DO NOT
-----------------------------------------------------------------
Two proofs of failure recovery already exist and neither exercises the scenario
the attempt counter was designed for.

  * `scripts/chaos_test.py` drives the real handlers against a real database and
    forces the lease to expire by ADVANCING A CLOCK. No worker is running, so
    nothing is alive to be stale.
  * the live kill test stops the agent process. A dead process cannot report, so
    what it proves is that the platform recovers — not that a late report from a
    machine which never stopped is refused.

A jury asks the obvious question: *your counter exists for a worker that is alive
but cut off; you have only ever tested a worker that died.* This script answers it.

It builds the one case neither of the others reaches: an agent that STAYS ALIVE,
whose container KEEPS RUNNING, which simply cannot reach the control plane for
longer than the lease. The server gives its run up, another machine finishes it,
and then the link comes back and the first machine tries to report work the server
has already replaced.

WHAT AN OWNERSHIP CHECK COULD NOT DO HERE
-----------------------------------------
This is the distinction that makes the counter worth having. When the link comes
back, agent 1 is still the machine that legitimately claimed the run, and it is
still alive to argue the point. What refuses it is not who it is but WHEN it is:
the run has moved from attempt 1 to attempt 2, and agent 1's message is stamped 1.

THE SEQUENCE
------------
  1. agent 1 registers and claims an untargeted run of the dummy workload.
  2. agent 2 registers and sits idle.
  3. THE CUT — agent 1's route to the control plane is blackholed. Its process is
     untouched and its container is untouched; only the path between them dies.
  4. no heartbeat arrives, so no lease is renewed; the reaper marks the run LOST
     and requeues it, with the attempt STILL 1 (the reaper never bumps it).
  5. agent 2 claims the requeued run. THE CLAIM bumps the attempt to 2. It runs it
     to completion, and that is the accepted result.
  6. throughout, this script samples the two facts that make the test what it is:
     agent 1's process is alive, and agent 1's container is still running.
  7. THE HEAL — the route is restored. Agent 1's next message about the run
     carries attempt 1 and is refused `409`. The agent stops and discards the run.
  8. assertions: exactly one accepted result, it is agent 2's, the stored attempt
     is 2, and the artifact list shows attempt 2 only.

THE CUT MECHANISM, AND ITS HONEST LIMIT
---------------------------------------
Agent 1 is pointed at a TCP relay on this machine which forwards to the control
plane. Cutting means the relay stops forwarding: it still ACCEPTS the connection
and then answers nothing, which is what a partitioned network does — packets are
dropped, not refused. A refusal would be a different failure (the server is down)
and the agent would learn of it in milliseconds rather than at its timeout.

Chosen over the alternatives on this machine:

  * a Windows Firewall rule needs administrator rights; this needs none;
  * running the agent inside a container and detaching it from the Docker network
    is the most faithful partition of all, but on Windows that agent would still
    need the Docker daemon and a host path shared with the containers it starts,
    and detaching it from the network risks cutting it off from the daemon too —
    a bigger and less honest change than the thing being tested.

The limit, stated plainly because it is the first thing to ask about: this is a
real interruption of the transport between the two processes, and it is not the
failure of a physical network interface. Agent 1's connection to the control plane
genuinely stops carrying data while its process and its container run untouched,
which is the condition the test needs. It is not a claim that a cable was pulled.

RUN IT
------
Docker Desktop up, the stack up, the workload image built, and NO other worker
online (the script refuses to start otherwise, because a stray staged worker could
claim the run and there would be nothing to partition):

    docker compose up -d
    docker build -t fyp-dummy:latest workloads/dummy
    .venv\\Scripts\\python.exe scripts\\chaos_partition.py

It writes its own timeline as it goes, then appends the whole run to the evidence
file under a dated RESULTS heading. It starts the two agents itself and stops them
again, and it leaves the database alone: it wipes nothing, so it is safe to run
beside a staged demo — except that the two nodes it registers stay in the pool
afterwards, because nothing in this platform deletes a node row (a node is its
token, not its name). Run it BEFORE a demo wipe, not after one.
"""

from __future__ import annotations

import argparse
import json
import os
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))
# One definition of how we verify the control plane, imported from the agent
# rather than copied. Two copies of a trust rule drift, and only one of them gets
# tested (the reasoning that fixed scripts/seed_job.py on 25 August).
from agent.tls import context_for, read_ca_pem  # noqa: E402

DEFAULT_USERNAME = os.environ.get("FYP_USERNAME", "admin")
DEFAULT_PASSWORD = os.environ.get("FYP_PASSWORD", "fyp-admin")
TERMINAL = {"SUCCEEDED", "FAILED"}

_CTX = None          # TLS context for THIS script's calls (never through the relay)
_T0 = time.monotonic()
_TRANSCRIPT: list[str] = []


# --- talking to the operator and to the file at the same time ---------------


def say(msg: str) -> None:
    """Every line this script prints is also kept for the evidence file, with the
    seconds since the start on it. A timeline reconstructed afterwards is worth
    much less than one recorded as it happened."""
    line = f"[{time.monotonic() - _T0:7.1f}s] {msg}"
    _TRANSCRIPT.append(line)
    print(line, flush=True)


def die(msg: str) -> None:
    say(f"STOPPED: {msg}")
    raise SystemExit(2)


# --- HTTP (stdlib, like the agent) ------------------------------------------


def _get(url: str, token: str | None = None, timeout: float = 10.0):
    req = urllib.request.Request(url, method="GET")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    with urllib.request.urlopen(req, timeout=timeout, context=_CTX) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _post(url: str, payload: dict, token: str | None = None, timeout: float = 10.0):
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST")
    req.add_header("Content-Type", "application/json")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    with urllib.request.urlopen(req, timeout=timeout, context=_CTX) as resp:
        body = resp.read().decode("utf-8")
        return json.loads(body) if body else {}


def login(server: str, username: str, password: str) -> str:
    """A token the same way the browser gets one. There is no registration
    endpoint; the one admin is bootstrapped by the control plane from its own
    environment."""
    try:
        return _post(
            f"{server}/auth/login", {"username": username, "password": password}
        )["token"]
    except urllib.error.HTTPError as exc:
        if exc.code == 401:
            die(
                "the control plane refused the login. Pass --username/--password, "
                "or set FYP_USERNAME / FYP_PASSWORD to match ADMIN_USERNAME / "
                "ADMIN_PASSWORD in docker-compose.yml."
            )
        raise


# --- the relay: the cut, and the lifting of it ------------------------------


class Relay:
    """A TCP relay that agent 1 talks to instead of the control plane directly.

    Open, it forwards bytes both ways and is invisible: the TLS handshake is end
    to end between the agent and the control plane, so the agent verifies the real
    server certificate exactly as it would without the relay. A certificate names
    hosts, not ports, and the agent still asks for the same hostname — so nothing
    about trust is weakened to make this work.

    Cut, it still ACCEPTS the connection and then does nothing with it. That is the
    point: a partitioned network drops packets, so the sender waits for its timeout
    rather than being told at once that nobody is listening. It also drops the
    connections that were already open, because a link that goes down takes the
    traffic already on it with it.
    """

    def __init__(self, listen_port: int, target_host: str, target_port: int, mode: str):
        self.listen_port = listen_port
        self.target = (target_host, target_port)
        self.mode = mode                      # 'blackhole' (default) or 'refuse'
        self.cut = threading.Event()
        self._stop = threading.Event()
        self._held: list[socket.socket] = []  # accepted-but-never-forwarded, while cut
        self._live: list[socket.socket] = []  # forwarding now; dropped when we cut
        self._lock = threading.Lock()
        self.accepted = 0
        self.blackholed = 0
        self._srv: socket.socket | None = None

    def start(self) -> None:
        self._srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._srv.bind(("127.0.0.1", self.listen_port))
        self._srv.listen(64)
        self._srv.settimeout(0.5)
        threading.Thread(target=self._accept_loop, daemon=True).start()

    def _accept_loop(self) -> None:
        while not self._stop.is_set():
            try:
                client, _addr = self._srv.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            self.accepted += 1
            if self.cut.is_set():
                if self.mode == "refuse":
                    try:
                        client.close()          # the "server is down" failure
                    except OSError:
                        pass
                else:
                    self.blackholed += 1
                    with self._lock:
                        self._held.append(client)  # accepted, and then silence
                continue
            threading.Thread(target=self._pair, args=(client,), daemon=True).start()

    def _pair(self, client: socket.socket) -> None:
        try:
            upstream = socket.create_connection(self.target, timeout=10)
        except OSError:
            try:
                client.close()
            except OSError:
                pass
            return
        with self._lock:
            self._live.extend([client, upstream])
        threading.Thread(target=self._pump, args=(client, upstream), daemon=True).start()
        threading.Thread(target=self._pump, args=(upstream, client), daemon=True).start()

    @staticmethod
    def _pump(src: socket.socket, dst: socket.socket) -> None:
        try:
            while True:
                data = src.recv(65536)
                if not data:
                    break
                dst.sendall(data)
        except OSError:
            pass
        finally:
            for s in (src, dst):
                try:
                    s.close()
                except OSError:
                    pass

    def apply_cut(self) -> None:
        self.cut.set()
        with self._lock:
            live, self._live = self._live, []
        for s in live:
            try:
                s.close()      # the traffic already on the link goes down with it
            except OSError:
                pass

    def heal(self) -> None:
        self.cut.clear()
        with self._lock:
            held, self._held = self._held, []
        for s in held:
            try:
                s.close()      # nothing was ever going to come back on these
            except OSError:
                pass

    def stop(self) -> None:
        self._stop.set()
        with self._lock:
            everything = list(self._held) + list(self._live)
            self._held, self._live = [], []
        for s in everything:
            try:
                s.close()
            except OSError:
                pass
        if self._srv is not None:
            try:
                self._srv.close()
            except OSError:
                pass


# --- docker, read-only ------------------------------------------------------


def docker(*args: str, timeout: float = 30.0) -> tuple[int, str]:
    try:
        p = subprocess.run(
            ["docker", *args], capture_output=True, text=True, timeout=timeout
        )
        return p.returncode, (p.stdout or "") + (p.stderr or "")
    except (OSError, subprocess.TimeoutExpired) as exc:
        return 1, str(exc)


def container_state(name: str) -> dict | None:
    """The container's own State dict, or None if it does not exist. This is the
    observation the whole test turns on: while the cut is in place, agent 1's
    container must still report Running true. If it does not, we have rebuilt the
    kill test and proven nothing new."""
    code, out = docker("inspect", "--format", "{{json .State}}", name)
    if code != 0:
        return None
    try:
        return json.loads(out.strip().splitlines()[-1])
    except (ValueError, IndexError):
        return None


# --- the agents -------------------------------------------------------------


SUPERSEDED = ".superseded-"


def _archive_existing_logs(log_dir: Path) -> list[Path]:
    """Move a previous run's agent logs aside before this run truncates them.

    THE DEFECT THIS CLOSES. The two agent logs are written to fixed names, opened
    with mode "w". So every run of this test silently destroyed the logs of the
    one before it. That is not a stale artefact: this test is the artefact that
    answers the hardest correct attack on the project -- a worker that is ALIVE
    but cut off, which is the case the fencing counter exists for and the one the
    chaos test has never exercised -- and its agent logs are what show agent 1
    still running throughout the cut. On 2026-08-29 a re-run at the new lease
    overwrote the 15-second run's logs, and they were only recovered because they
    happened to be committed.

    THE PATTERN IS BORROWED, NOT INVENTED. harness.py's guard_fresh() already
    settled this argument for measured rows: a timestamped rename, never a delete,
    because evidence someone spent an hour taking is not ours to throw away just
    because it is inconvenient. This is the same rule applied to the same kind of
    file. Nothing new is introduced; guard_fresh itself is experiment-only and
    reaches docs/evidence/experiments/, never this directory.

    Renaming rather than copying is deliberate: a copy leaves the original in
    place for "w" to truncate, which would preserve the bytes and lose the record
    of which run produced them.
    """
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    moved: list[Path] = []
    for existing in sorted(log_dir.glob("agent*.log")):
        # Skip what we archived on an earlier run. `agent*.log` matches
        # `agent1.superseded-....log` too, so without this the archives get
        # re-archived every run and the names grow a second stamp each time --
        # found by running this, not by reading it.
        if SUPERSEDED in existing.name:
            continue
        if not existing.is_file() or existing.stat().st_size == 0:
            continue
        target = existing.with_name(f"{existing.stem}{SUPERSEDED}{stamp}{existing.suffix}")
        # A second run inside the same second would otherwise collide and the
        # rename would fail on Windows, losing the very file we are protecting.
        n = 2
        while target.exists():
            target = existing.with_name(
                f"{existing.stem}{SUPERSEDED}{stamp}-{n}{existing.suffix}"
            )
            n += 1
        existing.rename(target)
        moved.append(target)
    if moved:
        say("  archived %d log(s) from a previous run (renamed, not deleted): %s"
            % (len(moved), ", ".join(p.name for p in moved)))
    return moved


class AgentProc:
    """One agent, launched as a plain child process rather than through a shell.

    Launched directly on purpose. Starting a worker through `Start-Process
    powershell -Command "... python -m agent"` produces TWO processes with the same
    command line, and killing the one you found leaves the other heart-beating. One process here, one handle, and stopping it is
    unambiguous — which matters, because "the agent stayed alive" is a claim this
    test has to be able to make honestly.
    """

    def __init__(self, name: str, server: str, ca_cert: str | None, log_path: Path,
                 python: str, state_file: Path):
        self.name = name
        self.server = server
        self.log_path = log_path
        self.state_file = state_file
        env = dict(os.environ)
        env["AGENT_STATE_FILE"] = str(state_file)
        env.pop("SERVER_URL", None)
        cmd = [python, "-m", "agent", "--server", server, "--name", name]
        if ca_cert:
            cmd += ["--ca-cert", ca_cert]
        self.cmd = cmd
        # "w" truncates. That is safe only because _archive_existing_logs() has
        # already moved any previous run's logs out of the way -- see its docstring.
        self._fh = open(log_path, "w", encoding="utf-8", newline="\n")
        self.proc = subprocess.Popen(
            cmd, cwd=str(_ROOT), env=env,
            stdout=self._fh, stderr=subprocess.STDOUT,
        )

    def alive(self) -> bool:
        return self.proc.poll() is None

    def log_text(self) -> str:
        try:
            return self.log_path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            return ""

    def stop(self) -> None:
        if self.alive():
            try:
                self.proc.terminate()
                self.proc.wait(timeout=15)
            except (OSError, subprocess.TimeoutExpired):
                try:
                    self.proc.kill()
                except OSError:
                    pass
        try:
            self._fh.close()
        except OSError:
            pass


def wait_for_node(server: str, token: str, name: str, deadline_s: float) -> dict | None:
    end = time.monotonic() + deadline_s
    while time.monotonic() < end:
        for n in _get(f"{server}/nodes", token):
            if n["name"] == name and n["online"]:
                return n
        time.sleep(1)
    return None


# --- the run row over time --------------------------------------------------


def run_row(server: str, token: str, job_id: str) -> dict:
    runs = _get(f"{server}/jobs/{job_id}/runs", token)
    return runs[0] if runs else {}


def describe(row: dict, nodes: dict) -> str:
    node = nodes.get(row.get("node_id"), row.get("node_id") or "-")
    return (
        f"status={str(row.get('status')):<10} node={str(node):<12} "
        f"attempt={row.get('attempt')} exit={row.get('exit_code')} "
        f"reason={row.get('failure_reason') or '-'}"
    )


def main(argv: list[str] | None = None) -> int:
    global _CTX
    ap = argparse.ArgumentParser(description="Network-partition test")
    ap.add_argument("--server", default=os.environ.get("SERVER_URL", "https://localhost:8000"))
    ap.add_argument("--ca-cert", default=str(_ROOT / "certs" / "ca.pem"))
    ap.add_argument("--relay-port", type=int, default=8300)
    ap.add_argument("--cut-mode", choices=["blackhole", "refuse"], default="blackhole")
    ap.add_argument("--epochs", type=int, default=20)
    ap.add_argument("--epoch-seconds", type=float, default=4.0)
    ap.add_argument("--cut-after", type=float, default=6.0,
                    help="seconds after the run is RUNNING before the link is cut")
    ap.add_argument("--observe-s", type=float, default=300.0,
                    help="deadline for agent 2 to finish the run while the cut holds")
    ap.add_argument("--refusal-deadline-s", type=float, default=300.0,
                    help="deadline, after healing, for agent 1's stale message to be refused")
    ap.add_argument("--username", default=DEFAULT_USERNAME)
    ap.add_argument("--password", default=DEFAULT_PASSWORD)
    ap.add_argument("--python", default=str(_ROOT / ".venv" / "Scripts" / "python.exe"))
    ap.add_argument("--out", default=str(_ROOT / "docs" / "evidence" / "partition_test_output.txt"))
    ap.add_argument("--keep", action="store_true", help="leave the two agents running at the end")
    args = ap.parse_args(argv)

    server = args.server.rstrip("/")
    is_https = server.lower().startswith("https://")
    ca = args.ca_cert if (is_https and args.ca_cert and Path(args.ca_cert).is_file()) else None
    if is_https and not ca:
        die(f"--server is https but there is no readable CA certificate at "
            f"{args.ca_cert}. Generate one with scripts/make_certs.py.")
    _CTX = context_for(read_ca_pem(ca))

    findings: dict = {"verdict": "not established"}
    relay = None
    a1 = a2 = None
    started_at = datetime.now(timezone.utc)

    try:
        # --- pre-flight: check everything BEFORE launching anything ---------
        # Validate, then act. The staging script learned this the hard way: it
        # used to walk past a dead path, wipe the database, and only then fail.
        say("PRE-FLIGHT")
        py = Path(args.python)
        if not py.is_file():
            die(f"no interpreter at {py}. Pass --python.")
        code, out = docker("version", "--format", "{{.Server.Version}}")
        if code != 0:
            die("the Docker engine is not reachable. Start Docker Desktop and retry.")
        say(f"  docker engine {out.strip().splitlines()[-1]}")
        code, out = docker("images", "--format", "{{.Repository}}:{{.Tag}}")
        if "fyp-dummy:latest" not in out:
            die("fyp-dummy:latest is not built. "
                "Run: docker build -t fyp-dummy:latest workloads/dummy")
        say("  workload image fyp-dummy:latest present")
        try:
            _get(f"{server}/health", timeout=5)
        except Exception as exc:  # noqa: BLE001
            die(f"the control plane at {server} did not answer /health ({exc}).")
        say(f"  control plane answering at {server}")
        token = login(server, args.username, args.password)
        say("  logged in")

        # A stray worker from a staged demo would claim the run and there would be
        # nothing to partition. Refuse rather than produce a quietly meaningless run.
        online = [n["name"] for n in _get(f"{server}/nodes", token) if n["online"]]
        if online:
            die("these workers are online and would compete for the run: "
                + ", ".join(online)
                + ". Stop them (scripts\\stop_demo.ps1) and run this again.")
        say("  no other worker online")

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            if s.connect_ex(("127.0.0.1", args.relay_port)) == 0:
                die(f"port {args.relay_port} is already in use. Pass --relay-port.")
        say(f"  relay port {args.relay_port} free")

        ev_dir = Path(args.out).parent
        ev_dir.mkdir(parents=True, exist_ok=True)
        log_dir = ev_dir / "partition_logs"
        log_dir.mkdir(exist_ok=True)
        _archive_existing_logs(log_dir)

        # --- the relay ------------------------------------------------------
        host = server.split("://", 1)[1].split("/", 1)[0]
        hostname, _, portstr = host.partition(":")
        hostname = hostname or "localhost"
        relay = Relay(args.relay_port, hostname, int(portstr or (443 if is_https else 80)),
                      args.cut_mode)
        relay.start()
        # The agent must still ask for a hostname the certificate covers, so the
        # relay is addressed by the SAME name as the control plane, on another port.
        relay_url = f"{'https' if is_https else 'http'}://{hostname}:{args.relay_port}"
        say(f"RELAY up on {relay_url} -> {server}  (cut mode: {args.cut_mode})")

        # --- agent 1, behind the relay ---------------------------------------
        tmp = Path(os.environ.get("TEMP", str(ev_dir)))
        state1, state2 = tmp / "fyp-partition-a.json", tmp / "fyp-partition-b.json"
        for p in (state1, state2):
            for suffix in ("", ".session"):
                try:
                    Path(str(p) + suffix).unlink()
                except OSError:
                    pass

        a1 = AgentProc("partition-a", relay_url, ca, log_dir / "agent1.log", str(py), state1)
        say("AGENT 1 (partition-a) starting — it reaches the control plane THROUGH the relay")
        n1 = wait_for_node(server, token, "partition-a", 60)
        if not n1:
            die("agent 1 never came online. See " + str(log_dir / "agent1.log"))
        say(f"  agent 1 online, node_id={n1['node_id'][:8]}")

        # --- the job ---------------------------------------------------------
        # UNTARGETED, and that is not a detail. A targeted job pins one run per
        # selected node, and a machine that has already run its share never takes a
        # requeued sibling — so a targeted run is the one run another node can
        # never rescue (found by rehearsing the demo runbook). The
        # recovery this test is about needs an untargeted run.
        job = _post(
            f"{server}/jobs",
            {
                "name": "partition-test",
                "image": "fyp-dummy:latest",
                "entrypoint": ["python", "train.py"],
                "env": {
                    "EPOCHS": str(args.epochs),
                    "EPOCH_SECONDS": str(args.epoch_seconds),
                },
                "replicas": 1,
            },
            token,
        )
        job_id, run_id = job["job_id"], job["run_ids"][0]
        expected_s = args.epochs * args.epoch_seconds
        say(f"JOB submitted: run {run_id[:8]}, about {expected_s:.0f}s of work per attempt")

        nodes = {n1["node_id"]: "partition-a"}
        end = time.monotonic() + 90
        row: dict = {}
        while time.monotonic() < end:
            row = run_row(server, token, job_id)
            if row.get("status") == "RUNNING":
                break
            time.sleep(1)
        if row.get("status") != "RUNNING" or row.get("node_id") != n1["node_id"]:
            die(f"the run did not start on agent 1 ({describe(row, nodes)}).")
        say(f"  {describe(row, nodes)}")
        c1_name = f"fyp-run-{run_id}-1"
        st = container_state(c1_name)
        if not (st and st.get("Running")):
            die(f"agent 1's container {c1_name} is not running; nothing to partition.")
        say(f"  agent 1's container {c1_name} is running")
        findings["run_id"] = run_id

        # --- agent 2, direct -------------------------------------------------
        a2 = AgentProc("partition-b", server, ca, log_dir / "agent2.log", str(py), state2)
        say("AGENT 2 (partition-b) starting — it reaches the control plane DIRECTLY")
        n2 = wait_for_node(server, token, "partition-b", 60)
        if not n2:
            die("agent 2 never came online. See " + str(log_dir / "agent2.log"))
        nodes[n2["node_id"]] = "partition-b"
        say(f"  agent 2 online, node_id={n2['node_id'][:8]}")

        # The lease this run is about to be judged by, READ from the running
        # container rather than assumed from a compose file. It is the number that
        # decides when the server gives the run up, so an evidence file that does not
        # state it cannot be read years later. It matters more since 2026-08-29, when
        # the shipped default moved 15s -> 60s: the same test on the same code takes
        # about 45s longer to reach the fence, and a reader comparing two runs needs
        # to see why.
        code, lease_out = docker("exec", "fyp-control-plane-1", "printenv", "LEASE_TTL_S")
        findings["lease_ttl_s"] = int(lease_out.strip()) if code == 0 and lease_out.strip().isdigit() else None
        say(f"  lease_ttl_s in the running control plane: {findings['lease_ttl_s']}s "
            f"(the run is declared LOST this long after agent 1's last heartbeat)")

        time.sleep(args.cut_after)

        # --- THE CUT ---------------------------------------------------------
        relay.apply_cut()
        cut_at = time.monotonic()
        say("=" * 70)
        say("THE CUT IS IN PLACE. Agent 1 cannot reach the control plane.")
        say("  Its process is untouched. Its container is untouched.")
        say("=" * 70)

        # --- watch, and keep proving agent 1 is alive ------------------------
        timeline: list[str] = []
        alive_samples = 0
        container_running_samples = 0
        c1_exit_at: float | None = None
        last = None
        end = time.monotonic() + args.observe_s
        while time.monotonic() < end:
            row = run_row(server, token, job_id)
            desc = describe(row, nodes)
            if desc != last:
                timeline.append(f"[{time.monotonic() - _T0:7.1f}s] {desc}")
                say("  RUN " + desc)
                last = desc
            if a1.alive():
                alive_samples += 1
            st = container_state(c1_name)
            if st and st.get("Running"):
                container_running_samples += 1
            elif st and c1_exit_at is None:
                c1_exit_at = time.monotonic()
                say(f"  agent 1's container exited (code {st.get('ExitCode')}) "
                    f"{c1_exit_at - cut_at:.0f}s into the cut — its report is now overdue")
            if row.get("status") in TERMINAL and (row.get("attempt") or 0) >= 2:
                break
            time.sleep(1)

        findings["agent1_alive_throughout"] = a1.alive()
        findings["agent1_alive_samples"] = alive_samples
        findings["agent1_container_running_samples"] = container_running_samples
        findings["timeline"] = timeline

        if not a1.alive():
            say("!! agent 1 DIED during the cut. This has become the kill test; "
                "the partition case is NOT proven by this run.")
        if row.get("status") != "SUCCEEDED" or row.get("attempt") != 2:
            say(f"!! the run did not finish on agent 2 at attempt 2 ({describe(row, nodes)}).")

        say(f"AGENT 1 alive at the end of the cut: {a1.alive()} "
            f"({alive_samples} samples with its process alive, "
            f"{container_running_samples} with its container still running)")

        # --- THE HEAL --------------------------------------------------------
        heal_at = time.monotonic()
        relay.heal()
        say("=" * 70)
        say("THE CUT IS LIFTED. Agent 1 can reach the control plane again.")
        say("  Its next message about this run carries attempt 1.")
        say("=" * 70)

        refused_at = None
        end = time.monotonic() + args.refusal_deadline_s
        while time.monotonic() < end:
            if "fenced by control plane" in a1.log_text():
                refused_at = time.monotonic()
                break
            time.sleep(1)

        if refused_at is None:
            say("!! agent 1 was not refused within the deadline. Its log is the record.")
        else:
            findings["seconds_from_heal_to_refusal"] = round(refused_at - heal_at, 1)
            say(f"REFUSED. Agent 1 was fenced {refused_at - heal_at:.1f}s after the link "
                f"came back, and stopped the run itself.")

        # Which door refused it, read from the control plane's own log rather than
        # assumed: the fence is on EVERY agent route (status, logs, samples,
        # artifacts), and the agent aborts on the first one it happens to try.
        code, cp_log = docker("compose", "logs", "--no-color", "--tail", "4000",
                              "control-plane", timeout=60)
        refusals = [ln.strip() for ln in cp_log.splitlines()
                    if " 409 " in ln and run_id[:8] in ln] or \
                   [ln.strip() for ln in cp_log.splitlines() if " 409 " in ln]
        findings["control_plane_409_lines"] = refusals[-20:]
        for ln in refusals[-10:]:
            say("  409 " + ln[-120:])

        # --- the assertions --------------------------------------------------
        final = run_row(server, token, job_id)
        try:
            arts = _get(f"{server}/runs/{run_id}/artifacts", token)
        except Exception:  # noqa: BLE001
            arts = []
        art_attempts = sorted({a.get("attempt") for a in arts}) if arts else []
        findings["final_row"] = final
        findings["artifact_attempts"] = art_attempts
        findings["artifact_count"] = len(arts)

        checks = [
            ("the run finished SUCCEEDED", final.get("status") == "SUCCEEDED"),
            ("the stored result is attempt 2", final.get("attempt") == 2),
            ("the stored result is agent 2's",
             nodes.get(final.get("node_id")) == "partition-b"),
            ("agent 1 stayed alive through the cut",
             bool(findings.get("agent1_alive_throughout"))),
            ("agent 1's container kept running during the cut",
             container_running_samples > 0),
            ("agent 1's stale message was refused", refused_at is not None),
            ("the artifact list shows attempt 2 only", art_attempts in ([2], [])),
        ]
        say("-" * 70)
        for label, ok in checks:
            say(f"  {'PASS' if ok else 'FAIL'}  {label}")
        findings["checks"] = {label: ok for label, ok in checks}
        passed = all(ok for _, ok in checks)
        findings["verdict"] = "PASS" if passed else "FAIL"
        say("-" * 70)
        say(f"VERDICT: {findings['verdict']}")
        return 0 if passed else 1

    except SystemExit:
        raise
    except Exception as exc:  # noqa: BLE001
        say(f"ERROR: {type(exc).__name__}: {exc}")
        findings["verdict"] = "ERROR"
        return 3
    finally:
        # --- clean up after ourselves ---------------------------------------
        if relay is not None:
            relay.heal()
        if not args.keep:
            for a in (a1, a2):
                if a is not None:
                    a.stop()
            say("agents stopped")
        if relay is not None:
            relay.stop()
        # A fenced run's container is removed by the agent itself; anything else
        # this test left behind is removed here.
        if not args.keep:
            code, out = docker("ps", "-aq", "--filter", "name=fyp-run-")
            leftovers = [c for c in out.split() if c]
            if leftovers:
                docker("rm", "-f", *leftovers, timeout=60)
                say(f"removed {len(leftovers)} leftover run container(s)")
        write_results(Path(args.out), started_at, findings, args)


# --- the evidence file ------------------------------------------------------


def write_results(path: Path, started_at: datetime, findings: dict, args) -> None:
    """Append this run under a dated RESULTS heading. Appends — never rewrites —
    so a run that went against us stays on the record beside one that did not."""
    stamp = started_at.strftime("%Y-%m-%d %H:%M:%S UTC")
    lines = [
        "",
        "=" * 78,
        f"RESULTS - run of {stamp}",
        "=" * 78,
        "",
        f"verdict: {findings.get('verdict')}",
        f"cut mechanism: TCP relay on port {args.relay_port}, mode {args.cut_mode}",
        f"workload: {args.epochs} epochs x {args.epoch_seconds}s "
        f"(about {args.epochs * args.epoch_seconds:.0f}s per attempt)",
        f"lease_ttl_s: {findings.get('lease_ttl_s', 'not read')} "
        f"(read from the running control plane, not assumed)",
        f"run id: {findings.get('run_id', '-')}",
        "",
        "-- the two facts that make this a partition and not a kill --",
        f"  agent 1's process alive at the end of the cut  : "
        f"{findings.get('agent1_alive_throughout')}",
        f"  samples with agent 1's process alive           : "
        f"{findings.get('agent1_alive_samples')}",
        f"  samples with agent 1's container still running : "
        f"{findings.get('agent1_container_running_samples')}",
        "",
        "-- the run over time --",
    ]
    lines += ["  " + t for t in findings.get("timeline", [])] or ["  (none recorded)"]
    lines += [
        "",
        "-- the refusal --",
        f"  seconds from the link coming back to the refusal: "
        f"{findings.get('seconds_from_heal_to_refusal', 'not observed')}",
        "  control plane 409 lines (which door refused it):",
    ]
    lines += ["    " + ln for ln in findings.get("control_plane_409_lines", [])] or \
             ["    (none captured)"]
    lines += [
        "",
        "-- what was stored --",
        f"  final run row     : {json.dumps(findings.get('final_row', {}), default=str)}",
        f"  artifact attempts : {findings.get('artifact_attempts')} "
        f"({findings.get('artifact_count')} file(s))",
        "",
        "-- checks --",
    ]
    for label, ok in (findings.get("checks") or {}).items():
        lines.append(f"  {'PASS' if ok else 'FAIL'}  {label}")
    lines += ["", "-- full transcript --"]
    lines += ["  " + t for t in _TRANSCRIPT]
    lines.append("")
    with open(path, "a", encoding="utf-8", newline="\n") as f:
        f.write("\n".join(lines))
    print(f"\nappended to {path}")


if __name__ == "__main__":
    raise SystemExit(main())
