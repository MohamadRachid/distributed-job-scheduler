"""Prove that a user reads only their own work, against the LIVE stack.

Run it from the repo root with the stack up:

    .venv\\Scripts\\python.exe scripts/read_scoping_proof.py --label AFTER

`--label` is printed at the top of the run and nowhere else: it names WHICH TREE
the control plane was serving when the answers below were collected, so the same
script can be pointed at the code before the fix and at the code after it and the
two runs can be read side by side. It changes nothing the script does.

Every line is MACHINE-PRINTED. Each limb is checked in code against values read
back out of the running control plane and reported HELD or WITHDRAWN with what was
actually seen — never with what was hoped for. A limb that prints only "PASS" is an
assertion, not evidence.

It pretends to be an agent rather than starting one: it registers a node,
heartbeats to claim the runs, and posts the same fenced messages a real agent posts
(a log line, a resource sample, a sealed result file, a terminal status). That is
deliberate — what is being proven is a rule the CONTROL PLANE applies to a reader,
and a real Docker container in the middle would add a dependency without adding
evidence. The one thing it cannot fake is the login: both users authenticate with
their own password through `POST /auth/login` and every request below carries the
token that came back.

The database is emptied first (jobs, runs, logs, samples, artifacts, keys, nodes,
and every user except the bootstrap admin), so the counts describe this run and not
the leftovers of a previous one.
"""

from __future__ import annotations

import argparse
import base64
import io
import json
import os
import secrets
import socket
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..",
                                "control-plane"))

CA = os.environ.get("FYP_CA_FILE", "certs/ca.pem")

if os.path.exists(CA):
    API = os.environ.get("FYP_API", "https://localhost:8000")
    _CTX = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    _CTX.verify_mode = ssl.CERT_REQUIRED
    _CTX.check_hostname = True
    _CTX.load_verify_locations(cafile=CA)
else:
    API = os.environ.get("FYP_API", "http://localhost:8000")
    _CTX = None

ADMIN = (os.environ.get("FYP_ADMIN_USER", "admin"),
         os.environ.get("FYP_ADMIN_PASS", "fyp-admin"))

_results: list[tuple[str, bool, str]] = []


def say(msg: str = "") -> None:
    print(msg, flush=True)


def limb(name: str, held: bool, detail: str) -> None:
    _results.append((name, held, detail))
    say(f"  [{'HELD' if held else 'WITHDRAWN'}] {name}: {detail}")


def _open(req, timeout=30.0):
    return (urllib.request.urlopen(req, timeout=timeout, context=_CTX)
            if _CTX else urllib.request.urlopen(req, timeout=timeout))


def call(method: str, path: str, body=None, token=None, form=None, files=None,
         raw_bytes=False):
    """One HTTP call. Returns (status, parsed-body-or-text). An HTTP error is a
    RESULT here, not an exception: half of what is being proven is a refusal."""
    url = f"{API}{path}"
    data = None
    headers = {}
    if files is not None:
        boundary = "----proof" + uuid.uuid4().hex
        buf = io.BytesIO()
        b = boundary.encode()
        for k, v in (form or {}).items():
            buf.write(b"--" + b + b"\r\n")
            buf.write(f'Content-Disposition: form-data; name="{k}"'.encode() + b"\r\n\r\n")
            buf.write(str(v).encode() + b"\r\n")
        for field, (fname, content) in files.items():
            buf.write(b"--" + b + b"\r\n")
            buf.write(
                f'Content-Disposition: form-data; name="{field}"; filename="{fname}"'
                .encode() + b"\r\n"
            )
            buf.write(b"Content-Type: application/octet-stream\r\n\r\n")
            buf.write(content + b"\r\n")
        buf.write(b"--" + b + b"--\r\n")
        data = buf.getvalue()
        headers["Content-Type"] = f"multipart/form-data; boundary={boundary}"
    elif body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    try:
        with _open(req) as resp:
            payload = resp.read()
            if raw_bytes:
                return resp.status, payload
            text = payload.decode("utf-8", "replace")
            if not text:
                return resp.status, None
            try:
                return resp.status, json.loads(text)
            except ValueError:
                # A result FILE comes back as its own bytes, not as JSON.
                return resp.status, text
    except urllib.error.HTTPError as exc:
        text = exc.read().decode("utf-8", "replace")
        try:
            return exc.code, json.loads(text)
        except ValueError:
            return exc.code, text


def detail_of(body) -> str:
    """The `detail` out of a refusal body, for the "does it say the id exists" check."""
    if isinstance(body, dict):
        return str(body.get("detail", ""))
    return str(body)


def login(username: str, password: str) -> str:
    status, body = call("POST", "/auth/login",
                        {"username": username, "password": password})
    if status != 200:
        raise SystemExit(f"cannot log in as {username}: {status} {body}")
    return body["token"]


def wipe() -> None:
    """Empty the database of everything this proof counts. The bootstrap admin
    survives — it is created from the deployment's environment at startup and is not
    re-made on demand — and every user it ever created does not."""
    sql = (
        "TRUNCATE run_logs, run_samples, run_log_archives, artifacts, key_tickets, "
        "job_keys, runs, jobs, node_events, nodes CASCADE; "
        "DELETE FROM users WHERE is_admin = false;"
    )
    rc = os.system(
        f'docker compose exec -T postgres psql -q -U fyp -d fyp -c "{sql}"'
    )
    say(f"  database emptied (psql exit={rc})")


# --- pretending to be an agent -----------------------------------------------------


def register_node(name: str) -> dict:
    status, body = call("POST", "/agent/register", {
        "name": name,
        "specs": {"cpu_cores": 4, "has_gpu": False, "ram_mb": 8192,
                  "capacity": 4, "agent_version": "0.12.0"},
    })
    if status != 200:
        raise SystemExit(f"register failed: {status} {body}")
    return body


def beat(node: dict) -> list:
    status, body = call("POST", "/agent/heartbeat", {
        "node_id": node["node_id"], "status": "idle", "running": [],
        "usage": {"cpu_pct": 1.0},
    }, token=node["token"])
    if status != 200:
        raise SystemExit(f"heartbeat failed: {status} {body}")
    return body["assignments"]


def seal_like_a_container(job_id: str, admin: str, plaintext: bytes) -> bytes:
    """A sealed job's results are sealed INSIDE the container before they leave it,
    and the control plane refuses an artefact that is not (api/artifacts). This proof
    stands in for the container, so it seals the same way one does, with the job's own
    key read out of the database — which is also what keeps the download limb honest
    below: A gets plaintext back because the seal genuinely opens."""
    from app.sealing import key_from_b64, seal_stream  # noqa: PLC0415

    out = os.popen(
        "docker compose exec -T postgres psql -tA -U fyp -d fyp -c "
        f"\"SELECT key_b64 FROM job_keys WHERE job_id = '{job_id}';\""
    ).read().strip()
    if not out:
        return plaintext
    return seal_stream(plaintext, key_from_b64(out))


def finished_job(node: dict, token: str, admin: str, who: str) -> dict:
    """Submit one job as `who`, run it to SUCCEEDED as the node, and leave behind a
    log line, a resource sample and a result file — so every scoped route has
    something real to refuse."""
    spec = json.dumps({
        "name": f"{who}-job", "image": "fyp-dummy:latest",
        "entrypoint": ["python", "train.py"], "replicas": 1,
    })
    status, made = call(
        "POST", "/jobs/with-input", token=token,
        form={"spec": spec},
        files={"file": (f"{who}-data.csv", f"secret rows belonging to {who}".encode())},
    )
    if status != 200:
        raise SystemExit(f"{who} could not submit: {status} {made}")
    job_id = made["job_id"]

    assignments = beat(node)
    if not assignments:
        raise SystemExit(f"nothing was assigned for {who}'s job")
    a = assignments[0]
    run_id, attempt = a["run_id"], a["attempt"]

    call("POST", f"/agent/runs/{run_id}/logs", token=node["token"],
         body={"attempt": attempt, "seq": 0, "chunk": f"{who}: loss=0.01\n"})
    call("POST", f"/agent/runs/{run_id}/samples", token=node["token"],
         body={"attempt": attempt, "samples": [
             {"ts": time.time(), "cpu_pct": 10.0, "mem_used_mb": 64.0,
              "mem_limit_mb": 128.0}]})
    body = seal_like_a_container(job_id, admin, f"{who}'s result".encode())
    status, up = call("POST", f"/agent/runs/{run_id}/artifacts", token=node["token"],
                      form={"attempt": attempt, "filename": "out.txt", "kind": "result"},
                      files={"file": ("out.txt", body)})
    if status != 200:
        raise SystemExit(f"could not upload {who}'s result: {status} {up}")
    call("POST", f"/agent/runs/{run_id}/status", token=node["token"],
         body={"attempt": attempt, "state": "SUCCEEDED", "exit_code": 0})
    return {"job_id": job_id, "run_id": run_id, "artifact_id": up["artifact_id"],
            "input_filename": f"{who}-data.csv"}


def reads(work: dict) -> list[tuple[str, str]]:
    """Every user-facing route that carries this job's data."""
    return [
        ("the job", f"/jobs/{work['job_id']}"),
        ("its runs", f"/jobs/{work['job_id']}/runs"),
        ("its log", f"/runs/{work['run_id']}/logs"),
        ("its resource samples", f"/runs/{work['run_id']}/samples"),
        ("its result listing", f"/runs/{work['run_id']}/artifacts"),
        ("its result FILE", f"/artifacts/{work['artifact_id']}/download"),
    ]


# --- the log socket, by hand (no web-socket library; the agent is stdlib-only) -----


def socket_handshake(run_id: str, token: str) -> str:
    """Open the live-log socket the way the dashboard does and return the HTTP status
    line of the handshake. A refusal before accept is an HTTP status, never a frame,
    which is why this reads the header and stops."""
    parsed = urllib.parse.urlparse(API)
    host = parsed.hostname or "localhost"
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    raw = socket.create_connection((host, port), timeout=30)
    sock = _CTX.wrap_socket(raw, server_hostname=host) if _CTX else raw
    key = base64.b64encode(secrets.token_bytes(16)).decode()
    path = f"/runs/{run_id}/logs?token={urllib.parse.quote(token)}"
    sock.sendall((
        f"GET {path} HTTP/1.1\r\nHost: {host}:{port}\r\n"
        "Upgrade: websocket\r\nConnection: Upgrade\r\n"
        f"Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n"
    ).encode())
    header = b""
    try:
        while b"\r\n\r\n" not in header:
            piece = sock.recv(4096)
            if not piece:
                break
            header += piece
    finally:
        sock.close()
    if not header:
        return "(the server closed the connection without answering)"
    return header.split(b"\r\n", 1)[0].decode("utf-8", "replace")


# ===================================================================================


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--label", default="AFTER",
                    help="which tree the control plane is serving; printed, not acted on")
    args = ap.parse_args()

    say("=" * 78)
    say("A USER READS ONLY THEIR OWN WORK — live proof")
    say(f"tree served  : {args.label}")
    say(f"date         : {time.strftime('%Y-%m-%d %H:%M:%S')}")
    say(f"api          : {API}")
    say("every line below is printed by this script from values read back out of")
    say("the running control plane; nothing here is typed by hand")
    say("=" * 78)

    status, health = call("GET", "/health")
    say(f"\ncontrol plane /health -> {status} {json.dumps(health)}")
    admin = login(*ADMIN)

    say("\n" + "-" * 78)
    say("SETUP — two users and one administrator on a fresh database")
    say("-" * 78)
    wipe()
    tag = uuid.uuid4().hex[:6]
    people = {}
    for who in ("alice", "bob"):
        name = f"{who}-{tag}"
        password = f"{name}-pass-123"
        status, made = call("POST", "/users", token=admin,
                            body={"username": name, "password": password,
                                  "tier": "standard"})
        if status != 200:
            raise SystemExit(f"could not create {name}: {status} {made}")
        token = login(name, password)
        call("POST", "/me/accept-limits", token=token)
        people[who] = {"name": name, "token": token, "id": made["user_id"]}
        say(f"  created {name} (id {made['user_id']}) and logged in with their own password")

    node = register_node(f"read-scoping-node-{tag}")
    say(f"  registered one worker: {node['node_id']}")
    work = {}
    for who in ("alice", "bob"):
        work[who] = finished_job(node, people[who]["token"], admin, people[who]["name"])
        say(f"  {people[who]['name']}: job {work[who]['job_id']}")
        say(f"    run {work[who]['run_id']}  result file {work[who]['artifact_id']}")
        say(f"    input file {work[who]['input_filename']}")

    A, B = people["alice"], people["bob"]
    wa, wb = work["alice"], work["bob"]

    # ---------------------------------------------------------------- R1 ----
    say("\n" + "-" * 78)
    say("R1 — the job list carries the caller's own jobs and nobody else's")
    say("-" * 78)
    listings = {}
    for who, person in (("alice", A), ("bob", B), ("admin", {"name": ADMIN[0], "token": admin})):
        status, rows = call("GET", "/jobs", token=person["token"])
        listings[who] = rows
        names = sorted(j["name"] for j in rows)
        say(f"  GET /jobs as {person['name']:>18} -> HTTP {status}, "
            f"{len(rows)} job(s): {names}")
    limb("R1.a A's list holds A's one job and none of B's",
         [j["job_id"] for j in listings["alice"]] == [wa["job_id"]],
         f"{len(listings['alice'])} job(s): {[j['job_id'] for j in listings['alice']]}")
    limb("R1.b B's list holds B's one job and none of A's",
         [j["job_id"] for j in listings["bob"]] == [wb["job_id"]],
         f"{len(listings['bob'])} job(s): {[j['job_id'] for j in listings['bob']]}")
    limb("R1.c the administrator's list holds both",
         {j["job_id"] for j in listings["admin"]} == {wa["job_id"], wb["job_id"]},
         f"{len(listings['admin'])} job(s)")
    limb("R1.d the input filename travels with the row it belongs to",
         all(j.get("input_filename") == wa["input_filename"] for j in listings["alice"]),
         f"A sees input_filename="
         f"{[j.get('input_filename') for j in listings['alice']]}")

    # ---------------------------------------------------------------- R2 ----
    say("\n" + "-" * 78)
    say("R2 — A asks for B's work by its identifier, one route at a time")
    say("-" * 78)
    refusals = []
    for what, path in reads(wb):
        status, body = call("GET", path, token=A["token"])
        refusals.append((what, path, status, detail_of(body)))
        shown = json.dumps(body)[:96] if not isinstance(body, str) else body[:96]
        say(f"  {what:>20}  GET {path}")
        say(f"  {'':>20}  -> HTTP {status}  {shown}")
    limb("R2.a every route carrying B's job data refuses A",
         all(s == 404 for _, _, s, _ in refusals),
         "; ".join(f"{w}={s}" for w, _, s, _ in refusals))

    say("\n  the same requests against identifiers that were never minted:")
    invented = [
        ("the job", "/jobs/no-such-job"),
        ("its runs", "/jobs/no-such-job/runs"),
        ("its log", "/runs/no-such-run/logs"),
        ("its resource samples", "/runs/no-such-run/samples"),
        ("its result listing", "/runs/no-such-run/artifacts"),
        ("its result FILE", "/artifacts/no-such-artifact/download"),
    ]
    same = True
    for (what, path), (_, _, real_status, real_detail) in zip(invented, refusals):
        status, body = call("GET", path, token=A["token"])
        matched = (status == real_status and detail_of(body) == real_detail)
        same = same and matched
        say(f"  {what:>20}  GET {path} -> HTTP {status} {detail_of(body)!r} "
            f"({'identical' if matched else 'DIFFERENT'})")
    limb("R2.b a refusal does not say whether the identifier exists",
         same, "same status and same words for B's identifiers and for invented ones")

    # ---------------------------------------------------------------- R3 ----
    say("\n" + "-" * 78)
    say("R3 — B's INPUT file, at both doors that could reach it")
    say("-" * 78)
    status, job_view = call("GET", f"/jobs/{wb['job_id']}", token=A["token"])
    say(f"  the door that NAMES it: GET /jobs/{wb['job_id']} as A -> HTTP {status}")
    limb("R3.a A cannot see the row that names B's input file", status == 404,
         f"HTTP {status}")
    status, sealed = call("GET", f"/agent/jobs/{wb['job_id']}/input", token=A["token"])
    say(f"  the door that SERVES it: GET /agent/jobs/{wb['job_id']}/input as A "
        f"-> HTTP {status} {json.dumps(sealed) if not isinstance(sealed, str) else sealed[:80]}")
    limb("R3.b the sealed input is behind a node token, and a user's login is not one",
         status == 401, f"HTTP {status}")
    status, ck = call("GET", f"/agent/runs/{wb['run_id']}/checkpoint?attempt=1",
                      token=A["token"])
    limb("R3.c the same is true of the checkpoint door", status == 401, f"HTTP {status}")

    # ---------------------------------------------------------------- R4 ----
    say("\n" + "-" * 78)
    say("R4 — the three routes that DESTROY, aimed at B's job")
    say("-" * 78)
    destructive = [
        ("cancel", "POST", f"/jobs/{wb['job_id']}/cancel"),
        ("crypto-shred", "DELETE", f"/jobs/{wb['job_id']}/key"),
        ("release storage", "DELETE", f"/jobs/{wb['job_id']}/storage"),
    ]
    codes = []
    for what, method, path in destructive:
        status, body = call(method, path, token=A["token"])
        codes.append(status)
        say(f"  {what:>16}  {method} {path} -> HTTP {status} {detail_of(body)!r}")
    limb("R4.a all three refuse A", all(s == 404 for s in codes),
         "; ".join(f"{w}={s}" for (w, _, _), s in zip(destructive, codes)))

    status, still = call("GET", f"/artifacts/{wb['artifact_id']}/download",
                         token=B["token"], raw_bytes=True)
    limb("R4.b B's job is untouched afterwards — the result still opens for B",
         status == 200 and still == f"{B['name']}'s result".encode(),
         f"HTTP {status}, {len(still) if isinstance(still, bytes) else 0} byte(s) back")
    status, runs_after = call("GET", f"/jobs/{wb['job_id']}/runs", token=B["token"])
    limb("R4.c B's run was not cancelled",
         status == 200 and all(r.get("cancel_requested_at") is None for r in runs_after),
         f"cancel_requested_at="
         f"{[r.get('cancel_requested_at') for r in runs_after] if status == 200 else status}")

    # ---------------------------------------------------------------- R5 ----
    say("\n" + "-" * 78)
    say("R5 — the other direction: A reaches all of A's own work")
    say("-" * 78)
    mine = []
    for what, path in reads(wa):
        status, _ = call("GET", path, token=A["token"])
        mine.append((what, status))
        say(f"  {what:>20}  GET {path} -> HTTP {status}")
    limb("R5.a every one of A's own routes answers 200",
         all(s == 200 for _, s in mine),
         "; ".join(f"{w}={s}" for w, s in mine))
    status, logs = call("GET", f"/runs/{wa['run_id']}/logs", token=A["token"])
    limb("R5.b and the log is A's own line",
         status == 200 and [x["chunk"] for x in logs] == [f"{A['name']}: loss=0.01\n"],
         f"chunks={[x['chunk'] for x in logs] if status == 200 else status}")
    status, blob = call("GET", f"/artifacts/{wa['artifact_id']}/download",
                        token=A["token"], raw_bytes=True)
    limb("R5.c and the result file comes back unsealed, in the clear",
         status == 200 and blob == f"{A['name']}'s result".encode(),
         f"HTTP {status} bytes={blob[:40]!r}")

    # ---------------------------------------------------------------- R6 ----
    say("\n" + "-" * 78)
    say("R6 — the administrator reaches both users' work")
    say("-" * 78)
    both = []
    for who, w in (("alice", wa), ("bob", wb)):
        for what, path in reads(w):
            status, _ = call("GET", path, token=admin)
            both.append((who, what, status))
            say(f"  {who:>6} {what:>20}  -> HTTP {status}")
    limb("R6.a the administrator is refused nothing", all(s == 200 for _, _, s in both),
         f"{len(both)} route(s), all 200")

    say("")
    say("  and that is a ROLE, not a name. B is refused A's log; set the role on B's")
    say("  row in the database and the SAME token reaches it, with no username")
    say("  anywhere in the comparison:")
    status_before, _ = call("GET", f"/runs/{wa['run_id']}/logs", token=B["token"])
    say(f"    B asking for A's log, as an ordinary user -> HTTP {status_before}")
    rc = os.system(
        'docker compose exec -T postgres psql -q -U fyp -d fyp -c '
        f"\"UPDATE users SET is_admin = true WHERE id = '{B['id']}';\""
    )
    say(f"    B promoted to administrator (psql exit={rc})")
    status_after, _ = call("GET", f"/runs/{wa['run_id']}/logs", token=B["token"])
    say(f"    B asking for A's log, same token, now an administrator -> HTTP {status_after}")
    limb("R6.b seeing everything is a role on the row, not a name",
         status_before == 404 and status_after == 200,
         f"the same token: {status_before} as a user, {status_after} as an administrator")
    os.system(
        'docker compose exec -T postgres psql -q -U fyp -d fyp -c '
        f"\"UPDATE users SET is_admin = false WHERE id = '{B['id']}';\""
    )

    # ---------------------------------------------------------------- R7 ----
    say("\n" + "-" * 78)
    say("R7 — the machine and pool views stay open to everyone")
    say("-" * 78)
    pool_ok = True
    for who, person in (("alice", A), ("bob", B)):
        status, pool = call("GET", "/nodes", token=person["token"])
        st2, events = call("GET", f"/nodes/{node['node_id']}/events",
                           token=person["token"])
        pool_ok = pool_ok and status == 200 and st2 == 200 and len(pool) == 1
        say(f"  {person['name']:>18}: GET /nodes -> HTTP {status}, {len(pool)} machine(s); "
            f"GET /nodes/{{id}}/events -> HTTP {st2}")
    limb("R7.a an ordinary user still sees the pool", pool_ok,
         "both users read the machine list and one machine's event history")

    # ---------------------------------------------------------------- R8 ----
    say("\n" + "-" * 78)
    say("R8 — the live-log socket, where the token rides in the URL")
    say("-" * 78)
    theirs = socket_handshake(wb["run_id"], A["token"])
    own = socket_handshake(wa["run_id"], A["token"])
    unknown = socket_handshake("no-such-run", A["token"])
    say(f"  A opening B's run   -> {theirs}")
    say(f"  A opening A's run   -> {own}")
    say(f"  A opening a run id that was never minted -> {unknown}")
    limb("R8.a the socket refuses B's run in the handshake", "403" in theirs, theirs)
    limb("R8.b the socket still upgrades for A's own run", "101" in own, own)
    limb("R8.c an unknown run is refused the same way, saying nothing about which "
         "identifiers exist", unknown.split()[1:2] == theirs.split()[1:2],
         f"unknown={unknown!r} vs another user's={theirs!r}")

    # ---------------------------------------------------------------- R9 ----
    say("\n" + "-" * 78)
    say("R9 — a job with no owner stays visible to everyone")
    say("-" * 78)
    say("  `jobs.user_id` is nullable and no HTTP door leaves it null, so a null")
    say("  owner means a row written straight into the database: the chaos test's")
    say("  jobs. Refusing those would break the project's centrepiece proof to")
    say("  protect data that belongs to nobody.")
    rc = os.system(
        'docker compose exec -T postgres psql -q -U fyp -d fyp -c '
        f"\"UPDATE jobs SET user_id = NULL WHERE id = '{wb['job_id']}';\""
    )
    say(f"  B's job stripped of its owner in the database (psql exit={rc})")
    status, orphan = call("GET", f"/jobs/{wb['job_id']}", token=A["token"])
    limb("R9.a an ownerless job answers 200 to any logged-in user", status == 200,
         f"HTTP {status}")
    status, listing = call("GET", "/jobs", token=A["token"])
    limb("R9.b and it appears in their list",
         status == 200 and {j["job_id"] for j in listing} == {wa["job_id"], wb["job_id"]},
         f"{len(listing)} job(s)")

    # --------------------------------------------------------------- done ---
    say("\n" + "=" * 78)
    held = sum(1 for _, ok, _ in _results if ok)
    for name, ok, detail in _results:
        say(f"  [{'HELD' if ok else 'WITHDRAWN'}] {name}")
    say(f"\nRESULT ({args.label}): {held} of {len(_results)} limbs held")
    say("=" * 78)
    return 0 if held == len(_results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
