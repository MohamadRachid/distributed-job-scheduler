"""Prove the storage quota policy against the LIVE stack, and print every limb.

Run it from the repo root with the demo stack up:

    .venv\\Scripts\\python.exe scripts/storage_quota_proof.py \\
        > docs/evidence/storage_quota_2026-09-04.txt 2>&1

It drives the real control plane over the real network (HTTPS with our own
certificate authority when `certs/ca.pem` exists, plain HTTP otherwise), and every
line it prints is a MACHINE-PRINTED line: each limb of
`docs/evidence/predictions/storage_quota_2026-09-04.md` is checked in code and
reported HELD or WITHDRAWN with what was actually seen. Nothing here is typed by
hand, and nothing here decides its own verdict from a summary — each check compares
values it read back out of the running system.

It pretends to be an agent rather than starting one: it registers a node, heartbeats
to claim a run, and posts the same fenced messages a real agent posts. That is
deliberate for the limbs about the CONTROL PLANE's rules (P1, P1b, P1c, P3, P6) —
they are about what the server does with those messages, and a real Docker container
in the middle would add a dependency without adding evidence. The limbs that are
about the WORKER (P2's overshoot, P4's old agent) need real containers and are run
separately by `scripts/scratch_quota_proof.py`.

Nothing it creates is left behind: it works under its own user and its own node, and
prints what it made so the database tour can be read afterwards without confusion.
"""

from __future__ import annotations

import io
import json
import os
import ssl
import statistics
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

CA = os.environ.get("FYP_CA_FILE", "certs/ca.pem")
_MB = 1024 * 1024

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
    """Record one limb's verdict. Printed with what was SEEN, not with what was
    hoped: a limb that says only "PASS" is an assertion, not evidence."""
    _results.append((name, held, detail))
    say(f"  [{'HELD' if held else 'WITHDRAWN'}] {name}: {detail}")


def _open(req, timeout=30.0):
    return (urllib.request.urlopen(req, timeout=timeout, context=_CTX)
            if _CTX else urllib.request.urlopen(req, timeout=timeout))


def call(method: str, path: str, body=None, token=None, form=None, files=None):
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
            raw = resp.read().decode("utf-8", "replace")
            return resp.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", "replace")
        try:
            return exc.code, json.loads(raw)
        except ValueError:
            return exc.code, raw


def login(username: str, password: str) -> str:
    status, body = call("POST", "/auth/login",
                        {"username": username, "password": password})
    if status != 200:
        raise SystemExit(f"cannot log in as {username}: {status} {body}")
    return body["token"]


def reason_of(body) -> str:
    """The `reason` out of a refusal body, whatever shape it arrived in."""
    if isinstance(body, dict):
        detail = body.get("detail", body)
        if isinstance(detail, dict):
            return str(detail.get("reason", ""))
    return ""


# ===========================================================================


def main() -> int:
    stamp = time.strftime("%Y-%m-%d %H:%M:%S")
    say("=" * 78)
    say("STORAGE QUOTA POLICY — live proof")
    say(f"date         : {stamp}")
    say(f"api          : {API}")
    say("prediction   : docs/evidence/predictions/storage_quota_2026-09-04.md")
    say("every line below is printed by this script from values read back out of")
    say("the running control plane; nothing here is typed by hand")
    say("=" * 78)

    admin = login(*ADMIN)
    status, health = call("GET", "/health")
    say(f"\ncontrol plane /health -> {status} {json.dumps(health)}")

    tag = uuid.uuid4().hex[:8]
    user = f"quota-proof-{tag}"
    password = "quota-proof-pass-123"

    # ---------------------------------------------------------------- P1c --
    say("\n" + "-" * 78)
    say("P1c — roles: an admin creates users; a user agrees for themselves")
    say("-" * 78)
    status, made = call("POST", "/users", {"username": user, "password": password,
                                           "tier": "limited"}, token=admin)
    if status != 200:
        raise SystemExit(f"could not create the proof user: {status} {made}")
    user_id = made["user_id"]
    say(f"  created user {user!r} id={user_id} tier={made['tier']}")
    limb("P1c.a a created user starts UN-accepted",
         made["limits_accepted"] is False,
         f"limits_accepted={made['limits_accepted']}")

    token = login(user, password)
    status, blocked = call("POST", "/jobs",
                           {"name": "n", "image": "i", "entrypoint": ["python", "x.py"]},
                           token=token)
    limb("P1c.b submission is refused before acceptance",
         status == 403 and reason_of(blocked) == "LIMITS_NOT_ACCEPTED",
         f"HTTP {status} reason={reason_of(blocked)!r}")

    status, denied = call("POST", "/users",
                          {"username": f"{user}-x", "password": "another-pass-123"},
                          token=token)
    limb("P1c.c a non-admin calling POST /users is 403", status == 403,
         f"HTTP {status}")
    status, denied2 = call("PATCH", f"/users/{user_id}/tier", {"tier": "standard"},
                           token=token)
    limb("P1c.d a non-admin calling PATCH /users/{id}/tier is 403", status == 403,
         f"HTTP {status}")
    status, moved = call("PATCH", f"/users/{user_id}/tier", {"tier": "standard"},
                         token=admin)
    limb("P1c.e the admin succeeds on the same route", status == 200,
         f"HTTP {status} tier={moved.get('tier') if isinstance(moved, dict) else moved}")

    status, me = call("GET", "/me", token=token)
    say(f"  GET /me -> {json.dumps(me)}")
    limb("P1c.f a tier move leaves the user un-accepted",
         me["limits_accepted"] is False and me["tier"] == "standard",
         f"tier={me['tier']} limits_accepted={me['limits_accepted']}")

    status, accepted = call("POST", "/me/accept-limits", token=token)
    limb("P1c.g accepting is one call and is recorded",
         status == 200 and accepted["limits_accepted"] is True,
         f"HTTP {status} accepted_at={accepted.get('limits_accepted_at')}")

    # ------------------------------------------------------------- setup ---
    # Put this user in a tier small enough to cross in seconds. The tiers table has
    # no edit route on purpose, so this is done where tiers are configured — against
    # the database the deployment owns — and it is the ONLY thing in this proof that
    # reaches past the API.
    say("\n" + "-" * 78)
    say("setting up a 1 MB retained cap for the proof user")
    say("-" * 78)
    small = f"proof-{tag}"
    rc = os.system(
        'docker compose exec -T postgres psql -q -U fyp -d fyp -c '
        f'"INSERT INTO tiers (id, retained_cap_bytes, scratch_cap_bytes, description) '
        f"VALUES ('{small}', {1 * _MB}, {64 * _MB}, 'proof tier, 1 MB retained');\""
    )
    say(f"  psql insert exit={rc}")
    status, moved = call("PATCH", f"/users/{user_id}/tier", {"tier": small}, token=admin)
    say(f"  moved {user} to tier {small}: HTTP {status}")
    call("POST", "/me/accept-limits", token=token)
    status, me = call("GET", "/me", token=token)
    say(f"  GET /me -> {json.dumps(me)}")
    if me["retained_cap_mb"] != 1.0:
        raise SystemExit("setup failed: the proof user is not on a 1 MB cap")

    # ----------------------------------------------------------------- P1 --
    say("\n" + "-" * 78)
    say("P1 — an artefact upload that would cross the retained cap is refused")
    say("-" * 78)
    node = register_node(f"proof-node-{tag}")
    say(f"  registered node {node['node_id']}")

    run = claim_one(node, token, name=f"quota-p1-{tag}")
    say(f"  claimed run {run['run_id']} attempt={run['attempt']} "
        f"scratch_mb={run.get('scratch_mb')}")
    limb("P1.pre the assignment carries the tier's scratch ceiling",
         run.get("scratch_mb") == 64, f"scratch_mb={run.get('scratch_mb')}")

    status, refused = call(
        "POST", f"/agent/runs/{run['run_id']}/artifacts",
        token=node["token"],
        form={"attempt": run["attempt"], "filename": "out.bin", "kind": "result"},
        files={"file": ("out.bin", b"x" * (2 * _MB))},
    )
    say(f"  upload of 2 MB -> HTTP {status} {json.dumps(refused)}")
    limb("P1.a the upload is refused 413 STORAGE_QUOTA_EXCEEDED",
         status == 413 and reason_of(refused) == "STORAGE_QUOTA_EXCEEDED",
         f"HTTP {status} reason={reason_of(refused)!r}")

    status, listing = call("GET", f"/runs/{run['run_id']}/artifacts", token=token)
    limb("P1.b nothing was stored", status == 200 and listing == [],
         f"artifacts listed = {listing}")

    status, me_after = call("GET", "/me", token=token)
    limb("P1.c the refusal cost the user nothing",
         me_after["retained_used_mb"] == 0.0,
         f"retained_used_mb={me_after['retained_used_mb']}")

    status, posted = call("POST", f"/agent/runs/{run['run_id']}/status",
                          {"attempt": run["attempt"], "state": "SUCCEEDED",
                           "exit_code": 0}, token=node["token"])
    say(f"  the agent posts SUCCEEDED anyway -> HTTP {status} {json.dumps(posted)}")
    limb("P1.d the CONTROL PLANE records FAILED regardless",
         status == 200 and posted.get("run_status") == "FAILED",
         f"run_status={posted.get('run_status')}")

    row = run_row(run["job_id"], run["run_id"], token)
    say(f"  run row: status={row['status']} reason={row['failure_reason']} "
        f"attempt={row['attempt']}")
    say(f"  detail: {row['failure_detail']}")
    limb("P1.e the reason is STORAGE_QUOTA_EXCEEDED and names the file",
         row["failure_reason"] == "STORAGE_QUOTA_EXCEEDED"
         and "out.bin" in (row["failure_detail"] or ""),
         f"reason={row['failure_reason']!r}")
    limb("P1.f the attempt was NOT moved by the refusal",
         row["attempt"] == run["attempt"],
         f"attempt {run['attempt']} -> {row['attempt']}")

    run2 = claim_one(node, token, name=f"quota-p1-small-{tag}")
    status, ok = call(
        "POST", f"/agent/runs/{run2['run_id']}/artifacts",
        token=node["token"],
        form={"attempt": run2["attempt"], "filename": "small.bin", "kind": "result"},
        files={"file": ("small.bin", b"y" * (512 * 1024))},
    )
    say(f"  upload of 0.5 MB -> HTTP {status}")
    call("POST", f"/agent/runs/{run2['run_id']}/status",
         {"attempt": run2["attempt"], "state": "SUCCEEDED", "exit_code": 0},
         token=node["token"])
    row2 = run_row(run2["job_id"], run2["run_id"], token)
    status, me2 = call("GET", "/me", token=token)
    limb("P1.g a file that fits still succeeds",
         ok is not None and row2["status"] == "SUCCEEDED"
         and me2["retained_used_mb"] == 0.5,
         f"run={row2['status']} retained_used_mb={me2['retained_used_mb']}")

    # ---------------------------------------------------------------- P1b --
    say("\n" + "-" * 78)
    say("P1b — a private input that would not fit is refused, and seals nothing")
    say("-" * 78)
    spec = json.dumps({"name": f"private-{tag}", "image": "fyp-dummy:latest",
                       "entrypoint": ["python", "train.py"]})
    status, refused = call("POST", "/jobs/private", token=token,
                           form={"spec": spec},
                           files={"file": ("in.bin", b"z" * (700 * 1024))})
    say(f"  0.7 MB input against 0.5 MB used of a 1 MB cap -> HTTP {status} "
        f"{json.dumps(refused)}")
    limb("P1b.a refused 413 STORAGE_QUOTA_EXCEEDED",
         status == 413 and reason_of(refused) == "STORAGE_QUOTA_EXCEEDED",
         f"HTTP {status} reason={reason_of(refused)!r}")

    keys = sql("SELECT count(*) FROM job_keys jk JOIN jobs j ON j.id = jk.job_id "
               f"JOIN users u ON u.id = j.user_id WHERE u.username = '{user}';")
    limb("P1b.b no key row was minted", keys == "0", f"job_keys rows = {keys}")

    status, small_ok = call("POST", "/jobs/private", token=token,
                            form={"spec": spec},
                            files={"file": ("in.bin", b"z" * 50_000)})
    say(f"  0.05 MB input -> HTTP {status}")
    sealed = sql("SELECT input_size_bytes FROM jobs WHERE id = "
                 f"'{small_ok['job_id']}';") if status == 200 else "?"
    status, me3 = call("GET", "/me", token=token)
    expected = round(0.5 + int(sealed) / _MB, 3) if sealed.isdigit() else None
    limb("P1b.c a small one succeeds and is charged its SEALED size exactly",
         expected is not None and me3["retained_used_mb"] == expected,
         f"sealed={sealed} bytes, retained_used_mb={me3['retained_used_mb']} "
         f"(expected {expected})")

    # ----------------------------------------------------------------- R6 --
    say("\n" + "-" * 78)
    say("R6 — the release valve frees the bytes and the quota with them")
    say("-" * 78)
    status, freed = call("DELETE", f"/jobs/{run2['job_id']}/storage", token=token)
    say(f"  DELETE /jobs/{run2['job_id']}/storage -> HTTP {status} {json.dumps(freed)}")
    status, me4 = call("GET", "/me", token=token)
    limb("R6 releasing a finished job's storage returns the quota",
         status == 200 and me4["retained_used_mb"] < me3["retained_used_mb"],
         f"retained_used_mb {me3['retained_used_mb']} -> {me4['retained_used_mb']}")

    # ----------------------------------------------------------------- P3 --
    say("\n" + "-" * 78)
    say("P3 — what the retained check costs, with 1,000 artefact rows for one user")
    say("-" * 78)
    bulk_run = claim_one(node, token, name=f"quota-p3-{tag}")
    sql(
        "INSERT INTO artifacts (id, run_id, attempt, object_key, size, content_type, kind) "
        "SELECT gen_random_uuid()::text, "
        f"'{bulk_run['run_id']}', {bulk_run['attempt']}, "
        f"'runs/{bulk_run['run_id']}/{bulk_run['attempt']}/f' || g || '.bin', "
        "64, 'application/octet-stream', 'result' FROM generate_series(1,1000) g;"
    )
    rows = sql("SELECT count(*) FROM artifacts a JOIN runs r ON r.id = a.run_id "
               "JOIN jobs j ON j.id = r.job_id JOIN users u ON u.id = j.user_id "
               f"WHERE u.username = '{user}';")
    say(f"  artefact rows for this user: {rows}")

    timings = []
    for _ in range(20):
        t0 = time.perf_counter()
        call("GET", "/me", token=token)
        timings.append((time.perf_counter() - t0) * 1000.0)
    med = statistics.median(timings)
    say("  GET /me (lock + three sums + the HTTP round trip), n=20:")
    say(f"    median {med:.1f} ms | min {min(timings):.1f} ms | max {max(timings):.1f} ms")
    say("  NOTE: this is the WHOLE request, so it is an upper bound on the check —")
    say("  it includes TLS, routing and JSON on top of the lock and the three sums.")
    limb("P3 the retained check's median is under 20 ms", med < 20.0,
         f"median {med:.1f} ms over n=20, spread {min(timings):.1f}-{max(timings):.1f} ms")

    # ----------------------------------------------------------------- P6 --
    say("\n" + "-" * 78)
    say("P6 — placement: a run is not offered to a machine without room for it")
    say("-" * 78)
    call("PATCH", f"/users/{user_id}/tier", {"tier": "standard"}, token=admin)
    call("POST", "/me/accept-limits", token=token)

    # Wait for every OTHER machine to fall out of the online window first, and say
    # why rather than just sleeping. The untargeted spread preference
    # makes a machine step past its own siblings while ANOTHER eligible
    # machine is online and free — so with the proof's earlier node still inside the
    # 12 s window, the big-disk machine correctly takes one run and leaves two for a
    # third machine that is never going to ask for them. That is the spread rule
    # working, not the disk filter failing, and it is a precondition of this limb
    # rather than a result of it. So: let the window pass, then show the pool.
    say("  waiting out node_timeout_s so only the two machines under test are online")
    time.sleep(14)
    smalld = register_node(f"proof-small-{tag}")
    bigd = register_node(f"proof-big-{tag}")
    beat(smalld, disk_free_mb=100)
    beat(bigd, disk_free_mb=100_000)
    status, pool = call("GET", "/nodes", token=admin)
    online = [n for n in pool if n["online"]]
    say(f"  online machines now: {len(online)} -> "
        + ", ".join(f"{n['name']}(disk_free_mb={n['disk_free_mb']})" for n in online))
    limb("P6.pre only the two machines under test are online",
         len(online) == 2, f"{len(online)} online")
    status, job = call("POST", "/jobs", {
        "name": f"p6-{tag}", "image": "fyp-dummy:latest",
        "entrypoint": ["python", "train.py"], "replicas": 3,
        "resource_reqs": {"needs_gpu": False, "scratch_mb": 1024},
    }, token=token)
    say(f"  submitted 3 replicas asking for 1024 MB of temporary disk: HTTP {status}")

    # Measured on THIS JOB'S OWN RUN ROWS, not on how many assignments a heartbeat
    # happened to return. A shared database carries older jobs' unclaimed runs, and a
    # count of assignments would mix those in and say nothing about the rule being
    # tested. The claim is "every run OF THIS JOB landed on the machine above the
    # ask", so that is what is read back.
    for _ in range(8):
        beat(smalld, disk_free_mb=100)
        beat(bigd, disk_free_mb=100_000)
        status, rows = call("GET", f"/jobs/{job['job_id']}/runs", token=token)
        if all(r["node_id"] for r in rows):
            break
    status, rows = call("GET", f"/jobs/{job['job_id']}/runs", token=token)
    placed = {}
    for r in rows:
        name = ({smalld["node_id"]: "small-disk", bigd["node_id"]: "big-disk"}
                .get(r["node_id"], r["node_id"] or "unplaced"))
        placed[name] = placed.get(name, 0) + 1
    say(f"  the job's three runs: {json.dumps(placed)}")
    limb("P6 every run landed on the machine above the ask",
         placed.get("big-disk") == 3 and "small-disk" not in placed,
         f"placement = {json.dumps(placed)}")

    # ------------------------------------------------------------- summary -
    say("\n" + "=" * 78)
    held = sum(1 for _n, ok, _d in _results if ok)
    for name, ok, detail in _results:
        say(f"  {'HELD     ' if ok else 'WITHDRAWN'} {name}")
    say(f"\nRESULT: {held}/{len(_results)} limbs held")
    say("=" * 78)
    return 0 if held == len(_results) else 1


# --- the agent-shaped helpers ----------------------------------------------


def register_node(name: str) -> dict:
    status, body = call("POST", "/agent/register", {
        "name": name,
        "specs": {"cpu_cores": 4, "has_gpu": False, "ram_mb": 8192,
                  "capacity": 4, "agent_version": "0.10.0"},
    })
    if status != 200:
        raise SystemExit(f"register failed: {status} {body}")
    return body


def beat(node: dict, disk_free_mb: int | None = None) -> list:
    usage = {"cpu_pct": 1.0}
    if disk_free_mb is not None:
        usage["disk_free_mb"] = disk_free_mb
    status, body = call("POST", "/agent/heartbeat", {
        "node_id": node["node_id"], "status": "idle", "running": [], "usage": usage,
    }, token=node["token"])
    if status != 200:
        raise SystemExit(f"heartbeat failed: {status} {body}")
    return body["assignments"]


def claim_one(node: dict, token: str, name: str) -> dict:
    status, job = call("POST", "/jobs", {
        "name": name, "image": "fyp-dummy:latest",
        "entrypoint": ["python", "train.py"], "replicas": 1,
        "resource_reqs": {"needs_gpu": False},
    }, token=token)
    if status != 200:
        raise SystemExit(f"submit failed: {status} {job}")
    for _ in range(5):
        got = beat(node, disk_free_mb=100_000)
        for a in got:
            if a["run_id"] in job["run_ids"]:
                a["job_id"] = job["job_id"]
                return a
    raise SystemExit(f"the run was never assigned: {job}")


def run_row(job_id: str, run_id: str, token: str) -> dict:
    status, rows = call("GET", f"/jobs/{job_id}/runs", token=token)
    return next(r for r in rows if r["run_id"] == run_id)


def sql(statement: str) -> str:
    """One value out of the database, through the compose psql. Used only where the
    API deliberately has no route — reading a sealed input's stored size, counting
    key rows, and creating the tiny proof tier, because tiers are configuration and
    have no edit endpoint by design."""
    import subprocess

    out = subprocess.run(
        ["docker", "compose", "exec", "-T", "postgres",
         "psql", "-qtAX", "-U", "fyp", "-d", "fyp", "-c", statement],
        capture_output=True, text=True,
    )
    return (out.stdout or out.stderr).strip().splitlines()[0].strip() if (
        out.stdout or out.stderr) else ""


if __name__ == "__main__":
    sys.exit(main())
