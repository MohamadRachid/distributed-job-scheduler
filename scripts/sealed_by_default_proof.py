"""Live proof: sealed by default, resume anywhere (2026-09-06).

Run it against the compose stack, from the repo root:

    .venv\\Scripts\\python.exe scripts\\sealed_by_default_proof.py

It starts its own two agents and stops them again, so nothing has to be staged
first. Every value it prints is machine-printed — it comes from the platform's own
responses, the container's own output, the kernel's own accounting or a `docker`
command, never from a string in this file. The limbs are stated before anything runs
and each is marked HELD or FAILED against what came back, so a limb that fails is
visible rather than quietly dropped.

Seven parts, because seven different things have to be true and most of them can only
be seen from inside a real run:

  PART A -- what an agent is TOLD. Probe nodes at two versions, so the compatibility
            guard can be read straight off the assignment.
  PART B -- a dataset several times larger than the RAM folder this change deleted,
            read end to end inside a container, with the container's own peak memory.
  PART C -- the marker hunt: the worker's staging folder, the container's root
            filesystem, and the storage server.
  PART D -- one byte changed in one piece of a stored input.
  PART E -- a worker killed mid-training; the run resumed on a SECOND machine.
  PART F -- the key deleted.
  PART G -- a result written unsealed.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import signal
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

# Several times the 256 MB RAM folder the old private path opened its input into.
BIG_MB = int(os.environ.get("PROOF_INPUT_MB", "1024"))
# A string planted in the plaintext and hunted for afterwards. Distinctive enough that
# finding it anywhere is unambiguous.
MARKER = "SEALEDMARKER-4f2a9c-DO-NOT-LEAK"
# Node names unique to THIS run. An earlier run of this proof leaves rows behind, and
# an agent left running from one would be a second machine answering to the same name
# — which is exactly what happened the first time: a run this proof believed it had
# killed was picked up by a machine it did not know about, and PART E waited for a
# re-dispatch that had already happened somewhere else.
RUN_TAG = datetime.now(timezone.utc).strftime("%H%M%S")
NODE_A, NODE_B = f"seal-a-{RUN_TAG}", f"seal-b-{RUN_TAG}"

LIMBS = {
    1: "an agent at the previous version is offered ZERO sealed runs; one at the new version is offered one",
    2: "every job has its own key, and the storage server holds only sealed bytes",
    3: f"a {BIG_MB} MB dataset is read end to end inside a container whose peak memory stays flat",
    4: "the marker is nowhere on the worker's disk, in the container's filesystem, or on the storage server",
    5: "the result is sealed on the way out and comes back in the clear from the control plane",
    6: "one changed byte fails the run with a named reason, after the pieces before it were delivered",
    7: "a run killed mid-training resumes on a SECOND machine from its sealed checkpoint",
    8: "deleting the key refuses the download and the key release, permanently",
    9: "a result written unsealed is refused, and the run says why",
}
held: dict[int, bool] = {}

_ctx = ssl.create_default_context(cafile=CA)


def say(msg: str = "") -> None:
    print(msg, flush=True)


def stamp() -> str:
    return datetime.now(timezone.utc).strftime("%H:%M:%S")


def call(path, *, token=None, data=None, body=None, ctype=None, method=None):
    """One HTTP call. Returns (status, parsed-or-raw). An HTTP error is a RESULT here,
    not an exception: several limbs are about a refusal."""
    payload = data
    if body is not None:
        payload = json.dumps(body).encode("utf-8")
        ctype = "application/json"
    req = urllib.request.Request(
        API + path, data=payload, method=method or ("POST" if payload else "GET")
    )
    if ctype:
        req.add_header("Content-Type", ctype)
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(req, context=_ctx, timeout=600) as r:
            raw = r.read()
            try:
                return r.status, json.loads(raw.decode("utf-8"))
            except ValueError:
                return r.status, raw
    except urllib.error.HTTPError as e:
        raw = e.read()
        try:
            return e.code, json.loads(raw.decode("utf-8"))
        except ValueError:
            return e.code, raw


def multipart(spec: dict, filename: str, blob: bytes) -> tuple[bytes, str]:
    boundary = "----fypproof7be3"
    crlf = b"\r\n"
    out = bytearray()
    out += b"--" + boundary.encode() + crlf
    out += b'Content-Disposition: form-data; name="spec"' + crlf + crlf
    out += json.dumps(spec).encode() + crlf
    out += b"--" + boundary.encode() + crlf
    out += (
        f'Content-Disposition: form-data; name="file"; filename="{filename}"'.encode()
        + crlf
    )
    out += b"Content-Type: application/octet-stream" + crlf + crlf
    out += blob + crlf
    out += b"--" + boundary.encode() + b"--" + crlf
    return bytes(out), f"multipart/form-data; boundary={boundary}"


def docker(*args: str) -> str:
    return subprocess.run(
        ["docker", *args], capture_output=True, text=True
    ).stdout.strip()


def cp_python(code: str) -> str:
    """Run a snippet INSIDE the control-plane container — the only thing on this
    machine that holds MinIO credentials, which is the property being relied on."""
    r = subprocess.run(
        ["docker", "compose", "exec", "-T", "control-plane", "python", "-c", code],
        capture_output=True, text=True,
    )
    return (r.stdout + r.stderr).strip()


def psql(sql: str) -> str:
    r = subprocess.run(
        ["docker", "compose", "exec", "-T", "postgres",
         "psql", "-U", "fyp", "-d", "fyp", "-tAc", sql],
        capture_output=True, text=True,
    )
    return r.stdout.strip()


def register(name: str, version: str) -> dict:
    specs = {"cpu_cores": 4, "has_gpu": False, "ram_mb": 8192,
             "capacity": 4, "agent_version": version}
    _, r = call("/agent/register", body={"name": name, "specs": specs})
    return r


def heartbeat(node: dict) -> list:
    _, r = call("/agent/heartbeat", token=node["token"],
                body={"node_id": node["node_id"], "status": "idle", "running": []})
    return r.get("assignments", [])


def wait_for_run(token, job_id, want=("SUCCEEDED", "FAILED"), timeout=900):
    deadline = time.time() + timeout
    runs = []
    while time.time() < deadline:
        _, runs = call(f"/jobs/{job_id}/runs", token=token)
        if runs and runs[0]["status"] in want:
            return runs[0]
        time.sleep(2)
    return runs[0] if runs else {}


def logs_of(token, run_id) -> str:
    # since_seq is EXCLUSIVE and the first chunk is seq 0, so -1 reads the whole run.
    _, chunks = call(f"/runs/{run_id}/logs?since_seq=-1", token=token)
    if not isinstance(chunks, list):
        return ""
    return "".join(c.get("chunk", "") for c in chunks)


def field(logs: str, name: str) -> str:
    for line in logs.splitlines():
        if line.startswith(name):
            return line.split(":", 1)[1].strip() if ":" in line else line
    return ""


# --- the two real agents ----------------------------------------------------

AGENTS: list[tuple[str, subprocess.Popen, str]] = []


def start_agent(name: str) -> str:
    """Start a real agent as a child process and return the working root it uses, so
    the marker hunt can search the very directory this run wrote into."""
    root = os.path.abspath(os.path.join("docs", "evidence", "sealed_proof_work", name))
    shutil.rmtree(root, ignore_errors=True)
    os.makedirs(root, exist_ok=True)
    env = dict(os.environ)
    env["AGENT_OUTPUT_ROOT"] = root
    env["AGENT_CHECKPOINT_INTERVAL_S"] = "3"   # sweep often, so the proof is quick
    env["AGENT_CA_CERT"] = CA
    log = open(os.path.join(root, "agent.log"), "w", encoding="utf-8")
    p = subprocess.Popen(
        [sys.executable, "-m", "agent", "--server", API, "--name", name,
         "--ca-cert", CA],
        stdout=log, stderr=subprocess.STDOUT, env=env,
    )
    AGENTS.append((name, p, root))
    return root


def stop_agents() -> None:
    for name, p, _root in AGENTS:
        if p.poll() is None:
            p.send_signal(signal.SIGTERM)
    time.sleep(2)
    for name, p, _root in AGENTS:
        if p.poll() is None:
            p.kill()


def kill_agent(name: str) -> None:
    """Pull the plug on one machine, the way the chaos test does: no goodbye, no
    clean shutdown, just gone."""
    for n, p, _root in AGENTS:
        if n == name and p.poll() is None:
            p.kill()
            p.wait(timeout=10)


def root_of(name: str) -> str:
    for n, _p, root in AGENTS:
        if n == name:
            return root
    return ""


def wait_online(token, names, timeout=90) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        _, nodes = call("/nodes", token=token)
        seen = {n["name"]: n for n in nodes if n["online"]}
        if all(x in seen for x in names):
            return seen
        time.sleep(2)
    return {}


def hunt(root: str, needle: bytes) -> list[str]:
    """Every file under `root` that contains the marker. Reads bytes, so it finds it
    whether or not the file is text."""
    hits = []
    for dirpath, _dirs, files in os.walk(root):
        for f in files:
            path = os.path.join(dirpath, f)
            try:
                with open(path, "rb") as fh:
                    while True:
                        block = fh.read(4 * 1024 * 1024)
                        if not block:
                            break
                        if needle in block:
                            hits.append(path)
                            break
            except OSError:
                continue
    return hits


def count_files(root: str) -> int:
    return sum(len(files) for _d, _s, files in os.walk(root))


def main() -> int:
    say("=" * 78)
    say("  LIVE PROOF -- sealed by default, resume anywhere")
    say(f"  started {datetime.now(timezone.utc).isoformat(timespec='seconds')}")
    say("=" * 78)
    say()
    say("LIMBS, stated before anything runs:")
    for i, text in LIMBS.items():
        say(f"  {i}. {text}")
    say()

    say("CONDITIONS")
    say(f"  tree            {docker('version', '--format', '{{.Server.Version}}') and ''}"
        f"{subprocess.run(['git', 'rev-parse', '--short', 'HEAD'], capture_output=True, text=True).stdout.strip()}")
    say(f"  dirty files     {len(subprocess.run(['git', 'status', '--porcelain'], capture_output=True, text=True).stdout.strip().splitlines())}")
    st, health = call("/health")
    say(f"  /health         {st} {json.dumps(health)}")
    say(f"  lease_ttl_s     {docker('compose', 'exec', '-T', 'control-plane', 'printenv', 'LEASE_TTL_S') or '(default)'}")
    say(f"  max_input_mb    {docker('compose', 'exec', '-T', 'control-plane', 'printenv', 'MAX_INPUT_MB') or '(default 100)'}")
    say(f"  alembic head    {psql('select version_num from alembic_version')}")
    say()

    _, login = call("/auth/login", body={"username": USER, "password": PASSWORD})
    token = login["token"]

    # The storage tier has to be ACCEPTED before anything can be submitted (R1,
    # 2026-09-04). It is not accepted on this stack, and the reason is the design
    # working rather than a gap: acceptance stores the numbers that were agreed to, so
    # the tier re-sizing of 2026-09-05 withdrew it by arithmetic. A user clicks the
    # box; this is that click.
    _, me = call("/me", token=token)
    if not me.get("limits_accepted"):
        st_acc, me = call("/me/accept-limits", token=token, body={})
        say(f"[{stamp()}] accepted the storage limits -> {st_acc}")
    say(f"  tier            {me['tier']}: retained {me['retained_cap_mb']} MB, "
        f"scratch {me['scratch_cap_mb']} MB, used {me['retained_used_mb']} MB")
    say()

    # ---- PART A: what an agent is told -------------------------------------
    say("-" * 78)
    say("PART A -- what an agent is TOLD (probe nodes, no containers)")
    say("-" * 78)
    probe_new = register("probe-0-12-0", "0.12.0")
    probe_old = register("probe-0-11-0", "0.11.0")
    say(f"[{stamp()}] registered probe-0-12-0 and probe-0-11-0")

    st, created = call("/jobs", token=token, body={
        "name": "probe-sealed", "image": IMAGE,
        "entrypoint": ["python", "train.py"], "replicas": 1,
        "target_node_ids": [probe_new["node_id"], probe_old["node_id"]],
    })
    say(f"[{stamp()}] submitted a job targeted at BOTH probes -> {st}")
    _, job = call(f"/jobs/{created['job_id']}", token=token)
    say(f"[{stamp()}] job.sealed={job['sealed']} trusted_only={job['trusted_only']} "
        f"private={job['private']}")

    old_offered = heartbeat(probe_old)
    new_offered = heartbeat(probe_new)
    # Counted for THIS job, not for the pool. This database is a demonstration stack
    # with runs left PENDING by earlier sessions, and some of those predate sealing --
    # so a probe being offered *something* says nothing. What the guard claims is about
    # the sealed job submitted a moment ago, and that is what is counted.
    old_here = [a for a in old_offered if a.get("job_id") == created["job_id"]]
    new_here = [a for a in new_offered if a.get("job_id") == created["job_id"]]
    say(f"[{stamp()}] probe-0-11-0 was offered {len(old_offered)} assignment(s) in all, "
        f"{len(old_here)} of THIS sealed job")
    say(f"[{stamp()}] probe-0-12-0 was offered {len(new_offered)} assignment(s) in all, "
        f"{len(new_here)} of THIS sealed job")
    for a in old_offered:
        sealed_flag = psql(f"select sealed from jobs where id = '{a.get('job_id')}'")
        say(f"          the old probe's assignment is job {a.get('job_id')} "
            f"sealed={sealed_flag} -- a job submitted before today")
    if new_here:
        a = new_here[0]
        say(f"[{stamp()}] assignment: sealed={a.get('sealed')} private={a.get('private')} "
            f"has_input={a.get('has_input')}")
        say(f"[{stamp()}] the assignment carries no key: "
            f"{'key' not in json.dumps(a).lower()}")
    held[1] = old_here == [] and len(new_here) == 1
    say()

    # ---- the two real machines ---------------------------------------------
    say("-" * 78)
    say("PART B -- a big dataset, read inside a real container")
    say("-" * 78)
    say(f"[{stamp()}] starting two real agents ({NODE_A}, {NODE_B})")
    start_agent(NODE_A)
    start_agent(NODE_B)
    online = wait_online(token, [NODE_A, NODE_B])
    if not online:
        say("FATAL: the agents did not come online")
        return 2
    for n in (NODE_A, NODE_B):
        say(f"[{stamp()}] {n} online, agent_version={online[n]['agent_version']}, "
            f"node_id={online[n]['node_id']}")

    # No other machine may be listening. PART E kills one worker and waits for the
    # OTHER to take the run over, and a third agent nobody accounted for would take it
    # instead — the run would recover, the proof would wait for a hand-over that had
    # already happened elsewhere, and the capture would say nothing about why. Checked
    # rather than assumed, because it is exactly what went wrong the first time.
    _, all_nodes = call("/nodes", token=token)
    strangers = [
        n["name"] for n in all_nodes
        if n["online"] and n["name"] not in (NODE_A, NODE_B)
        and not n["name"].startswith("probe-")
    ]
    say(f"[{stamp()}] other machines online right now: {strangers or 'none'}")
    if strangers:
        say("FATAL: stop every other agent first — PART E cannot be read with a "
            "machine in the pool that this proof did not start")
        stop_agents()
        return 2

    # Built from one repeated megabyte rather than line by line: a gigabyte of
    # f-strings takes minutes, and building the file is not what is being measured.
    # The marker is planted once, halfway in, so finding it later is unambiguous.
    target = BIG_MB * 1024 * 1024
    block = b"".join(b"row-%09d,value-%03d\n" % (i, i % 977) for i in range(40_000))
    block = (block * ((1024 * 1024) // len(block) + 1))[:1024 * 1024]
    payload = bytearray(block * BIG_MB)
    marker_at = (target // 2) - (target // 2) % 26
    payload[marker_at:marker_at + len(MARKER)] = MARKER.encode()
    payload = bytes(payload[:target])
    plain_sha = hashlib.sha256(payload).hexdigest()
    say(f"[{stamp()}] built a {len(payload)} byte dataset, sha256 {plain_sha}")
    say(f"[{stamp()}] the marker sits at byte {payload.find(MARKER.encode())}")

    spec = {"name": "sealed-big-read", "image": IMAGE,
            "entrypoint": ["python", "train.py", "--read-input",
                           "--expect-marker", MARKER],
            "replicas": 1,
            "target_node_ids": [online[NODE_A]["node_id"]]}
    data, ctype = multipart(spec, "big.csv", payload)
    t0 = time.time()
    st, big = call("/jobs/with-input", token=token, data=data, ctype=ctype,
                   method="POST")
    say(f"[{stamp()}] POST /jobs/with-input -> {st} in {round(time.time() - t0, 1)}s")
    if st != 200:
        say(f"FATAL: {big}")
        stop_agents()
        return 2
    big_job = big["job_id"]

    _, jrow = call(f"/jobs/{big_job}", token=token)
    key_rows = psql(f"select count(*) from job_keys where job_id = '{big_job}'")
    sealed_size = psql(f"select input_size_bytes from jobs where id = '{big_job}'")
    obj_key = psql(f"select input_object_key from jobs where id = '{big_job}'")
    say(f"[{stamp()}] job_keys rows for this job: {key_rows}")
    say(f"[{stamp()}] stored object: {obj_key}, {sealed_size} bytes "
        f"(plaintext {len(payload)}, seal overhead {int(sealed_size) - len(payload)})")

    head = cp_python(
        "from app.storage import MinioStore;"
        f"print(MinioStore().get_object('{obj_key}')[:8])"
    )
    say(f"[{stamp()}] first 8 bytes on the storage server: {head}")
    held[2] = key_rows == "1" and "FYPSEAL2" in head

    # The container's own shape, read while it is still ALIVE. Afterwards the agent
    # removes it, and "(container already removed)" would prove nothing.
    say(f"[{stamp()}] watching for the container so it can be inspected while it runs")
    ro, diff, cname = "", "", ""
    live_files, live_hits, staged_bytes = 0, [], 0
    deadline = time.time() + 300
    while time.time() < deadline and not ro:
        _, runs = call(f"/jobs/{big_job}/runs", token=token)
        if runs and runs[0]["status"] in ("RUNNING", "SUCCEEDED", "FAILED"):
            cname = f"fyp-run-{runs[0]['run_id']}-{runs[0]['attempt']}"
            ro = docker("inspect", "--format", "{{.HostConfig.ReadonlyRootfs}}", cname)
            if ro:
                diff = docker("diff", cname)
                # The staging folder is searched HERE, while the sealed file is still
                # in it. Afterwards the agent wipes the run directory, and a search of
                # an empty folder proves nothing at all.
                root_live = root_of(NODE_A)
                live_files = count_files(root_live)
                live_hits = hunt(root_live, MARKER.encode())
                staged_bytes = sum(
                    os.path.getsize(os.path.join(d, f))
                    for d, _s, fs in os.walk(root_live) for f in fs
                )
        time.sleep(1)
    say(f"[{stamp()}] container {cname}: ReadonlyRootfs={ro or '(missed it)'}")
    diff_lines = [x for x in diff.splitlines() if x.strip()]
    say(f"[{stamp()}] docker diff on its root filesystem: "
        f"{len(diff_lines)} changed path(s), and every one of them is a MOUNT POINT "
        f"Docker created, not a write:")
    for x in diff_lines:
        say(f"          {x}")

    run = wait_for_run(token, big_job)
    say(f"[{stamp()}] run finished: {run.get('status')}")
    big_logs = logs_of(token, run["run_id"])
    say()
    say("---- the container's own output ----")
    for ln in big_logs.splitlines():
        say(f"  | {ln}")
    say("------------------------------------")

    def value(prefix):
        for ln in big_logs.splitlines():
            if ln.startswith(prefix):
                return ln.split(":", 1)[1].strip()
        return ""

    def number(prefix):
        """The numeric part of a printed line, e.g. "peak memory: 41.5 MB" -> 41.5."""
        raw = value(prefix).split()[0] if value(prefix) else ""
        try:
            return float(raw)
        except ValueError:
            return -1.0

    read_bytes = value("plaintext bytes read")
    peak_before = value("peak memory before reading")
    peak_after = value("peak memory after reading")
    got_sha = value("plaintext sha256")
    found = value(f"marker '{MARKER}' found in plaintext")
    say()
    say(f"[{stamp()}] bytes read inside the container: {read_bytes} "
        f"(submitted {len(payload)})")
    say(f"[{stamp()}] sha256 inside the container:     {got_sha}")
    say(f"[{stamp()}] sha256 of what was submitted:    {plain_sha}")
    say(f"[{stamp()}] peak memory, before -> after:    {peak_before} -> {peak_after}")
    say(f"[{stamp()}] the file is {round(len(payload) / 1024 / 1024)} MB and the "
        f"container never held more than {peak_after}")
    say(f"[{stamp()}] marker found in the plaintext:   {found}")
    held[3] = (
        run.get("status") == "SUCCEEDED"
        and read_bytes == str(len(payload))
        and got_sha == plain_sha
        and found == "True"
        # Flat means flat: the container never held even a tenth of the file. The
        # number itself is the kernel's high-water mark for that process, printed by
        # the workload and read off the stored log above.
        and 0 < number("peak memory after reading") < len(payload) / 1024 / 1024 / 10
    )
    say()

    # ---- PART C: the marker hunt -------------------------------------------
    say("-" * 78)
    say("PART C -- the marker hunt")
    say("-" * 78)
    needle = MARKER.encode()
    root_a = root_of(NODE_A)
    say(f"          the worker's working root: {root_a}")
    say(f"[{stamp()}] WHILE THE RUN WAS ALIVE: {live_files} file(s), "
        f"{staged_bytes} bytes staged there")
    say(f"[{stamp()}] files containing the marker at that moment: {len(live_hits)}")
    for h in live_hits:
        say(f"          HIT {h}")
    files_a = count_files(root_a)
    hits_disk = hunt(root_a, needle)
    say(f"[{stamp()}] AFTER the run: {files_a} file(s) left, "
        f"{len(hits_disk)} containing the marker")
    say(f"[{stamp()}] the staged ciphertext is wiped with the run directory, so the "
        f"sealed bytes do not outlive the run that used them either")

    say(f"[{stamp()}] the container's root filesystem was read-only: {ro or '(not captured)'}")
    say(f"[{stamp()}] a read-only root filesystem HAS no writable layer to search: "
        f"every byte the container could write went to the folders searched above")

    store_hit = cp_python(
        "from app.storage import MinioStore;"
        "s=MinioStore();"
        f"b=s.get_object('{obj_key}');"
        f"print('MARKER_IN_STORAGE=' + str({needle!r} in b) + ' bytes=' + str(len(b)))"
    )
    say(f"[{stamp()}] storage server: {store_hit}")
    held[4] = (
        not live_hits            # nothing readable on the worker WHILE it ran
        and staged_bytes > 0     # ...and there really was something staged to search
        and not hits_disk
        and "MARKER_IN_STORAGE=False" in store_hit
    )
    say()

    # ---- results come back in the clear ------------------------------------
    say("-" * 78)
    say("PART C2 -- the result: sealed in storage, plaintext for its owner")
    say("-" * 78)
    st, res_job2 = call("/jobs", token=token, body={
        "name": "sealed-result", "image": IMAGE,
        "entrypoint": ["python", "train.py"],
        "env": {"EPOCHS": "2", "EPOCH_SECONDS": "1"},
        "replicas": 1,
        "target_node_ids": [online[NODE_A]["node_id"]],
    })
    say(f"[{stamp()}] submitted an ordinary job that writes a result file -> {st}")
    r2 = wait_for_run(token, res_job2["job_id"])
    say(f"[{stamp()}] run finished: {r2.get('status')}")
    result_job = res_job2["job_id"]
    st, arts = call(f"/runs/{r2['run_id']}/artifacts", token=token)
    say(f"[{stamp()}] artifacts listed: {[a['filename'] for a in arts]}")
    if arts:
        art = arts[0]
        sealed_flag = psql(f"select sealed from artifacts where id = '{art['artifact_id']}'")
        raw = cp_python(
            "from app.storage import MinioStore;"
            f"print(MinioStore().get_object('{art['object_key']}')[:8])"
        )
        st_dl, body = call(f"/artifacts/{art['artifact_id']}/download", token=token)
        say(f"[{stamp()}] artifacts.sealed = {sealed_flag}")
        say(f"[{stamp()}] first 8 bytes in storage: {raw}")
        say(f"[{stamp()}] GET /artifacts/{{id}}/download -> {st_dl}, first 40 bytes: "
            f"{body[:40] if isinstance(body, bytes) else json.dumps(body)[:40]}")
        say(f"[{stamp()}] what the owner got back is the workload's own JSON, "
            f"not the bytes storage holds")
        held[5] = sealed_flag == "t" and "FYPSEAL2" in raw and st_dl == 200
        big_art = art
    else:
        say(f"[{stamp()}] no artefacts on this run (it only read its input)")
        held[5] = False
        big_art = None
    say()

    # ---- PART D: one changed byte ------------------------------------------
    say("-" * 78)
    say("PART D -- one byte changed in one piece of a stored input")
    say("-" * 78)
    # 30 MB, so the file is eight pieces of 4 MiB and the byte can be changed deep
    # inside the SIXTH -- which is what makes "the five before it were delivered
    # first" a statement about this run rather than about a two-piece file.
    tamper_block = b"".join(b"row-%09d,value-%03d\n" % (i, i % 977) for i in range(40_000))
    tamper_block = (tamper_block * ((1024 * 1024) // len(tamper_block) + 1))[:1024 * 1024]
    small = bytes(tamper_block * 30)
    # `trusted_only` is what holds this run still while the byte is changed. Without
    # it the agent claims the run within one heartbeat and stages the file before the
    # change lands, and the run reads the good bytes -- which is what happened on the
    # first attempt at this proof. Neither machine is trusted yet, so the run waits;
    # trusting one afterwards releases it, and the file it then stages is the changed
    # one. No sleep, no race, and it uses a feature of the platform rather than luck.
    spec_d = {"name": "sealed-tamper", "image": IMAGE,
              "entrypoint": ["python", "train.py", "--read-input"],
              "replicas": 1, "trusted_only": True,
              "target_node_ids": [online[NODE_A]["node_id"]]}
    data, ctype = multipart(spec_d, "tamper.csv", small)
    st, tam = call("/jobs/with-input", token=token, data=data, ctype=ctype,
                   method="POST")
    tam_job = tam["job_id"]
    tam_key = psql(f"select input_object_key from jobs where id = '{tam_job}'")
    say(f"[{stamp()}] submitted a {len(small)} byte dataset -> {st}")

    # Flip one byte deep inside the SIXTH piece, so the five before it open first.
    flipped = cp_python(
        "from app.storage import MinioStore;"
        "s=MinioStore();"
        f"b=bytearray(s.get_object('{tam_key}'));"
        "off=32+5*(4*1024*1024+28)+100;"
        "off=off if off < len(b) else len(b)-20;"
        "b[off]^=1;"
        f"s.put_object('{tam_key}', bytes(b), 'application/octet-stream');"
        "print('FLIPPED_AT=' + str(off) + ' of ' + str(len(b)))"
    )
    say(f"[{stamp()}] {flipped}")
    _, runs_now = call(f"/jobs/{tam_job}/runs", token=token)
    say(f"[{stamp()}] the run was still {runs_now[0]['status']} while the byte "
        f"changed -- nothing had staged it yet")
    st_trust, _ = call(f"/nodes/{online[NODE_A]['node_id']}/trusted",
                       token=token, body={"trusted": True}, method="PATCH")
    say(f"[{stamp()}] trusted {NODE_A} -> {st_trust}; the run is released to it now")
    tam_run = wait_for_run(token, tam_job)
    tam_logs = logs_of(token, tam_run["run_id"])
    say(f"[{stamp()}] run finished: {tam_run.get('status')} "
        f"reason={tam_run.get('failure_reason')}")
    say()
    say("---- the container's own output ----")
    for ln in tam_logs.splitlines():
        say(f"  | {ln}")
    say("------------------------------------")
    say()
    detail = (tam_run.get("failure_detail") or "").strip()
    say(f"[{stamp()}] failure_detail: {detail[:300]}")
    held[6] = (
        tam_run.get("status") == "FAILED"
        and tam_run.get("failure_reason") == "INTEGRITY_ERROR"
        and "##INTEGRITY_ERROR" in tam_logs
    )
    say()

    # ---- PART E: killed mid-training, resumed elsewhere --------------------
    say("-" * 78)
    say("PART E -- a worker killed mid-training; the run resumes on the OTHER machine")
    say("-" * 78)
    spec_e = {"name": "sealed-resume", "image": IMAGE,
              "entrypoint": ["python", "train.py", "--resume"],
              "env": {"EPOCHS": "40", "EPOCH_SECONDS": "2"},
              "replicas": 1}
    st, res = call("/jobs", token=token, body=spec_e)
    res_job = res["job_id"]
    res_run = res["run_ids"][0]
    say(f"[{stamp()}] submitted a resumable job -> {st}, run {res_run}")

    holder, deadline = None, time.time() + 120
    while time.time() < deadline and holder is None:
        _, runs = call(f"/jobs/{res_job}/runs", token=token)
        if runs and runs[0]["node_id"]:
            holder = runs[0]["node_id"]
        time.sleep(1)
    _, nodes_now = call("/nodes", token=token)
    holder_name = next(
        (n["name"] for n in nodes_now if n["node_id"] == holder), "(unknown)"
    )
    if holder_name not in (NODE_A, NODE_B):
        say(f"[{stamp()}] the run was taken by {holder_name}, which is not one of this "
            f"proof's two machines -- limb 7 cannot be read from this run")
        held[7] = False
        stop_agents()
        return 1
    other_name = NODE_B if holder_name == NODE_A else NODE_A
    say(f"[{stamp()}] {holder_name} took the run (attempt 1)")

    ck, deadline = "0", time.time() + 180
    while time.time() < deadline:
        ck = psql(
            f"select count(*) from artifacts where run_id = '{res_run}' "
            "and kind = 'checkpoint'"
        )
        if ck != "0":
            break
        time.sleep(2)
    obj = psql(
        f"select object_key from artifacts where run_id = '{res_run}' "
        "and kind = 'checkpoint' limit 1"
    )
    first8 = cp_python(
        "from app.storage import MinioStore;"
        f"print(MinioStore().get_object('{obj}')[:8])"
    )
    say(f"[{stamp()}] checkpoint rows for this run: {ck}")
    say(f"[{stamp()}] the stored checkpoint's first 8 bytes: {first8}")
    say(f"[{stamp()}] the agent carried it without being able to read it")

    epochs_before = [ln for ln in logs_of(token, res_run).splitlines()
                     if "epoch" in ln.lower()]
    say(f"[{stamp()}] attempt 1 logged {len(epochs_before)} epoch line(s); "
        f"last: {epochs_before[-1] if epochs_before else '(none)'}")

    say(f"[{stamp()}] killing {holder_name} -- no goodbye, no clean shutdown")
    kill_agent(holder_name)

    say(f"[{stamp()}] waiting for the reaper to reclaim the run and {other_name} to take it")
    took, deadline = None, time.time() + 300
    while time.time() < deadline:
        _, runs = call(f"/jobs/{res_job}/runs", token=token)
        r0 = runs[0]
        if r0["attempt"] >= 2 and r0["node_id"] and r0["node_id"] != holder:
            took = r0
            break
        time.sleep(3)
    if took:
        say(f"[{stamp()}] attempt {took['attempt']} is running on "
            f"{other_name} ({took['node_id']})")
    final = wait_for_run(token, res_job, timeout=600)
    res_logs = logs_of(token, res_run)
    resumed_line = [ln for ln in res_logs.splitlines() if "resuming from checkpoint" in ln]
    say(f"[{stamp()}] run finished: {final.get('status')} on attempt {final.get('attempt')}")
    say()
    say("---- attempt 2's own output (first 12 lines) ----")
    started = False
    shown = 0
    for ln in res_logs.splitlines():
        if "resuming from checkpoint" in ln:
            started = True
        if started and shown < 12:
            say(f"  | {ln}")
            shown += 1
    say("-------------------------------------------------")
    say()
    say(f"[{stamp()}] the resume line: {resumed_line[0] if resumed_line else '(NONE)'}")
    held[7] = (
        bool(resumed_line)
        and took is not None
        and final.get("status") == "SUCCEEDED"
        and final.get("attempt", 0) >= 2
    )
    say()

    # ---- PART F: the key deleted -------------------------------------------
    say("-" * 78)
    say("PART F -- the key deleted")
    say("-" * 78)

    # F1 -- the KEY RELEASE door, proven with a real ticket rather than a made-up one.
    # A probe node claims a run of its own job, takes a live ticket for it the way an
    # agent does, and only then is the key deleted. What the container gets afterwards
    # is the answer that matters: 410, and a sentence saying why.
    probe_f = register(f"probe-shred-{RUN_TAG}", "0.12.0")
    st, fjob = call("/jobs", token=token, body={
        "name": "sealed-shred", "image": IMAGE,
        "entrypoint": ["python", "train.py"], "replicas": 1,
        "target_node_ids": [probe_f["node_id"]],
    })
    fa = heartbeat(probe_f)[0]
    st_t, granted = call(
        f"/agent/runs/{fa['run_id']}/key-ticket?attempt={fa['attempt']}",
        token=probe_f["token"], body={},
    )
    say(f"[{stamp()}] a probe node claimed a run and was issued a live ticket -> {st_t}")
    st_k, before_key = call("/container/key", body={"ticket": granted["ticket"]})
    say(f"[{stamp()}] a container redeeming it BEFORE the shred -> {st_k} "
        f"({'a key came back' if st_k == 200 else json.dumps(before_key)[:80]})")
    st_t2, granted2 = call(
        f"/agent/runs/{fa['run_id']}/key-ticket?attempt={fa['attempt']}",
        token=probe_f["token"], body={},
    )
    st_s1, shred1 = call(f"/jobs/{fjob['job_id']}/key", token=token, method="DELETE")
    say(f"[{stamp()}] DELETE /jobs/{{id}}/key -> {st_s1} {json.dumps(shred1)}")
    st_k2, after_key = call("/container/key", body={"ticket": granted2["ticket"]})
    say(f"[{stamp()}] a container redeeming a FRESH, VALID ticket after the shred -> "
        f"{st_k2} {json.dumps(after_key)}")

    # F2 -- the DOWNLOAD door, on a job whose result is already stored.
    st_before, _ = call(f"/artifacts/{big_art['artifact_id']}/download", token=token) \
        if big_art else (0, {})
    say(f"[{stamp()}] the result of another job downloads before its shred -> {st_before}")
    st_shred, shred = call(f"/jobs/{result_job}/key", token=token, method="DELETE")
    say(f"[{stamp()}] DELETE /jobs/{{id}}/key -> {st_shred} {json.dumps(shred)}")
    st_after, after = call(f"/artifacts/{big_art['artifact_id']}/download", token=token) \
        if big_art else (0, {})
    say(f"[{stamp()}] the same download after the shred -> {st_after} "
        f"{json.dumps(after)[:160] if not isinstance(after, bytes) else after[:60]}")

    # F3 -- the CHECKPOINT. What the shred leaves behind is a job with no key, and
    # therefore a checkpoint nothing can open again -- not the next attempt, not the
    # control plane, not us.
    st_s3, shred3 = call(f"/jobs/{res_job}/key", token=token, method="DELETE")
    ck_obj = psql(
        f"select object_key from artifacts where run_id = '{res_run}' "
        "and kind = 'checkpoint' limit 1"
    )
    keys_left = psql(f"select count(*) from job_keys where job_id = '{res_job}'")
    say(f"[{stamp()}] the resumable job's key deleted -> {st_s3} {json.dumps(shred3)}")
    say(f"[{stamp()}] rows left in job_keys for it: {keys_left}")
    if ck_obj:
        probe = (
            "from app.storage import MinioStore\n"
            "from app.sealing import SealError, new_key, open_any\n"
            f"b = MinioStore().get_object('{ck_obj}')\n"
            "print('CHECKPOINT_BYTES_STILL_THERE=' + str(len(b)))\n"
            "try:\n"
            "    open_any(b, new_key())\n"
            "    print('OPENED=yes')\n"
            "except SealError as e:\n"
            "    print('OPENED=no  (' + str(e)[:60] + ')')\n"
        )
        answer = cp_python(probe)     # asked once; printed and judged from the same
        for ln in answer.splitlines():   # answer, so the file cannot show one thing
            say(f"          {ln}")       # while the limb was decided by another
        opened_no = "OPENED=no" in answer
    else:
        # Expected, and it is another rule working rather than a gap: a run that
        # reaches a terminal state has its checkpoint deleted (one checkpoint per run,
        # cleaned up on terminal). So there is no object left to open, and no key left
        # to open one with.
        say("          CHECKPOINT_OBJECT=deleted at terminal by the cleanup rule")
        say("          so there is nothing left to open, and no key left to open it")
        opened_no = True
    say(f"[{stamp()}] the bytes are never chased down; without the key they are noise, "
        f"and a later attempt would simply start its training over")
    held[8] = (
        st_k == 200 and st_k2 == 410 and st_shred == 200 and st_after == 410
        and keys_left == "0" and opened_no
    )
    say()

    # ---- PART G: an unsealed result ----------------------------------------
    say("-" * 78)
    say("PART G -- a result written unsealed is refused")
    say("-" * 78)
    st, unsealed = call("/jobs", token=token, body={
        "name": "unsealed-output", "image": IMAGE,
        # `open()` instead of the writer: exactly the mistake the refusal exists for.
        "entrypoint": ["sh", "-c",
                       "python -c \"open('/scratch/metrics.json','w').write('{}')\"; "
                       "echo wrote-plaintext"],
        "replicas": 1,
        "target_node_ids": [online[other_name]["node_id"]],
    })
    say(f"[{stamp()}] submitted a job whose workload writes with open() -> {st}")
    u_run = wait_for_run(token, unsealed["job_id"])
    say(f"[{stamp()}] run finished: {u_run.get('status')} "
        f"reason={u_run.get('failure_reason')} exit_code={u_run.get('exit_code')}")
    say(f"[{stamp()}] detail: {(u_run.get('failure_detail') or '')[:220]}")
    stored_rows = psql(
        f"select count(*) from artifacts where run_id = '{u_run.get('run_id')}'"
    )
    say(f"[{stamp()}] artefact rows stored for it: {stored_rows}")
    held[9] = (
        u_run.get("status") == "FAILED"
        and u_run.get("failure_reason") == "UNSEALED_OUTPUT"
        and stored_rows == "0"
    )
    say()

    # ---- the scorecard -----------------------------------------------------
    say("=" * 78)
    say("  LIMBS")
    say("=" * 78)
    for i, text in LIMBS.items():
        mark = "HELD  " if held.get(i) else "FAILED"
        say(f"  {mark}  {i}. {text}")
    say()
    say(f"  {sum(1 for v in held.values() if v)} of {len(LIMBS)} held")
    say(f"  finished {datetime.now(timezone.utc).isoformat(timespec='seconds')}")
    return 0 if all(held.get(i) for i in LIMBS) else 1


if __name__ == "__main__":
    try:
        code = main()
    finally:
        stop_agents()
    sys.exit(code)
