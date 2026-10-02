"""Live proof: an ordinary job carries a dataset zip, and the container reads it.

Run it against a staged demo:

    .venv\\Scripts\\python.exe scripts\\job_input_proof.py

Every value this prints is machine-printed -- it comes from the platform's own
responses and the container's own output, never from a string in this file. The limbs
are stated before anything runs, and each is marked HELD or FAILED against what came
back, so a limb that fails is visible rather than quietly dropped.

Two parts, because two different things have to be true and only one of them can be
watched from outside a real run:

  PART A -- a probe node registers and heart-beats, so the ASSIGNMENT the agent
            receives can be read directly. This is where `has_input` is proven, and
            where an out-of-date agent is proven to be offered nothing.
  PART B -- a real run on a real agent, end to end, where the container itself
            reports what it can see.
"""

from __future__ import annotations

import hashlib
import io
import json
import ssl
import sys
import time
import urllib.request
import zipfile
from datetime import datetime, timezone

API = "https://localhost:8000"
CA = "certs/ca.pem"
USER, PASSWORD = "admin", "fyp-admin"
IMAGE = "fyp-dummy:latest"

LIMBS = {
    1: "an ordinary job is accepted WITH a file and is not marked private",
    2: "the file is charged to the owner's retained storage",
    3: "the assignment carries has_input=true and the uploaded filename",
    4: "an agent older than the guard is offered the job NOT AT ALL",
    5: "INPUT_PATH names the file inside the container, under the user's own name",
    6: "the bytes inside the container are identical to the bytes uploaded",
    7: "the archive opens inside the container and lists its members",
    8: "the mount is read-only -- writing to it is refused",
    9: "the agent did not unpack it: /input holds exactly one entry",
}
held: dict[int, bool] = {}

_ctx = ssl.create_default_context(cafile=CA)


def say(msg: str = "") -> None:
    print(msg, flush=True)


def stamp() -> str:
    return datetime.now(timezone.utc).strftime("%H:%M:%S")


def call(path, *, token=None, data=None, body=None, ctype=None, method=None):
    if body is not None:
        data, ctype, method = json.dumps(body).encode(), "application/json", method or "POST"
    req = urllib.request.Request(API + path, data=data, method=method)
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    if ctype:
        req.add_header("Content-Type", ctype)
    try:
        with urllib.request.urlopen(req, timeout=120, context=_ctx) as r:
            raw = r.read()
            return r.status, (json.loads(raw) if raw[:1] in (b"{", b"[") else raw)
    except urllib.error.HTTPError as e:
        return e.code, e.read()


def multipart(spec: dict, filename: str, blob: bytes) -> tuple[bytes, str]:
    b = "----fypproof20260905"
    out = io.BytesIO()
    out.write(f"--{b}\r\n".encode())
    out.write(b'Content-Disposition: form-data; name="spec"\r\n\r\n')
    out.write(json.dumps(spec).encode() + b"\r\n")
    out.write(f"--{b}\r\n".encode())
    out.write(f'Content-Disposition: form-data; name="file"; filename="{filename}"\r\n'.encode())
    out.write(b"Content-Type: application/zip\r\n\r\n")
    out.write(blob + b"\r\n")
    out.write(f"--{b}--\r\n".encode())
    return out.getvalue(), f"multipart/form-data; boundary={b}"


def make_zip(padding: int = 0) -> bytes:
    """A real archive, built here, so what the container opens is a genuine zip and
    not a file that merely starts with the right two bytes. `padding` makes it big
    enough that the retained-storage figure moves by a visible amount."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("train.csv", "feature,label\n1.0,0\n2.0,1\n3.0,0\n")
        z.writestr("notes.txt", "a dataset submitted as a zip, 2026-09-05\n")
        if padding:
            # Random-ish and incompressible, so the stored size is close to this.
            z.writestr("blob.bin", hashlib.sha256(b"seed").digest() * (padding // 32))
    return buf.getvalue()


def register(name: str, version: str) -> dict:
    specs = {"cpu_cores": 4, "has_gpu": False, "ram_mb": 8192,
             "capacity": 4, "agent_version": version}
    _, r = call("/agent/register", body={"name": name, "specs": specs})
    return r


def heartbeat(node: dict) -> list:
    _, r = call("/agent/heartbeat", token=node["token"],
                body={"node_id": node["node_id"], "status": "idle", "running": []})
    return r["assignments"]


def main() -> int:
    say("=" * 76)
    say("  LIVE PROOF -- an ordinary job carries a dataset file")
    say(f"  started {datetime.now(timezone.utc).isoformat(timespec='seconds')}")
    say("=" * 76)
    say()
    say("LIMBS, stated before anything runs:")
    for n, text in LIMBS.items():
        say(f"  {n}. {text}")
    say()

    _, tok = call("/auth/login", body={"username": USER, "password": PASSWORD})
    token = tok["token"]

    # ---- PART A: what the agent is told ------------------------------------
    say("-" * 76)
    say("PART A -- the assignment, read on probe nodes")
    say("-" * 76)

    probe_new = register("probe-current", "0.11.0")
    probe_old = register("probe-outdated", "0.10.0")
    say(f"[{stamp()}] registered probe-current (agent 0.11.0) and "
        f"probe-outdated (agent 0.10.0)")

    small = make_zip()
    spec_a = {"name": "probe-job", "image": IMAGE,
              "entrypoint": ["sh", "-c", "true"], "replicas": 1,
              "target_node_ids": [probe_new["node_id"], probe_old["node_id"]]}
    data, ctype = multipart(spec_a, "probe.zip", small)
    st, _ = call("/jobs/with-input", token=token, data=data, ctype=ctype, method="POST")
    say(f"[{stamp()}] submitted a targeted job with a file -> {st}")

    old_offered = heartbeat(probe_old)
    new_offered = heartbeat(probe_new)
    say(f"[{stamp()}] probe-outdated was offered {len(old_offered)} assignment(s)")
    say(f"[{stamp()}] probe-current  was offered {len(new_offered)} assignment(s)")
    a = new_offered[0] if new_offered else {}
    if a:
        say(f"[{stamp()}] assignment: has_input={a.get('has_input')} "
            f"private={a.get('private')} input_filename={a.get('input_filename')!r}")
    held[3] = a.get("has_input") is True and a.get("input_filename") == "probe.zip"
    held[4] = old_offered == []

    # ---- PART B: what the container sees -----------------------------------
    say()
    say("-" * 76)
    say("PART B -- a real run, end to end")
    say("-" * 76)

    _, me_before = call("/me", token=token)
    retained_before = me_before["retained_used_mb"]
    say(f"[{stamp()}] retained storage before: {retained_before} MB")

    blob = make_zip(padding=2 * 1024 * 1024)
    members = zipfile.ZipFile(io.BytesIO(blob)).namelist()
    expected = hashlib.sha256(blob).hexdigest()
    say(f"[{stamp()}] built a real zip: {len(blob)} bytes, members {members}")

    # The entrypoint IS the instrument. Everything after this is the container's own
    # report of its own world -- nothing here inspects the worker from outside.
    script = (
        'echo "INPUT_PATH=$INPUT_PATH";'
        'echo "LISTING:"; ls -1 /input;'
        'echo "ENTRIES=$(ls -1 /input | wc -l)";'
        'echo "SIZE=$(wc -c < $INPUT_PATH)";'
        'python -c "'
        "import os,zipfile,hashlib;"
        "p=os.environ['INPUT_PATH'];"
        "print('SHA256='+hashlib.sha256(open(p,'rb').read()).hexdigest());"
        "print('MEMBERS='+','.join(zipfile.ZipFile(p).namelist()))"
        '";'
        '( echo x > "$INPUT_PATH" ) 2>/dev/null && echo "WRITE=succeeded" '
        '|| echo "WRITE=refused"'
    )
    spec_b = {"name": "dataset-zip-proof", "image": IMAGE,
              "entrypoint": ["sh", "-c", script], "replicas": 1}
    data, ctype = multipart(spec_b, "dataset.zip", blob)
    st, created = call("/jobs/with-input", token=token, data=data, ctype=ctype,
                       method="POST")
    say(f"[{stamp()}] POST /jobs/with-input -> {st}  job {created['job_id']}")

    _, job = call(f"/jobs/{created['job_id']}", token=token)
    say(f"[{stamp()}] job.private={job.get('private')} "
        f"input_filename={job.get('input_filename')!r}")
    held[1] = st == 200 and job.get("private") is False

    _, me_after = call("/me", token=token)
    retained_after = me_after["retained_used_mb"]
    say(f"[{stamp()}] retained storage after:  {retained_after} MB "
        f"(+{round(retained_after - retained_before, 3)})")
    held[2] = retained_after > retained_before

    run_id = created["run_ids"][0]
    say(f"[{stamp()}] waiting for run {run_id} ...")
    state, deadline = "", time.time() + 300
    while time.time() < deadline:
        _, runs = call(f"/jobs/{created['job_id']}/runs", token=token)
        state = runs[0]["status"]
        if state in ("SUCCEEDED", "FAILED"):
            break
        time.sleep(3)
    say(f"[{stamp()}] run finished: {state}")

    # since_seq is EXCLUSIVE (chunks with seq > N), and the first chunk is seq 0 --
    # so -1 is what reads a run's whole output. Passing 0 silently drops chunk 0.
    _, chunks = call(f"/runs/{run_id}/logs?since_seq=-1", token=token)
    logs = "".join(c.get("chunk", "") for c in chunks)
    say()
    say("---- the container's own output ----")
    for line in logs.splitlines():
        say(f"  | {line}")
    say("------------------------------------")
    say()

    def field(name: str) -> str:
        for line in logs.splitlines():
            if line.startswith(name + "="):
                return line.split("=", 1)[1].strip()
        return ""

    held[5] = field("INPUT_PATH") == "/input/dataset.zip"
    held[6] = field("SHA256") == expected and field("SIZE") == str(len(blob))
    held[7] = "train.csv" in field("MEMBERS") and "notes.txt" in field("MEMBERS")
    held[8] = field("WRITE") == "refused"
    held[9] = field("ENTRIES") == "1"

    say(f"uploaded sha256 : {expected}")
    say(f"in-container    : {field('SHA256')}")
    say(f"uploaded bytes  : {len(blob)}")
    say(f"in-container    : {field('SIZE')}")
    say()
    say("RESULTS:")
    for n, text in LIMBS.items():
        say(f"  {n}. {'HELD  ' if held.get(n) else 'FAILED'}  {text}")
    ok = sum(1 for v in held.values() if v)
    say()
    say(f"{ok} of {len(LIMBS)} limbs held.  run state: {state}")
    say(f"finished {datetime.now(timezone.utc).isoformat(timespec='seconds')}")
    return 0 if ok == len(LIMBS) and state == "SUCCEEDED" else 1


if __name__ == "__main__":
    sys.exit(main())
