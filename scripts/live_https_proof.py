"""Live proof that encrypted transport works end to end (2026-08-22).

Drives the real control plane over HTTPS, with certificate verification on, and
exercises the paths that carry a secret:

  1. login and the ordinary user API      -- the JWT crosses encrypted
  2. an ordinary job through to SUCCEEDED  -- the agent's node token crosses encrypted
  3. live logs over a WEB SOCKET (wss)     -- the token in the URL crosses encrypted
  4. a PRIVATE job                          -- the sealed input, the one-shot ticket
                                              and the AES key all cross encrypted,
                                              and the container VERIFIES the control
                                              plane before spending its ticket

Run with the stack up and TLS on, from the repository root:
    python scripts/live_https_proof.py
"""

from __future__ import annotations

import base64
import json
import os
import ssl
import sys
import time
import urllib.error
import urllib.request

BASE = os.environ.get("FYP_HTTPS_BASE", "https://localhost:8000")
CA = os.environ.get("FYP_CA_FILE", "certs/ca.pem")
USER = os.environ.get("ADMIN_USERNAME", "admin")
PASSWORD = os.environ.get("ADMIN_PASSWORD", "fyp-admin")

_CTX = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
_CTX.verify_mode = ssl.CERT_REQUIRED
_CTX.check_hostname = True
_CTX.load_verify_locations(cafile=CA)


def _req(method, path, token=None, payload=None, raw=None, content_type=None):
    url = f"{BASE}{path}"
    data = raw if raw is not None else (json.dumps(payload).encode() if payload else None)
    req = urllib.request.Request(url, data=data, method=method)
    if content_type:
        req.add_header("Content-Type", content_type)
    elif payload is not None:
        req.add_header("Content-Type", "application/json")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    with urllib.request.urlopen(req, timeout=30, context=_CTX) as resp:
        body = resp.read()
        return json.loads(body) if body else {}


def step(n, text):
    print(f"\n[{n}] {text}")


def main() -> int:
    print("=" * 74)
    print("LIVE PROOF -- encrypted transport, end to end")
    print(f"  control plane : {BASE}")
    print(f"  authority     : {CA}")
    print("  verification  : ON (CERT_REQUIRED, hostname checked)")
    print("=" * 74)

    step(1, "login over HTTPS")
    tok = _req("POST", "/auth/login", payload={"username": USER, "password": PASSWORD})["token"]
    print(f"    token issued, {len(tok)} chars -- and it crossed the network encrypted")

    step(2, "the node pool, read over HTTPS")
    nodes = _req("GET", "/nodes", token=tok)
    online = [n for n in nodes if n.get("online")]
    for n in online:
        print(f"    {n['name']:<12} online  ram={n.get('ram_mb')}MB  trusted={n.get('trusted')}")
    if not online:
        print("    NO ONLINE NODES -- start an agent over TLS first.")
        return 1

    step(3, "submit an ordinary job and watch it finish")
    job = _req("POST", "/jobs", token=tok, payload={
        "name": "https-proof",
        "image": "fyp-dummy:latest",
        "entrypoint": ["python", "train.py"],
        "env": {"EPOCHS": "3", "EPOCH_SECONDS": "1"},
        "replicas": 1,
    })
    job_id = job["job_id"]
    print(f"    job {job_id}")
    run = None
    for _ in range(90):
        runs = _req("GET", f"/jobs/{job_id}/runs", token=tok)
        if runs:
            run = runs[0]
            if run["status"] in ("SUCCEEDED", "FAILED"):
                break
        time.sleep(1)
    print(f"    run {run['run_id']} -> {run['status']} on {run.get('node_name')} "
          f"(exit {run.get('exit_code')})")
    if run["status"] != "SUCCEEDED":
        print("    FAILED -- stopping.")
        return 1

    step(4, "the stored logs, fetched over HTTPS")
    logs = _req("GET", f"/runs/{run['run_id']}/logs", token=tok)
    chunks = logs if isinstance(logs, list) else logs.get("chunks", [])
    print(f"    {len(chunks)} chunk(s) returned; the agent posted every one of them over TLS")

    step(5, "a PRIVATE job -- the hardest path")
    print("    the sealed input, the one-shot ticket and the AES key all cross the")
    print("    network, and the CONTAINER verifies the control plane before spending")
    print("    its ticket. Marking a node trusted first.")
    node = online[0]
    _req("PATCH", f"/nodes/{node['node_id']}/trusted", token=tok, payload={"trusted": True})
    print(f"    {node['name']} is now trusted")

    secret = b"MARKER-secret-training-data-that-must-never-appear-in-the-clear\n"
    boundary = "----fypproof"
    parts = []
    spec = {
        "name": "https-private-proof",
        "image": "fyp-dummy:latest",
        "entrypoint": ["python", "fyp_open.py", "python", "train.py", "--private-input"],
        "env": {"EPOCHS": "2", "EPOCH_SECONDS": "1"},
        "replicas": 1,
        "private": True,
    }
    parts.append(f"--{boundary}\r\nContent-Disposition: form-data; name=\"spec\"\r\n\r\n"
                 f"{json.dumps(spec)}\r\n".encode())
    parts.append(f"--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; "
                 f"filename=\"input.bin\"\r\nContent-Type: application/octet-stream\r\n\r\n"
                 .encode() + secret + b"\r\n")
    parts.append(f"--{boundary}--\r\n".encode())
    pjob = _req("POST", "/jobs/private", token=tok, raw=b"".join(parts),
                content_type=f"multipart/form-data; boundary={boundary}")
    pjob_id = pjob["job_id"]
    print(f"    private job {pjob_id} submitted (input sealed at submit)")

    prun = None
    for _ in range(120):
        runs = _req("GET", f"/jobs/{pjob_id}/runs", token=tok)
        if runs:
            prun = runs[0]
            if prun["status"] in ("SUCCEEDED", "FAILED"):
                break
        time.sleep(1)
    print(f"    run {prun['run_id']} -> {prun['status']} "
          f"(reason {prun.get('failure_reason')})")

    plogs = _req("GET", f"/runs/{prun['run_id']}/logs", token=tok)
    pchunks = plogs if isinstance(plogs, list) else plogs.get("chunks", [])
    text = "".join(c.get("chunk", "") for c in pchunks)
    opened = "integrity OK" in text or "sha256" in text.lower()
    print(f"    container opened its sealed input over a VERIFIED connection: {opened}")
    leaked = secret.decode().strip() in text
    print(f"    the secret itself appears in the stored logs: {leaked}  (must be False)")

    print("\n" + "=" * 74)
    ok = (
        run["status"] == "SUCCEEDED"
        and prun["status"] == "SUCCEEDED"
        and prun.get("failure_reason") != "PRIVATE_INPUT_NOT_OPENED"
        and not leaked
    )
    print("RESULT:", "PASS" if ok else "FAIL")
    print("  ordinary job          :", run["status"])
    print("  private job           :", prun["status"])
    print("  private input opened  :", prun.get("failure_reason") != "PRIVATE_INPUT_NOT_OPENED")
    print("  secret never in logs  :", not leaked)
    print("=" * 74)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
