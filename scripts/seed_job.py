"""Seed a W2 job and watch it run to completion — the no-UI trigger for the W2
DoD.

It submits one job for the dummy workload via POST /jobs, then polls
GET /jobs/{id}/runs until every run is terminal, printing each transition. With a
live agent attached and Docker up, you watch a run go
PENDING -> ASSIGNED -> RUNNING -> SUCCEEDED.

IT LOGS IN FIRST, AND THAT IS WORTH A LINE IN THE REPORT.
Until 2026-08-15 this script carried no credentials at all, and had been broken
since W6 without anyone noticing: `POST /jobs` is gated by `require_user`, so the
seeder got a flat 401. Our own tooling could not get past our own authentication.
That is FR-11 and NFR-1 working — a security control that stops the people who
built it is a control that is genuinely on — and we found it by accident rather
than by test, which is the honest version of the finding. The fix needed no new
endpoint: it gets a token from `POST /auth/login` exactly as the browser does,
using the admin the control plane bootstraps from its own environment.

Stdlib only (like the agent). Usage:

    # build the workload image once (the agent runs it locally, no registry):
    docker build -t fyp-dummy:latest workloads/dummy

    python scripts/seed_job.py                      # success path
    python scripts/seed_job.py --fail               # exercise the FAILED path
    python scripts/seed_job.py --server http://HOST:8000 --epochs 8
    python scripts/seed_job.py --username admin --password secret
"""

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

# The control plane has spoken only TLS since the 22 August 2026 transport build, and
# this script spoke plain HTTP until 25 August, so it could not reach it at all. The
# context is built by the agent's own context_for(), never a second copy: one definition
# of how we verify, or the two drift and only one of them is tested.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from agent.tls import context_for, read_ca_pem  # noqa: E402

_CTX = None  # set from --ca-cert in main(); None means plain HTTP, as urlopen expects

TERMINAL = {"SUCCEEDED", "FAILED"}

# The demo defaults, which are the same ones docker-compose.yml hands the control
# plane to bootstrap its single admin. Overridable by flag or environment so this
# never becomes another hard-coded fact about one machine.
DEFAULT_USERNAME = os.environ.get("FYP_USERNAME", "admin")
DEFAULT_PASSWORD = os.environ.get("FYP_PASSWORD", "fyp-admin")


def _post(url: str, payload: dict, token: str | None = None, timeout: float = 10.0) -> dict:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST")
    req.add_header("Content-Type", "application/json")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    with urllib.request.urlopen(req, timeout=timeout, context=_CTX) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _get(url: str, token: str | None = None, timeout: float = 10.0) -> dict | list:
    req = urllib.request.Request(url, method="GET")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    with urllib.request.urlopen(req, timeout=timeout, context=_CTX) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _login(server: str, username: str, password: str) -> str:
    """Get a JWT the same way the dashboard does. No registration endpoint exists
    (and none should): the one admin is created by the control plane's startup
    bootstrap from ADMIN_USERNAME / ADMIN_PASSWORD."""
    try:
        return _post(
            f"{server}/auth/login", {"username": username, "password": password}
        )["token"]
    except urllib.error.HTTPError as exc:
        if exc.code == 401:
            raise SystemExit(
                f"login refused for user '{username}'. The control plane bootstraps "
                "its admin from ADMIN_USERNAME / ADMIN_PASSWORD (docker-compose.yml); "
                "pass --username/--password or set FYP_USERNAME/FYP_PASSWORD to match."
            ) from exc
        raise


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Seed a W2 dummy job and watch it run")
    # 2026-09-07 (walk 1, row 10): the default follows the same rule the control
    # plane itself follows -- https when the demo LAN's certificates exist, plain http
    # when they do not -- so the README's command works on a fresh clone, which serves
    # plain http because the certificates are never in the repository.
    p.add_argument(
        "--server", default=None,
        help="control-plane base URL (default: https://localhost:8000 when "
             "certs/ca.pem exists, otherwise http://localhost:8000)",
    )
    p.add_argument("--ca-cert", default=None,
                   help="authority to verify the control plane against; "
                        "pass an empty string for a plain-HTTP deployment")
    p.add_argument("--image", default="fyp-dummy:latest")
    p.add_argument("--epochs", type=int, default=5)
    p.add_argument("--epoch-seconds", type=float, default=1.0)
    p.add_argument("--fail", action="store_true", help="force the container to exit non-zero")
    p.add_argument("--timeout", type=float, default=120.0, help="seconds to wait for terminal")
    p.add_argument("--username", default=DEFAULT_USERNAME, help="login user (W6 auth)")
    p.add_argument("--password", default=DEFAULT_PASSWORD, help="login password (W6 auth)")
    args = p.parse_args(argv)
    if args.server is None:
        have_ca = Path("certs/ca.pem").exists()
        args.server = "https://localhost:8000" if have_ca else "http://localhost:8000"
        print(f"server: {args.server} ({'certs/ca.pem found' if have_ca else 'no certs/ca.pem, plain http'})")

    # Refuse the two configurations that look like success and are not: verifying
    # nothing over TLS, and pointing an authority at a plain-HTTP server. Same rule
    # the agent applies -- a misconfiguration that connects anyway is worse than one
    # that stops, because nobody investigates a green run.
    global _CTX
    if args.server.startswith("https://"):
        ca = "certs/ca.pem" if args.ca_cert is None else args.ca_cert
        if not ca:
            p.error("--server is https but --ca-cert is empty: refusing to skip verification")
        _CTX = context_for(read_ca_pem(ca))
    elif args.ca_cert:
        p.error("--ca-cert was given but --server is plain http")

    env = {"EPOCHS": str(args.epochs), "EPOCH_SECONDS": str(args.epoch_seconds)}
    if args.fail:
        env["FAIL"] = "1"

    job = {
        "name": "w2-smoke",
        "image": args.image,
        "entrypoint": ["python", "train.py"],
        "env": env,
        "resource_reqs": {"needs_gpu": False},
        "target_node_ids": None,   # any eligible node
        "replicas": 1,
    }

    try:
        token = _login(args.server, args.username, args.password)
        created = _post(f"{args.server}/jobs", job, token=token)
    except urllib.error.HTTPError as exc:
        print(f"control plane refused the request: {exc.code} {exc.reason}", file=sys.stderr)
        return 2
    except urllib.error.URLError as exc:
        print(f"could not reach control plane at {args.server}: {exc}", file=sys.stderr)
        return 2

    job_id = created["job_id"]
    print(f"submitted job {job_id} ({len(created['run_ids'])} run(s)); waiting for an agent...")

    last: dict[str, str] = {}
    deadline = args.timeout
    waited = 0.0
    while waited <= deadline:
        runs = _get(f"{args.server}/jobs/{job_id}/runs", token=token)
        for r in runs:
            prev = last.get(r["run_id"])
            if r["status"] != prev:
                node = r["node_id"] or "-"
                extra = f" exit={r['exit_code']}" if r["exit_code"] is not None else ""
                print(f"  run {r['run_id'][:8]} attempt={r['attempt']} "
                      f"node={node[:8]} -> {r['status']}{extra}")
                last[r["run_id"]] = r["status"]
        if runs and all(r["status"] in TERMINAL for r in runs):
            ok = all(r["status"] == "SUCCEEDED" for r in runs)
            # Plain ASCII — the Windows console (cp1252) can't encode emoji.
            print("RESULT:", "all runs SUCCEEDED" if ok else "a run FAILED")
            return 0 if ok else 1
        time.sleep(1.0)
        waited += 1.0

    print(f"timed out after {deadline:.0f}s — is an agent running with Docker up?",
          file=sys.stderr)
    return 3


if __name__ == "__main__":
    sys.exit(main())
