"""Live proof that untargeted replicas prefer a free machine.

Submits ONE untargeted job with `--replicas` runs against whatever real agents
are already registered and online, waits for every run to reach a terminal
state, and prints one machine-printed row per run: run id, the node that ran it,
start, finish, and final status. Nothing here is hand-typed into the capture —
every field is read back from `GET /jobs/{id}/runs`.

Run it with the stack up and the agents already heart-beating:

    python scripts/spread_replicas_proof.py --label "after the change"

It reads only; the sole write is the job submission itself.
"""

import argparse
import json
import ssl
import sys
import time
import urllib.request
from datetime import datetime, timezone

API = "https://localhost:8000"
CA = "certs/ca.pem"


def _ctx():
    c = ssl.create_default_context(cafile=CA)
    c.check_hostname = False
    return c


def _call(method, path, token=None, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(API + path, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    with urllib.request.urlopen(req, context=_ctx(), timeout=30) as r:
        return json.loads(r.read().decode())


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--replicas", type=int, default=3)
    ap.add_argument("--label", default="")
    args = ap.parse_args()

    token = _call("POST", "/auth/login",
                  body={"username": "admin", "password": "fyp-admin"})["token"]

    nodes = _call("GET", "/nodes", token)
    online = [n for n in nodes if n["online"]]
    print(f"# label: {args.label}")
    print(f"# utc: {datetime.now(timezone.utc).isoformat(timespec='seconds')}")
    print(f"# online nodes at submit: {len(online)}")
    for n in sorted(online, key=lambda x: x["name"]):
        print(f"#   node {n['node_id']}  name={n['name']}  capacity={n['capacity']}")

    job = _call("POST", "/jobs", token, {
        "name": f"spread-proof-{int(time.time())}",
        "image": "fyp-dummy:latest",
        "entrypoint": ["python", "train.py"],
        "env": {"EPOCHS": "5", "EPOCH_SECONDS": "1"},
        "resource_reqs": {"needs_gpu": False},
        "replicas": args.replicas,
        "target_node_ids": None,
    })
    job_id = job["job_id"]
    print(f"# job: {job_id}  replicas={args.replicas}  target_node_ids=None")

    deadline = time.time() + 300
    runs = []
    while time.time() < deadline:
        runs = _call("GET", f"/jobs/{job_id}/runs", token)
        if runs and all(r["status"] in ("SUCCEEDED", "FAILED") for r in runs):
            break
        time.sleep(2)

    names = {n["node_id"]: n["name"] for n in nodes}
    for r in sorted(runs, key=lambda x: (x.get("started_at") or "", x["run_id"])):
        print(
            f"run {r['run_id']}  node_id={r['node_id']} ({names.get(r['node_id'], '?')})  "
            f"started_at={r.get('started_at')}  finished_at={r.get('finished_at')}  "
            f"status={r['status']}  exit_code={r.get('exit_code')}"
        )
    print(f"# distinct node_id across the {len(runs)} runs: "
          f"{len({r['node_id'] for r in runs})}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
