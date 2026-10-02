"""Machine-printed proof for the checkpoint-use advisor (2026-09-05).

Everything this prints is READ or COMPUTED here and now. Nothing is transcribed by
hand: only a machine-printed line is citable.

Run from the repository root:

    python scripts/checkpoint_advisor_proof.py > docs/evidence/checkpoint_advisor_2026-09-06.txt
"""

import io
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone

# The evidence file carries em-dashes and the reason strings verbatim; on Windows the
# default console encoding would replace them with question marks and the capture
# would no longer be the text the code produced.
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "control-plane"))

from app.checkpoint_advisor import scan_text  # noqa: E402

FIXTURES = os.path.join(ROOT, "control-plane", "tests", "fixtures", "advisor")

# The six fixtures, in the order the brief lists their kinds: two that save and
# resume, two that save only, two that never checkpoint.
ORDER = [
    "saves_and_resumes_torch.py",
    "saves_and_resumes_fyp.py",
    "saves_only_torch.py",
    "saves_only_pickle.py",
    "no_checkpoint_plain.py",
    "no_checkpoint_saves_elsewhere.py",
]


def rule(title):
    print()
    print("=" * 78)
    print(title)
    print("=" * 78)


def read(path):
    with io.open(path, encoding="utf-8") as fh:
        return fh.read()


def git(*args):
    try:
        return subprocess.check_output(
            ["git", *args], cwd=ROOT, text=True, stderr=subprocess.DEVNULL
        ).strip()
    except Exception:
        return "(git unavailable)"


def live(source_text):
    """One real submission over HTTPS against the running compose stack.

    Not the in-process test client: this goes over TLS, through the real auth, into
    real PostgreSQL, and reads the advice back out of the read shape a browser would
    use. `requests` with the project's own CA file, because the control plane has
    served HTTPS only since 2026-08-22 and verifies against that authority alone."""
    try:
        import requests
    except ImportError:
        print("live section skipped: `requests` is not importable here")
        return

    base = os.environ.get("PROOF_SERVER_URL", "https://localhost:8000")
    ca = os.path.join(ROOT, "certs", "ca.pem")
    session = requests.Session()
    session.verify = ca
    try:
        health = session.get(base + "/health", timeout=15)
        print(f"GET {base}/health -> {health.status_code} {health.json()}")
        token = session.post(
            base + "/auth/login",
            json={
                "username": os.environ.get("PROOF_ADMIN_USER", "admin"),
                "password": os.environ.get("PROOF_ADMIN_PASS", "fyp-admin"),
            },
            timeout=15,
        ).json()["token"]
        session.headers["Authorization"] = f"Bearer {token}"

        body = {
            "name": "advisor-proof",
            "image": "fyp-dummy:latest",
            "entrypoint": ["python", "train.py"],
            "replicas": 1,
            "source_text": source_text,
        }
        print()
        print(f"POST {base}/jobs   (source_text: {len(source_text.encode('utf-8'))} bytes)")
        created = session.post(base + "/jobs", json=body, timeout=30)
        print(f"  -> {created.status_code} {json.dumps(created.json())}")
        job_id = created.json()["job_id"]

        # The advice is written by a background task that runs after the response, so
        # the first read can legitimately be early. Polled, and the number of reads it
        # took is printed rather than hidden behind a sleep.
        got = None
        for attempt in range(1, 11):
            page = session.get(base + f"/jobs/{job_id}", timeout=15).json()
            if page.get("checkpoint_advice"):
                got = page
                print(f"  advice present after {attempt} read(s)")
                break
            time.sleep(0.5)

        print()
        print(f"GET {base}/jobs/{job_id}")
        print(json.dumps(got, indent=2, sort_keys=True))
        print()
        if got is not None:
            print("source_text echoed back in the read shape:",
                  "source_text" in json.dumps(got))

        # The oversize door, live.
        print()
        oversize = dict(body, name="advisor-proof-oversize", source_text="x" * 65_537)
        resp = session.post(base + "/jobs", json=oversize, timeout=30)
        print(f"POST {base}/jobs with 65,537 bytes -> {resp.status_code} (expect 422)")
    except Exception as exc:
        print(f"live section could not run: {type(exc).__name__}: {exc}")
        print("(the stack must be up: docker compose up -d)")


def migration():
    """alembic up -> down -> up, in the pinned container against compose PostgreSQL."""
    cmd = [
        "docker", "compose", "run", "--rm", "control-plane", "sh", "-c",
        "alembic upgrade head && alembic current && "
        "alembic downgrade -1 && alembic current && "
        "alembic upgrade head && alembic current",
    ]
    print("$ " + " ".join(cmd[:6]) + " ...")
    try:
        out = subprocess.run(
            cmd, cwd=ROOT, text=True, capture_output=True, timeout=300
        )
        for line in (out.stdout + out.stderr).splitlines():
            if "alembic" in line or line.strip().startswith(("f1a2", "e0f1")):
                print("  " + line.rstrip())
        print(f"  exit code: {out.returncode}")
    except Exception as exc:
        print(f"  could not run: {type(exc).__name__}: {exc}")


def main():
    print("Checkpoint-use advisor — proof capture")
    print("Captured:", datetime.now(timezone.utc).isoformat())
    print("Tree:", git("rev-parse", "--short", "HEAD"))
    print("Working tree clean:", "yes" if not git("status", "--porcelain") else "no")
    print()
    print("From M. Ayli's suggestion of 2026-09-05: scan the code the user submits and")
    print("tell them whether it uses the platform's checkpoint saving, so that a")
    print("re-dispatched run can resume instead of training again from the top.")

    # --- 1. Arm A over the six fixtures ------------------------------------------
    rule("1. ARM A (plain scan, no network) — the six fixtures")
    for name in ORDER:
        r = scan_text(read(os.path.join(FIXTURES, name)))
        print()
        print(f"  {name}")
        print(f"    verdict : {r['verdict']}")
        print(f"    path    : {r['path_hits']}")
        print(f"    save    : {r['save_hits']}")
        print(f"    load    : {r['load_hits']}")
        print(f"    reason  : {r['reason']}")

    # --- 2. The regression: this repository's own two workloads -------------------
    rule("2. THE REGRESSION — the platform's own reference workloads")
    print()
    print("The brief specified arm A as: the literal path '/checkpoint' AND a save")
    print("call. Measured below, that rule scores BOTH of this repository's reference")
    print("workloads as no_checkpoint — and they are the two that checkpoint")
    print("CORRECTLY. They never write the literal path because they are not supposed")
    print("to: the platform's adoption contract is workloads/dummy/fyp_checkpoint.py,")
    print("whose whole point is that a workload calls load()/save() and the path")
    print("arrives in the environment (agent/runner.py sets CHECKPOINT_PATH and mounts")
    print("the directory). So the token set counts all three spellings of the same")
    print("fact, and the brief's literal is kept as one of them.")
    for parts in (
        ("workloads", "trainer", "train.py"),
        ("workloads", "dummy", "train.py"),
    ):
        path = os.path.join(ROOT, *parts)
        text = read(path)
        r = scan_text(text)
        print()
        print("  " + "/".join(parts))
        print(f"    literal '/checkpoint' occurrences : {text.count('/checkpoint')}")
        print(f"    'fyp_checkpoint' occurrences      : {text.count('fyp_checkpoint')}")
        print(f"    verdict under the shipped rule    : {r['verdict']}")
        print(f"    path hits                         : {r['path_hits']}")
        # The brief's rule, re-implemented here in one line so the claim above is
        # measured in this file rather than asserted by it.
        brief_rule = "/checkpoint" in text and any(
            t in text
            for t in ("torch.save", "save_checkpoint", "np.save", "pickle.dump", ".save(")
        )
        print(
            "    verdict under the brief's rule    : "
            + ("saves_checkpoint or better" if brief_rule else "no_checkpoint")
        )

    # --- 3. Sizes and the door ----------------------------------------------------
    rule("3. THE DOOR — the 64 KB cap is measured in bytes")
    # Read out of the source file with its line number rather than imported: this
    # script runs on the HOST, where pydantic is not installed, and quoting the file
    # is better evidence than an import anyway — it shows where the number lives.
    for path in (
        os.path.join("control-plane", "app", "schemas.py"),
        os.path.join("control-plane", "app", "checkpoint_advisor.py"),
    ):
        for n, line in enumerate(read(os.path.join(ROOT, path)).splitlines(), 1):
            if "65_536" in line:
                print(f"  {path}:{n}: {line.strip()}")
    print()
    print("Enforced on POST /jobs and POST /jobs/private as a 422; asserted by")
    print("tests/test_checkpoint_advisor.py at exactly the cap and one byte over,")
    print("and exercised live in section 4 below.")

    # --- 4. A real submission through the running control plane -------------------
    rule("4. LIVE — one real POST /jobs with a pasted script, then GET /jobs/{id}")
    live(read(os.path.join(FIXTURES, "saves_only_torch.py")))

    # --- 5. The migration, up -> down -> up on real PostgreSQL ---------------------
    rule("5. MIGRATION — up -> down -> up on real PostgreSQL")
    migration()

    rule("END")


if __name__ == "__main__":
    main()
