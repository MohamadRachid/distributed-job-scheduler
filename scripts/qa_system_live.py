"""Live QA against the dedicated 127.0.0.1:8001 test stack only.

Requires its disposable database/bucket and a worker registered there. Never
points at the demonstration API on port 8000. Credentials stay in memory.

The address is the IPv4 literal, not "localhost", on purpose (2026-09-07). The QA
container is published on 127.0.0.1 only, and Windows resolves "localhost" to ::1
first, so every new connection paid a 2 s IPv6 fallback before reaching the API
(docs/evidence/system_qa_2026-09-07/heartbeat_starvation_4_localhost_ipv6_fallback.txt:
2.05 s per GET by name, 0.015 s by address). The certificate names 127.0.0.1.
"""

import hashlib
import json
import ssl
import time
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[1]
BASE = "https://127.0.0.1:8001"
TLS = ssl.create_default_context(cafile=str(ROOT / "certs/ca.pem"))


def record(name, **facts):
    print(json.dumps({"check": name, **facts}), flush=True)


def checked(client, method, path, status=200, **kwargs):
    response = client.request(method, path, **kwargs)
    assert response.status_code == status, (path, response.status_code, response.text[:600])
    return response


def spec(name, args=None, **overrides):
    body = dict(name="system-qa-" + name, image="fyp-dummy:latest",
                entrypoint=["python", "train.py", *(args or [])],
                env={"EPOCHS": "3", "EPOCH_SECONDS": "0.3"},
                resource_reqs={"mem_limit_mb": 128, "scratch_mb": 32}, replicas=1)
    body.update(overrides)
    return body


def wait(client, job_id, expected, limit=100):
    deadline = time.monotonic() + limit
    while time.monotonic() < deadline:
        runs = checked(client, "GET", f"/jobs/{job_id}/runs").json()
        if runs and all(r["status"] in ("SUCCEEDED", "FAILED") for r in runs):
            assert [r["status"] for r in runs] == [expected], runs
            job = checked(client, "GET", f"/jobs/{job_id}").json()
            assert job["status"] == expected, job
            record("terminal", job_id=job_id, status=expected,
                   reasons=[r.get("failure_reason") for r in runs])
            return runs[0]
        time.sleep(1)
    raise AssertionError(("job timed out", job_id, runs))


def main():
    with httpx.Client(base_url=BASE, verify=TLS, timeout=20) as api:
        checked(api, "GET", "/jobs", status=401)
        checked(api, "POST", "/auth/login", status=401,
                json={"username": "admin", "password": "wrong"})
        token = checked(api, "POST", "/auth/login",
                        json={"username": "admin", "password": "fyp-admin"}).json()["token"]
        api.headers["Authorization"] = f"Bearer {token}"
        nodes = checked(api, "GET", "/nodes").json()
        assert any(n["name"] == "system-qa-node" for n in nodes)
        record("verified TLS, authentication, registration, heartbeat", nodes=len(nodes))

        job = checked(api, "POST", "/jobs", json=spec("success")).json()["job_id"]
        run = wait(api, job, "SUCCEEDED")
        logs = checked(api, "GET", f"/runs/{run['run_id']}/logs").json()
        assert logs, "missing run logs"
        files = checked(api, "GET", f"/runs/{run['run_id']}/artifacts").json()
        assert files, "missing result files"
        contents = [checked(api, "GET", f"/artifacts/{a['artifact_id']}/download").content for a in files]
        assert any(b"accuracy" in blob for blob in contents)
        record("training, log replay, sealed results decrypted on download", files=len(files))

        payload = b"qa dataset marker\n" * 100
        submitted = checked(api, "POST", "/jobs/with-input",
                            data={"spec": json.dumps(spec("input", ["--read-input", "--expect-marker", "qa dataset marker"]))},
                            files={"file": ("qa.csv", payload, "text/csv")}).json()["job_id"]
        data_run = wait(api, submitted, "SUCCEEDED")
        input_logs = checked(api, "GET", f"/runs/{data_run['run_id']}/logs").text
        assert hashlib.sha256(payload).hexdigest() in input_logs
        record("sealed input opened in real container", bytes=len(payload), sha256=hashlib.sha256(payload).hexdigest())

        crash = checked(api, "POST", "/jobs", json=spec("crash", ["--crash"])).json()["job_id"]
        assert wait(api, crash, "FAILED")["failure_reason"]
        oom = checked(api, "POST", "/jobs", json=spec("oom", ["--oom"])).json()["job_id"]
        assert wait(api, oom, "FAILED")["failure_reason"] == "OOM_KILLED"

        cancel = checked(api, "POST", "/jobs", json=spec("cancel", env={"EPOCHS": "100", "EPOCH_SECONDS": "1"})).json()["job_id"]
        deadline = time.monotonic() + 40
        while time.monotonic() < deadline:
            active = checked(api, "GET", f"/jobs/{cancel}/runs").json()[0]
            if active["status"] == "RUNNING":
                break
            time.sleep(1)
        else:
            raise AssertionError("cancel workload never started")
        checked(api, "POST", f"/jobs/{cancel}/cancel")
        assert wait(api, cancel, "FAILED")["failure_reason"] == "CANCELLED"

        account = "qa-reader-" + str(int(time.time()))
        checked(api, "POST", "/users", json={"username": account, "password": "qa-test-password", "tier": "limited"})
        with httpx.Client(base_url=BASE, verify=TLS, timeout=20) as other:
            other_token = checked(other, "POST", "/auth/login", json={"username": account, "password": "qa-test-password"}).json()["token"]
            other.headers["Authorization"] = f"Bearer {other_token}"
            assert checked(other, "GET", "/jobs").json() == []
            for path in (f"/jobs/{job}", f"/runs/{run['run_id']}/logs", f"/artifacts/{files[0]['artifact_id']}/download"):
                checked(other, "GET", path, status=404)
            checked(other, "POST", "/jobs", status=403, json=spec("unaccepted"))
            checked(other, "POST", "/me/accept-limits")
            checked(other, "POST", "/users", status=403, json={"username": "no", "password": "no"})
        record("account isolation, admin authorization, tier acceptance")

        checked(api, "DELETE", f"/jobs/{job}/key")
        checked(api, "GET", f"/artifacts/{files[0]['artifact_id']}/download", status=410)
        for job_id in (job, submitted, crash, oom, cancel):
            checked(api, "DELETE", f"/jobs/{job_id}/storage")
        assert checked(api, "GET", f"/runs/{run['run_id']}/artifacts").json() == []
        record("crypto-shred and release", status="PASS")


if __name__ == "__main__":
    main()
