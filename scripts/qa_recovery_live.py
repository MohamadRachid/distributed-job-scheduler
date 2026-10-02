"""Kill and restart an owned QA worker; prove real sealed checkpoint recovery.

Uses only qa_system_live's isolated 127.0.0.1:8001 stack. The live demo and its
workers are never stopped. Both attempts run on this same physical machine.
"""

import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import docker
import httpx
import psutil

from qa_system_live import BASE, ROOT, TLS, checked, record, spec, wait


def stop_owned(process):
    if process.poll() is not None:
        return
    parent = psutil.Process(process.pid)
    children = parent.children(recursive=True)
    for child in children:
        child.kill()
    parent.kill()
    process.wait(timeout=10)


def main():
    with tempfile.TemporaryDirectory(prefix="fyp-qa-recovery-") as temp_dir:
        environment = {**os.environ, "AGENT_STATE_FILE": str(Path(temp_dir) / "state.json"),
                       "AGENT_CAPACITY": "1", "AGENT_CHECKPOINT_INTERVAL_S": "1"}
        node_name = "system-qa-recovery-" + str(int(time.time()))
        command = [sys.executable, "-m", "agent", "--server", BASE,
                   "--name", node_name, "--ca-cert", str(ROOT / "certs/ca.pem")]
        log_path = ROOT / f"docs/evidence/system_qa_2026-09-07/recovery_agent_{node_name}.txt"
        with log_path.open("w", encoding="utf-8") as log:
            def start():
                return subprocess.Popen(command, cwd=ROOT, env=environment, stdout=log,
                                        stderr=subprocess.STDOUT,
                                        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))

            process = start()
            try:
                with httpx.Client(base_url=BASE, verify=TLS, timeout=20) as api:
                    token = checked(api, "POST", "/auth/login", json={"username": "admin", "password": "fyp-admin"}).json()["token"]
                    api.headers["Authorization"] = f"Bearer {token}"
                    deadline = time.monotonic() + 40
                    while time.monotonic() < deadline:
                        nodes = checked(api, "GET", "/nodes").json()
                        matches = [n for n in nodes if n["name"] == node_name]
                        if matches:
                            node_id = matches[-1]["node_id"]
                            break
                        time.sleep(1)
                    else:
                        raise AssertionError("QA worker failed to register")
                    body = spec("recovery", ["--resume"],
                                target_node_ids=[node_id],
                                env={"EPOCHS": "120", "EPOCH_SECONDS": "0.5"})
                    job_id = checked(api, "POST", "/jobs", json=body).json()["job_id"]
                    run_id = checked(api, "GET", f"/jobs/{job_id}/runs").json()[0]["run_id"]
                    db = docker.from_env().containers.get("fyp-postgres-1")
                    deadline = time.monotonic() + 50
                    while time.monotonic() < deadline:
                        result = db.exec_run(["psql", "-U", "fyp", "-d", "fyp_qa_live_20260907", "-Atc",
                                              f"SELECT count(*) FROM artifacts WHERE run_id='{run_id}' AND kind='checkpoint'"])
                        assert result.exit_code == 0
                        if int(result.output.strip()) > 0:
                            break
                        time.sleep(1)
                    else:
                        raise AssertionError("no checkpoint reached object storage")
                    record("checkpoint stored before worker kill", job_id=job_id, run_id=run_id)
                    stop_owned(process)
                    record("killed QA worker process tree; restarting same identity")
                    process = start()
                    run = wait(api, job_id, "SUCCEEDED", limit=160)
                    assert run["attempt"] == 2, run
                    logs = checked(api, "GET", f"/runs/{run_id}/logs").text
                    assert "resuming from checkpoint at epoch" in logs
                    files = checked(api, "GET", f"/runs/{run_id}/artifacts").json()
                    results = [json.loads(checked(api, "GET", f"/artifacts/{a['artifact_id']}/download").content) for a in files]
                    resumed = [r["resumed_from_epoch"] for r in results if "resumed_from_epoch" in r]
                    assert resumed and 0 < resumed[0] < 120, results
                    record("real checkpoint recovery", attempt=run["attempt"],
                           resumed_from_epoch=resumed[0], lease_seconds=60,
                           checkpoint_upload_interval_seconds=1, physical_hosts=1, status="PASS")
                    checked(api, "DELETE", f"/jobs/{job_id}/storage")
            finally:
                stop_owned(process)


if __name__ == "__main__":
    main()
