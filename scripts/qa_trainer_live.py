"""Check the real PyTorch demonstration workload on the isolated QA stack."""

import argparse
import json

import httpx

from qa_system_live import BASE, TLS, checked, record, spec, wait


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--image", default="fyp-trainer:latest")
    parser.add_argument("--expect", default="SUCCEEDED")
    args = parser.parse_args()
    with httpx.Client(base_url=BASE, verify=TLS, timeout=20) as api:
        token = checked(api, "POST", "/auth/login", json={"username": "admin", "password": "fyp-admin"}).json()["token"]
        api.headers["Authorization"] = f"Bearer {token}"
        nodes = checked(api, "GET", "/nodes").json()
        target = next(n["node_id"] for n in nodes if n["name"] == "system-qa-node")
        body = spec("pytorch", image=args.image, target_node_ids=[target],
                    env={"EPOCHS": "1", "TRAIN_SUBSET": "32", "TORCH_THREADS": "1"},
                    resource_reqs={"mem_limit_mb": 512, "scratch_mb": 64})
        job_id = checked(api, "POST", "/jobs", json=body).json()["job_id"]
        run = wait(api, job_id, args.expect, limit=150)
        record("PyTorch demonstration", image=args.image, status=run["status"], reason=run.get("failure_reason"))
        if args.expect == "SUCCEEDED":
            files = checked(api, "GET", f"/runs/{run['run_id']}/artifacts").json()
            outputs = {a["object_key"].rsplit("/", 1)[-1]: checked(api, "GET", f"/artifacts/{a['artifact_id']}/download").content for a in files}
            assert outputs["model.pt"].startswith(b"PK")
            assert outputs["curve.png"].startswith(b"\x89PNG")
            metrics = json.loads(outputs["metrics.json"])
            assert metrics["epochs"] == 1 and metrics["dataset"] == "fashion-mnist"
            record("PyTorch model, plot and metrics decrypt correctly", files=list(outputs), status="PASS")
        checked(api, "DELETE", f"/jobs/{job_id}/storage")


if __name__ == "__main__":
    main()
