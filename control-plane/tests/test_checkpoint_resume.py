"""Checkpoint and resume (2026-08-13) — the cross-attempt rule, in both directions.

The whole feature rests on one sentence: **the request is fenced, the object is not.**
An agent asking for a checkpoint must still own the run at its current attempt — the
ordinary fence, unchanged, applied to the reader. What is relaxed is only which
attempt the returned *object* came from, and it is relaxed for checkpoints alone.

So these tests are mostly about the boundary rather than the happy path:

  * RULE — `_artifact_query` allows a cross-attempt read for a checkpoint and refuses
    one for a result. Tested on the query itself, because the guard has to hold
    whichever route calls it, not because of which route called it.
  * ISOLATION — a checkpoint never appears in the results listing and can never be
    downloaded through the results door. This is the guard that keeps at-most-once
    true while resume works.
  * FENCE — the checkpoint route answers 404 / 409 / 409 exactly like a status post.
  * ABSENT — nothing saved, or bytes storage cannot produce, is 204 and not an error.
    A workload with no checkpoint has to behave exactly as it did before today.
  * BOUNDED — saving again overwrites one row and one object, and the stored digest
    follows the new bytes (a digest describing older bytes could never match again).
  * COMPAT — an agent that predates `kind` still uploads results.
"""

import hashlib

from app.api.artifacts import KIND_CHECKPOINT, KIND_RESULT, _artifact_query
from app.models import Artifact, Run
from conftest import seal_for_run

SPECS = {"cpu_cores": 4, "has_gpu": False, "ram_mb": 8192, "capacity": 4, "agent_version": "0.12.0"}


# --- helpers ----------------------------------------------------------------


def _node_auth(node):
    return {"Authorization": f"Bearer {node['token']}"}


async def _register(client, name="lab-pc-01"):
    resp = await client.post("/agent/register", json={"name": name, "specs": SPECS})
    assert resp.status_code == 200, resp.text
    return resp.json()


async def _claim(client, node):
    """Submit a job and heartbeat `node` so it owns the (attempt-1) run."""
    body = {
        "name": "j", "image": "fyp-dummy:latest", "entrypoint": ["python", "train.py"],
        "env": {}, "resource_reqs": {"needs_gpu": False}, "replicas": 1,
    }
    assert (await client.post("/jobs", json=body)).status_code == 200
    hb = await client.post(
        "/agent/heartbeat",
        headers=_node_auth(node),
        json={"node_id": node["node_id"], "status": "idle", "running": []},
    )
    assert hb.status_code == 200, hb.text
    return hb.json()["assignments"][0]


async def _upload(client, node, run_id, attempt, filename, content, kind=None):
    # 2026-09-06: a sealed job's outputs are sealed inside the container, and the
    # control plane refuses anything else. This helper stands in for a container, so
    # it seals with the run's real key (conftest.seal_for_run) -- which is also what
    # keeps the download assertions honest: they get plaintext back because the seal
    # opens, not because nothing was sealed.
    content = await seal_for_run(client.session_factory, run_id, content)
    data = {"attempt": str(attempt), "filename": filename}
    if kind is not None:
        data["kind"] = kind
    return await client.post(
        f"/agent/runs/{run_id}/artifacts",
        headers=_node_auth(node),
        data=data,
        files={"file": (filename, content, "application/octet-stream")},
    )


async def _opens_to(client, run_id, blob, expected):
    """The bytes the checkpoint route returned open, with this run's job key, to
    exactly what the earlier attempt saved (2026-09-06).

    The route hands the AGENT ciphertext -- it is a courier and could not read a
    checkpoint if it tried -- so the test opens it the way the next attempt's
    container does, rather than expecting plaintext off the wire."""
    from app.models import Job, JobKey, Run
    from app.sealing import key_from_b64, open_any

    async with client.session_factory() as s:
        run = await s.get(Run, run_id)
        job = await s.get(Job, run.job_id)
        row = await s.get(JobKey, job.id)
    assert row is not None, "a sealed job must have a key"
    return open_any(blob, key_from_b64(row.key_b64)) == expected


async def _bump_attempt(session_factory, run_id, to):
    """Simulate a re-dispatch: the reaper lost the run and a new claim bumped it."""
    async with session_factory() as s:
        run = await s.get(Run, run_id)
        run.attempt = to
        await s.commit()


async def _get_checkpoint(client, node, run_id, attempt):
    return await client.get(
        f"/agent/runs/{run_id}/checkpoint?attempt={attempt}", headers=_node_auth(node)
    )


# ===========================================================================
# THE RULE — tested on the query, not on a route
# ===========================================================================


async def test_query_allows_cross_attempt_checkpoints_and_refuses_results(
    session_factory,
):
    """Both directions of the one rule the feature rests on.

    The same run has an attempt-1 result and an attempt-1 checkpoint, and the run has
    since moved to attempt 2. Asked as attempt 2: the checkpoint is visible, the
    result is not. Nothing about a route is involved — this is the query itself, so
    the guard cannot be bypassed by adding a door later."""
    async with session_factory() as s:
        s.add(Artifact(
            run_id="r1", attempt=1, object_key="runs/r1/1/metrics.json",
            size=2, kind=KIND_RESULT, sha256="x",
        ))
        s.add(Artifact(
            run_id="r1", attempt=1, object_key="runs/r1/1/checkpoint",
            size=2, kind=KIND_CHECKPOINT, sha256="y",
        ))
        await s.commit()

        # Direction 1: a checkpoint written by attempt 1 IS readable at attempt 2.
        ckpt = (
            await s.execute(_artifact_query("r1", KIND_CHECKPOINT, 2))
        ).scalars().all()
        assert [a.object_key for a in ckpt] == ["runs/r1/1/checkpoint"]

        # Direction 2: a result written by attempt 1 is NOT readable at attempt 2.
        stale = (
            await s.execute(_artifact_query("r1", KIND_RESULT, 2))
        ).scalars().all()
        assert stale == []

        # ...and the same result IS readable at its own attempt, so direction 2 is
        # not passing for the wrong reason: what filtered it out was the attempt.
        own = (await s.execute(_artifact_query("r1", KIND_RESULT, 1))).scalars().all()
        assert [a.object_key for a in own] == ["runs/r1/1/metrics.json"]


# ===========================================================================
# ISOLATION — a checkpoint is never a result
# ===========================================================================


async def test_checkpoint_is_not_listed_as_a_result(client):
    node = await _register(client)
    a = await _claim(client, node)
    await _upload(client, node, a["run_id"], a["attempt"], "checkpoint", b"ck", "checkpoint")
    await _upload(client, node, a["run_id"], a["attempt"], "metrics.json", b"{}")

    listed = (await client.get(f"/runs/{a['run_id']}/artifacts")).json()
    assert [x["filename"] for x in listed] == ["metrics.json"]
    assert listed[0]["kind"] == KIND_RESULT


async def test_checkpoint_cannot_be_downloaded_as_a_result(client):
    """Even holding its id — the results door serves results, and refuses with the
    fence's own status code."""
    node = await _register(client)
    a = await _claim(client, node)
    up = await _upload(
        client, node, a["run_id"], a["attempt"], "checkpoint", b"ck", "checkpoint"
    )
    assert up.status_code == 200, up.text
    assert up.json()["kind"] == KIND_CHECKPOINT

    refused = await client.get(f"/artifacts/{up.json()['artifact_id']}/download")
    assert refused.status_code == 409, refused.text


# ===========================================================================
# THE HAPPY PATH — a later attempt resumes from an earlier one
# ===========================================================================


async def test_later_attempt_reads_the_earlier_attempts_checkpoint(
    client, session_factory
):
    """The point of the feature. Attempt 1 saves and its machine dies; attempt 2 —
    which may be a different machine — reads what attempt 1 left, with the digest of
    exactly those bytes so it can verify them before trusting them."""
    node = await _register(client)
    a = await _claim(client, node)
    body = b'{"epoch": 3}'
    await _upload(client, node, a["run_id"], a["attempt"], "checkpoint", body, "checkpoint")

    await _bump_attempt(session_factory, a["run_id"], a["attempt"] + 1)

    got = await _get_checkpoint(client, node, a["run_id"], a["attempt"] + 1)
    assert got.status_code == 200, got.text
    assert await _opens_to(client, a["run_id"], got.content, body)
    # The digest is of the bytes the control plane STORED, which since 2026-09-06 are
    # the sealed ones — a fact about what is in storage, which is what makes it worth
    # verifying a downloaded checkpoint against.
    assert got.headers["X-Checkpoint-Sha256"] == hashlib.sha256(got.content).hexdigest()
    assert got.headers["X-Checkpoint-Attempt"] == str(a["attempt"])


async def test_newest_checkpoint_wins_across_attempts(client, session_factory):
    node = await _register(client)
    a = await _claim(client, node)
    await _upload(client, node, a["run_id"], 1, "checkpoint", b"first", "checkpoint")
    await _bump_attempt(session_factory, a["run_id"], 2)
    await _upload(client, node, a["run_id"], 2, "checkpoint", b"second", "checkpoint")
    await _bump_attempt(session_factory, a["run_id"], 3)

    got = await _get_checkpoint(client, node, a["run_id"], 3)
    assert got.status_code == 200, got.text
    assert await _opens_to(client, a["run_id"], got.content, b"second")


# ===========================================================================
# THE FENCE — on the request, exactly like a status post
# ===========================================================================


async def test_checkpoint_unknown_run_404(client):
    node = await _register(client)
    r = await _get_checkpoint(client, node, "no-such-run", 1)
    assert r.status_code == 404, r.text


async def test_checkpoint_wrong_node_409(client):
    owner = await _register(client, "owner")
    other = await _register(client, "other")
    a = await _claim(client, owner)
    r = await _get_checkpoint(client, other, a["run_id"], a["attempt"])
    assert r.status_code == 409, r.text


async def test_checkpoint_stale_attempt_409(client, session_factory):
    """A node the control plane has moved past cannot pull the checkpoint for work
    that is no longer its own. Relaxing WHICH attempt the object came from does not
    relax WHO may ask for it."""
    node = await _register(client)
    a = await _claim(client, node)
    await _bump_attempt(session_factory, a["run_id"], a["attempt"] + 1)
    r = await _get_checkpoint(client, node, a["run_id"], a["attempt"])
    assert r.status_code == 409, r.text


async def test_checkpoint_requires_a_node_token(anon_client):
    node = await _register(anon_client)
    r = await anon_client.get(f"/agent/runs/{node['node_id']}/checkpoint?attempt=1")
    assert r.status_code == 401, r.text


# ===========================================================================
# ABSENT IS NOT AN ERROR
# ===========================================================================


async def test_no_checkpoint_is_204_not_404(client):
    """The ordinary case — a first attempt, or a workload that never saves. It has to
    read as "start at the beginning", which is what every run did before today."""
    node = await _register(client)
    a = await _claim(client, node)
    r = await _get_checkpoint(client, node, a["run_id"], a["attempt"])
    assert r.status_code == 204, r.text
    assert r.content == b""


async def test_a_result_is_not_offered_as_a_checkpoint(client, session_factory):
    """The other side of the isolation guard: uploading results does not accidentally
    give a later attempt something to resume from."""
    node = await _register(client)
    a = await _claim(client, node)
    await _upload(client, node, a["run_id"], a["attempt"], "metrics.json", b"{}")
    await _bump_attempt(session_factory, a["run_id"], a["attempt"] + 1)

    r = await _get_checkpoint(client, node, a["run_id"], a["attempt"] + 1)
    assert r.status_code == 204, r.text


async def test_unreadable_bytes_are_absent_not_an_error(client, mem_store):
    """Storage cannot produce the object. There is nothing to resume from, so the
    answer is the same as having none: start over. Failing the run over a lost
    checkpoint would trade repeated work for no work at all."""
    node = await _register(client)
    a = await _claim(client, node)
    await _upload(client, node, a["run_id"], a["attempt"], "checkpoint", b"ck", "checkpoint")
    mem_store._objs.clear()  # the bytes are gone; the row still says they exist

    r = await _get_checkpoint(client, node, a["run_id"], a["attempt"])
    assert r.status_code == 204, r.text


# ===========================================================================
# BOUNDED STORAGE + THE DIGEST
# ===========================================================================


async def test_saving_again_keeps_one_row_and_moves_the_digest(
    client, mem_store, session_factory
):
    """One checkpoint per attempt, stable name, overwritten in place — that is what
    bounds storage with no cleanup job. And the digest has to follow the new bytes:
    a row still describing the older ones could never be verified again, so every
    resume after the second save would silently fall back to starting over."""
    node = await _register(client)
    a = await _claim(client, node)
    first = await _upload(
        client, node, a["run_id"], a["attempt"], "checkpoint", b"epoch-1", "checkpoint"
    )
    later = b"epoch-2-longer"
    second = await _upload(
        client, node, a["run_id"], a["attempt"], "checkpoint", later, "checkpoint"
    )
    assert first.json()["artifact_id"] == second.json()["artifact_id"]  # one row

    async with session_factory() as s:
        rows = (
            await s.execute(_artifact_query(a["run_id"], KIND_CHECKPOINT, a["attempt"]))
        ).scalars().all()
    assert len(rows) == 1
    stored = mem_store.get_object(rows[0].object_key)
    # The digest is of the bytes the control plane STORED, which since 2026-09-06 are
    # the sealed ones — a fact about what is in storage, which is what makes it worth
    # verifying a downloaded checkpoint against.
    assert rows[0].size == len(stored) > len(later)
    assert rows[0].sha256 == hashlib.sha256(stored).hexdigest()
    assert len([k for k in mem_store._objs if k.endswith("/checkpoint")]) == 1

    got = await _get_checkpoint(client, node, a["run_id"], a["attempt"])
    assert await _opens_to(client, a["run_id"], got.content, later)
    assert got.headers["X-Checkpoint-Sha256"] == rows[0].sha256


async def test_digest_is_of_the_stored_bytes(client, session_factory, mem_store):
    node = await _register(client)
    a = await _claim(client, node)
    body = b'{"accuracy": 0.9}'
    up = await _upload(client, node, a["run_id"], a["attempt"], "metrics.json", body)
    async with session_factory() as s:
        art = await s.get(Artifact, up.json()["artifact_id"])
        assert art.sha256 == hashlib.sha256(mem_store.get_object(art.object_key)).hexdigest()


async def test_unknown_kind_is_422(client):
    node = await _register(client)
    a = await _claim(client, node)
    r = await _upload(
        client, node, a["run_id"], a["attempt"], "f.bin", b"x", "definitely-not-a-kind"
    )
    assert r.status_code == 422, r.text


# ===========================================================================
# COMPAT — an agent that predates `kind`
# ===========================================================================


async def test_upload_without_kind_stores_a_result(client, session_factory):
    """An older agent sends no `kind` field at all. Its file must still be a result,
    still be listed, and still be downloadable — nothing that worked yesterday may
    depend on a field that did not exist yesterday."""
    node = await _register(client)
    a = await _claim(client, node)
    up = await _upload(client, node, a["run_id"], a["attempt"], "metrics.json", b"{}")
    assert up.status_code == 200, up.text
    assert up.json()["kind"] == KIND_RESULT

    listed = (await client.get(f"/runs/{a['run_id']}/artifacts")).json()
    assert [x["filename"] for x in listed] == ["metrics.json"]
    dl = await client.get(f"/artifacts/{up.json()['artifact_id']}/download")
    assert dl.status_code == 200 and dl.content == b"{}"

    # ...and it gives a later attempt nothing to resume from, because it is a result.
    await _bump_attempt(session_factory, a["run_id"], a["attempt"] + 1)
    later = await _get_checkpoint(client, node, a["run_id"], a["attempt"] + 1)
    assert later.status_code == 204
