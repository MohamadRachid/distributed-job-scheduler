"""Checkpoint cleanup (2026-08-29) — one checkpoint per RUN, and none once it ends.

The supervisor's question on 28 August was about scale: what does a checkpoint cost
when there are thousands of runs? The honest answer before this change was worse
than it needed to be. A checkpoint is written to a stable name, so it was bounded
*within* an attempt — but a re-dispatched run left one behind per attempt, and a run
that finished left its last one behind for ever. Storage was "latest per attempt,
kept after the run ends". Nothing ever read those bytes again.

This closes both halves, and the interesting part of it is what must NOT be deleted:

  * TERMINAL — a run that reaches SUCCEEDED or FAILED has no checkpoint afterwards.
    Its result is stored, and a checkpoint is only ever a way of getting to a result.
  * REQUEUED — a run that reports FAILED and is sent back to PENDING (the W5c OOM
    escalation) KEEPS its checkpoint. This is the sharp edge: the agent posts the
    same word, `FAILED`, in both cases, and only the run's state *after* the
    escalation says which one happened. Deleting on the posted word rather than on
    the resulting state would throw away exactly the work the retry exists to save.
  * SUPERSEDED — a new attempt's checkpoint replaces the older attempts', so a run
    holds one, not one per attempt. The older one is deleted only after the new bytes
    have been read back out of storage and matched, because an unverifiable new
    checkpoint must never be the reason a verified old one is lost.
  * RESULTS — untouched. The delete is narrow by kind, and the result is the thing
    the run existed to produce.
  * THE FENCE — unmoved. A stale attempt's result is still refused. Cleanup deletes
    objects; it does not decide what may be read, and nothing here goes near the one
    query that does.
"""

import hashlib

from app.api.artifacts import KIND_CHECKPOINT, KIND_RESULT, _artifact_query
from app.models import Run, RunStatus
from conftest import seal_for_run


# --- helpers ----------------------------------------------------------------


def _specs(ram_mb=8192):
    return {"cpu_cores": 4, "has_gpu": False, "ram_mb": ram_mb, "capacity": 4, "agent_version": "0.12.0"}


def _auth(node):
    return {"Authorization": f"Bearer {node['token']}"}


async def _register(client, name="lab-pc-01", ram_mb=8192):
    r = await client.post(
        "/agent/register", json={"name": name, "specs": _specs(ram_mb)}
    )
    assert r.status_code == 200, r.text
    return r.json()


async def _claim(client, node):
    """Submit a job and heartbeat `node` so it owns the (attempt-1) run."""
    body = {
        "name": "j", "image": "fyp-dummy:latest", "entrypoint": ["python", "train.py"],
        "env": {}, "resource_reqs": {"needs_gpu": False}, "replicas": 1,
    }
    assert (await client.post("/jobs", json=body)).status_code == 200
    hb = await client.post(
        "/agent/heartbeat",
        headers=_auth(node),
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
        headers=_auth(node),
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


async def _status(client, node, run_id, attempt, state, exit_code=0, reason=None,
                  detail=None):
    body = {"attempt": attempt, "state": state, "exit_code": exit_code}
    if reason is not None:
        body["failure_reason"] = reason
        body["failure_detail"] = detail or "."
    return await client.post(
        f"/agent/runs/{run_id}/status", headers=_auth(node), json=body
    )


async def _bump_attempt(session_factory, run_id, to):
    """Simulate a re-dispatch: the reaper lost the run and a new claim bumped it."""
    async with session_factory() as s:
        run = await s.get(Run, run_id)
        run.attempt = to
        await s.commit()


async def _checkpoint_rows(session_factory, run_id):
    """Every checkpoint row for a run, whichever attempt wrote it. The attempt
    argument is inert for checkpoints — that is the cross-attempt rule — so this
    reads them all through the same query the product uses."""
    async with session_factory() as s:
        rows = (
            await s.execute(_artifact_query(run_id, KIND_CHECKPOINT, 0))
        ).scalars().all()
        return list(rows)


def _checkpoint_objects(mem_store, run_id):
    return sorted(
        k for k in mem_store._objs
        if k.startswith(f"runs/{run_id}/") and k.endswith("/checkpoint")
    )


# ===========================================================================
# (a) TERMINAL — the run ended, so the checkpoint goes
# ===========================================================================


async def test_terminal_success_deletes_the_checkpoint(
    client, mem_store, session_factory
):
    """The headline. A run that succeeded has a result; the intermediate state that
    got it there is of no use to anyone and must not be kept for ever."""
    node = await _register(client)
    a = await _claim(client, node)
    await _upload(client, node, a["run_id"], a["attempt"], "checkpoint", b"epoch-9",
                  KIND_CHECKPOINT)
    assert _checkpoint_objects(mem_store, a["run_id"]) != []

    done = await _status(client, node, a["run_id"], a["attempt"], "SUCCEEDED", 0)
    assert done.status_code == 200, done.text

    assert await _checkpoint_rows(session_factory, a["run_id"]) == []
    assert _checkpoint_objects(mem_store, a["run_id"]) == []


async def test_terminal_failure_deletes_the_checkpoint(
    client, mem_store, session_factory
):
    """The other terminal state. A run that is genuinely finished — failed and not
    being retried — is as done as one that succeeded."""
    node = await _register(client)
    a = await _claim(client, node)
    await _upload(client, node, a["run_id"], a["attempt"], "checkpoint", b"epoch-3",
                  KIND_CHECKPOINT)

    done = await _status(client, node, a["run_id"], a["attempt"], "FAILED", 1,
                         reason="APP_ERROR", detail="the program raised.")
    assert done.status_code == 200, done.text

    async with session_factory() as s:
        assert (await s.get(Run, a["run_id"])).status is RunStatus.FAILED
    assert await _checkpoint_rows(session_factory, a["run_id"]) == []
    assert _checkpoint_objects(mem_store, a["run_id"]) == []


async def test_terminal_cleanup_keeps_the_result(client, mem_store, session_factory):
    """The delete is narrow. What the run produced is the whole point of having run
    it; only the working state goes."""
    node = await _register(client)
    a = await _claim(client, node)
    await _upload(client, node, a["run_id"], a["attempt"], "checkpoint", b"ck",
                  KIND_CHECKPOINT)
    await _upload(client, node, a["run_id"], a["attempt"], "metrics.json", b'{"acc":1}')

    done = await _status(client, node, a["run_id"], a["attempt"], "SUCCEEDED", 0)
    assert done.status_code == 200, done.text

    listed = (await client.get(f"/runs/{a['run_id']}/artifacts")).json()
    assert [x["filename"] for x in listed] == ["metrics.json"]
    key = f"runs/{a['run_id']}/{a['attempt']}/metrics.json"
    # Sealed bytes (2026-09-06): the result is still there, and what is there is
    # ciphertext. The cleanup swept the checkpoint and left this alone, which is the
    # claim; what the object IS has not changed since it was uploaded.
    assert mem_store.get_object(key).startswith(b"FYPSEAL2")


async def test_terminal_cleanup_does_not_touch_another_runs_checkpoint(
    client, mem_store, session_factory
):
    """Scoped to the run that finished. A sweep that reached further would be a far
    more expensive bug than the one it was fixing."""
    node = await _register(client, "n1")
    a = await _claim(client, node)
    b = await _claim(client, node)
    assert a["run_id"] != b["run_id"]
    await _upload(client, node, a["run_id"], a["attempt"], "checkpoint", b"a",
                  KIND_CHECKPOINT)
    await _upload(client, node, b["run_id"], b["attempt"], "checkpoint", b"b",
                  KIND_CHECKPOINT)

    done = await _status(client, node, a["run_id"], a["attempt"], "SUCCEEDED", 0)
    assert done.status_code == 200, done.text

    assert await _checkpoint_rows(session_factory, a["run_id"]) == []
    assert len(await _checkpoint_rows(session_factory, b["run_id"])) == 1
    assert _checkpoint_objects(mem_store, b["run_id"]) != []


# ===========================================================================
# THE SHARP EDGE — FAILED does not always mean finished
# ===========================================================================


async def test_requeued_after_oom_keeps_its_checkpoint(
    client, mem_store, session_factory
):
    """A proven RAM kill on a machine too small requeues the run to a stronger one
    (W5c). The agent's post says FAILED, and the run's state a moment later says
    PENDING — it is being retried, not finished. Its checkpoint is exactly what the
    retry should resume from, so deleting it here would make the recovery this
    project is built on quietly worse than it was.

    This is why the cleanup keys on the run's state AFTER the escalation has run,
    never on the word the agent posted."""
    weak = await _register(client, "weak", ram_mb=512)
    a = await _claim(client, weak)
    await _upload(client, weak, a["run_id"], a["attempt"], "checkpoint", b"epoch-5",
                  KIND_CHECKPOINT)
    # A stronger machine exists in the pool, so the give-up rule says "requeue".
    await _register(client, "strong", ram_mb=4096)

    oom = await _status(client, weak, a["run_id"], a["attempt"], "FAILED", 137,
                        reason="OOM_KILLED", detail="the kernel killed it.")
    assert oom.status_code == 200, oom.text

    async with session_factory() as s:
        run = await s.get(Run, a["run_id"])
        assert run.status is RunStatus.PENDING, "precondition: this must be a requeue"

    assert len(await _checkpoint_rows(session_factory, a["run_id"])) == 1
    assert _checkpoint_objects(mem_store, a["run_id"]) != []


# ===========================================================================
# (b) SUPERSEDED — one per run, not one per attempt
# ===========================================================================


async def test_second_attempt_upload_leaves_one_checkpoint(
    client, mem_store, session_factory
):
    """Storage stops growing with the number of attempts. Attempt 2 saving means
    attempt 1's checkpoint can never be returned again — the read takes the newest —
    so keeping it is pure cost."""
    node = await _register(client)
    a = await _claim(client, node)
    await _upload(client, node, a["run_id"], 1, "checkpoint", b"first", KIND_CHECKPOINT)
    await _bump_attempt(session_factory, a["run_id"], 2)
    await _upload(client, node, a["run_id"], 2, "checkpoint", b"second", KIND_CHECKPOINT)

    rows = await _checkpoint_rows(session_factory, a["run_id"])
    assert len(rows) == 1
    assert rows[0].attempt == 2
    assert _checkpoint_objects(mem_store, a["run_id"]) == [
        f"runs/{a['run_id']}/2/checkpoint"
    ]


async def test_supersede_leaves_the_newest_readable(client, session_factory):
    """Deleting the older one must not cost the feature it exists to serve: a third
    attempt still resumes, from attempt 2's bytes."""
    node = await _register(client)
    a = await _claim(client, node)
    await _upload(client, node, a["run_id"], 1, "checkpoint", b"first", KIND_CHECKPOINT)
    await _bump_attempt(session_factory, a["run_id"], 2)
    await _upload(client, node, a["run_id"], 2, "checkpoint", b"second", KIND_CHECKPOINT)
    await _bump_attempt(session_factory, a["run_id"], 3)

    got = await client.get(
        f"/agent/runs/{a['run_id']}/checkpoint?attempt=3", headers=_auth(node)
    )
    assert got.status_code == 200, got.text
    assert await _opens_to(client, a["run_id"], got.content, b"second")
    # The digest is of the bytes the control plane STORED, which since 2026-09-06 are
    # the sealed ones — a fact about what is in storage, which is what makes it worth
    # verifying a downloaded checkpoint against.
    assert got.headers["X-Checkpoint-Sha256"] == hashlib.sha256(got.content).hexdigest()


async def test_supersede_is_skipped_when_the_new_bytes_cannot_be_read_back(
    client, mem_store, session_factory
):
    """The safety direction. The delete is authorised by reading the new checkpoint
    back out of storage and matching its digest; if that read cannot be trusted, the
    older checkpoint stays. Losing a verified checkpoint on the word of an
    unverifiable one is the one outcome worth writing a test against."""
    node = await _register(client)
    a = await _claim(client, node)
    await _upload(client, node, a["run_id"], 1, "checkpoint", b"first", KIND_CHECKPOINT)
    await _bump_attempt(session_factory, a["run_id"], 2)

    real_get = mem_store.get_object

    def _corrupt(key):
        if key.endswith("/2/checkpoint"):
            return b"not-what-was-stored"
        return real_get(key)

    mem_store.get_object = _corrupt
    try:
        up = await _upload(client, node, a["run_id"], 2, "checkpoint", b"second",
                           KIND_CHECKPOINT)
    finally:
        mem_store.get_object = real_get
    assert up.status_code == 200, up.text  # the upload itself still succeeds

    assert _checkpoint_objects(mem_store, a["run_id"]) == [
        f"runs/{a['run_id']}/1/checkpoint",
        f"runs/{a['run_id']}/2/checkpoint",
    ]
    assert len(await _checkpoint_rows(session_factory, a["run_id"])) == 2


# ===========================================================================
# (c) THE CONTROL — a run that never had a checkpoint is unaffected
# ===========================================================================


async def test_run_with_no_checkpoint_is_unaffected(client, session_factory):
    """Absent is not an error, and it was not an error before this change either.
    A workload that never saves has to finish exactly as it did yesterday — this is
    every job on the platform that has not adopted the two-line resume contract."""
    node = await _register(client)
    a = await _claim(client, node)
    await _upload(client, node, a["run_id"], a["attempt"], "metrics.json", b"{}")

    done = await _status(client, node, a["run_id"], a["attempt"], "SUCCEEDED", 0)
    assert done.status_code == 200, done.text
    assert done.json() == {"accepted": True, "run_status": "SUCCEEDED"}

    async with session_factory() as s:
        run = await s.get(Run, a["run_id"])
        assert run.status is RunStatus.SUCCEEDED
        assert run.exit_code == 0
        assert run.failure_reason is None
    listed = (await client.get(f"/runs/{a['run_id']}/artifacts")).json()
    assert [x["filename"] for x in listed] == ["metrics.json"]


async def test_a_second_terminal_post_is_still_idempotent(client, session_factory):
    """The cleanup runs inside the status handler, so the handler's own guarantees
    have to survive it. A repeat of an accepted terminal post is still a plain 200
    that changes nothing — including now that there is nothing left to delete."""
    node = await _register(client)
    a = await _claim(client, node)
    await _upload(client, node, a["run_id"], a["attempt"], "checkpoint", b"ck",
                  KIND_CHECKPOINT)
    first = await _status(client, node, a["run_id"], a["attempt"], "SUCCEEDED", 0)
    assert first.status_code == 200, first.text

    again = await _status(client, node, a["run_id"], a["attempt"], "SUCCEEDED", 0)
    assert again.status_code == 200, again.text
    assert again.json()["run_status"] == "SUCCEEDED"


# ===========================================================================
# (d) THE FENCE — unmoved
# ===========================================================================


async def test_stale_attempt_result_is_still_refused(client, session_factory):
    """Nothing in the cleanup goes near `_artifact_query`, and this says so from the
    outside: a result written by an attempt the platform has moved past is still
    invisible to the results door, and the checkpoint rule is still the one exception
    to that. The fence is what the whole project rests on; a storage change may not
    move it."""
    node = await _register(client)
    a = await _claim(client, node)
    up = await _upload(client, node, a["run_id"], 1, "metrics.json", b"stale")
    await _bump_attempt(session_factory, a["run_id"], 2)

    listed = (await client.get(f"/runs/{a['run_id']}/artifacts")).json()
    assert listed == []
    refused = await client.get(f"/artifacts/{up.json()['artifact_id']}/download")
    assert refused.status_code == 409, refused.text

    async with session_factory() as s:
        stale = (
            await s.execute(_artifact_query(a["run_id"], KIND_RESULT, 2))
        ).scalars().all()
        assert list(stale) == []


async def test_a_stale_attempt_cannot_trigger_the_cleanup(
    client, mem_store, session_factory
):
    """A presumed-dead machine reporting its old execution is refused at the fence,
    and is refused BEFORE anything is deleted — so the live attempt's checkpoint is
    still there. Otherwise a zombie could destroy the working state of the run that
    replaced it, which is a worse thing than the stale result the fence exists to
    stop."""
    node = await _register(client)
    a = await _claim(client, node)
    await _upload(client, node, a["run_id"], 1, "checkpoint", b"live", KIND_CHECKPOINT)
    await _bump_attempt(session_factory, a["run_id"], 2)

    zombie = await _status(client, node, a["run_id"], 1, "SUCCEEDED", 0)
    assert zombie.status_code == 409, zombie.text

    assert len(await _checkpoint_rows(session_factory, a["run_id"])) == 1
    assert _checkpoint_objects(mem_store, a["run_id"]) != []
