"""Storage quota policy — the retained cap, acceptance, roles and the release valve.

The scratch half of the policy is enforced on the worker and is tested in
`agent/tests/test_scratch_quota.py`; this file is the control plane's half.

One test per rule of the policy (brief §3), named after the rule it pins:

  R1  a user who has not accepted their limits cannot submit, on EITHER door
  R2  a user already at their retained cap cannot submit, on either door
  R3  an artefact upload that would cross the cap is refused, and nothing is stored
  R4  a refused upload makes that attempt FAILED whatever the agent posts, and the
      run is NOT re-dispatched
  R5  checkpoints count, and a repeat is charged only its growth
  R6  a user can free space, and only once the job's runs have all finished
  R11 GET /me tells the user where they stand

plus the two things the policy rests on that are easy to get wrong and impossible to
notice: a STALE attempt must never consume quota (the fence comes first), and an
UNOWNED job must not be checked at all (the chaos test submits through this handler
with no user).
"""

from datetime import datetime, timezone

from app.models import Artifact, Job, JobKey, Node, Run, RunStatus, Tier, User
from app.quota import accept_limits

from conftest import TEST_PASSWORD, TEST_USERNAME, seal_for_run

SPECS = {"cpu_cores": 4, "has_gpu": False, "ram_mb": 8192, "capacity": 4, "agent_version": "0.12.0"}
_MB = 1024 * 1024


# --- helpers ----------------------------------------------------------------


def _node_auth(node):
    return {"Authorization": f"Bearer {node['token']}"}


async def _register(client, name="lab-pc-01"):
    r = await client.post("/agent/register", json={"name": name, "specs": SPECS})
    assert r.status_code == 200, r.text
    return r.json()


async def _submit(client, **overrides):
    body = {
        "name": "quota-demo",
        "image": "fyp-dummy:latest",
        "entrypoint": ["python", "train.py"],
        "env": {},
        "resource_reqs": {"needs_gpu": False},
        "replicas": 1,
    }
    body.update(overrides)
    return await client.post("/jobs", json=body)


async def _claim(client, node):
    """Submit a job and heartbeat `node` so it owns the run at attempt 1."""
    r = await _submit(client)
    assert r.status_code == 200, r.text
    hb = await client.post(
        "/agent/heartbeat",
        headers=_node_auth(node),
        json={"node_id": node["node_id"], "status": "idle", "running": []},
    )
    assert hb.status_code == 200, hb.text
    return hb.json()["assignments"][0]


async def _upload(client, node, run_id, attempt, filename, content, kind="result"):
    # 2026-09-06: a sealed job's outputs are sealed inside the container, and the
    # control plane refuses anything else. This helper stands in for a container, so
    # it seals with the run's real key (conftest.seal_for_run) -- which is also what
    # keeps the download assertions honest: they get plaintext back because the seal
    # opens, not because nothing was sealed.
    content = await seal_for_run(client.session_factory, run_id, content)
    return await client.post(
        f"/agent/runs/{run_id}/artifacts",
        headers=_node_auth(node),
        data={"attempt": str(attempt), "filename": filename, "kind": kind},
        files={"file": (filename, content, "application/octet-stream")},
    )


async def _as_legacy_job(session_factory, job_id):
    """Mark a job UNSEALED — i.e. make it one of the jobs submitted before
    2026-09-06 (sealed by default).

    Needed by the tests below that are about the OLDER agent-version guards. Every
    job created today is sealed, and a sealed job is invisible to an agent older than
    0.12.0, so an 0.9.0 agent would be offered nothing at all and the test would pass
    for the wrong reason. The population those older guards still serve is exactly
    the jobs that predate sealing, and this is how a test builds one."""
    async with session_factory() as s2:
        job = await s2.get(Job, job_id)
        job.sealed = False
        await s2.commit()


async def _set_caps(session_factory, retained_mb, scratch_mb=64, reaccept=True):
    """Shrink the standard tier to something a test can cross in milliseconds, and
    (by default) re-stamp the test user's acceptance against the new figures.

    The re-stamp is necessary, and WHY it is necessary is itself the design working:
    acceptance records the numbers that were agreed to, so moving the numbers
    withdraws it. `reaccept=False` is how the test below proves exactly that."""
    async with session_factory() as s:
        tier = await s.get(Tier, "standard")
        tier.retained_cap_bytes = int(retained_mb * _MB)
        tier.scratch_cap_bytes = int(scratch_mb * _MB)
        if reaccept:
            user = (
                await s.execute(
                    __import__("sqlalchemy").select(User).where(
                        User.username == TEST_USERNAME
                    )
                )
            ).scalar_one()
            accept_limits(user, tier)
        await s.commit()


# ===========================================================================
# R11 — the user can see where they stand
# ===========================================================================


async def test_me_reports_tier_caps_usage_and_acceptance(client):
    me = await client.get("/me")
    assert me.status_code == 200, me.text
    body = me.json()
    assert body["username"] == TEST_USERNAME
    assert body["tier"] == "standard"
    assert body["retained_cap_mb"] > 0 and body["scratch_cap_mb"] > 0
    assert body["retained_used_mb"] == 0.0
    assert body["limits_accepted"] is True


async def test_me_usage_is_the_same_sum_the_refusal_uses(client, mem_store):
    """The bar on the screen and the number that refuses an upload are one query.

    If these were two different calculations they would drift, and a user would be
    refused at a figure the interface had never shown them."""
    node = await _register(client)
    a = await _claim(client, node)
    payload = b"z" * 4096
    up = await _upload(client, node, a["run_id"], a["attempt"], "metrics.json", payload)
    assert up.status_code == 200, up.text

    me = (await client.get("/me")).json()
    assert me["retained_used_mb"] == round(len(payload) / _MB, 3)


async def test_editing_the_tier_numbers_withdraws_acceptance(client, session_factory):
    """Acceptance is to NUMBERS, not to a word.

    Nothing resets a flag here — `limits_accepted` compares what was agreed against
    what now applies, so a figure that moves withdraws the agreement by arithmetic."""
    assert (await client.get("/me")).json()["limits_accepted"] is True
    await _set_caps(session_factory, retained_mb=10, reaccept=False)
    assert (await client.get("/me")).json()["limits_accepted"] is False

    # And the door is shut until it is agreed again (R1).
    refused = await _submit(client)
    assert refused.status_code == 403, refused.text
    assert refused.json()["detail"]["reason"] == "LIMITS_NOT_ACCEPTED"

    again = await client.post("/me/accept-limits")
    assert again.status_code == 200 and again.json()["limits_accepted"] is True
    assert (await _submit(client)).status_code == 200


# ===========================================================================
# R1 / R2 — the two submission doors
# ===========================================================================


async def test_r1_unaccepted_user_cannot_submit_on_either_door(
    client, session_factory
):
    await _set_caps(session_factory, retained_mb=10, reaccept=False)

    plain = await _submit(client)
    assert plain.status_code == 403
    assert plain.json()["detail"]["reason"] == "LIMITS_NOT_ACCEPTED"

    private = await client.post(
        "/jobs/private",
        data={"spec": '{"name":"p","image":"i","entrypoint":["python","x.py"]}'},
        files={"file": ("in.bin", b"hello", "application/octet-stream")},
    )
    assert private.status_code == 403
    assert private.json()["detail"]["reason"] == "LIMITS_NOT_ACCEPTED"


async def test_r2_a_full_user_cannot_submit(client, mem_store, session_factory):
    """Submission catches the user who is ALREADY full — before a worker spends
    hours on a job whose result could not be stored."""
    await _set_caps(session_factory, retained_mb=1)
    node = await _register(client)
    a = await _claim(client, node)
    # Fill the cap EXACTLY. Sixty bytes of the megabyte are the seal itself
    # (2026-09-06): a 32-byte header and one nonce-plus-tag on the single piece. What
    # is charged is the sealed length, because sealed bytes are what the platform
    # keeps — so the plaintext that exactly fills a 1 MB cap is 60 bytes short of a
    # megabyte.
    up = await _upload(
        client, node, a["run_id"], a["attempt"], "big.bin", b"x" * (_MB - 60)
    )
    assert up.status_code == 200, up.text

    refused = await _submit(client)
    assert refused.status_code == 403, refused.text
    body = refused.json()["detail"]
    assert body["reason"] == "STORAGE_QUOTA_EXCEEDED"
    assert body["used_mb"] == 1.0 and body["cap_mb"] == 1.0


async def test_r2_private_input_that_would_not_fit_is_refused_and_stores_nothing(
    client, mem_store, session_factory
):
    """P1b. The refusal lands BEFORE anything is stored, so a refused submission
    leaves no key row and no object — not a half-created job whose input is somewhere
    but whose key is not."""
    await _set_caps(session_factory, retained_mb=1)
    node = await _register(client)
    a = await _claim(client, node)
    # 0.9 MB used against a 1 MB cap.
    assert (
        await _upload(
            client, node, a["run_id"], a["attempt"], "used.bin", b"x" * int(0.9 * _MB)
        )
    ).status_code == 200

    before_keys = set(mem_store._objs)
    # Every job has its own key since 2026-09-06, including the one `_claim` just
    # submitted — so the claim this test makes is that the REFUSED submission added
    # none, not that the table is empty.
    async with session_factory() as s0:
        import sqlalchemy as sa

        keys_before = (
            await s0.execute(sa.select(sa.func.count()).select_from(JobKey))
        ).scalar_one()
    too_big = await client.post(
        "/jobs/private",
        data={"spec": '{"name":"p","image":"i","entrypoint":["python","x.py"]}'},
        files={"file": ("in.bin", b"y" * int(0.5 * _MB), "application/octet-stream")},
    )
    assert too_big.status_code == 413, too_big.text
    assert too_big.json()["detail"]["reason"] == "STORAGE_QUOTA_EXCEEDED"
    assert set(mem_store._objs) == before_keys
    assert not [k for k in mem_store._objs if k.startswith("inputs/")]

    async with session_factory() as s:
        import sqlalchemy as sa

        assert (
            await s.execute(sa.select(sa.func.count()).select_from(JobKey))
        ).scalar_one() == keys_before

    # A small one fits, and the usage rises by exactly the SEALED size.
    before_used = (await client.get("/me")).json()["retained_used_mb"]
    ok = await client.post(
        "/jobs/private",
        data={"spec": '{"name":"p","image":"i","entrypoint":["python","x.py"]}'},
        files={"file": ("in.bin", b"y" * 1024, "application/octet-stream")},
    )
    assert ok.status_code == 200, ok.text
    async with session_factory() as s:
        job = await s.get(Job, ok.json()["job_id"])
        sealed = job.input_size_bytes
        assert sealed and sealed >= 1024  # nonce + tag make the seal a little larger
    after_used = (await client.get("/me")).json()["retained_used_mb"]
    assert round(after_used - before_used, 3) == round(sealed / _MB, 3)


async def test_scratch_ask_above_the_tier_cap_is_422_never_a_silent_trim(
    client, session_factory
):
    await _set_caps(session_factory, retained_mb=100, scratch_mb=64)
    too_much = await _submit(
        client, resource_reqs={"needs_gpu": False, "scratch_mb": 65}
    )
    assert too_much.status_code == 422, too_much.text
    assert "64" in too_much.json()["detail"]
    assert (
        await _submit(client, resource_reqs={"needs_gpu": False, "scratch_mb": 64})
    ).status_code == 200


async def test_the_private_door_is_held_to_the_tier_like_every_other(
    client, session_factory
):
    """The RAM folder is gone (2026-09-06), and with it the second scratch ceiling.

    This door used to cap a scratch ask at `PRIVATE_TMPFS_MB`, because a private run's
    working space really was a RAM folder of that size. Its runs now get the ordinary
    three folders on disk, so the one ceiling that applies is the owner's tier — the
    same number, checked in the same place, for every job on every door. An ask over
    it is still a 422 and never a silent trim."""
    await _set_caps(session_factory, retained_mb=100, scratch_mb=256)
    r = await client.post(
        "/jobs/private",
        data={
            "spec": (
                '{"name":"p","image":"i","entrypoint":["python","x.py"],'
                '"resource_reqs":{"scratch_mb":512}}'
            )
        },
        files={"file": ("in.bin", b"hello", "application/octet-stream")},
    )
    assert r.status_code == 422, r.text
    assert "your tier" in r.json()["detail"]

    # And an ask INSIDE the tier is accepted, where the old ceiling of 256 MB would
    # have been the thing deciding it rather than the tier.
    ok = await client.post(
        "/jobs/private",
        data={
            "spec": (
                '{"name":"p","image":"i","entrypoint":["python","x.py"],'
                '"resource_reqs":{"scratch_mb":200}}'
            )
        },
        files={"file": ("in.bin", b"hello", "application/octet-stream")},
    )
    assert ok.status_code == 200, ok.text


# ===========================================================================
# R3 / R5 — the upload door
# ===========================================================================


async def test_r3_upload_over_the_cap_is_refused_and_nothing_is_stored(
    client, mem_store, session_factory
):
    """P1. Refused with 413 STORAGE_QUOTA_EXCEEDED; the object store is untouched;
    no artefact row appears; and the run's ATTEMPT is not moved by the refusal."""
    await _set_caps(session_factory, retained_mb=1)
    node = await _register(client)
    a = await _claim(client, node)

    before = set(mem_store._objs)
    refused = await _upload(
        client, node, a["run_id"], a["attempt"], "out.bin", b"x" * (2 * _MB)
    )
    assert refused.status_code == 413, refused.text
    body = refused.json()["detail"]
    assert body["reason"] == "STORAGE_QUOTA_EXCEEDED"
    assert body["cap_mb"] == 1.0 and body["file_mb"] == 2.0

    assert set(mem_store._objs) == before
    listing = await client.get(f"/runs/{a['run_id']}/artifacts")
    assert listing.json() == []
    async with session_factory() as s:
        run = await s.get(Run, a["run_id"])
        assert run.attempt == a["attempt"]  # the refusal is not a re-dispatch
        assert run.quota_refused_at is not None

    # And a file that fits still goes through.
    ok = await _upload(
        client, node, a["run_id"], a["attempt"], "small.bin", b"x" * 1024
    )
    assert ok.status_code == 200, ok.text


async def test_a_stale_attempt_can_never_consume_quota(
    client, mem_store, session_factory
):
    """The fence comes FIRST. A zombie execution the platform has already given up on
    is refused 409 before the quota check is reached, so a machine nobody is waiting
    for cannot spend the storage of a user whose work has moved on."""
    await _set_caps(session_factory, retained_mb=1)
    node = await _register(client)
    a = await _claim(client, node)
    async with session_factory() as s:
        run = await s.get(Run, a["run_id"])
        run.attempt += 1  # re-dispatched underneath the old execution
        await s.commit()

    stale = await _upload(
        client, node, a["run_id"], a["attempt"], "out.bin", b"x" * (2 * _MB)
    )
    assert stale.status_code == 409, stale.text
    async with session_factory() as s:
        run = await s.get(Run, a["run_id"])
        assert run.quota_refused_at is None  # not even marked — it never got that far


async def test_r5_a_repeated_checkpoint_is_charged_only_its_growth(
    client, mem_store, session_factory
):
    """A checkpoint is written to a stable object key, so a repeat REFRESHES one row.
    Charging the whole file every time would refuse a run for space it was not using."""
    await _set_caps(session_factory, retained_mb=1)
    node = await _register(client)
    a = await _claim(client, node)

    half = b"c" * int(0.6 * _MB)
    first = await _upload(
        client, node, a["run_id"], a["attempt"], "state", half, kind="checkpoint"
    )
    assert first.status_code == 200, first.text

    # The same size again: it replaces itself, so it still fits even though
    # 0.6 + 0.6 would not.
    again = await _upload(
        client, node, a["run_id"], a["attempt"], "state", half, kind="checkpoint"
    )
    assert again.status_code == 200, again.text
    assert (await client.get("/me")).json()["retained_used_mb"] == round(
        len(half) / _MB, 3
    )

    # Growing past the cap IS refused — only the growth is free, not the file.
    grown = await _upload(
        client,
        node,
        a["run_id"],
        a["attempt"],
        "state",
        b"c" * int(1.2 * _MB),
        kind="checkpoint",
    )
    assert grown.status_code == 413, grown.text
    assert grown.json()["detail"]["reason"] == "STORAGE_QUOTA_EXCEEDED"


async def test_an_unowned_job_is_never_quota_checked(
    client, mem_store, session_factory
):
    """The chaos test submits through this same handler with no user attached. No
    user means no tier and nothing to enforce — said out loud here so that "quota did
    not apply" is a property with a test rather than a hole nobody looked at."""
    await _set_caps(session_factory, retained_mb=1)
    node = await _register(client)
    a = await _claim(client, node)
    async with session_factory() as s:
        run = await s.get(Run, a["run_id"])
        job = await s.get(Job, run.job_id)
        job.user_id = None
        await s.commit()

    huge = await _upload(
        client, node, a["run_id"], a["attempt"], "out.bin", b"x" * (5 * _MB)
    )
    assert huge.status_code == 200, huge.text


# ===========================================================================
# R4 — the control plane decides the outcome, not the agent
# ===========================================================================


async def test_r4_a_refused_upload_fails_the_attempt_whatever_the_agent_posts(
    client, session_factory
):
    await _set_caps(session_factory, retained_mb=1)
    node = await _register(client)
    a = await _claim(client, node)
    assert (
        await _upload(
            client, node, a["run_id"], a["attempt"], "out.bin", b"x" * (2 * _MB)
        )
    ).status_code == 413

    # The agent claims success anyway. The platform does not take its word for it.
    posted = await client.post(
        f"/agent/runs/{a['run_id']}/status",
        headers=_node_auth(node),
        json={"attempt": a["attempt"], "state": "SUCCEEDED", "exit_code": 0},
    )
    assert posted.status_code == 200, posted.text
    assert posted.json()["run_status"] == "FAILED"

    runs = (await client.get(f"/jobs/{(await client.get('/jobs')).json()[0]['job_id']}/runs")).json()
    row = next(r for r in runs if r["run_id"] == a["run_id"])
    assert row["status"] == "FAILED"
    assert row["failure_reason"] == "STORAGE_QUOTA_EXCEEDED"
    assert "out.bin" in row["failure_detail"]


async def test_r4_a_quota_failure_is_not_re_dispatched(client, session_factory):
    """It is the USER'S cap, not the machine's: another machine would fill the same
    quota with the same bytes. Same rule W5c already applies to a kill at the user's
    own memory limit."""
    await _set_caps(session_factory, retained_mb=1)
    node = await _register(client)
    a = await _claim(client, node)
    await _upload(client, node, a["run_id"], a["attempt"], "out.bin", b"x" * (2 * _MB))
    await client.post(
        f"/agent/runs/{a['run_id']}/status",
        headers=_node_auth(node),
        json={"attempt": a["attempt"], "state": "FAILED", "exit_code": 1},
    )
    async with session_factory() as s:
        run = await s.get(Run, a["run_id"])
        assert run.status is RunStatus.FAILED
        assert run.node_id == node["node_id"]  # terminal where it fell, not requeued

    # A second heartbeat must not hand it out again.
    hb = await client.post(
        "/agent/heartbeat",
        headers=_node_auth(node),
        json={"node_id": node["node_id"], "status": "idle", "running": []},
    )
    assert hb.json()["assignments"] == []


async def test_a_requeued_attempt_does_not_inherit_the_old_ones_refusal(
    client, session_factory
):
    """A storage refusal belongs to the ATTEMPT it happened on, not to the run for ever.

    `quota_refused_at` is a column on `runs`, so the hole this closes is real and
    quiet: an attempt whose upload was refused, and whose lease then expired before it
    could report, is reaped LOST and requeued — and without clearing the stamp the NEXT
    attempt would be failed for a decision made about work that no longer exists. The
    new attempt may be writing a smaller file, or its owner may have freed space in
    between, and neither would save it.

    Cleared on the RUNNING post, which is the one universal signal that a fresh attempt
    has begun whatever path it arrived by — the same argument that already clears a
    stale loss reason on that line."""
    await _set_caps(session_factory, retained_mb=1)
    node = await _register(client, "first-machine")
    a = await _claim(client, node)
    refused = await _upload(
        client, node, a["run_id"], a["attempt"], "out.bin", b"x" * (2 * _MB)
    )
    assert refused.status_code == 413, refused.text

    # The lease expires before the agent can report, so the reaper requeues it. Done
    # here by hand rather than by waiting for a real lease, exactly as the W5 tests do.
    async with session_factory() as s:
        run = await s.get(Run, a["run_id"])
        assert run.quota_refused_at is not None
        run.status = RunStatus.PENDING
        run.node_id = None
        run.lease_expires_at = None
        await s.commit()

    second = await _register(client, "second-machine")
    hb = await client.post(
        "/agent/heartbeat",
        headers=_node_auth(second),
        json={"node_id": second["node_id"], "status": "idle", "running": []},
    )
    b = next(x for x in hb.json()["assignments"] if x["run_id"] == a["run_id"])
    assert b["attempt"] == a["attempt"] + 1

    running = await client.post(
        f"/agent/runs/{b['run_id']}/status",
        headers=_node_auth(second),
        json={"attempt": b["attempt"], "state": "RUNNING"},
    )
    assert running.status_code == 200, running.text
    async with session_factory() as s:
        run = await s.get(Run, a["run_id"])
        assert run.quota_refused_at is None and run.quota_refused_detail is None

    # A file that fits is stored, and SUCCEEDED is honoured rather than overwritten.
    ok = await _upload(
        client, second, b["run_id"], b["attempt"], "small.bin", b"y" * 4096
    )
    assert ok.status_code == 200, ok.text
    posted = await client.post(
        f"/agent/runs/{b['run_id']}/status",
        headers=_node_auth(second),
        json={"attempt": b["attempt"], "state": "SUCCEEDED", "exit_code": 0},
    )
    assert posted.json()["run_status"] == "SUCCEEDED", posted.text


# ===========================================================================
# R6 — the release valve
# ===========================================================================


async def test_r6_releasing_storage_frees_objects_rows_and_quota(
    client, mem_store, session_factory
):
    await _set_caps(session_factory, retained_mb=1)
    node = await _register(client)
    a = await _claim(client, node)
    await _upload(client, node, a["run_id"], a["attempt"], "r.bin", b"x" * int(0.5 * _MB))
    await _upload(
        client, node, a["run_id"], a["attempt"], "state", b"c" * 2048, kind="checkpoint"
    )
    await client.post(
        f"/agent/runs/{a['run_id']}/status",
        headers=_node_auth(node),
        json={"attempt": a["attempt"], "state": "SUCCEEDED", "exit_code": 0},
    )
    job_id = (await client.get("/jobs")).json()[0]["job_id"]
    used_before = (await client.get("/me")).json()["retained_used_mb"]
    assert used_before > 0

    freed = await client.delete(f"/jobs/{job_id}/storage")
    assert freed.status_code == 200, freed.text
    assert freed.json()["freed_mb"] > 0

    assert (await client.get("/me")).json()["retained_used_mb"] == 0.0
    async with session_factory() as s:
        import sqlalchemy as sa

        left = (
            await s.execute(sa.select(sa.func.count()).select_from(Artifact))
        ).scalar_one()
        assert left == 0
    assert not [k for k in mem_store._objs if k.startswith(f"runs/{a['run_id']}/")]

    # And the user can submit again — which is the whole point of a release valve.
    assert (await _submit(client)).status_code == 200


async def test_r6_refuses_while_a_run_is_still_in_flight(client, session_factory):
    """Deleting the results of a job that is still executing would race the very
    upload producing them."""
    await _set_caps(session_factory, retained_mb=10)
    node = await _register(client)
    await _claim(client, node)  # the run is ASSIGNED, so the job is still in flight
    job_id = (await client.get("/jobs")).json()[0]["job_id"]
    busy = await client.delete(f"/jobs/{job_id}/storage")
    assert busy.status_code == 409, busy.text
    assert "still in flight" in busy.json()["detail"]


async def test_r6_and_crypto_shred_are_different_actions(
    client, mem_store, session_factory
):
    """Crypto-shred removes READABILITY and deliberately leaves the sealed object
    where it is. R6 removes BYTES. Either may follow the other."""
    await _set_caps(session_factory, retained_mb=10)
    created = await client.post(
        "/jobs/private",
        data={"spec": '{"name":"p","image":"i","entrypoint":["python","x.py"]}'},
        files={"file": ("in.bin", b"secret bytes", "application/octet-stream")},
    )
    assert created.status_code == 200, created.text
    job_id = created.json()["job_id"]
    key = f"inputs/{job_id}/input.bin"
    assert key in mem_store._objs

    shred = await client.delete(f"/jobs/{job_id}/key")
    assert shred.status_code == 200 and shred.json()["shredded"] is True
    assert key in mem_store._objs  # still there — that IS the demonstration

    async with session_factory() as s:  # the runs must be terminal first
        import sqlalchemy as sa

        for run in (
            await s.execute(sa.select(Run).where(Run.job_id == job_id))
        ).scalars():
            run.status = RunStatus.FAILED
            run.finished_at = datetime.now(timezone.utc)
        await s.commit()

    released = await client.delete(f"/jobs/{job_id}/storage")
    assert released.status_code == 200, released.text
    assert key not in mem_store._objs  # now the bytes are gone too
    assert (await client.get("/me")).json()["retained_used_mb"] == 0.0


# ===========================================================================
# Roles — P1c
# ===========================================================================


async def _login(anon_client, username, password):
    r = await anon_client.post(
        "/auth/login", json={"username": username, "password": password}
    )
    assert r.status_code == 200, r.text
    return {"Authorization": f"Bearer {r.json()['token']}"}


async def test_admin_creates_a_user_who_starts_unaccepted(anon_client):
    admin = await _login(anon_client, TEST_USERNAME, TEST_PASSWORD)
    made = await anon_client.post(
        "/users",
        headers=admin,
        json={"username": "bob", "password": "bob-pass-123", "tier": "limited"},
    )
    assert made.status_code == 200, made.text
    assert made.json()["tier"] == "limited"
    assert made.json()["is_admin"] is False
    # An admin can choose which numbers apply to someone; they cannot agree for them.
    assert made.json()["limits_accepted"] is False

    bob = await _login(anon_client, "bob", "bob-pass-123")
    me = (await anon_client.get("/me", headers=bob)).json()
    assert me["limits_accepted"] is False
    blocked = await anon_client.post(
        "/jobs",
        headers=bob,
        json={"name": "n", "image": "i", "entrypoint": ["python", "x.py"]},
    )
    assert blocked.status_code == 403
    assert blocked.json()["detail"]["reason"] == "LIMITS_NOT_ACCEPTED"

    assert (await anon_client.post("/me/accept-limits", headers=bob)).status_code == 200
    allowed = await anon_client.post(
        "/jobs",
        headers=bob,
        json={"name": "n", "image": "i", "entrypoint": ["python", "x.py"]},
    )
    assert allowed.status_code == 200, allowed.text


async def test_admin_routes_are_403_for_a_non_admin(anon_client):
    """403, not 401. The caller is perfectly logged in; this is simply not theirs to
    do, and collapsing the two would bounce a valid session back to the login screen."""
    admin = await _login(anon_client, TEST_USERNAME, TEST_PASSWORD)
    made = await anon_client.post(
        "/users",
        headers=admin,
        json={"username": "carol", "password": "carol-pass-123"},
    )
    assert made.status_code == 200, made.text
    carol_id = made.json()["user_id"]
    carol = await _login(anon_client, "carol", "carol-pass-123")

    assert (
        await anon_client.post(
            "/users", headers=carol, json={"username": "d", "password": "d-pass-123"}
        )
    ).status_code == 403
    assert (
        await anon_client.patch(
            f"/users/{carol_id}/tier", headers=carol, json={"tier": "standard"}
        )
    ).status_code == 403
    assert (await anon_client.get("/users", headers=carol)).status_code == 403

    node = await _register(anon_client)
    assert (
        await anon_client.patch(
            f"/nodes/{node['node_id']}/trusted", headers=carol, json={"trusted": True}
        )
    ).status_code == 403
    assert (
        await anon_client.patch(
            f"/nodes/{node['node_id']}/trusted", headers=admin, json={"trusted": True}
        )
    ).status_code == 200


async def test_moving_a_user_to_another_tier_withdraws_their_acceptance(anon_client):
    admin = await _login(anon_client, TEST_USERNAME, TEST_PASSWORD)
    made = await anon_client.post(
        "/users", headers=admin, json={"username": "dave", "password": "dave-pass-123"}
    )
    dave_id = made.json()["user_id"]
    dave = await _login(anon_client, "dave", "dave-pass-123")
    await anon_client.post("/me/accept-limits", headers=dave)
    assert (await anon_client.get("/me", headers=dave)).json()["limits_accepted"] is True

    moved = await anon_client.patch(
        f"/users/{dave_id}/tier", headers=admin, json={"tier": "limited"}
    )
    assert moved.status_code == 200, moved.text
    after = (await anon_client.get("/me", headers=dave)).json()
    assert after["tier"] == "limited"
    assert after["limits_accepted"] is False  # different numbers, so a fresh agreement


async def test_unknown_tier_and_duplicate_username_are_named_refusals(anon_client):
    admin = await _login(anon_client, TEST_USERNAME, TEST_PASSWORD)
    bad_tier = await anon_client.post(
        "/users",
        headers=admin,
        json={"username": "eve", "password": "eve-pass-123", "tier": "platinum"},
    )
    assert bad_tier.status_code == 422 and "platinum" in bad_tier.json()["detail"]
    assert (
        await anon_client.post(
            "/users", headers=admin, json={"username": "eve", "password": "eve-pass-123"}
        )
    ).status_code == 200
    clash = await anon_client.post(
        "/users", headers=admin, json={"username": "eve", "password": "other-pass-123"}
    )
    assert clash.status_code == 409

async def test_a_padded_duplicate_username_is_409_not_a_database_error(anon_client):
    """The stored name is stripped, so the duplicate check has to look for the
    stripped name too. Before this, creating "bob" and then posting " bob " slipped
    past the check and hit the UNIQUE constraint — a 500 for a fact about the
    caller's request, with no race needed."""
    admin = await _login(anon_client, TEST_USERNAME, TEST_PASSWORD)
    first = await anon_client.post(
        "/users", headers=admin, json={"username": "bob", "password": "bob-pass-123"}
    )
    assert first.status_code == 200, first.text
    padded = await anon_client.post(
        "/users", headers=admin, json={"username": " bob ", "password": "other-pass-123"}
    )
    assert padded.status_code == 409, padded.text
    assert padded.json()["detail"] == "username already exists"
    # And the stripped name is the one that was stored — exactly one bob.
    listed = (await anon_client.get("/users", headers=admin)).json()
    assert [u["username"] for u in listed if u["username"].strip() == "bob"] == ["bob"]


async def test_two_admins_racing_on_one_username_end_in_409_not_500(
    anon_client, monkeypatch
):
    """Check-then-insert has a window: two requests can both pass the pre-check
    before either commits, and the loser hits the UNIQUE constraint. The database
    is the referee of last resort, so its refusal must come back as the documented
    409. The window is reproduced by making the pre-check say "free" for a name
    that is taken — which is exactly what the loser saw."""
    from app.api import users as users_module

    admin = await _login(anon_client, TEST_USERNAME, TEST_PASSWORD)
    first = await anon_client.post(
        "/users", headers=admin, json={"username": "frank", "password": "frank-pass-123"}
    )
    assert first.status_code == 200, first.text

    async def never_taken(session, username):
        return False

    monkeypatch.setattr(users_module, "_username_taken", never_taken)
    loser = await anon_client.post(
        "/users", headers=admin, json={"username": "frank", "password": "other-pass-123"}
    )
    assert loser.status_code == 409, loser.text
    assert loser.json()["detail"] == "username already exists"
    # The session was rolled back cleanly: the same admin can still create a user.
    ok = await anon_client.post(
        "/users", headers=admin, json={"username": "grace", "password": "grace-pass-123"}
    )
    assert ok.status_code == 200, ok.text


async def test_tiers_are_readable_and_have_no_edit_route(client):
    """The numbers are configuration. There is a read so the UI can show them and
    deliberately no write, because a cap anyone can raise is not a cap."""
    tiers = await client.get("/tiers")
    assert tiers.status_code == 200, tiers.text
    names = {t["tier"] for t in tiers.json()}
    assert {"standard", "limited"} <= names
    assert (await client.patch("/tiers/standard", json={"retained_cap_mb": 1})).status_code in (
        404,
        405,
    )


# ===========================================================================
# The audit's arithmetic (scripts/quota_audit.py runs this against real MinIO)
# ===========================================================================


def test_reconcile_counts_only_objects_that_exist():
    """A row whose object is gone must show as DRIFT, not balance out. This is the
    one direction a per-user sum cannot notice on its own, and it is why the audit
    exists at all."""
    from app.quota import reconcile

    rows = {"a": 100, "b": 200, "c": 50}
    objects = {"a": 100, "c": 50, "orphan": 999}
    present, missing = reconcile(rows, objects)
    assert present == 150            # b contributes nothing — it is not there
    assert missing == ["b"]
    assert sum(rows.values()) - present == 200  # the drift the audit reports


def test_reconcile_reports_a_size_that_disagrees_rather_than_hiding_it():
    from app.quota import reconcile

    present, missing = reconcile({"a": 100}, {"a": 40})
    assert present == 40 and missing == []  # rows claim 100, store holds 40 -> drift 60


# ===========================================================================
# Placement — temporary disk as an eligibility condition (R10, R12)
# ===========================================================================


async def _register_with(client, name, *, version="0.12.0", ram_mb=8192):
    r = await client.post(
        "/agent/register",
        json={
            "name": name,
            "specs": {
                "cpu_cores": 4, "has_gpu": False, "ram_mb": ram_mb,
                "capacity": 4, "agent_version": version,
            },
        },
    )
    assert r.status_code == 200, r.text
    return r.json()


async def _beat(client, node, disk_free_mb=None):
    usage = {"cpu_pct": 1.0}
    if disk_free_mb is not None:
        usage["disk_free_mb"] = disk_free_mb
    hb = await client.post(
        "/agent/heartbeat",
        headers=_node_auth(node),
        json={
            "node_id": node["node_id"], "status": "idle", "running": [], "usage": usage
        },
    )
    assert hb.status_code == 200, hb.text
    return hb.json()["assignments"]


async def test_free_disk_travels_from_the_heartbeat_into_a_column(client):
    """Lifted out of the free-form usage sample because, unlike everything else in it,
    the scheduler reads this one — and a JSON blob is the wrong place to match on."""
    node = await _register_with(client, "disky")
    await _beat(client, node, disk_free_mb=4096)
    rows = (await client.get("/nodes")).json()
    row = next(n for n in rows if n["node_id"] == node["node_id"])
    assert row["disk_free_mb"] == 4096
    assert row["usage"]["disk_free_mb"] == 4096  # still in the sample too


async def test_an_agent_that_omits_free_disk_does_not_wipe_the_last_value(client):
    node = await _register_with(client, "forgetful")
    await _beat(client, node, disk_free_mb=2048)
    await _beat(client, node, disk_free_mb=None)
    rows = (await client.get("/nodes")).json()
    assert next(n for n in rows if n["node_id"] == node["node_id"])["disk_free_mb"] == 2048


async def test_p6_every_run_lands_on_the_machine_with_room(client, session_factory):
    """P6. Two machines, one reporting less free disk than the job asks for. Three
    runs. Every one lands on the machine above the ask, and the small one is never
    given any."""
    await _set_caps(session_factory, retained_mb=100, scratch_mb=4096)
    small = await _register_with(client, "small-disk")
    big = await _register_with(client, "big-disk")
    await _beat(client, small, disk_free_mb=100)
    await _beat(client, big, disk_free_mb=10000)

    r = await _submit(
        client, replicas=3, resource_reqs={"needs_gpu": False, "scratch_mb": 1024}
    )
    assert r.status_code == 200, r.text

    landed = []
    for _ in range(4):  # a few polls each: the spread preference takes one per beat
        landed += await _beat(client, big, disk_free_mb=10000)
        assert await _beat(client, small, disk_free_mb=100) == []
    assert len(landed) == 3
    async with session_factory() as s:
        import sqlalchemy as sa

        owners = {
            row[0]
            for row in (
                await s.execute(sa.select(Run.node_id).where(Run.node_id.is_not(None)))
            ).all()
        }
    assert owners == {big["node_id"]}


async def test_unknown_free_disk_never_excludes_a_machine(client, session_factory):
    """A reading that failed is not evidence of a small disk. Refusing to place work
    because one `shutil.disk_usage` call raised would be a worse answer than placing
    it."""
    await _set_caps(session_factory, retained_mb=100, scratch_mb=4096)
    node = await _register_with(client, "quiet-about-disk")
    await _beat(client, node, disk_free_mb=None)  # never reported one
    assert (
        await _submit(client, resource_reqs={"needs_gpu": False, "scratch_mb": 2048})
    ).status_code == 200
    assert len(await _beat(client, node, disk_free_mb=None)) == 1


async def test_an_explicit_ask_is_never_offered_to_an_agent_that_cannot_enforce_it(
    client, session_factory
):
    """R12. An explicit ask is a REQUIREMENT, and an older agent cannot stop a run at a
    disk cap — so it is never sent one, and the run waits, exactly as a private run
    waits for a trusted node. The same compat-guard shape W6b already uses."""
    await _set_caps(session_factory, retained_mb=100, scratch_mb=4096)
    old = await _register_with(client, "old-agent", version="0.9.0")
    await _beat(client, old, disk_free_mb=100000)
    submitted = await _submit(
        client, resource_reqs={"needs_gpu": False, "scratch_mb": 512}
    )
    assert submitted.status_code == 200
    # One of the jobs this guard still serves: submitted before sealing (see
    # `_as_legacy_job`). Without this the 0.9.0 agent would be offered nothing
    # because the job is SEALED, and the test would pass for the wrong reason.
    await _as_legacy_job(session_factory, submitted.json()["job_id"])
    assert await _beat(client, old, disk_free_mb=100000) == []

    # A new agent takes the same run.
    new = await _register_with(client, "new-agent", version="0.10.0")
    assert len(await _beat(client, new, disk_free_mb=100000)) == 1


async def test_a_tier_ceiling_filters_nothing(client, session_factory):
    """The correction that keeps the pool from deadlocking.

    A job that named no scratch size carries its tier's ceiling as a LIMIT, not a
    requirement. If that ceiling filtered placement, the default tier's 500 GB would
    mean every ordinary run waits for a machine no lab has — so a machine with almost
    nothing free, running an agent too old to enforce anything, still takes it."""
    await _set_caps(session_factory, retained_mb=100, scratch_mb=500000)
    old = await _register_with(client, "old-and-full", version="0.9.0")
    await _beat(client, old, disk_free_mb=50)
    submitted = await _submit(client)
    assert submitted.status_code == 200
    await _as_legacy_job(session_factory, submitted.json()["job_id"])  # see above
    assert len(await _beat(client, old, disk_free_mb=50)) == 1


async def test_the_assignment_carries_the_ask_or_the_ceiling(client, session_factory):
    """One number reaches the agent: what it must stop the run at. The explicit ask
    when there was one, the tier ceiling otherwise."""
    await _set_caps(session_factory, retained_mb=100, scratch_mb=256)
    node = await _register_with(client, "carrier")
    await _beat(client, node, disk_free_mb=100000)

    await _submit(client, resource_reqs={"needs_gpu": False, "scratch_mb": 64})
    assert (await _beat(client, node, disk_free_mb=100000))[0]["scratch_mb"] == 64

    await _submit(client)  # no ask -> the tier ceiling travels instead
    assert (await _beat(client, node, disk_free_mb=100000))[0]["scratch_mb"] == 256


async def test_an_unowned_job_carries_no_cap_at_all(client, session_factory):
    """The chaos-test path again, from the scheduler's side: no user, no tier, no
    ceiling, and therefore no cap on the assignment — exactly what every run got
    before this feature existed."""
    await _set_caps(session_factory, retained_mb=100, scratch_mb=256)
    node = await _register_with(client, "unowned")
    await _beat(client, node, disk_free_mb=100000)
    created = await _submit(client)
    assert created.status_code == 200
    async with session_factory() as s:
        job = await s.get(Job, (await client.get("/jobs")).json()[0]["job_id"])
        job.user_id = None
        job.resource_reqs = {"needs_gpu": False}  # as a pre-2026-09-04 row looks
        await s.commit()
    assert (await _beat(client, node, disk_free_mb=100000))[0]["scratch_mb"] is None


async def test_the_scratch_reading_is_stored_and_read_back(client):
    """The sample the cap was enforced from is the sample the user later reads."""
    node = await _register_with(client, "sampler")
    a = await _claim(client, node)
    posted = await client.post(
        f"/agent/runs/{a['run_id']}/samples",
        headers=_node_auth(node),
        json={
            "attempt": a["attempt"],
            "samples": [
                {"ts": 1.0, "cpu_pct": 5.0, "mem_used_mb": 40.0, "scratch_used_mb": 12.5}
            ],
        },
    )
    assert posted.status_code == 200, posted.text
    rows = (await client.get(f"/runs/{a['run_id']}/samples")).json()
    assert rows[0]["scratch_used_mb"] == 12.5


async def test_an_agent_that_omits_the_reading_stores_null_not_zero(client):
    """"Did not report" and "wrote nothing" are different facts, and the column keeps
    them apart.

    The agent's VERSION is not what decides this — what decides it is whether the
    posted sample carries the field, which is what this posts and what an agent older
    than 2026-09-04 does. The node registers at the current version because a node
    older than 0.12.0 is offered no sealed work at all, and the claim below needs a
    run to exist."""
    node = await _register_with(client, "old-sampler")
    a = await _claim(client, node)
    await client.post(
        f"/agent/runs/{a['run_id']}/samples",
        headers=_node_auth(node),
        json={"attempt": a["attempt"], "samples": [{"ts": 1.0, "cpu_pct": 5.0}]},
    )
    rows = (await client.get(f"/runs/{a['run_id']}/samples")).json()
    assert rows[0]["scratch_used_mb"] is None


async def test_a_trusted_only_job_carries_its_scratch_numbers_like_any_other(
    client, session_factory
):
    """The private door's runs are ordinary runs now (2026-09-06), and they carry the
    two scratch numbers every other run carries.

    This asserts the reversal of what stood here before. A private run used to have no
    writable host mount at all, so neither number could travel with it: the ceiling
    was a cap over nothing measurable, and the explicit ask was worse — a PLACEMENT
    REQUIREMENT that would send the scheduler hunting for free disk a run would never
    touch. Both objections died with the RAM folder. What the door still does is
    restrict WHERE the run may go, and this checks that too: a trusted machine is
    offered it and the assignment says the run is sealed."""
    await _set_caps(session_factory, retained_mb=100, scratch_mb=256)
    created = await client.post(
        "/jobs/private",
        data={
            "spec": (
                '{"name":"p","image":"i","entrypoint":["python","x.py"],'
                '"resource_reqs":{"scratch_mb":64}}'
            )
        },
        files={"file": ("in.bin", b"secret", "application/octet-stream")},
    )
    assert created.status_code == 200, created.text
    async with session_factory() as s:
        job = await s.get(Job, created.json()["job_id"])
        assert job.resource_reqs["scratch_mb"] == 64      # the ask, as a requirement
        assert job.resource_reqs["scratch_cap_mb"] == 256  # the tier, as a ceiling
        assert job.sealed is True and job.trusted_only is True
        # The column that named the OLD shape is not set by this door any more.
        assert job.private is False

    node = await _register_with(client, "trusted-machine")
    async with session_factory() as s:
        n = await s.get(Node, node["node_id"])
        n.trusted = True
        await s.commit()
    # Filtered on free disk like any other run with an explicit ask: 1 MB free is not
    # enough for a 64 MB ask, and the run waits.
    assert await _beat(client, node, disk_free_mb=1) == []
    got = await _beat(client, node, disk_free_mb=100000)
    assert len(got) == 1
    assert got[0]["sealed"] is True and got[0]["scratch_mb"] == 64


async def test_a_trusted_only_job_is_invisible_to_an_untrusted_machine(
    client, session_factory
):
    """The one protection the private door still buys: placement.

    Nothing else about the run differs any more, so this is the whole of what the
    door means — and it is the same rule the `trusted_only` flag buys on any job."""
    await _set_caps(session_factory, retained_mb=100, scratch_mb=256)
    created = await client.post(
        "/jobs/private",
        data={"spec": '{"name":"p","image":"i","entrypoint":["python","x.py"]}'},
        files={"file": ("in.bin", b"secret", "application/octet-stream")},
    )
    assert created.status_code == 200, created.text

    untrusted = await _register_with(client, "untrusted-machine")
    assert await _beat(client, untrusted, disk_free_mb=100000) == []

    async with session_factory() as s:
        n = await s.get(Node, untrusted["node_id"])
        n.trusted = True
        await s.commit()
    assert len(await _beat(client, untrusted, disk_free_mb=100000)) == 1


# --- Tier sizing (2026-09-05) -----------------------------------------------------


async def test_the_seeded_tiers_carry_the_sized_figures(session_factory):
    """The two plans as a fresh deployment gets them.

    These are the 2026-09-05 figures, derived for the reference lab rather than the
    supervisor's examples they replaced. If this fails, `ensure_tiers` and the sizing
    migration have parted company."""
    from app.models import Tier
    from app.quota import ensure_tiers

    async with session_factory() as s:
        await ensure_tiers(s)
        standard = await s.get(Tier, "standard")
        limited = await s.get(Tier, "limited")

    gb = 1024 * 1024 * 1024
    assert standard.retained_cap_bytes == 200 * gb == 214_748_364_800
    assert standard.scratch_cap_bytes == 50 * gb == 53_687_091_200
    assert limited.retained_cap_bytes == 20 * gb == 21_474_836_480
    assert limited.scratch_cap_bytes == 5 * gb == 5_368_709_120
    # One tenth on both numbers, asserted as the relationship and not as two more
    # constants, so the rule survives the next re-sizing.
    assert standard.retained_cap_bytes == 10 * limited.retained_cap_bytes
    assert standard.scratch_cap_bytes == 10 * limited.scratch_cap_bytes


async def test_the_seed_and_the_sizing_migration_cannot_drift():
    """The figures live in TWO places on purpose — `app/quota.py` for a fresh
    database, and the migration for one that already exists, which must keep saying
    what it did even after the policy moves again.

    That is exactly the shape that once let the agent stage `<dir>/checkpoint` while
    the runner mounted `<dir>/state`: two self-consistent halves and a broken whole.
    This asserts the two copies agree, so the pair cannot part company silently."""
    import importlib.util
    import os

    from app import quota

    path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "alembic",
        "versions",
        "20260905_a2b3c4d5e6f7_tier_sizing.py",
    )
    spec = importlib.util.spec_from_file_location("tier_sizing_migration", path)
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)

    assert migration.STANDARD_RETAINED == quota.STANDARD_RETAINED_BYTES
    assert migration.STANDARD_SCRATCH == quota.STANDARD_SCRATCH_BYTES
    assert migration.LIMITED_RETAINED == quota.LIMITED_RETAINED_BYTES
    assert migration.LIMITED_SCRATCH == quota.LIMITED_SCRATCH_BYTES
    # The descriptions too: the word the supervisor used ("plan") is part of what
    # a user is shown, so a fresh database and a migrated one must say the same thing.
    assert migration.STANDARD_DESC == quota.STANDARD_DESCRIPTION
    assert migration.LIMITED_DESC == quota.LIMITED_DESCRIPTION
