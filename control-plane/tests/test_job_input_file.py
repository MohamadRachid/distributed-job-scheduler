"""An ordinary job can carry a dataset file (2026-09-05).

**What was missing.** `POST /jobs` takes JSON, and JSON cannot carry a file. So the
only way to upload a dataset was to mark the job private -- and a private job has no
writable host mount at all, which means no artefacts and no checkpoint. A user who
simply wanted to hand their code a file was paying for secrecy they never asked for.

`POST /jobs/with-input` is the door for that. Same multipart shape, same per-file cap,
same retained-storage check under the same row lock, same object door.

**Updated 2026-09-06 (sealed by default).** This route used to store the file exactly
as it arrived, and that was the whole difference from the private route. It seals now,
like every door, and what is left of the difference is placement: `/jobs/private`
forces `trusted_only` and this one does not. The agent still needs no key — it carries
ciphertext either way and could not read it if it tried.

**The compatibility test is the one that matters.** An agent that predates this
feature knows nothing about `has_input`. Offered such a job it would start the
container with no file, and the workload would fail looking for one -- or worse, train
on nothing and report success. So the claim query refuses to offer it, exactly as it
already refuses a private run to an agent too old to stage one, and
`test_an_old_agent_is_never_offered_a_job_with_a_file` is what pins that.

**The archive is not unpacked, and no test asks for it to be.** A zip is stored as a
zip, mounted as a zip, and opened by the container. Unpacking an uploaded archive is
untrusted-input work -- it can expand to fill a disk, or carry paths that escape the
directory it is opened into -- and the container already owns a measured, capped place
to do it. That is a design decision, so it is written down here rather than left to be
inferred from a test that is absent.
"""

import json

import pytest
from sqlalchemy import func, select

from app.config import get_settings
from app.models import Job

pytestmark = pytest.mark.asyncio

SPECS = {"cpu_cores": 4, "has_gpu": False, "ram_mb": 8192, "capacity": 4, "agent_version": "0.12.0"}
# At or above scheduler.MIN_SEALED_AGENT_VERSION, so this node may be offered today's
# work at all. It clears MIN_INPUT_AGENT_VERSION on the way past.
NEW_AGENT = dict(SPECS, agent_version="0.12.0")
# Below it, and above the input guard it used to fail — so the ONE thing that keeps
# this machine from being offered a job today is that it cannot keep a job's data
# sealed. Everything else about it is identical, so nothing else can explain a
# difference in what it is offered.
OLD_AGENT = dict(SPECS, agent_version="0.11.0")

DATASET = b"PK\x03\x04 pretend this is a zip of a dataset\n"


def _auth(node):
    return {"Authorization": f"Bearer {node['token']}"}


def _spec(**over):
    spec = {
        "name": "job-with-a-dataset",
        "image": "fyp-dummy:latest",
        "entrypoint": ["python", "train.py"],
        "replicas": 1,
    }
    spec.update(over)
    return spec


async def _register(client, name="worker", specs=NEW_AGENT):
    r = await client.post("/agent/register", json={"name": name, "specs": specs})
    assert r.status_code == 200, r.text
    return r.json()


async def _submit(client, data=DATASET, filename="dataset.zip", **over):
    return await client.post(
        "/jobs/with-input",
        data={"spec": json.dumps(_spec(**over))},
        files={"file": (filename, data, "application/zip")},
    )


async def _heartbeat(client, node):
    r = await client.post(
        "/agent/heartbeat",
        headers=_auth(node),
        json={"node_id": node["node_id"], "status": "idle", "running": []},
    )
    assert r.status_code == 200, r.text
    return r.json()["assignments"]


# --- the route ---------------------------------------------------------------


async def test_a_job_can_be_submitted_with_a_file(client, session_factory):
    r = await _submit(client)
    assert r.status_code == 200, r.text
    body = r.json()
    assert len(body["run_ids"]) == 1

    async with session_factory() as s:
        job = await s.get(Job, body["job_id"])
    # Not private: this route exists precisely so that carrying a file does not force
    # secrecy, and with it the loss of artefacts and checkpointing.
    assert job.private is False
    assert job.input_filename == "dataset.zip"
    # The SEALED length (2026-09-06), which is what storage holds and therefore what
    # the retained cap has to charge: the plaintext plus a 32-byte header and one
    # nonce-and-tag per piece.
    assert job.input_size_bytes == len(DATASET) + 60
    assert job.input_object_key
    # Sealed like every job, and not restricted in where it may run — which is the
    # whole of what is left between this door and `/jobs/private`.
    assert job.sealed is True and job.trusted_only is False


async def test_the_stored_bytes_are_sealed_and_open_to_what_was_uploaded(
    client, session_factory, mem_store
):
    """The reversal of what stood here before (2026-09-06).

    This route used to store the file exactly as it arrived, and the test said so. It
    seals now: storage holds ciphertext for every job, and the only thing that turns it
    back into the dataset is the job's own key — which lives in a different table and
    never leaves the control plane except as a one-shot ticket to a container."""
    from app.models import JobKey
    from app.sealing import key_from_b64, open_any

    r = await _submit(client)
    async with session_factory() as s:
        job = await s.get(Job, r.json()["job_id"])
        key_row = await s.get(JobKey, job.id)
    stored = mem_store.get_object(job.input_object_key)
    assert stored != DATASET and stored.startswith(b"FYPSEAL2")
    assert key_row is not None, "every job has its own key"
    assert open_any(stored, key_from_b64(key_row.key_b64)) == DATASET


async def test_the_users_filename_never_reaches_the_object_key(client, session_factory):
    """A submitted name is a label, never a path. If the name decided where the bytes
    landed, a crafted filename would decide where the platform writes -- the same class
    of mistake the artefact route already closes with its own basename reduction."""
    r = await _submit(client, filename="../../etc/passwd")
    assert r.status_code == 200, r.text
    async with session_factory() as s:
        job = await s.get(Job, r.json()["job_id"])
    assert job.input_object_key == f"inputs/{job.id}/input.bin"
    assert ".." not in job.input_object_key


async def test_private_is_refused_on_this_route(client):
    """`private` names the OLDER submission shape, and this door does not build one.

    Since 2026-09-06 the honest answer to "make this private" is that the data is
    already sealed and what is left to choose is placement — so the message points at
    `trusted_only`, and at the door that forces it."""
    r = await _submit(client, private=True)
    assert r.status_code == 422, r.text
    assert "trusted_only" in r.json()["detail"]


async def test_trusted_only_can_be_asked_for_on_this_route(client, session_factory):
    """The unbundling, from the user's side: the choice that used to require the
    private door is a field on the ordinary one."""
    r = await _submit(client, trusted_only=True)
    assert r.status_code == 200, r.text
    async with session_factory() as s:
        job = await s.get(Job, r.json()["job_id"])
    assert job.trusted_only is True and job.private is False


async def test_an_empty_file_is_refused(client):
    r = await _submit(client, data=b"")
    assert r.status_code == 422, r.text


async def test_a_file_over_the_cap_is_refused_and_nothing_is_stored(
    client, session_factory, monkeypatch
):
    """413, and -- the part worth asserting -- no job row either. A refusal that left a
    half-created job behind would be worse than no cap at all, so the check sits before
    anything is written."""
    monkeypatch.setattr(get_settings(), "max_input_mb", 1, raising=False)

    r = await _submit(client, data=b"x" * (2 * 1024 * 1024))
    assert r.status_code == 413, r.text

    async with session_factory() as s:
        count = (await s.execute(select(func.count()).select_from(Job))).scalar_one()
    assert count == 0


# --- who it is offered to ----------------------------------------------------


async def test_an_old_agent_is_never_offered_a_job_with_a_file(client):
    """THE compatibility test. An old agent cannot stage this run — before 2026-09-06
    because it did not know the field, and since then because it cannot fetch the
    ticket its container needs to open a sealed file. Either way it would start the
    container with no usable dataset, so refusing to offer it is the only safe answer,
    and it is the same guard shape the private route has used since W6b."""
    old = await _register(client, name="old-pc", specs=OLD_AGENT)
    r = await _submit(client)
    assert r.status_code == 200, r.text

    assert await _heartbeat(client, old) == []


async def test_an_old_agent_is_offered_nothing_at_all_now(client, session_factory):
    """The reversal of what stood here before, and it is the intended answer.

    This test used to pin that the input guard was NARROW: an old agent kept getting
    ordinary work and lost only the job with a file. Since 2026-09-06 every job is
    sealed, so an agent that cannot keep a job's data sealed is offered nothing new —
    an outcome the brief asked for in those words, and one an operator sees as an
    out-of-date machine going quiet rather than as work failing on it.

    What such an agent CAN still take is a job from before sealing, and the second
    half pins that, because "offered nothing at all, ever" would be a different and
    much worse rule."""
    old = await _register(client, name="old-pc", specs=OLD_AGENT)
    r = await client.post("/jobs", json=_spec(name="plain-job"))
    assert r.status_code == 200, r.text
    assert await _heartbeat(client, old) == []

    async with session_factory() as s:
        job = await s.get(Job, r.json()["job_id"])
        job.sealed = False          # a job submitted before 2026-09-06
        await s.commit()
    assignments = await _heartbeat(client, old)
    assert len(assignments) == 1
    assert assignments[0]["has_input"] is False


async def test_a_current_agent_is_told_the_run_carries_a_file(client):
    node = await _register(client)
    await _submit(client)

    a = (await _heartbeat(client, node))[0]
    assert a["has_input"] is True
    # `private` names the OLD staging path and is never set on a job built today; the
    # agent branches on `sealed` instead, and takes exactly one of the two paths.
    assert a["private"] is False
    assert a["sealed"] is True
    assert a["input_filename"] == "dataset.zip"
    assert a["job_id"]


# --- fetching it -------------------------------------------------------------


async def test_the_holder_of_a_live_run_can_download_the_file(client):
    node = await _register(client)
    await _submit(client)
    a = (await _heartbeat(client, node))[0]

    r = await client.get(f"/agent/jobs/{a['job_id']}/input", headers=_auth(node))
    assert r.status_code == 200, r.text
    # What the agent gets is CIPHERTEXT (2026-09-06). It is a courier: it stages these
    # bytes on the worker's disk read-only and could not open them if it tried, which
    # is what lets a machine carry data it is not allowed to read.
    assert r.content.startswith(b"FYPSEAL2") and r.content != DATASET


async def test_a_node_with_no_live_run_gets_nothing(client):
    """Unchanged from the sealed route, and re-asserted here because opening the
    endpoint to ordinary jobs must not have opened it to everybody."""
    holder = await _register(client, name="holder")
    stranger = await _register(client, name="stranger")
    await _submit(client)
    a = (await _heartbeat(client, holder))[0]

    r = await client.get(f"/agent/jobs/{a['job_id']}/input", headers=_auth(stranger))
    assert r.status_code == 409, r.text


async def test_a_job_with_no_file_has_nothing_to_download(client):
    node = await _register(client)
    r = await client.post("/jobs", json=_spec(name="plain-job"))
    job_id = r.json()["job_id"]
    await _heartbeat(client, node)

    r = await client.get(f"/agent/jobs/{job_id}/input", headers=_auth(node))
    assert r.status_code == 404, r.text
