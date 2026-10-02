"""Checkpoint-use advisor (2026-09-05) — the plain scan, the door, and the two workloads.

The advisor tells a submitter whether the script they pasted saves to the platform's
checkpoint location and loads from it on start, so a re-dispatched run resumes rather
than training again from the top.

What these tests pin, in the order they appear:

  1. the scan on all six fixtures, verdict AND the tokens the verdict was computed from;
  2. **the two reference workloads in this repository score `resumes`** — the
     regression that matters, because the rule as originally specified scored both of
     them `no_checkpoint` (see the module docstring in `app/checkpoint_advisor.py`);
  3. the 64 KB door answers 422, and a job with no pasted text records `not_checked`.

No test here reaches the network; the scan makes no outbound request.
"""

import io
import os

import pytest
import pytest_asyncio  # noqa: F401  -- the asyncio fixtures come from conftest

from app.checkpoint_advisor import advise, scan_text

pytestmark = pytest.mark.asyncio

_FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures", "advisor")

# (file, expected verdict). The two `no_checkpoint` rows fail for DIFFERENT reasons —
# one has nothing at all, the other saves diligently to a place we never collect from
# — and the reason strings are asserted separately below, because a verdict that is
# right for the wrong reason is a verdict that will be wrong on the next input.
_CASES = [
    ("saves_and_resumes_torch.py", "resumes"),
    ("saves_and_resumes_fyp.py", "resumes"),
    ("saves_only_torch.py", "saves_checkpoint"),
    ("saves_only_pickle.py", "saves_checkpoint"),
    ("no_checkpoint_plain.py", "no_checkpoint"),
    ("no_checkpoint_saves_elsewhere.py", "no_checkpoint"),
]


def _fixture(name: str) -> str:
    with io.open(os.path.join(_FIXTURES, name), encoding="utf-8") as fh:
        return fh.read()


def _repo_file(*parts: str):
    """Read a file from the repository root, or None when it is not reachable.

    The canonical suite runs INSIDE the pinned container, where only `control-plane/`
    is mounted — so `workloads/` genuinely is not there and the test below skips with
    that reason rather than failing on the environment. It runs on the host, and the
    proof file `docs/evidence/checkpoint_advisor_2026-09-06.txt` carries the same
    check against the real files, machine-printed, so the claim is not resting on a
    test that happens to skip everywhere it is run."""
    here = os.path.dirname(os.path.abspath(__file__))
    for _ in range(5):
        candidate = os.path.join(here, *parts)
        if os.path.isfile(candidate):
            with io.open(candidate, encoding="utf-8") as fh:
                return fh.read()
        here = os.path.dirname(here)
    return None


# --- 1. Arm A on the six fixtures -------------------------------------------------


@pytest.mark.parametrize("name,expected", _CASES)
async def test_arm_a_verdict_on_each_fixture(name, expected):
    assert scan_text(_fixture(name))["verdict"] == expected


async def test_arm_a_names_the_tokens_it_matched():
    """The reason is evidence, not a label. A user who disagrees with the verdict has
    to be able to see exactly which strings produced it."""
    r = scan_text(_fixture("saves_and_resumes_torch.py"))
    assert "torch.save" in r["save_hits"]
    assert "torch.load" in r["load_hits"]
    assert "/checkpoint" in r["path_hits"]
    assert "torch.save" in r["reason"] and "torch.load" in r["reason"]


async def test_the_two_no_checkpoint_cases_give_different_reasons():
    """Saving nowhere and saving to the wrong place are different problems with
    different fixes, so they must not collapse into one message."""
    nothing = scan_text(_fixture("no_checkpoint_plain.py"))
    elsewhere = scan_text(_fixture("no_checkpoint_saves_elsewhere.py"))

    assert nothing["verdict"] == elsewhere["verdict"] == "no_checkpoint"
    assert "no checkpoint location and no save call" in nothing["reason"]
    # This one DID save — the advice has to say so, or the user goes looking for a
    # missing save call that is right there in front of them.
    assert "torch.save" in elsewhere["save_hits"]
    assert "never refers to the checkpoint location" in elsewhere["reason"]


async def test_saves_only_says_a_redispatched_run_would_start_again():
    r = scan_text(_fixture("saves_only_torch.py"))
    assert r["verdict"] == "saves_checkpoint"
    assert "start again from the beginning" in r["reason"]


# --- 2. The regression that motivated widening the token set ----------------------


@pytest.mark.parametrize(
    "path",
    [
        ("workloads", "trainer", "train.py"),
        ("workloads", "dummy", "train.py"),
    ],
)
async def test_this_repositorys_own_workloads_score_resumes(path):
    """**The test that pins the defect.**

    Both reference workloads adopt the platform's checkpoint contract through
    `fyp_checkpoint.load()` / `.save()`, and neither writes the mount path literally —
    they are not supposed to; the path arrives in the environment. A rule that
    required the literal path scored both of them `no_checkpoint`, telling the two
    scripts that do this correctly that they do not checkpoint at all.

    If this test ever fails, the token set has been narrowed back and the advisor is
    now wrong about the only two workloads we ship."""
    text = _repo_file(*path)
    if text is None:
        pytest.skip(
            "workloads/ is not mounted in the pinned test container; this check is "
            "run on the host and captured in docs/evidence/checkpoint_advisor_"
            "2026-09-06.txt"
        )
    result = scan_text(text)
    assert result["verdict"] == "resumes", result["reason"]
    assert "fyp_checkpoint" in result["path_hits"]
    # Stated explicitly so the reason for the widening cannot be lost: the literal
    # path really is absent from these files.
    assert "/checkpoint" not in text


# --- 3. The door and the empty case ------------------------------------------------


async def test_source_text_over_64kb_is_refused_at_the_door(client):
    """The cap is bytes, not characters, so the payload is built in bytes."""
    body = {
        "name": "too-big",
        "image": "fyp-dummy:latest",
        "source_text": "x" * 65_537,
    }
    resp = await client.post("/jobs", json=body)
    assert resp.status_code == 422, resp.text


async def test_source_text_at_exactly_64kb_is_accepted(client):
    body = {
        "name": "at-the-cap",
        "image": "fyp-dummy:latest",
        "source_text": "x" * 65_536,
    }
    resp = await client.post("/jobs", json=body)
    assert resp.status_code == 200, resp.text


async def test_a_job_with_no_pasted_text_records_not_checked(client):
    """`not_checked` is a verdict, not an absence. "We did not look" and "we looked
    and found nothing" are different statements, and a user must not have to guess
    which one an empty field meant."""
    resp = await client.post("/jobs", json={"name": "n", "image": "fyp-dummy:latest"})
    assert resp.status_code == 200, resp.text
    job_id = resp.json()["job_id"]

    got = await client.get(f"/jobs/{job_id}")
    assert got.json()["checkpoint_advice"]["verdict"] == "not_checked"


async def test_submitting_a_script_produces_advice_on_the_job_page(client):
    """End to end through the real route: the background task runs after the response
    and the verdict is readable on the job."""
    resp = await client.post(
        "/jobs",
        json={
            "name": "advised",
            "image": "fyp-dummy:latest",
            "source_text": _fixture("saves_only_torch.py"),
        },
    )
    assert resp.status_code == 200, resp.text
    job_id = resp.json()["job_id"]

    advice = (await client.get(f"/jobs/{job_id}")).json()["checkpoint_advice"]
    assert advice["verdict"] == "saves_checkpoint"
    assert "torch.save" in advice["arm_a"]["save_hits"]
    assert advice["checked_at"]


async def test_the_job_read_shape_never_returns_the_pasted_source(client):
    """A read model returns the verdict and its evidence. It does not hand the user's
    own source code back out over the wire."""
    resp = await client.post(
        "/jobs",
        json={
            "name": "private-ish",
            "image": "fyp-dummy:latest",
            "source_text": "SECRET_MARKER_do_not_echo",
        },
    )
    job_id = resp.json()["job_id"]
    body = (await client.get(f"/jobs/{job_id}")).text
    assert "SECRET_MARKER_do_not_echo" not in body


async def test_advice_survives_a_job_that_vanished(session_factory):
    """The task must not raise when the row is gone — nothing depends on it, and a
    background task that throws is noise in a log nobody reads."""
    await advise("no-such-job", session_factory)


async def test_every_submission_door_advises(client):
    """**All three doors, or the silence looks like a verdict.**

    There are three ways to submit: `POST /jobs` (JSON), `POST /jobs/with-input`
    (multipart, an ordinary job carrying a dataset file) and `POST /jobs/private`
    (multipart, sealed input). A feature that answers on some of them is worse than
    one that answers on none, because a user on the quiet door reads the absence as
    "nothing to say" rather than "nobody looked".

    This was a real gap: the with-input door arrived on 2026-09-05 in parallel with
    the advisor, and the two merged cleanly at the text level while leaving that route
    with no hook at all. If a fourth door is ever added, this test is what notices."""
    import json as _json

    script = _fixture("saves_and_resumes_torch.py")
    spec = _json.dumps(
        {"name": "door", "image": "fyp-dummy:latest", "source_text": script}
    )

    plain = await client.post(
        "/jobs", json={"name": "door", "image": "fyp-dummy:latest", "source_text": script}
    )
    with_input = await client.post(
        "/jobs/with-input",
        data={"spec": spec},
        files={"file": ("data.bin", b"payload", "application/octet-stream")},
    )
    private = await client.post(
        "/jobs/private",
        data={"spec": spec},
        files={"file": ("data.bin", b"payload", "application/octet-stream")},
    )

    for name, resp in (
        ("POST /jobs", plain),
        ("POST /jobs/with-input", with_input),
        ("POST /jobs/private", private),
    ):
        assert resp.status_code == 200, f"{name}: {resp.text}"
        job_id = resp.json()["job_id"]
        advice = (await client.get(f"/jobs/{job_id}")).json()["checkpoint_advice"]
        assert advice is not None, f"{name} produced no advice at all"
        assert advice["verdict"] == "resumes", f"{name}: {advice}"
