"""Checkpoint-use advisor — does the script the user pasted actually checkpoint?

Scan the code a user submits and tell them whether it uses the platform's checkpoint
saving, so a re-dispatched run resumes instead of starting its training again from
the top.

**What this can and cannot see, said first because it bounds every claim below.**
The platform receives a container IMAGE, not source code. It cannot read the training
script on its own, and nothing here tries to: opening an image and guessing which of
its files is "the training script" would be a guess dressed as a check. So the text is
OPTIONAL and the user pastes it. Absent text is `not_checked` — a distinct verdict
from `no_checkpoint`, because "we did not look" and "we looked and found nothing" are
different statements and a user acting on the wrong one is the whole risk here.

**It is advice, and only advice.** It never blocks a submission, never changes
placement, never touches a run, and is computed AFTER the job row is committed. A
wrong verdict costs a misleading line on a page. That is the entire blast radius, and
it is why this runs as a background task rather than in the submit path: a slow or
broken advisor must not be able to stop anyone submitting work.

**The plain scan** (stored as `arm_a`) runs on the server with no network, no key and
no cost. It looks for the checkpoint PATH and for save/load calls near it.

**Why the scan looks for three path spellings and not the one the brief named.** The
brief specified the literal string `/checkpoint`. Measured against this repository's
own two reference workloads, that rule scores BOTH of them `no_checkpoint` — and they
are the workloads that checkpoint CORRECTLY:

    workloads/trainer/train.py   literal "/checkpoint": 0   fyp_checkpoint: 7
    workloads/dummy/train.py     literal "/checkpoint": 0   fyp_checkpoint: 4

They never write the literal path because they are not supposed to. The platform's
adoption contract is `workloads/dummy/fyp_checkpoint.py`, whose whole point is that a
workload calls `load()` and `save()` and never hard-codes a path; the path arrives as
the `CHECKPOINT_PATH` environment variable and the runner mounts it at `/checkpoint`
(`agent/runner.py`, `_CHECKPOINT_MOUNT` and `CHECKPOINT_FILENAME`). A checker that
fails on the recommended way of doing the thing is worse than no checker, because it
tells the careful user they got it wrong. So all three spellings of the same fact
count as path evidence, and the brief's literal is kept as one of them.
"""

import logging

log = logging.getLogger("advisor")

# The largest pasted script we will look at. Also enforced at the door as a 422 (see
# schemas.JobCreate) — this copy is the backstop for a row that reached the database
# by some other path, since a background task must not be the thing that discovers a
# 64 MB string.
MAX_SOURCE_BYTES = 65_536

# --- Arm A's evidence sets -------------------------------------------------------
#
# Three questions, asked in order: does this text refer to the checkpoint LOCATION at
# all; does it WRITE there; does it READ there on the way in. A save without the
# location is a script saving somewhere we will not collect from, which is exactly the
# case worth telling someone about, so the location is required and not optional.
#
# These are plain substring tests on purpose. A parser would be more precise and would
# also be a second language implementation to maintain and defend, for a feature whose
# output is one advisory line. What the reason string carries is the matched tokens
# themselves, so a user who disagrees can see precisely what was matched and why.

# Where the platform's checkpoint lives, in the three ways a real script refers to it.
_PATH_TOKENS = (
    "/checkpoint",        # the mount, written literally (the brief's rule)
    "CHECKPOINT_PATH",    # the environment variable the runner sets
    "fyp_checkpoint",     # the adoption contract module (workloads/dummy)
)

# Writing state out. `.save(` is deliberately broad: it catches `fyp_checkpoint.save`,
# `torch.save` via an alias, and every framework's own saver, at the cost of also
# catching an unrelated `.save(`. Broad is the right direction here — a false
# "saves_checkpoint" is a line that says "looks fine"; a false "no_checkpoint" tells a
# correct script it is broken, and that is the more expensive mistake.
_SAVE_TOKENS = (
    "torch.save",
    "save_checkpoint",
    "np.save",
    "pickle.dump",
    ".save(",
)

# Reading state back in at start. `os.path.exists` counts because the shape it belongs
# to — "if a checkpoint file is there, load it" — is the resume idiom itself.
_LOAD_TOKENS = (
    "torch.load",
    "load_checkpoint",
    "os.path.exists",
    ".load(",
)

# The four verdicts, ordered from "we know nothing" to "this fully uses the feature".
NOT_CHECKED = "not_checked"
NO_CHECKPOINT = "no_checkpoint"
SAVES = "saves_checkpoint"
RESUMES = "resumes"


def _matched(text: str, tokens: tuple[str, ...]) -> list[str]:
    """Which of `tokens` appear in `text`, in the order the token list declares them
    so the reason string is stable between runs and a test can assert on it."""
    return [t for t in tokens if t in text]


def _shown(hits: list[str]) -> str:
    """The matched tokens as a person reads them (walk 1, row 68). A token such as
    `.save(` carries its opening bracket so the scan matches a CALL and not a word;
    printed inside the reason's own brackets it read as `(.save()`, which looks
    unbalanced. The bracket is dropped for display and nothing else changes."""
    return ", ".join(t.rstrip("(") for t in hits)


def scan_text(text: str) -> dict:
    """Arm A. No network, no key, no cost, and no exception it can raise on a string.

    Returns `{verdict, path_hits, save_hits, load_hits, reason}`. The three hit lists
    are what the verdict was computed from, kept in the stored advice so a user (or a
    jury) can check the verdict against its own evidence rather than trusting it."""
    path_hits = _matched(text, _PATH_TOKENS)
    save_hits = _matched(text, _SAVE_TOKENS)
    load_hits = _matched(text, _LOAD_TOKENS)

    if not path_hits or not save_hits:
        # Two different reasons for the same verdict, and the user needs to know
        # WHICH: "you save, but not where we collect from" is a small fix, while
        # "you never save" is a design change to their script.
        if save_hits and not path_hits:
            reason = (
                "saves state (%s) but never refers to the checkpoint location — "
                "the platform collects only what is written to /checkpoint"
                % _shown(save_hits)
            )
        elif path_hits and not save_hits:
            reason = (
                "refers to the checkpoint location (%s) but no save call was found"
                % _shown(path_hits)
            )
        else:
            reason = "no checkpoint location and no save call found"
        verdict = NO_CHECKPOINT
    elif load_hits:
        verdict = RESUMES
        reason = "saves (%s) and loads (%s) at the checkpoint location (%s)" % (
            _shown(save_hits),
            _shown(load_hits),
            _shown(path_hits),
        )
    else:
        verdict = SAVES
        reason = (
            "saves (%s) at the checkpoint location (%s), but no load call was found — "
            "a re-dispatched run would start again from the beginning"
            % (_shown(save_hits), _shown(path_hits))
        )

    return {
        "verdict": verdict,
        "path_hits": path_hits,
        "save_hits": save_hits,
        "load_hits": load_hits,
        "reason": reason,
    }


def combine(text: str) -> dict:
    """Build the stored advice from the plain scan."""
    arm_a = scan_text(text)
    return {"verdict": arm_a["verdict"], "arm_a": arm_a}


async def advise(job_id: str, session_factory) -> None:
    """The background task: read the job's pasted text, store the advice, commit.

    Runs after the response has gone out (FastAPI background task), so the request's
    own session is already closed by the time this opens its own — which is what lets
    it work against the tests' single-connection SQLite as well as against Postgres.

    Swallows everything. A failure here must cost the advice line and nothing else:
    the job is committed, its runs are queued, and none of that depends on this."""
    from datetime import datetime, timezone

    from .models import Job

    try:
        async with session_factory() as session:
            job = await session.get(Job, job_id)
            if job is None:
                return
            text = (job.source_text or "").strip()
            if not text:
                # Nothing pasted. Recorded EXPLICITLY as not_checked rather than left
                # null, so the page can say "not checked" instead of showing nothing
                # and letting the reader supply their own meaning.
                job.checkpoint_advice = {
                    "verdict": NOT_CHECKED,
                    "arm_a": None,
                }
                await session.commit()
                return

            advice = combine(text)
            advice["checked_at"] = datetime.now(timezone.utc).isoformat()
            job.checkpoint_advice = advice
            await session.commit()
    except Exception as exc:  # noqa: BLE001
        log.warning("advisor failed for job %s: %s", job_id, type(exc).__name__)

