"""Who may read a job's data (2026-09-07).

Until this date every logged-in user could list every job and open any run's log,
result file and resource history. Authentication was built in W6 and answers "who
are you"; nothing anywhere answered "is this yours". That was tolerable while the
platform had one account and no way to make a second, and it stopped being
tolerable on 2026-09-04, when an admin gained `POST /users`. The gap is the whole
reason this module exists: a platform whose data story is that every job is sealed
against the storage server, the network, the agent and the worker's disk was
serving the plaintext to whoever asked through the front door.

**Three rules, written once here so that ten routes cannot each get them slightly
wrong.**

1. **A job belongs to the user who submitted it.** `jobs.user_id` has carried that
   since W1 — it was honest wiring with nothing reading it, and now something does.
2. **An administrator sees everything.** A ROLE (`users.is_admin`), never a name and
   never a username comparison: the deployment's bootstrap account is admin because
   somebody has to be able to create the second user, and any account an admin
   promotes is admin on exactly the same terms.
3. **A job with no owner is visible to everyone.** `jobs.user_id` is nullable and no
   HTTP door can leave it null — every submit attributes the job to its caller. A
   null owner therefore means a row written directly into the database: the chaos
   test's jobs, and the fixtures of the suites that predate authentication. Refusing
   those would break the project's centrepiece proof to protect data that belongs to
   nobody. It is the same rule `jobs.may_cancel` has applied since the cancel route
   landed, and it is stated here rather than left as a coincidence.

**A refusal never says whether the identifier exists.** Asking for another user's
job and asking for a job that was never created get the SAME answer, the same code
and the same words. The alternative leaks the shape of everyone else's work to
anyone willing to try identifiers: a `403` on one and a `404` on the other is a
lookup service for which jobs exist. This is why the reads here answer `404` rather
than `403` even though `403` is the more descriptive code — `403` is right where
the caller already knows the resource exists (an admin route, a cancel they were
told about), and wrong where the existence is the secret.

**Nothing here touches the agent routes.** An agent authenticates with a node token
and is already scoped by the run it holds, at the attempt it holds it — that is the
fence, and it is a stronger check than this one. `require_node` and `require_user`
remain two different code paths against two different columns.
"""

from __future__ import annotations

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from .models import Job, Run, User

# One string for both refusals, so the two cases cannot be told apart by their text
# any more than by their status code.
_UNKNOWN_JOB = "unknown job_id"
_UNKNOWN_RUN = "unknown run_id"


def owns(user: object, job: Job) -> bool:
    """May `user` read this job's data? Pure — no database, so it is tested directly.

    Deliberately the same three-part answer `jobs.may_cancel` gives, because reading
    a job's results and stopping it are the same question about the same row."""
    if not isinstance(user, User):
        return False
    return bool(user.is_admin) or job.user_id is None or job.user_id == user.id


def visible_jobs(user: object):
    """A `SELECT` over the jobs this user may see.

    An admin gets the unfiltered select; everyone else gets their own rows plus the
    ownerless ones. Written as a query rather than as a filter applied to fetched
    rows on purpose: a list that reads every row and then hides most of them is one
    forgotten `return` away from serving them all, and it makes the database do work
    for rows the caller will never be shown."""
    stmt = select(Job)
    if isinstance(user, User) and user.is_admin:
        return stmt
    user_id = user.id if isinstance(user, User) else None
    return stmt.where((Job.user_id.is_(None)) | (Job.user_id == user_id))


async def job_for_user(session: AsyncSession, job_id: str, user: object) -> Job:
    """The job, if this user may read it. `404` otherwise — for both reasons."""
    job = await session.get(Job, job_id)
    if job is None or not owns(user, job):
        raise HTTPException(status_code=404, detail=_UNKNOWN_JOB)
    return job


async def run_for_user(session: AsyncSession, run_id: str, user: object) -> Run:
    """The run, if this user may read the job it belongs to. `404` otherwise.

    A run carries no owner of its own and never will: it belongs to its job, and a
    second copy of the owner on the run row could disagree with the first."""
    run = await session.get(Run, run_id)
    if run is None:
        raise HTTPException(status_code=404, detail=_UNKNOWN_RUN)
    job = await session.get(Job, run.job_id)
    if job is None or not owns(user, job):
        raise HTTPException(status_code=404, detail=_UNKNOWN_RUN)
    return run


async def may_read_run(session: AsyncSession, run_id: str, user: object) -> bool:
    """The WebSocket's form of the same question: a bool, because a socket refuses by
    closing rather than by raising an HTTP error."""
    run = await session.get(Run, run_id)
    if run is None:
        return False
    job = await session.get(Job, run.job_id)
    return job is not None and owns(user, job)
