"""Which queue shape loses claimers: one job's replicas, or any queue at all?

The contention campaign seeds ONE job and expands it into runs, so when it found
that seven of thirty claimers were served on tree 3d00132 where thirty of thirty
were served on b1ddaf5~1, the shape of its queue was a suspect and not a control.
This probe makes it the variable.

It reuses the campaign's own claimers and its own simultaneous release, and changes
exactly one thing: whether the pending runs belong to one job or to as many jobs as
there are runs. Everything else -- the count of runs, the count of claimers, the
capacity, the warm-up against an empty queue, the release barrier -- is held equal.

It publishes no timing. The question is how many claimers were served, which an
unclean machine cannot change the answer to, so it is worth asking on a machine
that the campaign's own memory gate would refuse.

Result on 2026-09-06, tree 3d00132, recorded in
docs/evidence/contention_rerun_2026-09-06.txt:

     one job queue, 120 pending runs, 10 claimers -> 1 served, 9 empty-handed
   many jobs queue, 120 pending runs, 10 claimers -> 10 served, 0 empty-handed

The mechanism that explains it is `_candidates` applying the spread-replicas
preference of 2026-09-02i to every run after the first when they are all siblings,
which leaves the candidate list one row long -- and one row is the same row for
every claimer, so one wins it under SKIP LOCKED and the rest have no second choice
to fall back on.
"""

import pathlib
import sys
import uuid

sys.path.insert(0, str(pathlib.Path("scripts/experiments").resolve()))

import claim_contention as cc   # noqa: E402  seed_queue/register_claimers/fire_round
import harness as h             # noqa: E402  reset_platform/psql_exec/session

N_CLAIMERS = 10
N_RUNS = 120


def seed_multi_job(runs: int) -> None:
    """`runs` PENDING runs, each belonging to its OWN job.

    Deliberately the same columns and the same strictly increasing `created_at` as
    `claim_contention.seed_queue`, so the only difference between the two arms of
    this probe is how many jobs those runs are spread over."""
    for g in range(1, runs + 1):
        job_id = str(uuid.uuid4())
        h.psql_exec(
            "INSERT INTO jobs (id, name, image, entrypoint, env, resource_reqs, "
            "target_node_ids, replicas, status, created_at, private) VALUES ("
            f"'{job_id}', 'candidate-breadth probe', 'fyp-dummy:latest', "
            "json_build_array('python', 'train.py'), '{}'::json, '{}'::json, NULL, "
            "1, 'PENDING', now(), false);"
        )
        h.psql_exec(
            "INSERT INTO runs (id, job_id, node_id, status, attempt, "
            "lease_expires_at, retries_remaining, exit_code, started_at, "
            "finished_at, created_at, escalation_count) VALUES ("
            # 8 + 1 + 4 + 1 + 4 + 1 + 4 + 1 + 12 = 36, the width of the id column.
            f"'mp000000-0000-4000-8000-' || lpad('{g}', 12, '0'), '{job_id}', NULL, "
            "'PENDING', 0, NULL, 1, NULL, NULL, NULL, "
            f"now() - interval '1 hour' + ({g} * interval '1 millisecond'), 0);"
        )


def one_round(shape: str) -> None:
    h.reset_platform()
    claimers = cc.register_claimers(N_CLAIMERS, capacity=1)
    sessions = [h.session() for _ in claimers]
    # Warm up against an EMPTY queue, exactly as `run_once` does. A warm-up against
    # a full one spends the claimers' capacity before the measured release, and then
    # every arm reads zero for a reason that has nothing to do with the question.
    cc.fire_round(claimers, sessions)
    if shape == "one job":
        cc.seed_queue(N_RUNS)
    else:
        seed_multi_job(N_RUNS)
    result = cc.fire_round(claimers, sessions)
    served, ids = 0, []
    for r in result:
        ids.extend(r["run_ids"])
        if r["run_ids"]:
            served += 1
    for s in sessions:
        s.close()
    print(f"  {shape:>10} queue, {N_RUNS} pending runs, {N_CLAIMERS} claimers "
          f"released at one instant: {served} claimer(s) got work, "
          f"{len(set(ids))} distinct run(s) handed, "
          f"{N_CLAIMERS - served} came back empty-handed", flush=True)


def main() -> int:
    for shape in ("one job", "many jobs"):
        one_round(shape)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
