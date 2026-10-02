# Standards compliance

How the platform maps onto the engineering standards it was built against: what each
one covers, the mechanism in this codebase that satisfies it, and where to see it.

| Standard | Clause | Title | How the platform complies | Where to see it |
|---|---|---|---|---|
| ISO/IEC/IEEE 12207 | 6.4.7 | Implementation process | Training jobs are Docker images built to a fixed contract (image, entrypoint, output path); every worker constructs and runs the same isolated unit reproducibly. | The job contract in `protocol.md`; `workloads/dummy/` |
| ISO/IEC/IEEE 12207 | 6.3.5 | Configuration Management process | Trunk-based Git with short-lived feature branches; continuous integration runs every suite on every push; the schema is versioned by Alembic migrations, each proven up, down and up again on real PostgreSQL. | `.github/workflows/ci.yml`; `control-plane/alembic/versions/` |
| ISO/IEC/IEEE 12207 | 6.4.9 | Verification process | Nothing produces a published number until it has been run: a dry-pass gate before any measurement, every flag checked in **both** its states, and bug-fix tests run against the **pre-fix** code so a test is shown to fail before the fix and pass after it. | `scripts/experiments/harness.py`; the regression tests under `control-plane/tests/` and `agent/tests/` |
| ISO/IEC 25010 | 4.2.5 | Reliability (fault tolerance, recoverability) | Heartbeats and a reaper detect node and run failure; lease plus fencing token guarantee **at-most-once accepted result**; lost runs are re-dispatched automatically. | `scripts/chaos_test.py` and its capture `docs/evidence/chaos_test_2026-09-06.txt`; the fencing-off experiment in `docs/evidence/experiments/E1/` |
| ISO/IEC 25010 | 4.2.8 | Portability (adaptability, installability) | Agent and control plane are plain Python and Docker on common hardware; no cloud dependency; machines join and leave by starting and stopping their agent. | `docker-compose.yml`; `agent/` |
| ISO/IEC 27001 | A.5.15 | Access control | A JWT gates every user operation; agents authenticate with separate node tokens; a user reads only their own jobs, runs, logs and results, and a refusal does not reveal whether the identifier exists. | `control-plane/app/userauth.py`, `control-plane/app/ownership.py`; `control-plane/tests/test_read_scoping.py` |
| ISO/IEC 27001 | A.8.31 | Separation of environments | User code runs only inside isolated containers with a read-only root filesystem and no host credentials; the control plane brokers storage, so workers never hold storage credentials. | `agent/runner.py`; `control-plane/app/storage.py` |
| ISO/IEC 27001 | A.8.24 | Use of cryptography | **Every** job's data is sealed with **AES-GCM** under a fresh 256-bit key per job. The input is sealed at submit in independent pieces, each carrying its own tag and its own position, so a piece cannot be changed, moved, borrowed from another file or dropped off the end; results and checkpoints are sealed inside the container before they leave it. The key is stored apart from the ciphertext and released only through a fenced, single-use ticket. Deleting the key makes every sealed copy unreadable. Transport is TLS with the certificate **verified**, and there is no switch to skip that. | `control-plane/app/sealing.py`, `control-plane/app/api/keys.py`, `workloads/dummy/fyp_data.py`, `agent/tls.py` |

Clause and control numbers are those of the editions cited: ISO/IEC/IEEE 12207:2017,
ISO/IEC 25010:2011 and ISO/IEC 27001:2022. Control numbering changed between the 2013
and 2022 editions of 27001, so access control is **A.5.15** here and not the 2013
number A.9.4.

**One limit on the three 12207 rows.** The clause numbers and titles come from the
standard's own contents pages, which give a clause's number, title and page but not the
text of the requirement. The "how the platform complies" column is our reading of what
each named process covers, not a quotation from the standard.

**One limit on the cryptography row.** Sealing does not protect data from the owner of
the worker machine, who has root on it and can read a running container's memory. No
software prevents that; it needs confidential-computing hardware, which is future work.
