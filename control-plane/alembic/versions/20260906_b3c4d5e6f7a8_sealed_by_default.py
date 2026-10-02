"""Sealed by default — three booleans, and what each one is for

Purely additive: three columns, every one NOT NULL with a server default, nothing
altered and nothing dropped. Every existing row lands on a value by the schema's own
doing, with no data step, which is the same property the tier column bought on
2026-09-04.

  jobs.sealed        this job's data is sealed with its own key — inputs at submit,
                     outputs and checkpoints inside the container. Server default
                     FALSE, because every job that already exists was NOT sealed and
                     a migration must never make a claim about bytes it has not read.
                     Every job created from 2026-09-06 sets it TRUE at the door.
  jobs.trusted_only  placement: run only on machines an admin marked trusted. This is
                     the fourth protection the old private route bundled in, unbundled
                     into a choice available on ANY job, off by default.
  artifacts.sealed   these stored bytes are sealed, so the download door unseals them
                     before handing them to their owner. Set from the BYTES at upload
                     time (the framed format's magic), never from what the uploader
                     said about them.

**Why `jobs.private` is not dropped and not renamed.** It still means what it always
meant on the rows that carry it: a job submitted through `POST /jobs/private` before
today, whose input is sealed in the ONE-PIECE format and whose container opens it in a
RAM folder. Those rows are still runnable and are still run that way. `private` is now
the name of an old shape rather than a policy, and `sealed` + `trusted_only` are the
two things it used to mean, said separately. Dropping it would break the compatibility
tests that assert an old-shaped job still works, which is the property the freeze
exists to buy.

**No frozen READ SHAPE changes.** `GET /jobs/{id}` returns everything it returned
before plus two optional booleans an older caller ignores; the assignment gains one
optional boolean an older agent ignores — and the claim query is what stops a sealed
job reaching an agent that would ignore it (scheduler.MIN_SEALED_AGENT_VERSION).

Additive edit to the frozen contract: `protocol.md` updated in the same commit, the
same precedent as storage tiers
(e0f1a2b3c4d5), run-log tiering (d9e0f1a2b3c4) and checkpoint-and-resume
(c8d9e0f1a2b3).

Revision ID: b3c4d5e6f7a8
Revises: a2b3c4d5e6f7
Create Date: 2026-09-06
"""

import sqlalchemy as sa
from alembic import op

revision = "b3c4d5e6f7a8"
down_revision = "a2b3c4d5e6f7"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "jobs",
        sa.Column(
            "sealed", sa.Boolean(), nullable=False, server_default=sa.text("false")
        ),
    )
    op.add_column(
        "jobs",
        sa.Column(
            "trusted_only",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
    )
    op.add_column(
        "artifacts",
        sa.Column(
            "sealed", sa.Boolean(), nullable=False, server_default=sa.text("false")
        ),
    )
    # An existing PRIVATE job already runs on trusted machines only. Carrying that
    # forward as the new column's value is not a claim about anything — it is the
    # same rule, written where the scheduler now reads it, so a private job submitted
    # before today keeps landing on exactly the machines it would have landed on.
    # `sealed` is deliberately NOT set here: those inputs are sealed in the one-piece
    # format and are staged by the old path, and saying otherwise would send them
    # down a path their containers cannot follow.
    op.execute("UPDATE jobs SET trusted_only = true WHERE private = true")


def downgrade() -> None:
    # Reversed exactly. What comes off is the POLICY: after this, sealing is decided
    # by `private` alone again, which is what decided it before today. No stored
    # bytes are touched in either direction — a sealed object stays sealed and its
    # key stays in `job_keys`, so an upgrade rerun finds the data it left behind.
    op.drop_column("artifacts", "sealed")
    op.drop_column("jobs", "trusted_only")
    op.drop_column("jobs", "sealed")
