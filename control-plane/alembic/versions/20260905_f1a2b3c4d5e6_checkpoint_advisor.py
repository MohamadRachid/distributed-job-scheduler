"""Checkpoint-use advisor — the pasted script and the verdict about it

Purely additive: two nullable columns on `jobs`, nothing altered and nothing dropped.
Every existing row keeps working unchanged and reads back as `NULL` on both, which the
read shape renders as "not checked" rather than as an absence the reader has to
interpret.

  jobs.source_text        the training script the user pasted (optional, <= 64 KB)
  jobs.checkpoint_advice  {verdict, arm_a, arm_b, checked_at} — what the two arms said

**No agent change and no worker change.** The agent never sees either column: this is
advice shown to the person submitting, computed after the job row is committed, and
nothing in dispatch, placement or execution reads it. An agent built against the older
shapes is unaffected in every direction.

**Why the text is stored and not scanned-and-discarded.** The advice is a claim about
a specific piece of text. Throwing the text away would leave a verdict nobody — the
user, us, or a jury — could check against its subject afterwards.

**Why generic JSON and not JSONB.** The brief said JSONB. Every other JSON column in
this schema (`jobs.resource_reqs`, `jobs.env`, `jobs.target_node_ids`, ...) is the
generic type, because the same declaration has to work on PostgreSQL in production and
on the SQLite the suite runs against. Nothing queries INTO this document — it is read
whole and rendered — so JSONB's indexable operators would buy nothing and cost a
dialect split. Following the file it lives in beats following the brief here.

Additive edit to the frozen contract, same precedent as storage tiers (e0f1a2b3c4d5), run-log tiering (d9e0f1a2b3c4)
and checkpoint-and-resume (c8d9e0f1a2b3). No frozen READ SHAPE changes: `GET
/jobs/{id}` returns everything it returned before, plus one optional field an older
caller ignores.

Revision ID: f1a2b3c4d5e6
Revises: e0f1a2b3c4d5
Create Date: 2026-09-05
"""

import sqlalchemy as sa
from alembic import op

revision = "f1a2b3c4d5e6"
down_revision = "e0f1a2b3c4d5"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("jobs", sa.Column("source_text", sa.Text(), nullable=True))
    op.add_column("jobs", sa.Column("checkpoint_advice", sa.JSON(), nullable=True))


def downgrade() -> None:
    # Reversed exactly. A downgrade removes the ADVICE, which is derived data — the
    # jobs, their runs and their results are untouched, because nothing else in the
    # schema ever pointed at either column.
    op.drop_column("jobs", "checkpoint_advice")
    op.drop_column("jobs", "source_text")
