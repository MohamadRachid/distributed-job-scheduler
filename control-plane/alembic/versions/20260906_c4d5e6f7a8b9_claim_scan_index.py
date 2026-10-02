"""An index for the claim query's walk of the pending queue

One index, nothing else: `runs(status, created_at)`.

**Why it is needed now and was not before.** Until 2026-09-06 the claim query read a
fixed window of the queue's head — `LIMIT spare * 4` — so it never looked at more than
a handful of rows and an index bought nothing. That window was the head-of-line
blocking defect: a run this node could not take still occupied a place in it, so a
queue whose head was full of such runs hid everything behind them. The window is gone
and the scheduler now walks the queue until it finds work or reaches the end, which is
what makes starvation impossible — and what makes the shape of that walk worth
indexing.

`(status, created_at)` is that shape exactly: the filter first, the order second, so
the database reads the pending rows in the order the scan wants them rather than
sorting the whole table on every heartbeat from every machine.

**Purely additive, and reversible with nothing at stake.** An index is not data. The
downgrade drops it and the scheduler still returns the same answers, only by reading
more.

Revision ID: c4d5e6f7a8b9
Revises: b3c4d5e6f7a8
Create Date: 2026-09-06
"""

from alembic import op

revision = "c4d5e6f7a8b9"
down_revision = "b3c4d5e6f7a8"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index("ix_runs_status_created_at", "runs", ["status", "created_at"])


def downgrade() -> None:
    op.drop_index("ix_runs_status_created_at", table_name="runs")
