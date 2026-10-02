"""A run can be cancelled — one nullable column, and no new state

`runs.cancel_requested_at`: when a user asked for this run to stop, or null.

**Why a column and not a state (walk 1, row 64).** The run state machine
(protocol.md §3) is the thing the report draws and the jury asks about, and a
cancelled run does not need a sixth box: it ENDS, and a run that ends without a
result is `FAILED` — here with `failure_reason = CANCELLED`, which is where every
other explanation of an ending already lives. What a cancel needs that a state cannot
give is a *request* that outlives the click: the worker holding the run is told at
its next heartbeat (`commands: [{type: "cancel", run_id}]`, a field the contract has
carried since W1 and nothing ever filled), and if that worker never answers, the
reaper — which would requeue the run when its lease expires — finishes it as
cancelled instead. The stamp is what both of those read.

A `PENDING` run needs no stamp: nothing holds it, so the cancel route ends it in the
same request.

**Purely additive.** Nullable, no default needed, no row changes; every run made
before this column exists reads null, which means "nobody asked". The downgrade drops
the column and nothing else, and a run stamped in between simply forgets the request
— the worst that costs is a run finishing that someone had asked to stop.

Revision ID: d5e6f7a8b9c0
Revises: c4d5e6f7a8b9
Create Date: 2026-09-07
"""

import sqlalchemy as sa
from alembic import op

revision = "d5e6f7a8b9c0"
down_revision = "c4d5e6f7a8b9"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "runs",
        sa.Column("cancel_requested_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("runs", "cancel_requested_at")
