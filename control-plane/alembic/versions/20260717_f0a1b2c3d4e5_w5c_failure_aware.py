"""W5c failure-aware rescheduling — learned RAM requirement + escalation counter

Purely additive, so every existing row and every OLD agent keeps working. These
two columns are CONTROL-PLANE-ONLY (the agent's messages do not change):

  runs + learned_min_ram_mb (int, nullable)  the RAM a resource-proven OOM taught
                                             us this run needs strictly more of;
                                             the claim query filters on it
       + escalation_count   (int, NOT NULL, default 0)  how many times this run has
                                             already been escalated to a stronger
                                             node (bounds the retries)

`escalation_count` is NOT NULL with a server_default of 0 so the column backfills
cleanly on the existing rows of a live DB, while new ORM inserts use the model
default. Additive edit to the frozen contract (W5c),
same precedent as the W5b (c7e2f9a4b310) and W4-dashboard (b4d1c9e2a7f0) migrations.

Revision ID: f0a1b2c3d4e5
Revises: c7e2f9a4b310
Create Date: 2026-07-17
"""

import sqlalchemy as sa
from alembic import op

revision = "f0a1b2c3d4e5"
down_revision = "c7e2f9a4b310"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("runs", sa.Column("learned_min_ram_mb", sa.Integer(), nullable=True))
    op.add_column(
        "runs",
        sa.Column("escalation_count", sa.Integer(), nullable=False, server_default="0"),
    )


def downgrade() -> None:
    op.drop_column("runs", "escalation_count")
    op.drop_column("runs", "learned_min_ram_mb")
