"""nodes: add hw_specs + usage (rich dashboard, supervisor-requested)

Two nullable JSON columns — purely additive, so every existing row and every
old agent keeps working (an agent that never sends them just leaves NULLs).

  hw_specs  rich machine identity (CPU/GPU names, RAM speed, machine model,
            OS, software versions), reported once at registration
  usage     latest usage sample (cpu_pct, ram_pct, ...), refreshed each
            heartbeat — current value only, no history (history = the
            Prometheus/Grafana stretch goal)

Additive edit to the frozen contract.

Revision ID: b4d1c9e2a7f0
Revises: 3816ff8401ff
Create Date: 2026-07-06
"""

import sqlalchemy as sa
from alembic import op

revision = "b4d1c9e2a7f0"
down_revision = "3816ff8401ff"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("nodes", sa.Column("hw_specs", sa.JSON(), nullable=True))
    op.add_column("nodes", sa.Column("usage", sa.JSON(), nullable=True))


def downgrade() -> None:
    op.drop_column("nodes", "usage")
    op.drop_column("nodes", "hw_specs")
