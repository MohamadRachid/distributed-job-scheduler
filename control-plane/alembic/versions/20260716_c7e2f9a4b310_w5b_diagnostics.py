"""W5b run + node diagnostics — reasons, samples, progress, battery, node events

Purely additive, so every existing row and every OLD agent keeps working:
  runs         + failure_reason / failure_detail (why a dead run died),
               + progress / metrics_last (live ##PROGRESS tracking)  — all nullable
  nodes        + battery_pct / battery_charging (the last black-box picture) — nullable
  run_samples  NEW — the container's own CPU%/RAM-vs-limit history, one row per
               (run, attempt, sample); UNIQUE(run_id, attempt, ts) makes a resent
               batch idempotent (same idea as run_logs)
  node_events  NEW — one row per diagnosed node event (goodbye / comeback cause)

Additive edit to the frozen contract (W5b), same
precedent as the W4 dashboard migration b4d1c9e2a7f0.

Revision ID: c7e2f9a4b310
Revises: b4d1c9e2a7f0
Create Date: 2026-07-16
"""

import sqlalchemy as sa
from alembic import op

revision = "c7e2f9a4b310"
down_revision = "b4d1c9e2a7f0"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # runs: why-it-died + live-progress columns
    op.add_column("runs", sa.Column("failure_reason", sa.String(length=32), nullable=True))
    op.add_column("runs", sa.Column("failure_detail", sa.Text(), nullable=True))
    op.add_column("runs", sa.Column("progress", sa.Float(), nullable=True))
    op.add_column("runs", sa.Column("metrics_last", sa.JSON(), nullable=True))

    # nodes: last black-box battery reading
    op.add_column("nodes", sa.Column("battery_pct", sa.Float(), nullable=True))
    op.add_column("nodes", sa.Column("battery_charging", sa.Boolean(), nullable=True))

    # run_samples: per-run resource history
    op.create_table(
        "run_samples",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("run_id", sa.String(length=36), nullable=False),
        sa.Column("attempt", sa.Integer(), nullable=False),
        sa.Column("ts", sa.Float(), nullable=False),
        sa.Column("cpu_pct", sa.Float(), nullable=True),
        sa.Column("mem_used_mb", sa.Float(), nullable=True),
        sa.Column("mem_limit_mb", sa.Float(), nullable=True),
        sa.ForeignKeyConstraint(["run_id"], ["runs.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("run_id", "attempt", "ts", name="uq_run_samples_run_attempt_ts"),
    )
    op.create_index("ix_run_samples_run_id", "run_samples", ["run_id"], unique=False)

    # node_events: node postmortem history
    op.create_table(
        "node_events",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("node_id", sa.String(length=36), nullable=False),
        sa.Column("ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("event", sa.String(length=32), nullable=False),
        sa.Column("cause", sa.String(length=32), nullable=True),
        sa.Column("evidence", sa.JSON(), nullable=True),
        sa.ForeignKeyConstraint(["node_id"], ["nodes.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_node_events_node_id", "node_events", ["node_id"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_node_events_node_id", table_name="node_events")
    op.drop_table("node_events")
    op.drop_index("ix_run_samples_run_id", table_name="run_samples")
    op.drop_table("run_samples")
    op.drop_column("nodes", "battery_charging")
    op.drop_column("nodes", "battery_pct")
    op.drop_column("runs", "metrics_last")
    op.drop_column("runs", "progress")
    op.drop_column("runs", "failure_detail")
    op.drop_column("runs", "failure_reason")
