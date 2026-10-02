"""W6 artifacts — fencing attempt + idempotency uniqueness

Purely additive, so every existing row keeps working and OLD agents (which never
upload an artifact) stay valid:

  artifacts + attempt (int, NOT NULL, server_default 0)  the fencing token: the
                                                         attempt that produced the
                                                         file (like run_logs.attempt)
            + UNIQUE(run_id, attempt, object_key)        a re-sent upload upserts one
                                                         row (idempotent, same idea as
                                                         run_logs' UNIQUE); a stale
                                                         attempt's file cannot pose as
                                                         the accepted result

There are no artifact rows in any live DB before W6 (nothing wrote them), so the
NOT NULL column backfills trivially; server_default 0 keeps it safe regardless.
Additive edit to the frozen contract (W6), same
precedent as W5c (f0a1b2c3d4e5), W5b (c7e2f9a4b310), W4-dashboard (b4d1c9e2a7f0).

Revision ID: a1b2c3d4e5f6
Revises: f0a1b2c3d4e5
Create Date: 2026-07-24
"""

import sqlalchemy as sa
from alembic import op

revision = "a1b2c3d4e5f6"
down_revision = "f0a1b2c3d4e5"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "artifacts",
        sa.Column("attempt", sa.Integer(), nullable=False, server_default="0"),
    )
    op.create_unique_constraint(
        "uq_artifacts_run_attempt_key",
        "artifacts",
        ["run_id", "attempt", "object_key"],
    )


def downgrade() -> None:
    op.drop_constraint("uq_artifacts_run_attempt_key", "artifacts", type_="unique")
    op.drop_column("artifacts", "attempt")
