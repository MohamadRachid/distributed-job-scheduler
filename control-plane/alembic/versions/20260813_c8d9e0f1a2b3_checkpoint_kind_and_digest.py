"""Checkpoint and resume — artifact kind + stored digest

Purely additive, so every existing row keeps working and OLD agents (which never send
`kind` and never upload a checkpoint) stay valid:

  artifacts + kind   (str(16), NOT NULL, server_default 'result')  what this file IS.
                     Everything written before this date was a result, so the default
                     backfills every live row correctly and an old agent's upload
                     still lands as a result.
  artifacts + sha256 (str(64), NULL)  the digest of the bytes the control plane
                     stored, computed by the control plane. NULL on old rows: no
                     digest means "cannot verify", and an unverifiable checkpoint is
                     refused rather than accepted unchecked.

Why a column and not a filename prefix: the guard is "a checkpoint can never be
served as a finished result". A column is enforced by the query; a naming convention
is enforced by whoever remembers it.

Additive edit to the frozen contract, same precedent
as W6b (b7c8d9e0f1a2), W6 (a1b2c3d4e5f6), W5c (f0a1b2c3d4e5).

Revision ID: c8d9e0f1a2b3
Revises: b7c8d9e0f1a2
Create Date: 2026-08-13
"""

import sqlalchemy as sa
from alembic import op

revision = "c8d9e0f1a2b3"
down_revision = "b7c8d9e0f1a2"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "artifacts",
        sa.Column(
            "kind", sa.String(length=16), nullable=False, server_default="result"
        ),
    )
    op.add_column(
        "artifacts", sa.Column("sha256", sa.String(length=64), nullable=True)
    )


def downgrade() -> None:
    op.drop_column("artifacts", "sha256")
    op.drop_column("artifacts", "kind")
