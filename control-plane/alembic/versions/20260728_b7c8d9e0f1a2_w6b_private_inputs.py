"""W6b private job inputs — sealed delivery, trust tier, keys and tickets

Purely additive, so every existing row keeps working and OLD agents stay valid
(they can never be offered a private run — the claim query requires `trusted` AND
agent_version >= 0.8.0 — so their message shapes never change):

  jobs  + private (bool, NOT NULL, default false)   this job's input is SEALED
        + input_object_key (text, nullable)         where the ciphertext lives
        + input_filename (text, nullable)           the original name, for the UI

  nodes + trusted (bool, NOT NULL, default false)   may this machine run private
                                                    jobs? ADMIN-set only — nothing
                                                    a node reports can change it

  job_keys      job_id (PK/FK) · key_b64 · created_at
                The AES-GCM key for one private job. A SEPARATE TABLE on purpose:
                "the key is stored apart from the data it opens" is then literally
                visible in the DB tour, and crypto-shred is one DELETE.

  key_tickets   ticket (PK) · run_id (FK) · attempt · expires_at · redeemed_at
                · created_at
                The single-use pass a container redeems ONCE for the key, so the key
                never sits in an env var where `docker inspect` would show it.
                `attempt` is carried so the redeem can re-check the fence.

Defaults are chosen so a live database upgrades with no downtime and no backfill:
every existing job is non-private, every existing node is untrusted (fail-closed —
after this migration NO machine can receive private work until an admin says so).

Additive edit to the frozen contract (W6b), same
precedent as W6 (a1b2c3d4e5f6), W5c (f0a1b2c3d4e5), W5b (c7e2f9a4b310),
W4-dashboard (b4d1c9e2a7f0).

Revision ID: b7c8d9e0f1a2
Revises: a1b2c3d4e5f6
Create Date: 2026-07-28
"""

import sqlalchemy as sa
from alembic import op

revision = "b7c8d9e0f1a2"
down_revision = "a1b2c3d4e5f6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "jobs",
        sa.Column("private", sa.Boolean(), nullable=False, server_default="0"),
    )
    op.add_column("jobs", sa.Column("input_object_key", sa.String(512), nullable=True))
    op.add_column("jobs", sa.Column("input_filename", sa.String(255), nullable=True))

    op.add_column(
        "nodes",
        sa.Column("trusted", sa.Boolean(), nullable=False, server_default="0"),
    )

    op.create_table(
        "job_keys",
        sa.Column("job_id", sa.String(36), sa.ForeignKey("jobs.id"), primary_key=True),
        sa.Column("key_b64", sa.String(64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )

    op.create_table(
        "key_tickets",
        sa.Column("ticket", sa.String(64), primary_key=True),
        sa.Column("run_id", sa.String(36), sa.ForeignKey("runs.id"), nullable=False),
        sa.Column("attempt", sa.Integer(), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("redeemed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_key_tickets_run_id", "key_tickets", ["run_id"])


def downgrade() -> None:
    op.drop_index("ix_key_tickets_run_id", table_name="key_tickets")
    op.drop_table("key_tickets")
    op.drop_table("job_keys")
    op.drop_column("nodes", "trusted")
    op.drop_column("jobs", "input_filename")
    op.drop_column("jobs", "input_object_key")
    op.drop_column("jobs", "private")
