"""Run-log tiering — the archive pointer table

Purely additive: ONE new table, nothing altered and nothing dropped. Every existing
row, every existing query and every older agent keeps working unchanged, because
nothing reads this table unless a row exists in it, and no row exists until the
archiver has written, verified and purged one (run, attempt)'s log chunks.

  run_log_archives  (run_id, attempt)  PRIMARY KEY   where one attempt's logs went.

The primary key is the pair, not a surrogate id. That is deliberate and it is the
schema making a promise instead of a convention making one: "ONE compressed object
per (run, attempt)" is then enforced by the database, so
re-archiving the same attempt after a crash between the write and the purge is an
upsert, and a second pointer for one attempt cannot exist to be disagreed with.

  object_key   where the bytes are: logs/{run_id}/{attempt}.log.gz
  chunk_count  how many chunks the object holds — half of the purge receipt
  max_seq      the largest sequence number inside it, so a read whose cursor is
               already past the archive never fetches the object at all
  sha256       the digest of the canonical body, computed from the rows BEFORE
               they were deleted and re-verified against the bytes read back out
               of the object store. This is the other half of the receipt: it is
               what makes the purge a two-phase commit rather than a hopeful
               delete, and what a later integrity check would compare against
  archived_at  when the move happened

Why a table and not columns on `runs`: an archive belongs to a (run, ATTEMPT), and a
recovered run has more than one attempt. Columns on `runs` could hold exactly one
pointer per run, which is the wrong shape for the thing being pointed at.

Additive edit to the frozen contract, same precedent
as checkpoint-and-resume (c8d9e0f1a2b3), W6b (b7c8d9e0f1a2), W6 (a1b2c3d4e5f6) and
W5c (f0a1b2c3d4e5). The frozen READ SHAPES are untouched: `GET /runs/{id}/logs` and
`WS /runs/{id}/logs` return exactly the fields they returned before, whether a chunk
comes from a row or from the archive.

Revision ID: d9e0f1a2b3c4
Revises: c8d9e0f1a2b3
Create Date: 2026-08-22
"""

import sqlalchemy as sa
from alembic import op

revision = "d9e0f1a2b3c4"
down_revision = "c8d9e0f1a2b3"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "run_log_archives",
        sa.Column("run_id", sa.String(length=36), nullable=False),
        sa.Column("attempt", sa.Integer(), nullable=False),
        sa.Column("object_key", sa.String(length=512), nullable=False),
        sa.Column("chunk_count", sa.Integer(), nullable=False),
        sa.Column("max_seq", sa.Integer(), nullable=False),
        sa.Column("sha256", sa.String(length=64), nullable=False),
        sa.Column("archived_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["run_id"], ["runs.id"]),
        sa.PrimaryKeyConstraint("run_id", "attempt"),
    )


def downgrade() -> None:
    # Dropping the pointers does NOT delete the archived objects — they stay in the
    # object store, named by a key this table's own layout makes reconstructible
    # from (run_id, attempt). A downgrade therefore hides archived logs rather than
    # destroying them, which is the right way round for a reversal of a change
    # whose entire purpose was to not lose anything.
    op.drop_table("run_log_archives")
