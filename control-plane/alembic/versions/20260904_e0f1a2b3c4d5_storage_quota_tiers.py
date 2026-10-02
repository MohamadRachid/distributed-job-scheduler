"""Storage quota policy — tiers, roles, acceptance, and the columns the sum reads

Purely additive: ONE new table and eight new columns, nothing altered and nothing
dropped. Every existing row, every existing query and every older agent keeps working
unchanged — an agent that has never heard of a scratch cap reports no free disk,
receives no cap, and runs exactly as it did yesterday.

  tiers                       (id PK = the tier's name)      the two numbers
  users.is_admin                                            the one role flag
  users.tier_id                                             which numbers apply
  users.limits_accepted_tier / _at / _retained_cap_bytes / _scratch_cap_bytes
                                                            WHAT was agreed
  runs.quota_refused_at / _detail                           R3 writes, R4 reads
  run_log_archives.size_bytes                               archived logs, counted
  jobs.input_size_bytes                                     sealed inputs, counted
  nodes.disk_free_mb                                        placement's disk signal
  run_samples.scratch_used_mb                               the scratch reading

**Why the tier's primary key is its name.** A tier has no identity apart from what it
is called, so an `id` beside a UNIQUE `name` would be two columns making one
statement. Naming it directly is also what lets `users.tier_id` carry
`server_default='standard'`: every user row that existed before this migration lands
in a tier because the schema says so, not because a data-migration step ran. There is
no user row this can miss.

**Why acceptance is four columns and not a boolean.** A user agrees to two NUMBERS,
not to a word. Recording the tier and both caps as they stood at the moment of
agreement means a tier move — or an edit to a tier's figures — withdraws acceptance
by arithmetic (`quota.limits_accepted`), with nothing to remember to reset and
nothing that can drift out of step.

**The order below is load-bearing.** `tiers` is created and seeded BEFORE
`users.tier_id` is added, because that column is NOT NULL with a foreign key: the row
it defaults to has to exist first.

Additive edit to the frozen contract, same precedent
as run-log tiering (d9e0f1a2b3c4), checkpoint-and-resume (c8d9e0f1a2b3), W6b
(b7c8d9e0f1a2), W6 (a1b2c3d4e5f6) and W5c (f0a1b2c3d4e5). No frozen READ SHAPE
changes: every response listed in protocol.md §9–§10 returns the fields it returned
before, plus optional additions old callers ignore.

Revision ID: e0f1a2b3c4d5
Revises: d9e0f1a2b3c4
Create Date: 2026-09-04
"""

import sqlalchemy as sa
from alembic import op

revision = "e0f1a2b3c4d5"
down_revision = "d9e0f1a2b3c4"
branch_labels = None
depends_on = None

# The seeded tiers. These figures are CONFIGURATION and were never measured: they are
# the examples the supervisor gave on 2026-09-03. Every proof of the mechanism runs at
# megabytes instead, so a refusal happens live and in seconds.
_GB = 1024 * 1024 * 1024
_SEED = (
    (
        "standard",
        2000 * _GB,
        500 * _GB,
        "The default tier: 2,000 GB retained on the server, 500 GB of temporary "
        "disk per run on a worker.",
    ),
    (
        "limited",
        200 * _GB,
        50 * _GB,
        "A smaller allowance: 200 GB retained on the server, 50 GB of temporary "
        "disk per run on a worker.",
    ),
)


def upgrade() -> None:
    tiers = op.create_table(
        "tiers",
        sa.Column("id", sa.String(length=32), nullable=False),
        sa.Column("retained_cap_bytes", sa.BigInteger(), nullable=False),
        sa.Column("scratch_cap_bytes", sa.BigInteger(), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.bulk_insert(
        tiers,
        [
            {
                "id": tid,
                "retained_cap_bytes": retained,
                "scratch_cap_bytes": scratch,
                "description": desc,
            }
            for tid, retained, scratch, desc in _SEED
        ],
    )

    op.add_column(
        "users",
        sa.Column(
            "is_admin", sa.Boolean(), nullable=False, server_default=sa.text("false")
        ),
    )
    # The startup admin is the user the bootstrap created, and at our scope it is the
    # only user there is. Named by "earliest created" rather than by username, so the
    # statement stays correct whatever ADMIN_USERNAME was set to, and stays a single
    # row even if a deployment somehow has more.
    op.execute(
        "UPDATE users SET is_admin = true "
        "WHERE id = (SELECT id FROM users ORDER BY created_at LIMIT 1)"
    )
    op.add_column(
        "users",
        sa.Column(
            "tier_id",
            sa.String(length=32),
            nullable=False,
            server_default="standard",
        ),
    )
    op.create_foreign_key(
        "fk_users_tier_id", "users", "tiers", ["tier_id"], ["id"]
    )
    op.add_column(
        "users", sa.Column("limits_accepted_tier", sa.String(length=32), nullable=True)
    )
    op.create_foreign_key(
        "fk_users_limits_accepted_tier",
        "users",
        "tiers",
        ["limits_accepted_tier"],
        ["id"],
    )
    op.add_column(
        "users",
        sa.Column("limits_accepted_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "users",
        sa.Column("limits_accepted_retained_cap_bytes", sa.BigInteger(), nullable=True),
    )
    op.add_column(
        "users",
        sa.Column("limits_accepted_scratch_cap_bytes", sa.BigInteger(), nullable=True),
    )

    op.add_column(
        "runs", sa.Column("quota_refused_at", sa.DateTime(timezone=True), nullable=True)
    )
    op.add_column("runs", sa.Column("quota_refused_detail", sa.Text(), nullable=True))
    op.add_column("jobs", sa.Column("input_size_bytes", sa.BigInteger(), nullable=True))
    op.add_column(
        "run_log_archives", sa.Column("size_bytes", sa.BigInteger(), nullable=True)
    )
    op.add_column("nodes", sa.Column("disk_free_mb", sa.Integer(), nullable=True))
    op.add_column(
        "run_samples", sa.Column("scratch_used_mb", sa.Float(), nullable=True)
    )


def downgrade() -> None:
    # Reversed exactly, and in the mirror order: the columns that point at `tiers` go
    # before `tiers` does. Nothing here deletes a byte of anyone's data — a downgrade
    # removes the POLICY, not the objects it was counting, which is the right way
    # round for reversing a change whose whole purpose was to bound storage rather
    # than to reclaim it.
    op.drop_column("run_samples", "scratch_used_mb")
    op.drop_column("nodes", "disk_free_mb")
    op.drop_column("run_log_archives", "size_bytes")
    op.drop_column("jobs", "input_size_bytes")
    op.drop_column("runs", "quota_refused_detail")
    op.drop_column("runs", "quota_refused_at")

    op.drop_constraint("fk_users_limits_accepted_tier", "users", type_="foreignkey")
    op.drop_constraint("fk_users_tier_id", "users", type_="foreignkey")
    op.drop_column("users", "limits_accepted_scratch_cap_bytes")
    op.drop_column("users", "limits_accepted_retained_cap_bytes")
    op.drop_column("users", "limits_accepted_at")
    op.drop_column("users", "limits_accepted_tier")
    op.drop_column("users", "tier_id")
    op.drop_column("users", "is_admin")

    op.drop_table("tiers")
