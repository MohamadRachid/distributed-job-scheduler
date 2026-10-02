"""Tier sizing for the reference lab - the two caps derived rather than exemplified

The figures seeded on 2026-09-04 (2,000 GB retained / 500 GB scratch for `standard`)
were the supervisor's own examples, carried as configuration and labelled in
`protocol.md` as "never measured". So were the alternatives he offered afterwards
(50 GB / 2,000 GB). This migration replaces both with numbers DERIVED from two
anchors, for a stated reference lab. The derivation is the point; the numbers are its
output and they move if the anchors do.

**The reference lab (an assumption, and it goes to M. Ayli as a question).** Ten
workers, each about 500 GB free and able to host four runs at once; one storage server
with a 4 TB MinIO volume; twenty users created by the admin. Nothing in this
repository has ten machines — the demonstration host is one laptop — so this is a
stated model of a lab, not a measurement of ours, and the proof file prints the real
`df` of the demo host beside it so the gap is visible rather than implied.

**Scratch (temporary disk one run may use on a worker) -> 50 GB.**

    dataset pulled via the Dataset URL field            ~10 GB
    one checkpoint for a billion-parameter model         ~12 GB
        (12.044 bytes per parameter, measured:
         docs/evidence/checkpoint_reshape_2026-09-01.txt)
    output files and /tmp                                 ~3 GB
                                                        -------
                                                        ~25 GB, doubled -> 50 GB

    Fit: 4 concurrent runs x 50 GB = 200 GB, inside the 500 GB a worker has free.

**Retained (bytes the platform keeps on the server for one user) -> 200 GB.**

    ten kept sweeps of 100 jobs x 50 MB (MAX_ARTIFACT_MB)  50 GB
    five concurrent runs each holding one 12 GB checkpoint 60 GB
        (one checkpoint per run per attempt, stable object
         key, agent/runner.py CHECKPOINT_FILENAME)
    archived logs                                          ~a few GB
                                                          --------
                                                          ~110 GB, headroom -> 200 GB

    Fit: the sum of every user's cap stays inside 80% of the volume (3.2 TB) —
    5 standard + 15 limited = 5x200 + 15x20 = 1,300 GB.

`limited` is one tenth of `standard` on both numbers.

**What this migration does NOT do, and the reason.** It updates a seeded row only
while that row still holds the figure this project seeded. A deployment that edited
its own numbers keeps them — the same rule `quota.ensure_tiers` already follows
("doing nothing once a row is present means a deployment that edited its numbers keeps
them"). A migration that overwrote an operator's deliberate setting would be a config
change wearing a schema change's clothes.

**One real consequence, stated rather than discovered.** Acceptance is stored as WHAT
WAS AGREED — the tier and both caps as they stood. Changing a tier's
figures therefore WITHDRAWS every acceptance that referred to them, by arithmetic and
with no flag to reset. That is the designed behaviour and it is correct here: the
numbers a user agreed to are not the numbers that now apply, so they are asked again
before they submit. Every existing user must re-accept after this migration. Nobody
loses data and no job is touched.

No schema change: no table, no column, no type. Only the two seeded rows' values.

Revision ID: a2b3c4d5e6f7
Revises: f1a2b3c4d5e6
Create Date: 2026-09-05
"""

import sqlalchemy as sa
from alembic import op

revision = "a2b3c4d5e6f7"
down_revision = "f1a2b3c4d5e6"
branch_labels = None
depends_on = None

_GB = 1024 * 1024 * 1024

# Written out in full beside the arithmetic, so the number stored is readable in the
# migration without anyone having to multiply — and so a mistyped multiplier shows up
# as a disagreement between the two rather than as a silently wrong cap.
STANDARD_RETAINED = 200 * _GB          # 214,748,364,800 bytes
STANDARD_SCRATCH = 50 * _GB            #  53,687,091,200 bytes
LIMITED_RETAINED = 20 * _GB            #  21,474,836,480 bytes
LIMITED_SCRATCH = 5 * _GB              #   5,368,709,120 bytes

OLD_STANDARD_RETAINED = 2000 * _GB     # 2,147,483,648,000 bytes
OLD_STANDARD_SCRATCH = 500 * _GB       #   536,870,912,000 bytes
OLD_LIMITED_RETAINED = 200 * _GB       #   214,748,364,800 bytes
OLD_LIMITED_SCRATCH = 50 * _GB         #    53,687,091,200 bytes

# "plan" is the supervisor's own word for a tier, used once in each description.
STANDARD_DESC = (
    "The standard plan: 200 GB retained on the server, 50 GB of temporary disk per "
    "run on a worker. Sized for a reference lab of ten workers (~500 GB free each, "
    "four runs at once), one 4 TB storage volume and twenty users."
)
LIMITED_DESC = (
    "The limited plan: 20 GB retained on the server, 5 GB of temporary disk per run "
    "on a worker; one tenth of the standard plan on both numbers."
)

_MOVES = (
    # (id, old retained, old scratch, new retained, new scratch, new description)
    (
        "standard",
        OLD_STANDARD_RETAINED,
        OLD_STANDARD_SCRATCH,
        STANDARD_RETAINED,
        STANDARD_SCRATCH,
        STANDARD_DESC,
    ),
    (
        "limited",
        OLD_LIMITED_RETAINED,
        OLD_LIMITED_SCRATCH,
        LIMITED_RETAINED,
        LIMITED_SCRATCH,
        LIMITED_DESC,
    ),
)


def _move(rows) -> None:
    """Apply each (id, from, to) only where the row still holds the `from` figures."""
    conn = op.get_bind()
    for tier_id, from_retained, from_scratch, to_retained, to_scratch, desc in rows:
        conn.execute(
            sa.text(
                "UPDATE tiers SET retained_cap_bytes = :r, scratch_cap_bytes = :s, "
                "description = :d WHERE id = :i "
                "AND retained_cap_bytes = :fr AND scratch_cap_bytes = :fs"
            ),
            {
                "r": to_retained,
                "s": to_scratch,
                "d": desc,
                "i": tier_id,
                "fr": from_retained,
                "fs": from_scratch,
            },
        )


def upgrade() -> None:
    _move(_MOVES)


def downgrade() -> None:
    # The mirror: back to the 2026-09-04 seeded figures, and again only where the row
    # still holds what this migration wrote.
    conn = op.get_bind()
    for tier_id, to_retained, to_scratch, from_retained, from_scratch, _ in _MOVES:
        desc = (
            "The default tier: 2,000 GB retained on the server, 500 GB of temporary "
            "disk per run on a worker."
            if tier_id == "standard"
            else "A smaller allowance: 200 GB retained on the server, 50 GB of "
            "temporary disk per run on a worker."
        )
        conn.execute(
            sa.text(
                "UPDATE tiers SET retained_cap_bytes = :r, scratch_cap_bytes = :s, "
                "description = :d WHERE id = :i "
                "AND retained_cap_bytes = :fr AND scratch_cap_bytes = :fs"
            ),
            {
                "r": to_retained,
                "s": to_scratch,
                "d": desc,
                "i": tier_id,
                "fr": from_retained,
                "fs": from_scratch,
            },
        )
