"""Does the quota's arithmetic still describe what storage is actually holding?

Run it the way the chaos test is run — INSIDE the pinned control-plane image, with
`scripts/` mounted, because it imports the application and talks to both PostgreSQL
and MinIO and neither virtual environment on the development machine carries those:

    docker compose run --rm -v "${PWD}/scripts:/scripts" control-plane \\
        python /scripts/quota_audit.py

    docker compose run --rm -v "${PWD}/scripts:/scripts" control-plane \\
        python /scripts/quota_audit.py --backfill

**Why this exists.** The retained figure is a SUM over rows, and a sum cannot drift
away from the rows it is computed from. What it can drift away from is the object
store, because those are two systems: an object deleted without its row, a row
deleted without its object, or a row written before this feature existed and
therefore carrying no size at all. This script is the check that says so out loud
instead of leaving it to be believed — the same rule the rest of this project follows,
that when something must stay true it gets a check rather than a reminder.

It reports three things per user and one thing overall:

  * `rows_bytes`  — what `quota.retained_used_bytes` says the user is holding, which
    is the number that refuses their uploads;
  * `objects_bytes` — what the object store actually holds for the keys those rows
    point at;
  * `missing` / `unsized` — rows whose object is not in the store, and rows carrying
    no size at all (the pre-existing ones `--backfill` fills in);
  * `untracked_bytes` — objects in the bucket that NO row points at. These belong to
    nobody, so they are charged to nobody, and they are the one drift a per-user
    number can never reveal.

Exit code is non-zero when anything is out of step, so it can be run as a gate rather
than read as a report.

`--backfill` writes the sizes it can prove: `run_log_archives.size_bytes` and
`jobs.input_size_bytes` for rows written before 2026-09-04, taken from the object the
row already points at. It never invents a size for a missing object, and it never
touches `artifacts.size`, which has been written since W6.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from dataclasses import dataclass, field

sys.path.insert(0, "/app")  # the image's application root

from app.db import SessionLocal  # noqa: E402
from app.models import Tier  # noqa: E402
from app.models import Artifact, Job, Run, RunLogArchive, User  # noqa: E402
from app.quota import reconcile  # noqa: E402
from app.storage import get_storage  # noqa: E402
from sqlalchemy import select  # noqa: E402

_MB = 1024 * 1024


def mb(n: int) -> float:
    return round(n / _MB, 3)


def gb(n: int) -> float:
    return round(n / (1024 * _MB), 3)


@dataclass
class UserAudit:
    """One user's row-side and object-side pictures, and the gap between them."""

    username: str
    rows_bytes: int = 0
    objects_bytes: int = 0
    missing: list[str] = field(default_factory=list)
    unsized: list[str] = field(default_factory=list)

    @property
    def drift(self) -> int:
        return self.rows_bytes - self.objects_bytes

    @property
    def clean(self) -> bool:
        return self.drift == 0 and not self.missing and not self.unsized


def list_objects(storage) -> dict[str, int]:
    """Every object in the bucket, key -> size.

    Reaches past the narrow `ObjectStore` protocol to the MinIO client itself, and
    this is the one caller for which that is right: "an object nobody points at" is a
    question no row can answer, so the audit has to be able to see the whole store
    rather than only the parts the application already knows about. Synchronous,
    because the MinIO SDK is — the caller runs it on a worker thread."""
    client = storage._client  # noqa: SLF001 - see above
    bucket = storage._bucket  # noqa: SLF001
    return {
        obj.object_name: int(obj.size or 0)
        for obj in client.list_objects(bucket, recursive=True)
    }


async def audit(backfill: bool = False, volume_bytes: int | None = None) -> int:
    storage = get_storage()
    objects = await asyncio.to_thread(list_objects, storage)

    claimed: dict[str, int] = {}  # object key -> the size a row claims
    audits: list[UserAudit] = []

    async with SessionLocal() as session:
        users = (await session.execute(select(User).order_by(User.created_at))).scalars().all()
        for user in users:
            ua = UserAudit(username=user.username)

            arts = (
                await session.execute(
                    select(Artifact)
                    .join(Run, Run.id == Artifact.run_id)
                    .join(Job, Job.id == Run.job_id)
                    .where(Job.user_id == user.id)
                )
            ).scalars().all()
            archives = (
                await session.execute(
                    select(RunLogArchive)
                    .join(Run, Run.id == RunLogArchive.run_id)
                    .join(Job, Job.id == Run.job_id)
                    .where(Job.user_id == user.id)
                )
            ).scalars().all()
            jobs = (
                await session.execute(
                    select(Job).where(
                        Job.user_id == user.id, Job.input_object_key.is_not(None)
                    )
                )
            ).scalars().all()

            rows: dict[str, int] = {}
            for a in arts:
                rows[a.object_key] = int(a.size or 0)
                if a.size is None:
                    ua.unsized.append(a.object_key)
            for a in archives:
                if a.size_bytes is None:
                    ua.unsized.append(a.object_key)
                    if backfill and a.object_key in objects:
                        a.size_bytes = objects[a.object_key]
                        ua.unsized.pop()
                rows[a.object_key] = int(a.size_bytes or 0)
            for j in jobs:
                if j.input_size_bytes is None:
                    ua.unsized.append(j.input_object_key)
                    if backfill and j.input_object_key in objects:
                        j.input_size_bytes = objects[j.input_object_key]
                        ua.unsized.pop()
                rows[j.input_object_key] = int(j.input_size_bytes or 0)

            ua.rows_bytes = sum(rows.values())
            ua.objects_bytes, ua.missing = reconcile(rows, objects)
            claimed.update(rows)
            audits.append(ua)

        if backfill:
            await session.commit()

    untracked = {k: v for k, v in objects.items() if k not in claimed}

    print("=== storage quota audit ===")
    print(f"users: {len(audits)} | objects in bucket: {len(objects)}")
    print()
    print(f"{'user':<20} {'rows MB':>12} {'objects MB':>12} {'drift MB':>12}  notes")
    dirty = False
    for ua in audits:
        notes = []
        if ua.missing:
            notes.append(f"{len(ua.missing)} row(s) point at objects that are gone")
        if ua.unsized:
            notes.append(f"{len(ua.unsized)} row(s) carry no size")
        if not ua.clean:
            dirty = True
        print(
            f"{ua.username:<20} {mb(ua.rows_bytes):>12} {mb(ua.objects_bytes):>12} "
            f"{mb(ua.drift):>12}  {'; '.join(notes)}"
        )
        for key in ua.missing[:5]:
            print(f"    missing object: {key}")
        for key in ua.unsized[:5]:
            print(f"    unsized row:    {key}")

    print()
    if untracked:
        dirty = True
        print(
            f"UNTRACKED: {len(untracked)} object(s), {mb(sum(untracked.values()))} MB, "
            "that no row points at — held by storage and charged to nobody"
        )
        for key in sorted(untracked)[:10]:
            print(f"    {key} ({mb(untracked[key])} MB)")
    else:
        print("UNTRACKED: none — every object in the bucket is pointed at by a row")

    await commitment(volume_bytes)

    print()
    print("RESULT: " + ("DRIFT FOUND" if dirty else "clean — rows and objects agree"))
    return 1 if dirty else 0


async def commitment(volume_bytes: int | None) -> None:
    """What the deployment has PROMISED, beside what it actually has.

    This is not enforcement and deliberately not a refusal. Every user's retained cap
    is a promise the platform made independently of the others, so their sum can
    exceed the volume without anyone having done anything wrong — the same way a bank
    is not in breach the moment deposits exceed the cash in the vault. It only matters
    when everyone draws at once, so an admin needs to be able to SEE the ratio and
    decide. Refusing on it would refuse work that fits.

    The volume figure has to be supplied (`--volume-bytes`, or MINIO_VOLUME_BYTES),
    because the control plane cannot see the storage server's disk from where it runs
    — it talks to MinIO over an S3 API, not a filesystem. A guess dressed as a reading
    is worse than an honest gap, so when it is absent the line says so."""
    async with SessionLocal() as session:
        users = (await session.execute(select(User))).scalars().all()
        tiers = {
            t.id: t for t in (await session.execute(select(Tier))).scalars().all()
        }

    total = 0
    per_tier: dict[str, int] = {}
    for user in users:
        tier = tiers.get(user.tier_id)
        if tier is None:
            continue
        total += tier.retained_cap_bytes
        per_tier[tier.id] = per_tier.get(tier.id, 0) + 1

    print()
    print("=== commitment ===")
    shape = ", ".join(f"{n} x {tid}" for tid, n in sorted(per_tier.items())) or "none"
    print(f"users by tier: {shape}")
    print(f"sum of all users' retained caps: {gb(total)} GB")
    if volume_bytes:
        ratio = total / volume_bytes if volume_bytes else 0
        print(f"storage volume:                  {gb(volume_bytes)} GB")
        print(f"committed / volume:              {ratio:.2f}x")
        # 80% is the headroom the 2026-09-05 sizing derivation assumed. Named as the
        # assumption it is, not printed as a verdict.
        print(
            "  (the 2026-09-05 tier sizing assumed the sum stays inside 80% of the "
            f"volume: {'inside' if ratio <= 0.8 else 'OVER'})"
        )
    else:
        print("storage volume:                  not supplied")
        print(
            "  pass --volume-bytes N (or set MINIO_VOLUME_BYTES) to see the ratio. "
            "The control plane reaches MinIO over an S3 API and cannot read the "
            "storage server's filesystem, so this figure is the admin's to give."
        )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--backfill",
        action="store_true",
        help=(
            "write the sizes of archived-log and sealed-input rows that predate "
            "2026-09-04, read from the object each row already points at"
        ),
    )
    parser.add_argument(
        "--volume-bytes",
        type=int,
        default=int(os.environ.get("MINIO_VOLUME_BYTES", "0")) or None,
        help=(
            "total bytes of the volume MinIO stores into, so the audit can print "
            "the sum of every user's retained cap against it. The control plane "
            "cannot read that filesystem itself, so the admin supplies it"
        ),
    )
    args = parser.parse_args()
    return asyncio.run(
        audit(backfill=args.backfill, volume_bytes=args.volume_bytes)
    )


if __name__ == "__main__":
    raise SystemExit(main())
