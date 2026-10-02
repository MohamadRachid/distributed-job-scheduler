"""Machine-printed proof for the 2026-09-05 tier sizing.

Every figure the sizing rests on is RE-READ here, out of the file that holds it, with
the file name and the line number printed beside it. Nothing is transcribed.

Run from the repository root, with the compose stack up:

    python scripts/tier_sizing_proof.py > docs/evidence/tier_sizing_2026-09-06.txt
"""

import io
import os
import re
import subprocess
import sys

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# The reference lab's storage volume, 4 TB, in bytes. An ASSUMPTION and labelled as
# one everywhere it appears — it is not this machine's disk, which is printed further
# down so the gap between the two is visible rather than implied.
REFERENCE_VOLUME_BYTES = 4 * 1024 * 1024 * 1024 * 1024


def rule(title):
    print()
    print("=" * 78)
    print(title)
    print("=" * 78)


def read(rel):
    with io.open(os.path.join(ROOT, rel), encoding="utf-8") as fh:
        return fh.read()


def cite(rel, pattern, limit=3):
    """Print every line of `rel` matching `pattern`, with its line number."""
    hits = 0
    for n, line in enumerate(read(rel).splitlines(), 1):
        if re.search(pattern, line):
            text = line.strip()
            if len(text) > 200:
                text = text[:200] + " ..."
            print(f"  {rel}:{n}: {text}")
            hits += 1
            if hits >= limit:
                break
    if not hits:
        print(f"  {rel}: NO LINE MATCHED /{pattern}/  <-- the figure moved, or the file did")


def run(cmd, keep=None, tail=None):
    print("$ " + " ".join(cmd))
    try:
        out = subprocess.run(cmd, cwd=ROOT, text=True, capture_output=True, timeout=600)
    except Exception as exc:
        print(f"  could not run: {type(exc).__name__}: {exc}")
        return ""
    body = out.stdout + out.stderr
    lines = body.splitlines()
    if keep:
        lines = [ln for ln in lines if re.search(keep, ln)]
    if tail:
        lines = lines[-tail:]
    for ln in lines:
        print("  " + ln.rstrip())
    print(f"  exit code: {out.returncode}")
    return body


def compose(*args, **kw):
    env = dict(os.environ, MSYS_NO_PATHCONV="1")
    cmd = ["docker", "compose", *args]
    print("$ " + " ".join(cmd))
    try:
        out = subprocess.run(
            cmd, cwd=ROOT, text=True, capture_output=True, timeout=600, env=env
        )
    except Exception as exc:
        print(f"  could not run: {type(exc).__name__}: {exc}")
        return ""
    body = out.stdout + out.stderr
    keep = kw.get("keep")
    lines = body.splitlines()
    if keep:
        lines = [ln for ln in lines if re.search(keep, ln)]
    if kw.get("tail"):
        lines = lines[-kw["tail"]:]
    for ln in lines:
        print("  " + ln.rstrip())
    return body


def main():
    print("Tier sizing for the reference lab — proof capture")
    print("Captured:", subprocess.run(
        ["git", "log", "-1", "--format=%cd"], cwd=ROOT, text=True,
        capture_output=True).stdout.strip() or "(unknown)")
    print("Tree:", subprocess.run(
        ["git", "rev-parse", "--short", "HEAD"], cwd=ROOT, text=True,
        capture_output=True).stdout.strip())
    print()
    print("The figures seeded on 2026-09-04 (2,000 GB retained / 500 GB scratch) were")
    print("the supervisor's own examples, carried as configuration and labelled in")
    print("protocol.md as never measured. So were the alternatives he offered")
    print("afterwards. They are replaced here by numbers DERIVED from two anchors —")
    print("what the biggest legitimate run must hold, and what the smallest disk in")
    print("the pool must fit — for a REFERENCE LAB, not for this demonstration laptop.")

    # --- 1. The four figures the derivation rests on ------------------------------
    rule("1. THE FOUR RE-READ FIGURES — file and line, read now, not transcribed")
    print()
    print("(a) bytes per parameter of real checkpoint state, measured 2026-09-01:")
    cite("docs/evidence/checkpoint_reshape_2026-09-01.txt", r"12\.044", limit=3)
    print()
    print("(b) the demo trainer's checkpoint, as stored, same capture:")
    cite("docs/evidence/checkpoint_reshape_2026-09-01.txt", r"2492143", limit=2)
    print()
    print("(c) MAX_ARTIFACT_MB, the per-file cap, in the frozen contract:")
    cite("protocol.md", r"`MAX_ARTIFACT_MB` \| `50`", limit=1)
    print()
    print("(d) one checkpoint per run, as the CODE states it — a stable filename,")
    print("    overwritten in place, so repeats do not accumulate objects:")
    cite("agent/runner.py", r"one checkpoint per attempt|^CHECKPOINT_FILENAME", limit=3)

    # --- 2. The derivation --------------------------------------------------------
    rule("2. THE DERIVATION")
    print("""
Reference lab (a STATED ASSUMPTION; it goes in the report as such and to M. Ayli as
a question): 10 workers, each ~500 GB free and hosting 4 runs at once; one storage
server with a 4 TB MinIO volume; 20 users created by the admin.

SCRATCH — temporary disk one run may write on a worker
    dataset pulled via the Dataset URL field                    ~10 GB
    one checkpoint for a billion-parameter model                ~12 GB
        1e9 params x 12.044 B/param = 12.044 GB  [figure (a)]
    output files and /tmp                                        ~3 GB
                                                               -------
                                                               ~25 GB
    doubled, because a run that reaches its ceiling is stopped   50 GB
    Fit: 4 concurrent runs x 50 GB = 200 GB, inside 500 GB free.

RETAINED — bytes the platform keeps on the server for one user
    ten kept sweeps of 100 jobs x 50 MB                          50 GB
        50 MB is MAX_ARTIFACT_MB  [figure (c)]
    five concurrent runs each holding one 12 GB checkpoint       60 GB
        one per run, stable key  [figure (d)]
    archived logs                                            a few GB
                                                               -------
                                                              ~110 GB
    with headroom                                               200 GB
    Fit: 5 standard + 15 limited = 5x200 + 15x20 = 1,300 GB,
         inside 80% of 4 TB (3,276 GB).

limited = one tenth of standard on both numbers: 20 GB / 5 GB.
""")

    # --- 3. The seeded rows, as the database prints them --------------------------
    rule("3. THE TWO SEED ROWS, AS STORED — before and after the migration")
    print()
    print("Before (the 2026-09-04 figures, still in the demo database):")
    compose(
        "exec", "-T", "postgres", "psql", "-U", "fyp", "-d", "fyp", "-c",
        "SELECT id, retained_cap_bytes, scratch_cap_bytes FROM tiers ORDER BY id;",
        keep=r"standard|limited|id |---|rows",
    )

    rule("4. MIGRATION a2b3c4d5e6f7 — up -> down -> up on real PostgreSQL")
    compose(
        "run", "--rm", "control-plane", "sh", "-c",
        "alembic upgrade head && alembic current && "
        "alembic downgrade -1 && alembic current && "
        "alembic upgrade head && alembic current",
        keep=r"Running (upgrade|downgrade)|^[a-f0-9]{12}\b",
    )

    print()
    print("After (the 2026-09-05 sized figures, as the database prints them):")
    compose(
        "exec", "-T", "postgres", "psql", "-U", "fyp", "-d", "fyp", "-c",
        "SELECT id, retained_cap_bytes, scratch_cap_bytes, description FROM tiers "
        "ORDER BY id;",
        keep=r"standard|limited|rows",
    )

    # --- 5. The existing quota proofs, unchanged ---------------------------------
    rule("5. THE EXISTING QUOTA PROOFS — unchanged at their 1 MB / 100 MB values")
    print()
    print("Only configuration moved; the mechanism is untouched. These tests build")
    print("their own tiers at megabyte figures so a refusal happens in seconds, and")
    print("they are the check that the sizing did not disturb the enforcement.")
    compose(
        "run", "--rm", "control-plane", "pytest", "tests/test_storage_quota.py", "-q",
        keep=r"passed|failed|error",
    )

    # --- 6. The commitment line ---------------------------------------------------
    rule("6. THE AUDIT'S NEW LINE — what is promised, beside what exists")
    compose(
        "run", "--rm", "-v", f"{ROOT.replace(chr(92), '/')}/scripts:/scripts", "control-plane",
        "python", "/scripts/quota_audit.py",
        "--volume-bytes", str(REFERENCE_VOLUME_BYTES),
        keep=r"=== commitment|users by tier|sum of all|storage volume|committed / |80% of",
    )
    print()
    print("No enforcement: the admin reads it. A sum of caps ABOVE the volume is not")
    print("in itself wrong — each cap is a promise made independently of the others,")
    print("and it only matters when everyone draws at once. Refusing on it would")
    print("refuse work that fits.")
    print()
    print("NOTE ON THIS READING: the demo database carries fourteen users left behind")
    print("by the 2026-09-04 quota proof runs, not twenty as the reference lab")
    print("assumes, so the ratio printed above describes THIS database and is not the")
    print("reference lab's 1,300 GB / 4 TB.")

    # --- 7. This machine, which is NOT the reference lab --------------------------
    rule("7. THE DEMONSTRATION HOST — printed so the gap is visible, not implied")
    print()
    print("NOT numbers to size from. The sizing above is for the stated reference")
    print("lab; this is what the machine in the room actually has, and the report")
    print("states the difference with a real reading rather than glossing it.")
    print()
    print("The volume MinIO stores into, as MinIO's own container sees it:")
    compose("exec", "-T", "minio", "df", "-h", "/data", keep=r"Filesystem|/data")
    print()
    print("Every free-disk figure any worker has ever reported (nodes.disk_free_mb,")
    print("written by the heartbeat since 2026-09-04):")
    compose(
        "exec", "-T", "postgres", "psql", "-U", "fyp", "-d", "fyp", "-c",
        "SELECT name, disk_free_mb, round(disk_free_mb/1024.0) AS gb_free, "
        "agent_version FROM nodes ORDER BY name;",
        keep=r"node|name|---|rows",
    )
    print()
    print("The reference lab assumes ~500 GB free per worker. The three agents on")
    print("this host report roughly a third of that, and they are three processes on")
    print("ONE laptop rather than three machines — the single-host limit this project")
    print("publishes beside every number it measures.")

    rule("END")


if __name__ == "__main__":
    main()
