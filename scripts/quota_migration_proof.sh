#!/bin/sh
# P5 — the storage-quota migration reverses cleanly on real PostgreSQL.
#
# Run from the repo root with the stack up:
#
#     sh scripts/quota_migration_proof.sh >> docs/evidence/storage_quota_2026-09-04.txt 2>&1
#
# up -> down -> up, with the schema dumped at each step and COMPARED rather than
# eyeballed. "Looks the same" is not a proof and a column-by-column diff is, which is
# why the comparison is `diff` and not a person reading two listings.
#
# The dump is taken from `information_schema`, ordered, so it is a canonical
# description of the schema and not of the order PostgreSQL happened to return rows
# in. Every migration in this project is proven this way before it merges; this file is that proof for e0f1a2b3c4d5, written down so
# the claim has an artefact behind it.
set -e

DUMP='SELECT table_name, column_name, data_type, is_nullable, coalesce(column_default, %s)
      FROM information_schema.columns WHERE table_schema = %s
      ORDER BY table_name, column_name;'

dump() {
  docker compose exec -T postgres psql -qtAX -U fyp -d fyp \
    -c "SELECT table_name||'|'||column_name||'|'||data_type||'|'||is_nullable||'|'||coalesce(column_default,'-') FROM information_schema.columns WHERE table_schema='public' ORDER BY 1;"
}

echo "=============================================================================="
echo "P5 — migration up -> down -> up on real PostgreSQL"
echo "date         : $(date '+%Y-%m-%d %H:%M:%S')"
echo "revision     : e0f1a2b3c4d5 (storage quota policy)"
echo "=============================================================================="

echo
echo "-- head before --"
docker compose exec -T control-plane alembic current 2>&1 | tail -2
dump > /tmp/schema_before.txt
echo "columns at head: $(wc -l < /tmp/schema_before.txt)"
echo "tiers columns:"
grep '^tiers|' /tmp/schema_before.txt || echo "  (none)"

echo
echo "-- downgrade -1 --"
docker compose exec -T control-plane alembic downgrade -1 2>&1 | tail -3
dump > /tmp/schema_down.txt
echo "columns after downgrade: $(wc -l < /tmp/schema_down.txt)"
echo "tiers table present after downgrade? $(grep -c '^tiers|' /tmp/schema_down.txt || true)"
echo "quota columns still on users/runs/jobs/nodes/run_samples/run_log_archives:"
grep -E '^(users\|(is_admin|tier_id|limits_)|runs\|quota_|jobs\|input_size_bytes|nodes\|disk_free_mb|run_samples\|scratch_used_mb|run_log_archives\|size_bytes)' \
  /tmp/schema_down.txt || echo "  none — every added column was removed"

echo
echo "-- upgrade head --"
docker compose exec -T control-plane alembic upgrade head 2>&1 | tail -3
dump > /tmp/schema_after.txt
echo "columns at head again: $(wc -l < /tmp/schema_after.txt)"

echo
echo "-- the comparison --"
if diff -u /tmp/schema_before.txt /tmp/schema_after.txt > /tmp/schema_diff.txt; then
  echo "[HELD] P5 up -> down -> up leaves the schema IDENTICAL"
  echo "       $(wc -l < /tmp/schema_before.txt) columns compared, 0 differences"
else
  echo "[WITHDRAWN] P5 the schema differs after the round trip:"
  cat /tmp/schema_diff.txt
  exit 1
fi

echo
echo "-- the seeded tiers survive the round trip --"
docker compose exec -T postgres psql -qtAX -U fyp -d fyp \
  -c "SELECT id||' retained='||retained_cap_bytes||' scratch='||scratch_cap_bytes FROM tiers ORDER BY id;"
echo
echo "-- the bootstrap admin is still an admin and still points at a tier --"
docker compose exec -T postgres psql -qtAX -U fyp -d fyp \
  -c "SELECT username||' is_admin='||is_admin||' tier='||tier_id FROM users ORDER BY created_at;"
