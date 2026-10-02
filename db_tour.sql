\echo
\echo '###############  DISTRIBUTED TRAINING PLATFORM - DATABASE TOUR  ###############'
\echo
\echo '=====================  1) WORKER POOL  (nodes)  ====================='
\echo '(no "online" column - liveness is derived from last_heartbeat at read time)'
SELECT name, status, cpu_cores AS threads, has_gpu, ram_mb, capacity, agent_version
FROM nodes ORDER BY name;
\echo
\echo '=====================  2) JOBS SUBMITTED  (jobs)  ====================='
SELECT name, image, status, replicas FROM jobs ORDER BY created_at;
\echo
\echo '=====================  3) RUNS + FENCING TOKEN  (runs)  ====================='
\echo '(attempt = the fencing token; a stale attempt is rejected = at-most-once accepted)'
SELECT n.name AS node, r.status, r.attempt AS fencing_token, r.exit_code
FROM runs r LEFT JOIN nodes n ON n.id = r.node_id
ORDER BY r.created_at;
\echo
\echo '=====================  4) STORED LOGS  (run_logs)  ====================='
\echo '(UNIQUE(run_id, attempt, seq) = the DB itself prevents duplicate log lines)'
SELECT attempt, seq, left(replace(chunk, chr(10), ' '), 48) AS log_line
FROM run_logs ORDER BY run_id, attempt, seq LIMIT 8;
\echo
\echo '=====================  5) SCHEMA VERSION  (alembic_version)  ====================='
\echo '(proof the schema is versioned + change-controlled, not hand-edited)'
SELECT version_num AS schema_version FROM alembic_version;
\echo
\echo '(users and artifacts tables are empty on purpose - login and artifact'
\echo ' storage are later weeks, W6 - not built yet)'
\echo
