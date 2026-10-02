-- ###########################################################################
--  DISTRIBUTED TRAINING PLATFORM - DATABASE TOUR  (pgAdmin Query Tool version)
--
--  Open this in pgAdmin:  FYP Postgres > right-click > Query Tool > open this file.
--  Highlight one block and press F5 (or the ▶ button) to run just that block.
--  Run it AFTER you submit a job, so the tables have data.
--
--  Say first:  "the dashboard IS the database"  -  pool = nodes, runs = runs,
--  live logs = run_logs.  This tour just shows the same data in the tables.
-- ###########################################################################


-- ===================  1) WORKER POOL  (nodes)  ===================
-- No "online" column - liveness is derived from last_heartbeat at read time.
SELECT name, status, cpu_cores AS threads, has_gpu, ram_mb, capacity, agent_version
FROM nodes
ORDER BY name;


-- ===================  2) JOBS SUBMITTED  (jobs)  ===================
SELECT name, image, status, replicas
FROM jobs
ORDER BY created_at;


-- ===================  3) RUNS + FENCING TOKEN  (runs)  ===================
-- attempt = the fencing token; a stale attempt is rejected = at-most-once accepted.
SELECT n.name AS node, r.status, r.attempt AS fencing_token,
       r.exit_code, r.failure_reason
FROM runs r
LEFT JOIN nodes n ON n.id = r.node_id
ORDER BY r.created_at;


-- ===================  4) STORED LOGS  (run_logs)  ===================
-- UNIQUE(run_id, attempt, seq) = the DB itself prevents duplicate log lines.
SELECT attempt, seq, left(replace(chunk, chr(10), ' '), 60) AS log_line
FROM run_logs
ORDER BY run_id, attempt, seq
LIMIT 12;


-- ===================  5) PER-RUN RESOURCE SAMPLES  (run_samples, W5b)  ===================
-- The container's own CPU% and RAM-vs-limit, so a dead run shows its last picture.
SELECT run_id, attempt, cpu_pct, mem_used_mb, mem_limit_mb, ts
FROM run_samples
ORDER BY ts DESC
LIMIT 12;


-- ===================  6) NODE EVENTS  (node_events, W5b)  ===================
-- The node postmortem: how a silent machine explained itself when it came back.
SELECT n.name AS node, e.event, e.cause, e.ts
FROM node_events e
LEFT JOIN nodes n ON n.id = e.node_id
ORDER BY e.ts DESC
LIMIT 12;


-- ===================  7) SCHEMA VERSION  (alembic_version)  ===================
-- Proof the schema is versioned + change-controlled, not hand-edited.
SELECT version_num AS schema_version FROM alembic_version;
