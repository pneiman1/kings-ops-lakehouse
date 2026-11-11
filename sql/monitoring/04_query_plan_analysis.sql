-- Query plan analysis: skew, shuffle, spill
-- ADR-010 D5
--
-- Spark UI signals live in system.query.history.metrics_json.
-- The same data appears in the Spark UI task list and DAG view.
-- These queries surface it programmatically for trending.
--
-- Key signals (ADR-002):
--   bytes_scanned      — did file pruning / liquid clustering work?
--   shuffle_bytes      — high value = expensive sort-merge or large join
--   spill_bytes        — sort or hash join exceeded memory; fell back to disk
--   task_duration_p99  — skew indicator: p99 >> median means one task is hot
--
-- How to read the Spark UI:
--   Stage list → sort by "Duration" to find the bottleneck stage.
--   Task list  → sort by "Duration"; if one row is 10x the median, that is skew.
--   DAG view   → wide stages (many input partitions) with a narrow output are
--                the shuffle boundary. High shuffle read = skew or bad join type.
--   Summary metrics bar → "Spill (memory)" and "Spill (disk)" appear here.


-- ============================================================
-- 1. Recent queries: bytes scanned and shuffle bytes
-- ============================================================
-- metrics_json is a JSON string with Spark SQL plan-level aggregates.
-- Fields vary by Databricks runtime; extract defensively with ::.

SELECT
  q.statement_id,
  DATE(q.start_time)                                                AS run_date,
  q.duration / 1000                                                 AS elapsed_seconds,
  q.metrics_json:totalBytesRead::BIGINT / 1e9                       AS gb_scanned,
  q.metrics_json:shuffleWriteBytes::BIGINT / 1e9                    AS gb_shuffled,
  q.metrics_json:peakMemoryBytes::BIGINT / 1e9                      AS gb_peak_mem,
  q.metrics_json:spilledBytes::BIGINT / 1e9                         AS gb_spilled,
  q.statement_text
FROM system.query.history q
WHERE q.start_time >= CURRENT_TIMESTAMP - INTERVAL 7 DAYS
  AND q.status = 'FINISHED'
  AND q.statement_text NOT LIKE '%system.query.history%'  -- exclude self-referential
ORDER BY q.start_time DESC
LIMIT 100;


-- ============================================================
-- 2. Spill events: queries that exceeded memory
-- ============================================================
-- Spill means a sort or hash-join exceeded the executor's memory budget
-- and serialised intermediate data to local disk. It is not fatal but
-- can make a stage 10–100x slower.
--
-- Fix options:
--   a) Increase AQE shuffle partitions (spark.sql.shuffle.partitions)
--      so each partition is smaller and fits in memory.
--   b) Switch from sort-merge join to broadcast join if the smaller
--      side is < spark.sql.autoBroadcastJoinThreshold (default 10 MB).
--   c) Repartition before the sort: df.repartition(n, "join_key")

SELECT
  statement_id,
  DATE(start_time)                                                  AS run_date,
  duration / 1000                                                   AS elapsed_seconds,
  metrics_json:spilledBytes::BIGINT / 1e6                           AS mb_spilled,
  metrics_json:shuffleWriteBytes::BIGINT / 1e6                      AS mb_shuffled,
  LEFT(statement_text, 200)                                         AS query_preview
FROM system.query.history
WHERE start_time >= CURRENT_TIMESTAMP - INTERVAL 14 DAYS
  AND metrics_json:spilledBytes::BIGINT > 0
ORDER BY mb_spilled DESC
LIMIT 50;


-- ============================================================
-- 3. Skew detection: task duration spread within a stage
-- ============================================================
-- system.query.history does not expose per-task metrics directly.
-- The closest proxy is comparing the query's total_task_duration_ms
-- (sum of all task ms) to elapsed_ms * num_tasks.
-- A ratio >> 1 means tasks were serialised (data skew or insufficient
-- parallelism). True per-task p99 is only in the Spark UI task list.

SELECT
  statement_id,
  DATE(start_time)                                                  AS run_date,
  duration / 1000                                                   AS elapsed_seconds,
  metrics_json:totalTaskDurationMs::BIGINT / 1000                   AS total_task_seconds,
  -- parallelism efficiency: ideal is total_task / (elapsed * num_executors)
  -- >1 is normal (parallel); a low value means tasks ran sequentially (skew)
  ROUND(
    CAST(metrics_json:totalTaskDurationMs AS DOUBLE)
    / NULLIF(duration, 0),
    2
  )                                                                 AS parallelism_ratio,
  LEFT(statement_text, 200)                                         AS query_preview
FROM system.query.history
WHERE start_time >= CURRENT_TIMESTAMP - INTERVAL 7 DAYS
  AND status = 'FINISHED'
  AND metrics_json:totalTaskDurationMs::BIGINT IS NOT NULL
ORDER BY parallelism_ratio ASC  -- low ratio = serialised = skew
LIMIT 50;


-- ============================================================
-- 4. File pruning effectiveness: did liquid clustering help?
-- ============================================================
-- totalFilesSkipped / (totalFilesSkipped + totalFilesRead) is the
-- pruning ratio. For fact_admission with liquid clustering on
-- (game_id, gate), a game-scoped query should skip > 90% of files.

SELECT
  statement_id,
  DATE(start_time)                                                  AS run_date,
  metrics_json:numFilesAffected::BIGINT                             AS files_read,
  metrics_json:numFilesSkipped::BIGINT                              AS files_skipped,
  ROUND(
    100.0 * metrics_json:numFilesSkipped::BIGINT
    / NULLIF(
        metrics_json:numFilesAffected::BIGINT + metrics_json:numFilesSkipped::BIGINT,
        0
      ),
    1
  )                                                                 AS pct_files_skipped,
  LEFT(statement_text, 200)                                         AS query_preview
FROM system.query.history
WHERE start_time >= CURRENT_TIMESTAMP - INTERVAL 7 DAYS
  AND status = 'FINISHED'
  AND statement_text ILIKE '%fact_admission%'
ORDER BY pct_files_skipped ASC  -- low skip % = clustering not helping
LIMIT 50;


-- ============================================================
-- 5. Broadcast vs sort-merge join decision log
-- ============================================================
-- AQE can switch a planned sort-merge join to a broadcast join at
-- runtime if one side proves small enough. This query shows which
-- joins were broadcast (cheap) vs sort-merge (expensive).
-- Look for statement_text patterns that touch large fact tables
-- without a broadcast hint.

SELECT
  statement_id,
  DATE(start_time)                                                  AS run_date,
  CASE
    WHEN statement_text ILIKE '%broadcast%' THEN 'EXPLICIT_BROADCAST_HINT'
    WHEN metrics_json:shuffleWriteBytes::BIGINT = 0 THEN 'BROADCAST_NO_SHUFFLE'
    WHEN metrics_json:shuffleWriteBytes::BIGINT > 1e9 THEN 'SORT_MERGE_LARGE'
    ELSE 'SORT_MERGE_SMALL'
  END                                                               AS join_type_inferred,
  metrics_json:shuffleWriteBytes::BIGINT / 1e6                      AS mb_shuffled,
  LEFT(statement_text, 200)                                         AS query_preview
FROM system.query.history
WHERE start_time >= CURRENT_TIMESTAMP - INTERVAL 7 DAYS
  AND status = 'FINISHED'
  AND (statement_text ILIKE '%fact_admission%'
       OR statement_text ILIKE '%sv_admission%')
ORDER BY mb_shuffled DESC NULLS LAST
LIMIT 50;
