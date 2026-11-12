-- Pipeline event log analytics
-- ADR-010 D1
--
-- Lakeflow pipelines write an event log to a Delta table. The path is set
-- in the pipeline definition via the 'event_log' configuration block and
-- exposed here through a view for consistent querying.
--
-- The event log is the authoritative source for per-run row counts,
-- expectation outcomes, and stage durations. Job run status (success/fail)
-- is a coarser signal that cannot distinguish "ran and produced 0 rows"
-- from "ran and produced the expected 50k rows."
--
-- Databricks docs refer to this as the "pipeline event log"; it is a
-- Delta table, not a streaming endpoint.

-- ============================================================
-- Reference the event log
-- ============================================================
-- The event log path is pipeline-specific. Create a convenience view
-- in the ops schema so queries don't hard-code the path.

CREATE VIEW IF NOT EXISTS kings_prd.ops.v_pipeline_event_log AS
SELECT *
FROM delta.`/Volumes/kings_prd/ops/checkpoints/event_log/`;
-- In a bundle-parameterised pipeline, the path comes from kings.event_log_path.


-- ============================================================
-- 1. Run summary: latest run per pipeline
-- ============================================================
SELECT
    pipeline_id,
    MAX(timestamp)                           AS last_event_ts,
    COUNT_IF(event_type = 'FLOW_PROGRESS')   AS progress_events,
    COUNT_IF(level = 'ERROR')                AS error_count,
    COUNT_IF(level = 'WARN')                 AS warn_count
FROM kings_prd.ops.v_pipeline_event_log
WHERE timestamp >= CURRENT_TIMESTAMP - INTERVAL 24 HOURS
GROUP BY pipeline_id
ORDER BY last_event_ts DESC;


-- ============================================================
-- 2. Row counts per table per run
-- ============================================================
-- event_type = 'FLOW_PROGRESS' entries carry details.output_records.
-- Parsing the details column (JSON) exposes the per-table metrics.

SELECT
    pipeline_id,
    DATE_TRUNC('hour', timestamp)            AS run_hour,
    details:flow_progress:name               AS table_name,
    details:flow_progress:metrics:num_output_rows::BIGINT AS rows_written,
    details:flow_progress:metrics:num_input_rows::BIGINT  AS rows_read,
    details:flow_progress:status             AS flow_status
FROM kings_prd.ops.v_pipeline_event_log
WHERE event_type = 'FLOW_PROGRESS'
  AND details:flow_progress:status IN ('COMPLETED', 'FAILED')
ORDER BY run_hour DESC, table_name;


-- ============================================================
-- 3. Expectation violations: which @dp.expect fired?
-- ============================================================
-- Expectations that fire do NOT stop the pipeline (unless expect_or_fail).
-- They appear as WARN-level FLOW_PROGRESS events with violated_expectations > 0.

SELECT
    timestamp,
    details:flow_progress:name               AS table_name,
    details:flow_progress:data_quality:expectations AS expectations_json
FROM kings_prd.ops.v_pipeline_event_log
WHERE event_type = 'FLOW_PROGRESS'
  AND details:flow_progress:data_quality:expectations IS NOT NULL
  AND EXISTS (
    SELECT 1
    FROM JSON_TABLE(
      details:flow_progress:data_quality:expectations,
      '$[*]' COLUMNS (num_violations BIGINT PATH '$.num_violations')
    )
    WHERE num_violations > 0
  )
ORDER BY timestamp DESC;


-- ============================================================
-- 4. Stage duration: which table took longest?
-- ============================================================
SELECT
    pipeline_id,
    details:flow_progress:name               AS table_name,
    MIN(timestamp)                           AS stage_start,
    MAX(timestamp)                           AS stage_end,
    DATEDIFF(SECOND, MIN(timestamp), MAX(timestamp)) AS duration_seconds
FROM kings_prd.ops.v_pipeline_event_log
WHERE event_type = 'FLOW_PROGRESS'
  AND DATE(timestamp) = CURRENT_DATE
GROUP BY pipeline_id, table_name
ORDER BY duration_seconds DESC;


-- ============================================================
-- 5. Quarantine reconciliation: bronze count = silver + quarantine
-- ============================================================
-- This is the core invariant from the bronze pipeline design:
--   br_gate_scan rows = sv_admission rows + q_scan_invalid rows
-- A drift here means rows were lost between bronze and silver.

WITH row_counts AS (
  SELECT
    pipeline_id,
    details:flow_progress:name::STRING       AS table_name,
    SUM(details:flow_progress:metrics:num_output_rows::BIGINT) AS total_rows
  FROM kings_prd.ops.v_pipeline_event_log
  WHERE event_type = 'FLOW_PROGRESS'
    AND details:flow_progress:status = 'COMPLETED'
    AND DATE(timestamp) = CURRENT_DATE
  GROUP BY pipeline_id, table_name
)
SELECT
  br.total_rows                              AS bronze_rows,
  sv.total_rows                              AS silver_rows,
  q.total_rows                               AS quarantine_rows,
  (sv.total_rows + q.total_rows)             AS silver_plus_quarantine,
  br.total_rows - (sv.total_rows + q.total_rows) AS lost_rows  -- must be 0
FROM row_counts br
LEFT JOIN row_counts sv ON sv.table_name = 'sv_admission'
LEFT JOIN row_counts q  ON q.table_name  = 'q_scan_invalid'
WHERE br.table_name = 'br_gate_scan';
