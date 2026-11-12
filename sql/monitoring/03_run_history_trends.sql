-- Run-history trends: DBU consumption and row throughput
-- ADR-010 D3
--
-- Wall-clock seconds are dominated by queue wait and cold-start on serverless;
-- they do not reliably detect genuine regressions (ADR-002).
-- The stable signals are:
--   - DBU consumption from system.billing.usage
--   - Row throughput (rows per micro-batch) from the pipeline event log
-- A pipeline that runs in the same wall time but burns 2x DBUs has regressed.


-- ============================================================
-- 1. Daily DBU consumption per pipeline job (last 30 days)
-- ============================================================
-- system.billing.usage records one row per billing period per SKU.
-- usage_quantity is in DBUs. sku_name contains 'JOBS_COMPUTE' or
-- 'SERVERLESS_COMPUTE' depending on cluster type.

SELECT
  DATE(u.usage_start_time)                       AS usage_date,
  u.custom_tags['job_name']                       AS job_name,
  u.sku_name,
  SUM(u.usage_quantity)                           AS total_dbus,
  COUNT(DISTINCT u.usage_record_id)               AS billing_records
FROM system.billing.usage u
WHERE u.usage_start_time >= CURRENT_TIMESTAMP - INTERVAL 30 DAYS
  AND u.billing_origin_product IN ('JOBS', 'DLT')
GROUP BY usage_date, job_name, u.sku_name
ORDER BY usage_date DESC, total_dbus DESC;


-- ============================================================
-- 2. Week-over-week DBU trend: is the pipeline getting more expensive?
-- ============================================================
-- A 10%+ week-over-week increase warrants investigation.
-- Likely causes: data volume growth, partition explosion, skew.

WITH daily AS (
  SELECT
    DATE(usage_start_time)                        AS usage_date,
    custom_tags['job_name']                       AS job_name,
    SUM(usage_quantity)                           AS daily_dbus
  FROM system.billing.usage
  WHERE usage_start_time >= CURRENT_TIMESTAMP - INTERVAL 14 DAYS
    AND billing_origin_product IN ('JOBS', 'DLT')
  GROUP BY usage_date, job_name
),
weekly AS (
  SELECT
    job_name,
    SUM(CASE WHEN usage_date >= CURRENT_DATE - 7  THEN daily_dbus ELSE 0 END) AS this_week_dbus,
    SUM(CASE WHEN usage_date <  CURRENT_DATE - 7  THEN daily_dbus ELSE 0 END) AS last_week_dbus
  FROM daily
  GROUP BY job_name
)
SELECT
  job_name,
  ROUND(this_week_dbus, 2)                        AS this_week_dbus,
  ROUND(last_week_dbus, 2)                        AS last_week_dbus,
  ROUND(this_week_dbus - last_week_dbus, 2)       AS delta_dbus,
  CASE
    WHEN last_week_dbus = 0 THEN NULL
    ELSE ROUND(100.0 * (this_week_dbus - last_week_dbus) / last_week_dbus, 1)
  END                                             AS pct_change,
  CASE
    WHEN last_week_dbus = 0 THEN 'BASELINE'
    WHEN this_week_dbus > last_week_dbus * 1.10 THEN 'REGRESSED'
    WHEN this_week_dbus < last_week_dbus * 0.90 THEN 'IMPROVED'
    ELSE 'STABLE'
  END                                             AS trend_status
FROM weekly
ORDER BY pct_change DESC NULLS LAST;


-- ============================================================
-- 3. Row throughput trend from the pipeline event log
-- ============================================================
-- Rows written per pipeline run. A drop in throughput means either
-- the upstream source delivered fewer rows or a stage regressed.
-- Compare against baseline (rolling 7-day average).

WITH daily_rows AS (
  SELECT
    DATE(timestamp)                               AS run_date,
    details:flow_progress:name::STRING            AS table_name,
    SUM(details:flow_progress:metrics:num_output_rows::BIGINT) AS rows_written
  FROM kings_prd.ops.v_pipeline_event_log
  WHERE event_type = 'FLOW_PROGRESS'
    AND details:flow_progress:status = 'COMPLETED'
    AND timestamp >= CURRENT_TIMESTAMP - INTERVAL 30 DAYS
  GROUP BY run_date, table_name
)
SELECT
  run_date,
  table_name,
  rows_written,
  AVG(rows_written) OVER (
    PARTITION BY table_name
    ORDER BY run_date
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  )                                               AS rolling_7d_avg,
  rows_written - AVG(rows_written) OVER (
    PARTITION BY table_name
    ORDER BY run_date
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  )                                               AS delta_from_avg
FROM daily_rows
ORDER BY run_date DESC, table_name;


-- ============================================================
-- 4. Cost per 1 000 rows processed
-- ============================================================
-- Normalises DBU cost by throughput so that volume growth doesn't
-- mask efficiency regressions. If this ratio climbs, the pipeline
-- is doing more work per row (skew, extra shuffles, spill).
-- Requires joining billing data to event log by date and job tag.

WITH pipeline_rows AS (
  SELECT
    DATE(timestamp)                               AS run_date,
    SUM(details:flow_progress:metrics:num_output_rows::BIGINT) AS total_rows
  FROM kings_prd.ops.v_pipeline_event_log
  WHERE event_type = 'FLOW_PROGRESS'
    AND details:flow_progress:status = 'COMPLETED'
  GROUP BY run_date
),
pipeline_dbus AS (
  SELECT
    DATE(usage_start_time)                        AS run_date,
    SUM(usage_quantity)                           AS total_dbus
  FROM system.billing.usage
  WHERE billing_origin_product IN ('JOBS', 'DLT')
    AND custom_tags['job_name'] LIKE '%kings%'
  GROUP BY run_date
)
SELECT
  r.run_date,
  r.total_rows,
  d.total_dbus,
  ROUND(d.total_dbus * 1000.0 / NULLIF(r.total_rows, 0), 4)  AS dbus_per_1k_rows
FROM pipeline_rows r
JOIN pipeline_dbus d ON d.run_date = r.run_date
ORDER BY r.run_date DESC;


-- ============================================================
-- 5. Stage-level elapsed time trend (use with caution — ADR-002)
-- ============================================================
-- Wall time is noisy but a 5x jump in a single stage's elapsed time
-- is worth flagging even on serverless. Use as a triage signal, not
-- an SLO. Spikes here correlate with spill events (see query 04).

SELECT
  DATE(MIN(timestamp))                            AS run_date,
  details:flow_progress:name::STRING              AS table_name,
  AVG(DATEDIFF(SECOND, MIN(timestamp), MAX(timestamp))) AS avg_stage_seconds,
  MAX(DATEDIFF(SECOND, MIN(timestamp), MAX(timestamp))) AS max_stage_seconds
FROM kings_prd.ops.v_pipeline_event_log
WHERE event_type = 'FLOW_PROGRESS'
  AND timestamp >= CURRENT_TIMESTAMP - INTERVAL 14 DAYS
GROUP BY DATE(timestamp), table_name
ORDER BY run_date DESC, max_stage_seconds DESC;
