-- Freshness SLO checks
-- ADR-010 D2
--
-- SLO threshold: 120 minutes (freshness_sla_minutes in conf/base.yml).
-- Reads last_modified from system.information_schema.tables — no pipeline
-- instrumentation required beyond the pipeline actually writing the table.
--
-- To wire to a Databricks SQL Alert:
--   1. Save this query in Databricks SQL.
--   2. Create an Alert on the query: condition = freshness_status = 'SLO_BREACHED'.
--   3. Set notification destination (email / Slack webhook).
-- That is a one-line UI config change; the query is the check.

-- ============================================================
-- 1. Current freshness status for all silver and gold tables
-- ============================================================
WITH slo AS (
  SELECT 120 AS threshold_minutes  -- matches conf/base.yml freshness_sla_minutes
),
table_freshness AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    last_altered                                              AS last_write_ts,
    DATEDIFF(MINUTE, last_altered, CURRENT_TIMESTAMP)        AS minutes_stale
  FROM system.information_schema.tables
  WHERE table_catalog IN ('kings_prd', 'kings_stg')
    AND table_schema  IN ('silver', 'gold')
    AND table_type    = 'BASE TABLE'
)
SELECT
  f.table_catalog,
  f.table_schema,
  f.table_name,
  f.last_write_ts,
  f.minutes_stale,
  s.threshold_minutes,
  CASE
    WHEN f.minutes_stale > s.threshold_minutes THEN 'SLO_BREACHED'
    WHEN f.minutes_stale > s.threshold_minutes * 0.75 THEN 'SLO_WARNING'
    ELSE 'OK'
  END                                                        AS freshness_status
FROM table_freshness f
CROSS JOIN slo s
ORDER BY f.minutes_stale DESC;


-- ============================================================
-- 2. Tables breaching SLO right now (alert query)
-- ============================================================
-- Use this as the Databricks SQL Alert condition.
-- Alert fires when row count > 0.

SELECT
  table_catalog || '.' || table_schema || '.' || table_name  AS full_table_name,
  last_altered                                                AS last_write_ts,
  DATEDIFF(MINUTE, last_altered, CURRENT_TIMESTAMP)          AS minutes_stale,
  'SLO_BREACHED'                                             AS status
FROM system.information_schema.tables
WHERE table_catalog IN ('kings_prd')
  AND table_schema  IN ('silver', 'gold')
  AND table_type    = 'BASE TABLE'
  AND DATEDIFF(MINUTE, last_altered, CURRENT_TIMESTAMP) > 120
ORDER BY minutes_stale DESC;


-- ============================================================
-- 3. SLO history: has a table been stale before?
-- ============================================================
-- Uses system.query.history to find past SLO-check runs and their results.
-- This requires the system catalog to be enabled.

SELECT
  DATE(start_time)                                                AS check_date,
  COUNT_IF(result_row_count > 0)                                  AS breach_count,
  COUNT(*)                                                        AS total_checks
FROM system.query.history
WHERE statement_text ILIKE '%SLO_BREACHED%'
  AND start_time >= CURRENT_TIMESTAMP - INTERVAL 30 DAYS
GROUP BY check_date
ORDER BY check_date DESC;


-- ============================================================
-- 4. Expected vs actual row-count SLO
-- ============================================================
-- Beyond freshness (time since write), validate row counts are non-trivial.
-- conf/base.yml: min_expected_row_count = 1.
-- A table with 0 rows written in the last run is as bad as a stale table.

SELECT
  t.table_catalog,
  t.table_schema,
  t.table_name,
  p.`row_count`                                                   AS current_row_count,
  CASE
    WHEN p.`row_count` = 0 THEN 'EMPTY_TABLE'
    WHEN p.`row_count` IS NULL THEN 'STATS_MISSING'
    ELSE 'OK'
  END                                                             AS row_count_status
FROM system.information_schema.tables t
JOIN system.information_schema.table_constraints p
  ON  p.table_catalog = t.table_catalog
  AND p.table_schema  = t.table_schema
  AND p.table_name    = t.table_name
WHERE t.table_catalog = 'kings_prd'
  AND t.table_schema  IN ('silver', 'gold');


-- ============================================================
-- 5. Streaming pipeline lag (sv_admission event-time vs wall clock)
-- ============================================================
-- For the 48-hour-watermarked streaming table, the max scan_ts in
-- sv_admission should lag wall clock by at most 48h + pipeline trigger interval.
-- A larger lag means the pipeline is not running.

SELECT
  MAX(scan_ts)                                                AS latest_event_time,
  CURRENT_TIMESTAMP                                           AS wall_clock,
  DATEDIFF(HOUR, MAX(scan_ts), CURRENT_TIMESTAMP)            AS lag_hours,
  CASE
    WHEN DATEDIFF(HOUR, MAX(scan_ts), CURRENT_TIMESTAMP) > 50 THEN 'STREAMING_LAG_BREACH'
    ELSE 'OK'
  END                                                         AS lag_status
FROM kings_prd.silver.sv_admission;
