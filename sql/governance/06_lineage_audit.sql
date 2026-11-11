-- Lineage and audit via Unity Catalog system tables
-- ADR-009
--
-- System tables are populated automatically by UC. They require the
-- system catalog to be enabled for the account (one-time admin setup).
-- All queries here run in SQL warehouse; no cluster needed.
--
-- Key system schemas:
--   system.access        audit log, grants, table lineage, column lineage
--   system.query         query history (statement text, runtime, user)
--   system.compute       cluster and warehouse usage
--   system.billing       DBU consumption by SKU
--
-- Exam objective: demonstrate lineage tracking, audit log queries, and
-- system table usage for cost attribution. All four are here.


-- ============================================================
-- 1. Table lineage — which pipelines wrote to gold?
-- ============================================================
-- system.access.table_lineage tracks source → target relationships for
-- Delta tables written by Databricks jobs and pipelines.

SELECT
    source_table_full_name,
    target_table_full_name,
    created_by,
    entity_type,         -- PIPELINE, JOB, NOTEBOOK
    entity_run_id,
    event_time
FROM system.access.table_lineage
WHERE target_table_full_name LIKE 'kings_prd.gold.%'
ORDER BY event_time DESC
LIMIT 50;

-- Which tables feed sv_admission?
SELECT source_table_full_name, event_time
FROM system.access.table_lineage
WHERE target_table_full_name = 'kings_prd.silver.sv_admission'
ORDER BY event_time DESC;


-- ============================================================
-- 2. Column lineage — where does collected_revenue_usd come from?
-- ============================================================
SELECT
    source_table_full_name,
    source_column_name,
    target_table_full_name,
    target_column_name,
    event_time
FROM system.access.column_lineage
WHERE target_table_full_name = 'kings_prd.gold.fact_ticket_order'
  AND target_column_name     = 'collected_revenue_usd'
ORDER BY event_time DESC;


-- ============================================================
-- 3. Audit log — who accessed dim_account recently?
-- ============================================================
-- system.access.audit records every UC operation with the principal,
-- action, resource, and outcome.

SELECT
    event_time,
    user_identity.email          AS user_email,
    service_name,
    action_name,
    request_params,
    response.status_code         AS http_status,
    source_ip_address
FROM system.access.audit
WHERE service_name  = 'unityCatalog'
  AND action_name   IN ('getTable', 'generateTemporaryTableCredential')
  AND request_params LIKE '%dim_account%'
  AND event_time >= CURRENT_TIMESTAMP - INTERVAL 7 DAYS
ORDER BY event_time DESC;

-- Failed access attempts (useful for detecting misconfigured grants):
SELECT
    event_time,
    user_identity.email  AS user_email,
    action_name,
    request_params,
    response.error_message
FROM system.access.audit
WHERE response.status_code != 200
  AND service_name = 'unityCatalog'
  AND event_time >= CURRENT_TIMESTAMP - INTERVAL 1 DAY
ORDER BY event_time DESC;


-- ============================================================
-- 4. Query history — who queried raw PII tables in the last 24h?
-- ============================================================
SELECT
    start_time,
    executed_by,
    statement_text,
    total_duration_ms,
    read_rows,
    read_bytes,
    status
FROM system.query.history
WHERE (statement_text ILIKE '%dim_account%'
    OR statement_text ILIKE '%birth_date%')
  AND start_time >= CURRENT_TIMESTAMP - INTERVAL 24 HOURS
ORDER BY start_time DESC;


-- ============================================================
-- 5. Grant audit — who has SELECT on dim_account?
-- ============================================================
SHOW GRANTS ON TABLE kings_prd.silver.dim_account;

-- Inherited grants (via schema or catalog):
SHOW GRANTS ON SCHEMA kings_prd.silver;


-- ============================================================
-- 6. Cost attribution — DBU consumption by pipeline
-- ============================================================
SELECT
    usage_date,
    usage_metadata.cluster_id,
    usage_metadata.job_id,
    usage_metadata.pipeline_id,
    sku_name,
    SUM(usage_quantity) AS dbu_consumed
FROM system.billing.usage
WHERE usage_date >= CURRENT_DATE - INTERVAL 7 DAYS
  AND usage_metadata.pipeline_id IS NOT NULL
GROUP BY ALL
ORDER BY dbu_consumed DESC;

-- Per-job cost trend (ADR-002: measure by query plan, but cost attribution
-- requires billing data, not wall clock):
SELECT
    usage_date,
    usage_metadata.job_id,
    SUM(usage_quantity) AS total_dbu
FROM system.billing.usage
WHERE usage_date >= CURRENT_DATE - INTERVAL 30 DAYS
GROUP BY usage_date, usage_metadata.job_id
ORDER BY usage_date DESC, total_dbu DESC;


-- ============================================================
-- 7. Freshness SLO check via system tables
-- ============================================================
-- When did sv_admission last receive a write? Compare to the
-- freshness_sla_minutes threshold in conf/base.yml (120 minutes).

SELECT
    table_name,
    last_modified,
    DATEDIFF(MINUTE, last_modified, CURRENT_TIMESTAMP) AS minutes_since_write,
    CASE
        WHEN DATEDIFF(MINUTE, last_modified, CURRENT_TIMESTAMP) > 120 THEN 'SLO_BREACHED'
        ELSE 'OK'
    END AS freshness_status
FROM system.information_schema.tables
WHERE table_catalog = 'kings_prd'
  AND table_schema  = 'silver'
  AND table_name    IN ('sv_admission', 'sv_ticket_order', 'dim_account')
ORDER BY minutes_since_write DESC;
