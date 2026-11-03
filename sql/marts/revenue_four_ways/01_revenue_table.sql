-- Revenue-by-seat-class, as a MANAGED TABLE (batch CTAS snapshot).
--
-- WHEN THIS IS THE RIGHT CHOICE  (see docs/adr/ADR-007)
-- -----------------------------------------------------
-- Point-in-time snapshots and handoffs: a frozen month-end cut, or an extract a
-- downstream system reads once. A table stores the result and only changes when
-- you rerun this statement — so freshness is exactly as good as the last run, and
-- a full overwrite recomputes every row whether or not it changed.
--
-- It is the WRONG default for a dashboard-facing aggregate: you either over-refresh
-- (cost) or serve stale numbers (trust). For that, use the materialized view (03).
--
-- Templated with ${catalog} so the same file runs against kings_dev/stg/prd. These
-- four files are illustrative (ADR-007); the bundle deploys the MV form.

CREATE OR REPLACE TABLE ${catalog}.gold.mart_revenue_by_seat_class_tbl AS
SELECT
  game_id,
  seat_class,
  SUM(collected_revenue_usd)               AS collected_revenue_usd,
  SUM(CASE WHEN NOT is_refunded THEN 1 ELSE 0 END) AS tickets_sold
FROM ${catalog}.gold.fact_ticket_order
GROUP BY game_id, seat_class;
