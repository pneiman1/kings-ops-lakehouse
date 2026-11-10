-- Revenue-by-seat-class, as a VIEW (no storage, recomputed on every read).
--
-- WHEN THIS IS THE RIGHT CHOICE  (see docs/adr/ADR-007)
-- -----------------------------------------------------
-- Thin projections and rarely-queried objects: a view is always fresh and costs
-- nothing to store, because it holds no data — every query re-runs the SELECT, and
-- therefore the full lineage back to silver.
--
-- That freshness is exactly why it is WRONG for a hot aggregate over a 2.4M-row
-- fact: each dashboard load rebuilds the star. Reach for a view when the input
-- changes about as often as the result is read, or when the query is cheap.

CREATE OR REPLACE VIEW ${catalog}.gold.mart_revenue_by_seat_class_vw AS
SELECT
  game_id,
  seat_class,
  SUM(collected_revenue_usd)               AS collected_revenue_usd,
  SUM(CASE WHEN NOT is_refunded THEN 1 ELSE 0 END) AS tickets_sold
FROM ${catalog}.gold.fact_ticket_order
GROUP BY game_id, seat_class;
