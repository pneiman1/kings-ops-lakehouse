-- Revenue-by-seat-class, as a MATERIALIZED VIEW (stored, incremental refresh).
--
-- WHEN THIS IS THE RIGHT CHOICE  (see docs/adr/ADR-007)  --  the production choice
-- -------------------------------------------------------------------------------
-- Dashboard-facing aggregates queried far more often than their inputs change. A
-- materialized view stores the result (fast reads, like a table) AND refreshes
-- incrementally and lineage-aware (fresh, like a view) without recomputing rows
-- that did not change. It is the best of table and view for a hot read path.
--
-- Cost: it is pipeline-managed and consumes refresh compute. On Free Edition that
-- refresh runs within the single-pipeline budget (ADR-003), scheduled with silver,
-- not continuously. Over-using MVs for cold objects wastes that budget.

CREATE MATERIALIZED VIEW IF NOT EXISTS ${catalog}.gold.mart_revenue_by_seat_class_mv
  COMMENT 'Collected revenue by game and seat class. Incrementally maintained.'
AS
SELECT
  game_id,
  seat_class,
  SUM(collected_revenue_usd)               AS collected_revenue_usd,
  SUM(CASE WHEN NOT is_refunded THEN 1 ELSE 0 END) AS tickets_sold
FROM ${catalog}.gold.fact_ticket_order
GROUP BY game_id, seat_class;
