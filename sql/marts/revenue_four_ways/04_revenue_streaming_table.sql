-- Revenue-by-seat-class, as a STREAMING TABLE (append/stateful from a stream).
--
-- WHEN THIS IS THE RIGHT CHOICE  (see docs/adr/ADR-007)
-- -----------------------------------------------------
-- The input arrives as an unbounded stream and you want incremental, stateful
-- processing (running aggregates, dedup, late-data handling) rather than a periodic
-- full recompute. A streaming table reads STREAM(...) and maintains state across
-- micro-batches.
--
-- The catch, and why it is NOT the production choice for this mart: a streaming
-- source must be APPEND-ONLY. gold.fact_ticket_order is current-state (it updates on
-- refund/transfer), so it cannot be streamed. The genuinely unbounded, append-only
-- source here is the ticketing CDC in bronze — so this example aggregates collected
-- revenue directly from br_ticketing_cdc with a watermark on __commit_ts. It is the
-- object type that the admission fact (T4) will actually use, where the input (gate
-- scans) is a real stream.
--
-- NOTE: a stream sees each CDC event, not resolved order state, so this running
-- total treats SALE as revenue and REFUND as negative revenue. That is an
-- approximation of collected revenue suited to a live operational counter, not the
-- audited figure — the audited figure is the MV over the resolved fact (03).

CREATE OR REFRESH STREAMING TABLE ${catalog}.gold.mart_revenue_stream
  COMMENT 'Live running revenue by game/seat_class from the ticketing CDC stream.'
AS
SELECT
  game_id,
  seat_class,
  window(__commit_ts, '1 hour')                     AS commit_window,
  SUM(CASE
        WHEN event_type = 'SALE'   THEN paid_price_usd
        WHEN event_type = 'REFUND' THEN -paid_price_usd
        ELSE 0
      END)                                          AS running_revenue_usd
FROM STREAM(${catalog}.bronze.br_ticketing_cdc)
WATERMARK __commit_ts DELAY OF INTERVAL 48 HOURS
GROUP BY game_id, seat_class, window(__commit_ts, '1 hour');
