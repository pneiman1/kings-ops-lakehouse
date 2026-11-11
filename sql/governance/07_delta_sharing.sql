-- Delta Sharing: share gold tables with external partners
-- ADR-009 D6
--
-- Delta Sharing is an open protocol for sharing Delta Lake data without
-- copying it. The recipient gets a time-limited credential and reads
-- directly from the sharing server; the data never leaves the lakehouse.
--
-- Kings use case: a media analytics partner needs game-level P&L data
-- and the date dimension for broadcast planning. They should NOT have
-- credentials into the catalog itself.
--
-- Design rule (ADR-009 D6): shares expose ONLY gold tables. Bronze and
-- silver are never shared — bronze is raw/unmasked and silver is PII-bearing.

-- ============================================================
-- Create a share
-- ============================================================
CREATE SHARE IF NOT EXISTS kings_analytics_share
COMMENT 'Public-facing game analytics. Gold tables only. No PII.';

-- Add specific gold tables to the share:
ALTER SHARE kings_analytics_share ADD TABLE kings_prd.gold.fact_game_pnl;
ALTER SHARE kings_analytics_share ADD TABLE kings_prd.gold.dim_date;
ALTER SHARE kings_analytics_share ADD TABLE kings_prd.gold.mart_revenue_by_seat_class
  COMMENT 'Revenue by game and seat class. No individual order detail.';

-- Verify the share contents:
SHOW ALL IN SHARE kings_analytics_share;


-- ============================================================
-- Create a recipient
-- ============================================================
-- A recipient is a named external consumer. After creation, the activation
-- link contains a token the partner uses to configure their Delta Sharing client.

CREATE RECIPIENT IF NOT EXISTS media_analytics_partner
COMMENT 'Broadcast planning partner — read-only access to game P&L and calendar.';

-- Get the activation link (one-time; expires after 24h by default):
DESCRIBE RECIPIENT media_analytics_partner;
-- The activation_link field contains the URL the partner sends to their client.

-- For an open-sharing recipient (no IP restrictions), the partner can use:
--   spark.read.format("deltaSharing").option("responseFormat", "delta").load(...)


-- ============================================================
-- Grant the recipient access to the share
-- ============================================================
GRANT SELECT ON SHARE kings_analytics_share TO RECIPIENT media_analytics_partner;

-- Verify:
SHOW GRANTS ON SHARE kings_analytics_share;


-- ============================================================
-- Rotate or revoke access
-- ============================================================
-- Rotate the recipient token (invalidates the old activation link):
-- ALTER RECIPIENT media_analytics_partner ROTATE TOKEN;

-- Revoke access to a share:
-- REVOKE SELECT ON SHARE kings_analytics_share FROM RECIPIENT media_analytics_partner;

-- Remove a table from the share (recipient immediately loses access to it):
-- ALTER SHARE kings_analytics_share REMOVE TABLE kings_prd.gold.mart_revenue_by_seat_class;


-- ============================================================
-- What the recipient sees
-- ============================================================
-- From the recipient's side (using the open-source Delta Sharing client
-- or a Databricks workspace configured as a recipient):
--
-- SHOW SHARES IN PROVIDER <sharing_server>;
-- CREATE CATALOG IF NOT EXISTS kings_shared
--   USING SHARE <provider_name>.kings_analytics_share;
-- SELECT * FROM kings_shared.gold.fact_game_pnl;
