-- Lakehouse Federation: federated reads from the operational ticketing store
-- ADR-009 D7
--
-- Lakehouse Federation lets Unity Catalog govern and query external databases
-- (Postgres, MySQL, SQL Server, Snowflake, etc.) without copying data into
-- bronze first. Queries execute on the remote system; results return through UC.
--
-- Kings use case (migration path, not active):
--   ADR-006 notes that if the ticketing OLTP moves to Lakebase or a
--   directly-reachable operational store, the file-extract hop can be removed.
--   Federation is the read path for that scenario; Lakeflow Connect is the
--   streaming ingestion path.
--
-- Prerequisites:
--   1. A registered storage credential or service credential in UC for the
--      external database (admin setup, one-time per connection type).
--   2. Network connectivity from the Databricks control plane to the external DB.
--   3. The external database credentials stored in a Databricks secret scope,
--      never in a notebook or config file.

-- ============================================================
-- Step 1 — Create the connection
-- ============================================================
-- Connections are workspace-level objects that hold the driver type and
-- network coordinates. Credentials reference a secret scope.

CREATE CONNECTION IF NOT EXISTS ticketing_oltp_conn
    TYPE postgresql
    OPTIONS (
        host     'ticketing-db.internal.kingsops.net',
        port     '5432',
        database 'kings_tickets'
    )
    WITH CREDENTIAL SECRET SCOPE 'kings' KEY 'ticketing_oltp_creds'
COMMENT 'Operational ticketing database. Read-only service account. ADR-006 migration path.';

-- View connection details (credentials are never shown):
SHOW CONNECTIONS;
DESCRIBE CONNECTION ticketing_oltp_conn;


-- ============================================================
-- Step 2 — Create the foreign catalog
-- ============================================================
-- A foreign catalog maps the external database's schemas and tables into
-- Unity Catalog's namespace. The catalog is read-only by default.

CREATE FOREIGN CATALOG IF NOT EXISTS kings_ticketing_live
    USING CONNECTION ticketing_oltp_conn
COMMENT 'Live read view of the operational ticketing store. For break-glass and validation only.';

-- UC traversal grants apply to foreign catalogs identically to managed catalogs:
GRANT USE CATALOG ON CATALOG kings_ticketing_live TO GROUP kings_data_engineers;
GRANT USE SCHEMA  ON SCHEMA  kings_ticketing_live.public TO GROUP kings_data_engineers;
GRANT SELECT      ON SCHEMA  kings_ticketing_live.public TO GROUP kings_data_engineers;

-- Standard analysts must NOT read the live operational store:
DENY USE CATALOG ON CATALOG kings_ticketing_live TO GROUP kings_analysts;


-- ============================================================
-- Step 3 — Federated queries
-- ============================================================
-- Query the live operational table as if it were a Delta table.
-- Query pushdown: filter and projection predicates are translated to the
-- remote database's dialect and executed there; only the result set returns.

SELECT
    order_id,
    game_id,
    event_type,
    paid_price_usd,
    __commit_ts
FROM kings_ticketing_live.public.ticket_orders
WHERE __commit_ts >= CURRENT_TIMESTAMP - INTERVAL 15 MINUTES
ORDER BY __commit_ts DESC
LIMIT 100;

-- Cross-catalog join: federated live data joined to Delta Lake silver.
-- Use case: validate that the pipeline has processed the latest operational events.
SELECT
    live.order_id,
    live.__commit_ts              AS source_ts,
    silver.order_status           AS pipeline_status,
    silver.paid_price_usd         AS pipeline_price,
    live.paid_price_usd           AS source_price
FROM kings_ticketing_live.public.ticket_orders AS live
LEFT JOIN kings_prd.silver.sv_ticket_order AS silver USING (order_id)
WHERE live.__commit_ts >= CURRENT_TIMESTAMP - INTERVAL 1 HOUR
  AND silver.order_id IS NULL   -- orders the pipeline has not yet processed
ORDER BY live.__commit_ts DESC;


-- ============================================================
-- Step 4 — Lineage through federation
-- ============================================================
-- UC tracks lineage through federated queries: a gold table that JOINs
-- a foreign catalog table will show the foreign table as a source in
-- system.access.table_lineage. This is what makes federation preferable
-- to ad-hoc JDBC reads outside UC — lineage is maintained.


-- ============================================================
-- Step 5 — Remove the federation (rollback or migration complete)
-- ============================================================
-- DROP FOREIGN CATALOG kings_ticketing_live;    -- removes the catalog mapping only
-- DROP CONNECTION ticketing_oltp_conn;          -- removes the connection
-- Neither drops data on the remote system.
