-- Unity Catalog privilege model: traversal, GRANT, REVOKE, DENY
-- ADR-009 D2
--
-- The single most common governance mistake in UC deployments:
-- granting SELECT on a table without granting USE CATALOG + USE SCHEMA first.
-- The error message says "permission denied on table" but the missing grant
-- is usually on the schema. This script encodes the correct sequence.
--
-- Principals used in this platform:
--   kings_data_engineers  full pipeline access (read/write all schemas)
--   kings_analysts        read gold; masked view of silver PII
--   kings_pii_access      read silver PII columns unmasked
--   kings_hr_ops          read dim_account; minor row filter applies to ALL
--   svc_pipeline          service principal running the Lakeflow pipelines

-- ============================================================
-- Step 1 — Traversal: USE CATALOG and USE SCHEMA
-- ============================================================
-- Without these, every SELECT fails with "permission denied" even if SELECT
-- is granted. UC checks traversal before object privileges.

-- Analysts get read access to production gold:
GRANT USE CATALOG ON CATALOG kings_prd TO GROUP kings_analysts;
GRANT USE SCHEMA  ON SCHEMA  kings_prd.gold    TO GROUP kings_analysts;
GRANT USE SCHEMA  ON SCHEMA  kings_prd.silver  TO GROUP kings_analysts;

-- Engineers get full traversal:
GRANT USE CATALOG ON CATALOG kings_prd TO GROUP kings_data_engineers;
GRANT USE CATALOG ON CATALOG kings_dev TO GROUP kings_data_engineers;
GRANT USE CATALOG ON CATALOG kings_stg TO GROUP kings_data_engineers;

-- Pipeline service principal needs bronze and silver (read) + silver and gold (write):
GRANT USE CATALOG ON CATALOG kings_prd TO USER svc_pipeline;
GRANT USE SCHEMA  ON SCHEMA  kings_prd.bronze    TO USER svc_pipeline;
GRANT USE SCHEMA  ON SCHEMA  kings_prd.silver    TO USER svc_pipeline;
GRANT USE SCHEMA  ON SCHEMA  kings_prd.gold      TO USER svc_pipeline;
GRANT USE SCHEMA  ON SCHEMA  kings_prd.quarantine TO USER svc_pipeline;


-- ============================================================
-- Step 2 — Object privileges
-- ============================================================

-- Engineers: everything
GRANT ALL PRIVILEGES ON SCHEMA kings_prd.bronze     TO GROUP kings_data_engineers;
GRANT ALL PRIVILEGES ON SCHEMA kings_prd.silver     TO GROUP kings_data_engineers;
GRANT ALL PRIVILEGES ON SCHEMA kings_prd.gold       TO GROUP kings_data_engineers;
GRANT ALL PRIVILEGES ON SCHEMA kings_prd.quarantine TO GROUP kings_data_engineers;

-- Analysts: read gold only
GRANT SELECT ON SCHEMA kings_prd.gold TO GROUP kings_analysts;

-- Analysts also need silver for a few curated views (dim_account with masks applied):
GRANT SELECT ON TABLE kings_prd.silver.dim_account TO GROUP kings_analysts;
GRANT SELECT ON TABLE kings_prd.silver.sv_ticket_order TO GROUP kings_analysts;
GRANT SELECT ON TABLE kings_prd.silver.sv_admission    TO GROUP kings_analysts;

-- PII-cleared group: unmasked silver reads
GRANT SELECT ON SCHEMA kings_prd.silver TO GROUP kings_pii_access;

-- Pipeline: read bronze, write silver and gold
GRANT SELECT ON SCHEMA kings_prd.bronze TO USER svc_pipeline;
GRANT MODIFY ON SCHEMA kings_prd.silver TO USER svc_pipeline;
GRANT MODIFY ON SCHEMA kings_prd.gold   TO USER svc_pipeline;
GRANT MODIFY ON SCHEMA kings_prd.quarantine TO USER svc_pipeline;


-- ============================================================
-- Step 3 — DENY: hard exclusions that override all GRANTs
-- ============================================================
-- DENY overrides GRANT regardless of group hierarchy.
-- Use it only for hard exclusions — it cannot be "un-denied" by another GRANT.

-- Analysts must not read raw quarantine tables (contains unmasked failed-parse rows):
DENY SELECT ON SCHEMA kings_prd.quarantine TO GROUP kings_analysts;

-- Analysts must not read bronze (raw, unmasked, pre-contract):
DENY SELECT ON SCHEMA kings_prd.bronze TO GROUP kings_analysts;


-- ============================================================
-- Step 4 — Inspect effective privileges
-- ============================================================

-- What does a specific user actually have?
SHOW GRANTS ON TABLE kings_prd.gold.fact_ticket_order;

-- What can a group do on a schema?
SHOW GRANTS ON SCHEMA kings_prd.silver;

-- Check traversal grants on the catalog:
SHOW GRANTS ON CATALOG kings_prd;


-- ============================================================
-- Step 5 — Revoke
-- ============================================================

-- Revoking an object grant does not remove traversal grants.
-- Revoking USE SCHEMA removes access to all tables under it in one operation.
-- REVOKE SELECT ON TABLE kings_prd.silver.dim_account FROM GROUP kings_analysts;
-- REVOKE USE SCHEMA ON SCHEMA kings_prd.silver FROM GROUP kings_analysts;
