-- Tags and attribute-based access control (ABAC)
-- ADR-009 D5
--
-- Unity Catalog tags label objects at the catalog, schema, table, or column level.
-- Tags do NOT enforce access by themselves — enforcement is through masking
-- functions and row filters that check IS_MEMBER() or column-level tag metadata.
--
-- Tag taxonomy used in this platform:
--   sensitivity    pii | internal | public
--   pii_category   contact | financial | identity | behavioral
--   regulatory     coppa           (minor-related data)
--   quality        bronze | silver | gold | quarantine   (layer marker)
--   pipeline_owner <team name>

-- ============================================================
-- Apply tags to dim_account columns
-- ============================================================

ALTER TABLE kings_prd.silver.dim_account
  ALTER COLUMN first_name       SET TAGS ('sensitivity' = 'pii', 'pii_category' = 'identity');

ALTER TABLE kings_prd.silver.dim_account
  ALTER COLUMN last_name        SET TAGS ('sensitivity' = 'pii', 'pii_category' = 'identity');

ALTER TABLE kings_prd.silver.dim_account
  ALTER COLUMN email            SET TAGS ('sensitivity' = 'pii', 'pii_category' = 'contact');

ALTER TABLE kings_prd.silver.dim_account
  ALTER COLUMN phone            SET TAGS ('sensitivity' = 'pii', 'pii_category' = 'contact');

ALTER TABLE kings_prd.silver.dim_account
  ALTER COLUMN street_address   SET TAGS ('sensitivity' = 'pii', 'pii_category' = 'contact');

ALTER TABLE kings_prd.silver.dim_account
  ALTER COLUMN postal_code      SET TAGS ('sensitivity' = 'pii', 'pii_category' = 'contact');

ALTER TABLE kings_prd.silver.dim_account
  ALTER COLUMN payment_token_last4
                                SET TAGS ('sensitivity' = 'pii', 'pii_category' = 'financial');

-- birth_date: PII and COPPA-relevant. The minor row filter depends on this column.
ALTER TABLE kings_prd.silver.dim_account
  ALTER COLUMN birth_date       SET TAGS ('sensitivity' = 'pii',
                                          'pii_category' = 'identity',
                                          'regulatory' = 'coppa');


-- ============================================================
-- Apply tags at the table level
-- ============================================================

ALTER TABLE kings_prd.silver.dim_account
  SET TAGS ('sensitivity' = 'pii', 'pipeline_owner' = 'data_engineering');

-- Gold tables are not PII (PII was masked or dropped in silver → gold):
ALTER TABLE kings_prd.gold.fact_ticket_order
  SET TAGS ('sensitivity' = 'internal', 'pipeline_owner' = 'data_engineering');

ALTER TABLE kings_prd.gold.fact_game_pnl
  SET TAGS ('sensitivity' = 'internal', 'pipeline_owner' = 'data_engineering');


-- ============================================================
-- Using tags in ABAC masking (advanced pattern)
-- ============================================================
-- Instead of hard-coding column names in masking functions, an ABAC pattern
-- reads tag metadata to decide whether a column is PII. This is more
-- maintainable when the PII column set changes frequently.

-- Find all PII columns in production silver (useful for audits):
SELECT
    table_catalog,
    table_schema,
    table_name,
    column_name,
    tag_name,
    tag_value
FROM system.information_schema.column_tags
WHERE table_catalog = 'kings_prd'
  AND table_schema  = 'silver'
  AND tag_name      = 'sensitivity'
  AND tag_value     = 'pii'
ORDER BY table_name, column_name;

-- Find all COPPA-tagged columns:
SELECT table_name, column_name, tag_value
FROM system.information_schema.column_tags
WHERE table_catalog = 'kings_prd'
  AND tag_name      = 'regulatory'
  AND tag_value     = 'coppa';


-- ============================================================
-- Schema and catalog-level tags
-- ============================================================

ALTER SCHEMA kings_prd.silver SET TAGS ('quality' = 'silver');
ALTER SCHEMA kings_prd.gold   SET TAGS ('quality' = 'gold');
ALTER SCHEMA kings_prd.bronze SET TAGS ('quality' = 'bronze');

-- Catalog-level quality marker is set at catalog creation; here for reference:
-- ALTER CATALOG kings_prd SET TAGS ('environment' = 'production');
