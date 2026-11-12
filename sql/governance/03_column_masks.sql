-- Column masking on PII fields in dim_account
-- ADR-009 D3 + D4
--
-- dim_account carries: first_name, last_name, email, phone, street_address,
-- postal_code, payment_token_last4, birth_date.
-- All are declared is_pii=True in the data contract (definitions.py).
--
-- birth_date is NOT masked here — see the row filter (03_row_filters.sql)
-- and ADR-009 D4 for why masking it would break the minor access control.
--
-- Masking functions live in a governance schema so engineers can see and
-- audit them without touching the tables they protect.

CREATE SCHEMA IF NOT EXISTS kings_prd.governance
COMMENT 'Masking functions, row filter functions, and audit views.';


-- ============================================================
-- Generic string masking function
-- ============================================================
-- Callers who are account admins or members of kings_pii_access see the
-- real value. Everyone else sees the literal string '***'.
-- Using IS_MEMBER() means adding a user to the group immediately changes
-- what they see — no SQL change required.

CREATE OR REPLACE FUNCTION kings_prd.governance.mask_string(value STRING)
  RETURNS STRING
  COMMENT 'Returns value unmasked for account admins and kings_pii_access members; *** otherwise.'
  RETURN CASE
    WHEN is_account_admin() OR IS_MEMBER('kings_pii_access') THEN value
    ELSE '***'
  END;


-- ============================================================
-- Email-specific mask: preserve domain for analysis
-- ============================================================
-- Full masking of email destroys domain-level analytics (which email
-- providers do our tourists use?). The domain is not PII; the local-part is.
-- Analysts who are not PII-cleared see: ***@domain.com

CREATE OR REPLACE FUNCTION kings_prd.governance.mask_email(value STRING)
  RETURNS STRING
  COMMENT 'Masks local-part of email; preserves domain for analysts. Full value for pii_access.'
  RETURN CASE
    WHEN is_account_admin() OR IS_MEMBER('kings_pii_access') THEN value
    WHEN value IS NULL THEN NULL
    ELSE CONCAT('***@', SPLIT_PART(value, '@', 2))
  END;


-- ============================================================
-- Payment token: preserve last4 for ops, mask everything else
-- ============================================================
-- payment_token_last4 is already a last-4 representation. Analysts do not
-- need even this for aggregation. Mask entirely for non-PII principals.

CREATE OR REPLACE FUNCTION kings_prd.governance.mask_payment_token(value STRING)
  RETURNS STRING
  COMMENT 'Returns last4 token only for pii_access; *** for everyone else.'
  RETURN CASE
    WHEN is_account_admin() OR IS_MEMBER('kings_pii_access') THEN value
    ELSE '***'
  END;


-- ============================================================
-- Apply masks to dim_account columns
-- ============================================================
-- ALTER COLUMN ... SET MASK wires the function. The function is called
-- per-row, per-query, for every reader not short-circuited by is_account_admin().
-- Performance note: at the query volume of this platform this is not a concern.

ALTER TABLE kings_prd.silver.dim_account
  ALTER COLUMN first_name       SET MASK kings_prd.governance.mask_string;

ALTER TABLE kings_prd.silver.dim_account
  ALTER COLUMN last_name        SET MASK kings_prd.governance.mask_string;

ALTER TABLE kings_prd.silver.dim_account
  ALTER COLUMN email            SET MASK kings_prd.governance.mask_email;

ALTER TABLE kings_prd.silver.dim_account
  ALTER COLUMN phone            SET MASK kings_prd.governance.mask_string;

ALTER TABLE kings_prd.silver.dim_account
  ALTER COLUMN street_address   SET MASK kings_prd.governance.mask_string;

ALTER TABLE kings_prd.silver.dim_account
  ALTER COLUMN postal_code      SET MASK kings_prd.governance.mask_string;

ALTER TABLE kings_prd.silver.dim_account
  ALTER COLUMN payment_token_last4 SET MASK kings_prd.governance.mask_payment_token;

-- birth_date: explicitly NOT masked — the minor row filter reads it.
-- See 04_row_filters.sql and ADR-009 D4.


-- ============================================================
-- Verify: query as yourself, then query as a masked-view user
-- ============================================================

-- As a pii_access member, should see real values:
SELECT account_id, first_name, last_name, email, birth_date
FROM kings_prd.silver.dim_account LIMIT 5;

-- As a standard analyst, should see *** for contact fields, real birth_date:
-- (Use Run As > Other User in Databricks to test, or SET SESSION AUTHORIZATION)
