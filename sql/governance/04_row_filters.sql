-- Row filter for minor account holders (birth_date-based)
-- ADR-009 D4
--
-- Policy: accounts where the holder was under 18 at the start of the 2027
-- season must not be visible to general analysts. Only account admins and
-- members of kings_hr_ops (who handle youth camp rosters explicitly) may
-- see minor rows.
--
-- Why birth_date is NOT column-masked:
--   The row filter function receives the raw column value and compares it.
--   If birth_date were masked to '***' first, the comparison to a date would
--   fail (or raise a cast error), and the CASE WHEN would evaluate to the
--   ELSE branch — returning TRUE, making every row visible. That would be a
--   silent access control bypass. The column carries a PII tag and is
--   subject to access control at the TABLE level instead.
--
-- Season start for age calculation:
--   2027-04-01 (opening day). An account holder born after 2009-04-01
--   is under 18 at the start of the season and is considered a minor here.

CREATE OR REPLACE FUNCTION kings_prd.governance.minor_row_filter(birth_date DATE)
  RETURNS BOOLEAN
  COMMENT 'Returns TRUE (row visible) unless holder is a minor and caller lacks hr_ops access.'
  RETURN CASE
    WHEN is_account_admin() THEN TRUE
    WHEN IS_MEMBER('kings_hr_ops') THEN TRUE
    -- Minor: born after 2009-04-01 (< 18 at season start 2027-04-01)
    WHEN birth_date > DATE '2009-04-01' THEN FALSE
    ELSE TRUE
  END;


-- Apply the row filter to dim_account.
-- The filter column (birth_date) must be declared; UC calls the function
-- with the value from that column for each candidate row.
ALTER TABLE kings_prd.silver.dim_account
  SET ROW FILTER kings_prd.governance.minor_row_filter ON (birth_date);


-- ============================================================
-- Verify: check the minor count is consistent
-- ============================================================

-- As an account admin or hr_ops member — should see all rows including minors:
SELECT COUNT(*) AS total_accounts FROM kings_prd.silver.dim_account WHERE __is_current;

-- Count of minors in the dataset (admin view):
SELECT COUNT(*) AS minor_count
FROM kings_prd.silver.dim_account
WHERE __is_current
  AND birth_date > DATE '2009-04-01';
-- Expected at small scale: ~88 (from START_HERE.md defect table)

-- As a standard analyst — the same query must return 0 for minors:
-- (Test with "View as" / Run As another user)


-- ============================================================
-- Row filter on quarantine tables (defence in depth)
-- ============================================================
-- The DENY on the quarantine schema (02_privilege_model.sql) is the primary
-- control. This secondary filter on q_scan_invalid prevents an analyst who
-- somehow gains access from seeing orphan scan details that could reveal a
-- minor's presence at a game.

CREATE OR REPLACE FUNCTION kings_prd.governance.quarantine_access_filter()
  RETURNS BOOLEAN
  COMMENT 'Quarantine rows visible only to engineers and admins.'
  RETURN is_account_admin() OR IS_MEMBER('kings_data_engineers');

ALTER TABLE kings_prd.quarantine.q_scan_invalid
  SET ROW FILTER kings_prd.governance.quarantine_access_filter ON ();

ALTER TABLE kings_prd.quarantine.q_gate_scan
  SET ROW FILTER kings_prd.governance.quarantine_access_filter ON ();


-- ============================================================
-- Removing filters (for rollback or re-application after change)
-- ============================================================
-- ALTER TABLE kings_prd.silver.dim_account DROP ROW FILTER;
-- Then re-apply with SET ROW FILTER after any function update.
-- Note: DROP ROW FILTER makes ALL rows visible immediately. Do not run in
-- production without an immediate re-application.
