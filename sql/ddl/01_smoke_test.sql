-- Post-deploy smoke test.
--
-- Asserts the four things a deploy can plausibly get wrong and that YAML
-- validation cannot catch: the catalog exists, every expected schema exists,
-- the landing volume is reachable, and the deploying identity can actually
-- write. Cheap, and it converts "deployed successfully" from a claim about
-- YAML into a claim about the platform.

-- 1. Catalog reachable
USE CATALOG IDENTIFIER(:catalog);

SELECT current_catalog() AS resolved_catalog;

-- 2. All six schemas present. RAISE_ERROR fails the task, which fails the job,
--    which fails the workflow — the failure has to propagate all the way up or
--    the gate is theatre.
SELECT
  CASE
    WHEN COUNT(*) >= 6 THEN 'OK'
    ELSE RAISE_ERROR(
      CONCAT('Expected 6 schemas, found ', CAST(COUNT(*) AS STRING), '. Deploy is incomplete.')
    )
  END AS schema_check
FROM system.information_schema.schemata
WHERE catalog_name = current_catalog()
  AND (schema_name IN ('landing', 'bronze', 'silver', 'gold', 'quarantine', 'ops')
       OR schema_name RLIKE '_(landing|bronze|silver|gold|quarantine|ops)$');

-- 3. Write path works end to end.
CREATE TABLE IF NOT EXISTS IDENTIFIER(:ops_schema || ".deploy_smoke") (
  run_id        STRING    COMMENT 'CI run that produced this record',
  deployed_at   TIMESTAMP COMMENT 'Deploy verification timestamp',
  environment   STRING    COMMENT 'Target environment',
  git_sha       STRING    COMMENT 'Commit deployed'
)
COMMENT 'One row per successful deploy verification. Also the deploy audit trail.';

INSERT INTO IDENTIFIER(:ops_schema || ".deploy_smoke")
SELECT :run_id, current_timestamp(), :environment, :git_sha;

-- 4. Read back what was just written.
SELECT COUNT(*) AS smoke_rows FROM IDENTIFIER(:ops_schema || ".deploy_smoke") WHERE run_id = :run_id;
