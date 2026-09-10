-- Catalog bootstrap. Run ONCE per environment, by the bootstrap job.
--
-- Catalogs are not a bundle resource type, so they are created here rather than
-- in YAML. Everything below the catalog (schemas, volumes) IS bundle-managed
-- and must not be created by hand.
--
-- On Databricks Free Edition the signing-up user is the metastore admin of the
-- single available metastore, so CREATE CATALOG succeeds without additional
-- grants. In an enterprise workspace this script is run by a platform admin,
-- not by the pipeline identity.
--
-- Parameterized on :catalog so one script serves all three environments.

CREATE CATALOG IF NOT EXISTS IDENTIFIER(:catalog)
COMMENT 'Las Vegas Kings ballpark operations lakehouse. Environment isolation boundary — see ADR-001.';

-- Environment intent is recorded as a catalog property so that governance
-- queries in T6 can distinguish prd from non-prd without parsing names.
ALTER CATALOG IDENTIFIER(:catalog) SET TAGS ('environment' = :environment, 'project' = 'kings-ops-lakehouse');
