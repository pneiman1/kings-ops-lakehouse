-- Managed vs external tables in Unity Catalog
-- ADR-009 D1
--
-- Run this in a SQL warehouse against a dev catalog to observe the difference.
-- Understanding what DROP TABLE does is the clearest way to encode the choice.
--
-- Exam objective: explain when to use managed vs external locations and the
-- consequence of DROP on each.

-- ============================================================
-- MANAGED TABLE (default for CTAS, CREATE TABLE without LOCATION)
-- ============================================================

-- UC owns the metadata AND the data files. Files live under the catalog's
-- managed storage location (configured on the catalog at creation time).
CREATE TABLE IF NOT EXISTS kings_dev.bronze.demo_managed (
    id      BIGINT,
    payload STRING
)
USING DELTA
COMMENT 'Managed table demo — UC controls data lifecycle.';

-- Inspect where the files actually live:
DESCRIBE DETAIL kings_dev.bronze.demo_managed;
-- location will be under abfss://.../<catalog>/<schema>/demo_managed/

-- DROP TABLE removes BOTH the metadata entry AND the underlying Parquet/Delta files.
-- There is no recycle bin. This is the right behaviour for pipeline-owned tables:
-- a dropped table that re-runs the pipeline gets rebuilt from scratch.
-- DROP TABLE kings_dev.bronze.demo_managed;  -- removes files, not just metadata


-- ============================================================
-- EXTERNAL TABLE (requires a registered external location)
-- ============================================================

-- UC owns only the metadata. Files live wherever LOCATION points, which must be
-- under a registered external location that UC trusts.
-- Note: external locations are registered via storage credentials in UC admin.
CREATE TABLE IF NOT EXISTS kings_dev.bronze.demo_external (
    id      BIGINT,
    payload STRING
)
USING DELTA
LOCATION 'abfss://landing@<storage_account>.dfs.core.windows.net/demo_external/'
COMMENT 'External table demo — data files survive a DROP TABLE.';

-- DROP TABLE removes ONLY the metadata. Data files remain at LOCATION.
-- Correct for the landing Volume (raw source files the pipeline did not create).
-- DROP TABLE kings_dev.bronze.demo_external;  -- files survive


-- ============================================================
-- Kings platform choice
-- ============================================================

-- bronze / silver / gold schemas:  MANAGED
--   Pipeline owns the lifecycle. Rebuilding from source is always possible.
--   DROP TABLE + re-run = clean slate, which is the recovery path.

-- landing Volume (raw source files):  EXTERNAL LOCATION, not a table
--   Source files are not pipeline artifacts. The Volume is an external location
--   registered in UC; Auto Loader reads from it without owning it.

-- Consequence: never run DROP SCHEMA ... CASCADE on a production schema.
-- It is on the AUTONOMY.md STOP list for exactly this reason.
