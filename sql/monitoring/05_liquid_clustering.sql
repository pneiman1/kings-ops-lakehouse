-- Liquid clustering and predictive optimization
-- ADR-010 D4
--
-- fact_admission is the highest-cardinality fact table and the primary
-- target for game-day attendance queries. Liquid clustering on
-- (game_id, gate) lets Delta skip files when queries filter by game and
-- gate — the two most common predicates.
--
-- Why liquid over static partitioning (ADR-010 D4):
--   - game_id has ~81 distinct values at full scale; a partition-per-game
--     produces small files and high metadata overhead.
--   - gate is a secondary filter that static partitioning cannot help
--     without double-partitioning (which squares the small-file problem).
--   - Liquid clustering re-clusters automatically on write; no manual
--     OPTIMIZE + ZORDER schedule is needed.
--
-- Predictive optimization on silver and gold tables handles small-file
-- compaction automatically; no OPTIMIZE cron job required.


-- ============================================================
-- 1. Enable liquid clustering on fact_admission
-- ============================================================
-- Run once after the table is created by the gold pipeline.
-- CLUSTER BY replaces any existing ZORDER configuration.

ALTER TABLE kings_prd.gold.fact_admission
  CLUSTER BY (game_id, gate);

-- Verify the clustering columns were applied:
DESCRIBE DETAIL kings_prd.gold.fact_admission;
-- Look for "clusteringColumns" in the returned row.


-- ============================================================
-- 2. Enable predictive optimization on silver and gold tables
-- ============================================================
-- Predictive optimization runs OPTIMIZE and VACUUM automatically
-- based on table access patterns. It is a Databricks-managed feature
-- and must be enabled per table (or via schema/catalog default).
--
-- Note from ADR-010: not available in all regions; defaults to off.
-- Enable once — it persists across pipeline runs.

ALTER TABLE kings_prd.silver.sv_admission
  SET TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true',
                     'delta.autoOptimize.autoCompact'   = 'true');

ALTER TABLE kings_prd.silver.dim_seat
  SET TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true',
                     'delta.autoOptimize.autoCompact'   = 'true');

ALTER TABLE kings_prd.gold.fact_admission
  SET TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true',
                     'delta.autoOptimize.autoCompact'   = 'true');

ALTER TABLE kings_prd.gold.dim_account_gold
  SET TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true',
                     'delta.autoOptimize.autoCompact'   = 'true');


-- ============================================================
-- 3. Trigger an initial OPTIMIZE to seed the clustering layout
-- ============================================================
-- Liquid clustering applies to newly written files automatically,
-- but existing files are not re-clustered until the first OPTIMIZE
-- runs (ADR-010 Consequences). Run this once after enabling.

OPTIMIZE kings_prd.gold.fact_admission;

-- For a full re-cluster (e.g., after a large backfill):
-- OPTIMIZE kings_prd.gold.fact_admission FULL;


-- ============================================================
-- 4. Inspect clustering effectiveness via Delta log stats
-- ============================================================
-- DESCRIBE DETAIL returns file count, size, and clustering metadata.
-- Check num_files and avg file size: target is 128–256 MB per file.

DESCRIBE DETAIL kings_prd.gold.fact_admission;

-- Check which columns the clustering is using:
SELECT
  col_name,
  data_type,
  comment
FROM (DESCRIBE TABLE EXTENDED kings_prd.gold.fact_admission)
WHERE col_name IN ('game_id', 'gate');


-- ============================================================
-- 5. Clustering health: file skew and size distribution
-- ============================================================
-- Query the Delta log to see file size distribution.
-- Files that are too small (<= 32 MB) need compaction.
-- Files that are too large (>= 1 GB) indicate write amplification.

SELECT
  COUNT(*)                                        AS num_files,
  ROUND(AVG(size) / 1e6, 1)                       AS avg_mb,
  ROUND(MIN(size) / 1e6, 1)                       AS min_mb,
  ROUND(MAX(size) / 1e6, 1)                       AS max_mb,
  ROUND(PERCENTILE(size, 0.5) / 1e6, 1)           AS p50_mb,
  ROUND(PERCENTILE(size, 0.95) / 1e6, 1)          AS p95_mb,
  SUM(CASE WHEN size < 32 * 1e6 THEN 1 ELSE 0 END) AS small_files,
  SUM(CASE WHEN size > 512 * 1e6 THEN 1 ELSE 0 END) AS large_files
FROM (
  SELECT size FROM delta.`/Volumes/kings_prd/gold/fact_admission/` -- volume path
  -- In practice: SELECT size FROM kings_prd.gold.fact_admission.files
  -- (requires SHOW FILES or the Delta log API)
);


-- ============================================================
-- 6. VACUUM: remove files no longer needed by any snapshot
-- ============================================================
-- Delta retains old file versions for time-travel. Default retention
-- is 7 days. Run VACUUM periodically to reclaim storage.
-- DO NOT set retention below 7 days if time-travel queries are in use.

VACUUM kings_prd.gold.fact_admission RETAIN 168 HOURS;  -- 7 days
VACUUM kings_prd.silver.sv_admission RETAIN 168 HOURS;


-- ============================================================
-- 7. Confirm predictive optimization is active
-- ============================================================
-- Check TBLPROPERTIES to verify the settings are applied.

SHOW TBLPROPERTIES kings_prd.gold.fact_admission;
-- Look for:
--   delta.autoOptimize.optimizeWrite = true
--   delta.autoOptimize.autoCompact   = true
--   delta.clusteringColumns           = ["game_id","gate"]
