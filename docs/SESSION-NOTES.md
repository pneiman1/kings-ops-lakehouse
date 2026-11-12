---

## T6 — Governance (feat/governance)

**Branch:** feat/governance  
**Status:** committed and pushed, PR pending gh auth (B3)

**Files:**
- `docs/adr/ADR-009-unity-catalog-governance.md` — 7 decisions: managed vs external, traversal, IS_MEMBER masking, birth_date exclusion from masking, tags as labels, gold-only sharing, federation as migration path
- `sql/governance/01_managed_vs_external.sql` — DROP TABLE difference demo; why pipeline tables are managed
- `sql/governance/02_privilege_model.sql` — full GRANT/REVOKE/DENY with traversal (USE CATALOG → USE SCHEMA → SELECT); DENY overrides GRANT
- `sql/governance/03_column_masks.sql` — mask_string, mask_email (domain-preserved), mask_payment_token; birth_date explicitly excluded
- `sql/governance/04_row_filters.sql` — minor_row_filter(birth_date) on dim_account; quarantine_access_filter
- `sql/governance/05_tags_abac.sql` — tags by sensitivity/pii_category/regulatory/quality; ABAC via information_schema
- `sql/governance/06_lineage_audit.sql` — table/column lineage, audit log, query history, DBU cost, freshness SLO from system tables
- `sql/governance/07_delta_sharing.sql` — share creation, recipient lifecycle, token rotation, revoke path
- `sql/governance/08_federation.sql` — foreign catalog, traversal grants, federated join to silver, teardown

**Key decisions:**
- birth_date cannot be column-masked because the minor row filter reads it; masking it to '***' before the filter runs breaks the date comparison silently (silent access control bypass)
- Tags label columns/tables; UDFs enforce access — tags alone are not access control
- Delta Sharing is gold-only; bronze and silver are DENY-listed for external recipients

---

## T7 — Observability (feat/observability)

**Branch:** feat/observability  
**Status:** committed and pushed, PR pending gh auth (B3)

**Files:**
- `docs/adr/ADR-010-observability-approach.md` — 5 decisions: event log as primary signal, 120-min freshness SLO from conf/base.yml, DBU+throughput trends not wall clock (ADR-002), liquid clustering on fact_admission(game_id, gate), Spark UI query plan metrics
- `sql/monitoring/01_event_log_analytics.sql` — run summary, row counts per table (details JSON parsing), expectation violations, stage duration, quarantine reconciliation invariant (bronze = silver + quarantine)
- `sql/monitoring/02_freshness_slos.sql` — current status (OK/SLO_WARNING/SLO_BREACHED), alert query for Databricks SQL Alert, 30-day SLO history, row-count SLO, streaming lag check for sv_admission
- `sql/monitoring/03_run_history_trends.sql` — daily DBU from system.billing.usage, week-over-week trend with REGRESSED/STABLE/IMPROVED label, row throughput with 7-day rolling avg, cost per 1k rows, wall-time trend (triage signal only)
- `sql/monitoring/04_query_plan_analysis.sql` — bytes scanned, shuffle bytes, spill events with fix guidance, skew proxy via parallelism_ratio, file pruning ratio for liquid clustering, broadcast vs sort-merge join log
- `sql/monitoring/05_liquid_clustering.sql` — CLUSTER BY (game_id, gate) on fact_admission, predictive optimization TBLPROPERTIES on silver+gold, OPTIMIZE, file size distribution, VACUUM

**Key decisions:**
- Wall-clock seconds are not an SLO signal on serverless (queue wait + cold-start dominate); DBU consumption and row throughput are stable
- Liquid clustering on (game_id, gate) beats static partitioning: ~81 game_ids would create small-file partitions; gate as secondary filter can't be added without double-partitioning
- Existing files are not re-clustered until the first OPTIMIZE runs; must run OPTIMIZE once after enabling CLUSTER BY

**Blockers:**
- B3: gh auth login needed to open PRs for feat/governance, feat/observability, feat/silver-streaming, feat/generator-tests

---
