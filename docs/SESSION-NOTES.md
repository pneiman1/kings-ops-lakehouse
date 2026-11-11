# Session Notes

Appended per tranche as work lands.

---

## T4 — Silver streaming (feat/silver-streaming)

**Branch:** feat/silver-streaming
**Status:** committed and pushed, PR pending gh auth (B3)

**Files:**
- `docs/adr/ADR-008-streaming-watermark-strategy.md` — 48-hour watermark (gate lateness is gate-correlated, not random; tight watermark drops a whole gate's data)
- `src/kings_ops/transforms/admission.py` — pure transforms: enrich_scan_with_ticket (stream-static left join on order_id), enrich_scan_with_seat (SCD2 as-of join on seat_id + scan date), build_sv_admission, quarantine_reason
- `pipelines/silver/silver_streaming.py` — Lakeflow pipeline with watermark + dropDuplicates + stream-static joins
- `tests/unit/test_admission.py` — 16 tests, all pass

**Key decisions:**
- 48-hour watermark: outfield handheld scanners batch-upload hours late; lateness is correlated with gate, not random. A 5-15 min tight watermark would drop an entire gate's worth of data for a game.
- Watermark and dropDuplicates live in the pipeline wrapper (needs streaming context); enrichment logic is pure in transforms/

**Gold test false alarm:**
- test_gold.py appeared to fail (exit 1, no output). Root cause: Spark stage-progress carriage-return sequences in output confuse the tool's output capture. Running with `-s 2>/dev/null` confirmed 3 passed. Not a real failure.

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
- `sql/monitoring/03_run_history_trends.sql` — daily DBU from system.billing.usage, week-over-week trend with REGRESSED/STABLE/IMPROVED label, row throughput with 7-day rolling avg, cost per 1k rows
- `sql/monitoring/04_query_plan_analysis.sql` — bytes scanned, shuffle bytes, spill events with fix guidance, skew proxy via parallelism_ratio, file pruning ratio for liquid clustering, broadcast vs sort-merge join log
- `sql/monitoring/05_liquid_clustering.sql` — CLUSTER BY (game_id, gate) on fact_admission, predictive optimization TBLPROPERTIES on silver+gold, OPTIMIZE, file size distribution, VACUUM

**Key decisions:**
- Wall-clock seconds are not an SLO signal on serverless (queue wait + cold-start dominate); DBU consumption and row throughput are stable
- Liquid clustering on (game_id, gate) beats static partitioning: ~81 game_ids would create small-file partitions; gate as secondary filter can't be added without double-partitioning
- Existing files are not re-clustered until the first OPTIMIZE runs; must run OPTIMIZE once after enabling CLUSTER BY

---

## Cart allocation — baseline_v1 (feat/cart-allocation)

**Branch:** feat/cart-allocation
**Status:** committed and pushed, PR pending gh auth (B3)

**Files:**
- `docs/adr/ADR-011-cart-allocation.md` — 8 decisions: batch not streaming, 30-min windows, proportional baseline (demand_index × n_carts), replay-safe no-lookahead, 7th-inning alcohol cutoff at window 8 (165 min from first pitch), adjacency as metadata, portal mapping as metadata, 12 carts configurable
- `src/kings_ops/transforms/cart_allocation.py` — pure transforms: compute_demand (window binning using F.floor, not cast), add_demand_index, replay_prior_demand (window ROWS BETWEEN unboundedPreceding AND -1), allocate_carts, build_reco_cart_allocation
- `pipelines/gold/cart_allocation_pipeline.py` — Lakeflow wrapper writing gold.reco_cart_allocation
- `tests/unit/test_cart_allocation.py` — 14 tests, all pass

**Key decisions:**
- Batch not streaming: carts are physical objects; real-time reallocation within a window adds no operational value
- 30-min windows: shorter windows have more noise than repositioning time; longer windows miss intra-game surge
- Replay-safe: window function ROWS BETWEEN unboundedPreceding AND -1 so game N sees only games 1..N-1
- First game: prior_demand_index is NULL → coalesce to 1/6 (uniform across 6 zones)
- Alcohol cutoff: floor((90+165)/30)=8; window 8+ → 0 carts for alcohol zones; TEAM_STORE unaffected

**Bug found during development:**
- `(minutes_from_open / 30).cast("int")` truncates toward zero: -10 min → window 0 (wrong).
  Fixed to `F.floor(minutes_from_open / 30).cast("int")` which floors to -1 (correctly excluded).

**Blockers:**
- B3: gh auth login needed to open PRs. All branches pushed; use the GitHub URL printed by git push.
- Brief referenced as "the brief above" was not visible in this session. Built from: POS schema, summary hints (12 carts, 30-min windows, 7th-inning cutoff, baseline first, no lookahead), and ADR-002 performance guidance.
