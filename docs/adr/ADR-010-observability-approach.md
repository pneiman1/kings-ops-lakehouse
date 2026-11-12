# ADR-010 — Observability approach

**Status:** Accepted
**Context:** T7 — Observability layer

---

## Context

The Kings platform needs to know three things about every pipeline run:
1. Did it finish, and did it finish correctly? (event log)
2. Is the data fresh enough for the downstream consumers? (freshness SLO)
3. Is it getting slower, more expensive, or more skewed over time? (run-history trend)

An additional constraint from ADR-002: **performance is measured by query plan, not wall
clock**. The serverless warehouse has no cluster sizing knob and operates under a fair-usage
quota; raw elapsed time is dominated by queue wait and cold-start, not by plan quality.
The right signals are bytes scanned, files pruned, partitions skipped, shuffle bytes,
and spill — all readable from the query plan in the Spark UI or from system tables.

---

## Decisions

### D1 — Pipeline event log as the primary operational signal

Lakeflow pipelines write an event log to a Delta table at a path controlled by the pipeline
definition. The event log records each stage's start, end, duration, row counts, and
expectations outcome. It is the authoritative source for "did the pipeline produce what it
was supposed to?" at the row level.

The alternative (relying on job run status alone) only tells you whether the job exited
successfully — not whether the row counts were within expected ranges, which expectations
fired, or which table was slowest. The event log gives all of that.

### D2 — Freshness SLOs are defined in conf/base.yml and enforced by SQL

`freshness_sla_minutes: 120` in `conf/base.yml` is the agreed threshold. The SLO query
(see `sql/monitoring/02_freshness_slos.sql`) computes the minutes since last write for
each silver and gold table from `system.information_schema.tables` and compares to this
threshold. No additional tooling is required; the query is the check.

Alerting on SLO breach is out of scope for Free Edition (requires Databricks SQL Alerts on
a warehouse). The check is written as a query; wiring it to an alert is a one-line config
change in the SQL warehouse UI.

### D3 — Run-history trends measured by DBU consumption and row throughput, not seconds

For the reasons in ADR-002: wall-clock duration on serverless is not stable enough to
detect genuine regressions. DBU consumption (from `system.billing.usage`) and row
throughput (rows processed per micro-batch from the event log) are the stable signals.
A pipeline that takes the same elapsed time but consumes 2× DBUs has regressed.

### D4 — Liquid clustering on fact_admission; predictive optimization on gold facts

`fact_admission` is the highest-cardinality fact table and the primary target for
game-day attendance queries. Liquid clustering on `(game_id, gate)` lets Delta skip files
when filtering by game and gate — the two most common predicates. This is better than
static partitioning by game because:
- game_id has ~81 distinct values at full scale; a partition per game is small-file territory
- gate is a secondary filter that static partitioning cannot help without double-partitioning
- Liquid clustering re-clusters automatically on write; no manual OPTIMIZE schedule needed

Predictive optimization (`ALTER TABLE ... SET TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')`)
ensures small-file compaction runs automatically on the gold and silver tables. Combined
with liquid clustering on fact_admission, this keeps scan performance stable without
requiring manual OPTIMIZE + ZORDER after every pipeline run.

### D5 — Spark UI skew and spill are read from query plan metrics, not from logs

The exam tests identifying skew (one task taking 10× longer than the median), shuffle
spill (memory spill to disk during a sort or hash join), and sort-merge vs broadcast join
choice. These signals appear in:
- The Spark UI task list (per-task duration, spill metrics)
- `system.query.history.metrics_json` (programmatic access)
- The DAG view (which stage is the bottleneck)

Guidance on what to look for and how to fix each is in
`sql/monitoring/04_query_plan_analysis.sql`.

---

## Consequences

- The event log path must be set in the pipeline definition and consistent across
  dev/stg/prd targets. It is controlled by `kings.event_log_path` in the bundle config.
- Freshness SLO checking requires the `system` catalog to be enabled in the workspace
  (same requirement as T6 lineage queries).
- Liquid clustering on `fact_admission` applies only after the first OPTIMIZE run;
  newly written files are not yet clustered until the background OPTIMIZE fires or
  `OPTIMIZE table FULL` is run manually once.
- Predictive optimization is a Databricks-managed feature; it is not available in all
  regions and defaults to off. Enabling it via TBLPROPERTIES is a one-time operation.

---

## Exam relevance

The May 2026 syllabus tests: event log analytics, freshness SLOs, liquid clustering,
predictive optimization, Spark UI metrics (skew, shuffle, spill), and cost attribution
via system tables. All five areas are covered by the scripts in `sql/monitoring/`.
