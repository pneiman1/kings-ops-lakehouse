# Kings Ops Lakehouse

**Phil Neiman · NeimanAnalytics.com**

A production-pattern data lakehouse built on Databricks for the fictional **Las Vegas Kings** inaugural MLB season (2027). One dataset — ballpark operations — as the substrate for seven build modules covering ten exam domains.

**Primary goal:** Databricks Certified Data Engineer Professional certification portfolio.
**Secondary goal:** a reusable reference for medallion architecture, streaming, and UC governance patterns.

---

## Why this project is different

Most Databricks portfolio projects are "ingest a CSV, run a notebook." This one is not.

| What most portfolios do | What this one does |
|---|---|
| Notebook-per-transform | Pure-function transform layer (`src/kings_ops/transforms/`) — unit-tested without a cluster |
| Happy-path data | Injected defects that match real production failures |
| One environment | Bundle targets across dev/stg/prd; CI gate that can fail |
| Z-ORDER everywhere | Liquid clustering with a written rationale for why |
| Ignore streaming | 48-hour watermark design for correlated lateness — the reported Professional exam failure point |
| Ignore governance | Column masking, row filters, ABAC tags, Delta Sharing, Lakehouse Federation |
| Skip observability | Event log analytics, freshness SLOs, system-table cost attribution, Spark UI signals |

The deliberate data defects are realistic for an inaugural season — every system was three months old, staff were learning scanners, and sections 118–124 were renumbered in May:

- Duplicate CDC events with identical `__seq`
- Refunds landing before the orders they reverse (CDC ordering trap)
- Mid-season section renumbering producing orphan seat IDs
- Gate scans backdated up to 48 hours from offline handhelds, clustered by gate
- POS terminals dropping batches then double-submitting them
- Two terminals configured with the wrong tax rate for three weeks
- Malformed JSON (~0.2% of scans, ~0.1% of POS lines)

---

## Architecture

```
Source Systems → Landing Volume → Bronze (append-only) → Silver (conformed) → Gold (star schema)
                                                                               ↓
                                                              Observability / Governance / Serving
```

**Nine source systems, four ingestion patterns:**

| Source | Format | Technique |
|---|---|---|
| Gate scans | JSON stream | Auto Loader + Structured Streaming, 40× burst |
| Ticketing CDC | Parquet | MERGE ordered by `__seq`, not `__commit_ts` |
| Concession POS | JSON batches | Late batches, line → basket grain |
| CRM accounts | CSV snapshot | SCD Type 2, heavy PII |
| Deposit funnel | CSV event-driven | Expansion-only funnel, priority ranking |
| Game schedule | JSON | Conformed game dimension |
| Pricing feed | CSV intraday | Effective-dated ranges, overlapping validity |
| Sponsorship | Text | AI Functions clause extraction |
| Guest services | Parquet free text | AI classification, Vector Search |

Full architecture diagram: `docs/architecture/PROJECT_CHARTER.md`.

---

## Exam coverage

**Target:** Databricks Certified Data Engineer Professional (May 2026 syllabus)

| Domain | Weight | Where covered in this repo |
|---|---|---|
| Developing Code (Python + SQL) | 22% | `src/kings_ops/transforms/` — pure functions, unit-tested |
| Cost & Performance | 13% | `sql/monitoring/`, liquid clustering, ADR-002, ADR-010 |
| Data Transformation & Quality | 10% | `src/kings_ops/transforms/`, `@dp.expect` pipeline expectations |
| Monitoring & Alerting | 10% | `sql/monitoring/01–05`, freshness SLOs, system tables |
| Data Security & Compliance | 10% | `sql/governance/01–08`, column masks, row filters, ABAC |
| Debugging & Deploying | 10% | `databricks.yml`, GitHub Actions, `docs/adr/` |
| Data Ingestion & Acquisition | 7% | `pipelines/bronze/`, data contracts, schema evolution |
| Data Governance | 7% | `sql/governance/`, ADR-009, Unity Catalog privilege model |
| Data Modelling | 6% | Gold star schema, SCD2, grain statements in PROJECT_CHARTER |
| Data Sharing & Federation | 5% | `sql/governance/07_delta_sharing.sql`, `08_federation.sql` |

---

## Key engineering decisions

Each decision is documented in an ADR with the rejected alternatives.

| ADR | Decision | Why it matters |
|---|---|---|
| ADR-001 | Dev/stg/prd as catalogs, not workspaces | Free Edition has one workspace; stated migration path avoids treating a constraint as a choice |
| ADR-002 | Performance = bytes scanned, not wall clock | Serverless queue wait dominates elapsed time; raw seconds mislead |
| ADR-006 | Auto Loader for streaming, COPY INTO for bounded reference | Explicit method selection with the tradeoff table |
| ADR-007 | Gold materialized as MV not plain table | Incrementally maintained, stored for fast reads — the right choice for high-read low-change marts |
| ADR-008 | 48-hour watermark on gate scans | Lateness is **correlated with gate** (offline handhelds bulk-upload); a tight watermark drops an entire gate's data, not random late stragglers |
| ADR-009 | `birth_date` excluded from column masking | Minor row filter reads `birth_date`; masking it before the filter silently bypasses the access control |
| ADR-010 | DBU + row throughput as SLO signal, not wall clock | Restatement of ADR-002 for observability: a pipeline that takes the same time but burns 2× DBUs has regressed |
| ADR-011 | Replay-safe cart allocation, no lookahead | Window function `ROWS BETWEEN unboundedPreceding AND -1` ensures game N's allocation uses only games 1..N-1 |

---

## Repository layout

```
kings-ops-lakehouse/
├── README.md
├── databricks.yml                     # bundle definition, all targets
├── pyproject.toml                     # Python project + ruff + pytest config
├── conf/
│   ├── base.yml                       # environment-independent settings
│   ├── dev.yml  stg.yml  prd.yml      # target overrides
├── src/kings_ops/
│   ├── contracts/                     # data contracts per source (breaking-change policy)
│   ├── transforms/                    # PURE functions — the unit-tested core
│   │   ├── cdc.py                     # CDC resolution ordered by __seq
│   │   ├── scd2.py                    # SCD Type 2 merge + dim_seat renumbering
│   │   ├── gold.py                    # star schema builds (fact, dim, PnL)
│   │   ├── admission.py               # gate scan enrichment + quarantine routing
│   │   └── cart_allocation.py         # replay-safe cart allocation baseline_v1
│   └── quality/  observability/  io/
├── pipelines/
│   ├── bronze/                        # Auto Loader + schema evolution
│   ├── silver/                        # CDC MERGE, SCD2, streaming admission
│   └── gold/                          # star schema, cart allocation
├── sql/
│   ├── governance/                    # 01–08: privilege model, masks, filters, sharing
│   ├── monitoring/                    # 01–05: event log, SLOs, trends, query plan, clustering
│   └── marts/                         # revenue mart four ways (table/view/MV/streaming)
├── tests/
│   └── unit/                          # 50+ tests, all run without a cluster
├── tools/
│   └── generate_source_data.py        # synthetic data with injected defects
├── docs/
│   ├── adr/                           # ADR-001 through ADR-011
│   ├── architecture/PROJECT_CHARTER.md
│   ├── modules/                       # M1 platform, M2 ingestion concept briefs
│   └── case-study/                    # written case study + interview Q&A
└── .github/workflows/                 # CI: validate, lint, test on PR
```

---

## The pure-function transform pattern

Every business transformation is a function that takes a DataFrame and returns a DataFrame. No `SparkSession` construction, no reads, no writes inside the transform.

```python
# src/kings_ops/transforms/cdc.py — the whole correctness story in one function
def resolve_current_state(df: DataFrame, business_key: str = "order_id") -> DataFrame:
    """Current state = highest-__seq row. Never order by __commit_ts.

    The source backdates __commit_ts on corrections, so a refund can carry
    a timestamp earlier than the order it reverses. Order by __seq instead.
    """
    deduped = dedupe_cdc(df, (business_key, SEQ_COL))
    latest_first = Window.partitionBy(business_key).orderBy(F.col(SEQ_COL).desc())
    ...
```

Notebooks and pipeline files are thin wrappers. This is what makes the codebase unit-testable without a cluster, and it's the thing most portfolio repos get wrong.

---

## Running the tests

```bash
python -m venv .venv && .venv/bin/pip install -e ".[dev]"
pytest tests/unit/ -v
```

Tests marked `@pytest.mark.spark` start a local SparkSession. No cluster, no Databricks connection.

```
tests/unit/test_cdc.py          12 tests   CDC resolution, refund resurrection, dedupe
tests/unit/test_scd2.py         8 tests    SCD2 merge, seat renumbering, effective dates
tests/unit/test_gold.py         3 tests    section-as-of-purchase, collected revenue, dim_date
tests/unit/test_admission.py    16 tests   stream-static join, quarantine routing, SCD2 seat
tests/unit/test_cart_allocation.py 14 tests  window binning, replay no-lookahead, alcohol cutoff
```

---

## Deploying

```bash
# Validate bundle config
databricks bundle validate -t dev

# Deploy to dev
databricks bundle deploy -t dev

# Run the gold pipeline
databricks bundle run gold_pipeline -t dev
```

Environment variables are injected via bundle targets — no hardcoded workspace URLs.

---

## Free Edition constraints

This project was built on Databricks Free Edition (serverless only). The constraints that shape architectural decisions:

| Constraint | Architectural response |
|---|---|
| One workspace, one metastore | Dev/stg/prd as catalogs (ADR-001) |
| No cluster sizing | Performance measured by query plan, not wall clock (ADR-002) |
| No cluster startup visibility | Deployment troubleshooting from logs, not SSH |
| Serverless is always Photon | No Photon on/off comparison available |
| One 2X-Small SQL warehouse | Query plan metrics via `system.query.history` |

**Migration path to paid:** swap the `serverless_compute_id` in `databricks.yml` for a `job_cluster` spec. All bundle targets parameterize the compute type; nothing else changes.

---

## ADR trail

| # | Decision |
|---|---|
| ADR-001 | Environment isolation by catalog, not workspace |
| ADR-002 | Performance measured by query plan, not wall clock |
| ADR-003 | Pipeline concurrency budget (1 active pipeline per type) |
| ADR-004 | Vector Search via Delta Sync index |
| ADR-006 | Ingestion method selection matrix |
| ADR-007 | Gold materialization strategies (table/view/MV/streaming) |
| ADR-008 | 48-hour streaming watermark for correlated gate lateness |
| ADR-009 | Unity Catalog governance model (masks, filters, ABAC) |
| ADR-010 | Observability approach (event log, SLOs, system tables) |
| ADR-011 | Cart allocation baseline_v1 (replay-safe, no lookahead) |

---

## Background

**Phil Neiman** is a data engineer and analytics leader transitioning from a Power BI / Azure background into the Databricks ecosystem. This project was built as both certification preparation and a portfolio demonstration of production engineering patterns.

Associate certification: passed May 2026 (Troubleshooting/Optimization: 100%).
Target: Databricks Certified Data Engineer Professional.

[NeimanAnalytics.com](https://neimananalytics.com) · [LinkedIn](https://linkedin.com/in/philneiman)
