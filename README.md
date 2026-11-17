# Kings Ops Lakehouse

A production-pattern Databricks lakehouse for the fictional **Las Vegas Kings** MLB franchise (2027 inaugural season). Built to demonstrate end-to-end data engineering on Databricks Free Edition: Unity Catalog, Lakeflow Declarative Pipelines, AI Functions, Databricks Apps, and the full bronze → silver → gold medallion stack.

**Workspace:** `https://dbc-e2b6da17-c52e.cloud.databricks.com`
**GitHub:** `https://github.com/pneiman1/kings-ops-lakehouse`

---

## What's here

| Layer | What it does |
|-------|-------------|
| **Bronze** | Auto Loader (streaming) ingestion of 7 source types: gate scans (JSON), ticketing CDC (Parquet), POS (JSON), CRM accounts (CSV snapshot), seat manifest, guest services tickets (Parquet), sponsorship contracts (JSON). Quarantine routing, rescue columns, audit stamps, versioned data contracts. |
| **Silver** | CDC resolution by `__seq` (never `__commit_ts`), SCD Type 2 for seats and accounts, mid-season section renumbering handled via as-of join. |
| **Gold** | Star schema (`fact_ticket_order`, `fact_game_pnl`, `mart_revenue_by_seat_class`, `dim_date`), cart allocation recommendations (`reco_cart_allocation`), AI-enriched document intelligence (`sv_sponsorship_clause_enriched`, `sv_guest_ticket_enriched`). |
| **Serve** | Streamlit pricing console (Databricks App) over gold aggregates. |

### Key features demonstrated

- **Lakeflow Declarative Pipelines** — thin Python wrappers over pure, locally-testable transforms
- **Unity Catalog** — catalog-per-environment isolation (`kings_dev` / `kings_stg` / `kings_prd`), column masking, row filters, lineage
- **Databricks AI Functions** (`ai_classify`, `ai_extract`) with keyword/regex fallback for clause extraction and guest issue classification
- **Databricks Apps** — Streamlit pricing console wired to gold via `databricks-sql-connector`
- **Data contracts** — versioned, enforced schema at the bronze boundary; contract version stamped onto every row
- **SCD Type 2** — section renumbering event splits affected seats into closed + current versions
- **CDC ordering** — `__seq`-based resolution prevents backdated refunds from resurrecting as live orders
- **110 unit tests** — full transform layer tested locally with no cluster (PySpark local mode)
- **Databricks Asset Bundles** — `databricks.yml` + `resources/` for all jobs, pipelines, schemas, and apps

---

## Repo layout

```
kings-ops-lakehouse/
├── app/                          # T9 Databricks App (Streamlit pricing console)
│   ├── app.py
│   └── requirements.txt
├── conf/                         # Environment config (base + dev/stg/prd overrides)
├── databricks.yml                # Bundle root — catalog isolation, variable defaults
├── docs/
│   ├── adr/                      # Architecture Decision Records (ADR-006, 007, 011, 012, 013)
│   ├── architecture/PROJECT_CHARTER.md
│   └── BLOCKERS.md               # Live blocker log with exact error strings
├── pipelines/
│   ├── bronze/bronze_streaming.py
│   ├── silver/silver_batch.py
│   └── gold/                     # gold_marts.py, cart_allocation_pipeline.py, doc_intelligence.py
├── resources/                    # Bundle resource YAML (jobs, pipelines, schemas, apps)
├── scripts/dbxctl.py             # Cluster/warehouse cost checker
├── sql/                          # DDL, smoke tests, revenue-mart four-ways comparison
├── src/kings_ops/
│   ├── contracts/definitions.py  # 7 versioned data contracts
│   └── transforms/               # cart_allocation, cdc, document_intelligence, gold, scd2
├── tests/unit/                   # 110 tests, all passing locally
└── tools/generate_source_data.py # Synthetic data generator (--scale small/medium/large)
```

---

## Getting started

### Clone and set up

```bash
git clone https://github.com/pneiman1/kings-ops-lakehouse.git
cd kings-ops-lakehouse
python3 -m venv .venv
source .venv/bin/activate          # macOS / Linux
# .venv\Scripts\activate           # Windows
pip install -e ".[dev]"
```

### Authenticate to Databricks

```bash
databricks auth login              # opens browser — works on Mac terminal
```

Workspace: `https://dbc-e2b6da17-c52e.cloud.databricks.com`

### Run the unit tests (no cluster needed)

```bash
pytest tests/unit/
# 110 passed
```

### Deploy and run

```bash
# Generate synthetic data and upload to the landing volume
python tools/generate_source_data.py --scale medium --out /tmp/kings_data

# Deploy the bundle
databricks bundle deploy --target dev

# Run the bootstrap job (loads data, runs pipelines end to end)
databricks bundle run bootstrap --target dev

# Check compute cost
python scripts/dbxctl.py cost
```

> **B3 — currently blocked:** Triggering new runs for this Free Edition organisation is temporarily suspended by Databricks. Auth and deploy succeed; execution is blocked. See `docs/BLOCKERS.md` for the exact error strings and status.

---

## Branches and PRs

| Branch | PR | What |
|--------|----|------|
| `feat/cart-allocation` | #2 | ADR-011 rewrite — 11-zone ring layout, section-to-zone map, switching penalty |
| `feat/generator-tests` | #3 | Synthetic data generator tests |
| `feat/gold-star-schema` | #4 | Gold star schema transforms and pipeline |
| `feat/governance` | #5 | Unity Catalog tags, column masks, row filters |
| `feat/observability` | #6 | Event log analytics, freshness SLOs, alerting |
| `feat/ops-kit` | #7 | `dbxctl.py` cost/status CLI |
| `feat/silver-cdc-scd2` | #8 | Silver CDC + SCD2 (ticketing, seats, accounts) |
| `feat/silver-streaming` | #9 | Silver streaming pipeline |
| `feat/doc-intelligence` | #10 | T8 — document intelligence (AI Functions + fallback) |
| `feat/databricks-app` | #11 | T9 — Databricks App pricing console |

---

## Architecture decisions

| ADR | Decision |
|-----|----------|
| ADR-006 | Auto Loader (streaming) for all bronze sources — exactly-once file tracking |
| ADR-007 | Gold materialisation: materialized views for aggregates, plain tables for `dim_date` |
| ADR-011 | Cart allocation — 11-zone concourse ring, section-to-zone map, 10 min/zone switching cost |
| ADR-012 | Document intelligence — pure transform layer + AI Functions pipeline wrapper, PII drop at gold |
| ADR-013 | Databricks App — Streamlit over `databricks-sql-connector`, demo mode when env vars absent |

---

## Data model highlights

### CDC ordering (silver)
The ticketing source backdates `__commit_ts` on corrections. Ordering by it resurrects refunded orders. The pipeline orders by `__seq` exclusively — the only reliable monotonic key. This is the most important invariant in the silver layer.

### SCD Type 2 — section renumbering
Sections 118–124 were renumbered to 218–224 mid-season. `dim_seat` splits each affected seat into a closed version (old section) and a current version (new section). `fact_ticket_order` resolves seat section **as of purchase date** via an as-of join, so early-season orders still reference section 118 and late-season orders reference 218.

### Cart allocation (ADR-011)
11 concourse zones arranged in two rings with wraparound adjacency. Zone demand is estimated from prior-game POS transactions. Switching cost is 10 min/zone, capped at 2 moves per 30-min window. See `src/kings_ops/transforms/cart_allocation.py` and `docs/adr/ADR-011-cart-allocation.md`.

### Document intelligence (ADR-012)
Sponsorship contracts are split into individual clause sentences, classified with `ai_classify` (6 clause types) and quantity-extracted with `ai_extract`. Guest complaint tickets are classified into 7 issue categories. Both pipelines fall back to keyword/regex matching when AI Functions return NULL. Raw `issue_text` (PII-bearing) is dropped before writing to gold.

---

## Windows notes

The repo was built on Windows. The `.claude/hooks/validate.py` post-edit hook auto-detects the venv path (`.venv/Scripts/python.exe` on Windows, `.venv/bin/python` on macOS/Linux). No manual config needed.
