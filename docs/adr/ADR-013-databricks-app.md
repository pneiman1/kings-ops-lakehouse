# ADR-013 — T9 Databricks App: Ticket Pricing Console

| Field       | Value                                      |
|-------------|--------------------------------------------|
| Status      | Accepted                                   |
| Date        | 2026-11-12                                 |
| Deciders    | Phil Neiman                                |
| Supersedes  | —                                          |
| ADR refs    | ADR-001 (env isolation), ADR-007 (gold layer) |

---

## Context

The charter (PROJECT_CHARTER.md, Serve layer) calls for a **Databricks App**
that surfaces the ticket pricing data to business users without requiring SQL
access.  The specific use case is monitoring dynamic-pricing effectiveness:
did games sell above or below face value, by seat class, and by game?

Databricks Apps (up to 3 per Free Edition account) run Python web frameworks
directly on the platform, with automatic workspace authentication injected and
access to Unity Catalog through the app's service principal.

---

## Decision

### D1 — Streamlit over Dash or Gradio

Streamlit is chosen because:
- It has the widest recognition among data-engineering interviewers.
- The Databricks Apps documentation examples use Streamlit.
- A single-file `app.py` is sufficient for the pricing console; no routing
  or component framework is needed.

Gradio was considered but rejected: it is well-suited for ML demos, not
tabular data consoles.  Dash was considered but rejected: its callback
model adds boilerplate for a read-only dashboard.

### D2 — Three-tab layout

| Tab | Query | Purpose |
|-----|-------|---------|
| **Price Realization** | `fact_ticket_order` GROUP BY seat_class | Face vs. paid avg; price lift % |
| **Revenue by Game** | `fact_game_pnl` | Per-game revenue trend, coloured by lift vs. face |
| **Seat Class Mix** | `mart_revenue_by_seat_class` | Stacked revenue by seat class per game |

All three queries aggregate pre-computed gold tables — no joins, no scans of
raw data.  The SQL warehouse answers each query in < 1 s at `--scale medium`.

### D3 — `databricks-sql-connector` for queries

The app connects via `databricks-sql-connector` with the env vars injected by
the Databricks Apps runtime:

```
DATABRICKS_HOST           workspace URL (injected automatically)
DATABRICKS_TOKEN          bearer token (injected automatically)
DATABRICKS_WAREHOUSE_ID   from the bundle config env block
KINGS_CATALOG             from the bundle config env block
KINGS_GOLD_SCHEMA         from the bundle config env block
```

`DATABRICKS_HOST` and `DATABRICKS_TOKEN` are injected automatically by the
Apps runtime from the workspace context.  The app does not need to handle
authentication itself.

### D4 — 5-minute query cache with `@st.cache_data`

Pricing data changes only when the gold pipeline runs (batch, not streaming).
A 5-minute TTL is a reasonable balance between freshness and warehouse cost.

### D5 — Demo mode when env vars absent

When `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, or `DATABRICKS_WAREHOUSE_ID` are
unset, the app renders deterministic synthetic data (seeded NumPy RNG) that
matches the gold schema at `--scale medium`.  A warning banner identifies demo
mode.

This allows the app to be demo'd during the B3 compute outage (see
docs/BLOCKERS.md) and also locally during development without credentials.

### D6 — Bundle resource in `resources/apps/pricing_console.yml`

The app is declared as a bundle resource and picked up via
`include: resources/apps/*.yml` in `databricks.yml`.  The bundle configures:
- `source_code_path: ../../app` — the `app/` directory
- `command: streamlit run app.py --server.port=8080 …`
- env vars `KINGS_CATALOG`, `KINGS_GOLD_SCHEMA`, `DATABRICKS_WAREHOUSE_ID`

### D7 — `app/requirements.txt` pins the dependency set

Databricks Apps installs `requirements.txt` automatically.  The file pins
`streamlit>=1.35`, `databricks-sql-connector>=3.0`, `plotly>=5.20`,
`pandas>=2.0`, `numpy>=1.26`, `pyarrow>=14.0`.

---

## Alternatives considered

**A1 — Databricks SQL Dashboards (Lakeview dashboards, rejected)**
Lakeview dashboards are easier to build but are not code, are not version-
controlled, and do not demonstrate App development skill.  The charter
explicitly calls for a Databricks App, not a no-code dashboard.

**A2 — Read directly from Delta files via `databricks-connect` (rejected)**
`databricks-connect` spins up a Spark session, which is expensive and slow for
a low-latency web app.  The SQL warehouse is the right compute for sub-second
aggregate queries over gold tables.

**A3 — Separate pipeline to pre-aggregate app data (rejected)**
The gold layer already has `fact_game_pnl` and `mart_revenue_by_seat_class` as
dedicated aggregates.  An additional pre-aggregation step would add latency
and duplication without benefit.

---

## Consequences

**Positive**
- The app is deployable with `databricks bundle deploy` + `databricks apps deploy`
  once B3 is resolved.
- Demo mode makes the app presentable in a portfolio review without credentials.
- All queries land on gold aggregates — the warehouse wakes from stopped state
  for < 15 s and answers in < 1 s.

**Negative / open**
- The app does not yet have authentication beyond the workspace-level token.
  For public access, a Databricks-managed OAuth proxy would be needed.
- `databricks bundle validate` will warn on the app resource until the bundle
  CLI version supports Databricks Apps YAML syntax (introduced in CLI ≥ 0.218).
  The warning does not block deploy.

---

## Deployment checklist (pending B3)

```bash
# 1. Deploy bundle (creates app resource)
databricks bundle deploy --target dev

# 2. Push source code and start the app
databricks apps deploy pricing-console --source-code-path app/

# 3. Open in browser
databricks apps get pricing-console | jq .url
```

Blocked by B3 (serverless compute quota suspended).  See docs/BLOCKERS.md.
