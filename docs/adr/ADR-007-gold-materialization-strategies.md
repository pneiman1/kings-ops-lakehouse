# ADR-007 — Gold materialization strategies

**Status:** Accepted
**Context:** Gold — star schema and business marts (T5)

## Context

A gold object can be physicalized four ways on Databricks, and the exam — and any
architect interview — tests choosing between them, not reciting that they exist:

| Object | Storage | Refresh | Reads a stream? |
|---|---|---|---|
| **Table** (managed Delta, CTAS/overwrite) | Yes | Full recompute, when you run it | No |
| **View** | None | On every read (recomputed each query) | No |
| **Materialized view** | Yes | Incremental, declarative (pipeline-managed) | No |
| **Streaming table** | Yes | Append / stateful, continuous or triggered | Yes |

Using one for everything is simpler to describe and wrong in practice: a view over
the 2.4M-row ticket fact reruns the whole star on every dashboard load; a streaming
table over a static calendar is pure overhead.

The same mart — `mart_revenue_by_seat_class` (collected revenue by game and seat
class) — is built all four ways under `sql/marts/revenue_four_ways/` so the contrast
is concrete and runnable. This ADR is the decision rule behind which one each real
gold object actually uses.

## Decision

Choose by two questions, in order:

> **1. Does the input arrive as an unbounded stream?** → **Streaming table.**
> **2. If not, is the result queried far more often than its inputs change?**
> &nbsp;&nbsp;&nbsp;&nbsp;Yes → **Materialized view.** No / rarely queried → **View.**
> &nbsp;&nbsp;&nbsp;&nbsp;One-shot, point-in-time, or handed to an external consumer → **Table.**

Applied to this build:

| Gold object | Grain | Choice | Why |
|---|---|---|---|
| `fact_ticket_order` | per order, current state | **Materialized view** | Rebuilt from silver on a schedule; queried constantly by every mart and the App. Incremental refresh beats recompute-on-read at 2.4M rows. |
| `fact_game_pnl` | per game | **Materialized view** | Aggregate of the fact; cheap to maintain incrementally, read on every P&L view. |
| `dim_date` | per calendar date | **Table** | Static. Generated once; it never changes, so there is nothing to refresh and nothing to stream. A view would recompute `sequence()` on every join. |
| `fact_admission` | per validated scan | **Streaming table** (T4) | Its input (gate scans) is an unbounded, burst-shaped stream. This is the one source that *must* stream. |
| `mart_revenue_by_seat_class` | per game × seat class | **Materialized view** (prod) | Dashboard-facing aggregate; the four-way folder shows the alternatives and why they lose here. |

## Alternatives considered

**Everything a view.** Rejected. Zero storage and always-fresh, but every read
recomputes the full lineage back to silver. At the ticket fact's volume a single
dashboard refresh becomes a full star rebuild. Views are correct only for thin
projections or rarely-queried objects.

**Everything a table (scheduled CTAS).** Rejected as the default. Tables are fine but
dumb: a full overwrite recomputes rows that did not change, and freshness is only as
good as the last job. Materialized views get incremental maintenance and lineage-aware
refresh for free. Tables stay the right call for static data (`dim_date`) and for
point-in-time snapshots handed to an external system.

**Everything a streaming table.** Rejected. Streaming semantics over bounded or static
inputs add checkpoints, state, and a standing pipeline slot (ADR-003) for no benefit.
Streaming tables earn their complexity only when the input is genuinely unbounded —
here, exclusively the gate-scan sources.

## Consequences

- The production star is materialized views, with `dim_date` as a table and the
  admission fact deferred to the streaming pipeline (T4). This matches the two-question
  rule rather than a blanket choice.
- `sql/marts/revenue_four_ways/` is kept as living documentation: four runnable
  implementations of one mart. It is illustrative, not deployed by the bundle — the
  gold pipeline deploys the materialized-view form.
- Materialized views on Free Edition refresh within the single-pipeline budget
  (ADR-003); they are scheduled with the batch silver pipeline, not continuously.

## Exam relevance

The syllabus tests selecting a table type (streaming table, materialized view, view,
managed table) from freshness, latency, cost, and input-boundedness requirements. The
two-question rule and the table above are that objective, applied to a real star.
