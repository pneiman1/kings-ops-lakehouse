# Case Study — Kings Ops Lakehouse

**Phil Neiman · NeimanAnalytics.com**
**November 2026**

---

## The business problem

The Las Vegas Kings play their first Major League season in 2027. Every incumbent
club prices, staffs, and forecasts off last season. The Kings have no last season.

That single constraint — no historical baseline — reshapes every engineering decision.
When a demand model fails, there is no prior-year benchmark to diagnose it against.
When a pipeline produces unexpected row counts, there is no "same game last year" to
compare. When a section gets renumbered mid-season, every historical order references
an ID that no longer exists.

The platform had to be right from day one, because day one is the only signal it
has to work from.

---

## What was built

A production-pattern data lakehouse on Databricks Free Edition (serverless) covering
the full lifecycle from raw source data through governed, queryable gold tables:

- **Bronze:** nine source systems, four ingestion patterns, quarantine routing that
  reconciles (bronze row count = silver + quarantine, always)
- **Silver:** CDC MERGE with ordering guarantees, SCD Type 2 on accounts and seats,
  streaming gate scan pipeline with correlated-lateness watermark
- **Gold:** star schema with five fact tables, six dimensions, materialized views,
  and a replay-safe cart allocation model
- **Governance:** Unity Catalog privilege model, column masking on PII, row filter
  for minors, ABAC tags, Delta Sharing, Lakehouse Federation
- **Observability:** pipeline event log analytics, freshness SLOs, DBU consumption
  trends, Spark UI signals, liquid clustering health

---

## The hardest engineering decisions

### 1. The CDC ordering trap

The ticketing CDC source emits two columns: `__seq` (monotonic capture sequence)
and `__commit_ts` (source commit timestamp, which the source **backdates** on
corrections). A refund can carry a `__commit_ts` earlier than the order it reverses.

If you order by `__commit_ts` to find "latest state," the backdated refund sorts
before its own order, the order wins the "latest row" race, and a refunded
ticket is resurrected as collected revenue. The generator injects this at a configured
rate so the failure shows up in the counts.

**The fix:** order by `__seq`, not `__commit_ts`. This took one line to fix and
one ADR to document, but catching it requires understanding *why* both columns exist.

### 2. Correlated lateness in the streaming watermark

Gate scan lateness is not random. Handheld scanners at the outfield gates bulk-upload
hours later when connectivity returns. This means lateness is **correlated with gate**:
the entire outfield section's data for a game can arrive 36 hours late.

A tight watermark (5–15 minutes, the default recommendation) would drop the entire
outfield gate's data for every game — not random stragglers but a structurally missing
zone. ADR-008 chose 48 hours to match the contract lateness bound. At peak state size,
this is roughly 46,000 events (one game's scans) held in state between games.

The key insight: the standard watermark tutorial assumes lateness is IID across events.
It is not when hardware fails in a spatially correlated way.

### 3. The birth_date masking paradox

Unity Catalog column masking is applied at query time before the row filter sees the
data. The minor row filter (`minor_row_filter(birth_date)`) needs to read `birth_date`
to determine whether the row should be visible.

If `birth_date` is masked to `'***'` before the filter runs, the date comparison
silently fails: `'***' > DATE '2009-04-01'` evaluates to NULL, which is not TRUE,
so the filter returns TRUE for all rows — every minor's record becomes visible to
anyone who can query the table.

**The decision (ADR-009 D4):** `birth_date` is excluded from column masking. The
privacy protection comes from the row filter alone. This is documented and tagged
so the next engineer who considers adding a mask to `birth_date` sees the trap.

### 4. Liquid clustering over static partitioning

`fact_admission` has ~1.9M rows across 81 games, with two common filter predicates:
`game_id` (attendance queries) and `gate` (ops queries). Static partitioning by
`game_id` produces ~81 partitions averaging 23k rows each — small-file territory.
Adding `gate` as a secondary partition squares the small-file problem.

Liquid clustering on `(game_id, gate)` lets Delta skip files on either predicate
without the small-file overhead. The clustering re-applies on write; no manual
OPTIMIZE + ZORDER schedule is needed.

### 5. Replay safety in the cart allocation model

The cart allocation pipeline recommends mobile concession cart positions by zone
for each 30-minute game window. The model must be evaluated on historical games
to measure how well it would have done.

A naive implementation computes the average demand across all games and applies it
to every game — including the game being evaluated. This is data leakage: game N's
actual demand influences game N's recommendation.

The fix uses a window function with `ROWS BETWEEN unboundedPreceding AND -1`:
for game N, only games 1..N-1 contribute to the demand average. The first game
gets a uniform fallback (1/6 per zone). The same code that runs historically is
the code that runs in production — no separate "evaluation mode."

---

## What the Free Edition constraint taught

Building on a single serverless warehouse with no cluster sizing forced a discipline
that paid clusters can paper over: measuring performance by query plan, not elapsed time.

Serverless queue wait and cold-start dominate raw seconds. A query that improved
from 45 seconds to 30 seconds might just have had a shorter queue. Bytes scanned,
files skipped, shuffle bytes, and spill are stable signals that actually reflect
plan quality. This is ADR-002, and it's also what the exam tests — not because
Databricks wants you to have Free Edition experience, but because understanding
*why* wall clock is misleading is the point.

---

## Metrics

| Layer | Tables | Tests |
|---|---|---|
| Bronze | 9 sources, 1 quarantine schema | Data contracts + schema evolution |
| Silver | 6 conformed tables | 36 unit tests (CDC, SCD2, admission) |
| Gold | 5 fact, 6 dim, materialized views, cart allocation | 17 unit tests |
| Governance | 8 SQL scripts | Manual review |
| Observability | 5 SQL scripts | Manual review |

**Total unit tests:** 53, all passing, all run without a cluster in under 90 seconds.

---

## What I would do differently

1. **Build the streaming pipeline earlier.** Streaming state behavior interacts with
   schema evolution in ways that only become clear when you have real data. Building
   it before the gold layer would have surfaced those interactions sooner.

2. **Write ADRs before the first line of code.** ADR-008 (watermark) and ADR-009
   (governance) were written alongside the code. Some decisions I would have made
   differently if I had written the trade-off table first.

3. **Use Lakebase for the ticketing OLTP.** The charter calls this out as a modern
   capability. Using Lakebase to emit real CDC would have made the ingestion pipeline
   more credible than file-based CDC simulation. This remains on the roadmap for T8.

---

## What's next

- **T8 — Document intelligence:** Sponsorship contract clause extraction with AI
  Functions; guest-services Vector Search index
- **T9 — Databricks App:** Pricing console over gold materialized views
- **Paid trial week:** Cluster configuration, Spark UI hands-on, cost comparison —
  the material that Free Edition cannot teach
- **Exam:** Databricks Certified Data Engineer Professional
