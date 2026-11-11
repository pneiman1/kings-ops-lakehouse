# ADR-008 — Streaming watermark strategy for gate scan admission

**Status:** Accepted
**Context:** T4 — Silver streaming pipeline

---

## Context

Gate scan admissions are the only source that is genuinely streaming (gate_scans, ADR-006).
The pipeline must produce `sv_admission` — one row per validated scan — with two properties
that pull in opposite directions:

1. **Completeness**: every valid admission must be counted, including late-arriving ones from
   offline handhelds at the outfield gates.
2. **Bounded state**: the streaming state store cannot grow forever; it must be bounded so
   the pipeline recovers cleanly from restarts and does not exhaust memory.

Watermarks are the Structured Streaming mechanism that resolves this tradeoff. The watermark
declares how late an event can arrive and still be processed; events older than the watermark
are dropped, and state older than the watermark is evicted.

The gate scan stream has a property that makes the watermark choice non-obvious:

> **Lateness is correlated with gate, not random.**
> Offline handheld scanners at the outfield gates (gates G, H, J) lose connectivity and
> bulk-upload up to 48 hours after the scan. At those gates, every scan can be late. A tight
> watermark does not drop outliers — it drops an entire gate's data for an entire game.

The contract (`GATE_SCANS v1.1.0`) already enforces this: scans arriving more than 72 hours
after their source file are quarantined as `LATENESS_BEYOND_CONTRACT`.

---

## Options considered

### Option A — Tight watermark (5–15 minutes)

Watermark: `withWatermark("scan_ts", "15 minutes")`

- State evicts aggressively; tiny memory footprint.
- Acceptable for a uniform late-data distribution (random stragglers).
- **Wrong here**: at gates G, H, J, all handheld scans are late. The 15-minute watermark
  silently drops every outfield scan for a game, undercounting attendance there by 100%.
  This is not a data quality nuance — it is a systematic miscount by gate.

### Option B — Per-gate adaptive watermark

Custom logic: tight watermark for turnstile gates, loose for handheld gates.

- Theoretically correct.
- Structured Streaming does not support per-partition watermarks. Implementing this requires
  a custom `flatMapGroupsWithState` or `applyInPandasWithState` operator — significant
  complexity, no first-class Lakeflow support, and the custom state logic is itself a
  correctness risk.
- **Rejected**: the complexity does not pay for itself when a uniform 48h watermark is both
  correct and simple.

### Option C — 48-hour watermark (chosen)

Watermark: `withWatermark("scan_ts", "48 hours")`

- Matches the contract's lateness bound (72h with margin).
- State size: at the `small` scale (~23k scans/game), the state store holds ~46k events at
  peak (two games' worth of scan_ids). At `full` scale (~190k scans/game), ~380k events.
  Both are manageable for serverless compute.
- Watermark delay: `sv_admission` will be 48 hours behind the event-time frontier. For
  game operations, next-day analytics is acceptable; real-time dashboards would use
  `br_gate_scan` directly.
- Deduplication is correct: `dropDuplicates(["scan_id"])` with the 48h watermark evicts
  seen scan_ids after 48h of event time, which is sufficient since the same scan_id will
  not appear from two different games.

---

## Decision

**48-hour watermark on `scan_ts`.**

---

## Implementation details

### Trigger mode

Free Edition pipeline concurrency budget (ADR-003) means the streaming pipeline runs in
`availableNow` triggered mode (one micro-batch per run, then stops) rather than continuous
streaming. The watermark advances correctly in triggered mode: each micro-batch observes
the max `scan_ts` seen so far and advances the watermark accordingly.

For a production deployment without the Free Edition constraint, switching to continuous
(`trigger(processingTime="1 minute")`) requires no logic changes.

### Stateful deduplication

```python
(
    dp.read_stream("br_gate_scan")
    .withWatermark("scan_ts", "48 hours")
    .dropDuplicates(["scan_id"])           # stateful; evicts after watermark advance
)
```

`dropDuplicates` on a streaming DataFrame maintains a state store keyed by `scan_id`.
The watermark controls when entries are evicted. This is a bloom-filter-free exact set:
Spark maintains the full scan_id in state to guarantee no false deduplication.

Bronze already removes byte-identical duplicates (`dedupe_cdc` for CDC, file-level for
Auto Loader). The streaming dedup here handles the case where the same scan_id arrives
across two different files (device retry after a failed upload).

### Stream-static joins

Ticket orders (`sv_ticket_order`) and seat dimension (`dim_seat`) are batch-read in each
micro-batch:

```python
ticket_orders = spark.read.table(f"{SILVER_SCHEMA}.sv_ticket_order")
dim_seat       = spark.read.table(f"{SILVER_SCHEMA}.dim_seat")
```

Stream-stream joins would require watermarks on both sides, state on both sides, and
would delay output by the smaller watermark. Neither orders nor seats are streaming;
re-reading them each micro-batch picks up newly resolved orders without that overhead.

### Checkpoint recovery

Lakeflow manages the checkpoint at the pipeline's internal checkpoint location. On restart:

1. The committed Auto Loader offsets are read from the checkpoint — files already processed
   are not re-read (exactly-once file ingestion).
2. The watermark value is restored — late-data decisions resume from the same point.
3. The dedup state store is restored — scan_ids seen before the restart are still tracked.

A restart does not replay committed micro-batches. A crash mid-micro-batch replays that
batch from the last committed offset (at-least-once within the batch; the Delta sink's
transaction log makes this idempotent for the write).

### State store behavior between games

A game's scans arrive in a ~2-hour window. The watermark does not advance again until the
next batch of scans arrives. Between games (days apart), the watermark is effectively
paused. State from game N is evicted when game N+1's scans push event time forward past
(game N scan_ts max + 48h). For the small scale, this is 2–3 games' worth of scan_ids
in state at any one time — well within memory limits.

---

## Consequences

- `sv_admission` lags the event-time frontier by up to 48 hours. Real-time gate-count
  dashboards must read `br_gate_scan` directly and accept bronze-quality data.
- State store size scales with scans-per-game × 2 (two games' overlap). At full scale,
  budget ~380k × (scan_id string size ≈ 36 bytes) ≈ 14 MB — negligible.
- The 48h bound must be kept in sync with the contract. If the lateness contract tightens,
  the watermark should tighten correspondingly. ADR-008 is the cross-reference.

---

## Exam relevance

Databricks exam objectives test watermark configuration, state store behavior, trigger
modes, and checkpoint recovery. This ADR documents all four in a realistic context (burst
traffic, correlated lateness, non-trivial deduplication) rather than the textbook uniform-
distribution example.
