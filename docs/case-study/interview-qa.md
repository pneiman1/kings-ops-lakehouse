# Interview Q&A — Databricks Professional Exam Prep

Scenario-based questions mapped to exam domains. For each question, one answer is
correct and three distractors are plausible but wrong. The explanation covers why
each distractor fails — that is the exam's actual format.

---

## Domain 1 — Developing Code (22%)

### Q1 — CDC ordering

**Scenario:** A ticketing CDC pipeline uses `MERGE INTO` on `order_id`. The source
emits `__seq` (monotonic capture sequence) and `__commit_ts` (source commit time,
backdated on corrections). After a refund, the ticket reappears as ACTIVE in the
silver table. What is the most likely root cause?

**A.** The MERGE `WHEN MATCHED` clause is missing.
**B.** The pipeline is ordering events by `__commit_ts` instead of `__seq`.
**C.** The Auto Loader trigger interval is too long.
**D.** The refund event has a different `order_id` than the original order.

**Correct: B.**
The source backdates `__commit_ts` on corrections so a refund can carry a timestamp
earlier than the sale. When you pick "latest row" by `__commit_ts`, the sale sorts
after the refund and wins — the refunded order is resurrected.

*A:* A missing MATCHED clause would leave the row unchanged, not resurrect it.
*C:* Trigger interval controls when processing fires, not which row wins.
*D:* If the `order_id` differed, the MERGE would insert a new row (a different failure mode).

---

### Q2 — Streaming window binning

**Scenario:** A gate scan streaming pipeline assigns each scan to a 30-minute window
using `(scan_ts.cast("long") - gates_open_ts.cast("long")) / 1800).cast("int")`.
Pre-game scans arriving 10 minutes before gates open appear in window 0 instead of
being excluded. What is the root cause?

**A.** The watermark is set too short.
**B.** The window expression uses `cast("int")` which truncates toward zero, so
       −10 minutes gives window 0.
**C.** `scan_ts` is in a different timezone than `gates_open_ts`.
**D.** The pipeline is running in continuous mode instead of triggered mode.

**Correct: B.**
`(-10 / 30).cast("int")` = `(-0.33).cast("int")` = `0` because integer cast
truncates toward zero. `F.floor(-0.33)` = `-1`, which the downstream filter
correctly excludes.

*A:* Watermark controls late-data retention, not window assignment arithmetic.
*C:* A timezone mismatch would shift all windows uniformly, not cause sign confusion.
*D:* Trigger mode affects micro-batch frequency, not arithmetic correctness.

---

### Q3 — SCD Type 2 as-of join

**Scenario:** `dim_seat` is SCD Type 2. Section 118 was renumbered to 218 on
2027-05-15. An order placed 2027-04-01 for seat S-118 should resolve to section 118.
An order placed 2027-06-01 for the same seat should resolve to section 218. The join
condition is `seat_id = seat_id AND purchase_date BETWEEN effective_from AND effective_to`.
Orders placed on 2027-04-01 resolve to section 218 (wrong). What is most likely wrong?

**A.** The SCD merge used `>=` and `<=` instead of `>=` and `<` for the end boundary.
**B.** The wrong column (`__commit_ts` vs `purchased_ts`) is used as the event date.
**C.** The seat dimension only has the current version, not the historical version.
**D.** The join is an inner join, dropping orphan seats.

**Correct: C.**
If the SCD2 merge only keeps the current row (renumbered to 218) and discards the
pre-renumbering version, all orders — regardless of date — resolve to the current
section. The historical row must be retained with its `effective_to = 2027-05-15`.

*A:* Boundary convention affects single-day overlap edge cases, not all pre-renumbering orders.
*B:* Using `__commit_ts` vs `purchased_ts` would cause a systematic shift but not collapse
all history to the current version.
*D:* An inner join would drop orphans but wouldn't wrongly resolve them to 218.

---

## Domain 2 — Cost & Performance (13%)

### Q4 — Wall clock vs query plan

**Scenario:** A pipeline on a serverless warehouse ran in 45 seconds yesterday and
90 seconds today. Data volume is unchanged. A team member says the pipeline has
regressed and proposes adding a cluster size upgrade. What is the most accurate
diagnosis?

**A.** The pipeline has regressed. Doubling elapsed time with the same data is a
       clear signal of inefficiency.
**B.** Elapsed time on serverless is dominated by queue wait and cold start. Check
       bytes scanned and shuffle bytes from `system.query.history` before concluding.
**C.** The pipeline's watermark is too long, causing excessive state retention.
**D.** The warehouse size should be increased from 2X-Small to Small.

**Correct: B.**
Serverless has no cluster sizing knob and operates under a fair-usage quota. Queue
wait and cold start vary by time of day and concurrent workloads; they can easily
double elapsed time with no change in plan quality. The stable signals are bytes
scanned, files skipped, and shuffle bytes.

*A:* Same data + same plan = any elapsed-time difference is noise, not signal.
*C:* Watermark affects streaming state, not batch query elapsed time.
*D:* On Free Edition, there is one fixed warehouse size. Even on paid, upgrading
before measuring the plan is guessing, not engineering.

---

### Q5 — Liquid clustering vs partitioning

**Scenario:** `fact_admission` has 1.9M rows across 81 games. The most common queries
filter by `game_id` (81 distinct values) and `gate` (6 values). An engineer proposes
static partitioning by `(game_id, game_id)`. What is the best response?

**A.** Accept the proposal. Static partitioning on the primary filter column is always
       the right choice.
**B.** Partition by `game_id` only. Partitioning on `gate` as well is redundant.
**C.** Use liquid clustering on `(game_id, gate)`. Static partitioning on 81 × 6 = 486
       combinations produces small files; liquid clustering skips on either predicate
       without the small-file overhead.
**D.** Use Z-ORDER on `gate` after partitioning by `game_id`.

**Correct: C.**
81 games × 6 gates = 486 partition combinations. With 1.9M rows, each partition
averages ~4,000 rows — severe small-file territory. Liquid clustering skips files
on any combination of `game_id` and `gate` without pre-defined partition directories,
and re-clusters automatically on write.

*A:* Static partitioning is correct when partition cardinality is low and partition
size is large. Neither is true here.
*B:* Single-column partitioning on `game_id` doesn't help queries filtering by `gate` only.
*D:* Z-ORDER is the predecessor to liquid clustering and is now de-emphasized. It
still requires manual OPTIMIZE; liquid clustering applies automatically.

---

## Domain 4 — Monitoring & Alerting (10%)

### Q6 — Event log vs job run status

**Scenario:** A pipeline job reports SUCCESS. The silver table has 0 new rows. What
is the correct data source for diagnosing what happened?

**A.** The job run status — it shows SUCCESS so the pipeline ran correctly.
**B.** The pipeline event log (`delta.event_log`). Job run status only tells you
       the job exited without an error; the event log shows per-stage row counts
       and expectation outcomes.
**C.** `system.query.history` — it records every query executed by the warehouse.
**D.** `system.information_schema.tables.last_altered` — if it matches the job
       start time, rows were written.

**Correct: B.**
Job run status is binary: success or failure. A pipeline that wrote 0 rows and a
pipeline that wrote 50,000 rows both show SUCCESS if no stage threw an exception.
The event log records `num_output_rows` per stage per run.

*A:* This is the trap. SUCCESS ≠ correct output.
*C:* `system.query.history` records ad-hoc warehouse queries, not pipeline stage metrics.
*D:* `last_altered` changes when any write occurs; a write of 0 rows via `MERGE` with
no matches may not update it.

---

### Q7 — Freshness SLO

**Scenario:** `system.information_schema.tables.last_altered` for `gold.fact_admission`
shows a timestamp 150 minutes ago. The freshness SLA is 120 minutes. What status
should the monitoring query report?

**A.** `OK` — 150 minutes is within one standard deviation of the 120-minute threshold.
**B.** `SLO_WARNING` — 150 minutes is above 90 minutes (75% of 120) but below 120.
**C.** `SLO_BREACHED` — 150 minutes exceeds the 120-minute threshold.
**D.** Cannot determine — `last_altered` reflects DDL changes, not data writes.

**Correct: C.**
150 > 120 → SLO_BREACHED. The threshold is a hard bound, not a statistical tolerance.
The monitoring query in `sql/monitoring/02_freshness_slos.sql` uses `> threshold`
for BREACHED and `> threshold * 0.75` for WARNING.

*A:* SLOs are not statistical; there is no standard deviation tolerance.
*B:* 150 > 120 → BREACHED, not WARNING. WARNING applies to the 90–120 minute range.
*D:* In Databricks Unity Catalog, `last_altered` reflects the last data modification
to the table, not DDL-only changes.

---

## Domain 5 — Security & Compliance (10%)

### Q8 — Privilege traversal

**Scenario:** An analyst in `kings_analysts` runs `SELECT * FROM kings_prd.gold.fact_admission`
and gets `PERMISSION_DENIED`. They have been granted `SELECT` on `kings_prd.gold`. What
is missing?

**A.** They need `SELECT` on `kings_prd.gold.fact_admission` explicitly (table-level).
**B.** They need `USE CATALOG` on `kings_prd` and `USE SCHEMA` on `kings_prd.gold`.
**C.** The table is behind a row filter they don't satisfy.
**D.** `SELECT` on a schema grants access to views only, not tables.

**Correct: B.**
Unity Catalog requires explicit traversal grants: `USE CATALOG` to traverse into the
catalog, `USE SCHEMA` to traverse into the schema. Without these, the query fails even
if `SELECT` is granted at the schema level.

*A:* Schema-level `SELECT` does propagate to tables in UC; table-level explicit grant
is not required if schema-level is present.
*C:* A row filter violation returns rows, not a permission error.
*D:* Schema-level `SELECT` applies to both tables and views.

---

### Q9 — Column masking and row filter interaction

**Scenario:** `dim_account.birth_date` is masked to `'***'` for non-PII users. A row
filter `minor_row_filter(birth_date)` hides accounts where `birth_date > DATE '2009-04-01'`.
After applying the column mask, all minor accounts are visible to non-PII users. Why?

**A.** Column masks are applied after row filters, so the filter sees the real value.
**B.** Column masks are applied before row filters. `'***' > DATE '2009-04-01'` evaluates
       to NULL (not TRUE), so the filter condition returns TRUE for all rows.
**C.** The row filter function must be DEFINER RIGHTS to see the unmasked value.
**D.** Row filters and column masks cannot be applied to the same table.

**Correct: B.**
UC applies column transformations (masks) at query time before the row filter evaluates.
When the filter function receives `'***'` instead of a DATE, the comparison is undefined
and returns NULL, which is not filtered out — every row passes.

*A:* The order is reversed; masks apply first.
*C:* Definer rights affect whose permissions the function runs under, not the mask
application order.
*D:* UC supports both on the same table; the interaction (B) is the design concern.

---

## Domain 6 — Debugging & Deploying (10%)

### Q10 — Checkpoint recovery

**Scenario:** A streaming pipeline's checkpoint is corrupted and must be deleted. The
pipeline is restarted from scratch. What is the operational consequence?

**A.** The pipeline fails immediately because the state store is gone.
**B.** The pipeline reprocesses all source data from the beginning, potentially
       producing duplicate rows unless the output is idempotent or deduplicated.
**C.** The pipeline continues from the last committed offset stored in the Delta log.
**D.** The pipeline starts with an empty state but resumes from the last Kafka offset.

**Correct: B.**
The checkpoint stores the streaming query's progress: source offsets and stateful
operator state. Deleting it means the pipeline has no record of what it has processed.
Auto Loader will re-read all source files; without output idempotency (e.g., MERGE
on a unique key or `dropDuplicates` in the pipeline), rows will be duplicated.

*A:* The pipeline does not fail — it starts fresh, which is the problem.
*C:* The Delta log records table state, not streaming source offsets. These are different.
*D:* Auto Loader uses a checkpoint for file-offset tracking; there is no Kafka offset.

---

### Q11 — Schema change and checkpoint

**Scenario:** A streaming pipeline adds a new column `scan_device_os` to the source
schema mid-season. The pipeline has rescue mode enabled. What happens to the checkpoint?

**A.** The checkpoint is invalidated; the pipeline must restart from the beginning.
**B.** The new column is rescued into `_rescued_data` until the checkpoint is cleared.
**C.** With `schema_evolution_mode: rescue`, new columns are rescued (not silently
       dropped), and the checkpoint remains valid — no restart required.
**D.** The pipeline fails with a schema mismatch exception and stops.

**Correct: C.**
Rescue mode is designed for exactly this case. Unknown columns are written to
`_rescued_data` as JSON, allowing the pipeline to continue processing. The checkpoint
is unaffected because the streaming progress (source offsets) is independent of schema.
The column can be promoted to a first-class field in the next contract version.

*A:* Checkpoint invalidation happens when the query plan changes (e.g., adding a window
operation), not when new source columns appear in rescue mode.
*B:* Rescue handles new columns from the start; there is no deferral until checkpoint clear.
*D:* Failure mode applies to `failOnNewColumns`, not rescue mode.

---

## Domain 10 — Data Sharing & Federation (5%)

### Q12 — Delta Sharing vs Lakehouse Federation

**Scenario:** A partner club wants to query Kings ticketing data without copying it.
The Kings want to retain full governance control over what the partner can see.
Which approach is correct?

**A.** Lakehouse Federation — creates a foreign catalog in the partner's metastore
       that queries Kings data live.
**B.** Delta Sharing — creates a share, adds the gold tables, creates a recipient
       with a time-limited token. The partner queries via the Delta Sharing protocol.
**C.** COPY INTO from the Kings gold schema to the partner's S3 bucket.
**D.** Grant the partner's service principal SELECT on `kings_prd.gold`.

**Correct: B.**
Delta Sharing is designed for sharing data with external recipients outside the
metastore. The Kings control what is shared, to whom, and can revoke the token.
The partner queries via open protocol without touching the Kings workspace.

*A:* Lakehouse Federation is the reverse direction: it lets the Kings query a *partner's*
external database, not give the partner access to Kings data.
*C:* COPY INTO creates a copy that the Kings no longer control once transferred.
*D:* Granting a service principal direct access requires the partner to operate inside
the Kings Databricks workspace, which is a security boundary violation.

---

## Rapid-fire definitions (exam often tests these as one-liners)

| Term | Definition |
|---|---|
| Watermark | Threshold that tells Spark when to stop waiting for late events. Data older than `max_event_time - watermark_delay` is dropped from state. |
| State store | Per-task in-memory + checkpoint storage for stateful streaming operators (groupBy, dedup, joins). Evicted by watermark. |
| Liquid clustering | File-layout optimization that co-locates rows by clustering columns. Applied at write time; replaces Z-ORDER. |
| Predictive optimization | Databricks-managed service that runs OPTIMIZE and VACUUM automatically based on table access patterns. |
| `__seq` vs `__commit_ts` | `__seq` is a monotonic capture counter; `__commit_ts` is the source transaction time, which the source backdates on corrections. Order CDC by `__seq`. |
| Delta Sharing | Open protocol for sharing Delta tables with recipients outside the metastore. Share → recipient → token. |
| Lakehouse Federation | Querying external databases (Postgres, MySQL, Snowflake) via a foreign catalog in Unity Catalog without ingesting data. |
| Column mask | Function applied at query time to transform a column value based on the querying user's group membership. Applied before row filters. |
| Row filter | Function applied at query time to control which rows are visible. Receives column values after column masks are applied. |
| `USE CATALOG` / `USE SCHEMA` | Traversal grants required in UC before `SELECT` can be executed. Without them, `PERMISSION_DENIED` even with schema-level `SELECT`. |
| Parallelism ratio | `total_task_duration_ms / elapsed_ms`. Low ratio indicates tasks ran sequentially — a skew signal. |
| Spill | When a sort or hash join exceeds executor memory and serialises intermediate data to disk. 10–100× slower than in-memory. |
| Broadcast join | Join where the smaller side is replicated to every executor. Triggered by AQE when the smaller side is below `autoBroadcastJoinThreshold`. |
| `_rescued_data` | Column added by Auto Loader in rescue mode to capture fields not in the declared schema. |
| Accumulating snapshot | Fact table grain where one row per entity accumulates updates (e.g., `fact_deposit_conversion`). Contrast with transaction fact (one row per event). |
