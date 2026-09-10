# M2 — Data Ingestion and Loading

**Exam section:** Data Ingestion and Loading
**Build order position:** 2nd (after M5, which already exists)
**Estimated time:** 2 weeks

---

## 1. Concept brief

Read this before you open a single file of code.

### The problem ingestion tooling exists to solve

You have files landing in object storage. You want them in a table. The naive version is one line:

```python
spark.read.json("/Volumes/.../gate_scans").write.saveAsTable("bronze.scans")
```

That works exactly once. Then reality arrives:

- **Run it again and you double your data.** There is no memory of what was already loaded.
- **A new file lands mid-read** and you get a partial batch with no way to know it was partial.
- **The source adds a field** and either your job crashes or the field is silently dropped, and you cannot tell which happened.
- **A malformed record appears** and the entire job dies over one bad line out of two million.
- **A million files accumulate** and listing the directory takes longer than processing it.

Every ingestion feature you're about to learn exists to solve one of those five. That's the whole subject. When you can name which problem a feature solves, you understand it; when you can only recall its syntax, you don't.

### Auto Loader

Auto Loader is a streaming source that **remembers which files it has already processed**. That memory lives in a checkpoint. It gives you:

- **Exactly-once file processing** — the double-load problem, solved
- **Two discovery modes** — directory listing (simple, fine to millions of files) and file notification (cloud event queues, for very high file counts)
- **Schema inference and evolution** — with configurable behavior when the shape changes
- **Rescued data column** — a place for fields that don't match the schema, so nothing is silently lost

Two paths on disk, and confusing them is the single most common Auto Loader mistake:

| Path | Purpose |
|---|---|
| `cloudFiles.schemaLocation` | Tracks the inferred and rescued schema across restarts |
| Streaming checkpoint | Tracks which files have been processed and stream progress |

They serve different purposes. Deleting the schema location re-infers; deleting the checkpoint reprocesses everything.

### COPY INTO

`COPY INTO` is idempotent batch SQL. It tracks loaded files in the **target table's metadata** rather than in a checkpoint you manage. Re-running it does not duplicate rows.

**It is not "the batch version of Auto Loader."** The real distinction:

> Does this source produce files **continuously and unboundedly**?
> Yes → Auto Loader. It's a bounded, occasional drop → COPY INTO.

Running a streaming pipeline for a seat manifest that changes twice a season means managing a checkpoint and a schema location forever, to save nothing. Simpler wins.

### Lakeflow Connect

The managed connector layer. Two flavors:

- **Standard connectors** — you configure the ingestion, Databricks runs it
- **Managed connectors** — fully managed for specific SaaS and database sources (Salesforce, Workday, SQL Server, and similar)

You reach for Connect when the source is a *system* rather than *files* and a connector already exists. Building a bespoke JDBC extract when a managed connector exists is work you do not need to do — and the exam tests exactly this prioritization judgment.

### Schema evolution is a policy decision, not a setting

This is the most important idea in the module, and it's the one most people get wrong.

When a source sends a column you didn't expect, there are two defensible responses, and **which is correct depends entirely on what the unexpected column means**:

| Source | Unexpected column means | Correct response |
|---|---|---|
| Gate scans | Scanner firmware shipped an update | **Rescue** — absorb it, keep running |
| Ticketing CDC | The operational schema changed underneath us | **Fail** — stop before writing wrong data |

Same mechanism, opposite policies, both correct. A new field on a scan is a non-event; failing the stream over it would be an outage you caused yourself. A new column in CDC means someone changed the source database, and continuing writes subtly wrong data into silver — where subtly wrong is worse than stopped, because stopped is visible.

If you can articulate that distinction in an interview, you sound like someone who has run pipelines. If you say "I enable schema evolution," you sound like someone who has read about them.

### Bronze is not clean, on purpose

Bronze is a faithful record of what the source **actually sent**, plus audit columns. Cleaning in bronze destroys the ability to answer "what did they actually send us?" when silver and the source disagree — and that question comes up in every ingestion incident.

The test: **reprocessing from bronze must reproduce silver exactly.** That's only true if bronze is unmodified.

Concrete example from this build: two POS terminals were commissioned with a zero tax rate for three weeks. Those rows are *real* — the club really did fail to collect that tax. Bronze keeps them faithfully. Correcting them in bronze would erase evidence of an operational error that finance needs to know about. The anomaly gets surfaced as a metric in M3, not patched in M2.

### Quarantine vs. dropping

`expect_or_drop` satisfies the pipeline and silently loses data. That is the exact outcome contracts exist to prevent.

The pattern used here: one streaming view tags each row valid or invalid, then two tables read it with opposite predicates. Rows never vanish — they land in bronze or quarantine, and the counts reconcile to the source. Quarantine carries a reason column, because a dead-letter bin nobody can triage is just a slower way of dropping data.

---

## 2. Exam objectives covered

- ✅ Enable and detail ingestion patterns: batch, streaming, incremental
- ✅ Use COPY INTO to incrementally load files from cloud object storage into UC-governed tables
- ✅ Use Auto Loader with schema enforcement and schema evolution to land data into UC-governed tables
- ✅ Prioritize between Auto Loader, Lakeflow Connect, partner connectors, and other methods based on volume, frequency, data types, and governance needs
- ✅ Ingest semi-structured and nested data (JSON) into UC-governed Delta tables
- ◐ Configure Lakeflow Connect for enterprise sources — *conceptual only; Free Edition connector availability is limited*
- ◐ Use JDBC/ODBC or REST clients in notebooks — *covered in M4 orchestration*

---

## 3. What you build

| File | What it does |
|---|---|
| `src/kings_ops/contracts/definitions.py` | Data contracts — schema, keys, PII, evolution policy per source |
| `tests/unit/test_contracts.py` | 20 tests, no cluster required |
| `pipelines/bronze/bronze_streaming.py` | Auto Loader for scans, CDC, POS + quarantine routing |
| `pipelines/bronze/copy_into_reference.sql` | COPY INTO for seat manifest and pricing |
| `resources/pipelines/bronze.yml` | Pipeline as a bundle resource |
| `docs/adr/ADR-006-ingestion-method-selection.md` | The decision matrix |

### Deploy and run

```bash
pytest tests/unit -q                              # 45 tests, all green
databricks bundle validate --target dev
databricks bundle deploy --target dev
databricks bundle run bronze_ingestion --target dev
```

Then in the workspace: **Jobs & Pipelines** → open the pipeline → watch the DAG build. Click any table to see row counts and expectation results.

---

## 4. Break-it lab

Do all four. Each takes about ten minutes and teaches more than reading the working version.

### Lab 4.1 — Watch the two evolution policies diverge

Land a scan file with a field the contract has never seen:

```bash
cat > /tmp/drift.json <<'EOF'
{"scan_id":"SCN-DRIFT-001","game_id":"G2027001","order_id":"ORD-000000001","seat_id":"S000001","gate":"HOME_PLATE","scan_ts":"2027-04-05T18:35:00+00:00","device_id":"DEV-001","device_type":"TURNSTILE","scan_result":"ACCEPTED","ticket_medium":"MOBILE","biometric_hash":"abc123"}
EOF
databricks fs cp /tmp/drift.json dbfs:/Volumes/kings_dev/landing/files/gate_scans/dt=2027-04-05/hh=18/region=DRIFT/part-drift.json
databricks bundle run bronze_ingestion --target dev
```

**Then query:**
```sql
SELECT scan_id, _rescued_data FROM kings_dev.bronze.br_gate_scan WHERE _rescued_data IS NOT NULL;
```

**Expect:** the row loaded, with `biometric_hash` captured in `_rescued_data`. The pipeline did not fail.

Now do the equivalent to the CDC source — add any column to a parquet batch file and re-run.

**Expect:** the pipeline **fails**. Read the error. That is `failOnNewColumns` doing its job.

**The lesson:** same mechanism, opposite outcomes, both correct. Be able to say why.

### Lab 4.2 — Delete the schema location and watch what happens

```bash
databricks fs rm -r dbfs:/Volumes/kings_dev/ops/checkpoints/dev/_schema/gate_scans
databricks bundle run bronze_ingestion --target dev
```

**Expect:** the stream re-infers the schema. Note it does **not** reprocess data — that's the checkpoint's job, not the schema location's.

**The lesson:** these two paths are not the same thing. Most people conflate them and then cannot explain a reprocessing incident.

### Lab 4.3 — Prove quarantine reconciles

```sql
SELECT
  (SELECT COUNT(*) FROM kings_dev.bronze.br_gate_scan)  AS accepted,
  (SELECT COUNT(*) FROM kings_dev.quarantine.q_gate_scan) AS quarantined,
  (SELECT COUNT(*) FROM kings_dev.bronze.br_gate_scan)
  + (SELECT COUNT(*) FROM kings_dev.quarantine.q_gate_scan) AS total;
```

Compare `total` against `gate_scans` in `_manifest.json`, minus the malformed-JSON records (those fail at parse and appear in `_rescued_data`, not as rows).

**Then:**
```sql
SELECT _quarantine_reason, COUNT(*) FROM kings_dev.quarantine.q_gate_scan GROUP BY 1 ORDER BY 2 DESC;
```

**The lesson:** "the load succeeded" means no errors occurred. It does not mean the data is complete. A job that loads zero rows also produces no errors.

### Lab 4.4 — Break COPY INTO idempotency

Run `copy_into_reference.sql` twice in a row.

**Expect:** the second run loads zero rows. Then check the uniqueness assertion at the bottom still passes.

Now copy `seats.csv` to a second filename in the same directory and re-run.

**Expect:** duplicate `seat_id` values, and the `RAISE_ERROR` fires.

**The lesson:** COPY INTO's idempotency is per *file*, not per *row*. It will happily load the same content twice from two different filenames — which is exactly how the double-submitted POS batches would corrupt a naive load.

---

## 5. Self-check

Answers and explanations at the bottom. Do not scroll.

**Q1.** A pipeline ingests JSON from cloud storage. The source occasionally adds new optional fields. The team wants ingestion to continue uninterrupted while retaining any unexpected data for later review. Which configuration?

A. `cloudFiles.schemaEvolutionMode = failOnNewColumns`
B. `cloudFiles.schemaEvolutionMode = rescue` with `rescuedDataColumn` set
C. `cloudFiles.schemaEvolutionMode = none`
D. `cloudFiles.inferColumnTypes = true`

**Q2.** A data engineer must incrementally load a small reference CSV that is replaced roughly monthly. The job must be safe to re-run. Which is most appropriate?

A. Auto Loader with a checkpoint and schema location
B. `COPY INTO` with `FILEFORMAT = CSV`
C. `spark.read.csv(...).write.mode("append")`
D. `spark.read.csv(...).write.mode("overwrite")`

**Q3.** What is the difference between `cloudFiles.schemaLocation` and the streaming checkpoint?

A. They're the same; `schemaLocation` is an alias
B. `schemaLocation` tracks schema across restarts; the checkpoint tracks processed files and stream progress
C. `schemaLocation` is for Delta only; the checkpoint is for all formats
D. `schemaLocation` stores quarantined rows

**Q4.** A CDC feed from an operational database lands as Parquet. The team wants ingestion to stop immediately if the upstream schema changes. Which mode?

A. `rescue`
B. `addNewColumns`
C. `failOnNewColumns`
D. `none`

**Q5.** Which best describes bronze layer responsibility?

A. Clean and deduplicate so silver can join freely
B. Store source data faithfully with audit metadata, unmodified
C. Apply business rules and conform to the enterprise model
D. Serve BI queries directly

**Q6.** A source drops 40,000 small JSON files per hour into cloud storage. Directory listing has become the bottleneck. What should you change?

A. Increase `maxFilesPerTrigger`
B. Switch to file notification mode
C. Switch to `COPY INTO`
D. Enable `mergeSchema`

**Q7.** `COPY INTO` runs twice against the same directory. What happens on the second run?

A. Rows are duplicated
B. The command errors
C. Zero rows load — already-ingested files are tracked in target table metadata
D. Rows are overwritten

**Q8.** Which factor most strongly indicates Lakeflow Connect over Auto Loader?

A. The data is JSON
B. The source is a SaaS application or operational database rather than files in storage
C. The volume exceeds 1 TB
D. The target is a Unity Catalog managed table

---

## 6. Interview drill

Not exam questions. These are what a hiring manager actually asks, and what a strong answer contains.

### "Walk me through your ingestion layer."

**Weak:** "I used Auto Loader to read JSON into bronze Delta tables with schema evolution enabled."

**Strong:** Lead with the *decision*, not the tool. "Six sources with different characteristics, so different ingestion methods. Continuous unbounded file sources — scans and POS — use Auto Loader for exactly-once file tracking. Small stable reference data uses COPY INTO, because managing a checkpoint for a file that changes twice a season isn't worth the operational cost. The interesting part is schema evolution: scans rescue, CDC fails hard. A new field on a scan means firmware shipped; a new column in CDC means someone changed the source database. Failing the stream on the first would be a self-inflicted outage; continuing on the second writes subtly wrong data into silver."

### "How do you handle bad records?"

They're checking whether you drop data. "Quarantine, not drop. One view tags validity, two tables read it with opposite predicates, so counts reconcile to the source. Quarantine carries a reason column so it's triageable — a dead-letter table nobody can query is a slower way of dropping data. The one hard failure is a null CDC sequence number: an unorderable change event isn't recoverable by any downstream logic, so failing loudly beats quarantining quietly."

### "Your POS data has rows with a zero tax rate. Why didn't you fix that?"

The trap is agreeing it's a bug to fix in ingestion. "Those rows are accurate — two terminals were commissioned wrong and the club really didn't collect that tax. Bronze records what the source sent. Correcting it there would erase evidence of an operational error finance needs. It's surfaced as a metric in the gold layer and reported. Bronze's contract is fidelity; correction belongs where it's visible and auditable."

### "How would you add a seventh source?"

Testing whether the architecture generalizes. "Contract definition, a config entry in the source registry, and a pipeline function. No new bespoke notebook. If adding a source requires new plumbing rather than new metadata, the ingestion layer is wrong."

---

## Answers

**Q1 — B.** Rescue captures unexpected fields in `_rescued_data` while the stream continues. **A** fails the stream, which is the opposite of the requirement. **C** discards silently, losing the data they explicitly want retained. **D** is about type inference, unrelated to unexpected columns.

**Q2 — B.** COPY INTO is idempotent, tracks loaded files in target metadata, and needs no checkpoint. **A** works but is operational overkill for monthly reference data. **C** duplicates on every re-run. **D** is not incremental and destroys history.

**Q3 — B.** Distinct paths, distinct purposes. Deleting the schema location re-infers; deleting the checkpoint reprocesses everything. Conflating them is the most common Auto Loader mistake.

**Q4 — C.** `failOnNewColumns` stops on schema change. **A** and **B** both continue, which is exactly what you don't want when an operational schema shifts underneath a CDC feed. **D** ignores silently — worst of all.

**Q5 — B.** Bronze is faithful and append-only with audit metadata. **A** describes silver, **C** describes silver-to-gold, **D** describes gold. The test: reprocessing from bronze must reproduce silver exactly.

**Q6 — B.** File notification mode uses cloud event queues instead of listing the directory, which is the designed answer to high file counts. **A** changes batch size, not discovery cost. **C** makes it worse — COPY INTO also lists. **D** is unrelated.

**Q7 — C.** Idempotency is the defining property. Note the nuance from Lab 4.4: it's per *file*, so identical content under a new filename WILL load again.

**Q8 — B.** Connect exists for systems rather than files. Format, volume, and target type don't drive that choice.

**Scoring:** 7–8 solid. 5–6 re-read the concept brief. Below 5, redo the labs before moving to M3 — this is the heaviest ingestion section on the exam.
