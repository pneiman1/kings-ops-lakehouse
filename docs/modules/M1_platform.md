# M1 — Databricks Intelligence Platform

**Exam section:** Databricks Intelligence Platform
**Position:** First. Everything else assumes this.
**Estimated time:** 3–4 days

---

## 1. Concept brief

No code in this section. Read it before you touch the workspace.

### 1.1 What problem the lakehouse solves

For about fifteen years, serious analytics organizations ran two systems:

- A **data warehouse** — fast SQL, ACID transactions, governance, schema enforcement. Expensive, proprietary storage, bad at unstructured data, hostile to ML.
- A **data lake** — cheap object storage, any format, great for ML. No transactions, no schema enforcement, no governance. Reliably became a swamp.

You paid the **two-system tax**: data copied between them, two governance models, two security models, two sets of pipelines, and a permanent argument about which number is right.

The lakehouse claim is that you can have warehouse guarantees *directly on* lake storage. Cheap object storage stays the substrate; a transaction layer on top provides ACID, schema enforcement, and time travel; a governance layer on top of that provides one permission model for everything.

Two technologies make that claim true, and they're the two things you must understand cold:

- **Delta Lake** provides the transactional guarantees.
- **Unity Catalog** provides the governance.

Everything else on the platform is scaffolding around those two.

### 1.2 Control plane and compute plane

This split confuses more people than any other part of the architecture, and it's tested.

**Control plane** — Databricks-managed. The web UI, notebooks, job scheduler, cluster manager, query history, and metadata. Databricks runs this.

**Compute plane** — where your data is actually processed. Two flavors:

| | Classic compute plane | Serverless compute plane |
|---|---|---|
| Runs in | Your cloud account | Databricks' account |
| You control | VMs, networking, instance types | Nothing — it just runs |
| Startup | Minutes | Seconds |
| Cost model | DBUs + your cloud VM bill | One all-in rate |

**Where does the data live?** In cloud object storage — S3, ADLS, GCS. Not in the control plane. Databricks reads it, processes it, writes it back. This is why "moving to Databricks" doesn't mean moving your data.

**Free Edition:** serverless only. You cannot provision classic compute. That's an ADR in this repo, and it's also a study gap — you'll have to learn cluster configuration from documentation rather than by doing it.

### 1.3 Delta Lake — what it actually is

Strip away the marketing. A Delta table is:

```
my_table/
├── part-00000-....snappy.parquet     ← ordinary Parquet data files
├── part-00001-....snappy.parquet
└── _delta_log/
    ├── 00000000000000000000.json     ← commit 0
    ├── 00000000000000000001.json     ← commit 1
    ├── 00000000000000000002.json
    └── 00000000000000000010.checkpoint.parquet
```

**That's it.** Parquet files plus a transaction log. Delta is not a file format — it's Parquet plus a protocol for describing which Parquet files constitute the table right now.

Everything follows from that one design:

**ACID transactions.** A commit is writing the next numbered JSON file. Either that file exists or it doesn't — atomicity for free. Concurrent writers use optimistic concurrency: both prepare, both try to write commit *N*, one wins, the loser retries against the new state.

**Time travel.** Version 5 of the table is "replay log entries 0 through 5." The old Parquet files are still sitting there, unreferenced by the current version but not deleted. `VERSION AS OF` and `TIMESTAMP AS OF` replay to a point.

**Why VACUUM breaks time travel.** `VACUUM` deletes data files no longer referenced by recent versions — default retention 7 days. After vacuuming, you cannot time travel past the retention window, because the files that version pointed to are gone. People discover this during an incident. Don't be them.

**File skipping.** The log stores min/max statistics per file for the first 32 columns by default. A query filtering `WHERE game_id = 'G2027042'` reads the log, sees which files could possibly contain that value, and skips the rest without opening them. This is why file layout and clustering matter — the subject of M6.

**Schema enforcement.** Writes that don't match the schema are rejected at commit. This is the property a plain data lake lacks, and the reason lakes became swamps.

> **Mental model worth keeping:** the transaction log is the table. The Parquet files are just storage.

### 1.4 Unity Catalog — what it actually is

Before UC, permissions lived per workspace. Ten workspaces meant ten permission models and no way to answer "who can see customer PII?" across the organization.

Unity Catalog is one governance layer above all workspaces. The namespace is **three levels**:

```
catalog . schema . table
kings_dev . bronze . br_gate_scan
```

Above catalog sits the **metastore** — one per region, attached to workspaces. Free Edition gives you exactly one metastore and one workspace, which is why environments in this project are catalogs rather than workspaces.

**Securables** are the things you grant on: catalog, schema, table, view, volume, function, model.

**Privileges inherit downward.** `GRANT SELECT ON SCHEMA` covers current and future tables in it. But inheritance isn't sufficient by itself — to read a table you also need `USE CATALOG` on its catalog and `USE SCHEMA` on its schema. Forgetting the traversal privileges is the most common UC permission error, and it appears on the exam.

**Managed vs. external tables** — this distinction matters enormously and it's tested:

| | Managed | External |
|---|---|---|
| Storage location | UC decides | You specify with `LOCATION` |
| `DROP TABLE` | **Deletes the data** | Removes metadata only; data survives |
| Optimization | Predictive optimization available | Limited |
| Use when | Default. Almost always. | Data is shared with external tools, or you can't move it |

**Volumes** govern non-tabular files — the landing zone in this project. Same permission model as tables, applied to a directory. Before volumes, file access sat outside governance entirely, which was the gap that made "governed lakehouse" only partly true.

### 1.5 Compute — the part with a cost model

Four things you can run code on, and picking correctly is a tested objective:

| Type | Purpose | Lifecycle | Relative cost |
|---|---|---|---|
| **All-purpose compute** | Interactive notebook work, shared by people | You start and stop it; idles | Highest DBU rate |
| **Job compute** | Scheduled jobs | Created for the run, terminates after | Lower DBU rate |
| **SQL warehouse** | SQL and BI queries | Auto-start, auto-stop | Varies by class |
| **Serverless** | Any of the above, Databricks-managed | Instant, no config | All-in rate |

**The single most common cost mistake** in real organizations: running scheduled production jobs on all-purpose compute. It's more expensive per DBU *and* it idles between runs. Job compute exists precisely for this, and it's cheaper on both axes.

**DBU** — Databricks Unit, a normalized measure of processing capability per hour. On classic compute your bill is DBUs *plus* the cloud VM cost, billed by your cloud provider. On serverless it's a single all-in rate. This is why serverless can look more expensive per DBU while being cheaper in total: the VM bill is already inside it.

**SQL warehouse classes:** classic, pro, serverless. Serverless starts in seconds and scales automatically; classic can take minutes. For interactive BI, startup latency dominates the experience.

**Free Edition reality:** serverless only, one SQL warehouse limited to `2X-Small`, five concurrent job tasks, one active pipeline per type. You'll learn the selection *criteria* here but can't exercise them — plan a paid-trial week before the exam.

### 1.6 The medallion architecture, briefly

Three layers, each with one job:

- **Bronze** — what the source actually sent, plus audit columns. Append-only. Not clean, on purpose.
- **Silver** — conformed, deduplicated, validated, joinable entities.
- **Gold** — business-facing aggregates and the star schema. What analysts query.

**The test that keeps you honest:** reprocessing from bronze must reproduce silver and gold exactly. That's only true if bronze is unmodified — which is why bronze doesn't clean anything.

Why layers at all? Because the alternative is a single transformation from raw to reporting, and when a number is wrong you have no intermediate state to inspect. The layers are debuggability, not ceremony.

---

## 2. Exam objectives covered

- ✅ Understand core components of the platform: architecture, Delta Lake, Unity Catalog
- ✅ Understand compute services — characteristics, limitations, cost models — and select the most suitable option per workload
- ✅ Describe the three medallion layers and each layer's purpose *(also assessed in the transformation section)*

---

## 3. Hands-on

Run `notebooks/M1_platform_explorer.sql` in a SQL editor or notebook, cell by cell. It's built to make the abstractions above physically visible — you'll look at an actual `_delta_log`, watch a version get created, and time travel to an earlier state.

Do not read it and nod. Run it.

---

## 4. Break-it lab

### Lab 4.1 — Prove the log is the table

Create a table, insert twice, then look at `DESCRIBE HISTORY`. Note that two inserts produced two versions. Now `SELECT ... VERSION AS OF 1` and confirm you see the pre-second-insert state.

**The lesson:** nothing was deleted or rewritten. A new version is a new log entry pointing at a different set of files.

### Lab 4.2 — Managed vs. external, the destructive difference

Create one managed table and one external table over a Volume path. Put a row in each. Drop both. Then check whether the underlying files still exist.

**Expect:** the managed table's data is gone. The external table's files are still on disk.

**The lesson:** `DROP TABLE` means something different depending on table type. This is exactly the kind of thing people learn once, in production, on a Friday.

### Lab 4.3 — Break time travel on purpose

Create a table, make several versions, then run `VACUUM` with a zero-hour retention (you'll have to disable the safety check). Then try to time travel back.

**Expect:** it fails — the files that version referenced are gone.

**The lesson:** time travel is not a backup. It's a consequence of not having deleted files yet.

### Lab 4.4 — Find the traversal privilege trap

Grant a group `SELECT` on a table but *not* `USE CATALOG` and `USE SCHEMA`. Confirm they still can't read it.

**The lesson:** privileges inherit downward, but you need traversal rights at every level above. The most common UC permission error, and a likely exam question.

---

## 5. Self-check

**Q1.** What physically constitutes a Delta table?

A. A proprietary binary format optimized for Databricks
B. Parquet data files plus a `_delta_log` transaction log
C. ORC files with an external Hive metastore entry
D. A managed Postgres table with columnar indexes

**Q2.** A team runs a nightly ETL job on all-purpose compute. What is the primary problem?

A. All-purpose compute cannot run scheduled jobs
B. All-purpose has a higher DBU rate and idles between runs; job compute is cheaper on both counts
C. All-purpose does not support Delta Lake
D. All-purpose cannot access Unity Catalog

**Q3.** A user has `SELECT` on `sales.orders` but gets a permission error. What is most likely missing?

A. `MODIFY` on the table
B. `USE CATALOG` on the catalog and `USE SCHEMA` on the schema
C. `OWNER` on the table
D. Workspace admin rights

**Q4.** What happens to underlying data files when you `DROP` an **external** table?

A. Deleted immediately
B. Deleted after the retention period
C. Retained — only metadata is removed
D. Moved to a trash directory

**Q5.** A team vacuumed a table with the default retention and now cannot query a version from three weeks ago. Why?

A. Time travel is limited to 7 days by license
B. VACUUM removed data files that version referenced
C. The transaction log was truncated
D. Time travel requires the Pro tier

**Q6.** Where is customer data processed in the **classic** compute plane?

A. In Databricks' cloud account
B. In the customer's own cloud account
C. In the control plane
D. On the local driver

**Q7.** Which best describes the bronze layer's responsibility?

A. Cleaned, deduplicated, joinable entities
B. A faithful record of what the source sent, plus audit metadata
C. Business aggregates for BI
D. A staging area deleted after each run

**Q8.** Why does Delta enable file skipping?

A. Parquet files are sorted by default
B. The transaction log stores per-file min/max statistics
C. Delta indexes every column
D. Photon caches all files in memory

---

## 6. Interview drill

### "Explain the lakehouse to someone who runs a Snowflake warehouse."

Don't list features. Name the tax: two systems, two governance models, two copies of the data, and a permanent argument about which number is right. The lakehouse claim is warehouse guarantees directly on lake storage — Delta for transactions, Unity Catalog for governance, one copy. Then be honest about the tradeoff: a mature warehouse is still simpler to operate if all your data is structured and all your workloads are SQL. The lakehouse wins when ML and unstructured data are in scope.

### "What actually is a Delta table?"

They're testing whether you know or whether you've only used it. "Parquet files plus a transaction log. The log is the table — it's an ordered list of commits describing which files are live. ACID comes from atomic log writes, time travel comes from replaying to a version, file skipping comes from per-file statistics in the log. Once you see it as a log over immutable files, VACUUM breaking time travel stops being surprising."

### "How would you decide between serverless and classic compute?"

"Startup latency, control, and cost shape. Serverless starts in seconds with nothing to configure — right for interactive and bursty work. Classic gives you instance types, networking, and init scripts — right when you need specific hardware, VPC placement, or long-running tuned clusters. On cost, comparing DBU rates directly is a mistake, because classic bills DBUs plus your cloud VM spend and serverless is all-in. I'd measure total cost per workload, not rate."

---

## Answers

**Q1 — B.** Parquet plus `_delta_log`. Delta is a protocol over open files, not a proprietary format — which is why other engines can read it.

**Q2 — B.** All-purpose has a higher DBU rate and idles between runs. **A** is false (it can run jobs, just wastefully). **C** and **D** are fabrications.

**Q3 — B.** Traversal privileges. Privileges inherit downward, but you need `USE CATALOG` and `USE SCHEMA` above the table. The most common UC permission mistake.

**Q4 — C.** External tables retain their data on drop. Managed tables do not — that's the defining difference.

**Q5 — B.** VACUUM deleted the referenced files. Time travel is a consequence of not having deleted files, not a backup service.

**Q6 — B.** Classic compute runs in the customer's cloud account. Serverless runs in Databricks'. The control plane never processes data.

**Q7 — B.** Faithful and append-only with audit metadata. The test is that reprocessing from bronze reproduces silver exactly.

**Q8 — B.** Per-file min/max statistics in the log, for the first 32 columns by default. Not sorting, not indexing.

**Scoring:** 7–8, move to M2. 5–6, re-read 1.3 and 1.5. Below 5, run the hands-on notebook before rereading — these concepts stick through the fingers, not the eyes.
