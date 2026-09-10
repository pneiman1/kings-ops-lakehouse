# Learning Path — Kings Ops Lakehouse

**Target:** Databricks Certified Data Engineer **Professional**

One dataset — Las Vegas Kings ballpark operations — as the substrate for seven build modules covering ten exam domains. The same repo is the portfolio artifact and the curriculum.

---

## 1. Exam facts

| | |
|---|---|
| Scored items | 59 multiple choice |
| Time | 120 minutes |
| Fee | USD 200 |
| Format | **Scenario-based.** You are shown a Spark UI screenshot or a streaming query JSON and asked what is wrong. |
| Prerequisite | Associate-level knowledge assumed, not formally required |
| Validity | 2 years |

### Domain weights

| # | Domain | Weight |
|---|---|---|
| 1 | Developing Code for Data Processing using Python and SQL | **22%** |
| 2 | Cost & Performance Optimization | **13%** |
| 3 | Data Transformation, Cleansing, and Quality | 10% |
| 4 | Monitoring and Alerting | 10% |
| 5 | Ensuring Data Security and Compliance | 10% |
| 6 | Debugging and Deploying | 10% |
| 7 | Data Ingestion & Acquisition | 7% |
| 8 | Data Governance | 7% |
| 9 | Data Modelling | 6% |
| 10 | Data Sharing and Federation | 5% |

### What these numbers should change about how you study

**Ingestion is only 7%.** It is the foundation of the build and it is a small slice of the exam. Build M2 properly, then move on — do not spend three weeks there.

**Code is the largest domain at 22%.** Not concepts about code — actual Python and SQL for data processing. The pure-function transform layer in `src/kings_ops/transforms/` is directly aligned: testable business logic, no session construction inside, no reads or writes buried in transformations.

**Streaming is where candidates fail.** Reported failure post-mortems cite the streaming material more than anything else: watermarks, state stores, and checkpoint recovery. Streaming is therefore its own module (M4) rather than a slice of transformation.

**Optimization and observability together are 23%.** These were the thinnest parts of the original plan and are now the largest build investment after code.

**Data Sharing and Federation is 5% and was absent from earlier plans.** Delta Sharing and Lakehouse Federation were *removed* from the Associate syllabus, which is why they were missing. They are in Professional and are now part of M7.

---

## 2. The seven modules

| Module | Domains | Approx. exam weight | Status |
|---|---|---|---|
| **M1** Platform & Architecture | Data Modelling, part of Cost/Perf | ~8% | Built |
| **M2** Ingestion | Data Ingestion & Acquisition | 7% | Built |
| **M3** Transformation & Modeling | Transformation/Quality, Data Modelling, much of Developing Code | ~25% | Next |
| **M4** Streaming ⭐ | Developing Code (streaming), Transformation | ~15% | Not built |
| **M5** Orchestration & Deployment | Debugging and Deploying | 10% | Built (CI/CD spine) |
| **M6** Observability & Optimization | Monitoring/Alerting, Cost & Performance | 23% | Not built |
| **M7** Governance, Security & Sharing | Security/Compliance, Governance, Sharing/Federation | 22% | Not built |

### Build order

**M1 ✅ → M2 ✅ → M3 → M4 → M6 → M7 → M5 polish**

M5's CI/CD spine already exists because retrofitting environment parameterization into finished modules is the standard sequencing mistake. What remains in M5 is the deployment-troubleshooting material and one branch-commit-PR cycle done through Databricks Repos in the workspace UI.

Portfolio display order stays M1 → M7 regardless of build order.

---

## 3. Module detail

### M1 — Platform & Architecture ✅
Control plane vs. compute plane, Delta Lake internals (Parquet + `_delta_log`), Unity Catalog namespace and privilege traversal, managed vs. external tables, compute selection and DBU cost model, medallion rationale.
`docs/modules/M1_platform.md` · `notebooks/M1_platform_explorer.sql`

### M2 — Ingestion ✅
Data contracts per source, Auto Loader with schema evolution and rescue, COPY INTO for bounded reference data, the ingestion-method decision matrix, quarantine routing that reconciles.
`docs/modules/M2_ingestion.md` · `pipelines/bronze/` · `src/kings_ops/contracts/`

### M3 — Transformation & Modeling ⬅ next
CDC MERGE ordered by `__seq` not `__commit_ts`, SCD Type 2 on accounts and seats, the join taxonomy including broadcast, deduplication, gold star schema, and gold objects built four ways — table, view, materialized view, streaming table — with a written rationale for when each is correct.

Carries a large share of the 22% code domain, so the transform layer is written as pure functions with unit tests rather than notebook cells.

**Break-it lab:** MERGE ordered by arrival instead of `__seq`, and watch refunded orders resurrect.

### M4 — Streaming ⭐ Not built
Promoted to its own module because this is the reported failure point.

- Auto Loader on the burst-shaped gate scan stream (~40× throughput swing)
- Watermarking where **lateness is correlated with gate**, not random — handheld scanners at outfield gates bulk-upload hours later, which breaks the independence assumption most tutorials rely on
- **State store behavior**: what accumulates, what evicts, how watermark choice drives state size
- **Checkpoint recovery**: what survives a restart, what a schema change does to a checkpoint, when a full reset is the only option and what it costs
- Stream-static and stream-stream joins
- Reading a streaming query JSON and diagnosing the problem — the exam's actual format

**Break-it lab:** delete a checkpoint mid-stream and recover. Then change the schema and watch what a checkpoint will and will not tolerate.

### M5 — Orchestration & Deployment ✅ spine built
Bundle targets with variable overrides across dev/stg/prd, GitHub Actions gates, `bundle validate` matrix. Remaining: job control flow (retries, conditional branching, for-each loops), file-arrival and table-update triggers, deployment failure diagnosis, and one branch-commit-PR cycle through Databricks Repos in the workspace UI.

### M6 — Observability & Optimization — Not built · 23%
The largest remaining investment.

- Event log analytics, freshness SLOs, run-history trend comparison
- System tables for cost attribution and usage monitoring
- **Liquid clustering** over partitioning and Z-ordering — the current guidance, and the exam reflects it
- Predictive optimization
- **Spark UI diagnosis**: skew, shuffle, spill read from stage metrics. Game-date skew is natural in this dataset, so the skewed join is real rather than manufactured.
- Alerting and retry strategy

**Free Edition gap — see section 4.** Much of the cost half of this domain requires a paid trial.

### M7 — Governance, Security & Sharing — Not built · 22%
- Managed vs. external tables, and converting between them
- GRANT/REVOKE/DENY across the securable hierarchy, including traversal privileges
- Column masking on account PII, row filter for minors
- ABAC policies driven by tags
- Lineage and audit via system tables
- **Delta Sharing** — sharing data outside the metastore
- **Lakehouse Federation** — querying external sources without ingesting them

---

## 4. What Free Edition cannot teach you

Serverless-only Free Edition blocks a meaningful share of a 13%-weighted domain. Be honest with yourself about this rather than discovering it at the test centre.

| Topic | Why blocked | Domain |
|---|---|---|
| Cluster sizing, node types, autoscaling | No custom compute | Cost & Performance |
| Cluster startup failure diagnosis | Cannot provision clusters | Debugging |
| Library conflict resolution | No cluster libraries | Debugging |
| OOM diagnosis, driver/executor memory | No memory config exposed | Cost & Performance |
| `spark.sql.shuffle.partitions`, broadcast threshold | Serverless auto-tunes | Cost & Performance |
| All-purpose vs. job compute cost comparison | Only serverless available | Cost & Performance |
| Photon on/off comparison | Serverless is always Photon | Cost & Performance |
| Delta Sharing to an external recipient | Limited on Free Edition | Sharing & Federation |

**Mitigation:** build everything on Free Edition, then spend one week on a paid trial doing nothing but compute configuration, Spark UI analysis, and cost comparison. Do this immediately before the exam so it is fresh.

Other Free Edition limits shaping the build, each documented as an ADR rather than worked around silently: one workspace and one metastore, one SQL warehouse at `2X-Small`, one active pipeline per type, five concurrent job tasks, one AI Search endpoint.

---

## 5. Where you already stand

Associate is passed (May 2026 syllabus). Section scores ran roughly 66–100%, with
**Troubleshooting/Optimization at 100%**.

That result should shape study allocation rather than being a footnote:

| Professional domain | Associate signal | Implication |
|---|---|---|
| Cost & Performance (13%) | Troubleshooting/Optimization 100% | Reinforcement, not new ground. Professional adds Spark UI diagnosis and system-table cost attribution. |
| Monitoring & Alerting (10%) | Same section, 100% | As above. |
| Developing Code, Python + SQL (22%) | Barely tested at Associate level | **Largest genuine gap.** Highest weight, least prior signal. |
| Data Sharing & Federation (5%) | Removed from the Associate syllabus | Never seen it. Entirely new. |
| Streaming depth | Associate tested syntax, not state | Operational depth is new: state stores, checkpoint recovery, stream-stream joins. |

**Net:** the 23% covered by Cost/Performance and Monitoring is your strongest territory
already. Spend the recovered time on M3 and M4, which carry the code and streaming
domains — the two places where Professional is a step change rather than a deeper cut.

## 6. Module format

Every module ships in this shape:

1. **Concept brief** — the mental model, before any code. What problem does this mechanism solve, what did people do before it, what breaks without it.
2. **Exam objectives covered** — tagged, with domain weight.
3. **The build** — annotated production code, with the *why* in comments.
4. **Break-it lab** — a deliberate failure to cause and diagnose. Professional is a troubleshooting exam; reading a broken thing is the closest practice you get.
5. **Self-check** — scenario-style questions with explanations for why each distractor is wrong.
6. **Interview drill** — what a hiring manager asks, and what a strong answer contains.

---

## 7. Cadence

| Week | Focus |
|---|---|
| 1 | Setup, deploy, generate data. M1 review — mostly recall from Associate. |
| 2 | M2 ingestion |
| 3–4 | **M3 transformation and modeling** — carries the 22% code domain |
| 5–6 | **M4 streaming** — the heaviest module and the reported failure point |
| 6 end | Cold practice exam. Diagnostic for M6/M7 depth, not a go/no-go. |
| 7 | M6 observability and optimization — reinforcement given your 100% section |
| 8–9 | M7 governance, security, and **sharing/federation** — the one topic you have never seen |
| 10 | Paid-trial week: compute, Spark UI, cost comparison. **Sit the exam.** |
| 11 | Portfolio packaging: README, case study, LinkedIn article, screenshots |

Also on the stated certification roadmap ahead of Professional: **DP-750** (Azure Databricks
Data Engineer Associate) and **Databricks Generative AI Engineer Associate**. This build
serves DP-750 well — the platform overlap is heavy. If that ordering still holds, the
cadence above stretches, but the module sequence does not change.

## 8. Naming — the exam uses current names, most tutorials use old ones

| Current | Formerly |
|---|---|
| Declarative Automation Bundles (DABs) | Databricks Asset Bundles |
| Lakeflow Jobs | Databricks Workflows / Jobs |
| Lakeflow Spark Declarative Pipelines | Delta Live Tables (DLT) |
| `from pyspark import pipelines as dp` | `import dlt` |
| Liquid Clustering | Z-Ordering and partitioning (de-emphasized) |
| Lakeflow Connect | — |

A tutorial written last year uses the right-hand column. Translate on sight.
