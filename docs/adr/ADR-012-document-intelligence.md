# ADR-012 — Document Intelligence: Sponsorship Clause Extraction and Guest Issue Classification

| Field       | Value                              |
|-------------|------------------------------------|
| Status      | Accepted                           |
| Date        | 2026-11-12                         |
| Deciders    | Phil Neiman                        |
| Supersedes  | —                                  |
| ADR refs    | ADR-001 (env isolation), ADR-007 (gold layer) |

---

## Context

Two source datasets contain free-form text that must be structured before the
business can act on them:

**Sponsorship contracts** (`landing/sponsorship/contracts.json`, ~140 JSON
records per season).  Each record is a full agreement paragraph containing
three to six activation clauses concatenated as sentences.  The commercial
partnerships team needs a structured obligation register — clause type, numeric
quantity, and obligated party — to track fulfilment.

**Guest services tickets** (`landing/guest_services/guest_tickets.parquet`,
~31 k rows at `--scale medium`).  Each row is a free-text complaint.  The
guest experience team needs complaints categorised into seven buckets for
routing, and PII must be redacted before any analyst can query the gold table
(a row-filter policy is enforced on `sv_guest_ticket_enriched`).

Both sources were included in the data generator from the start
(`generate_guest_tickets`, `generate_sponsorships` in `tools/generate_source_data.py`)
but had no bronze ingestion or transform code until this ADR.

---

## Decision

### D1 — Pure-function transform layer, AI Functions in the pipeline only

All classification and redaction logic lives in
`src/kings_ops/transforms/document_intelligence.py` as plain
`str → str` / `DataFrame → DataFrame` functions with no I/O, no session
construction, and no AI Functions calls inside.

The pipeline (`pipelines/gold/doc_intelligence.py`) calls
`ai_classify` and `ai_extract` and passes the result columns (`ai_clause_type`,
`ai_quantity`, `ai_category`) to the transform functions as optional inputs.

**Why**: this is the same pattern used by every other transform in the
repo.  It keeps the entire transform layer locally testable without a cluster,
without a SQL warehouse, and without an Anthropic API key.  The 41 unit tests
in `tests/unit/test_document_intelligence.py` run in ~27 s on a laptop.

### D2 — Keyword/regex fallback for every AI-enriched column

Each `enrich_*` function uses `F.coalesce(ai_col, keyword_fallback_udf(text))`.
When `ai_classify` returns NULL (model uncertainty, quota exhausted, or the
workspace does not yet support AI Functions), the keyword/regex fallback
produces a label.  No row leaves the gold layer without a `clause_type` or
`issue_category`.

Fallback logic:
- **Clause type**: 6-keyword table (`videoboard`, `ticket`, `presenting partner`,
  `signage`, `activation`, `social media`).  First match wins.
- **Quantity**: regex chain (`no fewer than N`, `minimum of N`, `entitled to N`,
  `at least N`, `receive N`, `provide …N`, `deliver …N`, `include …N`,
  bare `\b\d+\b` last).  Returns `None` if no number found.
- **Issue category**: 23-keyword table across 7 categories (`transfer`, `scanner`,
  `waited`, `face value`, `restroom`, `usher`, `obstructed`, …).
- **PII redaction**: three compiled regexes (email, US phone, 16-digit card).

### D3 — Clause splitting by sentence boundary

The generator writes each contract as one paragraph with clause sentences
separated by `". "`.  The pipeline splits on that delimiter
(`F.split(contract_text, r"\. ")`), explodes the array, and skips
clause_num 0 (the preamble "SPONSORSHIP AGREEMENT between…") and any
trailing stub shorter than 20 characters.

This produces one row per clause per contract (3–6 clauses per agreement,
~700 clause rows per season at `--scale medium`).

**Limitation**: the split is brittle if a clause contains an abbreviation
with a mid-sentence period (e.g., "approx. 12 features").  The generated
clause sentences do not contain such abbreviations, so this is acceptable
for the current data shape.  A production system would use sentence
tokenisation or the AI Function directly on the full text.

### D4 — PII policy: raw `issue_text` dropped at gold

`sv_guest_ticket_enriched` retains only `issue_text_redacted`.  The
`.drop("issue_text")` call in the pipeline ensures the unredacted text
never lands in the gold layer.  Analysts querying gold see only the
redacted form; the raw PII-bearing text is accessible only in bronze
to privileged roles via Unity Catalog column masking.

### D5 — `issue_category_actual` withheld from the model

The generator stamps each guest ticket with the ground-truth category
(`issue_category_actual`).  This column is kept in bronze for evaluation
purposes (compare AI output with ground truth) but is dropped before the
AI Functions call in the gold pipeline, so the model cannot "see" the
answer.

### D6 — Bronze contracts for both new sources

Two new Auto Loader streaming tables are added to the bronze pipeline:

| Bronze table            | Source format | Landing path         |
|-------------------------|---------------|----------------------|
| `br_guest_ticket`       | Parquet       | `landing/guest_services/` |
| `br_sponsorship_contract` | JSON        | `landing/sponsorship/`    |

Both use `schemaEvolutionMode = rescue`.  Quarantine routing is not added:
the sources are machine-generated and highly regular; a rescue column is
sufficient to surface schema drift without the overhead of a full
quarantine split.

Data contracts (`GUEST_TICKET`, `SPONSORSHIP_CONTRACT`) are registered in
`src/kings_ops/contracts/definitions.py` following the existing pattern.

### D7 — Gold pipeline included via existing `gold.yml` glob

`pipelines/gold/doc_intelligence.py` is picked up automatically by the
`glob: include: ../../pipelines/gold/**` directive in `resources/pipelines/gold.yml`.
No changes to `gold.yml` are required.

---

## Alternatives considered

**A1 — Notebook-based AI Functions pipeline (rejected)**
Using a SQL notebook with `ai_classify` inline is simpler to write but
untestable locally and not composable with the existing transform test harness.
Rejected in favour of D1.

**A2 — Run ai_classify inside the transform UDF (rejected)**
Calling the Databricks REST API from inside a PySpark UDF introduces
network latency per row, driver-side HTTP pool management, and authentication
secrets inside a UDF serialisation context.  Rejected; the pipeline layer is
the right place for AI Functions.

**A3 — Store full contract text as one gold row (rejected)**
Clause extraction is per sentence, not per document.  A per-document gold
row would force consumers to re-parse the clause structure on every query.
Rejected in favour of the explode-at-ingest pattern (D3).

---

## Consequences

**Positive**
- 41 unit tests cover the full classify/extract/redact/enrich surface with no
  cluster dependency.
- The coalesce fallback means the gold tables are always fully populated,
  even when AI Functions are unavailable (B3 blocker).
- PII never reaches the gold layer in the redacted table.

**Negative / open**
- The sentence splitter (D3) is fragile to mid-clause periods.  Acceptable
  for synthetic data; must be revisited before production ingestion of real
  contracts.
- Quantity extraction from `ai_extract` returns a string; we cast to INT with
  possible NULL on failure.  The keyword regex fallback covers this for the
  generated data.

---

## Evaluation plan

| Check | Expected | Status |
|-------|----------|--------|
| `sv_sponsorship_clause_enriched` row count | ~700 (10 sponsors × ~4 clauses × 17 games) | Pending B3 |
| `clause_type != 'UNKNOWN'` fraction | ≥ 95 % with AI Functions, ≥ 85 % keyword-only | Pending B3 |
| `sv_guest_ticket_enriched` row count | = `br_guest_ticket` count (no drops) | Pending B3 |
| `pii_detected = true` fraction | ≈ 7 % (generator `guest_note_contains_pii = 0.07`) | Pending B3 |
| `issue_text` absent from gold | Column not present in schema | Pending B3 |
| `issue_text_redacted` contains no `@` where `pii_detected` | 100 % | Pending B3 |

Blocked by B3 (serverless compute quota suspended).  See docs/BLOCKERS.md.
