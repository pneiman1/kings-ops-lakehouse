# ADR-006 — Ingestion method selection

**Status:** Accepted
**Context:** M2 — Data Ingestion and Loading

## Context

Ten source systems land data for the Kings platform. They differ on volume, arrival
pattern, format, and governance sensitivity. Using one ingestion mechanism for all of
them would be simpler to describe and wrong in practice.

## Decision

Select per source using a single decisive question, with volume and format as
secondary considerations:

> **Does this source produce files continuously and unboundedly?**

| Source | Arrival | Volume (full) | Method | Rationale |
|---|---|---|---|---|
| `gate_scans` | Continuous, burst-shaped | ~1.9M rows | **Auto Loader** | Unbounded file production; needs exactly-once file tracking and burst throttling |
| `pos` | Per-terminal batches, continuous | ~2.6M rows | **Auto Loader** | Terminals double-submit; Auto Loader's file tracking is the idempotency mechanism |
| `ticketing_cdc` | Daily batch files, continuous | ~2.4M events | **Auto Loader** | Unbounded, and `failOnNewColumns` requires the explicit-schema path |
| `seat_manifest` | Occasional manual drop | ~33k rows | **COPY INTO** | Bounded, rare; a checkpoint for twice-a-season data is unjustified overhead |
| `pricing` | Intraday CSV drops | ~180k rows | **COPY INTO** | Bounded per drop, idempotent re-run is sufficient |
| `deposits` | One-time load | ~38k rows | **COPY INTO** | Expansion-season artifact; loads once, never again |
| `schedule` | Once per season, amended | 81 rows | **COPY INTO** | Trivially small |
| `crm_accounts` | Nightly full snapshot | ~44k rows | **COPY INTO** | Bounded, predictable; snapshot semantics handled in silver |
| `sponsorship` | Rare | 140 docs | **COPY INTO** | Bounded |
| `guest_services` | Periodic export | ~31k rows | **COPY INTO** | Bounded |

## Alternatives considered

**Auto Loader for everything.** Rejected. Uniform, but it means managing a checkpoint
and a schema location for every source including ones that change twice a season. The
operational surface grows with source count for no benefit on the bounded sources.

**COPY INTO for everything.** Rejected. COPY INTO cannot throttle a burst, has no
streaming semantics, and its idempotency is per-file — which does not protect against
the double-submitted POS batches, since identical content under a new filename loads
again.

**Lakeflow Connect.** Not applicable to this build: every source here is files in a
Volume, not a SaaS application or operational database. Connect would be the correct
choice if the ticketing system were reachable directly rather than via a file extract,
and that is the migration noted below.

## Consequences

- Two ingestion patterns to maintain rather than one. Acceptable: each is small, and
  the boundary between them is a single documented question.
- COPY INTO's per-file idempotency must be respected operationally. Landing identical
  content under two filenames duplicates rows. The bronze reconciliation query asserts
  key uniqueness to catch this.
- **Migration path:** if ticketing moves to Lakebase or a directly-reachable operational
  store, `ticketing_cdc` should move to a Lakeflow Connect managed connector, removing
  the file-extract hop entirely.

## Exam relevance

The May 2026 syllabus tests prioritizing between Auto Loader, Lakeflow Connect, partner
connectors, and other ingestion methods based on data volume, ingestion frequency, data
types, and governance needs. This table is that objective, applied.
