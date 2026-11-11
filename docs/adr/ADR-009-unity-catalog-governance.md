# ADR-009 — Unity Catalog governance approach

**Status:** Accepted
**Context:** T6 — Governance layer

---

## Context

The Kings platform holds three categories of sensitive data that require different
handling:

1. **PII in `dim_account`**: names, email, phone, address, payment token, birth_date.
   Contract v1.0.0 declares all as `is_pii=True`. Masking applies to contact fields.
   Birth date is specifically NOT masked because the minor row filter reads it — masking
   it first would make the filter always pass, defeating the access control.

2. **Minor account holders**: accounts whose birth_date implies age < 18 at the
   start of the season. Under COPPA-style policy, these rows must be invisible to
   general analysts and to any report that could expose a minor's attendance pattern.

3. **HR / payroll sensitivity**: guest services tickets (free text) and CRM notes can
   contain names and subject lines referencing HR matters. Classification and redaction
   is a T8 concern; governance here limits which principals can read raw text at all.

The governance model must also handle growth cleanly: adding a new analyst should be a
group membership change, not a SQL change.

---

## Decisions

### D1 — All bronze/silver/gold tables are managed (not external)

Unity Catalog managed tables store data in UC-controlled locations. `DROP TABLE` removes
both metadata and data files. External tables remove only the metadata.

Managed is the right choice for bronze/silver/gold because:
- The pipeline owns the full lifecycle of these tables.
- Accidental data loss from a `DROP TABLE` is an acceptable outcome only if the source
  Volume still exists (which it does for bronze) or the pipeline can reprocess.
- External tables are correct for the **landing Volume** (raw source files), which is
  declared as an external location and never managed by the pipeline.

### D2 — Traversal privileges must be granted explicitly before any object privilege

In Unity Catalog, a `SELECT` on a table is insufficient if the caller lacks `USE CATALOG`
on the catalog and `USE SCHEMA` on the schema. This is not obvious from the error message,
which reports a permission denied on the table, not on the schema.

Standard grant sequence (see `sql/governance/02_privilege_model.sql`):
```sql
GRANT USE CATALOG ON CATALOG kings_prd TO GROUP kings_analysts;
GRANT USE SCHEMA  ON SCHEMA kings_prd.gold TO GROUP kings_analysts;
GRANT SELECT      ON TABLE kings_prd.gold.fact_ticket_order TO GROUP kings_analysts;
```

`DENY` overrides `GRANT` regardless of group hierarchy. It is used only for hard
exclusions (e.g., preventing analysts from reading the quarantine schema directly).

### D3 — Column masking uses functions that check IS_MEMBER(), not literal user lists

Masking functions that contain literal user/email lists do not scale and cannot be
audited. Functions that check `IS_MEMBER('kings_pii_access')` scale to group size.
Adding a user who needs unmasked PII is a group membership change, not a SQL change.

```sql
-- Pattern for every PII column in dim_account:
CREATE FUNCTION kings_prd.governance.mask_string_pii(value STRING)
  RETURNS STRING
  RETURN CASE WHEN is_account_admin() OR IS_MEMBER('kings_pii_access')
              THEN value
              ELSE '***MASKED***'
         END;
```

### D4 — Minor row filter uses birth_date directly; birth_date is NOT column-masked

The row filter must evaluate birth_date to decide if the row is visible. If the column
were masked first, the filter function would receive `'***MASKED***'` and the comparison
to `current_date() - INTERVAL 18 YEARS` would always fail (or error), making every row
visible. This violates the access control.

Resolution: birth_date is tagged as PII and controlled by an access policy that restricts
who can query the table at all, but the column value itself is not obfuscated. The row
filter is what hides minor rows from non-privileged principals.

### D5 — Tags drive ABAC labeling; privilege enforcement uses functions and row filters

Unity Catalog tags (on columns, tables, or schemas) label data. They do not by themselves
enforce access. Enforcement happens through:
- Column masks on tagged columns (reading the tag via information_schema)
- Row filters on tagged tables
- Grants scoped to tagged objects via attribute-based policies

Tags on the Kings platform follow the schema: `'sensitivity' = 'pii'`,
`'pii_category' = 'contact' | 'financial' | 'identity'`, `'regulatory' = 'coppa'`.

### D6 — Delta Sharing for external partners uses share-level, not table-level grants

Share a named set of gold objects to a named recipient (token-authenticated). The share
boundary is the access surface; partners never get credentials into the catalog itself.

Shares expose only gold tables. Bronze and silver are never included in any share.

### D7 — Lakehouse Federation for the operational ticketing store (migration path)

ADR-006 notes that if ticketing moves to a directly-reachable operational store, Lakeflow
Connect is the correct ingestion path. Lakehouse Federation is the read path for the same
scenario: registering the operational database as a foreign catalog allows UC-governed
federated queries without duplicating data into bronze first.

This is currently a future migration path, not an active connection. The SQL in
`sql/governance/08_federation.sql` is illustrative.

---

## Consequences

- Every new schema requires a traversal grant sequence before any analyst can query it.
  This is the most common governance mistake in UC deployments; the scripts here encode
  the correct sequence as runnable SQL.
- Birth_date is a PII column with no mask applied. Any principal with SELECT on dim_account
  can see raw dates. The minor row filter is the access control; this is documented in the
  contract (`definitions.py`) and in the tags.
- Column masks add one function call per row per masked column per query. For the query
  volume on this platform (a single warehouse, analyst-facing), this is not a performance
  concern. If it were, row-level security via a view would be an alternative.

---

## Exam relevance

Exam objectives cover: Unity Catalog privilege model (traversal, inheritance, DENY),
column masks vs. row filters, managed vs. external tables, tags, Delta Sharing, and
Lakehouse Federation. This ADR documents all of them in a single decision context with
explicit rejected alternatives.
