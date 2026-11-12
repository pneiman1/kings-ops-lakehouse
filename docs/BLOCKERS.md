# Blockers

Items waiting on a permission or an answer from someone else.

| ID | What it needs | Owner | Raised |
|----|---------------|-------|--------|
| ~~B1~~ | ~~**Step 2 "merge the generators" has no second generator.**~~ RESOLVED 2026-11-03: Phil chose to drop step 2 — `src/kingsgen/` and the four entities (staffing, parking, incidents, rain delay) never existed and aren't in the charter. No merge; one generator stands. | Phil | 2026-11-03 |
| ~~B2~~ | ~~**`gh` CLI not installed**~~. RESOLVED 2026-11-12: `gh` 2.103.3 is installed. Needs `gh auth login` before PRs can be opened programmatically. | Phil | 2026-11-03 |
| B3 | **Free Edition serverless compute quota exhausted.** `RESOURCE_EXHAUSTED: Cannot create the resource, please try again later.` on all pipeline and warehouse starts. Zero active compute (confirmed: `dbxctl cost` returns none). Quota likely resets on a daily schedule (try off-peak / next day). SQL gate counts, refund check, SCD2 check, governance/observability SQL, and pipeline chain run all blocked until quota clears. | Phil | 2026-11-12 |
