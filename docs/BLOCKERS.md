# Blockers

Items waiting on a permission or an answer from someone else.

| ID | What it needs | Owner | Raised |
|----|---------------|-------|--------|
| ~~B1~~ | ~~**Step 2 "merge the generators" has no second generator.**~~ RESOLVED 2026-11-03: Phil chose to drop step 2 — `src/kingsgen/` and the four entities (staffing, parking, incidents, rain delay) never existed and aren't in the charter. No merge; one generator stands. | Phil | 2026-11-03 |
| B2 | **`gh` CLI not installed**, so PRs can't be opened programmatically (settings.json allows `gh pr:*`, but `where gh` finds nothing). Branches push fine. Workaround: open PRs from the URL git prints on push, or `winget install --id GitHub.cli -e`. | Phil | 2026-11-03 |
