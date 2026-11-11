# Blockers

Items waiting on a permission or an answer from someone else.

| ID | What it needs | Owner | Raised |
|----|---------------|-------|--------|
| ~~B1~~ | ~~**Step 2 "merge the generators" has no second generator.**~~ RESOLVED 2026-11-03: Phil chose to drop step 2 — `src/kingsgen/` and the four entities (staffing, parking, incidents, rain delay) never existed and aren't in the charter. No merge; one generator stands. | Phil | 2026-11-03 |
| ~~B2~~ | ~~**`gh` CLI not installed.**~~ RESOLVED: `gh` v2.103.3 is installed. | Phil | 2026-11-03 |
| B3 | **`gh auth login` not completed**, so `gh pr create` is blocked. All four feature branches (feat/generator-tests, feat/silver-streaming, feat/governance, feat/observability, feat/cart-allocation) have been pushed; PRs can be opened from the URL printed by `git push`. Run `gh auth login` once to unblock programmatic PR creation. | Phil | 2026-11-11 |
