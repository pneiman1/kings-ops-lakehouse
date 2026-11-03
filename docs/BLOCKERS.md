# Blockers

Items waiting on a permission or an answer from someone else.

| ID | What it needs | Owner | Raised |
|----|---------------|-------|--------|
| B1 | **Step 2 "merge the generators" has no second generator.** `src/kingsgen/` does not exist on any branch, in history, or in stash — there is one generator (`tools/generate_source_data.py`) with the charter's 9 sources. The four entities to fold in (staffing, parking, incidents, rain delay) exist nowhere and are not in the PROJECT_CHARTER source list. Need: the kingsgen source (if it lives on another machine/uncommitted), OR specs (schema, grain, defect rates, manifest shape) to build the four entities net-new, OR a decision to drop them. | Phil | 2026-11-03 |
| B2 | **`gh` CLI not installed**, so PRs can't be opened programmatically (settings.json allows `gh pr:*`, but `where gh` finds nothing). Branches push fine. Workaround: open PRs from the URL git prints on push, or `winget install --id GitHub.cli -e`. | Phil | 2026-11-03 |
