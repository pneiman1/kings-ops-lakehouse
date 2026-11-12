# Blockers

Items waiting on a permission or an answer from someone else.

| ID | What it needs | Owner | Raised |
|----|---------------|-------|--------|
| ~~B1~~ | ~~**Step 2 "merge the generators" has no second generator.**~~ RESOLVED 2026-11-03: Phil chose to drop step 2 — `src/kingsgen/` and the four entities (staffing, parking, incidents, rain delay) never existed and aren't in the charter. No merge; one generator stands. | Phil | 2026-11-03 |
| ~~B2~~ | ~~**`gh` CLI not installed.**~~ RESOLVED 2026-11-10: `gh` v2.103.3 was already installed. Needs `gh auth login` before `gh pr create` works — see B3. | — | 2026-11-10 |
| B3 | **`gh auth login` not completed.** `gh pr create` fails. Workaround: open PRs from the URL printed by `git push`. Pending PRs: `feat/generator-tests → main` (1 commit: generator defect-rate assertions + byte-reproducibility). Run `gh auth login` once to unblock. | Phil | 2026-11-10 |
