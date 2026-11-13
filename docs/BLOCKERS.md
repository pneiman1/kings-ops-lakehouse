# Blockers

Items waiting on a permission or an answer from someone else.

| ID | What it needs | Owner | Raised |
|----|---------------|-------|--------|
| ~~B1~~ | ~~**Step 2 "merge the generators" has no second generator.**~~ RESOLVED 2026-11-03: Phil chose to drop step 2 — `src/kingsgen/` and the four entities (staffing, parking, incidents, rain delay) never existed and aren't in the charter. No merge; one generator stands. | Phil | 2026-11-03 |
| ~~B2~~ | ~~**`gh` CLI not installed**~~. RESOLVED 2026-11-12: `gh` 2.103.3 is installed. `gh auth login` requires interactive TTY; workaround is `git credential fill` → GitHub REST API directly. All open branches already have PRs. | Phil | 2026-11-03 |
| B3 | **Free Edition compute suspended — two distinct errors, both blocking Phase 1.** (1) `databricks bundle run bootstrap --target dev` → `Error: cannot start job: Triggering new runs for organization 7474659185878219 is currently disabled temporarily.` (2) `databricks warehouses start 94979f5230a3c530` → `Error: Cannot create the resource, please try again later.` `dbxctl cost` confirms zero active compute. Auth is live (PAT, `current-user me` returns pneimn1@gmail.com). Deploy succeeds. Only execution is blocked. Phase 1 and Phase 4 (real-number packaging rewrite) both blocked until quota clears. Try off-peak or next calendar day. | Phil | 2026-11-12 |
