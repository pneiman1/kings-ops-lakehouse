# Session notes

Appended at the end of each work session, one entry per tranche landed.

---

## 2026-11-10 — Housekeeping + T4 silver streaming

### Closed

**Housekeeping**
- `gh` CLI confirmed installed (v2.103.3); needs `gh auth login` before `gh pr create` works. Logged as B3.
- `feat/ops-kit`, `feat/silver-cdc-scd2`, `feat/gold-star-schema` confirmed fully merged into `main` (all three are ancestors of `main` HEAD `40d0862`). Stale branches left on origin for now; delete after B3 unblocks PR review.
- `.claude/settings.json` hook command fixed: bare `python` → `.venv/Scripts/python` so the validation hook reaches ruff in the venv. Fix logged in TROUBLESHOOTING.md.
- `feat/generator-tests` has one unmerged commit (`a6d097e` — generator defect-rate assertions + byte-reproducibility). PR pending B3.

**T4 — Silver streaming (feat/silver-streaming)**
- ADR-008: watermark strategy decision. 48-hour watermark chosen over tight (5 min) and per-gate custom options. Rationale: lateness is gate-correlated (offline outfield handhelds), not random; a tight watermark systematically undercounts attendance at those gates. State size ~46k events at peak — manageable. See `docs/adr/ADR-008-streaming-watermark-strategy.md`.
- `src/kings_ops/transforms/admission.py`: pure transforms — `enrich_scan_with_ticket`, `enrich_scan_with_seat`, `build_sv_admission`, `quarantine_reason`. Stream-static join pattern documented inline.
- `pipelines/silver/silver_streaming.py`: pipeline wrapper — watermark + stateful `dropDuplicates(["scan_id"])` in wrapper (needs streaming context); enrichment delegated to pure transforms. Outputs `sv_admission` (valid) and `q_scan_invalid` (quarantine).
- `tests/unit/test_admission.py`: unit tests covering orphan ticket, ACCEPTED+matched, DUPLICATE/INVALID routing, SCD2 as-of seat join, quarantine reason strings.

### Decided

- 48h watermark (not tight, not per-gate) — correlated lateness means a tight watermark drops a whole gate's data, not random outliers. ADR-008.
- Stream-static joins (not stream-stream) — ticket orders and dim_seat are batch-resolved; stream-stream would add watermarks on both sides and state on both sides for no correctness gain.
- Triggered (`availableNow`) not continuous — Free Edition pipeline budget (ADR-003); watermark still advances correctly per micro-batch.

### Left / next

- B3: `gh auth login` → open `feat/generator-tests` PR → merge → clean stale branches.
- T6 governance layer (`feat/governance`).
- T7 observability (`feat/observability`).
- Cart allocation (`feat/cart-allocation`).
- T10 packaging.

### Gold test false alarm

`test_gold.py` appeared to fail (exit code 1, no output) when run without `-s`. Root
cause: the Spark stage-progress lines (the `[Stage X:...]\r` carriage-return sequences)
contain control characters that caused the tool's output capture to fail silently. Running
with `-s 2>/dev/null` confirmed **3 passed**. The tests are fine; it's a Windows bash
redirect artefact. No code change needed. Logged here so the next session does not
re-diagnose.
