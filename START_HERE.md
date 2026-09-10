# START HERE

The whole path, in order. Do not jump ahead — later steps consume values earlier ones produce.

---

## The shape of it

```
Day 1   Account + local setup          → you can run tests and reach the workspace
Day 2   Deploy + generate Kings data   → data exists in a Volume
Day 3-5 M1 platform fundamentals       → you understand what you just built on
Week 2  M2 ingestion                   → bronze layer
```

| Stage | Guide | Ends when |
|---|---|---|
| 1. Local setup | `docs/SETUP_RUNBOOK.md` Phase 0 | `pytest tests/unit` shows 45 passed |
| 2. Databricks account | Phase 1 | You have a workspace URL and warehouse name |
| 3. CLI + auth | Phase 2 | `databricks current-user me` returns your email |
| 4. First deploy | Phase 3 | `bundle deploy` succeeds; jobs visible in workspace |
| 5. **Kings data** | Phase 4 | `_manifest.json` exists in the landing Volume |
| 6. Smoke test + GitHub | Phase 5 | CI green on a PR |
| 7. Break it | Phase 6 | You've seen a test fail on purpose |
| 8. **M1** | `docs/modules/M1_platform.md` | Self-check ≥ 5 of 8 |
| 9. **M2** | `docs/modules/M2_ingestion.md` | Bronze tables + quarantine reconciling |

---

## What the Kings data actually is

Before you generate it, know what you're generating. Full detail in `docs/architecture/PROJECT_CHARTER.md`.

**Las Vegas Kings** — fictional MLB expansion franchise, inaugural 2027 season, 33,000-seat retractable-roof ballpark in Las Vegas. The platform serves the *business* side: ticketing, concessions, sponsorship. Not baseball operations.

Nine source systems land into one Volume:

| Source | Format | Why it exists in the build |
|---|---|---|
| `gate_scans` | JSON stream | Burst-shaped streaming, late data, schema drift |
| `ticketing/cdc` | Parquet | CDC MERGE, out-of-order events, duplicates |
| `pos` | JSON batches | Late batches, double submissions, tax misconfiguration |
| `crm_accounts` | CSV | PII, masking, minors row filter |
| `deposits` | CSV | Expansion-only funnel; cold-start forecasting |
| `seat_manifest` | CSV | COPY INTO; mid-season section renumbering |
| `schedule` | JSON | Conformed game dimension |
| `pricing` | CSV | Effective-dated ranges with overlaps |
| `sponsorship` / `guest_services` | JSON / Parquet | Document intelligence, PII in free text |

**The data is deliberately broken.** Duplicate CDC sequences, refunds backdated before their sale, orphan seat IDs, malformed JSON, scans with no valid ticket, zero-tax POS lines. Verified counts at `small` scale:

| Defect | Count |
|---|---|
| Malformed JSON | 10 scans, 39 POS lines |
| Schema drift (`scan_device_os`) | appears at game 3 |
| Invalid scans | 34 |
| Duplicate CDC `__seq` | 46 |
| Corrections backdated before their SALE | 30 |
| Renumbered seats | 156 |
| Zero-tax POS lines | 1,965 |
| Overlapping price windows | 8 |
| Accounts resolving as minors | 88 |
| PII in guest-services text | 28 |

Every one of those exists so a later module has something real to catch. Do not fix them in the generator.

### Scale profiles

| Profile | Games | Seats | Accounts | Runtime | Use for |
|---|---|---|---|---|---|
| `small` | 6 | 3,000 | 4,000 | ~1 min | Everything until M6 |
| `medium` | 27 | 12,000 | 18,000 | ~10 min | M6 clustering work |
| `full` | 81 | 33,000 | 44,000 | ~45 min | Portfolio screenshots only |

Start with `small`. The bootstrap job defaults to it in dev.

---

## Running the generator two ways

**On Databricks** (the normal path — the bootstrap job does this):
```bash
databricks bundle run bootstrap --target dev
```

**Locally** (useful for inspecting the data on your own machine):
```bash
source .venv/bin/activate
python tools/generate_source_data.py --output-dir ./landing --scale small --seed 42
cat landing/_manifest.json
```

Same seed and scale produces byte-identical output. That determinism is what makes the reconciliation tests in M2 possible at all.

---

## Rules that prevent the common mess-ups

1. **`source .venv/bin/activate` in every new terminal.** Skipping it produces import errors you'll blame on the code.
2. **Never edit the same file in two places in one session.** Pick your laptop or the workspace Repo, commit, then switch.
3. **Git is the source of truth; the workspace is a deployment target.** Nobody can see your Free Edition workspace — the portfolio is the public repo.
4. **`databricks bundle validate` before every deploy.** It catches config errors without touching the workspace, and it's a tested exam objective.
5. **Read the concept brief before the code in every module.** Reverse-engineering reasoning from finished code is how people end up fluent in syntax and shaky on judgment.
