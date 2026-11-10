# Troubleshooting

Every failure hit on this project, with its fix. Read this before debugging
anything.

---

## `validate.py` hook blocks every edit with pre-existing ruff errors in `src/`

**Symptom.** The moment `.claude/hooks/validate.py` is installed, every Edit/Write
is rejected with a wall of ruff `UP035` and `E501` errors in `src/kings_ops/`
files you never touched (`config.py`, `logging_utils.py`, `contracts/definitions.py`).

**Cause.** The hook lints `src/` (not just `scripts/`), but `src/` had never been
run through ruff before the hook existed — so it surfaces the whole backlog at once.
The edited file is still written (PostToolUse runs *after* the tool); the exit-2
output is feedback, not a rollback.

**Fix.** Clear the backlog once, then it stays green:
```
python -m ruff check src --fix          # auto-fixes UP035 import moves + import sort
```
`E501` (line too long) is not auto-fixable — wrap the offending `Column(...)`
contract lines by hand onto a second line. Do **not** shorten the descriptions;
the `__seq` / `__commit_ts` text is load-bearing documentation of the CDC
ordering trap.
