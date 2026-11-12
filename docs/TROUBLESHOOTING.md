# Troubleshooting

Every failure hit on this project, with its fix. Read this before debugging
anything.

---

## `validate.py` hook fails with "No module named ruff" on every edit

**Symptom.** Every Edit/Write triggers the PostToolUse hook and immediately errors with:

```
ruff:
C:\Users\...\Python311\python.exe: No module named ruff
```

**Cause.** The hook command in `.claude/settings.json` was `python .claude/hooks/validate.py`.
On this machine `python` resolves to the system Python at
`C:\Users\pneiman\AppData\Local\Programs\Python\Python311\python.exe`, which does not have
`ruff` installed. Only the project `.venv` has it.

**Fix.** Change the hook command to use the venv interpreter:

```json
"command": ".venv/Scripts/python .claude/hooks/validate.py"
```

The error will fire once more on the edit that changes `settings.json` itself (the hook runs
with the old command on that exact edit), then stays green from that point on.

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

---

## Local PySpark on Windows: "Python worker failed to connect back"

**Symptom.** A local `SparkSession` starts, then `.collect()`/`.show()` dies with
`SparkException: Python worker failed to connect back` and a
`SocketTimeoutException`, often naming `host.docker.internal` as the executor.

**Cause.** Two Windows-specific things: (1) Spark's driver binds to the machine's
resolved hostname (here `host.docker.internal`) instead of loopback, so the
Python worker can't connect back; (2) the worker may launch the wrong Python.

**Fix.** The `spark` pytest fixture in `tests/conftest.py` already applies this —
if you build a session by hand, mirror it:
```python
import os, sys
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
SparkSession.builder.master("local[1]") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1")
```
The `winutils.exe` / `HADOOP_HOME` WARN is harmless for in-memory DataFrame unit
tests (no file I/O), and can be ignored. Install the Spark deps with
`pip install "pyspark>=3.5" "chispa>=0.10"` — **not** `--user` (rejected inside
the `.venv`).
