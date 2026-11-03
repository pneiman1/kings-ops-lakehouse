# Conventions

These exist so an agent can work unattended. Each one closes a loop that
otherwise needs a human in it.

---

## 1. Every notebook returns a result

**This is the one that matters.** Without it, work can be submitted but not
observed, and you become the error channel.

Every notebook ends with a structured exit:

```python
import json

result = {
    "notebook": "nb_silver_build",
    "status": "ok",                    # ok | failed
    "tables_written": written,          # whatever the caller needs to verify
    "rows": row_counts,                 # dict of table -> count
    "watermarks": new_watermarks,
    "warnings": warnings,               # list of strings, may be empty
    "duration_seconds": round(elapsed, 1),
}
dbutils.notebook.exit(json.dumps(result))
```

`python scripts/dbxctl.py run <notebook>` retrieves and prints that payload.
A notebook that prints its results instead of returning them is invisible to
anything but a human watching the UI.

Rules:

- Always valid JSON. Never a bare string.
- Always include `status`. A notebook that fails should exit with
  `status: "failed"` and a `reason`, not raise — a raise loses the payload.
- Include counts, not adjectives. `"rows": {"orders": 48213}` is verifiable;
  `"loaded successfully"` is not.
- Wrap the body in try/except so an exception still produces a result:

```python
try:
    ...
except Exception as exc:
    dbutils.notebook.exit(json.dumps({
        "notebook": NAME, "status": "failed",
        "reason": f"{type(exc).__name__}: {exc}",
    }))
```

---

## 2. Config comes from widgets, never from constants

```python
dbutils.widgets.text("env", "dev")
dbutils.widgets.text("catalog", "kings_dev")
ENV = dbutils.widgets.get("env")
CATALOG = dbutils.widgets.get("catalog")
```

Hardcoded catalog names are how dev code writes to prod. The bundle passes
these per target; `dbxctl run --param env=dev` passes them ad hoc.

---

## 3. Environment lives in `databricks.yml` targets, not in code

Workspace URLs, catalog names, cluster sizes, schedules — all in the bundle's
`targets:` block. Nothing environment-specific goes in a notebook or a script.

The hook rejects a commit containing a workspace URL for this reason.

---

## 4. Secrets live in a secret scope

```python
token = dbutils.secrets.get(scope="kings", key="api_token")
```

Never in a notebook, never in a repo file, never in a widget default. The
hook greps for token-shaped strings on every edit.

---

## 5. Metadata drives the pipeline, code stays generic

Table lists, column contracts, load strategies and SQL definitions live in
YAML under `metadata/`. One executor reads them. Adding a table is a YAML
change, not a code change — which means an agent can add a table without
touching logic anyone has to review.

Generated artifacts are never hand-edited. If something generated is wrong,
the generator or its input is wrong.

---

## 6. `/verify` must stay green

One command runs every check: lint, YAML parse, bundle validate, row-count
reconciliation, and whatever else matters. It is the last step of every run
and the first step of the next one.

A check that fails is either a real break or a check that needs updating.
Never a check that gets deleted.

---

## 7. Compute gets stopped

Serverless by default. If a run starts a cluster or warehouse, the same run
stops it, and `dbxctl cost` output goes in the report. See `AUTONOMY.md`.

---

## 8. Failures get written down

Every tooling failure, with its fix, goes in `docs/TROUBLESHOOTING.md` the
moment it's solved. The next session will hit the same thing, and
rediscovering it costs more than recording it.
