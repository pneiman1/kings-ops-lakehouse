# Setup — autonomous Databricks work with Claude Code

Bootstrap order. Each step depends on the one before it.

Everything installs without admin rights.

---

## What you need open

- PowerShell (or a terminal), at the root of the repo
- A browser, signed in to the Databricks workspace

---

## 1. Install the CLI and lint

In the repo root:

```powershell
pip install --user databricks-cli ruff pyyaml
```

If `databricks` isn't recognized afterwards, the user Scripts folder isn't on
PATH. Add it and open a new shell:

```powershell
$userPath = [Environment]::GetEnvironmentVariable("PATH","User")
[Environment]::SetEnvironmentVariable("PATH", "$userPath;$env:APPDATA\Python\Python3xx\Scripts", "User")
```

Write to User scope, not `$env:PATH` — the latter is machine+user combined,
and writing it back to User duplicates every machine entry.

Do **not** work around this by calling the full path to `databricks.exe`. The
permission rules in `.claude/settings.json` match on `Bash(databricks ...)`.
A full-path invocation matches nothing and you get an approval prompt on
every single call, which defeats the entire setup.

## 2. Authenticate

```powershell
databricks auth login --host https://<your-workspace>.cloud.databricks.com
```

This is OAuth in the browser. It writes a profile to `~/.databrickscfg`, and
everything else — the CLI, `dbxctl.py`, the bundle — inherits it. No tokens
in the repo, no second auth path.

Confirm:

```powershell
python scripts/dbxctl.py whoami
```

You should get your username and a list of anything currently running.

## 3. Drop the files in

```
your-repo/
├── .claude/
│   ├── settings.json          permissions + the post-edit hook
│   └── hooks/
│       └── validate.py        lint, YAML, bundle validate, secret scan
├── docs/
│   ├── AUTONOMY.md            the PROCEED / DEFER / STOP contract
│   ├── CONVENTIONS.md         result contract, config, metadata rules
│   ├── TROUBLESHOOTING.md     create it empty; it fills up fast
│   └── BLOCKERS.md            create it empty
├── scripts/
│   └── dbxctl.py              run, sql, whoami, cost, stop
├── databricks.yml             the bundle (step 4)
└── CLAUDE.md                  step 5
```

## 4. Verify the hook actually runs

```powershell
python .claude/hooks/validate.py
```

It should exit silently. If it errors on a missing module, install it.

This matters more than it looks: the hook is non-blocking by design, so if
`python` resolves to the Windows Store alias shim, every check silently
no-ops and you'll never find out. Run it by hand once.

## 5. Initialize the bundle

```powershell
databricks bundle init default-python
```

Then edit `databricks.yml` so the targets are real:

```yaml
targets:
  dev:
    mode: development
    default: true
    workspace:
      host: https://<workspace>.cloud.databricks.com
    variables:
      catalog: kings_dev
  prod:
    mode: production
    workspace:
      host: https://<workspace>.cloud.databricks.com
      root_path: /Shared/.bundle/prod/${bundle.name}
    variables:
      catalog: kings_prod
```

`mode: development` prefixes every deployed job with your username and pauses
schedules, so dev deploys can't collide with prod or fire on their own.

Confirm:

```powershell
databricks bundle validate
databricks bundle deploy -t dev
```

## 6. Write CLAUDE.md

This is the file the agent reads on every session. Keep it short — it's a
pointer, not a manual:

```markdown
# <project> — working notes

Databricks medallion on Unity Catalog. Bronze/silver/gold in `kings_dev`.

## Before you start
- Read docs/AUTONOMY.md. Most stops are unnecessary.
- Read docs/CONVENTIONS.md. Every notebook returns JSON via
  dbutils.notebook.exit — that's how I see what you did.
- Check docs/TROUBLESHOOTING.md before debugging anything.

## How to run things
- A notebook:  python scripts/dbxctl.py run <path> --param env=dev
- A query:     python scripts/dbxctl.py sql "select ..."
- What's up:   python scripts/dbxctl.py cost
- Deploy:      databricks bundle deploy -t dev

## Rules
- Serverless unless there's a reason. Stop anything you start.
- Never deploy or run against -t prod.
- Metadata in YAML, logic generic. Adding a table is a YAML change.
- /verify must be green before you call something done.
```

## 7. Run it

Normal work:

```powershell
claude
```

Unattended, hours at a time:

```powershell
claude --dangerously-skip-permissions
```

That second one bypasses the deny list as well as the prompts, so the
destructive commands listed in `.claude/settings.json` become live. The STOP
list in `AUTONOMY.md` is then the only guard. Worth knowing before you use
it, not after.

---

## The thing that makes this work

It isn't the permissions file and it isn't MCP servers. It's that the agent
can **run work and see the result**. `dbxctl run` submits a notebook, waits,
and prints what the notebook returned. Without that the agent writes code
blind and stops every few minutes to ask you what happened.

Everything else here is support for that one loop.
