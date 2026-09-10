# Windows Setup — Kings Ops Lakehouse

PowerShell. Run one step at a time. Stop at any failure.

---

## 1. Extract

Download `kings-ops-lakehouse.tar.gz` to your Downloads folder, then:

```powershell
mkdir -Force $HOME\projects\kings-ops-lakehouse
cd $HOME\projects\kings-ops-lakehouse
tar xzf $HOME\Downloads\kings-ops-lakehouse.tar.gz
ls
```

**Want to see:** `databricks.yml`, `conf`, `docs`, `notebooks`, `pipelines`, `pyproject.toml`, `resources`, `sql`, `src`, `tests`, `tools`, `START_HERE.md`

The tarball has no wrapper folder, so `mkdir` first then extract into it. Skipping the
mkdir spills files into your projects directory.

---

## 2. Check Python

```powershell
python --version
```

**Want:** 3.10 or higher.

Not found? Install from python.org and tick **Add python.exe to PATH** during setup.
Then close and reopen PowerShell.

---

## 3. Virtual environment

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
```

**Want:** your prompt now starts with `(.venv)`

If you get an execution-policy error:
```powershell
Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
```
Answer `Y`, then retry the activate line.

> You need the activate line in **every new PowerShell window**. It does not persist.

---

## 4. Install the project

```powershell
pip install --upgrade pip
pip install -e ".[dev]"
```

**Want:** ends with `Successfully installed ... kings-ops-0.1.0 ...`

---

## 5. Run the tests

```powershell
pytest tests/unit -q
```

**Want:** a row of dots ending in `100%`. That is a pass — `-q` shows progress, not a count.

Want the actual number? `pytest tests/unit` without `-q` prints `45 passed`.

`ModuleNotFoundError: No module named 'kings_ops'` means the venv is not active. Redo step 3.

---

## 6. Install the Databricks CLI

```powershell
winget install Databricks.DatabricksCLI
```

**Then close PowerShell and open a new window**, `cd` back to the project, and re-activate:

```powershell
cd $HOME\projects\kings-ops-lakehouse
.venv\Scripts\Activate.ps1
databricks --version
```

**Want:** `v0.2xx.x` or higher.

If it prints `0.17.x` that is the legacy Python CLI, which cannot do bundles at all.
Remove it with `pip uninstall databricks-cli -y` and redo this step.

---

## 7. Authenticate

```powershell
databricks auth login --host https://dbc-c59f3ffd-08c3.cloud.databricks.com
```

A browser opens — approve it. When it asks for a profile name, type:

```
kings
```

**Want:** `Profile 'kings' was successfully saved`

---

## 8. Verify auth

```powershell
$env:DATABRICKS_CONFIG_PROFILE="kings"
databricks current-user me
```

**Want:** JSON containing your email.

> `$env:` only lasts for this window. To make it permanent:
> ```powershell
> [Environment]::SetEnvironmentVariable("DATABRICKS_CONFIG_PROFILE","kings","User")
> ```

---

## 9. Validate the bundle

```powershell
databricks bundle validate --target dev
```

**Want:** a summary with `Name: kings-ops-lakehouse`, `Target: dev`, and `Validation OK!`

This touches nothing in the workspace. It only checks your configuration.

---

## 10. See environment isolation work

Worth ten seconds — it is the core idea behind the whole CI/CD setup.

```powershell
databricks bundle validate --target dev --output json | Select-String -Pattern '"catalog"' | Select-Object -First 1
databricks bundle validate --target prd --output json | Select-String -Pattern '"catalog"' | Select-Object -First 1
```

**Want:** `kings_dev` from the first, `kings_prd` from the second.

Same code, same repo, zero file edits. That is bundle variable resolution.

---

## 11. Deploy

```powershell
databricks bundle deploy --target dev
```

**Want:** upload progress, then `Deployment complete!`

Your files went to `/Workspace/Users/<you>/.bundle/kings-ops-lakehouse/dev/`, and the
schemas, volumes, jobs, and bronze pipeline were created.

**Check in the browser:** left nav → **Jobs & Pipelines**. You should see jobs prefixed
with your username: `[dev] bootstrap`, `[dev] smoke test`, and the `[dev] bronze ingestion`
pipeline.

The `kings_dev` catalog will not exist yet. That is expected — the next step creates it.

---

## 12. Create the catalog and generate the Kings data

```powershell
databricks bundle run bootstrap --target dev
```

Two tasks, a few minutes:
1. `create_catalog` — creates `kings_dev`
2. `generate_source_data` — writes the synthetic Kings season into the landing Volume

**Want:** a run URL printed, then both tasks succeeding.

**If `generate_source_data` fails on package installs:** Free Edition restricts outbound
internet until you verify with LinkedIn. That is the cause — not the code.

---

## 13. Verify the data landed

Browser: **Catalog** → `kings_dev` → `landing` → **Volumes** → `files`

**Want to see:** `crm_accounts`, `deposits`, `gate_scans`, `guest_services`, `pos`,
`pricing`, `schedule`, `seat_manifest`, `sponsorship`, `ticketing`, and `_manifest.json`

Open `_manifest.json` — it has row counts per dataset. Keep it handy; M2 reconciles
against those numbers.

---

## 14. Smoke test

```powershell
databricks bundle run smoke_test --target dev
```

**Want:** passes, writing one row to `kings_dev.ops.deploy_smoke`.

Failing on the six-schema assertion means the deploy did not finish. Redo step 11.

---

## 15. Look at the raw data

**SQL Editor** in the workspace:

```sql
SELECT * FROM read_files(
  '/Volumes/kings_dev/landing/files/gate_scans',
  format => 'json'
) LIMIT 20;
```

Look for `scan_device_os` — null on early games, populated later. That is the deliberate
mid-season schema drift. Seeing it before you build ingestion is the point.

---

## Done. What next.

You now have: code deployed, catalog created, Kings data in a Volume, smoke test green.

Next is **`docs/modules/M1_platform.md`** — platform fundamentals, mostly recall if you
have the Associate. Then `docs/modules/M2_ingestion.md`.

Full plan: `docs/LEARNING_PATH.md`

---

## Commands you will use constantly

```powershell
.venv\Scripts\Activate.ps1                       # every new window
pytest tests/unit -q                             # before every commit
databricks bundle validate --target dev          # before every deploy
databricks bundle deploy --target dev            # push code to workspace
databricks bundle run <job_key> --target dev     # run a job
databricks bundle summary --target dev           # what is deployed right now
```
