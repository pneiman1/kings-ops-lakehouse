# Setup Runbook — Zero to First Deploy

Every step, in order, with the exact command or click, what you should see, and what to do when it goes wrong.

**Do not skip ahead.** Several steps produce values later steps need.

**Where each step happens** is marked, because this is the single most common source of confusion:
- 🖥️ **Terminal** — your local shell (WSL2 Ubuntu, macOS Terminal, etc.)
- 🌐 **Browser** — the Databricks or GitHub web UI
- 📝 **Editor** — a file on your machine

---

## Phase 0 — Local environment

### Step 0.1 🖥️ Confirm your shell and OS

```bash
uname -a
lsb_release -a 2>/dev/null || sw_vers
```

**Expect:** Linux with Ubuntu 22.04/24.04, or macOS. Anything works; you just need to know which, because paths differ.

### Step 0.2 🖥️ Confirm Python 3.10+

```bash
python3 --version
```

**Expect:** `Python 3.10.x` or higher.

**If missing or too old (Ubuntu):**
```bash
sudo apt update && sudo apt install -y python3 python3-pip python3-venv
```

### Step 0.3 🖥️ Confirm git

```bash
git --version
git config --global user.name
git config --global user.email
```

**If the last two print nothing:**
```bash
git config --global user.name "Phil Neiman"
git config --global user.email "your@email.com"
```

Use the email attached to your GitHub account or your commits will not link to your profile — which matters, because the contribution graph is part of what a hiring manager looks at.

### Step 0.4 🖥️ Create the project directory and extract the repo

```bash
mkdir -p ~/projects && cd ~/projects
tar xzf ~/Downloads/kings-ops-lakehouse.tar.gz --one-top-level=kings-ops-lakehouse
cd kings-ops-lakehouse
ls -la
```

**Expect:** `databricks.yml`, `conf/`, `src/`, `tests/`, `tools/`, `docs/`, `.github/`.

### Step 0.5 🖥️ Create a virtual environment and install

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -e ".[dev]"
```

**Expect:** ends with `Successfully installed ... kings-ops-0.1.0 ...`

**Note the `source` line.** Every new terminal session needs it again, or you will get import errors and blame the code.

### Step 0.6 🖥️ Prove the test suite runs

```bash
pytest tests/unit -v
```

**Expect:** 25 passed.

**If you see `ModuleNotFoundError: No module named 'kings_ops'`:** the venv is not active. Re-run `source .venv/bin/activate`.

✅ **Checkpoint:** you can run the tests locally. Nothing touches Databricks yet.

---

## Phase 1 — Databricks Free Edition account

### Step 1.1 🌐 Sign up

Go to `https://signup.databricks.com` and choose **Free Edition** (not the free trial — they are different products, and the trial expires while Free Edition does not).

Sign in with Google, Microsoft, or email OTP. There is no SSO on Free Edition.

### Step 1.2 🌐 Verify with LinkedIn — do not skip this

In the workspace, look for a **Verify with LinkedIn** option in account settings.

**Why it matters:** Free Edition restricts outbound internet access to a limited set of trusted domains until you verify. Verification unlocks outbound internet access and limited serverless GPU. Several later steps depend on it.

### Step 1.3 🌐 Record your workspace URL

Look at your browser address bar. It reads something like:

```
https://dbc-a1b2c3d4-e5f6.cloud.databricks.com/
```

**Write this down.** Referred to below as `<WORKSPACE_URL>`. Include `https://`, no trailing path.

### Step 1.4 🌐 Find your SQL warehouse name

Left nav → **SQL Warehouses**.

**Expect:** exactly one warehouse. Free Edition allows one, limited to `2X-Small`.

**Record its exact name.** Usually `Serverless Starter Warehouse`. If yours differs, you must edit `databricks.yml` in Step 3.3 — the bundle resolves the warehouse by name, not by ID.

### Step 1.5 🌐 Confirm Unity Catalog is present

Left nav → **Catalog**.

**Expect:** at least `samples` and a default catalog. Free Edition gives you one metastore and you are its admin, which is why you can create catalogs later.

✅ **Checkpoint:** you have a workspace URL and a warehouse name.

---

## Phase 2 — CLI and authentication

### Step 2.1 🖥️ Install the Databricks CLI

```bash
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sudo sh
databricks --version
```

**Expect:** `Databricks CLI v0.2xx.x` or higher.

**Critical:** if it prints something like `0.17.x` you have the *legacy Python* CLI, which does not support bundles at all. Remove it:
```bash
pip uninstall databricks-cli -y
```
then re-run the install above and re-check the version.

### Step 2.2 🖥️ Authenticate

```bash
databricks auth login --host <WORKSPACE_URL>
```

A browser opens for OAuth. Approve it. When prompted for a profile name, enter:

```
kings
```

**Expect:** `Profile 'kings' was successfully saved`.

### Step 2.3 🖥️ Verify authentication

```bash
databricks current-user me --profile kings
```

**Expect:** JSON containing your `userName` and email.

**If it fails with 401:** re-run Step 2.2. OAuth tokens on Free Edition expire and re-auth is normal.

### Step 2.4 🖥️ Set the profile as default for this project

```bash
export DATABRICKS_CONFIG_PROFILE=kings
echo 'export DATABRICKS_CONFIG_PROFILE=kings' >> ~/.bashrc
```

✅ **Checkpoint:** the CLI can talk to your workspace.

---

## Phase 3 — First bundle deploy

### Step 3.1 🖥️ Validate before deploying

```bash
cd ~/projects/kings-ops-lakehouse
databricks bundle validate --target dev
```

**Expect:** a summary showing `Name: kings-ops-lakehouse`, `Target: dev`, your host, and `Validation OK!`

**This step catches config errors without touching the workspace.** Get in the habit — it is also exactly what the exam's CI/CD section tests.

### Step 3.2 🖥️ Read the resolved configuration

```bash
databricks bundle validate --target dev --output json | head -60
```

Look at what `${var.catalog}` resolved to. It should be `kings_dev`. Now try:

```bash
databricks bundle validate --target prd --output json | grep -A2 '"catalog"'
```

It should resolve to `kings_prd`. **That difference — same code, different target, different catalog — is the entire point of bundle variables**, and it is a directly tested exam objective.

### Step 3.3 📝 Fix the warehouse name if needed

If Step 1.4 gave you a warehouse name other than `Serverless Starter Warehouse`, open `databricks.yml` and edit:

```yaml
  sql_warehouse_id:
    lookup:
      warehouse: "YOUR EXACT WAREHOUSE NAME"
```

Then re-run Step 3.1.

### Step 3.4 🖥️ Deploy to dev

```bash
databricks bundle deploy --target dev
```

**Expect:** upload progress, then `Deployment complete!`

**What just happened:** the CLI uploaded your files to `/Workspace/Users/<you>/.bundle/kings-ops-lakehouse/dev/`, then created the schemas, volumes, and jobs defined in `resources/`. Because the dev target uses `mode: development`, resource names are prefixed with your username and all schedules are paused.

### Step 3.5 🌐 See it in the workspace

Left nav → **Workflows**. You should see two jobs prefixed with your name: `[dev] bootstrap` and `[dev] smoke test`.

Left nav → **Catalog**. You may not see `kings_dev` yet — the catalog itself is created by the bootstrap job's first task, not by the bundle. That is Step 4.

✅ **Checkpoint:** your code is deployed. No data exists yet.

---

## Phase 4 — Create the catalog and land data

### Step 4.1 🖥️ Run the bootstrap job

```bash
databricks bundle run bootstrap --target dev
```

This runs two tasks in sequence:
1. `create_catalog` — creates `kings_dev` and tags it
2. `generate_source_data` — runs the generator at `small` scale into the landing volume

**Expect:** a run URL printed, then task-by-task progress. Takes a few minutes.

**If `create_catalog` fails with a permissions error:** you are not metastore admin. On Free Edition the signing-up user normally is. Confirm you are signed in as the account that created the workspace.

**If `generate_source_data` fails on imports:** the serverless environment spec in `resources/jobs/bootstrap.yml` declares pandas, numpy, and pyarrow. Check the task logs to see which one did not resolve — this is also where a missing LinkedIn verification (Step 1.2) shows up, since dependency resolution needs outbound access.

### Step 4.2 🌐 Verify the catalog and schemas

Left nav → **Catalog** → `kings_dev`.

**Expect six schemas:** `landing`, `bronze`, `silver`, `gold`, `quarantine`, `ops`.

### Step 4.3 🌐 Verify the landing data

Catalog → `kings_dev` → `landing` → **Volumes** → `files`.

**Expect these directories:** `crm_accounts`, `deposits`, `gate_scans`, `guest_services`, `pos`, `pricing`, `schedule`, `seat_manifest`, `sponsorship`, `ticketing`, plus `_manifest.json`.

Open `_manifest.json`. It has row counts per dataset. **Keep this open** — the bronze reconciliation in T2 checks against these numbers.

### Step 4.4 🌐 Look at the raw data with your own eyes

Open a SQL editor (left nav → **SQL Editor**) and run:

```sql
SELECT * FROM read_files(
  '/Volumes/kings_dev/landing/files/gate_scans',
  format => 'json'
) LIMIT 20;
```

**Look for:** the `scan_device_os` column being null on early games and populated later. That is the deliberate schema drift. Seeing it before you build the ingestion is the point.

✅ **Checkpoint:** synthetic data exists in a UC Volume.

---

## Phase 5 — Smoke test and GitHub

### Step 5.1 🖥️ Run the smoke test

```bash
databricks bundle run smoke_test --target dev
```

**Expect:** passes, writing one row to `kings_dev.ops.deploy_smoke`.

**If it fails asserting six schemas:** the bundle deploy did not complete. Re-run Step 3.4.

### Step 5.2 🌐 Create the GitHub repository

On GitHub: **New repository** → name `kings-ops-lakehouse` → **Public** (it is a portfolio; private defeats the purpose) → do **not** initialize with a README, since you already have files.

### Step 5.3 🖥️ Push

```bash
cd ~/projects/kings-ops-lakehouse
git init
git add .
git commit -m "T0: project charter, synthetic data generator, UC layout

T1: bundle targets for dev/stg/prd on catalog-per-environment,
typed config loader, structured logging, 25 unit tests, CI gates."
git branch -M main
git remote add origin https://github.com/<your-username>/kings-ops-lakehouse.git
git push -u origin main
```

**Before pushing, confirm nothing sensitive is staged:**
```bash
git status
```
There must be no `.databrickscfg`, no `.venv/`, no `landing/`, no tokens. `.gitignore` covers these, but check anyway — a leaked token in git history is genuinely painful to remove.

### Step 5.4 🌐 Create a Databricks personal access token for CI

In Databricks: **Settings** → **Developer** → **Access tokens** → **Generate new token**.

Comment: `github-actions`. Lifetime: 90 days.

**Copy the token now.** It is shown exactly once.

> **Why a PAT and not OIDC:** service principals are account-level objects, and Free Edition has no account console access, so OIDC federation is unavailable. In an enterprise workspace you would use a service principal with OIDC and no long-lived secret. Note this tradeoff in your README — it is the kind of thing an interviewer likes hearing you volunteer.

### Step 5.5 🌐 Add GitHub secrets

Repo → **Settings** → **Secrets and variables** → **Actions** → **New repository secret**.

| Name | Value |
|---|---|
| `DATABRICKS_HOST` | your `<WORKSPACE_URL>` |
| `DATABRICKS_TOKEN` | the token from 5.4 |

### Step 5.6 🌐 Create the protected environments

Repo → **Settings** → **Environments** → **New environment** → `staging`. No protection rules.

Then **New environment** → `production` → check **Required reviewers** → add yourself.

**Why:** `deploy.yml` targets these environments. The required reviewer on `production` is the only thing standing between a bad merge and prod. Configuring it is what makes the CI story real rather than decorative.

### Step 5.7 🖥️ Prove CI works by opening a PR

```bash
git checkout -b chore/verify-ci
echo "" >> README.md
git commit -am "chore: verify CI pipeline"
git push -u origin chore/verify-ci
```

Open the PR on GitHub. Watch the **Actions** tab.

**Expect:** `static-analysis`, `unit-tests`, and `bundle-validate` (three parallel jobs, one per target) all green.

**If `bundle-validate` fails on all three targets:** your secrets are wrong. Check for a trailing slash or missing `https://` on `DATABRICKS_HOST`.

✅ **Checkpoint:** code on GitHub, CI green, data in the lakehouse, deploy working end to end.

---

## Phase 6 — Deliberately break it

Do this. It takes ten minutes and it is worth more than the previous five phases combined.

### Step 6.1 📝 Introduce a config error

Open `conf/prd.yml` and change:

```yaml
catalog: kings_prd
```
to
```yaml
catalogue: kings_prd
```

### Step 6.2 🖥️ Watch the test suite catch it

```bash
pytest tests/unit -v
```

**Expect:** `test_typo_in_config_key_is_rejected_not_ignored` fails with `unknown configuration keys: ['catalogue']`.

**The lesson:** the loader raises on unknown keys rather than ignoring them. A silently-dropped typo is how a pipeline runs six weeks against the wrong catalog before anyone notices. Reverting a bad config is easy; discovering it is not.

### Step 6.3 🖥️ Revert and confirm green

```bash
git checkout conf/prd.yml
pytest tests/unit -q
```

---

## Where things live — quick reference

| Thing | Location |
|---|---|
| CLI auth profile | `~/.databrickscfg` |
| Deployed bundle files | `/Workspace/Users/<you>/.bundle/kings-ops-lakehouse/dev/` |
| Landing data | `/Volumes/kings_dev/landing/files/` |
| Streaming checkpoints | `/Volumes/kings_dev/ops/checkpoints/` |
| Deploy audit trail | `kings_dev.ops.deploy_smoke` |

## Commands you will run constantly

```bash
source .venv/bin/activate                        # every new shell
pytest tests/unit -q                             # before every commit
databricks bundle validate --target dev          # before every deploy
databricks bundle deploy --target dev            # push code to workspace
databricks bundle run <job_key> --target dev     # execute a job
databricks bundle summary --target dev           # what is deployed right now
```
