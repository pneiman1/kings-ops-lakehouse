#!/usr/bin/env python
"""Databricks operations that close the feedback loop for autonomous work.

Shells out to the `databricks` CLI, so authentication is whatever
`databricks auth login` left in ~/.databrickscfg - no second token path,
no secrets in this repo.

The point of this script is RESULT RETRIEVAL. Without it, an agent can submit
work but can't see what happened, and you become the error channel.

Subcommands:
    run       run a notebook or job, wait, print its returned result
    sql       run a query on a SQL warehouse and print rows as JSON
    whoami    print the resolved workspace, user, and what's running
    cost      list running clusters and warehouses with their uptime
    stop      terminate a cluster or stop a warehouse by name

Examples, run from the repo root:
    python scripts/dbxctl.py whoami
    python scripts/dbxctl.py run notebooks/nb_silver_build --param env=dev
    python scripts/dbxctl.py run --job-key silver_pipeline
    python scripts/dbxctl.py sql "select count(*) from main.silver.orders"
    python scripts/dbxctl.py cost
    python scripts/dbxctl.py stop --cluster dev-shared
"""

from __future__ import annotations

import argparse
import configparser
import json
import os
import subprocess
import sys
import time
from pathlib import Path

POLL_SECONDS = 10
RUN_TIMEOUT_SECONDS = 7200
TERMINAL_STATES = {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}


def dbx(*args: str, parse: bool = True, timeout: int = 300):
    """Call the databricks CLI. Returns parsed JSON when the response is JSON."""
    cmd = ["databricks", *args]
    profile = os.environ.get("DATABRICKS_PROFILE")
    if profile and "--profile" not in args:
        cmd += ["--profile", profile]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except FileNotFoundError:
        sys.exit(
            "databricks CLI not found.\n"
            "  Install:  winget install --id Databricks.DatabricksCLI -e\n"
            "  Auth:     databricks auth login --host https://<workspace>.cloud.databricks.com"
        )
    except subprocess.TimeoutExpired:
        sys.exit(f"databricks {' '.join(args)} timed out after {timeout}s")

    out = (proc.stdout or "").strip()
    err = (proc.stderr or "").strip()

    if proc.returncode != 0:
        sys.exit(f"databricks {' '.join(args)} failed (exit {proc.returncode}):\n{err or out}")

    if not parse or not out:
        return out
    try:
        return json.loads(out)
    except json.JSONDecodeError:
        return out


def api(method: str, path: str, body: dict | None = None, timeout: int = 300):
    """Generic REST passthrough, for anything the typed commands don't cover."""
    args = ["api", method.lower(), path]
    if body is not None:
        args += ["--json", json.dumps(body)]
    return dbx(*args, timeout=timeout)


# ---------------------------------------------------------------- run


def _submit_notebook(path: str, params: dict[str, str], cluster: str | None) -> int:
    task: dict = {
        "task_key": "adhoc",
        "notebook_task": {"notebook_path": path, "base_parameters": params},
    }
    if cluster:
        task["existing_cluster_id"] = cluster
    else:
        # Serverless. Cheapest default: nothing to leave running by accident.
        task["environment_key"] = "default"

    body: dict = {
        "run_name": f"dbxctl {path}",
        "tasks": [task],
    }
    if not cluster:
        body["environments"] = [
            {"environment_key": "default", "spec": {"client": "2"}}
        ]

    created = api("post", "/api/2.2/jobs/runs/submit", body)
    run_id = created.get("run_id") if isinstance(created, dict) else None
    if not run_id:
        sys.exit(f"no run_id came back from submit:\n{json.dumps(created, indent=2)}")
    return int(run_id)


def _run_now(job_key: str, params: dict[str, str]) -> int:
    jobs = api("get", "/api/2.2/jobs/list?limit=100")
    matches = [
        j for j in (jobs.get("jobs") or [])
        if j.get("settings", {}).get("name") == job_key
    ]
    if not matches:
        names = sorted(j.get("settings", {}).get("name", "?") for j in (jobs.get("jobs") or []))
        sys.exit(f"job {job_key!r} not found. Available: {names}")
    job_id = matches[0]["job_id"]
    body = {"job_id": job_id}
    if params:
        body["notebook_params"] = params
    created = api("post", "/api/2.2/jobs/run-now", body)
    return int(created["run_id"])


def _wait(run_id: int) -> dict:
    print(f"run {run_id} submitted")
    deadline = time.time() + RUN_TIMEOUT_SECONDS
    last = ""
    while time.time() < deadline:
        state = api("get", f"/api/2.2/jobs/runs/get?run_id={run_id}")
        st = state.get("state", {})
        life = st.get("life_cycle_state", "UNKNOWN")
        if life in TERMINAL_STATES:
            return state
        if life != last:
            print(f"  {life}...")
            last = life
        time.sleep(POLL_SECONDS)
    sys.exit(f"timed out after {RUN_TIMEOUT_SECONDS}s, last state {last}")


def cmd_run(args: argparse.Namespace) -> None:
    params = dict(p.split("=", 1) for p in (args.param or []))

    if args.job_key:
        run_id = _run_now(args.job_key, params)
    else:
        if not args.notebook:
            sys.exit("give a notebook path or --job-key")
        run_id = _submit_notebook(args.notebook, params, args.cluster)

    state = _wait(run_id)
    st = state.get("state", {})
    result_state = st.get("result_state", "UNKNOWN")
    print(f"finished: {result_state}")

    # THE IMPORTANT PART. A notebook ending in
    #     dbutils.notebook.exit(json.dumps({...}))
    # puts its structured result here. This is the feedback loop.
    tasks = state.get("tasks") or []
    for task in tasks:
        task_run_id = task.get("run_id")
        if not task_run_id:
            continue
        output = api("get", f"/api/2.2/jobs/runs/get-output?run_id={task_run_id}")
        payload = output.get("notebook_output", {}).get("result")
        if payload:
            try:
                print(json.dumps(json.loads(payload), indent=2))
            except json.JSONDecodeError:
                print(payload)
        if output.get("error"):
            print(f"\nERROR: {output['error']}", file=sys.stderr)
            trace = output.get("error_trace")
            if trace:
                print(trace[:4000], file=sys.stderr)

    url = state.get("run_page_url")
    if url:
        print(f"\n{url}")

    if result_state != "SUCCESS":
        sys.exit(1)


# ---------------------------------------------------------------- sql


def cmd_sql(args: argparse.Namespace) -> None:
    warehouse = args.warehouse or os.environ.get("DATABRICKS_WAREHOUSE_ID")
    if not warehouse:
        whs = api("get", "/api/2.0/sql/warehouses")
        running = [w for w in (whs.get("warehouses") or []) if w.get("state") == "RUNNING"]
        pool = running or (whs.get("warehouses") or [])
        if not pool:
            sys.exit("no SQL warehouse found. Pass --warehouse or set DATABRICKS_WAREHOUSE_ID")
        warehouse = pool[0]["id"]
        print(f"# using warehouse {pool[0].get('name')} ({warehouse})", file=sys.stderr)

    body = {
        "warehouse_id": warehouse,
        "statement": args.query,
        "wait_timeout": "50s",
        "format": "JSON_ARRAY",
        "disposition": "INLINE",
    }
    res = api("post", "/api/2.0/sql/statements", body)

    status = res.get("status", {})
    state = status.get("state")
    statement_id = res.get("statement_id")

    deadline = time.time() + 600
    while state in {"PENDING", "RUNNING"} and time.time() < deadline:
        time.sleep(3)
        res = api("get", f"/api/2.0/sql/statements/{statement_id}")
        state = res.get("status", {}).get("state")

    if state != "SUCCEEDED":
        err = res.get("status", {}).get("error", {})
        sys.exit(f"query {state}: {err.get('message', res)}")

    schema = res.get("manifest", {}).get("schema", {}).get("columns", [])
    names = [c["name"] for c in schema]
    rows = res.get("result", {}).get("data_array") or []
    print(json.dumps({"columns": names, "rows": rows, "row_count": len(rows)}, indent=2))


# ---------------------------------------------------------------- whoami / cost


def _config_host() -> str | None:
    """Read the workspace host from ~/.databrickscfg for the active profile.

    Mirrors how dbx() picks a profile: DATABRICKS_PROFILE if set, else DEFAULT.
    Reading the file directly beats shelling out to the deprecated
    `databricks auth env`, which resolves the profile independently (it needs
    DATABRICKS_CONFIG_PROFILE) and so fails in a shell where only
    DATABRICKS_PROFILE is set — even though the api() call above just succeeded.
    """
    cfg_path = Path.home() / ".databrickscfg"
    if not cfg_path.exists():
        return None
    parser = configparser.ConfigParser()
    try:
        parser.read(cfg_path, encoding="utf-8")
    except configparser.Error:
        return None
    profile = os.environ.get("DATABRICKS_PROFILE", "DEFAULT")
    # configparser treats [DEFAULT] specially: its keys live in defaults(),
    # not in a named section, so has_section("DEFAULT") is False.
    if profile == "DEFAULT":
        return parser.defaults().get("host")
    if parser.has_section(profile):
        return parser.get(profile, "host", fallback=None)
    return None


def cmd_whoami(args: argparse.Namespace) -> None:
    me = api("get", "/api/2.0/preview/scim/v2/Me")
    print(f"user: {me.get('userName', '?')}")
    host = _config_host()
    if host:
        print(f"host: {host}")
    cmd_cost(args)


def _age(started_ms) -> str:
    if not started_ms:
        return "?"
    mins = int((time.time() * 1000 - started_ms) / 60000)
    return f"{mins // 60}h{mins % 60:02d}m"


def cmd_cost(args: argparse.Namespace) -> None:
    clusters = api("get", "/api/2.1/clusters/list")
    live = [
        c for c in (clusters.get("clusters") or [])
        if c.get("state") in {"RUNNING", "PENDING", "RESIZING"}
    ]
    print("\nrunning clusters:")
    if not live:
        print("  (none)")
    for c in live:
        auto = c.get("autotermination_minutes")
        flag = "" if auto else "   <-- NO AUTO-TERMINATION"
        print(
            f"  {c.get('cluster_name')}  {c.get('state')}  "
            f"up {_age(c.get('start_time'))}  "
            f"auto-term {auto or 'off'}min{flag}"
        )

    whs = api("get", "/api/2.0/sql/warehouses")
    live_wh = [w for w in (whs.get("warehouses") or []) if w.get("state") == "RUNNING"]
    print("\nrunning warehouses:")
    if not live_wh:
        print("  (none)")
    for w in live_wh:
        print(
            f"  {w.get('name')}  {w.get('cluster_size')}  "
            f"auto-stop {w.get('auto_stop_mins', '?')}min"
        )


def cmd_stop(args: argparse.Namespace) -> None:
    if args.cluster:
        clusters = api("get", "/api/2.1/clusters/list")
        match = [
            c for c in (clusters.get("clusters") or [])
            if c.get("cluster_name") == args.cluster
        ]
        if not match:
            sys.exit(f"cluster {args.cluster!r} not found")
        api("post", "/api/2.1/clusters/delete", {"cluster_id": match[0]["cluster_id"]})
        print(f"terminated {args.cluster}")
    if args.warehouse:
        whs = api("get", "/api/2.0/sql/warehouses")
        match = [w for w in (whs.get("warehouses") or []) if w.get("name") == args.warehouse]
        if not match:
            sys.exit(f"warehouse {args.warehouse!r} not found")
        api("post", f"/api/2.0/sql/warehouses/{match[0]['id']}/stop")
        print(f"stopped {args.warehouse}")


# ---------------------------------------------------------------- main


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("run", help="run a notebook or job and print its result")
    p.add_argument("notebook", nargs="?", help="workspace notebook path")
    p.add_argument("--job-key", help="run an existing job by name instead")
    p.add_argument("--cluster", help="existing cluster id (default: serverless)")
    p.add_argument("--param", action="append", help="key=value, repeatable")
    p.set_defaults(func=cmd_run)

    p = sub.add_parser("sql", help="run a query and print rows as JSON")
    p.add_argument("query")
    p.add_argument("--warehouse", help="warehouse id")
    p.set_defaults(func=cmd_sql)

    p = sub.add_parser("whoami", help="who am I and what is running")
    p.set_defaults(func=cmd_whoami)

    p = sub.add_parser("cost", help="list running compute")
    p.set_defaults(func=cmd_cost)

    p = sub.add_parser("stop", help="terminate a cluster or stop a warehouse")
    p.add_argument("--cluster")
    p.add_argument("--warehouse")
    p.set_defaults(func=cmd_stop)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
