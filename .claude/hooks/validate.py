#!/usr/bin/env python
"""Post-edit validation hook.

Runs after every Edit/Write/MultiEdit so the agent finds its own breakage
instead of pushing it. Exit code 2 feeds stderr back as a correction prompt;
exit 0 stays silent.

Checks, cheapest first:
  1. ruff on scripts/ and .claude/hooks/
  2. every YAML under metadata/ and conf/ parses
  3. databricks.yml still validates, if the bundle CLI is available
  4. no hardcoded workspace URLs, tokens or dbfs secrets in tracked files

Wired up in .claude/settings.json under hooks.PostToolUse.

NOTE: run `python .claude/hooks/validate.py` by hand once after setup. On
Windows a bare `python` can hit the Store alias shim, in which case every
check silently no-ops and you never find out.
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent.parent
problems: list[str] = []


def run(cmd: list[str], timeout: int = 120) -> tuple[int, str]:
    try:
        proc = subprocess.run(cmd, cwd=REPO, capture_output=True, text=True, timeout=timeout)
    except FileNotFoundError:
        return 0, ""  # tool not installed - not this hook's job to complain
    except subprocess.TimeoutExpired:
        return 0, ""
    return proc.returncode, (proc.stdout + proc.stderr).strip()


# 1. lint
targets = [str(p) for p in ("scripts", ".claude/hooks", "src") if (REPO / p).is_dir()]
if targets:
    code, out = run([sys.executable, "-m", "ruff", "check", *targets])
    if code != 0 and out:
        problems.append(f"ruff:\n{out}")

# 2. yaml parses
try:
    import yaml

    for folder in ("metadata", "conf", "contracts"):
        d = REPO / folder
        if not d.is_dir():
            continue
        for path in sorted(d.rglob("*.y*ml")):
            try:
                with path.open(encoding="utf-8") as fh:
                    yaml.safe_load(fh)
            except yaml.YAMLError as exc:
                problems.append(f"{path.relative_to(REPO)} is not valid YAML: {exc}")
except ImportError:
    pass

# 3. the bundle still validates
if (REPO / "databricks.yml").exists():
    code, out = run(["databricks", "bundle", "validate"], timeout=180)
    if code != 0 and out:
        problems.append(f"databricks bundle validate failed:\n{out[:3000]}")

# 4. nothing secret committed
SECRET_PATTERNS = [
    (r"dapi[0-9a-f]{32}", "a Databricks personal access token"),
    (r"https://[a-z0-9-]+\.cloud\.databricks\.com", "a hardcoded workspace URL"),
    (r"https://adb-\d+\.\d+\.azuredatabricks\.net", "a hardcoded workspace URL"),
    (r"(?i)(password|client_secret)\s*[=:]\s*['\"][^'\"]{8,}", "an inline credential"),
]
code, out = run(["git", "diff", "--name-only", "HEAD"])
if code == 0 and out:
    for name in out.splitlines():
        path = REPO / name.strip()
        if not path.is_file() or path.suffix in {".png", ".jpg", ".parquet", ".whl"}:
            continue
        try:
            text = path.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue
        for pattern, label in SECRET_PATTERNS:
            if re.search(pattern, text):
                problems.append(
                    f"{name} contains what looks like {label}. "
                    "Workspace URLs belong in databricks.yml targets; "
                    "credentials belong in a Databricks secret scope."
                )
                break

if problems:
    print("\n\n".join(problems), file=sys.stderr)
    sys.exit(2)
