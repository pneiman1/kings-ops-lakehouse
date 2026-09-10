"""Shared pytest fixtures.

``conf_dir`` points at the real ``conf/`` directory rather than a synthetic
fixture. That is deliberate: these tests assert on the configuration this
platform actually deploys, so a bad edit to conf/prd.yml breaks the build.
Tests against fabricated config would prove only that the loader loads.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def conf_dir() -> Path:
    return REPO_ROOT / "conf"


@pytest.fixture(autouse=True)
def _clear_kings_env(monkeypatch):
    """Strip KINGS_* from the environment before each test.

    Without this, a developer with KINGS_CATALOG exported in their shell gets
    different results locally than CI does — the single most confusing class of
    flaky test there is.
    """
    for key in list(os.environ):
        if key.startswith("KINGS_"):
            monkeypatch.delenv(key, raising=False)
