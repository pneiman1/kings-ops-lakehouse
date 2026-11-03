"""Shared pytest fixtures.

``conf_dir`` points at the real ``conf/`` directory rather than a synthetic
fixture. That is deliberate: these tests assert on the configuration this
platform actually deploys, so a bad edit to conf/prd.yml breaks the build.
Tests against fabricated config would prove only that the loader loads.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def conf_dir() -> Path:
    return REPO_ROOT / "conf"


@pytest.fixture(scope="session")
def spark():
    """A local SparkSession for transform unit tests.

    ``pytest.importorskip`` turns the whole ``@pytest.mark.spark`` suite into a
    clean skip wherever pyspark is not installed (e.g. a lint-only CI job), so
    the no-cluster promise in the charter holds without the tests lying.

    The driver is pinned to loopback and the worker to this interpreter: on
    Windows, Spark otherwise binds to the machine hostname and the Python worker
    cannot connect back. See docs/TROUBLESHOOTING.md.
    """
    pytest.importorskip("pyspark")
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("kings-unit")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


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
