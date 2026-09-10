"""
Typed, layered configuration for the Kings Ops Lakehouse.

Configuration precedence, lowest to highest:

    conf/base.yml  ->  conf/{env}.yml  ->  environment variables  ->  explicit args

Why this exists rather than reading widget values inline
-------------------------------------------------------
Notebooks that scatter ``dbutils.widgets.get("catalog")`` across forty cells
cannot be unit tested, cannot be validated before deploy, and fail at runtime
with a KeyError instead of at load time with a useful message. Every setting in
this platform resolves through one typed object that validates itself on
construction. If the config is wrong, the job fails in the first second with a
message naming the offending key — not in minute nine of a Spark run.

The loader is deliberately free of any Databricks import so it can be exercised
by pytest on a laptop with no cluster and no workspace.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

import yaml

VALID_ENVIRONMENTS = ("dev", "stg", "prd")

# Unity Catalog identifiers: letters, digits, underscores. Enforced here because
# a malformed identifier becomes an injection surface the moment it is
# interpolated into a SQL string, which it inevitably will be.
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")


class ConfigError(ValueError):
    """Raised when configuration is absent, malformed, or internally inconsistent."""


# --------------------------------------------------------------------------- #
# Value objects
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class SourceConfig:
    """One upstream system's ingestion settings.

    ``contract_version`` is not decoration. Bronze stamps it onto every row, so a
    downstream failure can be traced to the exact contract in force when the row
    landed — which is the only way to answer "when did this break?" honestly.
    """

    name: str
    format: str
    landing_subpath: str
    contract_version: str
    schema_evolution_mode: str = "rescue"
    max_files_per_trigger: int | None = None
    is_streaming: bool = False

    VALID_FORMATS = ("json", "csv", "parquet", "text")
    VALID_EVOLUTION_MODES = ("rescue", "addNewColumns", "failOnNewColumns", "none")

    def __post_init__(self) -> None:
        if self.format not in self.VALID_FORMATS:
            raise ConfigError(
                f"source '{self.name}': format '{self.format}' not in {self.VALID_FORMATS}"
            )
        if self.schema_evolution_mode not in self.VALID_EVOLUTION_MODES:
            raise ConfigError(
                f"source '{self.name}': schema_evolution_mode "
                f"'{self.schema_evolution_mode}' not in {self.VALID_EVOLUTION_MODES}"
            )
        if self.landing_subpath.startswith("/"):
            raise ConfigError(
                f"source '{self.name}': landing_subpath must be relative to the volume root"
            )


@dataclass(frozen=True)
class QualityThresholds:
    """Thresholds that decide whether a load is publishable.

    Separating *warn* from *fail* is the whole point. A pipeline that fails on
    every anomaly gets its expectations disabled within a month; a pipeline that
    never fails is not a control. Both numbers belong in config so they can be
    tuned per environment without a code change.
    """

    max_quarantine_pct_warn: float = 1.0
    max_quarantine_pct_fail: float = 5.0
    max_null_key_pct_fail: float = 0.0
    min_expected_row_count: int = 1
    freshness_sla_minutes: int = 120

    def __post_init__(self) -> None:
        if not 0 <= self.max_quarantine_pct_warn <= self.max_quarantine_pct_fail <= 100:
            raise ConfigError(
                "quality thresholds must satisfy 0 <= warn <= fail <= 100 "
                f"(got warn={self.max_quarantine_pct_warn}, fail={self.max_quarantine_pct_fail})"
            )
        if self.freshness_sla_minutes <= 0:
            raise ConfigError("freshness_sla_minutes must be positive")


@dataclass(frozen=True)
class PipelineConfig:
    environment: str
    catalog: str
    landing_volume: str
    checkpoint_volume: str
    bronze_schema: str = "bronze"
    silver_schema: str = "silver"
    gold_schema: str = "gold"
    quarantine_schema: str = "quarantine"
    ops_schema: str = "ops"
    sources: Mapping[str, SourceConfig] = field(default_factory=dict)
    quality: QualityThresholds = field(default_factory=QualityThresholds)
    data_scale: str = "small"
    continuous_streaming: bool = False

    def __post_init__(self) -> None:
        if self.environment not in VALID_ENVIRONMENTS:
            raise ConfigError(
                f"environment '{self.environment}' not in {VALID_ENVIRONMENTS}"
            )
        for name in (
            self.catalog,
            self.bronze_schema,
            self.silver_schema,
            self.gold_schema,
            self.quarantine_schema,
            self.ops_schema,
        ):
            if not _IDENTIFIER_RE.match(name):
                raise ConfigError(f"'{name}' is not a valid Unity Catalog identifier")
        if not self.landing_volume.startswith("/Volumes/"):
            raise ConfigError(
                f"landing_volume must be a UC Volume path, got '{self.landing_volume}'"
            )

    # -- fully-qualified name helpers ---------------------------------------- #
    # Every table reference in the platform goes through one of these. No module
    # anywhere else is permitted to build a three-part name by string
    # concatenation; that rule is what makes a catalog rename a one-line change.

    def table(self, layer: str, name: str) -> str:
        schema = {
            "bronze": self.bronze_schema,
            "silver": self.silver_schema,
            "gold": self.gold_schema,
            "quarantine": self.quarantine_schema,
            "ops": self.ops_schema,
        }.get(layer)
        if schema is None:
            raise ConfigError(f"unknown layer '{layer}'")
        if not _IDENTIFIER_RE.match(name):
            raise ConfigError(f"'{name}' is not a valid table identifier")
        return f"{self.catalog}.{schema}.{name}"

    def landing_path(self, source_name: str) -> str:
        source = self.source(source_name)
        return f"{self.landing_volume.rstrip('/')}/{source.landing_subpath}"

    def checkpoint_path(self, stream_name: str) -> str:
        if not _IDENTIFIER_RE.match(stream_name):
            raise ConfigError(f"'{stream_name}' is not a valid stream identifier")
        return f"{self.checkpoint_volume.rstrip('/')}/{self.environment}/{stream_name}"

    def source(self, name: str) -> SourceConfig:
        try:
            return self.sources[name]
        except KeyError:
            known = ", ".join(sorted(self.sources)) or "<none>"
            raise ConfigError(f"unknown source '{name}'. Registered sources: {known}") from None


# --------------------------------------------------------------------------- #
# Loading
# --------------------------------------------------------------------------- #


def _deep_merge(base: dict[str, Any], override: Mapping[str, Any]) -> dict[str, Any]:
    """Recursive dict merge. Scalars and lists from ``override`` replace wholesale;
    only mappings merge. Lists deliberately do not concatenate — an environment
    that lists three sources means three, not three appended to base's six."""
    result = dict(base)
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, Mapping):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise ConfigError(f"configuration file not found: {path}")
    with path.open("r", encoding="utf-8") as handle:
        loaded = yaml.safe_load(handle) or {}
    if not isinstance(loaded, dict):
        raise ConfigError(f"{path} must contain a YAML mapping at the top level")
    return loaded


def _apply_env_overrides(raw: dict[str, Any]) -> dict[str, Any]:
    """Environment variables win over files.

    This is the seam that lets a bundle target inject ``${var.catalog}`` at
    deploy time without the YAML knowing anything about bundles, and lets CI
    point a test run at a scratch catalog without editing a file.
    """
    mapping = {
        "KINGS_CATALOG": "catalog",
        "KINGS_ENVIRONMENT": "environment",
        "KINGS_LANDING_VOLUME": "landing_volume",
        "KINGS_CHECKPOINT_VOLUME": "checkpoint_volume",
        "KINGS_DATA_SCALE": "data_scale",
    }
    for env_var, key in mapping.items():
        value = os.environ.get(env_var)
        if value:
            raw[key] = value
    continuous = os.environ.get("KINGS_CONTINUOUS_STREAMING")
    if continuous is not None:
        raw["continuous_streaming"] = continuous.strip().lower() in ("1", "true", "yes")
    return raw


def load_config(
    environment: str | None = None,
    conf_dir: Path | str = "conf",
    overrides: Mapping[str, Any] | None = None,
) -> PipelineConfig:
    """Build a validated :class:`PipelineConfig` from the layered sources.

    Parameters
    ----------
    environment:
        One of ``dev``/``stg``/``prd``. Falls back to ``KINGS_ENVIRONMENT``,
        then to ``dev``. Defaulting to dev is intentional: an unset environment
        should degrade to the least dangerous target, never to prd.
    conf_dir:
        Directory holding ``base.yml`` and the per-environment overlays.
    overrides:
        Final-word values, used by tests and by callers that already know better.
    """
    conf_dir = Path(conf_dir)
    env = environment or os.environ.get("KINGS_ENVIRONMENT") or "dev"
    if env not in VALID_ENVIRONMENTS:
        raise ConfigError(f"environment '{env}' not in {VALID_ENVIRONMENTS}")

    raw = _read_yaml(conf_dir / "base.yml")
    raw = _deep_merge(raw, _read_yaml(conf_dir / f"{env}.yml"))
    raw = _apply_env_overrides(raw)
    if overrides:
        raw = _deep_merge(raw, overrides)
    raw.setdefault("environment", env)

    sources_raw = raw.pop("sources", {}) or {}
    if not isinstance(sources_raw, dict):
        raise ConfigError("'sources' must be a mapping of source name to settings")
    sources = {
        name: SourceConfig(name=name, **settings) for name, settings in sources_raw.items()
    }

    quality = QualityThresholds(**(raw.pop("quality", {}) or {}))

    known_fields = {
        "environment",
        "catalog",
        "landing_volume",
        "checkpoint_volume",
        "bronze_schema",
        "silver_schema",
        "gold_schema",
        "quarantine_schema",
        "ops_schema",
        "data_scale",
        "continuous_streaming",
    }
    unknown = set(raw) - known_fields
    if unknown:
        # Fail loudly rather than silently ignoring a typo. A misspelled key that
        # is quietly dropped is how a pipeline runs for six weeks against the
        # wrong catalog without anyone noticing.
        raise ConfigError(f"unknown configuration keys: {sorted(unknown)}")

    return PipelineConfig(sources=sources, quality=quality, **raw)
