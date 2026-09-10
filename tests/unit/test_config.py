"""Unit tests for the layered configuration loader.

These run in CI with no cluster, no workspace, and no network. That is the
whole point: the majority of pipeline outages are configuration errors, and
configuration errors are exactly the class of bug that a cluster-free test
suite can catch in eight seconds instead of nine minutes.
"""

from __future__ import annotations

import pytest

from kings_ops.config import (
    ConfigError,
    PipelineConfig,
    QualityThresholds,
    SourceConfig,
    load_config,
)


# --------------------------------------------------------------------------- #
# Loading and layering
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("env,expected_catalog", [
    ("dev", "kings_dev"),
    ("stg", "kings_stg"),
    ("prd", "kings_prd"),
])
def test_each_environment_resolves_its_own_catalog(conf_dir, env, expected_catalog):
    cfg = load_config(environment=env, conf_dir=conf_dir)
    assert cfg.catalog == expected_catalog
    assert cfg.environment == env


def test_overlay_overrides_base_but_preserves_unset_keys(conf_dir):
    cfg = load_config(environment="dev", conf_dir=conf_dir)
    # dev.yml loosens the quarantine gate...
    assert cfg.quality.max_quarantine_pct_fail == 25.0
    # ...but does not mention this key, so base.yml's value survives.
    assert cfg.quality.max_null_key_pct_fail == 0.0


def test_staging_gates_match_production_gates(conf_dir):
    """A staging tier with softer gates than prd validates nothing.

    This test encodes that policy so a well-meaning future change that relaxes
    stg 'just to get the build green' fails here instead of in production.
    """
    stg = load_config(environment="stg", conf_dir=conf_dir).quality
    prd = load_config(environment="prd", conf_dir=conf_dir).quality
    assert stg.max_quarantine_pct_fail == prd.max_quarantine_pct_fail
    assert stg.max_quarantine_pct_warn == prd.max_quarantine_pct_warn
    assert stg.freshness_sla_minutes == prd.freshness_sla_minutes


def test_env_var_overrides_file(conf_dir, monkeypatch):
    monkeypatch.setenv("KINGS_CATALOG", "kings_scratch")
    cfg = load_config(environment="dev", conf_dir=conf_dir)
    assert cfg.catalog == "kings_scratch"


def test_explicit_override_beats_env_var(conf_dir, monkeypatch):
    monkeypatch.setenv("KINGS_CATALOG", "kings_scratch")
    cfg = load_config(environment="dev", conf_dir=conf_dir, overrides={"catalog": "kings_final"})
    assert cfg.catalog == "kings_final"


def test_unset_environment_defaults_to_dev_not_prd(conf_dir, monkeypatch):
    """Fail-safe direction matters. An unset environment must never land on prd."""
    monkeypatch.delenv("KINGS_ENVIRONMENT", raising=False)
    assert load_config(conf_dir=conf_dir).environment == "dev"


# --------------------------------------------------------------------------- #
# Validation — the loader must reject bad config loudly
# --------------------------------------------------------------------------- #


def test_unknown_environment_rejected(conf_dir):
    with pytest.raises(ConfigError, match="not in"):
        load_config(environment="uat", conf_dir=conf_dir)


def test_typo_in_config_key_is_rejected_not_ignored(conf_dir):
    with pytest.raises(ConfigError, match="unknown configuration keys"):
        load_config(environment="dev", conf_dir=conf_dir, overrides={"catalogue": "oops"})


def test_invalid_catalog_identifier_rejected(conf_dir):
    with pytest.raises(ConfigError, match="not a valid Unity Catalog identifier"):
        load_config(environment="dev", conf_dir=conf_dir, overrides={"catalog": "drop-table; --"})


def test_landing_volume_must_be_a_volume_path(conf_dir):
    with pytest.raises(ConfigError, match="must be a UC Volume path"):
        load_config(environment="dev", conf_dir=conf_dir, overrides={"landing_volume": "/dbfs/landing"})


def test_warn_threshold_above_fail_threshold_rejected():
    with pytest.raises(ConfigError, match="0 <= warn <= fail"):
        QualityThresholds(max_quarantine_pct_warn=10.0, max_quarantine_pct_fail=5.0)


def test_invalid_source_format_rejected():
    with pytest.raises(ConfigError, match="format 'avro' not in"):
        SourceConfig(
            name="telemetry", format="avro", landing_subpath="telemetry", contract_version="1.0.0"
        )


def test_absolute_landing_subpath_rejected():
    with pytest.raises(ConfigError, match="must be relative"):
        SourceConfig(
            name="telemetry", format="json", landing_subpath="/telemetry", contract_version="1.0.0"
        )


# --------------------------------------------------------------------------- #
# Name construction
# --------------------------------------------------------------------------- #


def test_table_names_are_three_part_and_environment_scoped(conf_dir):
    cfg = load_config(environment="prd", conf_dir=conf_dir)
    assert cfg.table("bronze", "br_gate_scan") == "kings_prd.bronze.br_gate_scan"
    assert cfg.table("gold", "fact_admission") == "kings_prd.gold.fact_admission"
    assert cfg.table("quarantine", "q_gate_scan") == "kings_prd.quarantine.q_gate_scan"


def test_unknown_layer_rejected(conf_dir):
    cfg = load_config(environment="dev", conf_dir=conf_dir)
    with pytest.raises(ConfigError, match="unknown layer"):
        cfg.table("platinum", "fact_thing")


def test_checkpoint_paths_are_environment_partitioned(conf_dir):
    """Two environments sharing a checkpoint directory is silent, total corruption.
    Environment must be in the path, not merely in the volume name."""
    dev = load_config(environment="dev", conf_dir=conf_dir).checkpoint_path("gate_scan_bronze")
    prd = load_config(environment="prd", conf_dir=conf_dir).checkpoint_path("gate_scan_bronze")
    assert dev != prd
    assert dev.endswith("/dev/gate_scan_bronze")
    assert prd.endswith("/prd/gate_scan_bronze")


def test_landing_path_composes_volume_and_source_subpath(conf_dir):
    cfg = load_config(environment="dev", conf_dir=conf_dir)
    assert cfg.landing_path("gate_scans") == "/Volumes/kings_dev/landing/files/gate_scans"
    assert cfg.landing_path("ticketing_cdc") == "/Volumes/kings_dev/landing/files/ticketing/cdc"


def test_unknown_source_error_lists_known_sources(conf_dir):
    cfg = load_config(environment="dev", conf_dir=conf_dir)
    with pytest.raises(ConfigError, match="Registered sources: .*gate_scans"):
        cfg.source("nonexistent_system")


# --------------------------------------------------------------------------- #
# Source registry contents
# --------------------------------------------------------------------------- #


def test_all_source_systems_registered(conf_dir):
    cfg = load_config(environment="dev", conf_dir=conf_dir)
    expected = {
        "gate_scans", "ticketing_cdc", "pos", "crm_accounts", "deposits",
        "seat_manifest", "schedule", "pricing", "sponsorship", "guest_services",
    }
    assert expected <= set(cfg.sources)


def test_cdc_source_fails_on_schema_drift_while_telemetry_rescues(conf_dir):
    """These two policies are opposites on purpose and the difference is load-bearing.

    Scanners gain fields as firmware ships — scan_device_os appeared mid-season —
    and failing the stream on that would be an outage caused by a non-event. CDC
    schema drift means the ticketing system's operational schema changed underneath
    us, which IS an incident and must stop the pipeline before it corrupts silver.
    """
    cfg = load_config(environment="dev", conf_dir=conf_dir)
    assert cfg.source("gate_scans").schema_evolution_mode == "rescue"
    assert cfg.source("ticketing_cdc").schema_evolution_mode == "failOnNewColumns"


def test_only_gate_scans_is_streaming(conf_dir):
    cfg = load_config(environment="dev", conf_dir=conf_dir)
    streaming = {n for n, s in cfg.sources.items() if s.is_streaming}
    assert streaming == {"gate_scans"}, (
        "Free Edition allows one active pipeline per type (ADR-003); a second "
        "streaming source would exceed the concurrency budget."
    )


def test_config_is_immutable(conf_dir):
    cfg = load_config(environment="dev", conf_dir=conf_dir)
    with pytest.raises(Exception):
        cfg.catalog = "kings_prd"  # type: ignore[misc]


def test_pipeline_config_rejects_direct_bad_construction():
    with pytest.raises(ConfigError):
        PipelineConfig(
            environment="qa",
            catalog="kings_dev",
            landing_volume="/Volumes/x/y/z",
            checkpoint_volume="/Volumes/x/y/c",
        )
