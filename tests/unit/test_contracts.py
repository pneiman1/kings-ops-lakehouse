"""Unit tests for data contracts.

No Spark, no cluster, no workspace. Contracts are data about data, so they are
testable as plain Python — which means a broken contract fails a PR in eight
seconds rather than failing a pipeline in minute nine.
"""

from __future__ import annotations

import pytest

from kings_ops.config import load_config
from kings_ops.contracts.definitions import (
    CONTRACTS,
    Column,
    DataContract,
    EvolutionPolicy,
    get_contract,
)


# --------------------------------------------------------------------------- #
# Structural validation — a malformed contract must fail at import, not runtime
# --------------------------------------------------------------------------- #


def test_duplicate_columns_rejected():
    with pytest.raises(ValueError, match="duplicate columns"):
        DataContract(
            source_name="bad", version="1.0.0",
            columns=(Column("a", "STRING"), Column("a", "INT")),
            primary_key=("a",), evolution_policy=EvolutionPolicy.RESCUE,
        )


def test_primary_key_must_be_a_declared_column():
    with pytest.raises(ValueError, match="primary key 'missing' is not a declared column"):
        DataContract(
            source_name="bad", version="1.0.0",
            columns=(Column("a", "STRING"),),
            primary_key=("missing",), evolution_policy=EvolutionPolicy.RESCUE,
        )


def test_empty_primary_key_rejected():
    with pytest.raises(ValueError, match="primary_key cannot be empty"):
        DataContract(
            source_name="bad", version="1.0.0",
            columns=(Column("a", "STRING"),),
            primary_key=(), evolution_policy=EvolutionPolicy.RESCUE,
        )


def test_event_time_column_must_be_declared():
    with pytest.raises(ValueError, match="event_time_column 'nope' is not declared"):
        DataContract(
            source_name="bad", version="1.0.0",
            columns=(Column("a", "STRING"),),
            primary_key=("a",), evolution_policy=EvolutionPolicy.RESCUE,
            event_time_column="nope",
        )


def test_version_must_be_semver():
    with pytest.raises(ValueError, match="MAJOR.MINOR.PATCH"):
        DataContract(
            source_name="bad", version="1.0",
            columns=(Column("a", "STRING"),),
            primary_key=("a",), evolution_policy=EvolutionPolicy.RESCUE,
        )


def test_unsupported_column_type_rejected():
    with pytest.raises(ValueError, match="unsupported type"):
        Column("a", "VARCHAR2")


def test_complex_types_allowed():
    assert Column("tags", "ARRAY<STRING>").to_ddl() == "tags ARRAY<STRING>"


# --------------------------------------------------------------------------- #
# DDL rendering
# --------------------------------------------------------------------------- #


def test_ddl_marks_non_nullable_columns():
    ddl = get_contract("gate_scans").schema_ddl()
    assert "scan_id STRING NOT NULL" in ddl
    assert "scan_device_os STRING" in ddl
    assert "scan_device_os STRING NOT NULL" not in ddl


def test_every_contract_renders_ddl():
    for name, contract in CONTRACTS.items():
        ddl = contract.schema_ddl()
        assert ddl, f"{name} rendered an empty schema"
        assert ddl.count(",") == len(contract.columns) - 1


# --------------------------------------------------------------------------- #
# Policy — these encode decisions, not mechanics
# --------------------------------------------------------------------------- #


def test_scans_rescue_while_cdc_fails_on_new_columns():
    """Opposite policies, deliberately, and the difference is load-bearing.

    Scanner firmware shipped scan_device_os mid-season. Failing the stream on
    that would be an outage caused by a non-event. A new column in ticketing CDC
    means the operational schema changed underneath us, which IS an incident:
    continuing writes subtly wrong data into silver, and subtly wrong is worse
    than stopped.
    """
    assert get_contract("gate_scans").evolution_policy is EvolutionPolicy.RESCUE
    assert get_contract("ticketing_cdc").evolution_policy is EvolutionPolicy.FAIL_ON_NEW_COLUMNS


def test_scan_device_os_is_nullable():
    """Added in contract 1.1.0. Rows from before the firmware rollout will never
    carry it; declaring it NOT NULL would quarantine half the season for a field
    that did not exist yet."""
    col = next(c for c in get_contract("gate_scans").columns if c.name == "scan_device_os")
    assert col.nullable is True


def test_cdc_orders_by_seq_not_commit_ts():
    """__commit_ts is backdated by the source when corrections are issued, so it
    cannot be the ordering key. __seq is monotonic and is part of the primary key
    precisely so this cannot be forgotten downstream."""
    cdc = get_contract("ticketing_cdc")
    assert "__seq" in cdc.primary_key
    assert "__commit_ts" not in cdc.primary_key


def test_every_streaming_source_declares_an_event_time_column():
    """Without an event-time column there is nothing to watermark against and
    late data cannot be reasoned about at all."""
    cfg = load_config(environment="dev", conf_dir="conf")
    for name, source in cfg.sources.items():
        if source.is_streaming and name in CONTRACTS:
            assert CONTRACTS[name].event_time_column is not None, (
                f"streaming source '{name}' has no event_time_column"
            )


def test_pii_is_declared_at_the_contract_boundary():
    """Declaring PII here rather than discovering it in a governance review is
    the difference between policy and archaeology. M7 masks exactly this set."""
    pii = get_contract("crm_accounts").pii_columns()
    assert {"email", "phone", "street_address", "birth_date", "payment_token_last4"} <= set(pii)
    assert "cohort" not in pii
    assert "account_id" not in pii


def test_birth_date_is_pii_but_drives_a_row_filter():
    """Masking birth_date would defeat the minor-status row filter that depends
    on it. Tagged as PII, handled by access control rather than masking."""
    contract = get_contract("crm_accounts")
    col = next(c for c in contract.columns if c.name == "birth_date")
    assert col.is_pii is True


# --------------------------------------------------------------------------- #
# Expectation predicates
# --------------------------------------------------------------------------- #


def test_primary_key_expectation_covers_composite_keys():
    expr = get_contract("ticketing_cdc").primary_key_not_null_expectation()
    assert expr == "order_id IS NOT NULL AND __seq IS NOT NULL"


def test_required_columns_expectation_falls_back_to_primary_key():
    contract = DataContract(
        source_name="minimal", version="1.0.0",
        columns=(Column("k", "STRING", nullable=False),),
        primary_key=("k",), evolution_policy=EvolutionPolicy.RESCUE,
    )
    assert contract.required_columns_expectation() == "k IS NOT NULL"


# --------------------------------------------------------------------------- #
# Registry consistency with config
# --------------------------------------------------------------------------- #


def test_unknown_contract_error_lists_known_sources():
    with pytest.raises(KeyError, match="Known: .*gate_scans"):
        get_contract("not_a_source")


def test_contract_versions_match_the_source_registry():
    """conf/base.yml and the contract module both carry a version. They must
    agree, or the version stamped onto bronze rows is a lie."""
    cfg = load_config(environment="dev", conf_dir="conf")
    for name, contract in CONTRACTS.items():
        assert cfg.source(name).contract_version == contract.version, (
            f"'{name}': conf says {cfg.source(name).contract_version}, "
            f"contract says {contract.version}"
        )


def test_evolution_policy_matches_the_source_registry():
    cfg = load_config(environment="dev", conf_dir="conf")
    for name, contract in CONTRACTS.items():
        assert cfg.source(name).schema_evolution_mode == contract.evolution_policy.value, (
            f"'{name}': conf and contract disagree on schema evolution policy"
        )
