"""Unit tests for the silver admission validation transforms.

No cluster, no streaming context. The pure functions in
``kings_ops.transforms.admission`` work on plain DataFrames, so every
correctness-critical behaviour is testable locally.

Key invariants tested:
  - ACCEPTED scan + matched ticket  → is_valid_admission=True
  - ACCEPTED scan + no matching order → is_orphan_ticket=True, invalid
  - Scan whose order_id is null itself → is_orphan_ticket=True, invalid
  - DUPLICATE / INVALID / UNKNOWN_CREDENTIAL scans → invalid regardless of ticket
  - Renumbered seat (SCD2 as-of join) → section_at_scan tracks scan date
  - Scan on a date with no seat version → section_at_scan=null (kept, not dropped)
  - quarantine_reason: device flag takes priority over orphan flag
  - Column projection: sv_admission schema never grows silently
"""

from __future__ import annotations

from datetime import date, datetime

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import functions as F  # noqa: E402

from kings_ops.transforms.admission import (  # noqa: E402
    build_sv_admission,
    enrich_scan_with_seat,
    enrich_scan_with_ticket,
    quarantine_reason,
)

pytestmark = pytest.mark.spark

# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #

_SCAN_SCHEMA = (
    "scan_id string, game_id string, seat_id string, gate string, "
    "scan_ts timestamp, device_type string, scan_result string, "
    "order_id string, _ingest_ts timestamp, _source_file string"
)

_TICKET_SCHEMA = "order_id string, account_id string, seat_id string"

_SEAT_SCHEMA = (
    "seat_id string, section int, "
    "__effective_from date, __effective_to date, __is_current boolean"
)


def _ts(s: str) -> datetime:
    return datetime.fromisoformat(s)


def _scans(spark, rows):
    return spark.createDataFrame(rows, _SCAN_SCHEMA)


def _tickets(spark, rows):
    return spark.createDataFrame(rows, _TICKET_SCHEMA)


def _seats(spark, rows):
    return spark.createDataFrame(rows, _SEAT_SCHEMA)


# --------------------------------------------------------------------------- #
# enrich_scan_with_ticket
# --------------------------------------------------------------------------- #


def test_accepted_scan_with_matched_ticket_not_orphan(spark):
    scans = _scans(
        spark,
        [("S1", "G1", "SEAT-A", "GateA", _ts("2027-04-10 19:00:00"),
          "TURNSTILE", "ACCEPTED", "ORD-1", _ts("2027-04-10 19:00:01"), "file1")],
    )
    tickets = _tickets(spark, [("ORD-1", "ACCT-1", "SEAT-A")])

    enriched = enrich_scan_with_ticket(scans, tickets).collect()
    assert len(enriched) == 1
    row = enriched[0]
    assert row["is_orphan_ticket"] is False
    assert row["account_id"] == "ACCT-1"


def test_scan_with_no_matching_order_is_orphan(spark):
    scans = _scans(
        spark,
        [("S2", "G1", "SEAT-B", "GateA", _ts("2027-04-10 19:01:00"),
          "TURNSTILE", "ACCEPTED", "ORD-MISSING", _ts("2027-04-10 19:01:01"), "file1")],
    )
    tickets = _tickets(spark, [("ORD-1", "ACCT-1", "SEAT-A")])

    enriched = enrich_scan_with_ticket(scans, tickets).collect()
    assert enriched[0]["is_orphan_ticket"] is True
    assert enriched[0]["account_id"] is None


def test_scan_with_null_order_id_is_orphan(spark):
    scans = _scans(
        spark,
        [("S3", "G1", "SEAT-C", "GateB", _ts("2027-04-10 19:02:00"),
          "HANDHELD", "UNKNOWN_CREDENTIAL", None, _ts("2027-04-10 19:02:01"), "file1")],
    )
    tickets = _tickets(spark, [("ORD-1", "ACCT-1", "SEAT-A")])

    enriched = enrich_scan_with_ticket(scans, tickets).collect()
    assert enriched[0]["is_orphan_ticket"] is True


# --------------------------------------------------------------------------- #
# enrich_scan_with_seat
# --------------------------------------------------------------------------- #


def test_scan_before_renumber_resolves_to_original_section(spark):
    # Section 118 renumbered to 218 on 2027-05-15.
    scans = _scans(
        spark,
        [("S4", "G1", "SEAT-118", "GateG", _ts("2027-04-10 19:30:00"),
          "HANDHELD", "ACCEPTED", "ORD-2", _ts("2027-04-10 19:30:01"), "file2")],
    )
    seats = _seats(
        spark,
        [
            # Version 1: section 118, open from season start, closed on renumber date
            ("SEAT-118", 118, date(2027, 1, 1), date(2027, 5, 15), False),
            # Version 2: section 218, current
            ("SEAT-118", 218, date(2027, 5, 15), date(9999, 12, 31), True),
        ],
    )
    enriched = enrich_scan_with_seat(scans, seats).collect()
    assert enriched[0]["section_at_scan"] == 118


def test_scan_after_renumber_resolves_to_new_section(spark):
    scans = _scans(
        spark,
        [("S5", "G1", "SEAT-118", "GateG", _ts("2027-06-01 19:30:00"),
          "HANDHELD", "ACCEPTED", "ORD-3", _ts("2027-06-01 19:30:01"), "file3")],
    )
    seats = _seats(
        spark,
        [
            ("SEAT-118", 118, date(2027, 1, 1), date(2027, 5, 15), False),
            ("SEAT-118", 218, date(2027, 5, 15), date(9999, 12, 31), True),
        ],
    )
    enriched = enrich_scan_with_seat(scans, seats).collect()
    assert enriched[0]["section_at_scan"] == 218


def test_scan_with_unknown_seat_id_has_null_section(spark):
    scans = _scans(
        spark,
        [("S6", "G1", "SEAT-UNKNOWN", "GateA", _ts("2027-04-10 19:00:00"),
          "TURNSTILE", "ACCEPTED", "ORD-4", _ts("2027-04-10 19:00:01"), "file1")],
    )
    seats = _seats(spark, [("SEAT-999", 100, date(2027, 1, 1), date(9999, 12, 31), True)])

    enriched = enrich_scan_with_seat(scans, seats).collect()
    assert enriched[0]["section_at_scan"] is None


# --------------------------------------------------------------------------- #
# build_sv_admission — end-to-end classification
# --------------------------------------------------------------------------- #


def _full_pipeline(spark, scan_rows, ticket_rows, seat_rows):
    scans = _scans(spark, scan_rows)
    tickets = _tickets(spark, ticket_rows)
    seats = _seats(spark, seat_rows)
    return build_sv_admission(scans, tickets, seats)


_DEFAULT_SEAT = [("SEAT-1", 100, date(2027, 1, 1), date(9999, 12, 31), True)]
_DEFAULT_TICKET = [("ORD-1", "ACCT-1", "SEAT-1")]


def test_accepted_matched_scan_is_valid_admission(spark):
    result = _full_pipeline(
        spark,
        [("S1", "G1", "SEAT-1", "GateA", _ts("2027-04-10 19:00:00"),
          "TURNSTILE", "ACCEPTED", "ORD-1", _ts("2027-04-10 19:00:01"), "f1")],
        _DEFAULT_TICKET,
        _DEFAULT_SEAT,
    ).collect()
    assert len(result) == 1
    assert result[0]["is_valid_admission"] is True
    assert result[0]["is_orphan_ticket"] is False
    assert result[0]["account_id"] == "ACCT-1"
    assert result[0]["section_at_scan"] == 100


def test_duplicate_scan_result_is_invalid_even_with_matched_ticket(spark):
    result = _full_pipeline(
        spark,
        [("S2", "G1", "SEAT-1", "GateA", _ts("2027-04-10 19:01:00"),
          "TURNSTILE", "DUPLICATE", "ORD-1", _ts("2027-04-10 19:01:01"), "f1")],
        _DEFAULT_TICKET,
        _DEFAULT_SEAT,
    ).collect()
    assert result[0]["is_valid_admission"] is False


def test_invalid_scan_result_is_invalid(spark):
    result = _full_pipeline(
        spark,
        [("S3", "G1", "SEAT-1", "GateA", _ts("2027-04-10 19:02:00"),
          "TURNSTILE", "INVALID", "ORD-1", _ts("2027-04-10 19:02:01"), "f1")],
        _DEFAULT_TICKET,
        _DEFAULT_SEAT,
    ).collect()
    assert result[0]["is_valid_admission"] is False


def test_orphan_ticket_accepted_scan_is_invalid(spark):
    result = _full_pipeline(
        spark,
        [("S4", "G1", "SEAT-1", "GateA", _ts("2027-04-10 19:03:00"),
          "TURNSTILE", "ACCEPTED", "ORD-GHOST", _ts("2027-04-10 19:03:01"), "f1")],
        _DEFAULT_TICKET,  # only has ORD-1
        _DEFAULT_SEAT,
    ).collect()
    assert result[0]["is_valid_admission"] is False
    assert result[0]["is_orphan_ticket"] is True


def test_output_columns_match_declared_schema(spark):
    """Schema stability: sv_admission must never grow silently."""
    from kings_ops.transforms.admission import _SV_ADMISSION_COLS

    result = _full_pipeline(
        spark,
        [("S1", "G1", "SEAT-1", "GateA", _ts("2027-04-10 19:00:00"),
          "TURNSTILE", "ACCEPTED", "ORD-1", _ts("2027-04-10 19:00:01"), "f1")],
        _DEFAULT_TICKET,
        _DEFAULT_SEAT,
    )
    assert set(result.columns) == set(_SV_ADMISSION_COLS)


# --------------------------------------------------------------------------- #
# quarantine_reason
# --------------------------------------------------------------------------- #


def _invalid_scans(spark, scan_result, is_orphan):
    rows = [(scan_result, is_orphan, False)]
    df = spark.createDataFrame(rows, "scan_result string, is_orphan_ticket boolean, is_valid_admission boolean")
    return quarantine_reason(df)


def test_duplicate_scan_reason(spark):
    row = _invalid_scans(spark, "DUPLICATE", False).collect()[0]
    assert row["_quarantine_reason"] == "DUPLICATE_SCAN"


def test_invalid_ticket_reason(spark):
    row = _invalid_scans(spark, "INVALID", False).collect()[0]
    assert row["_quarantine_reason"] == "INVALID_TICKET"


def test_unknown_credential_reason(spark):
    row = _invalid_scans(spark, "UNKNOWN_CREDENTIAL", False).collect()[0]
    assert row["_quarantine_reason"] == "UNKNOWN_CREDENTIAL"


def test_orphan_ticket_reason_when_accepted_device(spark):
    """ACCEPTED device result + no matching ticket → orphan (not device reject)."""
    rows = [("ACCEPTED", True, False)]
    df = spark.createDataFrame(rows, "scan_result string, is_orphan_ticket boolean, is_valid_admission boolean")
    row = quarantine_reason(df).collect()[0]
    assert row["_quarantine_reason"] == "NO_MATCHING_TICKET"


def test_device_flag_takes_priority_over_orphan(spark):
    """DUPLICATE device result + orphan → reason is DUPLICATE_SCAN, not NO_MATCHING_TICKET."""
    rows = [("DUPLICATE", True, False)]
    df = spark.createDataFrame(rows, "scan_result string, is_orphan_ticket boolean, is_valid_admission boolean")
    row = quarantine_reason(df).collect()[0]
    assert row["_quarantine_reason"] == "DUPLICATE_SCAN"
