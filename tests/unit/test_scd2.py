"""Unit tests for the SCD2 transforms.

``scd2_merge`` is exercised across two snapshots to prove it closes a changed
version and opens a new one, leaves unchanged and untracked-only changes alone,
and inserts new keys. ``build_seat_dimension`` is tested against the mid-season
renumbering, which is the concrete SCD2 driver for ``dim_seat``.
"""

from __future__ import annotations

import datetime as dt

import pytest

pytest.importorskip("pyspark")

from kings_ops.transforms.scd2 import (  # noqa: E402
    EFFECTIVE_FROM,
    EFFECTIVE_TO,
    IS_CURRENT,
    build_seat_dimension,
    current_version,
    scd2_merge,
)

pytestmark = pytest.mark.spark

_ACCT_SCHEMA = "account_id string, cohort string, email string, snapshot_date string"


def _rows(df):
    return {r["account_id"]: r.asDict() for r in df.collect()}


def test_scd2_first_load_opens_current(spark):
    batch1 = spark.createDataFrame(
        [("ACC-1", "LOCAL_PLAN", "a@x.com", "2027-01-01")], _ACCT_SCHEMA
    )
    dim = scd2_merge(None, batch1, key="account_id", tracked=("cohort",), effective_from="snapshot_date")
    rows = dim.collect()
    assert len(rows) == 1
    r = rows[0]
    assert r[IS_CURRENT] is True
    assert r[EFFECTIVE_FROM] == dt.date(2027, 1, 1)
    assert r[EFFECTIVE_TO] == dt.date(9999, 12, 31)
    assert "snapshot_date" not in r.asDict()  # consumed, not a business column


def test_scd2_tracks_change_ignores_untracked_and_inserts_new(spark):
    batch1 = spark.createDataFrame(
        [
            ("ACC-1", "TOURIST", "one@x.com", "2027-01-01"),   # will change cohort
            ("ACC-2", "LOCAL_PLAN", "two@x.com", "2027-01-01"),  # only email changes
            ("ACC-3", "GROUP_BLOCK", "three@x.com", "2027-01-01"),  # unchanged
        ],
        _ACCT_SCHEMA,
    )
    dim1 = scd2_merge(None, batch1, key="account_id", tracked=("cohort",), effective_from="snapshot_date")

    batch2 = spark.createDataFrame(
        [
            ("ACC-1", "LOCAL_PLAN", "one@x.com", "2027-06-01"),   # tracked change
            ("ACC-2", "LOCAL_PLAN", "changed@x.com", "2027-06-01"),  # untracked-only
            ("ACC-3", "GROUP_BLOCK", "three@x.com", "2027-06-01"),  # identical
            ("ACC-4", "TOURIST", "four@x.com", "2027-06-01"),     # brand new
        ],
        _ACCT_SCHEMA,
    )
    dim2 = scd2_merge(dim1, batch2, key="account_id", tracked=("cohort",), effective_from="snapshot_date")

    by_acct: dict[str, list] = {}
    for r in dim2.collect():
        by_acct.setdefault(r["account_id"], []).append(r.asDict())

    # ACC-1: cohort changed -> two versions, old closed on 2027-06-01, new current.
    acc1 = sorted(by_acct["ACC-1"], key=lambda d: d[EFFECTIVE_FROM])
    assert len(acc1) == 2
    assert acc1[0]["cohort"] == "TOURIST"
    assert acc1[0][IS_CURRENT] is False
    assert acc1[0][EFFECTIVE_TO] == dt.date(2027, 6, 1)
    assert acc1[1]["cohort"] == "LOCAL_PLAN"
    assert acc1[1][IS_CURRENT] is True

    # ACC-2: only the untracked email changed -> still one (current) version,
    # retaining the original email (tracked-subset semantics).
    assert len(by_acct["ACC-2"]) == 1
    assert by_acct["ACC-2"][0][IS_CURRENT] is True
    assert by_acct["ACC-2"][0]["email"] == "two@x.com"

    # ACC-3: identical -> one current version, idempotent.
    assert len(by_acct["ACC-3"]) == 1
    assert by_acct["ACC-3"][0][IS_CURRENT] is True

    # ACC-4: new key -> one current version.
    assert len(by_acct["ACC-4"]) == 1
    assert by_acct["ACC-4"][0][IS_CURRENT] is True

    # Exactly one current row per key.
    cur = current_version(dim2)
    assert cur.count() == 4


_SEAT_SCHEMA = (
    "seat_id string, section int, seat_class string, "
    "renumbered_to_section int, renumber_effective_date string"
)


def test_build_seat_dimension_renumbering(spark):
    seats = spark.createDataFrame(
        [
            ("S-NORMAL", 150, "OUTFIELD_RESERVED", None, None),
            ("S-MOVED", 120, "INFIELD_BOX", 220, "2027-05-15"),
        ],
        _SEAT_SCHEMA,
    )
    dim = build_seat_dimension(seats, season_start="2027-01-01")
    by_seat: dict[str, list] = {}
    for r in dim.collect():
        by_seat.setdefault(r["seat_id"], []).append(r.asDict())

    # Normal seat: single current version, section unchanged, open-ended.
    normal = by_seat["S-NORMAL"]
    assert len(normal) == 1
    assert normal[0]["section"] == 150
    assert normal[0][IS_CURRENT] is True
    assert normal[0][EFFECTIVE_FROM] == dt.date(2027, 1, 1)
    assert normal[0][EFFECTIVE_TO] == dt.date(9999, 12, 31)

    # Renumbered seat: two versions split on the renumber date.
    moved = sorted(by_seat["S-MOVED"], key=lambda d: d[EFFECTIVE_FROM])
    assert len(moved) == 2
    assert moved[0]["section"] == 120
    assert moved[0][IS_CURRENT] is False
    assert moved[0][EFFECTIVE_TO] == dt.date(2027, 5, 15)
    assert moved[1]["section"] == 220
    assert moved[1][IS_CURRENT] is True
    assert moved[1][EFFECTIVE_FROM] == dt.date(2027, 5, 15)
    assert moved[1][EFFECTIVE_TO] == dt.date(9999, 12, 31)

    # "renumbered_to_section" is dropped; it is metadata, not a dimension column.
    assert "renumbered_to_section" not in moved[0]
