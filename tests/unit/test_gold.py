"""Unit tests for the gold star-schema transforms.

The headline is ``test_fact_resolves_seat_section_as_of_purchase``: an order
placed before the May renumbering resolves to the old section and one placed after
to the new section — the SCD2 seat dimension doing exactly the job it exists for.
"""

from __future__ import annotations

from datetime import datetime

import pytest

pytest.importorskip("pyspark")

from kings_ops.transforms.gold import (  # noqa: E402
    build_dim_date,
    build_fact_game_pnl,
    build_fact_ticket_order,
)
from kings_ops.transforms.scd2 import build_seat_dimension, scd2_merge  # noqa: E402

pytestmark = pytest.mark.spark

_ORDER_SCHEMA = (
    "order_id string, game_id string, seat_id string, account_id string, "
    "seat_class string, face_price_usd double, paid_price_usd double, "
    "purchased_ts timestamp, is_refunded boolean, order_status string"
)
_SEAT_SCHEMA = (
    "seat_id string, section int, seat_class string, "
    "renumbered_to_section int, renumber_effective_date string"
)
_ACCT_SCHEMA = "account_id string, cohort string, origin_state string, eff string"


def _dim_seat(spark):
    seats = spark.createDataFrame(
        [
            ("S-MOVED", 120, "INFIELD_BOX", 220, "2027-05-15"),
            ("S-NORMAL", 150, "OUTFIELD_RESERVED", None, None),
        ],
        _SEAT_SCHEMA,
    )
    return build_seat_dimension(seats, season_start="2027-01-01")


def _dim_account(spark):
    snap = spark.createDataFrame(
        [("ACC-1", "TOURIST", "CA", "2027-01-01")], _ACCT_SCHEMA
    )
    return scd2_merge(None, snap, key="account_id", tracked=("cohort",), effective_from="eff")


def _orders(spark, rows):
    return spark.createDataFrame(rows, _ORDER_SCHEMA)


def test_fact_resolves_seat_section_as_of_purchase(spark):
    orders = _orders(
        spark,
        [
            # Same renumbered seat, bought before vs. after the May 15 change.
            ("ORD-A", "G1", "S-MOVED", "ACC-1", "INFIELD_BOX", 80.0, 80.0,
             datetime(2027, 4, 1, 12, 0), False, "ACTIVE"),
            ("ORD-B", "G2", "S-MOVED", "ACC-1", "INFIELD_BOX", 80.0, 80.0,
             datetime(2027, 6, 1, 12, 0), False, "ACTIVE"),
            ("ORD-C", "G1", "S-NORMAL", "ACC-1", "OUTFIELD_RESERVED", 30.0, 30.0,
             datetime(2027, 5, 1, 12, 0), False, "ACTIVE"),
            # Seat not in the dimension at all -> orphan.
            ("ORD-D", "G1", "S-GHOST", "ACC-1", "INFIELD_BOX", 80.0, 80.0,
             datetime(2027, 5, 1, 12, 0), False, "ACTIVE"),
        ],
    )
    fact = build_fact_ticket_order(orders, _dim_seat(spark), _dim_account(spark))
    by_order = {r["order_id"]: r.asDict() for r in fact.collect()}

    assert by_order["ORD-A"]["section_at_purchase"] == 120  # before renumber
    assert by_order["ORD-B"]["section_at_purchase"] == 220  # after renumber
    assert by_order["ORD-C"]["section_at_purchase"] == 150  # never renumbered
    assert by_order["ORD-D"]["section_at_purchase"] is None
    assert by_order["ORD-D"]["is_orphan_seat"] is True
    assert by_order["ORD-A"]["is_orphan_seat"] is False

    # Account dimension joined through.
    assert by_order["ORD-A"]["account_cohort"] == "TOURIST"
    assert by_order["ORD-A"]["account_origin_state"] == "CA"


def test_collected_revenue_excludes_refunds(spark):
    orders = _orders(
        spark,
        [
            ("ORD-A", "G1", "S-NORMAL", "ACC-1", "OUTFIELD_RESERVED", 30.0, 30.0,
             datetime(2027, 5, 1, 12, 0), False, "ACTIVE"),
            ("ORD-R", "G1", "S-NORMAL", "ACC-1", "OUTFIELD_RESERVED", 30.0, 30.0,
             datetime(2027, 5, 1, 12, 0), True, "REFUNDED"),
        ],
    )
    fact = build_fact_ticket_order(orders, _dim_seat(spark), _dim_account(spark))
    rev = {r["order_id"]: r["collected_revenue_usd"] for r in fact.collect()}
    assert rev["ORD-A"] == 30.0
    assert rev["ORD-R"] == 0.0

    pnl = build_fact_game_pnl(fact).collect()[0].asDict()
    assert pnl["game_id"] == "G1"
    assert pnl["tickets_sold"] == 1          # the refund does not count
    assert pnl["collected_revenue_usd"] == 30.0
    assert pnl["refunded_usd"] == 30.0
    assert pnl["order_rows"] == 2            # both rows still present


def test_build_dim_date(spark):
    dates = spark.sql("SELECT explode(sequence(DATE'2027-04-01', DATE'2027-04-07')) AS date")
    dim = build_dim_date(dates)
    rows = {r["date_key"]: r.asDict() for r in dim.collect()}
    assert len(rows) == 7
    apr3 = rows[20270403]              # a Saturday
    assert apr3["year"] == 2027
    assert apr3["month"] == 4
    assert apr3["day"] == 3
    assert apr3["is_weekend"] is True
    apr1 = rows[20270401]             # a Thursday
    assert apr1["is_weekend"] is False
