"""Unit tests for the silver CDC resolution transform.

The headline test is ``test_backdated_refund_does_not_resurrect_order``: it
encodes the exact trap the generator injects — a refund whose ``__commit_ts``
precedes its own sale — and proves that resolving by ``__seq`` keeps the order
refunded, while the ``__commit_ts`` ordering it guards against would resurrect it.
"""

from __future__ import annotations

from datetime import datetime

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import Window  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from kings_ops.transforms.cdc import (  # noqa: E402
    active_orders,
    dedupe_cdc,
    resolve_current_state,
)

pytestmark = pytest.mark.spark

_SCHEMA = (
    "order_id string, __seq long, __op string, event_type string, "
    "paid_price_usd double, __commit_ts timestamp"
)


def _cdc(spark, rows):
    return spark.createDataFrame(rows, _SCHEMA)


def test_backdated_refund_does_not_resurrect_order(spark):
    # SALE at seq 1 committed 2027-04-01; REFUND at seq 2 but its commit_ts is
    # BACKDATED to 2027-03-20 — ten days before the sale it reverses.
    df = _cdc(
        spark,
        [
            ("ORD-1", 1, "I", "SALE", 100.0, datetime(2027, 4, 1, 12, 0)),
            ("ORD-1", 2, "D", "REFUND", 100.0, datetime(2027, 3, 20, 9, 0)),
        ],
    )

    current = resolve_current_state(df).collect()
    assert len(current) == 1
    row = current[0]
    assert row["__seq"] == 2
    assert row["is_refunded"] is True
    assert row["order_status"] == "REFUNDED"

    # Guard documenting the trap: ordering by __commit_ts picks the SALE (the
    # backdated refund sorts first), which would wrongly report the order active.
    wrong = Window.partitionBy("order_id").orderBy(F.col("__commit_ts").desc())
    resurrected = (
        df.withColumn("_rn", F.row_number().over(wrong)).filter("_rn = 1").collect()[0]
    )
    assert resurrected["event_type"] == "SALE"  # the bug we are avoiding


def test_duplicate_seq_is_collapsed(spark):
    df = _cdc(
        spark,
        [
            ("ORD-2", 10, "I", "SALE", 55.0, datetime(2027, 4, 2, 10, 0)),
            ("ORD-2", 10, "I", "SALE", 55.0, datetime(2027, 4, 2, 10, 0)),  # dupe __seq
        ],
    )
    assert dedupe_cdc(df).count() == 1
    assert resolve_current_state(df).count() == 1


def test_latest_non_refund_stays_active(spark):
    df = _cdc(
        spark,
        [
            ("ORD-3", 5, "I", "SALE", 80.0, datetime(2027, 4, 3, 10, 0)),
            ("ORD-3", 6, "U", "TRANSFER", 80.0, datetime(2027, 4, 4, 10, 0)),
        ],
    )
    current = resolve_current_state(df)
    row = current.collect()[0]
    assert row["__seq"] == 6
    assert row["is_refunded"] is False
    assert row["order_status"] == "TRANSFERRED"
    assert active_orders(current).count() == 1


def test_active_orders_excludes_refunds(spark):
    df = _cdc(
        spark,
        [
            ("ORD-A", 1, "I", "SALE", 10.0, datetime(2027, 4, 1, 1, 0)),
            ("ORD-B", 2, "I", "SALE", 20.0, datetime(2027, 4, 1, 2, 0)),
            ("ORD-B", 3, "D", "REFUND", 20.0, datetime(2027, 4, 1, 3, 0)),
        ],
    )
    current = resolve_current_state(df)
    assert current.count() == 2  # one row per order
    active = {r["order_id"] for r in active_orders(current).collect()}
    assert active == {"ORD-A"}
