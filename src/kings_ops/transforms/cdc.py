"""Silver CDC resolution — reduce a ticketing change stream to current state.

Pure ``DataFrame -> DataFrame`` functions. No ``SparkSession``, no reads, no
writes: the pipeline wrapper handles I/O, everything correctness-critical lives
here so it is unit-testable without a cluster (see the charter's note on
``transforms/``).

The ordering trap
-----------------
The ticketing source emits two ordering-relevant columns:

* ``__seq``       — a monotonic sequence assigned as each change is captured.
* ``__commit_ts`` — the source commit time, which the source **backdates** when
  it issues a correction. A refund can therefore carry a ``__commit_ts`` that is
  *earlier* than the sale it reverses.

Resolve current state by ``__seq``. Ordering by ``__commit_ts`` lets a backdated
refund sort before its own sale, so the sale wins the "latest row" race and the
refunded order is **resurrected** — revenue reported that was never collected.
The generator injects this at a configured rate, so it shows up in the counts.
This is the single most important invariant in the silver layer.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

SEQ_COL = "__seq"
OP_COL = "__op"
EVENT_TYPE_COL = "event_type"

def _status_expr():
    """Order status from the latest event. __op is the CDC verb; event_type the
    business meaning. REFUND is the only state that removes collected revenue.

    Built lazily inside a function, never at module scope: a ``F.col`` at import
    time needs an active SparkContext and would break ``import`` in any process
    that hasn't started Spark (including plain test collection).
    """
    return (
        F.when(F.col(EVENT_TYPE_COL) == "REFUND", F.lit("REFUNDED"))
        .when(F.col(EVENT_TYPE_COL) == "TRANSFER", F.lit("TRANSFERRED"))
        .when(F.col(EVENT_TYPE_COL) == "RESALE", F.lit("RESOLD"))
        .otherwise(F.lit("ACTIVE"))
    )


def dedupe_cdc(df: DataFrame, key_cols: tuple[str, ...] = ("order_id", SEQ_COL)) -> DataFrame:
    """Collapse duplicate CDC events.

    The source occasionally emits the same change twice carrying an identical
    ``__seq``. Because the duplicate is byte-identical for ordering purposes,
    ``dropDuplicates`` on the event key ``(order_id, __seq)`` is exact — it
    removes the copy without touching any genuine event.
    """
    return df.dropDuplicates(list(key_cols))


def resolve_current_state(
    df: DataFrame,
    business_key: str = "order_id",
    seq_col: str = SEQ_COL,
) -> DataFrame:
    """Reduce a CDC event stream to one current-state row per order.

    Current state is the row carried by the **highest-``__seq``** event for the
    order; ``order_status`` and ``is_refunded`` are derived from that event's
    ``event_type`` / ``__op``. Duplicate ``__seq`` events are removed first so
    the row count is exact.

    Ordering is by ``__seq``, never ``__commit_ts`` — see the module docstring.
    """
    deduped = dedupe_cdc(df, (business_key, seq_col))
    latest_first = Window.partitionBy(business_key).orderBy(F.col(seq_col).desc())
    latest = (
        deduped.withColumn("_rn", F.row_number().over(latest_first))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
    return latest.withColumn("is_refunded", F.col(OP_COL) == F.lit("D")).withColumn(
        "order_status", _status_expr()
    )


def active_orders(current_state: DataFrame) -> DataFrame:
    """Orders that are still collected revenue — everything not refunded.

    A thin, named filter so downstream fact builders and tests share one
    definition of "active" rather than re-deriving ``NOT is_refunded`` in a
    dozen places.
    """
    return current_state.filter(~F.col("is_refunded"))
