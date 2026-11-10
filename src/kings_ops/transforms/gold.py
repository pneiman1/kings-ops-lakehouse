"""Gold star-schema transforms.

Pure ``DataFrame -> DataFrame`` functions per the grain statements in
docs/architecture/PROJECT_CHARTER.md section 4. No session, no reads, no writes:
a dimension that must be *generated* (dim_date) takes its date spine as an
argument so the session-bound part stays in the thin pipeline wrapper.

    fact_ticket_order   one row per order, current state     accumulating snapshot
    fact_game_pnl       one row per game                      periodic snapshot
    dim_date            one row per calendar date             static

Facts resolve dimensions **as of the event**, not as of now. The seat dimension
is why: sections 118-124 renumbered to 218-224 mid-season, so an order placed in
April must resolve to section 118 and one placed in June to section 218. The
as-of join on dim_seat turns "earlier orders reference IDs that no longer exist"
from a data-quality fire into a solved join. Orders with no covering seat version
are surfaced via ``is_orphan_seat`` rather than silently dropped.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from kings_ops.transforms.scd2 import EFFECTIVE_FROM, EFFECTIVE_TO, IS_CURRENT

_FACT_COLUMNS = (
    "order_id",
    "game_id",
    "seat_id",
    "section_at_purchase",
    "account_id",
    "account_cohort",
    "account_origin_state",
    "seat_class",
    "face_price_usd",
    "paid_price_usd",
    "collected_revenue_usd",
    "is_refunded",
    "order_status",
    "purchased_ts",
    "is_orphan_seat",
)


def resolve_seat_as_of(
    orders: DataFrame,
    dim_seat: DataFrame,
    event_col: str = "purchased_ts",
) -> DataFrame:
    """Attach each order's section **as of its purchase date**.

    Left join on ``seat_id`` and ``effective_from <= purchase_date < effective_to``.
    An order whose seat has no version covering the purchase date keeps a null
    ``section_at_purchase`` — the orphan signal, kept rather than hidden.
    """
    event_date = F.to_date(F.col(f"o.{event_col}"))
    o = orders.alias("o")
    s = dim_seat.select("seat_id", "section", EFFECTIVE_FROM, EFFECTIVE_TO).alias("s")
    return o.join(
        s,
        (F.col("o.seat_id") == F.col("s.seat_id"))
        & (event_date >= F.col(f"s.{EFFECTIVE_FROM}"))
        & (event_date < F.col(f"s.{EFFECTIVE_TO}")),
        "left",
    ).select("o.*", F.col("s.section").alias("section_at_purchase"))


def build_fact_ticket_order(
    orders: DataFrame,
    dim_seat: DataFrame,
    dim_account: DataFrame,
) -> DataFrame:
    """Build ``fact_ticket_order`` — one row per order, current state.

    * Seat section is resolved **as of purchase** (``resolve_seat_as_of``).
    * Account attributes join the **current** dimension version; with a single
      CRM snapshot current == as-of, and the as-of form becomes meaningful once
      multiple snapshots land.
    * ``collected_revenue_usd`` is the paid price for active orders and zero for
      refunds — the measure that makes the backdated-refund fix visible in money.
    """
    with_seat = resolve_seat_as_of(orders, dim_seat)
    account = dim_account.filter(F.col(IS_CURRENT)).select(
        F.col("account_id").alias("_acct_id"),
        F.col("cohort").alias("account_cohort"),
        F.col("origin_state").alias("account_origin_state"),
    )
    joined = with_seat.join(
        account, with_seat["account_id"] == account["_acct_id"], "left"
    ).drop("_acct_id")

    return (
        joined.withColumn(
            "collected_revenue_usd",
            F.when(F.col("is_refunded"), F.lit(0.0)).otherwise(F.col("paid_price_usd")),
        )
        .withColumn("is_orphan_seat", F.col("section_at_purchase").isNull())
        .select(*_FACT_COLUMNS)
    )


def build_fact_game_pnl(fact_ticket_order: DataFrame) -> DataFrame:
    """Build ``fact_game_pnl`` — one row per game, a periodic P&L snapshot.

    Revenue is *collected* revenue (refunds excluded), which is the whole point
    of resolving CDC by ``__seq``: a resurrected refund would overstate this.
    """
    active = (~F.col("is_refunded")).cast("int")
    return fact_ticket_order.groupBy("game_id").agg(
        F.sum(active).alias("tickets_sold"),
        F.sum("collected_revenue_usd").alias("collected_revenue_usd"),
        F.sum(
            F.when(F.col("is_refunded"), F.col("paid_price_usd")).otherwise(F.lit(0.0))
        ).alias("refunded_usd"),
        F.sum("face_price_usd").alias("total_face_usd"),
        F.count(F.lit(1)).alias("order_rows"),
    )


def revenue_by_seat_class(fact_ticket_order: DataFrame) -> DataFrame:
    """Collected revenue by game and seat class — the mart materialized four ways."""
    active = (~F.col("is_refunded")).cast("int")
    return fact_ticket_order.groupBy("game_id", "seat_class").agg(
        F.sum("collected_revenue_usd").alias("collected_revenue_usd"),
        F.sum(active).alias("tickets_sold"),
    )


def build_dim_date(dates: DataFrame, date_col: str = "date") -> DataFrame:
    """Enrich a one-column date spine into ``dim_date``.

    Pure: the caller supplies the spine (``spark.sql("SELECT explode(sequence(...))")``
    in the pipeline wrapper), so no session is constructed here.
    """
    d = F.col(date_col)
    return dates.select(
        d.alias("date"),
        F.date_format(d, "yyyyMMdd").cast("int").alias("date_key"),
        F.year(d).alias("year"),
        F.month(d).alias("month"),
        F.dayofmonth(d).alias("day"),
        F.dayofweek(d).alias("day_of_week"),
        F.date_format(d, "EEEE").alias("day_name"),
        F.dayofweek(d).isin(1, 7).alias("is_weekend"),
    )
