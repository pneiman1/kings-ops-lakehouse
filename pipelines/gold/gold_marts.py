"""Gold star-schema pipeline — thin wrapper over the pure transforms.

Runs inside a Lakeflow Spark Declarative Pipeline (``pyspark.pipelines`` as
``dp``). As with silver, all logic lives in ``kings_ops.transforms`` and is
unit-tested without a cluster; this file reads silver, calls those functions, and
declares the gold objects.

Materialization choices follow docs/adr/ADR-007:

* ``fact_ticket_order``, ``fact_game_pnl``, ``mart_revenue_by_seat_class`` —
  materialized views (``@dp.table`` on a batch query): stored for fast reads,
  incrementally maintained, queried far more than their inputs change.
* ``dim_date`` — static calendar. Still declared here for lineage, but it is
  generated once; ADR-007 notes a plain table is the strict-minimum form.

The four-way comparison of the revenue mart (table / view / materialized view /
streaming table) lives as runnable SQL in sql/marts/revenue_four_ways/.
"""

from pyspark import pipelines as dp

from kings_ops.transforms.gold import (
    build_dim_date,
    build_fact_game_pnl,
    build_fact_ticket_order,
    revenue_by_seat_class,
)

SILVER_SCHEMA = spark.conf.get("kings.silver_schema", "silver")  # noqa: F821 - spark injected
SEASON_START = spark.conf.get("kings.season_start", "2027-01-01")  # noqa: F821
SEASON_END = spark.conf.get("kings.season_end", "2027-12-31")  # noqa: F821


def _silver(table: str):
    return spark.read.table(f"{SILVER_SCHEMA}.{table}")  # noqa: F821


@dp.table(
    name="fact_ticket_order",
    comment="One row per order, current state. Seat section resolved as of purchase.",
    table_properties={"quality": "gold"},
)
@dp.expect("order_id_present", "order_id IS NOT NULL")
def fact_ticket_order():
    return build_fact_ticket_order(
        _silver("sv_ticket_order"), _silver("dim_seat"), _silver("dim_account")
    )


@dp.table(
    name="fact_game_pnl",
    comment="One row per game. Collected revenue excludes refunds (CDC resolved by __seq).",
    table_properties={"quality": "gold"},
)
def fact_game_pnl():
    return build_fact_game_pnl(dp.read("fact_ticket_order"))


@dp.table(
    name="mart_revenue_by_seat_class",
    comment="Collected revenue by game and seat class. Production form is this MV (ADR-007).",
    table_properties={"quality": "gold"},
)
def mart_revenue_by_seat_class():
    return revenue_by_seat_class(dp.read("fact_ticket_order"))


@dp.table(
    name="dim_date",
    comment="Calendar dimension for the season. Static (ADR-007).",
    table_properties={"quality": "gold"},
)
def dim_date():
    spine = spark.sql(  # noqa: F821
        f"SELECT explode(sequence(DATE'{SEASON_START}', DATE'{SEASON_END}')) AS date"
    )
    return build_dim_date(spine)
