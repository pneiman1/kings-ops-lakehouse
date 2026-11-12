"""Cart allocation pipeline — thin wrapper over pure transforms.

Runs inside a Lakeflow Spark Declarative Pipeline (``pyspark.pipelines`` as
``dp``).  All logic lives in ``kings_ops.transforms.cart_allocation`` and is
unit-tested without a cluster; this file reads silver/gold tables, calls those
functions, and declares the gold output.

Output table: ``gold.reco_cart_allocation``
Grain: one row per game × zone × 30-min window.
"""

from pyspark import pipelines as dp

from kings_ops.transforms.cart_allocation import build_reco_cart_allocation

SILVER_SCHEMA = spark.conf.get("kings.silver_schema", "silver")  # noqa: F821
GOLD_SCHEMA = spark.conf.get("kings.gold_schema", "gold")  # noqa: F821
N_CARTS = int(spark.conf.get("kings.cart_fleet_size", "12"))  # noqa: F821
ALCOHOL_CUTOFF = int(spark.conf.get("kings.alcohol_cutoff_minutes", "165"))  # noqa: F821


def _silver(table: str):
    return spark.read.table(f"{SILVER_SCHEMA}.{table}")  # noqa: F821


def _gold(table: str):
    return spark.read.table(f"{GOLD_SCHEMA}.{table}")  # noqa: F821


@dp.table(
    name="reco_cart_allocation",
    comment=(
        "Recommended mobile cart positions by game × zone × 30-min window. "
        "Baseline_v1: proportional to prior-game demand. No lookahead (ADR-011)."
    ),
    table_properties={"quality": "gold"},
)
@dp.expect("game_id_present", "game_id IS NOT NULL")
@dp.expect("zone_valid", "zone IS NOT NULL")
@dp.expect("carts_non_negative", "recommended_carts >= 0")
def reco_cart_allocation():
    return build_reco_cart_allocation(
        pos=_silver("sv_pos_basket"),
        dim_game=_gold("dim_game"),
        n_carts=N_CARTS,
        alcohol_cutoff_minutes=ALCOHOL_CUTOFF,
    )
