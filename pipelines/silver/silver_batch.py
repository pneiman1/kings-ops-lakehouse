"""Silver batch pipeline — thin wrapper over the pure transforms.

Runs inside a Lakeflow Spark Declarative Pipeline (``pyspark.pipelines`` as
``dp``). Every line of correctness-critical logic lives in
``kings_ops.transforms`` and is unit-tested without a cluster; this file only
reads bronze, calls those functions, and declares the silver tables. That split
is what keeps the test suite runnable on a laptop (see the charter's note on
``transforms/``).

What silver produces here
-------------------------
* ``sv_ticket_order`` — current state per order, resolved from the ticketing CDC
  stream **by __seq**. The source backdates ``__commit_ts`` on corrections, so
  ordering any other way resurrects refunded orders. See ``transforms.cdc``.
* ``dim_seat`` — SCD Type 2. The mid-season section renumbering (118-124 ->
  218-224) splits each affected seat into a closed and a current version, so an
  order placed before the change still resolves to the section it referenced.
* ``dim_account`` — SCD Type 2 over the CRM snapshot. ``scd2_merge`` is the
  reusable engine (unit-tested across multiple snapshots); with the single
  snapshot available today it opens one current version per account.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from kings_ops.transforms.cdc import resolve_current_state
from kings_ops.transforms.scd2 import build_seat_dimension, scd2_merge

BRONZE_SCHEMA = spark.conf.get("kings.bronze_schema", "bronze")  # noqa: F821 - spark is injected
_AUDIT = ("_ingest_ts", "_source_file", "_contract_version")

# Account attributes whose change opens a new SCD2 version. Contact PII changes
# (email/phone/address) are intentionally NOT tracked: they churn without
# business meaning for the renewal book, which is what dim_account serves.
_ACCOUNT_TRACKED = ("cohort", "origin_state", "is_active")


def _bronze(table: str):
    """Batch-read a bronze table from the same catalog, by schema-qualified name."""
    return spark.read.table(f"{BRONZE_SCHEMA}.{table}")  # noqa: F821


@dp.table(
    name="sv_ticket_order",
    comment="Current state per ticket order, resolved from CDC by __seq (never __commit_ts).",
    table_properties={"quality": "silver"},
)
@dp.expect_or_drop("order_id_present", "order_id IS NOT NULL")
def sv_ticket_order():
    return resolve_current_state(_bronze("br_ticketing_cdc"))


@dp.table(
    name="dim_seat",
    comment="SCD2 seat dimension. Renumbered sections split into closed + current versions.",
    table_properties={"quality": "silver"},
)
@dp.expect_or_drop("seat_id_present", "seat_id IS NOT NULL")
def dim_seat():
    seats = _bronze("br_seat_manifest").drop(*_AUDIT)
    return build_seat_dimension(seats)


@dp.table(
    name="dim_account",
    comment="SCD2 account dimension (PII-bearing). Versions on cohort/origin/active status.",
    table_properties={"quality": "silver"},
)
@dp.expect_or_drop("account_id_present", "account_id IS NOT NULL")
def dim_account():
    snapshot = (
        _bronze("br_account")
        .withColumn("__snapshot_date", F.col("_ingest_ts").cast("date"))
        .drop(*_AUDIT)
    )
    return scd2_merge(
        current=None,
        incoming=snapshot,
        key="account_id",
        tracked=_ACCOUNT_TRACKED,
        effective_from="__snapshot_date",
    )
