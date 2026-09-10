"""
Bronze layer — streaming ingestion via Auto Loader.

Runs inside a Lakeflow Spark Declarative Pipeline. Note the import: the API is
``pyspark.pipelines``, aliased ``dp``. The older ``dlt`` module still works but
is deprecated, and every tutorial written before 2026 uses it — translate on
sight.

What bronze is for
------------------
Bronze is a faithful, append-only record of what the source actually sent,
plus audit columns describing how and when we received it. It is deliberately
NOT clean. Cleaning in bronze destroys the one thing bronze exists to give you:
the ability to answer "what did they actually send us?" when silver and the
source disagree. Reprocessing from bronze must reproduce silver exactly; that
is only true if bronze is unmodified.

The quarantine pattern used here
--------------------------------
One streaming view reads the source and tags each row valid or invalid against
the contract. Two tables then read that view with opposite predicates. Rows
never vanish: they land in bronze or in quarantine, and the counts reconcile to
the source. Dropping rows with ``expect_or_drop`` alone would satisfy the
pipeline and quietly lose data, which is exactly the outcome contracts exist to
prevent.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from kings_ops.contracts.definitions import get_contract

# Pipeline configuration values, supplied by the bundle. Reads and writes default
# to the catalog and schema set in the pipeline definition, so table names here
# stay unqualified and the same code deploys to dev, stg, and prd unchanged.
LANDING_VOLUME = spark.conf.get("kings.landing_volume")  # noqa: F821 - spark is pipeline-injected
CHECKPOINT_VOLUME = spark.conf.get("kings.checkpoint_volume")  # noqa: F821
ENVIRONMENT = spark.conf.get("kings.environment")  # noqa: F821


def _audit_columns():
    """Provenance stamped on every bronze row.

    ``_ingest_ts`` and ``_source_file`` answer "when did this arrive and from
    where," which is the first question in any ingestion incident.
    ``_contract_version`` answers "what were we promising at the time," which is
    the only way to trace a downstream failure back to a schema change.
    """
    return [
        F.current_timestamp().alias("_ingest_ts"),
        F.col("_metadata.file_path").alias("_source_file"),
        F.col("_metadata.file_modification_time").alias("_source_file_modified_ts"),
        F.lit(ENVIRONMENT).alias("_environment"),
    ]


# --------------------------------------------------------------------------- #
# Gate scans — RESCUE policy
# --------------------------------------------------------------------------- #

_scans = get_contract("gate_scans")


@dp.temporary_view(name="gate_scans_raw")
def gate_scans_raw():
    """Auto Loader read of the scan landing zone.

    Design notes, each of which is a decision rather than boilerplate:

    * ``.schema()`` supplies the contract explicitly instead of letting Auto
      Loader infer. This matters more than it looks: an inferred schema cannot
      be violated, because there is nothing to violate. Explicit schema is what
      makes rescue and failure modes meaningful at all.

    * ``schemaEvolutionMode = rescue`` sends unexpected fields to
      ``_rescued_data`` rather than failing. Scanner firmware shipped
      ``scan_device_os`` mid-season; failing the stream on that would be an
      outage caused by a non-event.

    * ``cloudFiles.schemaLocation`` is where Auto Loader tracks the inferred and
      rescued schema across restarts. It is NOT the streaming checkpoint —
      confusing the two is a common and painful mistake.

    * ``maxFilesPerTrigger`` caps the batch. The scan burst is roughly 40× the
      steady rate, and an uncapped first run would try to swallow the entire
      backlog in one micro-batch.
    """
    return (
        spark.readStream  # noqa: F821
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{CHECKPOINT_VOLUME}/{ENVIRONMENT}/_schema/gate_scans")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("cloudFiles.maxFilesPerTrigger", 200)
        .option("rescuedDataColumn", "_rescued_data")
        .schema(_scans.schema_ddl())
        .load(f"{LANDING_VOLUME}/gate_scans")
        .select("*", *_audit_columns())
        .withColumn("_contract_version", F.lit(_scans.version))
        # Validity is computed once, here, so the bronze and quarantine tables
        # below are guaranteed to partition the input rather than overlap or
        # leak. Two independently-written predicates would drift.
        .withColumn(
            "_is_valid",
            F.expr(_scans.required_columns_expectation())
            & F.col("scan_ts").isNotNull()
            # A scan more than 48h before its file arrived is beyond the agreed
            # lateness bound for offline handhelds and is treated as suspect.
            & (F.col("scan_ts") >= F.col("_source_file_modified_ts") - F.expr("INTERVAL 72 HOURS")),
        )
    )


@dp.table(
    name="br_gate_scan",
    comment="Bronze admission scans. Append-only, contract-conformant rows only.",
    table_properties={"quality": "bronze", "delta.enableChangeDataFeed": "true"},
)
@dp.expect("scan_id_present", "scan_id IS NOT NULL")
@dp.expect("game_id_present", "game_id IS NOT NULL")
def br_gate_scan():
    """Valid scans.

    Note ``@dp.expect`` rather than ``expect_or_drop``: routing already happened
    in the view above, so these expectations exist to RECORD violations in the
    event log for M6 monitoring, not to silently drop rows. If one of these ever
    fires, the routing predicate has a bug — which is precisely the signal worth
    having.
    """
    return dp.read_stream("gate_scans_raw").filter("_is_valid").drop("_is_valid")


@dp.table(
    name="q_gate_scan",
    comment="Quarantined admission scans. Contract violations, retained for triage.",
    table_properties={"quality": "quarantine"},
)
def q_gate_scan():
    """Rejected scans, with the reason attached.

    Quarantine is not a dead-letter bin you never look at. It is the input to
    the data quality metrics in M6, and the reason column is what makes triage
    possible without re-deriving the logic.
    """
    return (
        dp.read_stream("gate_scans_raw")
        .filter("NOT _is_valid")
        .withColumn(
            "_quarantine_reason",
            F.when(F.col("scan_id").isNull(), F.lit("NULL_PRIMARY_KEY"))
            .when(F.col("game_id").isNull(), F.lit("NULL_GAME_ID"))
            .when(F.col("gate").isNull(), F.lit("NULL_GATE"))
            .when(F.col("scan_ts").isNull(), F.lit("NULL_EVENT_TIME"))
            .otherwise(F.lit("LATENESS_BEYOND_CONTRACT")),
        )
        .drop("_is_valid")
    )


# --------------------------------------------------------------------------- #
# Ticketing CDC — FAIL_ON_NEW_COLUMNS policy
# --------------------------------------------------------------------------- #

_cdc = get_contract("ticketing_cdc")


@dp.table(
    name="br_ticketing_cdc",
    comment="Bronze ticketing change events. Ordered by __seq, never by __commit_ts.",
    table_properties={"quality": "bronze"},
)
@dp.expect_or_fail("seq_present", "__seq IS NOT NULL")
@dp.expect("known_operation", "__op IN ('I', 'U', 'D')")
def br_ticketing_cdc():
    """CDC ingestion with the opposite evolution policy to gate scans.

    ``failOnNewColumns`` stops the pipeline when ticketing adds a column. That
    is intentional: an unexpected column here means the operational schema
    changed underneath us, and continuing would write subtly wrong data into
    silver. Subtly wrong is worse than stopped.

    ``expect_or_fail`` on ``__seq`` is the only hard failure in the bronze layer.
    Without a sequence number a change event cannot be ordered, and an
    unorderable CDC stream is not recoverable by any downstream logic — so
    failing loudly beats quarantining quietly.
    """
    return (
        spark.readStream  # noqa: F821
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", f"{CHECKPOINT_VOLUME}/{ENVIRONMENT}/_schema/ticketing_cdc")
        .option("cloudFiles.schemaEvolutionMode", "failOnNewColumns")
        .schema(_cdc.schema_ddl())
        .load(f"{LANDING_VOLUME}/ticketing/cdc")
        .select("*", *_audit_columns())
        .withColumn("_contract_version", F.lit(_cdc.version))
    )


# --------------------------------------------------------------------------- #
# POS — RESCUE policy, batch-shaped arrival
# --------------------------------------------------------------------------- #

_pos = get_contract("pos")


@dp.temporary_view(name="pos_raw")
def pos_raw():
    """POS terminal batches.

    Streaming rather than batch even though arrival is batch-shaped: Auto Loader
    gives exactly-once file tracking for free, which is what handles the
    double-submitted batches the terminals produce. A plain batch read would
    need its own idempotency mechanism, and hand-rolled idempotency is where
    bugs live.
    """
    return (
        spark.readStream  # noqa: F821
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{CHECKPOINT_VOLUME}/{ENVIRONMENT}/_schema/pos")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("rescuedDataColumn", "_rescued_data")
        .schema(_pos.schema_ddl())
        .load(f"{LANDING_VOLUME}/pos")
        .select("*", *_audit_columns())
        .withColumn("_contract_version", F.lit(_pos.version))
        .withColumn("_is_valid", F.expr(_pos.required_columns_expectation()))
    )


@dp.table(
    name="br_pos_line",
    comment="Bronze POS line items. Tax anomalies are NOT corrected here — see M3.",
    table_properties={"quality": "bronze"},
)
@dp.expect("positive_quantity", "quantity > 0")
# Deliberately @dp.expect, not expect_or_drop. Two terminals were commissioned
# with a zero tax rate for three weeks. Those rows are REAL — the club really
# did fail to collect that tax — so bronze must keep them faithfully. Correcting
# them here would erase the evidence of the operational error. The anomaly is
# surfaced as a metric in M3 and reported, not silently patched.
@dp.expect("tax_rate_configured", "tax_rate > 0")
def br_pos_line():
    return dp.read_stream("pos_raw").filter("_is_valid").drop("_is_valid")


@dp.table(
    name="q_pos_line",
    comment="Quarantined POS lines. Malformed records and contract violations.",
    table_properties={"quality": "quarantine"},
)
def q_pos_line():
    return (
        dp.read_stream("pos_raw")
        .filter("NOT _is_valid")
        .withColumn(
            "_quarantine_reason",
            F.when(F.col("txn_line_id").isNull(), F.lit("NULL_PRIMARY_KEY"))
            .when(F.col("txn_id").isNull(), F.lit("NULL_BASKET_ID"))
            .when(F.col("sku").isNull(), F.lit("NULL_SKU"))
            .when(F.col("txn_ts").isNull(), F.lit("NULL_EVENT_TIME"))
            .otherwise(F.lit("CONTRACT_VIOLATION")),
        )
        .drop("_is_valid")
    )
