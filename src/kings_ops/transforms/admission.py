"""Silver streaming transforms — gate scan admission validation.

Pure ``DataFrame -> DataFrame`` functions. No ``SparkSession``, no reads, no
writes. The streaming pipeline wrapper (``pipelines/silver/silver_streaming.py``)
owns the watermark, stateful deduplication, and I/O; every classification rule
lives here so it is unit-testable without a cluster.

Admission grain
---------------
One row per deduped scan event. A scan is a valid admission when ALL of:
  - ``scan_result == "ACCEPTED"`` (device confirmed the credential)
  - A matching order exists in ``sv_ticket_order`` (``is_orphan_ticket = False``)

Invalid scans are **not dropped** — gate counts + quarantine counts must
reconcile to ``br_gate_scan``. They are routed to ``q_scan_invalid`` with a
human-readable reason column.

Stream-static join pattern
--------------------------
``enrich_scan_with_ticket`` and ``enrich_scan_with_seat`` are written as pure
DataFrame transforms. The streaming pipeline calls them with the scan stream on
the left and static batch reads on the right, which is the standard Structured
Streaming stream-static join pattern. Neither function knows or cares whether
the left side is streaming.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from kings_ops.transforms.scd2 import EFFECTIVE_FROM, EFFECTIVE_TO

# Columns projected into sv_admission. Explicit projection prevents
# accidental schema growth when upstream tables add columns.
_SV_ADMISSION_COLS = (
    "scan_id",
    "game_id",
    "seat_id",
    "section_at_scan",
    "gate",
    "scan_ts",
    "device_type",
    "scan_result",
    "order_id",
    "account_id",
    "is_valid_admission",
    "is_orphan_ticket",
    "_ingest_ts",
    "_source_file",
)


def enrich_scan_with_ticket(
    scans: DataFrame,
    ticket_orders: DataFrame,
) -> DataFrame:
    """Left join scans to ticket orders on ``order_id``.

    Parameters
    ----------
    scans:
        Gate scan stream (or batch for testing), already watermarked and
        deduplicated by the pipeline wrapper.
    ticket_orders:
        ``sv_ticket_order`` batch read. Static side of the join; re-read each
        micro-batch so newly resolved orders are picked up automatically.

    Returns
    -------
    Scans with ``account_id`` and ``is_orphan_ticket`` added.
    ``is_orphan_ticket = True`` when ``order_id`` is null on the scan itself
    or when no matching order exists in ``sv_ticket_order``.
    """
    t = ticket_orders.select(
        F.col("order_id").alias("_t_order_id"),
        F.col("account_id"),
    )
    return (
        scans.join(t, scans["order_id"] == F.col("_t_order_id"), "left")
        .withColumn(
            "is_orphan_ticket",
            F.col("order_id").isNull() | F.col("_t_order_id").isNull(),
        )
        .drop("_t_order_id")
    )


def enrich_scan_with_seat(
    scans: DataFrame,
    dim_seat: DataFrame,
) -> DataFrame:
    """Left join scans to ``dim_seat`` as-of the scan date (SCD2).

    Joins on ``seat_id`` and the temporal condition::

        to_date(scan_ts) >= __effective_from AND to_date(scan_ts) < __effective_to

    Sections 118-124 were renumbered to 218-224 mid-season (May 2027). A scan
    taken in April resolves to section 118; one taken in June resolves to 218.
    That is what SCD2 is for.

    A scan whose ``seat_id`` has no dim_seat version covering the scan date gets
    ``section_at_scan = null``. This surfaces the orphan rather than hiding it.
    """
    scan_date = F.to_date(F.col("scan_ts"))
    s = dim_seat.select(
        F.col("seat_id").alias("_s_seat_id"),
        F.col("section").alias("section_at_scan"),
        EFFECTIVE_FROM,
        EFFECTIVE_TO,
    )
    return (
        scans.join(
            s,
            (scans["seat_id"] == F.col("_s_seat_id"))
            & (scan_date >= F.col(EFFECTIVE_FROM))
            & (scan_date < F.col(EFFECTIVE_TO)),
            "left",
        )
        .drop("_s_seat_id", EFFECTIVE_FROM, EFFECTIVE_TO)
    )


def build_sv_admission(
    scans: DataFrame,
    ticket_orders: DataFrame,
    dim_seat: DataFrame,
) -> DataFrame:
    """Build ``sv_admission`` rows by enriching and classifying each scan.

    Callers (the streaming pipeline) are responsible for watermarking and
    stateful deduplication before calling this function. This function is
    pure: same inputs always produce the same output.

    ``is_valid_admission = True`` iff ``scan_result == "ACCEPTED"``
    **and** the ticket order matched. Invalid scans are kept in the output
    so the pipeline can route them to quarantine without losing count.
    """
    with_ticket = enrich_scan_with_ticket(scans, ticket_orders)
    with_seat = enrich_scan_with_seat(with_ticket, dim_seat)

    is_valid = (F.col("scan_result") == F.lit("ACCEPTED")) & ~F.col("is_orphan_ticket")

    return with_seat.withColumn("is_valid_admission", is_valid).select(*_SV_ADMISSION_COLS)


def quarantine_reason(df: DataFrame) -> DataFrame:
    """Attach ``_quarantine_reason`` to invalid scans for triage.

    Priority order matters: ``is_orphan_ticket`` check comes last because an
    orphan scan can also carry ``scan_result = DUPLICATE`` (device-flagged),
    and the device flag is the more specific signal.
    """
    return df.withColumn(
        "_quarantine_reason",
        F.when(F.col("scan_result") == F.lit("DUPLICATE"), F.lit("DUPLICATE_SCAN"))
        .when(F.col("scan_result") == F.lit("INVALID"), F.lit("INVALID_TICKET"))
        .when(
            F.col("scan_result") == F.lit("UNKNOWN_CREDENTIAL"),
            F.lit("UNKNOWN_CREDENTIAL"),
        )
        .when(F.col("is_orphan_ticket"), F.lit("NO_MATCHING_TICKET"))
        .otherwise(F.lit("REJECTED_BY_DEVICE")),
    )
