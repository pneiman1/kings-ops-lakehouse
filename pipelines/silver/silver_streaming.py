"""Silver streaming pipeline — gate scan admission validation.

Runs inside a Lakeflow Spark Declarative Pipeline (``pyspark.pipelines`` as
``dp``). The thin wrapper pattern: watermarking and stateful deduplication live
here because they require an active streaming context; all classification logic
lives in ``kings_ops.transforms.admission`` and is unit-tested without a cluster.

Watermark: 48 hours on ``scan_ts``. See ADR-008.

Stream-static joins
-------------------
``sv_ticket_order`` and ``dim_seat`` are batch-read (static side). Re-reading
each micro-batch means newly resolved ticket orders are picked up automatically
without a stream-stream join (which would require watermarks on both sides and
state on both sides, with no correctness gain — both tables are batch-resolved).

Trigger mode
------------
Free Edition pipeline concurrency budget (ADR-003) means this pipeline runs in
``availableNow`` triggered mode: one micro-batch per run, then stops. Switching
to continuous streaming (``trigger(processingTime="1 minute")``) requires no
logic changes — only the pipeline YAML's trigger configuration.

Output tables
-------------
``sv_admission``      valid admissions: scan_result=ACCEPTED + matched ticket
``q_scan_invalid``    quarantine: orphan tickets, device flags, unknown credentials
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from kings_ops.transforms.admission import build_sv_admission, quarantine_reason

SILVER_SCHEMA = spark.conf.get("kings.silver_schema", "silver")  # noqa: F821 - spark injected

_WATERMARK_HOURS = 48  # see ADR-008 for the full decision


def _silver(table: str):
    """Batch-read a silver table from the same catalog by schema-qualified name."""
    return spark.read.table(f"{SILVER_SCHEMA}.{table}")  # noqa: F821


@dp.temporary_view(name="sv_admission_enriched")
def sv_admission_enriched():
    """Watermarked, deduped, enriched scan stream.

    Ordering of operations matters:

    1. ``withWatermark`` declares the event-time bound for state eviction.
    2. ``dropDuplicates(["scan_id"])`` is the stateful dedup; Spark maintains a
       set of seen ``scan_id`` values and evicts entries when the watermark
       advances past their event time. This handles the same scan arriving from
       multiple file uploads (device retry).
    3. ``build_sv_admission`` then enriches via stream-static joins without
       needing to know it is running in a streaming context.

    Deduplication note: bronze already collapses byte-identical rows via Auto
    Loader's exactly-once file tracking. This dedup layer catches the case where
    the same scan_id appears across two separate file uploads (e.g., a handheld
    that synced, failed, and re-synced).
    """
    scans = (
        dp.read_stream("br_gate_scan")
        .withWatermark("scan_ts", f"{_WATERMARK_HOURS} hours")
        .dropDuplicates(["scan_id"])
    )
    ticket_orders = _silver("sv_ticket_order")
    dim_seat = _silver("dim_seat")
    return build_sv_admission(scans, ticket_orders, dim_seat)


@dp.table(
    name="sv_admission",
    comment=(
        "Validated gate scan admissions. "
        "Grain: one row per deduped scan_id. "
        "is_valid_admission=True iff scan_result=ACCEPTED and ticket matched. "
        "Lag relative to event time: up to 48 hours (see ADR-008)."
    ),
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true",
        "kings.watermark_hours": str(_WATERMARK_HOURS),
    },
)
@dp.expect("scan_id_present", "scan_id IS NOT NULL")
@dp.expect("scan_ts_present", "scan_ts IS NOT NULL")
@dp.expect("game_id_present", "game_id IS NOT NULL")
def sv_admission():
    """Valid admissions routed from the enriched view.

    ``@dp.expect`` (not ``expect_or_drop``): routing already happened in
    ``sv_admission_enriched``. If these expectations fire, the routing predicate
    has a bug — which is exactly the signal worth having in the pipeline event log.
    """
    return dp.read_stream("sv_admission_enriched").filter("is_valid_admission")


@dp.table(
    name="q_scan_invalid",
    comment=(
        "Quarantined scans: orphan tickets, device-flagged duplicates, unknown credentials. "
        "Retained for triage. Counts must reconcile: "
        "br_gate_scan rows = sv_admission rows + q_scan_invalid rows."
    ),
    table_properties={"quality": "quarantine"},
)
def q_scan_invalid():
    """Invalid scans with a human-readable ``_quarantine_reason`` attached.

    The quarantine is not a dead-letter bin. It is the input to freshness and
    quality SLO queries in T7 (observability). The reason column is what makes
    triage possible without re-deriving the routing logic.
    """
    return (
        dp.read_stream("sv_admission_enriched")
        .filter("NOT is_valid_admission")
        .transform(quarantine_reason)
    )
