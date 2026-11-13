"""Document intelligence pipeline — T8.

Thin wrapper over kings_ops.transforms.document_intelligence.  Calls
Databricks AI Functions (ai_classify, ai_extract) on the two AI-enriched
sources and passes the result columns to the pure transforms for
post-processing and schema enforcement.

Output tables
-------------
gold.sv_sponsorship_clause_enriched
    One row per activation clause per contract.  Columns: contract_id,
    sponsor_name, tier, clause_num, clause_text, clause_type, quantity,
    obligation_party.

gold.sv_guest_ticket_enriched
    One row per guest complaint. Columns: guest_ticket_id, account_id,
    game_id, reported_date, channel, issue_category, issue_text_redacted,
    pii_detected.  Raw issue_text is dropped at this layer (PII policy).

Fallback behaviour
------------------
When ai_classify / ai_extract return NULL (model uncertainty, quota, or
the workspace does not yet support AI Functions), the transform layer's
keyword/regex fallback fills the gap.  No row leaves this pipeline without
a label.

See docs/adr/ADR-012-document-intelligence.md.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from kings_ops.transforms.document_intelligence import (
    CLAUSE_TYPES,
    ISSUE_CATEGORIES,
    enrich_guest_issues,
    enrich_sponsorship_clauses,
)

BRONZE_SCHEMA = spark.conf.get("kings.bronze_schema", "bronze")  # noqa: F821
GOLD_SCHEMA = spark.conf.get("kings.gold_schema", "gold")  # noqa: F821

# SQL array literals for ai_classify label sets (UNKNOWN excluded: ai_classify
# returns NULL when it cannot match, which the transform coalesces to UNKNOWN).
_AI_CLAUSE_LABELS = ", ".join(f"'{t}'" for t in CLAUSE_TYPES if t != "UNKNOWN")
_AI_ISSUE_LABELS = ", ".join(f"'{c}'" for c in ISSUE_CATEGORIES if c != "UNKNOWN")


def _bronze(table: str):
    return spark.read.table(f"{BRONZE_SCHEMA}.{table}")  # noqa: F821


# ---------------------------------------------------------------------------
# Sponsorship clause enrichment
# ---------------------------------------------------------------------------


@dp.table(
    name="sv_sponsorship_clause_enriched",
    comment=(
        "One row per activation clause per sponsorship contract. "
        "clause_type, quantity, obligation_party derived by ai_classify/ai_extract "
        "with keyword/regex fallback (ADR-012)."
    ),
    table_properties={"quality": "gold"},
)
@dp.expect("contract_id_present", "contract_id IS NOT NULL")
@dp.expect("clause_type_valid", "clause_type IS NOT NULL")
def sv_sponsorship_clause_enriched():
    contracts = _bronze("br_sponsorship_contract")

    # Split full contract prose into individual clause sentences.
    # The generator concatenates clauses with ". " separators, preceded by a
    # preamble sentence ("SPONSORSHIP AGREEMENT between...").
    # clause_num 0 is the preamble; we skip it and any trailing stub < 20 chars.
    with_clauses = (
        contracts
        .withColumn("clauses", F.split(F.col("contract_text"), r"\. "))
        .select(
            "contract_id", "sponsor_name", "tier",
            F.posexplode("clauses").alias("clause_num", "clause_text"),
        )
        .filter(
            (F.col("clause_num") > 0)
            & (F.length(F.trim(F.col("clause_text"))) > 20)
        )
    )

    # AI Functions — return NULL when unavailable; transform coalesces to fallback.
    with_ai = (
        with_clauses
        .withColumn(
            "ai_clause_type",
            F.expr(f"ai_classify(clause_text, array({_AI_CLAUSE_LABELS}))"),
        )
        .withColumn(
            "ai_quantity",
            F.expr(
                "CAST(ai_extract(clause_text, array('quantity')).quantity AS INT)"
            ),
        )
    )

    return enrich_sponsorship_clauses(with_ai)


# ---------------------------------------------------------------------------
# Guest issue enrichment
# ---------------------------------------------------------------------------


@dp.table(
    name="sv_guest_ticket_enriched",
    comment=(
        "Guest complaint tickets with AI issue classification and PII redaction. "
        "Raw issue_text is dropped at gold (PII policy); only issue_text_redacted "
        "is retained (ADR-012)."
    ),
    table_properties={"quality": "gold"},
)
@dp.expect("ticket_id_present", "guest_ticket_id IS NOT NULL")
@dp.expect("category_valid", "issue_category IS NOT NULL")
@dp.expect("redacted_text_present", "issue_text_redacted IS NOT NULL")
def sv_guest_ticket_enriched():
    tickets = (
        _bronze("br_guest_ticket")
        # issue_category_actual is ground truth; withheld from the model at gold.
        .drop("issue_category_actual")
    )

    # AI classification — NULL when unavailable; transform coalesces to keyword fallback.
    with_ai = tickets.withColumn(
        "ai_category",
        F.expr(f"ai_classify(issue_text, array({_AI_ISSUE_LABELS}))"),
    )

    enriched = enrich_guest_issues(with_ai)

    # Drop the raw PII-bearing text; only the redacted version leaves this layer.
    return enriched.drop("issue_text")
