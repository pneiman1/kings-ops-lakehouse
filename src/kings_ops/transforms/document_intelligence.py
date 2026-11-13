"""Document intelligence transforms — T8.

Two sources require AI-driven enrichment:

``sponsorship`` contracts (JSON text, ~140 docs):
  Clause extraction: structured obligation records from free-form contract prose.
  Columns produced: clause_type, quantity, unit, obligation_party.

``guest_services`` tickets (Parquet, ~31k rows with free text):
  Issue classification: categorise the guest complaint into one of seven buckets.
  PII redaction: replace names, emails, and phone numbers with [REDACTED] before
  writing to gold (required by the row-filter policy on sv_guest_ticket_enriched).

Design pattern — same as every other transform module:
  Pure ``str -> str`` and ``DataFrame -> DataFrame`` functions with no I/O, no
  session construction, no AI Functions calls inside.  The pipeline wrapper in
  ``pipelines/gold/doc_intelligence.py`` calls the Databricks AI Functions
  (``ai_classify``, ``ai_extract``) and passes the result columns here for
  post-processing and schema enforcement.  This keeps the transform logic fully
  unit-testable locally.

See docs/adr/ADR-012-document-intelligence.md for the full decision record.
"""

from __future__ import annotations

import re

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, IntegerType, StringType

# ---------------------------------------------------------------------------
# Sponsorship clause taxonomy
# ---------------------------------------------------------------------------

CLAUSE_TYPES: tuple[str, ...] = (
    "VIDEOBOARD_FEATURE",
    "TICKETS",
    "PRESENTING_PARTNER",
    "SIGNAGE",
    "ACTIVATION",
    "SOCIAL_MEDIA",
    "UNKNOWN",
)

#: Keyword fingerprints for each clause type (checked in order; first match wins).
_CLAUSE_KEYWORDS: tuple[tuple[str, str], ...] = (
    ("VIDEOBOARD_FEATURE", "videoboard"),
    ("TICKETS",            "ticket"),
    ("PRESENTING_PARTNER", "presenting partner"),
    ("SIGNAGE",            "signage"),
    ("ACTIVATION",         "activation"),
    ("SOCIAL_MEDIA",       "social media"),
)

#: Obligation party keywords.
_CLUB_OBLIGATION_SIGNALS = ("club shall", "club is", "club will")
_SPONSOR_OBLIGATION_SIGNALS = ("sponsor shall receive", "sponsor is entitled")


# ---------------------------------------------------------------------------
# Sponsorship clause extraction — pure Python layer
# ---------------------------------------------------------------------------


def extract_clause_type(text: str) -> str:
    """Return the clause taxonomy label for a contract sentence.

    Checks ``_CLAUSE_KEYWORDS`` in order; first keyword present in the
    lower-cased text wins.  Returns ``'UNKNOWN'`` if no keyword matches.

    Used as a local fallback when ``ai_classify`` is unavailable and in
    unit tests.  In production the pipeline uses ``ai_classify`` and calls
    this function only on rows where the AI response is null.
    """
    lower = text.lower()
    for clause_type, keyword in _CLAUSE_KEYWORDS:
        if keyword in lower:
            return clause_type
    return "UNKNOWN"


def extract_quantity(text: str) -> int | None:
    """Return the integer obligation quantity from a contract clause.

    Looks for patterns like:
      "no fewer than 12", "minimum of 4", "entitled to 3", "at least 6",
      "receive {n}" where n is a bare integer.

    Returns ``None`` if no quantity can be found.
    """
    patterns = [
        r"no fewer than\s+(\d+)",
        r"minimum of\s+(\d+)",
        r"entitled to\s+(\d+)",
        r"at least\s+(\d+)",
        r"receive\s+(\d+)",
        r"provide.*?(\d+)",
        r"deliver.*?(\d+)",
        r"include.*?(\d+)",
        r"\b(\d+)\b",  # bare number fallback
    ]
    for pattern in patterns:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            return int(m.group(1))
    return None


def extract_obligation_party(text: str) -> str:
    """Return who carries the obligation: 'CLUB' or 'SPONSOR'.

    If the club is the subject performing the action, returns 'CLUB'.
    If the sponsor is performing, returns 'SPONSOR'.  Defaults to 'CLUB'
    because all generator-produced clauses are club obligations.
    """
    lower = text.lower()
    for signal in _SPONSOR_OBLIGATION_SIGNALS:
        if signal in lower:
            # Sponsor receives/is entitled — obligation is on the club
            return "CLUB"
    for signal in _CLUB_OBLIGATION_SIGNALS:
        if signal in lower:
            return "CLUB"
    return "CLUB"


def enrich_sponsorship_clauses(df: DataFrame) -> DataFrame:
    """Add structured clause columns to a sponsorship DataFrame.

    Expects columns: ``contract_id``, ``sponsor_name``, ``clause_text``.
    Adds: ``clause_type``, ``quantity``, ``obligation_party``.

    When ``ai_clause_type`` is present (populated by the pipeline's AI
    Functions call), it is used in preference to the local keyword fallback.
    When absent or null, ``extract_clause_type`` fills the gap.

    Args:
        df: Sponsorship clause DataFrame.  Must include ``clause_text``.
            May optionally include ``ai_clause_type`` and ``ai_quantity``
            from an upstream ``ai_extract`` call.

    Returns:
        DataFrame with additional columns ``clause_type``, ``quantity``,
        ``obligation_party``.
    """
    extract_type_udf = F.udf(extract_clause_type, StringType())
    extract_qty_udf = F.udf(extract_quantity, IntegerType())
    extract_party_udf = F.udf(extract_obligation_party, StringType())

    has_ai_type = "ai_clause_type" in df.columns
    has_ai_qty = "ai_quantity" in df.columns

    clause_type_col = (
        F.coalesce(F.col("ai_clause_type"), extract_type_udf(F.col("clause_text")))
        if has_ai_type
        else extract_type_udf(F.col("clause_text"))
    )
    quantity_col = (
        F.coalesce(F.col("ai_quantity").cast(IntegerType()), extract_qty_udf(F.col("clause_text")))
        if has_ai_qty
        else extract_qty_udf(F.col("clause_text"))
    )

    return df.withColumn("clause_type", clause_type_col).withColumn(
        "quantity", quantity_col
    ).withColumn("obligation_party", extract_party_udf(F.col("clause_text")))


# ---------------------------------------------------------------------------
# Guest issue classification — pure Python layer
# ---------------------------------------------------------------------------

ISSUE_CATEGORIES: tuple[str, ...] = (
    "SEAT_QUALITY",
    "CONCESSION_WAIT",
    "ENTRY_DELAY",
    "PRICING_COMPLAINT",
    "CLEANLINESS",
    "STAFF_CONDUCT",
    "TICKET_TRANSFER",
    "UNKNOWN",
)

#: Keyword fingerprints (first match wins).
_ISSUE_KEYWORDS: tuple[tuple[str, str], ...] = (
    ("TICKET_TRANSFER",   "transfer"),
    ("TICKET_TRANSFER",   "forwarded"),
    ("ENTRY_DELAY",       "scanner"),
    ("ENTRY_DELAY",       "gate opened"),
    ("ENTRY_DELAY",       "first pitch"),
    ("ENTRY_DELAY",       "mobile ticket"),
    ("CONCESSION_WAIT",   "waited"),
    ("CONCESSION_WAIT",   "line for"),
    ("CONCESSION_WAIT",   "concourse stand"),
    ("CONCESSION_WAIT",   "registers"),
    ("PRICING_COMPLAINT", "face value"),
    ("PRICING_COMPLAINT", "dynamic pricing"),
    ("PRICING_COMPLAINT", "price protection"),
    ("PRICING_COMPLAINT", "resale"),
    ("CLEANLINESS",       "restroom"),
    ("CLEANLINESS",       "standing water"),
    ("CLEANLINESS",       "cleaned"),
    ("STAFF_CONDUCT",     "usher"),
    ("STAFF_CONDUCT",     "security"),
    ("STAFF_CONDUCT",     "rude"),
    ("STAFF_CONDUCT",     "dismissive"),
    ("SEAT_QUALITY",      "obstructed"),
    ("SEAT_QUALITY",      "sightline"),
    ("SEAT_QUALITY",      "shaded"),
    ("SEAT_QUALITY",      "section"),
)


def classify_issue_category(text: str) -> str:
    """Return the guest-issue category for a free-text complaint.

    Uses keyword matching as a local fallback / unit-test substitute for
    the ``ai_classify`` call in the production pipeline.  First match wins.
    Returns ``'UNKNOWN'`` if no keyword matches.
    """
    lower = text.lower()
    for category, keyword in _ISSUE_KEYWORDS:
        if keyword in lower:
            return category
    return "UNKNOWN"


# ---------------------------------------------------------------------------
# PII redaction
# ---------------------------------------------------------------------------

#: Patterns to redact from free text.
_PII_PATTERNS: tuple[re.Pattern, ...] = (
    # Email
    re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}", re.IGNORECASE),
    # US phone: (NXX) NXX-XXXX or NXX-NXX-XXXX or NXX.NXX.XXXX
    re.compile(r"\(?\d{3}\)?[\s.\-]?\d{3}[\s.\-]\d{4}"),
    # Credit/debit card: four groups of 4 digits
    re.compile(r"\b\d{4}[\s\-]\d{4}[\s\-]\d{4}[\s\-]\d{4}\b"),
)


def detect_pii(text: str) -> bool:
    """Return True if any PII pattern is found in the text."""
    return any(p.search(text) for p in _PII_PATTERNS)


def redact_pii(text: str) -> str:
    """Replace all detected PII patterns with ``[REDACTED]``."""
    for pattern in _PII_PATTERNS:
        text = pattern.sub("[REDACTED]", text)
    return text


def enrich_guest_issues(df: DataFrame) -> DataFrame:
    """Add classification and PII-redacted text to a guest-issue DataFrame.

    Expects columns: ``ticket_id``, ``issue_text``.
    May include ``ai_category`` from an upstream ``ai_classify`` call.
    Adds: ``issue_category``, ``issue_text_redacted``, ``pii_detected``.

    Args:
        df: Guest-issue DataFrame.  Must include ``issue_text``.
            May optionally include ``ai_category`` (string) from an AI
            Functions call.

    Returns:
        DataFrame with additional columns ``issue_category``,
        ``issue_text_redacted``, ``pii_detected``.
    """
    classify_udf = F.udf(classify_issue_category, StringType())
    redact_udf = F.udf(redact_pii, StringType())
    detect_udf = F.udf(detect_pii, BooleanType())

    has_ai = "ai_category" in df.columns
    category_col = (
        F.coalesce(F.col("ai_category"), classify_udf(F.col("issue_text")))
        if has_ai
        else classify_udf(F.col("issue_text"))
    )

    return (
        df
        .withColumn("issue_category", category_col)
        .withColumn("issue_text_redacted", redact_udf(F.col("issue_text")))
        .withColumn("pii_detected", detect_udf(F.col("issue_text")))
    )
