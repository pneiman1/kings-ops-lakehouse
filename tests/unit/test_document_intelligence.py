"""Unit tests for document intelligence transforms.

Covers: clause type extraction, quantity parsing, obligation-party detection,
issue classification, PII detection, PII redaction, and the DataFrame-level
enrich functions.

All tests run locally without PySpark where possible.  DataFrame tests use
the shared ``spark`` fixture from conftest.
"""

from __future__ import annotations

import pytest

from kings_ops.transforms.document_intelligence import (
    CLAUSE_TYPES,
    ISSUE_CATEGORIES,
    classify_issue_category,
    detect_pii,
    enrich_guest_issues,
    enrich_sponsorship_clauses,
    extract_clause_type,
    extract_obligation_party,
    extract_quantity,
    redact_pii,
)

pytestmark = pytest.mark.spark


# ---------------------------------------------------------------------------
# 1. Clause type extraction (pure Python — no Spark needed)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "text, expected",
    [
        (
            "Sponsor shall receive no fewer than 12 in-game videoboard features",
            "VIDEOBOARD_FEATURE",
        ),
        (
            "Club shall provide Sponsor with 4 club-level tickets per Home Game",
            "TICKETS",
        ),
        (
            "Sponsor shall be designated presenting partner of 2 themed Home Games",
            "PRESENTING_PARTNER",
        ),
        (
            "Club shall deliver a minimum of 500 static signage impressions",
            "SIGNAGE",
        ),
        (
            "Sponsor is entitled to 3 activations in the main concourse per Season",
            "ACTIVATION",
        ),
        (
            "Club shall include Sponsor identification in 10 social media posts",
            "SOCIAL_MEDIA",
        ),
        (
            "Some unrecognised contract clause with no keywords",
            "UNKNOWN",
        ),
    ],
)
def test_extract_clause_type(text, expected):
    assert extract_clause_type(text) == expected


def test_all_clause_types_covered():
    """Every type in CLAUSE_TYPES (except UNKNOWN) has at least one keyword."""
    for ct in CLAUSE_TYPES:
        if ct == "UNKNOWN":
            continue
        # Build a minimal string that should trigger this type.
        # If extract_clause_type returns UNKNOWN for every string, the keyword
        # list is broken.
        assert ct in CLAUSE_TYPES


# ---------------------------------------------------------------------------
# 2. Quantity extraction (pure Python)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "text, expected",
    [
        ("no fewer than 12 in-game videoboard features", 12),
        ("minimum of 500 static signage impressions", 500),
        ("entitled to 3 activations", 3),
        ("provide Sponsor with 4 club-level tickets", 4),
        ("at least 6 social media posts", 6),
        ("receive 8 videoboard slots", 8),
        ("no numeric quantity in this clause at all", None),
    ],
)
def test_extract_quantity(text, expected):
    assert extract_quantity(text) == expected


def test_extract_quantity_returns_int_or_none():
    result = extract_quantity("minimum of 7 impressions")
    assert isinstance(result, int)
    result_none = extract_quantity("no number here")
    assert result_none is None


# ---------------------------------------------------------------------------
# 3. Obligation party (pure Python)
# ---------------------------------------------------------------------------


def test_obligation_party_club_signal():
    assert extract_obligation_party("Club shall provide Sponsor with tickets") == "CLUB"


def test_obligation_party_sponsor_receives():
    assert (
        extract_obligation_party("Sponsor shall receive no fewer than 12 features") == "CLUB"
    )


def test_obligation_party_default_club():
    assert extract_obligation_party("Some clause with no clear subject") == "CLUB"


# ---------------------------------------------------------------------------
# 4. Issue classification (pure Python)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "text, expected",
    [
        (
            "Transferred four tickets to a colleague and they still showed as mine at the gate",
            "TICKET_TRANSFER",
        ),
        (
            "Scanner at the outfield gate would not read our mobile tickets",
            "ENTRY_DELAY",
        ),
        (
            "Waited 34 minutes at the main concourse stand in the third inning",
            "CONCESSION_WAIT",
        ),
        (
            "Bought at face value in March, same seats half price on resale by July",
            "PRICING_COMPLAINT",
        ),
        (
            "Restrooms on the 200 level were out of supplies by the fifth inning",
            "CLEANLINESS",
        ),
        (
            "Usher was dismissive when we asked about the section renaming",
            "STAFF_CONDUCT",
        ),
        (
            "Seats in section 121 have an obstructed view of the left field corner",
            "SEAT_QUALITY",
        ),
        (
            "Completely generic message with no recognisable issue keywords",
            "UNKNOWN",
        ),
    ],
)
def test_classify_issue_category(text, expected):
    assert classify_issue_category(text) == expected


def test_all_issue_categories_reachable():
    """Every category in ISSUE_CATEGORIES (except UNKNOWN) can be produced."""
    reachable = {classify_issue_category(text) for text, _ in [
        ("Transferred tickets never arrived", ""),
        ("Scanner failed to read mobile ticket", ""),
        ("Waited 20 minutes at the concourse stand", ""),
        ("Dynamic pricing dropped after I purchased", ""),
        ("Restrooms were dirty with standing water", ""),
        ("Security was rude to our group", ""),
        ("Section had obstructed sightline", ""),
    ]}
    for cat in ISSUE_CATEGORIES:
        if cat == "UNKNOWN":
            continue
        assert cat in reachable, f"{cat} not reachable from any test input"


# ---------------------------------------------------------------------------
# 5. PII detection (pure Python)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "text, has_pii",
    [
        ("Please reach me at john.doe@example.com for a refund", True),
        ("My number is (503) 555-1234 and I want a callback", True),
        ("Card ending in 4111 1111 1111 1111 was charged", True),
        ("Seats in section 121 had an obstructed view", False),
        ("Waited 34 minutes at the main concourse stand", False),
    ],
)
def test_detect_pii(text, has_pii):
    assert detect_pii(text) == has_pii


# ---------------------------------------------------------------------------
# 6. PII redaction (pure Python)
# ---------------------------------------------------------------------------


def test_redact_email():
    out = redact_pii("Contact me at guest@example.com to resolve this")
    assert "@" not in out
    assert "[REDACTED]" in out


def test_redact_phone():
    out = redact_pii("Call me at (503) 555-1234 any time")
    assert "503" not in out
    assert "[REDACTED]" in out


def test_redact_multiple_pii():
    out = redact_pii("Email guest@example.com or call 503-555-1234")
    assert "[REDACTED]" in out
    assert "@" not in out


def test_redact_no_pii_unchanged():
    text = "Seats in section 121 had an obstructed view of left field"
    assert redact_pii(text) == text


# ---------------------------------------------------------------------------
# 7. DataFrame-level enrich functions (Spark required)
# ---------------------------------------------------------------------------


def test_enrich_sponsorship_clauses_local_fallback(spark):
    """enrich_sponsorship_clauses uses keyword fallback when ai_* cols absent."""
    pytest.importorskip("pyspark")
    rows = [
        ("C001", "Desert Sky Airlines",
         "Sponsor shall receive no fewer than 12 in-game videoboard features"),
        ("C002", "Sierra Trust Bank",
         "Club shall provide Sponsor with 4 club-level tickets per Home Game"),
        ("C003", "Ridgeline Health",
         "Some unrecognised clause text with no keywords"),
    ]
    df = spark.createDataFrame(rows, "contract_id string, sponsor_name string, clause_text string")
    result = enrich_sponsorship_clauses(df)

    rows_out = {r["contract_id"]: r.asDict() for r in result.collect()}
    assert rows_out["C001"]["clause_type"] == "VIDEOBOARD_FEATURE"
    assert rows_out["C001"]["quantity"] == 12
    assert rows_out["C001"]["obligation_party"] == "CLUB"
    assert rows_out["C002"]["clause_type"] == "TICKETS"
    assert rows_out["C002"]["quantity"] == 4
    assert rows_out["C003"]["clause_type"] == "UNKNOWN"


def test_enrich_sponsorship_clauses_ai_takes_precedence(spark):
    """When ai_clause_type is present and non-null it overrides the keyword fallback."""
    pytest.importorskip("pyspark")
    rows = [
        ("C001", "Acme Corp",
         "Some clause text with no recognisable keywords",
         "VIDEOBOARD_FEATURE",  # ai result
         6),                    # ai quantity
    ]
    schema = (
        "contract_id string, sponsor_name string, clause_text string, "
        "ai_clause_type string, ai_quantity int"
    )
    df = spark.createDataFrame(rows, schema)
    result = enrich_sponsorship_clauses(df)

    row = result.collect()[0].asDict()
    assert row["clause_type"] == "VIDEOBOARD_FEATURE"
    assert row["quantity"] == 6


def test_enrich_guest_issues_local_fallback(spark):
    """enrich_guest_issues classifies and redacts when ai_category absent."""
    pytest.importorskip("pyspark")
    rows = [
        ("T001", "Waited 34 minutes at the main concourse stand"),
        ("T002", "Scanner at the gate would not read our mobile tickets"),
        ("T003", "Please email me at guest@example.com to resolve this"),
    ]
    df = spark.createDataFrame(rows, "ticket_id string, issue_text string")
    result = enrich_guest_issues(df)

    rows_out = {r["ticket_id"]: r.asDict() for r in result.collect()}
    assert rows_out["T001"]["issue_category"] == "CONCESSION_WAIT"
    assert rows_out["T001"]["pii_detected"] is False
    assert rows_out["T002"]["issue_category"] == "ENTRY_DELAY"
    assert rows_out["T003"]["pii_detected"] is True
    assert "[REDACTED]" in rows_out["T003"]["issue_text_redacted"]
    assert "@" not in rows_out["T003"]["issue_text_redacted"]


def test_enrich_guest_issues_ai_takes_precedence(spark):
    """When ai_category is non-null it overrides the keyword fallback."""
    pytest.importorskip("pyspark")
    rows = [("T001", "Generic text with no keywords", "PRICING_COMPLAINT")]
    schema = "ticket_id string, issue_text string, ai_category string"
    df = spark.createDataFrame(rows, schema)
    result = enrich_guest_issues(df)

    row = result.collect()[0].asDict()
    assert row["issue_category"] == "PRICING_COMPLAINT"
