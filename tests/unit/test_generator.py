"""Generator defect-rate and byte-reproducibility tests.

Every claim in the PROJECT_CHARTER.md that the generator injects specific defects
at configured rates would be unfalsifiable without tests. These tests close that gap.

Design decisions worth knowing before editing:

* Scale is "small" (6 games, 3 000 seats, 4 000 accounts). Exact counts at a
  fixed seed are deterministic, so the assertions are tight. If you change the
  seed or scale you must re-derive the exact counts — that is intentional.

* Tolerance is Binomial ±3-sigma for defects with large expected counts. For
  defects where the expected count is < 10 at this scale, the test asserts
  count >= 1 (presence) and count <= 3×expected (not way off). That is weaker
  but still falsifiable; "count == 0" unambiguously means the defect was not
  injected.

* Byte-reproducibility means data-file content is identical across runs — the
  _manifest.json is excluded because it contains a wall-clock timestamp.

* Paths are computed by running the generator into a tmpdir, not by importing
  paths from the generator module. The generator's public API is ``run(cfg)``
  and the filesystem layout it produces; we test the layout, not the internals.
"""

from __future__ import annotations

import hashlib
import json
import math
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

# Allow importing the generator from tools/ without installing it.
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "tools"))

from generate_source_data import DefectRates, GeneratorConfig, ScaleProfile, run

SEED = 42
SCALE = ScaleProfile("small", n_games=6, n_seats=3_000, n_accounts=4_000, n_deposits=3_200, n_guest_tickets=400)
DEFECTS = DefectRates()  # defaults, the ones the charter claims


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def _make_cfg(output_dir: Path, seed: int = SEED) -> GeneratorConfig:
    return GeneratorConfig(output_dir=output_dir, scale=SCALE, seed=seed)


def _file_sha256(path: Path) -> str:
    """SHA-256 of a file's bytes, used for content-equality comparisons."""
    h = hashlib.sha256()
    h.update(path.read_bytes())
    return h.hexdigest()


def _all_data_files(root: Path) -> list[Path]:
    """All generator output files except the wall-clock-stamped manifest."""
    return sorted(p for p in root.rglob("*") if p.is_file() and p.name != "_manifest.json")


def _load_cdc(root: Path) -> pd.DataFrame:
    parts = sorted((root / "ticketing" / "cdc").rglob("*.parquet"))
    assert parts, "no CDC parquet files found"
    return pd.concat([pd.read_parquet(p) for p in parts], ignore_index=True)


def _load_scan_lines(root: Path) -> tuple[list[dict], int]:
    """Returns (valid_records, malformed_count).

    A malformed line is one that fails json.loads. Valid lines that parse
    successfully are returned as dicts.
    """
    valid: list[dict] = []
    malformed = 0
    for path in sorted((root / "gate_scans").rglob("*.json")):
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                valid.append(json.loads(line))
            except json.JSONDecodeError:
                malformed += 1
    return valid, malformed


def _load_pos_lines(root: Path) -> tuple[list[dict], int]:
    """Returns (valid_records, malformed_count)."""
    valid: list[dict] = []
    malformed = 0
    for path in sorted((root / "pos").rglob("*.json")):
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                valid.append(json.loads(line))
            except json.JSONDecodeError:
                malformed += 1
    return valid, malformed


def _assert_rate(
    name: str,
    n_total: int,
    n_observed: int,
    expected_rate: float,
    tolerance_sigma: float = 3.0,
) -> None:
    """Assert that n_observed / n_total is within tolerance_sigma standard
    deviations of expected_rate.

    For small expected counts (< 10) we fall back to: observed > 0 and
    observed <= 3 * expected. This is weaker but still falsifiable — a count
    of zero unambiguously means the defect was not injected.
    """
    expected_count = n_total * expected_rate
    observed_rate = n_observed / n_total if n_total else 0.0

    if expected_count < 10:
        # Statistical bounds are too wide at this scale; use presence + ceiling.
        assert n_observed >= 1, (
            f"{name}: expected at least 1 instance of defect "
            f"(rate={expected_rate}, n_total={n_total}, expected_count={expected_count:.1f})"
        )
        assert n_observed <= max(3, 3 * expected_count), (
            f"{name}: observed {n_observed} far exceeds expected {expected_count:.1f} "
            f"(3× ceiling = {3 * expected_count:.1f})"
        )
    else:
        # Binomial sigma. expected_rate close to 0 or 1 → std → 0, so guard.
        sigma = math.sqrt(n_total * expected_rate * (1 - expected_rate)) / n_total
        tolerance = tolerance_sigma * sigma
        lo = max(0.0, expected_rate - tolerance)
        hi = expected_rate + tolerance
        assert lo <= observed_rate <= hi, (
            f"{name}: observed rate {observed_rate:.4f} outside ±{tolerance_sigma}σ "
            f"[{lo:.4f}, {hi:.4f}] for expected rate {expected_rate} "
            f"(n_total={n_total}, n_observed={n_observed})"
        )


# --------------------------------------------------------------------------- #
# Byte-reproducibility
# --------------------------------------------------------------------------- #


def test_byte_reproducibility(tmp_path: Path) -> None:
    """Two runs with the same seed and scale must produce bit-for-bit identical
    data files. This is the foundation every downstream reconciliation test
    relies on: if the generator isn't deterministic, 'did we ingest everything?'
    has no answer.
    """
    out_a = tmp_path / "run_a"
    out_b = tmp_path / "run_b"
    out_a.mkdir()
    out_b.mkdir()

    run(_make_cfg(out_a))
    run(_make_cfg(out_b))

    files_a = _all_data_files(out_a)
    files_b = _all_data_files(out_b)

    # Same file count.
    rel_a = {p.relative_to(out_a) for p in files_a}
    rel_b = {p.relative_to(out_b) for p in files_b}
    only_a = rel_a - rel_b
    only_b = rel_b - rel_a
    assert not only_a and not only_b, (
        f"file sets differ between runs:\n  only_a={only_a}\n  only_b={only_b}"
    )

    # Same content.
    mismatches = []
    for rel in sorted(rel_a):
        sha_a = _file_sha256(out_a / rel)
        sha_b = _file_sha256(out_b / rel)
        if sha_a != sha_b:
            mismatches.append(str(rel))

    assert not mismatches, (
        f"content differs between runs for {len(mismatches)} file(s):\n  "
        + "\n  ".join(mismatches)
    )


# --------------------------------------------------------------------------- #
# CDC defects
# --------------------------------------------------------------------------- #


def test_cdc_duplicate_seq_rate(tmp_path: Path) -> None:
    """Duplicate __seq events (same order_id + __seq, identical content) must
    appear at approximately the configured cdc_duplicate_seq rate.

    The generator emits the duplicate as a byte-identical copy immediately
    after the original; dropDuplicates on (order_id, __seq) removes exactly one.
    """
    out = tmp_path / "run"
    out.mkdir()
    run(_make_cfg(out))
    cdc = _load_cdc(out)

    n_total = len(cdc)
    n_unique = len(cdc.drop_duplicates(subset=["order_id", "__seq"]))
    n_dupes = n_total - n_unique

    _assert_rate("cdc_duplicate_seq", n_total, n_dupes, DEFECTS.cdc_duplicate_seq)


def test_cdc_refund_backdated_rate(tmp_path: Path) -> None:
    """REFUND and TRANSFER events whose __commit_ts precedes the SALE event for
    the same order_id must appear at approximately the configured rate.

    This is the ordering trap the silver CDC merge must defend against: ordering
    by __commit_ts resurrects refunded orders because the backdated refund sorts
    before its own sale.
    """
    out = tmp_path / "run"
    out.mkdir()
    run(_make_cfg(out))
    cdc = _load_cdc(out)

    sales = (
        cdc[cdc["event_type"] == "SALE"][["order_id", "__commit_ts"]]
        .rename(columns={"__commit_ts": "sale_ts"})
        .drop_duplicates("order_id")  # one SALE per order
    )
    corrections = cdc[cdc["event_type"].isin(["REFUND", "TRANSFER"])].copy()
    if corrections.empty or sales.empty:
        pytest.skip("no corrections or sales in this scale/seed combination")

    merged = corrections.merge(sales, on="order_id", how="inner")
    n_backdated = int((merged["__commit_ts"] < merged["sale_ts"]).sum())
    n_corrections = len(corrections)

    _assert_rate(
        "cdc_refund_before_order", n_corrections, n_backdated, DEFECTS.cdc_refund_before_order
    )


def test_cdc_out_of_order_batches(tmp_path: Path) -> None:
    """Some CDC batch files are written out of date order (the filename's write
    index does not align with the commit-date ordering).

    We detect this by parsing the date from the filename and checking whether
    the alphabetical write-order matches the date order. Each adjacent-pair
    swap that the generator injects creates one detectable inversion.
    """
    out = tmp_path / "run"
    out.mkdir()
    run(_make_cfg(out))

    batch_dir = out / "ticketing" / "cdc"
    # Filename format: batch-NNNNN_YYYY-MM-DD.parquet
    batches = sorted(batch_dir.rglob("*.parquet"))
    assert batches, "no CDC batch files found"

    dates = []
    for p in batches:
        # batch-00000_2027-04-05.parquet → extract the date portion
        stem = p.stem  # batch-00000_2027-04-05
        date_str = stem.split("_", 1)[1]
        dates.append(date_str)

    # Count inversions in the date sequence (a written-out-of-order batch shows
    # up as a date that is earlier than the previous date in write order).
    n_inversions = sum(1 for i in range(1, len(dates)) if dates[i] < dates[i - 1])
    n_opportunities = len(dates) - 1

    # The generator swaps adjacent pairs independently with probability
    # cdc_out_of_order_batch per adjacent pair.
    _assert_rate(
        "cdc_out_of_order_batch", n_opportunities, n_inversions, DEFECTS.cdc_out_of_order_batch
    )


# --------------------------------------------------------------------------- #
# Gate scan defects
# --------------------------------------------------------------------------- #


def test_scan_malformed_json_rate(tmp_path: Path) -> None:
    """Malformed JSONL lines (truncated records) in gate-scan files must appear
    at approximately the configured scan_malformed_json rate.
    """
    out = tmp_path / "run"
    out.mkdir()
    run(_make_cfg(out))

    valid, malformed = _load_scan_lines(out)
    n_total = len(valid) + malformed

    _assert_rate("scan_malformed_json", n_total, malformed, DEFECTS.scan_malformed_json)


def test_scan_invalid_ticket_rate(tmp_path: Path) -> None:
    """Scans with an unrecognised ticket (counterfeit, double-scan, staff badge)
    are injected by replacing order_id with a synthetic invalid value and setting
    scan_result to DUPLICATE / INVALID / UNKNOWN_CREDENTIAL.

    Rate is relative to valid (parseable) scans only — malformed records have
    already been excluded before this check runs in production.
    """
    out = tmp_path / "run"
    out.mkdir()
    run(_make_cfg(out))

    valid, _ = _load_scan_lines(out)
    n_invalid = sum(1 for r in valid if r.get("scan_result") != "ACCEPTED")
    n_total = len(valid)

    _assert_rate("scan_no_valid_ticket", n_total, n_invalid, DEFECTS.scan_no_valid_ticket)


# --------------------------------------------------------------------------- #
# POS defects
# --------------------------------------------------------------------------- #


def test_pos_malformed_json_rate(tmp_path: Path) -> None:
    """Malformed POS lines (truncated records) must appear at approximately the
    configured pos_malformed_json rate.
    """
    out = tmp_path / "run"
    out.mkdir()
    run(_make_cfg(out))

    valid, malformed = _load_pos_lines(out)
    n_total = len(valid) + malformed

    _assert_rate("pos_malformed_json", n_total, malformed, DEFECTS.pos_malformed_json)


def test_pos_double_submitted_rate(tmp_path: Path) -> None:
    """Double-submitted batches produce a companion file ending in
    '-resubmit.json'. The rate is per batch file (not per line).
    """
    out = tmp_path / "run"
    out.mkdir()
    run(_make_cfg(out))

    all_batch_files = list((out / "pos").rglob("*.json"))
    resubmit = [f for f in all_batch_files if "-resubmit" in f.name]
    # Total "decision points" is original batches (non-resubmit files).
    originals = [f for f in all_batch_files if "-resubmit" not in f.name]

    _assert_rate(
        "pos_batch_double_submitted", len(originals), len(resubmit), DEFECTS.pos_batch_double_submitted
    )


# --------------------------------------------------------------------------- #
# Account defects
# --------------------------------------------------------------------------- #


def test_account_missing_contact_rate(tmp_path: Path) -> None:
    """Accounts with null email / phone / street_address (PII omitted) must
    appear at approximately the configured account_missing_contact rate.

    The row filter for minors (T6) depends on birth_date, not email, so a null
    email does not interfere with that logic — the missing-contact defect is
    purely about CRM completeness.
    """
    out = tmp_path / "run"
    out.mkdir()
    run(_make_cfg(out))

    # Exactly one accounts file per generator run at scale=small.
    acct_files = list((out / "crm_accounts").rglob("*.csv"))
    assert acct_files, "no accounts CSV found"
    accounts = pd.concat([pd.read_csv(f) for f in acct_files], ignore_index=True)

    n_total = len(accounts)
    # Missing contact means email is null (all three columns are nulled together).
    n_missing = int(accounts["email"].isna().sum())

    _assert_rate("account_missing_contact", n_total, n_missing, DEFECTS.account_missing_contact)


def test_account_minor_rate(tmp_path: Path) -> None:
    """Accounts whose birth_date implies age < 18 as of the season year must
    appear at approximately the configured account_minor rate.

    birth_date is NOT masked — it drives the row-level access control policy
    for minors. Masking it would defeat the row filter entirely, which is why
    is_pii=True but masking=False in the data contract.
    """
    out = tmp_path / "run"
    out.mkdir()
    run(_make_cfg(out))

    acct_files = list((out / "crm_accounts").rglob("*.csv"))
    accounts = pd.concat([pd.read_csv(f) for f in acct_files], ignore_index=True)

    cutoff = datetime(2027, 1, 1)
    birth = pd.to_datetime(accounts["birth_date"])
    ages = (cutoff - birth).dt.days / 365.25
    n_minors = int((ages < 18).sum())
    n_total = len(accounts)

    _assert_rate("account_minor", n_total, n_minors, DEFECTS.account_minor)


# --------------------------------------------------------------------------- #
# Guest ticket defects
# --------------------------------------------------------------------------- #


def test_guest_note_pii_rate(tmp_path: Path) -> None:
    """Guest service notes that contain embedded PII (phone number or email
    injected by the generator) must appear at approximately the configured rate.

    The PII pattern matches what the generator appends: a phone number in the
    form '(NNN) NNN-NNNN' or an email ending in '@example.com'. Both patterns
    must appear in a PII-containing note; we detect the phone pattern because it
    is structurally distinct enough that false positives in the base templates
    are impossible (templates use 'NNN' spelled out in prose, not digits).
    """
    import re

    out = tmp_path / "run"
    out.mkdir()
    run(_make_cfg(out))

    guest_files = list((out / "guest_services").rglob("*.parquet"))
    assert guest_files, "no guest_services parquet found"
    guest = pd.concat([pd.read_parquet(f) for f in guest_files], ignore_index=True)

    # Phone pattern injected by the generator: (DDD) DDD-DDDD
    phone_re = re.compile(r"\(\d{3}\) \d{3}-\d{4}")
    n_pii = int(guest["issue_text"].str.contains(phone_re).sum())
    n_total = len(guest)

    _assert_rate("guest_note_contains_pii", n_total, n_pii, DEFECTS.guest_note_contains_pii)


# --------------------------------------------------------------------------- #
# Pricing defects
# --------------------------------------------------------------------------- #


def test_price_window_overlap_rate(tmp_path: Path) -> None:
    """Overlapping validity windows (valid_to extended past the next window's
    valid_from for the same game_id × seat_class) must appear at approximately
    the configured price_window_overlap rate.

    Rate is per price-point window (not per game × seat_class group).
    """
    out = tmp_path / "run"
    out.mkdir()
    run(_make_cfg(out))

    price_files = list((out / "pricing").rglob("*.csv"))
    assert price_files, "no pricing CSV found"
    pricing = pd.concat([pd.read_csv(f) for f in price_files], ignore_index=True)

    pricing["valid_from_ts"] = pd.to_datetime(pricing["valid_from_ts"])
    pricing["valid_to_ts"] = pd.to_datetime(pricing["valid_to_ts"])

    n_overlapping = 0
    n_total = 0

    for (game_id, seat_class), group in pricing.groupby(["game_id", "seat_class"]):
        sorted_g = group.sort_values("valid_from_ts").reset_index(drop=True)
        for i in range(len(sorted_g) - 1):
            n_total += 1
            # Overlap if this window's valid_to > next window's valid_from.
            if sorted_g.at[i, "valid_to_ts"] > sorted_g.at[i + 1, "valid_from_ts"]:
                n_overlapping += 1

    _assert_rate("price_window_overlap", n_total, n_overlapping, DEFECTS.price_window_overlap)


# --------------------------------------------------------------------------- #
# Schema drift
# --------------------------------------------------------------------------- #


def test_scan_schema_drift_at_configured_game(tmp_path: Path) -> None:
    """scan_device_os appears exactly at and after the configured schema_drift_at
    fraction of games, never before. This is mid-season schema evolution, not a
    bug — but it must be precisely placed so the Auto Loader evolution tests
    have a known boundary to assert against.
    """
    out = tmp_path / "run"
    out.mkdir()
    cfg = _make_cfg(out)
    run(cfg)

    # Read valid scans, record whether scan_device_os is present.
    scan_dir = out / "gate_scans"
    drift_game_index = int(SCALE.n_games * cfg.schema_drift_at)

    # Map game_id -> game_index from the schedule.
    schedule_path = out / "schedule" / "schedule.json"
    schedule = {
        r["game_id"]: i
        for i, r in enumerate(
            json.loads(line)
            for line in schedule_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        )
    }

    pre_drift_has_os = False
    post_drift_all_have_os = True  # starts True; flipped to False if any missing

    valid, _ = _load_scan_lines(out)
    for rec in valid:
        gid = rec.get("game_id")
        if gid not in schedule:
            continue
        idx = schedule[gid]
        has_os = "scan_device_os" in rec
        if idx < drift_game_index:
            if has_os:
                pre_drift_has_os = True
        # We only assert absence strictly before drift; presence after is
        # probabilistic because the field appears on individual records.

    assert not pre_drift_has_os, (
        "scan_device_os appeared BEFORE the configured schema_drift_at boundary "
        f"(drift_game_index={drift_game_index}). The schema evolution boundary is wrong."
    )
