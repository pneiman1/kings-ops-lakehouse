"""
Synthetic source-system generator — Las Vegas Kings inaugural season (2027).

Produces a landing zone mirroring nine heterogeneous upstream systems for a
first-year MLB expansion club's business operations. Every entity is fabricated;
no league, club, or vendor data is used.

The generator is *deliberately* imperfect. It injects duplicate CDC sequences,
refunds that land before their orders, a mid-season section renumbering, offline
scanners backfilling up to 48 hours, mid-season schema drift, malformed JSON,
misconfigured POS tax rates, overlapping price windows, and PII in free text.
Those defects are the reason the quality layer exists — do not "fix" them here.
In an inaugural season with three-month-old systems, they are also realistic.

Design decisions worth knowing before editing:

  * Determinism is non-negotiable. Every draw goes through one seeded
    ``numpy.random.Generator``. Same seed and scale produces identical output,
    which is what makes CI integration tests possible at all.

  * Defect rates are configuration, not literals. Quality tests assert against
    the configured rates, so a quarantine test can prove it caught exactly the
    rows that were planted.

  * Draws are vectorized per game rather than looped per person. A row-at-a-time
    loop over 1.9M scans is twenty minutes of nothing; the same work in numpy is
    seconds. Readability is preserved by keeping one game per iteration.

  * Output paths mirror the Unity Catalog Volume layout exactly, so the local
    filesystem and the Volume are interchangeable Auto Loader targets.

  * A manifest with per-dataset row counts is written last. Bronze
    reconciliation tests read it; without it "did we ingest everything?" is
    unanswerable.

Usage
-----
    python tools/generate_source_data.py --scale small  --output-dir ./landing
    python tools/generate_source_data.py --scale medium --output-dir /Volumes/kings_dev/landing/files
    python tools/generate_source_data.py --scale full   --output-dir ./landing --seed 42

Requires: python>=3.10, pandas, numpy, pyarrow
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

LOGGER = logging.getLogger("kings_ops.generator")

SEASON_YEAR = 2027
OPENING_DAY = datetime(SEASON_YEAR, 4, 5, 0, 0, 0)

# --------------------------------------------------------------------------- #
# Reference data — all fabricated
# --------------------------------------------------------------------------- #

# Fictional opponents with a draw factor. Draw is the single largest controllable
# driver of single-game demand and it is what novelty decay must be separated from.
OPPONENTS = [
    ("Portland Pioneers", 1.34), ("Brooklyn Bridges", 1.28), ("Austin Armadillos", 1.19),
    ("Charlotte Current", 1.11), ("Nashville Notes", 1.08), ("Sacramento Sierras", 1.02),
    ("Tucson Thorns", 0.94), ("Buffalo Blizzard", 0.88), ("Wichita Wheat", 0.82),
    ("Duluth Drift", 0.78),
]

# Seat classes: (class, share_of_house, face_price, per_cap_multiplier)
# Per-cap multiplier matters — premium buyers spend more inside the gates, so
# concession revenue is not proportional to headcount.
SEAT_CLASSES = [
    ("FIELD_PREMIUM", 0.06, 185.0, 2.40),
    ("INFIELD_BOX", 0.16, 96.0, 1.55),
    ("BASELINE_BOX", 0.14, 68.0, 1.25),
    ("OUTFIELD_RESERVED", 0.22, 42.0, 1.00),
    ("UPPER_INFIELD", 0.18, 34.0, 0.86),
    ("UPPER_RESERVED", 0.17, 22.0, 0.72),
    ("STANDING_ROOM", 0.04, 18.0, 1.10),
    ("SUITE", 0.03, 320.0, 3.10),
]

GATES = [
    ("HOME_PLATE", 0.31, False), ("LEFT_FIELD", 0.19, False), ("RIGHT_FIELD", 0.17, False),
    ("CENTER_FIELD", 0.14, True), ("SUITE_ENTRY", 0.07, False), ("OUTFIELD_A", 0.07, True),
    ("OUTFIELD_B", 0.05, True),
]

# Buyer cohorts. The whole Vegas analytic angle lives in these three rows: they
# differ on purchase timing, no-show rate, and spend simultaneously, so any model
# treating attendance as one population is wrong in three directions at once.
COHORTS = [
    # (cohort, share, no_show_rate, per_cap_usd, days_before_purchase_mean)
    ("LOCAL_PLAN", 0.34, 0.21, 21.50, 74.0),
    ("TOURIST", 0.47, 0.04, 41.80, 9.0),
    ("GROUP_BLOCK", 0.19, 0.13, 27.40, 96.0),
]

PROMOTIONS = [
    ("OPENING_DAY", 1.55), ("BOBBLEHEAD", 1.31), ("FIREWORKS", 1.24),
    ("DOLLAR_DOG", 1.14), ("JERSEY_GIVEAWAY", 1.29), ("KIDS_RUN_BASES", 1.09),
    ("HALF_PRICE_TUESDAY", 1.12), ("NONE", 1.00),
]

POS_ITEMS = [
    # (sku, name, category, price, attach_rate)
    ("SKU-1001", "Hot Dog", "FOOD", 7.50, 0.34), ("SKU-1002", "Nachos", "FOOD", 11.00, 0.16),
    ("SKU-1003", "Bratwurst", "FOOD", 9.75, 0.09), ("SKU-1004", "Street Tacos (3)", "FOOD", 14.00, 0.14),
    ("SKU-1005", "Pretzel", "FOOD", 8.00, 0.11), ("SKU-1006", "Chicken Tenders", "FOOD", 15.50, 0.08),
    ("SKU-2001", "Domestic Draft", "ALCOHOL", 13.00, 0.29), ("SKU-2002", "Craft Draft", "ALCOHOL", 16.00, 0.18),
    ("SKU-2003", "Seltzer", "ALCOHOL", 14.00, 0.11), ("SKU-2004", "Cocktail", "ALCOHOL", 19.00, 0.09),
    ("SKU-3001", "Soda", "NA_BEVERAGE", 7.00, 0.26), ("SKU-3002", "Bottled Water", "NA_BEVERAGE", 6.00, 0.22),
    ("SKU-4001", "Replica Cap", "MERCH", 42.00, 0.06), ("SKU-4002", "Inaugural Tee", "MERCH", 38.00, 0.09),
    ("SKU-4003", "Authentic Jersey", "MERCH", 165.00, 0.02), ("SKU-4004", "Program", "MERCH", 12.00, 0.04),
]

FIRST_NAMES = [
    "James", "Maria", "Robert", "Linda", "Michael", "Patricia", "David", "Jennifer", "Carlos",
    "Susan", "Daniel", "Karen", "Anthony", "Nancy", "Marcus", "Lisa", "Kevin", "Betty", "Jose",
    "Sandra", "Brian", "Ashley", "Steven", "Dorothy", "Andre", "Kimberly", "Raymond", "Emily",
    "Priya", "Wei", "Fatima", "Diego", "Hana", "Omar", "Grace", "Tyler",
]
LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez",
    "Martinez", "Hernandez", "Lopez", "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson",
    "Martin", "Lee", "Perez", "Thompson", "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis",
    "Nguyen", "Patel", "Kim", "Okafor", "Rossi", "Novak",
]

# Visitor origin for the tourist cohort. Origin is a real analytic dimension in
# this market: it predicts spend, arrival timing, and single-game vs plan buying.
VISITOR_ORIGINS = [
    ("CA", 0.31), ("AZ", 0.11), ("UT", 0.07), ("WA", 0.06), ("TX", 0.06), ("IL", 0.05),
    ("NY", 0.05), ("CO", 0.04), ("OR", 0.04), ("FL", 0.03), ("OTHER_US", 0.13), ("INTL", 0.05),
]

GUEST_ISSUE_TEMPLATES = {
    "SEAT_QUALITY": [
        "Seats in section 121 have an obstructed view of the left field corner, the support column blocks the entire line. Nobody mentioned this at purchase.",
        "Our row was directly in the sun until the sixth inning even with the roof configuration they announced. Section is advertised as shaded.",
    ],
    "CONCESSION_WAIT": [
        "Waited 34 minutes at the main concourse stand in the third inning. Only two registers open out of six.",
        "Line for beer wrapped around the concourse. Missed two innings. Staff seemed to be still learning the register system.",
    ],
    "ENTRY_DELAY": [
        "Scanner at the outfield gate would not read our mobile tickets, staff had to manually enter every one. Took 25 minutes to get through.",
        "Gate opened late and the handheld scanners kept disconnecting. We missed first pitch entirely.",
    ],
    "PRICING_COMPLAINT": [
        "Bought at face value in March and the same seats were half that price on the resale market by July. Feels like plan holders are being punished.",
        "Dynamic pricing dropped the price of my section by 40 percent three days after I purchased. No price protection offered.",
    ],
    "CLEANLINESS": [
        "Restrooms on the 200 level were out of supplies by the fifth inning and there was standing water on the floor.",
        "Our row had not been cleaned from the prior game, there were cups and wrappers under every seat.",
    ],
    "STAFF_CONDUCT": [
        "Usher was dismissive when we asked about the seat numbering, said the section had changed and would not explain further.",
        "Security at the suite entry was rude to our group and would not check the guest list they had been given.",
    ],
    "TICKET_TRANSFER": [
        "Transferred four tickets to a colleague and the system showed them as still assigned to me at the gate. Had to resolve at the box office.",
        "Forwarded tickets never arrived in the recipient account. Support could not locate the transfer for two days.",
    ],
}

SPONSOR_NAMES = [
    "Desert Sky Airlines", "Sierra Trust Bank", "Ridgeline Health Network", "Copper State Motors",
    "Highline Telecom", "Vista Beverages", "Redstone Insurance", "Nova Energy", "Arcadia Resorts",
    "Pinnacle Logistics",
]

SPONSOR_CLAUSES = [
    "Sponsor shall receive no fewer than {n} in-game videoboard features of not less than thirty (30) seconds per Home Game.",
    "Club shall provide Sponsor with {n} club-level tickets per Home Game for the duration of the Term.",
    "Sponsor shall be designated presenting partner of {n} themed Home Games per Season, dates subject to mutual approval.",
    "Club shall deliver a minimum of {n} static signage impressions per Home Game in the outfield rotational position.",
    "Sponsor is entitled to {n} activations in the main concourse per Season, each of not less than three (3) hours duration.",
    "Club shall include Sponsor identification in {n} social media posts per Season across Club-owned channels.",
]


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class DefectRates:
    """Injected defect rates. Quality tests assert against these values, so
    changing one here should make a corresponding test fail loudly."""

    cdc_duplicate_seq: float = 0.003
    cdc_refund_before_order: float = 0.02      # the classic CDC ordering trap
    cdc_out_of_order_batch: float = 0.06

    scan_no_valid_ticket: float = 0.004        # counterfeit / double-scan / staff badge
    scan_malformed_json: float = 0.002
    scan_late_upload: float = 0.030            # correlated with handheld gates
    scan_max_lateness_hours: int = 48

    pos_malformed_json: float = 0.001
    pos_batch_dropped: float = 0.012
    pos_batch_double_submitted: float = 0.008

    account_missing_contact: float = 0.018
    account_minor: float = 0.021               # youth camp rosters
    guest_note_contains_pii: float = 0.07

    price_window_overlap: float = 0.04


@dataclass(frozen=True)
class ScaleProfile:
    name: str
    n_games: int
    n_seats: int
    n_accounts: int
    n_deposits: int
    n_guest_tickets: int


SCALE_PROFILES: dict[str, ScaleProfile] = {
    "small": ScaleProfile("small", 6, 3_000, 4_000, 3_200, 400),
    "medium": ScaleProfile("medium", 27, 12_000, 18_000, 14_000, 4_200),
    "full": ScaleProfile("full", 81, 33_000, 44_000, 38_000, 31_000),
}


@dataclass(frozen=True)
class GeneratorConfig:
    output_dir: Path
    scale: ScaleProfile
    seed: int
    defects: DefectRates = field(default_factory=DefectRates)
    # Fraction of the season after which gate scanners start reporting device OS.
    schema_drift_at: float = 0.45
    # Fraction of the season at which sections 118-124 are renumbered to 218-224.
    renumber_at: float = 0.22


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def _weighted(rng: np.random.Generator, options: list[tuple], size: int, idx: int = 1) -> np.ndarray:
    values = [o[0] for o in options]
    weights = np.array([o[idx] for o in options], dtype=float)
    return rng.choice(values, size=size, p=weights / weights.sum())


def _write_parquet(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False, engine="pyarrow", compression="snappy")


def _write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def _write_jsonl(lines: list[str], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


# --------------------------------------------------------------------------- #
# Schedule
# --------------------------------------------------------------------------- #


def generate_schedule(cfg: GeneratorConfig, rng: np.random.Generator) -> pd.DataFrame:
    """81 home dates across a fabricated 2027 season.

    Demand drivers are explicit columns rather than baked into attendance, so the
    novelty-decay decomposition in gold has something real to control for. That
    decomposition is the analytic centerpiece of an expansion season: without
    opponent draw, day-of-week, and promo as separate factors, a sagging August
    is indistinguishable from a losing record.
    """
    n = cfg.scale.n_games
    rows = []
    date = OPENING_DAY
    opp_names = [o[0] for o in OPPONENTS]
    opp_draw = {o[0]: o[1] for o in OPPONENTS}

    wins = 0
    for i in range(n):
        # Home dates cluster in 3-4 game series with road gaps between.
        if i > 0:
            gap = 1 if (i % 3) else int(rng.integers(3, 8))
            date = date + timedelta(days=gap)

        opponent = str(rng.choice(opp_names))
        dow = date.weekday()  # 0=Mon
        is_weekend = dow >= 4
        night_game = bool(rng.random() < (0.58 if is_weekend else 0.86))
        first_pitch = date.replace(hour=19 if night_game else 13, minute=10)

        promo = str(_weighted(rng, PROMOTIONS, 1)[0]) if i > 0 else "OPENING_DAY"
        outside_temp = 62 + 44 * np.sin((date.timetuple().tm_yday - 100) / 365 * np.pi) + rng.normal(0, 6)
        # Retractable roof: closed above 95F outside, or in wind. Roof state drives
        # climate load, which is a real per-game operating cost in this market.
        roof_closed = bool(outside_temp > 95 or rng.random() < 0.12)

        # Inaugural-season novelty decay: steep early, asymptotic later. This is the
        # signal the platform must separate from won-loss effects.
        progress = i / max(n - 1, 1)
        novelty = 0.62 * np.exp(-3.1 * progress) + 0.55

        win_pct = wins / max(i, 1) if i else 0.5
        wins += int(rng.random() < 0.46)

        rows.append(
            {
                "game_id": f"G{SEASON_YEAR}{i + 1:03d}",
                "game_date": date.date().isoformat(),
                "first_pitch_ts": first_pitch.isoformat(),
                "opponent": opponent,
                "opponent_draw_factor": opp_draw[opponent],
                "day_of_week": date.strftime("%A"),
                "is_weekend": is_weekend,
                "is_night_game": night_game,
                "promotion": promo,
                "promotion_lift": dict(PROMOTIONS)[promo],
                "outside_temp_f": round(float(outside_temp), 1),
                "roof_closed": roof_closed,
                "home_game_number": i + 1,
                "novelty_index": round(float(novelty), 4),
                "entering_win_pct": round(float(win_pct), 3),
                "result": str(rng.choice(["W", "L"], p=[0.46, 0.54])),
            }
        )
    df = pd.DataFrame(rows)
    LOGGER.info("schedule: %d home games, %s to %s", len(df), df.game_date.iloc[0], df.game_date.iloc[-1])
    return df


# --------------------------------------------------------------------------- #
# Seats, accounts, deposits
# --------------------------------------------------------------------------- #


def generate_seats(cfg: GeneratorConfig, rng: np.random.Generator) -> pd.DataFrame:
    """Seat manifest.

    ``renumbered_to`` carries the May section renumbering (118-124 -> 218-224)
    that follows sightline complaints. Orders written before the change reference
    the old IDs, which is what produces orphan foreign keys downstream. This is
    the SCD2 driver for dim_seat and it is not decoration — first-year venues
    really do renumber.
    """
    n = cfg.scale.n_seats
    classes = _weighted(rng, SEAT_CLASSES, n)
    face = {c[0]: c[2] for c in SEAT_CLASSES}
    percap = {c[0]: c[3] for c in SEAT_CLASSES}

    sections = np.where(
        np.isin(classes, ["FIELD_PREMIUM", "INFIELD_BOX"]), rng.integers(101, 130, n),
        np.where(np.isin(classes, ["BASELINE_BOX", "OUTFIELD_RESERVED"]), rng.integers(130, 160, n),
                 np.where(classes == "SUITE", rng.integers(301, 340, n), rng.integers(201, 260, n))),
    )

    df = pd.DataFrame(
        {
            "seat_id": [f"S{i:06d}" for i in range(1, n + 1)],
            "section": sections.astype(int),
            "row_label": rng.choice(list("ABCDEFGHJKLMNPQRSTUV"), size=n),
            "seat_number": rng.integers(1, 30, size=n),
            "seat_class": classes,
            "face_price_usd": [face[c] for c in classes],
            "per_cap_multiplier": [percap[c] for c in classes],
            "is_ada": rng.random(n) < 0.021,
            "is_shaded_day": rng.random(n) < 0.44,
        }
    )

    # The renumbering: sections 118-124 become 218-224 mid-season.
    affected = df["section"].between(118, 124)
    df["renumbered_to_section"] = np.where(affected, df["section"] + 100, None)
    df["renumber_effective_date"] = np.where(
        affected,
        (OPENING_DAY + timedelta(days=int(180 * cfg.renumber_at))).date().isoformat(),
        None,
    )
    LOGGER.info("seats: %d rows (%d affected by renumbering)", len(df), int(affected.sum()))
    return df


def generate_accounts(cfg: GeneratorConfig, rng: np.random.Generator) -> pd.DataFrame:
    """CRM accounts, PII-heavy.

    ``birth_date`` is present so a fraction of accounts resolve as minors (youth
    camp rosters). Those rows are the reason the governance tranche needs a row
    filter and not merely a column mask — minors are not a masking problem, they
    are an access problem.
    """
    n = cfg.scale.n_accounts
    cohorts = _weighted(rng, COHORTS, n)
    first = rng.choice(FIRST_NAMES, size=n)
    last = rng.choice(LAST_NAMES, size=n)

    origins = np.where(
        cohorts == "TOURIST", _weighted(rng, VISITOR_ORIGINS, n), "NV"
    )

    is_minor = rng.random(n) < cfg.defects.account_minor
    ages = np.where(is_minor, rng.integers(8, 18, n), rng.integers(19, 78, n))
    birth = [(datetime(SEASON_YEAR, 1, 1) - timedelta(days=int(a) * 365 + int(rng.integers(0, 364)))).date().isoformat()
             for a in ages]

    df = pd.DataFrame(
        {
            "account_id": [f"ACC-{i:07d}" for i in range(1, n + 1)],
            "first_name": first,
            "last_name": last,
            "email": [f"{f.lower()}.{l.lower()}{rng.integers(1, 999)}@example.com" for f, l in zip(first, last)],
            "phone": [f"({rng.integers(200, 990)}) {rng.integers(200, 990)}-{rng.integers(1000, 9999)}" for _ in range(n)],
            "street_address": [f"{rng.integers(100, 9899)} {rng.choice(['Desert', 'Paradise', 'Flamingo', 'Sahara', 'Tropicana', 'Charleston'])} {rng.choice(['Rd', 'Ave', 'Blvd'])}" for _ in range(n)],
            "postal_code": [f"{rng.integers(89000, 89200)}" if o == "NV" else f"{rng.integers(10000, 99999)}" for o in origins],
            "origin_state": origins,
            "birth_date": birth,
            "cohort": cohorts,
            "payment_token_last4": [f"{rng.integers(1000, 9999)}" for _ in range(n)],
            "acquisition_channel": rng.choice(
                ["DEPOSIT_FUNNEL", "WEB_DIRECT", "HOTEL_PARTNER", "CONVENTION_BLOCK", "WALKUP", "RESALE_IMPORT"],
                size=n, p=[0.24, 0.31, 0.16, 0.11, 0.10, 0.08],
            ),
            "created_ts": [(OPENING_DAY - timedelta(days=int(rng.integers(1, 900)))).isoformat() for _ in range(n)],
            "is_active": rng.random(n) > 0.04,
        }
    )

    missing = rng.random(n) < cfg.defects.account_missing_contact
    df.loc[missing, ["email", "phone", "street_address"]] = None
    LOGGER.info("accounts: %d rows (%d minors, %d missing contact)",
                len(df), int(is_minor.sum()), int(missing.sum()))
    return df


def generate_deposits(cfg: GeneratorConfig, rng: np.random.Generator, accounts: pd.DataFrame) -> pd.DataFrame:
    """Season-ticket deposit funnel — an expansion-only artifact.

    Deposits were placed up to 30 months before first pitch and converted through
    priority-ranked seat-selection events. This is the ONLY forward-looking demand
    signal available before Opening Day, which makes it the anchor for cold-start
    forecasting. It will never exist again after year one.
    """
    n = min(cfg.scale.n_deposits, len(accounts))
    holders = accounts.sample(n=n, random_state=cfg.seed)["account_id"].to_numpy()

    placed_days_before = rng.integers(120, 900, size=n)
    priority = np.argsort(np.argsort(placed_days_before))[::-1] + 1  # earlier deposit = better rank

    # Conversion is priority-dependent: early depositors got the seats they wanted
    # and converted; late depositors were offered leftovers and walked.
    priority_pct = priority / n
    convert_p = np.clip(0.93 - 0.55 * priority_pct, 0.15, 0.95)
    converted = rng.random(n) < convert_p
    forfeited = (~converted) & (rng.random(n) < 0.42)

    selection_wave = np.select(
        [priority_pct < 0.15, priority_pct < 0.40, priority_pct < 0.72],
        ["WAVE_1_PREMIUM", "WAVE_2_PRIORITY", "WAVE_3_GENERAL"],
        default="WAVE_4_REMAINING",
    )

    df = pd.DataFrame(
        {
            "deposit_id": [f"DEP-{i:07d}" for i in range(1, n + 1)],
            "account_id": holders,
            "deposit_amount_usd": rng.choice([100.0, 250.0, 500.0, 1000.0], size=n, p=[0.44, 0.31, 0.17, 0.08]),
            "placed_ts": [(OPENING_DAY - timedelta(days=int(d))).isoformat() for d in placed_days_before],
            "priority_rank": priority.astype(int),
            "selection_wave": selection_wave,
            "selection_event_ts": [
                (OPENING_DAY - timedelta(days=int(rng.integers(35, 110)))).isoformat() if c else None
                for c in converted
            ],
            "converted_to_plan": converted,
            "plan_type": np.where(
                converted, rng.choice(["FULL_SEASON", "HALF_SEASON", "QUARTER_PLAN", "FLEX_20"],
                                      size=n, p=[0.21, 0.27, 0.31, 0.21]), None),
            "seats_selected": np.where(converted, rng.integers(1, 7, size=n), 0),
            "deposit_forfeited": forfeited,
            "refunded_ts": [
                (OPENING_DAY - timedelta(days=int(rng.integers(5, 60)))).isoformat()
                if (not c and not f) else None
                for c, f in zip(converted, forfeited)
            ],
        }
    )
    LOGGER.info("deposits: %d rows (%.1f%% converted, %d forfeited)",
                len(df), 100 * converted.mean(), int(forfeited.sum()))
    return df


# --------------------------------------------------------------------------- #
# Pricing
# --------------------------------------------------------------------------- #


def generate_pricing(cfg: GeneratorConfig, rng: np.random.Generator, schedule: pd.DataFrame) -> pd.DataFrame:
    """Dynamic pricing feed with effective-dated windows.

    A configured fraction of windows OVERLAP, which is the defect: a temporal join
    against overlapping validity silently fans out and doubles revenue. Catching
    that is exactly what the silver expectations are for.
    """
    rows = []
    classes = [c[0] for c in SEAT_CLASSES]
    face = {c[0]: c[2] for c in SEAT_CLASSES}

    for game in schedule.to_dict(orient="records"):
        first_pitch = datetime.fromisoformat(game["first_pitch_ts"])
        demand = (
            game["opponent_draw_factor"] * game["promotion_lift"] * game["novelty_index"]
            * (1.18 if game["is_weekend"] else 0.92)
        )
        for seat_class in classes:
            # Price walks as the game approaches; direction depends on demand.
            n_windows = int(rng.integers(4, 9))
            offsets = sorted(rng.integers(1, 120, size=n_windows).tolist(), reverse=True)
            base = face[seat_class]
            for w, days_out in enumerate(offsets):
                mult = demand ** (0.6 + 0.4 * (1 - days_out / 120)) * float(rng.uniform(0.88, 1.14))
                valid_from = first_pitch - timedelta(days=int(days_out))
                next_out = offsets[w + 1] if w + 1 < len(offsets) else 0
                valid_to = first_pitch - timedelta(days=int(next_out))

                # Defect: overlapping validity window.
                if rng.random() < cfg.defects.price_window_overlap:
                    valid_to = valid_to + timedelta(days=float(rng.uniform(0.5, 3.0)))

                # Normalize to second precision. Fractional-day arithmetic above
                # otherwise leaves microseconds on SOME rows and not others, and a
                # column with mixed ISO precision silently fails strict parsing
                # downstream. Unintentional defects are not acceptable here: every
                # defect in this generator must be configured and asserted, or it
                # is just a bug wearing a costume.
                valid_from = valid_from.replace(microsecond=0)
                valid_to = valid_to.replace(microsecond=0)

                rows.append(
                    {
                        "price_point_id": f"PP-{len(rows) + 1:08d}",
                        "game_id": game["game_id"],
                        "seat_class": seat_class,
                        "face_price_usd": base,
                        "listed_price_usd": round(base * mult, 2),
                        "valid_from_ts": valid_from.isoformat(),
                        "valid_to_ts": valid_to.isoformat(),
                        "pricing_rule": str(rng.choice(
                            ["DEMAND_ELASTIC", "INVENTORY_THRESHOLD", "COMP_MARKET", "MANUAL_OVERRIDE"],
                            p=[0.51, 0.24, 0.16, 0.09])),
                    }
                )
    df = pd.DataFrame(rows)
    LOGGER.info("pricing: %d price points", len(df))
    return df


# --------------------------------------------------------------------------- #
# Ticketing CDC
# --------------------------------------------------------------------------- #


def generate_ticket_cdc(
    cfg: GeneratorConfig, rng: np.random.Generator, schedule: pd.DataFrame,
    seats: pd.DataFrame, accounts: pd.DataFrame, pricing: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Ticket orders decomposed into CDC events.

    Three deliberate traps, all of which a naive append-only ingest gets wrong:

      1. Duplicate events carrying an identical ``__seq``.
      2. **Refund and transfer events whose ``__commit_ts`` precedes the SALE
         they reference.** Real CDC extracts do this when the source system
         backdates corrections. A MERGE ordered by arrival rather than by
         ``__seq`` will resurrect refunded orders.
      3. Orders written before the May renumbering that reference old seat IDs.

    Returns (orders, cdc_events).
    """
    price_lookup = (
        pricing.groupby(["game_id", "seat_class"])["listed_price_usd"].median().to_dict()
    )
    seat_arr = seats[["seat_id", "seat_class", "face_price_usd", "section"]].to_numpy(dtype=object)
    acct_arr = accounts[["account_id", "cohort"]].to_numpy(dtype=object)

    orders: list[dict] = []
    order_no = 0

    for game in schedule.to_dict(orient="records"):
        first_pitch = datetime.fromisoformat(game["first_pitch_ts"])
        demand = np.clip(
            game["opponent_draw_factor"] * game["promotion_lift"] * game["novelty_index"]
            * (1.16 if game["is_weekend"] else 0.90), 0.30, 1.35,
        )
        n_sold = int(len(seats) * demand * float(rng.uniform(0.86, 1.0)))
        n_sold = min(n_sold, len(seats))

        seat_idx = rng.choice(len(seat_arr), size=n_sold, replace=False)
        acct_idx = rng.integers(0, len(acct_arr), size=n_sold)

        cohorts = acct_arr[acct_idx, 1]
        # Purchase lead time is cohort-driven — tourists buy late, plans buy early.
        lead_mean = np.array([dict((c[0], c[4]) for c in COHORTS)[c] for c in cohorts])
        lead_days = np.clip(rng.exponential(lead_mean), 0.05, 300.0)

        for k in range(n_sold):
            order_no += 1
            si = seat_idx[k]
            seat_class = seat_arr[si, 1]
            purchased = first_pitch - timedelta(days=float(lead_days[k]))
            listed = price_lookup.get((game["game_id"], seat_class), float(seat_arr[si, 2]))

            transferred = rng.random() < 0.11
            resold = (not transferred) and rng.random() < 0.06
            refunded = (not transferred) and (not resold) and rng.random() < 0.023

            orders.append(
                {
                    "order_id": f"ORD-{order_no:09d}",
                    "game_id": game["game_id"],
                    "seat_id": seat_arr[si, 0],
                    "account_id": acct_arr[acct_idx[k], 0],
                    "seat_class": seat_class,
                    "channel": str(rng.choice(
                        ["MOBILE_APP", "WEB", "BOX_OFFICE", "HOTEL_PARTNER", "GROUP_SALES", "SECONDARY"],
                        p=[0.41, 0.27, 0.07, 0.12, 0.08, 0.05])),
                    "face_price_usd": float(seat_arr[si, 2]),
                    "paid_price_usd": round(float(listed) * float(rng.uniform(0.92, 1.06)), 2),
                    "purchased_ts": purchased,
                    "transferred_ts": purchased + timedelta(days=float(rng.uniform(0.5, max(lead_days[k], 1.0)))) if transferred else None,
                    "resold_ts": purchased + timedelta(days=float(rng.uniform(0.5, max(lead_days[k], 1.0)))) if resold else None,
                    "refunded_ts": purchased + timedelta(days=float(rng.uniform(0.5, max(lead_days[k], 1.0)))) if refunded else None,
                    "is_comp": bool(rng.random() < 0.018),
                }
            )

    orders_df = pd.DataFrame(orders)
    cdc = _orders_to_cdc(cfg, rng, orders_df)
    LOGGER.info("ticket orders: %d -> %d cdc events", len(orders_df), len(cdc))
    return orders_df, cdc


def _orders_to_cdc(cfg: GeneratorConfig, rng: np.random.Generator, orders: pd.DataFrame) -> pd.DataFrame:
    events: list[dict] = []
    seq = 0
    d = cfg.defects
    transitions = [
        ("purchased_ts", "SALE", "I"),
        ("transferred_ts", "TRANSFER", "U"),
        ("resold_ts", "RESALE", "U"),
        ("refunded_ts", "REFUND", "D"),
    ]

    for row in orders.to_dict(orient="records"):
        for ts_col, event_type, op in transitions:
            ts = row[ts_col]
            if ts is None or pd.isna(ts):
                continue
            seq += 1
            snap = dict(row)
            snap["event_type"] = event_type
            snap["__op"] = op
            snap["__seq"] = seq

            commit_ts = ts
            # Defect: correction backdated behind the originating SALE. __seq is
            # still correct; __commit_ts lies. Order by __seq or lose money.
            if event_type in ("REFUND", "TRANSFER") and rng.random() < d.cdc_refund_before_order:
                commit_ts = row["purchased_ts"] - timedelta(hours=float(rng.uniform(1, 72)))
            snap["__commit_ts"] = commit_ts

            events.append(snap)
            if rng.random() < d.cdc_duplicate_seq:
                events.append(dict(snap))

    cdc = pd.DataFrame(events)
    for col in ("purchased_ts", "transferred_ts", "resold_ts", "refunded_ts", "__commit_ts"):
        cdc[col] = pd.to_datetime(cdc[col])
    return cdc.sort_values("__commit_ts").reset_index(drop=True)


# --------------------------------------------------------------------------- #
# Gate scans (streaming source)
# --------------------------------------------------------------------------- #


def generate_gate_scans(
    cfg: GeneratorConfig, rng: np.random.Generator, schedule: pd.DataFrame,
    orders: pd.DataFrame, accounts: pd.DataFrame, out_root: Path,
) -> int:
    """Gate scans, written as JSONL partitioned by ARRIVAL date/hour.

    The burst is the point. Gates open 90 minutes out; the bulk of scans land in a
    ~25 minute window roughly 40 minutes before first pitch, then the rate collapses.
    A watermark tuned for the burst is wrong for the tail, which is the design
    tension the streaming tranche has to resolve.

    Handheld gates (CENTER_FIELD, OUTFIELD_A, OUTFIELD_B) lose connectivity and
    bulk-upload later, so lateness is CORRELATED WITH GATE rather than random —
    which breaks the independence assumption most streaming examples rely on.
    """
    d = cfg.defects
    total = 0
    n_games = len(schedule)
    drift_game = int(n_games * cfg.schema_drift_at)

    cohort_noshow = {c[0]: c[2] for c in COHORTS}
    acct_cohort = accounts.set_index("account_id")["cohort"].to_dict()
    gate_names = [g[0] for g in GATES]
    gate_share = np.array([g[1] for g in GATES]); gate_share = gate_share / gate_share.sum()
    handheld = {g[0]: g[2] for g in GATES}

    for gi, game in enumerate(schedule.to_dict(orient="records")):
        first_pitch = datetime.fromisoformat(game["first_pitch_ts"])
        schema_v2 = gi >= drift_game

        game_orders = orders.loc[
            (orders["game_id"] == game["game_id"]) & (orders["refunded_ts"].isna())
        ]
        if game_orders.empty:
            continue

        cohorts = np.array([acct_cohort.get(a, "TOURIST") for a in game_orders["account_id"]])
        noshow_p = np.array([cohort_noshow[c] for c in cohorts])
        attends = rng.random(len(game_orders)) > noshow_p
        attending = game_orders.loc[attends].reset_index(drop=True)
        n = len(attending)
        if n == 0:
            continue

        # Burst-shaped arrival: three-component mixture in minutes from first pitch.
        component = rng.choice([0, 1, 2], size=n, p=[0.60, 0.25, 0.15])
        offsets = np.where(
            component == 0, rng.normal(-32, 9, n),
            np.where(component == 1, rng.normal(-62, 17, n), rng.normal(19, 22, n)),
        )
        offsets = np.clip(offsets, -95, 130)

        gates = rng.choice(gate_names, size=n, p=gate_share)
        is_handheld = np.array([handheld[g] for g in gates])

        # Lateness correlated with handheld gates, not uniform.
        late_p = np.where(is_handheld, d.scan_late_upload * 6.0, d.scan_late_upload * 0.2)
        is_late = rng.random(n) < late_p
        upload_lag_h = np.where(
            is_late, rng.uniform(1.0, d.scan_max_lateness_hours, n), rng.uniform(0.0, 0.05, n)
        )

        malformed = rng.random(n) < d.scan_malformed_json
        by_hour: dict[tuple[str, int], list[str]] = {}

        for i in range(n):
            # Second precision on both: these are serialized to JSON as strings,
            # and a column with mixed ISO precision breaks strict parsing in a way
            # that is maddening to diagnose. Defects here must be deliberate.
            scan_ts = (first_pitch + timedelta(minutes=float(offsets[i]))).replace(microsecond=0)
            arrival_ts = (scan_ts + timedelta(hours=float(upload_lag_h[i]))).replace(microsecond=0)
            key = (arrival_ts.date().isoformat(), arrival_ts.hour)

            if malformed[i]:
                by_hour.setdefault(key, []).append(
                    '{"scan_id": "SCN-%s", "gate": "%s", "scan_ts":' % (f"{gi}-{i}", gates[i])
                )
                total += 1
                continue

            rec: dict[str, Any] = {
                "scan_id": f"SCN-{game['game_id']}-{i:07d}",
                "game_id": game["game_id"],
                "order_id": attending.at[i, "order_id"],
                "seat_id": attending.at[i, "seat_id"],
                "gate": gates[i],
                "scan_ts": scan_ts.replace(tzinfo=timezone.utc).isoformat(),
                "device_id": f"DEV-{abs(hash(gates[i])) % 400:03d}",
                "device_type": "HANDHELD" if is_handheld[i] else "TURNSTILE",
                "scan_result": "ACCEPTED",
                "ticket_medium": str(rng.choice(["MOBILE", "PRINT_AT_HOME", "CARD"], p=[0.83, 0.09, 0.08])),
            }
            if schema_v2:
                rec["scan_device_os"] = str(rng.choice(["iOS 19.2", "Android 16", "ZebraOS 4.1"]))

            # Defect: scan with no valid ticket — counterfeit, double scan, staff badge.
            if rng.random() < d.scan_no_valid_ticket:
                rec["order_id"] = f"ORD-{rng.integers(900000000, 999999999)}"
                rec["scan_result"] = str(rng.choice(["DUPLICATE", "INVALID", "UNKNOWN_CREDENTIAL"]))

            by_hour.setdefault(key, []).append(json.dumps(rec))
            total += 1

        for (dt, hh), lines in by_hour.items():
            path = out_root / "gate_scans" / f"dt={dt}" / f"hh={hh:02d}" / f"{game['game_id']}-part.json"
            existing = path.read_text(encoding="utf-8").splitlines() if path.exists() else []
            _write_jsonl(existing + lines, path)

        if (gi + 1) % 10 == 0:
            LOGGER.info("gate scans: %d/%d games, %d scans", gi + 1, n_games, total)

    LOGGER.info("gate scans: %d records across %d games", total, n_games)
    return total


# --------------------------------------------------------------------------- #
# POS
# --------------------------------------------------------------------------- #


def generate_pos(
    cfg: GeneratorConfig, rng: np.random.Generator, schedule: pd.DataFrame,
    orders: pd.DataFrame, seats: pd.DataFrame, accounts: pd.DataFrame, out_root: Path,
) -> int:
    """Concession and merchandise POS, written as per-terminal batch files.

    Per-cap spend is driven by cohort AND seat class simultaneously — tourists in
    premium seats spend multiples of a local in the upper deck. Modeling spend as
    a flat per-head average, which is what most clubs do, is wrong at both ends.

    Defects: two terminals carry the wrong tax rate for the first three weeks
    (a real first-season commissioning error), some batches are dropped entirely,
    and some are submitted twice.
    """
    d = cfg.defects
    total = 0
    percap = {c[0]: c[3] for c in SEAT_CLASSES}
    cohort_percap = {c[0]: c[3] for c in COHORTS}
    acct_cohort = accounts.set_index("account_id")["cohort"].to_dict()
    seat_class = seats.set_index("seat_id")["seat_class"].to_dict()

    items = [(i[0], i[1], i[2], i[3]) for i in POS_ITEMS]
    attach = np.array([i[4] for i in POS_ITEMS]); attach = attach / attach.sum()

    terminals = [f"POS-{i:03d}" for i in range(1, 41)]
    miscfg_terminals = {terminals[7], terminals[22]}          # wrong tax rate
    miscfg_until = OPENING_DAY + timedelta(days=21)

    for gi, game in enumerate(schedule.to_dict(orient="records")):
        first_pitch = datetime.fromisoformat(game["first_pitch_ts"])
        game_orders = orders.loc[
            (orders["game_id"] == game["game_id"]) & (orders["refunded_ts"].isna())
        ]
        if game_orders.empty:
            continue

        # Approximate attendance from the same no-show logic used for scans.
        cohorts = np.array([acct_cohort.get(a, "TOURIST") for a in game_orders["account_id"]])
        noshow = np.array([dict((c[0], c[2]) for c in COHORTS)[c] for c in cohorts])
        attending_mask = rng.random(len(game_orders)) > noshow
        attending = game_orders.loc[attending_mask]
        if attending.empty:
            continue

        att_cohorts = np.array([acct_cohort.get(a, "TOURIST") for a in attending["account_id"]])
        att_classes = np.array([seat_class.get(s, "OUTFIELD_RESERVED") for s in attending["seat_id"]])
        spend_index = np.array([cohort_percap[c] for c in att_cohorts]) * np.array(
            [percap[c] for c in att_classes]
        )
        n_lines = np.maximum(0, rng.poisson(spend_index / 12.0))
        total_lines = int(n_lines.sum())
        if total_lines == 0:
            continue

        item_idx = rng.choice(len(items), size=total_lines, p=attach)
        term = rng.choice(terminals, size=total_lines)
        minute_offset = rng.normal(35, 55, total_lines).clip(-70, 200)

        batches: dict[str, list[str]] = {}
        for j in range(total_lines):
            sku, name, category, price = items[item_idx[j]]
            txn_ts = (first_pitch + timedelta(minutes=float(minute_offset[j]))).replace(microsecond=0)
            qty = int(rng.choice([1, 1, 1, 2, 2, 3], size=1)[0])

            tax_rate = 0.0838
            if term[j] in miscfg_terminals and txn_ts < miscfg_until:
                tax_rate = 0.0000  # commissioning error: tax not configured

            if rng.random() < d.pos_malformed_json:
                batches.setdefault(term[j], []).append(
                    '{"txn_id": "TXN-%d", "sku": "%s", "qty":' % (j, sku)
                )
                total += 1
                continue

            rec = {
                "txn_line_id": f"TXL-{game['game_id']}-{j:07d}",
                "txn_id": f"TXN-{game['game_id']}-{j // 3:07d}",
                "game_id": game["game_id"],
                "terminal_id": term[j],
                "stand_location": str(rng.choice(["MAIN_CONCOURSE", "UPPER_CONCOURSE", "LEFT_FIELD_PLAZA",
                                                  "SUITE_LEVEL", "TEAM_STORE", "BULLPEN_BAR"])),
                "sku": sku,
                "item_name": name,
                "category": category,
                "unit_price_usd": price,
                "quantity": qty,
                "gross_amount_usd": round(price * qty, 2),
                "tax_rate": tax_rate,
                "tax_amount_usd": round(price * qty * tax_rate, 2),
                "tender_type": str(rng.choice(["CARD", "MOBILE_WALLET", "CASH", "LOADED_TICKET"],
                                              p=[0.52, 0.34, 0.06, 0.08])),
                "txn_ts": txn_ts.replace(tzinfo=timezone.utc).isoformat(),
            }
            batches.setdefault(term[j], []).append(json.dumps(rec))
            total += 1

        for terminal, lines in batches.items():
            # Defect: entire batch dropped (terminal never uploaded).
            if rng.random() < d.pos_batch_dropped:
                total -= len(lines)
                continue
            path = out_root / "pos" / f"dt={game['game_date']}" / f"{terminal}-{game['game_id']}.json"
            _write_jsonl(lines, path)
            # Defect: same batch submitted twice under a different filename.
            if rng.random() < d.pos_batch_double_submitted:
                dup = out_root / "pos" / f"dt={game['game_date']}" / f"{terminal}-{game['game_id']}-resubmit.json"
                _write_jsonl(lines, dup)
                total += len(lines)

        if (gi + 1) % 10 == 0:
            LOGGER.info("pos: %d/%d games, %d lines", gi + 1, len(schedule), total)

    LOGGER.info("pos: %d line items", total)
    return total


# --------------------------------------------------------------------------- #
# Guest services and sponsorship
# --------------------------------------------------------------------------- #


def generate_guest_tickets(
    cfg: GeneratorConfig, rng: np.random.Generator, schedule: pd.DataFrame, accounts: pd.DataFrame
) -> pd.DataFrame:
    """Guest services complaints. Free text correlated with issue category so the
    AI classification tranche has recoverable signal; a fraction carry embedded
    PII so the redaction path has something real to find."""
    n = cfg.scale.n_guest_tickets
    categories = list(GUEST_ISSUE_TEMPLATES)
    cat = rng.choice(categories, size=n)
    games = rng.choice(schedule["game_id"].to_numpy(), size=n)
    accts = rng.choice(accounts["account_id"].to_numpy(), size=n)
    game_date = schedule.set_index("game_id")["game_date"].to_dict()

    bodies = []
    for c in cat:
        body = str(rng.choice(GUEST_ISSUE_TEMPLATES[c]))
        if rng.random() < cfg.defects.guest_note_contains_pii:
            body += (
                f" Please contact {rng.choice(FIRST_NAMES)} {rng.choice(LAST_NAMES)} at "
                f"({rng.integers(200, 990)}) {rng.integers(200, 990)}-{rng.integers(1000, 9999)} "
                f"or {rng.choice(FIRST_NAMES).lower()}@example.com."
            )
        bodies.append(body)

    df = pd.DataFrame(
        {
            "guest_ticket_id": [f"GST-{i:07d}" for i in range(1, n + 1)],
            "account_id": accts,
            "game_id": games,
            "reported_date": [game_date[g] for g in games],
            "issue_category_actual": cat,   # ground truth, withheld from the model
            "issue_text": bodies,
            "channel": rng.choice(["IN_VENUE_KIOSK", "EMAIL", "PHONE", "APP", "SOCIAL"],
                                  size=n, p=[0.21, 0.34, 0.13, 0.24, 0.08]),
            "resolution_status": rng.choice(["OPEN", "RESOLVED", "ESCALATED", "CLOSED_NO_ACTION"],
                                            size=n, p=[0.11, 0.63, 0.09, 0.17]),
            "compensation_usd": np.round(np.where(rng.random(n) < 0.24, rng.uniform(10, 180, n), 0.0), 2),
        }
    )
    LOGGER.info("guest tickets: %d rows", len(df))
    return df


def generate_sponsorships(cfg: GeneratorConfig, rng: np.random.Generator) -> list[dict]:
    """Sponsorship agreements with quantified activation clauses.

    Clauses are written in contract register with spelled-out numerals, because
    the point of the document-intelligence tranche is extracting obligations from
    prose — not parsing a tidy JSON field that was never prose to begin with.
    """
    contracts = []
    for i, sponsor in enumerate(SPONSOR_NAMES, start=1):
        n_clauses = int(rng.integers(3, 7))
        chosen = rng.choice(len(SPONSOR_CLAUSES), size=n_clauses, replace=False)
        clauses = [SPONSOR_CLAUSES[c].format(n=int(rng.choice([2, 3, 4, 6, 8, 10, 12, 20, 25]))) for c in chosen]
        contracts.append(
            {
                "contract_id": f"SPN-{i:04d}",
                "sponsor_name": sponsor,
                "tier": str(rng.choice(["FOUNDING", "CORNERSTONE", "OFFICIAL_PARTNER", "SUPPORTING"],
                                       p=[0.12, 0.22, 0.41, 0.25])),
                "annual_value_usd": int(rng.choice([150_000, 400_000, 900_000, 2_400_000])),
                "term_start": f"{SEASON_YEAR}-01-01",
                "term_end": f"{SEASON_YEAR + int(rng.integers(1, 6))}-12-31",
                "contract_text": (
                    f"SPONSORSHIP AGREEMENT between Las Vegas Kings Baseball Club, LLC "
                    f"(\"Club\") and {sponsor} (\"Sponsor\"). "
                    + " ".join(clauses)
                ),
            }
        )
    LOGGER.info("sponsorships: %d agreements", len(contracts))
    return contracts


# --------------------------------------------------------------------------- #
# Orchestration
# --------------------------------------------------------------------------- #


def write_cdc_batches(cfg: GeneratorConfig, rng: np.random.Generator, cdc: pd.DataFrame, out_root: Path) -> int:
    """Split CDC into daily batch files, with a fraction written OUT OF ORDER.

    File arrival order must not be assumed to match commit order — the single
    most common CDC bug in production, and the reason silver must order by __seq.
    """
    cdc = cdc.copy()
    cdc["batch_date"] = cdc["__commit_ts"].dt.date
    batches = sorted(cdc["batch_date"].unique())

    order = list(range(len(batches)))
    for i in range(1, len(order)):
        if rng.random() < cfg.defects.cdc_out_of_order_batch:
            order[i - 1], order[i] = order[i], order[i - 1]

    written = 0
    for write_idx, batch_idx in enumerate(order):
        b = batches[batch_idx]
        part = cdc.loc[cdc["batch_date"] == b].drop(columns=["batch_date"])
        _write_parquet(part, out_root / "ticketing" / "cdc" / f"batch-{write_idx:05d}_{b.isoformat()}.parquet")
        written += len(part)
    LOGGER.info("cdc batches: %d files, %d rows", len(batches), written)
    return written


def run(cfg: GeneratorConfig) -> dict:
    rng = np.random.default_rng(cfg.seed)
    out = cfg.output_dir
    # Volumes are UC objects, not directories - the volume root already exists.
    LOGGER.info("generating scale=%s seed=%d season=%d", cfg.scale.name, cfg.seed, SEASON_YEAR)

    schedule = generate_schedule(cfg, rng)
    seats = generate_seats(cfg, rng)
    accounts = generate_accounts(cfg, rng)
    deposits = generate_deposits(cfg, rng, accounts)
    pricing = generate_pricing(cfg, rng, schedule)
    orders, cdc = generate_ticket_cdc(cfg, rng, schedule, seats, accounts, pricing)
    guest = generate_guest_tickets(cfg, rng, schedule, accounts)
    sponsors = generate_sponsorships(cfg, rng)

    _write_jsonl([json.dumps(r) for r in schedule.to_dict(orient="records")], out / "schedule" / "schedule.json")
    _write_csv(seats, out / "seat_manifest" / "seats.csv")
    _write_csv(accounts, out / "crm_accounts" / f"accounts_{OPENING_DAY.date().isoformat()}.csv")
    _write_csv(deposits, out / "deposits" / "deposits.csv")
    _write_csv(pricing, out / "pricing" / "price_points.csv")
    _write_parquet(guest, out / "guest_services" / "guest_tickets.parquet")
    _write_jsonl([json.dumps(c) for c in sponsors], out / "sponsorship" / "contracts.json")

    cdc_rows = write_cdc_batches(cfg, rng, cdc, out)
    scan_rows = generate_gate_scans(cfg, rng, schedule, orders, accounts, out)
    pos_rows = generate_pos(cfg, rng, schedule, orders, seats, accounts, out)

    manifest = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "club": "Las Vegas Kings",
        "season": SEASON_YEAR,
        "seed": cfg.seed,
        "scale": asdict(cfg.scale),
        "defect_rates": asdict(cfg.defects),
        "row_counts": {
            "schedule": len(schedule),
            "seat_manifest": len(seats),
            "crm_accounts": len(accounts),
            "deposits": len(deposits),
            "pricing": len(pricing),
            "ticket_orders_logical": len(orders),
            "ticket_cdc_events": cdc_rows,
            "gate_scans": scan_rows,
            "pos_lines": pos_rows,
            "guest_tickets": len(guest),
            "sponsorship_contracts": len(sponsors),
        },
    }
    (out / "_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    LOGGER.info("manifest written: %s", out / "_manifest.json")
    return manifest


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Generate synthetic Las Vegas Kings source data.")
    p.add_argument("--output-dir", type=Path, required=True, help="Landing zone root (local path or UC Volume).")
    p.add_argument("--scale", choices=sorted(SCALE_PROFILES), default="small")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
        stream=sys.stdout,
    )
    cfg = GeneratorConfig(output_dir=args.output_dir, scale=SCALE_PROFILES[args.scale], seed=args.seed)
    manifest = run(cfg)
    LOGGER.info("done: %s", json.dumps(manifest["row_counts"]))
    return 0


if __name__ == "__main__":
    # Deliberately NOT `raise SystemExit(main())`, idiomatic though that is.
    # Databricks runs spark_python_task inside a notebook-like runtime that
    # treats ANY SystemExit as a task failure, including SystemExit(0). A clean
    # run would be reported as INTERNAL_ERROR. Only raise on genuine failure.
    _rc = main()
    if _rc != 0:
        raise SystemExit(_rc)
