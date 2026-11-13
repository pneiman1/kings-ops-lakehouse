"""Unit tests for the cart allocation transforms.

Exercises: window binning, demand normalisation, replay-safe prior averaging
(no lookahead), proportional allocation, and alcohol-cutoff enforcement.

All DataFrames are constructed in-process; no cluster reads or writes.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import functions as F  # noqa: E402

from kings_ops.transforms.cart_allocation import (  # noqa: E402
    ALCOHOL_CUTOFF_MINUTES,
    ALCOHOL_ZONES,
    GATES_OPEN_OFFSET_MINUTES,
    TOTAL_CARTS,
    WINDOW_MINUTES,
    ZONES,
    _ALCOHOL_CUTOFF_WINDOW,
    add_demand_index,
    allocate_carts,
    build_reco_cart_allocation,
    compute_demand,
    replay_prior_demand,
)

pytestmark = pytest.mark.spark

# ---------------------------------------------------------------------------
# Shared schema strings
# ---------------------------------------------------------------------------

_POS_SCHEMA = "game_id string, stand_location string, txn_ts timestamp"
_GAME_SCHEMA = "game_id string, game_date date, first_pitch_ts timestamp"
_DEMAND_SCHEMA = "game_id string, game_date date, zone string, window_num int, txn_count long"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ts(dt: datetime):
    """Ensure datetime is UTC-aware for Spark TIMESTAMP columns."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def _game_first_pitch(game_id: str, game_date_str: str, hour: int = 19, minute: int = 0):
    """Return a (game_id, game_date, first_pitch_ts) tuple."""
    from datetime import date
    gd = date.fromisoformat(game_date_str)
    fp = datetime(gd.year, gd.month, gd.day, hour, minute, 0, tzinfo=timezone.utc)
    return (game_id, gd, fp)


def _minutes_from_open(minutes_after_first_pitch: int) -> datetime:
    """Return a txn_ts for a game whose first pitch is at 2027-04-01 19:00 UTC."""
    base_first_pitch = datetime(2027, 4, 1, 19, 0, 0, tzinfo=timezone.utc)
    gates_open = base_first_pitch.replace(
        hour=base_first_pitch.hour - GATES_OPEN_OFFSET_MINUTES // 60,
        minute=base_first_pitch.minute - GATES_OPEN_OFFSET_MINUTES % 60,
    )
    # Simpler arithmetic:
    total_seconds = (
        GATES_OPEN_OFFSET_MINUTES * 60  # offset to gates-open from midnight ref
        + minutes_after_first_pitch * 60
    )
    # txn_ts = gates_open + minutes_after_first_pitch minutes
    import datetime as dt_mod
    return base_first_pitch + dt_mod.timedelta(minutes=minutes_after_first_pitch)


# ---------------------------------------------------------------------------
# 1. Window assignment: compute_demand
# ---------------------------------------------------------------------------

def test_compute_demand_window_binning(spark):
    """Transactions are assigned to 30-min windows relative to gates-open.

    Gates open 90 min before first pitch.  First pitch = 2027-04-01 19:00 UTC.
    Gates open = 17:30 UTC.

    Transaction times and expected windows (from gates-open):
      17:30 UTC = minute 0 → window 0
      17:59 UTC = minute 29 → window 0
      18:00 UTC = minute 30 → window 1
      19:00 UTC = minute 90 → window 3   (first pitch)
      17:29 UTC = minute -1 → excluded (before gates open)
    """
    first_pitch = datetime(2027, 4, 1, 19, 0, 0, tzinfo=timezone.utc)
    import datetime as dt_mod
    gates_open = first_pitch - dt_mod.timedelta(minutes=GATES_OPEN_OFFSET_MINUTES)

    pos_rows = [
        ("G1", "MC_HOMEPLATE", gates_open),                        # min 0 → w0
        ("G1", "MC_HOMEPLATE", gates_open + dt_mod.timedelta(minutes=29)),  # min 29 → w0
        ("G1", "UC_INFIELD",   gates_open + dt_mod.timedelta(minutes=30)),  # min 30 → w1
        ("G1", "MC_LEFTFIELD", gates_open + dt_mod.timedelta(minutes=90)),  # min 90 → w3
        ("G1", "BULLPEN_BAR",  gates_open - dt_mod.timedelta(seconds=1)),   # before gates → excluded
    ]
    pos = spark.createDataFrame(pos_rows, _POS_SCHEMA)

    game_rows = [_game_first_pitch("G1", "2027-04-01")]
    dim_game = spark.createDataFrame(game_rows, _GAME_SCHEMA)

    demand = compute_demand(pos, dim_game)
    result = {
        (r["zone"], r["window_num"]): r["txn_count"]
        for r in demand.collect()
    }

    assert result[("MC_HOMEPLATE", 0)] == 2
    assert result[("UC_INFIELD", 1)] == 1
    assert result[("MC_LEFTFIELD", 3)] == 1
    # Pre-gates transaction must not appear.
    assert all(wn >= 0 for _, wn in result.keys())


def test_compute_demand_excludes_pre_gates_transactions(spark):
    """Transactions timestamped before gates-open are excluded entirely."""
    import datetime as dt_mod
    first_pitch = datetime(2027, 4, 1, 19, 0, 0, tzinfo=timezone.utc)
    gates_open = first_pitch - dt_mod.timedelta(minutes=GATES_OPEN_OFFSET_MINUTES)

    pos_rows = [
        ("G1", "MC_HOMEPLATE", gates_open - dt_mod.timedelta(minutes=10)),
        ("G1", "MC_HOMEPLATE", gates_open - dt_mod.timedelta(minutes=1)),
    ]
    pos = spark.createDataFrame(pos_rows, _POS_SCHEMA)
    dim_game = spark.createDataFrame([_game_first_pitch("G1", "2027-04-01")], _GAME_SCHEMA)

    demand = compute_demand(pos, dim_game)
    assert demand.count() == 0


# ---------------------------------------------------------------------------
# 2. Demand index: add_demand_index
# ---------------------------------------------------------------------------

def test_demand_index_sums_to_one_per_window(spark):
    """demand_index sums to 1.0 across zones within each (game_id, window_num)."""
    from datetime import date
    demand_rows = [
        ("G1", date(2027, 4, 1), "MC_HOMEPLATE", 0, 3),
        ("G1", date(2027, 4, 1), "UC_INFIELD",   0, 1),
        ("G1", date(2027, 4, 1), "MC_HOMEPLATE", 1, 2),
        ("G1", date(2027, 4, 1), "MC_LEFTFIELD", 1, 2),
    ]
    demand = spark.createDataFrame(demand_rows, _DEMAND_SCHEMA)
    with_index = add_demand_index(demand)

    totals = (
        with_index.groupBy("game_id", "window_num")
        .agg(F.sum("demand_index").alias("total"))
        .collect()
    )
    for row in totals:
        assert abs(row["total"] - 1.0) < 1e-9, (
            f"demand_index does not sum to 1.0 for window {row['window_num']}: {row['total']}"
        )


def test_demand_index_proportions(spark):
    """Zone with 3/4 of transactions gets demand_index = 0.75."""
    from datetime import date
    demand_rows = [
        ("G1", date(2027, 4, 1), "MC_HOMEPLATE", 0, 3),
        ("G1", date(2027, 4, 1), "UC_INFIELD",   0, 1),
    ]
    demand = spark.createDataFrame(demand_rows, _DEMAND_SCHEMA)
    with_index = add_demand_index(demand)
    result = {r["zone"]: r["demand_index"] for r in with_index.collect()}

    assert abs(result["MC_HOMEPLATE"] - 0.75) < 1e-9
    assert abs(result["UC_INFIELD"] - 0.25) < 1e-9


# ---------------------------------------------------------------------------
# 3. Replay: prior demand, no lookahead
# ---------------------------------------------------------------------------

def test_replay_first_game_has_null_prior(spark):
    """The first game in the season has no prior data: prior_demand_index is NULL."""
    from datetime import date
    demand_rows = [
        ("G1", date(2027, 4, 1), "MC_HOMEPLATE", 0, 4),
        ("G1", date(2027, 4, 1), "UC_INFIELD",   0, 1),
    ]
    demand = spark.createDataFrame(demand_rows, _DEMAND_SCHEMA)
    with_index = add_demand_index(demand)
    prior = replay_prior_demand(with_index)

    rows = {r["zone"]: r for r in prior.collect()}
    assert rows["MC_HOMEPLATE"]["prior_demand_index"] is None
    assert rows["UC_INFIELD"]["prior_demand_index"] is None


def test_replay_second_game_uses_only_first(spark):
    """Game 2's prior is game 1's demand_index — not game 2's own data."""
    from datetime import date
    demand_rows = [
        # Game 1: MC_HOMEPLATE=3, UC_INFIELD=1 -> demand_index 0.75, 0.25
        ("G1", date(2027, 4, 1), "MC_HOMEPLATE", 0, 3),
        ("G1", date(2027, 4, 1), "UC_INFIELD",   0, 1),
        # Game 2: very different demand
        ("G2", date(2027, 4, 2), "MC_HOMEPLATE", 0, 1),
        ("G2", date(2027, 4, 2), "UC_INFIELD",   0, 9),
    ]
    demand = spark.createDataFrame(demand_rows, _DEMAND_SCHEMA)
    with_index = add_demand_index(demand)
    prior = replay_prior_demand(with_index)

    g2 = {
        r["zone"]: r["prior_demand_index"]
        for r in prior.filter("game_id = 'G2'").collect()
    }
    # Game 2's prior must reflect game 1's distribution, not game 2's.
    assert abs(g2["MC_HOMEPLATE"] - 0.75) < 1e-9
    assert abs(g2["UC_INFIELD"] - 0.25) < 1e-9


def test_replay_third_game_averages_prior_two(spark):
    """Game 3's prior is the average of game 1 and game 2 demand_index values."""
    from datetime import date
    demand_rows = [
        ("G1", date(2027, 4, 1), "MC_HOMEPLATE", 0, 3),  # index = 0.75
        ("G1", date(2027, 4, 1), "UC_INFIELD",   0, 1),  # index = 0.25
        ("G2", date(2027, 4, 2), "MC_HOMEPLATE", 0, 1),  # index = 0.5
        ("G2", date(2027, 4, 2), "UC_INFIELD",   0, 1),  # index = 0.5
        ("G3", date(2027, 4, 3), "MC_HOMEPLATE", 0, 2),  # own data irrelevant
        ("G3", date(2027, 4, 3), "UC_INFIELD",   0, 8),
    ]
    demand = spark.createDataFrame(demand_rows, _DEMAND_SCHEMA)
    with_index = add_demand_index(demand)
    prior = replay_prior_demand(with_index)

    g3 = {
        r["zone"]: r["prior_demand_index"]
        for r in prior.filter("game_id = 'G3'").collect()
    }
    # avg(0.75, 0.5) = 0.625
    assert abs(g3["MC_HOMEPLATE"] - 0.625) < 1e-9
    # avg(0.25, 0.5) = 0.375
    assert abs(g3["UC_INFIELD"] - 0.375) < 1e-9


# ---------------------------------------------------------------------------
# 4. Alcohol cutoff enforcement: allocate_carts
# ---------------------------------------------------------------------------

def test_alcohol_cutoff_window_constant():
    """Cutoff window is derived correctly from named constants."""
    expected = (GATES_OPEN_OFFSET_MINUTES + ALCOHOL_CUTOFF_MINUTES) // WINDOW_MINUTES
    assert _ALCOHOL_CUTOFF_WINDOW == expected


def test_alcohol_zones_get_zero_carts_post_cutoff(spark):
    """Alcohol zones receive 0 recommended_carts in windows >= cutoff window."""
    from datetime import date

    cutoff_w = _ALCOHOL_CUTOFF_WINDOW
    demand_rows = [
        # One window before cutoff, one at cutoff.
        # Order: (game_id, game_date, window_num, zone, prior_demand_index)
        ("G2", date(2027, 4, 2), cutoff_w - 1, "MC_HOMEPLATE", 0.5),
        ("G2", date(2027, 4, 2), cutoff_w - 1, "UC_INFIELD",   0.5),
        ("G2", date(2027, 4, 2), cutoff_w,     "MC_HOMEPLATE", 0.5),
        ("G2", date(2027, 4, 2), cutoff_w,     "UC_INFIELD",   0.5),
    ]
    prior_schema = (
        "game_id string, game_date date, window_num int, zone string, prior_demand_index double"
    )
    prior = spark.createDataFrame(demand_rows, prior_schema)
    result = allocate_carts(prior, n_carts=TOTAL_CARTS)

    rows = {
        (r["zone"], r["window_num"]): r.asDict()
        for r in result.collect()
    }

    # Before cutoff: alcohol zone gets carts.
    assert rows[("MC_HOMEPLATE", cutoff_w - 1)]["recommended_carts"] > 0
    assert rows[("MC_HOMEPLATE", cutoff_w - 1)]["is_alcohol_allowed"] is True

    # At cutoff: alcohol zone gets 0.
    assert rows[("MC_HOMEPLATE", cutoff_w)]["recommended_carts"] == 0
    assert rows[("MC_HOMEPLATE", cutoff_w)]["is_alcohol_allowed"] is False
    assert rows[("UC_INFIELD", cutoff_w)]["recommended_carts"] == 0


def test_team_store_unaffected_by_alcohol_cutoff(spark):
    """TEAM_STORE is not an alcohol zone and continues to receive carts post-cutoff."""
    from datetime import date

    cutoff_w = _ALCOHOL_CUTOFF_WINDOW
    prior_schema = (
        "game_id string, game_date date, window_num int, zone string, prior_demand_index double"
    )
    prior_rows = [
        ("G2", date(2027, 4, 2), cutoff_w, "TEAM_STORE",   0.5),
        ("G2", date(2027, 4, 2), cutoff_w, "MC_HOMEPLATE", 0.5),
    ]
    prior = spark.createDataFrame(prior_rows, prior_schema)
    result = allocate_carts(prior, n_carts=TOTAL_CARTS)

    rows = {r["zone"]: r.asDict() for r in result.collect()}
    # TEAM_STORE is not in ALCOHOL_ZONES, so is_alcohol_allowed stays True
    # and it gets its proportional carts.
    assert rows["TEAM_STORE"]["is_alcohol_allowed"] is True
    assert rows["TEAM_STORE"]["recommended_carts"] > 0
    assert "TEAM_STORE" not in ALCOHOL_ZONES


# ---------------------------------------------------------------------------
# 5. First-game uniform fallback: allocate_carts with NULL prior
# ---------------------------------------------------------------------------

def test_first_game_uniform_fallback(spark):
    """When prior_demand_index is NULL (first game), allocation uses 1/n_zones.

    With 11 zones: uniform = 1/11.  round(12/11) = round(1.09) = 1 cart per zone.
    Total = 11, one below fleet size — the rounding slack documented in ADR-011 D9.
    """
    from datetime import date

    prior_schema = (
        "game_id string, game_date date, window_num int, zone string, prior_demand_index double"
    )
    # All 11 zones with NULL prior (first game of the season).
    prior_rows = [("G1", date(2027, 4, 1), 0, z, None) for z in ZONES]
    prior = spark.createDataFrame(prior_rows, prior_schema)
    result = allocate_carts(prior, n_carts=12)

    rows = {r["zone"]: r["recommended_carts"] for r in result.collect()}
    # round(1/11 * 12) = round(1.0909) = 1 cart per zone.
    expected = round(1.0 / len(ZONES) * 12)
    for zone, carts in rows.items():
        assert carts == expected, (
            f"Expected {expected} carts for {zone} in first game, got {carts}"
        )


# ---------------------------------------------------------------------------
# 6. Algorithm version tag
# ---------------------------------------------------------------------------

def test_algorithm_version_tagged(spark):
    """Every output row carries the algorithm_version tag for lineage."""
    from datetime import date

    prior_schema = (
        "game_id string, game_date date, window_num int, zone string, prior_demand_index double"
    )
    prior_rows = [("G2", date(2027, 4, 2), 0, "MAIN_CONCOURSE", 0.5)]
    prior = spark.createDataFrame(prior_rows, prior_schema)
    result = allocate_carts(prior, n_carts=TOTAL_CARTS)

    for row in result.collect():
        assert row["algorithm_version"] == "baseline_v1"


# ---------------------------------------------------------------------------
# 7. End-to-end: build_reco_cart_allocation
# ---------------------------------------------------------------------------

def test_build_reco_e2e_first_game_uniform(spark):
    """End-to-end: single game produces uniform allocation (no prior data).

    With 11 zones and n_carts=12: round(12/11) = 1 cart per zone.
    """
    import datetime as dt_mod
    first_pitch = datetime(2027, 4, 1, 19, 0, 0, tzinfo=timezone.utc)
    gates_open = first_pitch - dt_mod.timedelta(minutes=GATES_OPEN_OFFSET_MINUTES)

    # Give every zone 1 transaction in window 0.
    pos_rows = [
        ("G1", z, gates_open + dt_mod.timedelta(minutes=5))
        for z in ZONES
    ]
    pos = spark.createDataFrame(pos_rows, _POS_SCHEMA)

    from datetime import date
    dim_game = spark.createDataFrame(
        [("G1", date(2027, 4, 1), first_pitch)],
        _GAME_SCHEMA,
    )

    result = build_reco_cart_allocation(pos, dim_game, n_carts=12)
    rows = {r["zone"]: r.asDict() for r in result.filter("window_num = 0").collect()}

    expected = round(1.0 / len(ZONES) * 12)  # round(12/11) = 1
    for z in ZONES:
        assert z in rows, f"Zone {z} missing from result"
        assert rows[z]["recommended_carts"] == expected


def test_build_reco_e2e_second_game_uses_prior(spark):
    """End-to-end: second game's allocation reflects first game's skewed demand."""
    import datetime as dt_mod
    from datetime import date

    fp_g1 = datetime(2027, 4, 1, 19, 0, 0, tzinfo=timezone.utc)
    fp_g2 = datetime(2027, 4, 2, 19, 0, 0, tzinfo=timezone.utc)
    gates_open_g1 = fp_g1 - dt_mod.timedelta(minutes=GATES_OPEN_OFFSET_MINUTES)
    gates_open_g2 = fp_g2 - dt_mod.timedelta(minutes=GATES_OPEN_OFFSET_MINUTES)
    offset = dt_mod.timedelta(minutes=5)

    other_zones = [z for z in ZONES if z != "MC_HOMEPLATE"]

    # Game 1: MC_HOMEPLATE gets 10 transactions; all other zones get 1 each.
    pos_rows = (
        [("G1", "MC_HOMEPLATE", gates_open_g1 + offset)] * 10
        + [("G1", z, gates_open_g1 + offset) for z in other_zones]
    )
    # Game 2: also has transactions (they must NOT affect its own allocation).
    pos_rows += [("G2", "MC_HOMEPLATE", gates_open_g2 + offset)]
    pos_rows += [("G2", z, gates_open_g2 + offset) for z in other_zones]

    pos = spark.createDataFrame(pos_rows, _POS_SCHEMA)
    dim_game = spark.createDataFrame(
        [
            ("G1", date(2027, 4, 1), fp_g1),
            ("G2", date(2027, 4, 2), fp_g2),
        ],
        _GAME_SCHEMA,
    )

    result = build_reco_cart_allocation(pos, dim_game, n_carts=12)
    g2_w0 = {
        r["zone"]: r["recommended_carts"]
        for r in result.filter("game_id = 'G2' AND window_num = 0").collect()
    }

    # Game 1: MC_HOMEPLATE=10, others=1 each -> total=10+10=20
    # MC_HOMEPLATE demand_index = 10/20 = 0.5
    # Game 2 prior = game 1 demand_index -> MC_HOMEPLATE gets round(0.5*12)=6 carts.
    assert g2_w0["MC_HOMEPLATE"] >= 5, (
        f"Expected MC_HOMEPLATE to get majority of carts, got {g2_w0['MC_HOMEPLATE']}"
    )
    total = sum(g2_w0.values())
    assert total <= 12 + len(g2_w0), "Total carts should not exceed n_carts + rounding slack"
