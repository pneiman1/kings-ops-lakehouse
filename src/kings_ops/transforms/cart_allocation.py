"""Mobile cart allocation transforms.

Batch pipeline: for each upcoming game, allocate ``n_carts`` mobile concession
carts across six ballpark zones in 30-minute windows from gates-open through
end of game.

Algorithm — baseline_v1
-----------------------
1. Compute actual POS demand: transactions per zone × window per game.
2. Normalise to ``demand_index`` (zone's share of total transactions in that
   window).
3. For each game, compute the *prior* average demand_index using only games
   that pre-date the target game (replay-safe, no lookahead).
4. Allocate ``round(prior_demand_index × n_carts)`` carts per zone per window.
5. Zero out alcohol zones for windows past the 7th-inning cutoff.

See docs/adr/ADR-011-cart-allocation.md for the full decision record including
zone adjacency and portal mapping metadata reserved for enhanced_v2.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Configuration constants
# ---------------------------------------------------------------------------

TOTAL_CARTS: int = 12

#: Minutes from gates-open to first pitch (gates open 90 min pre-game).
GATES_OPEN_OFFSET_MINUTES: int = 90

#: Duration of each allocation window in minutes.
WINDOW_MINUTES: int = 30

#: Minutes after first pitch at which alcohol service stops (top of 7th).
#: Conservative bound covering all game paces; override via pipeline conf.
ALCOHOL_CUTOFF_MINUTES: int = 165

#: First window number (from gates-open) where alcohol service is prohibited.
#: = floor((GATES_OPEN_OFFSET + ALCOHOL_CUTOFF) / WINDOW_MINUTES)
_ALCOHOL_CUTOFF_WINDOW: int = (
    GATES_OPEN_OFFSET_MINUTES + ALCOHOL_CUTOFF_MINUTES
) // WINDOW_MINUTES

ALGORITHM_VERSION: str = "baseline_v1"

# ---------------------------------------------------------------------------
# Zone metadata (static; layout from ADR-011)
# ---------------------------------------------------------------------------

#: All POS stand_location values in scope.
ZONES: tuple[str, ...] = (
    "MAIN_CONCOURSE",
    "UPPER_CONCOURSE",
    "LEFT_FIELD_PLAZA",
    "SUITE_LEVEL",
    "TEAM_STORE",
    "BULLPEN_BAR",
)

#: Zones where alcohol is served. TEAM_STORE is merchandise-only.
ALCOHOL_ZONES: frozenset[str] = frozenset(ZONES) - {"TEAM_STORE"}

#: Walkable adjacency between zones (undirected). Reserved for enhanced_v2.
ZONE_ADJACENCY: dict[str, list[str]] = {
    "MAIN_CONCOURSE": ["LEFT_FIELD_PLAZA", "BULLPEN_BAR"],
    "LEFT_FIELD_PLAZA": ["MAIN_CONCOURSE"],
    "BULLPEN_BAR": ["MAIN_CONCOURSE"],
    "UPPER_CONCOURSE": ["SUITE_LEVEL"],
    "SUITE_LEVEL": ["UPPER_CONCOURSE"],
    "TEAM_STORE": [],
}

#: Which zones each physical gate feeds into (ADR-011 D7). Reserved for
#: portal-adjusted model in enhanced_v2.
GATE_PORTAL_MAP: dict[str, str] = {
    "GATE_A": "MAIN_CONCOURSE",
    "GATE_B": "MAIN_CONCOURSE",
    "GATE_C": "LEFT_FIELD_PLAZA",
    "GATE_D": "BULLPEN_BAR",
}

# ---------------------------------------------------------------------------
# Public transforms
# ---------------------------------------------------------------------------


def compute_demand(
    pos: DataFrame,
    dim_game: DataFrame,
) -> DataFrame:
    """Transaction count by game × zone × 30-min window from gates-open.

    Joins POS lines to ``dim_game`` on ``game_id`` to obtain
    ``first_pitch_ts``, then bins each transaction into the window that
    contains its ``txn_ts``.  Only windows ≥ 0 (after gates open) are kept.

    Args:
        pos:       Silver POS basket table (or bronze POS lines).  Must have
                   columns ``game_id``, ``stand_location``, ``txn_ts``.
        dim_game:  Gold game dimension.  Must have ``game_id``,
                   ``game_date``, ``first_pitch_ts``.

    Returns:
        DataFrame with columns:
        ``game_id | game_date | zone | window_num | txn_count``
    """
    game_attrs = dim_game.select(
        F.col("game_id").alias("_g_game_id"),
        F.col("game_date"),
        F.col("first_pitch_ts"),
    )

    joined = pos.join(
        game_attrs,
        pos["game_id"] == F.col("_g_game_id"),
        "inner",
    ).drop("_g_game_id")

    # Minutes elapsed since gates opened.
    gates_open_ts = F.col("first_pitch_ts") - F.expr(
        f"INTERVAL {GATES_OPEN_OFFSET_MINUTES} MINUTES"
    )
    minutes_from_open = (
        F.col("txn_ts").cast("long") - gates_open_ts.cast("long")
    ) / 60.0
    # floor() is required: cast("int") truncates toward zero, so a transaction
    # at -10 min gives window (-10/30)=−0.33 → int → 0 (passes the >= 0 filter
    # incorrectly).  floor(-0.33) = -1, which is correctly excluded.
    window_num = F.floor(minutes_from_open / WINDOW_MINUTES).cast("int")

    return (
        joined
        .withColumn("window_num", window_num)
        .filter(F.col("window_num") >= 0)
        .groupBy("game_id", "game_date", "stand_location", "window_num")
        .agg(F.count(F.lit(1)).alias("txn_count"))
        .withColumnRenamed("stand_location", "zone")
    )


def add_demand_index(demand: DataFrame) -> DataFrame:
    """Normalise ``txn_count`` to a ``demand_index`` ∈ [0, 1].

    ``demand_index`` is a zone's share of total transactions across all zones
    in a given (game_id, window_num).  Values sum to 1.0 per window.

    Used as the input signal for proportional cart allocation.
    """
    total_w = Window.partitionBy("game_id", "window_num")
    return demand.withColumn(
        "demand_index",
        F.col("txn_count") / F.sum("txn_count").over(total_w),
    )


def replay_prior_demand(demand_with_index: DataFrame) -> DataFrame:
    """Rolling prior-game average demand_index — no lookahead.

    For each (zone, window_num, game), computes the mean ``demand_index``
    from all *earlier* games (ordered by ``game_date``).  The window is
    ``ROWS BETWEEN unboundedPreceding AND -1``, so the current game's own
    demand is excluded.

    The first game in the season sees no prior rows; ``prior_demand_index``
    is NULL for it.  The caller must coalesce to a uniform fallback (1 / n_zones).

    Args:
        demand_with_index:  Output of :func:`add_demand_index`.

    Returns:
        DataFrame with columns:
        ``game_id | game_date | window_num | zone | prior_demand_index``
    """
    prior_w = (
        Window
        .partitionBy("zone", "window_num")
        .orderBy("game_date")
        .rowsBetween(Window.unboundedPreceding, -1)
    )
    return (
        demand_with_index
        .withColumn("prior_demand_index", F.avg("demand_index").over(prior_w))
        .select("game_id", "game_date", "window_num", "zone", "prior_demand_index")
    )


def allocate_carts(
    prior_demand: DataFrame,
    n_carts: int = TOTAL_CARTS,
    alcohol_cutoff_window: int = _ALCOHOL_CUTOFF_WINDOW,
) -> DataFrame:
    """Proportionally allocate carts from prior demand, enforcing alcohol cutoff.

    Steps:
    1. Coalesce NULL ``prior_demand_index`` to uniform fallback (first game).
    2. ``raw_carts = round(demand_index_eff × n_carts)`` as int.
    3. Zero out ``raw_carts`` for alcohol zones in post-cutoff windows.
    4. Tag with algorithm version.

    Note on rounding: total allocated carts per window may differ from
    ``n_carts`` by at most ``len(ZONES)`` due to integer rounding.
    A remainder redistribution step is deferred to enhanced_v2 (ADR-011 D3).

    Args:
        prior_demand:         Output of :func:`replay_prior_demand`.
        n_carts:              Total carts in the mobile fleet.
        alcohol_cutoff_window: First window number where alcohol is prohibited.

    Returns:
        DataFrame with columns:
        ``game_id | game_date | window_num | zone | prior_demand_index``
        ``| demand_index_eff | recommended_carts | is_alcohol_allowed``
        ``| algorithm_version``
    """
    uniform = F.lit(1.0 / len(ZONES))
    with_eff = prior_demand.withColumn(
        "demand_index_eff",
        F.coalesce(F.col("prior_demand_index"), uniform),
    )

    is_alcohol_zone = F.col("zone").isin(list(ALCOHOL_ZONES))
    is_post_cutoff = F.col("window_num") >= F.lit(alcohol_cutoff_window)
    is_alcohol_allowed = ~(is_alcohol_zone & is_post_cutoff)

    raw_carts = F.round(F.col("demand_index_eff") * F.lit(n_carts)).cast("int")

    return (
        with_eff
        .withColumn("is_alcohol_allowed", is_alcohol_allowed)
        .withColumn(
            "recommended_carts",
            F.when(~is_alcohol_allowed, F.lit(0)).otherwise(raw_carts),
        )
        .withColumn("algorithm_version", F.lit(ALGORITHM_VERSION))
    )


def build_reco_cart_allocation(
    pos: DataFrame,
    dim_game: DataFrame,
    n_carts: int = TOTAL_CARTS,
    alcohol_cutoff_minutes: int = ALCOHOL_CUTOFF_MINUTES,
) -> DataFrame:
    """Full replay-safe cart allocation for all games in dim_game.

    Orchestrates :func:`compute_demand`, :func:`add_demand_index`,
    :func:`replay_prior_demand`, and :func:`allocate_carts` into a single
    end-to-end transform.

    For each game, carts are allocated using only demand from prior games —
    no lookahead.  Game 1 falls back to uniform allocation across all zones.

    Args:
        pos:                   Silver POS table.
        dim_game:              Gold game dimension (all 81 games).
        n_carts:               Mobile cart fleet size (default 12).
        alcohol_cutoff_minutes: Minutes after first pitch to stop alcohol
                                service (default 165, top of 7th).

    Returns:
        DataFrame suitable for writing to ``gold.reco_cart_allocation``.
        Columns: ``game_id | game_date | window_num | zone``
                 ``| prior_demand_index | demand_index_eff``
                 ``| recommended_carts | is_alcohol_allowed | algorithm_version``
    """
    cutoff_window = (GATES_OPEN_OFFSET_MINUTES + alcohol_cutoff_minutes) // WINDOW_MINUTES

    demand = compute_demand(pos, dim_game)
    with_index = add_demand_index(demand)
    prior = replay_prior_demand(with_index)
    return allocate_carts(prior, n_carts=n_carts, alcohol_cutoff_window=cutoff_window)
