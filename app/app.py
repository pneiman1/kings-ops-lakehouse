"""Ticket pricing console — T9 Databricks App.

Displays dynamic-pricing effectiveness and per-game revenue breakdown
from gold.fact_ticket_order, gold.fact_game_pnl, and
gold.mart_revenue_by_seat_class.

Env vars (injected by the bundle at deploy time):
  DATABRICKS_HOST          workspace URL  (e.g. https://adb-xxx.azuredatabricks.net)
  DATABRICKS_TOKEN         bearer token (service-principal or PAT)
  DATABRICKS_WAREHOUSE_ID  SQL warehouse ID
  KINGS_CATALOG            Unity Catalog catalog name (default: kings_dev)
  KINGS_GOLD_SCHEMA        gold schema name (default: gold)

When those vars are absent the app renders synthetic demo data so it can be
reviewed visually without a live workspace (useful during the B3 compute
outage — see docs/BLOCKERS.md).

See docs/adr/ADR-013-databricks-app.md.
"""

from __future__ import annotations

import os

import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

_HOST = os.getenv("DATABRICKS_HOST", "")
_TOKEN = os.getenv("DATABRICKS_TOKEN", "")
_WAREHOUSE = os.getenv("DATABRICKS_WAREHOUSE_ID", "")
CATALOG = os.getenv("KINGS_CATALOG", "kings_dev")
GOLD = os.getenv("KINGS_GOLD_SCHEMA", "gold")

_LIVE = bool(_HOST and _TOKEN and _WAREHOUSE)

_SEAT_CLASSES = [
    "FIELD_PREMIUM",
    "INFIELD_BOX",
    "BASELINE_BOX",
    "OUTFIELD_RESERVED",
    "UPPER_RESERVED",
    "SUITE",
    "STANDING_ROOM",
]
_N_GAMES = 81


# ---------------------------------------------------------------------------
# Data access — live path
# ---------------------------------------------------------------------------


@st.cache_data(ttl=300)
def _query(sql: str) -> pd.DataFrame:
    """Execute SQL against the Databricks SQL Warehouse and return a DataFrame."""
    from databricks import sql as dbsql  # noqa: PLC0415

    hostname = _HOST.removeprefix("https://")
    with dbsql.connect(
        server_hostname=hostname,
        http_path=f"/sql/1.0/warehouses/{_WAREHOUSE}",
        access_token=_TOKEN,
    ) as conn, conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchall_arrow().to_pandas()


def load_price_realization() -> pd.DataFrame:
    if not _LIVE:
        return _demo_price_realization()
    return _query(f"""
        SELECT
            seat_class,
            ROUND(AVG(face_price_usd), 2)                                        AS avg_face_usd,
            ROUND(AVG(paid_price_usd), 2)                                        AS avg_paid_usd,
            COUNT(*)                                                              AS orders,
            ROUND(AVG(paid_price_usd) / NULLIF(AVG(face_price_usd), 0) - 1, 4)  AS price_lift
        FROM {CATALOG}.{GOLD}.fact_ticket_order
        WHERE NOT is_refunded
        GROUP BY seat_class
        ORDER BY avg_face_usd DESC
    """)


def load_revenue_by_game() -> pd.DataFrame:
    if not _LIVE:
        return _demo_revenue_by_game()
    return _query(f"""
        SELECT
            game_id,
            tickets_sold,
            ROUND(collected_revenue_usd, 2)                     AS collected_revenue_usd,
            ROUND(refunded_usd, 2)                              AS refunded_usd,
            ROUND(collected_revenue_usd
                  / NULLIF(total_face_usd, 0) - 1, 4)          AS revenue_lift
        FROM {CATALOG}.{GOLD}.fact_game_pnl
        ORDER BY game_id
    """)


def load_seat_class_mix() -> pd.DataFrame:
    if not _LIVE:
        return _demo_seat_class_mix()
    return _query(f"""
        SELECT
            game_id,
            seat_class,
            ROUND(collected_revenue_usd, 2) AS collected_revenue_usd,
            tickets_sold
        FROM {CATALOG}.{GOLD}.mart_revenue_by_seat_class
        ORDER BY game_id, seat_class
    """)


# ---------------------------------------------------------------------------
# Demo data — synthetic, matches gold schema; displayed when env vars absent
# ---------------------------------------------------------------------------

_FACE_PRICES: dict[str, float] = {
    "FIELD_PREMIUM": 285.0,
    "INFIELD_BOX": 175.0,
    "BASELINE_BOX": 115.0,
    "OUTFIELD_RESERVED": 65.0,
    "UPPER_RESERVED": 38.0,
    "SUITE": 480.0,
    "STANDING_ROOM": 22.0,
}
_CAPACITIES: dict[str, int] = {
    "FIELD_PREMIUM": 500,
    "INFIELD_BOX": 2_200,
    "BASELINE_BOX": 4_800,
    "OUTFIELD_RESERVED": 6_500,
    "UPPER_RESERVED": 8_000,
    "SUITE": 400,
    "STANDING_ROOM": 600,
}


def _demo_price_realization() -> pd.DataFrame:
    rng = np.random.default_rng(42)
    rows = []
    for cls, face in _FACE_PRICES.items():
        lift = float(rng.uniform(-0.08, 0.14))
        rows.append({
            "seat_class": cls,
            "avg_face_usd": face,
            "avg_paid_usd": round(face * (1 + lift), 2),
            "orders": int(rng.integers(80, 800)),
            "price_lift": round(lift, 4),
        })
    return pd.DataFrame(rows)


def _demo_revenue_by_game() -> pd.DataFrame:
    rng = np.random.default_rng(7)
    rows = []
    for g in range(1, _N_GAMES + 1):
        base_rev = float(rng.uniform(180_000, 320_000))
        rows.append({
            "game_id": f"G2027{g:03d}",
            "tickets_sold": int(rng.integers(14_000, 28_000)),
            "collected_revenue_usd": round(base_rev, 2),
            "refunded_usd": round(base_rev * float(rng.uniform(0.01, 0.04)), 2),
            "revenue_lift": round(float(rng.uniform(-0.03, 0.09)), 4),
        })
    return pd.DataFrame(rows)


def _demo_seat_class_mix() -> pd.DataFrame:
    rng = np.random.default_rng(13)
    rows = []
    for g in range(1, _N_GAMES + 1):
        for cls, cap in _CAPACITIES.items():
            sold = int(rng.integers(int(cap * 0.6), cap + 1))
            lift = float(rng.uniform(-0.06, 0.12))
            rows.append({
                "game_id": f"G2027{g:03d}",
                "seat_class": cls,
                "collected_revenue_usd": round(sold * _FACE_PRICES[cls] * (1 + lift), 2),
                "tickets_sold": sold,
            })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# App layout
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Kings Pricing Console",
    layout="wide",
)

if not _LIVE:
    st.warning(
        "**Demo mode** — DATABRICKS_HOST / TOKEN / WAREHOUSE_ID not configured. "
        "Showing synthetic data matching the gold schema (`--scale medium`).",
        icon="⚠️",
    )

st.title("Las Vegas Kings — Ticket Pricing Console")
st.caption(
    f"{'Live · ' + CATALOG + '.' + GOLD if _LIVE else 'Demo data · gold schema synthetic'}"
)

tab_price, tab_revenue, tab_mix = st.tabs(
    ["Price Realization", "Revenue by Game", "Seat Class Mix"]
)

# ---------------------------------------------------------------------------
# Tab 1: Price Realization
# ---------------------------------------------------------------------------
with tab_price:
    st.subheader("Dynamic Pricing Effectiveness by Seat Class")
    df_pr = load_price_realization()

    col_chart, col_table = st.columns([3, 2])
    with col_chart:
        fig_pr = px.bar(
            df_pr.melt(
                id_vars="seat_class",
                value_vars=["avg_face_usd", "avg_paid_usd"],
                var_name="price_type",
                value_name="usd",
            ),
            x="seat_class",
            y="usd",
            color="price_type",
            barmode="group",
            labels={
                "usd": "Average Price (USD)",
                "seat_class": "Seat Class",
                "price_type": "",
            },
            color_discrete_map={
                "avg_face_usd": "#1f77b4",
                "avg_paid_usd": "#ff7f0e",
            },
            title="Face vs. Paid Price — premium classes absorb upward lift",
        )
        st.plotly_chart(fig_pr, use_container_width=True)

    with col_table:
        st.dataframe(
            df_pr[["seat_class", "avg_face_usd", "avg_paid_usd", "price_lift", "orders"]]
            .style.format({
                "avg_face_usd": "${:.2f}",
                "avg_paid_usd": "${:.2f}",
                "price_lift": "{:+.1%}",
                "orders": "{:,}",
            }),
            hide_index=True,
            use_container_width=True,
        )

# ---------------------------------------------------------------------------
# Tab 2: Revenue by Game
# ---------------------------------------------------------------------------
with tab_revenue:
    st.subheader("Per-Game Revenue Trend (81-game season)")
    df_rev = load_revenue_by_game()

    game_range = st.slider(
        "Game range", 1, _N_GAMES, (1, _N_GAMES), key="rev_range"
    )
    df_rev_f = df_rev.iloc[game_range[0] - 1 : game_range[1]]

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Games shown", len(df_rev_f))
    m2.metric(
        "Total Revenue",
        f"${df_rev_f['collected_revenue_usd'].sum():,.0f}",
    )
    m3.metric("Tickets Sold", f"{df_rev_f['tickets_sold'].sum():,}")
    m4.metric(
        "Avg Revenue / Game",
        f"${df_rev_f['collected_revenue_usd'].mean():,.0f}",
    )

    fig_rev = px.bar(
        df_rev_f,
        x="game_id",
        y="collected_revenue_usd",
        color="revenue_lift",
        color_continuous_scale="RdYlGn",
        color_continuous_midpoint=0,
        labels={
            "collected_revenue_usd": "Collected Revenue (USD)",
            "game_id": "Game",
            "revenue_lift": "Lift vs Face",
        },
        title="Collected Revenue by Game — green = sold above face, red = below",
    )
    fig_rev.update_layout(xaxis_tickangle=-75)
    st.plotly_chart(fig_rev, use_container_width=True)

# ---------------------------------------------------------------------------
# Tab 3: Seat Class Mix
# ---------------------------------------------------------------------------
with tab_mix:
    st.subheader("Revenue Mix by Seat Class per Game")
    df_mix = load_seat_class_mix()

    selected = st.multiselect(
        "Seat classes to include",
        _SEAT_CLASSES,
        default=_SEAT_CLASSES,
        key="mix_classes",
    )
    df_mix_f = df_mix[df_mix["seat_class"].isin(selected)]

    fig_mix = px.bar(
        df_mix_f,
        x="game_id",
        y="collected_revenue_usd",
        color="seat_class",
        labels={
            "collected_revenue_usd": "Collected Revenue (USD)",
            "game_id": "Game",
            "seat_class": "Seat Class",
        },
        title="Revenue by Seat Class per Game (stacked)",
    )
    fig_mix.update_layout(xaxis_tickangle=-75, legend_title_text="Seat Class")
    st.plotly_chart(fig_mix, use_container_width=True)
