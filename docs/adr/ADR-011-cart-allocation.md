# ADR-011 — Mobile cart allocation

**Status:** Accepted
**Context:** Cart allocation batch pipeline

---

## Context

The Kings have ~12 mobile concession carts that can be repositioned between six
ballpark zones at the boundary of each 30-minute window. Where to position each
cart is currently decided by gut feel on game day. The question is whether a
data-driven allocation — trained on POS transaction history — produces a
measurably better distribution of demand across zones.

The six zones (from the POS `stand_location` field):

| Zone | Description | Level | Alcohol |
|---|---|---|---|
| `MAIN_CONCOURSE` | Home plate–side ground level | Ground | Yes |
| `UPPER_CONCOURSE` | Upper deck ring | Upper | Yes |
| `LEFT_FIELD_PLAZA` | Left field bleachers/standing | Ground | Yes |
| `SUITE_LEVEL` | Premium level | Premium | Yes |
| `TEAM_STORE` | Merchandise — no food/alcohol | Ground | No |
| `BULLPEN_BAR` | Right-field specialty bar | Ground | Yes |

Ring adjacency (carts can be repositioned to adjacent zones between windows):

```
MAIN_CONCOURSE ↔ LEFT_FIELD_PLAZA
MAIN_CONCOURSE ↔ BULLPEN_BAR
UPPER_CONCOURSE ↔ SUITE_LEVEL
TEAM_STORE       — no adjacency (fixed merchandise station)
```

Portal mapping (which zones each gate feeds into):

```
Gate A → MAIN_CONCOURSE
Gate B → MAIN_CONCOURSE
Gate C → LEFT_FIELD_PLAZA
Gate D → BULLPEN_BAR
```

---

## Decisions

### D1 — Batch pipeline, not streaming

Carts are physical objects that cannot be repositioned faster than the 30-minute
reposition window. Real-time allocation adds pipeline complexity with no
operational benefit. The pipeline runs once per game day, producing the full
window-by-window schedule before gates open.

The alternative (streaming allocation) would add watermarks, state stores, and
checkpoint management for a problem that is fundamentally a batch scheduling
task.

### D2 — 30-minute windows from gates-open

Gates open 90 minutes before first pitch. The window sequence starts at
gates-open (window 0), so the full timeline is:

```
Window 0: gates_open + 0  to +30 min  (gate entry surge)
Window 1: gates_open + 30 to +60 min  (pre-game peak)
Window 2: gates_open + 60 to +90 min  (trailing entry)
Window 3: gates_open + 90 min = first pitch → innings 1–2
...
Window N: post-game (carts retract)
```

Shorter windows (15 min) have too much noise relative to the physical reposition
time. Longer windows (60 min) miss the intra-game surge during pitching changes
and 7th-inning stretch.

### D3 — Baseline algorithm: proportional allocation (demand_index × n_carts)

For each (zone, window_num), `demand_index` is that zone's share of total
transactions across all zones in that window, computed from prior-game history.
Allocate `round(demand_index × 12)` carts.

The baseline intentionally does not use adjacency or portal mapping. It
establishes a measured baseline for future comparison with:
- **Enhanced v2**: portal-adjusted initial windows (more carts at gate-adjacent
  zones during windows 0–1)
- **Enhanced v3**: adjacency-aware rebalancing (shift surplus carts from low-
  demand to adjacent high-demand zones between windows)

Tracking performance against baseline_v1 is only meaningful if baseline_v1 is
cleanly defined. Mixing portal and adjacency logic into the first version makes
the comparison undefined.

### D4 — Replay-safe, no lookahead

For each game N, the allocation is built from games 1..N-1 only. No data from
game N (or any later game) is used. This is enforced by a window function:

```python
Window.partitionBy("zone", "window_num")
      .orderBy("game_date")
      .rowsBetween(Window.unboundedPreceding, -1)
```

The first game in the season has no prior data and falls back to a uniform
allocation (1/6 per zone per window).

This matters for two reasons:
1. **Operational validity**: in production the pipeline runs before the game,
   so future data does not exist.
2. **Evaluation validity**: measuring the allocation against the game's actual
   demand is only meaningful if the allocation was made without that data.

### D5 — Alcohol cutoff at 165 minutes after first pitch (top of 7th)

MLB alcohol service stops at a fixed clock time, typically top of the 7th
inning. 165 minutes after first pitch (2h45m) is a conservative bound that
covers all game paces. Any window starting at or after minute 165 receives
`is_alcohol_allowed = False` for alcohol zones. Zero carts are allocated to
those zones in those windows.

The cutoff is a named constant (`ALCOHOL_CUTOFF_MINUTES = 165`) so it can be
overridden by catalog/environment config without touching the transform logic.

`TEAM_STORE` sells merchandise only and is not subject to the alcohol cutoff;
it receives its demand-proportional allocation through all windows.

### D6 — Zone adjacency as static metadata, not enforced by baseline_v1

The adjacency map is defined as a constant (`ZONE_ADJACENCY`) and included in
the ADR for documentation. The baseline model does not use it. It is reserved
for the rebalancing step in enhanced_v2 or later.

Reason: adding adjacency-constrained rebalancing to the baseline makes the
allocation harder to explain and audit. "Proportional to history" is easy to
verify; "proportional, then rebalanced across adjacency graph" is harder.

### D7 — Portal mapping as static metadata, not enforced by baseline_v1

Similarly, the gate→zone mapping is documented and reserved. Gate A and B feed
MAIN_CONCOURSE; in windows 0–1 that zone sees a surge from admission entry that
does not appear in POS data until 10–15 minutes later (admission → concession
lag). A portal-adjusted model would apply a forward-shifted demand boost to
MAIN_CONCOURSE in those windows. Baseline_v1 uses actual POS timing, which
already captures this naturally from the data.

### D8 — 12 carts total, configurable

The fleet size is passed as `n_carts` (default 12). The transforms scale
correctly to any fleet size. If the club expands to 15 carts for high-demand
games, the same logic applies.

---

## Consequences

- The first game of the season uses uniform allocation (1/6 per zone per
  window). There is no prior data for game 1; this is explicit and expected.
- Integer rounding means the allocated cart count may not sum exactly to 12
  for every window. The error is at most `len(ZONES) = 6` carts total.
  Baseline_v1 accepts this; a rounding-correction step is in scope for v2.
- Alcohol cutoff is a time boundary, not an inning boundary. If the game runs
  unusually long (extra innings), the cutoff still fires at minute 165 from
  first pitch regardless of actual inning.
- Portal and adjacency metadata are code constants, not catalog tables. If they
  need to change (ballpark layout changes, new gates), edit the constant in
  `src/kings_ops/transforms/cart_allocation.py`.

---

## Exam relevance

This pipeline exercises: batch DataFrame transforms, window functions (rolling
historical average with lookahead prevention), proportional allocation, integer
rounding, and service-rule enforcement (alcohol cutoff) — all in the pure-
function pattern required for unit testing without a cluster.
