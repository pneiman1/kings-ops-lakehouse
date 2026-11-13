# ADR-011 — Mobile cart allocation

**Status:** Accepted
**Context:** Cart allocation batch pipeline
**Supersedes:** ADR-011 v1 (written without the venue spec; did not reflect
the actual concourse topology, section numbering, or adjacency structure)

---

## Context

The Kings have 12 mobile concession carts that can be repositioned between
ballpark zones at the boundary of each 30-minute window. Where to position each
cart is currently gut-feel. The question is whether a data-driven allocation —
trained on POS transaction history — produces a measurably better distribution
of demand coverage across zones.

### Venue layout — 33,000-seat retractable-roof ballpark

The ballpark has three concourse levels. Mobile carts operate on two of them.

**Main concourse (ground level, 5 zones — a closed ring):**

| Zone | Sections | Seat classes |
|---|---|---|
| `MC_HOMEPLATE` | 101–108 | FIELD_PREMIUM, INFIELD_BOX |
| `MC_FIRSTBASE` | 109–118 | FIELD_PREMIUM, INFIELD_BOX |
| `MC_RIGHTFIELD` | 119–131 | BASELINE_BOX, OUTFIELD_RESERVED |
| `MC_LEFTFIELD` | 132–148 | BASELINE_BOX, OUTFIELD_RESERVED |
| `MC_THIRDBASE` | 149–159 | BASELINE_BOX, INFIELD_BOX |

The main concourse is a **closed ring** — `MC_THIRDBASE` is physically adjacent
to `MC_HOMEPLATE` (the concourse wraps around the outfield side under the
scoreboard). A cart at `MC_THIRDBASE` can reach `MC_HOMEPLATE` without passing
through any other zone.

**Upper concourse (3 zones — a closed ring):**

| Zone | Sections | Seat classes |
|---|---|---|
| `UC_INFIELD` | 201–220 | UPPER_INFIELD, UPPER_RESERVED |
| `UC_RIGHTSIDE` | 221–239 | UPPER_INFIELD, UPPER_RESERVED, STANDING_ROOM |
| `UC_LEFTSIDE` | 240–259 | UPPER_RESERVED, STANDING_ROOM |

The upper concourse also wraps — `UC_LEFTSIDE` is adjacent to `UC_INFIELD`
via the outfield upper concourse walkway.

**Note on renumbered sections:** Sections 118–124 (originally assigned to
`MC_FIRSTBASE`) were renumbered to 218–224 (now `UC_RIGHTSIDE`) in May after
sightline complaints established they were physically on the upper deck. This
is the SCD2 event in `dim_seat`. The section-to-zone mapping below must be
applied with awareness of this boundary: a seat in section 120 maps to
`MC_FIRSTBASE` before the renumbering date and to `UC_RIGHTSIDE` after.

**Premium levels (not on either ring):**

| Zone | Sections | Note |
|---|---|---|
| `SUITE_RING` | 301–339 | Accessible from `UC_INFIELD` via escalator |

**Fixed stations (carts do not reposition):**

| Zone | Note |
|---|---|
| `BULLPEN_BAR` | Right-field specialty bar, corner of `MC_RIGHTFIELD` |
| `TEAM_STORE` | Merchandise only, no food/alcohol, behind `MC_HOMEPLATE` |

`BULLPEN_BAR` and `TEAM_STORE` are staffed year-round. Mobile cart allocation
does not reposition carts into or out of these stations.

### Ring adjacency with wraparound

Carts reposition between adjacent zones only. The two concourse rings have
full wraparound: the last zone on each ring is adjacent to the first.

```
Main ring (clockwise):
  MC_HOMEPLATE — MC_FIRSTBASE — MC_RIGHTFIELD — MC_LEFTFIELD — MC_THIRDBASE
       |_______________________________________________|  (wraparound)

Upper ring (clockwise):
  UC_INFIELD — UC_RIGHTSIDE
      |_________________________|  (wraparound via outfield upper walkway)
  UC_LEFTSIDE — UC_INFIELD

Cross-level ramp connections (not on either ring):
  MC_HOMEPLATE <-> UC_INFIELD
  MC_FIRSTBASE <-> UC_RIGHTSIDE
  MC_THIRDBASE <-> UC_LEFTSIDE
  UC_INFIELD   <-> SUITE_RING

Fixed-station spurs (carts never enter or exit via these):
  BULLPEN_BAR connected to MC_RIGHTFIELD
  TEAM_STORE  connected to MC_HOMEPLATE
```

### Section-to-zone portal mapping

The gate-to-zone mapping in v1 was wrong in kind, not just detail. Gate
entries tell us which concourse level fans enter; they do not tell us where
fans will buy, which is determined by their seat location. The correct mapping
is **section-to-zone**: a fan in section 115 will buy at `MC_FIRSTBASE` or
its nearest stand, not at the gate they happened to enter through.

| Section range | Zone | Level |
|---|---|---|
| 101–108 | `MC_HOMEPLATE` | Ground |
| 109–118 | `MC_FIRSTBASE` | Ground |
| 119–131 | `MC_RIGHTFIELD` | Ground |
| 132–148 | `MC_LEFTFIELD` | Ground |
| 149–159 | `MC_THIRDBASE` | Ground |
| 201–220 | `UC_INFIELD` | Upper |
| 221–239 | `UC_RIGHTSIDE` | Upper |
| 240–259 | `UC_LEFTSIDE` | Upper |
| 301–339 | `SUITE_RING` | Premium |

This mapping is used in enhanced_v2 to portal-weight demand: zones whose
sections are densely ticketed for a given game see boosted initial allocation
in windows 0–1 before POS timing data is available. It is also the join key
for attributing `sv_admission` rows to zones.

### Switching penalty

Repositioning a cart requires shutdown at origin, transit, and setup at
destination. Measured in pre-season operations drills:

| Moves | Time cost | Productive time remaining (of 30 min) |
|---|---|---|
| 0 (stay) | 0 min | 30 min |
| 1 adjacent zone | ~10 min | ~20 min |
| 2 adjacent zones | ~20 min | ~10 min |
| 3+ zones | >28 min | <2 min — not feasible |

Constant: `SWITCHING_COST_MINUTES_PER_ZONE = 10`.
Hard limit: `MAX_MOVES_PER_WINDOW = 2`.

A cart that moves 3+ zones in one window loses the entire window and arrives
late to setup. The constraint enforces that no enhanced model recommends a
reposition that cannot physically happen.

### Baseline demand numbers (--scale medium, B3 pending)

At `--scale medium` (27 games, 12,000 seats, 18,000 accounts):

Expected POS transaction volume (derived from generator parameters):
- Attendance rate: ~75% of seats (new team, strong early demand)
- Transactions per attendee per game: ~2.5 items (from SKU attach rates)
- Fleet: 12 carts across 11 zones (9 mobile + 2 fixed)

| Metric | Derived estimate | Notes |
|---|---|---|
| POS lines / season | ~243,000 | 12k × 0.75 × 2.5 × 27, minus ~1.2% dropped batches |
| POS lines / game | ~9,000 | |
| Per zone per game (uniform) | ~818 | Generator draws zone uniformly; real demand clusters by section |
| Per zone per window (8 windows) | ~102 | Varies ±3x by window; gate surge in windows 0–1 |
| Uplift from optimization (v1) | ~0% | v1 is proportional to history; with uniform draws, history is uniform |

**Key limitation of the synthetic baseline:** The generator assigns
`stand_location` values with equal probability across the 6 original zone
names. Because the draw is uniform, `demand_index` is flat across zones and
the baseline_v1 allocation cannot improve on uniform. Real uplift will appear
only after the generator is updated to cluster POS draws by seat section
(FIELD_PREMIUM seats in sections 101–108 buy at MC_HOMEPLATE, not randomly
across zones). D10 covers the generator update.

Actual numbers from a `--scale medium` pipeline run will replace these
estimates when Phase 1 compute is available (blocked by B3).

---

## Decisions

### D1 — Batch pipeline, not streaming

Carts are physical objects. Repositioning takes ~10 minutes per zone and cannot
happen faster than a 30-minute window. Real-time allocation adds pipeline
complexity with no operational benefit. The pipeline runs once per game day,
producing the full window-by-window schedule before gates open.

### D2 — 30-minute windows from gates-open

Gates open 90 minutes before first pitch. The window sequence starts at
gates-open (window 0):

```
Window 0: gates_open + 0  to +30 min  (admission surge)
Window 1: gates_open + 30 to +60 min  (pre-game peak)
Window 2: gates_open + 60 to +90 min  (trailing entry)
Window 3: first pitch -> innings 1-2
...
Window N: post-game retraction
```

Window size is 30 minutes because:
- 15-min windows: too much noise relative to repositioning time
- 60-min windows: miss the intra-game surge during 7th-inning stretch and
  pitching changes

### D3 — Baseline algorithm: proportional allocation (demand_index x n_carts)

For each (zone, window_num), `demand_index` is that zone's share of total
transactions in that window across all zones, computed from prior-game history.
Allocate `round(demand_index x 12)` carts.

Baseline_v1 intentionally excludes adjacency and section weighting. It
establishes a measured baseline for comparison with:
- **Enhanced v2**: section-weighted portal adjustment (boost zones whose
  sections are heavily ticketed for windows 0–1)
- **Enhanced v3**: adjacency-constrained rebalancing with switching penalty
  (shift surplus carts to adjacent high-demand zones between windows, subject
  to `MAX_MOVES_PER_WINDOW = 2`)

Tracking improvement against baseline_v1 is only meaningful if baseline_v1
is cleanly defined without adjacency or portal logic mixed in.

### D4 — Replay-safe, no lookahead

For each game N, the allocation uses games 1..N-1 only:

```python
Window.partitionBy("zone", "window_num")
      .orderBy("game_date")
      .rowsBetween(Window.unboundedPreceding, -1)
```

Game 1 falls back to uniform (1/n_zones per zone). This matters because:
1. Operationally, the pipeline runs before the game — future data does not exist.
2. For evaluation, measuring allocation against a game's actual demand is only
   valid if the allocation was made without that data.

### D5 — Alcohol cutoff at 165 minutes after first pitch (top of 7th)

MLB alcohol service stops at a fixed clock time, typically top of the 7th.
165 minutes after first pitch covers all reasonable game paces. Any window
starting at or after minute 165 sets `is_alcohol_allowed = False` for alcohol
zones; zero carts are allocated there.

Cutoff applies to all zones except `TEAM_STORE` (merchandise only). `BULLPEN_BAR`
is an alcohol zone and is subject to the cutoff.

### D6 — Zone adjacency as static metadata, reserved for enhanced_v2

`ZONE_ADJACENCY` is defined as a constant (the full ring structure) and
included in the code for documentation and for enhanced_v2+. The baseline model
does not use it.

### D7 — Section-to-zone mapping as static metadata, reserved for enhanced_v2

`SECTION_ZONE_MAP` is defined as a constant and reserved for enhanced_v2.
The baseline uses actual POS timing data, which captures section-based demand
naturally once the generator emits section-stratified stand locations.

### D8 — Switching penalty as static metadata, reserved for enhanced_v3

`SWITCHING_COST_MINUTES_PER_ZONE = 10` and `MAX_MOVES_PER_WINDOW = 2` are
defined as constants and reserved for enhanced_v3. The baseline does not
compute or penalise cart movements.

### D9 — 12 carts total, configurable

The fleet size is passed as `n_carts` (default 12). Integer rounding means the
allocated count may differ from `n_carts` by at most `len(ZONES)` carts total.
With 11 zones: `round(12/11) = 1` cart per zone on game 1, total 11 (one
below fleet size). Acceptable; rounding correction is in scope for v2.

### D10 — Generator update deferred

`tools/generate_source_data.py` currently draws `stand_location` uniformly
from the 6 original zone names (MAIN_CONCOURSE, UPPER_CONCOURSE, etc.). These
names do not match the 11-zone layout in this ADR. The generator needs to be
updated to:
1. Emit the new zone names
2. Weight draws by the section distribution for each zone (FIELD_PREMIUM in
   MC_HOMEPLATE/MC_FIRSTBASE, OUTFIELD_RESERVED in MC_RIGHTFIELD/MC_LEFTFIELD,
   etc.)

Until the generator is updated, the pipeline runs with 6-zone demand input
and 11-zone zone metadata — which is internally consistent but not realistic.

---

## Evaluation plan

Target: `--scale medium` (27 games, 12,000 seats, 18,000 accounts).

Blocked by B3 (workspace job triggering disabled as of 2026-11-12).

Metrics to report once Phase 1 compute is available:

| Metric | Source | Expected value |
|---|---|---|
| POS lines ingested | `_manifest.json` vs bronze count | ~243k ± batch drop rate |
| Zones in demand output | `compute_demand` distinct zones | 11 (or 6 until D10) |
| Windows per game | `compute_demand` distinct window_nums | 8–12 by game length |
| Mean carts/window (v1) | `gold.reco_cart_allocation` | 12 ± rounding slack |
| Null prior count | rows where prior_demand_index IS NULL | <= n_zones (game 1 only) |

---

## Consequences

- 11 zones replace 6. The generator's `stand_location` values must be updated
  to match (D10 above). Until then, the pipeline runs with 6-zone demand input
  and 11-zone zone metadata — internally consistent but not realistic.
- Integer rounding with 11 zones and 12 carts: `round(12/11) = 1` cart per zone
  on game 1. Total = 11, one cart below fleet size. Acceptable.
- The main concourse wraparound (MC_THIRDBASE <-> MC_HOMEPLATE) and upper
  concourse wraparound (UC_LEFTSIDE <-> UC_INFIELD) must be tested explicitly
  in enhanced_v2 — routing that ignores the wraparound would take 4 hops
  around the long way instead of 1 hop the short way.
- The section-to-zone mapping is time-dependent due to the section renumbering
  event in May. Any enhanced model must handle the boundary case where sections
  118–124 switch zones mid-season.

---

## Exam relevance

This pipeline exercises: batch DataFrame transforms, window functions (rolling
historical average with lookahead prevention), proportional allocation, integer
rounding, service-rule enforcement (alcohol cutoff), graph-structure metadata
(ring adjacency with wraparound), and time-dependent dimensional mapping
(section-to-zone with SCD2). The batch-vs-streaming justification (D1) is a
directly tested objective.
