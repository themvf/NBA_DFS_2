# DraftKings NFL DFS — Spec (2026-09-01)

Target: a dedicated `/dfs/nfl` workspace covering both DK NFL contest
structures (Classic weekly, Showdown single-game) and both contest
intents (PvP/cash, Tournament/GPP), fed by a manually uploaded DK salary
CSV, producing a DK bulk-entry-uploadable lineup CSV.

**Visibility amendment (2026-09-02):** At the user's direction, `/dfs/nfl`
ships before the projection-model gate as an explicitly labelled intake and
readiness workspace. It exposes the verified salary parser and pool audit but
keeps projections, optimization, and entry export locked. This does not waive
steps 4–6 or permit DK average points to be presented as our model.

## Decisions taken 2026-09-01

User-selected, recorded so they are not silently revisited.

| Decision | Choice | Consequence |
|---|---|---|
| Projection source | **Build our own weekly model** | The page does not ship before the model exists. Dependency order below is data → model → UI, matching the standing Jira-First Delivery Contract. |
| Route | **Dedicated `/dfs/nfl`** | New page, new server actions, new optimizer module. Reuses the sport-agnostic `dk_slates`/`dk_players`/`dk_lineups` tables. Cannot regress live NBA/MLB, which share an 8,551-line `actions.ts` and a 4,177-line client. |
| Export | **DK bulk-entry upload format only** | Requires the user to upload the entry file downloaded from the DK contest; entry IDs come from that file, not from us. A plain review CSV was explicitly not requested and is not built. |

## 1. What DraftKings actually requires — verified, not assumed

Everything in this section is now **VERIFIED** against DK's official
Classic and Showdown Captain Mode rules pages, supplied by the user
2026-09-01 after automated access failed (403 to fetch, domain blocked
in the browser tool), plus the user's own Week 1 exports for everything
the files themselves prove.

The scoring table previously recorded here from secondary sources turned
out to be **correct in every line**, including the DST points-allowed
tiers. Two things it was missing or wrong about are corrected below and
called out explicitly rather than quietly patched.

### The real column layout — VERIFIED against Week 1 exports

Both files (Classic, 719 rows; Showdown, 126 rows) carry:

```
Position, Name + ID, Name, ID, Roster Position, Salary,
Game Info, TeamAbbrev, AvgPointsPerGame, Status
```

A UTF-8 BOM precedes `Position`. Two columns matter more than they look:

- **`Status` exists**, with values `OUT`, `IR`, `Q` and blank — 56 of the
  719 Classic players are `IR`/`OUT` and cannot play. The NBA/MLB parser
  in `actions.ts` states the opposite (*"CSV doesn't carry DK injury
  status — rely on LineStar for is_out"*). That is true for NBA and
  **false for NFL**, so NFL needs no LineStar dependency for
  availability. An `IR` player left in the pool silently wastes a roster
  slot.
- **`Position` and `Roster Position` diverge completely on Showdown**
  (`Roster Position` is `CPT`/`FLEX` there), which is why both are read.
  On Classic, `Roster Position` carries the FLEX eligibility directly:
  `QB`, `RB/FLEX`, `WR/FLEX`, `TE/FLEX`, `DST`.

`Q` (questionable) is treated as risk, not absence — whether to accept it
is a contest-type judgement, so the parser flags it and the optimizer
decides.

### Classic (weekly) — VERIFIED

```
QB  RB  RB  WR  WR  WR  TE  FLEX  DST      9 players, $50,000 cap
FLEX = RB / WR / TE.  No kicker in DK NFL Classic.
```

The no-kicker claim is now verified against a real export rather than
inferred: the Week 1 Classic pool is 91 QB / 153 RB / 295 WR / 156 TE /
24 DST and **zero K**. Showdown pools *do* include kickers (2 in the
NE@SEA file), so the position set is format-specific, not sport-wide.

**Correction (2026-09-01).** An earlier draft of this spec recorded the
Classic constraint as *"at least 2 different NFL teams AND at least 2
different games"*, taken from a secondary source. DK's official rule is
narrower — **"must include players from at least 2 different NFL
games"**, and nothing about a team count. The team clause was redundant
anyway (two games implies at least two teams), so the effect is one
fewer constraint for the optimizer to enforce, not a behaviour change.

The games rule is still real and still load-bearing: neither the NBA nor
the MLB optimizer in this repo has anything like it, so it is new code.

### Showdown Captain Mode (single game) — VERIFIED

```
CPT  FLEX  FLEX  FLEX  FLEX  FLEX          6 players, $50,000 cap
CPT scores 1.5x and costs 1.5x salary.
Every slot, CPT included, is eligible to QB / RB / WR / TE / K / DST
  (Showdown is the only DK NFL format where two QBs are legal, and a
   DST or a kicker may be captained).
Lineups must include players from BOTH teams.
```

DK states the duplicate-row trap as a rule in its own words: *"a player
cannot be added at both Captain and another position in the same
lineup."* That is exactly the constraint the parser's CPT/FLEX collapse
exists to make representable.

The DK Showdown salary CSV lists **each player twice** — one `CPT` row
and one `FLEX` row, with different `ID` and different `Salary`. The
ingestion must treat those two rows as one underlying player with two
purchasable roles, or the optimizer will happily roster the same human
twice.

Confirmed on the real NE@SEA file: 126 rows collapse to **63 players**,
every one priced at both CPT and FLEX, with the CPT/FLEX salary ratio
exactly 1.500 across all 63 (min 1.500, max 1.500). The parser still
reads DK's own CPT salary rather than deriving it — the multiplier is
only a fallback, since a derived value would drift the moment DK rounds.

The two rows are **not adjacent**: DK sorts the file by salary, so a
player's CPT row can sit dozens of lines from his FLEX row. Pairing is by
name + team, never by position in the file.

### Scoring — VERIFIED against DK's official rules

Identical between Classic and Showdown, except that Showdown adds
kickers and multiplies the captain.

Offense (full PPR):

```
Passing TD            +4      Rushing TD              +6
Passing yard        +0.04     Rushing yard           +0.1
300+ pass yd game     +3      100+ rush yd game       +3
Interception          -1      Reception               +1
Fumble lost           -1      Receiving yard         +0.1
2pt conversion        +2      Receiving TD            +6
Return TD             +6      100+ rec yd game        +3
```

DST:

```
Sack +1 | INT +2 | Fumble recovery +2 | Safety +2 | Blocked kick +2
Any defensive or special-teams TD +6 | 2pt return +2
Points allowed:  0 = +10 | 1-6 = +7 | 7-13 = +4 | 14-20 = +1
                21-27 = 0 | 28-34 = -1 | 35+ = -4
```

**Kicker — Showdown only** (Classic has no kicker slot at all):

```
Extra point +1 | 0-39 yd FG +3 | 40-49 yd FG +4 | 50+ yd FG +5
```

Two notes on this, both load-bearing:

- DK restricts these categories to kickers: *"Kickers are only eligible
  for extra points and field goals made. Non-kickers are not eligible."*
  So a position player who kicks in an emergency scores nothing for it,
  and the scorer must gate on position rather than on the stat's
  presence.
- This is **exactly Yahoo's distance-tiered kicker formula**, which this
  repo already implements as `yahoo_kicker_points()` in
  `ingest/ff_independent.py`, fed by the same nflverse per-distance
  buckets (`fg_made_0_19` … `fg_made_60_`). The DFS kicker path should
  reuse those buckets rather than re-derive them — and note the trap
  already recorded for fantasy football: nflverse's generic
  `fantasy_points` field is **0 for essentially every kicker**, so
  scoring must come from the buckets, never from that column.

**Points Allowed is not the opponent's final score.** DK: *"Points
Allowed only includes points surrendered while DST is on the field —
doesn't include points given up by team's offense."* A pick-six thrown by
your DST's own offense does not count against it. Special-teams and
blocked-kick return TDs, extra points and field goals all do.

This matters for section 3's DST design, which maps the opponent's
Vegas implied total through the tiers: that proxy is **biased slightly
high**, because a fraction of the opponent's implied points arrive via
defensive scores the DST is not charged for. The bias is small but
systematic and one-directional, so it should be measured and corrected
rather than assumed negligible.

The three yardage **bonuses are threshold events, not linear terms**.
A mean-based projection cannot price them: a 250-yard mean, and a
250-yard mean with twice the variance, carry different bonus
expectation. This is the same mean-versus-distribution error already
recorded in this repo's MLB Totals section, and it is why the model is
simulation-based rather than a point estimate (section 3).

## 2. Data sources

| Input | Source | Status today | Work needed |
|---|---|---|---|
| Player pool, salaries, roster positions, game info, **injury status** | **User-uploaded DK CSV** | ✅ **Built + validated** — `web/src/lib/nfl-dfs/dk-salary-csv.ts` | Done. Validated against real Week 1 Classic (719 players / 12 games / 24 teams) and Showdown (63 players / 1 game / 2 teams) exports, 0 warnings on both. |
| Game environment: total, spread, implied team totals | `nfl_matchups` | **Live**, `refresh_nfl_vegas` 1×/day, 16 credits | Nothing. Already flowing. |
| Player prop lines | The Odds API, `americanfootball_nfl` | **Not ingested** | New `ingest/nfl_prop_odds.py` |
| Weekly actuals + recent form | `ff_player_week_stats` | Prior completed seasons only | In-season weekly refresh |
| Roster, position, team, depth | `ff_players` (nflverse) | Live | Reuse |
| Projected ownership | LineStar | NBA=5, CBB=4; **NFL sport id unknown** | Empirical discovery, or defer — see section 6 |

**Availability no longer needs LineStar.** DK's own `Status` column
supplies `OUT`/`IR`/`Q` directly, so the only remaining LineStar use for
NFL is projected ownership, which section 6 defers.

### Prop markets — keys confirmed, coverage NOT confirmed

Confirmed against The Odds API market documentation 2026-09-01:

```
player_pass_yds   player_pass_tds   player_pass_interceptions
player_rush_yds   player_receptions player_reception_yds
player_anytime_td
```

All are event-scoped (`/events/{id}/odds`), never bulk. Using the
`bookmakers=` parameter rather than `regions=` prices these at
**markets × 1** — the cost model already established and measured in
this repo's MLB D4 work — so 7 markets is **7 credits/event**, roughly
**112 credits for a full 16-game Sunday slate**.

Against the current ~395/day scheduled burn (~11,850/30d of a 20,000
key shared by every sport), a user-triggered fetch once or twice a week
costs ~500–900/month and fits the remaining headroom. It must be
**user-triggered and deduped**, exactly like the existing NBA and MLB
"Fetch Player Props" buttons — never a cron. Read
[`docs/the-odds-api.md`](the-odds-api.md) before changing this.

**P0 gate, do not skip:** a market key existing in documentation is not
the same as books posting it, and this repo has already been burned by
exactly that distinction twice — `batter_home_runs`, where DraftKings
posted nothing, and `batter_runs_scored`, where Pinnacle posted a
different line every time. Probe all 7 markets against 5 real NFL events
and record per-market **DraftKings presence, Pinnacle presence, and
same-line agreement** before any model code depends on a market.

## 3. Projection model

`model/nfl_dfs_projections.py`. Architecture follows the pattern this
repo has already validated twice: **market lines replace the formula
where they exist, history fills the rest**, and the output is a
distribution rather than a point estimate.

### Stage 1 — per-stat expectation

For each player, per stat, in priority order:

1. **Prop line, de-vigged.** A prop is a line *and* a price; the pair
   pins P(over) rather than only the median. Both are used. Prices are
   averaged in probability space, never in American odds — the
   arithmetic-averaging bug already fixed once in this repo.
2. **History + environment fallback**, for the majority of a ~500-player
   NFL pool that carries no prop market: per-game rates from
   `ff_player_week_stats`, scaled by the team's Vegas implied total and
   by depth-chart role.

Anytime-TD props are the single highest-leverage input — a touchdown is
6 of the ~15 points a typical flex player scores — and they exist for
nearly every skill player, so prop coverage is better than the
skill-position count alone suggests.

### Stage 2 — simulation, not addition

`compute_monte_carlo()` is already sport-agnostic and is reused. Per
player, draw correlated stat outcomes, apply DK scoring **including the
three yardage bonuses evaluated per draw**, and keep the distribution:

```
proj (mean) | floor (P10) | ceiling (P90) | boom rate
```

Bonus expectation falls out of the simulation instead of being bolted
on afterward. Per-player variance comes from weekly history.

### Stage 3 — DST

DST has no player props and is handled from game odds alone, the same
way this repo's soccer spec handles goalkeepers: the opponent's implied
total maps through the points-allowed tiers, plus sack and turnover
rates from team history. This is explicitly the weakest projection in
the pool and must be labelled as such in the UI, not presented at parity
with a prop-driven skill projection.

### Kill criteria — fixed now, before any data is examined

Graded walk-forward on completed 2026 weeks, one week at a time, never
in-sample:

- **Primary:** mean absolute error per player against DK actuals, by
  position, versus a DK `AvgPointsPerGame` baseline. The model must beat
  that baseline out of sample at **every** skill position, or the
  position it fails on falls back to the baseline rather than shipping a
  worse number dressed up as a projection.
- **Secondary:** rank correlation within position — for DFS, ordering
  matters more than level, the same finding already recorded for
  season-long DST in this repo.
- **Calibration:** realized P10/P90 coverage must be near 10%/90%. An
  uncalibrated ceiling is worse than useless in GPP, where the entire
  strategy is built on it.
- **DST specifically:** if DST cannot beat a flat positional constant,
  it ships as a flat constant and says so — the exact outcome already
  reached, with evidence, for season-long DST in this repo.

Minimum sample before any verdict: **4 completed weeks**. No conclusion
is drawn earlier, and no constant is tuned on a week that has already
been graded.

## 4. Optimizer

`web/src/app/dfs/nfl/nfl-optimizer.ts`. Modelled on the shape of
`optimizer.ts` (NBA) and `mlb-optimizer.ts`, not shared with them.
Rules NFL genuinely needs:

**Classic**
- 9 slots, FLEX = RB/WR/TE, $50k
- **≥ 2 different games** (DK-enforced, new to this repo; there is no
  separate team-count rule — see the correction in section 1)
- QB stacking: QB plus 1–2 pass catchers from his own team; the
  strongest correlation in DFS and the core of NFL GPP
- Bring-back: one opposing player, capturing shootout game script
- Anti-correlation: never roster a DST against your own stacked QB
- RB and DST from the same team is mildly positive (game script),
  unlike MLB's pitcher-versus-batter rule, which is strictly negative

**Showdown**
- 6 slots, CPT at 1.5× points and 1.5× salary; K and DST are
  CPT-eligible like everyone else
- One game; **both teams must be represented**
- The same human may not occupy both the CPT and a FLEX slot — the
  CPT/FLEX duplicate-row trap from section 1
- CPT choice dominates Showdown outcomes and is enumerated explicitly
  rather than left to the generic slot filler

**PvP (cash) vs Tournament (GPP)**

| | PvP / cash | Tournament / GPP |
|---|---|---|
| Objective | maximise floor (P10) | maximise ceiling (P90) |
| Stacking | discouraged — it adds variance | required |
| Ownership | ignored | leverage: fade the chalk |
| Exposure | one strong lineup | N diversified lineups with exposure caps |

This mirrors the `OptimizerMode` split already in `optimizer-mode.ts`
and the contest-type objective functions in the standing Model
Improvement Roadmap.

## 5. Export — DK bulk entry

DK mass entry requires the entry file **downloaded from the contest**,
which carries one row per paid entry with its Entry ID. We fill that
file; we never invent Entry IDs.

```
Upload DK entry file → N lineups generated → each lineup written into
one entry row, player IDs in DK's exact column order → download →
upload to DK
```

Column order is format-specific (`QB,RB,RB,WR,WR,WR,TE,FLEX,DST` for
Classic; `CPT,FLEX,FLEX,FLEX,FLEX,FLEX` for Showdown) and must be read
from the uploaded file's own header rather than hardcoded, so a DK
column change degrades to a clear error instead of a silently misaligned
upload. If the generated lineup count and the entry-row count disagree,
the export fails closed and says so — a partially filled entry file
silently enters the wrong lineups for real money.

## 6. Deliberately out of scope for v1

- **Projected ownership.** LineStar's NFL sport id is unknown and needs
  empirical discovery. Without it, GPP leverage falls back to our own
  ownership estimate — the salary-adjusted formula already used for NBA.
  Real projected ownership is a phase-2 improvement.
- **Late swap.** DK allows swapping players whose game has not started.
  Real, valuable, and a separate feature.
- **Tiers contests.** A third DK format, with no salary cap. Not
  requested.
- **In-season model retraining.** v1 grades weekly; automatic
  recalibration is phase 2, and per the standing discipline in this
  repo a calibration loop may only ever downgrade confidence
  automatically, never promote it.

## 7. Dependency order

Per the Jira-First Delivery Contract, no step starts before its
prerequisites pass.

| # | Step | Gate |
|---|---|---|
| 1 | DK scoring verification | ✅ **Done** — official Classic + Showdown rules supplied 2026-09-01; table confirmed correct, kicker scoring and the Points Allowed definition added |
| 2 | Schema: `sport='nfl'` on `dk_slates`, NFL columns on `dk_players`, `nfl_player_props` | Migrations applied, NBA/MLB untouched |
| 3 | DK CSV ingestion, Classic + Showdown | ✅ **Done** — real Week 1 exports parse with 0 warnings; 126 Showdown rows resolve to 63 players |
| 4 | Prop market probe, then prop ingestion + in-season weekly stats | Per-market DK/Pinnacle/same-line coverage recorded before any model code depends on a market; coverage counts reported, not assumed |
| 5a | DK scoring module | ✅ **Done** — `web/src/lib/nfl-dfs/scoring.ts`, offence/kicker/DST + captain, verified against DK's official tables |
| 5b | Projection model | Kill criteria in section 3 evaluated honestly |
| 6 | Optimizer, both formats, both modes | Lineups are DK-legal, including the 2-team/2-game rule |
| 7 | `/dfs/nfl` UI | **Intake/pool audit visible**; projections, optimizer controls, and exposure remain gated |
| 8 | Entry-file export | Round-trips against a real DK entry file |
| 9 | Tests + real-data verification | Real slate, end to end |

Step 5 gates 6–9.

**Status (2026-09-01):** steps 1, 3 and 5a complete — DK's rules and
scoring are verified from the official pages, the salary parser is
validated against real Week 1 exports, and the scoring module is built
and tested. Step 4's prop-market probe is still blocked on an Odds API
key. Step 2 (schema) is unblocked and next.
