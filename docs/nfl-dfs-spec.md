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
| Player prop lines | The Odds API, `americanfootball_nfl` | **Deferred** | Apply only after the prop-free historical baseline passes its promotion gate |
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

All are event-scoped (`/events/{id}/odds`), never bulk.

**Cost correction (2026-09-01).** An earlier draft of this spec said the
`bookmakers=` parameter prices a call at `markets × 1` *regardless of
book count*. That is wrong, and `ingest/mlb_prop_odds.py` records the
measured rule:

```
credits = n_markets × ceil(n_books / 10)     per event
```

8, 9 and 10 books all cost `markets × 1`; **the 11th book doubles the
bill**. So ten books is a hard cap, not a preference. `regions=` remains
strictly worse (`markets × n_regions`).

At 7 markets and ≤10 books that is **7 credits/event**, or roughly
**112 credits for a full 16-game Sunday slate**.

Against the current ~395/day scheduled burn (~11,850/30d of a 20,000
key shared by every sport), a user-triggered fetch once or twice a week
costs ~500–900/month and fits the remaining headroom. It must be
**user-triggered and deduped**, exactly like the existing NBA and MLB
"Fetch Player Props" buttons — never a cron. Read
[`docs/the-odds-api.md`](the-odds-api.md) before changing this.

**P0 gate.** A market key existing in documentation is not the same as
books posting it, and this repo has been burned by exactly that
distinction twice — `batter_home_runs`, where DraftKings posted nothing,
and `batter_runs_scored`, where Pinnacle posted a different line every
time. `ingest/nfl_prop_probe.py` measures it: per market, DraftKings
presence, Pinnacle presence, paired-quote availability and same-line
agreement, across N real events. It writes nothing and prints its exact
credit cost. Run it via the manual `NFL Prop Market Probe` workflow,
which reads `ODDS_API_KEY` from GitHub Secrets — the key is held in
Vercel and GitHub, never in a local `.env`.

### P0 probe RESULT — measured 2026-09-02, 5 events, 35 credits

Run 33628093963, seven days before Week 1 kickoff.

```
market                        events   DK  PIN  DK+PIN  paired  same-line  DK players
player_pass_yds                 5/5     5    2       2       5          0          10
player_pass_tds                 5/5     5    2       2       5          2          10
player_pass_interceptions       3/5     0    0       0       0          0           0
player_rush_yds                 5/5     5    2       2       5          2          18
player_receptions               5/5     5    2       2       5          2          30
player_reception_yds            5/5     5    2       2       5          2          30
player_anytime_td               5/5     5    0       0       0          0          99
```

**Six of seven markets are usable.** DraftKings posts them on every
probed event with paired over/under quotes.

**`player_pass_interceptions` is dead for us.** DraftKings posts it on
**0 of 5** events; only BetRivers quotes it at all (3/5). This is the
`batter_home_runs` situation exactly — the key exists, the book doesn't
post it. Interceptions fall back to history. Cost is low: an
interception is −1 point, the smallest term in the offensive table.

**`player_anytime_td` is the deepest market on the board** — 99 DK
players across 5 games, roughly 20 per game, against 2 per game for
passing yards. Confirmed yes-only (`name=yes, description=<player>,
point=no`), exactly the shape MLB's paired-only rule would have
discarded. It is the most valuable projection input and it is fully
available; not copying that rule was load-bearing, not academic.

**A DK-vs-Pinnacle NFL prop detector is NOT viable at this coverage.**
Pinnacle appears on only 2 of 5 events and **0 of 5** for anytime-TD, and
same-line agreement on passing yards is 0/5. NFL props are not MLB props;
do not assume the `dk_prop_value` architecture ports over. This spec's
consumer is the DFS projection, which needs neither Pinnacle nor paired
quotes, so nothing here is blocked.

**Caveat — this is a floor, not a steady state.** The probe ran seven
days before kickoff and prop boards fill out closer to game time. The
per-game player counts should be re-measured against a live slate before
any coverage assumption is baked into the model. The pool has 719
players and the props cover on the order of 20–25 per game, so the
**history + environment fallback is the majority path, not an edge
case** — particularly for the low-salary players where DFS leverage
actually lives.

**Paired quotes gate a detector, not a projection — do not copy MLB's
rule blindly.** `ingest/mlb_prop_odds.py` rejects one-sided markets
because a DK-vs-Pinnacle *value detector* needs a paired same-line quote
to de-vig against. A DFS *projection* has no such requirement:
`player_anytime_td` is a yes-only market by nature and is the single
most valuable NFL projection input — a touchdown is 6 of the ~15 points
a typical flex player scores — and it is de-vigged **across players** in
the market, the same power method already used in
`model/soccer_first_scorer.py`. Judge each market against the consumer
that will actually use it.

## 3. Projection model

The first implementation is `model/nfl_dfs_historical.py`. It is deliberately
**historical first and prop free**. Market inputs do not get to hide a weak
football model: the historical baseline is built, versioned, and graded on its
own before props are permitted to update any component. The output is a
distribution rather than a point estimate.

### Stage 1 — per-stat expectation

For each player, the baseline uses only completed weeks strictly before the
target kickoff:

1. Resample complete historical stat lines with exponential recency weights.
   Keeping whole games preserves the real correlations between volume,
   yardage, touchdowns, and bonuses.
2. Shrink sparse samples toward comparable players selected by their prior
   production profile. Peer selection happens at the player level; selecting
   individual games close to a mean would manufacture narrow uncertainty.
3. Apply capped opponent and Vegas team-total factors whose values and source
   are stored in the projection snapshot. Missing pregame context is a visible
   fallback, never a zero.
4. Rookies or players with fewer than two eligible games receive an explicit
   `position_prior` status and zero confidence. They are not silently assigned
   a normal veteran projection.

After the baseline passes its gate, de-vigged props may update a named stat
component. A prop overlay must cite the immutable baseline run/player row,
store line and price evidence, and expose the before/after delta. The untouched
baseline remains queryable. Prices are averaged in probability space, never
in American odds.

### Stage 2 — simulation, not addition

Per player, draw complete correlated stat outcomes, apply DK scoring
**including the three yardage bonuses evaluated per draw**, and keep the
distribution:

```
proj (mean) | floor (P10) | ceiling (P90) | boom rate
```

Bonus expectation falls out of the simulation instead of being bolted
on afterward. Per-player variance comes from weekly history.

### Stage 3 — DST

DST remains unavailable in the first historical model. The existing stored DST
weeks use Yahoo scoring, while DraftKings has different points-allowed rules.
Re-labelling those rows as DK outcomes would be false precision. DST may ship
only after DK-compatible historical outcomes are derived and pass the separate
flat-constant comparison.

### Kill criteria — fixed now, before any data is examined

The initial shadow gate is graded walk-forward on the immutable 2020–2025
nflverse snapshots, one week at a time, never in-sample. The convenience
`ff_player_week_stats` table is not the validation source because its current-
roster match creates survivorship bias. Prospective 2026 grading follows once
weeks complete.

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

Minimum prospective sample before any 2026 verdict: **4 completed weeks**. No
constant is tuned on a week that has already been graded.

### Historical v1 shadow result (recorded 2026-09-02)

`artifacts/nfl_dfs_historical_backtest_2025.json` contains the immutable source
hashes, configuration, seed, cutoff rule, and position metrics for 4,523 held-
out 2025 player-weeks. The adjusted candidate did **not** beat the unadjusted
recency baseline at any position (MAE delta: QB +0.036, RB +0.020, WR +0.016,
TE +0.002; lower is better). It therefore remains shadow-only. This is not a
claim about performance versus DK Avg—the historical salary archive required
for that benchmark does not yet exist.

The next model iteration must add pre-kickoff role/opportunity features from
the roster-aware V2 pipeline and improve P10/P90 calibration. Props stay
deferred until that prop-free version passes its fixed gate.

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
| 4 | Historical model + immutable audit snapshots | ✅ **Built in shadow** — prop-free engine, run/player tables, and 2025 walk-forward artifact; promotion gate failed |
| 5a | DK scoring module | ✅ **Done** — `web/src/lib/nfl-dfs/scoring.ts`, offence/kicker/DST + captain, verified against DK's official tables |
| 5b | Role/opportunity model refinement | Add point-in-time V2 opportunity inputs, recalibrate uncertainty, and re-run the fixed gate |
| 5c | Prop market probe and overlay | Starts only after 5b passes; per-market DK/Pinnacle/same-line coverage recorded before the overlay depends on a market |
| 6 | Optimizer, both formats, both modes | Lineups are DK-legal, including the 2-team/2-game rule |
| 7 | `/dfs/nfl` UI | **Intake/pool audit visible**; projections, optimizer controls, and exposure remain gated |
| 8 | Entry-file export | Round-trips against a real DK entry file |
| 9 | Tests + real-data verification | Real slate, end to end |

Steps 5b and 5c gate 6–9.

**Status (2026-09-02):** steps 1, 3, 4 and 5a are implemented. The
historical model remains shadow-only because its first held-out run did not
beat the recency benchmark; this is the gate working as designed. Props are
intentionally deferred. Step 5b is next: add roster-aware team opportunity and
role evidence without using any target-week outcomes.
