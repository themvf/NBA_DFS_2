# NFL DFS — Projection Compression Study (pre-registered 2026-09-25)

Registered and committed **before** any tier, slope, or bias was computed. Only
table structure and row counts were inspected beforehand.

## Question

Does `nfl-dfs-historical` compress projections, projecting the top of the pool
too low and the bottom too high?

Motivation: on the Thursday ATL@GB Showdown (2026-09-24), the pool-wide mean
error was near zero (+0.4) while WRs ran +4.3 and RBs +2.5, and saved lineups
beat their projections by about 25 points. Those numbers generated this
hypothesis, so that slate is part of the population, not independent
confirmation of it.

## Mechanism under test

`project_player` shrinks every player toward a position prior. If the shrinkage
is too strong for established starters, their projections are pulled down and
backups' pulled up, so realized points spread wider than projections do.

## Data

- Source: `nfl_dfs_slate_report_cards` (`nfl-dfs-slate-report-v1`): every
  player DraftKings listed on a completed slate, `actual` under DraftKings'
  convention (0 for a listed player whose completed game has no stat line).
- Weeks 1–3 of 2026, all slates graded as of registration (7 distinct slates).
- **Leakage guard:** a row is used only if its projection run's history cutoff
  `(history_cutoff_season, history_cutoff_week)` is strictly before the slate's
  `(season, week)`. A slate re-uploaded after its games can carry a run whose
  history includes those games.
- **De-duplication:** one row per `(ff_player_id, game_key)`. The same game can
  appear on several uploads and on both a Classic and a Showdown slate. Keep the
  row from the eligible projection run with the latest `as_of_at`.
- Excluded: cohort `out`; rows with no projection; `projected <= 0`.

## Unit and clustering

Player-game. Uncertainty from a bootstrap that resamples **games**
(`game_key`), since teammates and opponents in one game share an environment.
10,000 draws, seed 20260925. Weeks are too few (3) to cluster on.

## Primary test

**Population:** cohort `hist_6_plus` (six or more games of history), the
players the shrinkage mechanism is about.

**Statistic:** calibration slope β from OLS `actual = α + β · projected`.
β > 1 means realized points spread wider than projections: compression.

**Verdict:**
- **CONFIRMED** if the 95% interval for β lies entirely above 1.0.
- **REVERSED** (over-dispersed) if it lies entirely below 1.0.
- **NOT CONFIRMED** if it contains 1.0.
- **INSUFFICIENT** if the population has fewer than 300 player-games or 20
  games. No verdict is drawn then.

## Secondary (descriptive only, no verdict)

1. Mean `actual − projected` by projection tier, with game-clustered 95%
   intervals. Tiers in DraftKings points: **18+**, **12–18**, **6–12**,
   **under 6**. The tier with 18+ is the "stars" question from the motivation.
2. β within each position (QB, RB, WR, TE, DST), because a pooled slope can come
   from position mix alone (QBs project high and vary most).
3. β for all non-out cohorts together.

## What a result licenses

- CONFIRMED licenses a **separate, separately registered** change study: the
  strength of the position prior for established players, graded walk-forward.
  It does not license changing the model directly from this result.
- NOT CONFIRMED means no action at this sample; re-run when more weeks are
  graded, against the same bars.
- Secondary slices cannot rescue a primary NOT CONFIRMED. With one primary test
  and a family of descriptive cells, any single striking cell is a candidate for
  its own registration, not a finding.

## Known limitations, stated in advance

- Three weeks, 7 slates: the interval will be wide.
- Availability flags come from each upload's time. A late inactive on a
  post-kickoff upload is marked OUT and excluded, removing a zero that a
  pregame projection would have eaten. This mostly touches the lower tiers.
- `actual = 0` for listed players without a stat line (DraftKings' rule)
  includes healthy scratches. That pulls low-tier means down and can steepen β
  on its own; `hist_6_plus` limits but does not remove it.
- Noise in the projections pulls β toward and below 1 (attenuation), so a
  CONFIRMED verdict is conservative in that respect, and NOT CONFIRMED does not
  rule compression out.

## Amendment 1 (2026-09-25, before any outcome was seen)

The leakage guard above says a run is used only if its history cutoff is
"strictly before" the slate week. That assumed the cutoff names the last week
included. It does not: `_history` in `ingest/nfl_dfs_projections.py` reads
weeks strictly **before** `history_cutoff_week`, and a week-1 run built on
2026-09-03 records cutoff week 1. The first run of the study returned zero
eligible rows for exactly this reason, and no tier, slope or bias had been
computed.

The intent is unchanged (a projection's history must not include the slate's
own games). The implementation now keeps a run when
`(history_cutoff_season, history_cutoff_week) <= (season, week)` and drops it
only when the cutoff is after the slate week. Nothing else changes.

## Result (2026-09-25): NOT CONFIRMED

`python -m model.nfl_dfs_projection_tier_study`. 98 report cards, 1,293
eligible player-games across weeks 1-3 of 2026.

**Primary** (`hist_6_plus`, n=782 player-games, 27 games): calibration slope
**0.923, 95% [0.792, 1.068]**. The interval contains 1.0, and the point
estimate is below it. The model does not compress established players.

Descriptive only (no verdicts):

| Projection tier | n | actual − projected | 95% |
|---|---:|---:|---|
| 18+ | 46 | −0.2 | [−3.9, +3.8] |
| 12–18 | 116 | −3.4 | [−5.2, −1.6] |
| 6–12 | 240 | −1.9 | [−2.6, −1.0] |
| under 6 | 380 | −0.9 | [−1.2, −0.6] |

The top tier is not under-projected. The middle tiers are projected too
**high**, which is the opposite of the motivating hypothesis.

| Position | n | slope | 95% |
|---|---:|---:|---|
| QB | 110 | 1.279 | [1.021, 1.550] |
| RB | 170 | 1.037 | [0.839, 1.241] |
| WR | 281 | 0.930 | [0.725, 1.155] |
| TE | 161 | 0.846 | [0.575, 1.099] |
| DST | 54 | 0.420 | [−0.503, 1.248] |

All non-out cohorts together: slope 0.776 [0.661, 0.900], below 1, the
signature of the short-history priors projecting backups too high (already the
subject of the zero-history prior study).

**Reading.** The Thursday observation that motivated this (stars under, lineups
beating projection by ~25) was one high-scoring game, not a model-wide
compression. The QB slope is the one cell whose interval excludes 1.0; it is one
of eight descriptive cells, found rather than predicted, and does not license a
change. It is a candidate for its own registration if it persists as more weeks
are graded. Per the registration, no model change follows from this study.
