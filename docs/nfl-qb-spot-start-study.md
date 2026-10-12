# Expected-starter quarterback construction (`nfl-qb-spot-start-v1`)

Registered 2026-10-12, before any outcome in this study was computed. Choices
below are fixed; a change after results are seen is a new version.

## Problem

The DFS projection model (`nfl-dfs-historical-v5`) projects a quarterback by
replaying his own past game lines. A backup's lines are relief appearances
with a handful of attempts, so when he starts, his projected volume stays at
backup level: Tyson Bagent was projected at 63 passing yards against a
DraftKings line of 200.5 (week 4), Tyler Huntley at 145 against 180.5
(week 5). The slate's QB-transfer rule patches the case where the starter is
ruled out, but not a starter change by choice, a backup with fewer than two
games, or a transfer capped at four times his own volume.

## The construction

For a quarterback the caller names as the expected starter:

1. **Volume is a team number.** Expected pass attempts = the team's pass
   attempts per game over its prior games, recency-weighted (half-life H
   games), shrunk toward the league mean with P pseudo-games. The quarterback
   does not own the volume; the offense does.
2. **Efficiency is personal, with a spot-starter prior.** Lines are drawn
   from the quarterback's own games with at least M attempts, blended by
   `n/(n + prior_equivalent_games)` with a pool of other quarterbacks' first
   S starts (their first S games with 15+ attempts), which is what a
   quarterback with little starting history looks like.
3. Each drawn line is rescaled so its attempts equal the team volume (passing
   yards, touchdowns, interceptions and two-point passes move with it;
   rushing stays his own), the ratio clipped to [0.5, 2.0], then the usual
   environment adjustment applies.

A quarterback not named the expected starter is projected exactly as today.

## Who counts as the expected starter (production rule, not part of the grade)

The team's depth-chart QB1 if the chart is fresh and he is not out;
otherwise the shallowest listed quarterback not out; otherwise the
quarterback who led the team's last game in attempts, if not out. The study
takes the starter as given (the quarterback who led his team in attempts that
game), so it measures the construction, not the naming rule. The naming rule
is checked separately against 2026 weeks 1-5.

## Population and windows

Regular-season team-games 2016-2025 from the nflverse weekly player file in
which one quarterback had at least 15 attempts and led his team. Two cohorts:

- **Spot start**: his mean attempts over his previous six games (any team,
  previous season included) was below 15, or he had fewer than two prior games.
- **Established**: every other such game, sampled one in four.

Development: 2020-2025. Test, one look: 2016-2019.

## Fitted quantities (development only)

Grid, chosen by the spot-start cohort's DraftKings-points MAE:
H ∈ {2, 4, 8}; P ∈ {2, 4, 8}; S ∈ {4, 8, 16}; M ∈ {10, 15}.

## Metrics and gates (test seasons, 2016-2019)

Per cohort, paired against the current model (v5 as it stands, no transfer):
DraftKings points MAE (primary), passing yards MAE and bias, attempts bias.
Intervals: bootstrap resampling weeks (2,000 resamples, seed 20261012).

- **Spot-start gate**: the 95% interval of the MAE change is wholly below zero.
- **Established gate**: the interval's upper bound is at most +0.10 points,
  so the construction may not make the quarterbacks it was not built for worse.

Both must pass for the construction to go live. Reported, not gating: the
same metrics on development; the 2026 weeks 1-5 spot starts against
DraftKings' attempt and passing-yard lines; and how often the production
naming rule would have named the actual starter.

## Limits

- The study conditions on the correct starter. In production a wrong naming
  is a wrong projection for two quarterbacks; the naming check above sizes it.
- Team pass volume ignores the opponent and the game's expected script.
- Lines are rescaled proportionally; a backup's rushing stays his own.

## v1 result (2026-10-12): spot starts pass, established starters do not

Development fit (180 spot starts, 2020-2025): H = 8, P = 2, S = 16, M = 10.

| Cohort | Window | n | DK points MAE, current → new | Change, 95% interval | Gate |
| --- | --- | ---: | --- | --- | --- |
| Spot start | Development 2020-25 | 219 | 8.17 → 6.51 | −1.66 [−2.42, −0.82] | pass |
| Established | Development 2020-25 | 741 | 6.42 → 6.37 | −0.05 [−0.19, +0.10] | pass |
| Spot start | **Test 2016-19** | 131 | 7.38 → 5.79 | **−1.59 [−2.76, −0.53]** | **pass** |
| Established | **Test 2016-19** | 477 | 6.26 → 6.29 | +0.03 [−0.14, +0.20] | **fail** (limit +0.10) |

Passing-yard error on blind spot starts fell from 93 to 64 yards; the attempts
bias went from −11.2 to +3.5. On established starters the construction added
nothing and the interval does not rule out a small cost, so v1 as registered
(apply to every expected starter) is not promoted.

2026 weeks 1-5, descriptive (9 spot starts with DraftKings lines): attempts
DraftKings 28.4, current model 16.7, construction 30.1, actual 28.9; passing
yards MAE DraftKings 49, current 80, construction 53.

## v2 (registered 2026-10-12, after the v1 result): apply only where it helped

`nfl-qb-spot-start-v2` keeps the v1 construction and constants unchanged and
applies it only to an expected starter whose mean attempts over his previous
six games is below 15, or who has fewer than two prior games: the spot-start
definition above, computed from pregame history. Every other quarterback is
projected exactly as today, so the established cohort is unchanged by
construction and the spot-cohort numbers above are v2's numbers. This is a
restriction chosen after seeing v1's result, stated as such; it introduces no
new fitted quantity. The production naming rule's hit rate cannot be replayed
for 2026 weeks 1-5 (depth order was not stored with past projections); it is
recorded in every run's availability report from week 6 on.
