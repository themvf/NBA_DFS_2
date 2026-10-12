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
