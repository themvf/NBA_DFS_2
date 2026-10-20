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

`nfl-qb-spot-start-v2` (`model/nfl_qb_starter.py`, an overlay applied by the
production writer after the base projection; the base model file and version
are untouched) keeps the v1 construction and constants unchanged and
applies it only to an expected starter whose mean attempts over his previous
six games is below 15, or who has fewer than two prior games: the spot-start
definition above, computed from pregame history. Every other quarterback is
projected exactly as today, so the established cohort is unchanged by
construction and the spot-cohort numbers above are v2's numbers. This is a
restriction chosen after seeing v1's result, stated as such; it introduces no
new fitted quantity. The production naming rule's hit rate cannot be replayed
for 2026 weeks 1-5 (depth order was not stored with past projections); it is
recorded in every run's availability report from week 6 on.

## v3 (registered 2026-10-18, before any v3 projection is computed): fix the eligibility test

`nfl-qb-spot-start-v3`. Draft registration; nothing is shipped and v2 stays
in production until the gates below are graded.

### Why

v2's test of "thin history" is the mean of a quarterback's attempts over his
previous six games. A backup in his second or third start mixes relief
appearances with starts, so a couple of starts lift the mean over 15 and the
construction switches off. Meanwhile the base model keeps drawing his relief
lines, so his projected volume stays near backup level. Week 6, 2026 (projection
run `33bb4d8d`):

| Quarterback | Previous attempts (oldest first) | Mean | v2 eligible | Stored attempts | Construction, if applied | Actual |
| --- | --- | ---: | --- | ---: | ---: | ---: |
| Tyler Huntley (BAL) | 22, 0, 10, 20, 9, 32 | 15.5 | no | 19.2 | 26.0 | 31 |
| Jalon Daniels (TB) | 3, 27, 25 | 18.3 | no | 21.6 | 31.7 | 43 |
| Tanner McKee (PHI) | 4, 41, 0, 3, 0, 40 | 14.7 | yes | 30.8 | 30.5 | 45 |

McKee was handled as designed. Philadelphia trailed all game, and the team's
volume ignores game script (a known limit above). The two misses are the
eligibility test.

### Seen before registering (disclosure)

- The 2026 week 6 results for these three quarterbacks, and the construction's
  output for them (the table above).
- One descriptive count over 2023-2026, which includes development seasons
  2023-2025. It covers 95 starts (20+ attempts) by quarterbacks whose
  trailing six-game mean was 15-22. They averaged 31.6 attempts against a
  trailing mean of 19.1. This counted attempts only; no v3 projection or error
  was computed.
- **The test window is not blind for v3.** 2016-2019 was graded once for v1,
  and v2's restriction was read off that result. v1's established gate failed
  there: +0.03 [−0.14, +0.20] when the construction was applied to every
  established starter. v3's newly eligible games are a subset of that
  established cohort. Grading v3 on 2016-2019, as registered here, is a second
  look at the window, and a pass is weaker evidence than v1's spot result. The
  forward 2026 check below is the only untouched evidence. 2012-2015 has not
  been used by this study and would be the clean window for any confirmation.

### The rule

A **starter game** is a prior game with at least 15 pass attempts, the same
threshold `spot_start_prior_rows` uses. Over his previous min(6, n) games:

```
eligible_v3 = eligible_v2  OR  (starter games < K)
gap cohort  = eligible_v3 AND NOT eligible_v2
```

K is fitted on development from {3, 4, 5}. Nothing else changes: the
construction, its constants (H = 8, P = 2, S = 16, M = 10), and the naming rule
are v2's. Because v3 is a superset of v2, v2's spot cohort is projected
identically. That equality is asserted by the runner, not graded.

Examples:

| Case | Starter games | Eligible at K |
| --- | --- | --- |
| Huntley | 3 of 6 | 4 and 5 |
| Daniels | 2 of 3 | 3, 4 and 5 |
| An established starter who left one game early | 5 of 6 | none in the grid |

### Population

The qualifying team-games are v1's: the team's attempts leader had at least 15.
One change: the established cohort is **not** sampled one in four. The gap
cohort lives inside it, so sampling would quarter the sample.

### Fit (development 2020-2025 only)

K is chosen by DraftKings-points MAE over every qualifying game (spot plus
every established game), v3 against the current model. A whole-population
objective penalizes a K that pulls in starters the construction hurts. Ties go
to the smallest K.

### Gates (2016-2019, one look for v3)

The comparison is v3 against v2 on the gap cohort, computed directly rather
than assumed equal to the current model; v2's own test report shows a +0.01
residual on established games. Bootstrap resamples weeks: 2,000 resamples,
seed 20261018.

1. **Gap gate:** the 95% interval of the DraftKings-points MAE change on the
   gap cohort is wholly below zero.
2. **Calibration guard:** the gap cohort's mean attempts bias under v3 is
   within ±5.0. This stops an overshoot that MAE could hide; v1 overshot blind
   spot starts by +3.5.
3. **Sample floor:** at least 60 gap-cohort test games. Below the floor the
   verdict is "insufficient" and v2 stays.

All three must pass. Development metrics are reported but not gating, because
K was fitted there.

### Reported, not gating

- Per-season test breakdown, passing-yards MAE and bias, and the gap cohort's
  size at each K on development.
- Forward 2026, from week 7: every game where v3's projection differs from
  v2's, computed as an offline shadow (production stays v2). Each is shown
  against DraftKings' attempts and passing-yard lines and the actual result,
  until at least 20 games exist.
- The three week 6 quarterbacks re-projected under the fitted K. These are
  illustrative only, since they motivated the change.

### Kill

If any gate fails, v2 stays. Not allowed after a failure:

- trying a K outside the grid;
- changing the 15-attempt starter threshold;
- changing the six-game window;
- re-slicing the gap cohort.

Another eligibility idea would be a new registration graded on a window this
study has not used (2012-2015, or 2026 forward).

### If it passes (not part of this registration's work)

- The overlay version becomes `nfl-qb-spot-start-v3`, and
  `thin_starting_history` gains the starter-game rule with the fitted K.
- `production_config()` changes the `qb_starter_mode` value (for example
  `"v3"`). This makes the configuration identity change with the output. Cohorts
  that pin `baseline_config_hash` will see a new configuration at their next
  study boundary, as they did when v2 shipped.
- Tests pin K and the rule.

### Runner

`research/nfl_qb_spot_start_study.py` gains a v3 path:

- `STUDY = nfl-qb-spot-start-v3` and `BOOT_SEED = 20261018`;
- no established sampling;
- the K grid applied as an eligibility override passed to
  `project_expected_starter` (it calls `thin_starting_history` itself);
- separate artifacts (`*-v3.jsonl`, `*-v3.json`), with the existing refusal to
  overwrite.

The nflverse weekly files for 2014-2025 must be re-cached in
`data/nflverse_cache`; this checkout holds one file.

### v3 development fit (2026-10-20, frozen before the 2016-2019 look)

Inputs: the nflverse weekly player files for 2014-2025, downloaded
2026-10-20 (digests in `artifacts/nfl-qb-spot-start/nflverse-weekly-files-v3.json`).
The records (`development-v3.jsonl`, 3,183 qualifying games 2020-2025)
reproduce v2's development spot cohort exactly (219 games, 8.17 -> 6.51), so
file revisions since v1 changed none of those games. The runner's check that
the forced construction equals v2 wherever v2 applies never fired.

Fit (`fit-v3.json`), DraftKings-points MAE over every qualifying game:

| | Current | v2 | v3, K=3 | v3, K=4 | v3, K=5 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Population MAE | 6.538 | 6.424 | 6.411 | 6.377 | **6.336** |

**K = 5 is fixed for the test**: a quarterback is newly eligible when at
most 4 of his previous six games had 15+ attempts. K = 5 is the edge of the
registered grid. Larger K is not allowed (see Kill). Its next step would
reach starters with one short game, the cohort where v1 failed its
established gate.

Gap cohort on development (reported, not gating; K was fitted here):

| K | Games | Weeks | DK MAE, v2 -> v3 | Change, 95% interval | Attempts bias, v2 -> v3 |
| ---: | ---: | ---: | --- | --- | --- |
| 3 | 63 | 48 | 6.87 -> 6.21 | −0.66 [−1.80, +0.39] | −7.2 -> +1.7 |
| 4 | 154 | 82 | 6.99 -> 6.01 | −0.98 [−1.69, −0.27] | −8.0 -> +1.5 |
| 5 | 324 | 105 | 6.71 -> 5.84 | −0.87 [−1.27, −0.41] | −7.3 -> +1.3 |

Passing-yards MAE on the K = 5 gap cohort falls from 74 to 57. The test grade
(`run-v3 --seasons 2016-2019`, then `grade-v3` with this fit) has not been run.
