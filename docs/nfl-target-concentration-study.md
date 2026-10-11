# Team target concentration study (`nfl-target-concentration-v1`)

Registered 2026-10-11, before any result in this study was computed. Choices
below are fixed; a change after results are seen is a new version.

## Question

Do some teams consistently funnel targets to one or two players while others
spread them, and does knowing a team's habit predict next week's target split
better than the rule the game-leader model uses today (shares in proportion to
each player's recent per-game rate)? The motivating case: Zay Flowers
questionable on Sunday night, week 5 2026.

## Data

Box scores from the frozen captures (`final-repaired-capture-20261008.json.gz`
for 2023–2025, `calibration-v1/capture-2021-2022.json.gz` for 2021–2022) and
nflverse gameday inactives (`INA`, `RES`). Regular season only. A team-game's
pool is every non-quarterback with at least one target.

## Measures

- **Concentration** of a team-game: the Herfindahl index of target shares,
  HHI = Σ share², and the top player's share. Reported for receptions too.

## H1: concentration is a team trait within a season

For each team-season with at least 12 games, the mean HHI over odd weeks
against the mean over even weeks. Pearson r across team-seasons, bootstrap
95% interval resampling team-seasons (2,000 resamples, seed 20261011).
**Passes** if the lower bound is above zero, on development (2023–2025) and
again on test (2021–2022). Also reported: the share of HHI variance between
team-seasons (intraclass correlation) and year-over-year r for the same team.

## H2: a team-specific concentration predicts next week's split

Walk forward through every team-game. Candidates are players with a target in
the team's previous six games who are not listed inactive; targets to anyone
else fall in an "other" bucket whose share is the team's other share over its
previous six games (floored at 1%). Each candidate's weight w is his per-game
target rate in games he played, shrunk toward his all-games rate by one game
(the role rule's formula). Predicted share ∝ w^k, scaled to leave the other
bucket its share.

- **M0**: k = 1 (today's rule).
- **M1**: one league k, fitted on development (grid 0.50–2.50, step 0.05).
- **M2**: a team k, fitted by maximum likelihood on the team's previous 17
  team-games, then shrunk toward the league k:
  k_team = (n·k̂ + λ·k_league)/(n + λ), with λ on {8, 17, 34, 68} chosen on
  development.

Score: multinomial log-likelihood of the observed target counts per target,
per team-game. Team-games need at least 17 prior team-games for M2 and at least
three prior games for any model. **H2 passes** if, on the test seasons, M2
beats M1 with a game-clustered bootstrap 95% interval wholly above zero.
Reported regardless: M1 against M0, which says whether today's proportional
rule should be sharpened or flattened in general.

## H3 (descriptive only, no gate): the Flowers case

Team-games where the team's previous-six-game target leader is inactive: the
share the next-highest candidate actually received minus M0's prediction,
split by the team's prior concentration tertile. Too few events to gate on.

## Windows

Development: 2023–2025. Test, one look: 2021–2022 (2021 games need the
previous six team-games, so its first weeks draw on 2021 only and are scored
once three prior games exist).

## Outcomes

- **H1 and H2 pass**: a team concentration term is justified for the role rule,
  as a new role-rule version graded like any other change.
- **H1 passes, H2 fails**: the habit is real but adds nothing beyond players'
  own recent rates, which already encode it. No model change.
- **H1 fails**: concentration is week-to-week noise. No model change.
No re-slicing by team, position or season after the result.

## Result (2026-10-11): the habit is real, and the current rule already captures it

| | Development 2023–2025 | Test 2021–2022 |
| --- | --- | --- |
| H1 split-half r (team-seasons) | 0.71 [0.59, 0.81] (96) | 0.59 [0.44, 0.71] (64) |
| Share of game-level variance between team-seasons | 19% | 10% |
| Same team, next season r | 0.49 | 0.19 |
| H2 team k minus league k, log-lik per target | +0.0001 [−0.0002, +0.0005] | −0.0002 [−0.0006, +0.0003] |

- **H1 passes on both.** Within a season, a team that concentrates targets in
  odd weeks does so in even weeks too. Across seasons the habit carries over
  only partly (r 0.49, then 0.19), so it belongs to the roster more than the
  franchise. Most concentrated, 2023–2025: NYG 2024, PHI 2023–2025, SEA 2025.
  Most spread: GB 2023, DEN 2023 and 2025, KC 2023, BUF 2025. Baltimore sits at
  the league median every season (ranks 13–20 of 32).
- **H2 fails.** The best single league exponent is exactly 1.00, which is the
  proportional rule the model uses today; team-specific exponents (fitted range
  0.92–1.09 after shrinkage) add nothing on either set.
- **H3 (descriptive)**: when a team's target leader is inactive, the next man's
  share is within ±4 points of the proportional prediction in every
  concentration tertile, with signs that differ between development and test.

Reading: concentration is a stable team trait, but it is already contained in
the players' own recent target rates. A concentrated team simply has one
player with a much larger rate. No model change. The receiving and receptions
weakness found in `nfl-game-leaders-calibration-v1` is not about the shape of
the split and must be looked for elsewhere.
