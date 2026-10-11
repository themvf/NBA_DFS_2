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
