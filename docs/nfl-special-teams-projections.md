# NFL DFS special teams projections

## What the candidate does

`nfl-special-teams-pregame-v1` freezes a mean, P10, median, P90, and boom
probability for every eligible DST and kicker in the ordinary historical
projection run. It uses the same pre-target-week whole-game draw and DraftKings
point scale as the saved historical model. The normal projection columns remain
unchanged; the candidate is stored in `feature_snapshot.special_teams_candidate`
and named in the run manifest. A missing total, opponent, or exact scoring
component produces an explicit unavailable reason and null candidate values.

- **DST (Classic and Showdown):** preserve exact scored special-teams events,
  rescore the nonlinear points-allowed tier after a shift by the opponent's
  implied total, and adjust sack/takeaway points by that offense's prior
  games against DSTs. Opponent event rates are shrunk by 16 equivalent games
  and bounded to 0.75–1.25 times league average.
- **Kicker (Showdown):** preserve the historical field-goal distance mix and
  adjust PAT points by half the bounded ratio of the team's implied total to
  22.5. This does not infer field-goal tries from points or assign a starting
  role from a depth rank.

Our projections use a complete saved DST or kicker candidate
automatically. If a slate predates the candidate or required data is missing,
the player keeps the historical baseline and the build notes name the fallback
and reason. The builder shows coverage without another user choice. The
optimizer consumes the candidate's saved P10/P90 and boom on the same point
scale as the offensive historical objective, records the method on lineup
slots, and includes the frozen candidate in the optimizer audit. Refresh
projections to add game context to an older saved slate.

## Showdown lineup coherence

Both Classic and Showdown permit a small opposing offensive bring-back with a
DST. They reject a DST with four or more opposing QB/RB/WR/TE players. Showdown
also rejects an opposing offensive Captain plus two more offensive teammates.
The same rules block export of saved lineups through the shared lineup check
and pre-export QA. This is a lineup construction rule; displayed P90 values remain
individual ceilings rather than a joint lineup forecast.

Comparison ownership must declare `Format` as `Classic` or `Showdown`, and the
declared format must match the salary slate before any rows are saved. Older
LineStar ownership imports without matching format evidence are ignored by new
builds; their projections remain available for comparison.

The main Build panel now lists ordinary projection sources and links to Model
Lab for research. It no longer offers experimental defensive modes, the air
matchup rule, or uncalibrated ownership leverage as build choices. Earlier
saved research lineups remain reviewable; reopening their form starts a new
build from Our projections with those unvalidated inputs turned off.

## Diagnostic evidence and limits

Run `python -m research.nfl_special_teams_diagnostic --season 2025 --draws 300`
to reproduce the read-only comparison. On exact scored regular-season player
games with archived quoted lines, the current candidate compared with historical
v5 as follows:

| Season | Position | Paired games | Mean absolute error, baseline → candidate | Central 80% interval score, baseline → candidate | Boom Brier, baseline → candidate |
| --- | --- | ---: | ---: | ---: | ---: |
| 2024 | DST | 544 | 4.3661 → 4.2351 | 20.5379 → 19.5409 | 0.080690 → 0.077339 |
| 2024 | K | 388 | 3.8434 → 3.8242 | 18.5271 → 18.5944 | 0.105717 → 0.105846 |
| 2025 | DST | 544 | 4.4785 → 4.3438 | 22.1121 → 21.1281 | 0.080267 → 0.076200 |
| 2025 | K | 471 | 3.9467 → 3.9391 | 18.1926 → 18.1769 | 0.117224 → 0.117138 |

These seasons were inspected while designing the candidate. The source holds
archived quotes, but their original pre-lock availability is unverified. The
table is a retrospective mechanics screen, not an untouched evaluation or a
promotion verdict. Kicker interval and boom results worsened slightly in 2024;
the kicker adjustment is intentionally small. The registered coherent scenario
study remains separate. Its forward 8-week, 200 paired player games per position,
50 NFL games, and untouched Showdown/Classic lineup gates cannot be declared
passed from this retrospective screen. The automatic selection is a product
decision; the forward study still determines whether the adjustment improves
outcomes and whether its formula should be revised.

## Availability

Kicker role identity still needs game-week Sleeper and FantasyPros evidence,
the week-matched FantasyPros injury observation, and an official inactive list
when published. A depth rank alone does not establish attempts or game-day
availability. The candidate models a kicker's scoring conditional on playing;
it does not certify which player will kick.

## Regressions

`tests/test_nfl_special_teams_projection.py` checks exact DK scoring, cutoff,
opponent and total direction, distribution freezing, and unavailable inputs.
`web/scripts/test-nfl-special-teams-projection.ts` checks the saved payload
reader, Classic/Showdown objective use, and hard stops on missing data.
