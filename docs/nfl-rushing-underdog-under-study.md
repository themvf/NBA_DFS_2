# Rushing-yards unders on underdogs of 3+ (`nfl-rushing-underdog-under-v1`)

**Registered 2026-11-15 05:30 UTC**, before the first week-10 Sunday game (14:30 UTC).
Constants are frozen in `docs/nfl-rushing-underdog-under-study.json` and pinned by
`tests/test_nfl_rushing_underdog_under.py`; a change is a new version. Graded Monday
and Tuesday by `refresh_nfl_dfs_postweek.yml` into the append-only
`nfl_rushing_under_runs`; the one verdict goes to `nfl_rushing_under_verdicts`.

Measurement only. Nothing is bet on it, and no result changes a model, a star cap or
a display by itself.

## Where it came from (cannot confirm anything)

`docs/nfl-rushing-multibook-study.md`. On six books' archived lines, the under on
every player whose team was an underdog of 3+ points, taken at the US book with the
highest line: 2023 +7.1%, 2024 +6.9% (1,032 bets), 2025 +6.0% (506), 2026 weeks 1-10
+8.1% (309). The 2025-2026 interval is +0.4% to +12.5%. It was not the rule frozen
before those two seasons were scored (running backs only was, and that came in at
+3.9%, interval through zero), so every number here is discovery. The reading is game
script: a team that trails stops running. Favourites' unders were about flat.

## Population (frozen)

- 2026 regular-season games that kick off after the registration, weeks 10-18.
- Every player in `player_rush_yds` (any position) quoted by at least two of
  DraftKings, FanDuel, BetMGM, Caesars and Fanatics in the latest capture before
  kickoff, whose team is an underdog of 3.0 points or more by
  `nfl_season_games.market_spread_line` captured before kickoff.
- The player's team is the team on his nflverse stat row for that game. A player with
  no stat row has no team and is not bet (the parent studies void him).

## Bet and settlement (frozen)

Under at the listed book with the highest line, ties to the best under price, $1 flat.
Below the line wins at the price, above loses $1, equal returns 0.

## Metric and verdict

- **Primary:** flat-stake ROI, 95% bootstrap interval resampling game weeks (2,000
  resamples, seed 20261115).
- **Floors:** 200 graded bets over 7 game weeks.
- **One look**, on the first grade run after every cohort game through week 18 is
  graded: PASS (interval wholly above zero), NO_EDGE, LOSES, or INCONCLUSIVE (floors
  not met). A PASS is evidence for the owner to weigh; any betting or display use is
  a separate decision recorded here.

## Power, stated before any result

About 31 qualifying bets a week, so roughly 270 by week 18. With per-bet ROI standard
deviation near 0.95 the interval will be about 8 to 10 points either side. An edge of
+7% would pass less than half the time; +3% would almost always read NO_EDGE. A
NO_EDGE here does not close the question; a 2027 version with a full season would.

## Limits

- The quote is a snapshot about 75 minutes before kickoff. It may not be bettable at
  that book at that moment, and books limit prop accounts.
- The spread is the stored market line, not the price at the moment of the prop capture.
- Bets in one game move together; the game-week bootstrap covers that, the count does not.

## Commands

    python -m research.nfl_rushing_underdog_under grade --out artifacts/nfl_rushing_underdog_under_latest.json
    python -m research.nfl_rushing_underdog_under grade --write-db
