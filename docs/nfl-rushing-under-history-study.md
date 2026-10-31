# Every DraftKings NFL rushing-yards under, 2023-2025 (`nfl-rushing-under-history-v1`)

**Registered 2026-10-31 21:10 UTC, before any 2023-2025 rushing prop line was
pulled.** No prop line from those seasons existed in this repo's database when
this was frozen. Constants are in `docs/nfl-rushing-under-history-study.json`,
pinned by `tests/test_nfl_rushing_under_history.py`; a change is a new version.
Graded once by `research/nfl_rushing_under_history.py` into the same append-only
tables as the forward study (`nfl_rushing_under_runs` /
`nfl_rushing_under_verdicts`, keyed by study).

Measurement only. Nothing is bet on it, and no result of it changes a model,
a star cap or a display by itself.

## Why

The 2026 discovery (weeks 1-6, `research/nfl_prop_outcomes.py` and the forward
study's own counts) had DraftKings rushing-yards overs hitting 43.2% against
49.8% implied, every under +7.1%. The forward test, `nfl-rushing-under-v1`
(`docs/nfl-rushing-under-study.md`), cannot confirm an edge that size before
about week 15, and cannot confirm a +4.6% edge in 2026 at all. Three earlier
seasons have never been seen here and are a clean out-of-sample test of whether
the bias is structural. They cannot say whether it still holds in 2026; the
forward study keeps running for that and is not changed by this one.

## Population (frozen)

- Every player DraftKings lists in `player_rush_yds` (all positions) in a
  regular-season game, weeks 1-18 of 2023, 2024 and 2025.
- The line is the Odds API archive snapshot at or before 90 minutes before
  kickoff, pulled by `ingest/nfl_prop_backfill.py` (the archive returns the
  latest snapshot at or before the requested time). This is a little earlier
  than the forward study's live close (60-75 minutes before kickoff); both are
  pregame.
- DraftKings only, with a line and both prices. A game whose archived event
  cannot be matched to the schedule (both teams, kickoff within 15 minutes) is
  skipped and listed in the pull log.

## Bet and settlement (frozen, identical to the forward study)

- Under at DraftKings' under price, $1 flat.
- Settled from the nflverse weekly player file for the line's season with the
  parent rules (`settle` in `research/nfl_prop_under_study.py`): below the line
  wins at the price, above loses $1, equal is a push returning 0. Found by
  nflverse `game_id` plus normalized display name; no stat row is a void, as is
  two players with the same normalized name in one game.

## Metric and verdict

- **Primary:** flat-stake ROI with a 95% bootstrap interval resampling
  **(season, week)** game weeks, 2,000 resamples, seed 20261031.
- **Floors:** at least 2,000 graded bets over at least 36 game weeks. Expected
  from the 2026 counts (about 5.9 DraftKings lines a game, 816 games) is about
  4,800 lines. The floor allows for archive gaps; if the archive has few or no
  DraftKings rushing lines, the verdict is INCONCLUSIVE, not a failure.
- **Stability:** a PASS also needs a positive ROI in at least 2 of the 3
  seasons, so one season cannot carry the result.
- **One look:** computed on the first grade run after the pull for all three
  seasons has finished.
  - **PASS**: the bias is structural in past seasons. Evidence the owner can
    weigh with the forward study; any betting or display use needs its own
    decision, recorded here.
  - **NO_EDGE**: not confirmed. Nothing is built on it.
  - **LOSES**: interval wholly below zero, as the vig alone predicts for a
    calibrated market.
  - **INCONCLUSIVE**: floors not met.
- **Secondary, licenses nothing on its own:** ROI on lines under 60 yards minus
  ROI on lines of 60 or more, with the same bootstrap. The 2026 discovery read
  showed the gap in the smaller lines; this was seen after the fact, so it is
  tested here only as a reported comparison.
- **Reported, gating nothing:** ROI and over-hit against the de-vigged implied
  rate per season and per week, voids by reason.

## Power, stated before any result

At about 4,800 graded bets in 54 game weeks and per-bet ROI standard deviation
near 0.95, the interval should be about plus or minus 3 to 3.5 ROI points. An
edge the size of the 2026 discovery (+7%) would clear it easily; +4.6% would
pass about three times in four; +2% would usually read NO_EDGE.

## Cost

One market, 10 credits per game from the archive plus 1 per kickoff time: about
8,160 + ~370 = ~8,500 credits for the three seasons. The pull runs season by
season with a credit cap, and stops early if 2023 week 1 shows no DraftKings
rushing lines.

## Commands

    python -m ingest.nfl_prop_backfill --season 2023 --weeks 1-18 --markets player_rush_yds          # dry run
    python -m ingest.nfl_prop_backfill --season 2023 --weeks 1-18 --markets player_rush_yds --apply --max-credits 3000
    python -m research.nfl_rushing_under_history grade --final --write-db --out artifacts/nfl_rushing_under_history.json   # after all three pulls
