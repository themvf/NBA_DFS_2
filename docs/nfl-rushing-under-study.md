# Every DraftKings NFL rushing-yards under, forward (`nfl-rushing-under-v1`)

**Registered 2026-10-23 14:05 UTC**, before the first week-7 Sunday game (2026-10-25
13:30 UTC). Constants are frozen in `docs/nfl-rushing-under-study.json` and pinned by
`tests/test_nfl_rushing_under.py`; a change after this point is a new version. Graded
Monday and Tuesday by `refresh_nfl_dfs_postweek.yml` (job `grade-prop-under`) into
the append-only `nfl_rushing_under_runs`; the one verdict goes to
`nfl_rushing_under_verdicts`.

This is a measurement. Nothing is bet on it, and no result of it changes a model,
a star cap or a display by itself.

## Why

`research/nfl_prop_outcomes.py` graded the books' closing lines for 2026 weeks 1-5.
Rushing yards was the one market that did not look calibrated: overs hit 44.6%
against 49.8% implied by the price, a gap of -5.2% (game-clustered 95% CI -8.8% to
-1.6%, 453 lines in 78 games), and betting every under returned +4.6% (CI -2.3% to
+11.3%). Receptions and receiving yards were calibrated. Several cells were read at
once, so that is a discovery, not a finding. On week 6 the rushing unders that
`nfl-prop-under-v2` flagged went 28-13, which is discovery too (and a different,
model-selected population).

This study asks the plain question on games nobody has seen yet.

## Question

Does betting the under on every DraftKings rushing-yards line, at DraftKings' closing
price, return a profit over the rest of the 2026 regular season?

## Population (frozen)

- Every player DraftKings lists in `player_rush_yds` (all positions, QBs included)
  in a regular-season game of 2026 weeks 7-18 that **kicks off after `registered_at`**.
  The week-7 Thursday game (NE at CHI, 2026-10-23 00:15 UTC) kicked off before
  registration and is out. Weeks 1-6 are discovery and never count.
- The line is the **closing pregame DraftKings quote**: the latest
  `prop_odds_history` capture before kickoff whose DraftKings entry has a line, an
  over price and an under price. Captures run the day before (22:07 UTC) and 60-75
  minutes before kickoff, so the close is normally the second. A prop event is tied
  to a schedule game by both team names and a kickoff within 120 minutes.
- If DraftKings dropped a player's line before the last capture, his last DraftKings
  quote still counts; how often that happens is reported every week.
- DraftKings only. No other book is used, as a fallback or otherwise (the discovery
  study took DraftKings first and another book when it was missing).
- Independent of `nfl-prop-under-v2`: that study's flagged rushing unders are a subset
  of this population, and neither study reads the other.

## Bet and settlement (frozen)

- Under at the closing DraftKings under price, $1 flat.
- Settled from the nflverse weekly player file (`stats_player_week_2026.csv`, regular
  season), with the parent study's rules (`settle` in
  `research/nfl_prop_under_study.py`, imported unchanged): rushing yards below the
  line wins at the price, above loses $1, equal is a push returning 0.
- The player is found by the game (nflverse `game_id`) and his normalized display name
  (`normalize_name`, which drops accents, punctuation and Jr/Sr/II/III/IV). **No stat
  row is a void**, the same rule as the prop-under study: books void an inactive
  player, and the file cannot separate an inactive player from one who played without
  recording a stat. Two players with the same normalized name in one game is a void
  too. Voids are counted by reason and listed by name in every run.
- A game is graded once it is final and the nflverse file has rows for both teams.

## Metric and verdict

- **Primary:** flat-stake ROI over graded bets (pushes count as graded with 0), with a
  95% interval from a bootstrap that resamples **game weeks** (2,000 resamples, seed
  20261023).
- **Floors:** at least 500 graded bets over at least 8 game weeks.
- **One look:** the verdict is computed once, on the first grade run after every
  cohort game through week 18 is graded. Nothing earlier is a verdict.
  - **PASS** (interval wholly above zero): rushing-yards unders at DraftKings' close
    made money on unseen games. It is evidence the owner can weigh; any betting or
    display use needs its own decision, recorded in this file.
  - **NO_EDGE** (interval includes zero): the discovery result is not confirmed.
    Nothing is built on it.
  - **LOSES** (interval wholly below zero): unders lost, as the vig alone predicts
    for a calibrated market.
  - **INCONCLUSIVE** (floors not met at the final look): no conclusion; a 2027 version
    may re-ask.
- **Reported every week, gating nothing:** the over-hit rate against DraftKings'
  de-vigged implied over rate (proportional de-vig, `model/nfl_prop_implied.p_over`),
  per week and pooled with a week-clustered interval; each week's record and ROI;
  voids by reason; lines whose DraftKings quote was gone from the final capture.

## Size and power, stated before any result

Counted from DraftKings closing quotes only (`sizes`), no results read:

| week | lines | games | per game |
|---:|---:|---:|---:|
| 1 | 91 | 16 | 5.7 |
| 2 | 92 | 16 | 5.8 |
| 3 | 94 | 16 | 5.9 |
| 4 | 96 | 16 | 6.0 |
| 5 | 88 | 15 | 5.9 |
| 6 | 89 | 14 | 6.4 |

About 5.9 lines a game. The cohort has 178 games over 12 game weeks (13 left in week
7, 165 in weeks 8-18), so roughly 1,050 lines. On weeks 1-6, 6 of 550 lines would
have voided (all players with no stat row, none a name mismatch), so about 1,040
graded bets. The floor is about half of that, because week 18 can thin out when teams
rest starters and a capture can fail. Eight weeks of the twelve is the week floor.

With per-bet ROI standard deviation near 0.95 and only 12 week clusters, the interval
will be roughly 6-8 ROI points wide either side. A PASS needs a realized ROI of about
+7% or more. If the discovery's +4.6% were the true value, this test would pass only
about one time in four. So a NO_EDGE verdict does not show the effect is zero; it
shows the season was not enough to confirm it. The prior expectation, from every other
market in this repo that has been checked against closing lines, is NO_EDGE or LOSES.

## Limits

- One book, one closing capture per game, no closing-line value.
- A name DraftKings spells differently from nflverse voids every week; the void list
  in each run shows it.
- Rushing lines on one game move together (game script), so the effective sample is
  smaller than the bet count. Resampling whole weeks is conservative about that.
- The rule was suggested by weeks 1-6. It is frozen before any later game is seen.

## Commands

    python -m research.nfl_rushing_under sizes --week 6   # line counts only, no results
    python -m research.nfl_rushing_under grade [--write-db] [--out path]

With `--write-db` it appends one run row and, at the final look only, the verdict.
Tests: `tests/test_nfl_rushing_under.py`.
