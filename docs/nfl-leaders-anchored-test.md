# Market-anchored game leaders: a test beside our own model (`nfl-leaders-anchored-test-v1`)

Registered 2026-10-17 19:00 UTC, before any line for 2026 week 6's Sunday games
was captured. Constants in `docs/nfl-leaders-anchored-test.json`; model in
`model/nfl_game_leaders_anchored.py` (`nfl-leaders-market-anchored-v1`); runner
and grader in `research/nfl_game_leaders_anchored.py`.

## What it is

A third number on the game-leaders page, next to our own leader share and the
DK-implied share. It does not change the game-leaders model, which stays
market-free and pinned for `nfl-prop-under-v1`. It takes that model's simulated
games and reshapes each listed player's rushing yards, receiving yards and
receptions so that the share of games above every DraftKings line, the main
line and each rung of the alternate ladder, equals DraftKings' de-vigged
probability. Total yards is rushing plus receiving after reshaping, then
reshaped again to DraftKings' rush-plus-receiving line when one exists. The
reshaping is monotone for each player, so the simulation's pattern of
teammates competing for carries and targets is kept. Players DraftKings does
not list keep our draws. No temperature is applied.

The existing DK-implied share shifts each player by one line only and has no
total-yards version; this one uses the full ladder and covers all four markets.

## Why

Our model's volume runs low for stars and true lead backs (Smith-Njigba,
Tuten, Collins), and DraftKings' leader picks beat ours on the games graded so
far (2 to 8 where we disagreed). If the market's player lines carry the
information our model lacks, a leader forecast built on them should score
better than ours.

## Question and rule (frozen)

For each market, on games kicking off after registration through week 18:

- **Rows:** for each game, the latest pricing run whose `computed_at` and
  `prop_captured_at` are before kickoff; `origin = 'prospective'`, version
  `nfl-leaders-market-anchored-v1`.
- **Score:** log loss of the actual leader, ties split, unlisted leaders
  credited to that team's unnamed slot, probabilities floored at 0.0001. Actual
  stats from the nflverse weekly player file; a game waits until both teams
  are in it.
- **Primary:** mean over games of (anchored log loss minus our log loss), our
  share being the published v4 share re-created from the same draws.
- **Interval:** 95%, bootstrap over games (2,000 resamples, seed 20261018).
- **Floor:** 60 graded games per market. **Verdict once per market:** PASS if
  the interval's upper bound is below zero, FAIL otherwise. Before the floor,
  results are shown as descriptive.
- **Reported, not gating:** the DK-implied share's log loss on the same games,
  and how many games each version won.

## What a pass licenses

A pass in a market licenses showing the anchored share as the page's headline
number for that market, with ours beside it. It does not change the
game-leaders model or anything the prop study depends on.

## Discovery

The 15 games priced before registration (week 5 and SEA at DEN) are stored
with `origin = 'discovery'`, re-created with the current model. They are
reported separately and never count toward a verdict.

## Limits

- Anchoring to DraftKings' lines makes this forecast partly DraftKings' own
  view of each player. A pass says those lines plus our joint simulation
  forecast leaders better than our model alone, not that either beats
  DraftKings' own leader prices, which our odds feed does not carry.
- One capture per game, about 90 minutes before kickoff.
- Below DraftKings' lowest rung a simulated zero stays zero, so injury and
  game-script zeros from our model survive unless the ladder rules them out.
