# NFL player-prop unders flagged by the game-leaders model (`nfl-prop-under-v1`)

Registered 2026-10-17 16:30 UTC, before any line for 2026 week 6's Sunday games
was captured. Constants are frozen in `docs/nfl-prop-under-study.json` and in
`research/nfl_prop_under_study.py`; a change after this point is a new version.

## Discovery (cannot confirm anything)

Every priced game stores, for each DraftKings player line, DK's de-vigged
probability of the over (`fair_over`) and the game-leaders simulation's
probability of the over (`model_p_over`), in `nfl_game_leader_market`. On the
15 games priced before registration (2026 week 5 and SEA at DEN, week 6),
424 settled DK lines, inactive players voided:

| rule | bets | record | ROI at DK's price |
| --- | ---: | --- | ---: |
| Under, model P(over) at least 10 points below DK's | 201 | 119-82 | +11.9% |
| Under, model does not say so | 223 | 107-116 | -9.6% |
| Every under | 424 | 226-198 | +0.6% |

Four thresholds (5, 10, 15, 20 points) were looked at before 10 was chosen,
and 13 of the 15 forecasts were made by `nfl-game-leaders-v3`, not the v4
model this study grades. Bets within a game are correlated, so the effective
sample is far smaller than 201. None of these games count below.

## Hypothesis

**H.** Taking the DraftKings under on a player rushing-yards, receiving-yards
or receptions line, whenever the game-leaders model's probability of the over
is at least 10 percentage points below DraftKings' de-vigged probability,
returns a positive flat-stake ROI at DraftKings' own price.

## Population (frozen)

- Rows of `nfl_game_leader_market` with `version = 'nfl-leaders-market-implied-v1'`,
  `book = 'draftkings'`, a line, both prices, `fair_over` and `model_p_over`;
  metric `rushing_yards`, `receiving_yards` or `receptions`; a named player
  (not an `OTHER:` slot).
- For each (game, metric, player), the latest row whose `computed_at` and
  `prop_captured_at` are both before the game's kickoff, and whose
  `prop_captured_at` is at or after the registration time. That is the price
  and forecast available to bet before kickoff.
- 2026 regular season, games kicking off after registration, through week 18.
- The forecast is the published game-leaders model at pricing time. The model
  files it depends on are pinned by hash in the JSON; a pin change is recorded
  as an amendment with its date, and only rows priced before the first model
  amendment belong to v1.

## Bet and settlement

- Selection: **under only**, when `model_p_over - fair_over <= -0.10`.
- Price: DraftKings' under price on that row, $1 flat stake.
- Result: the player's stat from the nflverse weekly player file, matched on
  GSIS id and week. Over the line loses, under wins, equal pushes (0).
- **Void** when the player has no stat row for that game. DraftKings voids a
  player who does not play; we cannot tell "inactive" from "active with no
  touch" in that file, so an active player with zero catches is voided rather
  than graded as an under win. This biases the result against the hypothesis.
- A game is graded only when it is final and the nflverse file has rows for
  both teams that week; otherwise it waits.

## Metric, floors and verdict

- **Primary:** flat-stake ROI on graded bets (pushes count as 0 return in the
  denominator; voids are excluded), with a 95% interval from a bootstrap that
  resamples **game weeks** (2,000 resamples, seed 20261017).
- **Floors, all required:** at least 300 graded bets and at least 10 distinct
  game weeks.
- **Verdict, once:** the first post-week run that meets both floors writes the
  verdict and it is never rewritten. **Pass** if the interval's lower bound is
  above zero; **Fail** otherwise. No early stop for success.
- **Reported every week, gating nothing:** record and ROI so far (labelled
  descriptive until the floors are met), the two controls (every under on the
  same lines, and unders the model does not flag), ROI by market, bets per
  game (largest game share), and the void count.

## What a pass licenses

A pass licenses showing these unders on the site as a tracked signal and a
live period at small stakes. It does not license changing the model, the
threshold or the markets; each of those is a new version graded on new games.
A fail ends this line of work: the model's low lean on player lines is noise.

## Limits

- One book, one capture per game (about 90 minutes before kickoff); no
  closing-line value is measured, because no later capture exists.
- The model leans under on about two thirds of lines. If the edge is a general
  under bias in DK's player lines, the every-under control will show it, and
  the study is then about DK's lines, not about our model.
- Execution is assumed at the captured price; lines can move between capture
  and a real bet.

## Amendment 1 and `nfl-prop-under-v2` (2026-10-18)

The game-leaders role rule changed before any v1 bet was graded
(`model/nfl_game_leaders_roles.py`, `nfl-game-leaders-roles-v2`): a player the
team's five-game window never saw touch the ball now keeps his prior-season
rate only at a role-giving depth rank (QB 1, RB 1-2, WR/TE 1-3) and never when
Sleeper lists him NA or inactive. Under v1 Josh Jacobs carried 34% of Green
Bay's carries while away from the team with no 2026 game, and across the week-6
Sunday slate a median 14% of each team's carries and 17% of its targets went to
players with zero 2026 touches (PHI 59%, GB 37%, CLE 36%, TEN 35%, KC 32%).
Measured on 2024-25 active rosters, such players touched the ball in the next
game 32% of the time for a mean 1.7% of team touches. The owner chose to amend
and ship before the 4 PM ET games rather than leave the page wrong until the
verdict.

- `docs/nfl-prop-under-study.json` carries the dated amendment with the new
  pin. Rows priced from **2026-10-18 19:45 UTC** leave v1; v1 therefore holds
  the week-6 games priced before that time (the London and 1 PM ET games, plus
  any 4 PM game not republished in time) and can never reach its floors. It is
  reported as descriptive, not failed.
- `docs/nfl-prop-under-study-v2.json` registers `nfl-prop-under-v2` at that
  same time with identical markets, threshold, book, side, floors and
  bootstrap, pinned to the files as shipped. The grader takes
  `--registration`; the post-week workflow grades both. Neither cohort pools
  with the other.
