# NBA points ladder v2: power de-vig for one-sided rungs (pre-registration)

Registered 2026-10-26, before the candidate reader was implemented or run on any
data. The commit that adds this file is the freeze; anything that changes the
hypothesis, metric, population, floors or decision rule after it is a new
registration with a new version, never an edit to this one.

## What prompted it (discovery only, cannot confirm anything)

The nightly grade (`model/nba_points_calibration.py`, `nba-points-grade-v1`)
scores each game's last pre-tip capture, read by `nba-points-ladder-v1`, against
the ESPN box score. Over the first six graded nights, 2026-10-20 to 2026-10-25
(572 graded player-games), every tail rung was stated above what happened:

| rung | n | books said | happened | Brier |
|---|---:|---:|---:|---:|
| over the line | 572 | 50.1% | 50.3% | 0.2500 |
| 20+ | 555 | 27.9% | 26.0% | 0.1331 |
| 25+ | 450 | 18.5% | 13.6% | 0.0922 |
| 30+ | 297 | 13.4% | 9.8% | 0.0795 |
| 35+ | 206 | 8.8% | 5.8% | 0.0509 |
| 40+ | 117 | 6.2% | 4.3% | 0.0396 |

Floor/ceiling coverage on 257 rows with both ends: 10.5% below the floor
(target 10%), 6.6% above the ceiling (target 10%).

The main line is calibrated and the tails are not, which points at how the
one-sided rungs are de-vigged rather than at the market. v1 divides an
over-only rung's implied chance by its book's main-line overround (proportional
de-vig). Books shade long prices harder than the main line, so proportional
de-vig leaves margin in the top rungs; the v1 ladder code already says high-end
chances "read slightly high". Six nights and a pattern noticed by eye are a
hypothesis, not evidence. These nights are excluded from the test below.

## The change being tested (`nba-points-ladder-v2`)

Exactly one thing differs from v1: how a rung quoted on one side only is
de-vigged.

- For each book with a valid two-sided main line, solve the power exponent
  `k` with `implied(over)^k + implied(under)^k = 1` (bisection on [0.25, 4],
  tolerance 1e-10). At -110/-110, `k` is about 1.072.
- An over-only rung becomes `implied(over)^k`; an under-only rung becomes
  `1 - implied(under)^k`, with that book's `k`.
- If `k` cannot be solved for a book, its one-sided rungs are skipped and
  counted in `skippedRungs`, as v1 does for a book with no main line.

Unchanged: the main line itself and two-sided rungs stay proportional (so
P(over the line) is identical in v1 and v2 by construction), the per-point
median across books, the non-increasing fit, the no-extrapolation rule,
`MIN_CURVE_POINTS`, the 20/25/30/35/40 thresholds and the floor/ceiling
quantiles.

Why it should help: with `k > 1`, `p^k` shrinks a small `p` proportionally more
than a large one (at `k = 1.072`, a 10% rung becomes 8.5% where v1 gives 9.5%),
which is the shape of a favourite-longshot margin.

## Population

- Every NBA game with `game_date >= 2026-10-27` (Eastern) whose last pre-tip
  capture of `player_points` exists in `prop_odds_history`. Discovery nights
  2026-10-20 to 2026-10-26 are excluded entirely, including from any pooled
  secondary.
- Both readers are recomputed from the same stored capture rows (main and
  `player_points_alternate`, last capture before `commence_time`) and graded
  against the same `nba_box_score_players` row, with the v1 grade's matching
  and status rules (`graded` only; `dnp`, `unmatched`, `ambiguous`,
  `no_box_score` and `push` are excluded). Short nights (under 15 minutes) stay
  in, as books settle them.
- Integrity check before any metric: the recomputed v1 reading must equal the
  stored `nba_points_calibration` v1 row for every graded player-game (to
  1e-9). A mismatch stops the evaluation; it is a reader bug, not a result.

## Primary metric and decision

- Unit: a (graded player-game, rung) pair for rungs 25+, 30+, 35+ and 40+
  where BOTH readers produce a value.
- Metric: the mean over pairs of `Brier_v2 - Brier_v1`, where
  `Brier = (stated - hit)^2`.
- Uncertainty: 95% interval from a bootstrap that resamples whole nights
  (10,000 resamples, seed 20261026), because players on one night share a
  slate.
- **PASS** if the interval's upper bound is below 0. **FAIL** otherwise.
- Computed once, when both floors are met, and frozen. No interim look for
  efficacy.

## Floors (both required)

- At least 30 prospective nights.
- At least 1,000 graded player-games carrying a 35+ value under both readers.

At the discovery rate (about 34 such rows a night) the floors meet around
early December. If they are not met by 2027-04-12 the result is INCONCLUSIVE
and v1 stays.

## Reported always, gating nothing

- Per rung: mean stated vs hit rate for both readers, Brier for both.
- Floor and ceiling coverage for both readers.
- Skill against the line-only baseline (the panel's leave-one-night-out
  reference) for both readers.
- Count of one-sided rungs, skipped rungs and the distribution of `k` by book.

## What each outcome does

- **PASS**: v2 replaces v1 in both the TypeScript page reader and the Python
  port, the shared fixture is regenerated, the ladder version becomes
  `nba-points-ladder-v2` and the grade version `nba-points-grade-v2`. v1 rows
  stay as they are. Top-three and trio work builds on v2.
- **FAIL**: v1 stays. No other de-vig variant (Shin, additive, a different
  exponent source) is tried on the same nights; a new idea needs its own
  registration and its own prospective window.
- **INCONCLUSIVE**: v1 stays; the question can be re-registered next season.

## Non-negotiables

- No re-slicing to rescue a FAIL (by rung, line band, book, or capture age).
- No change to the thresholds, the population or the bootstrap after this
  commit.
- The six discovery nights motivated this; they never count toward it.
