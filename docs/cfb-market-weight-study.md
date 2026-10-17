# CFB market weight study (`cfb-market-weight-v1`)

**Registered:** 2026-10-17, before the line-timing check was run and before
any model-versus-line residual was computed.
**Status:** steps 1 and 2 run 2026-10-17 (results below). The 2026 confirmation is pending until after 2026-12-07.
**Scope:** research only. Nothing in this study changes a page, a forecast,
a star rating or a bet.

## Question

How much of the CFB football score model's forecast should be kept once the
closing line is known? Put formally: in

```
forecast = line + w * (model - line)
```

what weight `w` fits 2023-2025 games out of sample, and is it different from
zero?

`w = 0` means the football model adds nothing the line does not already
contain. `w = 1` means the line adds nothing. `research/cfb_market_anchor.py`
currently fixes `w = 0.25` by assumption. This study measures it on
history instead.

This is Fix 1 of the 2026-10-17 outside review of the CFB score model. It is
a measurement, not a new model. It does not add a fifth score-model version.

### Why not v4

`research/cfb_forecast_v4.py` already fits a market weight
(`fit_market` in `research/cfb_v4_calibration.py`). It can only fit on 2026
games, because our own timestamped DraftKings quotes (`game_odds_history`,
`sport='cfb'`) begin in 2026. That uses the season this study keeps
untouched. This study uses 2022-2025 CFBD lines and does not read any 2026
score.

## Football model

The frozen `cfb-score-context-v1` (`research/cfb_forecast_v1.py`), unchanged:
the same replay, features, ridge (alpha 100), `PRIOR_GAMES = 4`,
`MIN_CURRENT_GAMES = 2` and point-in-time history. v1 was chosen because it
has the longest prospective record of the three published versions and is
the simplest. v2 and v3 are not tested here. Testing three versions and
reporting the best would be the multiple-comparisons error the review warns
about.

Out-of-sample forecasts come from season-by-season refits:

| forecast season | v1 trained on |
|---|---|
| 2023 | 2022 |
| 2024 | 2022-2023 |
| 2025 | 2022-2024 |

Each refit uses only seasons before the forecast season, so every forecast in
the study is out of sample. 2022 has no out-of-sample forecasts, because
nothing earlier is loaded (`cfb_plays` starts in 2022). It is training data
only.

## Line

`cfb_historical_game_lines`, CFBD `/lines`, ingested 2026-09-03 by
`ingest/cfb_history.py`. Per game and market, the row flagged
`is_canonical_reference`, with designation `historical_reference` (CFBD's
`spread` / `overUnder` / moneylines). The canonical provider follows
`PROVIDER_PRIORITY` (consensus, draftkings, fanduel, betmgm, caesars): it is
consensus in 2022 and mostly DraftKings in 2023-2025.

- **Margin line** = `-home_value` of the spread row (CFBD spreads are
  home-relative, negative when the home team is favored).
- **Total line** = `home_value` of the total row.
- **Moneyline reference** = the no-vig home probability from the canonical
  moneyline pair. It exists only where both prices are present.

Before any residual is computed, the runner checks the spread's sign against
outcomes and stops unless `corr(margin line, actual margin) > 0.5`. This is a
data check, not a model result.

CFBD records no time for these lines. The next section fixes how that is
resolved.

## Step 1: what time a CFBD line represents

Runs before the fit. It reads 2026 line values only, never 2026 scores.

**Method** (`research/cfb_line_timing_check.py`). Fetch CFBD `/lines` for 2026
and map games through `cfb_matchups.cfbd_game_id`. Keep only games that
kicked off more than two days earlier, so CFBD's value has settled. For each
CFBD provider that matches one of our captured books, compare CFBD's value
with that book's series in `game_odds_history`: our first pregame capture
("open") and our last pregame capture ("close"). The mapping is
`draftkings` → `draftkings` and `william hill (new jersey)` → `williamhill_us`.
CFBD's `consensus` is compared with the median spread across all our books
at each capture.

Per game and provider, the outcome is one of:

- `no_move`: our open equals our close, so the game says nothing about timing;
- `close_only`, `open_only` or `neither`: whichever of our open and close
  CFBD's value equals (exact match on the half point).

The check also reports how many hours before kickoff the latest capture
matching CFBD's value was taken, plus the same match rates for `spreadOpen`
and for totals.

**Decision rule, fixed now.** Use DraftKings if it has at least 50 games that
moved; otherwise use consensus.

- **CLOSE_LIKE**: at least 70% of moved games are `close_only` and at most
  15% are `open_only`.
- **OPEN_LIKE**: the reverse.
- **INDETERMINATE**: anything else.

The fit uses `historical_reference` whatever the outcome; there is no other
historical line to switch to. The outcome decides only how results are read:

- **CLOSE_LIKE**: the fitted weight is residual skill against the close, the
  hardest benchmark. Production forecasts are frozen hours before kickoff and
  anchored to an earlier quote. A weight fitted against the close and applied
  to an earlier quote probably *understates* how far the line still moves
  toward the forecast. Any production use must say so.
- **OPEN_LIKE**: the weight measures skill against an opener. It is not a
  measure of beating the close and is never reported as one.
- **INDETERMINATE**: every result carries "line of unknown timing".

**Known limits of the check.** Our first capture is not the true market open:
The Odds API lists most CFB games only 2-5 days ahead. 2026 provider
behaviour may differ from the 2022 consensus feed. Both limits are reported
with the result.

## Step 2: the fit

For each market, `x = model - line` and `y = actual - line`:

- **Primary estimate:** `w = sum(x*y) / sum(x*x)`, least squares through the
  origin. It is not clipped, so negative values and values above 1 are allowed
  and reported as found.
- **Moneyline:** a logistic model with the market as a fixed offset,
  `logit(p) = logit(q_market) + theta * (logit(p_model) - logit(q_market))`,
  where `p_model` is v1's normal-CDF win probability. There is no intercept,
  and the fit is plain maximum likelihood.
- **Sensitivity, reported but not a decision input:** the same fits with an
  intercept.

**Pooled headline.** `w_margin`, `w_total` and `theta` are fitted on every
2023-2025 out-of-sample game. Each carries a 95% interval from a 2,000-draw
bootstrap that resamples game dates.

**Walk-forward check:**

| fold | weight fitted on | graded on |
|---|---|---|
| A | 2023 | 2024 |
| B | 2023-2024 | 2025 |

Each fold grades the anchored forecast `line + w*(model - line)` against the
line alone. The paired mean absolute error difference gets a game-date
clustered 95% interval; RMSE is also reported. The fixed `w = 0.25` anchor and
v1 alone are graded on the same games for comparison. Win probabilities are
graded by Brier score and log loss against the no-vig moneyline.

**Exclusions**, each counted and reported:

- games v1 does not forecast (fewer than 2 current-season games for either
  team);
- games with no canonical line for the market;
- moneyline games without both prices.

Neutral-site games stay in, under v1's home flag, as v1 treats them, and are
counted.

## Expected result (written before the fit)

| quantity | expected |
|---|---|
| pooled `w_margin` | 0.00 to 0.15; the 95% interval includes 0 or sits just above it |
| pooled `w_total` | 0.00 to 0.10; the interval includes 0 |
| pooled `theta` | 0.00 to 0.15 |
| fold A and B MAE, anchored minus line | between -0.10 and +0.05 points; intervals include 0 |
| fixed `w = 0.25` minus line | positive (worse than the line) in both folds |
| timing check | CLOSE_LIKE |

Stated as the market weight the review asked for, that is 0.85 to 1.00. A
weight near zero is the expected result, not a failure.

## Decision rules

- **The pooled `w_margin` interval includes 0:** v1 shows no residual skill
  against CFBD lines on 2023-2025. The recommendation becomes "anchor weight
  0". Changing `cfb_market_anchor.py` is a new version (`cfb-market-anchor-v2`)
  and is the owner's call.
- **The interval excludes 0 with `w > 0`, and both fold MAE intervals are
  below 0:** some residual skill. A `cfb-market-anchor-v2` may use the fitted
  pooled `w`, still research-only, and its 2026 grade below is the
  confirmation.
- **The interval excludes 0 with `w > 0`, but a fold interval is not below
  0:** inconclusive. The weight is recorded and nothing changes.
- **The interval excludes 0 with `w < 0`:** v1 is anti-informative against the
  line. Recorded. It strengthens the case for retiring v1-v3 as published
  numbers.
- **Totals and moneyline** follow the same rules separately and never pool
  with the margin.

## 2026 confirmation (registered now, run later)

After the 2026 regular season, and not before 2026-12-07, the frozen pooled
weights are applied once to every 2026 game v1 forecast. The line is our own
last DraftKings paired quote before kickoff, the line production can actually
anchor to. The anchored forecast is graded against that line with a game-date
clustered interval. This is the only use of 2026 data, and the weights are
not refitted.

## Non-negotiables

- No 2026 score is read before the confirmation date.
- No alpha, prior weight, feature or version change to v1 for this study.
- No slicing by conference, week, line size or favorite/underdog to rescue a
  null result. A slice is a new registration.
- No page, forecast, star rating or bet changes because of this study. A
  `cfb-market-anchor-v2` is a separate change the owner approves.

## Files

| file | role |
|---|---|
| `docs/cfb-market-weight-study.md` | this registration |
| `research/cfb_line_timing_check.py` | step 1 |
| `.github/workflows/cfb_line_timing_check.yml` | runs step 1 with the CFBD key |
| `research/cfb_market_weight.py` | step 2 (written after step 1, unchanged rules) |
| `artifacts/cfb-market-weight-v1/` | step 1 and step 2 outputs |

## Results

### Step 1: CLOSE_LIKE (2026-10-17, recorded before step 2 was run)

[Run 39075581818](https://github.com/themvf/NBA_DFS_2/actions/runs/39075581818),
452 settled 2026 games. The decision basis is DraftKings: 285 games moved,
against the rule's minimum of 50.

| CFBD field (DraftKings) | moved | matches our close | matches our open | neither | label |
|---|---:|---:|---:|---:|---|
| `spread` (the fit's line) | 285 | 268 (94.0%) | 9 (3.2%) | 8 | CLOSE_LIKE |
| `overUnder` | 286 | 255 (89.2%) | 8 (2.8%) | 23 | CLOSE_LIKE |
| `spreadOpen` | 283 | 24 (8.5%) | 118 (41.7%) | 141 | INDETERMINATE |
| `overUnderOpen` | 254 | 28 (11.0%) | 100 (39.4%) | 126 | INDETERMINATE |

The latest capture matching CFBD's `spread` sits a median 0.03 hours (about
2 minutes) before kickoff. CFBD's `spread` and `overUnder` are therefore
closing lines. `spreadOpen` is mostly earlier than our first capture, which
fits our captures starting 2-5 days out.

Sign check: CFBD's spread had the same sign as our close in 404 games, a
different value in 36, and a flipped sign in none.

**Reading for step 2:** the fitted weights measure residual skill against the
close. The 2026 payload carries only DraftKings and Bovada, so the 2022
`consensus` rows (the canonical provider that season) cannot be checked
directly. They are read as close-like by analogy, and that is an assumption.

### Step 2: NO_RESIDUAL_SKILL for margin, total and moneyline (2026-10-17)

The runner was committed before it ran: `e593e5e7`. Output is in
`artifacts/cfb-market-weight-v1/fit.json`. The spread sign check passed
(`corr = 0.624`). Lines are DraftKings in 99% of games; Bovada is the
fallback in the rest.

| season | completed FBS games | v1 forecasts | not forecast by v1 | spread / total / moneyline pair | neutral site |
|---|---:|---:|---:|---|---:|
| 2023 | 796 | 643 | 153 | 643 / 643 / 629 | 55 |
| 2024 | 802 | 648 | 154 | 648 / 648 / 644 | 67 |
| 2025 | 809 | 653 | 156 | 653 / 653 / 641 | 57 |

**Pooled 2023-2025** (1,944 games, 201 dates; moneyline 1,914 games):

| market | fitted weight | 95% interval | expected | decision |
|---|---:|---|---|---|
| margin `w` | -0.008 | [-0.117, +0.107] | 0.00 to 0.15 | NO_RESIDUAL_SKILL |
| total `w` | +0.102 | [-0.059, +0.263] | 0.00 to 0.10 | NO_RESIDUAL_SKILL |
| moneyline `theta` | +0.007 | [-0.200, +0.207] | 0.00 to 0.15 | NO_RESIDUAL_SKILL |

**Walk-forward**, MAE in points; deltas are paired against the line, with
game-date clustered intervals:

| fold | market | line | anchored | fixed 0.25 | v1 alone | anchored - line | fixed 0.25 - line |
|---|---|---:|---:|---:|---:|---|---|
| A (graded 2024) | margin | 11.745 | 11.745 | 11.873 | 12.789 | 0.000 [-0.002, +0.002] | **+0.127 [+0.007, +0.266]** |
| B (graded 2025) | margin | 11.772 | 11.772 | 11.841 | 12.768 | +0.001 [-0.002, +0.003] | +0.069 [-0.037, +0.196] |
| A | total | 12.921 | 12.910 | 12.873 | 13.135 | -0.010 [-0.025, +0.005] | -0.048 [-0.132, +0.038] |
| B | total | 12.382 | 12.393 | 12.418 | 12.796 | +0.011 [-0.029, +0.050] | +0.036 [-0.050, +0.119] |

Moneyline Brier scores (market / anchored / v1 alone) were 0.1866 / 0.1867 /
0.2021 in fold A and 0.1838 / 0.1842 / 0.1932 in fold B. The anchored-minus-
market interval includes zero in both folds.

**Against the expected result.** Margin, moneyline, the fold deltas and the
timing check all landed where expected. The margin weight sits 0.008 below
the expected range, with an interval centred on zero. The total weight sits at
the top of its range, and its interval includes zero. One expectation missed:
"fixed 0.25 worse than the line in both folds" held for the margin, where it
was significant in fold A only, but not for totals in fold A (-0.048, interval
includes zero).

**Reading.** Against CFBD's closing line, v1's forecast carries no
information the close lacks, in any of the three markets. v1 alone misses the
margin by about 1.0 point more than the close, which matches what 2026's
prospective grades showed. Under the registered rules the recommendation is
**anchor weight 0**. Whether to change `research/cfb_market_anchor.py` (a new
`cfb-market-anchor-v2`) is the owner's decision. The 2026 confirmation still
runs after 2026-12-07 with the frozen pooled weights.

**Not a finding.** The intercept sensitivity fits put the margin's intercept
at +0.59 points toward the listed home team. That was not a registered
quantity and licenses nothing; testing it would be a new registration.
