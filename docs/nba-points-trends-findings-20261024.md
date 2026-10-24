# NBA actual-points trends: first historical evaluation

October 24, 2026. Target: basketball points scored, conditional on the player appearing. No fantasy points, payout model or tournament optimization is used.

## Study design and verified coverage

The [protocol](nba-points-trends-study.md) was recorded before fitting and inspecting the final model results. Source: 2025–26 regular-season NBA player and team game logs in our warehouse. All 1,230 game identities have two reciprocal team rows; all 2,460 player scoring sums match team totals. No duplicate game/player identities or missing player/team/opponent joins were found. Raw opponent counting fields in team logs were missing in all 2,460 records; opposing teams' own records supply those fields through the verified reciprocal joins.

Of 26,651 player-game rows, two are non-appearances/missing points and 5,506 lack ten prior player appearances and/or ten prior games for both teams. The eligible sample is 21,143 appearances. Features include only earlier dates; a regression test changes target/future points and shot counts and verifies that target-game features remain unchanged.

- Development: November 9–December 31, 2025; 6,173 appearances.
- Validation: January 2026; 4,824 appearances. Used for history-window and hyperparameter selection.
- Reserved evaluation: February 1–April 12, 2026; 10,146 appearances over 501 games and 64 dates.
- Primary higher-scorer cohort: 1,016 held-out appearances by 80 players, selected because their prior-ten-appearance scoring average was at least 20. Today's actual points never define the cohort.

These are lagged-statistical replays, not archived pre-lock predictions: historical data were retrieved after the season. Pregame injury reports, expected lineups, minutes restrictions and betting spreads were not verified. Conditional appearance selection misses fatigue effects that manifest as DNPs. Requiring ten prior games means this experiment does not validate opening-week forecasts or cross-offseason transfer. Only one earlier season is presently available in the warehouse.

## 1. More stable history was the strongest clear finding

Mean absolute error (MAE), in actual basketball points, for the primary held-out cohort:

| History rule | MAE | RMSE | Mean prediction minus actual |
|---|---:|---:|---:|
| Prior 5 appearances | 6.835 | 8.829 | +1.339 |
| Prior 10 appearances | 6.517 | 8.408 | +1.380 |
| Prior 20 appearances | 6.297 | 8.150 | +0.796 |
| Prior 10, EWMA alpha .25 | 6.640 | 8.558 | +1.348 |

The 20-appearance baseline was selected on January validation, before the held-out period was read. Its MAE advantage is 0.538 points over the five-game mean and 0.220 over the ten-game mean. Exploratory date-clustered 95% bootstrap intervals for those differences favor the longer history: 0.351–0.718 and 0.096–0.342 points, respectively. On all eligible players, the ten- and twenty-game rules were almost tied (MAE 4.793 and 4.789); the stronger history-window finding is specific to the predefined higher-scorer cohort.

This supports caution about treating short hot streaks as a new scoring level. It does not establish the best lookback across seasons, injury returns or changed roles. The high-scorer cohort is selected by its recent ten-game average and therefore includes regression-to-the-mean effects; a longer average's advantage is not evidence that real role changes should be ignored.

## 2. Rest and defense changes added little for higher scorers

Standardized Ridge models, fitted on earlier data with January-selected regularization:

| Model | Higher-scorer MAE | Change versus learned history |
|---|---:|---:|
| Learned scoring history | 6.261 | — |
| + prior minutes, shots and FT opportunity | 6.218 | −0.042 |
| + home/away | 6.260 | −0.001 |
| + rest, B2B and schedule density | 6.242 | −0.019 |
| + opponent points allowed | 6.262 | +0.002 |
| + pace and defense per estimated possession | 6.270 | +0.009 |
| + scoring-style/defense interactions | 6.257 | −0.004 |
| + prior head-to-head residuals | 6.261 | approximately 0 |
| All feature families together | 6.223 | −0.037 |

Every individual family's primary-cohort paired interval includes no improvement. For example, adding opportunity yields −0.091 to +0.008 MAE points versus learned history; rest yields −0.052 to +0.013. These are small differences, not evidence for large automatic point adjustments. Leave-one-family-out checks also show small changes.

The combined Ridge model improves all-player MAE from the selected naive baseline's 4.789 to 4.653, and RMSE from 6.274 to 6.099. Its date-bootstrap MAE difference is −0.162 to −0.111. That is a modest secondary-cohort accuracy improvement in this split. It still underpredicts the all-player average by 0.504 points. For higher scorers it improves MAE from 6.297 to 6.223; that interval includes zero (−0.158 to +0.015). Its higher-scorer mean bias improves to +0.229.

A nonlinear gradient-boosting model with all features performed best on January validation (5.889 MAE) but worsened held-out higher-scorer MAE to 6.354. Its RMSE also worsened to 8.247 versus the naive baseline's 8.150. More complex fitting did not reliably improve these higher-scorer forecasts. It was not promoted.

Excluding April does not reverse the broad pattern: learned history MAE 6.236, history plus opportunity 6.197, combined Ridge 6.195 and nonlinear full model 6.340.

## 3. Back-to-backs do not support a blanket scoring deduction here

Among primary-cohort held-out appearances:

| Schedule | n | Actual points | Actual minutes | Points minus prior-ten average |
|---|---:|---:|---:|---:|
| Not a team B2B | 840 | 22.407 | 32.256 | −1.491 |
| Team B2B | 176 | 23.011 | 32.313 | −0.849 |

There is no negative aggregate B2B scoring difference in this conditional sample. Balancing by player and requiring at least three B2B and three non-B2B observations per player yields a +0.175-point average difference in baseline residuals across 28 players. This remains descriptive and can reflect selection, opponents and availability; it is not a recommendation to add points on B2Bs.

Team schedule rest is explicitly separate from days since the player's own last appearance. Three-in-four, four-in-six, opponent rest, prior minutes and B2B road/workload interactions were tested as part of the rest family. They did not establish a material held-out improvement for the primary cohort. Resting stars who never appeared are outside this dataset's target, a major reason not to interpret this as proof that fatigue does not matter.

## 4. A weak defense can come with reduced minutes

Opponent bands use their prior-ten-game defensive rating per 100 estimated possessions, calculated before the target date. Possessions use both teams' FGA + .44 FTA − offensive rebounds + turnovers; pace is normalized for game duration. These are approximations, not official NBA possession counts.

| Opponent's earlier defensive rating | Primary appearances | Actual points | Actual minutes | Games ending with margin >=15 |
|---|---:|---:|---:|---:|
| Under 110 | 352 | 22.537 | 33.255 | 33.5% |
| 110–115 | 313 | 22.540 | 33.001 | 34.2% |
| Over 115 | 351 | 22.462 | 30.618 | 54.7% |

The weaker-defense band did not produce higher average scoring in this sample. It had 2.64 fewer player minutes than the strongest-defense band, and more lopsided final scores. This is consistent with easier matchups sometimes offering better scoring conditions but less playing time; it is not a causal decomposition. The final margins and actual minutes are postgame diagnostics only and were never predictor inputs. Projecting minutes from verified pregame information is a separate missing step.

Descriptive team-specific patterns did exist. The primary-cohort score minus the player's own prior-ten average was −4.71 against Philadelphia (35 observations), −4.53 against San Antonio (40), +1.59 against Utah (28) and +3.08 against Indiana (32). The cohort's overall difference was −1.38, so zero is not a neutral schedule-adjusted benchmark. These named extremes were selected from 30 teams after inspection and have small samples; they are not validated next-game adjustments or current-season defensive rankings.

An explicitly labeled post-hoc extension checked opponent identity using regularized categorical effects rather than just numeric ratings. Opponent identity reduced MAE from 6.261 to 6.236, with an interval spanning zero (−0.059 to +0.009). Adding opponent identity to a player-identity model likewise remained uncertain. The full model with both identities scored 6.210, but was developed after the original held-out results were seen. It is exploratory and needs another untouched season or future period.

## 5. Correlations were weak in aggregate, with noisy individual exceptions

For held-out player pairs where each player's pregame ten-game scoring average was at least 15, compute same-game correlations of scoring deviations after removing the validation-selected model's point forecast:

- Teammates: −0.021 across 2,337 pair-games from 471 games.
- Opposing players: +0.056 across 2,784 pair-games from 421 games.

Using Ridge history or combined Ridge forecasts instead gives teammates −0.021 to −0.028 and opposing players +0.049 to +0.059. Thus there is no strong universal relationship in these aggregates. These are contemporaneous, descriptive correlations; neither player's actual future score is available at prediction time. Many pair observations reuse players/games, so they are not independent trials.

Seventy-one individual same-team pairs have at least 12 eligible shared games; some estimated correlations are large. Their small samples and selection among many pairs make them research leads, not established player-specific rules. Historical position-specific defense is not evaluated because a dated position source was not verified. Scoring-style proxies are included, but they do not replace lineup or individual-defender evidence.

## Implications for actual points forecasting

The result supports stabilizing the scoring baseline and testing modest context adjustments. It does not support blanket B2B deductions, large boosts against weak defenses, or a forecast based on a handful of past meetings. Across all players the combined context model helps modestly, while high-scorer forecasts remain uncertain by roughly six MAE points in this evaluation.

Next missing evidence is a pregame estimate of minutes and shot opportunities that accounts for known lineup/role changes and minutes restrictions, plus a multi-season test that includes opening weeks. Current-game minutes and final blowouts cannot be used to retroactively claim successful forecasts. Historical sportsbook points snapshots are also absent; none of these comparisons establishes improvement over Vegas.

No production forecasts, live selections, market probabilities or prior frozen benchmark files were changed.

## Reproduce and inspect

Code: `research/nba_points_trends.py`, exploratory extension `research/nba_points_trends_identity.py`, leakage/join/rest tests `tests/test_nba_points_trends.py`.

Local artifacts under `artifacts/nba-points-trends-20261024/`:

- `inputs.json`: frozen source records.
- `run-v1/registration.json`: pre-fit source, code and protocol hashes and chronological split.
- `run-v1/results.json`: every model's validation and final-period metrics, ablations, trend summaries and correlations.
- `run-v1/heldout-predictions.csv`: row-level inputs and final predictions for independent checking.
- `supplement.json`: all naive baseline results, predictor correlations and postgame defense diagnostics.
- `correlation-sensitivity.json`: aggregate residual correlations under three forecast baselines.
- `identity-extension-v1/`: clearly labeled post-hoc registration, predictions and results.

Run the primary experiment with a new output directory to avoid overwriting evidence:

```powershell
python -m research.nba_points_trends --input artifacts/nba-points-trends-20261024/inputs.json --output artifacts/nba-points-trends-20261024/new-run
python -m pytest tests/test_nba_points_trends.py -q
```

Bootstrap intervals resample 64 held-out dates (4,000 repetitions) and preserve all observations within a date. They are exploratory, unadjusted for many comparisons and do not fully capture persistent player-level dependence. Training/hyperparameter decisions used January, and the primary final-period scores were not used to refit a promoted model. The identity extension is separately marked post-hoc.
