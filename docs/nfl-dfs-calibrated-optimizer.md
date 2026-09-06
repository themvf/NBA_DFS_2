# Calibrated optimizer source

The NFL DFS workspace (`/dfs/nfl`) offers **Calibrated QB/DST (experimental)** as an explicit optimizer source. Load or resume a saved salary slate, inspect the projection comparison, then choose **Use calibrated projections**. Historical projections remain the default.

Qualified QB and DST snapshots supply their mean, P10, P50, P90 and boom probability to Classic and Showdown optimization. Other positions use historical projections, then the existing optional DK fallback. Each saved run retains the exact snapshot, recipe, forecast timestamp and fallback reason. No eligible calibrated rows produces an explicit error.

## Evidence and limits

The release replays frozen predictions: 2023 fit, 2024 selection, and previously inspected 2025 retrospective diagnostics. Both years must improve MAE, 80% interval score and boom Brier on at least 100 paired observations. This is not an untouched holdout or a contest-return backtest.

| Position | 2025 MAE baseline → candidate | 80% interval score baseline → candidate | Opt-in |
|---|---|---|---|
| QB | 7.055 → 6.954 | 30.58 → 29.74 | Yes |
| DST | 4.384 → 4.264 | 20.73 → 19.32 | Yes |
| RB / WR / TE | Mean gains | Worse ranges | No |

The comparison uses market-free frozen forecasts. The pool's existing Our projection may include environment adjustments. QB calibration incorporates prior workload; DST calibration corrects historical scoring. Current roster and injury counterfactuals are not implemented. Forward validation remains pending, and the existing shadow study and production historical model are unchanged.

Forecasts must match player ID, position, team, opponent, season/week and exact salary-file kickoff. They must precede kickoff, have a strictly earlier history cutoff, be no more than 72 hours old, and match the pinned study and recipe. Availability is checked again on generation. Missing or rejected forecasts have visible fallback reasons; a missing shadow table does not prevent baseline use.

The existing schema initializer expands the optimizer-run source constraint to accept `calibrated`, preserving all existing source values and records. Python schema creation accepts the same values. Browser verification resumed the 719-player saved slate, found 78 eligible candidate rows, and saved one legal Classic lineup after this migration. Eligibility means a matching forecast, not confirmed starter status: depth-chart and injury-role checks remain necessary.

Cash and GPP use source-specific player tail estimates. Summed player tails are explicitly labeled search heuristics, not complete-lineup percentiles. External sources no longer inherit historical model tails or boom probabilities. Joint scenarios remain in Scenario Lab. Kelly sizing requires calibrated contest payouts and portfolio dependence, so it is deferred.

## Reproduction

From the repository root, run `python -m ingest.nfl_dfs_optimizer_release` with the saved research artifacts available. This regenerates `web/src/lib/nfl-dfs/calibrated-release.json`, including paired metrics and artifact digests, without fitting or database writes.

From `web`, run `npm run test:nfl-calibrated`, `npm run test:nfl-dfs-workspace`, and `npm run build`. The calibrated tests cover identity and time rejection, source-specific objectives, actual lineup changes, historical fallback, empty coverage rejection, and Showdown captain scaling.
