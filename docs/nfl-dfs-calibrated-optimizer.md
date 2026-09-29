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

## Release v2: generated from the shadow pin (2026-09-29, B4)

The release named study `8bab9091` in code. The shadow job was re-pinned twice (current `7ff4d404`, recomputed against historical-v5), so from week 3 the page read a study that froze nothing and the source silently produced no forecast. `ingest/nfl_dfs_optimizer_release.py` now reads the study from `artifacts/nfl_dfs_shadow_config.json`, refuses a config whose report disagrees, and records each position's study status; tests assert the release names the shadow pin (`tests/test_nfl_dfs_optimizer_release.py`, `test:nfl-calibrated`). The same gate re-run against `7ff4d404`:

| Position | 2025 MAE baseline → candidate | 80% interval score | Study status | Opt-in |
|---|---|---|---|---|
| DST | 4.430 → 4.275 | 21.94 → 19.32 | eligible_for_shadow_only | Yes |
| QB | 6.971 → 6.931 | 31.69 → 29.94 | not_eligible | No |
| RB / WR / TE | small mean gains | worse ranges | not_eligible | No |

QB still passes the release's own metric screen but the study no longer freezes a QB candidate (it fell below the study's ≥1% retrospective MAE gain once v5 improved the baseline), so it cannot be offered. The source is therefore **Calibrated (experimental)** with DST only; the table above supersedes the v1 table for current use. Snapshots are read at or before the slate's projection cutoff, so a later daily freeze no longer replaces a saved slate's candidates.

## Reproduction

From the repository root, run `python -m ingest.nfl_dfs_optimizer_release` with the saved research artifacts available. This regenerates `web/src/lib/nfl-dfs/calibrated-release.json` from the pinned study, including paired metrics and artifact digests, without fitting or database writes. Re-run it whenever the shadow config is re-pinned.

From `web`, run `npm run test:nfl-calibrated`, `npm run test:nfl-dfs-workspace`, and `npm run build`. The calibrated tests cover identity and time rejection, source-specific objectives, actual lineup changes, historical fallback, empty coverage rejection, and Showdown captain scaling.

## Roster availability increment

The player pool now displays a roster-role/status badge with source and retrieval timestamp in its tooltip. Fresh (72-hour), exact player/team/position Sleeper roster evidence labels QB1 as expected starter and excludes listed QB2+ from all projection sources. OUT, IR, PUP, NFI, SUSPENDED and INACTIVE records also exclude players. Existing DK exclusions are never cleared. Questionable players remain eligible. Missing, future-dated, stale or mismatched records stay unresolved and do not assert availability. A listed starter is not game-day confirmation.

Generation reloads roster evidence on the server and freezes each player's availability and effective exclusion in the saved input snapshot. This increment does not promote replacement starters, redistribute WR targets, or change projection means/ranges. Those require validated workload scenarios. Run `npm run test:nfl-availability` for the evidence resolver checks.
