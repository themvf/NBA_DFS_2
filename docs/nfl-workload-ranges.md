# WR workload ranges and salary targets

Run `python -m ingest.nfl_dfs_workload_ranges --source-root "C:/Docs/_AI Python Projects/NBADFS_v2"` from the worktree. This reads verified weekly caches, the pinned production-algorithm replay and read-only database salary metadata. It writes a UI report and compressed paired prediction archive under ignored `artifacts/nfl_volume_share/`.

## Frozen experiment

Group by prior weighted targets: below 4, 4 to below 7, or at least 7. Require 100 earlier scored residuals in that group, retain at most 2,000, and add those uncentered residuals to the volume/share mean. Uncentered residuals adjust the mean too; this is not merely changing intervals. Update residual pools only after the entire week. No target-game workload, injuries or salaries enter the forecast. No parameter sweep was performed against these results.

Compare exactly paired WR player-games to the existing production algorithm with market inputs disabled. Previously inspected 2024/2025 remain diagnostic seasons, not untouched validation. Missing/DNP games are outside the cohort. Source and recipe hashes and paired prediction hashes are recorded.

| Season | Paired games | MAE historical → workload | Interval score historical → workload | Workload below P10 / above P90 |
|---|---:|---:|---:|---:|
| 2024 | 1,958 | 5.060 → 5.094 | 23.915 → 24.053 | 9.7% / 11.0% |
| 2025 | 2,019 | 4.706 → 4.742 | 21.277 → 22.033 | 10.8% / 9.3% |

The screen requires lower MAE and interval score plus no worse 25-point Brier error in both years. It fails. Optimizer activation remains false. Better overall tail frequencies do not prove conditional calibration: the 2025 40–60% group for 25 points forecasts 47.3% versus 16.7% observed over only 12 games. Do not present this group as a proven high-upside ranking.

## Salary evidence

The September 6 audit found 2,876 NFL salary upload rows, all inspected game strings belonging to 2026, and no salaries on the 4,338 saved NFL projection rows (season 2026). Legacy DK slates cover NBA/MLB only. No salary-named historical files were found in the existing source cache. This does not assert no external historical salaries exist; there is no verified 2024/2025 player-game-slate salary mapping in this experiment. `verified_replay_salary_rows=0` means none mapped, not an automatic claim about every possible database table. A future ingestion must establish identity, date, slate format and salary provenance before changing this status.

Therefore fixed 10/15/20/25-point probabilities are evaluated with Brier scores and predicted/observed probability bins. Historical salary-tier and 2×/3×/4× validation remain unavailable. Current salaries never substitute for past salaries.

## UI

NFL DFS → Model Lab → Volume/share contains range comparisons, selectable season/point-target reliability charts and a hypothetical salary calculator. The calculator uses completed 2023–2025 residuals and manual base projection, prior targets and salary. It is for exploration only, not a historical replay or live recommendation. It displays inclusive 2×/3×/4× hit probabilities, adjusted mean, P10/P50/P90 and strict probability above adjusted mean. Negative residual draws are retained; this is not a stat-level generator. Classic/FLEX values only, no CPT scaling or lineup payout claim.

Tests cover inclusive thresholds, monotonic salary multipliers, invalid salaries, insufficient residual evidence and same-week leakage. Next modeling work should improve the underlying conditional outcome distribution, not repeatedly tune thresholds to these inspected seasons. Historical salary calibration still requires verified salary records.
