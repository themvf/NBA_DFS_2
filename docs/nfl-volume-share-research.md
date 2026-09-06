# Pass-volume and target-share research release

The report lives at `/dfs/nfl/model/workload`, linked from Model Lab and the WR context panel. It contains team budgets, WR target/point estimates, player P10/P50/P90, retrospective scoring comparisons and source digests. The unadjusted WR forecasts now also feed an opt-in optimizer source; see [workload optimizer release](nfl-workload-optimizer-release.md). The default source remains unchanged.

## Model

`model/nfl_dfs_target_share.py` predicts team attempts from eight prior games with a four-game half-life. Attempts times prior target/attempt ratio defines the target budget. Receiver shares use same-team history; the last four prior team games define the historical recipient universe. Shares above 100% normalize to the budget. Remaining work stays unallocated. Missing players and new roles do not acquire invented workloads.

The DK mean adjustment uses the target change times a capped historical DK-points-per-target proxy (0.5–3.5). That proxy includes other scoring contributions and is not a complete receiving-stat simulator. Both the weighted-scoring comparator and candidate use the same prior-only WR residual procedure to estimate marginal ranges. Residual updates happen after each entire week; the target outcome and other same-week outcomes cannot alter its forecast.

The forward-only scenario removes fresh listed-out players and redistributes half their known share proportionally among remaining historical recipients. The other half stays unallocated. Unmatched historical recipients retain their allocation explicitly; it is not silently transferred to current players. This redistribution fraction is an unvalidated assumption, not learned evidence. Pregame evidence requires exact player/team/position source matching, retrieval no later than the forecast time, forecast strictly before kickoff, and at most 72 hours of age. Questionable and unknown statuses do not imply absence.

## Evidence

| Diagnostic season | Scored WR games | Baseline / candidate DK MAE | Baseline / candidate interval score | Baseline / candidate target MAE |
|---|---:|---:|---:|---:|
| 2024 | 1,958 | 5.085 / 4.971 | 26.486 / 26.425 | 2.003 / 2.035 |
| 2025 | 2,019 | 4.714 / 4.515 | 24.345 / 24.100 | 1.873 / 1.851 |

These are previously inspected retrospective seasons, using 2023 for warmup. Neither is a fresh holdout. The comparator is the experiment's weighted scoring baseline, not the production optimizer model. Missing/DNP stat rows are excluded; this is not a slate-profitability or availability-conditioned backtest. Target accuracy regressed slightly in 2024, and no statistical significance is claimed.

The read-only availability audit found zero observations for the replay seasons and 27,151 for 2026. Consequently historical predictions do not consume target-week availability. Current roster snapshots produce a separate pregame scenario for 32 teams / 119 WRs with matching history. It is explicitly dated and expires for current use after 72 hours; the page is a static evidence report, not a live forecasting feed.

## Reproduce

Run from the repo root:

```
python -m pytest tests/test_nfl_dfs_target_share.py -q
python -m ingest.nfl_dfs_target_share --source-root "C:/Docs/_AI Python Projects/NBADFS_v2"
```

The ingestion command verifies cached 2023–2025 weekly-stat bytes against the source manifest. PostgreSQL is read-only; no existing study, shadow prediction, optimizer record or production model changes. It writes a dated local archive under `artifacts/nfl_volume_share/` and the current UI report at `web/src/data/nfl-volume-share-report.json`. Database evidence changes across runs; snapshot, roster and recipe digests make that visible.

Tests cover budget conservation, unavailable shares, unknown reserve, invalid numbers, temporal cutoff, team isolation, pregame source age and same-week residual leakage. Before promotion, compare against the actual optimizer on identical cohorts and collect timestamped 2026 forward outcomes, including non-participation. Keep injury-adjustment calibration separate from the volume/share mean improvement.
