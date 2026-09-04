# NFL DFS weekly player review and variance experiment

Built and verified 2026-09-04; prepared for main release. Hosting deployment is verified separately.

## Weekly review

Route: `/dfs/nfl/review`, linked from the NFL board and DFS workspace. Filter by season, week, player/team, position and production / market-free baseline / opportunity candidate. One saved weekly report is loaded at a time; a selected player's compact history loads separately so a full season does not serialize every player's audit payload into the browser.

The page shows coverage, scored/unscored counts, MAE, signed error (`actual - projected`), interval coverage, overdue-result warnings, per-player weekly forecast/actual charts, and frozen component forecasts where available. Every row has downloadable identity, model, cutoff, input digest, source and result-revision evidence. Component forecasts absent from old snapshots are explicitly unavailable, never reconstructed after the game.

`ingest.nfl_dfs_reportcard` saves append-only JSON reports to `nfl_dfs_weekly_report_cards`. The workflow adds this after results/projections/shadow steps and still attempts reporting if those stages fail. No browser request writes grading data or creates tables. The latest report per week is displayed; repeated observations do not inflate player counts.

Report selection uses conservative production availability (`max(run as-of, run created, player created)`) and excludes at/after-kickoff snapshots. Shadow forecasts use their already-enforced pregame freeze. Corrected or subsequently excluded results win over older results; zero is a valid actual, while missing rows never become zero. Forecasted players survive later inactive roster flags.

The 48-hour missing-result grace is measured from canonical kickoff because an independently timestamped final whistle is not available. Report age over 36 hours is surfaced separately. These are in-app warnings, not a new external notification service.

Initial persisted 2026 Week 1 coverage: 1,039 production forecasts; 645 market-free/shadow candidates; 394 players without shadow forecasts; zero realized outcomes, appropriately pending. The canonical-roster population is not a DK salary slate. Verified DNP/eligibility ingestion and a fully snapshotted weekly roster are still missing. Legacy shadow team mapping uses current team plus exact frozen kickoff and fails closed if that cannot be resolved; new freezes retain team/opponent directly.

Commands:

```powershell
python -m ingest.nfl_dfs_reportcard --season 2026 --week 1
python -m ingest.nfl_dfs_reportcard --season 2026
```

Without a week argument, all weeks with a saved projection/shadow observation or completed game are refreshed, so delayed results and corrections remain visible.

## Player-specific variance: first experiment, not promotion

`model.nfl_dfs_variance` uses prior out-of-sample baseline errors. It estimates recency-weighted individual residual variance, reports effective sample size, and shrinks toward a position-level prior. No future or same-week outcomes enter a forecast's variance inputs. Mean projections are unchanged. Role-specific priors, injuries, and new opportunity/efficiency components are not implemented in this first experiment.

Protocol: 2023 residual warm-up; choose shrinkage strength 4/12/24 on 2024 interval score; inspect 2025 retrospectively. The residual pool updates only after each completed target week. Historical source-publication latency is unknown; 2025 was previously inspected, and these are not fresh forward results. The approximation scales centered empirical residuals, not newly simulated valid component-stat lines.

Saved 12,525 player-week distributions across 2024–2025:

`artifacts/nfl_dfs_variance_9374b13a398bc932/report.json`

`artifacts/nfl_dfs_variance_9374b13a398bc932/predictions.json`

2025 diagnostic (80% interval score penalizes both misses and unnecessary width; lower is better):

| Position | Baseline score | Candidate score | Candidate coverage | Assessment |
|---|---:|---:|---:|---|
| QB | 30.58 | 29.92 | 78.6% | Better point estimate |
| DST | 20.73 | 19.70 | 80.3% | Better point estimate |
| RB | 22.09 | 22.78 | 81.2% | Worse despite wider coverage |
| WR | 20.76 | 21.16 | 81.9% | Worse despite wider coverage |
| TE | 17.44 | 17.69 | 81.6% | Worse despite wider coverage |

No candidate was activated in forward shadow or production. No significance/return claim is made from these comparisons. In particular, this tests variance around the market-free baseline, not around the opportunity candidate. Point-error metrics are identical by design because the expected means are unchanged.

Next: use the weekly report to verify real grading coverage, then investigate position/role differences. Do not install one universal wider-range formula merely because its nominal interval coverage is closer to 80%.

## Verification

- 76 Python regression tests passed, including no future/same-week variance leakage, unchanged means, result corrections, latest excluded-result handling, missing-versus-zero outcomes, and deduplicated pregame grading.
- TypeScript typecheck, targeted ESLint, and the report-card summary/denominator script passed.
- Browser checked against the live saved Week 1 report: player search, production/opportunity switching, single-week P10/mean/P90 markers, pending outcomes, and component-evidence missingness.
- No paid data calls or production model promotion occurred in this iteration.
