# NFL DFS model development study

Status: research and forward-shadow implementation; production defaults unchanged.

## Protocol fixed before this run

1. Benchmark the historical v2 algorithm, **with market inputs disabled**, against realized DraftKings points by season and QB/RB/WR/TE/DST. This is a controlled reconstructed baseline, not a claim to reproduce archived live forecasts. Use 200 Monte Carlo draws and seed 20260903 throughout both retrospective and shadow runs.
2. Fit two residual models independently for each position:
   - Opportunity: baseline projection, number of prior games, and prior-four-game opportunity (QB pass attempts; RB carries plus targets; WR/TE targets; zero for DST).
   - Closing exploratory: those features plus own implied total, opponent implied total, and spread. nflverse uses positive-home-favored spreads; the adapter converts the sign explicitly.
3. Fit on 2023, select ridge regularization using 2024 only, refit the selected specification on 2023–2024, then inspect 2025 once for this recipe. **2025 was previously examined by another harness and is not called untouched.** Forward 2026 is the genuinely fresh evaluation.
4. Save every sample and prediction. Measure MAE, RMSE, bias, P10/P90 coverage, boom Brier/calibration, rank correlation, and top-decile precision within the weekly positional cohort. Bootstrap paired MAE differences in whole-game clusters (500 replicates). These are not DK-slate rankings or ROI metrics.
5. Only opportunity candidates meeting the recorded shadow gate may receive forward candidate forecasts. Otherwise record baseline forecasts only. No model or optimizer setting is automatically promoted.

Candidate intervals and boom probabilities use the fitted model's training residual distribution. Their out-of-sample coverage is measured; they are not assumed calibrated. Selection is by validation MAE only. All alpha trials are retained. This remains exploratory multiple-candidate research, not confirmatory proof of an edge.

## Data and leakage controls

- Full cached nflverse player-stat cohorts for 2020–2025 avoid filtering history by today's active players. DST uses all 32 team identities in the 2023–2025 result ledger.
- Forecast each entire week before appending any outcomes from that week. Own opportunity uses only the prior four recorded games. Minimum history is two games; excluded rows are counted by season and position.
- The cohort is conditional on a recorded stat row. Missing rows are **not** labeled DNP, zero, or eligible DK players. Historical salary, injury/active-status and actual ownership controls remain unavailable.
- Historical closing references lack availability timestamps. Their candidate is explicitly ineligible for shadow/production, regardless of apparent performance. No paid Odds API calls are made.
- DST scoring is derived from source components and not independently reconciled to official contest exports. Prior source/scoring revisions remain stored.
- Production comparisons, salary-adjusted value, ownership leverage, and complete optimal-lineup reconstruction stay blocked until the required archives exist.

## Persistence and reproducibility

Content-addressed local artifacts store samples, frozen history, predictions, report, source file hashes, code hashes, model version, configurations, and cutoffs. `nfl_dfs_research_runs`, `nfl_dfs_research_samples`, and `nfl_dfs_research_history` persist the same research evidence. Replays insert no duplicates and do not overwrite previous studies.

Forward `nfl_dfs_shadow_predictions` are immutable, timestamped after computation, and constrained to precede kickoff. They reference a pinned study ID/digest and preserve the full historical cohort plus subsequently available weeks. `nfl_dfs_shadow_outcomes` appends realized result versions without mutating forecasts. Evaluations select the last accepted pregame forecast per player-week and latest scored result so daily runs cannot inflate sample size. Evaluation reports are persisted separately.

The daily NFL projection workflow refreshes current-season schedule, player-week and DST inputs, materializes results, builds normal projections, and runs the independent shadow step. Research changes neither the production formula nor optimizer defaults. A future model-promotion decision still requires adequate forward samples, calibration, stability, and relevant salary/slate controls. See `docs/nfl-dfs-player-variance-and-weekly-review.md` for operational details, the simulated junior-dev discussion, and the next-phase design.

## Commands

```powershell
python -m ingest.nfl_dfs_research --source-root "C:\Docs\_AI Python Projects\NBADFS_v2" --persist
python -m ingest.nfl_dfs_shadow --season 2026 --week 1
python -m ingest.nfl_dfs_shadow --settle-only
```

The shadow command requires `artifacts/nfl_dfs_shadow_config.json` to match a saved study. It fails closed on a missing/mismatched study rather than silently choosing the newest model.
