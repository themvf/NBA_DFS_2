# Experimental WR workload optimizer source

## What changes
NFL DFS (`/dfs/nfl`) now offers **WR workload (experimental)** in Portfolio settings, with a **Use experimental WR workload** shortcut and a player-change table. The existing volume/share model's unadjusted WR mean, P10, and P90 now drive real optimizer selections. Cash uses P10; GPP uses P90. No boom probability is invented for this model. Other players keep explicitly labeled historical projections, with optional DK fallback only when enabled. The default source remains historical.

This increment activates the existing WR model, not every research component. RB carry forecasts, rookie/new-team role assumptions, and the separate 50% injury-redistribution scenario are not activated. Player tail sums remain search heuristics, not complete-lineup percentiles.

## Eligibility and provenance
The server reloads the salary slate and current roster evidence before optimization. It joins the canonical database player ID to GSIS identity, requiring a unique same-team WR forecast, matching season/week, strictly prior-season history, a future salary/schedule kickoff match, fresh resolved roster evidence, at least four historical games, and finite ordered ranges. Current OUT exclusions remain effective. The experimental optimizer and both comparison arms require fresh offensive roster evidence and resolved expected-starter QB1 roles; unresolved quarterbacks cannot enter through fallback. No fuzzy workload matching is used.

A workload snapshot expires after 72 hours and is unusable after kickoff. Runs with no eligible workload players fail explicitly rather than claiming a workload run made entirely from fallback. Final save checks prevent workload forecasts expiring during optimization. Frozen optimizer inputs retain canonical IDs, salary/game information, availability, complete workload forecasts, source/roster/recipe digests, and fallback reasons. Showdown preserves CPT salary and scoring multipliers.

The shipped forecast is a refreshed **2026 Week 1** snapshot (32 teams / 119 historically supported WRs before slate eligibility checks). This source currently consumes the versioned report; the existing daily historical-model workflow does not refresh it. To refresh this release, run:

```powershell
python -m ingest.nfl_dfs_target_share --source-root "C:/Docs/_AI Python Projects/NBADFS_v2"
```

Review and deploy the updated `web/src/data/nfl-volume-share-report.json`. Refreshing the workspace reloads eligibility and the deployed forecast; it does not recompute this Python model. Wrong-week and stale snapshots fall back per player, or fail if none qualify. Later-week rollout requires a newly supported and validated forecast snapshot, not reusing Week 1 numbers.

## Same-slate comparison
**Generate matched comparison** reads one server-side player snapshot and saves two optimizer runs: historical and experimental WR workload. Both use the same common pregame player pool, salary constraints, locks/exclusions, exposure controls, stack settings, and zero randomness. Up to five lineups per source keep the comparison small; exposure counts are rounded for that portfolio size. Forecasts without a baseline or enabled DK fallback are excluded from both arms. Downloads retain both snapshots and settings.

The comparison displays actual player choices and projected scores, with warnings for incomplete portfolios. Differences in projected totals are not performance gains. Saved runs and canonical player IDs support grading after games finish. Each run is saved separately; if the second save fails, the first run may remain saved and the UI reports failure rather than a completed pair.

## Historical evidence
Reran `ingest.nfl_dfs_volume_benchmark` against the digest-verified production-algorithm replay, with market inputs disabled:

| Season | Paired WR games | Production / workload MAE | Production / workload interval error |
|---|---:|---:|---:|
| 2024 | 1,958 | 5.060 / 4.971 | 23.915 / 26.425 |
| 2025 | 2,019 | 4.706 / 4.515 | 21.277 / 24.100 |

Means improved; range quality worsened. Both seasons were already inspected. There is no fresh holdout or verified historical salary mapping for a valid lineup/payout replay. No default promotion or claimed contest edge is justified by these results.

## Verification
`test:nfl-workload-optimizer` tests identity, week, timestamp, kickoff, eligibility, missingness and range guards; actual cash/GPP selection changes; deterministic runs; historical fallback; unchanged inputs; and Showdown CPT scoring. Existing workspace and calibrated-source regression suites, Python target-share tests, TypeScript and targeted lint also cover this integration. The read-only `web/scripts/verify-nfl-workload-pair.ts` checks identical saved inputs/settings, actual source use, roster legality, and starting-QB evidence. Browser verification exercises live forecast coverage, source selection and saved paired generation. The Python schema and runtime migration both allow the new projection source.

Live verification on 2026-09-06 qualified 71 WRs in the saved slate. Historical run `2a47825c-f527-4a10-9e5a-cf42004e07ec` and workload run `cb3eba82-d604-4421-91fe-ffb26a663c99` each saved five legal lineups with identical frozen inputs and controls. All five lineups differed, five total roster slots used workload forecasts, and every selected QB had expected-starter evidence. Actual performance is pending. Production build and targeted lint passed.
