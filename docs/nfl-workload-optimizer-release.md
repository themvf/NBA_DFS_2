# Experimental WR workload optimizer source

The subsequent [combined position release](nfl-position-workload-release.md) adds independent QB/RB/TE controls. This document preserves the initial WR release and its verification.

## What changes
NFL DFS (`/dfs/nfl`) now offers **WR workload (experimental)** in Portfolio settings, with a **Use experimental WR workload** shortcut and a player-change table. The existing volume/share model's unadjusted WR mean, P10, and P90 now drive real optimizer selections. Cash uses P10; GPP uses P90. No boom probability is invented for this model. Other players keep explicitly labeled historical projections, with optional DK fallback only when enabled. The default source remains historical.

This increment activates the existing WR model, not every research component. RB carry forecasts, rookie/new-team role assumptions, and the separate 50% injury-redistribution scenario are not activated. Player tail sums remain search heuristics, not complete-lineup percentiles.

## Eligibility and provenance
The server reloads the salary slate and current roster evidence before optimization. It joins the canonical database player ID to GSIS identity, requiring a unique same-team WR forecast, matching season/week, history strictly before the target week (run v2; see below), a future salary/schedule kickoff match, current resolved roster evidence, at least four historical games, and finite ordered ranges. "Current" means evidence evaluated at the slate's projection cutoff while that run is still the newest for the week, or live evidence under a minute old (`availabilityCurrent`); before B4 every saved slate failed the 60-second rule once its cutoff was a minute old. Current OUT exclusions remain effective. The experimental optimizer and both comparison arms require fresh offensive roster evidence and resolved expected-starter QB1 roles; unresolved quarterbacks cannot enter through fallback. No fuzzy workload matching is used.

A workload snapshot expires after 72 hours and is unusable after kickoff. Runs with no eligible workload players fail explicitly rather than claiming a workload run made entirely from fallback. Final save checks prevent workload forecasts expiring during optimization. Frozen optimizer inputs retain canonical IDs, salary/game information, availability, complete workload forecasts, source/roster/recipe digests, and fallback reasons. Showdown preserves CPT salary and scoring multipliers.

### Weekly refresh (B4, 2026-09-29; run v2)

Until 2026-09-29 the forecast was a committed **2026 Week 1** JSON that nothing refreshed, so this source could not work after week 1. It is now a weekly database-backed run:

- `ingest/nfl_dfs_target_share.py` builds one run for a target week (default: the next unstarted week) and appends it to `nfl_dfs_volume_share_runs`. It runs as the "Freeze research-only WR volume-share forecasts" step of `refresh_nfl_dfs_research.yml`, after every successful production refresh. `--dry-run` reads only (no DDL) and prints the summary; `--output PATH` also writes the full payload locally.
- History is the stored nflverse weekly rows in `ff_player_week_stats` (REG): player rows for targets and DK points, team-week rows for team attempts and targets, three prior seasons plus the current one.
- A saved slate reads the newest run for its week captured **at or before its projection cutoff** (`web/src/db/nfl-volume-share.ts`). Research runs after production, so a run applies to slates on the next projection run; the page says so when that is the reason nothing applies yet.
- The model recipe (`nfl-dfs-volume-share-v1`) is unchanged. The run container is `nfl-dfs-volume-share-run-v2`; the committed v1 JSON is retired (see git history) and no longer accepted.

**History rule (a stated product decision, not a silent relaxation).** Run v1 required every history season to precede the target season. Run v2 requires every row to precede the target **week**. The only evidence for this source is a walk-forward replay over every regular-season week of 2024–2025 (`ingest/nfl_dfs_volume_benchmark.py`, the table below): forecasts at in-season cutoffs from same-season history. Week 1, with prior-season history only, was one cutoff of that population, not the population, so in-season forecasts are what was evaluated, with the same caveat as before (previously inspected seasons, not a fresh holdout). Reverting to the old rule is one guard in `readWorkloadProjection`.

**Stored-rows caveat, measured.** `ff_player_week_stats` holds players on the current fantasy roster table, so departed players are missing from older seasons; team totals come from the complete team-week feed (all 1,632 2023–2025 team games match the digest-verified parquet exactly). The only effect is share normalisation. For the week-4 forecast, 160 WRs compared against complete 2023–2025 parquet history plus stored 2026 rows: mean difference +0.07 DK points, 57.5% identical, largest 1.37. On older replay cutoffs, where more players are missing, paired candidate means run +0.67 higher and paired MAE is within 0.02 (2024: 5.259 vs 5.237; 2025: 4.654 vs 4.635). The replay shown in the run payload therefore uses a narrower, current-roster population than the published benchmark below and is labeled as such.

A dry run for 2026 week 4 on 2026-09-29 produced 32 teams / 123 WRs; 119 pass the reader's distribution checks.

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
