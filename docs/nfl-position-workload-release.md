# Combined experimental position workload source

NFL DFS → **Position workload (experimental)** now supports QB, RB, WR and TE in one optimizer run. Four independent switches control which candidate projections are used. QB and WR start enabled in the new UI; RB and TE start disabled and can be enabled individually. The app's default source remains historical. Disabled positions or missing candidates use labeled historical projections, then optional DK average fallback. DST continues to use the historical fallback in this source; the separate calibrated QB/DST source remains available.

## Actual models connected

| Position | Existing candidate used | Historical 2025 point MAE | Historical 2025 range error |
|---|---|---|---|
| QB | Pinned opportunity calibration using recent pass attempts | 7.055 → 6.954 | 30.578 → 29.740 |
| RB | Pinned opportunity calibration using recent carries + targets | 4.733 → 4.664 | 22.092 → 24.223 |
| WR | Existing team pass-volume and target-share forecast | 4.706 → 4.515 | 21.277 → 24.100 |
| TE | Pinned opportunity calibration using recent targets | 3.692 → 3.650 | 17.438 → 18.683 |

Lower error is better. QB/RB/TE are the existing shadow-study opportunity regressions, **not** the separate full team-budget/stat simulator. Their prior-opportunity reference is labeled historical rather than displayed as a new carry/target forecast. WR retains its separately validated data adapter. These previously inspected cohorts differ by position and do not establish a contest edge. RB/WR/TE range quality worsened; there is no default promotion or automatic injury redistribution.

## Optimizer behavior and integrity

- Cash uses the selected candidate P10; GPP uses its P90. QB/RB/TE retain their saved candidate boom probabilities; WR does not acquire an invented bonus. Mean projections and displayed player-tail sums use the same selected source. CPT scoring applies the same 1.5 multiplier.
- Each position can be enabled independently, including an RB-only or TE-only experiment. All-disabled or malformed controls fail explicitly. A run with no usable candidates for enabled positions fails rather than claiming an all-fallback experiment.
- Server actions reload candidate and roster evidence. Existing canonical ID, team/opponent, target week, pinned study/recipe, historical cutoff, freshness, salary kickoff and ordered-distribution guards remain. Current QB1 evidence is required for quarterbacks. Missing or expired forecasts fall back visibly. Expiration/kickoff is rechecked before saving.
- The new reader permits explicit experimental access to pinned RB/TE recipes without changing `calibrated-release.json` or opening RB/TE in the older calibrated source.
- Saved optimizer settings now include the four position switches. Saved inputs retain both WR and position-study forecasts, identifiers, means, tails, boom probability, capture/kickoff times, recipe/study provenance, availability and fallback reasons.
- Old settings without `workloadPositions` remain WR-only for reproducibility. New UI settings enable QB+WR. The source is versioned `nfl-dfs-ilp-v4-position-workload`.
- The source table shows candidate/historical/DK fallback or exclusion reasons per player. Changing position switches after generation does not relabel saved lineups; the app requests regeneration. Matched comparisons display the controls used when captured.

## Refresh and evaluation

QB/RB/TE read the existing daily shadow forecast in `nfl_dfs_shadow_predictions`. WR still uses the dated volume-share report documented in [the WR release](nfl-workload-optimizer-release.md). All forecasts expire after 72 hours or kickoff. No new provider or live scheme feed is introduced.

The matched comparison freezes one common pregame pool, controls, salaries, and zero randomness for historical and selected workload sources. Up to five lineups per arm are saved; postgame performance remains pending. No historical lineup replay is fabricated without verified salary pools.

## Verification

`test:nfl-position-workload` covers pinned QB/RB/TE validation, stale/wrong-week/identity/matchup/history failures, legacy calibrated isolation, independent and combined cash/GPP choices, Showdown CPT behavior, historical fallback, controls, unchanged inputs, and deterministic replay. Existing WR, calibrated and workspace regression suites also pass. `audit-nfl-position-workload.ts` reads Week 1 snapshot coverage; `verify-nfl-workload-pair.ts` audits saved identical inputs/settings, actual per-position candidate use, legal rosters and QB starter evidence.

Live browser/database verification on 2026-09-06 found 22 eligible QB, 70 RB, 71 WR and 69 TE candidates in the saved salary pool. Historical run `491561c6-ed81-4806-8d05-bfebed02a0dc` and all-position workload run `293d39e4-5a3c-4f0f-9392-e4dfe2289f26` each saved five legal lineups. The read-only audit confirmed identical frozen inputs and controls, changes in all five lineups, and actual workload usage across all four positions: 2 QB, 7 RB, 8 WR and 2 TE roster slots. Every selected QB had expected-starter evidence. These counts are a functional test, not measured performance.
