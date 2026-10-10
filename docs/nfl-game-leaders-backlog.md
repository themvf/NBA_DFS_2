# NFL game-leader model: open fixes

Updated: 2026-10-09. Checked items have local implementation and verification;
release status is separate. The remaining primary-source discrepancies are open.
Model contract: [nfl-game-leaders-model.md](nfl-game-leaders-model.md).

## High priority: recent-history reconciliation and forecast quality

- [x] **Correct receiving-yard accounting on fumble plays.**
  Evidence: `2026_03_BAL_DAL`, play `2346`, Q3 10:40, Jake Ferguson.
  The current air-yards-plus-YAC calculation yields a game total of 24 receiving
  yards, while the captured published box total is 23. Resolve the actual player
  stat credit using raw source stat fields; do not assume net play gain is always
  an individual player's receiving-yard credit. Preserve catch, target, fumble,
  and yardage as separate facts. Acceptance: this player's five game statistics
  reconcile, with a regression check for this play and other fumble cases.

- [x] **Attribute receiving yards earned after a lateral.**
  Evidence: `2026_04_DAL_HOU`, play `3957`, Q4 2:34. Dalton Schultz catches
  the pass for 2 yards and laterals to David Montgomery for another 19 yards.
  Our captured participant list contains Schultz as receiver only; Montgomery's
  published game box credits 19 receiving yards with zero catches and targets.
  Extend capture/attribution to retain lateral recipient identity and official
  yardage credit. Acceptance: credit the original catch once, retain Montgomery's
  19 receiving yards without inventing a target or reception, and reconcile both
  players and the team. Check multiple laterals and unresolved identity handling.

- [x] **Keep verified workload history separate from unresolved event detail.**
  Today a discrepancy for any player excludes the whole game from all training.
  Both Dallas Weeks 3 and 4 were excluded even though Lamb's box totals matched
  the reconstructed events, including Week 4's 17 catches and 189 yards.
  Acceptance: independently reconciled full-field box totals can supply workload
  history under an explicit source contract; unresolved events remain excluded
  from per-touch distributions until repaired. No fabricated events or forced
  reconciliation. Show the source and sample size for each calculation.

- [x] **Block unsupported recommendations from incomplete recent history.**
  The saved Thursday forecast continued after dropping Dallas's latest two
  games. Acceptance: record expected, included, and excluded recent game IDs,
  per-player/per-stat discrepancies, and effects on workload inputs. Define and
  document the adequacy gate before implementation; failed coverage must be
  visible in the displayed result and prevent an actionable recommendation or
  edge claim. Never silently substitute older games for missing current games.

- [x] **Audit the other reconciliation failures and rerun evaluation.**
  These findings explain two Dallas exclusions, not all 94 original historical
  reconciliation failures or all prediction errors. Acceptance: categorize the
  remaining failures with reproducible evidence; preserve original captures,
  predictions, and scores; produce separately versioned corrected forecasts
  and evaluations with identical game scope, tie rules, and baseline comparison.
  Any already examined weeks are descriptive reruns, not new untouched holdouts.
  Passing stat reconciliation alone does not establish model accuracy.

- [ ] **Resolve nine remaining primary PBP/player-box disagreements.**
  There are 14 discrepant player statistics in nine games from 2023–2025.
  Final primary-field aggregation itself disagrees with the published boxes;
  these are not repaired by adding fictitious events or redistributing yards.
  Verified boxes remain in workload history, but the complete event games stay
  quarantined. Four historical 2026 Week 1 forecasts are blocked by this gate.
  Evidence: `final-remaining-repair-audit-20261008.json` and
  `final-repaired-reconciliation-20261008.json` under the artifact directory.

## Completed repair evidence

- `research/nfl_game_leaders_source.py`: primary stat-credit capture and
  independently checked full-field box workload. Canonical joins, original
  descriptions, source digests and capture times are preserved.
- `tests/test_nfl_game_leaders.py`: fumble, lateral, penalty/replay final status,
  temporal capture, duplicate/canonical identity, separate workload, forecast
  coverage and publication gate regressions. 25 focused checks passed.
- Full Python suite: 1,732 passed, two expected skips; `repair-full-suite.xml`.
- `final-repaired-capture-20261008.json.gz`: 880 verified workload games,
  871 reconciled event games. All current 2026 games reconcile, including both
  formerly excluded Dallas games.
- `final-repaired-thursday-forecast-20261008.json`: fresh forward request,
  30,000 draws, all four games for both teams, no excluded recent event games.
- `final-repaired-evaluation-2026.json`: 60 graded, four blocked; compare only
  matching game scope via `repair-matched-evaluation-20261008.json`.
- The local page updates Thursday and the historical review; the other 14
  saved weekly forecasts retain their earlier capture times and show that recent
  coverage was not recorded. No deployment has occurred.

## Evidence to retain

- Source capture: `artifacts/nfl-game-leaders/full-capture-20261008.json.gz`.
- Original Thursday forecast: `artifacts/nfl-game-leaders/final-week5-forecast.json`,
  game `2026_05_TB_DAL`, decision time `2026-10-08T12:34:22.681692+00:00`.
- Full 64-game results: `artifacts/nfl-game-leaders/weekly-review-all-games.json`.
- Failed-model review: `artifacts/nfl-game-leaders/model-quality-review.md`.

Each checked item must link its verification evidence. Track release status
separately from implementation and model validation.


## Shared simulation expansion: first-tranche status on 2026-10-08

- [x] Separate empirical workload-variability candidate with prior-only inputs,
  finite-count noise correction, sample counts and explicit fallback.
- [x] Export joint individual draws for leader and DFS consumers with matching
  scenario IDs; preserve distinct unresolved people before leader grouping.
- [x] Require timestamped evidence for explicit full-field replacement shares.
- [x] Add interval coverage and interval score to result grading/development reports.
- [x] Score partial production and complete supplied DFS banks through the canonical
  scorer, legal-lineup validation and captain scoring; no automatic optimizer switch.
- [x] Separate expanded full DFS event-model copies from registered studies so
  prior validation contracts remain intact.
- [ ] Capture routes/snaps with source/time/identity coverage and matchup role data.
- [ ] Fit replacement mixtures, early exits and shared player-role changes.
- [ ] Add depth/YAC and game-state-conditioned efficiency and opportunities.
- [ ] Validate ranges by pregame workload cohort, position and game-level uncertainty;
  retain zero-forecast coverage separately rather than presenting pooled calibration.
- [ ] Freeze complete current-game DFS inputs and run the expanded full candidate;
  the Thursday view currently shows partial production only.
- [ ] Add ownership/contest-field evaluation before treating salary targets as
  tournament-selection guidance. See the expansion contract for all paths.

## Current joint candidate and weekly-page gaps, 2026-10-09

This section supersedes the first-tranche implementation status above. Code
completion, data availability and predictive qualification are separate.

- [x] Add pooled depth-conditioned gains, sourced exit fitting, non-exit role
  fitting, remaining-work redistribution, guarded market reweighting, complete
  DFS weight transfer and supplied-field contest scoring. Local evidence is in
  `nfl-joint-outcomes-local-review.md` and the final verification manifest.
- [x] Fix weekly percentage rounding and floating-point range display; explain
  the original weekly model versus the separate expanded replay on the page.
  Verified the DET/ARI rendered route, TypeScript type check and page lint.
- [x] Audit DET/ARI defensive-style and game-state field coverage. Current 2026
  Weeks 1-4 have zero populated scheme fields on 316 dropbacks; score and clock
  exist on all 515 run/pass plays. The source map records denominators, timing
  constraints and evidence paths.
- [ ] **Connect the joint candidate to a versioned weekly per-game publisher.**
  The expanded research engine currently has a separate single-game replay.
  Acceptance: explicitly selectable original/candidate forecasts, identical
  frozen requests and field scope, source/fit digests and no silent model switch.
- [ ] **Fit opportunities to score and clock state.** Data already exists.
  Acceptance: leading/tied/trailing and remaining-time effects are fitted on
  earlier games; both teams share simulated states; opportunity conservation and
  opponent-adjusted held-out joint scores are checked. Aggregate workload scaling
  must not be described as a sequential game script.
- [ ] **Capture current defensive scheme and role matchups.** Missing current
  man/zone, coverage, blitz and pressure observations need source coverage, not
  invented values. Acceptance: canonical unique play joins, capture/label times,
  missingness by season and role, current data support and pooled historical fits.
  Prior-season style is an explicitly uncertain prior, not a current observation.
- [ ] **Fit scheme-to-role response where coverage permits.** Acceptance:
  distinguish receiver depth/role and RB/QB usage; regularize sparse interactions;
  compare registered variants with the existing broad opponent adjustment.
- [ ] Supply adjudicated historical exit exposure, matched NFL alternate ladders,
  ownership labels and realistic contest fields; then execute registered
  incremental comparisons. Those consumers exist; their fitted production
  evidence and qualification are outstanding.
- [ ] Add sourced routes/snaps, offensive-line availability, weather and QB/coach
  transitions only with decision-time coverage and registered incremental tests.
- [ ] Complete tail/range and joint-ranking qualification on a genuinely untouched
  or forward population. Already examined weeks cannot be recycled as a holdout.
