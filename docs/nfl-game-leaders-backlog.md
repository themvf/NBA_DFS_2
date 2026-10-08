# NFL game-leader model: open fixes

Updated: 2026-10-08. These items are recorded, not implemented or validated.
Model contract: [nfl-game-leaders-model.md](nfl-game-leaders-model.md).

## High priority: recent-history reconciliation and forecast quality

- [ ] **Correct receiving-yard accounting on fumble plays.**
  Evidence: `2026_03_BAL_DAL`, play `2346`, Q3 10:40, Jake Ferguson.
  The current air-yards-plus-YAC calculation yields a game total of 24 receiving
  yards, while the captured published box total is 23. Resolve the actual player
  stat credit using raw source stat fields; do not assume net play gain is always
  an individual player's receiving-yard credit. Preserve catch, target, fumble,
  and yardage as separate facts. Acceptance: this player's five game statistics
  reconcile, with a regression check for this play and other fumble cases.

- [ ] **Attribute receiving yards earned after a lateral.**
  Evidence: `2026_04_DAL_HOU`, play `3957`, Q4 2:34. Dalton Schultz catches
  the pass for 2 yards and laterals to David Montgomery for another 19 yards.
  Our captured participant list contains Schultz as receiver only; Montgomery's
  published game box credits 19 receiving yards with zero catches and targets.
  Extend capture/attribution to retain lateral recipient identity and official
  yardage credit. Acceptance: credit the original catch once, retain Montgomery's
  19 receiving yards without inventing a target or reception, and reconcile both
  players and the team. Check multiple laterals and unresolved identity handling.

- [ ] **Keep verified workload history separate from unresolved event detail.**
  Today a discrepancy for any player excludes the whole game from all training.
  Both Dallas Weeks 3 and 4 were excluded even though Lamb's box totals matched
  the reconstructed events, including Week 4's 17 catches and 189 yards.
  Acceptance: independently reconciled full-field box totals can supply workload
  history under an explicit source contract; unresolved events remain excluded
  from per-touch distributions until repaired. No fabricated events or forced
  reconciliation. Show the source and sample size for each calculation.

- [ ] **Block unsupported recommendations from incomplete recent history.**
  The saved Thursday forecast continued after dropping Dallas's latest two
  games. Acceptance: record expected, included, and excluded recent game IDs,
  per-player/per-stat discrepancies, and effects on workload inputs. Define and
  document the adequacy gate before implementation; failed coverage must be
  visible in the displayed result and prevent an actionable recommendation or
  edge claim. Never silently substitute older games for missing current games.

- [ ] **Audit the other reconciliation failures and rerun evaluation.**
  These findings explain two Dallas exclusions, not all 94 original historical
  reconciliation failures or all prediction errors. Acceptance: categorize the
  remaining failures with reproducible evidence; preserve original captures,
  predictions, and scores; produce separately versioned corrected forecasts
  and evaluations with identical game scope, tie rules, and baseline comparison.
  Any already examined weeks are descriptive reruns, not new untouched holdouts.
  Passing stat reconciliation alone does not establish model accuracy.

## Evidence to retain

- Source capture: `artifacts/nfl-game-leaders/full-capture-20261008.json.gz`.
- Original Thursday forecast: `artifacts/nfl-game-leaders/final-week5-forecast.json`,
  game `2026_05_TB_DAL`, decision time `2026-10-08T12:34:22.681692+00:00`.
- Full 64-game results: `artifacts/nfl-game-leaders/weekly-review-all-games.json`.
- Failed-model review: `artifacts/nfl-game-leaders/model-quality-review.md`.

Each checked item must link its verification evidence. Track release status
separately from implementation and model validation.
