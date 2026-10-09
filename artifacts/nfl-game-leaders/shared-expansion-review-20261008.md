# Shared leader / DFS first expansion

Local development only. Saved Thursday inputs retain the original 2026-10-08
23:14:08 UTC decision; this is a replay, not newly verified availability.

Implemented: prior-only empirical role concentration with explicit fallback;
common role-share sampler; aligned per-person draws; timestamped complete-field
replacement assumptions; interval coverage and proper interval scores; canonical
partial DFS production scoring and complete event-bank/lineup consumer. Separate
expanded full DFS generator preserves the registered original code hashes.

The 5,000-draw leader example is `shared-v3-example-20261008.json.gz`.
Its component report is `shared-v3-dfs-production-20261008.json`. Common fitted
role reports are in `shared-v3-role-evidence-20261008.json`. The complete DFS
mechanical fixture is synthetic, not a real-game projection; no optimizer switch
or deployment occurred. The local page is `/nfl/game-model`.

Measured role concentrations: carries 7.07; targets 29.15, from 128 team-season
groups / 1,760 game-team observations. Injuries and changing roles contribute to
these estimates; conditional role-state calibration remains missing. Combining
pooled dispersion with replacement assumptions can double-count uncertainty.

The eight examined Week 4 games do not establish improved predictive accuracy.
Rushing/receptions/receiving-yard first-choice results are unchanged; total-yard
choice credit is 4/8 fixed versus 5/8 empirical. Probability scores are mixed.
Pooled range coverage includes zero forecasts and dependent player-game rows;
it is not position/workload-cohort calibration. Original records are preserved.

Verification: 1,741 Python tests passed, two expected skips. Three final focused
source-boundary/replacement/event-ledger checks passed after the final metadata
change. Typecheck, focused lint, canonical DFS scoring tests and local page HTTP
checks passed. Registered study hash verification passed without modifying its
original pinned modules. Remaining source disagreements, routes/snaps, role-specific
defense, sequential scripts, early exits, fitted replacement mixtures, full real
DFS input freeze and forward/untouched calibration remain in the backlog.
