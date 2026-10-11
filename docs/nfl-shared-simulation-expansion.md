# Shared leader and DFS simulation expansion

## Acceptance criteria
- Preserve original v2 forecasts and keep the fixed-dispersion baseline replayable.
- Fit workload variability using prior reconciled game counts only, expose sample
  size and fallback reasons, and do not call fitted variability calibrated.
- Export player draws with stable game/scenario/GSIS identities. Both leader and
  DFS consumers must use the SAME draw order; unresolved people remain separate.
- Document replacement scenarios with decision-time evidence. Never translate
  depth rank, unknown routes or an absent starter directly into touches.
- Score DFS through the canonical TypeScript scoring and scenario modules.
  Missing touchdowns/passing/turnovers cannot become zero-filled full projections.
- Summarize complete coherent DFS banks separately from the partial yardage bank;
  derive slate-field leader comparisons from the same full bank when available.
- Preserve salary constraints, captain multipliers and joint lineup scores.
- Evaluate interval coverage and interval score alongside leader proper scores.
  Examined historical games are development diagnostics, not untouched validation.

## Local first expansion
Empirical role dispersion, traceable shared draw export, documented role scenario
requests, interval diagnostics, partial DFS production scoring, and an adapter
for existing complete coherent DFS banks. A local example replays frozen Thursday
inputs. No refreshed availability or optimizer projection replacement is implied.

## Subsequent development targets
Routes/snaps and role-specific opponent coverage, quarterback/line/weather
conditioned efficiency, sequential score/clock scenarios, early exits and fitted
replacement mixtures. These require timestamped source coverage and chronological
comparison before enabling. No unseen source is assumed available.

## Canonical paths
Leader simulation: model/nfl_game_leaders.py; dispersion: model/nfl_role_dispersion.py.
Shared draw consumer: web/src/lib/nfl-dfs/shared-game-model.ts.
Local research CLI: web/scripts/analyze-nfl-shared-game.ts.
Full expanded event-ledger generator: model/nfl_shared_matchup_scenarios.py,
model/nfl_shared_dfs_efficiency.py and research/nfl_shared_dfs_export.py. These
separate candidates are based on pinned v5/v3 implementations. The registered
originals and their code hashes remain intact. This engine has separate inputs and
marginals; it is not a drop-in completion of the yardage-only engine.
DK scoring: web/src/lib/nfl-dfs/scoring.ts; strict complete scenario validation,
lineup scoring and legal roster checks: scenarios.ts and lineups.ts.

## Reproduce the local example

```powershell
python -m research.nfl_game_leaders forecast --input CAPTURE.json.gz --request REQUEST.json --draws 5000 --role-dispersion empirical --export-draws --output SHARED.json.gz
# From web/:
node --import tsx scripts/analyze-nfl-shared-game.ts partial ../SHARED.json.gz ../PRODUCTION.json
```

For complete DFS, freeze the full generator's keyword inputs (slate, both team
forecasts, history, team_rows, identities, baseline_means, source_manifest,
decision_at and draws) and run `python -m research.nfl_shared_dfs_export --input
INPUTS.json --role-evidence ROLE_FIT.json --output FULL.json.gz`. Then use the
same TypeScript CLI with `complete` mode. Optional `replacement_roles` and
`replacement_evidence` belong in the frozen inputs; complete shares must include
OTHER. Role fits record their source digest and training cutoff.

The full generator retains actual source capture time in bank metadata. Strict
DFS scoring rejects later-than-decision captures. Explicit retrospective export
can produce an audit artifact; it does not make that artifact eligible as a
pregame bank. Do not backdate its capture to pass the scoring gate.

## Current evidence
The frozen Thursday example covers all four current-season games for both teams.
No availability refresh was performed. The 8-game examined Week 4 comparison is
mixed: first-choice rushing/receptions/receiving-yard results are unchanged;
total-yard choice credit is 4/8 fixed versus 5/8 empirical. Proper probability
scores do not consistently improve. This small diagnostic is not a promotion gate.
Actual expanded full DFS generation was checked on a synthetic mechanical fixture,
not presented as a current-game projection. Tests cover scoring, per-draw bonuses,
identity/coverage failures, correlations, captain/legal lineup constraints, event
ledger conservation and preservation of original registered study hashes.
