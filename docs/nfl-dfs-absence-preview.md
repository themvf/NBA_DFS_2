# Verified absence scenario preview

NFL DFS (`/dfs/nfl`) now includes a WR scenario panel below availability. Select a receiver and a QB/WR/TE teammate from the same salary-slate team/game. The server reloads current roster and official injury evidence for each request; browser flags cannot authorize a scenario.

A fresh, game-matched official inactive report is required. FantasyPros observations with unresolved update timezone, questionable designations, stale evidence, excluded receivers, and started games cannot trigger the preview. Current roster evidence must pass the existing roster resolver. This does not infer that an eligible receiver is fully healthy or has a guaranteed workload.

The historical match uses unique exact normalized name and position, then the current team's 2025 games only. It does not fuzzy-match identities or transfer old-team roles. Rookies and players without sufficient same-team splits receive an explicit unavailable result. Both players must appear in the uploaded salary slate; expanding the selector to the full roster is still needed.

The calculation reuses the existing empirical workload split: at least six scored games and three observed games in each teammate state; matching absent games receive weight n/(n+8), with the remainder on the player's baseline. Zero recorded participation requires complete personnel coverage. Targets, mean and P10/P50/P90 can rise or fall. Inclusive 2x/3x/4x probabilities sum the weights of complete game scores meeting multiple * current salary / 1000. These are historical sensitivity estimates at a current Classic/FLEX salary, not validated forward probabilities or historical salary backtests. No team-budget or causal redistribution model is claimed.

The prior replay failed its accuracy gate, so optimizerEnabled remains false. The preview cannot write optimizer projections, custom overrides, or roster state. QB/scheme/opponent changes and simultaneous absences are not modeled. Next model work requires timestamped pregame cohorts and a team-conserving target-allocation model evaluated on point error, interval quality, and salary-hit calibration before activation.

JSON downloads retain both players' current evidence, historical source hashes, identities, salary, evaluation time, version, result and digest. Downloading is explicit; the server does not store a separate scenario run. Source hashes identify the bundled historical dataset, while fixed inputs and decision time reproduce the pure calculation.

Validation: npm run test:nfl-absence-preview, npm run test:nfl-workload, npm run test:nfl-dfs-workspace, game-availability tests, targeted ESLint and production build passed. Tests cover source/week/time/kickoff rejection, identity ambiguity, new-team isolation, incomplete participation, inclusive salary thresholds, downward scenarios and unchanged inputs.

Browser verification resumed the existing 719-player Classic slate, displayed the panel, filtered Zay Flowers to BAL teammates, and rejected a Rashod Bateman absence request without an official report. The successful numeric path was tested with synthetic evidence; no live official inactive observation was fabricated or imported for testing.
