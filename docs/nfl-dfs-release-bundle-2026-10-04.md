# NFL DFS integration handoff — October 4, 2026

Branch: `codex/nfl-dfs-release-bundle`, based on `origin/main` at `901bf8f4`.

## Included work

1. Automatic matchup-aware DST and kicker forecasts for Our projections, Build
   panel simplification, opposing-DST lineup rules and ownership import format
   validation. Sources: `b9c01032`, `976bb739`, `6e0cbb9b`, `cd16a6bd`, `cf0f473f`.
2. Standalone fitted ownership forecasting, frozen evidence for 629 players and
   weekly grading tools. Sources: `72c402a3`, `d94f815c`.
3. Showdown prior v3 and ownership evaluation. Source: `8f5f7263`, with required
   field-structure parser, contest import and schema support from `039a6135`.

Individual commits remain separate. Documentation conflicts retained the
ownership registration and added the field-structure and Showdown findings.
The top-projected coverage and opportunity-signal fixtures supply valid game
identity, with their DST in a second game, satisfying the existing Classic
multi-game requirement.
No primary-checkout edits were incorporated.

## Product behavior and limits

- An offensive Captain plus two offensive teammates cannot coexist with the
  opposing DST. Both formats reject four opposing offensive players with a DST.
  Generation, saved-lineup export and pre-export QA apply the rules.
- Comparison ownership requires matching Classic/Showdown format evidence.
  Legacy imports without that evidence no longer override the prior.
- Build has no new experimental controls. Special teams candidates apply
  automatically when complete; missing candidates use named historical
  fallbacks. Older projection snapshots need refresh for candidate coverage.
- The fitted ownership workflow remains standalone. Its frozen estimates are
  auditable but are not wired to production lineup generation by this release.
  They declare uncalibrated status and keep validated ownership disabled.
- Prior v3 changes Showdown value weighting only; it remains heuristic. This
  release does not promote it to validated leverage or duplication modeling.
- The saved Sunday ownership evidence retains its original availability.
  Production use needs a fresh pregame snapshot and subsequent calibration.
- Player ceilings remain individual P90s. Joint scenario ranking is not
  promoted. The ownership evaluation harness uses a simplified research
  optimizer; it is not evidence that the production solver improves returns.

## Local verification

- 75 Python tests passed across special teams, projection availability,
  fitted ownership, Showdown ownership evaluation and field structure.
- 24 web suites passed: special teams, Showdown legality, workspace, QA,
  balanced plan, archetypes, baseline, ownership capability, ownership prior,
  top projected coverage, chalk leverage, GPP leverage, build form, cash,
  Showdown salary floor, punts, exposures, salary policy, portfolio selection,
  backtest, Captain availability, Captain minimums, workload optimizer and
  opportunity signals.
- TypeScript check passed. Targeted lint has zero errors and one existing
  `openSaved` effect dependency warning in the client.
- Full web lint passed with zero errors and 63 warnings from the existing
  repository warning backlog.
- Six sealed ownership artifacts verified. Refit coefficients and all 629
  saved player forecasts reproduced exactly; validation remains disabled.
- Showdown evaluation command loads and its help command succeeds, including
  the parser dependency. No database or production writes were run.

Merge, production deployment and projection refresh are separate release steps.
