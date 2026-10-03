# NFL Classic GPP: air-yard matchup buildout

**Status:** Proposed implementation contract, 2026-10-03. No projection or optimizer default is approved by this document.
**Scope:** DraftKings NFL Classic large-field GPP. The contracts can later support YAC and goal-line signals after separate validation.
**Parent contracts:** [NFL DFS specification](./nfl-dfs-spec.md), [GPP portfolio specification](./nfl-gpp-portfolio-improvement-spec.md), and [Team Identity source map](./nfl-team-identity-source-map.md).

## Decision and current baseline

Build three ordered releases: (1) a point-in-time air-yard matchup feature, (2) an experimental coherent-scenario portfolio arm that consumes it, and (3) a walk-forward contest backtest that decides whether to promote it. A defense's raw three-game target-air-yard total is a descriptive display value, not a point boost or automatic lineup rule.

Current main already has AIR_VOLUME chips from prior receiver-role PBP, an optional Classic GPP minimum of one selected chip per lineup, a heuristic ownership prior, and manual QB + one/two WR-or-TE and opponent RB/WR/TE bring-back constraints. The Classic GPP form defaults to QB + one pass catcher + bring-back. The portfolio selector can use aligned joint score banks, but live R7 selection still awaits an approved model-generated bank. The coherent matchup simulator is separately registered research, not a production-approved adapter. Earlier opponent-points and pass-volume screens did not establish a durable held-out lift. This feature must earn promotion independently.

### Non-goals

- No fixed fantasy-point or percentage bonus for facing a top-ten air-yard defense.
- No inference that a chip, missing ownership, or heuristic ownership proves leverage.
- No universal bring-back requirement in experimental candidate generation. Explicit user settings remain binding.
- No contest finish probability or ROI without qualified field and payout inputs.
- No post-cutoff PBP corrections, odds, injury news, ownership, or results as pregame inputs.

## Shared source and audit contract

The unit is a saved Classic slate at its linked projection run cutoff. Join the slate player's FF ID to the same-season FF player, then GSIS ID to PBP participants. Join receiver roles to plays on both game ID and play ID, deduplicating role-play rows before aggregation. For defense, use the defending team on each unique pass play. Use the canonical season-game/team bridge and verified abbreviation mapping: nflverse LA is the Rams' DK LAR. Never join a team by display name. Read the Team Identity source map before changing these joins.

Filter to regular-season games strictly before the slate week, with play-label time at or before the projection cutoff. Keep the current rolling three-week player chip for display. A longer defense window or prior-season baseline may enter the model only when available at cutoff. Target air yards include incomplete passes; caught air yards require a completed target. Persist source versions, games, targets, null coverage, cutoff, and the selected market quote ID. Select spread, total, and moneyline from timestamped odds captured before cutoff and kickoff, not current convenience fields.

PBP participants lack their own as-of timestamp. A historical replay reconstructed from today's participant table is not point-in-time exact. Freeze resolved feature rows and digests in live optimizer inputs. For a promotion backtest, require frozen pre-lock participant/source snapshots. Otherwise mark the slate historical_reconstruction_only and exclude it from promotion evidence. Unknown identity, opponent coverage, and odds remain null with reasons, never zero.

The frozen NflAirMatchupEvidence contract contains:

| Group | Required values |
| --- | --- |
| Identity | upload/run IDs, season/week, DK/FF/GSIS IDs, player and opponent teams, game ID, cutoff |
| Player usage | games, targets, target and caught air yards, deep targets, team target-air-yard share, target share, air yards per target |
| Defense | games, pass targets faced, total target air yards faced, target air yards per target, caught air yards faced, coverage |
| Context | projected team pass attempts, as-of spread/total/moneyline and quote time, availability, ownership capability/source |
| Derived | regressed matchup-depth factor, uncertainty, feature/training versions, source digests, state/reason |

## Step 1 — Build and audit the matchup feature

**Owner boundary:** read-only source query and pure feature calculation. Add a versioned DB reader beside the player-signal query, a pure calculator under web/src/lib/nfl-dfs, and a frozen field on the workspace/run snapshot. Update the source map if joins or sources change.

1. Calculate player target share and mean air yards per target from prior games. Keep total target-air-yard volume and deep-target count as separate explanatory fields; do not multiply both into one score.
2. Measure defense air yards per pass target faced and target volume faced separately. Regress depth toward a league or available prior-season baseline using effective sample size chosen only on training weeks. Three-game totals alone never rank matchup strength. Show both total and per-target values in the UI.
3. Form expected target-air-yard opportunity from projected team attempts, player target share, and player target depth. Bound the opponent adjustment and widen uncertainty for short samples. Fit or freeze weights, shrinkage, and caps before holdout. Audit overlap with the existing market/team and opponent projection terms to avoid double-counting. In Step 1 the feature is shadow evidence only: no change to mean, P90, chip threshold, or optimizer score.
4. Show prior player targets/air yards, opponent total/per-target allowance, sample size, as-of market context, and an experimental or missing-data state in the player explanation. Retain the existing air-volume chip definition.

**Acceptance:** identical cutoff gives identical output; role-play and season joins are unique; no future week or later play label enters; Rams mapping is verified; 32 defense game counts reconcile; incompletions contribute target but not caught air yards; more targets faced do not automatically imply a better normalized matchup; missing data earns no adjustment; a saved run reproduces its exact evidence.

## Step 2 — Use it in GPP scenarios and portfolio selection

**Owner boundary:** separate feature-flagged air_matchup_scenario_v1 arm. Production projections and manual builder behavior stay unchanged until Step 3 promotion. Consume frozen Step 1 evidence and an approved joint-scenario adapter; never create a synthetic additive P90.

1. Draw game pace and both teams' scoring/pass-attempt budgets conditional on the as-of moneyline, spread, and total. Allocate team targets among eligible players with shared draws. Sample target depth, completion, catch yardage/YAC, and touchdowns conditional on role and depth. Receiving completions/yards reconcile with the QB passing line. Preserve plausible opponent response without forcing both teams to erupt. Independent player marginals cannot be labeled coherent.
2. Generate a broad legal candidate set covering QB solo, QB + one WR/TE, QB + two WR/TE, and with/without an opposing RB/WR/TE. User stack settings, locks, exclusions, exposure limits, salary policy, availability, and DK legality are hard constraints. When a user requires a stack/bring-back, every candidate honors it; otherwise the scenario selector may choose the mix. A favorable air-yard chip never implies a bring-back.
3. Score candidates on a common selection bank with the existing portfolio selector; choose lineups for marginal portfolio contribution under overlap, exposure, and duplication rules; evaluate the selected set on an independent bank. Use a score-threshold objective without a qualified contest field. Top-percent/payout objectives require the existing field and ownership capability gates. Unknown ownership is not a low-owned bonus; the current prior stays labeled heuristic.
4. Review UI shows QB stack, bring-back, air-matchup players, scenario contribution, relevant game scripts, ownership source, and reasons for candidate rejection. Immutable run metadata records feature/model versions, market quote IDs, source digests, seeds, bank IDs, and selection/evaluation sizes.

**Acceptance:** game events conserve targets and fantasy scoring; QB–WR/TE shared outcomes show expected dependence in diagnostics; lineup quantiles come from complete lineup draws; selection/evaluation banks are disjoint; missing or weak matchup evidence does not change production; user-required stack/bring-back is never relaxed; scenario-bank failure is explicit, not silent additive fallback; same snapshot/settings/seeds reproduces lineups and metrics.

## Step 3 — Walk-forward test and promotion decision

**Owner boundary:** extend the existing R8 harness with an air-matchup experiment manifest. Before inspecting holdout outcomes, freeze eligible slates, feature versions, arms, objective, minimum useful improvement, uncertainty method, and exclusion rules.

Use immutable pre-lock salaries, roles, PBP/participant facts, odds, availability, projections, and ownership estimates. Actual scores and field lineups are evaluation-only. Split chronologically by slate/week; all contests from one slate stay together. Replay with equal lineup count, salary/entry budget, candidate budget, locks, exposure limits, and comparable seeds. A participant history reconstructed without a pre-lock snapshot counts only toward coverage reporting.

Required paired arms:

| Arm | Purpose |
| --- | --- |
| Current Classic builder and manual stack/bring-back defaults | Production comparator |
| Same builder with air chips only | Isolate the construction constraint |
| Coherent scenarios without air matchup | Isolate scenario selection |
| Coherent scenarios plus air matchup | Measure incremental matchup value |
| Same scenario arm with optional versus required bring-back | Test stack-policy interaction |
| Same arms with heuristic ownership off/on where capability permits | Detect ownership artifacts |

The primary decision metric is pre-registered best-of-N portfolio top-1% finish rate when complete contest field/results exist. Otherwise use a pre-registered lineup score threshold and label it a proxy. Report top-0.1%, best lineup score/rank, candidate recall, legal/export-ready rate, mean/tail calibration, air-target and completion calibration, stack/bring-back mix, salary-left/overlap, ownership calibration, and errors by role and opponent sample size. Net payout/ROI and duplication require complete field, fee, payout/tie rules, and a qualified field model; omit them otherwise. Use paired slate-cluster uncertainty intervals and leave-one-slate/matchup-out sensitivity.

Promote a default only if all source, leakage, and invariant gates pass; the holdout primary metric exceeds the pre-registered useful-improvement threshold with its slate-cluster interval above zero; legal candidate recall is not materially worse; and no single slate or parameter explains the gain. If eligible frozen slates are too few for a useful interval, retain labeled shadow research. Publish negative as well as positive ablations. Roll out as opt-in experiment first, then feature-flagged default only after approval; saved run versions preserve old behavior.

## Implementation map

| Step | Primary code touchpoints | Required fixture/report |
| --- | --- | --- |
| 1 | New web/src/db/nfl-dfs-air-matchup.ts and web/src/lib/nfl-dfs/air-matchup.ts; web/src/app/dfs/nfl/actions.ts and player-explanation-panel.tsx; source map | Hand-inspected game/play join, 32-defense reconciliation, same-cutoff and later-label rejection |
| 2 | Research adapter in model/nfl_matchup_scenarios.py and research/nfl_matchup_scenario_export.py; web/src/lib/nfl-dfs/scenarios.ts and portfolio-selection.ts; Classic candidate generation and review UI | Frozen joint-bank score/target-conservation fixture, stack-policy matrix, independent-bank replay |
| 3 | Extend web/scripts/test-nfl-gpp-backtest.ts with a versioned experiment manifest and research report | Frozen train/holdout slate list, paired ablation output, uncertainty and promotion memo |

Keep the new feature and scenario settings optional and versioned in saved runs. Ship Step 1 independently as visible shadow evidence. Step 2 depends on Step 1's frozen evidence and an approved joint bank. Step 3 can run research arms before Step 2 is user-facing, but no default changes before its promotion gate passes.

## Delivery sequence

| Milestone | Reviewable output | Gate |
| --- | --- | --- |
| M1 feature | Versioned query/calculator, frozen snapshot, player explanation, reconciliation fixture | Join, as-of, coverage, and leakage checks |
| M2 scenario | Shadow joint bank, diverse candidates, selector and review evidence | Conservation, dependence, reproducibility, failure checks |
| M3 backtest | Pre-registered manifest, walk-forward ablations, uncertainty and promotion memo | Promotion gate above or explicit retain-as-experiment decision |

The first October 4, 2026 report may describe Weeks 1–3 observations and generate shadow lineups. One slate cannot establish a profitable adjustment or a universal bring-back rule.

### Initial matchup evidence and sensitivity check

The first implementation exposes a versioned, player-level shadow record: player and team target/air-yard denominators, prior-game counts, opponent raw and regressed air yards per target, projected team pass attempts, neutral and matchup target-air-yard opportunity, and the selected as-of market quote. The saved optimizer input snapshot retains it. A deterministic paired simulation varies team attempts by an assumed 15% dispersion, allocates player targets binomially, and varies target depth with an assumed lognormal dispersion. It compares the same draws under neutral and bounded opponent-depth factors for leading/neutral/trailing attempt budgets (0.9/1.0/1.1). These assumptions are diagnostic; no fantasy points, lineup score, ownership, ROI, or full game dependence are inferred. The scenario is not the coherent joint-bank Step 2 arm and does not change production projections or optimizer rankings.

## Current opt-in bridge

Classic GPP generation has an experimental air-yard matchup chip and an optional minimum lineup percentage. The percentage rounds up to a lineup count: 25% of 20 lineups requires at least five lineups with a qualifying player. Additional qualifying lineups are allowed when selected naturally. A 100% preset retains the previous every-lineup behavior. The realized count appears in lineup review, and the saved run records the requested percentage and frozen chips. This construction rule does not promote a point adjustment or complete the coherent-scenario and walk-forward gates above.

Classic GPP generation also offers an independent goal-line RB minimum percentage using the existing `INSIDE_FIVE` chip (at least three carries inside the opponent's five and eight total carries across the prior three regular-season weeks). The percentage rounds up the same way and can be combined with the air-yard matchup rule. The optimizer requires a tagged RB only when needed to meet the portfolio minimum, obeys exposure and roster constraints, and reports the realized count and infeasibility. Saved settings and lineup review preserve the choice. It changes lineup construction only; prior goal-line carries do not add fantasy points, touchdowns, or ownership discounts.

Normal lineup generation with Our model now defaults to the integrated experimental profile. It uses the existing PFR efficiency capture for QBs and the allowed-rushing-volume capture for RBs in one optimizer run; WRs, TEs, and other positions keep their saved historical forecast. The optimizer consumes a player's complete frozen mean/P10/P90/stat-line distribution only when identity, timing, and baseline-reproduction checks pass. A missing or invalid capture retains that player's baseline, and the run reports the applied count. The two captures are never compounded on one player. Saved settings, player bundles, and export QA record the actual profile used per player. Other projection sources retain their own forecasts. Users can still select a single profile or Off, and existing saved run settings restore as recorded. Air-yard matchup and goal-line chips remain separate, adjustable lineup-coverage rules; rushing volume is not an inside-five TD multiplier.
