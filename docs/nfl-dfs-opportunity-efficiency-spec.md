# NFL DFS opportunity-and-efficiency model + visual delivery

Date: 2026-09-04

Status: Slice A released; Slices B and C are implemented locally through 2026-09-05 for the currently audited fields. The saved candidate uses team-coupled offense, conditional rates, exact DraftKings scoring, a separate opponent-conditioned DST process, Weekly Review grading, and Model Lab visuals. Role/depth-chart evidence remains unresolved. No production promotion is implemented.

Intent: [NFL research roadmap](../intent.md) and [visual delivery requirement](../intention.md#project-wide-addition-visual-model-delivery--2026-09-04).

## 1. Outcome and boundaries

Build a football-component forecast that can answer: What workload do we expect? What efficiency converts it to scoring? Why did the projection change? Where did it miss? Does the new version improve on simpler alternatives?

Each increment delivers computation, immutable evidence, evaluation, and a visible application view together. Research visuals are available before any model becomes an optimizer input. Existing weekly audits remain the outcome record, not an automatic calibration engine.

Scope: NFL regular-season QB/RB/WR/TE and DST, DraftKings scoring first. Preserve a scoring-independent component contract for a later redraft adapter. K, new scoring formats, paid sources, props overlays, automatic promotion, and lineup-profitability claims are outside this release. DST is included but modeled separately, never treated as an offensive player.

## 2. Verified starting point

- `model/nfl_dfs_historical.py` resamples historical stat lines and scores each draw, including nonlinear bonuses. This is the baseline to preserve, not a new implementation to replace silently.
- `model/nfl_dfs_research.py` has an opportunity candidate using baseline, history count, and prior opportunity. It is a correction to expected fantasy points, not a team-consistent workload/efficiency simulator.
- `model/nfl_dfs_variance.py` tests player/position residual shrinkage around the market-free baseline. Gains are mixed and it is not activated.
- `ingest/nfl_dfs_shadow.py` freezes eligible baseline/candidate forecasts separately from production. Weekly Review now carries a fourth, explicitly labeled `efficiency_research` stream without changing the production stream.
- `/dfs/nfl/review` already displays saved production/shadow forecasts and outcomes. Available frozen stat means are not evidence that every proposed workload component has been forecast.
- Historical raw player rows and versioned result evidence exist. Routes, snaps, red-zone usage, historical role membership, and timestamped injuries still need a field-level coverage audit. A field in a raw feed is not proof of reliable historical coverage.

Historical findings and limitations: [study findings](nfl-dfs-model-study-findings.md), [weekly review delivery](nfl-dfs-weekly-review-delivery.md). Previously inspected 2025 data cannot become an untouched holdout again.

## 3. Visual experience

Proposed entry: NFL DFS → **Model Lab**, at `/dfs/nfl/model`. Link back to Weekly Player Review and preserve selected season/week/player/model where applicable. All routes and tabs below are proposed.

| Slice | Question | Required visual | Saved evidence |
|---|---|---|---|
| Coverage | Which inputs can this forecast actually use? | Field-by-season coverage grid, missing/excluded counts, source-age indicators | Source audit and input availability records |
| Workload | How much work will this player receive? | Recent actual workload with forecast marker/range; team carry/target allocation bars including unallocated share | Frozen team budget, player shares, role/prior and component forecasts |
| Efficiency | How does that work become fantasy points? | Expected DK-point contribution bars by scoring component; recent conversion-rate comparisons | Component draws/summaries, scorer version, contribution totals |
| Change | Why did this projection move? | Prior/current forecast comparison with component deltas and source-update timeline | Two immutable snapshots at stated cutoffs |
| Distribution | What outcomes and uncertainty does the model imply? | Distribution comparison, P10/median/mean/P90 and boom probability; sample/prior-weight indicators | Seed, draw artifact hash, quantiles and residual/prior evidence |
| Evaluation | Is the candidate actually better? | Paired error-delta chart with uncertainty, calibration curves, weekly trend and position/role breakdown | Frozen cohort, experiment, predictions and corrected results |
| Operations | Is the system collecting and grading correctly? | Last-run/coverage cards, stale inputs and overdue results | Job status, forecast/result counts, source timestamps |

Core visuals render automatically from saved data. Expensive optional analysis may use Generate Visuals, but never require it to see the projection rationale. Use concise personal-project labels, no "understand" language.

Every chart supplies units, sample size, period, model status/version and an accessible data table/export. Distinguish retrospective reconstruction from forward capture. Single observations show a marker; gaps remain gaps. Do not imply that mean-component contributions are causal feature importance. Visuals are explanatory, not a claim that a lineup is optimal or profitable.

## 4. First slice: source audit and feature contract

Read stored source payloads before adding feeds. Inventory identity, season/week, field definitions, units, population, missingness, update/capture timestamps, and revisions. Audit raw rows separately from successfully modeled players so selection cannot hide missingness.

Classify each input: usable with known availability; usable only for retrospective reconstruction; missing; ambiguous definition; or unsupported. A missing injury report means unknown, not healthy. A missing stat row is not an inactive zero.

Initial requested fields: attempts/completions; carries; targets/receptions; passing/rushing/receiving yards and TDs; interceptions/fumbles; team attempts/carries and defense scoring components. Add routes/snaps, red-zone splits, teammate availability and role transitions only if audited contracts support them. Retain other scoring components or explicitly mark a forecast incomplete; do not silently discard rare points to improve error.

Exit: a saved coverage report, a real-data coverage screen, versioned alias/units mapping, and a declared supported cohort. The first implementation is this slice, not the full simulator. Unsupported feature families remain visibly deferred.

## 5. Modeling design

### 5.1 Workload: budget first, then player allocation

Estimate team pass attempts and rush attempts from lagged team history with recency weighting and shrinkage toward training-only priors. Begin market-free; opponent/history adjustments are separate experiments. Define pass attempts versus dropbacks explicitly so sacks and scrambles are not double-counted as passes.

Allocate receiving targets and rushing attempts to the known pregame roster using prior shares and supported role evidence. Targets cannot exceed team pass attempts; retain a non-targeted-pass category. Rushing allocations include QB carries. Shares are nonnegative and sum to one including an explicit unallocated/other category. Do not force absent players' work into the visible pool just because their identity/history is missing.

Rookies and changed-team players receive a documented role/position prior, not an invented workload. If role evidence is absent, return a flagged fallback or unavailable candidate; preserve the baseline separately. Role labels use only prior information and versioned thresholds.

First experiment: workload-only replacement, holding the prior efficiency estimator fixed. This isolates whether forecasting opportunities improves on resampling prior totals.

### 5.2 Efficiency and scoring

Estimate completion/catch rates, yards per applicable opportunity, touchdown rates and turnover rates with training-only shrinkage. Rate denominators must be explicit; zero opportunities means an undefined rate that uses the prior, not a zero-efficiency observation. Rare touchdown rates receive stronger pooling only if validation supports it.

Conceptual receiving sequence: targets → receptions → receiving yards/TDs → scoring. Simulate conditional outcomes and average scored draws; do not assume multiplying marginal expectations captures all dependencies.

Generate coherent team/player draws: receptions ≤ targets, completions consistent with allocated receptions, receiving TDs ≤ receptions, and passing/receiving scoring consistent across a team's simulated players including the unallocated bucket. Count fields are integers; yardage may be negative where football permits. Document source-definition exceptions rather than enforcing invalid identities.

Score each draw with the existing versioned scoring contract. Expected bonus points come from the simulated threshold probability, not a bonus applied to mean yardage. Keep fumbles, two-point plays and other supported scoring components visible. Decompose expected fantasy points using the same scored draws so component contributions reconcile exactly within stated rounding tolerance.

Candidate progression: workload-only; workload plus efficiency; then uncertainty/role extensions. Store all tested candidates, not only the winner.

### 5.3 DST

Use a separate opponent-volume and defensive-outcome model: dropbacks/possessions when available, sacks, interceptions, recoveries, return scores, and points allowed. Opponent drives are deferred if unsupported; do not substitute pass attempts and relabel them drives.

Reuse exact versioned DST scoring and its opponent/final-score dependencies. Simulate points-allowed scoring bands rather than scoring mean points allowed. Preserve negative scores and rare-event uncertainty. The existing DST opportunity correction is calibration-only and remains a separate benchmark.

### 5.4 Uncertainty and role changes

Separate randomness conditional on workload from uncertainty about workload/availability. Test residual shrinkage around the new candidate itself; baseline residuals are not interchangeable. Record effective sample size, prior weight, role definition and lower/upper-tail performance.

Normal/limited/inactive scenarios require supported timestamped evidence. If probabilities cannot be estimated defensibly, expose an explicitly hypothetical what-if scenario, not an official weighted forecast. Such scenarios cannot enter primary grading or optimizer inputs.

### 5.5 Miss attribution

Display frozen component forecast versus actual first. Classify workload, conversion, touchdowns/bonuses, and turnover differences only where the required forecast and actual inputs exist. Zero-denominator cases stay unclassified.

If adding an additive miss waterfall, version the counterfactual replacement order, preserve interaction/unexplained contributions, and reconcile to total actual-minus-projected points. Label it accounting attribution, not causal blame. Never reconstruct a missing historical forecast after seeing the outcome.

## 6. Persistence, identity and read contract

Reuse canonical game/player IDs and existing forecast/result/research infrastructure where compatible. New normalized tables, if needed after the audit, are proposed as `nfl_dfs_component_runs`, `nfl_dfs_component_forecasts`, and `nfl_dfs_feature_audits`; do not create a second outcome ledger.

Minimum run contract: immutable run/model/config/scoring versions, implementation hash, training cutoff, feature cutoff, forecast capture time, forecast horizon, season/week, seed, input manifest/hash, experiment ID, status and schema version.

Minimum player/game contract: IDs, team/opponent at snapshot, kickoff snapshot, identity/eligibility status, component names/units/means/quantiles, FPTS mean/median/P10/P90/boom threshold and probability, support/prior weights, source/fallback/missing reasons, team allocation reference and draw artifact checksum.

Source manifests retain observed/available/captured times where known. Availability must precede forecast capture and capture must precede applicable kickoff. Revisions append new evidence; they never rewrite the original forecast's source state. Unknown historical publication latency is explicitly retrospective-only, not a fabricated availability timestamp.

Use immutable run identity plus player/game for forecast uniqueness; retries of the same run reuse the same records. A changed source/config creates a new run. Persist compact draw artifacts with versioned binning/summary data so the UI can reproduce distributions without shipping all raw draws to the browser. Audit export resolves to the exact hashes.

Materialized evaluations reference forecast IDs, latest result revisions and cohort definitions. Expose one primary last-valid-pregame forecast per player/game/model; horizon-specific studies are separate cohorts. Record schedule changes and reject forecasts captured after the corrected kickoff. Never treat repeated snapshots as independent outcomes.

Web queries are read-only, typed, parameterized, paginated and return one selected cohort/player at a time. Python owns model/scoring math; TypeScript displays saved summaries. Golden fixtures reconcile Python payloads, query mappings, chart totals and exports.

Migration requirement: Python and Drizzle schemas change together. The targeted DFS bootstrap currently creates tables but does not supply arbitrary ALTER migrations; add an explicit, tested migration path if extending existing tables. Readers must tolerate legacy payloads with missing component fields. Extend model-stream handling in grading, query/UI selectors and tests together; do not overload the existing opportunity label for a new model.

## 7. Experiment and release protocol

1. Freeze audited cohort, features, role definitions, parameter grid, metrics and comparison policy before fitting. Persist failed and excluded trials.
2. Use expanding-window evaluation, with all target-week forecasts produced before any target-week outcomes enter training. Fit transformations and priors within the training window only. Historical publication limits remain disclosed.
3. Compare simple recency, market-free historical baseline, existing opportunity candidate and new component candidates on matched player-weeks. Archived production forecasts are a separate forward comparison, not a reconstructed production claim.
4. Primary mean metric: paired MAE delta. Report RMSE and bias using `actual - projected`; adapt the existing research metric's opposite sign explicitly. Components use their own units and error metrics.
5. Distribution metrics: P10/P90 pinball loss, 80% interval score/width/coverage, and boom Brier/calibration. Wider intervals alone cannot qualify. Display n, exclusions and paired game-clustered uncertainty; preserve week chronology and inspect sensitivity to week-level grouping.
6. Register feature families and exploratory slices to disclose multiple comparisons. Existing 2025 data remains a retrospective diagnostic, not a fresh gate that can be repeatedly tuned until it passes.
7. Specify future forward start date, version, evaluation window, minimum player-weeks/games by position and material-regression tolerances after the coverage audit and before any new candidate forward outcomes are inspected. These numeric gates are not finalized in this draft; automatic promotion remains prohibited.
8. Successful research permits only a separately configured shadow candidate. Production adoption requires forward evidence and explicit approval with rollback to the prior version. Historical improvement alone cannot change `ourProj` or optimizer defaults.

Weekly jobs freeze upcoming forecasts, append result revisions, grade past forecasts and publish saved visual summaries. Weekly feedback proposes new experiments, not automatic player-specific bias corrections after one bad game. Later lineup-level evaluation requires real salary/slate/contest evidence and joint outcomes; no ROI proxy is added here.

## 8. Implementation slices and code seams

| Order | Backend and tests | Visual deliverable | Completion condition |
|---|---|---|---|
| A | Proposed `ingest/nfl_dfs_feature_audit.py`, pure coverage validation and fixtures | Model Lab shell + source coverage | Counts reconcile to actual stored payloads; unavailable inputs visible |
| B | Proposed `model/nfl_dfs_components.py`, lagged team budgets/shares and frozen forecast writer | Workload trends + allocation bars | Pregame-only inputs; budgets reconcile including unknown share; fallback tests |
| C | Conditional efficiency/scoring and separate DST implementation | Scoring contribution chart + component actuals in weekly review | **Implemented locally:** shared team draws reconcile offense, DST is separate and opponent-conditioned, and the fourth report-card stream is saved |
| D | Candidate-specific role/variance experiment and saved distributions | Distribution comparison + sample/prior indicators | Deterministic replay; sparse-history behavior; width-aware evaluation |
| E | Extend research/shadow/report-card adapters without changing production | Experiment comparison + change timeline + operational state | End-to-end candidate identity, immutable results/revisions and matched cohorts |

Proposed UI files: `web/src/app/dfs/nfl/model/page.tsx`, a client view and focused components; dedicated typed query/contract modules under `web/src/db/` and `web/src/lib/nfl-dfs/`. Extend the existing `/dfs/nfl/review` rather than duplicating its result audit. Add visible navigation from the DFS workspace and NFL board in the same slice that introduces the route.

Use the existing daily NFL workflow for approved lightweight materialization only. Backtests run explicitly/offline, not on page load. New ingestion providers and heavier scheduling remain separate decisions. Schema installation precedes writers; compatible readers handle both old and new payloads during rollout.

## 9. Acceptance checklist

- Future data, target-week outcomes and late revisions cannot alter frozen feature inputs or forecasts.
- Team budgets reconcile; partial rosters retain unknown allocation; no current roster is retroactively substituted for a historical roster.
- Zero opportunity, zero actual, missing result, verified inactive, excluded scoring and unsupported component have distinct behavior.
- Seed/config/input hashes reproduce component draws, scoring totals and chart summaries. One-observation and all-missing charts are honest.
- Bias sign, component contribution totals, result revision selection and model labels agree across Python, TypeScript, exports and UI.
- Every slice has real saved data or an explicit empty/unavailable state; no placeholder charts presented as observations.
- Browser verification covers navigation, filters, model changes, keyboard access, narrow screens, chart/table agreement, query failures and large-cohort pagination.
- Research failures are visible without breaking production projection, lineup generation or existing weekly review.
- No slice is marked delivered until its visual and evidence are verified. No component candidate reaches production through this specification alone.

## 10. Next action

Slices B and C are implemented locally. Next, begin Slice D by measuring candidate-specific residual distributions and role uncertainty around the new workload-plus-efficiency mean, with saved distribution comparisons and width-aware backtests. In parallel, audit a timestamped depth-chart/injury source; player allocations remain forward research because historical weekly roster membership is not verified. The current candidate remains shadow research and cannot alter production without forward evidence and explicit approval.
