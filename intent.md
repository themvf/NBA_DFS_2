# Intent: Vegas Environment Calibration and DFS Outcome Research

Status: Phase 0 rerun after schedule/result remediation; market and slate-history gates remain blocked  
Created: 2026-09-03  
Initial implementation target: NFL DFS, followed by NBA, MLB, and CFB adapters

## 1. Intent

Build an auditable historical research system that measures how pregame sports-betting expectations relate to realized team scoring and individual DFS performance.

The system must answer two connected questions:

1. How well did pregame implied team totals predict actual team scoring?
2. Did players on teams with the highest pregame implied totals outperform comparable players on other teams?

The goal is not to assume that high-total teams provide an edge. The goal is to measure whether market-derived game environments add stable, out-of-sample predictive information after accounting for salary, position, projection, ownership, role, and slate structure.

## 2. Core distinction

Two different effects must not be conflated:

- **Scoring level:** A team with a high implied total is expected to score more raw points.
- **Market calibration:** A team exceeding its own implied total should ordinarily occur near 50% after pushes are handled, if the market is well calibrated.

The useful DFS question is therefore not merely whether high-total teams exceed their totals more often. It is whether their players generate more ceiling outcomes, better salary-adjusted value, or more optimal-lineup appearances than the market, salaries, projections, and ownership already imply.

## 3. Research hypotheses

All hypotheses are provisional until tested.

### H1 — Team-total calibration

Closing and checkpoint implied team totals are calibrated to realized team points.

```text
team_scoring_residual = actual_team_points - implied_team_total
team_total_hit        = actual_team_points > implied_team_total
team_total_push       = actual_team_points = implied_team_total
```

Report wins and losses excluding pushes, while always reporting push count separately.

### H2 — High-environment player lift

Players on teams in the top implied-total percentile produce higher raw DFS scores and ceiling rates than players in lower percentiles.

### H3 — Incremental lift

Pregame implied-total rank and line movement predict player residual outcomes after controlling for known pregame information.

```text
projection_residual = actual_dk_fpts - frozen_pregame_projection
salary_value        = actual_dk_fpts / (salary / 1000)
```

This is the primary modeling hypothesis. Raw fantasy-point differences alone are not evidence of an exploitable signal.

### H4 — Conditional upside

Players on high-total teams have a different upper-tail distribution even if their mean outcomes are already priced efficiently.

Measure top-10%, top-5%, and slate-winning outcomes rather than relying only on averages.

### H5 — Market movement contains information

Movement from opening to T-24, T-6, T-90, T-30/T-15, and closing may improve prediction beyond the absolute implied total.

Test line movement direction, magnitude, book agreement, price support, and whether the move persisted into the close.

## 4. Non-goals

- Do not label correlation as causation.
- Do not claim a betting or DFS edge from in-sample results.
- Do not use actual team performance as a feature for that same game.
- Do not select the best checkpoint, threshold, or subgroup on the full dataset and report it as validated.
- Do not blend FantasyPros, LineStar, sportsbook observations, or other projections into `ourProj` without an explicit model version and validation run.
- Do not silently substitute the latest available odds for a missing historical checkpoint.
- Do not discard pushes, unmapped events, DNPs, or missing projections without reporting them.

## 5. Pregame data contract

Every observation must be reproducible from information available before the event began.

### 5.1 Market observation

Required fields:

```text
sport
event_id / canonical matchup_id
scheduled_start
capture_key
captured_at
provider_event_updated_at, when available
home_team / away_team
game_total_line
home_spread_line
home_implied_total
away_implied_total
eligible_book_count
selected_line_book_count
consensus_method
book quotes or source-row reference
```

Team implied totals should be derived from the spread and game total when both are available:

```text
home_implied = (game_total - home_spread) / 2
away_implied = game_total - home_implied
```

The exact sign convention used by each source adapter must be tested with golden fixtures.

### 5.2 Supported checkpoints

Initial comparisons:

- Open
- T-48 hours
- T-24 hours
- T-6 hours
- T-90 minutes
- T-30 or T-15 minutes, depending on sport cadence
- Close: final accepted observation strictly before commencement

Checkpoint windows, not exact instants, define eligibility. A snapshot must retain its actual capture time and lead time.

### 5.3 Team outcome

Required fields:

```text
actual_home_points
actual_away_points
final_status
overtime_flag, when available
settled_at
source reference
```

Postponed, cancelled, suspended, incomplete, and abandoned events are excluded through explicit status rules rather than score null checks.

### 5.4 Player outcome

Required pregame fields:

```text
canonical_player_id
team / opponent
position
salary
roster eligibility
frozen projection and model version
projected ownership, when available
injury and active status
role or opportunity features available at cutoff
```

Required result fields:

```text
actual DK fantasy points
actual ownership, when available
minutes/snaps/plate appearances or sport-specific opportunity
DNP/inactive status
optimal-lineup indicator, when computable
```

## 6. Leakage policy

Leakage prevention is a hard acceptance gate.

1. A market row is eligible only when `captured_at < scheduled_start`.
2. Provider update timestamps must also precede the start when supplied.
3. Player projections, salaries, ownership, injuries, and roles must be frozen at or before the chosen cutoff.
4. Schedule changes use the event start time known at the observation's availability time when revision history exists.
5. Closing means the last accepted pregame observation, never an arbitrary row labeled `close` after kickoff.
6. Whether the team later exceeded its total is an outcome used for explanation, not a feature available to a future model.
7. Feature generation and evaluation splits must be time ordered.

Every analysis run must report rejected post-start rows and missing-cutoff substitutions. Silent fallback is prohibited.

## 7. Cohorts and comparisons

Rank implied team totals within the actual DFS slate whenever a slate identity is available. Date-wide rankings may be reported separately but cannot be mixed with slate rankings.

Primary cohorts:

- Highest implied team total
- Top three teams
- Top 10%
- Top quartile
- Middle 50%
- Bottom quartile

Player results must also be segmented by:

- Sport
- Season
- Position
- Salary tier
- Pregame projection tier
- Ownership tier
- Starter/role tier
- Home/away
- Favorite/underdog
- Spread or expected game-script band
- Overtime versus regulation, for realized-total diagnostics

Comparisons should be matched or regression-adjusted where possible. A high-total quarterback should not be compared naively with a low-salary defense.

## 8. Metrics

### 8.1 Team calibration

- Exceed rate, loss rate, and push rate
- Mean and median scoring residual
- Mean absolute error
- Root mean squared error
- Residual distribution and tail rates
- Calibration by implied-total bucket
- Opening versus closing accuracy
- Accuracy by checkpoint and sport
- Book-count and selected-line-support sensitivity

### 8.2 Player performance

- Mean and median actual DK fantasy points
- Mean and median projection residual
- Projection beat rate
- Fantasy points per $1,000 salary
- Ceiling rate, defined before analysis
- Top-10% and top-5% slate finish rate
- Optimal-lineup appearance rate
- Ownership-adjusted leverage outcome
- Opportunity-adjusted performance

### 8.3 Incremental predictive value

Compare nested models:

```text
Baseline:
  position + salary + frozen_projection + ownership + role

Candidate:
  baseline
  + implied_team_total
  + within_slate_total_percentile
  + spread
  + checkpoint movement
  + book agreement/support
```

Evaluate changes in out-of-sample MAE, RMSE, log loss for binary ceiling outcomes, calibration, and top-tail ranking quality.

## 9. Statistical protocol

- Use chronological train, validation, and holdout periods.
- Never random-split rows from the same slate across folds.
- Cluster uncertainty by game or slate because player outcomes within a game are correlated.
- Report sample size beside every percentage.
- Use bootstrap confidence intervals clustered by game/slate.
- Apply minimum-sample gates before displaying subgroup conclusions.
- Correct or clearly flag broad multiple-hypothesis exploration.
- Report effect sizes and uncertainty, not only p-values.
- Re-run by season to test stability and regime drift.

An effect graduates into a production projection feature only when it:

1. Improves a predeclared holdout metric.
2. Is directionally stable across multiple seasons or independent holdouts.
3. Has sufficient coverage and sample size.
4. Survives salary, projection, ownership, position, and role controls.
5. Can be produced before lock with acceptable freshness.

## 10. Auditable persistence

Add append-only research tables rather than storing only rendered aggregates.

### `vegas_environment_runs`

Stores:

- Run ID
- Analysis version
- Sport and seasons
- Cutoff/checkpoint policy
- Cohort definitions
- Feature specification
- Train/validation/holdout boundaries
- Source snapshot IDs
- Input digest
- Code commit
- Created timestamp

### `vegas_environment_team_samples`

One row per team, event, checkpoint, and analysis run:

- Frozen market inputs
- Implied-total rank and percentile
- Actual score
- Residual and hit/push result
- Mapping and eligibility evidence

### `vegas_environment_player_samples`

One row per player and frozen slate observation:

- Player identity and team sample link
- Salary, projection, ownership, role, and position
- Actual DFS result
- Residual, value, ceiling, and optimal-lineup labels
- Exclusion reason when not evaluable

### `vegas_environment_metrics`

Stores versioned aggregate results by cohort, checkpoint, split, position, salary tier, and season, including numerator, denominator, confidence interval, and metric definition.

Derived data must always point back to immutable source rows or source snapshot IDs.

## 11. Product surface

Add a Vegas Environment section under analytics after the research tables are populated.

Recommended views:

1. **Team-total calibration curve** — implied total bucket versus actual points and exceed rate.
2. **Player lift curve** — implied-total percentile versus projection residual, value, and ceiling rate.
3. **Opening-to-close movement** — whether movement improved team and player forecasts.
4. **Position response** — QB/RB/WR/TE/DST and sport-specific positions.
5. **Optimal-lineup concentration** — share of optimal players from each team-total cohort.
6. **Season stability** — effect size and confidence interval by season.
7. **Data-health panel** — mapping, market, projection, salary, result, and ownership coverage.

Every chart must expose:

- Sample size
- Date range
- Sport
- Checkpoint
- Exact metric definition
- Confidence interval
- Analysis version
- Missing/excluded row counts

## 12. Sport rollout

### Phase 1 — NFL DFS

Start with NFL because the project now has an immutable NFL projection pipeline, DK salary ingestion, canonical schedules, odds history, player-week history, and auditable optimizer runs.

Phase 0 must verify historical coverage for:

- Pregame spreads and totals at each checkpoint
- Final team scores
- Historical DK salaries or a defensible salary proxy
- Frozen pregame projections
- Actual player DK scoring
- Actual ownership and optimal-lineup reconstruction

Do not treat current tables as complete until this coverage audit is saved.

### Phase 2 — NBA

Reuse the shared framework with NBA pace, rest, minutes, and overtime annotations. Slate boundaries and late-swap timing require explicit handling.

### Phase 3 — MLB

Use implied team runs and account for starting pitcher confirmation, batting-order availability, weather, postponements, and doubleheaders.

### Phase 4 — CFB

Reuse canonical mapping, checkpoint capture, and consensus policy from the CFB terminal. Address larger talent disparities, overtime scoring, and sparse player salary/result history separately.

## 13. Development sequence

### Phase 0 — Coverage and identity audit

Deliver:

- Row counts and date ranges by source and sport
- Mapping success rate
- Checkpoint availability rate
- Pregame versus post-start rejection counts
- Team-score coverage
- Player-result, salary, projection, and ownership coverage
- A documented go/no-go decision for NFL MVP

No model conclusions are permitted in Phase 0.

### Phase 1 — Canonical sample builder

Create a Python module that deterministically materializes team and player samples from explicit source snapshots and cutoff rules.

Suggested files:

```text
model/vegas_environment.py
ingest/vegas_environment_samples.py
tests/test_vegas_environment.py
```

Golden tests must cover spread sign, team implied-total arithmetic, pushes, postponed games, post-start rejection, checkpoint selection, slate ranking, DNP handling, and doubleheaders/reschedules where applicable.

### Phase 2 — Descriptive backtest

Create the team calibration and player lift tables with clustered confidence intervals. Save run metadata, samples, and metrics before building UI.

### Phase 3 — Controlled predictive test

Fit baseline and candidate models using chronological splits. Record whether Vegas features improve holdout performance and where they fail.

### Phase 4 — Analytics UI

Build the analytics views from persisted metrics. The UI must not recompute research definitions independently in TypeScript.

### Phase 5 — Model integration gate

Only after passing the graduation criteria, add a versioned Vegas feature to the relevant projection model. Run it in shadow before it can influence optimizer defaults.

## 14. Phase 0 repository findings and open checks

A preliminary code scan confirms that the repository already contains several required components:

- Append-only game odds history and per-book quote payloads
- NFL canonical schedules and matchup records
- NBA/MLB matchup and DFS result pipelines
- NFL player-week historical statistics
- Immutable NFL DFS projection and optimizer runs
- Existing `actual_fpts` and ownership ingestion patterns

The scan does not prove that historical coverage is sufficient. The first development task is to quantify, not assume, the following:

1. Which sports and seasons have reliable pregame captures?
2. How many events have enough books to compute the approved consensus?
3. Are historical final scores mapped to the same canonical matchup IDs?
4. For NFL, can every player-week result be linked to the applicable DK slate salary and frozen projection?
5. Can optimal lineups be reconstructed under the correct contest roster rules?
6. Are actual ownership files numerous enough for ownership-adjusted conclusions?

## 15. MVP acceptance criteria

The NFL MVP is complete when:

- Every included row is demonstrably pregame.
- Team and player source coverage is reported and reproducible.
- Team implied totals pass golden arithmetic fixtures.
- Pushes and exclusions are explicit.
- Cohorts are ranked within the correct slate.
- Team calibration and player lift are saved with sample sizes and confidence intervals.
- Baseline and candidate models use chronological holdouts.
- The analysis can be rerun to the same input digest and metrics.
- No result is described as an edge unless it passes the graduation gate.
- The analytics UI displays research status, coverage, version, and limitations.

## 16. Immediate next action

Implement Phase 0 as a read-only coverage audit. It should inspect existing database tables, produce a dated JSON artifact under `artifacts/`, and recommend one of:

- `GO_NFL_MVP`
- `GO_WITH_LIMITED_SEASONS`
- `BLOCKED_MISSING_MARKETS`
- `BLOCKED_MISSING_PLAYER_LINKAGE`
- `BLOCKED_OTHER`

The audit must not mutate historical sports, odds, or DFS records. Its only permitted write is the versioned audit artifact.

## 17. Phase 0 result — 2026-09-03

Artifact: `artifacts/vegas_environment_phase0_nfl_2026-09-03.json`  
Audit version: `vegas-environment-phase0-v2`  
Recommendation: `BLOCKED_MISSING_MARKETS`

Measured findings:

- 1,686 NFL odds-history rows exist; all 1,686 map to a kickoff, contain a complete pregame total/spread, and precede commencement.
- Canonical 2023–2025 schedules now provide 816 completed regular-season games and final scores.
- All 816 completed games carry nflverse historical spread and total references, but those references have no per-game availability timestamp. They are therefore reported separately and are not treated as eligible open/checkpoint captures.
- Zero completed games currently have timestamped pregame `game_odds_history` captures, so the strict market gate remains blocked.
- All 272 upcoming regular-season games map to canonical NFL matchups.
- 1 of 272 upcoming games currently has an eligible total-and-spread capture with four or more books.
- 17,232 historical regular-season player-week results cover 2023–2025.
- All 17,232 historical player weeks now link to a canonical game by season, week, and team.
- All 17,232 player weeks now have exact, versioned DraftKings realized scoring, including 1,632 DST weeks. DST evidence retains sacks, takeaways, touchdowns, safeties, blocked kicks, two-point returns, and adjusted points allowed so league-specific redraft rules can reuse the components.
- Two uploaded NFL DFS workspaces contain 1,438 salary rows, including 1,380 canonical player identities and 1,334 model projections.
- Zero slates currently link salary, frozen projection, and realized result on the same slate.
- Zero rows contain actual contest ownership, so ownership-adjusted conclusions are not yet possible.

Required remediation before Phase 1:

1. Accumulate the 2026 capture cadence through completed games and/or backfill historical markets with defensible pregame availability timestamps.
2. Persist historical DK salary slates and link their players to exact `nfl_dfs_player_week_results` rows.
3. Collect actual contest ownership separately from projected ownership.
4. Re-run the same Phase 0 audit and require its gates to pass before fitting a predictive model.

Completed remediation:

- Loaded and verified all 272 regular-season games for each of 2023, 2024, 2025, and 2026.
- Replaced the season loader's fixed UTC-5 kickoff offset with `America/New_York`, preserving EDT/EST correctly.
- Added append-only `nfl_dfs_player_week_results` with source digests, scoring-version provenance, exact results, auditable exclusions, and reusable DST components for Classic DFS and custom redraft scoring.

Historical odds cost gate:

- The Odds API historical featured-market endpoint returns the closest snapshot at or before the requested timestamp and has five-minute resolution for these seasons.
- Historical requests cost 10 credits per region per market. With one region and three markets, each bulk snapshot costs 30 credits.
- Full 2023–2025 coverage at T-48, T-24, T-6, T-90, T-15, and a T-5 close proxy requires 2,177 distinct bulk snapshots, estimated at 65,310 credits.
- A 2025 pilot at T-6, T-15, and T-5 requires 378 snapshots, estimated at 11,340 credits.
- These are saved dry-run plans; no paid historical API calls were made. Explicit approval of a plan is required before executing it.

## 18. Model study and forward shadow — authorized development

The five-step study is defined in `docs/nfl-dfs-model-development-study.md`.
It benchmarks a market-free reconstruction of the historical model, fits
prior-opportunity and separately labeled closing-reference candidates, and
stores full samples/predictions. The strict market/slate gates above remain
in force for production and timestamped market research. They do not make
prior-opportunity research depend on purchasing historical odds.

2025 has already been inspected and is a retrospective diagnostic, not an
untouched holdout. Forward 2026 forecasts are frozen in a separate shadow
ledger; only qualified non-market candidates are allowed there. No automatic
production promotion is implemented. Missing salary, ownership and DNP evidence
is surfaced explicitly rather than filled with proxies.

Executed study: 18,581 player-week evaluations; 43,631 saved variant/sample
records; 36,551 frozen history rows. All five opportunity candidates qualified
for shadow-only use, with retrospective 2025 MAE reductions of 1.15%–2.75%.
The first 645 Week 1 player-week baseline/candidate pairs have been frozen.
Forward results remain pending. See `docs/nfl-dfs-model-study-findings.md` for
the numerical comparison and limitations. No production default was changed.

## 19. 2026-09-04 release and next iteration

Research/shadow release includes a daily current-season player/DST result refresh, weekly shadow grading summaries, portable code-pin verification, and append-only DST corrections that include opponent/final-score dependencies. Production model formulas and optimizer defaults remain unchanged.

Next design: `docs/nfl-dfs-player-variance-and-weekly-review.md`. Prioritize a per-player weekly report card with missing-data coverage, then player/role-specific variance shrinkage and opportunity/efficiency features. This is a simulated junior-developer review and proposed follow-on scope, not a claim that the new variance model or UI is built.

## 20. Weekly player review and variance experiment — 2026-09-04

Implemented: `/dfs/nfl/review`, append-only report-card materialization, separate production/shadow grading, missing-result coverage, per-player interval charts, component/audit details, and daily workflow wiring. First Week 1 report is persisted. A player/position variance experiment saved 12,525 retrospective distributions; interval-score improvement is mixed (QB/DST better, RB/WR/TE worse). No variance candidate was activated and production formulas are unchanged. Role/injury modeling and new opportunity/efficiency forecasts remain future work. See `docs/nfl-dfs-weekly-review-delivery.md` for evidence and limitations. Prepared for main release; hosting deployment is verified separately.
