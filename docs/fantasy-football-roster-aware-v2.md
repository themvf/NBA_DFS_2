# Fantasy Football Roster-Aware V2 Specification

**Status:** Approved architecture; shadow challenger; not active
**Products:** Redraft rankings and DraftKings NFL Best Ball Draft Lab
**Primary routes:** `/fantasy-football/redraft`, `/fantasy-football/best-ball`
**Created:** 2026-08-28

This specification turns the approved roster-aware V2 concept into an
implementation contract shared by redraft and Best Ball. It supplements
[`fantasy-football-draft-spec.md`](fantasy-football-draft-spec.md) and
[`nfl-best-ball-model-improvement-spec.md`](nfl-best-ball-model-improvement-spec.md).
The executable provenance rules and source-by-source field, license, cadence,
history, cutoff, and fallback contracts are defined in
[`fantasy-football-v2-source-contracts.md`](fantasy-football-v2-source-contracts.md).
The versioned Team Opportunity persistence and reconciliation contract is
defined in
[`fantasy-football-v2-team-opportunity-contract.md`](fantasy-football-v2-team-opportunity-contract.md).
The uncalibrated V2-008 forecasting engine, eligible archived training context,
and conservative missing-source fallbacks are documented in
[`fantasy-football-v2-team-opportunity-model.md`](fantasy-football-v2-team-opportunity-model.md).
The populated 2020-2025 roster and team-week foundation, exact count semantics,
coverage audit, and deterministic replay instructions are documented in
[`fantasy-football-v2-historical-context.md`](fantasy-football-v2-historical-context.md).
The Best Ball specification governs contest-specific roster simulation when the
documents differ.

V2 must remain a shadow challenger until its chronological validation and
promotion gates pass. It must not overwrite the displayed live projection,
ranking, or recommendation merely because the architecture has been implemented.

## 1. Product decision

Build one versioned football projection pipeline:

```text
immutable as-of source snapshots
        ↓
team weekly opportunity
        ↓
player role allocation
        ↓
efficiency and touchdown allocation
        ↓
weekly player stat and fantasy-point distributions
        ├─ sum into redraft season projections
        └─ simulate Best Ball counted-roster value
```

Redraft and Best Ball must consume the same weekly football distributions.
They may apply different scoring rules and decision objectives, but they may
not maintain separate team totals, player roles, or touchdown assumptions.

## 2. Current boundary

- `ingest/ff_independent.py` produces the live season-total baseline.
- The Best Ball shadow simulator already performs deterministic paired weekly
  simulation and maximum legal lineup selection, but its weekly variance is
  not yet powered by roster-aware football stat distributions.
- The V2 explainer is descriptive only. Its values must not be presented as
  active projections before promotion.
- The live champion is `ff-independent-v1.14`, frozen in
  `artifacts/ff_champion_baseline_v1.14.json`. Ranking-snapshot metadata is the
  authoritative runtime identity for redraft, Best Ball, draft sessions, and
  advisors; V2 remains separate and inactive.

## 3. Goals

1. Forecast finite weekly team opportunity pools with uncertainty.
2. Reconcile every relevant player's role to those pools.
3. Treat trades, free agency, rookies, injuries, and depth-chart competitions
   as effective-dated scenarios rather than carrying last year's volume forward.
4. Separate touchdown opportunity from touchdown conversion.
5. Produce calibrated weekly distributions for every draftable player.
6. Aggregate the weekly distributions for redraft and simulate automatic
   counted lineups for Best Ball.
7. Preserve source provenance, as-of eligibility, model version, fallbacks,
   missingness, and explanations.
8. Prove improvement or complementary value chronologically before promotion.

## 4. Non-goals

- Hard-code a projection adjustment for Mike Evans or any other player.
- Move a departed player's prior-year fantasy points or touchdowns directly
  from one team to another.
- Allow projected player targets, carries, touchdowns, or snaps to grow
  independently beyond a plausible team total.
- Use ADP, ECR, or an LLM as a football-performance feature.
- Replace the live redraft or Best Ball rank during shadow development.
- Extend this work to DFS, waiver advice, trades, or start/sit decisions.

## 5. Source and identity contracts

Reuse `ff_source_snapshots` for immutable source provenance. New ingestion must
record source, dataset, season/week, requested parameters, fetched time,
source-published time where available, response hash, row count, match coverage,
missingness, status, and model eligibility.

`ingest/ff_source_contracts.py` is the executable contract. Re-fetching an
identical response must resolve the original snapshot without rewriting its
provenance, and post-cutoff availability must fail closed.

Required sources:

| Input | Preferred source | Purpose |
|---|---|---|
| Player identity and current roster | nflverse weekly rosters, enriched by Sleeper | Canonical player/team identity and current depth context |
| Weekly player and team results | nflverse weekly stats | Historical targets, carries, yards, touchdowns, and fantasy outcomes |
| Team opportunity and role evidence | nflverse play-by-play | Plays, attempts, rushes, red zone, goal line, and game script |
| Snaps/routes/participation | nflverse participation | Route, snap, personnel, and role stability evidence |
| Opponent, venue, and bye | Versioned nflverse schedule | Week 1–17 environment |
| Transactions | Effective-dated nflverse/Sleeper roster history | Departures, arrivals, and changed-team scenarios |
| Play caller and scheme | Versioned attributable dataset | Pace, pass tendency, personnel, and allocation priors |
| Injuries and availability | Existing injury observations plus verified sources | Separate active probability and role scenarios |

Historical records are eligible only when they were available by the simulated
as-of cutoff. Current depth charts, transactions, injuries, or coaching context
must never leak into a historical preseason decision.

## 6. Data model

The exact physical schema may be normalized further during implementation, but
the following logical contracts are required.

### 6.1 Team-week historical facts

One row per team and completed game:

```text
season, week, team, opponent, game_id
plays, drives, pass_attempts, allocatable_targets
rush_attempts, rb_carries, rb_targets
pass_touchdowns, rush_touchdowns
red_zone_trips, goal_line_carries, end_zone_targets
neutral_pass_rate, pace, score_state_features
quarterback_id, play_caller_id
source_snapshot_ids, observed_at
```

Define every count precisely. For example, quarterback kneels and scrambles
must not silently enter the running-back carry pool, and sacks/throwaways must
be handled consistently when reconciling pass attempts and targets.

### 6.2 Team-week opportunity forecasts

```text
run_id, model_version, season, week, team, opponent, as_of_at
expected_plays, expected_pass_attempts, expected_rush_attempts
expected_allocatable_targets, expected_rb_carries, expected_rb_targets
expected_pass_touchdowns, expected_rush_touchdowns
p10, p50, p90 and dispersion for each forecast
game_script_scenario_probabilities
source_snapshot_ids, missing_flags, fallback_tier, created_at
```

### 6.3 Effective-dated transactions

```text
player_id, from_team, to_team, effective_at, transaction_type
contract_or_investment_evidence, source_snapshot_id, observed_at
```

### 6.4 Player role scenarios

```text
run_id, player_id, season, week, team, scenario_id
scenario_probability, active_probability, starter_probability
snap_share, route_share, target_share, rush_share
rb_target_share, red_zone_target_share, end_zone_target_share
goal_line_rush_share
evidence, source_snapshot_ids, missing_flags, confidence
```

### 6.5 Weekly player distributions

Use the existing Best Ball distribution contract and extend its underlying stat
line so every scoring result can be reproduced:

```text
player_id, model_version, feature_snapshot_id, season, week
active_probability
passing/rushing/receiving stat distribution parameters
touchdown-opportunity and conversion components
p10, p25, median, mean, p75, p90
spike and yardage-bonus probabilities
scenario_probabilities, calibration_version, created_at
```

## 7. Team opportunity model

Forecast the team before forecasting players. The first implementation should
be interpretable and hard to overfit:

```text
team baseline
  = recency-weighted historical team rate
  + league-average shrinkage

weekly adjustment
  = opponent
  + quarterback continuity
  + play-caller continuity
  + venue and expected scoring environment
```

Required forecast pools:

- offensive plays and drives;
- pass attempts and allocatable targets;
- rush attempts and running-back carries;
- running-back targets;
- passing touchdowns and rushing touchdowns.

Use positive, neutral, and negative game-script scenarios or an equivalently
coherent joint distribution. Counts must not be sampled independently: a
leading script should generally increase carries, a trailing script should
generally increase pass volume, and a high-scoring script should increase
touchdown opportunity. Preserve correlated historical residuals or use another
validated joint method.

Missing inputs widen uncertainty and select an explicit fallback tier:

- **A:** team, opponent, quarterback, play caller, and schedule context;
- **B:** team/opponent history without one or more current context inputs;
- **C:** league and position priors for new or sparse contexts.

## 8. Reconciled player allocation

Allocate normalized shares across every relevant teammate. Do not apply
independent player multipliers.

```text
player targets = allocatable team targets × player target share
player RB targets = team RB targets × player RB-target share
player carries = allocatable team carries × player rush share
player receiving-TD opportunity = team pass TDs × receiving-TD share
player rushing-TD opportunity = team rush TDs × rushing-TD share
```

At minimum, the allocation layer must represent early-down, receiving,
red-zone, end-zone, and goal-line roles separately. A slot receiver, boundary
receiver, pass-catching back, and tight end are not interchangeable recipients
of vacated opportunity.

Unresolved competitions must be modeled as explicit scenarios such as starter
wins role, committee persists, rookie earns role, or injury limits role. All
scenario probabilities and overrides must be versioned, attributable, and
effective-dated.

## 9. Touchdown opportunity and conversion

Do not redistribute raw prior-year touchdowns. Model:

```text
expected touchdown opportunity
    × context- and player-adjusted conversion
    = projected touchdowns
```

Opportunity uses team passing/rushing touchdowns, end-zone targets, targets
inside the 10, goal-line carries, routes, and role. Conversion is regressed
toward position, role, age, and sample-size priors while retaining validated
persistent player skill. This prevents both full carry-forward of touchdown
luck and complete erasure of repeatable end-zone skill.

No automatic workload penalty is permitted. Volume, efficiency, availability,
and touchdown conversion are separately estimated components.

## 10. Mike Evans changed-team validation fixture

Mike Evans's move from Tampa Bay to San Francisco is the first required
end-to-end changed-team fixture. It is a validation case, not a model rule.

### 10.1 Tampa Bay departure

1. Estimate Evans's prior route, target, air-yard, end-zone-target, red-zone,
   and expected-touchdown shares.
2. Use Tampa Bay's games with and without Evans as evidence, with appropriate
   controls for other unavailable teammates and quarterback/game context.
3. Combine that team-specific evidence with league-wide transition priors.
4. Reallocate the vacated role across every relevant Tampa Bay receiver, tight
   end, and back according to role compatibility.
5. Permit Tampa Bay's team passing efficiency and TD pool to change; do not
   assume all of Evans's opportunity survives unchanged.

### 10.2 San Francisco arrival

1. Forecast San Francisco's team opportunity and touchdown pools without
   adding Evans's Tampa Bay totals.
2. Insert Evans as an outside/end-zone role demand within the existing depth
   chart and scheme.
3. Recalculate all teammate shares jointly so displaced opportunity is visible.
4. Permit a bounded, evidence-backed team-efficiency change from Evans's
   presence.
5. Produce featured-role, standard-starting-role, and age/injury-limited
   scenarios unless evidence justifies another documented scenario set.

### 10.3 Fixture assertions

- Tampa Bay and San Francisco each reconcile independently.
- No Evans touchdown is copied from one team to the other.
- Every teammate affected by the allocation is included, including downgrades.
- With/without-Evans evidence is as-of safe and does not treat other injuries
  as Evans effects.
- Fixed inputs and seed reproduce identical scenarios and forecasts.
- The explanation identifies team-pool change, role-share change, conversion,
  uncertainty, and the largest teammate effects.

## 11. Redraft and Best Ball consumption

### 11.1 Redraft

Aggregate weekly means under the selected scoring contract:

```text
season expected points = sum(weekly expected fantasy points)
```

Expose the active-game rate, expected availability, season P10/P50/P90,
changed-team scenario spread, confidence, and derivation. Rank by the existing
redraft value/replacement policy only after the V2 projection is promoted.

### 11.2 Best Ball

Feed weekly stat-line draws into the existing exact DraftKings scorer and
maximum legal lineup simulation. Candidate value remains marginal counted
roster value, including counted points/weeks, bye coverage, spike capture,
correlation, roster P90, and return probability.

Until promotion, both products display V2 only as a clearly labeled shadow
comparison. The live projection and order remain unchanged.

## 12. Validation

Use rolling-origin evaluation across 2020–2025 with a declared preseason cutoff
for each season. Training may use only earlier eligible data. Preserve immutable
run artifacts, fixed seeds, source hashes, model version, calibration version,
and feature coverage.

The implemented split, prediction/outcome isolation, scoring-envelope,
persistence, and replay contract is documented in
[`fantasy-football-v2-backtest-harness.md`](fantasy-football-v2-backtest-harness.md).
The frozen metric definitions, cohort behavior, baseline labels, Week 1–17
product boundary, and paired-bootstrap units are documented in
[`fantasy-football-v2-metric-suite.md`](fantasy-football-v2-metric-suite.md).
The fail-closed feature eligibility, leakage, deterministic-seed, exact
artifact replay, and frozen champion-versus-challenger contract is documented
in [`fantasy-football-v2-validation-audits.md`](fantasy-football-v2-validation-audits.md).

Team-opportunity metrics:

- MAE and bias for plays, attempts, targets, RB carries, RB targets, passing
  touchdowns, and rushing touchdowns;
- proper distribution score such as CRPS;
- P10/P50/P90 empirical coverage;
- calibration by team stability, quarterback/play-caller change, and fallback
  tier;
- comparison with simple rolling-average and league-average baselines.

Player and product metrics remain those in the parent specifications, including
season-total MAE/bias, weekly distribution calibration, Top-12 precision/recall,
spike recall, Best Ball counted points, and draft-decision regret with paired
bootstrap confidence intervals.

## 13. Reconciliation and quality gates

Automated tests must demonstrate, within documented tolerances:

```text
sum(player targets) = allocatable team targets
sum(RB player targets) = team RB targets
sum(player carries) = allocatable team carries
sum(player receiving-TD opportunity) = team passing TDs
sum(player rushing-TD opportunity) = team rushing TDs
0 ≤ every share and probability ≤ 1
P10 ≤ median ≤ P90
```

Also require:

- unique team-week and player-week identities;
- complete schedule and bye coverage;
- no future/as-of leakage;
- deterministic replay from stored seed and run metadata;
- save/resume compatibility for draft simulations;
- identical football inputs for redraft, Best Ball, and AI advisors;
- explicit stale/missing/fallback UI states;
- representative real 2026 processing;
- desktop and mobile visual verification before promotion.

## 14. Dependency-ordered delivery

1. **Foundations:** freeze and label the live champion; complete immutable
   historical data and effective-dated roster/team context.
2. **Validation framework:** rolling-origin harness, metrics, bootstrap,
   leakage checks, deterministic artifacts.
3. **Team opportunity:** forecast and calibrate reconciled weekly team pools.
4. **Player allocation:** transition priors, role scenarios, and team-share
   conservation.
5. **Efficiency and touchdowns:** contextual efficiency plus separate TD
   opportunity and conversion.
6. **Weekly outcomes:** calibrated player weeks, availability, correlation,
   and exact scoring.
7. **Promotion gate:** compare V2 with the frozen champion and market baselines;
   return `PROMOTE`, `REVISE`, or `RETAIN`.
8. **Product delivery:** shadow UI, explanations, decision snapshots, real-data
   and visual verification; activate only after an explicit promotion decision.

No later wave may be treated as complete because partial code already exists.
Existing Best Ball shadow simulation is reusable evidence only after its
applicable acceptance criteria are independently verified.

## 15. Initial implementation slice

The smallest first vertical slice is:

1. freeze and identify the current champion projection artifact;
2. ingest one reproducible historical team-week opportunity dataset;
3. define and test the counting/reconciliation semantics;
4. forecast RB carries, RB targets, and passing/rushing touchdowns with
   uncertainty in a rolling-origin harness;
5. persist the forecast and derivation without changing displayed points.

The Evans fixture starts only after team forecasts reconcile and the player
allocation dependency is eligible.
