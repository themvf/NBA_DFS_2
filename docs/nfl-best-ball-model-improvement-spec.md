# NFL Best Ball Model Improvement Specification

**Status:** Approved working hypothesis — implementation authorized in Jira dependency order; V2 remains a challenger until promotion gates pass
**Product:** DraftKings NFL Best Ball Draft Lab  
**Route:** `/fantasy-football/best-ball`  
**Date:** 2026-08-02

This document extends the broader season-long architecture in `docs/fantasy-football-draft-spec.md`. Where the two differ, this specification governs the DraftKings Best Ball model only.

## 1. Executive decision

The Best Ball page should stop ranking players primarily by projected season-long PPR points and replacement value. It should rank and recommend players by their **incremental contribution to a specific Best Ball roster**, using weekly outcome distributions, automatic optimal-lineup selection, DraftKings scoring, player correlation, bye-week coverage, draft price, and the probability that a player will remain available.

The target model is not intended to predict one exact season total. It should answer:

> Given the current 12-team room, my roster, the current pick, the DraftKings scoring rules, and uncertainty in weekly outcomes, which available player adds the most points to my automatically selected lineup over the full Weeks 1–17 season?

The model must remain independent and reproducible. ADP is a market comparison and draft-timing input, not a hidden component of the player performance projection.

### 1.1 Approved V2 proposition

V2 models running-back production through constrained team opportunity, role allocation, contextual efficiency, touchdown share, and weekly uncertainty. The architecture is intended to prevent projections from ignoring team changes or exceeding plausible team ceilings.

This is a working hypothesis, not an outperformance claim. Coefficients and transition priors must be learned from leakage-safe historical data. V2 operates beside `ff-independent-v1.4` as a challenger until the final champion-versus-challenger gate returns `PROMOTE`.

The following values are expressly prohibited as unverified production assumptions:

- a fixed percentage of vacated opportunity that must transfer to one player;
- a fixed workload-to-efficiency penalty;
- a fixed same-team correlation coefficient;
- an arbitrary player/league blend, team adjustment, or `n / 32` shrinkage rule presented as calibrated.

Such values may exist only as named candidate hyperparameters inside the training and validation system. Their source, training window, uncertainty, and holdout result must be recorded.

## 2. Current-state audit

### 2.1 What exists

The current independent projection pipeline in `ingest/ff_independent.py`:

- uses up to three prior NFL seasons;
- converts season totals to fantasy points per game;
- applies fixed recency weights;
- regresses points per game toward a position prior using the player's actual eligible historical games plus four equivalent prior games;
- converts the resulting active-game rate to a 17-game displayed season baseline;
- estimates availability separately without deducting missed-game risk from that displayed baseline;
- applies a bounded depth-chart role factor;
- assigns a low/high range with fixed multipliers;
- ranks players by season projection and position-level value over replacement;
- stores current Fantasy Football Calculator ADP separately from the projection;
- explains which seasons, weights, historical-game sample, priors, 17-game baseline, depth order, and separate availability estimate were used.

The `ff-independent-v1.4` correction deliberately does not blend FantasyPros into our projection. FantasyPros remains a comparison benchmark used to reveal disagreement and evaluate calibration. The correction fixes an internal arithmetic error: established players had previously been shrunk as though their evidence sample were roughly one weighted season, and their active-game rate was then multiplied by fewer than 17 games.

The Best Ball route then takes the top 260 QB/RB/WR/TE players from that PPR ranking set. The local draft simulator correctly handles 12 teams, 20 snake rounds, automatic pick advancement, roster validation, persistence, and draft-results tracking.

### 2.2 What the current model does not do

The existing Best Ball page already discloses the central omissions:

- DraftKings yardage bonuses;
- weekly performance distributions and spike weeks;
- automatic weekly best-lineup selection;
- teammate and game-level correlation;
- current teammates and reconciled team opportunity;
- offensive-line and coaching/play-caller context;
- 2026 weekly opponent schedule and bye-week coverage;
- direct Best Ball roster-construction value;
- probability a player survives to the user's next pick;
- chronological validation of Best Ball decisions.

These are not small tuning gaps. They mean the current rank is a useful baseline player list, but it is not yet a Best Ball decision model.

## 3. Goals and non-goals

### 3.1 Goals

1. Produce a calibrated weekly probability distribution for every draftable player.
2. Apply the exact DraftKings scoring and yardage bonuses to each simulated week.
3. Calculate automatic weekly Best Ball starters from the full roster.
4. Measure the marginal value of each candidate to the user's current roster.
5. Model empirically supported teammate and game correlations.
6. Value bye-week backups and non-overlapping spike weeks across the full Weeks 1–17 season.
7. Use ADP to estimate draft cost and return probability without using ADP to manufacture performance.
8. Explain every recommendation in plain language with source, freshness, sample, confidence, and principal drivers.
9. Prove improvement with chronological holdouts before promoting the model as the default rank.

### 3.2 Non-goals

- DFS slate projections or lineup optimization;
- in-season waiver, trade, or start/sit advice;
- blind imitation of FantasyPros or another public ranking;
- an opaque single-number machine-learning rank with no decomposable evidence;
- portfolio exposure across multiple Best Ball entries in the first release;
- live DraftKings room automation or authenticated draft-group discovery;
- tournament-round advancement, contest cut lines, or special playoff-week weighting.

## 4. Numbered acceptance criteria

The implementation is complete only when every applicable criterion passes.

1. Every draftable player has weekly P10, median, mean, P75, and P90 fantasy-point distributions for Weeks 1–17.
2. Weekly scoring uses the DraftKings Best Ball rules currently displayed in the application, including 300-yard passing and 100-yard rushing/receiving bonuses.
3. A simulation selects exactly 1 QB, 2 RB, 3 WR, 1 TE, and 1 RB/WR/TE FLEX as the highest-scoring eligible lineup each week.
4. The model reports expected cumulative automatically counted roster points and distribution percentiles for the complete Weeks 1–17 season.
5. Candidate value is calculated as the change in full-season automatically counted roster points caused by adding that player, not as the player's standalone season points.
6. Every player has a weekly probability of entering the automatically counted lineup and an expected number of counted weeks.
7. Every roster has a weekly position/lineup coverage report that identifies bye-week shortages and estimates the points lost because an eligible backup is unavailable.
8. Same-team and opponent correlations are estimated from historical weekly data, stored by relationship type, and capped or shrunk when samples are weak.
9. Weekly availability is modeled separately from points conditional on being active.
10. Team-level projected attempts, carries, targets, touchdowns, and plays reconcile within documented tolerances.
11. Rookies and players changing teams use scenario distributions rather than inheriting last season's NFL role unchanged.
12. Every Week 1–17 opponent, venue, and bye comes from a versioned schedule snapshot.
13. ADP is stored as a separate time-stamped market snapshot and is not a feature in the player performance distribution.
14. The recommendation engine reports both `Draft now` value and estimated probability of availability at the user's next selection.
15. Recommendations recalculate after every pick and reflect position counts, roster correlations, bye-week lineup coverage, spike overlap, and remaining draft capital.
16. Every recommended player has at least three concise drivers: performance, roster fit/correlation, and draft timing/risk.
17. Missing, stale, or low-coverage inputs reduce confidence and are visible; they are never silently replaced by zero.
18. All training examples and backtests enforce as-of dates and exclude data not available on the simulated draft date.
19. The new model beats or meaningfully complements the current season-total/VOR baseline on predeclared holdout metrics before becoming the default.
20. Automated unit, integration, leakage, calibration, and policy tests pass.
21. Representative real 2026 data is processed end to end.
22. The rendered application is visually inspected at desktop and mobile widths, with evidence attached to the operational work item.

## 5. Source and data contracts

All inputs must be stored as immutable source snapshots with a source name, dataset name, season/week, requested parameters, fetched time, source-published time when available, row count, matched count, missingness, response hash, and model eligibility flag.

| Input | Preferred source | Current status | Use | Required freshness in draft season | Fallback |
|---|---|---:|---|---:|---|
| Canonical NFL player metadata | Sleeper player directory | Connected | IDs, name, team, position, rookie status | Daily | Last successful snapshot with stale flag |
| Weekly rosters and depth context | nflverse weekly rosters | Connected | Team, roster status, depth context, identity | Daily | Last successful snapshot |
| Historical weekly stats | nflverse weekly player stats | Connected | Baseline volume, efficiency, fantasy outcomes | Weekly/offseason refresh | Cached immutable history |
| Schedule, opponents, venues, byes | nflverse schedules | Connected | Week 1–17 opponent, venue, and bye coverage | Daily after schedule changes | Last successful snapshot |
| Play-by-play | nflverse play-by-play | New requirement | Team plays, pass rate, EPA, red zone, game environment | Weekly | Weekly stats with lower confidence |
| Participation/snaps | nflverse participation data | New requirement | Routes, snaps, personnel usage, role stability | Weekly | Roster/depth plus weekly opportunities |
| Current ADP | Fantasy Football Calculator 12-team feeds | Connected | Draft cost, value, next-pick survival | Every 6 hours | Last success, then no timing recommendation |
| Historical PPR ADP | FantasyPros `type=ADP` historical consensus endpoint | 2020–2025 contracts verified | Draft-time market baseline | Immutable preseason snapshot | Dated FantasyPros ranking archive as ECR-only comparison; never silently relabel ECR as ADP |
| DraftKings Best Ball scoring | Versioned application rules contract | Partially represented in UI | Simulation scoring and bonuses | On rule change | Block model promotion if unknown |
| Injuries/PUP/suspensions | Existing player metadata plus verified injury source | Partial | Weekly availability and uncertainty | Daily; faster near draft | Status unknown, confidence reduction |
| Transactions/team changes | Sleeper/nflverse roster history | Partial | Effective-dated team and role changes | Daily | Manual review queue |
| Coaching/play caller | Versioned manual or licensed dataset | Missing | Pace, pass rate, role allocation priors | On change | Team prior with high uncertainty |
| Offensive-line quality | Versioned licensed/open dataset | Missing | Pressure, rushing efficiency, sack context | Weekly/monthly | League-average prior |
| Weather | Not required for preseason rank | Not connected | Optional late-draft weekly distribution | When material | Neutral weather distribution |

### 5.1 Source-selection rules

- Do not add a source until its license, access method, fields, cadence, and historical availability are documented.
- Do not scrape DraftKings pages as a hidden production dependency.
- Do not use an LLM call, including DeepSeek, to create numeric player projections.
- Human notes may adjust scenario probabilities only through an effective-dated, attributable override with a reason and expiry.
- Market rank and model rank must always remain separate fields.
- Historical roster files with only season/week semantics may support week-level joins, but they may not be represented as arbitrary preseason draft-date snapshots.
- A historical ADP row is eligible only when its provider timestamp is on or before the declared simulated draft cutoff.

### 5.2 Verified historical-source checkpoint

Real-data FantasyPros audits passed all eight required endpoint contracts in each historical season. PPR ADP coverage is:

| Season | Rows | Provider updated (UTC) | Response SHA-256 | GitHub run |
|---:|---:|---|---|---:|
| 2020 | 583 | 2020-09-10 13:37:44 | `7ef1bfe597735ba66aff354ff060a22806eaf18f8982a2f62446f0de2711595a` | 30765418531 |
| 2021 | 487 | 2021-09-09 12:37:00 | `87b14329bf577ac364cc97426638ebef2c7c8b65cae4d1b567b07b51558183de` | 30765717079 |
| 2022 | 353 | 2022-09-08 13:12:00 | `1ed7580c700fc42f76a72974f54719a3d7373f00cc0ae42d2fbbfbcf784d029c` | 30765789358 |
| 2023 | 593 | 2023-09-06 07:09:38 | `a44f504a3d74ebe7dd279438a5dd35f615e0eeb6eb4269ce9b02516b2f7b45a5` | 30765822087 |
| 2024 | 948 | 2024-09-04 07:09:48 | `6756b293b7b990b0ef0934d39ddf9e3c3d0509d41ae5bbe7b81dc078e4966145` | 30765862022 |
| 2025 | 985 | 2025-09-03 07:10:28 | `fc40f657e5dc6c2640d59744bdc3bd8aef6294c32f5a794efc4edf915f019afe` | 30765720804 |

All six PPR ADP timestamps precede the first regular-season kickoff for their season. Endpoint accessibility and draft-time eligibility remain separate checks: for example, the currently returned 2023 PPR draft-ranking response is timestamped 2024-02-12 and is not eligible for a 2023 preseason decision, while the 2023 PPR ADP response is eligible.

nflverse weekly roster partitions exist for every 2020–2025 season and contain `gsis_id`, but they do not publish a source-effective timestamp. They are therefore week-granular roster evidence, not proof of a roster state at every possible preseason draft timestamp.

The DynastyProcess/nflverse FantasyPros ranking archive supplies dated ECR/ranking snapshots from 2019 onward. It is a secondary market-rank baseline. It is not the ADP baseline unless the source explicitly identifies the record as ADP.

## 6. Proposed data model

This section defines contracts, not migrations.

### 6.1 Weekly feature snapshot

One row per player, target season, target week, and as-of date:

```text
player_id, season, week, as_of_at, team, opponent, venue
active_probability, starter_probability, role_scenario
team_plays, team_pass_attempts, team_rush_attempts, team_points
snap_share, route_share, target_share, rush_share
red_zone_target_share, goal_line_rush_share
air_yards_share, targets_per_route, yards_per_route
qb_attempts, qb_designed_rushes, qb_scrambles
opponent_features, schedule_features
feature_snapshot_id, source_snapshot_ids, missing_flags
```

### 6.2 Weekly player distribution

```text
player_id, model_version, feature_snapshot_id, season, week
active_probability
p10, p25, median, mean, p75, p90
spike_20_probability, spike_25_probability, spike_30_probability
dk_bonus_pass_300_probability
dk_bonus_rush_100_probability
dk_bonus_receive_100_probability
scenario_probabilities
calibration_version, created_at
```

Spike thresholds must be position-aware in the UI even if common thresholds are retained for comparison.

### 6.3 Correlation contract

```text
relationship_type: QB_WR | QB_TE | QB_RB | WR_WR | RB_WR | OPP_PASS | OPP_GAME
player_a_id, player_b_id, team/game context
raw_correlation, shrunk_correlation, sample_weeks
season_window, as_of_at, source_snapshot_ids
```

### 6.4 Draft decision snapshot

Every recommendation shown during a draft should be reproducible:

```text
draft_session_id, overall_pick, roster_player_ids, available_player_ids
ranking_snapshot_id, model_version, simulation_seed, simulation_count
candidate_player_id, marginal_utility, draft_now_rank
next_pick_number, return_probability, roster_fit_score
season_counted_points_delta, expected_counted_weeks_delta
bye_coverage_delta, spike_capture_delta
correlation_delta, fragility_delta, explanation_json, created_at
```

## 7. Target model

### 7.1 Model layers

```text
immutable source snapshots
        ↓
as-of player, team, schedule, and role features
        ↓
weekly opportunity + efficiency + availability distributions
        ↓
correlated weekly stat simulation
        ↓
DraftKings scoring and automatic best-lineup selection
        ↓
full-season counted roster-point distribution
        ↓
candidate marginal value + draft-timing decision
```

### 7.2 Team environment

Forecast each team's weekly opportunity before allocating it to players:

```text
expected_plays = f(team pace, opponent pace, neutral pace, score environment)
expected_pass_rate = f(neutral pass rate, PROE, play caller, QB, opponent)
expected_attempts = expected_plays × expected_pass_rate
expected_rushes = expected_plays − expected_attempts − expected_sacks
expected_touchdowns = f(team scoring rate, opponent defense, venue)
```

Team forecasts must be shrunk toward league averages. Player allocations must reconcile:

```text
Σ player target shares ≈ 1.00
Σ player rush shares ≈ 1.00
Σ player route shares cannot imply more routes than team dropbacks
Σ player touchdown expectations must remain plausible versus team touchdowns
```

The pipeline should fail its quality gate when reconciliation exceeds the configured tolerance.

### 7.3 Player opportunity

Model opportunity separately by position:

- **QB:** attempts, designed rushes, scrambles, sacks, red-zone attempts.
- **RB:** carries, targets, routes, goal-line carries, two-minute usage.
- **WR/TE:** routes, targets per route, target depth, red-zone/end-zone targets.

Use hierarchical partial pooling. A player estimate should combine individual history, current role, team context, position/role priors, and uncertainty proportional to sample size.

Example structure:

```text
player_weekly_targets = team_attempts
                      × route_participation
                      × targets_per_route

posterior_targets_per_route = weighted(
    player history,
    role/position prior,
    current-team scenario,
    evidence sample size
)
```

### 7.4 Efficiency and touchdown regression

Efficiency should be modeled after opportunity and regressed more strongly than volume:

- catch probability by target depth and quarterback context;
- yards per reception/rush after opponent and role adjustment;
- passing/rushing/receiving touchdown rate from high-value opportunities;
- interception, sack, and fumble-lost probability;
- return and offensive fumble-recovery touchdowns as rare-event components.

Do not project touchdowns by carrying forward last season's rate. Use red-zone and goal-line opportunity, team scoring expectation, and a position/role prior.

### 7.5 Availability

Keep availability separate from active-game performance:

```text
weekly_points = 0                              if inactive
weekly_points ~ active_performance_distribution if active
```

Availability features include injury designation, PUP/IR/suspension status, recurring versus one-time injury class when available, prior active games, age, position, and role. Unknown injury detail must widen uncertainty instead of forcing a precise penalty.

### 7.6 Rookie and changed-team scenarios

Rookies and materially changed roles require explicit scenarios, for example:

```text
earns full role | partial/committee role | reserve role
team target leader | secondary target | rotational receiver
starting QB | delayed starter | backup
```

Scenario probabilities should use draft capital, age, college production when licensed/available, depth chart, transaction investment, vacated opportunity, and preseason role evidence. Until those inputs exist, use wider priors and mark the missing evidence.

### 7.7 Weekly simulation and DraftKings scoring

For each simulation and week:

1. Draw active/inactive status.
2. Draw team plays, attempts, rushes, and touchdowns.
3. Draw reconciled player opportunity.
4. Draw correlated efficiency and scoring outcomes.
5. Apply DraftKings points:

```text
passing:   yards / 25 + TD × 4 − interceptions
rushing:   yards / 10 + TD × 6 + two-point conversions × 2
receiving: receptions + yards / 10 + TD × 6 + two-point conversions × 2
other:     return TD × 6 − fumbles lost + offensive recovery TD × 6
bonuses:   +3 for 300 passing yards
           +3 for 100 rushing yards
           +3 for 100 receiving yards
```

Bonuses must be applied to the simulated stat line, not approximated as a constant point bump.

### 7.8 Automatic weekly lineup

For each simulated roster/week, select the maximum legal score:

```text
1 QB
2 RB
3 WR
1 TE
1 FLEX from remaining RB/WR/TE
```

The model must preserve zeroes for inactive players and naturally capture the Best Ball value of depth, contingent upside, and non-overlapping spike weeks.

Define the counted weekly and season scores directly:

```text
CountedScore_week(R) = max over every legal lineup L contained in roster R
                       of Σ simulated_points(player, week) for player in L

SeasonScore(R) = Σ CountedScore_week(R), for week = 1...17
```

A player's simulated points count only when that player enters the highest-scoring legal lineup for that week. Standalone season points that remain on the simulated bench have no direct roster value.

Example: if a roster's wide receivers score 28, 21, 14, 9, and 4 points, the optimizer counts the best legal combination of those receivers across the three WR slots and FLEX. The 28-point spike is valuable because it replaces a lower counted score; the 4-point result has no value if it remains on the bench. FLEX eligibility must be optimized jointly with RB and TE rather than assigned after independently choosing the top three WRs.

### 7.9 Bye-week backup and lineup coverage

Bye-week analysis must be position- and week-specific. Counting total players or counting repeated bye weeks is not sufficient.

For each week and simulation, calculate:

```text
eligible QB/RB/WR/TE players after byes and inactive outcomes
whether a complete legal lineup can be formed
expected counted score with the actual bye schedule
expected counted score under a neutral/no-bye counterfactual
bye_coverage_loss = neutral_score − actual_bye_score
```

A backup has high bye value when he is eligible during a week in which the roster would otherwise lose a starting slot or be forced to count a materially weaker player. The model should reward complementary bye schedules but avoid mechanically penalizing two players with the same bye when the roster already has adequate coverage.

Example: if both rostered quarterbacks have Week 8 byes, the roster records no QB points that week. A later quarterback with a different bye has measurable value equal to the Week 8 QB points he is expected to restore. At RB, WR, and TE, the value is the improvement to the jointly optimized position-plus-FLEX lineup, not a generic reward for having a different bye.

Required outputs:

- probability of filling every required position by week;
- probability of filling FLEX by week;
- expected points lost to bye-week shortages;
- candidate reduction in bye-week coverage loss;
- the specific weeks and positions improved by the candidate.

### 7.10 Spike-game capture

Best Ball value is driven by whether a player's high outcome enters the automatically selected lineup. Calculate:

```text
counted_probability(player, week) = P(player is in the optimal legal lineup)

expected_counted_points(player, week) =
    E[player_points × I(player is counted)]

spike_capture_delta(candidate) =
    E[SeasonScore(roster + candidate) − SeasonScore(roster)]
    attributable to candidate outcomes above the incumbent counted score
```

The model should distinguish:

- a stable player who frequently covers weak or bye-week lineups;
- a volatile player whose ceiling creates several counted spike weeks;
- a player with strong standalone projections whose points rarely count because the roster already has stronger, similarly timed options;
- correlated teammates whose spike games are likely to occur together and raise the roster ceiling.

Do not equate volatility with value automatically. A spike matters only when it improves the maximum legal lineup score.

### 7.11 Correlation

Estimate correlation on weekly residuals after removing common opponent and scoring-environment effects. Use shrinkage toward relationship priors and require minimum samples.

Expected relationship directions are hypotheses, not hard-coded truth:

- QB with own WR/TE: usually positive;
- QB with pass-catching RB: role-dependent;
- two receivers competing for the same team volume: often negative conditional on team output;
- RB with own passing game: game-script and role dependent;
- opposing passing players: potentially positive through shared shootout environment.

The simulation must use a positive-semidefinite correlation structure. If a player-pair estimate is unavailable, use the shrunk relationship prior and show lower confidence. Correlation value is measured through its effect on counted weekly and full-season roster points.

### 7.12 Full-season utility

The model has one primary objective:

```text
maximize the distribution of cumulative automatically counted roster points
across Weeks 1–17
```

Report the mean, median, P75, P90, and downside percentile of `SeasonScore`. Weeks 15–17 are ordinary scoring weeks under the same objective; they receive no special advancement or playoff multiplier.

## 8. Draft decision policy

### 8.1 Candidate marginal value

For every eligible available player `c`:

```text
marginal_value(c) = E[U(roster ∪ c)] − E[U(roster)]
```

`U` is the simulated full-season automatically counted roster score, not a fixed weighted sum of standalone player statistics. Report the components:

```text
full-season counted-points delta
expected counted-weeks delta
bye-week coverage delta
spike-capture delta
lineup-counted probability
stack/correlation delta
roster fragility delta
```

### 8.2 Draft now versus wait

Fit an ADP pick-distribution model using time-stamped 12-team ADP snapshots and observed/manual room behavior when sufficient history exists.

```text
P(return) = P(player remains undrafted through user's next pick)

wait_value(c) = P(return) × expected future marginal value
```

The UI should distinguish:

- **Draft now:** high marginal roster value and unlikely to return.
- **Can wait:** valuable, but likely available at the next pick.
- **Roster fit:** not the highest standalone rank, but materially improves construction.
- **Avoid for this roster:** good player whose role/correlation duplicates existing exposure.

ADP must never change the simulated football outcomes.

### 8.3 Roster-construction policy

The decision engine should enforce only hard DraftKings roster rules. Other construction principles are evidence-based recommendations, not blockers:

- QB/pass-catcher stacks;
- adequate but not excessive position depth;
- complementary spike profiles and empirically supported teammate correlation;
- avoidance of a single fragile NFL offense dominating the roster;
- enough non-bye eligible backups to fill every weekly lineup, with emphasis on positions where a shortage would force a zero or weak counted score;
- contingent-value RBs and asymmetric late-round outcomes;
- quarterback and tight-end timing based on remaining tiers and return probability.

Every policy must have a backtestable definition and an explanation. No unexplained `best_ball_boost` is permitted.

## 9. Validation and promotion gates

### 9.1 Chronological design

Use immutable as-of snapshots and rolling-origin evaluation across 2020–2025. For each evaluation season, train only on eligible records from prior seasons and freeze the model before scoring that season. The exact preseason decision cutoff for every season must be declared before fitting. Process 2026 prospectively without using results to tune preseason ranks.

If a required historical source does not exist for an as-of date, exclude that feature from both training and comparison. Do not backfill today's depth chart into an old draft.

### 9.2 Baselines

Compare against:

1. current independent season-total/VOR model;
2. prior-season fantasy points per game;
3. market ADP;
4. market rank/ECR only when a licensed snapshot is actually available;
5. ablations with schedule, correlation, availability, and roster-fit layers removed.

### 9.3 Metrics

Player-week distribution metrics:

- weighted interval coverage;
- CRPS or an equivalent proper distribution score;
- MAE/RMSE as secondary point metrics;
- Brier score and calibration for 15+, 20+, 25+, and 30+ point outcomes;
- bonus-threshold calibration;
- active/inactive probability calibration.

Roster and decision metrics:

- simulated versus realized optimal weekly roster score;
- lineup-counted probability calibration;
- cumulative Weeks 1–17 counted-score error and distribution calibration;
- bye-week coverage-loss calibration;
- spike-game probability and spike-capture calibration;
- rank correlation for Best Ball roster contribution;
- candidate decision regret at historical picks;
- top-N recommendation hit rate;
- stack and roster-construction ablation lift.

The definitive promotion set is:

- season-total MAE and bias;
- weekly CRPS or another preregistered proper scoring rule;
- P10/P50/P90 empirical coverage and calibration error;
- Top-12 precision and recall;
- spike-week recall, where the spike definition is versioned and position-aware;
- simulated Best Ball counted points;
- draft-decision regret versus the declared market/baseline decision.

Every comparison must include paired bootstrap confidence intervals, sample sizes, and the exact resampling unit. The season-total MAE improvement gate uses a preregistered paired-bootstrap significance threshold. Overall season-total bias must be within ±5 points and must also be reported by cohort.

Report every metric overall and for QB/RB/WR/TE, rookies, changed-team players, injured players, and small-sample roles.

### 9.4 Promotion policy

The model may replace the current Best Ball rank only when:

- holdout distribution calibration is within the predefined tolerance;
- it improves at least one primary roster/decision metric without materially degrading the others;
- improvement persists across multiple positions and is not driven by a small player segment;
- all leakage and provenance checks pass;
- the UI can explain the new rank;
- a shadow-mode comparison has run on real 2026 draft snapshots.
- the final decision is explicitly `PROMOTE`, `REVISE`, or `RETAIN V1.4`.

Until then, show it as `Best Ball model — shadow` beside the existing baseline.

## 10. Application behavior and UI specification

### 10.1 Player board

Add Best Ball-specific fields without removing the current source context:

```text
Best Ball Rank | Overall/Pos Rank | ADP | Return Probability
Weekly Median | P90 | Spike Rate | Expected Counted Weeks
Bye Coverage | Stack Fit | Roster Marginal Value | Confidence
```

Use progressive disclosure so the virtualized board remains fast. The default row should show only the most decision-relevant values.

### 10.2 Recommendation panel

When a team is on the clock, show:

```text
Best pick now
Why he helps this roster
Why now instead of waiting
Expected full-season counted-points contribution
Bye weeks/positions covered
Expected spike weeks added to the counted lineup
Stack/correlation effect
Main downside and confidence
Two alternatives with tradeoffs
```

Example message:

> Draft WR A now. He adds 2.3 expected counted weeks, covers your WR shortage in Week 8, raises the roster's full-season P90 by 8.4 points, and has only a 22% chance to return at your next pick. WR B projects similarly by season total but his spike weeks are less likely to enter your current lineup.

### 10.3 Player evidence drawer

Show:

- weekly distribution chart;
- role and opportunity forecast;
- active probability;
- team allocation and reconciliation status;
- Week 1–17 opponents and bye;
- weekly counted-lineup probability and expected counted points;
- bye-week coverage improvement by position;
- spike outcomes that replace the current counted player;
- correlation with rostered players;
- model versus ADP disagreement;
- source timestamps, missing inputs, samples, and model version;
- scenario probabilities for uncertain roles.

### 10.4 Missing-data messages

Use explicit language:

- `ADP unavailable — return probability not calculated.`
- `Participation data missing — role distribution uses depth-chart prior.`
- `Play-caller context unavailable — team pass rate is shrunk to league average.`
- `Schedule snapshot stale — opponent and bye-week adjustments disabled.`
- `Low sample — correlation uses relationship prior.`

Do not show `0`, `neutral`, or a confident badge when the input is missing.

## 11. Quality, leakage, and observability

Required automated checks:

- player/team identity match coverage;
- duplicate player-week rows;
- schedule and bye coverage;
- team opportunity reconciliation;
- active-probability bounds;
- distribution monotonicity (`P10 ≤ median ≤ P90`);
- finite simulated scores and stable random seeds;
- correlation matrix validity;
- model input timestamps no later than the simulated as-of date;
- ADP excluded from performance features;
- DraftKings scoring unit tests at every threshold boundary;
- weekly lineup optimizer tests, including FLEX and ties;
- bye-week coverage tests for every position and FLEX;
- spike-capture tests proving that only points entering the maximum legal lineup receive roster credit;
- draft recommendation reproducibility from a stored decision snapshot.

Operational telemetry:

```text
feature coverage by source
snapshot age and failure reason
players using fallbacks
simulation duration and count
recommendation latency
calibration version
model/ADP disagreement distribution
draft action and recommendation acceptance
```

## 12. Dependency-ordered implementation plan

No implementation should begin until the preceding required phase passes.

### Phase 0 — Infrastructure

- complete source contracts and immutable historical as-of snapshots;
- populate 2020–2025 source partitions and effective-dated roster/team context;
- freeze a reproducible `ff-independent-v1.4` baseline;
- freeze a timestamped draft-time PPR ADP baseline.

### Phase 1 — Validation framework

- build the chronological 2020–2025 backtest engine;
- implement the definitive metric suite and paired bootstrap intervals;
- enforce rolling-origin splits, leakage checks, deterministic seeds, and reproducible run artifacts;
- establish the champion-versus-challenger comparison protocol before fitting V2.

### Phase 2 — Team opportunity forecast

- forecast team carries, team RB targets, and team touchdowns with uncertainty;
- constrain every player allocation to reconciled team ceilings.

### Phase 3 — Player allocation

- learn historical transition priors for vacated opportunity;
- allocate early-down, receiving, and goal-line roles;
- model competition, changed-team, rookie, and injury scenarios.

### Phase 4 — Efficiency and touchdowns

- estimate contextual efficiency from eligible player and team evidence;
- model touchdown share separately from volume and yardage;
- do not impose an automatic workload penalty.

### Phase 5 — Weekly distributions and roster simulation

- generate game-script and availability scenarios;
- estimate relationship-type correlation priors with hierarchical shrinkage;
- simulate player weeks, exact DraftKings scoring, automatic lineups, bye coverage, and spike capture.

### Phase 6 — Validation gate

- compare V2 with V1.4 and draft-time ADP using the preregistered metrics;
- return `PROMOTE`, `REVISE`, or `RETAIN V1.4`;
- keep V2 as challenger when evidence is inconclusive or any required gate fails.

### Phase 7 — Product delivery and verification

- calculate candidate marginal roster utility and ADP return probability;
- expose source-aware backend payloads and persist decision snapshots;
- add the recommendation UI and evidence drawer without regressing browser performance;
- run automated unit, integration, policy, and leakage tests;
- process representative real 2026 data;
- run shadow decisions in real draft rooms;
- inspect the rendered application;
- attach screenshots, examples, commands, metrics, and model cards;
- promote only if every gate passes.

## 13. Definition of Done

An issue may be marked Done only when:

- every linked acceptance criterion passes;
- data is populated, not merely represented by columns;
- provenance, source freshness, missingness, and sample size are recorded;
- backend calculations consume the intended snapshot;
- the result is visible and understandable in the application;
- missing/stale information is explicit;
- automated tests pass;
- representative real data is processed;
- chronological validation evidence is attached;
- the application is visually inspected;
- screenshots demonstrate the user-facing behavior;
- tested dates, examples, commands, and results are attached to Jira;
- relevant files and commits are linked;
- no acceptance criterion is silently deferred.

Required completion matrix:

| Order | Jira issue | Acceptance criterion | Data | Backend | UI | Tests | Real-data verification | Screenshot | Result |
|---:|---|---|---|---|---|---|---|---|---|
| 1 | TBD | Source contracts/provenance | — | — | — | — | — | — | NOT STARTED |
| 2 | TBD | Historical population/quality | — | — | — | — | — | — | NOT STARTED |
| 3 | TBD | Weekly distributions | — | — | — | — | — | — | NOT STARTED |
| 4 | TBD | Weekly max-lineup, bye, spike, and correlation simulation | — | — | — | — | — | — | NOT STARTED |
| 5 | TBD | Draft decision policy | — | — | — | — | — | — | NOT STARTED |
| 6 | TBD | Backend/UI delivery | — | — | — | — | — | — | NOT STARTED |
| 7 | TBD | Real-data and visual verification | — | — | — | — | — | — | NOT STARTED |

No overall `complete` result is permitted while a required row is FAIL, BLOCKED, or NOT STARTED.

## 14. Open source and engineering decisions

1. Approve sources for participation, injuries, coaching/play caller, and offensive line.
2. Preserve the approved historical cutoff policy: the first scheduled regular-season kickoff from the nflverse schedule.
3. Decide whether manual analyst scenario overrides are desired in the first release.
4. Choose the simulation budget for interactive draft recommendations after benchmarking accuracy versus latency.
5. Decide whether the initial model remains local/single-user or must support server-persisted multi-device draft sessions.

## 15. First deliverable

The first release is a **shadow Best Ball challenger** that:

- uses existing nflverse/Sleeper/FFC sources plus play-by-play and participation;
- produces weekly DraftKings distributions;
- performs automatic weekly lineup simulation;
- reports full-season marginal counted points, expected counted weeks, bye-week coverage, spike capture, P90 change, stack fit, and return probability;
- displays its rank beside the current rank;
- collects prospective 2026 decisions for later calibration.

This deliverable creates actionable Best Ball intelligence while preserving a measurable comparison with the current model.
