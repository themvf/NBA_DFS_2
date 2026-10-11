# NFL joint player outcomes: developer implementation and validation plan

Prepared: 2026-10-09. Status: implementation specification; proposed modules,
schemas, studies, and acceptance gates below are not claims of completed work.

## 1. Objective and decision scope

Build a reproducible per-game simulation of the joint distribution of NFL player
production. Use the same aligned game scenarios to estimate individual ranges,
game leaders, exact top-three sets, and eventually complete DFS lineup outcomes.
Keep an independent statistical model and an explicitly market-conditioned model
as separate products. Improve the existing local system rather than replace it
with a second unconnected simulator.

The immediate problem is uncertainty in opportunity, efficiency, teammate
competition, and player exits. Thursday's unsuccessful selection motivates an
audit, but does not establish which mechanism failed. The original calculation
reportedly used a scratchpad `ladder.py`, player-level lognormal fits, and manually
chosen correlations. Recover that script and its inputs before treating its
reported 1.1% trio estimate as a reproduced fact.

### Required outcomes

| Outcome | Definition | Primary consumer |
| --- | --- | --- |
| Receptions | Official catches, including overtime unless a contract explicitly excludes it | Individual ranges, reception leaders |
| Receiving yards | Official individual receiving credit, including lateral credit where applicable | Ranges, receiving-yard leaders |
| Rushing yards | Official individual rushing credit, including QB kneels | Ranges, rushing-yard leaders |
| Total yards from scrimmage | Rushing plus receiving yards in the same scenario; excludes passing and returns | Total-yard leaders |
| Exact top-three set | The unordered set of three leaders for one specified metric and eligible field | Top-three contest analysis |
| Longest touchdown | Longest qualifying scoring play under an explicit scoring/period/settlement contract | Separate TD leader consumer |
| Full fantasy points | Complete stat line scored through canonical site rules | DFS lineup analysis |

Do not infer longest-TD probabilities from long gains, add component percentiles
to produce total-yard percentiles, or replace DFS lineup outcomes with yardage
rankings. A conventional DFS GPP and an exact-top-three contest are distinct
decisions with distinct payout rules.

### Deliverable levels

1. **Research-ready:** replayable artifacts, mechanical invariants, disclosed
   missing sources, and historical diagnostic scores. Exploratory estimates may
   be shown to the owner with this status.
2. **Prediction-qualified:** passes a registered, previously unexamined evaluation
   for a named outcome, population, and decision cutoff.
3. **Decision-qualified:** additionally has applicable settlement, executable
   prices or contest payouts, and forward evidence for that decision.

Failure to qualify does not prevent exploratory development or owner-visible
research. It prevents relabeling the result as validated, profitable, or a proven
replacement for the baseline.

## 2. Repository checkpoint and integration constraints

This plan was grounded in the local worktree:
`C:\Docs\_AI Python Projects\NBADFS_v2\.worktrees\nfl-game-leaders`,
branch `codex/nfl-game-leaders`, HEAD `557a46cd` at preparation time.
That commit is a checkpoint, not a claim that its changes are on main.

The primary checkout has unrelated work and a different set of available modules.
For example, it contains prospective NFL prop ingestion that this worktree lacks.
Inspect both branches and resolve dependencies deliberately. Do not copy over a
dirty checkout or reset unrelated changes. Make focused local commits; batch any
eventual push through the integrating session according to `AGENTS.md`. This
document does not authorize production deployment or paid bulk historical capture.

Read first:

- `AGENTS.md`.
- `docs/nfl-team-identity-source-map.md`.
- `docs/nfl-game-leaders-model.md`.
- `docs/nfl-game-leaders-backlog.md`.
- `docs/nfl-shared-simulation-expansion.md`.
- `docs/nfl-longest-touchdown-model.md` before touching TD outcomes.
- `docs/the-odds-api.md` and the existing NFL event-relative capture contract before
  changing provider requests or scheduling.

### Existing code to reuse

| Path | Verified current purpose | Required treatment |
| --- | --- | --- |
| `model/nfl_game_leaders.py` | Joint workload, competitive shares, catches, empirical gains, four leader outcomes | Preserve replayable fixed-dispersion baseline |
| `model/nfl_role_dispersion.py` | Shared role dispersion candidate and interval diagnostics | Reuse allocation helpers; fitted variation is not calibrated accuracy |
| `research/nfl_game_leaders_source.py` | Primary stat-credit enrichment and source reconciliation | Extend coverage; retain quarantine rules |
| `research/nfl_game_leaders.py` | Capture, request, forecast, batch, historical scoring | Add versioned commands/adapters without breaking old artifacts |
| `model/nfl_shared_matchup_scenarios.py` | Candidate full coherent DFS event ledger | Reuse conservation and complete-bank contracts |
| `model/nfl_shared_dfs_efficiency.py` | Candidate DFS efficiency implementation | Separate its marginals from the partial leader engine |
| `research/nfl_shared_dfs_export.py` | Frozen full DFS export | Preserve source-time and retrospective checks |
| `web/src/lib/nfl-dfs/shared-game-model.ts` | Partial production and complete-bank consumers | Preserve the explicit distinction between the two |
| `web/src/lib/nfl-dfs/scoring.ts`, `scenarios.ts`, `lineups.ts` | Canonical scoring, draw validation, legal lineup handling | Remain authoritative |
| `model/nfl_replacement_upside_fit.py` | Descriptive pregame-inactive replacement fantasy-point mixture | NOT a mid-game exit or redistribution model |
| `ingest/nfl_prop_odds.py` in primary checkout | Raw prospective evidence plus normalized ordinary NFL props | Integrate after branch/schema audit; not verified alt-ladder coverage |
| `ingest/nfl_prop_probe.py` in primary checkout | Coverage probe for seven ordinary market keys | Extend with tested alternate keys; do not assume provider availability |

The current local leader engine already has joint simulations. Its shared
historical game volumes and Dirichlet allocations are not independent player
lognormals. Do not attribute the scratchpad's assumptions to this implementation.
The complete DFS candidate has been mechanically checked on a synthetic fixture;
it is not an empirically validated completion of the partial yardage engine.

Protect original registered engine hashes and prior study files. The source map
documents why separate candidate modules were introduced. New integrations must
not silently rewrite protected implementations.

## 3. Phase 0: reproduce Thursday and freeze the study protocol

### 3.1 Recover the actual decision

Create a read-only audit bundle containing:

- Original `ladder.py`, dependencies, RNG seed, simulation count, and code digest.
- Exact event/game, target metric, eligible player field, and contest rules.
- All quoted thresholds, sides, prices, bookmaker identities, and price format.
- Original screenshots/raw inputs and known versus unknown observation times.
- Lognormal parameterization and fitted parameters, fitting objective, residuals,
  bounds, margin assumption, and correlation construction.
- Confirm whether correlations were applied to latent Gaussian variables or actual
  simulated yards; those are not interchangeable correlations.
- Whether the correlation matrix was positive semidefinite and how it was repaired
  if not; preserve any repair rather than replacing it invisibly.
- Original forecast, selected trio, all evaluated alternatives, and result source.

Require independent canonical player/game identities and official final production
before stating Irving + Pickens + Flournoy was the winning set for that contract.
The reported selection and probabilities are audit leads, not verified labels.

If code, seed, or timestamps are unavailable, enumerate what cannot be reproduced.
Do not manufacture a pregame record from a postgame capture. Preserve screenshots
with unknown timestamps as manual observations, not timestamp-verified quotes.

### 3.2 Replay the local leader model

Use the latest eligible frozen inputs at or before the ORIGINAL decision time,
not simply the latest snapshot taken before kickoff. Later same-day information
is not eligible for an earlier decision. Preserve original baseline and candidate
outputs separately. Run:

1. The exact scratchpad calculation, when recoverable.
2. Existing fixed-dispersion leader baseline with aligned exported draws.
3. Existing empirical-dispersion candidate, separately labeled.

Add a scorer for the specific trio if the existing API exports only single-winner
probabilities. Do not guess a trio probability from marginal leader probabilities.

Compare opportunity means/ranges, catch rates, per-play gains, player yardage
quantiles, teammate joint behavior, and exact-set probabilities. Record whether
the winning set is even representable in the candidate field. Missing-player
support is different from an assigned low probability.

If frozen eligible inputs do not exist, produce a retrospective reconstruction
with the missing provenance stated. This is a sanity check, not a validation set.
Never select future parameters to make this one result likely.

### 3.3 Register the entire experiment before scoring variants

Proposed file: `research/studies/nfl_joint_outcomes_v1.json`, with an immutable
digest archived beside results. It must specify:

- Requested outcomes and primary decision endpoint.
- Game seasons/types, player eligibility, decision times, overtime and tie rules.
- Source requirements, expected coverage, exclusions, and missingness analysis.
- Development, tuning, locked test, and forward-shadow periods.
- Exact variants and their permitted parameter grids.
- Baselines, metrics, primary comparisons, multiplicity treatment, and gate values.
- Fixed simulation budgets, Monte Carlo error policy, bootstrap design, and seeds.
- Rules for schema/stat-credit corrections versus model changes.
- Maximum exploratory searches and what happens after the holdout is opened.

Do not call all of 2025 a new untouched test season. Existing reports already
examined substantial 2025 and 2026 periods, including the challenger test period.
Create an exposure registry from study artifacts and prior work. Use genuinely
unexamined historical data only if its status is established; otherwise freeze
future weeks for the final test. Earlier seasons can support development, but are
not a later chronological holdout for a model trained on newer seasons.

## 4. Data architecture and time contracts

### 4.1 Canonical identities and source coverage

- Games: `nfl_season_games.nflverse_game_id` to PBP `game_id`; verify unique season,
  week, teams, kickoff, and game type. Market event mapping additionally verifies
  `matchup_id`, provider event ID, teams, and kickoff.
- Players: GSIS as canonical identity, with explicit versioned mappings for provider,
  roster, fantasy, and DK identifiers. Names assist reconciliation, not automatic
  identity joins. Transfers must resolve to the correct team at the decision time.
- Plays: unique `(game_id, play_id)`; aggregate deduplicated role participants before
  joining. Multi-participant joins must never multiply plays or yards.
- Labels: full nflverse weekly player statistics, supplemented by separate
  published team aggregates. The latter is a second aggregation from the same
  provider, not independent provider confirmation.
- Do not substitute `ff_player_week_stats` as the historical full-field label source.
  Its current-player filter omits historical contributors.
- Audit 2020-25 downloads before extending the present 2023-26 capture. Record
  coverage by year/week/team and field; routes, snaps, pressure, and personnel are
  not assumed populated because participant rows exist.

Reconcile carries, targets, receptions, rushing yards, and receiving yards per
player and game. Preserve zero-touch eligible games. Retain official kneels,
negative yardage, fumble credit, final replay/penalty status, and lateral recipients.
Lateral receiving yards can occur without a reception or target. Thus neither
`receptions = 0 => receiving_yards = 0` nor naive catch-times-gain accounting is a
universal invariant.

Keep verified box workload separate from unresolved event detail. Quarantined
event games cannot supply invented gain samples. Preserve the existing canonical
recent-history coverage gate; source expansion must not silently weaken it.

### 4.2 Time semantics

Every evidence item records:

| Field | Meaning |
| --- | --- |
| `event_at` / `effective_at` | When the underlying fact applies |
| `source_published_at` | When the provider says it published the observation; nullable |
| `observed_at` | When our system actually received it |
| `decision_at` | Latest information permitted for this forecast |
| `generated_at` | When the model ran |
| `source_ref`, `payload_digest` | Reproducible evidence reference |
| `capture_mode` | Prospective, imported archive, or retrospective reconstruction |

For strict prospective forecasts, `observed_at <= decision_at < kickoff`, and any
known publication time must also be eligible. A previously unavailable observation
cannot be backdated using its effective time. Imported historical archives retain
both archive market time and later import time; their eligibility is explicitly
an archive-based historical study, not our system's original prospective record.

Historical corrected statistics may serve as labels after the game. Historical
features corrected later must disclose reconstruction and cannot establish strict
pregame data availability. New corrections append superseding versions; never
overwrite original forecasts, evidence, or grades.

### 4.3 Availability and participation

For actual player-role and availability decisions, capture both Sleeper and
FantasyPros depth evidence for the same team/time, week-matched FantasyPros injury
observations, and official inactives when available. Reuse:

```powershell
python -m research.nfl_dual_depth_audit --season YEAR --team ABBR --output NEW_PATH
```

Missing/stale/conflicting providers remain visible. Depth rank is not projected
snap share or target share. Separate confirmed out, eligible active, uncertain
availability, expected starting role, and unknown role. Final inactives are not
available for earlier cutoffs; evaluate separate cutoffs rather than using them
retroactively. Do not claim retrospective new captures were pre-lock evidence.

### 4.4 Proposed persistence contracts

Adapt existing evidence tables first. Add migrations only after inspecting the
actual integration branch. Suggested logical records, names subject to that audit:

1. `nfl_model_runs`: immutable run manifest, version, implementation digest, source
   manifest digest, game, decision time, fit cutoff, seed, draw count, model branch,
   validation status, coverage flags, and draw-bank URI/digest.
2. `nfl_model_fit_artifacts`: training population, fitting settings, role/effect
   parameters, sample counts, fallback reasons, cutoff, and artifact digest.
3. `nfl_prop_quote_observations`: canonical event/player plus raw provider identity,
   book, market, side, line, comparator, price format/value, publication/capture
   times, raw evidence reference, mapping version, and eligibility flags.
4. `nfl_model_evaluations`: forecast run ID, versioned final labels, rules version,
   scores, exclusions, and evaluation/study digest.
5. `nfl_exit_observations`: player/game, reason class, time interval, return status,
   exposure, evidence references, label confidence, and adjudication version.

Raw evidence remains append-only. Normalized quote identity includes market,
player, book, side, line, comparator, and capture identity: never reduce a ladder
to one `over` price and one `line` field. Existing ordinary-prop convenience fields
must not become authoritative for multi-rung normalization.

Store compressed scenario banks outside normal page JSON; serve summaries and
audit references to the web layer. Exclude keys and credentials from manifests.
Start with local immutable artifacts; production storage decisions follow a
measured size/performance review.

## 5. Proposed model architecture

### 5.1 Branches and component boundaries

Use a single scenario schema with these explicit branches:

- `independent`: statistical player model, no player market inputs.
- `game-market-conditioned`: spread/total or other eligible game markets condition
  volume/state expectations; player prop prices do not enter.
- `player-market-reweighted`: additionally adjusts scenario weights using eligible
  player quotes; retain its unreweighted parent bank.

An independent forecast can be compared with a price. A forecast forced to match
that same price cannot claim the match proves an edge. Claims about unquoted tails
or joint events require their own evaluation.

Candidate modules (proposed, not existing):

- `model/nfl_joint_outcomes.py`: orchestration and shared scenario contract.
- `model/nfl_opportunity_process.py`: game/team volume and state process.
- `model/nfl_receiving_roles.py`: role/type allocations and hierarchical priors.
- `model/nfl_gain_distribution.py`: fitted gain candidates and field constraints.
- `model/nfl_midgame_exits.py`: exit hazard, duration, return, redistribution.
- `model/nfl_market_reweighting.py`: quote constraints, solver, diagnostics.
- `model/nfl_joint_decisions.py`: exact sets, ties, settlement, contest outcomes.
- `research/nfl_joint_outcomes.py`: capture/replay/fit/evaluate/export orchestration.

Use adapters to existing engines first. Extract common pure helpers only when the
baseline replay and registered-code protections remain intact. Avoid a third DFS
scorer or a second authoritative identity map.

### 5.2 Team volume and game state

First candidate: preserve existing paired historical team-volume sampling and
compare it against a fitted count model using prior pace, competitive play calling,
opponent effects, rest/home context, and eligible spread/total when enabled.

Model both teams jointly. Shared pace can lift both offenses, but extra passes can
replace rushes; a shared shock must not mechanically increase every statistic.
Separate dropbacks from official pass attempts: sacks and some scrambles consume
plays without becoming targeted attempts. Separate unattributed attempts from
attributed targets.

A later sequential candidate represents possession, score, clock, down/distance,
field position, and overtime. Learn conditional play calling rather than equating
underdog with deep passing. Pregame odds constrain expected outcomes, not a fixed
sequence of leads/trails. A drive-based approximation is acceptable for a tested
first candidate, but cannot be advertised as a full play/clock simulation.

Preserve a simple block-based model as the baseline. Any count or sequential model
must report volume predictive checks and incremental joint-score improvement.

### 5.3 Player shares and role changes

For team action volume `N`, begin with:

```
s ~ Dirichlet(kappa * m)
counts ~ Multinomial(N, s)
sum(m) = sum(s) = 1
```

`m` is a fitted mean-share vector and `kappa` controls variability. Player share
competition is conditional on a common volume; absolute player production can
still move together when team volume changes. Estimate variability from prior
games with finite-count noise accounted for; retain `nfl_role_dispersion.py` as a
registered candidate rather than assuming its settings are universally better.

Allocate over the complete eligible field. Preserve distinct unknown individuals
before resolving ranks. An `OTHER` accounting bucket must never compete as one
fictional player's combined production. If individually modeled surprise players
cannot be identified well enough, mark full-field exact-set coverage unresolved.

Extend shares by receiving role and opportunity type, not just WR1/WR2 names.
Possible measured roles include short/intermediate/deep target use and RB/TE/WR
participation. Do not label a player a slot receiver from an unavailable alignment
field. Pool sparse players with comparable measured roles and record shrinkage.

Test a structured logistic-normal or role-regime mixture only if residual
dependence shows the basic Dirichlet inadequate. This can represent two receivers
benefiting together from a personnel/role change while another group loses work.
Register alternatives in advance rather than adding distributions after misses.

### 5.4 Catch probability and receiving gains

For a target with player, QB, defense, type/depth, and context:

```
P(catch) = logistic(player/role + depth + QB + opponent + eligible context)
```

Fit coefficients with hierarchical regularization. Use a shared game efficiency
term only if supported by fitted residuals. Never derive catch rate from all pass
attempts when the denominator should be attributed targets. Incomplete targets
still inform opportunity and air-depth distributions.

Conditional on a completion, model official receiving credit using target depth
and YAC when measured. Candidate comparisons:

1. Existing empirical player/position gain sampling.
2. Role/depth-conditioned empirical sampling with partial pooling.
3. A registered ordinary/explosive mixture with physically constrained gains.

Retain losses and short gains, field-position limits, and lateral exceptions.
Unknown air depth or YAC remains missing; do not create fake decompositions.
A lognormal is already heavy-tailed, so distribution naming is not a selection
criterion. Fit and grade exceedance probabilities and full distributions.

Without a sequential field-position process, disclose a gain candidate's physical
approximation. Do not independently sample a play beyond the goal line and call
the clipped value a validated touchdown-distance model.

### 5.5 Rushing gains and total yards

Model RB designed runs, QB designed runs, scrambles, and kneels as distinct event
families when labels support them. Designed-run volume does not automatically
predict QB rushing. Pool sparse players by measured role. Apply opponent effects
after offense quality adjustment and shrink sparse defenses toward a prior.

Keep offensive line, QB availability, weather, coaching changes, and defensive
personnel as candidate features with measured coverage. No manual multiplier is
a fitted effect. The first role/gain candidate can omit unavailable features but
must report that omission and evaluate relevant failure cohorts.

Compute total yards from the two official components within each scenario.

### 5.6 Mid-game exits and redistribution

This is a new fitted mechanism. Separate pregame absence, injury exit, return after
temporary absence, performance benching, and blowout/rest substitutions.

Build labels from participation segments, injury/event evidence, and verified
return observations. Zero later targets alone is not an injury label. Treat vague
exit times as interval-censored and games with no documented exit as censored
exposure, not proof of perfect injury tracking. Publish label sensitivity and
missingness; do not assume reliable 2020-25 injury timing exists.

A proposed discrete-time hazard is:

```
P(exit in segment k | still participating) = logistic(role + exposure + context)
```

Use clock-, drive-, or opportunity-based exposure consistently. Estimate sparse
effects with pooling. Separate elapsed-game opportunities from player exposure.
Multiple exits and possible returns need explicit state transitions.

After an exit, redistribute ONLY remaining opportunities. A replacement transition
matrix maps role/action type to eligible recipients and an explicit unresolved
allocation; rows sum to one. Infer weights from historical transitions controlling
for score state and remaining time. Distinguish absence of a valid recipient from
a known replacement receiving zero projected opportunities.

Prevent double-counting: compare a no-explicit-exit historical variability baseline
with a conditional non-exit role fit plus explicit exit mixture. Do not layer an
exit distribution onto unchanged all-game dispersion without a registered check.
Do not use `nfl_replacement_upside_fit.py` as fitted transition weights.

If exit labels are inadequate, implement clearly labeled sensitivity scenarios
without claiming fitted hazards. Keep this branch disabled by default until its
data and evaluation gates pass.

### 5.7 Touchdowns and full DFS

Continue to treat longest TD as a distinct outcome with its own field-position
eligible opportunities, scorer attribution, periods, and no-score rules. Existing
longest-TD research excludes overtime while current yardage leaders include it;
do not combine them under an undocumented universal game rule.

For full DFS, use a coherent event ledger that includes passing, rushing and
receiving yards/TDs, interceptions, fumbles, two-point events, kicking, and DST
events under canonical scoring. Match passing TDs to receiving TDs where applicable,
turnovers to opposing recoveries/interceptions, and team scoring to its event ledger.
DK QB passing totals follow provider scoring credits, not blindly net offensive
yards including sacks/laterals. Preserve strict draw-level reconciliation rules.

A yardage-only scenario bank remains partial production, neither complete fantasy
points nor a guaranteed lower bound. Missing turnovers can create negative points.
No optimizer replacement is enabled simply because a complete bank parses.

## 6. Start NFL market capture alongside model development

### 6.1 Coverage and cost pilot

Inspect current ordinary-prop ingestion and its schemas on the integration branch.
Verify current provider documentation and actual book posting for NFL alternate
receiving yards, receptions, and rushing yards. Do not invent market keys or
assume parity with MLB. Count matched over/under pairs and one-sided rungs by book,
player, event, line, and capture time. Record unsupported or empty responses.

Produce a dry-run request/budget plan before enabling scheduled paid requests.
Estimate current-call cost as:

```
events * captures_per_event * requested_markets * ceil(book_count / 10)
```

Treat historical pricing's documented multiplier separately and verify it before
purchase. Log actual quota headers for every response. The September documentation
measured a 100,000-credit tier; verify live remaining quota, existing consumers,
reset policy, and safety reserve rather than treating that historical tier as a
current entitlement. Prefer at most ten named productive books per event request.

Suggested initial prospective snapshots: T-24h, T-90m, T-15m, with an idempotent
manual snapshot when an analysis is requested. This is a proposed pilot cadence,
not a change to existing production cadence. Forecast cutoffs use the most recent
eligible observation actually captured. Expired schedules do not create backdated
captures. Increase cadence only after measuring usable coverage and budget.

Quota exhaustion, authentication failure, unavailable markets, and unmapped events
are distinct states. Retries must not flood shared quota or create duplicate
charges for the same successful snapshot. Preserve raw payloads before convenience
normalization and display capture failures even when discovery endpoints work.

### 6.2 Price and settlement normalization

- Convert American/decimal prices explicitly; preserve the original quote.
- Match over/under within the same book, line, market, player, and capture.
- Record strict `>` versus `>=` semantics. A quoted 31+ event is not automatically
  the same as Over 31.0. Integer lines can push; half-lines cannot.
- Matched over/under odds at integer lines may imply probabilities conditional on
  no push, not unconditional survival probabilities. Model push mass explicitly
  before using them as constraints.
- One-sided quotes remain useful observations but cannot uniquely identify their
  own vig. Report a margin sensitivity interval, not an invented fair probability.
- Compare normalization and power methods as registered candidates. Two-outcome
  Shin equals additive; it is not an extra independent benchmark.
- Different ladder thresholds are nested events, not mutually exclusive winners.
  Do not normalize across thresholds or across anytime-TD scorers as if they were
  a one-winner market; several players can score a TD in the same game.
- Retain stale/update-time flags and monotonicity conflicts. Do not blend books
  and timestamps into a supposedly observed coherent ladder.

The 31+ price at -115 is roughly 53.5% raw implied probability and approximately
51.2% under division by 1.045. That supports a near-31 median only under assumptions;
it does not identify a 90th percentile or prove a 31-33 median interval.

## 7. Market conditioning through relative-entropy reweighting

Treat reweighting as a separate candidate after the unreweighted model is fitted
and its support inspected. Preserve parent scenario values; change weights rather
than silently refitting every player's distribution.

For original normalized weights `q_s`, solve:

```
minimize    sum_s w_s * log(w_s / q_s)
subject to  w_s >= 0, sum_s w_s = 1
            sum_s w_s * indicator(Y_i,s satisfies rung j) ~= p_i,j
```

Use probabilistic interval/soft constraints reflecting quote and de-vig uncertainty
as the first candidate. A hard-equality variant may be tested only when constraints
are coherent and feasible. Check comparator/push semantics before constructing
indicators. Specify the penalty form and its fixed tuning grid in registration.

Finite rungs do not identify the upper tail or the joint dependence. Reweighting
can change dependence even if constraints are marginal. It cannot create a long
gain or surprise contributor absent from the original support. Preserve and
display which tail conclusions come from the learned prior rather than quotes.

Required solver diagnostics:

- Feasibility and convergence status; incompatible constraint IDs.
- Per-rung target, achieved probability, tolerance, and residual.
- Weight entropy/KL divergence and maximum normalized weight.
- Effective sample size `ESS = 1 / sum_s(w_s^2)` and ESS fraction.
- Parent-versus-reweighted quantiles, pairwise/joint events, and exact sets.
- Quote-pair coverage, timestamp/mapping uncertainty, and held-out rung performance.

Proposed numerical guardrails, to freeze before evaluation: warn below ESS/N 0.25;
reject the combined forecast below 0.10, on infeasibility, or on material residual
violations. These are design choices, not statistically established constants.
Rejection falls back to the explicitly unreweighted model with a reason; it never
quietly presents failed weights as a market-calibrated forecast.

Test sensitivity to margin treatment, books, ladders, solver tolerance, and parent
simulation support. Do not grade fit quality solely on the rungs imposed as
constraints; reserve available rungs for predictive checking. Most importantly,
grade the joint decision against real games.

## 8. Joint decisions, ties, uncertainty, and expected value

### 8.1 Exact top-three sets

Given one metric and complete field, sort individuals in each scenario. For a
specified unordered trio A:

```
P(A is exact top three) = sum_s w_s * credit(A, scenario_s, rules)
```

For unique ranks, credit is 1 when the three identities match A, otherwise 0.
For a research random-tiebreak convention, if two players are strictly above the
boundary and three tie for the final place, each admissible trio gets 1/3 credit.
Generalize with the appropriate combinatorial allocation, ensuring credits sum
to one across all admissible sets. Real contest grading uses its actual rules;
an undefined boundary tie is unresolved settlement, not an invented winner.

Within-top-three ties do not matter when the same three players are securely above
the fourth. Do not confuse ordered finishes with unordered sets, first-or-tied
probabilities with dead-heat credit, or a DK salary pool with a whole-game field.
No separate Plackett-Luce layer is required to grade probabilities from scenarios.

For exact-set distributions, report normalization, residual/unresolved-field mass,
top displayed sets, and an actual-set log score. Do not renormalize a truncated
top-ten display and call it the complete distribution. Evaluate single leaders
under the already documented split-tie contract separately.

### 8.2 Monte Carlo and model uncertainty

Reported probabilities have at least three uncertainty sources: finite simulation,
fitted parameters, and uncertain pregame roles/evidence. Distinguish them.
For independent equal-weight simulation, an indicative sampling error is
`sqrt(p*(1-p)/N)`; weighted or correlated sampling requires appropriate diagnostics,
replicate runs, and potentially a bootstrap over simulation batches.

Use fixed seeds/common random numbers for paired model comparisons. Predefine
draw-count convergence checks for log scores and rare exact sets. A zero sampled
probability is not proof the event is impossible. Do not arbitrarily add a floor
and then advertise the floor as a calibrated probability. A scoring-only numerical
floor must be disclosed and tested for sensitivity. If finite support makes log
scores unstable, preregister a coherent full-distribution smoothing method or use
an additional stable proper score; do not repair only the realized outcome.

Archive uncertain role scenarios with their weights and evidence. Manually chosen
weights remain assumptions. Simulation size is not empirical validation.

### 8.3 Decision and portfolio layer

For simple single-winner, no-push fixed odds, unit-stake expected net return is
`p * decimal_odds - 1`. Dead heats, ties, pushes, voids, and shared payouts require
explicit scenario settlement rather than that shortcut. Compare executable odds
with applicable payout-adjusted probabilities, not an incomplete-field de-vig.

For a top-three contest, value depends on selection counts/ownership, payout sharing,
entry cost, and contest rules. Exact-set probability alone is insufficient. Model
ownership separately and preserve uncertainty. Without those inputs, rank likelihood
and disclose missing economic value rather than inventing expected profit.

If several selections or bets are considered, simulate their combined bankroll
returns using the same scenarios and settlement rules. They are often mutually
exclusive or correlated; separate Kelly fractions cannot simply be added. Add
portfolio sizing only after the probabilities and payout mechanics qualify, with
explicit budget limits. No automatic wagering is in scope.

For conventional DFS, use legal salary-constrained full fantasy-point lineups,
captain multipliers where relevant, a modeled field, ownership/duplication, and
payout simulation. Cross-game lineup banks require an explicit cross-game joining
policy; matching scenario index numbers alone does not establish meaningful
cross-game correlation.

## 9. Historical validation and promotion protocol

### 9.1 Baselines and registered candidates

Preserve these distinct comparators:

- Current fixed-dispersion joint leader model.
- Current empirical-dispersion candidate.
- Recent-average point ranking for accuracy context; it is not a probabilistic
  baseline for log score unless a distribution is explicitly fitted.
- Existing empirical gain distribution versus role/depth-conditioned candidate.
- Original lognormal ladder method where actual eligible ladders are recoverable.
- Market-only marginal fit with registered dependence assumptions, if implemented.
- Independent, game-market-conditioned, and player-market-reweighted variants.

Historical ladder comparisons require historical quotes, not current quotes or
postgame thresholds. A history-only physical model can validate its own tails;
it cannot establish market tail mispricing without market evidence.

Register component ablations and one combined candidate. Use development/tuning
data to select the final candidate once, then open the locked evaluation once.
Where multiple primary claims are made, specify a correction such as Holm or a
hierarchical gate. New experiments after an opened test need new registration and
fresh evaluation. Document selection of the candidate even when results disappoint.

### 9.2 Metrics

| Layer | Required measurements |
| --- | --- |
| Source | Canonical coverage, reconciliation, exclusions, missingness by cohort/time |
| Volume/share | Counts, zeros, variance, share changes, replacement transition residuals |
| Individual distribution | CRPS; discrete count log score where supported; interval scores; p50/p80/p90/p95 exceedance and coverage |
| Joint distribution | Single-leader Brier/log loss; exact-set log score and categorical Brier; preregistered pairwise/co-exceedance checks |
| Decision | Top-choice/top-set hit rates with ties, coverage, payout outcomes where supported |
| Complete DFS | Fantasy-point ranges, joint lineup scores, tail outcomes, ownership/duplication and payout assumptions |

Use CRPS for yardage distributions without pretending a Monte Carlo histogram is
a well-estimated continuous density. Discrete reception mass can be scored directly
subject to zero-mass policy. Fractional tie targets require the registered proper
score definition, consistently for all baselines.

Inspect role cohorts: primary/secondary receivers, RB receiving roles, sparse
newcomers, QB rushing, pregame absences, documented exits, and opponent cohorts.
Define cohorts from pregame features, not the eventual winner. Pooling all players
can hide a badly wrong reserve-receiver tail. Report smaller samples rather than
removing inconvenient cohorts after grading.

Resample paired whole games for score-difference intervals; player rows within a
game are dependent. Consider week/block sensitivity for shared temporal effects.
Report calibration sample sizes and uncertainty, not just a pooled percentage.
Hundreds of games are a target, not a universal proof of adequate power for a rare
1% exact set. Perform power/precision planning with development data before fixing
the locked sample size.

### 9.3 Proposed acceptance gates to freeze in Phase 0

**Mechanical and provenance gates (mandatory):**

- Zero identity duplication, incompatible canonical joins, or later-than-decision
  evidence in a strict forward run.
- Exact opportunity conservation and valid catches/targets for attributed passes;
  separately tracked unassigned attempts, sacks, scrambles, and laterals.
- No unavailable/out player receiving post-exit opportunities; redistribution
  conserves remaining shares among eligible recipients.
- Exact-set credit normalization under the chosen tie contract.
- Shared scenario IDs/order preserved across leader and DFS consumers.
- Immutable replay produces matching digests/scores within documented numeric
  tolerance; fail explicitly on missing artifacts or schema mismatch.
- Existing reconciliation/publication gates and registered baselines stay intact.

**Prediction promotion gates (proposed design thresholds):**

- The locked candidate's primary exact-set log loss improves over the strongest
  registered applicable probabilistic baseline, with the paired-game 95% interval
  for candidate-minus-baseline loss entirely below zero after specified multiplicity
  handling. Apply a separately registered gate for each claimed leader family.
- CRPS non-inferiority: no more than 2% relative degradation on the registered
  aggregate, with uncertainty, plus no material registered role-cohort failure.
- For p90 calibration, target 10% exceedance; proposed absolute-error tolerance
  3 percentage points in adequately powered registered cohorts. Wide intervals
  mean inconclusive, not automatically passing. Calibrate randomized discrete
  quantiles or coverage intervals for receptions; ordinary percentile exceedance
  need not equal 10% for a discrete count distribution.
- Market-reweighted forecasts pass all solver/ESS gates and show decision-score
  improvement beyond merely reproducing constrained quotes.
- A forward-shadow period passes prespecified source, stability, and decision-time
  checks before replacing any owner-facing default or optimizer projection.

These thresholds are engineering/research choices, not guarantees of profit.
Finalize their feasibility and minimum detectable improvement using development
data only, then freeze them. Do not weaken a failed gate after examining results.
Report "failed" or "inconclusive" separately. Statistical improvement does not by
itself establish expected monetary return after contest costs and payouts.

## 10. UI and operating workflow

Keep `/nfl/pbp` as evidence drilldown and `/nfl/team-identity` as team-season
context. Use the existing `/nfl/game-model` for per-game scenario summaries, with
links to `/nfl/game-leaders`, `/nfl/projections`, and the underlying PBP evidence.
No DK upload is needed for full-field production ranges or game-leader analysis.
DFS lineup analysis still requires site salaries, eligibility, and contest inputs.

The game-model page should show:

1. Canonical matchup, kickoff, frozen decision time, generated time, model version,
   branch, evidence coverage, and research/qualification status.
2. Player mean/median, p10/p90 ranges, catches/targets/carries, and leader credit.
3. Exact top-three sets for the chosen metric with tie/field contract and unresolved
   mass visible; percentages do not imply validated precision.
4. Explanation of volume, role, opponent, efficiency, and availability evidence.
5. Independent versus market-conditioned ranges and probabilities in separate
   columns; source book/line/time and model-versus-quote discrepancy when available.
6. Historical review counts, baseline comparisons, exclusions, calibration, and
   latest forward grading. "Not evaluated" is different from a score of zero.
7. Postgame original forecast versus verified actuals, retaining every forecast
   version and displaying late stat corrections separately.
8. Complete DFS forecasts only when all required stats and scoring contracts are
   satisfied; partial production retains its existing explicit label.

Use plain explanations such as "Both receivers can benefit from more team passes,
but they compete for those targets." Do not show internal model terminology as a
substitute for why a range changed. Missing data must appear unavailable rather
than zero, a invented multiplier, or an unexplained exclusion.

Do not refresh inputs invisibly after lock. A new capture produces a new immutable
run. Post-kickoff live models, if later requested, need a separate contract.

## 11. Test matrix and implementation stages

### Mandatory meaningful checks

- Regression fixtures: fumbles, laterals, nullified/reviewed plays, duplicate
  participants, transfer identities, kneels, overtime, and incomplete recent history.
- Opportunity invariants: target/carry allocation, catch bounds, unattributed
  attempts, gain/stat-credit reconciliation, and sparse-role fallback.
- Exit fixtures: early/late/temporary exit, return, two teammates exiting, no valid
  replacement, and benching distinguishable from injury. No reassignment of past
  touches and no invalid post-exit participation.
- Market fixtures: paired/missing sides, multiple rungs, integer pushes, 31+
  semantics, suspended/stale quotes, inconsistent monotonicity, unknown identities,
  quota exhaustion, idempotent capture, and no pregame use of in-play prices.
- Solver fixtures: attainable/infeasible constraints, weight normalization,
  support limitation, low ESS, soft-constraint residuals, and fallback behavior.
- Ranking fixtures: unique sets, internal ties, boundary ties with multiple
  admissible sets, negative production, zero-stat crowded fields, unresolved
  contributors, and no pooled-OTHER fictional leader.
- DFS fixtures: canonical score equality, bonuses, captain treatment, legal lineup
  constraints, turnover/TD reconciliation, and strict rejection of partial banks
  when full scoring is requested.
- Temporal fixtures: late source capture, retrospective imports, unavailable
  historical injury timestamps, forecast cutoffs preceding final inactives, and
  immutable grade revisions.
- Study fixtures: chronological fitting, no test leakage, baseline/source-scope
  parity, game-cluster scoring, reproduction of prior registered digests.

Synthetic tests prove mechanics only. They are not historical prediction evidence.
Run focused tests while developing, then the required Python/TypeScript/typecheck
and integration gates once per completed batch. Do not expand optional tests after
sufficient verification without a concrete remaining risk.

### Stage A — audit and registration

Deliver: recovered Thursday bundle or explicit missing-artifact report, comparison
scorer, exposure registry, and frozen study specification. Acceptance: all reported
comparisons reproducible; prospective versus retrospective scope is accurate.

### Stage B — capture pilot and historical source audit (parallel workstream)

Deliver: ordinary/alternate coverage report, dry-run cost plan, raw+normalized
append-only capture, quota telemetry, and 2020-25 source coverage report.
Acceptance: real supported market/field coverage, no multi-rung overwrite, matched
quote semantics, canonical identities, and recorded unknowns. Enable actual paid
capture only under an established budget; the existing capture pilot is not a
license for bulk historical purchases.

### Stage C — opportunity and gain candidates

Deliver: versioned adapters, role/type features, pooled fits, draw-bank exports,
and registered development comparisons. Acceptance: source-to-output mechanical
gates, no source regressions, and defined baselines preserved. Do not promote from
development-only improvements.

### Stage D — mid-game exits

Deliver: adjudicated label subset, coverage report, pooled hazard/transition fit,
double-counting ablation, and documented sensitivity fallback if labels inadequate.
Acceptance: timing and conservation tests plus registered incremental evaluation.

### Stage E — market reweighting

Deliver: coherent quote constraints, soft-KL solver, support/ESS diagnostics,
unreweighted/weighted paired outputs, and held-out rung/joint-score reports.
Acceptance: normalization, eligibility, solver guardrails, and no precision or
edge claims derived merely from reproducing the calibration inputs.

### Stage F — locked evaluation and forward shadow

Deliver: frozen final candidate, one locked evaluation, all failed/inconclusive
outcomes, and genuinely prospective forecasts and settlement records. Acceptance:
registered gates; a failed candidate remains a research option with the baseline
as default. Record any next experiment as a new study.

### Stage G — DFS and decision integration

Deliver: complete aligned event-bank consumer, site scoring, legal lineup/payout
handling, separate ownership model and uncertainty, game-model UI, operational
fallbacks, and owner review. Acceptance: complete-stat contract, payout-specific
evaluation, and no silent overwrite of existing optimizer projections.

Stages B and C can proceed alongside each other after registration; exit and
market studies depend on their source coverage. Full DFS integration depends on
complete coherent events, not merely the success of a yardage model. Successful
one-game replays never bypass Stage F.

## 12. Developer handoff and definition of done

Use `artifacts/nfl-joint-outcomes/<study_id>/<run_id>/` for new bundles with:

```
manifest.json
study-registration.json
source-manifest.json
coverage-report.json
fit.json
request.json
draws.json.gz
forecast.json
market-constraints.json
solver-diagnostics.json
evaluation.json
review.md
```

Not all files apply to every branch; the manifest explicitly identifies omitted
artifacts and why. Use exclusive creation, stable IDs, schema versions, and digests.
Reusing an existing artifact directory is acceptable when consistent with its
contracts. No large scenario banks need to be committed to Git; register their
digests and durable recovery location before considering local evidence preserved.

Proposed CLI responsibilities: `audit-thursday`, `audit-sources`, `fit`, `forecast`,
`reweight`, `grade`, and `evaluate`. These names are proposed interfaces; implement
`--help` and documented reproducible examples before presenting them as runnable.
Defaults must not make paid calls, write production tables, backdate evidence, or
replace a published forecast. Use explicit modes for any such authorized operation.

The developer's completion report must include:

- Local commit SHAs, changed files, integration dependencies, and conflicts.
- Which stages and acceptance criteria passed, failed, or remain blocked.
- Exact source, fit, request, study, and implementation digests.
- Reproduction commands, tests performed, and archived outputs.
- Historical source coverage and genuinely untouched/forward evidence scope.
- Model-versus-baseline scores with uncertainty, exclusions, and role cohorts.
- Quota usage, ongoing capture cost, freshness/failure monitoring, and recovery.
- Whether each displayed result is partial, complete, exploratory, or qualified.
- Release flags, fallback/rollback behavior, and unchanged protected baselines.

The work is complete only when another developer can reproduce the forecast and
its grade, trace every material assumption, and identify what the evidence does
and does not support. A page rendering or a simulation running is insufficient.

## 13. Technical references

- Attilio Meucci, *Fully Flexible Views: Theory and Practice*:
  https://arxiv.org/abs/1012.2848 — relative-entropy scenario reweighting.
- Official `implied` package vignette:
  https://stat.ethz.ch/CRAN/web/packages/implied/vignettes/introduction.html — margin
  removal alternatives and two-outcome Shin/additive equivalence.
- Official PlackettLuce package:
  https://hturner.github.io/PlackettLuce/ — an optional ranking-model comparator;
  not required to grade exact sets from our scenario distribution.

Provider capabilities/pricing must be rechecked during the coverage pilot. These
references justify methods, not claims that the proposed NFL model is calibrated.
