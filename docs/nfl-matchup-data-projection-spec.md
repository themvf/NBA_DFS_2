# NFL matchup data: pick'em and DFS projection improvement specification

Date: 2026-09-27
Status: proposed implementation contract; no model promotion or deployment is authorized by this document.
Applies to: NFL pick'em, NFL DFS projections, and contest-specific recommendations.

Implementation evidence: [September 27 implementation and slate verification](./nfl-matchup-implementation-2026-09-27.md).
The contract below retains its original qualification requirements; implementation
does not itself constitute a model promotion.

## 1. Objective and intended result

Use the PFR opponent and player charting we have collected, together with
play-by-play, availability, market prices, usage, and contest information, to
make more accurate football forecasts and better contest decisions.

**Opponent data can help DFS.** A defense can change how much a team runs,
how efficiently a player gains yards, how often a quarterback loses a
dropback to a sack, and which combinations of players succeed together.
Those are concrete mechanisms for improving projections. The task is to
estimate their incremental effects on top of the information already in
the model, then use the resulting distributions to select entries.

For pick'em, the same evidence can identify a matchup that the current
market probability does not fully describe. It can also explain why an
apparent upset opportunity is weak. For DFS, a better projection can move
a player's mean up or down, widen or narrow the likely scoring range, or
change the value of a stack without materially changing individual means.
"Improve projections" does not mean increasing every projected score.

Deliver four user-facing products:

| Product | Required output |
|---|---|
| Weekly straight pick'em | Football win probabilities; highest expected-correct card; weekly-prize recommendation when pool inputs support it |
| Season-long straight pick'em | Standings-aware recommendation, expected score cost of differentiation, and consequences for both weekly and season prizes |
| DFS single-entry / three-entry tournaments | Legal complete lineups compared against the relevant contest; evaluate three entries together when applicable |
| DFS large-field multi-entry tournaments | A legal portfolio assessed for coverage of strong outcomes, field competition, duplication, entry cost, and payouts |

Single-entry remains a tournament product. It is not a floor-maximizing
cash-game preset. Straight pick'em awards one point per correct selection;
confidence weights are outside this user's format.

## 2. Relationship to existing work

This is the implementation specification for the data-to-forecast connection
outlined in [the decision plan](./nfl-pickem-dfs-decision-plan.md). It supplies
features, manifests, forecast outputs, evaluation, and page behavior. It
does not restart existing studies or replace the following contracts:

- [Opponent-term spec, merged through PR #287](https://github.com/themvf/NBA_DFS_2/blob/main/docs/nfl-dfs-opponent-term-spec.md):
  authoritative for the frozen `opp_carries` study, its grader, v6 promotion,
  shadow re-pin, and rollback. This file was absent from the local checkout
  when this spec was written; reconcile against merged main before coding.
- [NFL context engine](./nfl-context-engine-implementation-spec.md) and
  [consumer registry](./nfl-context-consumers.json): authoritative for evidence,
  revisions, qualifications, consumer dependencies, and active policies.
- [Availability model](./nfl-availability-model-improvement-spec.md):
  authoritative for shared availability resolution and opportunity transfers.
- [GPP portfolio spec, revision 2](./nfl-gpp-portfolio-improvement-spec.md):
  authoritative for existing scenario selection, real scenario-bank delivery
  (D3), contest outcomes (D4), defaults-on checks, and portfolio promotion.
- [PFR supplement](./nfl-pfr-supplement.md) and
  [matchup reports](./nfl-matchup-reports.md): current ingestion and descriptive
  reporting behavior.

The engineer must record the implementation commit, deployment version, and
study registrations actually in use. Local files, merged main, and the live
page are not interchangeable evidence that a capability has shipped.

### 2.1 Current state and the missing connection

As of this task's September 27 audit:

- PFR advanced passing, rushing, receiving, and defense sections were stored
  for 33 completed 2026 games, totaling 1,812 player/section rows. These are
  coverage counts, not independent training samples. The working source is
  the public nflverse distribution of PFR charting. Direct PFR scraping is
  blocked by a browser challenge; it is not the production dependency.
- The first retained PFR capture was September 27. Earlier game dates do
  not prove the data was available to this system before earlier locks.
- PFR starters and snap-count sections are absent from this import. Four
  advanced sections do not make the entire PFR supplement complete.
- Local pick'em code displays PFR evidence and freezes it with the game
  context. It does not change win probabilities and has not been deployed
  as part of this task. The reader drops `snapshot_id` after selecting it;
  its returned evidence also needs parser/schema and actual source identity.
- The current DFS production opponent-points-allowed switch is off. The
  separate allowed-carries variant exists in shadow; its missing grader is
  the first deliverable of the merged opponent-term spec.
- Existing DFS simulation and portfolio code should be connected and
  extended. The missing work is not a second optimizer built from scratch.

Collection, display, numerical prediction, and contest decision authority
are distinct states. Implement all four connections explicitly.

## 3. How the data improves an assessment

| Data already available or being captured | Football question and proposed use | Pick'em effect to test | DFS effect to test |
|---|---|---|---|
| Moneylines, spread, total, timestamped movement | Establish what the market currently expects | Baseline win probability; test evidence residuals against that identical baseline | Preserve the existing team environment; evaluate incremental matchup changes |
| PBP attempts, carries, targets, scoring-area usage, player participation | Estimate opportunity and role before judging efficiency | Team offensive capacity and availability-conditioned scenarios | Team opportunity budgets and player shares; workload distributions |
| PBP drives, EPA, field position, turnovers, explosive plays | Distinguish sustained offense from short fields and unusual scoring | Candidate residual signals for game outcomes | Candidate residual team scoring and game-script signals |
| PFR offensive pressure faced and sacks; opponent QB rows against a defense | Match protection/QB susceptibility with defensive pressure evidence | Whether the matchup changes win likelihood beyond current prices | Sack/downside distributions for QBs and stacks; a separately qualified DST effect |
| PFR rushing carries and yards before/after contact | Estimate matchup-specific rushing efficiency | Whether run efficiency adds residual game-level information | RB yards per carry and scoring ranges, separately from carry volume |
| PFR drops, bad throws, receiving and defensive charting | Separate opportunity, throw quality, and conversion hypotheses | Supporting evidence; only qualified residuals may move probabilities | Catch/yardage uncertainty; no automatic positive-regression bonus |
| Availability, depth, official inactives, platform eligibility | Determine who can play, how much, and who receives missing work | QB/personnel scenarios that remain incremental to market prices | Eligible player pool, role states, conserved redistribution, replacement-specific efficiency |
| Pool pick shares, prior entrant cards, standings | Estimate this pool's choices and the value of differentiation | Weekly and season prize decisions; does not change football probabilities | Not a DFS forecast input |
| Salaries, ownership, contest lineups, entry limits and payouts | Estimate which legal entries compete and what outcomes pay | Not a football forecast input | Lineup/portfolio selection and duplication; does not mechanically inflate player scores |
| Actual results and frozen predictions | Measure what improved | Forecast calibration and pool decision grading | Workload, points, distributions, dependence, and contest grading |

Ownership and complete contest-field data are coverage-dependent inputs,
not assumed to be fully available. Previously imported contest files must
be inventoried and matched to slate, entry rules, and capture time. Public
pick shares are a proxy for a private pool, not observations of that pool.
No new subscription is required by this specification.

## 4. Architecture and qualification

Use one evidence layer with separate forecast and decision consumers:

```text
PBP + PFR + availability + markets + usage
                 |
        immutable source observations
                 |
   versioned matchup features + coverage manifest
                 |
       consumer-specific qualification
          /                       \
 pick'em probabilities       DFS stat distributions
          |                       |
 pool/standings policy       coherent scenario banks
          |                       |
       pick card          contest/portfolio policy
          \                       /
      frozen Why evidence + paired outcome grading
```

Reuse `nfl_evidence_observations`, fact releases/revisions, context definitions
and snapshots, qualifications, active policies, and consumer manifests from
the context engine. Do not create a parallel "approved feature" boolean.
Approval is scoped to definition, consumer, use case, cohort, and usage.

The same pressure fact may be descriptive for pick'em, in shadow for QB
projections, and unavailable for DST production. A DFS predictive approval
does not license a pick'em probability change or an optimizer objective.
Product labels may read **Evidence**, **Under evaluation**, or **Used in
projection**, backed by those qualifications.

An unavailable or unqualified optional feature contributes no adjustment
and records a reason. Preserve the independently approved baseline. Reject
an unqualified requested dependency rather than silently using it; do not
make the whole page or ordinary lineup generation fail because an optional
PFR section is absent.

## 5. Data contracts and reproducibility

### 5.1 Required source and feature manifest

Extend existing manifests with these fields; use existing column names
where their semantics match. New names below describe the logical contract.

| Group | Required contents |
|---|---|
| Target | Canonical game/slate ID, season/week, team/opponent, player ID where relevant, kickoff, decision cutoff, per-game lock state |
| Source identity | Snapshot/observation IDs, provider, actual source URL, source-file and selected-payload hashes, parser version, schema version |
| Time | Event time, source publication time if known, first observed/captured time, ingested/recorded time, feature build time, original consumer freeze time where one exists, replay build time where applicable |
| Joins | Frozen player and team identity mappings, crosswalk/version or mapping hash, roster/team effective time, unresolved/conflicting join reasons |
| Feature definition | Definition/version, lookback, priors, units, inclusion rules, numerator/denominator definitions, missingness, coverage and qualifying cohort |
| Model and policy | Forecast version, config/hash, qualification IDs, baseline reference, availability scenarios, scoring version, contest-policy version |
| Reproduction | Input manifest hash, candidate bank identity, generation/selection/evaluation seeds, selected output, user overrides and reasons |

Enforce `available_at <= decision_cutoff <= original_freeze_time < target_kickoff`
for each actual pregame forecast, with its features built by that freeze.
`available_at` must reflect when the actual
consumer could access the input: later ingestion cannot be backdated to an
earlier event or claimed publication time. A historically replayed feature
may be computed later only from source revisions that were demonstrably
available at the original cutoff, and must be labeled as a replay.
Store `replay_built_at` separately. If no actual pregame forecast was frozen,
leave `original_freeze_time` absent; a historical reconstruction must never
manufacture one. An unknown publisher timestamp is acceptable when retained
capture and recording evidence proves consumer availability before cutoff.

PFR uses the original captured and recorded times; PBP needs the retained
source revision ledger. Today's repaired PBP and a label's `labelled_at`
are not sufficient proof of original availability. Revisions create new
snapshots. Never replace a frozen explanation with a latest-view query.

The PFR reader must retain selected snapshot IDs and source metadata all
the way into `FrozenEvidence`. Replace hardcoded source links with the
stored provider URL. Freeze identity resolution too: replaying an old
snapshot against today's mutable player crosswalk is not reproducible.

### 5.2 Units, denominators, and missingness

- Normalize canonical team aliases on both sides of every join. Resolve
  PFR-to-GSIS player identity explicitly; unmatched or conflicting players
  remain unresolved, not guessed by a similar name.
- Preserve the source schema. The nflverse PFR adapter converts fractional
  rates to percentage points; the HTML parser has a different input shape.
  Test that semantically equivalent inputs produce equivalent feature units.
- Retain nulls. Zero means a measured zero with adequate coverage, not a
  missing player, unpublished file, missing section, or failed join.
- Never invert a rounded `times_pressured_pct` to invent an exact exposure
  denominator. Do not weight that rate using a different PBP denominator
  as though the two sources count identical plays.
- Where the source supplies only game-level percentages, retain each game
  rate and its definition. An across-game average must be labeled unweighted
  unless a verified matching denominator exists. A registered model may
  use these rates with explicit reliability assumptions; it may not claim
  exposure-weighted precision it does not have.
- Do not add defender pressure credits into unique team pressure events.
  A team-defense summary can use complete opposing QB rows under a tested
  aggregation contract; missing backup-QB rows make that game incomplete.
- PFR game aggregates do not provide pressure-tagged EPA, clean-pocket EPA,
  four-man-rush pressure, exact coverage assignments, or CB-WR matchup splits.
- Aggregate RB contact yards with carries from the same PFR rows. Call the
  result charted RB rushing, not designed-run efficiency unless the source
  can separate that scope. PBP can separately exclude kneels and distinguish
  scrambles; do not subtract its play counts from unmatched PFR yard totals.
- Every aggregate reports eligible games, last observed date, applicable
  exposure, identity/role continuity, and missing-section reasons.

An older completed-game observation is not stale merely because days have
passed. Report both age and whether the latest eligible completed games
have been published and incorporated. Fast-changing odds and availability
have separate freshness rules.

## 6. First feature families and their destinations

Build a shared feature extractor with offense, opposing-defense, and matchup
interaction terms. Do not use a raw defensive rank as a universal multiplier.
Use regularization and training-only priors; freeze lookbacks, shrinkage,
interaction definitions, and roster-continuity rules before evaluation.

### 6.1 Pressure and sack matchup

Inputs: eligible PFR QB pressure/blitz/sack observations, complete opposing
QB observations against the target defense, PBP dropbacks/sacks, QB identity,
and available personnel/role states. Pressure faced reflects protection,
QB behavior, opponents, and scheme; it is not a pure offensive-line rating.

First research targets:

1. Next-game sacks conditional on the PBP dropback budget. Freeze the PBP
   dropback definition and handle sacks/scrambles/attempts consistently.
2. Passing-yardage and fantasy-point downside after existing opportunity
   and team-environment inputs.
3. Incremental game-win log loss against same-time market probabilities.

Use source pressure rates as covariates only under section 5's denominator
contract. Do not simulate an asserted unique pressure count from a rounded
rate with an unknown denominator. If later play-level tags arrive, they
require a new source/feature version and qualification.

For DFS, route an approved effect into a specific sack or passing-efficiency
component. Avoid a sack adjustment plus a separate generic QB penalty for
the same evidence. Any DST predictive change needs its own registered
test; coherent QB-sack/DST outcomes alone do not establish defensive skill.

### 6.2 Rushing contact and efficiency

Inputs: PFR RB carries, yards before contact, yards after contact, player
role, and complete corresponding opponent-allowed RB rows. Compute
same-source per-carry rates with exact denominators. Shrink small samples;
yards before contact reflect scheme, defense, blocking, and runner choices.

Test residual rushing-yard efficiency given workload and the current team
environment. Separate player running quality from opponent allowance using
training-fitted effects. Exclude unsupported QB-designed-run claims.

An approved contact effect changes rushing-yard efficiency and its
distribution. It does not automatically increase carries or touchdown
probability. Touchdowns require a distinct conditional scoring model and
qualification. This separation prevents double application alongside
`opp_carries`.

### 6.3 Availability and role

Reuse the shared availability resolver and existing improvement program.
Represent supported full-role, limited-role, and unavailable states; do not
infer a medical probability from an injury label alone. Each state must
identify its evidence or be an explicitly labeled sensitivity scenario.

Use platform eligibility and lock rules for legal lineups. Within each
state, conserve team carries, targets, pass attempts, and scoring opportunity
as defined by the existing model. Transfer work to supported replacements
with their own efficiency, rather than copying an absent starter's output.
Retain unallocated opportunity when the available evidence cannot assign it.

For pick'em, test the incremental effect against odds captured at the same
decision time. News already reflected in the line is not a second automatic
penalty. Shared availability facts do not require identical coefficients
in the pick'em and DFS consumers.

### 6.4 Receiving quality and sustainable offense

Drops, bad throws, short fields, turnovers, explosives, and drive production
are immediately useful explanation inputs. Register narrow candidate
effects, such as conversion uncertainty conditional on targets, rather
than automatically awarding lost yardage or removing all unfavorable plays.

Previously failed passing-volume, archetype-pace, red-zone, and redistribution
variants remain closed under their existing definitions. The neutral-snap-
interval study's approximately 0.028-play MAE gain failed its 0.25-play
materiality gate. New definitions or consumers need a new registration;
an appealing narrative is not permission to revive a failed adjustment.

### 6.5 Preserve the allowed-carries study

Implement the merged opponent spec's missing `context_variants` grader and
freeze health checks in parallel with the PFR work. Its frozen formula is:

```text
candidate_carries = own_carries + 0.5 * (opponent_allowed_carries - league_carries)
factor = clip(candidate_carries / own_carries, 0.5, 1.5)
```

The existing shadow applies that factor after the team environment to
`rushing_yards` and `rushing_tds` only, for QB/RB/WR/TE. Missing/invalid own
or opponent history gives a neutral factor with an explicit reason. It
does not yet implement a conserved team carry-allocation model.

Do not alter its weight, clamp, prior, fields, cohort, or gate. No verdict
before eight scorable forward weeks; RB paired MAE CI upper bound must be
below zero versus `env_baseline`, without the specified QB/WR/TE harm.
On PASS, follow the v6 promotion and same-PR shadow re-pin; after promotion,
run the registered four-week v6-versus-v5 rollback comparison. On FAIL,
close the variant. Points-allowed `opponent_mode` stays off.

Keep PFR challengers in separate named studies. A later conserved component
model is another experiment, not a rewrite of this frozen variant. Reserve
`nfl-dfs-historical-v6` for the existing promotion contract.

Determine scorable counts from valid, deduplicated player-week records and
the registered study pin, not a hardcoded 644-row assertion. Multiple
captures and study IDs exist. Verify each target week's pre-kickoff freezes;
the earlier failed-job history alone does not prove a future week was lost.

## 7. Pick'em forecast and card policy

### 7.1 Market-residual probability model

Keep the no-vig market forecast as the comparison baseline. Register a
regularized challenger of this form, or an equivalently auditable residual:

```text
logit(p_home_candidate_given_no_tie)
  = logit(p_home_market_given_no_tie) + fitted_matchup_residual(features)
```

Train and tune only on eligible earlier data. Residual zero must reproduce
the baseline exactly. Evaluate injuries, PBP, and PFR incrementally with
family ablations; reconstructing the spread is not evidence of winner edge.
Freeze the market quote/aggregation rule, quote time, forecast, and model
version together. Baseline and challenger must use the identical cutoff.

Retain the existing market/model fallback hierarchy when prices are missing.
Do not apply a market-residual model to a different baseline it was not
qualified against. Existing freshness limits are two hours within 24 hours
of kickoff, otherwise 24 hours; surface stale quotes and check the deployed
policy before release.

Output home/away/tie probabilities summing to one. The moneyline-derived
probability is conditional on no tie; use a separately specified tie model
or label tie sensitivity when unavailable. Apply the pool's game-tie and
void rules. Game ties and tied standings/prize splitting are separate rules.
For this matchup-residual study, baseline and challenger must share the same
frozen tie probability. Changing the tie forecast is a separately registered
family. A conditional two-way forecast without a specified tie component
cannot claim a qualified three-way contest forecast.

### 7.2 Preserve the pool's actual pick shares

Correct `web/src/lib/nfl/pickem-strategy.ts` before claiming observed-share
strategy results. Currently it applies an entered whole-field share to
non-chalk rivals and then adds a separate all-favorites block. For example,
60% home picks and 25% all-home-favorite rivals become 70% home picks.

Declare whether shares describe rivals only or all entries including ours.
Use the actual integer entrant count. If an observation includes our entry,
first remove our pick as observed in that snapshot:

```text
q_rivals = (entry_count * q_all - observed_own_home_pick) / (entry_count - 1)
```

Freeze that observation, population denominator, and our observed pick.
Do not subtract each hypothetical candidate pick during card comparisons:
the rivals must remain the same across candidate cards. If our observed pick
or inclusion is unknown, represent that as field uncertainty rather than
assuming a rivals-only denominator. With no rivals, skip this conversion.
For rivals, preserve the whole-field
marginal with:

```text
c = actual_chalk_rival_count / rival_count
q_nonchalk = (q_total - c * chalk_home_pick) / (1 - c)
```

Preserve observed 0% and 100%; do not clamp those facts to 1%/99%. Handle
zero rivals, all-chalk fields, both favorite directions, and infeasible
chalk assumptions. Reject the inconsistency or visibly reduce the assumed
chalk count; never silently distort the observation. Matching marginals
does not identify dependence across an entrant's complete card.

### 7.3 Weekly and season-long decisions

Persist a `PoolConfig` with scoring/tie rules, entrant population and count,
weekly/season payouts, standings, remaining weeks, lock rules, available
pick-share observations and timestamps, and whether the same card serves
both prizes. Unknown fields remain unknown.

Compare the maximum-expected-correct card with weekly and season prize
policies. Simulate score gaps and future weeks for season decisions; label
future probabilities that use ratings/scenarios rather than observed
markets. When one card serves both prizes and payouts are supplied, use
expected weekly prize plus expected remaining-season prize, with probability
of first reported separately. Do not invent a weighting when payouts are
unknown; show parameterized alternatives and their expected-score costs.

For a switch away from the selected favorite when a game tie scores zero,
the expected-correct cost is
`(1 - p_tie) * (2 * p_selected_given_no_tie - 1)`. Use the full scoring rule
for other tie treatments. An upset is useful only if its contest benefit
justifies that cost under the specified objective and uncertain field.

Use common random numbers when comparing candidate cards, then independent
draws to evaluate the selected card. Report Monte Carlo uncertainty and
field-model sensitivity; do not label tiny simulated differences decisive.
Missing pool inputs permit a football forecast and sensitivity analysis,
not a claim of a uniquely optimal contest card.

## 8. DFS projection and tournament connection

### 8.1 Component forecast and adjustment ledger

Create one ordered component ledger per player/game forecast:

1. Availability and role states.
2. Team opportunities: plays, dropbacks, carries, targets and scoring chances.
3. Player allocation of those opportunities.
4. Conditional efficiency: catches, yards, sacks, turnovers and touchdowns.
5. Exact platform scoring per simulated outcome, including applicable bonuses.

For each applied adjustment record baseline value, eligible feature IDs,
qualification, before/after value, uncertainty, and attributable expected
fantasy-point delta. Freeze the attribution method and order. With
nonlinear interactions, disclose that sequential attribution is ordered;
the steps must reconcile to the final result.

Assign each effect to one component. Do not apply the same defense through
the team total, a global opponent multiplier, and a second efficiency boost.
The fitted incremental model must demonstrate what remains after baseline
inputs. Explain intentionally shared dependence rather than deleting it.

Produce means, medians, P10/P90, registered boom-event probabilities,
opportunity summaries, and availability-conditioned ranges. Player P90s
cannot be summed and called lineup P90. A lower mean with a wider upper
tail can affect a tournament choice; that conclusion requires the joint
lineup and field evaluation below.

### 8.2 Connect real scenario banks to the existing optimizer

Reuse `model/nfl_dfs_efficiency.py` team simulation, the canonical scoring
code, and `web/src/lib/nfl-dfs/scenario-lab.ts`. Extend the adapter and
dependence model to deliver real slates in the existing `NflScenarioBank`
contract, fulfilling D3 of the GPP spec.

Each draw must represent a coherent game: shared pace and script,
availability states, opposing-team effects, passing/receiving reconciliation,
and offense-DST event consistency. Receiver catches/yards/TDs must reconcile
with the QB/team ledger, including unknown/unallocated players. A sack
cannot help the DST without the corresponding offensive event. Captain and
Flex reference the same underlying player outcome with their scoring rules.

Fit and validate dependence on eligible history; fixed assumed correlations
or arbitrary extra noise do not establish realistic tails. Persist bank
identity, input manifest, draw count, and separate generation, selection,
and evaluation seeds. Independence refers to random streams, not unrelated
football outcomes within a draw.

The scenario adapter must identify whether it preserves approved player and
DST marginal distributions or changes them. Register numeric marginal-
preservation tolerances and audit means, quantiles, and relevant event rates.
If existing marginal forecasts cannot jointly satisfy the football ledger,
report the incompatibility and retain the approved baseline path, or
register and qualify changes to all affected forecasts. A common sack event
must not silently introduce an unqualified DST mean or tail adjustment.

Retain current candidate generation, salary/roster legality, exposures,
overlap, locks, and export QA. First evaluate and rerank legal candidates
using the new banks in shadow. Extend candidate search only if the candidate
set demonstrably limits the approved objective. Do not create a competing
portfolio engine or bypass existing release flags.

### 8.3 Contest-specific selection

Persist a `ContestConfig` with platform/scoring, slate and roster rules,
format, total field entries, allowed/user entry count, entry fee, payout
table, standings tie treatment, lock/late-swap rules, ownership capability,
field-model version, and user constraints.

Evaluate complete legal lineups against complete sampled field lineups or
a validated equivalent joint distribution. Missing ownership is unknown,
not zero. Marginal player ownership alone does not give lineup duplication.
Showdown Captain/Flex ownership requires separate supported slot estimates.

Report distinct objectives: expected net payout, probability of at least
one top finish, and construction-only coverage. They are not interchangeable.
Expected portfolio payout is additive over entries, while chance of at
least one strong finish depends on their joint outcomes. Diversification
does not create free expected value; optimize the objective actually named.

Use contest-specific comparisons for single-entry and three-entry, and
equal-cost/equal-entry portfolio comparisons for multi-entry. Account for
all entry fees and tied payout splitting. When field/payout evidence is
missing, keep construction-only results and explicit sensitivity views;
do not relabel them expected ROI or probability of winning the real contest.

Showdown remains the first default-promotion scope of the existing GPP
program. Classic and each contest-size/entry-mode cohort need their own
qualification; a Showdown result does not silently approve Classic.

## 9. What the pages must explain

### Pick'em game detail

Display market baseline, active adjusted probability if qualified, freshness,
evidence coverage, and a compact explanation of any change. Separately show
football favorite, pool recommendation, crowd-share source, expected-score
cost, and the stated weekly/season objective. Show which supported changes
would reverse the recommendation.

Research probabilities belong in an **Under evaluation** comparison and
must not silently become the default pick. Evidence-only data may still
explain protection, contact, usage, and missing information immediately.

### DFS player Why panel and lineup review

Show expected workload, matchup-sensitive components, final scoring range,
and the component delta ledger. Identify evidence that was informative but
did not alter the production number, with the reason. Preserve the existing
opponent-rush-volume audit step when that study earns promotion.

Lineup review shows a complete-lineup distribution, correlated assumptions,
relevant contest mode, ownership capability, and field-relative results only
where supported. Saved explanations must load their exact frozen inputs.

### Worked acceptance walkthroughs

- **Tampa Bay-Minnesota:** show actual captured pressure/protection evidence,
  QB/personnel state, PBP opportunity, and the contemporaneous market baseline.
  The pressure challenger must identify the specific component and candidate
  probability/points delta. Until qualified, the active forecast remains the
  approved baseline. Do not manufacture a numerical matchup adjustment for
  the demonstration.
- **RB versus a defense allowing many carries:** expose the existing
  `opp_carries` calculation separately from PFR contact efficiency. A player
  can receive more expected rushing production from volume while facing
  lower estimated per-carry efficiency. Show both effects and their final
  total; do not add a third generic "good matchup" bonus.
- **Missing PFR or ownership:** the page and ordinary legal lineup generation
  remain usable. Coverage/fallback reasons are visible, and unsupported
  advantage, duplication, and ROI claims disappear.

## 10. Evaluation, release gates, and rollback

### 10.1 Register before evaluating

Every new feature family must have a machine-readable study manifest with
its hypothesis, exact definition and source versions, consumer/cohort,
baseline/config, chronological train/tune/holdout windows, snapshot cutoff
rules, primary target/metric, sample floor, materiality threshold, harm
margins, multiplicity rule, interval method, seeds, fallback, and rollback.
Incomplete manifests cannot produce a promotion verdict.

Do not backdate September 27 PFR captures into earlier executable forecasts.
Older data can support labeled retrospective development or a historical
study only where trustworthy pre-lock availability is actually retained.
Preserve prospective captures now. A study of revised historical data must
not be reported as the performance of recommendations the system made then.

Pair comparisons on the same eligible games/players/slates. Report both
the covered cohort and the full deployment population with registered
fallbacks, so selectively missing hard cases cannot manufacture a gain.
Cluster uncertainty at the appropriate game/slate/week level; correlated
players, rival entries, and Monte Carlo draws are not independent games.
Do not repeatedly examine a holdout and tune to it.

Qualify the proposed active model configuration as a whole, not just its
individual feature families. Pressure and contact passing separately does
not establish that their combination with availability, markets, and an
approved opponent term helps. Freeze the combined configuration and compare
it with the stated current-production baseline using the applicable gate;
retain family ablations for attribution. Register candidate combinations
up front so they can share the same untouched forward window and declared
multiplicity correction. Descriptive-only additions do not require a new
forecast trial. Protected study arms remain unchanged.

### 10.2 Proposed gates for new forecast families only

The following are initial project acceptance thresholds, not measured gains
or assurances of statistical power. Register them before examining holdout
labels; any amendment requires a new untouched evaluation window. They do
not change the existing opponent, availability, or GPP study gates.

| Challenger purpose | Minimum prospective evaluation | Primary promotion condition | Guardrails |
|---|---|---|---|
| Pick'em market-residual forecast | 8 scorable forward NFL weeks and 100 paired games | Mean candidate-minus-baseline log loss <= -0.001, with multiplicity-adjusted 95% CI upper bound < 0 | Brier delta CI upper bound < +0.002; fixed-bin calibration error increase <= 0.01; valid H/A/tie probabilities |
| DFS mean forecast, by registered position/role cohort | 8 scorable forward weeks and 200 paired player-games from at least 50 NFL games | Fantasy-point MAE improvement >= 0.05 points and adjusted 95% CI upper bound on error delta < 0 | P90 pinball-loss delta CI upper bound < +0.05 points; each declared affected position's mean-MAE delta CI upper bound < +0.10 points |
| DFS distribution-only change | Same cohort floor as above | At least 1% relative improvement in frozen weighted interval score, with adjusted 95% CI upper bound on error delta < 0 | Mean-MAE delta CI upper bound < +0.10 points; declared coverage/width and boom calibration checks pass |

Specify quantile levels, interval-score weights, calibration bins, boom
thresholds, affected cohorts, and multiplicity family in the registration.
DFS point metrics use the registered platform/scoring version without
pooling incompatible scoring systems. Pick'em primary loss is three-class
natural-log loss over home/away/tie; Brier is the sum of the three squared
probability errors. An observed tie is scored as a tie, not dropped or
assigned to a team. Use the shared frozen tie component and identical
probability clipping at 1e-6 followed by normalization for both forecasts.
Register calibration bins and class-aggregation rules before holdout access.
Use week-clustered intervals for forward football comparisons and a
predeclared familywise correction when screening multiple candidates.
Insufficient precision or a missing cohort returns **no verdict**, not PASS.

Check mechanism metrics too: attempts/carries/targets, sack counts, yards
per opportunity, bias, and role-state calibration. A component improvement
alone does not approve a worse final fantasy forecast. Conversely, a
qualified distribution improvement need not pretend it improved mean MAE.
Publishing all attempted families and failures prevents winner-only reporting.

### 10.3 Existing gates remain authoritative

- `opp_carries`: retain the exact eight-forward-week PASS/kill rules and
  subsequent four-week v6/v5 rollback from the merged opponent spec.
- GPP revision 2: construction-only primary is best selected portfolio
  lineup's actual score divided by the best achievable legal same-pool score.
  Once D4 is complete, primary is portfolio top-1% hit rate. Promotion
  requires the lower 95% interval above legacy over at least 20 untouched
  Showdown slates, 100% legality, at least 99% lineup fulfillment, and no
  increase in QA blockers. The prescribed comparison arms remain in force.
- Before a defaults change, run at least three real saved slates with
  shipped defaults: inexpensive-player Showdown, missing-ownership Showdown,
  and Classic. Compare lineup count, legality, and baseline fingerprints.
- Availability phases and ownership capability must satisfy their own
  existing contracts; PFR integration does not bypass them.

Separate forecast promotion from decision-policy promotion. Store old and
new forecasts/policies concurrently during evaluation. An independent
evaluation bank assesses the selected portfolio, rather than reusing the
draws that selected it. Real contest results remain the ultimate test;
simulated superiority is not proof of realized prize improvement.

### 10.4 New-family rollback

Version and activate forecast and decision policies separately. Immediately
disable an affected feature on a failed as-of, identity, unit, or dependency
invariant, preserving the approved fallback and recording the incident.

For newly promoted forecast families, freeze a baseline comparison stream
for the next eight scorable forward weeks. At that fixed endpoint, roll
back if the paired primary-error delta's 95% CI lower bound is above zero,
or a registered harm margin is exceeded under its registered uncertainty
rule. A new failure cannot be repaired by silently refitting the same
version. Existing study-specific rollback rules take precedence.

## 11. Delivery work packages

Each package needs a real-data artifact plus the checks that prove its
contract. Compilation or a new column alone is not completion.

| Package | Work and integration points | Completion evidence |
|---|---|---|
| WP0: Preserve current experiments | Reconcile merged opponent spec; implement its `context_variants` grader in the report card; verify study pins, deduplication and pre-lock freezes | Correct scorable-week report, gate tests, scheduled shadow health; can proceed in parallel with WP1 |
| WP1: Evidence contract | Extend `db/nfl_pfr_schema.py`, PFR adapters, context observations, `web/src/db/pickem-pfr.ts` and `web/src/lib/nfl/pickem-pfr.ts` to preserve exact source and identity manifests | Frozen snapshot replay; unit/coverage tests; no latest-data substitution |
| WP2: Shared matchup features | Add a single versioned extractor, proposed `model/nfl_matchup_features.py`, publishing through the context engine; register pressure and RB-contact families separately | Real target-game feature manifest, matching Python/UI units, descriptive explanations and coverage |
| WP3: Pick'em challenger and policy repair | Add registered market-residual shadow forecasts; extend probability contract for ties; repair crowd marginals in `pickem-strategy.ts`; add pool configuration and standings-aware policy | Paired market/challenger report, share recovery tests, weekly/season scenario comparisons |
| WP4: DFS forecast challengers | Integrate qualified feature reads into projection/shadow paths; expose component ledgers; reuse availability resolution; retain frozen opponent variant | Baseline and challenger stat/point distributions from one real slate, exact audit reconciliation, prospective registration |
| WP5: Scenario and contest connection | Supply GPP D3 through existing simulator/Scenario Lab; inventory/import D4 from available contest files; persist complete contest/field/seed contracts | Real-slate dependence diagnostics, shadow candidate reranking, existing integration gate; construction-only mode where D4 is absent |
| WP6: Product delivery | Extend pick'em evidence/Why and DFS projection-audit/player explanation/lineup review; freeze all displayed comparisons | Visual QA for active, research, missing, stale, and saved-run cases; no unqualified defaults |
| WP7: Operations and grading | Connect refresh health, target-lock checks, coverage, forecast grading, contest grading, active-policy and rollback reports | Reproducible morning report and postgame grade; original-versus-revised outcome handling; actionable failure alerts |

WP2 requires WP1; WP3 and WP4 may run in parallel afterward. The pick'em
crowd repair and WP0 do not need to wait for PFR research. WP5 can begin with
approved baseline forecasts while new matchup families collect forward
evidence. WP6 can release descriptive evidence before numeric promotion.
This sequencing yields useful explanations and auditable shadow forecasts
without waiting for every data source or contest record.

## 12. Operating cadence and lock behavior

The currently configured refresh is a local Codex automation at 7:00 a.m.
America/New_York on Monday, Tuesday, and Friday. It requires the host and
app to run. This document does not turn that into a deployed scheduler.

- **Friday morning:** refresh available PBP/PFR revisions, usage, markets
  and availability; publish an initial upcoming-game/slate assessment with
  capture times, missing reports, and research-versus-active status.
- **Monday morning:** grade completed eligible games and Sunday contests;
  refresh the remaining Monday game while preserving already locked inputs.
- **Tuesday morning:** incorporate Monday results, grade the completed slate,
  and report source revisions and study sample progress.
- **Before each decision/lock:** verify market freshness, current supported
  availability/inactives, platform eligibility, legal locked selections,
  feature qualification, and bank/manifest consistency. Friday morning
  cannot certify readiness for Sunday; final injury information arrives later.

Implement pre-lock verification in the existing projection/selection paths
and coordinate its scheduling with existing refresh jobs. Record the owner
and deployed scheduler only when that operational change actually ships.
Use timezone-aware local schedules across daylight-saving changes.

Unpublished PFR files are pending coverage, not zero performance. Retry
under the existing bounded source policy and alert on missed expected
coverage or failed freezes. Newly available post-lock data starts a new
snapshot and may improve the next game; it must not rewrite a saved pick
or projection. Corrections to outcomes may update grades with a retained
original-result revision, never the original prediction.

## 13. Required acceptance checks

| Check | Required result |
|---|---|
| Add a later PFR/PBP/identity revision after freeze | Original features, forecast, card/lineup, and Why explanation reproduce identically from the saved manifest |
| Equivalent HTML and nflverse inputs | Same normalized units and supported feature values; unsupported fields remain explicitly absent |
| Rounded pressure rate without denominator | No invented exact exposure, pooled event count, pressured EPA, or summed defender-event estimate |
| Missing player, backup QB row, snap section, unpublished required file, or unprovable availability | Appropriate incomplete state and registered fallback, never a measured zero; unknown publication timestamp alone does not invalidate a proven pre-cutoff capture |
| Market residual is zero or feature is unqualified | Exact approved baseline output; unqualified evidence remains descriptive/shadow |
| Matchup adjustment changes a component | Only declared components change; audit deltas reconcile; protected `opp_carries` fields/constants stay intact |
| Availability or team opportunity state changes | Shared resolver used; allocations conserve the declared team budget; unknown residual remains visible |
| Scenario event is a sack, completion, or TD | Offense/receiver/DST ledgers agree; exact scorer and Captain/Flex treatment agree |
| Observed shares include 0/100%, chalk, or no rivals | Population boundaries preserved; simulated marginals recover valid inputs within declared Monte Carlo tolerance |
| NFL game tie or tied contest finish | Correct distinct game scoring and prize splitting; probabilities and payouts reconcile |
| New portfolio selected | Independent evaluation stream, complete rules, equal entry cost comparison, and no sum-of-player-percentiles claim |
| Missing field/ownership/payout data | Legal baseline construction works; uncertainty visible; unsupported contest-win/ROI assertions withheld |
| Real saved slates with shipped defaults | Existing three-slate GPP gate passes; no feed absence creates new default exclusion |
| New study reaches sample floor | Frozen numeric gate runs; insufficient precision gives no verdict; failure does not silently retune |
| Morning or pre-lock source refresh fails | Health report identifies affected game/feature and fallback; started games retain frozen decisions |

The handoff is complete when an engineer can trace a displayed recommendation
to exact eligible source observations, see which components changed and why,
replay the result, and grade it against a frozen baseline. The improvement
claim becomes earned only when the relevant forecast or contest gate passes.
