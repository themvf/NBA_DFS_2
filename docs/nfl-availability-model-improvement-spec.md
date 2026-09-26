# NFL availability, depth chart, and PBP model improvement specification

Status: approved implementation contract; Phases 0–2 complete, Phase 3 built and development-verified with prospective acceptance pending, visual QA pending
Date: 2026-09-25
Scope: NFL market explanation, DFS projections, and player props

This specification supersedes the availability-consumer and development-order
portions of `docs/nfl-dfs-pregame-availability.md` and
`docs/nfl-dfs-game-availability.md`. `docs/NFL Injury.md` continues to govern
raw injury observations, canonical injury episodes, and provider-event
history. Where the older DFS documents conflict with this contract, this
contract controls.

## 1. Decision and objective

The project has enough live injury and depth-chart data to improve the model.
The next constraint is not acquiring another general injury API. It is turning
the observations already captured into one point-in-time, source-attributable
availability and role state that every downstream consumer uses consistently.

The implementation has three product goals:

1. Explain which football conditions are consistent with a market price or
   line movement without claiming to know why a sportsbook acted.
2. Remove known-unavailable players from DFS projections and estimate how
   their opportunities move to active replacements.
3. Produce availability-conditioned player distributions that can eventually
   be evaluated against NFL prop markets.

The shared dependency is a saved answer to this question:

> At a specified pregame time, what did the system know about each player's
> availability, team, depth-chart role, expected participation, and likely
> replacement, and which observations support that answer?

No consumer may independently reinterpret raw injury strings or use current
roster state to reconstruct an earlier week.

## 2. Current evidence and implementation baseline

The production database audit on 2026-09-25 found:

- 188 Sleeper 2026 player snapshots from 2026-08-02 through 2026-09-25.
- 76,301 Sleeper injury observations covering 1,055 players.
- Sleeper depth order for 91 of 127 quarterbacks and substantial RB, WR, and
  TE coverage.
- 6,489 FantasyPros injury observations covering 305 players.
- Week-scoped FantasyPros captures for Weeks 1 through 3.
- No imported official inactive reports.
- No populated normalized practice-status or participation-probability fields
  in the current observation set.

The existing DFS projection path already:

- selects Sleeper depth order from `ff_players.metadata`;
- rejects stale or future depth evidence with a 72-hour bound;
- selects the newest FantasyPros week observation captured before each
  player's kickoff;
- zeros OUT-class players;
- transfers quarterback opportunity to a qualified replacement; and
- saves the availability adjustment with projection evidence.

This is a useful first implementation, not yet the final source contract. In
particular, it reads FantasyPros week observations even when their source
snapshot is marked `model_eligible=false`. That mismatch must be resolved
before expanding availability effects.

## 3. Governing principles

1. **Point in time first.** Every feature must have `available_at <= as_of_at <
   kickoff`. Current truth cannot be backfilled into a historical decision.
2. **Observation and inference stay separate.** Provider status, normalized
   status, resolved game availability, expected role, and modeled
   participation probability are different fields.
3. **One shared resolution.** Market, DFS, props, and Vercel read the same
   saved availability context snapshot.
4. **Sources do not silently overwrite one another.** Conflicts are retained,
   classified, and displayed.
5. **Platform eligibility is absolute for that platform.** A DraftKings OUT
   player cannot enter a DraftKings lineup even if another provider says
   healthy.
6. **Official inactive is game-specific.** An official INACTIVE report settles
   participation for that game. An official ACTIVE listing does not prove a
   starting role, full workload, or health.
7. **Opportunity transfers; efficiency does not.** Replacements inherit some
   team opportunity and retain their own efficiency distribution.
8. **Unknown is not healthy.** Missing, stale, conflicting, and unsupported are
   explicit states.
9. **Explanation is not causation.** Market output describes factors consistent
   with a price and measured movement around events. It does not claim access
   to sportsbook intent or bettor identity.
10. **Promotion is consumer-specific.** Descriptive UI, predictive shadow,
    production projection, optimizer, and betting use require separate gates.

## 4. Shared source policy

The resolver will retain all eligible observations and apply purpose-specific
authority rather than a single destructive precedence rule.

| Source | Primary use | Authority and limits |
|---|---|---|
| Official NFL inactive report | Game participation | Highest authority for INACTIVE when identity, game, publication, capture, and kickoff checks pass. ACTIVE does not establish role or erase other injury context. |
| DraftKings slate status | DFS eligibility | An explicit platform OUT excludes the player from that DraftKings slate. It does not become the canonical medical status. |
| Sleeper | Current status and depth order | Primary automated source for current roster role and live structured status when identity, freshness, and timestamps pass. Current-state rows are never projected backward. |
| FantasyPros week injuries | Injury detail and corroboration | Preserve status, body part, comments, practice fields, and return data. Until provider timestamp semantics are resolved, capture time proves the system saw the row but not that the provider record was freshly updated. It may corroborate or create a conflict; it may not independently override a fresher authoritative disagreement. |
| nflverse weekly roster/PBP | Historical roster and realized participation | Supplies historical membership and postgame truth. PBP participation cannot be used as a pregame feature for the same game. |
| News | Human-readable context | Display and review only until a structured, timestamped extraction is independently validated. |

The initial resolver will produce one of:

```text
ACTIVE_CONFIRMED
EXPECTED_ACTIVE
QUESTIONABLE
DOUBTFUL
OUT_CONFIRMED
CONFLICT
STALE
UNKNOWN
```

Platform eligibility is deliberately absent from this state. It is a separate
decision object because a DraftKings restriction says nothing about whether a
player can participate in the NFL game.

It will separately emit expected role:

```text
QB1, QB2, QB3_PLUS
RB1, RB_COMMITTEE, RB_DEPTH
WR_STARTER, WR_ROTATION, WR_DEPTH
TE1, TE_ROTATION, TE_DEPTH
UNRESOLVED
```

## 5. Shared saved context contract

Reuse the immutable evidence, injury observation, context snapshot,
qualification, and manifest infrastructure. Do not introduce another mutable
current-state cache as a source of truth.

Publish these revisioned context definitions:

### `player_game_availability@v1`

One saved object per player and game as of a requested time:

```text
season, week, game_id, player_id, team, position
as_of_at, kickoff
resolved_availability_state
normalized_status
expected_role, depth_order
participation_probability: null in v1 unless calibrated
replacement_player_id: nullable
freshness_state, conflict_state, confidence_tier
observation_ids, source_snapshot_ids
resolution_policy_version, evidence_digest
```

### `team_qb_state@v1`

One saved object per team-game:

```text
expected_starter_player_id
starter_availability_state
replacement_player_id
starter_change_state
days_since_change: nullable
starter_quality_baseline_id
depth_evidence
availability_evidence
coverage and confidence
```

`starter_quality_baseline_id` identifies an independently frozen efficiency
model. It is not encoded into the availability resolver.

### `team_skill_absence_load@v1`

Initially descriptive only:

```text
available and unavailable players by position
prior opportunity represented by unavailable players
unresolved opportunity share
source conflicts
coverage and freshness
```

The definitions must be resolved through the existing consumer-policy and
snapshot-manifest system. A Vercel request reads saved objects; it does not
calculate football state in TypeScript.

### `platform_slate_eligibility@v1`

One saved decision per player, platform, and slate:

```text
platform, slate_id, player_id, game_id
decision_at, available_at
eligibility_state: ELIGIBLE | INELIGIBLE | UNRESOLVED
platform_status, reason
source, source_capture_at, source_record_id
policy_version, evidence_digest
```

This object can remove a player from a DraftKings pool. It cannot alter
`player_game_availability@v1`, Vegas explanations, or non-DraftKings prop
participation estimates.

### Time contract

`source_published_at` is the provider's claimed publication time when known.
`fetched_at` is retrieval time. `observed_at` is the time the normalized row
was durably available to this system. For a decision, `available_at` is the
later of the source snapshot's `fetched_at` and the observation's
`observed_at`; it represents completed, usable ingestion rather than the start
of an HTTP request. Every decision enforces:

```text
available_at <= decision_at (= requested as_of_at) < kickoff
```

A provider timestamp may add a stricter constraint. It can never replace the
system availability test.

## 6. Evidence decision table

| Evidence condition | Canonical game availability | DFS projection | Optimizer eligibility | Persistence/clear rule |
|---|---|---|---|---|
| Fresh official INACTIVE for exact game | `OUT_CONFIRMED` | Zero | Exclude | Expires after that game; only a corrected official record for the same game can replace it. |
| Platform/slate OUT only | Unchanged | Zero on that platform's resolved slate view | Exclude on that platform/slate | Expires with the slate; never writes canonical game availability. |
| Fresh qualified Sleeper OUT-class | `OUT_CONFIRMED` with source tier | Zero | Exclude | Persists for the game until a newer qualified same-source active observation or stronger exact-game evidence resolves it. Missing rows and failed/partial refreshes cannot clear it. |
| Fresh qualified healthy/active | `EXPECTED_ACTIVE` | Baseline unless a qualified workload adjustment exists | Eligible subject to role/platform rules | Clears an earlier same-source OUT only when the refresh is complete and newer. ACTIVE does not guarantee normal workload. |
| QUESTIONABLE | `QUESTIONABLE` | Baseline in v1 | Eligible | No numerical discount until participation is calibrated. |
| DOUBTFUL | `DOUBTFUL` | Baseline in v1 | Eligible with warning | No asserted near-zero weight until calibrated. |
| Conflicting qualified sources | `CONFLICT` unless exact-game official evidence settles participation | Baseline; preserve any independent platform exclusion | Eligible only if platform-eligible; warn and block unqualified redistribution | No source is silently discarded. |
| Stale evidence | `STALE` | Baseline; do not create a new exclusion or transfer | Platform decision controls; otherwise eligible with warning | A previously saved exact-game decision remains replayable, but stale current evidence cannot authorize a new decision. |
| Missing evidence | `UNKNOWN` | Baseline | Platform decision controls; otherwise eligible with warning | Absence never means healthy and never clears established OUT. |
| Partial/failed refresh | Prior qualified saved state remains available for replay; new current resolution is flagged partial/failed | No new exclusion, clearance, or transfer | Preserve existing platform and exact-game exclusions | Failure performs no clearing writes. |
| Ineligible FantasyPros row | Display-only evidence | Cannot zero, clear, transfer, or cause an exclusion indirectly through conflict resolution | No effect | Retained for audit and future qualification. |

“Fail closed” in this specification means refusing the unsupported *new model
effect*. It does not mean treating every unresolved player as unavailable.

## 7. Web calculation migration inventory

The following current paths must be retired or narrowed as the saved context
reader is introduced:

| Current path | Current responsibility | Migration |
|---|---|---|
| `web/src/lib/nfl-dfs/availability.ts` | Re-resolves roster/injury state at request time | Becomes a typed presenter for pinned `player_game_availability@v1`; it performs no source precedence. |
| `web/src/db/nfl-dfs-availability.ts` | Reads mutable current roster and latest injury rows | Adds pinned context and platform-eligibility readers; raw evidence query remains audit-only. |
| `web/src/app/dfs/nfl/actions.ts::workspaceSlate` | Resolves availability and redistributes opportunity during page reads | Reads the projection run's pinned adjustment plus the slate's pinned platform decision. It may zero a newly platform-ineligible player but cannot redistribute without a saved late-swap decision manifest. |
| `web/src/lib/nfl-dfs/opportunity-redistribution.ts` | Applies read-time opportunity changes | Retained as pure research/replay logic, removed from ordinary workspace reads after the saved adjustment writer is live. |
| `web/src/lib/nfl-dfs/situation-context.ts` | Re-resolves current QB role | Reads the same pinned team-QB context as the projection run. |

The projection drawer, player pool, optimizer, exports, and saved-lineup audit
must expose the same context snapshot ID, platform decision ID, adjustment ID,
and decision time. An integration test must prove one player receives one
adjustment exactly once across all of those surfaces.

## 8. Development order

There are eleven phases, numbered 0 through 10.

### Phase 0 — Freeze the baseline and register the experiment

**Purpose:** make every later gain measurable and prevent an implementation
change from being mistaken for model improvement.

**Work:**

1. Freeze the current projection model version, its configuration, Week 1–3
   inputs, outputs, and availability reports.
2. Save current DFS player, position-group, and lineup metrics.
3. Freeze the current market-attribution model and its held-out results.
4. Inventory current prop capture coverage, markets, players, books, and
   capture lead times.
5. Register every candidate in a mandatory promotion registry before fitting.
   Each entry must state its numerical pass/fail threshold, minimum number of
   independent games or availability events, evaluation horizon, comparison
   baseline, uncertainty method, and what happens when the sample is
   insufficient. “Held-out improvement” without these values is not a gate.
6. Add a point-in-time coverage matrix for every frozen Week 1–3 output. Mark
   roster, depth, injury, platform, kickoff, and market inputs individually as
   reconstructable, current-state-only, missing, or invalid. A reproducible
   output is not automatically a valid historical evaluation row.

**Exit gate:** one reproducible baseline manifest exists for each applicable
consumer, each historical row has a point-in-time integrity classification,
and numerical promotion gates are frozen. Cohorts without sufficient valid
history are assigned prospective shadow horizons rather than evaluated on
mutable inputs.

### Phase 1 — Repair the source-eligibility boundary

**Purpose:** ensure the existing production projection cannot consume a source
under rules that its own snapshot says are ineligible.

**Work:**

1. Replace the direct FantasyPros query in the DFS projection builder with a
   shared availability reader.
2. Make the reader enforce source snapshot eligibility, game/week scope,
   identity, `available_at <= as_of_at < kickoff`, freshness, and conflict
   rules.
3. Treat current FantasyPros rows with unresolved timestamp semantics as
   corroborating evidence, not an independent production exclusion.
4. Preserve existing DraftKings OUT and valid Sleeper OUT exclusions.
5. Log every changed decision in a before/after audit report.
6. Begin the web migration by pinning the projection run's resolved context and
   preventing page-read code from applying an upstream adjustment twice.
7. Save an immutable manifest for every platform or late-swap eligibility
   decision, not only a final slate-lock snapshot.
8. Ship source-coverage, staleness, conflict, and changed-decision monitoring
   in this slice. Add a tested rollback to the previous qualified projection
   policy while retaining confirmed-game and platform exclusions. Rollback may
   not restore the FantasyPros eligibility bypass.

**Tests:** future captures, `as_of_at < available_at < kickoff`, post-kickoff
captures, stale rows, wrong-week rows, partial snapshots, ambiguous identities,
source conflicts, and empty feeds follow the decision table. A failed provider
refresh never clears an injury. A web integration test proves the projection
drawer, player pool, optimizer, export, and saved audit use one pinned context
and apply one adjustment exactly once.

**Exit gate:** every production exclusion names the qualifying observation,
source snapshot, policy version, as-of time, kickoff, and reason.

### Phase 2 — Build and publish the shared availability resolver

**Purpose:** create one canonical, replayable input for all three model goals.

**Work:**

1. Implement deterministic source normalization and conflict classification.
2. Derive `player_game_availability@v1` and `team_qb_state@v1` from observations
   that were knowable by the requested as-of time.
3. Publish immutable context snapshots and manifests.
4. Add pinned and current-eligible reads using the existing context engine.
5. Add a coverage report by game, team, position, source, freshness state, and
   conflict state.

**Tests:** exact replay, current-state correction without mutation of prior
snapshots, no current-depth backfill, stable evidence digest, and distinct
handling for zero, missing, stale, and conflict.

**Exit gate:** DFS, market research, props research, and Vercel can retrieve the
same saved state and evidence identifiers for a selected player-game-as-of.

### Phase 3 — Harden capture cadence and game-day truth

**Purpose:** reduce unknown and stale states at the times decisions are made.

**Work:**

1. Run the lightweight Sleeper injury/depth refresh every two hours, increasing
   cadence near kickoff within provider and infrastructure limits.
2. Run week-scoped FantasyPros capture independently so its failure cannot
   block projections.
3. Persist raw practice fields from the provider payload even while their
   modeled meaning remains disabled.
4. Automate coverage/freshness alerts for missing teams, implausible row-count
   changes, stale depth charts, and rising identity failures.
5. Operationalize official inactive ingestion. Start with reviewed imports;
   automate only if a stable official source contract is verified.
6. Capture an immutable final pre-lock availability manifest for every slate.

**Exit gate:** all slate games have a pre-kickoff manifest; freshness and source
coverage are visible; official-inactive coverage is measured rather than
assumed.

### Phase 4 — Complete the deterministic DFS quarterback correction

**Purpose:** eliminate the highest-confidence class of projection error before
fitting uncertain injury effects.

**Work:**

1. Read `team_qb_state@v1` instead of raw injury/depth rows.
2. Project confirmed OUT quarterbacks at zero.
3. Select the next eligible quarterback from fresh point-in-time depth evidence.
4. Treat preservation of the absent starter's team play and pass volume as the
   first baseline hypothesis, not a football identity. Competing replacement
   scenarios may change pace, pass rate, scramble rate, and designed rushing;
   apply the replacement's own efficiency and uncertainty.
5. Reconcile quarterback, receiver, rusher, kicker, and opposing DST event
   totals within each scenario. Event accounting must reconcile, but total
   fantasy points are allowed to change when volume, efficiency, turnovers, or
   scoring expectations change.
6. Save baseline and adjusted player distributions, not only adjusted means.
7. Expose the adjustment and its evidence in the existing DFS projection
   drawer and model review surface.

**Validation:** walk-forward team-QB group MAE, replacement MAE, distribution
CRPS/WIS, interval coverage, and downstream receiver/DST error. Report how often
the rule fires and the OUT-to-did-not-play precision.

**Exit gate:** promote only if the preregistered held-out gate passes and no
reconciliation invariant regresses. Otherwise retain zeroing and retire the
transfer candidate.

### Phase 5 — Add availability to market explanation in shadow

**Purpose:** determine how much injury and starter information helps explain
opening prices and subsequent movement beyond existing scoring and PBP
descriptors.

**Work:**

1. Join as-of `team_qb_state@v1` and `team_skill_absence_load@v1` to opening,
   intermediate, and closing odds snapshots.
   Every availability feature and every historical PBP aggregate must have an
   availability time strictly before the specific quote being explained—not
   merely before kickoff.
2. Add interpretable features: starter unavailable, recent starter change,
   starter-to-backup quality difference, unresolved QB state, and unavailable
   prior opportunity by position.
3. Keep the existing PBP descriptors—success rate, pass rate, pressure/sack
   behavior, drive finishing, explosive plays, neutral pace, and archetypes—as
   separate feature families.
4. Run ablations in development order:
   baseline market model; plus PBP; plus availability; plus PBP and
   availability; then interactions.
5. Run event studies around timestamped availability changes to describe when
   the consensus line moved. Use unchanged/ineligible games as controls.
6. Publish contribution ranges and source evidence to the Vegas card.

**Validation:** expanding-season holdouts, game/week-clustered uncertainty,
opening-spread MAE, movement MAE, calibration, feature stability, and negative
controls. Player status captured after a market snapshot is never joined to it.

**Exit gate:** descriptive release requires replay, coverage, preregistered
held-out reconstruction quality, stability across evaluation horizons, and
negative-control checks. Predictive use requires its separately registered
thresholds. The UI must say “consistent with the price,” never “Vegas moved
the line because.”

### Phase 6 — Calibrate uncertain participation and workload

**Purpose:** replace unsupported QUESTIONABLE/DOUBTFUL heuristics with measured
probabilities and workload distributions.

**Work:**

1. Create historical labels from official participation, snaps, routes,
   attempts, and touches after games complete.
2. Fit participation separately from workload conditional on participation.
3. Candidate features include normalized designation, status-transition path,
   time to kickoff, position, prior workload, practice progression when valid,
   source agreement, and recurrence/return context.
4. Use season-forward validation only. Calibrate probabilities with held-out
   reliability curves and Brier/log loss.
5. Emit P10/P50/P90 workload conditional on each availability scenario.
6. Keep probability null where the evidence cohort is unsupported.

**Exit gate:** participation probabilities and intervals must beat designation-
only baselines out of sample and meet calibration tolerances. Until then,
QUESTIONABLE remains unchanged in production projections.

### Phase 7 — Model RB, WR, and TE opportunity redistribution

**Purpose:** extend beyond quarterback only after role uncertainty is modeled.

**Work:**

1. Identify historical team-weeks with point-in-time known absences.
2. Estimate redistribution separately for carries, targets, routes, air yards,
   red-zone work, and personnel-specific snaps.
3. Use depth order, prior role, PBP personnel, alignment, play archetypes, and
   teammate role compatibility. Do not use a uniform “next man gets all” rule.
4. Preserve an unresolved opportunity bucket when the active roster cannot
   account for the full team budget.
5. Re-apply each recipient's own efficiency distribution.
6. Fit and validate RB, WR, and TE independently; one position may promote
   while another remains shadow-only.

**Validation:** player and team opportunity MAE, rank correlation, CRPS/WIS,
coverage, and reconciliation to team totals. Compare against no-redistribution
and simple proportional baselines.

**Exit gate:** position-specific held-out improvement with stable calibration
and no systematic team-budget inflation.

### Phase 8 — Build availability-conditioned prop distributions

**Purpose:** test whether the shared context improves player-stat distributions
before asking whether it produces a betting edge.

**Work:**

1. Generate joint game scenarios from team play volume, quarterback state,
   position opportunity, player efficiency, opponent PBP context, and game
   environment.
2. Reconcile passing yards with receiving yards, passing touchdowns with
   receiving touchdowns, turnovers with opposing defense, and player volume
   with team volume in every simulation.
3. Produce distributions for passing attempts/yards/TDs, rushing attempts/yards,
   receptions/targets/yards, anytime touchdowns, interceptions, and sacks only
   where source coverage and sample size support them.
4. Join a prop price only if it was captured before the decision time. Retain
   book, line, price, hold, capture time, and market shape.
5. Evaluate projection quality first; evaluate price decisions second.

**Validation:** MAE, CRPS/WIS, PIT and interval calibration, threshold
probability calibration, performance versus closing consensus, and ablations
with availability/PBP removed. Betting evaluation must include vig, pushes,
limits of one-sided markets, multiple-testing control, and realistic execution
timing.

**Exit gate:** a prop model can become predictive only after held-out
distribution improvement. It can issue betting recommendations only after a
separate prospective decision gate demonstrates stable value after price and
execution costs.

### Phase 9 — Integrate joint DFS scenarios and optimizer decisions

**Purpose:** let availability uncertainty affect lineup upside and correlation,
not just median projections.

**Work:**

1. Feed the availability-conditioned joint distributions into the existing
   scenario harness.
2. Use separate random streams for candidate selection and evaluation.
3. Preserve game-level correlations and mutually exclusive starter scenarios.
4. Evaluate lineup target probability, exposure, duplication-sensitive upside,
   and late-swap behavior against the baseline optimizer.
5. Keep portfolio and betting authority disabled until their own promotion
   gates pass.

**Exit gate:** deterministic replay, valid football accounting, calibrated
player marginals, and held-out lineup improvement using verified historical
salary and contest inputs.

### Phase 10 — Production promotion and monitoring

**Purpose:** make improvements reversible, observable, and safe after release.

**Work:**

1. Promote by consumer and cohort through context qualifications and policy
   pointers; never by a global feature flag that grants every use.
2. Run shadow and production models side by side before each promotion.
3. Monitor coverage, staleness, conflicts, resolver changes, rule-fire counts,
   projection deltas, calibration drift, and realized error.
4. Provide a kill switch that falls back to the last qualified policy without
   deleting observations or manifests.
5. Retain every projection, explanation, scenario, and decision manifest for
   replay.

**Exit gate:** operational dashboards and alerts are live, rollback is tested,
and the Vercel surface displays the active model/policy version and evidence
freshness.

## 9. Vercel product behavior

Vercel will present the same saved context differently by user task:

### Vegas/PBP game page

- Show the opening/current spread and total with capture times.
- Show the resolved quarterback state and material skill-position absences.
- Separate observed facts, model associations, conflicts, and unknowns.
- Show PBP descriptors and availability contributions as separate groups.
- Explain the number as a range of measured associations, not a causal story.
- Link every material status to source and capture freshness.

### DFS workspace

- Exclude platform-OUT and confirmed-OUT players visibly.
- Label expected starter, promoted replacement, uncertain role, and unresolved
  role.
- Show baseline projection, availability adjustment, adjusted range, and the
  opportunity that moved.
- Warn rather than fabricate an adjustment when depth or freshness fails.
- Preserve the exact availability snapshot in saved optimizer inputs.

### Prop research surface

- Show the projected distribution, market line/price, capture time, and model
  probability.
- Show which availability scenario drives the distribution.
- Distinguish “projection edge,” “price-qualified candidate,” and “validated
  recommendation.”
- Keep recommendations disabled while the model is descriptive or shadow-only.

## 10. Release sequence

The deployable slices are deliberately smaller than the research phases:

1. Source-boundary repair and audit report.
2. Shared resolver plus availability coverage UI.
3. Capture/freshness monitoring and final pre-lock manifests.
4. Qualified DFS OUT zeroing through the shared resolver.
5. Shadow quarterback transfer and review UI.
6. Descriptive availability-aware Vegas card.
7. Calibrated participation model in shadow.
8. Position-specific redistribution candidates.
9. Prop distributions and scorecard, without recommendations.
10. Joint DFS scenario integration.
11. Consumer-specific promotions that pass their gates.

Each slice must include schema/read compatibility, unit tests, replay tests,
data-quality checks, a saved real-data artifact, browser verification, and a
documented rollback path.

## 11. Definition of done

This program is complete when:

- every NFL game has a replayable pregame availability manifest;
- DFS, Vegas, and props resolve the same player and team state for the same
  as-of time;
- known unavailable players cannot retain live production projections;
- replacement opportunity is position-specific and reconciles to team totals;
- market explanations show measured availability and PBP contributions without
  causal overclaiming;
- prop distributions are point-in-time, calibrated, and evaluated separately
  from betting decisions;
- current depth charts are never used as historical evidence;
- unknown, stale, conflict, inactive, active, and platform-ineligible remain
  distinct throughout storage, modeling, and UI;
- every promoted use has an explicit held-out gate, active policy version,
  evidence manifest, monitoring, and tested rollback; and
- a failed or missing provider degrades to a visible unknown state rather than
  a fabricated healthy player or silent model change.

## 12. Immediate next implementation slice

Begin with Phases 0 and 1 together:

1. freeze the current Week 1–3 DFS outputs and reports;
2. add a regression test proving an ineligible FantasyPros snapshot cannot
   independently zero a player;
3. add the `as_of_at < available_at < kickoff` regression and define
   `available_at` as completed usable ingestion time;
4. implement the shared point-in-time availability reader and concrete
   decision table;
5. route the existing projection builder through that reader;
6. inventory and begin replacing the web-side resolver and redistribution
   paths, with exactly-once integration coverage;
7. produce a before/after audit for every affected player;
8. add monitoring, immutable late-swap manifests, and tested rollback; and
9. deploy the audit state to Vercel before changing any opportunity transfer.

This removes the present source-contract ambiguity while preserving the
already-correct safety behavior for platform OUT and qualified live status. It
also creates the boundary needed by every later market, DFS, and prop phase.

### Local implementation checkpoint — 2026-09-25

Completed in the first local slice:

- froze the selected pre-first-kickoff Week 1–3 run identities and documented
  which inputs are and are not historically reconstructable;
- registered numerical promotion gates and insufficient-sample behavior;
- added the shared Python point-in-time resolver and concrete evidence states;
- enforced completed ingestion time, requested `as_of_at`, kickoff, source
  eligibility, snapshot completeness, freshness, exact official-game scope,
  and conflict handling;
- removed the direct model-ineligible FantasyPros decision path while retaining
  those rows as display-only evidence;
- persisted the exact resolver decision and qualifying source snapshot on each
  new projection row;
- added migration-difference and availability-health reports to each projection
  manifest;
- added an executable safety rollback that preserves qualified OUT zeroing but
  disables transfer and cannot restore the source bypass;
- made Vercel prefer the projection row's pinned decision and use mutable
  current evidence only as a clearly warned legacy fallback;
- created a separate, immutable, digest-addressed DraftKings slate eligibility
  manifest and carried it into player-pool, drawer, and optimizer evidence;
- added an exactly-once guard that rejects simultaneous upstream and web-side
  opportunity adjustments; and
- added regression coverage for future-at-decision observations, ineligible
  sources, stale/failed/partial evidence, clearing, conflicts, exact-game
  official evidence, rollback, platform scope, and duplicate adjustments.

Deployment verification completed against the configured development database:

- additive projection-run, slate-upload, and slate-player manifest columns are
  installed;
- shared-policy run `fe221e8e-613c-51db-91b6-837bc0333dbe` saved 1,087 player
  decisions, 85 qualified OUT projections, zero FantasyPros-authorized
  decisions, and a run-level health/migration manifest;
- salary upload `81544667-eb2d-4444-bb2e-e42a673adf94` pinned all 53 matched
  players to that run and platform manifest
  `f3412b91b50e5aa8647cd17de1c314d70d105129381bd924f8916293be6f96dc`;
- the workspace reader and projection drawer returned the same game-decision
  and platform-manifest identities for the verification player;
- rollback run `94adab80-ae76-59ce-8e8c-d695d6ceec45` retained all 85 OUT
  zeroes, emitted zero transfers, and authorized zero FantasyPros decisions;
- a subsequent shared-policy run was made newest so the rollback verification
  is not the default for later salary uploads;
- the operational workspace renders pinned/legacy counts, conflicts, unknowns,
  decision time, platform digest, and rollback policy; and
- the optimized production build, TypeScript checks, Python tests, workspace
  tests, persistence tests, and saved-workspace tests pass.

The Windows computer-use runtime could not initialize because its kernel-assets
path was missing, including after the required reset. Automated server/action
verification completed, but a human visual pass of the rendered disclosure
panel remains the only Phase 1 acceptance item not executed in this session.

### Phase 2 implementation checkpoint — 2026-09-25

- extended the shared context engine with structured payloads and distinct
  decision (`as_of_at`) and publication (`available_at`) timestamps;
- published `player_game_availability@v1` and `team_qb_state@v1` from frozen
  projection-run inputs without querying mutable roster state;
- added stable evidence digests, observation and source-snapshot identities,
  position-specific expected roles, qualified replacements, confidence,
  freshness, conflict, and coverage fields;
- kept participation probability null and the quarterback quality baseline
  unresolved rather than promoting uncalibrated estimates;
- added descriptive-only policy qualifications for Vercel, DFS audit, market
  research, and prop research, all resolving the same immutable snapshots;
- added current-eligible and pinned replay reads in Python and TypeScript;
- added a Vercel availability coverage panel for games with published context;
- published development run `52d359ac-cc58-5c9a-9eac-c1bc1ee9a26f`, containing
  1,087 player contexts and 32 team-QB contexts across 16 games and 32 teams;
- saved release `a3585d602944ef196a7cdac3e6573d8999f502bfef2d792044a1481ca705d7dd`
  and snapshot manifest digest
  `35cda4814264a1caa6497140f690080f8d3ec1614a39e3033ed99688dd483bab`;
- persisted coverage by game, team, position, chosen source, freshness state,
  conflict state, and resolved availability state; and
- verified on `2026_03_ARI_SF` that Vercel, DFS, market, and props consumers
  resolve snapshot
  `195dfaf526918456fc67e71e0504be1c57acd881527e3847e2c82b150e733710`
  and replay it exactly.

Phase 2 automated acceptance passed: 53 targeted Python tests, TypeScript
validation, the optimized production web build, and the live four-consumer
replay verifier. The next implementation slice is Phase 3 capture cadence,
freshness monitoring, official inactive operations, and final pre-lock
manifests. The existing browser-runtime issue still prevents automated visual
inspection; no production Vercel deployment has been performed.

### Phase 3 implementation checkpoint — 2026-09-25

- added a dedicated cadence-aware availability workflow that runs hourly near
  kickoff and every two hours otherwise;
- separated the authoritative Sleeper refresh from fail-soft FantasyPros
  corroboration so one provider cannot block the other;
- added complete-response and identity hard floors before Sleeper performs any
  roster or injury writes;
- retained practice fields in raw FantasyPros observations while leaving their
  modeled effect disabled;
- added monitoring for missing teams, missing QB contexts, source age,
  implausible row-count changes, canonical-identity coverage drops, stale-state
  rate, and official-inactive coverage inside the pre-lock window;
- added reviewed, exact-game official-inactive imports requiring a named
  reviewer, pre-kickoff source and review timestamps, schedule validation,
  stable player identity, and explicit INACTIVE rows—list omission never
  implies ACTIVE;
- added immutable kickoff-wave pre-lock manifests that freeze the projection
  run, player/team context snapshot IDs, source snapshot IDs, and coverage;
- added removal/withdrawal handling so corrected rosters cannot leave obsolete
  player-game snapshots marked current; and
- caught and corrected the live Sleeper `WAS` to canonical `WSH` alias before
  accepting the operational run.

Development evidence:

- Sleeper snapshot `4802` captured 9,422 external rows, matched 1,060 canonical
  players, and persisted 1,060 point-in-time observations;
- refreshed projection/context run
  `222068bb-d4d5-5b56-8ae2-96fa202c0794` restored 1,087 projected players and
  all 32 team contexts;
- health run `6ac9c21fa0b957f31eee0c19934a659d5f83a91052a5385b085836b8e8a461f2`
  passed with 16 games, 32 team contexts, 32 QB contexts, no alerts, and a
  Sleeper age of roughly six minutes at evaluation; and
- official inactive coverage is explicitly reported as zero observations
  across zero games rather than being assumed.

The Phase 3 code and development health gate are complete, but the prospective
exit gate remains open: no game was inside the 90-minute window during this
implementation session, so no artifact was mislabeled as a *final* pre-lock
manifest, and no reviewed official inactive document was supplied. The
workflow is deployed, the scheduler will create those manifests at the correct
time; Phase 3 becomes fully accepted only after every slate game has
prospective pre-lock coverage.
