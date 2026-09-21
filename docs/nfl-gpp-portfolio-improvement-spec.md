# NFL GPP Portfolio Improvement Specification

**Status:** Proposed  
**Date:** 2026-09-20  
**Applies to:** `/dfs/nfl`, with Showdown as the first release target  
**Primary reference:** [`docs/nfl-dfs-spec.md`](./nfl-dfs-spec.md)

## 1. Purpose

This specification defines the implementation required to turn the current NFL lineup generator into a deliberate GPP portfolio builder. It addresses eight improvements in the required delivery order:

1. Role-aware no-punt controls.
2. Ownership-missing safeguards.
3. Separate Captain/Flex and minimum/maximum exposures.
4. Explicit fade and game-script portfolio presets.
5. Salary-left and duplication controls.
6. Pre-export portfolio quality assurance.
7. Correlated scenario scoring inside lineup generation and selection.
8. Point-in-time backtesting against actual contest outcomes.

The product goal is not to promise wins. The goal is to create legal, evidence-backed, internally coherent portfolios that express intentional tournament theses, control fragile salary relief, and can be evaluated honestly against historical contests.

This document is a product and engineering addendum to `docs/nfl-dfs-spec.md`. Where the two documents overlap, the main NFL DFS specification remains authoritative for source provenance, as-of discipline, scenario mathematics, contest simulation, portfolio optimization, auditability, and payout accounting. This document defines the near-term product behavior, UI, settings, release sequence, and acceptance gates.

## 2. Current-state findings

The live Showdown page inspected on 2026-09-20 provides lineup count, minimum lineup salary, maximum exposure, minimum uniqueness, minimum player salary, randomness, and an observed-game requirement. Its saved portfolio nevertheless exposes several structural limitations:

- A salary floor can remove `$200` players but cannot distinguish a legitimate cheap role from a nonviable punt.
- Missing ownership behaves like zero in the local optimizer objective, which falsely rewards players whose ownership is unknown.
- A single maximum exposure applies across all roster slots; Captain and Flex strategy cannot be controlled separately.
- Fade lineups occur incidentally through sequential exposure pressure instead of being allocated as named portfolio strategies.
- The default `$49,000` lineup salary floor compresses construction toward duplicated combinations.
- GPP scoring sums player upper-tail values even though that sum is not a lineup percentile.
- Portfolio checks do not provide a single, blocking readiness decision before export.
- The separate Scenario Lab is not yet the scoring engine used to select the generated portfolio.

There is also a source/deployment mismatch: controls visible in the deployed page are not present in the inspected local `main` implementation. Phase 0 below must be completed before changing behavior.

## 3. Scope and non-goals

### 3.1 In scope

- DraftKings NFL Showdown candidate generation, portfolio selection, QA, export, and historical evaluation.
- Classic-mode-compatible contracts where practical, without delaying Showdown.
- Explicit handling of unknown data, evidence freshness, user overrides, and reproducibility.
- Strategy presets that result in enforceable optimizer constraints and portfolio quotas.
- Migration of existing settings without silently changing saved historical runs.

### 3.2 Out of scope

- Claims or guarantees of profitability.
- Automated contest entry or late swap submission.
- Treating heuristic ownership or duplication estimates as observed facts.
- Reconstructing historical inputs with data learned after lock.
- Replacing the canonical projection pipeline or availability system.
- Building an unrestricted strategy-language editor in the first release.

## 4. Normative language and terminology

`MUST`, `MUST NOT`, `SHOULD`, and `MAY` are normative.

- **Punt:** a cheap player whose projected viability is driven primarily by salary rather than verified opportunity.
- **Salary-relief player:** a cheap player who passes the configured role and evidence requirements. A salary-relief player is not automatically a punt.
- **Unknown:** a value not supplied or not validated. Unknown MUST NOT be coerced to zero.
- **Fade:** intentional underexposure or exclusion of a high-owned player, paired with a coherent alternate scoring path.
- **Archetype:** a named set of lineup-level constraints expressing a game script or leverage thesis.
- **Projection-only mode:** generation without validated ownership and field-model inputs. It can optimize scoring distributions but MUST NOT claim leverage or duplication accuracy.
- **Selection bank:** scenario draws used to generate and select lineups.
- **Evaluation bank:** independent scenario draws used to estimate out-of-sample lineup and portfolio performance.
- **Blocker:** condition that prevents export unless a specifically authorized manual override exists.
- **Warning:** condition that permits export after explicit acknowledgement.
- **Info:** non-blocking context.

## 5. Phase 0 — reconcile the implementation baseline

Before Phase 1 development:

1. Identify the commit and branch deployed to `nbadfs.vercel.app`.
2. Diff the deployed implementation against local `main` for NFL settings, optimizer logic, API contracts, and persisted run schemas.
3. Choose one canonical baseline and record it in the delivery ticket.
4. Add a build/version identifier to the NFL page and every generated run.
5. Capture current behavior in regression fixtures before modifying it.

### Acceptance criteria

- **P0-AC1:** The live footer or diagnostics panel shows commit SHA, build time, settings schema version, and optimizer version.
- **P0-AC2:** A checked-in fixture reproduces the current Showdown settings and a deterministic generated portfolio from a fixed player pool.
- **P0-AC3:** No Phase 1 code is merged until the deployed/local divergence is resolved or explicitly documented as intentional.

## 6. Cross-cutting architecture

The target flow is:

```text
point-in-time slate inputs
  -> eligibility and punt policy
  -> ownership/field-model capability assessment
  -> strategy and exposure plan
  -> legal candidate generation
  -> correlated scenario scoring
  -> portfolio selection with quotas and overlap controls
  -> independent evaluation
  -> pre-export QA
  -> immutable run/export audit
```

Every run MUST persist:

- source snapshot identifiers and as-of timestamps;
- projection, availability, ownership, field-model, and salary digests;
- complete normalized settings;
- optimizer and schema versions;
- random seeds for candidate generation, selection draws, and evaluation draws;
- eligible and excluded player sets with reason codes;
- lineup archetype and fade labels;
- constraint relaxations and overrides;
- QA results;
- generated, selected, and exported lineup identifiers.

No UI-only setting is permitted. Every control MUST map to a versioned API field, persisted run field, solver behavior, and test.

## 7. Shared data contracts

The following TypeScript shapes are illustrative contracts. Exact file placement is an implementation decision, but semantic fields and unknown-state behavior are required.

```ts
type EvidenceState = "confirmed" | "probable" | "unknown" | "stale";

type PuntReasonCode =
  | "ABSOLUTE_SALARY_BLOCK"
  | "ROLE_UNKNOWN"
  | "ROLE_UNRESOLVED"
  | "NO_PROJECTED_OPPORTUNITY"
  | "EVIDENCE_STALE"
  | "INACTIVE"
  | "MANUAL_EXCLUSION";

interface NflPlayerRoleEvidence {
  playerId: string;
  verifiedActive: boolean | null;
  availabilityState: EvidenceState;
  depthRole: string | null;
  roleConfidence: number | null;       // 0..1; null is unknown
  projectedOpportunities: number | null;
  opportunityUnit: "touch" | "target" | "attempt" | "kick" | "defense" | null;
  observedGameCount: number | null;
  sourceIds: string[];
  evidenceAsOf: string | null;
}

interface NflPuntPolicy {
  mode: "no_punts" | "role_qualified" | "custom";
  absoluteMinSalary: number;
  roleEvidenceRequiredBelowSalary: number;
  minimumRoleConfidence: number;
  minimumProjectedOpportunities: number | null;
  maxSalaryReliefPlayersPerLineup: number;
  allowlistedPlayerIds: string[];
  denylistedPlayerIds: string[];
}

interface ExposureRange {
  minPct: number | null;
  maxPct: number | null;
}

interface PlayerExposurePolicy {
  playerId: string;
  overall: ExposureRange;
  captain: ExposureRange;
  flex: ExposureRange;
  exactTargetMode: boolean;
}

type OwnershipCapability =
  | "validated"
  | "heuristic_uncalibrated"
  | "unavailable";

interface NflOwnershipInput {
  playerId: string;
  flexPct: number | null;
  captainPct: number | null;
  source: string | null;
  asOf: string | null;
}

interface ArchetypeQuota {
  archetypeId: string;
  minLineups: number;
  maxLineups: number;
  enabled: boolean;
}

interface SalaryConstructionPolicy {
  minSalaryUsed: number;
  maxSalaryUsed: number;
  minSalaryLeft: number;
  maxSalaryLeft: number;
  salaryLeftBands: Array<{
    min: number;
    max: number;
    minLineups: number;
    maxLineups: number;
  }>;
}
```

Percentages MUST be stored as decimals in APIs and normalized state. Counts are derived at solve time using `ceil(n * minPct)` for minimums and `floor(n * maxPct)` for maximums, consistent with the main NFL DFS specification.

## 8. Phase 1 — role-aware no-punt policy

### 8.1 Product behavior

Showdown MUST default to the `no_punts` preset:

- Players priced `$200–$800` are hard-blocked by default.
- Players below `$3,000` require verified active status, fresh role evidence, nonzero projected opportunity, and sufficient role confidence.
- Unknown or unresolved role state fails closed below the role-evidence threshold.
- A lineup may contain at most one salary-relief player by default.
- Users may allowlist a cheap player only by recording a reason. Example: “Brashard Smith active as returner/RB3 with a verified package and projected opportunities.” Salary alone is not a valid reason.
- The allowlist does not bypass inactive status or stale slate identity validation.

The default values MUST be configuration, not hard-coded optimizer constants. Admin or environment configuration MAY establish sport-wide defaults; a run stores the resolved values.

### 8.2 UI

Add a `Cheap-player policy` card above general optimizer settings:

- Preset selector: `No punts (recommended)`, `Role-qualified`, `Custom`.
- Read-only summary of the resolved rules.
- Cheap-player review table with salary, position, depth role, projected opportunity, evidence state/time, eligibility, and reason.
- `Allow for this run` action requiring a reason and displaying an override badge.
- Lineup-level counter showing salary-relief players used.

The player pool MUST display why each blocked player is ineligible. It MUST distinguish `salary block`, `role unknown`, `no opportunity`, `inactive`, and `stale evidence`.

### 8.3 Solver rules

- Ineligible players MUST be removed before candidate construction.
- The max salary-relief count MUST be a lineup constraint, not a post-generation filter.
- Captain eligibility MAY be stricter than Flex eligibility. In the first release, any player requiring a manual cheap-player override is Flex-only unless a separate Captain override is recorded.
- Locks cannot silently override punt rules. A conflicting lock produces an infeasibility explanation.

### 8.4 Acceptance criteria

- **P1-AC1:** A `$200` or `$400` unknown-role player appears blocked under the default preset and cannot enter a generated lineup.
- **P1-AC2:** A sub-`$3,000` player with validated opportunity can be marked role-qualified without a manual override.
- **P1-AC3:** An allowlisted cheap player stores user, timestamp, reason, input snapshot, and affected run.
- **P1-AC4:** No lineup exceeds the configured salary-relief maximum.
- **P1-AC5:** A locked but ineligible player produces a readable infeasibility error rather than zero lineups with a generic failure.

## 9. Phase 2 — ownership capability and missing-data safeguards

### 9.1 Validation

Ownership ingestion MUST support Captain and Flex ownership separately for Showdown. Validation MUST include:

- player identity and slate membership;
- value bounds of `0–100%`;
- source and as-of timestamp;
- coverage percentage for eligible players and projected fantasy-point mass;
- approximate slate totals: Captain ownership near `100%` and Flex ownership near `500%`, within configured tolerances;
- duplicate rows and missing high-projection players;
- staleness relative to lock.

Missing ownership remains `null`. It MUST NOT become `0`, and the objective MUST NOT reward it as low ownership.

### 9.2 Capability states

- `validated`: leverage, fade, field sampling, and model-based duplication features may run.
- `heuristic_uncalibrated`: estimates may be displayed and used only after explicit opt-in; every affected metric is labeled `Uncalibrated estimate`.
- `unavailable`: the optimizer operates in projection-only mode. Leverage, ownership-based fade quotas, and duplication estimates are disabled.

If only partial validated ownership exists, the run is projection-only unless coverage and mass thresholds are met. Initial release thresholds:

- at least 95% of eligible players covered;
- 99% of median-projection mass covered;
- both Captain and Flex totals pass validation.

Thresholds MUST be configurable and versioned.

### 9.3 UI

Add an ownership status panel showing capability, source, age, coverage, slot totals, validation errors, and enabled/disabled features. The Generate button remains available in projection-only mode, but its objective label must say `Projection-only GPP` and no leverage claims may appear.

### 9.4 Acceptance criteria

- **P2-AC1:** Null ownership receives neither a zero-ownership bonus nor a leverage label.
- **P2-AC2:** A malformed upload with Captain ownership totaling 35% fails validation.
- **P2-AC3:** Projection-only exports include an ownership-unavailable disclosure in their audit metadata.
- **P2-AC4:** Captain and Flex ownership are never combined into a single player percentage for scoring or reporting.
- **P2-AC5:** A heuristic estimate cannot be mistaken for a validated feed in the UI, API, logs, or export.

## 10. Phase 3 — role-specific exposure ranges

### 10.1 Required controls

Replace the single exposure target model with independent ranges:

- portfolio-wide minimum and maximum exposure;
- Captain minimum and maximum exposure;
- Flex minimum and maximum exposure;
- optional exact-target mode, off by default.

Global defaults remain available, but per-player rules override them. Captain and Flex counts must reconcile with overall counts.

### 10.2 Validation and solve behavior

- Reject ranges outside `0–100%` or where minimum exceeds maximum.
- Show derived lineup counts before generation.
- Detect impossible combinations before the solve, including excessive aggregate Captain minimums, insufficient Captain maximum capacity, and conflicts with locks/exclusions.
- Exact-target mode sets identical min/max counts only when the user explicitly enables it.
- Sequential generation MAY remain as a candidate-generation technique, but final exposure compliance MUST be enforced at portfolio-selection level.
- Captain exposure is counted independently from Flex exposure; overall exposure is their union.

### 10.3 UI

The exposure table MUST include `Overall min/max`, `CPT min/max`, and `Flex min/max`, plus projected counts for the requested portfolio size. The results view MUST show requested versus realized counts and identify the binding constraint.

### 10.4 Acceptance criteria

- **P3-AC1:** Setting a player to `0% CPT` and `20–50% Flex` never places that player at Captain.
- **P3-AC2:** A 20-lineup portfolio with a 12% minimum yields at least 3 appearances; a 12% maximum permits at most 2.
- **P3-AC3:** Exact-target behavior is impossible to activate accidentally through a single target field.
- **P3-AC4:** Infeasible exposure plans are rejected before lineup generation with the conflicting rules identified.
- **P3-AC5:** Saved runs preserve both requested and realized slot-specific exposures.

## 11. Phase 4 — fade and game-script archetypes

### 11.1 First-release archetypes

The system MUST provide these named Showdown archetypes:

1. `Standard ceiling` — no forced chalk fade; strongest evaluated lineups subject to portfolio constraints.
2. `Single-chalk fade` — fades one selected high-owned player and requires a declared beneficiary path.
3. `Double fade` — fades two selected high-owned players and requires a coherent alternate allocation of touchdowns, volume, or team scoring.
4. `Contrarian Captain` — uses a Captain below a configurable ownership ceiling with correlated teammates.
5. `Favorite onslaught` — favorite wins decisively; configurable 4-2 or 5-1 construction and scoring beneficiaries.
6. `Underdog comeback` — underdog lead or passing-volume response; supports underdog Captain and opponent bring-back rules.
7. `Low-scoring K/DST` — reduced touchdown environment with explicit kicker/defense requirements and offensive exclusions or caps.

Preset names describe strategy, not expected profitability.

### 11.2 Archetype contract

Every archetype MUST compile to explicit constraints:

- eligible Captain set or ownership band;
- team-count range;
- required and forbidden player relationships;
- required beneficiary groups;
- optional salary-left band;
- optional player or slot exposure rules;
- optional game-total or team-total scenario filter once Phase 7 is active;
- minimum and maximum portfolio quota.

A fade MUST NOT be represented only as `exclude player X`. It must specify at least one alternate scoring path, such as:

- replacement receiver or tight end gains targets/touchdowns;
- running game captures touchdowns and clock;
- opposing offense forces a different game environment;
- kicker/defense benefits from stalled drives or turnovers.

### 11.3 Correlation rules

The first release MUST support configurable Showdown rules including:

- WR/TE Captain paired with own quarterback unless explicitly waived by archetype;
- quarterback Captain with a configurable minimum number of pass catchers;
- running-back Captain may pair with own defense or kicker;
- defense Captain may require opposing offensive limits;
- kicker and defense combinations may be limited or required by archetype;
- maximum skill players opposing a defense;
- 5-1, 4-2, and 3-3 team construction controls.

Rules SHOULD default to warnings while empirical validation is incomplete, except logically contradictory combinations selected by a preset, which are hard constraints.

### 11.4 Portfolio planning UI

Add a `Portfolio plan` step before generation:

- choose archetypes and lineup quotas;
- choose fade candidates and beneficiaries;
- preview the number of requested lineups by archetype;
- validate quota feasibility;
- show the resulting Captain, team construction, and fade allocations.

Every selected lineup MUST have exactly one primary archetype label, may have secondary tags, and must list its faded players and beneficiary rules. No lineup can be counted toward two primary quotas.

### 11.5 Acceptance criteria

- **P4-AC1:** A five-lineup plan can request distinct archetypes and returns the requested count for each or a precise infeasibility report.
- **P4-AC2:** A single-chalk-fade lineup records the faded player and at least one satisfied beneficiary rule.
- **P4-AC3:** A lineup created only because exposure pressure omitted a popular player is not mislabeled as an intentional fade.
- **P4-AC4:** Archetype labels and constraints survive save, reload, export, and historical evaluation.
- **P4-AC5:** Every preset has deterministic unit fixtures proving its compiled constraints.

## 12. Phase 5 — salary-left distribution and duplication controls

### 12.1 Salary construction

Replace reliance on a single high minimum salary with explicit construction controls:

- minimum and maximum salary used;
- minimum and maximum salary left;
- portfolio quotas by salary-left band;
- archetype-specific salary bands;
- lineup overlap and exact-duplicate prevention.

The recommended Showdown preset SHOULD allow approximately `$45,000–$50,000` salary used, not force every lineup above `$49,000`. The exact default must be validated in Phase 8 before being promoted from `recommended experiment` to `validated default`.

Example 20-lineup experimental allocation:

| Salary left | Portfolio range |
|---|---:|
| `$0–$400` | 0–35% |
| `$500–$1,400` | 25–70% |
| `$1,500–$3,000` | 10–50% |
| `>$3,000` | 0–20% |

These are starting hypotheses, not universal winning rules.

### 12.2 Duplication

Two modes are required:

- **Model-based:** estimated using validated slot ownership plus a field-lineup model or sampled contest field.
- **Heuristic:** based on salary used, Captain popularity, aggregate construction popularity, and other declared features. It MUST be labeled uncalibrated and MUST NOT be shown as an expected duplicate count.

The product MUST NOT estimate full-lineup probability by naïvely multiplying marginal player ownership values. Model-based duplication must operate on complete lineup probabilities or sampled field frequency.

Portfolio selection MAY penalize expected duplicate concentration, but it must not discard high-upside lineups solely for leaving too little salary without a validated rule. Users can set a maximum model-based duplicate estimate only when the ownership capability is `validated`.

### 12.3 Acceptance criteria

- **P5-AC1:** Users can intentionally construct valid lineups leaving `$1,500+` without weakening the min-player role rules.
- **P5-AC2:** Salary-band quotas are enforced across the selected portfolio.
- **P5-AC3:** No label says `expected duplicates` unless it comes from a validated field model.
- **P5-AC4:** Exact duplicate lineups are impossible, and configured overlap limits are enforced.
- **P5-AC5:** Results show salary used, salary left, salary band, and duplication capability for every lineup.

## 13. Phase 6 — pre-export portfolio QA

### 13.1 QA framework

Export MUST run a versioned QA ruleset and display one decision: `Ready`, `Ready with warnings`, or `Blocked`. The report must cover:

| Check | Default severity | Required behavior |
|---|---|---|
| Illegal roster or salary | Blocker | Never overridable |
| Ineligible punt or inactive player | Blocker | Never overridable for inactive; punt override only through Phase 1 flow |
| Unknown role below threshold | Blocker | Requires recorded player override |
| Stale projections or newer run available | Warning; blocker after configured age | Show timestamps and replacement run |
| Availability freshness/coverage | Blocker | Follow canonical availability policy |
| Ownership unavailable | Info in projection-only mode | Disable leverage/duplication claims |
| Ownership invalid but leverage enabled | Blocker | Disable feature or repair input |
| Exposure range violation | Blocker | Identify player, slot, requested and realized counts |
| Archetype quota violation | Blocker | Identify missing/excess counts |
| Fade without beneficiary | Blocker | Fix lineup or remove fade label |
| Captain concentration | Warning by default | Show HHI, top share, and configured cap |
| Salary-left concentration | Warning | Compare with plan bands |
| Excess model-based duplication | Warning or configured blocker | Only available with validated field model |
| Excess lineup overlap | Blocker | Identify lineup pairs |
| Correlation-rule violation | Preset-defined | Identify rule and players |
| Selection/evaluation bank collision | Blocker in Phase 7 | Seeds/digests must differ |

### 13.2 Overrides

- Rules declare whether they are overridable.
- Overrides require a reason and persist user, time, ruleset version, run ID, and before/after state.
- Export files include a sidecar JSON audit or embedded run reference.
- Re-running generation invalidates prior QA and acknowledgements.

### 13.3 QA UI

The report MUST provide:

- summary counts by severity;
- direct links to the affected lineup or setting;
- portfolio distributions for Captain, Flex, team construction, salary left, archetype, and fades;
- ownership and source capability banner;
- `Fix` actions where deterministic;
- export disabled while a blocker remains.

### 13.4 Acceptance criteria

- **P6-AC1:** A portfolio containing an unapproved `$400` player cannot be exported.
- **P6-AC2:** Any exposure or archetype shortfall names the exact unsatisfied constraint.
- **P6-AC3:** Changing a setting after QA forces QA to run again.
- **P6-AC4:** Every exported portfolio can reconstruct the QA report and overrides from persisted data.
- **P6-AC5:** Projection-only portfolios can export, but their audit cannot claim ownership leverage or expected duplication.

## 14. Phase 7 — correlated scenario scoring and portfolio selection

### 14.1 Replace additive upper-tail scoring

The optimizer MUST stop describing the sum of player P90 values as lineup P90 or lineup ceiling. During migration it MAY retain the legacy score under the label `Additive player-tail score (legacy)` for controlled comparison only.

The existing Scenario Lab must become the scoring service for candidates:

1. Generate a sufficiently broad legal candidate set under eligibility, locks, and archetype constraints.
2. Evaluate every candidate on the same aligned selection scenarios.
3. Calculate lineup outcome distributions and contest-relative metrics.
4. Select a portfolio using marginal contribution, exposure/archetype quotas, overlap, and duplication controls.
5. Re-evaluate the selected portfolio on an independent evaluation bank.

### 14.2 Scenario requirements

Showdown scenarios MUST preserve at least:

- game total and team scoring dependence;
- pass/rush allocation;
- quarterback–receiver and quarterback–tight-end dependence;
- touchdown allocation and multi-touchdown upside;
- running-back volume, efficiency, and receiving relationships;
- kicker opportunity conditional on drives and touchdowns;
- defense scoring conditional on sacks, turnovers, points allowed, and opponent play;
- player availability and role states;
- Captain scoring as a multiplier of the same simulated underlying player result, not a separate independent projection.

All candidates in a run MUST use common random scenarios so differences are attributable to lineup construction rather than sampling noise.

### 14.3 Candidate metrics

At minimum calculate:

- mean, median, P75, P90, P95, and P99 lineup score;
- probability of exceeding configurable score thresholds;
- probability of top 10%, top 1%, and top 0.1% finish in a sampled field;
- probability of first place or first-place tie where field and payout inputs support it;
- expected payout and ROI only when contest field, entry fee, payout table, and tie splitting are valid;
- scenario coverage and marginal contribution to portfolio success events.

When required inputs are unavailable, omit the metric rather than substituting a misleading proxy.

### 14.4 Portfolio objective

The initial supported objectives are:

- maximize probability at least one lineup reaches top 1%;
- maximize probability at least one lineup reaches top 0.1%;
- maximize first-place probability when a validated field exists;
- maximize expected payout when full contest inputs exist;
- blended utility with explicit, displayed weights.

Selection MUST account for redundant lineup outcomes. Two lineups with similar individual upside but nearly identical success scenarios should contribute less together than two lineups covering different plausible game states.

### 14.5 Reproducibility and failure behavior

- Selection and evaluation banks MUST use separate seeds and digests.
- Run metadata MUST record draw count, model version, calibration version, and scenario input digest.
- If scenario generation fails, the system MUST not silently fall back to additive P90. It may offer a clearly labeled legacy run as a separate user action.
- UI confidence intervals or Monte Carlo standard errors SHOULD be shown for rare-event probabilities.

### 14.6 Acceptance criteria

- **P7-AC1:** Re-running the same snapshot, settings, versions, and seeds reproduces selected lineups and metrics.
- **P7-AC2:** WR Captain and quarterback outcomes show positive dependence in scenario diagnostics where the football model implies it.
- **P7-AC3:** Captain and Flex versions of a player use the same simulated fantasy score before the Captain multiplier.
- **P7-AC4:** The selected portfolio is evaluated on draws not used for selection.
- **P7-AC5:** No user-facing metric calls a sum of marginal player quantiles a lineup quantile.
- **P7-AC6:** If ownership/field data is unavailable, top-percent and payout metrics that require a field are absent, not guessed.

## 15. Phase 8 — point-in-time contest backtesting

### 15.1 Historical dataset

Backtests MUST use immutable, point-in-time slate packages containing:

- salaries and contest roster rules available before lock;
- projections, roles, injury/availability knowledge, odds, and ownership estimates with original as-of times;
- actual player results;
- actual contest ownership split by Captain/Flex where available;
- actual field lineups, entry fee, payout table, and tie rules where licensed and available;
- contest identifier, field size, max entries, and slate type;
- source and transformation digests.

Post-lock facts MUST NOT enter generation inputs. Missing historical facts remain missing and are reflected in capability state.

### 15.2 Evaluation design

Use walk-forward evaluation by season/week. Hyperparameters and default presets are selected only on prior windows, then frozen for the next holdout window. Results must be clustered by slate or contest; treating lineups from the same slate as independent observations is prohibited.

Compare at least:

1. current production baseline;
2. legacy additive-tail plus randomness;
3. no-punt policy only;
4. archetype and salary distribution portfolio;
5. correlated scenario selection;
6. correlated selection plus validated field/duplication model.

Pre-register primary metrics and decision thresholds before examining holdout results.

### 15.3 Metrics

- legality and export-readiness rate;
- player and lineup distribution calibration;
- candidate recall for actual top-1%, top-0.1%, and winning lineups when reconstructable;
- portfolio probability calibration for top-1%, top-0.1%, and first place;
- realized top-10%, top-1%, top-0.1%, cash, and first-place rates;
- net payout and ROI only for contests with complete field, fee, payout, and tie data;
- ownership calibration by slot and price/role bucket;
- duplication prediction error and calibration;
- average and tail salary-left distribution;
- portfolio overlap, Captain concentration, and archetype representation;
- punt utilization and outcomes by eligibility reason.

Report confidence intervals and slate-level result distributions. A few large wins must not be presented without the corresponding downside and uncertainty.

### 15.4 Promotion gates

A new default may be promoted only when:

- it passes all correctness and leakage audits;
- it does not materially degrade legal-lineup generation or candidate recall;
- its primary holdout metric meets the pre-registered threshold over a meaningful slate count;
- sensitivity analysis does not show the result depends on one slate, one contest, or one parameter choice;
- observed limitations are documented in the release note.

If full contest data is unavailable, the release may improve construction quality but MUST NOT claim improved ROI.

### 15.5 Acceptance criteria

- **P8-AC1:** Every backtest row can be traced to a pre-lock input snapshot and code/model version.
- **P8-AC2:** A leakage test fails if a source timestamp is after contest lock.
- **P8-AC3:** The report separates contests with complete payout/field data from projection-only historical slates.
- **P8-AC4:** Model selection and reported holdout evaluation use disjoint time periods.
- **P8-AC5:** Default changes cite the experiment, sample, uncertainty interval, and promotion decision.

## 16. API and persistence requirements

Introduce a versioned request such as `nfl_gpp_portfolio_v1`. The normalized generation request must contain:

```ts
interface NflGppPortfolioRequestV1 {
  schemaVersion: "nfl_gpp_portfolio_v1";
  slateId: string;
  format: "showdown" | "classic";
  lineupCount: number;
  objective: string;
  puntPolicy: NflPuntPolicy;
  ownershipCapability: OwnershipCapability;
  exposurePolicies: PlayerExposurePolicy[];
  archetypeQuotas: ArchetypeQuota[];
  salaryPolicy: SalaryConstructionPolicy;
  maxPairwiseOverlap: number;
  locks: Array<{ playerId: string; slot?: "CPT" | "FLEX" }>;
  exclusions: string[];
  sourceSnapshotIds: string[];
  seeds: {
    candidateGeneration: number;
    selectionScenarios: number;
    evaluationScenarios: number;
  };
}
```

The response must include:

- resolved capability state;
- normalized request and constraint summary;
- candidate and selected-lineup counts;
- selected lineups with slot, salary, archetype, fade, beneficiary, scenario, ownership, and duplication fields where available;
- realized portfolio distributions;
- infeasibility diagnostics or relaxation log;
- QA result;
- immutable run ID and version/digest metadata.

Legacy saved runs remain readable under their original schema and labels. They MUST NOT be silently reinterpreted using new defaults. Editing a legacy run creates a new versioned run.

## 17. UI workflow

The NFL optimizer should become a six-step workflow while retaining a compact expert view:

1. **Data readiness:** projections, availability, roles, ownership, and build version.
2. **Player eligibility:** no-punt policy and cheap-player review.
3. **Portfolio plan:** lineup count, archetype quotas, fades, beneficiaries, and team constructions.
4. **Exposure and construction:** overall/Captain/Flex ranges, salary-left bands, and overlap.
5. **Generate and compare:** candidate/portfolio metrics, distributions, and scenario explanations.
6. **QA and export:** blockers, warnings, overrides, audit, and download.

The UI MUST retain advanced transparency:

- always show which features are disabled and why;
- separate facts, model estimates, heuristics, and user assumptions visually;
- show normalized count equivalents beside percentages;
- make every lineup's strategy understandable without inspecting solver logs;
- never hide an infeasible constraint by silently relaxing it.

## 18. Testing strategy

### 18.1 Unit tests

- punt classification and reason precedence;
- unknown-state propagation;
- ownership validation and capability resolution;
- percentage-to-count rounding;
- exposure reconciliation;
- archetype compiler outputs;
- salary-band counting;
- QA severity and override policy;
- scenario metric calculations and Captain multiplier;
- seed and digest reproducibility.

### 18.2 Property and solver tests

- every returned lineup is legal;
- no ineligible player appears;
- all hard ranges and quotas are satisfied;
- impossible requests produce diagnostics, never partial unlabeled success;
- permuting input row order does not change deterministic output;
- selection and evaluation scenario IDs never overlap;
- portfolio metrics remain within mathematical bounds.

### 18.3 Integration fixtures

Maintain small synthetic Showdown slates covering:

- a `$200` inactive punt;
- a `$400` unknown-role player;
- a `$1,000` verified package player;
- missing ownership for a star;
- separate Captain and Flex ownership;
- infeasible Captain minimums;
- single- and double-fade beneficiary paths;
- low-scoring K/DST construction;
- duplicated and diversified portfolio candidates;
- stale projections and a newer run.

### 18.4 End-to-end tests

- configure five distinct lineups, generate, inspect strategy labels, run QA, export, reload, and reproduce;
- verify projection-only mode from missing ownership;
- verify export blockage from an unapproved punt;
- verify a valid user override is persisted and visible;
- verify a Phase 7 run does not fall back silently when scenarios fail.

## 19. Observability and operating metrics

Track by optimizer version and slate type:

- generation success, infeasibility, timeout, and fallback rates;
- count of blocked punts, role-qualified salary relief, and manual overrides;
- ownership capability distribution;
- requested versus realized exposure and archetype quotas;
- salary-left and Captain concentration distributions;
- QA blocker/warning frequency;
- candidate count, scenario runtime, portfolio-selection runtime, and Monte Carlo error;
- export rate and reason for abandonment;
- backtest and live post-lock calibration metrics when outcomes arrive.

Logs MUST use stable reason codes and run IDs. They MUST NOT contain uploaded secrets or unnecessary personal information.

## 20. Delivery order and release gates

| Release | Scope | Gate to advance |
|---|---|---|
| `R0` | Baseline reconciliation and version display | P0 acceptance complete |
| `R1` | No-punt policy | No blocked player can enter or export |
| `R2` | Ownership capability safeguards | Unknown ownership cannot create leverage |
| `R3` | Slot-specific exposure ranges | Portfolio-level enforcement and infeasibility diagnostics pass |
| `R4` | Archetypes, fades, beneficiaries | Every lineup is intentionally labeled and quotas are auditable |
| `R5` | Salary distribution and duplication modes | No misleading duplicate estimate; bands enforced |
| `R6` | Unified pre-export QA | All blocker paths and audits verified |
| `R7` | Correlated scenario scoring and selection | Independent-bank reproducibility and calibration checks pass |
| `R8` | Historical contest backtest and default promotion | Point-in-time holdout report and promotion decision published |

Each release SHOULD be feature-flagged and shadow-tested on saved slate fixtures. `R1–R6` may ship with projection-only scoring while `R7` is developed, provided legacy additive-tail metrics are accurately labeled and no unsupported field-relative claims are made.

## 21. Definition of done

The program is complete when all of the following are true:

- Cheap players are admitted by verified role, not merely by salary, and `$200–$800` punts are blocked by default.
- Missing ownership is represented as unknown and disables unsupported leverage/duplication behavior.
- Overall, Captain, and Flex exposures have independent min/max constraints.
- Fade lineups and game scripts are declared before selection, carry beneficiary logic, and meet portfolio quotas.
- Salary-left diversity is intentional and visible; duplication claims are capability-gated.
- Export is protected by a reproducible QA report with explicit override rules.
- Lineups are scored with coherent joint scenarios and selected for marginal portfolio contribution using independent evaluation draws.
- Historical claims use point-in-time inputs, actual contest data where available, walk-forward holdouts, and uncertainty reporting.
- The product communicates uncertainty and never describes a construction rule or modeled edge as a guarantee of winning a GPP.

## 22. Initial product defaults pending backtest

These defaults are approved for experimentation, not yet as empirically validated winning settings:

- `No punts` preset enabled.
- Absolute minimum player salary `$1,000`; `$200–$800` excluded.
- Fresh role evidence required below `$3,000`.
- Maximum one role-qualified salary-relief player per lineup.
- Minimum lineup salary `$45,000`, with salary-left portfolio bands rather than a `$49,000` floor.
- Overall player maximum exposure `60%` unless overridden.
- Minimum two unique players between Showdown lineups.
- Explicit archetype quotas for multi-lineup generation.
- Ownership-dependent features off unless validated ownership is present.
- Additive player-tail score labeled legacy until Phase 7 replaces it.

Phase 8 determines whether these values become defaults, are revised, or remain optional presets.

---

## Implementation record (added 2026-09-21, not part of the original 2026-09-20 spec)

R0–R8 were implemented phase by phase on the `nfl-gpp-portfolio` branch and
merged to `main` via [PR #219](https://github.com/themvf/NBA_DFS_2/pull/219).
Every acceptance criterion P0-AC1…P8-AC5 is encoded in a checked-in test suite
(`web/scripts/test-nfl-gpp-*.ts`, run via `npm run test:nfl-gpp-<phase>`).

A post-implementation review fixed nine integration findings before merge
(commit `ade1fa3`): the exposure-floor regression, the punt policy blocking all
sub-$3k players on feedless slates, ownership validation that could never
validate (LineStar is now a declared heuristic with an explicit labeled opt-in
for leverage), missing archetype configuration UI, cross-run QA state mixing,
archetype team-range binding, archetype restore on reload, all-zero captain-max
detection, and a baseline test that now pins a frozen main-parity fingerprint.

Deliberately open, per the spec's own honesty rules: R7's live wiring awaits
model-generated scenario banks, and R8's ROI metrics await licensed contest
field/payout data — both engines are capability-gated and refuse to fabricate
results (P7-AC6, P8-AC3).
