# CFB Market Context Engine and Signal Research — Specification

Version: `cfb-market-context-v2`  
Contract revision: `3` — identity, quote grain, policy dependencies, publication scopes, and study/erasure persistence specified  
Date: 2026-09-23  
Status: Proposed implementation contract; research/paper only  
Scope: Pregame CFB spreads, totals, moneylines, contextual evidence, and prospective evaluation

Execution addendum: September 25, 2026 — Section 13.1 defines the remaining delivery order and evidence requirements. This clarification retains contract revision 3 and does not amend any frozen study configuration.

## 1. Objective and authority

Build a reusable CFB market-context engine that turns preserved observations into reproducible context for forecasts, pick'em, survivor, research, alerts, postgame evaluation, exports, and the Vercel interface. The interface is one consumer; it does not define metrics or determine qualification.

Separate an observed movement, an explanatory hypothesis, a predictive estimate, and permission to affect a decision. A signal firing proves only that its documented conditions were met.

This document and its [normative implementation appendices](cfb-market-context-engine-implementation-contract.md) govern new implementation. Appendix A explicitly classifies legacy rules as preserved, superseded, or out of scope. The appendices control implementation details where this document summarizes them. Historical signal definitions, origins, deduplication, grades, and qualification are not rewritten. Preserve v1 for replay.

The review supplied with this specification is motivation, not a verified benchmark artifact. Its reported results must be reproduced from a pinned ledger before being published as validated measurements. No trigger family is promoted by this specification.

### Non-goals

- Automated wagering, bankroll allocation, or real-money recommendations.
- A full CFB play simulator or compulsory migration of every NFL module.
- Importing NFL qualification, thresholds, or feature definitions into CFB without evaluation.
- Paid historical expansion or a new prop collector as a prerequisite for this work.
- Retrospectively relabeling reconstructed signals as prospective observations.

## 2. Architecture

| Layer | Responsibility |
|---|---|
| Evidence | Preserve provider payloads, quote timestamps, identities, acquisition records, source revisions, and digests. |
| Canonical facts | Resolve games, teams, competitions, schedules, quotes, final scores, and source corrections. |
| Context | Publish versioned movement, coverage, continuity, matchup, and historical measurements with provenance. |
| Forecast/research | Estimate outcomes or future movement from eligible context; record exact model and input artifacts. |
| Consumer policy | Authorize descriptive, predictive, scenario, or decision use with freshness and fallback rules. |
| Presentation | Render stored evidence, calculations, qualification, and limitations. |

Reuse shared infrastructure and identifiers where compatible. CFB definitions and qualification remain sport-specific. Python builders and thin Python/TypeScript readers are sufficient; a separate service is not required.

Inventory consumers before finalizing readers. For each, record subject grain, target, time boundary, existing local calculations, market dependence, fallback, and whether context changes an output. Migrate consumers incrementally; do not silently replace their current inputs.

### 2.1 CollegeFootballData is a first-class evidence source

The dedicated CollegeFootballData (CFBD) integration is part of this engine, not an optional dashboard enrichment. Combine its football evidence with timestamped sportsbook evidence through canonical game/team identities and compatible as-of snapshots. Preserve the distinction between football-derived context and market-derived context so consumers can evaluate their incremental contribution.

The following map describes repository integration paths, not a claim that every dataset is populated, current, or approved for prediction. Phase 0 must verify database coverage, job execution, source entitlement, temporal provenance, and actual downstream consumers.

| CFBD evidence / existing path | Intended context | Required qualification |
|---|---|---|
| Teams, schedules, games, media — `ingest/cfb_schedule.py` | Identity, competition, kickoff, venue/home-neutral setting, rest, scheduling and results | Canonical mapping, schedule revisions, field coverage; results unavailable to pregame inputs. |
| Historical games and reference lines — `ingest/cfb_history.py` | Historical cohorts, opponent/results context, reference-price comparisons | Historical reference lines are not an intraday odds history or an executable quote. |
| Plays and drives — `ingest/cfb_plays.py` | Possession volume, precisely defined pace, field position, down/distance, explosive and scoring-opportunity context where supported | Explicit eligible-play/drive definitions, missingness, source revisions, season/rule consistency, and coverage audit. |
| Roster snapshots — `ingest/cfb_rosters.py` | Roster membership and position-group continuity | Membership is not a depth-chart role, starting assignment, injury status, or game availability. |
| Returning-production captures — `ingest/cfb_rosters.py` | Source-supported returning production | Preserve source meaning; do not substitute roster counts for production-weighted continuity or infer defensive production from offensive fields. |
| Portal and talent captures — `ingest/cfb_rosters.py` | Transfer movement and talent context | Point-in-time snapshots and field coverage; transfer count/rating is not an estimated on-field contribution. |
| Coach captures — `ingest/cfb_rosters.py` | Supported coaching-regime continuity | Existing head-coach records do not establish coordinator identity or play-caller continuity. |
| Existing feature builder — `model/cfb_team_features.py` | Reusable team-history and opponent-adjusted estimates | Preserve model/weighting versions, fit only eligible history, and expose raw versus estimated values. |

Ratings, advanced statistics, and additional player data may be added after an endpoint/field audit establishes actual access, source meaning, coverage, cost, and availability. API capability does not establish that the repository currently ingests or consumes a field. Missing injuries, depth charts, coordinator records, or defensive returning-production evidence remain explicit gaps until verified integrations supply them.

A past play's event date does not prove that today's fetched revision was available on that date. Historical PBP remains useful for retrospective research, but prospective replay requires preserved observations/revisions. The stronger no-as-of-ambiguity wording currently present in the play-backfill module must not override this contract.

Track each source field through `accessible → captured → normalized → contextualized → consumed → qualified`, with coverage and a named consumer at every applicable stage. This exposes valuable stored inputs that currently affect no calculation.

### 2.2 Football-context integration acceptance

In addition to the market-movement slice, deliver one bounded CFBD context integration before declaring the reusable engine complete. Select the definition after the source audit: completed-game possession volume or a precisely defined roster-continuity measurement are candidates, not automatically approved features.

The same immutable CFBD-derived snapshot must be consumed by a Python shadow study and displayed/exported through shared readers. Compare a frozen market-only baseline against the same baseline plus the football context on an untouched prospective cohort. Freeze both recipes before enrollment; hold other inputs and model settings fixed, or preregister every necessary difference and its limitation for attributing improvement to the context. Report lack of improvement as a valid result; descriptive reuse does not require proving predictive value, but decision use does.

## 3. Evidence, identity, and temporal contract

Preserve `event_occurred_at` where meaningful, `source_published_at` where supplied, `bookmaker_updated_at`, and `system_observed_at`. Record acquisition start/end separately when needed. Do not invent publication timestamps from acquisition times.

A real-system replay may use only evidence observed by its exact `as_of_at`. Reconstructed historical research is a different mode and must identify its availability assumptions. Training, ratings, transformations, imputation, selection, calibration, and explanation reference data follow the same temporal boundary.

Identity requirements:

- Canonical event and team IDs; provider IDs and original names remain preserved.
- Effective-dated competition membership and schedule revisions.
- Market, selection, signed line, book, price, and settlement-rule identity.
- Exact source-record revision and raw artifact reference.
- Unknown classification remains unknown; ambiguous matching is rejected with a reason.

Represent quote sides as observations. A paired market references compatible opposite-side observations at the same book, line, market, and defined time tolerance. Never pair an old under with a fresh over without disclosing the mismatch and applying eligibility rules.

Preserve permitted evidence before transformation under Appendix F's entitlement and retention contract. A digest verifies an artifact but does not replace retaining it. Normalized-only and legacy evidence remain explicitly classified; raw retention is not assumed permissible for any provider. Repeated ingestion is idempotent; later corrections append revisions.

## 4. Context and reader contract

A context snapshot contains:

```text
context_snapshot_id
definition_id + definition_version
sport, subject_type, subject_id, target_event_id
as_of_at, created_at, measurement_window
value, unit, numerator, denominator
coverage, included_count, excluded_count, exclusion_reasons
observed_or_estimated, estimation_artifact_id, uncertainty_method
source_release_ids, source_record_ids, transformation_artifact_id
evidence_origin, availability_basis, market_dependency
quality_flags, input_digest
```

Fields not applicable to a definition are explicitly null with documented semantics. Unknown, observed zero, not applicable, unsupported, and temporarily unavailable are distinct states. Estimated or shrunk values do not overwrite raw measurements.

Every definition declares eligibility, aggregation, signs, units, windowing, missingness, freshness, and invalidation rules. Exploratory definitions remain in a research namespace until assigned a maintainer role and reviewed for definition stability, leakage, and coverage.

Readers support two operations:

1. **Pinned read:** return the exact saved snapshot and manifest; never substitute corrected inputs.
2. **Current eligible read:** resolve compatible inputs under a registered consumer policy and return the resolved manifest, freshness decision, and fallback used.

Policies are centrally registered and versioned. Callers cannot grant themselves permission. Qualification is keyed by definition/model version, consumer, use case, market, and applicable cohort; it is not a universal maturity rank.

Initial permissions allow descriptive use and explicitly registered shadow research. Production decision use is denied. A rejected dependency may invoke a separately approved fallback, recorded in the consumer run. Every production integration must use the policy-aware reader or an equivalently validated published artifact.

## 5. Publication and invalidation

Publish evidence, context, forecasts, and evaluations independently. A consumer manifest pins their compatible releases, policy version, code/configuration artifacts, target, and as-of boundary.

Build and validate before atomically advancing a consumer release pointer. Independent `MAX(created_at)` queries are not a compatibility contract. A failed market collector must not prevent publication of unrelated historical context.

Track dependency references so a schedule, quote, or availability correction can invalidate affected current outputs. Frozen outputs remain replayable. Record revocation/invalidation separately from immutable payloads. Readers expose why the last valid release is being served and whether it remains eligible.

## 6. Signal policy and CFB definitions

Preserve existing v1 thresholds as frozen hypotheses. Any threshold, book universe, timing, pairing, or deduplication change creates a new signal version and a new prospective evaluation cohort.

| Family | Initial treatment |
|---|---|
| Spread steam, reversal, reference-led, total walking | Continue prospective paper collection; no ranking or edge claim. |
| Spread walking, key cross | Descriptive observations; continue measurements for diagnosis. |
| Total steam | Collect; insufficient evidence is not a positive finding. |
| Moneyline value, steam, walking, late move | Observation-only; block actionable interpretation pending audit and fresh qualification. |
| Price pressure, book disagreement, convergence | Collect eligible opportunities and rejection reasons before changing thresholds. |

Observation-only does not stop raw collection or hypothetical grading. The same no-decision policy applies to all unqualified families.

Do not treat 3, 7, 10, or 14 as economically validated CFB key numbers merely because existing detectors watch them. Freeze the exact crossing definition, including direction, equality, skipped values, and home/selection sign convention.

For consensus movement, retain the book set at each endpoint and measure same-book changes on their intersection. A consensus shift caused solely by book membership changes must not be classified as coordinated movement. Book count does not prove independent information sources or betting liquidity.

Reference-led observations preserve lead/lag timestamps and distinguish ordering resolution from true leadership. If polling cannot establish order, report it as unknown. Freeze the quote available at detection and later latency checkpoints; retail follow-through may eliminate any available discrepancy.

All surfaced CFB language, including backend logs and exports, must use neutral research terminology. Remove unqualified “informed money,” “sharp,” and “BET-GRADE VALUE” assertions from CFB paths without changing other sports implicitly.

## 7. Capture and detector health

Continue existing prospective capture under existing quota guards while development proceeds. Do not wait for the engine rewrite or silently increase paid acquisition. Historical acquisitions require their own coverage and cost plan and remain historical-origin evidence.

Report capture acceptance, missing mappings, quote-free responses, scheduler delay, provider failures, stale bookmaker updates, quota deferrals, and rejected post-boundary observations. Preserve scheduled-boundary close methodology; do not present it as verified actual kickoff.

For every detector/version, persist a funnel:

```text
scheduled events → captured markets → eligible comparisons
→ threshold matches → dedupe suppressions → persisted observations
→ downstream publications → economic settlements / CLV evaluations
```

Record counts and machine-readable rejection reasons for each stage. Include examples of rejected opportunities without storing redundant raw payload copies.

A zero-fire detector must be distinguishable from absent source coverage, impossible conditions, or a publication defect. Diagnose missed triggers as capture, eligibility, detection, persistence, or publication failures. Replayed findings never backfill the prospective cohort.

Measure health for triggered propositions and relevant execution/reference books, not only all games. Show missingness by matchup class, market, lead time, and family.

## 8. Economic settlement and CLV

Create a canonical, versioned economic view over existing grades and legacy moneyline economics. Do not assume `pnl_units` is the sole source. Apply Appendix D's source precedence, correction-chain selection, conflict quarantine, and unit normalization. Preserve input grade references.

Economic settlement and CLV availability are independent:

- A valid entry quote and official outcome can settle hypothetical P&L without a verified close.
- A missing close yields null CLV, never zero, and does not erase an economic loss.
- A missing execution quote permits a descriptive observation but no invented P&L.
- Corrections append grades; reports pin one explicit grade revision per observation.

For one-unit hypothetical stakes at decimal price `d`, profit is `d - 1` for a win, `-1` for a loss, and `0` for a push. Voids return the stake and are reported separately. Primary ROI uses total profit divided by settled non-void stake, including pushes; pending and void stakes are excluded and displayed. Reports state the denominator and never call a mean over a filtered subset the full-cohort ROI.

For probability models, expected profit per unit is `p_win * (d - 1) - p_loss`; push probability remains explicit. Paired no-vig probabilities use a named method and are distinct from offered-price break-even thresholds.

Keep CLV dimensions separate:

| Metric | Contract |
|---|---|
| Spread line CLV | Selection handicap at entry minus selection handicap at close; positive is a better entry. |
| Total line CLV | Over: close total minus entry total. Under: entry total minus close total. |
| Same-line price comparison | Same selection, line, book or explicitly named benchmark, and comparable settlement rules; preserve both prices. |
| Probability CLV | Closing fair probability minus entry fair probability for the same proposition, in percentage points, using a fixed no-vig method. |

Name execution-versus-reference price discrepancies separately from temporal probability CLV. Never average line points with probability points, or spread and total point CLV into a promotion metric. A line-to-probability conversion requires its own validated, versioned model.

Show mean, median, better/equal/worse fractions, missing count, and distribution by family and market. Neither zero median nor a small positive mean alone establishes economic value or its absence.

Settlement rules identify overtime, cancellations, postponements, and applicable market exceptions. Flag overtime outcomes for sensitivity analysis without removing unfavorable outcomes after inspection.

## 9. Moneyline audit

Before reconsidering any decision use:

1. Reconcile all signal counts to won/lost/push/void/pending/missing-entry outcomes, including the supplied report's 29 observations versus 28 W/L settlements.
2. Verify selection identity, decimal/American conversion, payout units, timestamps, and quote freshness.
3. Audit paired reference quotes and no-vig methodology.
4. Inspect entry probability ranges, matchup classes, book support, and latency availability.
5. Evaluate calibration only for actual probability estimates; a movement observation is not a probability forecast.
6. Freeze any revised calibration or eligibility rule before new prospective testing.

Favorite–longshot calibration error is a hypothesis until these checks support it. A price cap alone is not validation. Historical losses remain visible after a revised detector is introduced.

## 10. Evaluation design

Persist study ID, exact versions, input/grade manifests, cohort rules, primary metric, economically meaningful effect, date windows, clustering method, candidate-selection procedure, execution assumptions, and review schedule before an untouched window starts.

Report both observation-level diagnostics and a separately defined paper-decision cohort. Define the latter's deduplication/selection policy in advance. Multiple families on one game do not create independent evidence or automatically imply multiple stakes.

Report unique games, event dates, proposition keys, signal overlaps, opposing selections, and hypothetical correlated exposure. Proposition identity includes event, market, selection, line, and settlement rules; cross-line and cross-family observations remain correlated at game level.

Required analysis:

- Market/family-specific economic and CLV summaries with explicit denominators.
- Game-clustered and game-date-clustered uncertainty; prespecify the primary method.
- Leave-one-date-out sensitivity and profit/CLV concentration by game, date, team, and conference.
- Small-cluster warnings: eight dates do not support precise interpretation of bootstrap endpoints.
- Multiple-candidate selection controls; the best observed family is not automatically the best future candidate.
- A comparison cohort whose matching algorithm, calipers, replacement policy, and unmatched-row treatment are frozen before enrollment, or an explicit preregistered decision to omit that comparison. It cannot be introduced selectively after outcomes are inspected.
- Latency, stale-quote exclusion, and adverse-price sensitivity with assumptions fixed before evaluation.

Predefine a limited diagnostic segmentation: FBS–FBS/FBS–FCS/other or unknown; competitive/mismatch line bands; early/near-kickoff horizons; coverage tiers; and early/later-season regimes. Exact boundaries belong in the frozen study configuration, not outcome-driven filters. Unknown cohorts stay visible. Exploratory subgroup findings require a new untouched test before qualification.

The previously inspected 2025 favorite-range hypothesis and already reviewed 2026 observations are research evidence, not a new candidate's untouched confirmation sample.

## 11. Qualification and consumer gates

Maintain separate axes:

- Research state: exploratory, prospective-paper, paper-qualified, retired.
- Data state: complete, partial, stale, unresolved, invalidated.
- Consumer permission: descriptive, shadow-predictive, decision-denied, or an explicit future scoped approval.

Paper qualification requires a preregistered adequately powered cohort, adequate capture/identity health, meaningful primary-metric improvement with the specified uncertainty criterion, acceptable execution sensitivity, and no material unresolved leakage or settlement defect. No universal observation-count threshold is invented here.

The study configuration must specify numeric health floors, minimum independent clusters, precision/power target, effect size, boundaries, and multiplicity handling before enrollment. Missing configuration blocks qualification, not collection. Use pilot variance for planning, excluding that pilot from confirmation.

A second untouched window must confirm the selected frozen candidate. Any subsequent production decision integration requires a separate consumer-specific acceptance and activation record. Positive CLV alone does not prove positive expected return. This specification authorizes no real-money workflow.

## 12. First vertical slice

Implement `cfb_market_movement_context_v1`: a saved endpoint movement measurement with market/selection signs, exact source IDs, time interval, book intersection, quote freshness, coverage, and origin. This is context, not a new predictive score.

Appendix E fixes its market scope, endpoint algorithm, 30-minute maximum interval, five-minute quote freshness, four-book intersection, signs, and identity. These are versioned descriptive-measurement defaults, not changes to existing trigger thresholds. Phase 0 must also publish the mandatory CFBD feature-selection artifact described in Appendix A before Phase 3 begins.

Use one identical snapshot in:

1. An offline Python shadow-study job measuring subsequent movement and outcomes under frozen cohort rules.
2. The CFB terminal's evidence detail.
3. A versioned postgame evaluation export.

The terminal links to the study's pinned snapshot; a general latest-state view is labeled separately. No consumer independently recalculates the movement. No production selection changes in this slice.

## 13. Implementation sequence and repository integration

| Phase | Deliverable | Exit condition |
|---|---|---|
| 0 | Reproduce supplied report; inventory consumers and CFBD source-to-consumer coverage; preserve ongoing capture | Pinned baseline, reconciled counts, frozen CFBD selection artifact, provider evidence-policy records, explicit stored-but-unused inputs and source gaps, no acquisition interruption. |
| 1 | Canonical economics, separate CLV units, neutral CFB language | Moneyline losses visible; denominator and grade reconciliation pass. |
| 2 | Evidence/context contracts, policy registry, manifests/readers | Pinned replay, current resolution, and denied decision reads pass. |
| 3 | Market-movement slice, bounded CFBD football-context integration, and detector opportunity funnel | Python studies, terminal, and exports share exact snapshot IDs; market-only versus added-football-context study is registered. |
| 4 | Moneyline audit and preregistered CFB study | Audit documented; complete configuration frozen before enrollment. |
| 5 | First and second untouched windows | Independent evaluation reports; failed gates remain visible. |
| 6 | Optional consumer-specific activation proposal | Separate reviewed evidence and implementation acceptance; no automatic promotion. |

Expected integration points, subject to implementation-time ownership checks:

- `model/line_alerts.py`: detector versions, source references, CFB terminology, settlement adapters.
- `model/signal_observations.py`: observation identities and shared evidence integration.
- `ingest/cfb_capture_audit.py`, `ingest/cfb_movements.py`: capture/detector funnels and replay diagnostics.
- `ingest/cfb_historical_replay.py`: preserve historical origin and isolated evaluation.
- `ingest/cfb_schedule.py`, `ingest/cfb_history.py`, `ingest/cfb_plays.py`, `ingest/cfb_rosters.py`: CFBD evidence, revision/availability audit, identity integration, and source-to-consumer coverage.
- `model/cfb_team_features.py`: expose eligible football context through shared snapshots rather than duplicate feature calculations.
- `web/src/db/queries.ts`: canonical economic queries and unit-separated scorecards.
- `web/src/lib/cfb-movement.ts`, `web/src/app/cfb/cfb-terminal-client.tsx`: render stored context and policy state.
- Existing shared schema ownership: additive migrations with Python/web contract agreement; no request-time schema creation.

Add engine modules only where existing ownership cannot accommodate the contract. Do not change shared NFL or other-sport behavior through a CFB-only fix. Preserve legacy first-breach behavior until an explicit versioned migration replaces it.

### 13.1 Remaining delivery order and completion evidence — September 25, 2026

The [senior-developer handoff](cfb-market-context-engine-handoff-2026-09-25.md) records substantial research infrastructure, but end-to-end engine delivery remains incomplete. Prioritize trustworthy collection, shared consumption, and the required football-context experiment before threshold tuning or feature expansion. Implemented workflow files, passing component tests, and registered consumers do not by themselves establish deployed operation or completed integration.

#### Priority 0 — verify and protect the active pilot

Data/platform engineering must produce a pinned **pilot operations evidence report** as the next delivery artifact. It must identify:

- The deployed commit, execution environment, study version/configuration digest, reporting interval, and evidence cutoff in UTC.
- Expected scheduled runs, actual run IDs/links and outcomes, missing or failed runs, and the first successful run executing the deployed collection path.
- Reconciled counts and record references across provider captures → normalized quotes → detector opportunities → persisted signals → study observations → canonical economics. Explain exclusions, deduplication, pending settlement, and failures at each boundary; these stages need not have equal counts.
- Persisted funnels for every scheduled detector/version, including zero-match runs, with bounded rejection evidence and publication receipts. A missing funnel is an operational failure, not evidence of zero opportunities.
- A reproducible real-data trace through every populated stage. If no qualifying signals exist, demonstrate successful capture/eligibility/detection execution and explain the zero using the opportunity funnel; leave downstream stages explicitly empty rather than manufacturing a signal.
- Monitoring evidence for missing scheduled runs, zero eligible opportunities across an expected slate, temporal rejection spikes, stale-book spikes, and persistence/publication failures. A zero final trigger count alone is not an incident. Define operational alert thresholds in a versioned operations contract without changing study gates.

Successful scheduled runs and reconciled evidence are required before describing collection as operationally verified. Preserve quota and capture ownership rules; this work authorizes no additional API spending. Report failures promptly and correct the collection path without silently changing enrolled signal definitions or study rules.

#### Priority 1 — correct status and metric reporting

Application/research owners must align the handoff, terminal status, and acceptance summaries with the actual frozen configuration and delivered scope:

- Moneyline study version 4 uses `decimal_price_ratio_pct` as its primary metric, with a minimum primary mean of 0.5% and a maximum 95% interval half-width of 1.0 percentage point. Do not label this primary metric as realized ROI. Realized P&L per stake and the positive 1% adverse-price ROI check are separate economic measures.
- Describe the existing acceptance result as **implemented checks pass; end-to-end acceptance remains incomplete** while required integrations or operational controls lack evidence. The machine label `implemented_with_external_gates` does not prove that all engineering work is complete.
- Track engineering omissions, external approvals, and future research windows separately. Each open item must have an owner role, required completion evidence, and its affected delivery or activation gate.
- Include the market-only versus added-football-context experiment as an explicit unfinished deliverable, separate from the existing moneyline trigger study.

#### Priority 2 — complete shared consumer integration

Application/data engineering must implement `cfb-postgame-export` and connect Python shadow research through shared policy-aware readers. Terminal evidence views, shadow research, and the versioned export must retrieve the same pinned market and `cfb_offensive_drive_volume` version 1 snapshots, subject to each consumer's policy. Do not independently recalculate their values or replace pinned inputs with latest-state queries.

Completion requires both a pinned reconciliation fixture and a reproducible real-data acceptance sample comparing snapshot IDs, stored values, definition versions, units, source manifests, and as-of boundaries across all three consumers. Include explicit missing/denied behavior and replay after a source correction. Registering a consumer or publishing snapshots without consuming them does not satisfy Phase 3.

#### Priority 3 — register the incremental football-context experiment

Research/modeling must freeze a market-only baseline and an otherwise identical challenger using the selected drive-volume context before enrollment in a new untouched prospective window. The registration must pin both recipes, eligible inputs and manifests, population, observation/decision unit, deduplication, temporal boundaries, primary comparison metric, uncertainty method, health floors, sample/precision requirements, and decision rules. Hold other settings fixed or preregister differences and attribution limits as required by Section 2.2.

Use a separate study registration and evidence record; do not retrofit this experiment into enrolled moneyline study version 4. Its trigger evaluation cannot establish incremental football-context value. Report improvement, no improvement, degradation, or insufficient evidence under the frozen comparison rules. Descriptive reuse does not require predictive improvement; predictive or decision qualification requires evidence for the specific intended use.

#### Priority 4 — complete operational and permission controls

These workstreams may proceed alongside collection and integration; the ordering above is not permission to defer a control needed for a particular operation:

| Workstream | Owner role | Required evidence |
|---|---|---|
| Concurrent publication and invalidation | Data/platform engineering | Production-like transaction tests for binding/pointer generation conflicts, lock ordering, consumer-specific replacement selection, unavailable pointers, and saved-decision invalidation. |
| Provider evidence policy | Product/legal/account owner | Account-specific terms artifact and reviewed, versioned permissions for representations, retention, redistribution, scope, and duration; unresolved permissions remain unknown under Appendix F. |
| Database privilege isolation | Platform/security engineering | Dedicated decision-service identity can retrieve policy-approved outputs while raw evidence, unqualified snapshots, and legacy bypass reads are denied. |
| Operational erasure | Data/platform engineering | Executable workflow and test receipts covering applicable normalized fields, object storage, dependent manifests, replicas/backups, and replay-unavailable propagation; schema constraints alone are insufficient. |

#### Priority 5 — evaluate frozen windows and keep activation separate

Preserve moneyline study version 4 and its [frozen configuration](../artifacts/cfb_moneyline_study_364db068d16ef1e272489e0523bb0cc8643f938c29caafdf270aa9e087b8152c.json). The authoritative configuration digest is `364db068d16ef1e272489e0523bb0cc8643f938c29caafdf270aa9e087b8152c`. The schedule below records its existing windows, not new enrollment or revised gates:

| Window | Collection interval (UTC, start inclusive/end exclusive) | Earliest review (UTC) | Role |
|---|---|---|---|
| Pilot | 2026-09-25 00:00 → 2026-10-14 00:00 | 2026-10-17 00:00 | Diagnostic only; cannot qualify a consumer. |
| Confirmation 1 | 2026-10-14 00:00 → 2026-11-25 00:00 | 2026-11-28 00:00 | Must pass independently. |
| Confirmation 2 | 2027-08-20 00:00 → 2027-12-01 00:00 | 2027-12-04 00:00 | Must pass independently. |

Review health and coverage before interpreting performance. Preserve exact evaluation inputs and report failed or inconclusive results without relaxing gates or selecting a passing subgroup after observing outcomes. Changes to thresholds, candidate families, deduplication, gates, or dates require a new study version and a new untouched window.

Even if both confirmations pass, activation requires resolved applicable provider permissions, proven privilege isolation and operational controls, and a separately reviewed acceptance record for one named consumer and bounded use case. Any approved activation appends a new policy/binding generation with monitoring and rollback; research success does not automatically change production behavior. Profitable signals are not an engineering completion requirement.

## 14. Acceptance and adversarial cases

| Case | Required result |
|---|---|
| Legacy moneyline economic result is outside `pnl_units` | Canonical report includes it with provenance, without double counting. |
| Economic sources disagree | Diagnostic emitted; no silent source choice. |
| Valid loss but missing close | Loss appears in ROI; CLV remains missing. |
| Spread and moneyline observations share a family name | Separate CLV units and qualification cohorts. |
| Same proposition fires several families | All observations retained; paper cohort follows frozen dedupe policy. |
| Consensus changes only because a book disappears | Not labeled coordinated steam. |
| Zero detector fires despite captured games | Eligible-opportunity and rejection funnel explains the zero. |
| Trigger missed live but found by replay | Historical diagnostic only; prospective count unchanged. |
| Quote existed before detection but is stale afterward | No assumed executable fill; latency evidence and policy shown. |
| Kickoff changes or worker arrives late | Schedule revision retained; invalid captures rejected; no synthetic close. |
| Source/score correction arrives after freeze | New revision/grade; pinned old report remains reproducible. |
| Caller requests unapproved decision context | Reader denies; any approved fallback is recorded. |
| One dataset publication fails | Last compatible eligible manifest remains available; health is explicit. |
| One profitable game date is removed | Sensitivity report exposes dependence; no automatic promotion. |

Validate pure sign/payout/denominator rules, identity and temporal rejection, idempotent persistence, reader permissions, and report reconciliation with meaningful fixtures. Run existing affected CFB and shared-market regression checks. Verify all three vertical-slice consumers against a pinned fixture and a reproducible real-data sample. Add negative tests for historical-origin leakage and wrong-unit aggregation.

Completion means a non-UI consumer can retrieve, use, explain, and replay the same qualified CFB context as the terminal while enforcing its own policy. A dashboard alone does not satisfy this specification.

## 15. Expected results

The expected outcome is a working CFB context engine with trustworthy research outputs and demonstrated reuse outside the UI. Engineering results are delivery requirements. Predictive improvement, positive CLV, and profitability are hypotheses to evaluate, not promised consequences of implementing the engine.

### 15.1 What users and downstream systems gain

| Area | Expected result | Evidence of delivery |
|---|---|---|
| Game context | A consumer can retrieve the available market and CFBD football context for a canonical game at an exact as-of boundary. Missing inputs and incompatible revisions are explicit. | A saved manifest identifies every selected context snapshot, source release, and eligibility decision. |
| CFBD utilization | The project can distinguish inputs that are accessible, stored, normalized, used, and qualified. At least one bounded football-context definition is actually used outside the UI. | Source-to-consumer inventory plus a Python shadow-study run referencing the same snapshot shown in the terminal/export. |
| Consistent interpretation | Research jobs, reports, and the terminal agree on signs, units, definitions, and source evidence when reading the same snapshot. | Cross-consumer reconciliation against pinned fixtures and a reproducible real-data cohort. |
| Honest performance reporting | Moneyline economics are included; missing CLV does not hide losses; duplicate observations do not masquerade as independent bets. | Reconciled settlement counts, stake denominators, source-grade references, and separate observation/decision summaries. |
| Detector diagnosis | A zero-fire or missed detector has an identifiable stage and reason rather than an unexplained absence of alerts. | Opportunity funnels and incident reports separating capture, eligibility, detection, persistence, and publication failures. |
| Model development | A study can test whether football context adds information beyond the market and identify where an estimate misses. | Frozen baseline/challenger comparisons, subgroup diagnostics, and separately versioned outcomes. |
| Decision protection | Descriptive or shadow evidence cannot silently affect an unapproved production consumer. | Recorded policy decisions, denied-read tests, and explicit fallback/activation records. |

For example, a market movement observation could be accompanied by supported prior-game possession volume and roster-continuity context. A research job can test whether those inputs improve a forecast; the terminal can show the exact evidence; postgame evaluation can explain the comparison. Their coexistence does not establish that the football context caused the market move.

### 15.2 Expected results by implementation stage

- **Phases 0–1:** Produce a corrected, reproducible baseline scorecard, a source/consumer inventory, and visible economic and coverage gaps. Reported historical ROI may improve or deteriorate after corrections; accuracy of accounting is the success criterion.
- **Phases 2–3:** Deliver pinned/current readers, enforceable consumer policies, and both market-context and CFBD football-context integrations. Reuse must be demonstrated in Python, the terminal, and exports before calling the engine delivered.
- **Phase 4:** Produce a documented moneyline failure audit and a fully specified prospective study. The audit may identify a software defect, data limitation, calibration issue, several interacting causes, or insufficient evidence to isolate a cause.
- **Phase 5:** Produce untouched-window evaluations with uncertainty, execution sensitivity, concentration, and missingness. A candidate may qualify for paper use, fail, or remain inconclusive. None of these states may be concealed by selectively reporting a successful subgroup.
- **Phase 6, if justified:** Prepare a scoped consumer activation proposal with its own acceptance evidence. A successful research study does not automatically change pick'em, survivor, projections, alerts, or any other production output.

### 15.3 Measurable engineering outcomes

For the delivered integrations and declared evaluation cohort, require:

1. Every published context and forecast resolves to its definition, input manifest, and retained source evidence; no accepted output has an unresolved provenance reference.
2. Pinned replay reproduces stored deterministic outputs. Any numerical tolerance or stochastic replay requirement is defined in the versioned computation contract before verification.
3. Consumers reading the same snapshot return identical stored measurements and units. Display rounding may differ only under an explicit presentation rule.
4. Every signal in the report cohort reconciles to an economic state and a separate CLV-availability state. Valid economic results are neither omitted because a close is missing nor counted more than once through grade revisions.
5. No accepted prospective input or reconstructed observation violates its declared temporal/origin eligibility; detected violations block the affected publication or qualification.
6. All attempted unapproved decision reads in the acceptance suite are denied, and any fallback is traceable to an approved policy.
7. Every evaluated detector/version exposes eligible-opportunity counts and exclusion reasons, including when no signal fires.
8. The required CFBD definition is consumed by at least one non-UI study and is traceable to the same snapshot in the terminal and export.

These requirements concern accepted outputs, not an assumption of complete provider coverage. Capture completion, freshness, latency, query performance, storage growth, and API consumption must be measured against the Phase 0 baseline. Numeric operational targets are set in the relevant release/study contract before evaluation; this spec does not invent unsupported service guarantees or authorize higher API spending.

### 15.4 Expected research conclusions and their consequences

| Finding | Interpretation | Required next action |
|---|---|---|
| Added CFBD context improves the prespecified forecast metric and survives confirmation | Evidence of incremental value for the tested consumer, horizon, and cohort. | Consider scoped paper qualification or a separate activation proposal; do not generalize to unrelated markets. |
| Context improves explanations but not forecasts | Useful descriptive evidence without demonstrated predictive lift. | Retain descriptive use; do not force the feature into predictive models. |
| A movement signal has positive CLV but poor or uncertain economic results | Market-following evidence is not yet a demonstrated executable advantage. | Investigate price, latency, settlement, variance, and benchmark dependence; retain research status. |
| A candidate fails or degrades calibration/performance | The tested definition does not justify the intended use. | Retire or revise the hypothesis under a new version and new prospective window. |
| Results depend on one date, subgroup, or a few longshots | Broad qualification is unsupported. | Report concentration; investigate without retroactively narrowing the confirmatory cohort. |
| Coverage or independent sample size remains inadequate | The question remains unanswered. | Continue bounded collection or address the source gap; do not convert inconclusive evidence into approval or a claim of failure. |

No target win rate, ROI, CLV uplift, or fixed promotion date is promised. Effect-size and precision targets must be justified and preregistered for each study. An engine that reliably exposes weak signals, prevents inappropriate reuse, and produces reproducible negative findings still delivers its intended infrastructure and research value.

### 15.5 Completion report

Close implementation with a pinned report listing delivered consumers and definitions, source coverage, replay/reconciliation evidence, accounting corrections, detector incidents, operational measurements, study IDs and windows, qualification decisions, and remaining gaps. Clearly separate implemented capability, observed empirical results, and hypotheses awaiting evaluation.
