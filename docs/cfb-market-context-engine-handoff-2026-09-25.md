# CFB Market Context Engine — Senior Developer Handoff

**As of:** September 25, 2026  
**Architecture:** `cfb-market-context-v2`  
**Implementation contract:** revision 3  
**Current state:** research collection is active; every production betting consumer remains decision-denied

## BLUF

The repository now has the core data, policy, provenance, accounting, and study infrastructure needed to evaluate the existing College Football market triggers without silently promoting them into betting logic. Historical CFB records have been normalized into typed, reproducible engine records; economics have been reconciled; market and football-context measurements can be published; the dashboard reports the corrected cohorts; and a frozen prospective study is collecting.

This is not a production betting release. Pick'em and Survivor cannot consume the research signals, no stake-sizing or automatic wagering path was added, and the implementation never interprets line movement as proof of sharp action. Activation is blocked by unresolved provider-retention terms, missing database-role isolation, two uncompleted confirmation windows, and the absence of a consumer-specific acceptance record.

The immediate engineering objective is operational reliability and evidence collection, not threshold tuning. The active pilot currently contains zero qualifying observations. That may be a legitimate zero-fire result, but the collection path and detector opportunity funnels must be monitored so a silent ingestion or publication failure cannot be mistaken for “no signals.”

## What the system does now

The current implementation supports the following research loop:

```text
scheduled source collection
        ↓
typed events, teams, captures, and per-book quotes
        ↓
versioned market/football-context snapshots and manifests
        ↓
existing version-pinned trigger observations
        ↓
canonical hypothetical economics and settlement
        ↓
frozen prospective study evaluation
        ↓
consumer-specific activation gate (currently denied)
```

In practical terms, the system now:

- Captures and preserves bookmaker-level price, line, observation-time, and bookmaker-update-time evidence.
- Produces deterministic market-movement context for full-game spreads and totals.
- Produces prospective trailing offensive-drive-volume context from CFBD data.
- Retains the existing trigger algorithms and signal versions rather than changing thresholds during evaluation.
- Reconciles wins, losses, pushes, voids, entry prices, one-unit P&L, and CLV availability independently.
- Tracks hypothetical performance without generating an approved wager or recommendation.
- Freezes study definitions, windows, inputs, and evaluations so results cannot be rewritten after outcomes are known.
- Exposes research status and corrected performance in the CFB terminal using neutral, non-promotional language.

It does not currently:

- Place bets, recommend wagers, or size stakes.
- Feed CFB context into Pick'em, Survivor, or another production optimizer.
- Treat a signal, movement, or positive small sample as a qualified edge.
- Impute missing context, carry old values forward, or substitute stale prices.
- Automatically activate a consumer after a study result.

## Phase status

| Phase | Contract outcome | Status | Remaining work |
|---|---|---|---|
| 0 — baseline and source inventory | Freeze coverage, identity, consumer, provider-policy, feature-selection, and legacy-adapter artifacts | Engineering work complete | Provider entitlement remains `unknown` until human review records account-specific terms |
| 1 — canonical economics | Reconcile historical grades and produce a reproducible scorecard | Complete | Continue appending audited correction revisions; never mutate a frozen result |
| 2 — engine and policy foundation | Add typed storage, manifests, policies, readers, release pointers, invalidations, and audit controls | Core foundation complete | Prove production database-role isolation and exercise full invalidation/replacement races in a production-like environment |
| 3 — vertical integrations | Publish market and CFBD context and reuse it consistently across consumers | Partially complete | Market and drive publishers exist; the terminal is integrated, but the declared postgame export and full cross-consumer reconciliation are not complete |
| 4 — moneyline audit and frozen study | Diagnose historical behavior and enroll a prospective, immutable study | Complete | Preserve version 4 unchanged while it is enrolled |
| 5 — untouched evaluation | Collect pilot and confirmation evidence; evaluate uncertainty, concentration, execution sensitivity, and missingness | Active: pilot collecting | Monitor collection, finalize only after each review time, and retain exact evaluation inputs |
| 6 — scoped activation proposal | Prepare a consumer-specific proposal only if all prior gates pass | Not eligible | Requires both confirmations, provider approval, role isolation, and a separately reviewed acceptance record |

“Core foundation complete” does not mean the original specification's end-to-end delivery criterion has been met. The specification requires a non-UI consumer and export to retrieve and replay the same qualified context as the terminal. The postgame export is still listed as `not_yet_implemented`, and production activation controls have not been proven under a dedicated service role.

## Completed implementation

### 1. Contract-revision-3 schema and integrity controls

[`db/cfb_context_schema.py`](../db/cfb_context_schema.py) defines the additive engine schema. It includes:

- Typed canonical subject bridges for CFB events and teams.
- Evidence policies, artifacts, normalized sources, schedule revisions, captures, and per-book quote observations.
- Versioned context definitions, snapshots, manifests, and manifest dependencies.
- Consumer policies, binding generations, qualification records, release pointers, policy decisions, invalidations, and audit events.
- Canonical economic resolutions and detector funnel/publication records.
- Frozen studies, windows, hypotheses, evaluations, metrics, and legacy links.
- Controlled erasure requests, targets, and completion events.

Database constraints and triggers reject untyped subjects, frozen-record updates, manifest cycles, policy/schema mismatches, decision/binding mismatches, and false erasure completion. The manifest schema distinguishes consumer policy from evidence policy; these are intentionally separate relationships.

[`ingest/cfb_context_migrate.py`](../ingest/cfb_context_migrate.py) provides explicit validation and `--apply` behavior. The schema has been applied to the configured development database; this does not imply it has been deployed to every environment.

### 2. Legacy normalization and typed identity migration

[`ingest/cfb_context_bootstrap.py`](../ingest/cfb_context_bootstrap.py) migrates existing canonical CFB identity and odds history into the new engine without replacing the legacy system of record.

The completed migration produced:

| Record | Count |
|---|---:|
| Canonical teams / typed team bridges | 272 |
| Canonical events / typed event bridges | 9,022 |
| Legacy captures | 7,929 |
| Normalized per-book quote observations | 284,556 |
| Captures preserving different update times among books | 7,926 |

Migrated evidence is explicitly marked `legacy_unverified`. It is useful for descriptive analysis but is not silently upgraded to prospective or confirmation-quality evidence.

Initial policies and release pointers were created for:

- `cfb-terminal` — descriptive
- `cfb-shadow-study` — shadow predictive
- `cfb-postgame-export` — descriptive, implementation pending
- `pickem` — decision denied
- `survivor` — decision denied

Every current release pointer was initialized as unavailable. No consumer inherited a last-known value or another consumer's release.

### 3. Canonical economics and corrected reporting

[`model/cfb_context_economics.py`](../model/cfb_context_economics.py) implements `cfb_economics_resolver_v1`. The resolver:

- Applies the required grade/P&L precedence.
- Resolves entry price only from frozen, mapped evidence.
- Recovers valid football pushes.
- Keeps P&L settlement independent from CLV availability.
- Quarantines inconsistent outcomes, propositions, and economic values instead of selecting a convenient source.
- Uses pushes in the ROI stake denominator and excludes void stakes.

[`ingest/cfb_economics_migrate.py`](../ingest/cfb_economics_migrate.py) persists immutable canonical resolution heads. The current migration state contains 260 unsuperseded heads and zero economic conflicts. Reruns are idempotent.

The CFB reporting queries and terminal were updated in:

- [`web/src/db/queries.ts`](../web/src/db/queries.ts)
- [`web/src/components/market-signal-scorecard.tsx`](../web/src/components/market-signal-scorecard.tsx)
- [`web/src/app/cfb/cfb-terminal-client.tsx`](../web/src/app/cfb/cfb-terminal-client.tsx)
- [`web/src/app/cfb/page.tsx`](../web/src/app/cfb/page.tsx)

The UI now separates moneyline, spread, and total families; labels CLV units explicitly; shows W-L-P-V and excluded counts; and no longer presents the output as a “Top 10” or “bet-grade” list.

The frozen Phase 0 baseline shows that the largest historical moneyline cohorts are negative:

| Signal/version | Settled | Record | Hypothetical P&L | ROI |
|---|---:|---:|---:|---:|
| `dk_value / cfb-lines-v1` | 28 | 2-26 | -15.3000u | -54.64% |
| `steam / cfb-lines-v1` | 25 | 10-15 | -7.4250u | -29.70% |
| `walking / cfb-lines-v1` | 35 | 12-23 | -9.7515u | -27.86% |
| `late_move / market-structure-v1` | 17 | 8-9 | -2.8659u | -16.86% |
| `key_cross / cfb-lines-v1` | 15 | 6-8-1 | -2.2895u | -15.26% |

Some small historical subgroups are positive, including `pinnacle_divergence`, `reference_led`, `reversal`, and `spread_steam`. They are not qualified: the samples are small, correlated, retrospectively observed, and do not satisfy the prospective confirmation contract.

### 4. Deterministic market-movement context

[`model/cfb_market_context.py`](../model/cfb_market_context.py) implements the exact revision-3 movement definition for pregame full-game home spreads and totals:

- Endpoints are selected before book intersection.
- The start capture must be 15–30 minutes before the endpoint under the same schedule revision.
- Quotes must be no more than 300 seconds old using bookmaker time.
- At least four allowlisted books must be eligible at both endpoints.
- Movement is the lower median of per-book changes, not the difference between endpoint medians.
- Spread and total direction signs follow the contract.
- Exact capture IDs, quote IDs, ages, exclusions, and book-level deltas are preserved.

[`ingest/cfb_context_publish.py`](../ingest/cfb_context_publish.py) publishes these measurements as immutable snapshots. The bounded legacy publication created 380 market-context snapshots. Future accepted schedule captures call the dual-write and publication path from [`ingest/cfb_schedule.py`](../ingest/cfb_schedule.py).

### 5. First CFBD football-context vertical slice

[`model/cfb_context_features.py`](../model/cfb_context_features.py) implements `cfb_offensive_drive_volume` version 1: the mean number of distinct offensive drives over the team's latest four eligible completed FBS-versus-FBS games available at the requested as-of boundary.

The definition deliberately has no imputation:

- Four eligible games produces `complete`.
- One to three eligible games produces `partial`.
- No eligible games produces `missing`.

Historical backfill cannot be used to pretend the feature was available before its recorded ingestion time.

[`ingest/cfb_drive_context_publish.py`](../ingest/cfb_drive_context_publish.py) produced the initial prospective 14-day publication:

| Item | Count |
|---|---:|
| Target games | 133 |
| Team-game snapshots | 266 |
| Complete | 249 |
| Partial | 1 |
| Missing | 16 |
| Distinct source drives | 6,432 |
| Manifest items | 11,675 |

Acceptance checks found zero snapshots after kickoff and zero sources observed after a snapshot's as-of boundary.

### 6. Policy-aware resolution and detector accounting

[`model/cfb_context_reader.py`](../model/cfb_context_reader.py) implements deterministic current and pinned resolution behavior:

- Exact definition-version allowlists.
- Context and source freshness enforcement.
- Origin and invalidation filtering.
- Ordered policy fallback.
- Explicit denial when no eligible dependency set exists.
- No implicit zero, league prior, stale override, or last-value carry.

[`model/cfb_detector_funnel.py`](../model/cfb_detector_funnel.py) implements the required opportunity partitions and bounded deterministic rejection samples. It can distinguish capture/eligibility failure, below-threshold opportunities, matched observations, deduplication, successful persistence, and persistence failure.

The storage model for policy decisions, release generations, invalidations, funnels, and publication receipts exists. Full production transaction/race testing and routine funnel publication remain next-step work; schema presence alone should not be treated as production enforcement.

### 7. Frozen moneyline study and evaluation engine

[`research/cfb_moneyline_audit.py`](../research/cfb_moneyline_audit.py) creates the moneyline audit and frozen study configuration. [`research/cfb_register_moneyline_study.py`](../research/cfb_register_moneyline_study.py) registers immutable study versions.

Versions 1–3 remain as immutable audit history because readiness issues were discovered before enrollment. The enrolled version is:

| Field | Value |
|---|---|
| Study ID | `63aa4086-2963-5aad-951a-0943b33a14ee` |
| Study version | 4 |
| Configuration digest | `364db068d16ef1e272489e0523bb0cc8643f938c29caafdf270aa9e087b8152c` |
| Frozen artifact | `artifacts/cfb_moneyline_study_364db068d16ef1e272489e0523bb0cc8643f938c29caafdf270aa9e087b8152c.json` |

The exact candidate pairs are:

- `dk_value / cfb-lines-v1`
- `steam / cfb-lines-v1`
- `walking / cfb-lines-v1`
- `pinnacle_divergence / cfb-lines-v1`
- `late_move / market-structure-v1`

[`research/cfb_study_evaluation.py`](../research/cfb_study_evaluation.py) evaluates only those exact pairs and includes:

- Game-date clustered bootstrap uncertainty.
- Game-cluster sensitivity.
- Leave-one-date-out sensitivity.
- P&L concentration.
- Missingness and health floors.
- A 1% adverse-price sensitivity check.
- Immutable evaluation reports, input manifests, and metric records.
- Refusal to finalize a window before its frozen settlement grace period ends.

The frozen windows are:

| Window | Collection period (UTC) | Earliest review (UTC) | Current state |
|---|---|---|---|
| Pilot | 2026-09-25 00:00 through 2026-10-14 00:00 | 2026-10-17 00:00 | Collecting; use the pinned pilot operations report for the current count |
| Confirmation 1 | 2026-10-14 00:00 through 2026-11-25 00:00 | 2026-11-28 00:00 | Scheduled |
| Confirmation 2 | 2027-08-20 00:00 through 2027-12-01 00:00 | 2027-12-04 00:00 | Scheduled |

The pass gates require all health floors, at least 20 independent game dates, primary mean `decimal_price_ratio_pct` of at least 0.5%, a lower confidence bound above zero, interval half-width no greater than 1.0 percentage point, and positive performance after the 1% adverse-price ROI haircut. Realized P&L per stake is a separate economic measure. The pilot is diagnostic and cannot qualify a consumer even if it passes.

### 8. Activation protection

[`research/cfb_activation_gate.py`](../research/cfb_activation_gate.py) assesses readiness but never activates a consumer. As of this handoff, both Pick'em and Survivor are denied for all of these reasons:

- Confirmation 1 has not passed.
- Confirmation 2 has not passed.
- CFBD and The Odds API entitlement records remain unresolved.
- A production decision-service role has not demonstrated denial of raw-evidence access.
- No consumer-specific acceptance record exists.
- The current consumer policy is `decision-denied`.

[`model/line_alerts.py`](../model/line_alerts.py) now uses neutral CFB terminology and invokes canonical economics reconciliation after every CFB scan, including pending signals. It does not describe CFB observations as “bet-grade value” or validated sharp action.

### 9. Scheduled integration

The scheduled workflows were extended so the new research path stays current:

- [`.github/workflows/refresh_cfb_terminal.yml`](../.github/workflows/refresh_cfb_terminal.yml) applies/validates the context schema, publishes drive context, and attempts only due study finalizations.
- [`.github/workflows/capture_event_closes.yml`](../.github/workflows/capture_event_closes.yml) applies/validates the context schema, normalizes stored CFB captures, and publishes versioned movement context.

These jobs preserve the existing quota and capture ownership rules. The context work does not authorize additional API spending.

## Verification performed

### Pilot operations continuation

The movement-context publisher now persists an input manifest, run record, and bounded event/market rejection funnels in the same transaction as its snapshots. A **manual diagnostic** execution recorded 272 endpoint opportunities across 92 funnels: 108 accepted measurements were already present, and 84 event/market funnels had zero matches. The run is marked `cfb:manual`; it does not satisfy the scheduled workflow evidence gate. Workflow runs will skip endpoint captures already covered by an earlier successful workflow run, while a failed transaction leaves them eligible for retry. The frozen moneyline alert families still require their own per-detector/version opportunity funnels and publication receipts.

The acceptance artifact now explicitly reports “Implemented checks pass; end-to-end acceptance remains incomplete” and lists open gates with owner roles. The terminal identifies `decimal_price_ratio_pct` as the frozen primary metric and keeps ROI measures separate.

The current implementation was checked on September 25, 2026:

- `129` CFB tests passed.
- The revision-3 acceptance suite returned `implemented_with_external_gates` with no failing implemented checks. End-to-end acceptance remains incomplete while the integration and operational gaps below lack evidence.
- Typed identity, policy/schema mismatch, decision binding, frozen study, manifest-cycle, and erasure-completion negative checks passed.
- Prospective temporal checks found no after-kickoff snapshots and no source observations newer than their snapshot boundary.
- The CFB web production build passed after the terminal study-status integration.
- Scoped linting for the new CFB modules passed. Whole-file linting of the pre-existing `model/line_alerts.py` still exposes unrelated legacy style findings and should not be represented as globally clean.

Primary evidence artifacts:

- [`artifacts/cfb_market_context_phase0_rev3.json`](../artifacts/cfb_market_context_phase0_rev3.json)
- [`artifacts/cfb_moneyline_audit_rev3.json`](../artifacts/cfb_moneyline_audit_rev3.json)
- [`artifacts/cfb_context_acceptance_rev3.json`](../artifacts/cfb_context_acceptance_rev3.json)
- [`artifacts/cfb_pilot_operations.json`](../artifacts/cfb_pilot_operations.json) — pinned pilot run and stage evidence; its `incomplete` status is authoritative for operational verification at its cutoff.
- [`artifacts/cfb_moneyline_study_364db068d16ef1e272489e0523bb0cc8643f938c29caafdf270aa9e087b8152c.json`](../artifacts/cfb_moneyline_study_364db068d16ef1e272489e0523bb0cc8643f938c29caafdf270aa9e087b8152c.json)

## Known limitations and unresolved risks

### External gates

1. **Provider terms are unresolved.** Both `collegefootballdata` and `the-odds-api` have evidence-policy mode `unknown`. A human must review the account-specific agreement and record permitted representations, retention, redistribution, duration, scope, reviewer, and terms artifact.
2. **Production privilege separation is unproven.** A dedicated decision-service database role has not demonstrated that it can read approved manifests while being denied direct access to raw evidence tables.
3. **Prospective evidence remains immature.** The pinned operations report now traces pilot captures and signals into pending economic heads, but the pilot is still collecting and no confirmation evaluation or qualification exists.
4. **Consumer acceptance is absent.** Even successful studies require a separately reviewed, consumer-specific activation record and a new policy generation.

### Engineering gaps

1. **Postgame export is not implemented.** The consumer is registered, but its declared repository path remains `not_yet_implemented`.
2. **Cross-consumer replay is incomplete.** The same pinned snapshot still needs end-to-end reconciliation across the terminal, shadow research, and export.
3. **Operational detector funnels need routine persistence.** The partition logic exists, but every scheduled detector/version must emit complete denominators and bounded rejection evidence so zero observations are explainable.
4. **Invalidation and replacement behavior needs production-like concurrency tests.** Constraints exist, but lock ordering, generation conflicts, consumer-specific replacements, and unavailable-pointer behavior must be exercised with concurrent transactions.
5. **Erasure execution is not an operational service.** Schema and false-completion safeguards exist; provider-specific deletion, object-store removal, backup receipts, and replay-unavailable propagation still require an executable workflow.
6. **Only one CFBD feature is frozen.** Drive volume is the selected vertical slice. Plays, roster membership, returning production, portal, talent, and coaching data remain stored/unused until each receives its own temporal and feature contract.
7. **The repository is currently dirty.** CFB work is mixed with unrelated NFL, tennis, generated, and user files. Do not bulk-stage, reset, or clean the worktree. Isolate the CFB change set deliberately before review.

## Expected next steps

### Priority 0 — protect the active pilot

1. Confirm the scheduled capture and refresh workflows are succeeding after the pilot boundary of `2026-09-25T00:00:00Z`.
2. Verify new `line_alerts` use one of the five frozen signal/version pairs and have prospective timestamps inside the active window.
3. Persist and inspect detector funnels for every scheduled run, including runs with zero matches.
4. Reconcile counts at each boundary: provider capture → normalized quotes → detector opportunities → persisted signals → study observations → settled economics.
5. Alert on missing scheduled runs, zero eligible opportunities across an expected slate, temporal rejection spikes, stale-book spikes, and publication failures. Do not alert merely because the final trigger count is zero.
6. Do not change thresholds, candidate families, dedupe rules, gates, or dates in study version 4. A necessary change requires a new study version and a new untouched window.

### Priority 1 — finish the declared Phase 3 delivery

1. Implement `cfb-postgame-export` as a policy-aware consumer using pinned manifests.
2. Wire the shadow study to consume the same context snapshot/manifest visible in the terminal, including `cfb_offensive_drive_volume` version 1.
3. Add a reconciliation fixture and a real-data acceptance sample proving that terminal, shadow study, and export return the same stored value, definition version, unit, and source manifest.
4. Persist detector funnels and publication receipts from the live detector path rather than only exercising the pure partition logic.
5. Add production-like transaction tests for pointer-generation conflicts, invalidation races, consumer-specific replacement selection, and saved-decision invalidation.

### Priority 2 — resolve governance and retention

1. Have the responsible owner review the CFBD and odds-provider agreements.
2. Record approved evidence policies and terms artifacts; keep `unknown` if the review is inconclusive.
3. Implement a dedicated database role for future decision services and prove raw-table `SELECT` denial while approved manifest reads remain possible.
4. Implement and test the operational erasure workflow, including object storage, normalized field targets, dependent manifests, replicas/backups, receipts, and replay-unavailable state.

### Priority 3 — execute the frozen evaluation schedule

1. Continue collection through the pilot end at `2026-10-14T00:00:00Z`.
2. Do not finalize the pilot before `2026-10-17T00:00:00Z` (October 16 at 8:00 p.m. EDT).
3. Review health and coverage before interpreting ROI. A failed data-health gate makes the window inconclusive or failed; it is not permission to relax the gate.
4. Preserve the pilot as diagnostic only.
5. Collect Confirmation 1 through `2026-11-25T00:00:00Z` and review no earlier than `2026-11-28T00:00:00Z`.
6. Collect the independent 2027 Confirmation 2 window and review no earlier than `2027-12-04T00:00:00Z`.
7. Treat failure or inconclusive evidence as a research result. Do not mine a passing subgroup and relabel it as the frozen primary cohort.

### Priority 4 — activation only if justified

If and only if both confirmation windows pass and the external gates are closed:

1. Prepare a proposal for one named consumer and one bounded use case.
2. Define its approved inputs, freshness, fallback/deny behavior, stake or decision constraints, monitoring, rollback, and owner.
3. Run a separate consumer acceptance suite and record the reviewed acceptance artifact.
4. Append a new policy/binding generation; do not mutate the research policy or reuse another consumer's pointer.
5. Begin with paper/shadow use unless the separately reviewed proposal explicitly authorizes a broader action.

## Operational commands

The commands below are safe status or controlled publication entry points when run with the configured database. Apply flags should be used only in the intended environment.

```powershell
# Validate the additive schema without applying it
python -m ingest.cfb_context_migrate

# Apply the schema in the selected environment
python -m ingest.cfb_context_migrate --apply

# Preview or publish prospective drive context
python -m ingest.cfb_drive_context_publish
python -m ingest.cfb_drive_context_publish --apply

# Show the latest frozen study/window status
python -m research.cfb_study_evaluation

# Finalize only windows whose frozen grace period has elapsed
python -m research.cfb_study_evaluation --finalize-due

# Re-run non-destructive contract acceptance checks
python -m research.cfb_context_acceptance

# Inspect activation blockers; this command never activates the consumer
python -m research.cfb_activation_gate pickem
python -m research.cfb_activation_gate survivor
```

Do not manually invoke a study finalization early, rewrite a frozen artifact, or change release pointers to bypass an unavailable/denied state.

## Recommended ownership split

| Workstream | Suggested owner | Completion evidence |
|---|---|---|
| Pilot operations and funnel monitoring | Data/platform engineering | Scheduled-run ledger and end-to-end count reconciliation |
| Postgame export and cross-consumer replay | Application/data engineering | Pinned fixture plus reproducible real-data comparison |
| Provider entitlement | Product/legal/account owner | Approved versioned evidence-policy records and terms artifacts |
| Database privilege isolation | Platform/security engineering | Role grants and negative access tests |
| Statistical evaluation | Research/modeling | Frozen evaluation report and exact input manifest after each review time |
| Consumer activation | Consumer owner plus reviewer | Separate acceptance artifact and new policy generation |

## Handoff conclusion

The main success so far is not a profitable trigger. It is that CFB market hypotheses can now be measured with explicit identities, times, prices, provenance, economic denominators, frozen rules, and consumer permissions. The historical evidence argues against trusting the principal moneyline triggers today, while the prospective study provides a defensible way to determine whether any signal survives untouched evaluation.

The correct next move is to keep the collection path healthy, make zero-fire behavior explainable, finish the non-UI/export integration, and close the governance controls. Production betting use should remain unavailable unless the two independent confirmation windows and every consumer-specific gate pass.
