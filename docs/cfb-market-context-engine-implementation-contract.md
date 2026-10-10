# CFB Context Engine — Normative Implementation Appendices

Parent specification: [CFB Market Context Engine](cfb-market-context-engine-spec.md)  
Contract revision: `3`  
Status: Proposed; no migrations, consumer activation, or provider permissions are implied

The rules below are normative for new implementation. Parameters introduced here define reproducible engineering behavior, not empirically validated trigger thresholds. Schema names are proposed additions; implementation must map existing canonical IDs without duplicating identity registries.

## Appendix A. Compatibility, namespaces, and required artifacts

### A.1 Legacy compatibility matrix

| Existing rule | Disposition | New contract |
|---|---|---|
| Prospective observations are not recommendations | Preserved | Applies to all consumers and backend output. |
| Immutable source-to-signal-to-grade chain | Preserved/extended | Add typed artifacts, policy decisions, manifests, and evidence classifications. |
| V1 spread/total steam and walking thresholds | Preserved | Existing signals retain their exact version and algorithm. |
| V1 lower-median consensus and book-support rules | Preserved | Apply to v1 detectors; Appendix E is a separate context measurement. |
| V1 key-cross, reversal, price-pressure, reference-led definitions | Preserved | Research/descriptive only; any modification receives a new signal version. |
| Generic CFB moneyline detector execution | Preserved with restricted consumption | Keep observations and grading; prohibit unapproved decision use. |
| Scheduled-boundary verified close | Preserved | Preserve methodology/quality; do not claim actual-start verification. |
| Missing close prevents economic settlement | Superseded | Outcome/P&L and CLV availability are independent, as already specified by capture operations. |
| Final scores and market-specific overtime treatment | Preserved | Settlement-rule version is pinned; exceptions remain explicit. |
| One-unit P&L and correction history | Preserved/clarified | Appendix D fixes source resolution and denominator. |
| V1 line CLV primary | Preserved for v1 studies | New study metric is market-specific and frozen; no mixed-unit promotion metric. |
| Time-ordered fitting and historical-origin separation | Preserved | Applies to context, model fitting, calibration, and explanations. |
| Version-grouped reporting and clustered uncertainty | Preserved/extended | Add overlap accounting, small-cluster sensitivity, and frozen report grades. |
| Segment diagnostics, overtime flags, visible pushes/voids | Preserved | No post hoc changes to confirmation cohorts. |
| V1 paper-qualification gates and second untouched window | Preserved/extended | Add executable policy/configuration gates; no previously unmet gate is waived. |
| Legacy first-breach uniqueness constraint | Preserved | Do not migrate it as a side effect of this engine. |
| Capture cadence, quotas, event mapping, historical archive isolation | Preserved | Existing operational settings remain; new measurements do not authorize spending changes. |
| Historical/team-context hypotheses and source limitations | Preserved | No automatic requalification or input substitution. |
| Dashboard-only ownership of metrics | Superseded | Shared definitions/builders and policy-aware consumers own calculations. |
| Live/in-game strategies and automatic wagering | Out of scope | No implementation or activation under this contract. |

An unlisted legacy behavior is not silently superseded: record its classification in the implementation change log before modifying it.

### A.2 Distinct namespaces

- Architecture: `cfb-market-context-v2`, contract revision `3`.
- Signal: existing `signal_version` identifiers, unchanged; never derived from architecture version.
- Context: `definition_id=cfb_market_movement_context`, `definition_version=1`; `cfb_market_movement_context_v1` is its display alias.
- Policy: `(consumer_id, policy_version)`.
- Study: `(study_id, study_version)` with a frozen configuration digest.
- Model: immutable artifact ID and configuration digest.
- Snapshot, manifest, economic resolution: independent UUIDs; timestamps are not version numbers.

No lexical comparison of version strings determines compatibility.

### A.3 Phase 0 artifacts

Before Phase 3, persist a CFBD feature-selection artifact containing exactly one selected initial definition; source endpoints/fields; verified entitlement record; coverage report; grain; types/units; full calculation; eligibility and missingness; temporal rules; baseline/challenger design; consumer IDs; owner role; acceptance fixtures; and configuration digest. Proposed definition and policy rows are registered and frozen with it. A later choice is a new artifact and requires revising the not-yet-enrolled study, not substituting the feature mid-study.

Phase 0 also produces a pinned baseline ledger, canonical-ID mapping, consumer inventory, provider evidence-policy records, and legacy economic-adapter mapping. Unknown entitlement blocks new raw retention, not lawful processing or normalized collection already permitted by existing contracts.

## Appendix B. Concrete logical schemas

### B.1 Types and invariants

Use PostgreSQL `uuid` for new IDs, `timestamptz` in UTC, `bigint` counts, `numeric` for stored economic/measurement decimals, `boolean`, `text`, and `jsonb`. All fields are NOT NULL unless marked `?`. `PK`, `UQ`, and `FK` below are required database constraints, not documentation-only relationships. State vocabularies are CHECK constraints; counts are nonnegative; start times cannot exceed end times.

Canonical keys use the typed bridges in B.1a. `subject_key` references `cfb_engine_subjects(subject_key)`; every `target_event_key` and funnel `event_key` references `cfb_engine_events(event_key)`. Do not cast differently scoped provider IDs into one numeric namespace. Composite definition/policy FKs must include their version. Unless explicitly stated otherwise, FKs use ON DELETE RESTRICT; historical evidence cannot disappear through cascading deletes.

Every append-only table includes `created_at timestamptz`, `producer_artifact_id uuid FK`, and `record_digest text` (64 lowercase SHA-256 hex characters). Artifact records are exempt from the producer FK bootstrap requirement and identify their producer through metadata. Content digests use UTF-8 canonical JSON with sorted object keys, UTC microsecond timestamps, exact decimal strings, and sorted set-like arrays. Ordered events/steps retain order. Implement shared cross-language digest fixtures. Digests exclude generated UUIDs and storage timestamps unless a timestamp is semantically part of the observation.

Rows with the same idempotency key and digest return the existing ID. The same key with different content is an integrity error, never an upsert overwrite. Database roles/triggers prohibit UPDATE/DELETE on scientific payload tables except authorized retention erasure under Appendix F. Mutable pointers are listed separately. Schema JSON is validated against a pinned JSON Schema artifact before inserts; validation and FK checks are part of publication.

### B.1a Typed canonical identity bridges

| Table | Required columns and constraints |
|---|---|
| `cfb_engine_subjects` | `subject_key text PK`, `namespace text CHECK = 'cfb'`, `entity_type text CHECK IN ('event','team')`; UQ `(subject_key,entity_type)`. |
| `cfb_engine_events` | `event_key text PK`, `entity_type text CHECK = 'event'`, `matchup_id integer UQ FK cfb_matchups(id)`; composite FK `(event_key,entity_type)` to subjects; CHECK `event_key = 'cfb:event:' || matchup_id::text`. |
| `cfb_engine_teams` | `team_key text PK`, `entity_type text CHECK = 'team'`, `team_id integer UQ FK cfb_teams(team_id)`; composite FK `(team_key,entity_type)` to subjects; CHECK `team_key = 'cfb:team:' || team_id::text`. |

Insert the subject and its bridge in one transaction. Deferred constraint triggers require exactly one correctly typed bridge for each subject at commit. These tables contain no provider alias, team name, score, or schedule master fields. Existing `cfb_matchups` and `cfb_teams` remain authoritative for identity; their CFBD IDs are not substituted for internal primary keys. Identity rows cannot be reassigned to another canonical ID. An identity correction revokes affected evidence and creates corrected references under an audited migration. New subject types require an explicit schema extension; no unconstrained string identities are accepted.

### B.2 Evidence and definition tables

| Table | Required columns and constraints |
|---|---|
| `cfb_engine_artifacts` | `artifact_id uuid PK`, `kind text`, `digest text`, `uri text?`, `evidence_policy_id uuid? FK`, `representation text` (`raw_redacted`, `normalized`, `code`, `schema`, `configuration`, `report`), `byte_count bigint`, `metadata jsonb`, `created_at`; UQ `(kind,digest,evidence_policy_id)` using NULLS NOT DISTINCT semantics. |
| `cfb_engine_evidence_policies` | `policy_id uuid PK`, `provider text`, `version integer`, `retention_mode text` (`raw_allowed`, `normalized_only`, `unknown`, `prohibited`), `terms_artifact_id uuid FK`, `approved_by text?`, `approved_at timestamptz?`, `retain_until timestamptz?`, `scope jsonb`; UQ `(provider,version)`. Unknown has no approval; usable modes require approval and documentary basis. |
| `cfb_engine_sources` | `source_id uuid PK`, `provider text`, `provider_record_key text`, `revision_key text`, `artifact_id uuid FK`, `representation text` (`raw_backed`, `normalized_complete`, `normalized_partial`, `legacy_unverified`), `event_at timestamptz?`, `published_at timestamptz?`, `observed_at timestamptz`, `origin text` (`prospective`, `historical`, `legacy`), `projection_schema_id uuid FK`; UQ `(provider,provider_record_key,revision_key)`. Revision key incorporates payload digest. This is a source-record revision, not a many-book quote timestamp; per-book timing belongs to B.2a. |
| `cfb_context_definitions` | `definition_id text`, `definition_version integer`, `payload_schema_id uuid FK`, `calculation_artifact_id uuid FK`, `subject_type text`, `unit text`, `measurement_kind text`, `required_source_fields jsonb`, `default_freshness_seconds bigint`, `default_source_age_seconds bigint`, `parameters jsonb`, `owner_role text`; PK `(definition_id,definition_version)`. |

### B.2a Capture and quote observations

| Table | Required columns and constraints |
|---|---|
| `cfb_engine_schedule_revisions` | `schedule_revision_id uuid PK`, `event_key text FK`, `source_id uuid FK`, `scheduled_kickoff timestamptz?`, `observed_at timestamptz`, `revision_digest text`; UQ `(event_key,revision_digest)`, UQ `(schedule_revision_id,event_key)`. Preserve unknown kickoff rather than guessing. |
| `cfb_engine_settlement_rules` | `rule_id uuid PK`, `book text`, `market text`, `rule_version text`, `rules_artifact_id uuid FK`; UQ `(book,market,rule_version)`, UQ `(rule_id,book,market)`. |
| `cfb_engine_captures` | `capture_id uuid PK`, `event_key text FK`, `history_id integer? FK game_odds_history(id)`, `source_id uuid FK`, `schedule_revision_id uuid`, `provider text`, `request_key text`, `observed_at timestamptz`, `origin text` (`prospective`,`historical`,`legacy`), `pregame_state text` (`pregame`,`in_play`,`unknown`), `normalization_version text`; composite FK `(schedule_revision_id,event_key)` to schedule revisions; UQ `(provider,request_key,event_key,normalization_version)`, partial UQ `(history_id,normalization_version)` WHERE history_id IS NOT NULL. |
| `cfb_engine_quote_observations` | `quote_id uuid PK`, `capture_id uuid FK`, `source_id uuid FK`, `source_locator text`, `book text`, `market text CHECK IN ('spread','total','moneyline')`, `selection text CHECK IN ('home','away','over','under')`, `line numeric?`, `decimal_price numeric CHECK > 1`, `bookmaker_updated_at timestamptz?`, `system_observed_at timestamptz`, `settlement_rule_id uuid?`, `line_role text` (`main`,`alternate`,`unknown`), `quote_digest text`; UQ `(capture_id,source_locator)`, UQ `(capture_id,book,market,selection,line,decimal_price,bookmaker_updated_at,line_role)` with NULLS NOT DISTINCT semantics; composite FK `(settlement_rule_id,book,market)` to settlement rules. CHECK spread/moneyline use home/away, total uses over/under; moneyline line is null, other lines nonnull. |

The capture grain is one event in one acquisition under one normalization version. Quote grain is one distinct side/line/book observation inside that capture. `source_locator` identifies the normalized source element; identical duplicate quote elements collapse deterministically to the lexicographically first locator. Conflicting duplicates remain separate rows for explicit rejection by Appendix E. Unknown settlement rules or bookmaker timestamps can be stored, but cannot pass movement eligibility requiring them.

Constraint triggers verify that quote source evidence belongs to the capture's retained payload/projection, quote observation time equals capture observation time, and any linked history row is `sport='cfb'` with `matchup_id` equal to the event bridge. Known-future bookmaker timestamps are retained for diagnostics and rejected by freshness validation, not normalized into the past.

An unchanged quote in a later acquisition creates a new capture and quote observation, retaining its original bookmaker update time. Source artifacts/revisions may be shared when bytes are unchanged. A repeat processing attempt of the same acquisition is idempotent. A corrected normalization creates a new capture version and invalidates affected current outputs; it is not a new prospective acquisition. Appendix E endpoints are capture IDs selected within a single pinned normalization version; history IDs are provenance, not quote identities.

Manifest items below may directly reference capture/quote IDs, making their timing and dependency closure replayable. Economic entries should reference the exact quote observation where available; legacy economics remain supported through preserved source evidence.

### B.3 Snapshot and publication tables

| Table | Required columns and constraints |
|---|---|
| `cfb_context_snapshots` | `snapshot_id uuid PK`, `(definition_id text,definition_version integer) FK`, `subject_key text FK`, `target_event_key text? FK`, `as_of_at timestamptz`, `window_start timestamptz?`, `window_end timestamptz?`, `payload jsonb`, `scalar_value numeric?`, `coverage_state text` (`complete`,`partial`,`missing`,`not_applicable`), `measurement_kind text` (`observed`,`estimated`), `availability_basis text`, `origin text`, `input_manifest_id uuid FK`, `configuration_artifact_id uuid FK`, `scenario_key text`, `idempotency_key text UQ`. |
| `cfb_context_manifests` | `manifest_id uuid PK`, `kind text` (`inputs`,`context`,`forecast`,`evaluation`), `scope_key text`, `as_of_at timestamptz`, `manifest_digest text UQ`, `policy_id uuid? FK`. Rows are inserted complete with all items in one transaction; there is no mutable partial scientific manifest. Staging uses a separate job workspace. |
| `cfb_context_manifest_items` | `manifest_id uuid FK`, `slot text`, `ordinal integer`, `source_id uuid? FK`, `capture_id uuid? FK`, `quote_id uuid? FK`, `snapshot_id uuid? FK`, `artifact_id uuid? FK`, `child_manifest_id uuid? FK`, `economic_resolution_id uuid? FK`; PK `(manifest_id,slot,ordinal)`; CHECK exactly one target FK is nonnull. Graph must be acyclic, validated before commit. |
| `cfb_context_release_pointers` | `consumer_id text`, `policy_generation bigint`, `scope_key text`, `manifest_id uuid? FK`, `generation bigint`, `availability text` (`ready`,`unavailable`), `updated_at timestamptz`; PK `(consumer_id,policy_generation,scope_key)`; composite FK `(consumer_id,policy_generation)` to binding revisions in B.4; CHECK ready iff manifest is nonnull. Mutable only by publisher transaction. |

`payload` is always a JSON object validated by the definition's schema. Non-scalar vectors, distributions, and per-book measurements live there with typed elements and units. `scalar_value` is only an optional indexed projection; its equality to the schema-designated payload field is verified before publication. It is not an alternative source of truth.

Snapshot idempotency hashes definition/version, subject, target, exact as-of, window, scenario, input-manifest digest, and configuration digest. A corrected input creates a new snapshot even at the same as-of. Creation time is not identity.

### B.4 Policy, decisions, and invalidations

| Table | Required columns and constraints |
|---|---|
| `cfb_context_consumer_policies` | `policy_id uuid PK`, `consumer_id text`, `policy_version integer`, `subject_scope jsonb`, `usage text`, `allowed_origins jsonb`, `max_context_age_seconds bigint`, `max_source_age_seconds bigint`, `owner_role text`; UQ `(consumer_id,policy_version)`, UQ `(policy_id,consumer_id)`. Dependency/qualification references are relational rows in B.4a, not JSON IDs. |
| `cfb_context_binding_revisions` | `consumer_id text`, `policy_generation bigint`, `policy_id uuid`, `effective_at timestamptz`; PK `(consumer_id,policy_generation)`; composite FK `(policy_id,consumer_id)` to policies. Immutable registration of each consumer policy generation. |
| `cfb_context_policy_bindings` | `consumer_id text PK`, `generation bigint`, `updated_at timestamptz`; composite FK `(consumer_id,generation)` to binding revisions; mutable by policy administrator only. Binding changes append an audit event; exact policy ID is obtained from the selected revision. |
| `cfb_context_qualifications` | `qualification_id uuid PK`, `consumer_id text`, `(definition_id,definition_version) FK`, `usage text`, `cohort_schema_id uuid FK`, `cohort jsonb`, `evidence_manifest_id uuid FK`, `decision text` (`allow`,`deny`), `valid_from timestamptz`, `valid_until timestamptz?`. Policies reference exact IDs, never a caller-supplied maturity label. |
| `cfb_context_policy_decisions` | `decision_id uuid PK`, `request_id uuid UQ`, `consumer_id text`, `policy_generation bigint?`, `policy_id uuid? FK`, `mode text` (`pinned`,`current`), `requested_as_of timestamptz`, `evaluation_at timestamptz`, `request_digest text`, `result text` (`allow`,`fallback`,`deny`), `resolved_manifest_id uuid? FK`, `selected_fallback_index integer?`, `reason_codes jsonb`, `candidate_audit_artifact_id uuid? FK`; composite FK `(consumer_id,policy_generation)` to binding revisions. CHECK allow/fallback requires policy, generation and resolved manifest; null policy/generation permitted only for a denied unbound consumer. Constraint trigger verifies policy matches binding revision. |
| `cfb_context_invalidations` | `invalidation_id uuid PK`, `snapshot_id uuid? FK`, `manifest_id uuid? FK`, `source_id uuid? FK`, `qualification_id uuid? FK`, `action text` (`flag`,`revoke`,`clear_flag`), `references_invalidation_id uuid? FK`, `reason_code text`, `effective_at timestamptz`, `replacement_manifest_id uuid? FK`; CHECK exactly one target. `clear_flag` references a prior flag; revocations cannot be cleared. |
| `cfb_context_audit_events` | `audit_id uuid PK`, `event_type text` (`policy_binding`,`publication`,`decision_recheck_denied`,`retention_erasure`,`administrative_access`), `actor_identity text`, `scope_key text`, `decision_id uuid? FK`, `manifest_id uuid? FK`, `policy_id uuid? FK`, `artifact_id uuid? FK`, `before_generation bigint?`, `after_generation bigint?`, `details jsonb`, `idempotency_key text UQ`. Referenced targets and old/new values are required by event-type schema. |

Observed invalidation state is `valid`, `flagged`, or `revoked`, derived at the requested evaluation time. Current decision/predictive reads reject flagged and revoked dependencies. Replacement evidence gets new IDs. Dependency closure includes manifest items and qualifications. Pinned research reads may return flagged/revoked historical evidence with its annotations; they never confer current decision permission.

### B.4a Relational policy dependencies and fallback order

| Table | Required columns and constraints |
|---|---|
| `cfb_policy_resolution_steps` | `policy_id uuid FK`, `step_index integer CHECK >= 0`, `action text` (`resolve`,`pinned_baseline`,`deny`), `baseline_manifest_id uuid? FK`, `compatibility_schema_id uuid FK`, `compatibility_parameters jsonb`; PK `(policy_id,step_index)`. Step 0 is primary; steps 1+ are ordered fallbacks. CHECK baseline manifest present iff action is pinned_baseline. |
| `cfb_policy_dependency_slots` | `policy_id uuid`, `step_index integer`, `slot text`, `required boolean`, `max_context_age_seconds bigint`, `max_source_age_seconds bigint`; PK `(policy_id,step_index,slot)`; composite FK `(policy_id,step_index)` to resolution steps. |
| `cfb_policy_slot_versions` | `policy_id uuid`, `step_index integer`, `slot text`, `preference integer CHECK >= 0`, `definition_id text`, `definition_version integer`, `payload_schema_id uuid FK`; PK `(policy_id,step_index,slot,preference)`; composite FK to dependency slot and composite FK to definition; UQ `(policy_id,step_index,slot,definition_id,definition_version)`. Constraint trigger verifies schema equals the registered definition payload schema. |
| `cfb_policy_step_qualifications` | `policy_id uuid`, `step_index integer`, `qualification_id uuid FK`; PK `(policy_id,step_index,qualification_id)`; composite FK to resolution step. Constraint trigger verifies consumer and usage match policy and definition/cohort covers the corresponding resolved dependency. |

Deferred constraint triggers enforce contiguous steps starting at 0, action=resolve for step 0, deny only as the final step, at least one allowed version per required slot, contiguous preferences, and no dependency slots for deny. All step rows are frozen atomically with the policy. Pinned baselines must satisfy their declared slot schemas/qualifications and temporal constraints too. Any signed JSON policy export is generated from these relational rows; it is not an alternative editable source of references. Nonreference predicate values remain validated JSON; external IDs in them cannot establish permission.

### B.5 Economic and detector tables

| Table | Required columns and constraints |
|---|---|
| `cfb_economic_resolutions` | `resolution_id uuid PK`, `alert_id bigint FK line_alerts`, `resolver_version text`, `grade_evidence_manifest_id uuid FK`, `entry_source_id uuid? FK`, `entry_quote_id uuid? FK`, `outcome_source_id uuid? FK`, `result_state text` (`settled`,`pending`,`void`,`missing_entry`,`conflict`), `outcome text?` (`won`,`lost`,`push`,`void`), `entry_decimal numeric?`, `stake_units numeric`, `pnl_units numeric?`, `roi_stake_units numeric?`, `clv_state text`, `metrics jsonb`, `supersedes_resolution_id uuid? FK`, `idempotency_key text UQ`. Pinned source copies reference exact legacy grade IDs; the manifest preserves their bytes. |
| `cfb_detector_runs` | `run_id uuid PK`, `detector_id text`, `detector_version text`, `input_manifest_id uuid FK`, `scope_key text`, `comparison_policy_artifact_id uuid FK`, `run_key text UQ`, `completed_at timestamptz`. |
| `cfb_detector_funnels` | `run_id uuid FK`, `event_key text FK`, `market text`, `candidate_count bigint`, `eligible_count bigint`, `matched_count bigint`, `deduped_count bigint`, `persisted_count bigint`, `failed_persistence_count bigint`, `rejection_counts jsonb`, `sample_artifact_id uuid? FK`; PK `(run_id,event_key,market)`. |
| `cfb_detector_publication_receipts` | `observation_key text`, `consumer_id text`, `receipt_version integer`, `status text` (`published`,`failed`), `evidence_artifact_id uuid FK`; PK `(observation_key,consumer_id,receipt_version)`. Settlement/CLV counts are joined from resolutions, not overwritten in the original detector funnel. |

### B.6 Frozen studies and evaluation records

Use a new immutable registry for engine studies. Existing `cfb_hypotheses` remains the hypothesis catalog; a constrained bridge preserves its identity without treating mutable legacy status as qualification.

| Table | Required columns and constraints |
|---|---|
| `cfb_engine_studies` | `study_id uuid`, `study_version integer`, `configuration_artifact_id uuid FK`, `configuration_digest text`, `cohort_schema_id uuid FK`, `cohort_rules jsonb`, `primary_metric text`, `primary_unit text`, `clustering_method text`, `multiple_testing_family text`, `frozen_at timestamptz`, `minimum_effect numeric`, `minimum_clusters bigint`, `power_precision_plan jsonb`, `comparison_plan jsonb`, `execution_plan jsonb`, `health_floors jsonb`; PK `(study_id,study_version)`. All plans validate against pinned config schema; config digest must match artifact. |
| `cfb_engine_study_windows` | `study_id uuid`, `study_version integer`, `window_key text`, `purpose text` (`pilot`,`fit`,`selection`,`confirmation_1`,`confirmation_2`), `start_at timestamptz`, `end_at timestamptz`; PK `(study_id,study_version,window_key)`; composite FK to study; CHECK start < end. |
| `cfb_engine_study_dependencies` | `study_id uuid`, `study_version integer`, `role text`, `ordinal integer`, `definition_id text?`, `definition_version integer?`, `artifact_id uuid? FK`, `policy_id uuid? FK`; PK `(study_id,study_version,role,ordinal)`; FK to study and optional MATCH FULL composite FK to definition; CHECK exactly one of definition pair, artifact, policy is selected. Signal/model/config versions are pinned artifact targets, not free-form mutable names. |
| `cfb_engine_study_hypotheses` | `study_id uuid`, `study_version integer`, `hypothesis_id bigint FK cfb_hypotheses(id)`, `registration_artifact_id uuid FK`; PK `(study_id,study_version,hypothesis_id)`; FK to study. Retained registration artifact freezes legacy contents. |
| `cfb_engine_evaluations` | `evaluation_id uuid PK`, `study_id uuid`, `study_version integer`, `window_key text`, `report_revision integer`, `input_manifest_id uuid FK`, `report_artifact_id uuid FK`, `evaluated_through timestamptz`, `result text` (`pass`,`fail`,`inconclusive`,`invalid`), `supersedes_evaluation_id uuid? FK`, `idempotency_key text UQ`; composite FK to study window; UQ `(study_id,study_version,window_key,report_revision)`. Manifest pins forecasts, cohort membership, exclusions, and selected economic/outcome revisions. |
| `cfb_engine_evaluation_metrics` | `evaluation_id uuid FK`, `metric_key text`, `cohort_key text`, `unit text`, `value numeric?`, `lower_bound numeric?`, `upper_bound numeric?`, `n_observations bigint`, `n_games bigint`, `n_dates bigint`, `missing_count bigint`, `method_artifact_id uuid FK`; PK `(evaluation_id,metric_key,cohort_key)`. |
| `cfb_engine_evaluation_legacy_links` | `evaluation_id uuid FK`, `legacy_result_id bigint FK cfb_hypothesis_results(id)`, `legacy_result_artifact_id uuid FK`; PK `(evaluation_id,legacy_result_id)`. Optional historical linkage does not classify old evidence as prospective confirmation. |

Registration transaction validates window non-overlap, both confirmation windows strictly after study freeze, confirmation_2 after confirmation_1, complete numeric gates, and complete dependencies. Once frozen, study rows/children cannot be extended or edited; changes create a new study version. Evaluation revisions never mutate registrations. Qualification evidence manifests reference the evaluation report artifact and its input manifest; pass is not automatically a consumer grant.

### B.7 Retention tombstones and erasure completion

| Table | Required columns and constraints |
|---|---|
| `cfb_engine_erasures` | `erasure_id uuid PK`, `evidence_policy_id uuid FK`, `reason_code text`, `authorized_by text`, `requested_at timestamptz`, `idempotency_key text UQ`. Contains no deleted payload values. |
| `cfb_engine_erasure_targets` | `target_id uuid PK`, `erasure_id uuid FK`, `artifact_id uuid? FK`, `source_id uuid? FK`, `manifest_id uuid? FK`, `quote_id uuid? FK`, `capture_id uuid? FK`, `snapshot_id uuid? FK`, `field_path text?`, `disposition text` (`erase_bytes`,`erase_field`,`invalidate_replay`); CHECK exactly one target FK; CHECK field_path present iff erase_field. UQ over erasure, target IDs, path and disposition using NULLS NOT DISTINCT. Field paths are validated JSON Pointers or allowlisted column names, never dynamic SQL. |
| `cfb_engine_erasure_events` | `erasure_id uuid FK`, `event_sequence integer`, `target_id uuid? FK`, `state text` (`planned`,`started`,`target_completed`,`failed`,`completed`), `occurred_at timestamptz`, `execution_artifact_id uuid? FK`, `details jsonb`; PK `(erasure_id,event_sequence)`. Composite/constraint validation ensures target belongs to this erasure. |

Append target rows for every affected stored object/field and every dependent manifest's replay status; processing may append newly discovered descendants before final completion. Lock the erasure request while allocating event sequence/completing targets. Final completed event requires a completed receipt for every target and the configured replicas/backups, with restricted evidence locations in the execution artifact. Failures remain retryable with new events, not overwritten tombstones. Prevent new dependencies on objects under pending erasure.

Keep artifact/source identity metadata to satisfy RESTRICT FKs where permitted; erase payloads through the privileged retention path and mark their digest as historical, not a hash of surviving bytes. If even identifying metadata cannot be retained, the evidence-policy-specific erasure plan must supply an approved structural placeholder or scoped destructive migration before completion; do not claim deletion complete while prohibited data remains. These are implementation contracts, not provider-specific legal determinations.

## Appendix C. Deterministic reader and publication algorithms

### C.1 Policy selection and trust boundary

1. Authenticate the consumer through its service identity. The credential maps to exactly one consumer ID or an administrator-controlled allowlist; a request body cannot override it.
2. Load that consumer's exact policy binding and immutable binding revision, recording its policy generation. Missing binding denies. Reject requests outside the policy's subject/usage scope; no wildcard/specific-policy precedence is inferred. Resolve step 0 and fallbacks from B.4a relational rows only.
3. Resolve only exact version lists and referenced qualification records. A deny or invalidation affecting a required grant wins. No “newer is compatible” inference is permitted.

Production decision services receive no SELECT rights on raw evidence, unqualified snapshots, or legacy mutable model-source tables for the migrated path. They use the resolver service or a policy-enforcing database function with a fixed search path and least-privilege owner. Publishers/research jobs have separate roles. An administrator can bypass these controls by design; administrator access is audited and is not a production service credential. Until a migrated consumer's legacy bypass is removed and tested, it is not considered policy-enforced and cannot activate this integration.

### C.2 Current eligible read

Use one repeatable-read transaction and record `evaluation_at` from the database clock. `requested_as_of` is the information boundary. For live decision requests it must equal the request's server-assigned boundary; historical callers cannot present an old boundary to make stale evidence current.

For each required slot, enumerate complete published candidates whose subject/target match, origin is permitted, definition version is in the slot's exact ordered allowlist, snapshot as-of is no later than the boundary, and all prospective source/capture/quote observation times are no later than the boundary. Reuse of an older source revision does not make a later capture available earlier. Definition-specific temporal rules also apply to training and estimated artifacts.

Freshness uses the live evaluation time for live requests, or the historical boundary for an explicitly permitted historical research request. Require nonnegative context age within `max_context_age_seconds`; apply the source's definition-specific age field and `max_source_age_seconds` separately. Book quote freshness uses bookmaker update time, not collector arrival time. A missing required age field rejects; zero tolerance is exact freshness, never “unlimited.” Policy overrides must be explicit, otherwise copy definition defaults when registering the policy.

Reject candidates with flagged/revoked dependency closure, expired required qualifications, incompatible target/scenario/as-of contracts, or unavailable required evidence. Rank remaining candidates by definition-version preference, then `as_of_at DESC`, then `created_at DESC`, then UUID ascending. Resolve combinations in that deterministic order, accepting the first combination satisfying every slot's compatibility predicates. Proceed to the ordered fallback list only if no primary combination qualifies.

Fallback steps reference another exact dependency set/approved baseline or `deny`; they cannot recurse into another policy. Evaluate each once in listed order under its own explicit temporal/quality conditions. No implicit zero, league prior, last-value carry, or stale override. If none qualifies, return `deny` with reason codes and no decision data.

Persist the resolved manifest and policy decision atomically. Retrying the same request ID requires an identical request digest and returns its saved decision; new resolution requires a new request ID. Serving a saved decision to a current decision consumer rechecks present invalidations/expiry; a now-invalid decision returns denied, with a new linked audit event, rather than rerunning under the old request ID.

### C.3 Pinned read and invalidation transactions

Pinned research reads resolve only the supplied manifest; no fallback or replacement. Report original policy decisions and subsequent invalidations separately. Missing/expired retained evidence returns `replay_unavailable`; persisted summary viewing may remain possible but must not claim successful replay. Pinned decision consumption still checks current policy permission and invalidation state.

Publication transaction: lock the consumer binding, then the `(consumer_id,policy_generation,scope_key)` release pointer; verify both binding generation and expected pointer generation; validate candidate dependency closure, exact bound policy and schemas; insert the complete manifest/items; advance pointer generation and manifest ID; append publication audit including consumer/policy generation; commit. Pointer manifests must identify that policy. A generation conflict restarts resolution. The resolver never reads uncommitted staging.

Invalidation transaction: lock affected consumer bindings in consumer-ID order, then pointers in `(consumer_id,policy_generation,scope_key)` order; append invalidation and affected-manifest revocations; independently select each consumer's highest-ranked eligible replacement under its bound policy and advance that pointer only. If every prior release is stale or otherwise invalid, publish `availability=unavailable`, `manifest_id=NULL`; do not keep serving an ineligible last-good value as current. All current reads also check dependency invalidations, covering descendants awaiting background index refresh. Recheck permission immediately before a downstream decision is published, under both binding and pointer generation guards.

A policy activation appends a new binding revision and creates new-generation pointers as unavailable (or ready only after fresh resolution), then atomically switches the binding. Prior-generation pointers remain historical and are never fallback candidates merely because they were once ready. Shared policy-neutral input/context manifests remain reusable; consumer output pointers and replacement decisions do not leak across policies. Candidate eligibility in current reads always checks the bound generation, regardless of what another consumer has published.

## Appendix D. Canonical economics and grade selection

### D.1 Authority and precedence

Phase 1 freezes `cfb_economics_resolver_v1` and its adapter artifact before reporting the corrected cohort. Current resolutions are selected as of the report cutoff; reports pin the resulting resolution IDs. Subsequent corrections require a new resolution and report revision.

1. Prefer an approved canonical resolution with a validated explicit correction chain. There must be one unsuperseded head at the cutoff. Competing heads or cycles are conflicts, not “latest timestamp wins.”
2. For unconverted legacy observations, preserve the relevant `alert_grades` revisions and `line_alerts` fields in an immutable evidence manifest. Import the unique legacy `is_current` grade at migration time if present. Multiple current grades quarantine. Do not claim this migration snapshot reconstructs an earlier report's historical grade selection.
3. Within that selected grade, precedence is: nonnull `alert_grades.pnl_units`; then its `grading_json.pnl_units`; then immutable migration-copy `line_alerts.pnl_units`; then recomputation from frozen entry and independently supported outcome. Lower-precedence fields are consistency checks, not silent overrides. Missing values are not zeros.
4. For entry price, use the referenced frozen execution quote; otherwise the adapter's declared `details_json.exec_decimal`; otherwise `details_json.dk_decimal`. These legacy alternatives are usable only for explicitly mapped family/version combinations with matching selection/book and frozen timing. Never read a current market price to fill a historical entry.
5. Compute one-unit economics from the selected entry and outcome whenever possible and compare every applicable stored candidate. Differences exceeding `0.0001` units, inconsistent outcomes, or different proposition identities quarantine the resolution. The tolerance handles documented four-decimal legacy rounding; it is not license to reconcile different prices.

A structured price-less grade may be reported as `legacy_unverified` economics with provenance when its stake/outcome semantics are established, but it is excluded from confirmatory executable cohorts. Otherwise return missing-entry or conflict. Incomplete grades may contribute the fields they support; lack of CLV never blocks valid P&L, and a newer incomplete correction does not revive superseded economic claims. Recompute against the correction's authoritative inputs or mark pending.

Conflict records preserve all candidate sources, reason, and affected totals. No numeric zero replaces a conflict. Reports show unresolved counts and cannot claim full-cohort economics or promote while material economic conflicts remain. Resolution requires a new audited correction referencing the conflicting evidence, never deletion.

### D.2 Denominators and price comparison

`stake_units=1`. For wins/losses/pushes, `roi_stake_units=1`; for voids, `roi_stake_units=0`, `pnl_units=0`. Pending, missing-entry, and conflict economics have null ROI stake/P&L unless a separately supported settled component is explicitly represented in a new resolution. ROI is `SUM(pnl_units)/SUM(roi_stake_units)` over economically resolved rows; pushes enter the denominator. Zero denominator yields null. Report excluded states next to that result.

Same-line execution-price CLV is `100 * (entry_decimal / close_decimal - 1)`, labeled `decimal_price_ratio_pct`. It requires the same book, selection, line, and settlement-rule version. A distinct reference-book comparison uses a separately named metric and benchmark ID. Missing comparability yields null. This price ratio is not a fair-probability change, vig-adjusted EV, or line-point CLV.

Retain line and probability CLV formulas from the parent spec with explicit units. A selected economic resolution pins its close source and grade independently from outcome evidence. Corrected close evidence appends a new resolution even if P&L is unchanged.

## Appendix E. Exact first movement definition

Definition: `(cfb_market_movement_context,1)`. Supports pregame full-game home spread and full-game total only. Spread selection is home; total reports both over/under directional values from one measurement. Moneyline, alternate lines, first halves, and live quotes are unsupported in v1.

For each newly accepted distinct capture `E` for a canonical game/market:

1. Use its system observation time as `as_of_at` and endpoint time. Require the source's pregame classification and observation strictly before the pinned scheduled kickoff. A later schedule correction may revoke eligibility; it does not rewrite this snapshot.
2. Select start capture `S` as the latest accepted capture at or before `E.time - 15 minutes` and no earlier than `E.time - 30 minutes`, with the same game, market, and kickoff revision. Ties use capture ID ascending. Select endpoints before examining book intersection; do not search for a more favorable start if this pair fails. No start produces `no_start_capture`.
3. At each endpoint retain books from the exact allowlist artifact pinned in the definition configuration. Require the provider-designated main line, compatible paired sides at that line, valid decimal prices greater than 1, and bookmaker timestamps no later than capture time and at most 300 seconds old. Ambiguous multiple main lines, absent timestamps, contradictory duplicate quotes, or incompatible settlement rules exclude that book with a reason. Identical duplicate quotes collapse.
4. Take the intersection of endpoint-eligible books. Require at least four. A missing/returning book may count only if valid at both endpoints; no imputation. Record excluded/membership-only books. Intermediate absence does not invalidate an endpoint comparison, but no continuous-path claim is made.
5. For each common book `b`, compute home-direction spread movement `S.home_handicap[b] - E.home_handicap[b]`; positive means movement toward the home team. For totals, compute `E.total[b] - S.total[b]`; positive is toward over; under is its negative.
6. Scalar movement is the lower median of these per-book deltas: sorted element at zero-based index `floor((n-1)/2)`. Also store each book's endpoints/delta, endpoint lower medians on the same intersection, interval seconds, and positive/zero/negative counts. Do not substitute the difference of endpoint medians for median per-book movement.

Snapshot identity includes both capture IDs, kickoff revision, definition/version, allowlist/config digest, canonical game and market. Retry returns the same snapshot; distinct endpoint pairs may coexist for a game, including overlapping windows. They are not independent observations. The shadow study must freeze its own interval selection policy before enrollment; default paper diagnostics show all intervals clustered by game/date, without inventing a betting stake for each interval.

Fixtures must cover four and five books, mixed directions, a disappeared book, duplicated main lines, a timestamp exactly 300 seconds old, 301 seconds old, exact 15/30-minute boundaries, tie captures, no eligible start, and membership-only median changes. Example: home handicaps moving from -3 to -4 produce +1 toward home; totals from 50 to 51 produce +1 toward over. These signs differ intentionally from selection-handicap CLV in the economic contract.

## Appendix F. Entitlement, evidence retention, and erasure

No provider is declared to permit raw retention by this specification. Phase 0 records the applicable account agreement/terms artifact, scope, approved representation, permitted duration, redistribution constraints, and reviewer for CFBD and the odds provider separately. Unknown permission is an unresolved source-contract item; do not infer rights from API accessibility.

Before persistence, remove authorization headers, API keys, URL credential parameters, cookies, and unrelated personal/account metadata. Hash retained redacted bytes and label the digest accordingly; do not claim it verifies the unredacted response. Keep provider request ID, endpoint, nonsecret parameters, acquisition times, and selected quota metadata.

When permitted, store redacted raw JSON as compressed objects outside transactional Postgres; manifests keep URI, compressed/uncompressed sizes, schema, and digest. Normalized records remain in Postgres. Default internal retention target for evidence supporting a frozen study is study closure plus 24 months; other accepted evidence is 24 months after capture. Provider-mandated limits take precedence. Definitions, code/configuration, aggregate reports, and nonrestricted audit metadata are retained indefinitely unless a documented obligation requires removal.

When raw retention is prohibited but normalized retention is permitted, retain the exact normalized projection of every field required to reproduce the definition and its validity checks, with projection schema and digest. `normalized_complete` is replay-eligible for those definitions only; it cannot support future definitions requiring discarded fields. `normalized_partial` and `legacy_unverified` remain usable descriptively, but are ineligible for confirmation until the specific definition's source-completeness audit passes. Do not fabricate missing raw artifacts or discard all existing history for lacking them.

Where neither representation can legally support replay through the required study window, the dataset is ineligible for that study. Existing permitted captures continue under their established contract; do not expand retention without a verified policy.

Deletion is a controlled exception to append-only storage: append a tombstone with policy/reason, impacted artifacts/manifests, and execution time; erase required bytes and affected prohibited normalized fields; mark dependent replay unavailable and revoke affected current uses. Preserve permitted digests/metadata only. A retained report is not proof its deleted inputs remain replayable. Backups and replicas follow the same entitlement/erasure schedule.

## Appendix G. Bounded detector funnels

A detector run is one deterministic evaluation of a detector/version over an immutable input manifest. `run_key=SHA256(detector/version + input manifest digest + comparison-policy digest + scope)`. Retry reuses it; a revised input is a new run. Operational job attempts have separate logs and do not increase scientific counts.

An opportunity is a tuple of canonical game, market, allowed selection, endpoint pair (or definition-specific comparison anchor), and detector/version. The detector's registered comparison-policy artifact must enumerate exactly how these tuples are generated before threshold evaluation. It must state whether one or two sides are eligible and how unsupported markets are excluded. Missing evidence yields a candidate rejected before eligibility, not an invented successful comparison. Existing detectors require this adapter before their funnel can claim complete opportunity coverage.

Store one aggregate funnel row per run/game/market, not one database row per failed opportunity. Counts form these partitions:

- `candidate_count = pre_eligibility_rejected + eligible_count`.
- `eligible_count = below_threshold_count + matched_count`.
- `matched_count = deduped_count + persisted_count + failed_persistence_count`.

Assign one primary rejection reason using this fixed precedence: identity/schedule, unsupported market/selection, missing endpoint, missing required fields, invalid quote, stale quote, insufficient book intersection, temporal/origin violation, then definition-specific eligibility reasons in registered order. Threshold misses are counted separately. Extra diagnostic flags do not add to rejection totals.

Persist every matched observation or its persistence-failure reference. For rejections, retain at most three examples per primary reason per funnel row, chosen by the smallest SHA-256 opportunity keys. Samples contain source IDs and computed checks, never duplicate full provider payloads. Preserve the input manifest and comparison-policy artifact so complete denominators can be reconstructed during their permitted retention window.

Keep detailed samples for 90 days by default; then delete sample artifacts through retention bookkeeping while retaining exact aggregate counters for the study retention period. Retain evidence referenced by an unresolved incident or study at least through the applicable study window, subject to provider limits. Never compact distinct detector versions together or sum overlapping re-evaluations as new opportunities. Cross-run reports deduplicate opportunity keys by replaying pinned comparison manifests; if evidence is unavailable, label distinct-opportunity totals unavailable rather than summing aggregate run counts.

Publication receipts and economic/CLV states are separate append-only records. Join them at a pinned report cutoff so changing settlement counts do not mutate original detection funnels.

## Appendix H. Implementation-readiness checks

Before migration approval, validate schema DDL against these constraints, freeze required Phase 0 artifacts, and demonstrate resolver and economics fixtures. Before consumer activation, prove raw-table privilege denial, invalidation races, unavailable-pointer behavior, correct fallback order, and pinned report stability after corrections. Document each unresolved entitlement/source constraint as a scoped blocker, not a claim that the whole research engine is unusable.

Revision 3 additionally requires schema/transaction fixtures proving:

- An event key cannot reference a team or a different canonical matchup ID, and every subject has exactly one typed bridge.
- A multi-book capture retains different update times; unchanged later quotes create new observations without resetting bookmaker freshness.
- Deleting or referencing nonexistent policy qualifications, allowed definitions, schemas, or fallback manifests fails referential validation.
- Two consumers with different policies can select different replacements after the same source invalidation; changing one policy generation cannot advance or overwrite the other's pointer.
- Frozen study windows/configurations cannot be edited; corrected evaluations create new revisions referencing exact inputs and grades.
- Erasure failures cannot be reported as completed; field-level targets, dependent manifests, backup receipts, and replay-unavailable state remain traceable without retaining erased payload values.

This revision makes the engineering contracts deterministic. It does not supply provider permissions, empirical qualification, or completion evidence that must be obtained during implementation.
