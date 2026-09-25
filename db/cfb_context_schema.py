"""Additive PostgreSQL DDL for the CFB context engine (contract revision 3).

Kept outside ``db.schema`` so ordinary application requests never perform this
migration.  Use ``ingest/cfb_context_migrate.py`` explicitly.
"""

from __future__ import annotations


SCHEMA_VERSION = "cfb-context-schema-r3-v1"

DDL = r"""
CREATE TABLE IF NOT EXISTS cfb_engine_subjects (
  subject_key text PRIMARY KEY, namespace text NOT NULL CHECK (namespace='cfb'),
  entity_type text NOT NULL CHECK (entity_type IN ('event','team')),
  UNIQUE(subject_key,entity_type)
);
CREATE TABLE IF NOT EXISTS cfb_engine_events (
  event_key text PRIMARY KEY, entity_type text NOT NULL DEFAULT 'event' CHECK(entity_type='event'),
  matchup_id integer NOT NULL UNIQUE REFERENCES cfb_matchups(id),
  FOREIGN KEY(event_key,entity_type) REFERENCES cfb_engine_subjects(subject_key,entity_type),
  CHECK(event_key='cfb:event:'||matchup_id::text)
);
CREATE TABLE IF NOT EXISTS cfb_engine_teams (
  team_key text PRIMARY KEY, entity_type text NOT NULL DEFAULT 'team' CHECK(entity_type='team'),
  team_id integer NOT NULL UNIQUE REFERENCES cfb_teams(team_id),
  FOREIGN KEY(team_key,entity_type) REFERENCES cfb_engine_subjects(subject_key,entity_type),
  CHECK(team_key='cfb:team:'||team_id::text)
);

CREATE TABLE IF NOT EXISTS cfb_engine_artifacts (
  artifact_id uuid PRIMARY KEY, kind text NOT NULL, digest text NOT NULL, uri text,
  evidence_policy_id uuid, representation text NOT NULL CHECK(representation IN ('raw_redacted','normalized','code','schema','configuration','report')),
  byte_count bigint NOT NULL CHECK(byte_count>=0), metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(), UNIQUE NULLS NOT DISTINCT(kind,digest,evidence_policy_id)
);
CREATE TABLE IF NOT EXISTS cfb_engine_evidence_policies (
  policy_id uuid PRIMARY KEY, provider text NOT NULL, version integer NOT NULL CHECK(version>0),
  retention_mode text NOT NULL CHECK(retention_mode IN ('raw_allowed','normalized_only','unknown','prohibited')),
  terms_artifact_id uuid NOT NULL REFERENCES cfb_engine_artifacts(artifact_id), approved_by text,
  approved_at timestamptz, retain_until timestamptz, scope jsonb NOT NULL DEFAULT '{}'::jsonb,
  UNIQUE(provider,version),
  CHECK((retention_mode='unknown' AND approved_by IS NULL AND approved_at IS NULL) OR
        (retention_mode<>'unknown' AND approved_by IS NOT NULL AND approved_at IS NOT NULL))
);
DO $$ BEGIN
  ALTER TABLE cfb_engine_artifacts ADD CONSTRAINT cfb_artifact_policy_fk
    FOREIGN KEY(evidence_policy_id) REFERENCES cfb_engine_evidence_policies(policy_id);
EXCEPTION WHEN duplicate_object THEN NULL; END $$;
CREATE TABLE IF NOT EXISTS cfb_engine_sources (
  source_id uuid PRIMARY KEY, provider text NOT NULL, provider_record_key text NOT NULL,
  revision_key text NOT NULL, artifact_id uuid NOT NULL REFERENCES cfb_engine_artifacts(artifact_id),
  representation text NOT NULL CHECK(representation IN ('raw_backed','normalized_complete','normalized_partial','legacy_unverified')),
  event_at timestamptz, published_at timestamptz, observed_at timestamptz NOT NULL,
  origin text NOT NULL CHECK(origin IN ('prospective','historical','legacy')),
  projection_schema_id uuid NOT NULL REFERENCES cfb_engine_artifacts(artifact_id),
  UNIQUE(provider,provider_record_key,revision_key)
);
CREATE TABLE IF NOT EXISTS cfb_context_definitions (
  definition_id text NOT NULL, definition_version integer NOT NULL CHECK(definition_version>0),
  payload_schema_id uuid NOT NULL REFERENCES cfb_engine_artifacts(artifact_id),
  calculation_artifact_id uuid NOT NULL REFERENCES cfb_engine_artifacts(artifact_id),
  subject_type text NOT NULL CHECK(subject_type IN ('event','team')), unit text NOT NULL,
  measurement_kind text NOT NULL CHECK(measurement_kind IN ('observed','estimated')),
  required_source_fields jsonb NOT NULL, default_freshness_seconds bigint NOT NULL CHECK(default_freshness_seconds>=0),
  default_source_age_seconds bigint NOT NULL CHECK(default_source_age_seconds>=0),
  parameters jsonb NOT NULL, owner_role text NOT NULL, PRIMARY KEY(definition_id,definition_version)
);
CREATE TABLE IF NOT EXISTS cfb_engine_schedule_revisions (
  schedule_revision_id uuid PRIMARY KEY, event_key text NOT NULL REFERENCES cfb_engine_events(event_key),
  source_id uuid NOT NULL REFERENCES cfb_engine_sources(source_id), scheduled_kickoff timestamptz,
  observed_at timestamptz NOT NULL, revision_digest text NOT NULL,
  UNIQUE(event_key,revision_digest), UNIQUE(schedule_revision_id,event_key)
);
CREATE TABLE IF NOT EXISTS cfb_engine_settlement_rules (
  rule_id uuid PRIMARY KEY, book text NOT NULL, market text NOT NULL CHECK(market IN ('spread','total','moneyline')),
  rule_version text NOT NULL, rules_artifact_id uuid NOT NULL REFERENCES cfb_engine_artifacts(artifact_id),
  UNIQUE(book,market,rule_version), UNIQUE(rule_id,book,market)
);
CREATE TABLE IF NOT EXISTS cfb_engine_captures (
  capture_id uuid PRIMARY KEY, event_key text NOT NULL REFERENCES cfb_engine_events(event_key),
  history_id integer REFERENCES game_odds_history(id), source_id uuid NOT NULL REFERENCES cfb_engine_sources(source_id),
  schedule_revision_id uuid NOT NULL, provider text NOT NULL, request_key text NOT NULL,
  observed_at timestamptz NOT NULL, origin text NOT NULL CHECK(origin IN ('prospective','historical','legacy')),
  pregame_state text NOT NULL CHECK(pregame_state IN ('pregame','in_play','unknown')),
  normalization_version text NOT NULL,
  FOREIGN KEY(schedule_revision_id,event_key) REFERENCES cfb_engine_schedule_revisions(schedule_revision_id,event_key),
  UNIQUE(provider,request_key,event_key,normalization_version)
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_cfb_capture_history_normalization
  ON cfb_engine_captures(history_id,normalization_version) WHERE history_id IS NOT NULL;
CREATE TABLE IF NOT EXISTS cfb_engine_quote_observations (
  quote_id uuid PRIMARY KEY, capture_id uuid NOT NULL REFERENCES cfb_engine_captures(capture_id),
  source_id uuid NOT NULL REFERENCES cfb_engine_sources(source_id), source_locator text NOT NULL,
  book text NOT NULL, market text NOT NULL CHECK(market IN ('spread','total','moneyline')),
  selection text NOT NULL CHECK(selection IN ('home','away','over','under')), line numeric,
  decimal_price numeric NOT NULL CHECK(decimal_price>1), bookmaker_updated_at timestamptz,
  system_observed_at timestamptz NOT NULL, settlement_rule_id uuid, line_role text NOT NULL CHECK(line_role IN ('main','alternate','unknown')),
  quote_digest text NOT NULL, UNIQUE(capture_id,source_locator),
  UNIQUE NULLS NOT DISTINCT(capture_id,book,market,selection,line,decimal_price,bookmaker_updated_at,line_role),
  FOREIGN KEY(settlement_rule_id,book,market) REFERENCES cfb_engine_settlement_rules(rule_id,book,market),
  CHECK((market IN ('spread','moneyline') AND selection IN ('home','away')) OR (market='total' AND selection IN ('over','under'))),
  CHECK((market='moneyline' AND line IS NULL) OR (market IN ('spread','total') AND line IS NOT NULL))
);

CREATE TABLE IF NOT EXISTS cfb_context_manifests (
  manifest_id uuid PRIMARY KEY, kind text NOT NULL CHECK(kind IN ('inputs','context','forecast','evaluation')),
  scope_key text NOT NULL, as_of_at timestamptz NOT NULL, manifest_digest text NOT NULL UNIQUE,
  policy_id uuid, evidence_policy_id uuid REFERENCES cfb_engine_evidence_policies(policy_id)
);
ALTER TABLE cfb_context_manifests ADD COLUMN IF NOT EXISTS evidence_policy_id uuid REFERENCES cfb_engine_evidence_policies(policy_id);
CREATE TABLE IF NOT EXISTS cfb_context_snapshots (
  snapshot_id uuid PRIMARY KEY, definition_id text NOT NULL, definition_version integer NOT NULL,
  subject_key text NOT NULL REFERENCES cfb_engine_subjects(subject_key), target_event_key text REFERENCES cfb_engine_events(event_key),
  as_of_at timestamptz NOT NULL, window_start timestamptz, window_end timestamptz, payload jsonb NOT NULL,
  scalar_value numeric, coverage_state text NOT NULL CHECK(coverage_state IN ('complete','partial','missing','not_applicable')),
  measurement_kind text NOT NULL CHECK(measurement_kind IN ('observed','estimated')), availability_basis text NOT NULL,
  origin text NOT NULL CHECK(origin IN ('prospective','historical','legacy')), input_manifest_id uuid NOT NULL REFERENCES cfb_context_manifests(manifest_id),
  configuration_artifact_id uuid NOT NULL REFERENCES cfb_engine_artifacts(artifact_id), scenario_key text NOT NULL,
  idempotency_key text NOT NULL UNIQUE, created_at timestamptz NOT NULL DEFAULT now(),
  FOREIGN KEY(definition_id,definition_version) REFERENCES cfb_context_definitions(definition_id,definition_version),
  CHECK(window_start IS NULL OR window_end IS NULL OR window_start<=window_end)
);
ALTER TABLE cfb_context_snapshots ADD COLUMN IF NOT EXISTS created_at timestamptz NOT NULL DEFAULT now();
CREATE TABLE IF NOT EXISTS cfb_context_consumer_policies (
  policy_id uuid PRIMARY KEY, consumer_id text NOT NULL, policy_version integer NOT NULL CHECK(policy_version>0),
  subject_scope jsonb NOT NULL, usage text NOT NULL, allowed_origins jsonb NOT NULL,
  max_context_age_seconds bigint NOT NULL CHECK(max_context_age_seconds>=0),
  max_source_age_seconds bigint NOT NULL CHECK(max_source_age_seconds>=0), owner_role text NOT NULL,
  UNIQUE(consumer_id,policy_version), UNIQUE(policy_id,consumer_id)
);
DO $$ BEGIN
  IF EXISTS(SELECT 1 FROM pg_constraint WHERE conname='cfb_context_manifests_policy_id_fkey'
            AND conrelid='cfb_context_manifests'::regclass) THEN
    UPDATE cfb_context_manifests m SET evidence_policy_id=m.policy_id
      WHERE m.policy_id IS NOT NULL AND EXISTS(
        SELECT 1 FROM cfb_engine_evidence_policies e WHERE e.policy_id=m.policy_id);
    UPDATE cfb_context_manifests SET policy_id=NULL WHERE policy_id IS NOT NULL;
    ALTER TABLE cfb_context_manifests DROP CONSTRAINT cfb_context_manifests_policy_id_fkey;
  END IF;
  IF NOT EXISTS(SELECT 1 FROM pg_constraint WHERE conname='cfb_manifest_consumer_policy_fk') THEN
    ALTER TABLE cfb_context_manifests ADD CONSTRAINT cfb_manifest_consumer_policy_fk
      FOREIGN KEY(policy_id) REFERENCES cfb_context_consumer_policies(policy_id);
  END IF;
END $$;
CREATE TABLE IF NOT EXISTS cfb_context_binding_revisions (
  consumer_id text NOT NULL, policy_generation bigint NOT NULL CHECK(policy_generation>0),
  policy_id uuid NOT NULL, effective_at timestamptz NOT NULL,
  PRIMARY KEY(consumer_id,policy_generation),
  FOREIGN KEY(policy_id,consumer_id) REFERENCES cfb_context_consumer_policies(policy_id,consumer_id)
);
CREATE TABLE IF NOT EXISTS cfb_context_policy_bindings (
  consumer_id text PRIMARY KEY, generation bigint NOT NULL, updated_at timestamptz NOT NULL,
  FOREIGN KEY(consumer_id,generation) REFERENCES cfb_context_binding_revisions(consumer_id,policy_generation)
);
CREATE TABLE IF NOT EXISTS cfb_context_qualifications (
  qualification_id uuid PRIMARY KEY, consumer_id text NOT NULL, definition_id text NOT NULL,
  definition_version integer NOT NULL, usage text NOT NULL,
  cohort_schema_id uuid NOT NULL REFERENCES cfb_engine_artifacts(artifact_id), cohort jsonb NOT NULL,
  evidence_manifest_id uuid NOT NULL REFERENCES cfb_context_manifests(manifest_id),
  decision text NOT NULL CHECK(decision IN ('allow','deny')), valid_from timestamptz NOT NULL, valid_until timestamptz,
  FOREIGN KEY(definition_id,definition_version) REFERENCES cfb_context_definitions(definition_id,definition_version),
  CHECK(valid_until IS NULL OR valid_until>valid_from)
);
CREATE TABLE IF NOT EXISTS cfb_policy_resolution_steps (
  policy_id uuid NOT NULL REFERENCES cfb_context_consumer_policies(policy_id), step_index integer NOT NULL CHECK(step_index>=0),
  action text NOT NULL CHECK(action IN ('resolve','pinned_baseline','deny')), baseline_manifest_id uuid REFERENCES cfb_context_manifests(manifest_id),
  compatibility_schema_id uuid NOT NULL REFERENCES cfb_engine_artifacts(artifact_id), compatibility_parameters jsonb NOT NULL,
  PRIMARY KEY(policy_id,step_index), CHECK((action='pinned_baseline')=(baseline_manifest_id IS NOT NULL))
);
CREATE TABLE IF NOT EXISTS cfb_policy_dependency_slots (
  policy_id uuid NOT NULL, step_index integer NOT NULL, slot text NOT NULL, required boolean NOT NULL,
  max_context_age_seconds bigint NOT NULL CHECK(max_context_age_seconds>=0), max_source_age_seconds bigint NOT NULL CHECK(max_source_age_seconds>=0),
  PRIMARY KEY(policy_id,step_index,slot), FOREIGN KEY(policy_id,step_index) REFERENCES cfb_policy_resolution_steps(policy_id,step_index)
);
CREATE TABLE IF NOT EXISTS cfb_policy_slot_versions (
  policy_id uuid NOT NULL, step_index integer NOT NULL, slot text NOT NULL, preference integer NOT NULL CHECK(preference>=0),
  definition_id text NOT NULL, definition_version integer NOT NULL, payload_schema_id uuid NOT NULL REFERENCES cfb_engine_artifacts(artifact_id),
  PRIMARY KEY(policy_id,step_index,slot,preference),
  FOREIGN KEY(policy_id,step_index,slot) REFERENCES cfb_policy_dependency_slots(policy_id,step_index,slot),
  FOREIGN KEY(definition_id,definition_version) REFERENCES cfb_context_definitions(definition_id,definition_version),
  UNIQUE(policy_id,step_index,slot,definition_id,definition_version)
);
CREATE TABLE IF NOT EXISTS cfb_policy_step_qualifications (
  policy_id uuid NOT NULL, step_index integer NOT NULL, qualification_id uuid NOT NULL REFERENCES cfb_context_qualifications(qualification_id),
  PRIMARY KEY(policy_id,step_index,qualification_id), FOREIGN KEY(policy_id,step_index) REFERENCES cfb_policy_resolution_steps(policy_id,step_index)
);
CREATE TABLE IF NOT EXISTS cfb_context_release_pointers (
  consumer_id text NOT NULL, policy_generation bigint NOT NULL, scope_key text NOT NULL,
  manifest_id uuid REFERENCES cfb_context_manifests(manifest_id), generation bigint NOT NULL CHECK(generation>0),
  availability text NOT NULL CHECK(availability IN ('ready','unavailable')), updated_at timestamptz NOT NULL,
  PRIMARY KEY(consumer_id,policy_generation,scope_key),
  FOREIGN KEY(consumer_id,policy_generation) REFERENCES cfb_context_binding_revisions(consumer_id,policy_generation),
  CHECK((availability='ready')=(manifest_id IS NOT NULL))
);
CREATE TABLE IF NOT EXISTS cfb_context_policy_decisions (
  decision_id uuid PRIMARY KEY, request_id uuid NOT NULL UNIQUE, consumer_id text NOT NULL, policy_generation bigint,
  policy_id uuid REFERENCES cfb_context_consumer_policies(policy_id), mode text NOT NULL CHECK(mode IN ('pinned','current')),
  requested_as_of timestamptz NOT NULL, evaluation_at timestamptz NOT NULL, request_digest text NOT NULL,
  result text NOT NULL CHECK(result IN ('allow','fallback','deny')), resolved_manifest_id uuid REFERENCES cfb_context_manifests(manifest_id),
  selected_fallback_index integer, reason_codes jsonb NOT NULL, candidate_audit_artifact_id uuid REFERENCES cfb_engine_artifacts(artifact_id),
  FOREIGN KEY(consumer_id,policy_generation) REFERENCES cfb_context_binding_revisions(consumer_id,policy_generation),
  CHECK((result IN ('allow','fallback') AND policy_id IS NOT NULL AND policy_generation IS NOT NULL AND resolved_manifest_id IS NOT NULL)
     OR (result='deny' AND resolved_manifest_id IS NULL))
);
CREATE TABLE IF NOT EXISTS cfb_context_invalidations (
  invalidation_id uuid PRIMARY KEY, snapshot_id uuid REFERENCES cfb_context_snapshots(snapshot_id),
  manifest_id uuid REFERENCES cfb_context_manifests(manifest_id), source_id uuid REFERENCES cfb_engine_sources(source_id),
  qualification_id uuid REFERENCES cfb_context_qualifications(qualification_id),
  action text NOT NULL CHECK(action IN ('flag','revoke','clear_flag')), references_invalidation_id uuid REFERENCES cfb_context_invalidations(invalidation_id),
  reason_code text NOT NULL, effective_at timestamptz NOT NULL, replacement_manifest_id uuid REFERENCES cfb_context_manifests(manifest_id),
  CHECK(num_nonnulls(snapshot_id,manifest_id,source_id,qualification_id)=1),
  CHECK((action='clear_flag')=(references_invalidation_id IS NOT NULL))
);
CREATE TABLE IF NOT EXISTS cfb_context_audit_events (
  audit_id uuid PRIMARY KEY, event_type text NOT NULL CHECK(event_type IN (
    'policy_binding','publication','decision_recheck_denied','retention_erasure','administrative_access')),
  actor_identity text NOT NULL, scope_key text NOT NULL,
  decision_id uuid REFERENCES cfb_context_policy_decisions(decision_id),
  manifest_id uuid REFERENCES cfb_context_manifests(manifest_id),
  policy_id uuid REFERENCES cfb_context_consumer_policies(policy_id),
  artifact_id uuid REFERENCES cfb_engine_artifacts(artifact_id),
  before_generation bigint, after_generation bigint, details jsonb NOT NULL,
  idempotency_key text NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS cfb_economic_resolutions (
  resolution_id uuid PRIMARY KEY, alert_id bigint NOT NULL REFERENCES line_alerts(id), resolver_version text NOT NULL,
  grade_evidence_manifest_id uuid NOT NULL REFERENCES cfb_context_manifests(manifest_id),
  entry_source_id uuid REFERENCES cfb_engine_sources(source_id), entry_quote_id uuid REFERENCES cfb_engine_quote_observations(quote_id),
  outcome_source_id uuid REFERENCES cfb_engine_sources(source_id), result_state text NOT NULL CHECK(result_state IN ('settled','pending','void','missing_entry','conflict')),
  outcome text CHECK(outcome IN ('won','lost','push','void')), entry_decimal numeric CHECK(entry_decimal>1), stake_units numeric NOT NULL CHECK(stake_units>=0),
  pnl_units numeric, roi_stake_units numeric CHECK(roi_stake_units>=0), clv_state text NOT NULL, metrics jsonb NOT NULL,
  supersedes_resolution_id uuid REFERENCES cfb_economic_resolutions(resolution_id), idempotency_key text NOT NULL UNIQUE
);
CREATE TABLE IF NOT EXISTS cfb_context_manifest_items (
  manifest_id uuid NOT NULL REFERENCES cfb_context_manifests(manifest_id), slot text NOT NULL, ordinal integer NOT NULL CHECK(ordinal>=0),
  source_id uuid REFERENCES cfb_engine_sources(source_id), capture_id uuid REFERENCES cfb_engine_captures(capture_id),
  quote_id uuid REFERENCES cfb_engine_quote_observations(quote_id), snapshot_id uuid REFERENCES cfb_context_snapshots(snapshot_id),
  artifact_id uuid REFERENCES cfb_engine_artifacts(artifact_id), child_manifest_id uuid REFERENCES cfb_context_manifests(manifest_id),
  economic_resolution_id uuid REFERENCES cfb_economic_resolutions(resolution_id), PRIMARY KEY(manifest_id,slot,ordinal),
  CHECK(num_nonnulls(source_id,capture_id,quote_id,snapshot_id,artifact_id,child_manifest_id,economic_resolution_id)=1)
);
CREATE TABLE IF NOT EXISTS cfb_detector_runs (
  run_id uuid PRIMARY KEY, detector_id text NOT NULL, detector_version text NOT NULL,
  input_manifest_id uuid NOT NULL REFERENCES cfb_context_manifests(manifest_id), scope_key text NOT NULL,
  comparison_policy_artifact_id uuid NOT NULL REFERENCES cfb_engine_artifacts(artifact_id), run_key text NOT NULL UNIQUE,
  completed_at timestamptz NOT NULL
);
CREATE TABLE IF NOT EXISTS cfb_detector_funnels (
  run_id uuid NOT NULL REFERENCES cfb_detector_runs(run_id), event_key text NOT NULL REFERENCES cfb_engine_events(event_key),
  market text NOT NULL CHECK(market IN ('spread','total','moneyline')), candidate_count bigint NOT NULL CHECK(candidate_count>=0),
  eligible_count bigint NOT NULL CHECK(eligible_count>=0), matched_count bigint NOT NULL CHECK(matched_count>=0),
  deduped_count bigint NOT NULL CHECK(deduped_count>=0), persisted_count bigint NOT NULL CHECK(persisted_count>=0),
  failed_persistence_count bigint NOT NULL CHECK(failed_persistence_count>=0), rejection_counts jsonb NOT NULL,
  sample_artifact_id uuid REFERENCES cfb_engine_artifacts(artifact_id), PRIMARY KEY(run_id,event_key,market),
  CHECK(candidate_count>=eligible_count AND eligible_count>=matched_count AND matched_count=deduped_count+persisted_count+failed_persistence_count)
);
CREATE TABLE IF NOT EXISTS cfb_detector_publication_receipts (
  observation_key text NOT NULL, consumer_id text NOT NULL, receipt_version integer NOT NULL CHECK(receipt_version>0),
  status text NOT NULL CHECK(status IN ('published','failed')), evidence_artifact_id uuid NOT NULL REFERENCES cfb_engine_artifacts(artifact_id),
  PRIMARY KEY(observation_key,consumer_id,receipt_version)
);

CREATE TABLE IF NOT EXISTS cfb_engine_studies (
  study_id uuid NOT NULL, study_version integer NOT NULL CHECK(study_version>0),
  configuration_artifact_id uuid NOT NULL REFERENCES cfb_engine_artifacts(artifact_id), configuration_digest text NOT NULL,
  cohort_schema_id uuid NOT NULL REFERENCES cfb_engine_artifacts(artifact_id), cohort_rules jsonb NOT NULL,
  primary_metric text NOT NULL, primary_unit text NOT NULL, clustering_method text NOT NULL, multiple_testing_family text NOT NULL,
  frozen_at timestamptz NOT NULL, minimum_effect numeric NOT NULL, minimum_clusters bigint NOT NULL CHECK(minimum_clusters>0),
  power_precision_plan jsonb NOT NULL, comparison_plan jsonb NOT NULL, execution_plan jsonb NOT NULL, health_floors jsonb NOT NULL,
  PRIMARY KEY(study_id,study_version)
);
CREATE TABLE IF NOT EXISTS cfb_engine_study_windows (
  study_id uuid NOT NULL, study_version integer NOT NULL, window_key text NOT NULL,
  purpose text NOT NULL CHECK(purpose IN ('pilot','fit','selection','confirmation_1','confirmation_2')),
  start_at timestamptz NOT NULL, end_at timestamptz NOT NULL, PRIMARY KEY(study_id,study_version,window_key),
  FOREIGN KEY(study_id,study_version) REFERENCES cfb_engine_studies(study_id,study_version), CHECK(start_at<end_at)
);
CREATE TABLE IF NOT EXISTS cfb_engine_study_dependencies (
  study_id uuid NOT NULL, study_version integer NOT NULL, role text NOT NULL, ordinal integer NOT NULL CHECK(ordinal>=0),
  definition_id text, definition_version integer, artifact_id uuid REFERENCES cfb_engine_artifacts(artifact_id),
  policy_id uuid REFERENCES cfb_context_consumer_policies(policy_id), PRIMARY KEY(study_id,study_version,role,ordinal),
  FOREIGN KEY(study_id,study_version) REFERENCES cfb_engine_studies(study_id,study_version),
  FOREIGN KEY(definition_id,definition_version) REFERENCES cfb_context_definitions(definition_id,definition_version) MATCH FULL,
  CHECK((CASE WHEN definition_id IS NOT NULL THEN 1 ELSE 0 END)+(CASE WHEN artifact_id IS NOT NULL THEN 1 ELSE 0 END)+(CASE WHEN policy_id IS NOT NULL THEN 1 ELSE 0 END)=1)
);
CREATE TABLE IF NOT EXISTS cfb_engine_study_hypotheses (
  study_id uuid NOT NULL, study_version integer NOT NULL, hypothesis_id bigint NOT NULL REFERENCES cfb_hypotheses(id),
  registration_artifact_id uuid NOT NULL REFERENCES cfb_engine_artifacts(artifact_id), PRIMARY KEY(study_id,study_version,hypothesis_id),
  FOREIGN KEY(study_id,study_version) REFERENCES cfb_engine_studies(study_id,study_version)
);
CREATE TABLE IF NOT EXISTS cfb_engine_evaluations (
  evaluation_id uuid PRIMARY KEY, study_id uuid NOT NULL, study_version integer NOT NULL, window_key text NOT NULL,
  report_revision integer NOT NULL CHECK(report_revision>0), input_manifest_id uuid NOT NULL REFERENCES cfb_context_manifests(manifest_id),
  report_artifact_id uuid NOT NULL REFERENCES cfb_engine_artifacts(artifact_id), evaluated_through timestamptz NOT NULL,
  result text NOT NULL CHECK(result IN ('pass','fail','inconclusive','invalid')),
  supersedes_evaluation_id uuid REFERENCES cfb_engine_evaluations(evaluation_id), idempotency_key text NOT NULL UNIQUE,
  FOREIGN KEY(study_id,study_version,window_key) REFERENCES cfb_engine_study_windows(study_id,study_version,window_key),
  UNIQUE(study_id,study_version,window_key,report_revision)
);
CREATE TABLE IF NOT EXISTS cfb_engine_evaluation_metrics (
  evaluation_id uuid NOT NULL REFERENCES cfb_engine_evaluations(evaluation_id), metric_key text NOT NULL, cohort_key text NOT NULL,
  unit text NOT NULL, value numeric, lower_bound numeric, upper_bound numeric, n_observations bigint NOT NULL CHECK(n_observations>=0),
  n_games bigint NOT NULL CHECK(n_games>=0), n_dates bigint NOT NULL CHECK(n_dates>=0), missing_count bigint NOT NULL CHECK(missing_count>=0),
  method_artifact_id uuid NOT NULL REFERENCES cfb_engine_artifacts(artifact_id), PRIMARY KEY(evaluation_id,metric_key,cohort_key)
);
CREATE TABLE IF NOT EXISTS cfb_engine_evaluation_legacy_links (
  evaluation_id uuid NOT NULL REFERENCES cfb_engine_evaluations(evaluation_id), legacy_result_id bigint NOT NULL REFERENCES cfb_hypothesis_results(id),
  legacy_result_artifact_id uuid NOT NULL REFERENCES cfb_engine_artifacts(artifact_id), PRIMARY KEY(evaluation_id,legacy_result_id)
);

CREATE TABLE IF NOT EXISTS cfb_engine_erasures (
  erasure_id uuid PRIMARY KEY, evidence_policy_id uuid NOT NULL REFERENCES cfb_engine_evidence_policies(policy_id),
  reason_code text NOT NULL, authorized_by text NOT NULL, requested_at timestamptz NOT NULL, idempotency_key text NOT NULL UNIQUE
);
CREATE TABLE IF NOT EXISTS cfb_engine_erasure_targets (
  target_id uuid PRIMARY KEY, erasure_id uuid NOT NULL REFERENCES cfb_engine_erasures(erasure_id), artifact_id uuid REFERENCES cfb_engine_artifacts(artifact_id),
  source_id uuid REFERENCES cfb_engine_sources(source_id), manifest_id uuid REFERENCES cfb_context_manifests(manifest_id),
  quote_id uuid REFERENCES cfb_engine_quote_observations(quote_id), capture_id uuid REFERENCES cfb_engine_captures(capture_id),
  snapshot_id uuid REFERENCES cfb_context_snapshots(snapshot_id), field_path text,
  disposition text NOT NULL CHECK(disposition IN ('erase_bytes','erase_field','invalidate_replay')),
  CHECK(num_nonnulls(artifact_id,source_id,manifest_id,quote_id,capture_id,snapshot_id)=1),
  CHECK((disposition='erase_field')=(field_path IS NOT NULL)),
  UNIQUE NULLS NOT DISTINCT(erasure_id,artifact_id,source_id,manifest_id,quote_id,capture_id,snapshot_id,field_path,disposition)
);
DO $$ BEGIN
  IF NOT EXISTS(SELECT 1 FROM pg_constraint WHERE conname='cfb_erasure_target_owner_uq') THEN
    ALTER TABLE cfb_engine_erasure_targets ADD CONSTRAINT cfb_erasure_target_owner_uq UNIQUE(target_id,erasure_id);
  END IF;
END $$;
CREATE TABLE IF NOT EXISTS cfb_engine_erasure_events (
  erasure_id uuid NOT NULL REFERENCES cfb_engine_erasures(erasure_id), event_sequence integer NOT NULL CHECK(event_sequence>=0),
  target_id uuid REFERENCES cfb_engine_erasure_targets(target_id), state text NOT NULL CHECK(state IN ('planned','started','target_completed','failed','completed')),
  occurred_at timestamptz NOT NULL, execution_artifact_id uuid REFERENCES cfb_engine_artifacts(artifact_id), details jsonb NOT NULL,
  PRIMARY KEY(erasure_id,event_sequence)
);
DO $$ BEGIN
  ALTER TABLE cfb_engine_erasure_events ADD CONSTRAINT cfb_erasure_event_target_fk
    FOREIGN KEY(target_id,erasure_id) REFERENCES cfb_engine_erasure_targets(target_id,erasure_id);
EXCEPTION WHEN duplicate_object THEN NULL; END $$;
DO $$ BEGIN
  ALTER TABLE cfb_engine_erasure_events ADD CONSTRAINT cfb_erasure_event_target_state_ck CHECK(
    (state IN ('target_completed','failed') AND target_id IS NOT NULL) OR
    (state IN ('planned','started','completed')));
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

CREATE OR REPLACE FUNCTION cfb_require_typed_subject_bridge() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE k text; t text; n integer;
BEGIN
  IF TG_TABLE_NAME='cfb_engine_subjects' THEN
    k := CASE WHEN TG_OP='DELETE' THEN OLD.subject_key ELSE NEW.subject_key END;
  ELSIF TG_TABLE_NAME='cfb_engine_events' THEN
    k := CASE WHEN TG_OP='DELETE' THEN OLD.event_key ELSE NEW.event_key END;
  ELSE
    k := CASE WHEN TG_OP='DELETE' THEN OLD.team_key ELSE NEW.team_key END;
  END IF;
  SELECT entity_type INTO t FROM cfb_engine_subjects WHERE subject_key=k;
  SELECT (CASE WHEN EXISTS(SELECT 1 FROM cfb_engine_events WHERE event_key=k) THEN 1 ELSE 0 END +
          CASE WHEN EXISTS(SELECT 1 FROM cfb_engine_teams WHERE team_key=k) THEN 1 ELSE 0 END) INTO n;
  IF t IS NULL OR n<>1 OR (t='event' AND NOT EXISTS(SELECT 1 FROM cfb_engine_events WHERE event_key=k))
     OR (t='team' AND NOT EXISTS(SELECT 1 FROM cfb_engine_teams WHERE team_key=k)) THEN
    RAISE EXCEPTION 'subject % must have exactly one correctly typed bridge', k;
  END IF;
  RETURN NULL;
END $$;
DROP TRIGGER IF EXISTS cfb_subject_bridge_subject_check ON cfb_engine_subjects;
CREATE CONSTRAINT TRIGGER cfb_subject_bridge_subject_check AFTER INSERT OR UPDATE ON cfb_engine_subjects
  DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION cfb_require_typed_subject_bridge();
DROP TRIGGER IF EXISTS cfb_subject_bridge_event_check ON cfb_engine_events;
CREATE CONSTRAINT TRIGGER cfb_subject_bridge_event_check AFTER INSERT OR UPDATE OR DELETE ON cfb_engine_events
  DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION cfb_require_typed_subject_bridge();
DROP TRIGGER IF EXISTS cfb_subject_bridge_team_check ON cfb_engine_teams;
CREATE CONSTRAINT TRIGGER cfb_subject_bridge_team_check AFTER INSERT OR UPDATE OR DELETE ON cfb_engine_teams
  DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION cfb_require_typed_subject_bridge();

CREATE OR REPLACE FUNCTION cfb_reject_frozen_update() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION '% is immutable; append a new revision', TG_TABLE_NAME; END $$;
DROP TRIGGER IF EXISTS cfb_study_immutable ON cfb_engine_studies;
CREATE TRIGGER cfb_study_immutable BEFORE UPDATE OR DELETE ON cfb_engine_studies FOR EACH ROW EXECUTE FUNCTION cfb_reject_frozen_update();
DROP TRIGGER IF EXISTS cfb_study_window_immutable ON cfb_engine_study_windows;
CREATE TRIGGER cfb_study_window_immutable BEFORE UPDATE OR DELETE ON cfb_engine_study_windows FOR EACH ROW EXECUTE FUNCTION cfb_reject_frozen_update();
DROP TRIGGER IF EXISTS cfb_evaluation_immutable ON cfb_engine_evaluations;
CREATE TRIGGER cfb_evaluation_immutable BEFORE UPDATE OR DELETE ON cfb_engine_evaluations FOR EACH ROW EXECUTE FUNCTION cfb_reject_frozen_update();
DROP TRIGGER IF EXISTS cfb_evaluation_metric_immutable ON cfb_engine_evaluation_metrics;
CREATE TRIGGER cfb_evaluation_metric_immutable BEFORE UPDATE OR DELETE ON cfb_engine_evaluation_metrics FOR EACH ROW EXECUTE FUNCTION cfb_reject_frozen_update();
DROP TRIGGER IF EXISTS cfb_study_dependency_immutable ON cfb_engine_study_dependencies;
CREATE TRIGGER cfb_study_dependency_immutable BEFORE UPDATE OR DELETE ON cfb_engine_study_dependencies FOR EACH ROW EXECUTE FUNCTION cfb_reject_frozen_update();
DROP TRIGGER IF EXISTS cfb_study_hypothesis_immutable ON cfb_engine_study_hypotheses;
CREATE TRIGGER cfb_study_hypothesis_immutable BEFORE UPDATE OR DELETE ON cfb_engine_study_hypotheses FOR EACH ROW EXECUTE FUNCTION cfb_reject_frozen_update();

CREATE OR REPLACE FUNCTION cfb_validate_manifest_edge() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.child_manifest_id IS NULL THEN RETURN NEW; END IF;
  IF NEW.child_manifest_id=NEW.manifest_id OR EXISTS(
    WITH RECURSIVE descendants(id) AS (
      SELECT NEW.child_manifest_id UNION
      SELECT i.child_manifest_id FROM cfb_context_manifest_items i JOIN descendants d ON i.manifest_id=d.id
      WHERE i.child_manifest_id IS NOT NULL)
    SELECT 1 FROM descendants WHERE id=NEW.manifest_id) THEN
    RAISE EXCEPTION 'manifest graph cycle';
  END IF;
  RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS cfb_manifest_acyclic ON cfb_context_manifest_items;
CREATE TRIGGER cfb_manifest_acyclic BEFORE INSERT ON cfb_context_manifest_items FOR EACH ROW EXECUTE FUNCTION cfb_validate_manifest_edge();

CREATE OR REPLACE FUNCTION cfb_validate_slot_schema() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE expected uuid;
BEGIN
  SELECT payload_schema_id INTO expected FROM cfb_context_definitions
   WHERE definition_id=NEW.definition_id AND definition_version=NEW.definition_version;
  IF expected IS DISTINCT FROM NEW.payload_schema_id THEN RAISE EXCEPTION 'slot schema does not match definition'; END IF;
  RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS cfb_slot_schema_check ON cfb_policy_slot_versions;
CREATE TRIGGER cfb_slot_schema_check BEFORE INSERT OR UPDATE ON cfb_policy_slot_versions FOR EACH ROW EXECUTE FUNCTION cfb_validate_slot_schema();

CREATE OR REPLACE FUNCTION cfb_validate_decision_binding() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE expected uuid;
BEGIN
  IF NEW.policy_generation IS NULL THEN RETURN NEW; END IF;
  SELECT policy_id INTO expected FROM cfb_context_binding_revisions
    WHERE consumer_id=NEW.consumer_id AND policy_generation=NEW.policy_generation;
  IF expected IS DISTINCT FROM NEW.policy_id THEN RAISE EXCEPTION 'decision policy does not match binding revision'; END IF;
  RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS cfb_decision_binding_check ON cfb_context_policy_decisions;
CREATE CONSTRAINT TRIGGER cfb_decision_binding_check AFTER INSERT OR UPDATE ON cfb_context_policy_decisions
  DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION cfb_validate_decision_binding();

CREATE OR REPLACE FUNCTION cfb_validate_erasure_completion() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.state<>'completed' THEN RETURN NEW; END IF;
  IF NOT EXISTS(SELECT 1 FROM cfb_engine_erasure_targets t WHERE t.erasure_id=NEW.erasure_id) OR EXISTS(
    SELECT 1 FROM cfb_engine_erasure_targets t WHERE t.erasure_id=NEW.erasure_id AND NOT EXISTS(
      SELECT 1 FROM cfb_engine_erasure_events e WHERE e.erasure_id=NEW.erasure_id
       AND e.target_id=t.target_id AND e.state='target_completed')) THEN
    RAISE EXCEPTION 'erasure cannot complete before every target completes';
  END IF;
  RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS cfb_erasure_completion_check ON cfb_engine_erasure_events;
CREATE CONSTRAINT TRIGGER cfb_erasure_completion_check AFTER INSERT ON cfb_engine_erasure_events
  DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION cfb_validate_erasure_completion();
"""
