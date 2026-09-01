import { sql } from "drizzle-orm";

import { db } from ".";

let ensurePolymarketWatchlistTablesPromise: Promise<void> | null = null;
let ensureDkPlayerPropColumnsPromise: Promise<void> | null = null;
let ensureProjectionExperimentTablesPromise: Promise<void> | null = null;
let ensureOwnershipExperimentTablesPromise: Promise<void> | null = null;
let ensureMlbBlowupTrackingTablesPromise: Promise<void> | null = null;
let ensureMlbHomerunTrackingTablesPromise: Promise<void> | null = null;
let ensureOddsSignalTablesPromise: Promise<void> | null = null;
let ensureOddsHistoryTablesPromise: Promise<void> | null = null;
let ensureAnalyticsColumnsPromise: Promise<void> | null = null;
let ensureVideoAnalysisTablesPromise: Promise<void> | null = null;
let ensureYoutubePickChannelsTablePromise: Promise<void> | null = null;
let ensureOddsApiPropFetchLogPromise: Promise<void> | null = null;
let ensureMlbGamePredictionTablesPromise: Promise<void> | null = null;
let ensureFantasyFootballTablesPromise: Promise<void> | null = null;
let ensureSurvivorTablesPromise: Promise<void> | null = null;

const FANTASY_FOOTBALL_DDLS = [
  `CREATE TABLE IF NOT EXISTS ff_source_snapshots (id BIGSERIAL PRIMARY KEY, source TEXT NOT NULL, dataset TEXT NOT NULL, season INTEGER NOT NULL, scoring TEXT, ranking_type TEXT, request_params JSONB NOT NULL DEFAULT '{}'::jsonb, source_updated_at TIMESTAMPTZ, fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), response_hash TEXT NOT NULL, row_count INTEGER NOT NULL, matched_count INTEGER NOT NULL DEFAULT 0, unmatched_count INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL, error_summary TEXT, UNIQUE(source, dataset, response_hash))`,
  `ALTER TABLE ff_source_snapshots ADD COLUMN IF NOT EXISTS week INTEGER`,
  `ALTER TABLE ff_source_snapshots ADD COLUMN IF NOT EXISTS contract_key TEXT`,
  `ALTER TABLE ff_source_snapshots ADD COLUMN IF NOT EXISTS source_published_at TIMESTAMPTZ`,
  `ALTER TABLE ff_source_snapshots ADD COLUMN IF NOT EXISTS available_at TIMESTAMPTZ`,
  `ALTER TABLE ff_source_snapshots ADD COLUMN IF NOT EXISTS as_of_at TIMESTAMPTZ`,
  `ALTER TABLE ff_source_snapshots ADD COLUMN IF NOT EXISTS missingness JSONB NOT NULL DEFAULT '{}'::jsonb`,
  `ALTER TABLE ff_source_snapshots ADD COLUMN IF NOT EXISTS fallback_tier TEXT`,
  `ALTER TABLE ff_source_snapshots ADD COLUMN IF NOT EXISTS confidence_multiplier DOUBLE PRECISION NOT NULL DEFAULT 1.0`,
  `ALTER TABLE ff_source_snapshots ADD COLUMN IF NOT EXISTS model_eligible BOOLEAN NOT NULL DEFAULT TRUE`,
  `ALTER TABLE ff_source_snapshots ADD COLUMN IF NOT EXISTS eligibility_reason TEXT`,
  `CREATE TABLE IF NOT EXISTS ff_players (id BIGSERIAL PRIMARY KEY, season INTEGER NOT NULL, canonical_name TEXT NOT NULL, normalized_name TEXT NOT NULL, position TEXT NOT NULL, nfl_team_id INTEGER REFERENCES nfl_teams(team_id), team_abbrev TEXT, fantasypros_player_id INTEGER, sleeper_player_id TEXT, gsis_id TEXT, espn_id TEXT, yahoo_id TEXT, mfl_id TEXT, draftkings_id TEXT, active BOOLEAN NOT NULL DEFAULT TRUE, rookie BOOLEAN NOT NULL DEFAULT FALSE, bye_week INTEGER, injury_status TEXT, metadata JSONB NOT NULL DEFAULT '{}'::jsonb, fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), UNIQUE(season, fantasypros_player_id))`,
  `CREATE TABLE IF NOT EXISTS ff_player_injury_observations (id BIGSERIAL PRIMARY KEY, player_id BIGINT NOT NULL REFERENCES ff_players(id) ON DELETE CASCADE, season INTEGER NOT NULL, source TEXT NOT NULL, source_snapshot_id BIGINT REFERENCES ff_source_snapshots(id) ON DELETE SET NULL, source_status TEXT, normalized_status TEXT NOT NULL, body_part TEXT, injury_type TEXT, description TEXT, practice_status TEXT, injury_started_at TIMESTAMPTZ, provider_updated_at TIMESTAMPTZ, expected_return_min DATE, expected_return_max DATE, weeks_out_min DOUBLE PRECISION, weeks_out_max DOUBLE PRECISION, availability_probability DOUBLE PRECISION, raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb, response_hash TEXT NOT NULL, observed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), UNIQUE(source_snapshot_id, player_id, source))`,
  `CREATE TABLE IF NOT EXISTS ff_player_injuries (id BIGSERIAL PRIMARY KEY, player_id BIGINT NOT NULL REFERENCES ff_players(id) ON DELETE CASCADE, season INTEGER NOT NULL, status TEXT NOT NULL, body_part TEXT, injury_type TEXT, first_seen_at TIMESTAMPTZ NOT NULL, last_confirmed_at TIMESTAMPTZ NOT NULL, cleared_at TIMESTAMPTZ, expected_return_min DATE, expected_return_max DATE, weeks_out_min DOUBLE PRECISION, weeks_out_max DOUBLE PRECISION, estimate_basis TEXT NOT NULL DEFAULT 'unknown', confidence DOUBLE PRECISION, primary_source TEXT NOT NULL, source_conflict BOOLEAN NOT NULL DEFAULT FALSE, active BOOLEAN NOT NULL DEFAULT TRUE)`,
  `CREATE TABLE IF NOT EXISTS ff_injury_events (id BIGSERIAL PRIMARY KEY, injury_id BIGINT REFERENCES ff_player_injuries(id) ON DELETE CASCADE, player_id BIGINT NOT NULL REFERENCES ff_players(id) ON DELETE CASCADE, observation_id BIGINT REFERENCES ff_player_injury_observations(id) ON DELETE SET NULL, event_type TEXT NOT NULL, previous_state JSONB NOT NULL DEFAULT '{}'::jsonb, new_state JSONB NOT NULL DEFAULT '{}'::jsonb, source TEXT NOT NULL, event_key TEXT NOT NULL UNIQUE, occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW())`,
  `CREATE TABLE IF NOT EXISTS ff_player_source_projections (id BIGSERIAL PRIMARY KEY, source_snapshot_id BIGINT NOT NULL REFERENCES ff_source_snapshots(id) ON DELETE CASCADE, player_id BIGINT NOT NULL REFERENCES ff_players(id) ON DELETE CASCADE, source TEXT NOT NULL, season INTEGER NOT NULL, scoring TEXT NOT NULL, projected_points DOUBLE PRECISION, projected_stats JSONB NOT NULL DEFAULT '{}'::jsonb, match_method TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), UNIQUE(source_snapshot_id, player_id, scoring), CHECK(scoring IN ('STD','HALF','PPR')))`,
  `CREATE TABLE IF NOT EXISTS ff_ranking_sets (id BIGSERIAL PRIMARY KEY, season INTEGER NOT NULL, name TEXT NOT NULL, source TEXT NOT NULL, source_snapshot_id BIGINT REFERENCES ff_source_snapshots(id), source_date DATE, scoring_profile JSONB NOT NULL, ranking_type TEXT NOT NULL DEFAULT 'DRAFT', is_baseline BOOLEAN NOT NULL DEFAULT FALSE, import_summary JSONB, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW())`,
  `CREATE TABLE IF NOT EXISTS ff_player_rankings (id BIGSERIAL PRIMARY KEY, ranking_set_id BIGINT NOT NULL REFERENCES ff_ranking_sets(id) ON DELETE CASCADE, player_id BIGINT NOT NULL REFERENCES ff_players(id), overall_rank INTEGER, position_rank INTEGER, tier INTEGER, adp DOUBLE PRECISION, projected_points DOUBLE PRECISION, projection_low DOUBLE PRECISION, projection_high DOUBLE PRECISION, projected_stats JSONB, rank_min DOUBLE PRECISION, rank_max DOUBLE PRECISION, rank_std DOUBLE PRECISION, our_rank INTEGER, our_projected_points DOUBLE PRECISION, expected_games DOUBLE PRECISION, confidence DOUBLE PRECISION, source_row JSONB, notes TEXT, UNIQUE(ranking_set_id, player_id))`,
  `CREATE TABLE IF NOT EXISTS ff_player_season_features (id BIGSERIAL PRIMARY KEY, player_id BIGINT NOT NULL REFERENCES ff_players(id), season INTEGER NOT NULL, source TEXT NOT NULL, games INTEGER, fantasy_points_std DOUBLE PRECISION, fantasy_points_ppr DOUBLE PRECISION, targets DOUBLE PRECISION, receptions DOUBLE PRECISION, receiving_yards DOUBLE PRECISION, receiving_tds DOUBLE PRECISION, carries DOUBLE PRECISION, rushing_yards DOUBLE PRECISION, rushing_tds DOUBLE PRECISION, target_share DOUBLE PRECISION, rush_share DOUBLE PRECISION, team_target_rank INTEGER, team_rush_rank INTEGER, nfl_target_rank INTEGER, nfl_rush_td_rank INTEGER, source_row JSONB NOT NULL DEFAULT '{}'::jsonb, fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), UNIQUE(player_id, season, source))`,
  `CREATE TABLE IF NOT EXISTS ff_player_indicators (id BIGSERIAL PRIMARY KEY, ranking_set_id BIGINT NOT NULL REFERENCES ff_ranking_sets(id) ON DELETE CASCADE, player_id BIGINT NOT NULL REFERENCES ff_players(id), indicator_code TEXT NOT NULL, indicator_class TEXT NOT NULL, label TEXT NOT NULL, metric_value DOUBLE PRECISION, league_rank INTEGER, percentile DOUBLE PRECISION, confidence DOUBLE PRECISION, season INTEGER, related_player_id BIGINT REFERENCES ff_players(id), evidence JSONB NOT NULL DEFAULT '{}'::jsonb, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), UNIQUE(ranking_set_id, player_id, indicator_code))`,
  `CREATE TABLE IF NOT EXISTS ff_draft_sessions (id UUID PRIMARY KEY, owner_key TEXT, name TEXT NOT NULL, season INTEGER NOT NULL, status TEXT NOT NULL DEFAULT 'ready', draft_type TEXT NOT NULL DEFAULT 'snake', team_count INTEGER NOT NULL, controlled_slot INTEGER NOT NULL, round_count INTEGER NOT NULL, roster_config JSONB NOT NULL, scoring_config JSONB NOT NULL, recommendation_config JSONB NOT NULL DEFAULT '{}'::jsonb, ranking_set_id BIGINT NOT NULL REFERENCES ff_ranking_sets(id), sleeper_draft_id TEXT, current_pick INTEGER NOT NULL DEFAULT 1, revision INTEGER NOT NULL DEFAULT 0, started_at TIMESTAMPTZ, completed_at TIMESTAMPTZ, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW())`,
  `CREATE TABLE IF NOT EXISTS ff_draft_teams (id BIGSERIAL PRIMARY KEY, draft_id UUID NOT NULL REFERENCES ff_draft_sessions(id) ON DELETE CASCADE, slot INTEGER NOT NULL, name TEXT NOT NULL, is_controlled BOOLEAN NOT NULL DEFAULT FALSE, external_roster_id TEXT, UNIQUE(draft_id, slot))`,
  `CREATE TABLE IF NOT EXISTS ff_draft_slots (id BIGSERIAL PRIMARY KEY, draft_id UUID NOT NULL REFERENCES ff_draft_sessions(id) ON DELETE CASCADE, overall_pick INTEGER NOT NULL, round INTEGER NOT NULL, pick_in_round INTEGER NOT NULL, draft_team_id BIGINT NOT NULL REFERENCES ff_draft_teams(id), UNIQUE(draft_id, overall_pick))`,
  `CREATE TABLE IF NOT EXISTS ff_draft_events (id BIGSERIAL PRIMARY KEY, draft_id UUID NOT NULL REFERENCES ff_draft_sessions(id) ON DELETE CASCADE, event_type TEXT NOT NULL, overall_pick INTEGER, player_id BIGINT REFERENCES ff_players(id), draft_team_id BIGINT REFERENCES ff_draft_teams(id), source TEXT NOT NULL, external_pick_id TEXT, reverses_event_id BIGINT REFERENCES ff_draft_events(id), payload JSONB NOT NULL DEFAULT '{}'::jsonb, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW())`,
  `CREATE TABLE IF NOT EXISTS ff_draft_player_preferences (draft_id UUID NOT NULL REFERENCES ff_draft_sessions(id) ON DELETE CASCADE, player_id BIGINT NOT NULL REFERENCES ff_players(id), preference TEXT NOT NULL, note TEXT, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), PRIMARY KEY(draft_id, player_id))`,
  // Append-only ADP time series -- see db/schema.py for the canonical definition
  // and ingest/ff_adp_snapshot.py for the 12-hour capture job.
  `CREATE TABLE IF NOT EXISTS ff_adp_snapshots (id BIGSERIAL PRIMARY KEY, player_id BIGINT NOT NULL REFERENCES ff_players(id) ON DELETE CASCADE, season INTEGER NOT NULL, scoring TEXT NOT NULL, captured_at TIMESTAMPTZ NOT NULL, source_snapshot_id BIGINT REFERENCES ff_source_snapshots(id), adp DOUBLE PRECISION NOT NULL, adp_stdev DOUBLE PRECISION, adp_high DOUBLE PRECISION, adp_low DOUBLE PRECISION, times_drafted INTEGER, UNIQUE(player_id, scoring, captured_at))`,
  // DraftKings' own Best Ball ADP -- manual, cookie-gated capture, see
  // db/schema.py and ingest/ff_dk_bestball_adp.py for the full contract.
  `CREATE TABLE IF NOT EXISTS ff_dk_bestball_adp (id BIGSERIAL PRIMARY KEY, draft_group_id INTEGER NOT NULL, season INTEGER NOT NULL, dk_player_id BIGINT NOT NULL, player_id BIGINT REFERENCES ff_players(id) ON DELETE SET NULL, display_name TEXT NOT NULL, dk_team_id INTEGER, average_draft_position DOUBLE PRECISION, draft_percentage DOUBLE PRECISION, rank INTEGER, is_available BOOLEAN NOT NULL DEFAULT TRUE, captured_at TIMESTAMPTZ NOT NULL, source_snapshot_id BIGINT REFERENCES ff_source_snapshots(id), UNIQUE(draft_group_id, dk_player_id, captured_at))`,
  `CREATE TABLE IF NOT EXISTS ff_yahoo_predraft_captures (source_snapshot_id BIGINT PRIMARY KEY REFERENCES ff_source_snapshots(id) ON DELETE CASCADE, season INTEGER NOT NULL, captured_at TIMESTAMPTZ NOT NULL, raw_text TEXT NOT NULL, format_version TEXT NOT NULL DEFAULT 'yahoo-paste-v1', source_label TEXT NOT NULL DEFAULT 'Yahoo Fantasy Pre-Draft Rankings')`,
  `CREATE TABLE IF NOT EXISTS ff_yahoo_predraft_rankings (id BIGSERIAL PRIMARY KEY, source_snapshot_id BIGINT NOT NULL REFERENCES ff_source_snapshots(id) ON DELETE CASCADE, season INTEGER NOT NULL, player_id BIGINT REFERENCES ff_players(id) ON DELETE SET NULL, source_order INTEGER NOT NULL, display_name TEXT NOT NULL, position TEXT NOT NULL, team_abbrev TEXT, bye_week INTEGER, xrank DOUBLE PRECISION NOT NULL, adp DOUBLE PRECISION, captured_at TIMESTAMPTZ NOT NULL, match_method TEXT NOT NULL, raw_row JSONB NOT NULL DEFAULT '{}'::jsonb, UNIQUE(source_snapshot_id, source_order))`,
  // Hand-written per-player scouting notes, authored in the /fantasy-football/notes
  // admin page and surfaced as the tooltip on the redraft board, the Best Ball
  // board, and the Best Ball Shadow panel. Editorial only -- read by the display
  // layer, never by a projection, VOR, rank, or ADP calculation.
  //
  // Written by the web app, not by Python -- same ownership as
  // youtube_pick_channels. player_id is safe as the key because the ingest
  // resolves-then-UPDATEs existing ff_players rows (never re-inserting) and
  // nothing in the repo deletes them, so the id survives every refresh.
  //
  // Notes are SEASON-SCOPED as a consequence: ff_players is keyed by season, so
  // a 2027 refresh creates new player rows and 2026 notes stay attached to the
  // 2026 rows rather than silently following a player forward. normalized_name
  // and position are denormalized here so a deliberate carry-forward is a
  // re-link by name rather than a re-type.
  `CREATE TABLE IF NOT EXISTS ff_player_notes (
      id BIGSERIAL PRIMARY KEY,
      player_id BIGINT NOT NULL REFERENCES ff_players(id) ON DELETE CASCADE,
      season INTEGER NOT NULL,
      normalized_name TEXT NOT NULL,
      position TEXT NOT NULL,
      verdict TEXT NOT NULL,
      verdict_label TEXT NOT NULL,
      note TEXT NOT NULL,
      list_rank INTEGER,
      source_team TEXT,
      source_adp DOUBLE PRECISION,
      author TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      category TEXT NOT NULL DEFAULT 'draft-board',
      UNIQUE(player_id, category),
      CHECK (verdict IN ('target','fair','caution','fade'))
    )`,
  // Migration for tables created before categories existed: a player may now
  // carry one note per list, so the old UNIQUE(player_id) has to give way to
  // UNIQUE(player_id, category). Written as idempotent ALTERs because the
  // CREATE above is IF NOT EXISTS and never re-runs on an existing table.
  // Existing rows are the draft-board list, which is what the column defaults to.
  `ALTER TABLE ff_player_notes ADD COLUMN IF NOT EXISTS category TEXT NOT NULL DEFAULT 'draft-board'`,
  `ALTER TABLE ff_player_notes DROP CONSTRAINT IF EXISTS ff_player_notes_player_id_key`,
  `CREATE UNIQUE INDEX IF NOT EXISTS idx_ff_player_notes_player_category ON ff_player_notes(player_id, category)`,
  `CREATE INDEX IF NOT EXISTS idx_ff_player_notes_season ON ff_player_notes(season, updated_at DESC)`,
  `CREATE TABLE IF NOT EXISTS ff_v2_context_runs (run_id UUID PRIMARY KEY, transform_version TEXT NOT NULL, seasons JSONB NOT NULL, source_snapshot_ids JSONB NOT NULL, coverage_report JSONB NOT NULL, artifact_digest TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), UNIQUE(transform_version, artifact_digest))`,
  `CREATE TABLE IF NOT EXISTS ff_v2_team_week_context (id BIGSERIAL PRIMARY KEY, run_id UUID NOT NULL REFERENCES ff_v2_context_runs(run_id) ON DELETE CASCADE, season INTEGER NOT NULL, week INTEGER NOT NULL, team TEXT NOT NULL, is_bye BOOLEAN NOT NULL, game_id TEXT, game_date DATE, kickoff_at TIMESTAMPTZ, opponent TEXT, is_home BOOLEAN, location TEXT, stadium TEXT, stadium_id TEXT, roof TEXT, surface TEXT, quarterback_gsis_id TEXT, quarterback_name TEXT, head_coach TEXT, play_caller_id TEXT, source_snapshot_id BIGINT NOT NULL REFERENCES ff_source_snapshots(id), row_digest TEXT NOT NULL, observed_at TIMESTAMPTZ NOT NULL, UNIQUE(run_id, season, week, team))`,
  `CREATE TABLE IF NOT EXISTS ff_v2_roster_weeks (id BIGSERIAL PRIMARY KEY, run_id UUID NOT NULL REFERENCES ff_v2_context_runs(run_id) ON DELETE CASCADE, season INTEGER NOT NULL, week INTEGER NOT NULL, player_gsis_id TEXT NOT NULL, player_name TEXT NOT NULL, position TEXT, depth_chart_position TEXT, team TEXT NOT NULL, roster_status TEXT, resolution_method TEXT NOT NULL, effective_at TIMESTAMPTZ NOT NULL, source_snapshot_id BIGINT NOT NULL REFERENCES ff_source_snapshots(id), row_digest TEXT NOT NULL, observed_at TIMESTAMPTZ NOT NULL, UNIQUE(run_id, season, week, player_gsis_id))`,
  `CREATE TABLE IF NOT EXISTS ff_v2_transactions (id BIGSERIAL PRIMARY KEY, run_id UUID NOT NULL REFERENCES ff_v2_context_runs(run_id) ON DELETE CASCADE, player_gsis_id TEXT NOT NULL, player_name TEXT NOT NULL, from_team TEXT, to_team TEXT NOT NULL, effective_at TIMESTAMPTZ NOT NULL, transaction_type TEXT NOT NULL, source_snapshot_id BIGINT NOT NULL REFERENCES ff_source_snapshots(id), evidence JSONB NOT NULL DEFAULT '{}'::jsonb, row_digest TEXT NOT NULL, observed_at TIMESTAMPTZ NOT NULL, UNIQUE(run_id, player_gsis_id, effective_at, to_team))`,
  `CREATE TABLE IF NOT EXISTS ff_v2_team_week_facts (id BIGSERIAL PRIMARY KEY, run_id UUID NOT NULL REFERENCES ff_v2_context_runs(run_id) ON DELETE CASCADE, season INTEGER NOT NULL, week INTEGER NOT NULL, game_id TEXT NOT NULL, game_date DATE NOT NULL, team TEXT NOT NULL, opponent TEXT NOT NULL, plays INTEGER NOT NULL, drives INTEGER NOT NULL, pass_attempts INTEGER NOT NULL, dropbacks INTEGER NOT NULL, sacks INTEGER NOT NULL, allocatable_targets INTEGER NOT NULL, rush_attempts INTEGER NOT NULL, rb_carries INTEGER NOT NULL, rb_targets INTEGER NOT NULL, pass_touchdowns INTEGER NOT NULL, rush_touchdowns INTEGER NOT NULL, red_zone_trips INTEGER NOT NULL, goal_line_carries INTEGER NOT NULL, end_zone_targets INTEGER NOT NULL, neutral_pass_rate DOUBLE PRECISION, seconds_per_play DOUBLE PRECISION, score_state_features JSONB NOT NULL DEFAULT '{}'::jsonb, quarterback_gsis_id TEXT, quarterback_name TEXT, play_caller_id TEXT, source_snapshot_ids JSONB NOT NULL, derivation JSONB NOT NULL, fact_digest TEXT NOT NULL, observed_at TIMESTAMPTZ NOT NULL, UNIQUE(run_id, game_id, team))`,
  `CREATE TABLE IF NOT EXISTS ff_v2_team_opportunity_forecast_runs (run_id UUID PRIMARY KEY, contract_version TEXT NOT NULL, context_run_id UUID NOT NULL REFERENCES ff_v2_context_runs(run_id), model_version TEXT NOT NULL, calibration_version TEXT NOT NULL, as_of_at TIMESTAMPTZ NOT NULL, source_snapshot_ids JSONB NOT NULL, model_config JSONB NOT NULL DEFAULT '{}'::jsonb, forecast_count INTEGER NOT NULL, artifact_digest TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), UNIQUE(contract_version, artifact_digest))`,
  `CREATE TABLE IF NOT EXISTS ff_v2_team_opportunity_forecasts (id BIGSERIAL PRIMARY KEY, forecast_run_id UUID NOT NULL REFERENCES ff_v2_team_opportunity_forecast_runs(run_id) ON DELETE CASCADE, context_fact_id BIGINT NOT NULL REFERENCES ff_v2_team_week_facts(id), context_fact_digest TEXT NOT NULL, season INTEGER NOT NULL, week INTEGER NOT NULL, game_id TEXT NOT NULL, game_date DATE NOT NULL, team TEXT NOT NULL, opponent TEXT NOT NULL, fallback_tier TEXT NOT NULL, confidence_multiplier DOUBLE PRECISION NOT NULL, source_snapshot_ids JSONB NOT NULL, feature_provenance JSONB NOT NULL, forecast_digest TEXT NOT NULL, as_of_at TIMESTAMPTZ NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), UNIQUE(forecast_run_id, game_id, team), CHECK(fallback_tier IN ('A', 'B', 'C')), CHECK(confidence_multiplier > 0 AND confidence_multiplier <= 1))`,
  `CREATE TABLE IF NOT EXISTS ff_v2_team_opportunity_distributions (id BIGSERIAL PRIMARY KEY, forecast_id BIGINT NOT NULL REFERENCES ff_v2_team_opportunity_forecasts(id) ON DELETE CASCADE, opportunity_type TEXT NOT NULL, expected_value DOUBLE PRECISION NOT NULL, dispersion DOUBLE PRECISION NOT NULL, p10 DOUBLE PRECISION NOT NULL, p50 DOUBLE PRECISION NOT NULL, p90 DOUBLE PRECISION NOT NULL, distribution_family TEXT NOT NULL, parameters JSONB NOT NULL DEFAULT '{}'::jsonb, distribution_digest TEXT NOT NULL, UNIQUE(forecast_id, opportunity_type), CHECK(opportunity_type IN ('plays', 'pass_attempts', 'allocatable_targets', 'rush_attempts', 'rb_carries', 'rb_targets', 'pass_touchdowns', 'rush_touchdowns')), CHECK(expected_value >= 0 AND dispersion >= 0), CHECK(p10 >= 0 AND p10 <= p50 AND p50 <= p90))`,
  `CREATE TABLE IF NOT EXISTS ff_v2_backtest_runs (run_id UUID PRIMARY KEY, harness_version TEXT NOT NULL, status TEXT NOT NULL, context_run_id UUID NOT NULL REFERENCES ff_v2_context_runs(run_id), model_version TEXT NOT NULL, calibration_version TEXT NOT NULL, seed BIGINT NOT NULL, evaluation_seasons JSONB NOT NULL, preseason_cutoffs JSONB NOT NULL, source_snapshot_ids JSONB NOT NULL, cohort_counts JSONB NOT NULL, config JSONB NOT NULL, output_digest TEXT NOT NULL, artifact_path TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), UNIQUE(harness_version, output_digest))`,
  `CREATE TABLE IF NOT EXISTS ff_v2_backtest_splits (id BIGSERIAL PRIMARY KEY, run_id UUID NOT NULL REFERENCES ff_v2_backtest_runs(run_id) ON DELETE CASCADE, evaluation_season INTEGER NOT NULL, preseason_cutoff TIMESTAMPTZ NOT NULL, training_seasons JSONB NOT NULL, training_row_counts JSONB NOT NULL, evaluation_row_counts JSONB NOT NULL, training_digest TEXT NOT NULL, evaluation_digest TEXT NOT NULL, split_digest TEXT NOT NULL, scorable BOOLEAN NOT NULL, exclusion_reason TEXT, UNIQUE(run_id, evaluation_season))`,
  `CREATE INDEX IF NOT EXISTS idx_ff_rank_sets_latest ON ff_ranking_sets(season, ranking_type, created_at DESC)`,
  `CREATE INDEX IF NOT EXISTS idx_ff_rankings_board ON ff_player_rankings(ranking_set_id, COALESCE(our_rank, overall_rank))`,
  `CREATE INDEX IF NOT EXISTS idx_ff_drafts_recent ON ff_draft_sessions(updated_at DESC)`,
  `CREATE INDEX IF NOT EXISTS idx_ff_adp_snapshots_player ON ff_adp_snapshots(player_id, scoring, captured_at DESC)`,
  `CREATE INDEX IF NOT EXISTS idx_ff_adp_snapshots_captured ON ff_adp_snapshots(season, scoring, captured_at DESC)`,
  `CREATE INDEX IF NOT EXISTS idx_ff_dk_bestball_adp_group ON ff_dk_bestball_adp(draft_group_id, captured_at DESC)`,
  `CREATE INDEX IF NOT EXISTS idx_ff_dk_bestball_adp_player ON ff_dk_bestball_adp(player_id, captured_at DESC)`,
  `CREATE INDEX IF NOT EXISTS idx_ff_yahoo_predraft_player ON ff_yahoo_predraft_rankings(player_id, captured_at DESC)`,
  `CREATE INDEX IF NOT EXISTS idx_ff_yahoo_predraft_snapshot ON ff_yahoo_predraft_rankings(source_snapshot_id, source_order)`,
  `CREATE INDEX IF NOT EXISTS idx_ff_injury_observations_player ON ff_player_injury_observations(player_id, observed_at DESC)`,
  `CREATE INDEX IF NOT EXISTS idx_ff_injury_observations_source ON ff_player_injury_observations(source, season, observed_at DESC)`,
  `CREATE UNIQUE INDEX IF NOT EXISTS idx_ff_player_injuries_one_active ON ff_player_injuries(player_id) WHERE active`,
  `CREATE INDEX IF NOT EXISTS idx_ff_player_injuries_active ON ff_player_injuries(season, active, status)`,
  `CREATE INDEX IF NOT EXISTS idx_ff_injury_events_recent ON ff_injury_events(player_id, occurred_at DESC)`,
];

// Columns added to dk_slates / dk_players after the initial table creation.
// ALTER TABLE ... ADD COLUMN IF NOT EXISTS is idempotent — safe to run every deploy.
const DK_SLATE_COLUMN_DDLS = [
  `ALTER TABLE dk_slates ADD COLUMN IF NOT EXISTS cash_line DOUBLE PRECISION`,
  `ALTER TABLE dk_players ADD COLUMN IF NOT EXISTS proj_floor REAL`,
  `ALTER TABLE dk_players ADD COLUMN IF NOT EXISTS proj_ceiling REAL`,
  `ALTER TABLE dk_players ADD COLUMN IF NOT EXISTS boom_rate REAL`,
];

const DK_PLAYER_PROP_COLUMN_DDLS = [
  `ALTER TABLE dk_players ADD COLUMN IF NOT EXISTS prop_pts_price INTEGER`,
  `ALTER TABLE dk_players ADD COLUMN IF NOT EXISTS prop_pts_book TEXT`,
  `ALTER TABLE dk_players ADD COLUMN IF NOT EXISTS prop_reb_price INTEGER`,
  `ALTER TABLE dk_players ADD COLUMN IF NOT EXISTS prop_reb_book TEXT`,
  `ALTER TABLE dk_players ADD COLUMN IF NOT EXISTS prop_ast_price INTEGER`,
  `ALTER TABLE dk_players ADD COLUMN IF NOT EXISTS prop_ast_book TEXT`,
  `ALTER TABLE dk_players ADD COLUMN IF NOT EXISTS prop_blk REAL`,
  `ALTER TABLE dk_players ADD COLUMN IF NOT EXISTS prop_blk_price INTEGER`,
  `ALTER TABLE dk_players ADD COLUMN IF NOT EXISTS prop_blk_book TEXT`,
  `ALTER TABLE dk_players ADD COLUMN IF NOT EXISTS prop_stl REAL`,
  `ALTER TABLE dk_players ADD COLUMN IF NOT EXISTS prop_stl_price INTEGER`,
  `ALTER TABLE dk_players ADD COLUMN IF NOT EXISTS prop_stl_book TEXT`,
  `ALTER TABLE dk_players ADD COLUMN IF NOT EXISTS dk_in_starting_lineup BOOLEAN`,
  `ALTER TABLE dk_players ADD COLUMN IF NOT EXISTS dk_starting_lineup_order INTEGER`,
  `ALTER TABLE dk_players ADD COLUMN IF NOT EXISTS dk_team_lineup_confirmed BOOLEAN`,
  `ALTER TABLE dk_players ADD COLUMN IF NOT EXISTS expected_hr REAL`,
  `ALTER TABLE dk_players ADD COLUMN IF NOT EXISTS hr_prob_1plus REAL`,
  `ALTER TABLE dk_players ADD COLUMN IF NOT EXISTS actual_hr INTEGER`,
  `ALTER TABLE dk_players ADD COLUMN IF NOT EXISTS live_proj REAL`,
  `ALTER TABLE dk_players ADD COLUMN IF NOT EXISTS live_leverage REAL`,
  `ALTER TABLE dk_players ADD COLUMN IF NOT EXISTS live_own_pct REAL`,
  `ALTER TABLE dk_players ADD COLUMN IF NOT EXISTS linestar_own_pct REAL`,
];

const PROJECTION_EXPERIMENT_DDLS = [
  `CREATE TABLE IF NOT EXISTS projection_runs (
      id SERIAL PRIMARY KEY,
      sport TEXT NOT NULL,
      slate_id INTEGER NOT NULL REFERENCES dk_slates(id) ON DELETE CASCADE,
      model_version TEXT NOT NULL,
      source TEXT NOT NULL,
      config_json JSONB NOT NULL DEFAULT '{}'::jsonb,
      notes TEXT,
      created_at TIMESTAMPTZ DEFAULT NOW()
    )`,
  `CREATE TABLE IF NOT EXISTS projection_player_snapshots (
      id SERIAL PRIMARY KEY,
      run_id INTEGER NOT NULL REFERENCES projection_runs(id) ON DELETE CASCADE,
      slate_id INTEGER NOT NULL REFERENCES dk_slates(id) ON DELETE CASCADE,
      dk_player_id BIGINT NOT NULL,
      name TEXT NOT NULL,
      team_id INTEGER,
      salary INTEGER NOT NULL,
      is_out BOOLEAN DEFAULT FALSE,
      model_proj_fpts REAL,
      market_proj_fpts REAL,
      linestar_proj_fpts REAL,
      final_proj_fpts REAL,
      model_confidence REAL,
      market_confidence REAL,
      ls_confidence REAL,
      model_weight REAL,
      market_weight REAL,
      ls_weight REAL,
      flags_json JSONB NOT NULL DEFAULT '[]'::jsonb,
      model_stats_json JSONB,
      market_stats_json JSONB,
      actual_fpts REAL,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      UNIQUE(run_id, dk_player_id)
    )`,
  `CREATE INDEX IF NOT EXISTS idx_projection_runs_slate ON projection_runs(slate_id, created_at DESC)`,
  `CREATE INDEX IF NOT EXISTS idx_projection_runs_model ON projection_runs(model_version, created_at DESC)`,
  `CREATE INDEX IF NOT EXISTS idx_projection_snapshots_run ON projection_player_snapshots(run_id, dk_player_id)`,
  `CREATE INDEX IF NOT EXISTS idx_projection_snapshots_slate ON projection_player_snapshots(slate_id, dk_player_id)`,
];

const OWNERSHIP_EXPERIMENT_DDLS = [
  `CREATE TABLE IF NOT EXISTS ownership_runs (
      id SERIAL PRIMARY KEY,
      sport TEXT NOT NULL,
      slate_id INTEGER NOT NULL REFERENCES dk_slates(id) ON DELETE CASCADE,
      ownership_version TEXT NOT NULL,
      source TEXT NOT NULL,
      config_json JSONB NOT NULL DEFAULT '{}'::jsonb,
      notes TEXT,
      created_at TIMESTAMPTZ DEFAULT NOW()
    )`,
  `CREATE TABLE IF NOT EXISTS ownership_player_snapshots (
      id SERIAL PRIMARY KEY,
      run_id INTEGER NOT NULL REFERENCES ownership_runs(id) ON DELETE CASCADE,
      slate_id INTEGER NOT NULL REFERENCES dk_slates(id) ON DELETE CASCADE,
      dk_player_id BIGINT NOT NULL,
      name TEXT NOT NULL,
      team_id INTEGER,
      salary INTEGER NOT NULL,
      eligible_positions TEXT,
      is_out BOOLEAN DEFAULT FALSE,
      linestar_proj_fpts REAL,
      our_proj_fpts REAL,
      live_proj_fpts REAL,
      linestar_own_pct REAL,
      field_own_pct REAL,
      our_own_pct REAL,
      live_own_pct REAL,
      actual_own_pct REAL,
      lineup_order INTEGER,
      lineup_confirmed BOOLEAN,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      UNIQUE(run_id, dk_player_id)
    )`,
  `CREATE INDEX IF NOT EXISTS idx_ownership_runs_slate ON ownership_runs(slate_id, created_at DESC)`,
  `CREATE INDEX IF NOT EXISTS idx_ownership_runs_model ON ownership_runs(ownership_version, created_at DESC)`,
  `CREATE INDEX IF NOT EXISTS idx_ownership_snapshots_run ON ownership_player_snapshots(run_id, dk_player_id)`,
  `CREATE INDEX IF NOT EXISTS idx_ownership_snapshots_slate ON ownership_player_snapshots(slate_id, dk_player_id)`,
];

const MLB_BLOWUP_TRACKING_DDLS = [
  `CREATE TABLE IF NOT EXISTS mlb_blowup_runs (
      id SERIAL PRIMARY KEY,
      slate_id INTEGER NOT NULL REFERENCES dk_slates(id) ON DELETE CASCADE,
      analysis_version TEXT NOT NULL,
      source TEXT NOT NULL,
      config_json JSONB NOT NULL DEFAULT '{}'::jsonb,
      notes TEXT,
      created_at TIMESTAMPTZ DEFAULT NOW()
    )`,
  `CREATE TABLE IF NOT EXISTS mlb_blowup_player_snapshots (
      id SERIAL PRIMARY KEY,
      run_id INTEGER NOT NULL REFERENCES mlb_blowup_runs(id) ON DELETE CASCADE,
      slate_id INTEGER NOT NULL REFERENCES dk_slates(id) ON DELETE CASCADE,
      dk_player_id BIGINT NOT NULL,
      name TEXT NOT NULL,
      team_id INTEGER,
      team_abbrev TEXT,
      salary INTEGER NOT NULL,
      eligible_positions TEXT,
      lineup_order INTEGER,
      team_total REAL,
      projected_fpts REAL,
      projected_ceiling REAL,
      projected_value REAL,
      blowup_score REAL,
      candidate_rank INTEGER,
      actual_fpts REAL,
      actual_own_pct REAL,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      UNIQUE(run_id, dk_player_id)
    )`,
  `CREATE INDEX IF NOT EXISTS idx_mlb_blowup_runs_slate ON mlb_blowup_runs(slate_id, created_at DESC)`,
  `CREATE INDEX IF NOT EXISTS idx_mlb_blowup_runs_model ON mlb_blowup_runs(analysis_version, created_at DESC)`,
  `CREATE INDEX IF NOT EXISTS idx_mlb_blowup_snapshots_run ON mlb_blowup_player_snapshots(run_id, dk_player_id)`,
  `CREATE INDEX IF NOT EXISTS idx_mlb_blowup_snapshots_slate ON mlb_blowup_player_snapshots(slate_id, candidate_rank)`,
];

const MLB_HOMERUN_TRACKING_DDLS = [
  `ALTER TABLE dk_players ADD COLUMN IF NOT EXISTS actual_hr INTEGER`,
  `CREATE TABLE IF NOT EXISTS mlb_homerun_runs (
      id SERIAL PRIMARY KEY,
      slate_id INTEGER NOT NULL REFERENCES dk_slates(id) ON DELETE CASCADE,
      analysis_version TEXT NOT NULL,
      source TEXT NOT NULL,
      config_json JSONB NOT NULL DEFAULT '{}'::jsonb,
      notes TEXT,
      created_at TIMESTAMPTZ DEFAULT NOW()
    )`,
  `CREATE TABLE IF NOT EXISTS mlb_homerun_player_snapshots (
      id SERIAL PRIMARY KEY,
      run_id INTEGER NOT NULL REFERENCES mlb_homerun_runs(id) ON DELETE CASCADE,
      slate_id INTEGER NOT NULL REFERENCES dk_slates(id) ON DELETE CASCADE,
      dk_player_id BIGINT NOT NULL,
      name TEXT NOT NULL,
      team_id INTEGER,
      team_abbrev TEXT,
      salary INTEGER NOT NULL,
      eligible_positions TEXT,
      is_out BOOLEAN DEFAULT FALSE,
      lineup_order INTEGER,
      lineup_confirmed BOOLEAN,
      expected_hr REAL,
      hr_prob_1plus REAL,
      hitter_hr_pg REAL,
      hitter_iso REAL,
      hitter_slg REAL,
      hitter_pa_pg REAL,
      hitter_wrc_plus REAL,
      hitter_split_wrc_plus REAL,
      team_total REAL,
      vegas_total REAL,
      park_hr_factor REAL,
      weather_temp REAL,
      wind_speed REAL,
      opposing_pitcher_name TEXT,
      opposing_pitcher_hand TEXT,
      opposing_pitcher_hr_per_9 REAL,
      opposing_pitcher_hr_fb_pct REAL,
      opposing_pitcher_xfip REAL,
      opposing_pitcher_era REAL,
      actual_hr INTEGER,
      hit_hr_1plus BOOLEAN,
      actual_fpts REAL,
      actual_own_pct REAL,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      UNIQUE(run_id, dk_player_id)
    )`,
  `CREATE INDEX IF NOT EXISTS idx_mlb_homerun_runs_slate ON mlb_homerun_runs(slate_id, created_at DESC)`,
  `CREATE INDEX IF NOT EXISTS idx_mlb_homerun_runs_model ON mlb_homerun_runs(analysis_version, created_at DESC)`,
  `CREATE INDEX IF NOT EXISTS idx_mlb_homerun_snapshots_run ON mlb_homerun_player_snapshots(run_id, dk_player_id)`,
  `CREATE INDEX IF NOT EXISTS idx_mlb_homerun_snapshots_slate ON mlb_homerun_player_snapshots(slate_id, hr_prob_1plus DESC NULLS LAST)`,
];

const ODDS_SIGNAL_DDLS = [
  `CREATE TABLE IF NOT EXISTS odds_signal_runs (
      id SERIAL PRIMARY KEY,
      sport TEXT NOT NULL,
      slate_id INTEGER NOT NULL REFERENCES dk_slates(id) ON DELETE CASCADE,
      analysis_version TEXT NOT NULL,
      sample_size INTEGER NOT NULL DEFAULT 0,
      report_json JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      updated_at TIMESTAMPTZ DEFAULT NOW(),
      UNIQUE(slate_id)
    )`,
  `CREATE INDEX IF NOT EXISTS idx_odds_signal_runs_sport_created ON odds_signal_runs(sport, created_at DESC)`,
];

const ODDS_HISTORY_DDLS = [
  `ALTER TABLE nba_matchups ADD COLUMN IF NOT EXISTS home_spread DOUBLE PRECISION`,
  `CREATE TABLE IF NOT EXISTS nfl_teams (
      team_id SERIAL PRIMARY KEY,
      name TEXT NOT NULL UNIQUE,
      abbreviation TEXT NOT NULL UNIQUE,
      odds_api_name TEXT NOT NULL UNIQUE,
      city TEXT,
      conference TEXT,
      division TEXT,
      active BOOLEAN NOT NULL DEFAULT TRUE,
      logo_url TEXT DEFAULT '',
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )`,
  `CREATE TABLE IF NOT EXISTS nfl_matchups (
      id SERIAL PRIMARY KEY,
      event_id TEXT NOT NULL UNIQUE,
      season INTEGER,
      season_type TEXT,
      week INTEGER,
      game_date DATE NOT NULL,
      commence_time TIMESTAMPTZ NOT NULL,
      home_team_id INTEGER NOT NULL REFERENCES nfl_teams(team_id),
      away_team_id INTEGER NOT NULL REFERENCES nfl_teams(team_id),
      game_status TEXT,
      completed BOOLEAN NOT NULL DEFAULT FALSE,
      home_score INTEGER,
      away_score INTEGER,
      vegas_total DOUBLE PRECISION,
      home_ml INTEGER,
      away_ml INTEGER,
      home_spread DOUBLE PRECISION,
      vegas_prob_home DOUBLE PRECISION,
      home_implied DOUBLE PRECISION,
      away_implied DOUBLE PRECISION,
      fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      score_fetched_at TIMESTAMPTZ,
      final_at TIMESTAMPTZ,
      CHECK (home_team_id <> away_team_id)
    )`,
  `CREATE INDEX IF NOT EXISTS idx_nfl_matchups_date ON nfl_matchups(game_date, commence_time)`,
  `CREATE INDEX IF NOT EXISTS idx_nfl_matchups_upcoming ON nfl_matchups(commence_time) WHERE completed = FALSE`,
  `CREATE TABLE IF NOT EXISTS game_odds_history (
      id SERIAL PRIMARY KEY,
      sport TEXT NOT NULL,
      matchup_id INTEGER NOT NULL,
      event_id TEXT,
      game_date DATE NOT NULL,
      home_team_id INTEGER,
      away_team_id INTEGER,
      home_team_name TEXT,
      away_team_name TEXT,
      bookmaker_count INTEGER NOT NULL DEFAULT 0,
      home_ml INTEGER,
      away_ml INTEGER,
      home_spread DOUBLE PRECISION,
      vegas_total DOUBLE PRECISION,
      vegas_prob_home DOUBLE PRECISION,
      home_implied DOUBLE PRECISION,
      away_implied DOUBLE PRECISION,
      capture_key TEXT NOT NULL,
      captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      UNIQUE (sport, matchup_id, capture_key)
    )`,
  `CREATE INDEX IF NOT EXISTS idx_game_odds_history_lookup ON game_odds_history(sport, game_date, captured_at DESC)`,
  `CREATE INDEX IF NOT EXISTS idx_game_odds_history_matchup ON game_odds_history(sport, matchup_id, captured_at DESC)`,
  `CREATE TABLE IF NOT EXISTS player_prop_history (
      id SERIAL PRIMARY KEY,
      sport TEXT NOT NULL,
      slate_id INTEGER REFERENCES dk_slates(id) ON DELETE CASCADE,
      dk_player_id BIGINT NOT NULL,
      player_name TEXT NOT NULL,
      team_id INTEGER,
      event_id TEXT,
      market_key TEXT NOT NULL,
      line DOUBLE PRECISION,
      price INTEGER,
      bookmaker_key TEXT,
      bookmaker_title TEXT,
      book_count INTEGER NOT NULL DEFAULT 0,
      capture_key TEXT NOT NULL,
      captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      UNIQUE (sport, slate_id, dk_player_id, market_key, capture_key)
    )`,
  `CREATE INDEX IF NOT EXISTS idx_player_prop_history_lookup ON player_prop_history(sport, slate_id, market_key, captured_at DESC)`,
  `CREATE INDEX IF NOT EXISTS idx_player_prop_history_player ON player_prop_history(sport, dk_player_id, market_key, captured_at DESC)`,
];

const MLB_GAME_PREDICTION_DDLS = [
  `CREATE TABLE IF NOT EXISTS mlb_prediction_runs (
      id SERIAL PRIMARY KEY,
      generated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      trained_through DATE,
      model_version TEXT NOT NULL,
      git_sha TEXT,
      origin TEXT NOT NULL CHECK (origin IN ('prospective', 'retrospective_backfill')),
      source TEXT NOT NULL,
      config_json JSONB NOT NULL DEFAULT '{}'::jsonb
    )`,
  `CREATE TABLE IF NOT EXISTS mlb_game_prediction_snapshots (
      id SERIAL PRIMARY KEY,
      run_id INTEGER NOT NULL REFERENCES mlb_prediction_runs(id),
      matchup_id INTEGER NOT NULL REFERENCES mlb_matchups(id),
      odds_snapshot_id INTEGER REFERENCES game_odds_history(id),
      market TEXT NOT NULL CHECK (market IN ('moneyline', 'total')),
      decision_phase TEXT NOT NULL DEFAULT 'pregame',
      event_commence TIMESTAMPTZ NOT NULL,
      feature_available_at TIMESTAMPTZ NOT NULL,
      feature_values JSONB NOT NULL,
      missingness_json JSONB NOT NULL DEFAULT '{}'::jsonb,
      market_line DOUBLE PRECISION,
      market_odds INTEGER,
      market_prob DOUBLE PRECISION,
      book TEXT,
      raw_prediction DOUBLE PRECISION NOT NULL,
      calibrated_probability DOUBLE PRECISION,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      UNIQUE(run_id, matchup_id, market)
    )`,
  `ALTER TABLE mlb_bets ADD COLUMN IF NOT EXISTS prediction_snapshot_id INTEGER REFERENCES mlb_game_prediction_snapshots(id)`,
  `ALTER TABLE mlb_bets ADD COLUMN IF NOT EXISTS origin TEXT NOT NULL DEFAULT 'legacy'`,
  `ALTER TABLE mlb_bets ADD COLUMN IF NOT EXISTS odds_snapshot_id INTEGER REFERENCES game_odds_history(id)`,
  `ALTER TABLE mlb_bet_snapshots ADD COLUMN IF NOT EXISTS prediction_snapshot_id INTEGER REFERENCES mlb_game_prediction_snapshots(id)`,
  `ALTER TABLE mlb_bet_snapshots ADD COLUMN IF NOT EXISTS odds_snapshot_id INTEGER REFERENCES game_odds_history(id)`,
  `ALTER TABLE mlb_bet_snapshots ADD COLUMN IF NOT EXISTS book TEXT`,
  `ALTER TABLE mlb_bet_snapshots ADD COLUMN IF NOT EXISTS selection_label TEXT`,
  `ALTER TABLE mlb_bet_snapshots ADD COLUMN IF NOT EXISTS market_line DOUBLE PRECISION`,
  `CREATE INDEX IF NOT EXISTS idx_mlb_prediction_runs_origin ON mlb_prediction_runs(origin, model_version, generated_at DESC)`,
  `CREATE INDEX IF NOT EXISTS idx_mlb_game_prediction_matchup ON mlb_game_prediction_snapshots(matchup_id, market, created_at DESC)`,
  `CREATE OR REPLACE FUNCTION reject_mlb_prediction_mutation()
     RETURNS trigger LANGUAGE plpgsql AS $$
     BEGIN
       RAISE EXCEPTION 'MLB prediction provenance is append-only';
     END $$`,
  `DO $$ BEGIN
     IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'mlb_prediction_runs_immutable') THEN
       CREATE TRIGGER mlb_prediction_runs_immutable
       BEFORE UPDATE OR DELETE ON mlb_prediction_runs
       FOR EACH ROW EXECUTE FUNCTION reject_mlb_prediction_mutation();
     END IF;
     IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'mlb_game_prediction_snapshots_immutable') THEN
       CREATE TRIGGER mlb_game_prediction_snapshots_immutable
       BEFORE UPDATE OR DELETE ON mlb_game_prediction_snapshots
       FOR EACH ROW EXECUTE FUNCTION reject_mlb_prediction_mutation();
     END IF;
  END $$`,
];

export async function ensureDkPlayerPropColumns(): Promise<void> {
  if (!ensureDkPlayerPropColumnsPromise) {
    ensureDkPlayerPropColumnsPromise = (async () => {
      for (const ddl of [...DK_SLATE_COLUMN_DDLS, ...DK_PLAYER_PROP_COLUMN_DDLS]) {
        await db.execute(sql.raw(ddl));
      }
    })().catch((error) => {
      ensureDkPlayerPropColumnsPromise = null;
      throw error;
    });
  }
  await ensureDkPlayerPropColumnsPromise;
}

export async function ensureProjectionExperimentTables(): Promise<void> {
  if (!ensureProjectionExperimentTablesPromise) {
    ensureProjectionExperimentTablesPromise = (async () => {
      for (const ddl of PROJECTION_EXPERIMENT_DDLS) {
        await db.execute(sql.raw(ddl));
      }
    })().catch((error) => {
      ensureProjectionExperimentTablesPromise = null;
      throw error;
    });
  }
  await ensureProjectionExperimentTablesPromise;
}

export async function ensureOwnershipExperimentTables(): Promise<void> {
  if (!ensureOwnershipExperimentTablesPromise) {
    ensureOwnershipExperimentTablesPromise = (async () => {
      for (const ddl of OWNERSHIP_EXPERIMENT_DDLS) {
        await db.execute(sql.raw(ddl));
      }
    })().catch((error) => {
      ensureOwnershipExperimentTablesPromise = null;
      throw error;
    });
  }
  await ensureOwnershipExperimentTablesPromise;
}

export async function ensureMlbBlowupTrackingTables(): Promise<void> {
  if (!ensureMlbBlowupTrackingTablesPromise) {
    ensureMlbBlowupTrackingTablesPromise = (async () => {
      for (const ddl of MLB_BLOWUP_TRACKING_DDLS) {
        await db.execute(sql.raw(ddl));
      }
    })().catch((error) => {
      ensureMlbBlowupTrackingTablesPromise = null;
      throw error;
    });
  }
  await ensureMlbBlowupTrackingTablesPromise;
}

export async function ensureMlbHomerunTrackingTables(): Promise<void> {
  if (!ensureMlbHomerunTrackingTablesPromise) {
    ensureMlbHomerunTrackingTablesPromise = (async () => {
      for (const ddl of MLB_HOMERUN_TRACKING_DDLS) {
        await db.execute(sql.raw(ddl));
      }
    })().catch((error) => {
      ensureMlbHomerunTrackingTablesPromise = null;
      throw error;
    });
  }
  await ensureMlbHomerunTrackingTablesPromise;
}

export async function ensureOddsSignalTables(): Promise<void> {
  if (!ensureOddsSignalTablesPromise) {
    ensureOddsSignalTablesPromise = (async () => {
      for (const ddl of ODDS_SIGNAL_DDLS) {
        await db.execute(sql.raw(ddl));
      }
    })().catch((error) => {
      ensureOddsSignalTablesPromise = null;
      throw error;
    });
  }
  await ensureOddsSignalTablesPromise;
}

export async function ensureOddsHistoryTables(): Promise<void> {
  if (!ensureOddsHistoryTablesPromise) {
    ensureOddsHistoryTablesPromise = (async () => {
      for (const ddl of ODDS_HISTORY_DDLS) {
        await db.execute(sql.raw(ddl));
      }
    })().catch((error) => {
      ensureOddsHistoryTablesPromise = null;
      throw error;
    });
  }
  await ensureOddsHistoryTablesPromise;
}

export async function ensureMlbGamePredictionTables(): Promise<void> {
  if (!ensureMlbGamePredictionTablesPromise) {
    ensureMlbGamePredictionTablesPromise = (async () => {
      await ensureOddsHistoryTables();
      for (const ddl of MLB_GAME_PREDICTION_DDLS) {
        await db.execute(sql.raw(ddl));
      }
    })().catch((error) => {
      ensureMlbGamePredictionTablesPromise = null;
      throw error;
    });
  }
  await ensureMlbGamePredictionTablesPromise;
}

// Columns added for per-stat projection tracking and game-total model (commit 28950da).
// Added here so Vercel picks them up on first request without a manual schema.py run.
const ANALYTICS_COLUMN_DDLS = [
  `ALTER TABLE dk_players ADD COLUMN IF NOT EXISTS actual_pts REAL`,
  `ALTER TABLE dk_players ADD COLUMN IF NOT EXISTS actual_reb REAL`,
  `ALTER TABLE dk_players ADD COLUMN IF NOT EXISTS actual_ast REAL`,
  `ALTER TABLE dk_players ADD COLUMN IF NOT EXISTS actual_stl REAL`,
  `ALTER TABLE dk_players ADD COLUMN IF NOT EXISTS actual_blk REAL`,
  `ALTER TABLE dk_players ADD COLUMN IF NOT EXISTS actual_tov REAL`,
  `ALTER TABLE dk_players ADD COLUMN IF NOT EXISTS actual_3pm REAL`,
  `ALTER TABLE nba_matchups ADD COLUMN IF NOT EXISTS our_game_total_pred DOUBLE PRECISION`,
  `ALTER TABLE nba_matchups ADD COLUMN IF NOT EXISTS home_score INTEGER`,
  `ALTER TABLE nba_matchups ADD COLUMN IF NOT EXISTS away_score INTEGER`,
  `ALTER TABLE mlb_matchups ADD COLUMN IF NOT EXISTS home_sp_name TEXT`,
  `ALTER TABLE mlb_matchups ADD COLUMN IF NOT EXISTS away_sp_name TEXT`,
];

// Video Analysis feature (2026-07-05): user pastes a YouTube URL, we fetch
// its transcript and ask DeepSeek for a per-team/per-player breakdown.
// Sport-agnostic -- not scoped to nba/mlb like most tables here.
const VIDEO_ANALYSIS_DDLS = [
  `CREATE TABLE IF NOT EXISTS video_analysis (
      id SERIAL PRIMARY KEY,
      video_url TEXT NOT NULL,
      video_id TEXT NOT NULL,
      title TEXT,
      channel_name TEXT,
      transcript_text TEXT NOT NULL,
      analysis_json JSONB NOT NULL,
      model_version TEXT NOT NULL,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      UNIQUE (video_id)
    )`,
  `CREATE INDEX IF NOT EXISTS idx_video_analysis_created ON video_analysis(created_at DESC)`,
];

export async function ensureVideoAnalysisTables(): Promise<void> {
  if (!ensureVideoAnalysisTablesPromise) {
    ensureVideoAnalysisTablesPromise = (async () => {
      for (const ddl of VIDEO_ANALYSIS_DDLS) {
        await db.execute(sql.raw(ddl));
      }
    })().catch((error) => {
      ensureVideoAnalysisTablesPromise = null;
      throw error;
    });
  }
  await ensureVideoAnalysisTablesPromise;
}

// YouTube pick channels (2026-07-05): written from the web app's "Add
// Channel" action, read by the Python ingest script to know what to scrape.
// Also defined in db/schema.py so it self-provisions regardless of which
// side runs first -- same pattern as game_odds_history/player_prop_history.
// Pools, entries, picks and the recommendation ledger are written by the web
// app and read/settled by Python (model/survivor_settlement.py), so both sides
// provision them -- the same shared-ownership pattern as youtube_pick_channels.
// The probability tables these reference are Python-owned and are NOT created
// here: the page must fail visibly if the model has never run rather than
// render an empty grid that looks like a quiet season.
const SURVIVOR_DDLS = [
  `CREATE TABLE IF NOT EXISTS survivor_pools (
      id SERIAL PRIMARY KEY,
      name TEXT NOT NULL,
      season INTEGER NOT NULL,
      entry_count INTEGER NOT NULL DEFAULT 1,
      pool_size INTEGER,
      tie_rule TEXT NOT NULL DEFAULT 'tie_loses',
      strikes INTEGER NOT NULL DEFAULT 0,
      start_week INTEGER NOT NULL DEFAULT 1,
      end_week INTEGER NOT NULL DEFAULT 18,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      CHECK (tie_rule IN ('tie_loses', 'tie_survives')),
      CHECK (end_week >= start_week)
  )`,
  `CREATE TABLE IF NOT EXISTS survivor_entries (
      id SERIAL PRIMARY KEY,
      pool_id INTEGER NOT NULL REFERENCES survivor_pools(id) ON DELETE CASCADE,
      label TEXT NOT NULL,
      status TEXT NOT NULL DEFAULT 'alive',
      strikes_used INTEGER NOT NULL DEFAULT 0,
      eliminated_week INTEGER,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      UNIQUE(pool_id, label),
      CHECK (status IN ('alive', 'eliminated'))
  )`,
  `CREATE TABLE IF NOT EXISTS survivor_entry_picks (
      id SERIAL PRIMARY KEY,
      entry_id INTEGER NOT NULL REFERENCES survivor_entries(id) ON DELETE CASCADE,
      week INTEGER NOT NULL,
      team_id INTEGER NOT NULL REFERENCES nfl_teams(team_id),
      game_id INTEGER,
      p_advance_at_pick DOUBLE PRECISION,
      provenance_at_pick TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      locked_at TIMESTAMPTZ,
      result TEXT NOT NULL DEFAULT 'pending',
      settled_at TIMESTAMPTZ,
      UNIQUE(entry_id, week),
      CHECK (result IN ('pending', 'won', 'lost', 'push', 'void'))
  )`,
  `CREATE TABLE IF NOT EXISTS survivor_recommendations (
      id SERIAL PRIMARY KEY,
      pool_id INTEGER REFERENCES survivor_pools(id) ON DELETE CASCADE,
      entry_id INTEGER REFERENCES survivor_entries(id) ON DELETE CASCADE,
      season INTEGER NOT NULL,
      week INTEGER NOT NULL,
      recommended_team_id INTEGER NOT NULL REFERENCES nfl_teams(team_id),
      game_id INTEGER,
      p_advance DOUBLE PRECISION,
      provenance TEXT,
      objective_mode TEXT NOT NULL DEFAULT 'survive',
      path_json JSONB NOT NULL DEFAULT '[]'::jsonb,
      path_survival_prob DOUBLE PRECISION,
      opportunity_cost DOUBLE PRECISION,
      fsv_json JSONB NOT NULL DEFAULT '{}'::jsonb,
      pick_pct_at_rec DOUBLE PRECISION,
      alternatives_json JSONB NOT NULL DEFAULT '[]'::jsonb,
      constraints_json JSONB NOT NULL DEFAULT '{}'::jsonb,
      model_version TEXT NOT NULL,
      frozen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      event_commence TIMESTAMPTZ,
      superseded_by INTEGER REFERENCES survivor_recommendations(id),
      result TEXT NOT NULL DEFAULT 'pending',
      settled_at TIMESTAMPTZ,
      CHECK (result IN ('pending', 'won', 'lost', 'push', 'void'))
  )`,
  `CREATE INDEX IF NOT EXISTS idx_survivor_entries_pool ON survivor_entries(pool_id)`,
  `CREATE INDEX IF NOT EXISTS idx_survivor_picks_entry ON survivor_entry_picks(entry_id, week)`,
  `CREATE INDEX IF NOT EXISTS idx_survivor_recs_entry ON survivor_recommendations(entry_id, week, frozen_at DESC)`,
];

export async function ensureSurvivorTables(): Promise<void> {
  if (!ensureSurvivorTablesPromise) {
    ensureSurvivorTablesPromise = (async () => {
      for (const ddl of SURVIVOR_DDLS) {
        await db.execute(sql.raw(ddl));
      }
    })().catch((error) => {
      ensureSurvivorTablesPromise = null;
      throw error;
    });
  }
  await ensureSurvivorTablesPromise;
}

const YOUTUBE_PICK_CHANNELS_DDLS = [
  `CREATE TABLE IF NOT EXISTS youtube_pick_channels (
      id SERIAL PRIMARY KEY,
      channel_id TEXT NOT NULL UNIQUE,
      channel_name TEXT NOT NULL,
      handle TEXT,
      active BOOLEAN NOT NULL DEFAULT TRUE,
      added_at TIMESTAMPTZ DEFAULT NOW()
    )`,
  `CREATE INDEX IF NOT EXISTS idx_youtube_pick_channels_active ON youtube_pick_channels(active)`,
];

export async function ensureYoutubePickChannelsTable(): Promise<void> {
  if (!ensureYoutubePickChannelsTablePromise) {
    ensureYoutubePickChannelsTablePromise = (async () => {
      for (const ddl of YOUTUBE_PICK_CHANNELS_DDLS) {
        await db.execute(sql.raw(ddl));
      }
    })().catch((error) => {
      ensureYoutubePickChannelsTablePromise = null;
      throw error;
    });
  }
  await ensureYoutubePickChannelsTablePromise;
}

// Records every per-event Odds API prop call so a repeat click can be skipped.
// The DFS "Fetch Player Props" buttons are per-event paid calls (MLB 8 credits/
// event, NBA 5) with no dedupe -- clicking twice used to cost twice. Vercel is
// stateless per invocation, so the guard has to live in the DB, not memory.
const ODDS_API_PROP_FETCH_LOG_DDLS = [
  `CREATE TABLE IF NOT EXISTS odds_api_prop_fetch_log (
      id SERIAL PRIMARY KEY,
      sport TEXT NOT NULL,
      event_id TEXT NOT NULL,
      fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      credits_estimate INTEGER,
      UNIQUE (sport, event_id)
    )`,
  `CREATE INDEX IF NOT EXISTS idx_odds_api_prop_fetch_log_lookup
     ON odds_api_prop_fetch_log(sport, fetched_at DESC)`,
];

export async function ensureOddsApiPropFetchLog(): Promise<void> {
  if (!ensureOddsApiPropFetchLogPromise) {
    ensureOddsApiPropFetchLogPromise = (async () => {
      for (const ddl of ODDS_API_PROP_FETCH_LOG_DDLS) {
        await db.execute(sql.raw(ddl));
      }
    })().catch((error) => {
      ensureOddsApiPropFetchLogPromise = null;
      throw error;
    });
  }
  await ensureOddsApiPropFetchLogPromise;
}


// Frozen Polymarket wallet cohort + open-position snapshots. Python
// (ingest/polymarket_watchlist.py) owns the writes; the web app reads only.
// Declared here too so the page renders rather than 500s on a machine where
// the ingester has never run -- same pattern as the other experimental tables.
const POLYMARKET_WATCHLIST_DDLS = [
  `CREATE TABLE IF NOT EXISTS polymarket_watchlist_wallets (
      id SERIAL PRIMARY KEY,
      cohort_version TEXT NOT NULL,
      wallet TEXT NOT NULL,
      display_name TEXT,
      validated_sport TEXT NOT NULL,
      model_version TEXT NOT NULL,
      dev_clv DOUBLE PRECISION,
      dev_markets INTEGER,
      holdout_clv_at_freeze DOUBLE PRECISION,
      holdout_markets_at_freeze INTEGER,
      rank_at_freeze INTEGER,
      cohort_group TEXT NOT NULL DEFAULT 'selected',
      frozen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      UNIQUE (cohort_version, wallet)
    )`,
  // The CREATE above only helps a database that does not have the table
  // yet. A database provisioned before cohort_group existed keeps the old
  // shape forever, because CREATE TABLE IF NOT EXISTS no-ops -- and every
  // read in queries.ts references this column. The Python writer carries
  // the same repair; both sides need it or whichever runs first wins.
  `ALTER TABLE polymarket_watchlist_wallets
     ADD COLUMN IF NOT EXISTS cohort_group TEXT NOT NULL DEFAULT 'selected'`,
  `CREATE TABLE IF NOT EXISTS polymarket_watchlist_positions (
      id SERIAL PRIMARY KEY,
      cohort_version TEXT NOT NULL,
      wallet TEXT NOT NULL,
      condition_id TEXT,
      event_slug TEXT,
      title TEXT,
      outcome TEXT,
      sport TEXT,
      market_type TEXT,
      is_in_scope BOOLEAN NOT NULL DEFAULT FALSE,
      size DOUBLE PRECISION,
      avg_price DOUBLE PRECISION,
      cur_price DOUBLE PRECISION,
      current_value DOUBLE PRECISION,
      cash_pnl DOUBLE PRECISION,
      percent_pnl DOUBLE PRECISION,
      end_date TIMESTAMPTZ,
      captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      UNIQUE (cohort_version, wallet, condition_id, outcome, captured_at)
    )`,
  `CREATE TABLE IF NOT EXISTS polymarket_watchlist_forward (
      id SERIAL PRIMARY KEY,
      cohort_version TEXT NOT NULL,
      wallet TEXT NOT NULL,
      scored_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      markets INTEGER NOT NULL,
      stake DOUBLE PRECISION NOT NULL,
      clv DOUBLE PRECISION,
      window_start TIMESTAMPTZ,
      window_end TIMESTAMPTZ,
      UNIQUE (cohort_version, wallet, scored_at)
    )`,
  `CREATE TABLE IF NOT EXISTS polymarket_watchlist_captures (
      id SERIAL PRIMARY KEY,
      cohort_version TEXT NOT NULL,
      captured_at TIMESTAMPTZ NOT NULL,
      wallets_expected INTEGER NOT NULL,
      wallets_written INTEGER NOT NULL,
      positions_written INTEGER NOT NULL,
      completed_at TIMESTAMPTZ,
      UNIQUE (cohort_version, captured_at)
    )`,
  `CREATE INDEX IF NOT EXISTS idx_pm_watchlist_pos_lookup
     ON polymarket_watchlist_positions(cohort_version, captured_at DESC)`,
];

export async function ensurePolymarketWatchlistTables(): Promise<void> {
  if (!ensurePolymarketWatchlistTablesPromise) {
    ensurePolymarketWatchlistTablesPromise = (async () => {
      for (const ddl of POLYMARKET_WATCHLIST_DDLS) {
        await db.execute(sql.raw(ddl));
      }
    })().catch((error) => {
      ensurePolymarketWatchlistTablesPromise = null;
      throw error;
    });
  }
  await ensurePolymarketWatchlistTablesPromise;
}

export async function ensureAnalyticsColumns(): Promise<void> {
  if (!ensureAnalyticsColumnsPromise) {
    ensureAnalyticsColumnsPromise = (async () => {
      for (const ddl of ANALYTICS_COLUMN_DDLS) {
        await db.execute(sql.raw(ddl));
      }
    })().catch((error) => {
      ensureAnalyticsColumnsPromise = null;
      throw error;
    });
  }
  await ensureAnalyticsColumnsPromise;
}

export async function ensureFantasyFootballTables(): Promise<void> {
  if (!ensureFantasyFootballTablesPromise) {
    ensureFantasyFootballTablesPromise = (async () => {
      await ensureOddsHistoryTables();
      for (const ddl of FANTASY_FOOTBALL_DDLS) {
        await db.execute(sql.raw(ddl));
      }
    })().catch((error) => {
      ensureFantasyFootballTablesPromise = null;
      throw error;
    });
  }
  await ensureFantasyFootballTablesPromise;
}
