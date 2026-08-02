import { sql } from "drizzle-orm";

import { db } from ".";

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
let ensureMlbGamePredictionTablesPromise: Promise<void> | null = null;
let ensureFantasyFootballTablesPromise: Promise<void> | null = null;

const FANTASY_FOOTBALL_DDLS = [
  `CREATE TABLE IF NOT EXISTS ff_source_snapshots (id BIGSERIAL PRIMARY KEY, source TEXT NOT NULL, dataset TEXT NOT NULL, season INTEGER NOT NULL, scoring TEXT, ranking_type TEXT, request_params JSONB NOT NULL DEFAULT '{}'::jsonb, source_updated_at TIMESTAMPTZ, fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), response_hash TEXT NOT NULL, row_count INTEGER NOT NULL, matched_count INTEGER NOT NULL DEFAULT 0, unmatched_count INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL, error_summary TEXT, UNIQUE(source, dataset, response_hash))`,
  `CREATE TABLE IF NOT EXISTS ff_players (id BIGSERIAL PRIMARY KEY, season INTEGER NOT NULL, canonical_name TEXT NOT NULL, normalized_name TEXT NOT NULL, position TEXT NOT NULL, nfl_team_id INTEGER REFERENCES nfl_teams(team_id), team_abbrev TEXT, fantasypros_player_id INTEGER, sleeper_player_id TEXT, gsis_id TEXT, espn_id TEXT, yahoo_id TEXT, mfl_id TEXT, draftkings_id TEXT, active BOOLEAN NOT NULL DEFAULT TRUE, rookie BOOLEAN NOT NULL DEFAULT FALSE, bye_week INTEGER, injury_status TEXT, metadata JSONB NOT NULL DEFAULT '{}'::jsonb, fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), UNIQUE(season, fantasypros_player_id))`,
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
  `CREATE INDEX IF NOT EXISTS idx_ff_rank_sets_latest ON ff_ranking_sets(season, ranking_type, created_at DESC)`,
  `CREATE INDEX IF NOT EXISTS idx_ff_rankings_board ON ff_player_rankings(ranking_set_id, COALESCE(our_rank, overall_rank))`,
  `CREATE INDEX IF NOT EXISTS idx_ff_drafts_recent ON ff_draft_sessions(updated_at DESC)`,
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
