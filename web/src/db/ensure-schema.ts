import { sql } from "drizzle-orm";

import { db } from ".";

let ensureDkPlayerPropColumnsPromise: Promise<void> | null = null;
let ensureProjectionExperimentTablesPromise: Promise<void> | null = null;
let ensureOwnershipExperimentTablesPromise: Promise<void> | null = null;
let ensureMlbBlowupTrackingTablesPromise: Promise<void> | null = null;
let ensureOddsSignalTablesPromise: Promise<void> | null = null;
let ensureOddsHistoryTablesPromise: Promise<void> | null = null;
let ensureAnalyticsColumnsPromise: Promise<void> | null = null;

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
