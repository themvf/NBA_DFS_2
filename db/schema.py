"""PostgreSQL schema for NBA DFS v2 + MLB expansion.

Tables:
  NBA:
  - teams              30 NBA teams with standard 3-letter abbreviations
  - nba_team_stats     Pace, OffRtg, DefRtg per team per season
  - nba_player_stats   Rolling 10-game averages per player
  - nba_player_game_logs  Raw per-game player logs from stats.nba.com
  - nba_team_game_logs    Raw per-game team logs from stats.nba.com
  - nba_matchups       Daily game schedule with Vegas odds

  MLB:
  - mlb_teams          30 MLB teams with ballpark info
  - mlb_park_factors   Run/HR park factor multipliers per team per season
  - mlb_matchups       Daily schedule with Vegas odds + confirmed starters
  - mlb_batter_stats   Rolling per-game batting stats (15-game EWMA)
  - mlb_pitcher_stats  Rolling per-game pitching stats
  - mlb_team_stats     Team offensive + bullpen environment

  Shared:
  - dk_slates          DraftKings slate per date (sport column: 'nba' | 'mlb')
  - dk_players         Player pool per slate (sport-agnostic structure)
  - dk_lineups         Generated lineups for strategy comparison
"""

TABLES = [
    # ── NBA teams ─────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS teams (
        team_id SERIAL PRIMARY KEY,
        name TEXT NOT NULL UNIQUE,
        abbreviation TEXT NOT NULL UNIQUE,
        conference TEXT DEFAULT '',
        division TEXT DEFAULT '',
        nba_id INTEGER,
        logo_url TEXT DEFAULT '',
        created_at TIMESTAMPTZ DEFAULT NOW()
    )
    """,

    # ── Team pace + efficiency ratings (from NBA API) ─────────
    """
    CREATE TABLE IF NOT EXISTS nba_team_stats (
        id SERIAL PRIMARY KEY,
        team_id INTEGER NOT NULL REFERENCES teams(team_id),
        season TEXT NOT NULL,
        pace DOUBLE PRECISION,
        off_rtg DOUBLE PRECISION,
        def_rtg DOUBLE PRECISION,
        fetched_at TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE(team_id, season)
    )
    """,

    # ── Player rolling stats (from LeagueGameLog) ────────────
    """
    CREATE TABLE IF NOT EXISTS nba_player_stats (
        id SERIAL PRIMARY KEY,
        player_id INTEGER NOT NULL,
        season TEXT NOT NULL,
        team_id INTEGER REFERENCES teams(team_id),
        name TEXT NOT NULL,
        position TEXT,
        games INTEGER,
        avg_minutes DOUBLE PRECISION,
        ppg DOUBLE PRECISION,
        rpg DOUBLE PRECISION,
        apg DOUBLE PRECISION,
        spg DOUBLE PRECISION,
        bpg DOUBLE PRECISION,
        tovpg DOUBLE PRECISION,
        threefgm_pg DOUBLE PRECISION,
        usage_rate DOUBLE PRECISION,
        dd_rate DOUBLE PRECISION,
        fpts_std REAL,
        fetched_at TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE(player_id, season)
    )
    """,

    # ── Daily NBA schedule + Vegas odds ──────────────────────
    """
    CREATE TABLE IF NOT EXISTS nba_player_game_logs (
        id SERIAL PRIMARY KEY,
        season TEXT NOT NULL,
        season_type TEXT NOT NULL,
        player_id INTEGER NOT NULL,
        name TEXT NOT NULL,
        team_id INTEGER REFERENCES teams(team_id),
        opponent_team_id INTEGER REFERENCES teams(team_id),
        game_id TEXT NOT NULL,
        game_date DATE,
        matchup TEXT,
        team_abbreviation TEXT,
        opponent_abbreviation TEXT,
        is_home BOOLEAN,
        win_loss TEXT,
        minutes DOUBLE PRECISION,
        points DOUBLE PRECISION,
        rebounds DOUBLE PRECISION,
        assists DOUBLE PRECISION,
        steals DOUBLE PRECISION,
        blocks DOUBLE PRECISION,
        turnovers DOUBLE PRECISION,
        fgm DOUBLE PRECISION,
        fga DOUBLE PRECISION,
        fg3m DOUBLE PRECISION,
        fg3a DOUBLE PRECISION,
        ftm DOUBLE PRECISION,
        fta DOUBLE PRECISION,
        plus_minus DOUBLE PRECISION,
        fetched_at TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE(season, season_type, player_id, game_id)
    )
    """,

    """
    CREATE TABLE IF NOT EXISTS nba_team_game_logs (
        id SERIAL PRIMARY KEY,
        season TEXT NOT NULL,
        season_type TEXT NOT NULL,
        team_id INTEGER NOT NULL REFERENCES teams(team_id),
        opponent_team_id INTEGER REFERENCES teams(team_id),
        team_name TEXT NOT NULL,
        team_abbreviation TEXT,
        opponent_abbreviation TEXT,
        game_id TEXT NOT NULL,
        game_date DATE,
        matchup TEXT,
        is_home BOOLEAN,
        win_loss TEXT,
        fg3m DOUBLE PRECISION,
        fg3a DOUBLE PRECISION,
        opp_fg3m DOUBLE PRECISION,
        opp_fg3a DOUBLE PRECISION,
        pts DOUBLE PRECISION,
        opp_pts DOUBLE PRECISION,
        ast DOUBLE PRECISION,
        reb DOUBLE PRECISION,
        opp_ast DOUBLE PRECISION,
        opp_reb DOUBLE PRECISION,
        fga DOUBLE PRECISION,
        fta DOUBLE PRECISION,
        oreb DOUBLE PRECISION,
        tov DOUBLE PRECISION,
        opp_fga DOUBLE PRECISION,
        opp_fta DOUBLE PRECISION,
        opp_oreb DOUBLE PRECISION,
        opp_tov DOUBLE PRECISION,
        plus_minus DOUBLE PRECISION,
        fetched_at TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE(season, season_type, team_id, game_id)
    )
    """,

    """
    CREATE TABLE IF NOT EXISTS nba_matchups (
        id SERIAL PRIMARY KEY,
        game_date DATE NOT NULL,
        game_id TEXT UNIQUE,
        home_team_id INTEGER REFERENCES teams(team_id),
        away_team_id INTEGER REFERENCES teams(team_id),
        vegas_total DOUBLE PRECISION,
        home_ml INTEGER,
        away_ml INTEGER,
        home_spread DOUBLE PRECISION,
        vegas_prob_home DOUBLE PRECISION,
        fetched_at TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE(game_date, home_team_id, away_team_id)
    )
    """,

    # ── MLB teams ─────────────────────────────────────────────
    # Separate table from NBA `teams` — different ID space, ballpark metadata,
    # and dk_abbrev overrides (DK uses non-standard MLB abbreviations).
    """
    CREATE TABLE IF NOT EXISTS mlb_teams (
        team_id SERIAL PRIMARY KEY,
        name TEXT NOT NULL UNIQUE,
        abbreviation TEXT NOT NULL UNIQUE,
        dk_abbrev TEXT,
        ballpark TEXT,
        city TEXT,
        division TEXT,
        mlb_id INTEGER UNIQUE,
        logo_url TEXT DEFAULT '',
        created_at TIMESTAMPTZ DEFAULT NOW()
    )
    """,

    # ── MLB park factors (updated annually) ──────────────────
    # runs_factor: e.g. 1.15 at Coors, 0.88 at Petco Park.
    # hr_factor: separate — parks affect HR more than other hits.
    """
    CREATE TABLE IF NOT EXISTS mlb_park_factors (
        id SERIAL PRIMARY KEY,
        team_id INTEGER NOT NULL REFERENCES mlb_teams(team_id),
        season TEXT NOT NULL,
        runs_factor DOUBLE PRECISION DEFAULT 1.0,
        hr_factor DOUBLE PRECISION DEFAULT 1.0,
        UNIQUE(team_id, season)
    )
    """,

    # ── Daily MLB schedule + Vegas odds + confirmed starters ──
    """
    CREATE TABLE IF NOT EXISTS mlb_matchups (
        id SERIAL PRIMARY KEY,
        game_date DATE NOT NULL,
        game_id TEXT UNIQUE,
        home_team_id INTEGER REFERENCES mlb_teams(team_id),
        away_team_id INTEGER REFERENCES mlb_teams(team_id),
        home_sp_id INTEGER,
        away_sp_id INTEGER,
        vegas_total DOUBLE PRECISION,
        home_ml INTEGER,
        away_ml INTEGER,
        vegas_prob_home DOUBLE PRECISION,
        home_implied DOUBLE PRECISION,
        away_implied DOUBLE PRECISION,
        ballpark TEXT,
        weather_temp INTEGER,
        wind_speed INTEGER,
        wind_direction TEXT,
        fetched_at TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE(game_date, home_team_id, away_team_id)
    )
    """,

    # ── MLB batter stats (15-game EWMA, same α=0.25 as NBA) ──
    # wrc_plus_vs_l / wrc_plus_vs_r: L/R split for pitcher matchup.
    # fpts_std: per-game FPTS standard deviation for Monte Carlo.
    """
    CREATE TABLE IF NOT EXISTS mlb_batter_stats (
        id SERIAL PRIMARY KEY,
        player_id INTEGER NOT NULL,
        season TEXT NOT NULL,
        team_id INTEGER REFERENCES mlb_teams(team_id),
        name TEXT NOT NULL,
        batting_order INTEGER,
        games INTEGER,
        pa_pg DOUBLE PRECISION,
        avg DOUBLE PRECISION,
        obp DOUBLE PRECISION,
        slg DOUBLE PRECISION,
        iso DOUBLE PRECISION,
        babip DOUBLE PRECISION,
        wrc_plus DOUBLE PRECISION,
        k_pct DOUBLE PRECISION,
        bb_pct DOUBLE PRECISION,
        hr_pg DOUBLE PRECISION,
        singles_pg DOUBLE PRECISION,
        doubles_pg DOUBLE PRECISION,
        triples_pg DOUBLE PRECISION,
        rbi_pg DOUBLE PRECISION,
        runs_pg DOUBLE PRECISION,
        sb_pg DOUBLE PRECISION,
        hbp_pg DOUBLE PRECISION,
        wrc_plus_vs_l DOUBLE PRECISION,
        wrc_plus_vs_r DOUBLE PRECISION,
        avg_fpts_pg DOUBLE PRECISION,
        fpts_std DOUBLE PRECISION,
        fetched_at TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE(player_id, season)
    )
    """,

    # ── MLB pitcher stats ─────────────────────────────────────
    # hand: 'R' or 'L' — critical for batter L/R split application.
    # xfip: best ERA predictor; preferred over ERA for projections.
    # win_pct + qs_pct: used to estimate W bonus and QS probability.
    """
    CREATE TABLE IF NOT EXISTS mlb_pitcher_stats (
        id SERIAL PRIMARY KEY,
        player_id INTEGER NOT NULL,
        season TEXT NOT NULL,
        team_id INTEGER REFERENCES mlb_teams(team_id),
        name TEXT NOT NULL,
        hand TEXT,
        games INTEGER,
        ip_pg DOUBLE PRECISION,
        era DOUBLE PRECISION,
        fip DOUBLE PRECISION,
        xfip DOUBLE PRECISION,
        k_per_9 DOUBLE PRECISION,
        bb_per_9 DOUBLE PRECISION,
        hr_per_9 DOUBLE PRECISION,
        k_pct DOUBLE PRECISION,
        bb_pct DOUBLE PRECISION,
        hr_fb_pct DOUBLE PRECISION,
        whip DOUBLE PRECISION,
        avg_fpts_pg DOUBLE PRECISION,
        fpts_std DOUBLE PRECISION,
        win_pct DOUBLE PRECISION,
        qs_pct DOUBLE PRECISION,
        fetched_at TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE(player_id, season)
    )
    """,

    # ── MLB team offensive + bullpen environment ──────────────
    # team_wrc_plus: opposing lineup quality index (100 = avg).
    # team_k_pct: how often the team strikes out (scales pitcher K count).
    # bullpen_era/fip: used when SP is projected to not finish the game.
    """
    CREATE TABLE IF NOT EXISTS mlb_team_stats (
        id SERIAL PRIMARY KEY,
        team_id INTEGER NOT NULL REFERENCES mlb_teams(team_id),
        season TEXT NOT NULL,
        team_wrc_plus DOUBLE PRECISION,
        team_k_pct DOUBLE PRECISION,
        team_bb_pct DOUBLE PRECISION,
        team_iso DOUBLE PRECISION,
        team_ops DOUBLE PRECISION,
        bullpen_era DOUBLE PRECISION,
        bullpen_fip DOUBLE PRECISION,
        staff_k_pct DOUBLE PRECISION,
        staff_bb_pct DOUBLE PRECISION,
        fetched_at TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE(team_id, season)
    )
    """,

    # ── DFS slates ────────────────────────────────────────────
    # sport: 'nba' | 'mlb' — distinguishes same-date slates across sports.
    # UNIQUE includes sport so an NBA GPP and MLB GPP on the same date
    # are stored as separate rows.
    """
    CREATE TABLE IF NOT EXISTS dk_slates (
        id SERIAL PRIMARY KEY,
        sport TEXT DEFAULT 'nba',
        slate_date DATE NOT NULL,
        game_count INTEGER DEFAULT 0,
        dk_draft_group_id INTEGER,
        linestar_period_id INTEGER,
        cash_line DOUBLE PRECISION,
        contest_type TEXT DEFAULT 'main',
        field_size INTEGER,
        contest_format TEXT DEFAULT 'gpp',
        created_at TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE(slate_date, contest_type, contest_format, sport)
    )
    """,

    # ── DFS player pool ───────────────────────────────────────
    # team_id: FK to NBA teams (NULL for MLB players).
    # mlb_team_id: FK to mlb_teams (NULL for NBA players).
    # matchup_id: plain integer — refers to nba_matchups or mlb_matchups
    #   depending on the parent slate's sport column.
    """
    CREATE TABLE IF NOT EXISTS dk_players (
        id SERIAL PRIMARY KEY,
        slate_id INTEGER NOT NULL REFERENCES dk_slates(id) ON DELETE CASCADE,
        dk_player_id BIGINT NOT NULL,
        name TEXT NOT NULL,
        team_abbrev TEXT NOT NULL,
        team_id INTEGER REFERENCES teams(team_id),
        mlb_team_id INTEGER REFERENCES mlb_teams(team_id),
        matchup_id INTEGER,
        eligible_positions TEXT NOT NULL,
        salary INTEGER NOT NULL,
        game_info TEXT,
        avg_fpts_dk REAL,
        linestar_proj REAL,
        linestar_own_pct REAL,
        proj_own_pct REAL,
        our_proj REAL,
        live_proj REAL,
        expected_hr REAL,
        hr_prob_1plus REAL,
        our_leverage REAL,
        our_own_pct REAL,
        live_leverage REAL,
        live_own_pct REAL,
        prop_pts REAL,
        prop_pts_price INTEGER,
        prop_pts_book TEXT,
        prop_reb REAL,
        prop_reb_price INTEGER,
        prop_reb_book TEXT,
        prop_ast REAL,
        prop_ast_price INTEGER,
        prop_ast_book TEXT,
        prop_blk REAL,
        prop_blk_price INTEGER,
        prop_blk_book TEXT,
        prop_stl REAL,
        prop_stl_price INTEGER,
        prop_stl_book TEXT,
        proj_floor REAL,
        proj_ceiling REAL,
        boom_rate REAL,
        dk_in_starting_lineup BOOLEAN,
        dk_starting_lineup_order INTEGER,
        dk_team_lineup_confirmed BOOLEAN,
        is_out BOOLEAN DEFAULT FALSE,
        actual_fpts REAL,
        actual_own_pct REAL,
        UNIQUE(slate_id, dk_player_id)
    )
    """,

    # ── DFS generated lineups ─────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS dk_lineups (
        id SERIAL PRIMARY KEY,
        slate_id INTEGER NOT NULL REFERENCES dk_slates(id) ON DELETE CASCADE,
        strategy TEXT NOT NULL,
        lineup_num INTEGER NOT NULL,
        player_ids TEXT NOT NULL,
        total_salary INTEGER,
        proj_fpts REAL,
        leverage REAL,
        stack_team TEXT,
        actual_fpts REAL,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE(slate_id, strategy, lineup_num)
    )
    """,

    # â”€â”€ Durable optimizer jobs â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    """
    CREATE TABLE IF NOT EXISTS game_odds_history (
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
        UNIQUE(sport, matchup_id, capture_key)
    )
    """,

    """
    CREATE TABLE IF NOT EXISTS player_prop_history (
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
        UNIQUE(sport, slate_id, dk_player_id, market_key, capture_key)
    )
    """,

    """
    CREATE TABLE IF NOT EXISTS projection_runs (
        id SERIAL PRIMARY KEY,
        sport TEXT NOT NULL,
        slate_id INTEGER NOT NULL REFERENCES dk_slates(id) ON DELETE CASCADE,
        model_version TEXT NOT NULL,
        source TEXT NOT NULL,
        config_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        notes TEXT,
        created_at TIMESTAMPTZ DEFAULT NOW()
    )
    """,

    """
    CREATE TABLE IF NOT EXISTS projection_player_snapshots (
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
    )
    """,

    """
    CREATE TABLE IF NOT EXISTS ownership_runs (
        id SERIAL PRIMARY KEY,
        sport TEXT NOT NULL,
        slate_id INTEGER NOT NULL REFERENCES dk_slates(id) ON DELETE CASCADE,
        ownership_version TEXT NOT NULL,
        source TEXT NOT NULL,
        config_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        notes TEXT,
        created_at TIMESTAMPTZ DEFAULT NOW()
    )
    """,

    """
    CREATE TABLE IF NOT EXISTS ownership_player_snapshots (
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
    )
    """,

    """
    CREATE TABLE IF NOT EXISTS optimizer_jobs (
        id SERIAL PRIMARY KEY,
        sport TEXT NOT NULL,
        slate_id INTEGER NOT NULL REFERENCES dk_slates(id) ON DELETE CASCADE,
        client_token TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'queued',
        requested_lineups INTEGER NOT NULL,
        built_lineups INTEGER NOT NULL DEFAULT 0,
        eligible_count INTEGER,
        settings_json JSONB NOT NULL,
        snapshot_json JSONB NOT NULL,
        selected_matchups_json JSONB NOT NULL,
        pool_snapshot_json JSONB NOT NULL,
        effective_settings_json JSONB,
        probe_summary_json JSONB NOT NULL DEFAULT '[]'::jsonb,
        relaxed_constraints_json JSONB NOT NULL DEFAULT '[]'::jsonb,
        workflow_run_id TEXT,
        probe_ms INTEGER,
        total_ms INTEGER,
        termination_reason TEXT,
        warning TEXT,
        error TEXT,
        started_at TIMESTAMPTZ,
        finished_at TIMESTAMPTZ,
        heartbeat_at TIMESTAMPTZ,
        created_at TIMESTAMPTZ DEFAULT NOW()
    )
    """,

    """
    CREATE TABLE IF NOT EXISTS optimizer_job_lineups (
        id SERIAL PRIMARY KEY,
        job_id INTEGER NOT NULL REFERENCES optimizer_jobs(id) ON DELETE CASCADE,
        lineup_num INTEGER NOT NULL,
        slot_player_ids_json JSONB NOT NULL,
        player_ids_json JSONB NOT NULL,
        total_salary INTEGER NOT NULL,
        proj_fpts DOUBLE PRECISION NOT NULL,
        leverage DOUBLE PRECISION NOT NULL,
        actual_fpts DOUBLE PRECISION,
        duration_ms INTEGER NOT NULL,
        winning_stage TEXT,
        attempts_json JSONB NOT NULL DEFAULT '[]'::jsonb,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE(job_id, lineup_num)
    )
    """,
]

MIGRATIONS = [
    # 2026-04-12: Persist actual results for durable optimizer lineup tracking
    """DO $$ BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'optimizer_job_lineups' AND column_name = 'actual_fpts'
        ) THEN
            ALTER TABLE optimizer_job_lineups ADD COLUMN actual_fpts DOUBLE PRECISION;
        END IF;
    END $$""",

    # 2026-04-11: Add final scores + implied totals to nba_matchups for Vegas analysis
    """DO $$ BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'nba_matchups' AND column_name = 'home_score'
        ) THEN
            ALTER TABLE nba_matchups ADD COLUMN home_score INTEGER;
        END IF;
    END $$""",
    """DO $$ BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'nba_matchups' AND column_name = 'away_score'
        ) THEN
            ALTER TABLE nba_matchups ADD COLUMN away_score INTEGER;
        END IF;
    END $$""",
    """DO $$ BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'nba_matchups' AND column_name = 'home_implied'
        ) THEN
            ALTER TABLE nba_matchups ADD COLUMN home_implied DOUBLE PRECISION;
        END IF;
    END $$""",
    """DO $$ BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'nba_matchups' AND column_name = 'away_implied'
        ) THEN
            ALTER TABLE nba_matchups ADD COLUMN away_implied DOUBLE PRECISION;
        END IF;
    END $$""",
    # 2026-03-28: Relax game_id NOT NULL → nullable so TS web can insert without it
    "ALTER TABLE nba_matchups ALTER COLUMN game_id DROP NOT NULL",
    """DO $$ BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'nba_matchups' AND column_name = 'home_spread'
        ) THEN
            ALTER TABLE nba_matchups ADD COLUMN home_spread DOUBLE PRECISION;
        END IF;
    END $$""",
    # 2026-04-12: Add scores + run line to mlb_matchups for Vegas analysis
    """DO $$ BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'mlb_matchups' AND column_name = 'home_score'
        ) THEN
            ALTER TABLE mlb_matchups ADD COLUMN home_score INTEGER;
        END IF;
    END $$""",
    """DO $$ BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'mlb_matchups' AND column_name = 'away_score'
        ) THEN
            ALTER TABLE mlb_matchups ADD COLUMN away_score INTEGER;
        END IF;
    END $$""",
    """DO $$ BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'mlb_matchups' AND column_name = 'home_spread'
        ) THEN
            ALTER TABLE mlb_matchups ADD COLUMN home_spread DOUBLE PRECISION;
        END IF;
    END $$""",
    # 2026-03-28: Add composite unique on (game_date, home, away) to match Drizzle schema
    """DO $$ BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM pg_constraint WHERE conname = 'nba_matchups_date_teams_key'
        ) THEN
            ALTER TABLE nba_matchups
            ADD CONSTRAINT nba_matchups_date_teams_key
            UNIQUE (game_date, home_team_id, away_team_id);
        END IF;
    END $$""",
    # 2026-04-04: Add live optimizer fields to dk_players for NBA
    """DO $$ BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'dk_players' AND column_name = 'live_proj'
        ) THEN
            ALTER TABLE dk_players ADD COLUMN live_proj REAL;
        END IF;
    END $$""",
    """DO $$ BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'dk_players' AND column_name = 'live_leverage'
        ) THEN
            ALTER TABLE dk_players ADD COLUMN live_leverage REAL;
        END IF;
    END $$""",
    """DO $$ BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'dk_players' AND column_name = 'live_own_pct'
        ) THEN
            ALTER TABLE dk_players ADD COLUMN live_own_pct REAL;
        END IF;
    END $$""",
    """DO $$ BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'dk_players' AND column_name = 'linestar_own_pct'
        ) THEN
            ALTER TABLE dk_players ADD COLUMN linestar_own_pct REAL;
        END IF;
    END $$""",
    # 2026-03-28: Add nba_id to teams if missing (matches Drizzle schema)
    """DO $$ BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'teams' AND column_name = 'nba_id'
        ) THEN
            ALTER TABLE teams ADD COLUMN nba_id INTEGER;
        END IF;
    END $$""",
    # 2026-03-28: Add position/games to nba_player_stats if missing (matches Drizzle)
    """DO $$ BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'nba_player_stats' AND column_name = 'position'
        ) THEN
            ALTER TABLE nba_player_stats ADD COLUMN position TEXT;
        END IF;
    END $$""",
    """DO $$ BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'nba_player_stats' AND column_name = 'games'
        ) THEN
            ALTER TABLE nba_player_stats ADD COLUMN games INTEGER;
        END IF;
    END $$""",
    # 2026-03-28: Add our_own_pct to dk_players for ownership model tracking
    """DO $$ BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'dk_players' AND column_name = 'our_own_pct'
        ) THEN
            ALTER TABLE dk_players ADD COLUMN our_own_pct REAL;
        END IF;
    END $$""",
    # 2026-03-28: Add player prop lines (pts/reb/ast over-under from Odds API)
    """DO $$ BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'dk_players' AND column_name = 'prop_pts'
        ) THEN
            ALTER TABLE dk_players ADD COLUMN prop_pts REAL;
        END IF;
    END $$""",
    """DO $$ BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'dk_players' AND column_name = 'prop_reb'
        ) THEN
            ALTER TABLE dk_players ADD COLUMN prop_reb REAL;
        END IF;
    END $$""",
    """DO $$ BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'dk_players' AND column_name = 'prop_ast'
        ) THEN
            ALTER TABLE dk_players ADD COLUMN prop_ast REAL;
        END IF;
    END $$""",
    """DO $$ BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'dk_players' AND column_name = 'prop_pts_price'
        ) THEN
            ALTER TABLE dk_players ADD COLUMN prop_pts_price INTEGER;
        END IF;
    END $$""",
    """DO $$ BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'dk_players' AND column_name = 'prop_pts_book'
        ) THEN
            ALTER TABLE dk_players ADD COLUMN prop_pts_book TEXT;
        END IF;
    END $$""",
    """DO $$ BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'dk_players' AND column_name = 'prop_reb_price'
        ) THEN
            ALTER TABLE dk_players ADD COLUMN prop_reb_price INTEGER;
        END IF;
    END $$""",
    """DO $$ BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'dk_players' AND column_name = 'prop_reb_book'
        ) THEN
            ALTER TABLE dk_players ADD COLUMN prop_reb_book TEXT;
        END IF;
    END $$""",
    """DO $$ BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'dk_players' AND column_name = 'prop_ast_price'
        ) THEN
            ALTER TABLE dk_players ADD COLUMN prop_ast_price INTEGER;
        END IF;
    END $$""",
    """DO $$ BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'dk_players' AND column_name = 'prop_ast_book'
        ) THEN
            ALTER TABLE dk_players ADD COLUMN prop_ast_book TEXT;
        END IF;
    END $$""",
    """DO $$ BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'dk_players' AND column_name = 'prop_blk'
        ) THEN
            ALTER TABLE dk_players ADD COLUMN prop_blk REAL;
        END IF;
    END $$""",
    """DO $$ BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'dk_players' AND column_name = 'prop_blk_price'
        ) THEN
            ALTER TABLE dk_players ADD COLUMN prop_blk_price INTEGER;
        END IF;
    END $$""",
    """DO $$ BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'dk_players' AND column_name = 'prop_blk_book'
        ) THEN
            ALTER TABLE dk_players ADD COLUMN prop_blk_book TEXT;
        END IF;
    END $$""",
    """DO $$ BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'dk_players' AND column_name = 'prop_stl'
        ) THEN
            ALTER TABLE dk_players ADD COLUMN prop_stl REAL;
        END IF;
    END $$""",
    """DO $$ BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'dk_players' AND column_name = 'prop_stl_price'
        ) THEN
            ALTER TABLE dk_players ADD COLUMN prop_stl_price INTEGER;
        END IF;
    END $$""",
    """DO $$ BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'dk_players' AND column_name = 'prop_stl_book'
        ) THEN
            ALTER TABLE dk_players ADD COLUMN prop_stl_book TEXT;
        END IF;
    END $$""",
    # 2026-03-28: Add contest metadata to dk_slates
    """DO $$ BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'dk_slates' AND column_name = 'contest_type'
        ) THEN
            ALTER TABLE dk_slates ADD COLUMN contest_type TEXT DEFAULT 'main';
        END IF;
    END $$""",
    """DO $$ BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'dk_slates' AND column_name = 'field_size'
        ) THEN
            ALTER TABLE dk_slates ADD COLUMN field_size INTEGER;
        END IF;
    END $$""",
    """DO $$ BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'dk_slates' AND column_name = 'contest_format'
        ) THEN
            ALTER TABLE dk_slates ADD COLUMN contest_format TEXT DEFAULT 'gpp';
        END IF;
    END $$""",
    # 2026-03-28: Add fpts_std to nba_player_stats for Monte Carlo variance
    """DO $$ BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'nba_player_stats' AND column_name = 'fpts_std'
        ) THEN
            ALTER TABLE nba_player_stats ADD COLUMN fpts_std REAL;
        END IF;
    END $$""",
    # 2026-03-28: Add Monte Carlo columns to dk_players
    """DO $$ BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'dk_players' AND column_name = 'proj_floor'
        ) THEN
            ALTER TABLE dk_players ADD COLUMN proj_floor REAL;
        END IF;
    END $$""",
    """DO $$ BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'dk_players' AND column_name = 'proj_ceiling'
        ) THEN
            ALTER TABLE dk_players ADD COLUMN proj_ceiling REAL;
        END IF;
    END $$""",
    """DO $$ BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'dk_players' AND column_name = 'boom_rate'
        ) THEN
            ALTER TABLE dk_players ADD COLUMN boom_rate REAL;
        END IF;
    END $$""",
    # 2026-03-28: Retire the legacy dk_slates unique constraints that predate
    # sport-aware slate identity. Do not recreate the non-sport-aware
    # (slate_date, contest_type, contest_format) key here; later migrations add
    # the correct sport-aware unique constraint.
    """DO $$ BEGIN
        IF EXISTS (
            SELECT 1 FROM pg_constraint WHERE conname = 'dk_slates_slate_date_key'
        ) THEN
            ALTER TABLE dk_slates DROP CONSTRAINT dk_slates_slate_date_key;
        END IF;
        IF EXISTS (
            SELECT 1 FROM pg_constraint WHERE conname = 'dk_slates_date_type_format_key'
        ) THEN
            ALTER TABLE dk_slates DROP CONSTRAINT dk_slates_date_type_format_key;
        END IF;
    END $$""",

    # ── 2026-03-29: MLB Expansion ─────────────────────────────

    # Add sport column to dk_slates (default 'nba' for all existing rows)
    """DO $$ BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'dk_slates' AND column_name = 'sport'
        ) THEN
            ALTER TABLE dk_slates ADD COLUMN sport TEXT DEFAULT 'nba';
            UPDATE dk_slates SET sport = 'nba' WHERE sport IS NULL;
        END IF;
    END $$""",
    # Migrate dk_slates unique constraint to include sport column.
    # NBA + MLB slates on the same date are now distinct rows.
    """DO $$ BEGIN
        IF EXISTS (
            SELECT 1 FROM pg_constraint WHERE conname = 'dk_slates_date_type_format_key'
        ) THEN
            ALTER TABLE dk_slates DROP CONSTRAINT dk_slates_date_type_format_key;
        END IF;
        IF NOT EXISTS (
            SELECT 1 FROM pg_constraint WHERE conname = 'dk_slates_date_type_format_sport_key'
        ) THEN
            ALTER TABLE dk_slates ADD CONSTRAINT dk_slates_date_type_format_sport_key
            UNIQUE (slate_date, contest_type, contest_format, sport);
        END IF;
    END $$""",
    # Add mlb_team_id to dk_players for MLB slate support.
    # team_id (NBA) remains for NBA players; mlb_team_id is set for MLB players.
    """DO $$ BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'dk_players' AND column_name = 'mlb_team_id'
        ) THEN
            ALTER TABLE dk_players ADD COLUMN mlb_team_id INTEGER REFERENCES mlb_teams(team_id);
        END IF;
    END $$""",
    # 2026-04-02: Add DK MLB lineup-confirmation columns to dk_players
    """DO $$ BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'dk_players' AND column_name = 'dk_in_starting_lineup'
        ) THEN
            ALTER TABLE dk_players ADD COLUMN dk_in_starting_lineup BOOLEAN;
        END IF;
    END $$""",
    """DO $$ BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'dk_players' AND column_name = 'dk_starting_lineup_order'
        ) THEN
            ALTER TABLE dk_players ADD COLUMN dk_starting_lineup_order INTEGER;
        END IF;
    END $$""",
    """DO $$ BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'dk_players' AND column_name = 'dk_team_lineup_confirmed'
        ) THEN
            ALTER TABLE dk_players ADD COLUMN dk_team_lineup_confirmed BOOLEAN;
        END IF;
    END $$""",
    # 2026-04-04: Add MLB HR signal columns to dk_players
    """DO $$ BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'dk_players' AND column_name = 'expected_hr'
        ) THEN
            ALTER TABLE dk_players ADD COLUMN expected_hr REAL;
        END IF;
    END $$""",
    """DO $$ BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'dk_players' AND column_name = 'hr_prob_1plus'
        ) THEN
            ALTER TABLE dk_players ADD COLUMN hr_prob_1plus REAL;
        END IF;
    END $$""",

    # 2026-04-13: Game-total model prediction column on nba_matchups
    """DO $$ BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'nba_matchups' AND column_name = 'our_game_total_pred'
        ) THEN
            ALTER TABLE nba_matchups ADD COLUMN our_game_total_pred DOUBLE PRECISION;
        END IF;
    END $$""",

    # 2026-04-13: Actual per-stat lines on dk_players for DFS model calibration
    """DO $$ BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'dk_players' AND column_name = 'actual_pts'
        ) THEN
            ALTER TABLE dk_players ADD COLUMN actual_pts REAL;
        END IF;
    END $$""",
    """DO $$ BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'dk_players' AND column_name = 'actual_reb'
        ) THEN
            ALTER TABLE dk_players ADD COLUMN actual_reb REAL;
        END IF;
    END $$""",
    """DO $$ BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'dk_players' AND column_name = 'actual_ast'
        ) THEN
            ALTER TABLE dk_players ADD COLUMN actual_ast REAL;
        END IF;
    END $$""",
    """DO $$ BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'dk_players' AND column_name = 'actual_stl'
        ) THEN
            ALTER TABLE dk_players ADD COLUMN actual_stl REAL;
        END IF;
    END $$""",
    """DO $$ BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'dk_players' AND column_name = 'actual_blk'
        ) THEN
            ALTER TABLE dk_players ADD COLUMN actual_blk REAL;
        END IF;
    END $$""",
    """DO $$ BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'dk_players' AND column_name = 'actual_tov'
        ) THEN
            ALTER TABLE dk_players ADD COLUMN actual_tov REAL;
        END IF;
    END $$""",
    """DO $$ BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'dk_players' AND column_name = 'actual_3pm'
        ) THEN
            ALTER TABLE dk_players ADD COLUMN actual_3pm REAL;
        END IF;
    END $$""",
]

INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_nba_team_stats_season ON nba_team_stats(team_id, season)",
    "CREATE INDEX IF NOT EXISTS idx_nba_player_stats_team ON nba_player_stats(team_id, season)",
    "CREATE INDEX IF NOT EXISTS idx_nba_player_stats_player ON nba_player_stats(player_id, season)",
    "CREATE INDEX IF NOT EXISTS idx_nba_player_game_logs_player_date ON nba_player_game_logs(player_id, game_date DESC)",
    "CREATE INDEX IF NOT EXISTS idx_nba_player_game_logs_team_date ON nba_player_game_logs(team_id, game_date DESC)",
    "CREATE INDEX IF NOT EXISTS idx_nba_team_game_logs_team_date ON nba_team_game_logs(team_id, game_date DESC)",
    "CREATE INDEX IF NOT EXISTS idx_nba_team_game_logs_opp_date ON nba_team_game_logs(opponent_team_id, game_date DESC)",
    "CREATE INDEX IF NOT EXISTS idx_nba_matchups_date ON nba_matchups(game_date)",
    "CREATE INDEX IF NOT EXISTS idx_dk_players_slate ON dk_players(slate_id, our_leverage DESC NULLS LAST)",
    "CREATE INDEX IF NOT EXISTS idx_dk_players_team ON dk_players(team_id, slate_id)",
    "CREATE INDEX IF NOT EXISTS idx_dk_lineups_slate ON dk_lineups(slate_id, strategy)",
    "CREATE INDEX IF NOT EXISTS idx_game_odds_history_lookup ON game_odds_history(sport, game_date, captured_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_game_odds_history_matchup ON game_odds_history(sport, matchup_id, captured_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_player_prop_history_lookup ON player_prop_history(sport, slate_id, market_key, captured_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_player_prop_history_player ON player_prop_history(sport, dk_player_id, market_key, captured_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_projection_runs_slate ON projection_runs(slate_id, created_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_projection_runs_model ON projection_runs(model_version, created_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_projection_snapshots_run ON projection_player_snapshots(run_id, dk_player_id)",
    "CREATE INDEX IF NOT EXISTS idx_projection_snapshots_slate ON projection_player_snapshots(slate_id, dk_player_id)",
    "CREATE INDEX IF NOT EXISTS idx_ownership_runs_slate ON ownership_runs(slate_id, created_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_ownership_runs_model ON ownership_runs(ownership_version, created_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_ownership_snapshots_run ON ownership_player_snapshots(run_id, dk_player_id)",
    "CREATE INDEX IF NOT EXISTS idx_ownership_snapshots_slate ON ownership_player_snapshots(slate_id, dk_player_id)",
    # MLB indexes
    "CREATE INDEX IF NOT EXISTS idx_mlb_matchups_date ON mlb_matchups(game_date)",
    "CREATE INDEX IF NOT EXISTS idx_mlb_batter_stats_team ON mlb_batter_stats(team_id, season)",
    "CREATE INDEX IF NOT EXISTS idx_mlb_batter_stats_player ON mlb_batter_stats(player_id, season)",
    "CREATE INDEX IF NOT EXISTS idx_mlb_pitcher_stats_team ON mlb_pitcher_stats(team_id, season)",
    "CREATE INDEX IF NOT EXISTS idx_mlb_pitcher_stats_player ON mlb_pitcher_stats(player_id, season)",
    "CREATE INDEX IF NOT EXISTS idx_mlb_team_stats_season ON mlb_team_stats(team_id, season)",
    "CREATE INDEX IF NOT EXISTS idx_dk_players_mlb_team ON dk_players(mlb_team_id, slate_id)",
    "CREATE INDEX IF NOT EXISTS idx_dk_slates_sport_date ON dk_slates(sport, slate_date DESC)",
    "CREATE INDEX IF NOT EXISTS idx_optimizer_jobs_lookup ON optimizer_jobs(client_token, sport, slate_id, status)",
    "CREATE INDEX IF NOT EXISTS idx_optimizer_jobs_created ON optimizer_jobs(created_at)",
    "CREATE INDEX IF NOT EXISTS idx_optimizer_job_lineups_job ON optimizer_job_lineups(job_id, lineup_num)",
]
