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
  - mlb_team_stats_history, mlb_pitcher_stats_history
                       Append-only dated snapshots of the two tables above,
                       so betting models can join point-in-time ("as of this
                       game's date") instead of leaking the current-state
                       row into historical predictions.
  - mlb_beat_articles, mlb_beat_facts
                       Beat-writer information-latency pilot: raw scraped
                       articles + DeepSeek-extracted structured facts
                       (starter changes, injury status, bullpen notes).

  Shared:
  - dk_slates          DraftKings slate per date (sport column: 'nba' | 'mlb')
  - dk_players         Player pool per slate (sport-agnostic structure)
  - dk_lineups         Generated lineups for strategy comparison
  - youtube_pick_videos, youtube_picks
                       Automated YouTube picks-channel pipeline: raw scraped
                       videos/transcripts + DeepSeek-extracted structured
                       picks (sport-agnostic, one channel's track record).
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
        home_sp_name TEXT,
        away_sp_id INTEGER,
        away_sp_name TEXT,
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
        fetched_at TIMESTAMPTZ DEFAULT NOW()
        -- NOTE: no UNIQUE(game_date, home_team_id, away_team_id). Row identity
        -- is game_id-first (MLB gamePk, UNIQUE above): a split doubleheader is
        -- two rows with the same (date, teams) and distinct gamePks, and a
        -- rescheduled makeup game MOVES its row to the new date (2026-07-07
        -- STL/MIL incident). Slot lookups use idx_mlb_matchups_slot.
    )
    """,

    # Immutable revisions from the official MLB schedule feed. The mutable
    # mlb_matchups row is a convenience cache; this table preserves what the
    # application knew at each capture.
    """
    CREATE TABLE IF NOT EXISTS mlb_schedule_revisions (
        id SERIAL PRIMARY KEY,
        matchup_id INTEGER NOT NULL REFERENCES mlb_matchups(id),
        game_id TEXT NOT NULL,
        revision_hash TEXT NOT NULL,
        game_date DATE NOT NULL,
        commence_time TIMESTAMPTZ,
        home_team_id INTEGER REFERENCES mlb_teams(team_id),
        away_team_id INTEGER REFERENCES mlb_teams(team_id),
        venue_id INTEGER,
        venue_name TEXT,
        home_sp_id INTEGER,
        home_sp_name TEXT,
        home_sp_status TEXT NOT NULL,
        away_sp_id INTEGER,
        away_sp_name TEXT,
        away_sp_status TEXT NOT NULL,
        game_status TEXT,
        source TEXT NOT NULL,
        source_available_at TIMESTAMPTZ NOT NULL,
        captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        raw_json JSONB NOT NULL,
        UNIQUE(game_id, revision_hash)
    )
    """,

    """
    CREATE TABLE IF NOT EXISTS mlb_starter_workload_snapshots (
        id SERIAL PRIMARY KEY,
        matchup_id INTEGER NOT NULL REFERENCES mlb_matchups(id),
        side TEXT NOT NULL CHECK (side IN ('home', 'away')),
        pitcher_id INTEGER NOT NULL,
        pitcher_name TEXT,
        event_commence TIMESTAMPTZ NOT NULL,
        last_start_date DATE,
        days_rest INTEGER,
        starts_sample INTEGER NOT NULL,
        pitches_last_start INTEGER,
        avg_pitches_last_3 DOUBLE PRECISION,
        avg_innings_last_3 DOUBLE PRECISION,
        season_ip_per_start DOUBLE PRECISION,
        expected_innings DOUBLE PRECISION,
        source TEXT NOT NULL,
        stats_through_at TIMESTAMPTZ NOT NULL,
        available_at TIMESTAMPTZ NOT NULL,
        transformation_version TEXT NOT NULL,
        raw_checksum TEXT NOT NULL,
        raw_json JSONB NOT NULL,
        UNIQUE(matchup_id, side, raw_checksum)
    )
    """,

    """
    CREATE TABLE IF NOT EXISTS mlb_team_offense_split_snapshots (
        id SERIAL PRIMARY KEY,
        team_id INTEGER NOT NULL REFERENCES mlb_teams(team_id),
        season TEXT NOT NULL,
        wrc_plus_vs_l DOUBLE PRECISION,
        wrc_plus_vs_r DOUBLE PRECISION,
        players_vs_l INTEGER NOT NULL,
        players_vs_r INTEGER NOT NULL,
        pa_weight_vs_l DOUBLE PRECISION,
        pa_weight_vs_r DOUBLE PRECISION,
        source TEXT NOT NULL,
        stats_through_at TIMESTAMPTZ NOT NULL,
        available_at TIMESTAMPTZ NOT NULL,
        transformation_version TEXT NOT NULL,
        raw_checksum TEXT NOT NULL,
        UNIQUE(team_id, season, raw_checksum)
    )
    """,

    """
    CREATE TABLE IF NOT EXISTS mlb_relief_appearances (
        id SERIAL PRIMARY KEY,
        matchup_id INTEGER NOT NULL REFERENCES mlb_matchups(id),
        game_id TEXT NOT NULL,
        game_date DATE NOT NULL,
        team_id INTEGER NOT NULL REFERENCES mlb_teams(team_id),
        pitcher_id INTEGER NOT NULL,
        pitcher_name TEXT NOT NULL,
        appearance_order INTEGER NOT NULL,
        outs INTEGER NOT NULL,
        pitches INTEGER,
        batters_faced INTEGER,
        hits INTEGER,
        earned_runs INTEGER,
        home_runs INTEGER,
        walks INTEGER,
        intentional_walks INTEGER,
        hit_batters INTEGER,
        strikeouts INTEGER,
        source TEXT NOT NULL,
        source_available_at TIMESTAMPTZ NOT NULL,
        captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        raw_checksum TEXT NOT NULL,
        raw_json JSONB NOT NULL,
        UNIQUE(game_id, team_id, pitcher_id, raw_checksum)
    )
    """,

    """
    CREATE TABLE IF NOT EXISTS mlb_bullpen_snapshots (
        id SERIAL PRIMARY KEY,
        matchup_id INTEGER NOT NULL REFERENCES mlb_matchups(id),
        team_id INTEGER NOT NULL REFERENCES mlb_teams(team_id),
        event_commence TIMESTAMPTZ NOT NULL,
        cutoff_at TIMESTAMPTZ NOT NULL,
        quality_window_days INTEGER NOT NULL,
        quality_outs INTEGER NOT NULL,
        quality_batters_faced INTEGER NOT NULL,
        reliever_era DOUBLE PRECISION,
        reliever_fip DOUBLE PRECISION,
        reliever_k_pct DOUBLE PRECISION,
        reliever_bb_pct DOUBLE PRECISION,
        pitches_1d INTEGER NOT NULL,
        pitches_3d INTEGER NOT NULL,
        pitches_7d INTEGER NOT NULL,
        appearances_1d INTEGER NOT NULL,
        appearances_3d INTEGER NOT NULL,
        appearances_7d INTEGER NOT NULL,
        relievers_used_1d INTEGER NOT NULL,
        relievers_used_3d INTEGER NOT NULL,
        relievers_back_to_back INTEGER NOT NULL,
        source TEXT NOT NULL,
        available_at TIMESTAMPTZ NOT NULL,
        transformation_version TEXT NOT NULL,
        raw_checksum TEXT NOT NULL,
        UNIQUE(matchup_id, team_id, raw_checksum)
    )
    """,

    """
    CREATE TABLE IF NOT EXISTS mlb_weather_forecast_snapshots (
        id SERIAL PRIMARY KEY,
        matchup_id INTEGER NOT NULL REFERENCES mlb_matchups(id),
        event_commence TIMESTAMPTZ NOT NULL,
        venue_name TEXT,
        latitude DOUBLE PRECISION NOT NULL,
        longitude DOUBLE PRECISION NOT NULL,
        provider TEXT NOT NULL,
        provider_model TEXT,
        provider_issued_at TIMESTAMPTZ,
        valid_at TIMESTAMPTZ NOT NULL,
        available_at TIMESTAMPTZ NOT NULL,
        temperature_f DOUBLE PRECISION,
        relative_humidity_pct DOUBLE PRECISION,
        precipitation_probability_pct DOUBLE PRECISION,
        wind_speed_mph DOUBLE PRECISION,
        wind_direction TEXT,
        roof_capability TEXT NOT NULL,
        roof_state TEXT NOT NULL,
        roof_source TEXT NOT NULL,
        source_status TEXT NOT NULL,
        raw_checksum TEXT NOT NULL,
        raw_json JSONB NOT NULL,
        UNIQUE(matchup_id, provider, raw_checksum)
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

    # ── MLB pitcher stats — dated snapshots (point-in-time history) ──
    # Same rationale as mlb_team_stats_history: mlb_pitcher_stats is a
    # single current-state row per (player, season), overwritten daily.
    """
    CREATE TABLE IF NOT EXISTS mlb_pitcher_stats_history (
        id SERIAL PRIMARY KEY,
        player_id INTEGER NOT NULL,
        season TEXT NOT NULL,
        snapshot_date DATE NOT NULL,
        team_id INTEGER REFERENCES mlb_teams(team_id),
        name TEXT NOT NULL,
        hand TEXT,
        games INTEGER,
        games_started INTEGER,
        innings_pitched DOUBLE PRECISION,
        ip_per_start DOUBLE PRECISION,
        k_per_9 DOUBLE PRECISION,
        bb_per_9 DOUBLE PRECISION,
        fip DOUBLE PRECISION,
        xfip DOUBLE PRECISION,
        era DOUBLE PRECISION,
        source TEXT,
        available_at TIMESTAMPTZ,
        stats_through_at TIMESTAMPTZ,
        sample_size INTEGER,
        window_label TEXT,
        transformation_version TEXT,
        raw_checksum TEXT,
        fetched_at TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE(player_id, season, snapshot_date)
    )
    """,

    # ── MLB beat-writer articles (information-latency pilot) ──
    # Raw scraped articles. published_at is the SITE'S OWN displayed publish
    # timestamp (parsed from the page), not scrape time -- this is the whole
    # point-in-time signal the pilot depends on. See CLAUDE.md "MLB
    # Beat-Writer Information-Latency Pilot" (2026-07-05).
    """
    CREATE TABLE IF NOT EXISTS mlb_beat_articles (
        id SERIAL PRIMARY KEY,
        source TEXT NOT NULL,
        team_id INTEGER REFERENCES mlb_teams(team_id),
        url TEXT NOT NULL UNIQUE,
        title TEXT NOT NULL,
        published_at TIMESTAMPTZ,
        raw_text TEXT,
        scraped_at TIMESTAMPTZ DEFAULT NOW()
    )
    """,

    # ── MLB beat-writer extracted facts (DeepSeek structured extraction) ──
    # One row per extracted fact (or none, for most articles). quote is a
    # mandatory verbatim substring of the source article's raw_text -- the
    # grounding check against hallucination. model_version distinguishes
    # prompt/model revisions so they never silently mix in the Phase 0/1
    # feasibility + timing-study reports.
    """
    CREATE TABLE IF NOT EXISTS mlb_beat_facts (
        id SERIAL PRIMARY KEY,
        article_id INTEGER NOT NULL REFERENCES mlb_beat_articles(id),
        fact_type TEXT NOT NULL,
        team_id INTEGER REFERENCES mlb_teams(team_id),
        player_name TEXT,
        description TEXT NOT NULL,
        quote TEXT NOT NULL,
        model_version TEXT NOT NULL,
        extracted_at TIMESTAMPTZ DEFAULT NOW()
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

    # ── MLB team stats — dated snapshots (point-in-time history) ──
    # mlb_team_stats above is a single current-state row per (team, season),
    # overwritten daily — it cannot answer "what did we know on date X".
    # This table is append-only (one row per team/season/day) so betting
    # models can join "the latest snapshot at or before this game's date"
    # instead of leaking future-season stats into past-game predictions.
    # See CLAUDE.md "MLB Moneyline — Point-in-Time Leak Finding" (2026-07-05).
    """
    CREATE TABLE IF NOT EXISTS mlb_team_stats_history (
        id SERIAL PRIMARY KEY,
        team_id INTEGER NOT NULL REFERENCES mlb_teams(team_id),
        season TEXT NOT NULL,
        snapshot_date DATE NOT NULL,
        team_wrc_plus DOUBLE PRECISION,
        team_k_pct DOUBLE PRECISION,
        team_bb_pct DOUBLE PRECISION,
        team_iso DOUBLE PRECISION,
        team_ops DOUBLE PRECISION,
        bullpen_era DOUBLE PRECISION,
        bullpen_fip DOUBLE PRECISION,
        staff_k_pct DOUBLE PRECISION,
        staff_bb_pct DOUBLE PRECISION,
        source TEXT,
        available_at TIMESTAMPTZ,
        stats_through_at TIMESTAMPTZ,
        sample_size INTEGER,
        window_label TEXT,
        transformation_version TEXT,
        raw_checksum TEXT,
        fetched_at TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE(team_id, season, snapshot_date)
    )
    """,

    # ── DFS slates ────────────────────────────────────────────
    # sport: 'nba' | 'mlb' — distinguishes same-date slates across sports.
    # UNIQUE includes sport so an NBA GPP and MLB GPP on the same date
    # are stored as separate rows.
    # The next table is intentionally separate from DFS; the dk_slates table
    # follows immediately after it.
    # MLB baseball-only home run training rows.
    # This excludes DK salary/position/ownership and market odds.
    """
    -- Baseball-only HR training rows; excludes DK salary/position/ownership and market odds.
    CREATE TABLE IF NOT EXISTS mlb_homerun_training_games (
        id SERIAL PRIMARY KEY,
        season TEXT NOT NULL,
        game_date DATE NOT NULL,
        game_id TEXT NOT NULL,
        hitter_mlb_id INTEGER NOT NULL,
        hitter_name TEXT NOT NULL,
        hitter_team_id INTEGER REFERENCES mlb_teams(team_id),
        hitter_team_abbrev TEXT,
        opponent_team_id INTEGER REFERENCES mlb_teams(team_id),
        opponent_team_abbrev TEXT,
        is_home BOOLEAN,
        ballpark TEXT,
        batting_order INTEGER,
        plate_appearances INTEGER,
        at_bats INTEGER,
        opposing_sp_mlb_id INTEGER,
        opposing_sp_name TEXT,
        opposing_sp_hand TEXT,
        hitter_games INTEGER,
        hitter_pa_pg DOUBLE PRECISION,
        hitter_hr_pg DOUBLE PRECISION,
        hitter_iso DOUBLE PRECISION,
        hitter_slg DOUBLE PRECISION,
        hitter_wrc_plus DOUBLE PRECISION,
        hitter_split_wrc_plus DOUBLE PRECISION,
        pitcher_games INTEGER,
        pitcher_ip_pg DOUBLE PRECISION,
        pitcher_hr_per_9 DOUBLE PRECISION,
        pitcher_hr_fb_pct DOUBLE PRECISION,
        pitcher_xfip DOUBLE PRECISION,
        pitcher_fip DOUBLE PRECISION,
        pitcher_k_per_9 DOUBLE PRECISION,
        pitcher_bb_per_9 DOUBLE PRECISION,
        pitcher_whip DOUBLE PRECISION,
        pitcher_era DOUBLE PRECISION,
        park_runs_factor DOUBLE PRECISION,
        park_hr_factor DOUBLE PRECISION,
        weather_temp INTEGER,
        wind_speed INTEGER,
        wind_direction TEXT,
        actual_hr INTEGER NOT NULL DEFAULT 0,
        hit_hr_1plus BOOLEAN NOT NULL DEFAULT FALSE,
        feature_source TEXT NOT NULL DEFAULT 'season_aggregate',
        source TEXT NOT NULL DEFAULT 'mlb_statsapi_boxscore',
        fetched_at TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE(game_id, hitter_mlb_id)
    )
    """,

    # DFS slates.
    """
    -- DFS slates.
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
        dk_status TEXT,
        is_out BOOLEAN DEFAULT FALSE,
        actual_fpts REAL,
        actual_own_pct REAL,
        actual_hr INTEGER,
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

    # ── YouTube picks-channel pipeline (sport-agnostic) ────────
    # Automated ingestion for betting-picks YouTube channels: RSS-based new-
    # video detection, proxied transcript fetch, DeepSeek structured pick
    # extraction. See CLAUDE.md "YouTube Picks Channel Tracking".
    #
    # youtube_pick_channels is written from the web app (adding a channel
    # via the UI) and read by the Python ingest script (which channels to
    # scrape) -- defined here too (not just in web/src/db/ensure-schema.ts)
    # so it self-provisions regardless of which side runs first, same
    # pattern already used for game_odds_history/player_prop_history.
    """
    CREATE TABLE IF NOT EXISTS youtube_pick_channels (
        id SERIAL PRIMARY KEY,
        channel_id TEXT NOT NULL UNIQUE,
        channel_name TEXT NOT NULL,
        handle TEXT,
        active BOOLEAN NOT NULL DEFAULT TRUE,
        added_at TIMESTAMPTZ DEFAULT NOW()
    )
    """,

    """
    CREATE TABLE IF NOT EXISTS youtube_pick_videos (
        id SERIAL PRIMARY KEY,
        channel_id TEXT NOT NULL,
        channel_name TEXT NOT NULL,
        video_id TEXT NOT NULL UNIQUE,
        title TEXT NOT NULL,
        published_at TIMESTAMPTZ,
        transcript_text TEXT,
        scraped_at TIMESTAMPTZ DEFAULT NOW()
    )
    """,

    # One row per extracted pick (or none, for most non-picks videos). quote
    # is a mandatory verbatim substring of the transcript -- same grounding
    # discipline as mlb_beat_facts. status/matchup_ref are settlement fields,
    # populated by a later phase (not built yet) -- pending means "extracted,
    # not yet resolved to a real game or graded."
    """
    CREATE TABLE IF NOT EXISTS youtube_picks (
        id SERIAL PRIMARY KEY,
        video_id INTEGER NOT NULL REFERENCES youtube_pick_videos(id),
        sport TEXT NOT NULL,
        bet_type TEXT NOT NULL,
        subject TEXT NOT NULL,
        opponent TEXT,
        selection TEXT NOT NULL,
        odds_american INTEGER,
        game_context TEXT,
        confidence_label TEXT,
        quote TEXT NOT NULL,
        model_version TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'pending',
        matchup_ref TEXT,
        result_detail TEXT,
        extracted_at TIMESTAMPTZ DEFAULT NOW(),
        settled_at TIMESTAMPTZ
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
    CREATE TABLE IF NOT EXISTS mlb_homerun_runs (
        id SERIAL PRIMARY KEY,
        slate_id INTEGER NOT NULL REFERENCES dk_slates(id) ON DELETE CASCADE,
        analysis_version TEXT NOT NULL,
        source TEXT NOT NULL,
        config_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        notes TEXT,
        created_at TIMESTAMPTZ DEFAULT NOW()
    )
    """,

    """
    CREATE TABLE IF NOT EXISTS mlb_homerun_player_snapshots (
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

    # ── Soccer / World Cup teams ──────────────────────────────
    # National teams (48 at the 2026 World Cup).  Separate ID space from
    # NBA `teams` / `mlb_teams`.  `abbreviation` holds the 3-letter FIFA code
    # when known; name is the canonical conflict key because odds feeds are the
    # source of truth for naming and a few nations qualify late.
    """
    CREATE TABLE IF NOT EXISTS soccer_teams (
        team_id SERIAL PRIMARY KEY,
        name TEXT NOT NULL UNIQUE,
        abbreviation TEXT,
        dk_abbrev TEXT,
        confederation TEXT,
        logo_url TEXT DEFAULT '',
        created_at TIMESTAMPTZ DEFAULT NOW()
    )
    """,

    # ── Daily soccer schedule + Vegas odds (3-way) ────────────
    # Soccer differs from NBA/MLB: the moneyline is 3-way (home/draw/away),
    # vegas_total is GOALS (e.g. 2.75), and home_implied/away_implied are
    # expected goals per side, derived from the supremacy implied by win prob.
    """
    CREATE TABLE IF NOT EXISTS soccer_matchups (
        id SERIAL PRIMARY KEY,
        game_date DATE NOT NULL,
        game_id TEXT UNIQUE,
        commence_time TIMESTAMPTZ,
        home_team_id INTEGER REFERENCES soccer_teams(team_id),
        away_team_id INTEGER REFERENCES soccer_teams(team_id),
        stage TEXT,
        vegas_total DOUBLE PRECISION,
        home_ml INTEGER,
        draw_ml INTEGER,
        away_ml INTEGER,
        vegas_prob_home DOUBLE PRECISION,
        vegas_prob_draw DOUBLE PRECISION,
        vegas_prob_away DOUBLE PRECISION,
        home_implied DOUBLE PRECISION,
        away_implied DOUBLE PRECISION,
        home_score INTEGER,
        away_score INTEGER,
        fetched_at TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE(game_date, home_team_id, away_team_id)
    )
    """,

    # ── Soccer team strength ratings (our model) ──────────────
    # elo: World-Football-style Elo from historical international results.
    # attack/defense: Dixon-Coles Poisson coefficients (log scale).
    # Re-trained from history + completed World Cup games; feeds the bivariate
    # Poisson goal model in model/soccer_predictions.py.
    """
    CREATE TABLE IF NOT EXISTS soccer_team_ratings (
        team_id INTEGER PRIMARY KEY REFERENCES soccer_teams(team_id),
        elo DOUBLE PRECISION,
        attack DOUBLE PRECISION,
        defense DOUBLE PRECISION,
        matches INTEGER DEFAULT 0,
        rating_date DATE,
        updated_at TIMESTAMPTZ DEFAULT NOW()
    )
    """,

    # ── Soccer model global params (singleton) ────────────────
    # mu (log base goals) + home_adv from the Poisson fit.  Stored in the DB so
    # the prediction step is self-sufficient in CI (the data/ json cache is
    # gitignored and absent on fresh checkouts).
    """
    CREATE TABLE IF NOT EXISTS soccer_model_params (
        id INTEGER PRIMARY KEY DEFAULT 1,
        mu DOUBLE PRECISION,
        home_adv DOUBLE PRECISION,
        n_matches INTEGER,
        trained_at DATE,
        updated_at TIMESTAMPTZ DEFAULT NOW(),
        CONSTRAINT soccer_model_params_singleton CHECK (id = 1)
    )
    """,

    # ── World Cup group assignments (derived from fixtures) ───
    # team_id → group label.  Populated by model/soccer_futures.py from the
    # loaded group-stage fixtures; group-winner bets only activate for groups
    # with a clean set of teams (no fabricated groups).
    """
    CREATE TABLE IF NOT EXISTS soccer_groups (
        team_id INTEGER PRIMARY KEY REFERENCES soccer_teams(team_id),
        group_label TEXT,
        derived_at TIMESTAMPTZ DEFAULT NOW()
    )
    """,

    # ── Soccer bet ledger (the auditable running list) ───────
    # One row per (bet_type, scope, selection, model_version), upserted each run.
    # Rows LOCK at event_commence so the backtest uses the closing recommendation
    # we committed to — never edited after the event starts.
    """
    CREATE TABLE IF NOT EXISTS soccer_bets (
        id SERIAL PRIMARY KEY,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        updated_at TIMESTAMPTZ DEFAULT NOW(),
        model_version TEXT NOT NULL,
        bet_type TEXT NOT NULL,
        scope TEXT NOT NULL,
        matchup_id INTEGER REFERENCES soccer_matchups(id),
        subject_team_id INTEGER REFERENCES soccer_teams(team_id),
        selection_label TEXT NOT NULL,
        market_odds INTEGER,
        market_decimal DOUBLE PRECISION,
        market_prob DOUBLE PRECISION,
        book TEXT,
        our_prob DOUBLE PRECISION NOT NULL,
        edge DOUBLE PRECISION,
        ev DOUBLE PRECISION,
        stars SMALLINT NOT NULL,
        inputs_json JSONB,
        event_commence TIMESTAMPTZ,
        locked BOOLEAN DEFAULT FALSE,
        status TEXT NOT NULL DEFAULT 'pending',
        result_detail TEXT,
        settled_at TIMESTAMPTZ,
        UNIQUE(bet_type, scope, selection_label, model_version)
    )
    """,

    # ── Append-only audit trail for every bet recommendation ─
    # Each refresh logs each selection's view, so the full lineage of how a
    # recommendation evolved is reproducible (accountability).
    """
    CREATE TABLE IF NOT EXISTS soccer_bet_snapshots (
        id SERIAL PRIMARY KEY,
        bet_id INTEGER REFERENCES soccer_bets(id) ON DELETE CASCADE,
        captured_at TIMESTAMPTZ DEFAULT NOW(),
        capture_key TEXT,
        our_prob DOUBLE PRECISION,
        market_prob DOUBLE PRECISION,
        market_odds INTEGER,
        edge DOUBLE PRECISION,
        ev DOUBLE PRECISION,
        stars SMALLINT
    )
    """,

    # ── Tennis matches (MVP: Wimbledon Vegas odds only) ──────
    # Single sport-agnostic table; player NAMES are stored inline from the Odds
    # API feed (home_team/away_team), so no separate players table is needed for
    # the odds-only MVP.  2-way market (no draw) → simpler than soccer.  game_id
    # is the Odds API event id.  Result columns are written later by the
    # tennis-data.co.uk settlement job (set/game scores → settle bets).
    """
    CREATE TABLE IF NOT EXISTS tennis_matches (
        id SERIAL PRIMARY KEY,
        game_id TEXT UNIQUE,
        tour TEXT NOT NULL,                 -- 'ATP' | 'WTA'
        tournament TEXT DEFAULT 'Wimbledon',
        match_date DATE NOT NULL,
        commence_time TIMESTAMPTZ,
        home_player TEXT NOT NULL,
        away_player TEXT NOT NULL,
        home_ml INTEGER,
        away_ml INTEGER,
        home_win_prob DOUBLE PRECISION,     -- vig-removed 2-way consensus
        away_win_prob DOUBLE PRECISION,
        total_games_line DOUBLE PRECISION,
        over_odds INTEGER,
        under_odds INTEGER,
        set_handicap DOUBLE PRECISION,      -- favorite handicap line (games/sets)
        handicap_home_odds INTEGER,
        handicap_away_odds INTEGER,
        n_books INTEGER,
        home_sets INTEGER,
        away_sets INTEGER,
        home_games INTEGER,
        away_games INTEGER,
        winner TEXT,                        -- 'home' | 'away' | 'retired'
        -- our model (Elo + market anchor); NULL until model/tennis_predictions runs
        our_prob_home DOUBLE PRECISION,
        our_prob_away DOUBLE PRECISION,
        our_total_pred DOUBLE PRECISION,
        fetched_at TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE(tour, match_date, home_player, away_player)
    )
    """,

    # ── Tennis player ratings (from match history, per tour) ──
    # Rebuilt by ingest/tennis_history.py from two sources (see memory
    # tennis-data-sources):
    #   ATP — TML-Database CSVs (with serve stats). norm_name = full-name concat.
    #   WTA — tennis-data.co.uk xlsx (results only, NO serve stats). norm_name =
    #         (surname+first-initial), since tennis-data carries only "Surname I.".
    # overall_elo: all-surface Elo (robust). grass_elo: grass-only Elo (sparse →
    # blended with overall in predictions). serve/return points-won% on grass
    # (ATP only; NULL for WTA) feed totals later.
    # NOTE the norm_name key differs by tour → the predictions layer must derive
    # the WTA lookup key as (surname+initial), not the full-name concat.
    """
    CREATE TABLE IF NOT EXISTS tennis_player_ratings (
        tour TEXT NOT NULL,
        norm_name TEXT NOT NULL,
        display_name TEXT,
        overall_elo DOUBLE PRECISION,
        grass_elo DOUBLE PRECISION,
        grass_matches INTEGER DEFAULT 0,
        serve_pts_won_pct DOUBLE PRECISION,
        return_pts_won_pct DOUBLE PRECISION,
        matches INTEGER DEFAULT 0,
        updated_at TIMESTAMPTZ DEFAULT NOW(),
        PRIMARY KEY (tour, norm_name)
    )
    """,

    # ── Tennis bet ledger (star-rated recommendations) ───────
    # Mirrors soccer_bets: one row per (bet_type, match, side, model_version),
    # locked at event_commence so the backtest uses the closing recommendation.
    # MVP rates moneyline only (the market that carries edge).
    """
    CREATE TABLE IF NOT EXISTS tennis_bets (
        id SERIAL PRIMARY KEY,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        updated_at TIMESTAMPTZ DEFAULT NOW(),
        model_version TEXT NOT NULL,
        bet_type TEXT NOT NULL,
        match_id INTEGER REFERENCES tennis_matches(id),
        side TEXT,                          -- 'home' | 'away'
        selection_label TEXT NOT NULL,
        market_odds INTEGER,
        market_decimal DOUBLE PRECISION,
        market_prob DOUBLE PRECISION,
        our_prob DOUBLE PRECISION NOT NULL,
        edge DOUBLE PRECISION,
        ev DOUBLE PRECISION,
        stars SMALLINT NOT NULL,
        inputs_json JSONB,
        event_commence TIMESTAMPTZ,
        locked BOOLEAN DEFAULT FALSE,
        status TEXT NOT NULL DEFAULT 'pending',
        result_detail TEXT,
        settled_at TIMESTAMPTZ,
        UNIQUE(bet_type, match_id, side, model_version)
    )
    """,

    # ── Player-level historical stats for the stat-based first-scorer model ──
    # Aggregated from StatsBomb open data (WC 2018 + 2022 + continental tourneys).
    # xg_per_90 is the primary input to firstscorer-v3.  normalized_name is the
    # accent-stripped lowercase key for matching against market player names.
    """
    CREATE TABLE IF NOT EXISTS soccer_player_stats (
        id SERIAL PRIMARY KEY,
        player_name TEXT NOT NULL,
        normalized_name TEXT NOT NULL,
        team_id INTEGER REFERENCES soccer_teams(team_id),
        team_name TEXT,
        season TEXT NOT NULL DEFAULT 'combined',
        position TEXT,
        matches INTEGER DEFAULT 0,
        minutes_played REAL DEFAULT 0,
        goals INTEGER DEFAULT 0,
        shots INTEGER DEFAULT 0,
        shots_on_target INTEGER DEFAULT 0,
        xg REAL DEFAULT 0.0,
        npxg REAL DEFAULT 0.0,
        goals_per_90 REAL,
        shots_per_90 REAL,
        xg_per_90 REAL,
        npxg_per_90 REAL,
        is_penalty_taker BOOLEAN DEFAULT FALSE,
        updated_at TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE (player_name, season)
    )
    """,
]

MIGRATIONS = [
    # 2026-04-20: Persist actual home run outcomes for MLB HR model tracking
    """DO $$ BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'dk_players' AND column_name = 'actual_hr'
        ) THEN
            ALTER TABLE dk_players ADD COLUMN actual_hr INTEGER;
        END IF;
    END $$""",

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
    # 2026-07-07: Doubleheader support — drop the one-row-per-(date,teams)
    # constraint; row identity is game_id-first (gamePk). A split DH is two
    # rows with the same slot; a rescheduled makeup game moves its row.
    """DO $$ BEGIN
        IF EXISTS (
            SELECT 1 FROM pg_constraint
            WHERE conname = 'mlb_matchups_game_date_home_team_id_away_team_id_key'
        ) THEN
            ALTER TABLE mlb_matchups
                DROP CONSTRAINT mlb_matchups_game_date_home_team_id_away_team_id_key;
        END IF;
    END $$""",
    "CREATE INDEX IF NOT EXISTS idx_mlb_matchups_slot ON mlb_matchups(game_date, home_team_id, away_team_id)",
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
            WHERE table_name = 'mlb_matchups' AND column_name = 'home_sp_name'
        ) THEN
            ALTER TABLE mlb_matchups ADD COLUMN home_sp_name TEXT;
        END IF;
    END $$""",
    """DO $$ BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'mlb_matchups' AND column_name = 'away_sp_name'
        ) THEN
            ALTER TABLE mlb_matchups ADD COLUMN away_sp_name TEXT;
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
    # 2026-06-13: Soccer prediction model (P2) — our own numbers vs the market.
    # Bivariate Poisson goal model output, written by model/soccer_predictions.py.
    """ALTER TABLE soccer_matchups ADD COLUMN IF NOT EXISTS our_total_pred DOUBLE PRECISION""",
    """ALTER TABLE soccer_matchups ADD COLUMN IF NOT EXISTS our_home_xg DOUBLE PRECISION""",
    """ALTER TABLE soccer_matchups ADD COLUMN IF NOT EXISTS our_away_xg DOUBLE PRECISION""",
    """ALTER TABLE soccer_matchups ADD COLUMN IF NOT EXISTS our_prob_home DOUBLE PRECISION""",
    """ALTER TABLE soccer_matchups ADD COLUMN IF NOT EXISTS our_prob_draw DOUBLE PRECISION""",
    """ALTER TABLE soccer_matchups ADD COLUMN IF NOT EXISTS our_prob_away DOUBLE PRECISION""",
    # 2026-06-13: Over/Under consensus prices (at the consensus total line) so the
    # O/U bet model can compute EV.  vegas_total holds the line.
    """ALTER TABLE soccer_matchups ADD COLUMN IF NOT EXISTS over_odds INTEGER""",
    """ALTER TABLE soccer_matchups ADD COLUMN IF NOT EXISTS under_odds INTEGER""",
    # 2026-06-16: Goal-timing stats for firstscorer-v4 early-goal model.
    """ALTER TABLE soccer_player_stats ADD COLUMN IF NOT EXISTS first_scorer_matches INTEGER DEFAULT 0""",
    """ALTER TABLE soccer_player_stats ADD COLUMN IF NOT EXISTS early_goals INTEGER DEFAULT 0""",
    """ALTER TABLE soccer_player_stats ADD COLUMN IF NOT EXISTS early_goal_rate REAL""",
    """ALTER TABLE soccer_player_stats ADD COLUMN IF NOT EXISTS first_scorer_rate REAL""",
    # 2026-06-14: Player-level historical stats for firstscorer-v3.
    """CREATE TABLE IF NOT EXISTS soccer_player_stats (
        id SERIAL PRIMARY KEY,
        player_name TEXT NOT NULL,
        normalized_name TEXT NOT NULL,
        team_id INTEGER REFERENCES soccer_teams(team_id),
        team_name TEXT,
        season TEXT NOT NULL DEFAULT 'combined',
        position TEXT,
        matches INTEGER DEFAULT 0,
        minutes_played REAL DEFAULT 0,
        goals INTEGER DEFAULT 0,
        shots INTEGER DEFAULT 0,
        shots_on_target INTEGER DEFAULT 0,
        xg REAL DEFAULT 0.0,
        npxg REAL DEFAULT 0.0,
        goals_per_90 REAL,
        shots_per_90 REAL,
        xg_per_90 REAL,
        npxg_per_90 REAL,
        is_penalty_taker BOOLEAN DEFAULT FALSE,
        first_scorer_matches INTEGER DEFAULT 0,
        early_goals INTEGER DEFAULT 0,
        early_goal_rate REAL,
        first_scorer_rate REAL,
        updated_at TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE (player_name, season)
    )""",
    # 2026-06-16: Actual first-scorer results per WC 2026 match (TheSportsDB source).
    # Enables calibration: do our model's favorites actually score first?
    """CREATE TABLE IF NOT EXISTS soccer_match_scorers (
        id SERIAL PRIMARY KEY,
        game_id TEXT UNIQUE,
        game_date DATE,
        scorer_name TEXT NOT NULL,
        scorer_team TEXT,
        goal_minute INTEGER,
        tsdb_event_id TEXT,
        source TEXT DEFAULT 'thesportsdb',
        created_at TIMESTAMPTZ DEFAULT NOW()
    )""",
    # 2026-06-16: Pinnacle h2h — sharpest-book comparison alongside consensus.
    """ALTER TABLE soccer_matchups ADD COLUMN IF NOT EXISTS pinnacle_prob_home DOUBLE PRECISION""",
    """ALTER TABLE soccer_matchups ADD COLUMN IF NOT EXISTS pinnacle_prob_draw DOUBLE PRECISION""",
    """ALTER TABLE soccer_matchups ADD COLUMN IF NOT EXISTS pinnacle_prob_away DOUBLE PRECISION""",
    # 2026-06-16: All goals per match (not just first scorer) for result overlay.
    """CREATE TABLE IF NOT EXISTS soccer_match_goals (
        id SERIAL PRIMARY KEY,
        game_id TEXT NOT NULL,
        game_date DATE NOT NULL,
        player_name TEXT NOT NULL,
        player_team TEXT,
        assist_name TEXT,
        goal_minute INTEGER,
        is_first_goal BOOLEAN DEFAULT FALSE,
        source TEXT DEFAULT 'thesportsdb',
        created_at TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE (game_id, player_name, goal_minute)
    )""",
    "ALTER TABLE soccer_match_goals ADD COLUMN IF NOT EXISTS assist_name TEXT",
    # 2026-06-19: matchday-3 motivation / dead-rubber state, written by
    # model/soccer_motivation.py and applied in soccer_predictions.py.
    "ALTER TABLE soccer_matchups ADD COLUMN IF NOT EXISTS motivation TEXT",
    # 2026-06-18: MLB game-total model — our own number vs the market.
    # Ridge residual-over-Vegas total, written by model/mlb_game_total_model.py.
    # Mirrors nba_matchups.our_game_total_pred / soccer_matchups.our_total_pred.
    "ALTER TABLE mlb_matchups ADD COLUMN IF NOT EXISTS our_total_pred DOUBLE PRECISION",
    # 2026-06-18: MLB moneyline model — our independent P(home win) vs the market.
    # Market-anchored logistic, written by model/mlb_moneyline_model.py.
    "ALTER TABLE mlb_matchups ADD COLUMN IF NOT EXISTS our_prob_home DOUBLE PRECISION",
    # 2026-06-18: first-pitch timestamp so MLB bets can lock at game start
    # (the kickoff-lock the soccer accountability ledger uses).
    "ALTER TABLE mlb_matchups ADD COLUMN IF NOT EXISTS commence_time TIMESTAMPTZ",
    # 2026-06-18: MLB bet ledger — parity with the soccer accountability framework.
    # One immutable, model_version-stamped, lock-at-first-pitch row per rated bet;
    # totals + moneyline. Mirrors soccer_bets / soccer_bet_snapshots.
    """
    CREATE TABLE IF NOT EXISTS mlb_bets (
        id SERIAL PRIMARY KEY,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        updated_at TIMESTAMPTZ DEFAULT NOW(),
        model_version TEXT NOT NULL,
        bet_type TEXT NOT NULL,
        scope TEXT NOT NULL,
        matchup_id INTEGER REFERENCES mlb_matchups(id),
        subject_team_id INTEGER REFERENCES mlb_teams(team_id),
        selection_label TEXT NOT NULL,
        market_odds INTEGER,
        market_decimal DOUBLE PRECISION,
        market_prob DOUBLE PRECISION,
        book TEXT,
        our_prob DOUBLE PRECISION NOT NULL,
        edge DOUBLE PRECISION,
        ev DOUBLE PRECISION,
        stars SMALLINT NOT NULL,
        inputs_json JSONB,
        event_commence TIMESTAMPTZ,
        locked BOOLEAN DEFAULT FALSE,
        status TEXT NOT NULL DEFAULT 'pending',
        result_detail TEXT,
        settled_at TIMESTAMPTZ,
        UNIQUE(bet_type, scope, selection_label, model_version)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS mlb_bet_snapshots (
        id SERIAL PRIMARY KEY,
        bet_id INTEGER REFERENCES mlb_bets(id) ON DELETE CASCADE,
        captured_at TIMESTAMPTZ DEFAULT NOW(),
        capture_key TEXT,
        our_prob DOUBLE PRECISION,
        market_prob DOUBLE PRECISION,
        market_odds INTEGER,
        edge DOUBLE PRECISION,
        ev DOUBLE PRECISION,
        stars SMALLINT
    )
    """,
    # 2026-07-11: immutable MLB game-line prediction provenance.
    """CREATE TABLE IF NOT EXISTS mlb_prediction_runs (
        id SERIAL PRIMARY KEY,
        generated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        trained_through DATE,
        model_version TEXT NOT NULL,
        git_sha TEXT,
        origin TEXT NOT NULL CHECK (origin IN ('prospective', 'retrospective_backfill')),
        source TEXT NOT NULL,
        config_json JSONB NOT NULL DEFAULT '{}'::jsonb
    )""",
    """CREATE TABLE IF NOT EXISTS mlb_game_prediction_snapshots (
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
    )""",
    "ALTER TABLE mlb_bets ADD COLUMN IF NOT EXISTS prediction_snapshot_id INTEGER REFERENCES mlb_game_prediction_snapshots(id)",
    "ALTER TABLE mlb_bets ADD COLUMN IF NOT EXISTS origin TEXT NOT NULL DEFAULT 'legacy'",
    "ALTER TABLE mlb_bets ADD COLUMN IF NOT EXISTS odds_snapshot_id INTEGER REFERENCES game_odds_history(id)",
    "ALTER TABLE mlb_bet_snapshots ADD COLUMN IF NOT EXISTS prediction_snapshot_id INTEGER REFERENCES mlb_game_prediction_snapshots(id)",
    "ALTER TABLE mlb_bet_snapshots ADD COLUMN IF NOT EXISTS odds_snapshot_id INTEGER REFERENCES game_odds_history(id)",
    "ALTER TABLE mlb_bet_snapshots ADD COLUMN IF NOT EXISTS book TEXT",
    "ALTER TABLE mlb_bet_snapshots ADD COLUMN IF NOT EXISTS selection_label TEXT",
    "ALTER TABLE mlb_bet_snapshots ADD COLUMN IF NOT EXISTS market_line DOUBLE PRECISION",
    "CREATE INDEX IF NOT EXISTS idx_mlb_prediction_runs_origin ON mlb_prediction_runs(origin, model_version, generated_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_mlb_game_prediction_matchup ON mlb_game_prediction_snapshots(matchup_id, market, created_at DESC)",
    """CREATE OR REPLACE FUNCTION reject_mlb_prediction_mutation()
       RETURNS trigger LANGUAGE plpgsql AS $$
       BEGIN
         RAISE EXCEPTION 'MLB prediction provenance is append-only';
       END $$""",
    """DO $$ BEGIN
       IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'mlb_prediction_runs_immutable') THEN
         CREATE TRIGGER mlb_prediction_runs_immutable BEFORE UPDATE OR DELETE
         ON mlb_prediction_runs FOR EACH ROW EXECUTE FUNCTION reject_mlb_prediction_mutation();
       END IF;
       IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'mlb_game_prediction_snapshots_immutable') THEN
         CREATE TRIGGER mlb_game_prediction_snapshots_immutable BEFORE UPDATE OR DELETE
         ON mlb_game_prediction_snapshots FOR EACH ROW EXECUTE FUNCTION reject_mlb_prediction_mutation();
       END IF;
    END $$""",
    "ALTER TABLE mlb_team_stats_history ADD COLUMN IF NOT EXISTS source TEXT",
    "ALTER TABLE mlb_team_stats_history ADD COLUMN IF NOT EXISTS available_at TIMESTAMPTZ",
    "ALTER TABLE mlb_team_stats_history ADD COLUMN IF NOT EXISTS stats_through_at TIMESTAMPTZ",
    "ALTER TABLE mlb_team_stats_history ADD COLUMN IF NOT EXISTS sample_size INTEGER",
    "ALTER TABLE mlb_team_stats_history ADD COLUMN IF NOT EXISTS window_label TEXT",
    "ALTER TABLE mlb_team_stats_history ADD COLUMN IF NOT EXISTS transformation_version TEXT",
    "ALTER TABLE mlb_team_stats_history ADD COLUMN IF NOT EXISTS raw_checksum TEXT",
    "ALTER TABLE mlb_pitcher_stats_history ADD COLUMN IF NOT EXISTS source TEXT",
    "ALTER TABLE mlb_pitcher_stats_history ADD COLUMN IF NOT EXISTS available_at TIMESTAMPTZ",
    "ALTER TABLE mlb_pitcher_stats_history ADD COLUMN IF NOT EXISTS stats_through_at TIMESTAMPTZ",
    "ALTER TABLE mlb_pitcher_stats_history ADD COLUMN IF NOT EXISTS sample_size INTEGER",
    "ALTER TABLE mlb_pitcher_stats_history ADD COLUMN IF NOT EXISTS window_label TEXT",
    "ALTER TABLE mlb_pitcher_stats_history ADD COLUMN IF NOT EXISTS transformation_version TEXT",
    "ALTER TABLE mlb_pitcher_stats_history ADD COLUMN IF NOT EXISTS raw_checksum TEXT",
    "ALTER TABLE mlb_pitcher_stats_history ADD COLUMN IF NOT EXISTS hand TEXT",
    "ALTER TABLE mlb_pitcher_stats_history ADD COLUMN IF NOT EXISTS games INTEGER",
    "ALTER TABLE mlb_pitcher_stats_history ADD COLUMN IF NOT EXISTS games_started INTEGER",
    "ALTER TABLE mlb_pitcher_stats_history ADD COLUMN IF NOT EXISTS innings_pitched DOUBLE PRECISION",
    "ALTER TABLE mlb_pitcher_stats_history ADD COLUMN IF NOT EXISTS ip_per_start DOUBLE PRECISION",
    "ALTER TABLE mlb_pitcher_stats_history ADD COLUMN IF NOT EXISTS bb_per_9 DOUBLE PRECISION",
    "ALTER TABLE mlb_pitcher_stats_history ADD COLUMN IF NOT EXISTS fip DOUBLE PRECISION",
    "ALTER TABLE mlb_team_stats_history DROP CONSTRAINT IF EXISTS mlb_team_stats_history_team_id_season_snapshot_date_key",
    "ALTER TABLE mlb_pitcher_stats_history DROP CONSTRAINT IF EXISTS mlb_pitcher_stats_history_player_id_season_snapshot_date_key",
    """CREATE OR REPLACE FUNCTION reject_mlb_stats_history_mutation()
    RETURNS trigger AS $$
    BEGIN
        RAISE EXCEPTION 'MLB point-in-time stat history is append-only';
    END;
    $$ LANGUAGE plpgsql""",
    "DROP TRIGGER IF EXISTS mlb_team_stats_history_immutable ON mlb_team_stats_history",
    """CREATE TRIGGER mlb_team_stats_history_immutable
    BEFORE UPDATE OR DELETE ON mlb_team_stats_history
    FOR EACH ROW EXECUTE FUNCTION reject_mlb_stats_history_mutation()""",
    "DROP TRIGGER IF EXISTS mlb_pitcher_stats_history_immutable ON mlb_pitcher_stats_history",
    """CREATE TRIGGER mlb_pitcher_stats_history_immutable
    BEFORE UPDATE OR DELETE ON mlb_pitcher_stats_history
    FOR EACH ROW EXECUTE FUNCTION reject_mlb_stats_history_mutation()""",
    """CREATE OR REPLACE FUNCTION reject_mlb_schedule_revision_mutation()
    RETURNS trigger AS $$
    BEGIN
        RAISE EXCEPTION 'MLB schedule revisions are append-only';
    END;
    $$ LANGUAGE plpgsql""",
    "DROP TRIGGER IF EXISTS mlb_schedule_revisions_immutable ON mlb_schedule_revisions",
    """CREATE TRIGGER mlb_schedule_revisions_immutable
    BEFORE UPDATE OR DELETE ON mlb_schedule_revisions
    FOR EACH ROW EXECUTE FUNCTION reject_mlb_schedule_revision_mutation()""",
    "DROP TRIGGER IF EXISTS mlb_starter_workload_immutable ON mlb_starter_workload_snapshots",
    """CREATE TRIGGER mlb_starter_workload_immutable
    BEFORE UPDATE OR DELETE ON mlb_starter_workload_snapshots
    FOR EACH ROW EXECUTE FUNCTION reject_mlb_stats_history_mutation()""",
    "DROP TRIGGER IF EXISTS mlb_team_offense_splits_immutable ON mlb_team_offense_split_snapshots",
    """CREATE TRIGGER mlb_team_offense_splits_immutable
    BEFORE UPDATE OR DELETE ON mlb_team_offense_split_snapshots
    FOR EACH ROW EXECUTE FUNCTION reject_mlb_stats_history_mutation()""",
    "DROP TRIGGER IF EXISTS mlb_relief_appearances_immutable ON mlb_relief_appearances",
    """CREATE TRIGGER mlb_relief_appearances_immutable
    BEFORE UPDATE OR DELETE ON mlb_relief_appearances
    FOR EACH ROW EXECUTE FUNCTION reject_mlb_stats_history_mutation()""",
    "DROP TRIGGER IF EXISTS mlb_bullpen_snapshots_immutable ON mlb_bullpen_snapshots",
    """CREATE TRIGGER mlb_bullpen_snapshots_immutable
    BEFORE UPDATE OR DELETE ON mlb_bullpen_snapshots
    FOR EACH ROW EXECUTE FUNCTION reject_mlb_stats_history_mutation()""",
    "DROP TRIGGER IF EXISTS mlb_weather_forecasts_immutable ON mlb_weather_forecast_snapshots",
    """CREATE TRIGGER mlb_weather_forecasts_immutable
    BEFORE UPDATE OR DELETE ON mlb_weather_forecast_snapshots
    FOR EACH ROW EXECUTE FUNCTION reject_mlb_stats_history_mutation()""",
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
    "CREATE INDEX IF NOT EXISTS idx_mlb_homerun_runs_slate ON mlb_homerun_runs(slate_id, created_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_mlb_homerun_runs_model ON mlb_homerun_runs(analysis_version, created_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_mlb_homerun_snapshots_run ON mlb_homerun_player_snapshots(run_id, dk_player_id)",
    "CREATE INDEX IF NOT EXISTS idx_mlb_homerun_snapshots_slate ON mlb_homerun_player_snapshots(slate_id, hr_prob_1plus DESC NULLS LAST)",
    # MLB indexes
    "CREATE INDEX IF NOT EXISTS idx_mlb_matchups_date ON mlb_matchups(game_date)",
    "CREATE INDEX IF NOT EXISTS idx_mlb_schedule_revisions_game ON mlb_schedule_revisions(game_id, captured_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_mlb_schedule_revisions_matchup ON mlb_schedule_revisions(matchup_id, captured_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_mlb_starter_workload_matchup ON mlb_starter_workload_snapshots(matchup_id, side, available_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_mlb_team_offense_splits_available ON mlb_team_offense_split_snapshots(team_id, season, available_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_mlb_relief_appearances_team_date ON mlb_relief_appearances(team_id, game_date DESC)",
    "CREATE INDEX IF NOT EXISTS idx_mlb_relief_appearances_pitcher_date ON mlb_relief_appearances(pitcher_id, game_date DESC)",
    "CREATE INDEX IF NOT EXISTS idx_mlb_bullpen_snapshots_matchup ON mlb_bullpen_snapshots(matchup_id, team_id, available_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_mlb_weather_forecasts_matchup ON mlb_weather_forecast_snapshots(matchup_id, available_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_mlb_batter_stats_team ON mlb_batter_stats(team_id, season)",
    "CREATE INDEX IF NOT EXISTS idx_mlb_batter_stats_player ON mlb_batter_stats(player_id, season)",
    "CREATE INDEX IF NOT EXISTS idx_mlb_pitcher_stats_team ON mlb_pitcher_stats(team_id, season)",
    "CREATE INDEX IF NOT EXISTS idx_mlb_pitcher_stats_player ON mlb_pitcher_stats(player_id, season)",
    "CREATE INDEX IF NOT EXISTS idx_mlb_team_stats_season ON mlb_team_stats(team_id, season)",
    "CREATE INDEX IF NOT EXISTS idx_mlb_team_stats_history_asof ON mlb_team_stats_history(team_id, season, snapshot_date)",
    "CREATE INDEX IF NOT EXISTS idx_mlb_pitcher_stats_history_asof ON mlb_pitcher_stats_history(player_id, season, snapshot_date)",
    "CREATE INDEX IF NOT EXISTS idx_mlb_team_stats_history_available ON mlb_team_stats_history(team_id, season, available_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_mlb_pitcher_stats_history_available ON mlb_pitcher_stats_history(player_id, season, available_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_mlb_beat_articles_team_date ON mlb_beat_articles(team_id, published_at)",
    "CREATE INDEX IF NOT EXISTS idx_mlb_beat_facts_article ON mlb_beat_facts(article_id)",
    "CREATE INDEX IF NOT EXISTS idx_mlb_beat_facts_type ON mlb_beat_facts(fact_type, team_id)",
    "CREATE INDEX IF NOT EXISTS idx_youtube_pick_channels_active ON youtube_pick_channels(active)",
    "CREATE INDEX IF NOT EXISTS idx_youtube_pick_videos_channel ON youtube_pick_videos(channel_id, published_at)",
    "CREATE INDEX IF NOT EXISTS idx_youtube_picks_video ON youtube_picks(video_id)",
    "CREATE INDEX IF NOT EXISTS idx_youtube_picks_sport_status ON youtube_picks(sport, status)",
    # Dedup guard: one row per (video, sport, bet_type, subject, selection) so
    # overlapping extraction passes can't double-insert the same pick.
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_youtube_picks_dedup ON youtube_picks(video_id, sport, bet_type, subject, selection)",
    "CREATE INDEX IF NOT EXISTS idx_mlb_hr_training_season_date ON mlb_homerun_training_games(season, game_date)",
    "CREATE INDEX IF NOT EXISTS idx_mlb_hr_training_hitter ON mlb_homerun_training_games(hitter_mlb_id, game_date)",
    "CREATE INDEX IF NOT EXISTS idx_mlb_hr_training_target ON mlb_homerun_training_games(season, hit_hr_1plus)",
    "CREATE INDEX IF NOT EXISTS idx_dk_players_mlb_team ON dk_players(mlb_team_id, slate_id)",
    "CREATE INDEX IF NOT EXISTS idx_dk_slates_sport_date ON dk_slates(sport, slate_date DESC)",
    "CREATE INDEX IF NOT EXISTS idx_optimizer_jobs_lookup ON optimizer_jobs(client_token, sport, slate_id, status)",
    "CREATE INDEX IF NOT EXISTS idx_optimizer_jobs_created ON optimizer_jobs(created_at)",
    "CREATE INDEX IF NOT EXISTS idx_optimizer_job_lineups_job ON optimizer_job_lineups(job_id, lineup_num)",
    # Soccer indexes
    "CREATE INDEX IF NOT EXISTS idx_soccer_player_stats_norm ON soccer_player_stats(normalized_name, season)",
    "CREATE INDEX IF NOT EXISTS idx_soccer_player_stats_team ON soccer_player_stats(team_id, season)",
    "CREATE INDEX IF NOT EXISTS idx_soccer_match_scorers_game ON soccer_match_scorers(game_id)",
    "CREATE INDEX IF NOT EXISTS idx_soccer_match_scorers_name ON soccer_match_scorers(scorer_name)",
    "CREATE INDEX IF NOT EXISTS idx_soccer_matchups_date ON soccer_matchups(game_date)",
    "CREATE INDEX IF NOT EXISTS idx_soccer_bets_type_status ON soccer_bets(bet_type, status, stars)",
    "CREATE INDEX IF NOT EXISTS idx_soccer_bets_scope ON soccer_bets(scope)",
    "CREATE INDEX IF NOT EXISTS idx_soccer_bets_settle ON soccer_bets(status, stars)",
    "CREATE INDEX IF NOT EXISTS idx_soccer_bet_snapshots_bet ON soccer_bet_snapshots(bet_id, captured_at DESC)",
    # Tennis (Wimbledon MVP)
    "CREATE INDEX IF NOT EXISTS idx_tennis_matches_date ON tennis_matches(match_date, tour)",
    "CREATE INDEX IF NOT EXISTS idx_tennis_bets_type_status ON tennis_bets(bet_type, status, stars)",
    # Prediction columns added after the odds-only MVP table already existed.
    "ALTER TABLE tennis_matches ADD COLUMN IF NOT EXISTS our_prob_home DOUBLE PRECISION",
    "ALTER TABLE tennis_matches ADD COLUMN IF NOT EXISTS our_prob_away DOUBLE PRECISION",
    "ALTER TABLE tennis_matches ADD COLUMN IF NOT EXISTS our_total_pred DOUBLE PRECISION",
    # 2026-06-28: Draw No Bet market for knockout rounds — 2-way (void on draw).
    # dk_dnb_*_ml  = DraftKings' posted price (used for EV — the book the user bets at).
    # dnb_*_prob   = Pinnacle vig-free reference (or consensus 2-way if Pinnacle missing).
    "ALTER TABLE soccer_matchups ADD COLUMN IF NOT EXISTS dk_dnb_home_ml INTEGER",
    "ALTER TABLE soccer_matchups ADD COLUMN IF NOT EXISTS dk_dnb_away_ml INTEGER",
    "ALTER TABLE soccer_matchups ADD COLUMN IF NOT EXISTS dnb_home_prob DOUBLE PRECISION",
    "ALTER TABLE soccer_matchups ADD COLUMN IF NOT EXISTS dnb_away_prob DOUBLE PRECISION",
    # 2026-06-28: Knockout bracket position (1..16 top→bottom) for the R32 ties, so
    # the bracket tree + exact deep-run sim use the REAL pairings (consecutive slots
    # meet each round) instead of strength-seeded random re-pairing. Populated by
    # ingest/soccer_bracket.py from the published bracket. NULL outside knockouts.
    "ALTER TABLE soccer_matchups ADD COLUMN IF NOT EXISTS bracket_slot INTEGER",
    # 2026-07-04: bracket_round distinguishes WHICH knockout round bracket_slot
    # numbers within (r32/r16/qf/sf/final) — bracket_slot alone was only ever
    # populated for R32, so once R16 fixtures became known (real matchups with
    # real odds already loaded in soccer_matchups) the bracket tree had no way
    # to find them and fell back to a Monte-Carlo "who's still alive" proxy
    # for the whole rest of the tree, showing 100% for every already-through
    # team instead of the real upcoming match. NULL bracket_round on an
    # existing bracket_slot row means 'r32' (back-compat for rows written
    # before this column existed).
    "ALTER TABLE soccer_matchups ADD COLUMN IF NOT EXISTS bracket_round TEXT",
    # 2026-06-29: Explicit winner for knockout ties. home_score/away_score capture
    # 90+ET goals but cannot encode penalty shootout results (no goals scored).
    # winner_team_id is written by soccer_results.resolve_knockout_winners:
    # decisive ties from the score; penalty ties from TheSportsDB's
    # intHomeScoreExtra / intAwayScoreExtra (+ strResult) via a single-event
    # lookup. NULL for group stage and unplayed knockout ties.
    "ALTER TABLE soccer_matchups ADD COLUMN IF NOT EXISTS winner_team_id INTEGER REFERENCES soccer_teams(team_id)",
    # 2026-07-01: 90-minute (regulation) score. home_score/away_score is the
    # ET-inclusive final (what the Odds API publishes; drives knockout
    # advancement + ratings), but moneyline/totals/DNB markets settle on the
    # 90-minute result — Belgium 3-2 aet Senegal grades ML as Draw (2-2 at 90').
    # Filled by soccer_results.derive_regulation_scores (= final for group
    # games; rebuilt from the soccer_match_goals timeline for knockout ties,
    # where TheSportsDB caps stoppage goals at the period boundary so
    # minute <= 90 is regulation). Manual override: --reg-score.
    "ALTER TABLE soccer_matchups ADD COLUMN IF NOT EXISTS reg_home_score INTEGER",
    "ALTER TABLE soccer_matchups ADD COLUMN IF NOT EXISTS reg_away_score INTEGER",
    # 2026-07-02: MLB game status from the Stats API (detailedState). Stamped by
    # ingest.mlb_schedule.fetch_scores for Postponed/Cancelled games so
    # mlb_game_bets.settle() can VOID their pending bets — a postponed game is
    # made up on a later date under the SAME gamePk, so without this the makeup
    # score would land on the original date's row and grade tickets every book
    # voided (10 weather PPDs sat pending May-June 2026).
    "ALTER TABLE mlb_matchups ADD COLUMN IF NOT EXISTS game_status TEXT",
    # 2026-07-02: per-book odds detail for sharp-movement detection (Edge-Finding
    # Roadmap P1). The consensus columns average away the exact structure sharp
    # detection needs: which book moved first (Pinnacle leads, retail follows),
    # and line-vs-price moves. books = {book_key: {ml_home, ml_away, total_line,
    # over, under, spread_home, last_update}} straight from the Odds API payload
    # we already fetch. vegas_total_raw is the UNROUNDED consensus total (the
    # 0.5-rounded vegas_total hides half-point moves across key numbers).
    "ALTER TABLE game_odds_history ADD COLUMN IF NOT EXISTS books JSONB",
    "ALTER TABLE game_odds_history ADD COLUMN IF NOT EXISTS vegas_total_raw DOUBLE PRECISION",
    # 2026-07-02: soccer joins the odds-history trail (3-way market → draw leg;
    # per-book draw prices live in books JSONB as ml_draw).
    "ALTER TABLE game_odds_history ADD COLUMN IF NOT EXISTS draw_ml INTEGER",
    # 2026-07-02: tennis bet snapshots — append-only audit trail mirroring
    # soccer_bet_snapshots/mlb_bet_snapshots, so tennis bets get entry→close
    # CLV measurement (model/clv_report.py). Prerequisite for the tennis
    # post-Wimbledon upgrade (see Edge-Finding Roadmap).
    """CREATE TABLE IF NOT EXISTS tennis_bet_snapshots (
        id SERIAL PRIMARY KEY,
        bet_id INTEGER REFERENCES tennis_bets(id) ON DELETE CASCADE,
        captured_at TIMESTAMPTZ DEFAULT NOW(),
        capture_key TEXT,
        our_prob DOUBLE PRECISION,
        market_prob DOUBLE PRECISION,
        market_odds INTEGER,
        edge DOUBLE PRECISION,
        ev DOUBLE PRECISION,
        stars SMALLINT
    )""",
    "CREATE INDEX IF NOT EXISTS idx_tennis_bet_snapshots_bet ON tennis_bet_snapshots(bet_id, captured_at DESC)",
    # 2026-07-02: sharp line-movement ALERTS — an auditable ledger, not a toast.
    # Each row freezes the trigger-time market state (first breach only —
    # ON CONFLICT DO NOTHING; escalations never rewrite history), then gets
    # graded twice by model/line_alerts.py settle():
    #   clv_pp  = close_prob − alert_prob (did the market close toward the
    #             flagged side? converges in days — the primary audit metric)
    #   outcome = did the flagged side win (soccer grades on the 90-minute
    #             regulation score, per betting convention)
    # If an alert type can't beat the close, its thresholds are noise and the
    # backtest will say so.
    """CREATE TABLE IF NOT EXISTS line_alerts (
        id SERIAL PRIMARY KEY,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        sport TEXT NOT NULL,
        matchup_id INTEGER NOT NULL,
        game_date DATE,
        matchup TEXT,
        commence_time TIMESTAMPTZ,
        alert_type TEXT NOT NULL,           -- 'pinnacle_divergence' | 'steam'
        side TEXT NOT NULL,                 -- 'home' | 'away' | 'draw'
        capture_key TEXT,
        alert_prob DOUBLE PRECISION,        -- retail vig-free P(side) at trigger
        sharp_prob DOUBLE PRECISION,        -- Pinnacle vig-free P(side) at trigger
        details_json JSONB,
        close_prob DOUBLE PRECISION,        -- vig-free P(side) at the close
        clv_pp DOUBLE PRECISION,            -- (close_prob − alert_prob) × 100
        outcome TEXT,                       -- 'won' | 'lost' | 'void'
        settled_at TIMESTAMPTZ,
        UNIQUE (sport, matchup_id, alert_type, side)
    )""",
    "CREATE INDEX IF NOT EXISTS idx_line_alerts_open ON line_alerts(sport, settled_at) WHERE settled_at IS NULL",
    # 2026-07-02: player-prop odds capture (MLB v1: pitcher_strikeouts +
    # batter_total_bases). Expanded 2026-07-08 (D4) to 5 markets — added
    # pitcher_hits_allowed, pitcher_earned_runs, pitcher_outs, each verified
    # DraftKings+Pinnacle across a 5-event probe. One row per (event, market,
    # player, capture): books = {book: {line, over, under, last_update}}.
    # Feeds the dk_prop_value / prop_line_gap detectors in
    # model/line_alerts.py; 3 captures/day via refresh_mlb_vegas (props are
    # per-event calls — bookmakers=draftkings,pinnacle costs markets×1 credit,
    # ~5/event now — so the 30-min game-line cadence would blow the quota).
    """CREATE TABLE IF NOT EXISTS prop_odds_history (
        id SERIAL PRIMARY KEY,
        sport TEXT NOT NULL,
        event_id TEXT NOT NULL,
        matchup_id INTEGER,
        game_date DATE,
        commence_time TIMESTAMPTZ,
        home_team_name TEXT,
        away_team_name TEXT,
        market TEXT NOT NULL,
        player TEXT NOT NULL,
        books JSONB NOT NULL,
        capture_key TEXT NOT NULL,
        captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE (sport, event_id, market, player, capture_key)
    )""",
    "CREATE INDEX IF NOT EXISTS idx_prop_odds_latest ON prop_odds_history(sport, event_id, market, player, captured_at DESC)",
    # 2026-07-02: execution-book CLV. clv_pp measures the REFERENCE market
    # (did the sharp consensus move toward the flagged side — informational
    # content); dk_clv_pct measures the EXECUTION book (did DK's own price on
    # the flagged side worsen after the alert — was the discrepancy a
    # temporarily executable window). entry from details_json.dk_decimal
    # frozen at trigger; close from the last pre-commence capture. Positive =
    # the alerted price was better than DK's close = the window was real.
    "ALTER TABLE line_alerts ADD COLUMN IF NOT EXISTS dk_close_decimal DOUBLE PRECISION",
    "ALTER TABLE line_alerts ADD COLUMN IF NOT EXISTS dk_clv_pct DOUBLE PRECISION",
    # 2026-07-03: full price-context grading. pin_close_prob stores the sharp
    # book's closing fair probability as its own quantity (clv_pp's close is
    # the RETAIL consensus); convergence classifies HOW an alert-time gap
    # closed (EXECUTION_CONVERGED_TO_REFERENCE / REFERENCE_CONVERGED_TO_
    # EXECUTION / BOTH_MOVED_TOWARD_BET / BOTH_MOVED_AGAINST_BET /
    # DIVERGENCE_PERSISTED) — Pinnacle moving toward DK's number is evidence
    # the "stale" quote was information, and cannot be inferred from two CLV
    # scalars alone; dk_survival_min = minutes until DK's alerted price first
    # changed (NULL = survived to the close), the decay/availability measure.
    "ALTER TABLE line_alerts ADD COLUMN IF NOT EXISTS pin_close_prob DOUBLE PRECISION",
    "ALTER TABLE line_alerts ADD COLUMN IF NOT EXISTS convergence TEXT",
    "ALTER TABLE line_alerts ADD COLUMN IF NOT EXISTS dk_survival_min DOUBLE PRECISION",
    # 2026-07-03: movement MAGNITUDE alongside the categorical label so
    # near-boundary cases aren't hidden and epsilon sensitivity is testable:
    # {gap_initial_pp, gap_final_pp, gap_max_closure_pp, gap_closure_ratio,
    #  d_dk_pp, d_pin_pp, epsilon_pp, n_captures,
    #  survival_lower_min, survival_upper_min, last_same_at, first_changed_at}.
    # Survival is INTERVAL-CENSORED by capture cadence (the true change lies
    # between last-seen-unchanged and first-seen-changed); dk_survival_min is
    # the UPPER bound kept for back-compat. The metric is "observed quote
    # persistence" — NOT verified execution availability, which this system
    # cannot measure. Convergence is a PATH classification, not a quality
    # verdict: REFERENCE_CONVERGED_TO_EXECUTION means the sharp move was more
    # consistent with DK's original price than the reference's — evidence DK
    # may have led price discovery, not proof.
    "ALTER TABLE line_alerts ADD COLUMN IF NOT EXISTS grading_json JSONB",
    # 2026-07-03: proposition comparability made EXPLICIT + queryable, not
    # implicit in alert_type — so a future same-sounding alert type can't be
    # silently routed into convergence with a reference prob about a DIFFERENT
    # proposition (the Herrera lesson). comparison_status in
    # {SAME_PROPOSITION, DIFFERENT_LINE, NO_REFERENCE, RULE_MISMATCH,
    # INSUFFICIENT_CAPTURE}; NULL convergence on a non-SAME_PROPOSITION row is
    # not-applicable-BY-DESIGN, never missing data. grading_version stamps the
    # classifier (epsilon/eligibility/path logic) so rows regrade under a later
    # method without erasing which rule produced the original label; the input
    # capture series is reproducible from the append-only *_odds_history tables.
    "ALTER TABLE line_alerts ADD COLUMN IF NOT EXISTS comparison_status TEXT",
    "ALTER TABLE line_alerts ADD COLUMN IF NOT EXISTS grading_version TEXT",
    # 2026-07-03: APPEND-ONLY grade history. line_alerts holds the CURRENT
    # grade for ordinary querying (denormalized); alert_grades preserves EVERY
    # grade event immutably. Reproducibility (from the append-only *_odds_history
    # captures) lets the old answer be RECREATED; this table PROVES what the old
    # answer actually was — distinguishing what the system concluded at the
    # time, what a later methodology concluded from the same evidence, and
    # whether a change came from new captures, corrected data, or revised logic.
    # is_current flags the latest grade per alert; never UPDATE a prior row's
    # grade fields, only flip is_current.
    """CREATE TABLE IF NOT EXISTS alert_grades (
        id SERIAL PRIMARY KEY,
        alert_id INTEGER NOT NULL REFERENCES line_alerts(id) ON DELETE CASCADE,
        grading_version TEXT NOT NULL,
        graded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        comparison_status TEXT,
        convergence TEXT,
        outcome TEXT,
        dk_clv_pct DOUBLE PRECISION,
        grading_json JSONB,
        is_current BOOLEAN NOT NULL DEFAULT TRUE
    )""",
    "CREATE INDEX IF NOT EXISTS idx_alert_grades_current ON alert_grades(alert_id) WHERE is_current",
]
