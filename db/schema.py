"""PostgreSQL schema for NBA DFS v2.

Tables:
  - teams              30 NBA teams with standard 3-letter abbreviations
  - nba_team_stats     Pace, OffRtg, DefRtg per team per season
  - nba_player_stats   Rolling 10-game averages per player
  - nba_matchups       Daily game schedule with Vegas odds
  - dk_slates          DraftKings slate per date
  - dk_players         Player pool per slate (merged DK + LineStar)
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
        fetched_at TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE(player_id, season)
    )
    """,

    # ── Daily game schedule + Vegas odds ─────────────────────
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
        vegas_prob_home DOUBLE PRECISION,
        fetched_at TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE(game_date, home_team_id, away_team_id)
    )
    """,

    # ── DFS slates ────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS dk_slates (
        id SERIAL PRIMARY KEY,
        slate_date DATE NOT NULL,
        game_count INTEGER DEFAULT 0,
        dk_draft_group_id INTEGER,
        linestar_period_id INTEGER,
        cash_line DOUBLE PRECISION,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE(slate_date)
    )
    """,

    # ── DFS player pool ───────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS dk_players (
        id SERIAL PRIMARY KEY,
        slate_id INTEGER NOT NULL REFERENCES dk_slates(id) ON DELETE CASCADE,
        dk_player_id BIGINT NOT NULL,
        name TEXT NOT NULL,
        team_abbrev TEXT NOT NULL,
        team_id INTEGER REFERENCES teams(team_id),
        matchup_id INTEGER,
        eligible_positions TEXT NOT NULL,
        salary INTEGER NOT NULL,
        game_info TEXT,
        avg_fpts_dk REAL,
        linestar_proj REAL,
        proj_own_pct REAL,
        our_proj REAL,
        our_leverage REAL,
        our_own_pct REAL,
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
]

MIGRATIONS = [
    # 2026-03-28: Relax game_id NOT NULL → nullable so TS web can insert without it
    "ALTER TABLE nba_matchups ALTER COLUMN game_id DROP NOT NULL",
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
]

INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_nba_team_stats_season ON nba_team_stats(team_id, season)",
    "CREATE INDEX IF NOT EXISTS idx_nba_player_stats_team ON nba_player_stats(team_id, season)",
    "CREATE INDEX IF NOT EXISTS idx_nba_player_stats_player ON nba_player_stats(player_id, season)",
    "CREATE INDEX IF NOT EXISTS idx_nba_matchups_date ON nba_matchups(game_date)",
    "CREATE INDEX IF NOT EXISTS idx_dk_players_slate ON dk_players(slate_id, our_leverage DESC NULLS LAST)",
    "CREATE INDEX IF NOT EXISTS idx_dk_players_team ON dk_players(team_id, slate_id)",
    "CREATE INDEX IF NOT EXISTS idx_dk_lineups_slate ON dk_lineups(slate_id, strategy)",
]
