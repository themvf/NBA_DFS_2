"""NBA box scores (ESPN) and the nightly points-line calibration grade.

Three tables, all written by scheduled Python jobs and read by the Top
Scorers page (``web/src/db/nba-top-scorers.ts``):

- ``nba_box_score_games`` / ``nba_box_score_players``: one row per game and
  per player-game from ESPN's public scoreboard and summary feeds
  (``ingest/nba_box_scores.py``). ESPN ids are the identity because
  stats.nba.com's ids are unreachable from a GitHub runner; the rows carry
  the player name and both team names so a line can be matched by name
  within its game. ``fetched_at`` is restamped on every write, so the
  freshness monitor can watch it.
- ``nba_points_calibration``: one row per (game, player, grade version):
  what the books' LAST pre-tip capture said (consensus line, P(over),
  floor, ceiling, P(20+)...P(40+)) beside what the player scored
  (``model/nba_points_calibration.py``). Rows are upserted: a regrade with
  the same version rewrites the row and restamps ``graded_at``; a changed
  reader is a new ``grade_version`` and new rows, never a silent rewrite.

Appended to ``db.schema.MIGRATIONS`` so the next job's schema pass creates
them; every statement is idempotent.
"""

STATEMENTS = [
    """CREATE TABLE IF NOT EXISTS nba_box_score_games (
        espn_event_id TEXT PRIMARY KEY,
        season TEXT NOT NULL,
        game_date DATE NOT NULL,
        start_time TIMESTAMPTZ,
        status TEXT NOT NULL,
        completed BOOLEAN NOT NULL DEFAULT FALSE,
        home_team TEXT NOT NULL,
        home_abbrev TEXT,
        home_espn_id TEXT,
        home_score INTEGER,
        away_team TEXT NOT NULL,
        away_abbrev TEXT,
        away_espn_id TEXT,
        away_score INTEGER,
        players INTEGER NOT NULL DEFAULT 0,
        source TEXT NOT NULL DEFAULT 'espn',
        fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )""",
    "CREATE INDEX IF NOT EXISTS idx_nba_box_score_games_date ON nba_box_score_games(game_date DESC)",
    """CREATE TABLE IF NOT EXISTS nba_box_score_players (
        id BIGSERIAL PRIMARY KEY,
        espn_event_id TEXT NOT NULL REFERENCES nba_box_score_games(espn_event_id),
        game_date DATE NOT NULL,
        espn_athlete_id TEXT NOT NULL,
        player TEXT NOT NULL,
        player_key TEXT NOT NULL,
        team TEXT NOT NULL,
        team_abbrev TEXT,
        is_home BOOLEAN NOT NULL,
        starter BOOLEAN,
        played BOOLEAN NOT NULL,
        dnp_reason TEXT,
        minutes DOUBLE PRECISION,
        points DOUBLE PRECISION,
        fgm DOUBLE PRECISION,
        fga DOUBLE PRECISION,
        fg3m DOUBLE PRECISION,
        fg3a DOUBLE PRECISION,
        ftm DOUBLE PRECISION,
        fta DOUBLE PRECISION,
        rebounds DOUBLE PRECISION,
        assists DOUBLE PRECISION,
        steals DOUBLE PRECISION,
        blocks DOUBLE PRECISION,
        turnovers DOUBLE PRECISION,
        fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE (espn_event_id, espn_athlete_id)
    )""",
    "CREATE INDEX IF NOT EXISTS idx_nba_box_score_players_date ON nba_box_score_players(game_date DESC, player_key)",
    """CREATE TABLE IF NOT EXISTS nba_points_calibration (
        id BIGSERIAL PRIMARY KEY,
        grade_version TEXT NOT NULL,
        ladder_version TEXT NOT NULL,
        game_date DATE NOT NULL,
        event_id TEXT NOT NULL,
        espn_event_id TEXT,
        player TEXT NOT NULL,
        player_key TEXT NOT NULL,
        team TEXT,
        commence_time TIMESTAMPTZ NOT NULL,
        captured_at TIMESTAMPTZ NOT NULL,
        minutes_before_tip DOUBLE PRECISION NOT NULL,
        line DOUBLE PRECISION NOT NULL,
        p_over DOUBLE PRECISION,
        books INTEGER NOT NULL,
        floor DOUBLE PRECISION,
        ceiling DOUBLE PRECISION,
        p20 DOUBLE PRECISION,
        p25 DOUBLE PRECISION,
        p30 DOUBLE PRECISION,
        p35 DOUBLE PRECISION,
        p40 DOUBLE PRECISION,
        ladder_books INTEGER NOT NULL DEFAULT 0,
        status TEXT NOT NULL,
        actual_points DOUBLE PRECISION,
        minutes DOUBLE PRECISION,
        graded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE (grade_version, event_id, player_key)
    )""",
    "CREATE INDEX IF NOT EXISTS idx_nba_points_calibration_date ON nba_points_calibration(grade_version, game_date DESC)",
]
