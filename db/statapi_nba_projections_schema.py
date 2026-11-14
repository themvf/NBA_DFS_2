"""stat-api's own NBA player projections, as frozen at tip-off.

``statapi_nba_player_projections``: one row per (stat-api game, player,
projection engine). stat-api keeps "one current row per game, player and
engine version; it updates until tip and never changes after", so a row read
after the game is final is the version they had at tip. Written by
``ingest/statapi_nba_projections.py`` (manual, research only) and read by
``research/nba_statapi_projection_compare.py``.

Insert-only (``ON CONFLICT DO NOTHING``): a row that is already stored is
never rewritten. ``raw`` keeps the full response row so later readers do not
need to refetch.

Appended to ``db.schema.MIGRATIONS``; every statement is idempotent.
"""

STATEMENTS = [
    """CREATE TABLE IF NOT EXISTS statapi_nba_player_projections (
        statapi_game_id BIGINT NOT NULL,
        statapi_player_id BIGINT NOT NULL,
        projection_id INTEGER NOT NULL,
        game_date DATE NOT NULL,
        game_time TIMESTAMPTZ,
        game_status TEXT,
        home_team TEXT,
        away_team TEXT,
        team TEXT,
        player TEXT,
        player_key TEXT,
        status TEXT,
        status_source TEXT,
        zero_reason TEXT,
        play_probability DOUBLE PRECISION,
        start_probability DOUBLE PRECISION,
        expected_starter BOOLEAN,
        lineup_confirmed BOOLEAN,
        minutes_mean DOUBLE PRECISION,
        pts DOUBLE PRECISION,
        pts_p10 DOUBLE PRECISION,
        pts_p50 DOUBLE PRECISION,
        pts_p90 DOUBLE PRECISION,
        inputs_as_of TIMESTAMPTZ,
        raw JSONB NOT NULL,
        fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (statapi_game_id, statapi_player_id, projection_id)
    )""",
    """CREATE INDEX IF NOT EXISTS idx_statapi_nba_proj_date_key
        ON statapi_nba_player_projections(game_date, player_key)""",
]
