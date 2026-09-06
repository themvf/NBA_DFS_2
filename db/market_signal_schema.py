"""Targeted, additive movement-observation schema shared by both migration paths."""

SCHEMA = """
CREATE TABLE IF NOT EXISTS market_signal_observations (
    id BIGSERIAL PRIMARY KEY,
    sport TEXT NOT NULL,
    matchup_id INTEGER NOT NULL,
    market TEXT NOT NULL,
    alert_type TEXT NOT NULL,
    side TEXT NOT NULL,
    detector_version TEXT NOT NULL,
    history_id INTEGER NOT NULL REFERENCES game_odds_history(id),
    trigger_history_id INTEGER NOT NULL REFERENCES game_odds_history(id),
    baseline_history_id INTEGER REFERENCES game_odds_history(id),
    observed_at TIMESTAMPTZ NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    state TEXT NOT NULL,
    details_json JSONB NOT NULL,
    UNIQUE(sport, matchup_id, market, alert_type, side, detector_version, history_id)
)
"""
INDEX = """CREATE INDEX IF NOT EXISTS market_signal_observations_lookup
ON market_signal_observations(sport, matchup_id, observed_at DESC)"""
