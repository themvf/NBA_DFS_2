CREATE TABLE IF NOT EXISTS nfl_td_comparison_forecasts (
  game_id TEXT PRIMARY KEY,
  frozen_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  digest TEXT NOT NULL,
  payload JSONB NOT NULL
);
CREATE TABLE IF NOT EXISTS nfl_td_comparison_results (
  id BIGSERIAL PRIMARY KEY,
  game_id TEXT NOT NULL REFERENCES nfl_td_comparison_forecasts(game_id),
  imported_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  digest TEXT NOT NULL,
  payload JSONB NOT NULL,
  UNIQUE (game_id, digest)
);
-- No UPDATE/DELETE publisher operation: forecasts freeze once; results are revisions.
