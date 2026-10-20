"""DDL for `nfl_ownership_scorecard_rows` (docs/nfl-ownership-scorecard-v1.md section 7).

Dependency-free so db/schema.py and the writer (research/nfl_ownership_scorecard.py)
apply the same statements. Append-only: one row per (scorecard version, grader
version, row kind, contest, horizon, source capture, forecast version,
population, slot). A re-grade under a new scorecard or grader version appends;
UPDATE/DELETE are rejected by trigger.
"""

STATEMENTS = [
    """CREATE TABLE IF NOT EXISTS nfl_ownership_scorecard_rows (
        id BIGSERIAL PRIMARY KEY,
        scorecard_version TEXT NOT NULL,
        grader_version TEXT NOT NULL,
        row_kind TEXT NOT NULL,
        season INTEGER,
        week INTEGER,
        format TEXT NOT NULL,
        slate_key TEXT NOT NULL,
        contest_id TEXT NOT NULL,
        contest_role TEXT NOT NULL,
        entry_count INTEGER,
        horizon TEXT NOT NULL,
        source TEXT NOT NULL,
        capture_id TEXT NOT NULL,
        captured_at TIMESTAMPTZ,
        minutes_before_kickoff DOUBLE PRECISION,
        forecast_version TEXT NOT NULL,
        stored_version TEXT,
        design_tag TEXT NOT NULL,
        population TEXT NOT NULL,
        slot TEXT NOT NULL,
        n INTEGER,
        mae DOUBLE PRECISION,
        bias DOUBLE PRECISION,
        rmse DOUBLE PRECISION,
        spearman DOUBLE PRECISION,
        pearson DOUBLE PRECISION,
        labels_complete BOOLEAN,
        label_mass DOUBLE PRECISION,
        class_counts JSONB NOT NULL DEFAULT '{}'::jsonb,
        metrics JSONB NOT NULL DEFAULT '{}'::jsonb,
        details JSONB NOT NULL DEFAULT '{}'::jsonb,
        graded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CHECK (row_kind IN ('contest','pooled','no_frozen_forecast')),
        CHECK (horizon IN ('canonical','early')),
        CHECK (slot IN ('overall','CPT','FLEX','none')),
        UNIQUE (scorecard_version, grader_version, row_kind, contest_id, horizon, capture_id,
                forecast_version, population, slot)
    )""",
    """CREATE INDEX IF NOT EXISTS idx_nfl_ownership_scorecard_rows_slate
        ON nfl_ownership_scorecard_rows (scorecard_version, format, week, population)""",
    """CREATE OR REPLACE FUNCTION reject_nfl_ownership_scorecard_mutation()
       RETURNS trigger LANGUAGE plpgsql AS $$
       BEGIN
         RAISE EXCEPTION 'nfl_ownership_scorecard_rows is append-only; re-grade under a new version';
       END $$""",
    """DO $$ BEGIN
       IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'nfl_ownership_scorecard_rows_immutable') THEN
         CREATE TRIGGER nfl_ownership_scorecard_rows_immutable BEFORE UPDATE OR DELETE
         ON nfl_ownership_scorecard_rows FOR EACH ROW EXECUTE FUNCTION reject_nfl_ownership_scorecard_mutation();
       END IF;
    END $$""",
]
