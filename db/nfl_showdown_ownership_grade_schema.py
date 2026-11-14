"""DDL for the Showdown ownership after-slate grade (`nfl-showdown-ownership-grade-v1`).

Dependency-free, and applied by the writer itself (ingest/nfl_showdown_ownership_grade.py)
the way the ownership scorecard applies its own table, so adding these two tables does
not change the digest of db/schema.py and trigger a full DDL pass in every other job.

- `nfl_showdown_ownership_grades`: one row per graded Showdown slate and grade input.
  Append-only: a grade is written once. If the inputs change (a later forecast capture,
  a fuller contest import) a new row is appended under a new `input_key`; readers take
  the newest per slate. UPDATE and DELETE are rejected by trigger.
- `nfl_showdown_ownership_grade_runs`: one row per run of the job, written on every
  successful run whether or not it graded anything. It is what the health checklist
  watches, and it names every slate still waiting and why.
"""

STATEMENTS = [
    """CREATE TABLE IF NOT EXISTS nfl_showdown_ownership_grades (
        id BIGSERIAL PRIMARY KEY,
        grade_version TEXT NOT NULL,
        slate_key TEXT NOT NULL,
        input_key TEXT NOT NULL,
        season INTEGER,
        week INTEGER,
        game TEXT NOT NULL,
        first_kickoff TIMESTAMPTZ NOT NULL,
        capture_source TEXT NOT NULL,
        capture_id TEXT NOT NULL,
        captured_at TIMESTAMPTZ NOT NULL,
        contest_id TEXT NOT NULL,
        contest_name TEXT,
        entry_count INTEGER,
        labels_complete BOOLEAN NOT NULL,
        models JSONB NOT NULL,
        detail JSONB NOT NULL DEFAULT '{}'::jsonb,
        graded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CHECK (capture_source IN ('optimizer_run','ghost_freeze')),
        UNIQUE (grade_version, slate_key, input_key)
    )""",
    """CREATE INDEX IF NOT EXISTS idx_nfl_showdown_ownership_grades_kickoff
        ON nfl_showdown_ownership_grades (grade_version, first_kickoff DESC)""",
    """CREATE OR REPLACE FUNCTION reject_nfl_showdown_ownership_grade_mutation()
       RETURNS trigger LANGUAGE plpgsql AS $$
       BEGIN
         RAISE EXCEPTION 'nfl_showdown_ownership_grades is append-only; a changed input appends a new row';
       END $$""",
    """DO $$ BEGIN
       IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'nfl_showdown_ownership_grades_immutable') THEN
         CREATE TRIGGER nfl_showdown_ownership_grades_immutable BEFORE UPDATE OR DELETE
         ON nfl_showdown_ownership_grades FOR EACH ROW EXECUTE FUNCTION reject_nfl_showdown_ownership_grade_mutation();
       END IF;
    END $$""",
    """CREATE TABLE IF NOT EXISTS nfl_showdown_ownership_grade_runs (
        id BIGSERIAL PRIMARY KEY,
        grade_version TEXT NOT NULL,
        season INTEGER NOT NULL,
        slates INTEGER NOT NULL,
        graded INTEGER NOT NULL,
        new_grades INTEGER NOT NULL,
        waiting JSONB NOT NULL DEFAULT '[]'::jsonb,
        ran_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )""",
]
