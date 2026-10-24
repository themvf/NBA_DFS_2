"""DDL for the `nfl-ownership-news-v1` tables (docs/nfl-ownership-news-chalk-study.md).

Dependency-free so db/schema.py and research/nfl_ownership_news.py apply the
same statements. All three tables are append-only: UPDATE and DELETE are
rejected by trigger, and each unique key lets a row be written once.
"""

_TABLES = ("nfl_ownership_news_freezes", "nfl_ownership_news_grades", "nfl_ownership_news_verdicts")

STATEMENTS = [
    """CREATE TABLE IF NOT EXISTS nfl_ownership_news_freezes (
        id BIGSERIAL PRIMARY KEY,
        study TEXT NOT NULL,
        format TEXT NOT NULL,
        decision TEXT NOT NULL,
        candidate TEXT,
        m DOUBLE PRECISION,
        commit_sha TEXT NOT NULL,
        design JSONB NOT NULL DEFAULT '{}'::jsonb,
        frozen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CHECK (format IN ('classic','showdown')),
        CHECK (decision IN ('CHALLENGER','NOT_BUILT')),
        UNIQUE (study, format)
    )""",
    """CREATE TABLE IF NOT EXISTS nfl_ownership_news_grades (
        id BIGSERIAL PRIMARY KEY,
        study TEXT NOT NULL,
        format TEXT NOT NULL,
        slate_key TEXT NOT NULL,
        contest_id TEXT NOT NULL,
        week INTEGER,
        first_kickoff TIMESTAMPTZ,
        imported_at TIMESTAMPTZ,
        candidate TEXT NOT NULL,
        flagged_n INTEGER NOT NULL,
        unlabeled_n INTEGER NOT NULL,
        diff_excluded DOUBLE PRECISION,
        diff_worst DOUBLE PRECISION,
        diff_observed DOUBLE PRECISION,
        details JSONB NOT NULL DEFAULT '{}'::jsonb,
        graded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE (study, format, slate_key)
    )""",
    """CREATE TABLE IF NOT EXISTS nfl_ownership_news_verdicts (
        id BIGSERIAL PRIMARY KEY,
        study TEXT NOT NULL,
        format TEXT NOT NULL,
        verdict TEXT NOT NULL,
        n_slates INTEGER NOT NULL,
        n_rows INTEGER NOT NULL,
        details JSONB NOT NULL DEFAULT '{}'::jsonb,
        decided_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CHECK (verdict IN ('PASS','FAIL','INCONCLUSIVE')),
        UNIQUE (study, format)
    )""",
    """CREATE OR REPLACE FUNCTION reject_nfl_ownership_news_mutation()
       RETURNS trigger LANGUAGE plpgsql AS $$
       BEGIN
         RAISE EXCEPTION '% is append-only', TG_TABLE_NAME;
       END $$""",
] + [
    f"""DO $$ BEGIN
       IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = '{t}_immutable') THEN
         CREATE TRIGGER {t}_immutable BEFORE UPDATE OR DELETE ON {t}
           FOR EACH ROW EXECUTE FUNCTION reject_nfl_ownership_news_mutation();
       END IF;
       END $$"""
    for t in _TABLES
]
