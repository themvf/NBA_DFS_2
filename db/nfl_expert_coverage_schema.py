"""DDL for the NFL DFS expert-coverage collection (docs/nfl-expert-coverage-study.md).

Dependency-free so db/schema.py and the collector (ingest/nfl_expert_coverage.py)
apply the same statements. Every table is append-only: UPDATE and DELETE are
rejected by trigger. A re-fetched article whose text changed appends a new
revision row; an unchanged re-fetch appends only a fetch-log row.

Tables:
  nfl_expert_capture_runs     one row per collector run (heartbeat for /health)
  nfl_expert_robots_checks    the robots.txt we evaluated, per source per run
  nfl_expert_fetch_log        every page decision: stored, unchanged, skipped and why
  nfl_expert_articles         article revisions: raw text, published_at, captured_at
  nfl_expert_extractions      one per (article revision, extraction model version)
  nfl_expert_article_slates   which slate(s) a revision covers, with timing class
  nfl_expert_player_mentions  grounded per-player records
  nfl_expert_sample_lineups   grounded sample lineups
"""

APPEND_ONLY_TABLES = (
    "nfl_expert_capture_runs",
    "nfl_expert_robots_checks",
    "nfl_expert_fetch_log",
    "nfl_expert_articles",
    "nfl_expert_extractions",
    "nfl_expert_article_slates",
    "nfl_expert_player_mentions",
    "nfl_expert_sample_lineups",
)

STATEMENTS = [
    """CREATE TABLE IF NOT EXISTS nfl_expert_capture_runs (
        id BIGSERIAL PRIMARY KEY,
        run_uuid TEXT NOT NULL UNIQUE,
        collector_version TEXT NOT NULL,
        pass_label TEXT NOT NULL,
        started_at TIMESTAMPTZ NOT NULL,
        finished_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        ok BOOLEAN NOT NULL,
        sources JSONB NOT NULL DEFAULT '{}'::jsonb,
        articles_new INTEGER NOT NULL DEFAULT 0,
        revisions_new INTEGER NOT NULL DEFAULT 0,
        unchanged INTEGER NOT NULL DEFAULT 0,
        skipped INTEGER NOT NULL DEFAULT 0,
        errors INTEGER NOT NULL DEFAULT 0,
        detail JSONB NOT NULL DEFAULT '{}'::jsonb
    )""",
    """CREATE TABLE IF NOT EXISTS nfl_expert_robots_checks (
        id BIGSERIAL PRIMARY KEY,
        run_uuid TEXT NOT NULL,
        source TEXT NOT NULL,
        robots_url TEXT NOT NULL,
        fetched_at TIMESTAMPTZ NOT NULL,
        http_status INTEGER,
        body_sha256 TEXT,
        body TEXT,
        user_agent TEXT NOT NULL,
        decision TEXT NOT NULL,
        evaluated_paths JSONB NOT NULL DEFAULT '{}'::jsonb,
        CHECK (decision IN ('allowed','no_robots_file','disallowed','unreachable'))
    )""",
    """CREATE TABLE IF NOT EXISTS nfl_expert_articles (
        id BIGSERIAL PRIMARY KEY,
        collector_version TEXT NOT NULL,
        source TEXT NOT NULL,
        url TEXT NOT NULL,
        revision INTEGER NOT NULL,
        content_sha256 TEXT NOT NULL,
        title TEXT,
        author TEXT,
        published_at TIMESTAMPTZ,
        published_at_raw TEXT,
        published_at_method TEXT NOT NULL,
        published_precision TEXT NOT NULL,
        modified_at TIMESTAMPTZ,
        captured_at TIMESTAMPTZ NOT NULL,
        run_uuid TEXT NOT NULL,
        pass_label TEXT NOT NULL,
        http_status INTEGER NOT NULL,
        raw_text TEXT NOT NULL,
        text_length INTEGER NOT NULL,
        discovery JSONB NOT NULL DEFAULT '{}'::jsonb,
        flags JSONB NOT NULL DEFAULT '{}'::jsonb,
        CHECK (published_precision IN ('second','day','none')),
        UNIQUE (url, revision),
        UNIQUE (url, content_sha256)
    )""",
    """CREATE INDEX IF NOT EXISTS idx_nfl_expert_articles_captured
        ON nfl_expert_articles (captured_at DESC)""",
    """CREATE TABLE IF NOT EXISTS nfl_expert_fetch_log (
        id BIGSERIAL PRIMARY KEY,
        run_uuid TEXT NOT NULL,
        source TEXT NOT NULL,
        url TEXT NOT NULL,
        fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        outcome TEXT NOT NULL,
        http_status INTEGER,
        article_id BIGINT REFERENCES nfl_expert_articles(id),
        detail TEXT,
        CHECK (outcome IN ('new_article','new_revision','unchanged','robots_disallowed',
                           'paywalled','http_error','too_short','not_nfl_dfs','fetch_error'))
    )""",
    """CREATE INDEX IF NOT EXISTS idx_nfl_expert_fetch_log_run ON nfl_expert_fetch_log (run_uuid)""",
    """CREATE TABLE IF NOT EXISTS nfl_expert_extractions (
        id BIGSERIAL PRIMARY KEY,
        article_id BIGINT NOT NULL REFERENCES nfl_expert_articles(id),
        extraction_version TEXT NOT NULL,
        llm_model TEXT NOT NULL,
        extracted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        status TEXT NOT NULL,
        slate_raw JSONB NOT NULL DEFAULT '{}'::jsonb,
        mentions_raw INTEGER NOT NULL DEFAULT 0,
        mentions_grounded INTEGER NOT NULL DEFAULT 0,
        mentions_discarded INTEGER NOT NULL DEFAULT 0,
        lineups_raw INTEGER NOT NULL DEFAULT 0,
        lineups_grounded INTEGER NOT NULL DEFAULT 0,
        lineups_discarded INTEGER NOT NULL DEFAULT 0,
        discard_reasons JSONB NOT NULL DEFAULT '{}'::jsonb,
        response JSONB,
        CHECK (status IN ('ok','empty','not_nfl_dfs','malformed')),
        UNIQUE (article_id, extraction_version)
    )""",
    """CREATE TABLE IF NOT EXISTS nfl_expert_article_slates (
        id BIGSERIAL PRIMARY KEY,
        extraction_id BIGINT NOT NULL REFERENCES nfl_expert_extractions(id),
        article_id BIGINT NOT NULL REFERENCES nfl_expert_articles(id),
        season INTEGER NOT NULL,
        week INTEGER NOT NULL,
        format TEXT NOT NULL,
        slate_key TEXT NOT NULL,
        games JSONB NOT NULL,
        first_kickoff TIMESTAMPTZ NOT NULL,
        captured_at TIMESTAMPTZ NOT NULL,
        minutes_before_kickoff DOUBLE PRECISION NOT NULL,
        timing_class TEXT NOT NULL,
        resolution TEXT NOT NULL,
        CHECK (format IN ('showdown','classic')),
        CHECK (timing_class IN ('eligible','late','retrospective')),
        UNIQUE (extraction_id, slate_key)
    )""",
    """CREATE INDEX IF NOT EXISTS idx_nfl_expert_article_slates_slate
        ON nfl_expert_article_slates (slate_key, timing_class)""",
    """CREATE TABLE IF NOT EXISTS nfl_expert_player_mentions (
        id BIGSERIAL PRIMARY KEY,
        extraction_id BIGINT NOT NULL REFERENCES nfl_expert_extractions(id),
        article_id BIGINT NOT NULL REFERENCES nfl_expert_articles(id),
        source TEXT NOT NULL,
        slate_key TEXT,
        player_name TEXT NOT NULL,
        team TEXT,
        role TEXT NOT NULL,
        slot TEXT,
        published_own_pct DOUBLE PRECISION,
        own_definition TEXT,
        quote TEXT NOT NULL,
        match_status TEXT NOT NULL,
        match_method TEXT,
        dk_pid BIGINT,
        dk_name TEXT,
        dk_team TEXT,
        dk_position TEXT,
        dk_salary INTEGER,
        pool_snapshot_id UUID,
        CHECK (role IN ('captain','core','value','contrarian','fade','mention')),
        CHECK (match_status IN ('matched','unmatched','ambiguous','no_pool'))
    )""",
    """CREATE INDEX IF NOT EXISTS idx_nfl_expert_mentions_slate
        ON nfl_expert_player_mentions (slate_key, dk_pid)""",
    """CREATE TABLE IF NOT EXISTS nfl_expert_sample_lineups (
        id BIGSERIAL PRIMARY KEY,
        extraction_id BIGINT NOT NULL REFERENCES nfl_expert_extractions(id),
        article_id BIGINT NOT NULL REFERENCES nfl_expert_articles(id),
        source TEXT NOT NULL,
        slate_key TEXT,
        lineup_index INTEGER NOT NULL,
        label TEXT,
        players JSONB NOT NULL
    )""",
    """CREATE OR REPLACE FUNCTION reject_nfl_expert_coverage_mutation()
       RETURNS trigger LANGUAGE plpgsql AS $$
       BEGIN
         RAISE EXCEPTION '% is append-only; write a new revision or version', TG_TABLE_NAME;
       END $$""",
]

for _table in APPEND_ONLY_TABLES:
    STATEMENTS.append(f"""DO $$ BEGIN
       IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = '{_table}_immutable') THEN
         CREATE TRIGGER {_table}_immutable BEFORE UPDATE OR DELETE
         ON {_table} FOR EACH ROW EXECUTE FUNCTION reject_nfl_expert_coverage_mutation();
       END IF;
    END $$""")
