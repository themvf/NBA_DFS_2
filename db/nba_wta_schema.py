"""Winner-take-all top-3 scorer contest entries and their grades (research/nba_wta_trios.py).

- ``nba_wta_portfolios``: one row per freeze, append-only. A freeze runs after each
  daytime line capture while every game on the date is still to tip; the CHECK
  refuses a row created at or after the slate's first tip. An unchanged capture
  produces an identical payload and is skipped by the digest key. The grade reads
  the latest row for the date.
- ``nba_wta_grades``: one row per (portfolio, grade version), upserted so a
  corrected box score can be re-graded; ``graded_at`` is restamped on each write.

Appended to ``db.schema.MIGRATIONS``; every statement is idempotent.
"""

STATEMENTS = [
    """CREATE TABLE IF NOT EXISTS nba_wta_portfolios (
        id BIGSERIAL PRIMARY KEY,
        version TEXT NOT NULL,
        game_date DATE NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        first_tip TIMESTAMPTZ NOT NULL,
        inputs_captured_at TIMESTAMPTZ NOT NULL,
        payload JSONB NOT NULL,
        payload_sha256 TEXT NOT NULL,
        CONSTRAINT nba_wta_portfolios_before_tip CHECK (created_at < first_tip),
        CONSTRAINT nba_wta_portfolios_unique UNIQUE (version, game_date, payload_sha256)
    )""",
    "CREATE INDEX IF NOT EXISTS idx_nba_wta_portfolios_date ON nba_wta_portfolios(version, game_date, created_at DESC)",
    """CREATE TABLE IF NOT EXISTS nba_wta_grades (
        id BIGSERIAL PRIMARY KEY,
        portfolio_id BIGINT NOT NULL REFERENCES nba_wta_portfolios(id),
        version TEXT NOT NULL,
        grade_version TEXT NOT NULL,
        game_date DATE NOT NULL,
        graded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        payload JSONB NOT NULL,
        CONSTRAINT nba_wta_grades_unique UNIQUE (portfolio_id, grade_version)
    )""",
    "CREATE INDEX IF NOT EXISTS idx_nba_wta_grades_date ON nba_wta_grades(version, grade_version, game_date)",
]
