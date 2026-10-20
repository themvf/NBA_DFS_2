"""DDL for `nfl_ownership_label_provenance` (model/nfl_ownership_provenance.py).

Dependency-free so db/schema.py and the writer (research/nfl_ownership_provenance.py)
apply the same statements. Append-only: one row per (contest, player, slot,
classifier version); a re-classification under new rules is a new version, and
UPDATE/DELETE are rejected by trigger.
"""

STATEMENTS = [
    """CREATE TABLE IF NOT EXISTS nfl_ownership_label_provenance (
        id BIGSERIAL PRIMARY KEY,
        contest_id TEXT NOT NULL,
        normalized_name TEXT NOT NULL,
        slot TEXT NOT NULL,
        version TEXT NOT NULL,
        label_class TEXT NOT NULL,
        label_pct DOUBLE PRECISION,
        stored_pct DOUBLE PRECISION,
        basis TEXT NOT NULL,
        import_path TEXT NOT NULL,
        in_ownership_table BOOLEAN NOT NULL,
        in_salary_pool BOOLEAN,
        slate_upload_id UUID,
        contest_mass DOUBLE PRECISION,
        record_complete BOOLEAN NOT NULL,
        full_standings BOOLEAN NOT NULL,
        params JSONB NOT NULL DEFAULT '{}'::jsonb,
        classified_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CHECK (slot IN ('overall','CPT','FLEX')),
        CHECK (label_class IN ('observed','reconciled_zero','synthesized_zero','unresolved')),
        CHECK ((label_class IN ('observed','reconciled_zero')) = (label_pct IS NOT NULL)),
        UNIQUE (contest_id, normalized_name, slot, version)
    )""",
    """CREATE INDEX IF NOT EXISTS idx_nfl_ownership_label_provenance_contest
        ON nfl_ownership_label_provenance (contest_id, version, label_class)""",
    """CREATE OR REPLACE FUNCTION reject_nfl_ownership_provenance_mutation()
       RETURNS trigger LANGUAGE plpgsql AS $$
       BEGIN
         RAISE EXCEPTION 'nfl_ownership_label_provenance is append-only; classify under a new version';
       END $$""",
    """DO $$ BEGIN
       IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'nfl_ownership_label_provenance_immutable') THEN
         CREATE TRIGGER nfl_ownership_label_provenance_immutable BEFORE UPDATE OR DELETE
         ON nfl_ownership_label_provenance FOR EACH ROW EXECUTE FUNCTION reject_nfl_ownership_provenance_mutation();
       END IF;
    END $$""",
]
