"""Additive identity evidence storage; existing canonical player rows are untouched."""

DDL = '''
CREATE TABLE IF NOT EXISTS nfl_player_identity_claims (
 claim_digest TEXT PRIMARY KEY,
 namespace TEXT NOT NULL, external_id TEXT NOT NULL, gsis_id TEXT NOT NULL,
 player_name TEXT NOT NULL, season INTEGER NOT NULL, team TEXT, position TEXT,
 method TEXT NOT NULL, source_digest TEXT NOT NULL, evidence JSONB NOT NULL,
 recorded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS nfl_identity_namespace_lookup ON nfl_player_identity_claims(namespace,external_id);
CREATE INDEX IF NOT EXISTS nfl_identity_gsis_lookup ON nfl_player_identity_claims(gsis_id);
CREATE OR REPLACE VIEW nfl_player_identity_crosswalk AS
 SELECT namespace, external_id,
   CASE WHEN COUNT(DISTINCT gsis_id)=1 THEN MIN(gsis_id) END AS gsis_id,
   CASE WHEN COUNT(DISTINCT gsis_id)=1 THEN 'resolved' ELSE 'conflict' END AS status,
   ARRAY_AGG(DISTINCT gsis_id ORDER BY gsis_id) AS candidates,
   COUNT(*) AS evidence_count
 FROM nfl_player_identity_claims GROUP BY namespace,external_id;
CREATE TABLE IF NOT EXISTS nfl_player_identity_runs (
 run_digest TEXT PRIMARY KEY, report JSONB NOT NULL,
 recorded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS nfl_dk_archive_result_reconciliation (
 result_digest TEXT PRIMARY KEY, run_digest TEXT NOT NULL REFERENCES nfl_player_identity_runs(run_digest),
 draft_group_id BIGINT NOT NULL, player_id BIGINT NOT NULL, game_id TEXT NOT NULL,
 gsis_id TEXT, actual_dk_fpts DOUBLE PRECISION, status TEXT NOT NULL, evidence JSONB NOT NULL,
 UNIQUE(run_digest,draft_group_id,player_id)
);
'''

SLATE_MIGRATION = '''
ALTER TABLE nfl_dfs_slate_players ADD COLUMN IF NOT EXISTS identity_evidence JSONB NOT NULL DEFAULT '{}'::jsonb;
DO $$ BEGIN
 IF EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='nfl_dfs_slate_players'::regclass
   AND conname='nfl_dfs_slate_players_identity_method_check'
   AND pg_get_constraintdef(oid) NOT LIKE '%identifier_conflict%') THEN
  ALTER TABLE nfl_dfs_slate_players DROP CONSTRAINT nfl_dfs_slate_players_identity_method_check;
 END IF;
 IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='nfl_dfs_slate_players'::regclass
   AND conname='nfl_dfs_slate_players_identity_method_check') THEN
  ALTER TABLE nfl_dfs_slate_players ADD CONSTRAINT nfl_dfs_slate_players_identity_method_check
   CHECK(identity_method IN ('gsis_id','exact_name_position_team','exact_name_position','unmatched','ambiguous',
   'team_position_dst','team_conflict','position_conflict','missing_team','identifier_conflict'));
 END IF;
END $$;
'''
