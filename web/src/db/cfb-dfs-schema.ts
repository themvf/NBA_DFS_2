import { sql } from "drizzle-orm";
import { db } from "@/db";

/**
 * CFB DFS tables. Kept apart from the NFL DFS tables on purpose: the two
 * products share no rows, and a change to one cannot migrate the other.
 *
 * Completion markers: the web database commits every statement on its own, so
 * each save writes its detail rows first and the parent row (cfb_dfs_slates,
 * cfb_dfs_lineup_runs) LAST. Readers only list parents, so an interrupted save
 * is invisible rather than half-shown.
 */
const CFB_DFS_DDLS = [
  `CREATE TABLE IF NOT EXISTS cfb_dfs_slate_players (
    upload_id UUID NOT NULL, dk_id BIGINT NOT NULL, name TEXT NOT NULL, position TEXT NOT NULL,
    team TEXT NOT NULL, opponent TEXT NOT NULL, game TEXT NOT NULL, kickoff TIMESTAMPTZ,
    salary INTEGER NOT NULL, dk_avg DOUBLE PRECISION NOT NULL, status TEXT NOT NULL DEFAULT '',
    proj DOUBLE PRECISION, rate DOUBLE PRECISION, env DOUBLE PRECISION,
    games_2026 INTEGER, games_2025 INTEGER, cfbd_player_id TEXT, match_method TEXT,
    PRIMARY KEY (upload_id, dk_id))`,
  `CREATE TABLE IF NOT EXISTS cfb_dfs_slates (
    upload_id UUID PRIMARY KEY, file_name TEXT NOT NULL, file_digest TEXT NOT NULL,
    games JSONB NOT NULL, first_kickoff TIMESTAMPTZ, player_count INTEGER NOT NULL,
    team_map JSONB NOT NULL DEFAULT '{}'::jsonb, projection_version TEXT, projected_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW())`,
  `CREATE TABLE IF NOT EXISTS cfb_dfs_lineups (
    run_id UUID NOT NULL, lineup_number INTEGER NOT NULL, slots JSONB NOT NULL,
    salary INTEGER NOT NULL, projection DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (run_id, lineup_number))`,
  `CREATE TABLE IF NOT EXISTS cfb_dfs_lineup_runs (
    run_id UUID PRIMARY KEY, upload_id UUID NOT NULL, settings JSONB NOT NULL,
    projection_version TEXT NOT NULL, optimizer_version TEXT NOT NULL,
    lineup_count INTEGER NOT NULL, stopped_early TEXT, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW())`,
  `CREATE INDEX IF NOT EXISTS idx_cfb_dfs_lineup_runs_upload ON cfb_dfs_lineup_runs (upload_id, created_at DESC)`,
  // Contest results (DraftKings standings). Players first, the contest row last.
  `CREATE TABLE IF NOT EXISTS cfb_dfs_contest_players (
    contest_id TEXT NOT NULL, player_key TEXT NOT NULL, name TEXT NOT NULL,
    drafted_pct DOUBLE PRECISION NOT NULL, drafted_by_slot JSONB NOT NULL, fpts DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (contest_id, player_key))`,
  `CREATE TABLE IF NOT EXISTS cfb_dfs_contests (
    contest_id TEXT PRIMARY KEY, upload_id UUID NOT NULL, file_name TEXT NOT NULL, file_digest TEXT NOT NULL,
    entry_count INTEGER NOT NULL, winning_score DOUBLE PRECISION, median_score DOUBLE PRECISION,
    score_curve JSONB NOT NULL, imported_at TIMESTAMPTZ NOT NULL DEFAULT NOW())`,
  `CREATE INDEX IF NOT EXISTS idx_cfb_dfs_contests_upload ON cfb_dfs_contests (upload_id, imported_at DESC)`,
];

let ready: Promise<void> | null = null;

export async function ensureCfbDfsTables(): Promise<void> {
  if (!ready) {
    ready = (async () => {
      for (const ddl of CFB_DFS_DDLS) await db.execute(sql.raw(ddl));
    })().catch((error) => { ready = null; throw error; });
  }
  await ready;
}
