import "server-only";
import { sql } from "drizzle-orm";
import { db } from "@/db";
import { readBoomBoard, type BoardSnapshot } from "@/lib/nba-boom-lineups";

/**
 * The date's first prospective ghost v2 snapshot, slimmed in SQL to the fields
 * the boom builder reads (a snapshot runs to ~0.8 MB), as a board. Null when
 * there is none or its stored digest disagrees with the row's.
 */
export async function getBoomBoard(date: string) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) return null;
  try {
    const result = await db.execute(sql`
      SELECT s.snapshot_sha256, s.snapshot->>'version' AS version, s.snapshot->>'mode' AS mode,
        s.snapshot->>'game_date' AS game_date, s.snapshot->>'sha256' AS sha256, s.snapshot->>'model_sha256' AS model_sha256,
        s.snapshot->>'created_at' AS created_at, s.snapshot->>'lock_at' AS lock_at,
        (SELECT jsonb_agg(jsonb_build_object(
            'event_id'::text, c->'event_id', 'player_key'::text, c->'player_key', 'player'::text, c->'player',
            'history_status'::text, c->'history_status',
            'availability'::text, jsonb_build_object('status'::text, c->'availability'->'status'),
            'predictions'::text, jsonb_build_object('market_line'::text, c->'predictions'->'market_line'),
            'features'::text, CASE WHEN jsonb_typeof(c->'features') = 'object' THEN jsonb_build_object(
              'shots_growth'::text, c->'features'->'shots_growth', 'ft_growth'::text, c->'features'->'ft_growth',
              'fga3'::text, c->'features'->'fga3', 'fta3'::text, c->'features'->'fta3') END))
         FROM jsonb_array_elements(s.snapshot->'candidates') c) AS candidates
      FROM nba_ghost_snapshots s
      WHERE s.game_date = ${date}::date AND s.version = 'nba-points-ghost-v2' AND s.mode = 'prospective'
      ORDER BY s.created_at ASC LIMIT 1`);
    const row = result.rows[0] as (BoardSnapshot & { snapshot_sha256: string }) | undefined;
    if (!row) return null;
    const board = readBoomBoard(row, date);
    return board?.snapshot === row.snapshot_sha256 ? board : null;
  } catch (error) {
    if (/relation .* does not exist/i.test(String(error))) return null;
    throw error;
  }
}
