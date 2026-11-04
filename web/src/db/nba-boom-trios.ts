import {isMissingRelation} from "@/lib/database-errors";
import "server-only";
import {verifySavedShotGrowth} from "@/lib/nba-trio-results";

import { sql } from "drizzle-orm";
import { db } from "@/db";
import type { BoomCandidate, BoomGradeRow, BoomSnapshot } from "@/lib/nba-boom-trios";

/**
 * One frozen ghost v2 snapshot per slate date (the first prospective one, else
 * the first), slimmed in SQL to the fields the boom rules read (snapshots run
 * up to ~0.8 MB), with the latest grade's rows for it when there is one. Same
 * selection as research/nba_boom_trios.py `_load`. Read-only.
 */
export async function getBoomTrioDays(): Promise<{ snapshot: BoomSnapshot; rows: BoomGradeRow[] | null }[]> {
  try {
    const snaps = await db.execute(sql`
      SELECT s.game_date::text AS game_date, s.snapshot_sha256,
        s.snapshot->>'created_at' AS created_at, s.snapshot->>'mode' AS mode, s.snapshot->>'sha256' AS sha256,
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
      WHERE s.version = 'nba-points-ghost-v2'
      ORDER BY s.game_date, s.created_at`);
    const grades = await db.execute(sql`
      SELECT DISTINCT ON (g.snapshot_sha256) g.snapshot_sha256,
        (SELECT jsonb_agg(jsonb_build_object('event_id'::text, r->'event_id', 'player_key'::text, r->'player_key',
            'status'::text, r->'status', 'actual_points'::text, r->'actual_points', 'historical_p90'::text, r->'historical_p90'))
         FROM jsonb_array_elements(g.grade->'rows') r) AS rows
      FROM nba_ghost_grades g
      ORDER BY g.snapshot_sha256, g.graded_at DESC, g.id DESC`);
    const rowsBySnap = new Map((grades.rows as { snapshot_sha256: string; rows: BoomGradeRow[] | null }[]).map((r) => [r.snapshot_sha256, r.rows]));
    const byDate = new Map<string, { snapshot: BoomSnapshot; rows: BoomGradeRow[] | null }>();
    for (const r of snaps.rows as { game_date: string; snapshot_sha256: string; created_at: string | null; mode: string | null; sha256: string | null; candidates: BoomCandidate[] | null }[]) {
      const snapshot: BoomSnapshot = { game_date: r.game_date, sha256: r.sha256, created_at: r.created_at, mode: r.mode, candidates: r.candidates ?? [] };
      const prev = byDate.get(r.game_date);
      if (!prev || (prev.snapshot.mode !== "prospective" && r.mode === "prospective")) byDate.set(r.game_date, { snapshot, rows: rowsBySnap.get(r.snapshot_sha256) ?? null });
    }
    return [...byDate.values()].sort((a, b) => (a.snapshot.game_date < b.snapshot.game_date ? -1 : 1));
  } catch (error) {
    if (isMissingRelation(error)) return [];
    throw error;
  }
}

export type SavedBoomLineup = {
  id: string; gameDate: string; createdAt: string; lockAt: string; selected: string | null; requested: number; maxDrop: number;
  baseline: string[][]; adjusted: string[][]; snapshotSha:string; policy:string; rows:BoomGradeRow[]|null; supported:boolean; selectedName:string|null;
};

/** Saved boom-lineup comparisons (nba_boom_lineup_records), newest first. Empty before the first save creates the table. */
export async function getSavedBoomLineups(limit = 100): Promise<SavedBoomLineup[]> {
  try {
    const result = await db.execute(sql`
      SELECT b.id,b.game_date::text AS game_date,b.created_at,b.lock_at,b.snapshot_sha256,b.policy,b.payload,
        b.payload->'settings' AS settings,
        (SELECT jsonb_agg(t->'ids') FROM jsonb_array_elements(CASE WHEN jsonb_typeof(b.payload->'baseline')='array' THEN b.payload->'baseline' ELSE '[]'::jsonb END) t) AS baseline,
        (SELECT jsonb_agg(t->'ids') FROM jsonb_array_elements(CASE WHEN jsonb_typeof(b.payload->'adjusted')='array' THEN b.payload->'adjusted' ELSE '[]'::jsonb END) t) AS adjusted,
        (SELECT jsonb_agg(jsonb_build_object('event_id',c->'event_id','player_key',c->'player_key','history_status',c->'history_status',
          'predictions',jsonb_build_object('market_line',c->'predictions'->'market_line'),'features',c->'features'))
         FROM jsonb_array_elements(s.snapshot->'candidates') c) AS source_candidates,
        g.grade->'rows' AS outcome_rows
      FROM nba_boom_lineup_records b
      LEFT JOIN nba_ghost_snapshots s ON s.snapshot_sha256=b.snapshot_sha256 AND s.snapshot->>'sha256'=b.snapshot_sha256 AND s.mode='prospective' AND s.game_date=b.game_date
      LEFT JOIN LATERAL (SELECT grade FROM nba_ghost_grades
        WHERE snapshot_sha256=b.snapshot_sha256 AND grade->>'snapshot_sha256'=b.snapshot_sha256
        ORDER BY graded_at DESC,id DESC LIMIT 1) g ON true
      ORDER BY b.created_at DESC LIMIT ${limit}`);
    return (result.rows as Record<string, unknown>[]).map((r) => {
      const s = (r.settings ?? {}) as { selected?: string | null; requested?: number; maxDrop?: number };
      const payload=r.payload as {version?:string;board?:{snapshot?:string;players?:{id:string;points:number;boom:number|null;name?:string}[]}};
      const supported=String(r.policy)===payload?.version && new Date(String(r.created_at))<new Date(String(r.lock_at)) && verifySavedShotGrowth(payload,String(r.snapshot_sha256),(r.source_candidates??[]) as Parameters<typeof verifySavedShotGrowth>[2]);
      return {snapshotSha:String(r.snapshot_sha256),policy:String(r.policy),supported,
        selectedName:payload?.board?.players?.find(p=>p.id===s.selected)?.name??null,
        rows:supported?(r.outcome_rows as BoomGradeRow[]|null):null, id: String(r.id), gameDate: String(r.game_date), createdAt: String(r.created_at), lockAt: String(r.lock_at),
        selected: s.selected ?? null, requested: Number(s.requested ?? 0), maxDrop: Number(s.maxDrop ?? 0),
        baseline: (r.baseline as string[][] | null) ?? [], adjusted: (r.adjusted as string[][] | null) ?? [] };
    });
  } catch (error) {
    if (isMissingRelation(error)) return [];
    throw error;
  }
}
