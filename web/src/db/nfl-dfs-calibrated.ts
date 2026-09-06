import "server-only";
import { sql } from "drizzle-orm";
import { db } from "@/db";
import { calibratedRelease, type CalibrationSnapshot } from "@/lib/nfl-dfs/calibrated-projection";

/** Read-only: existing daily shadow job owns immutable forecast snapshots. */
export async function getCalibratedSnapshots(season: number, week: number): Promise<CalibrationSnapshot[]> {
  const result = await db.execute(sql`SELECT DISTINCT ON (player_id)
    id::text,player_id,season,week,captured_at,kickoff,payload
    FROM nfl_dfs_shadow_predictions
    WHERE study_run_id=${calibratedRelease.studyId} AND season=${season} AND week=${week}
    ORDER BY player_id,captured_at DESC,id DESC`);
  return result.rows.map(r => ({ id: String(r.id), playerId: Number(r.player_id), season: Number(r.season), week: Number(r.week), capturedAt: new Date(r.captured_at as string).toISOString(), kickoff: new Date(r.kickoff as string).toISOString(), payload: r.payload }));
}
