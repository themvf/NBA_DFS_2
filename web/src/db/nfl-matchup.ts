import 'server-only';
import { sql } from 'drizzle-orm';
import { db } from '@/db';
import { matchupExplanation, type FrozenMatchupProjection, type MatchupProjectionComparison } from '@/lib/nfl-dfs/matchup-explanation';
export type { MatchupProjectionComparison } from '@/lib/nfl-dfs/matchup-explanation';

/** Separate research comparison; never replaces the selected production row. */
export async function readMatchupComparison(uploadId:string, playerId:number):Promise<MatchupProjectionComparison|null> {
  try {
    const result=await db.execute(sql`SELECT r.as_of_at,r.baseline_run_id,p.projection
      FROM nfl_matchup_player_forecasts p JOIN nfl_matchup_forecast_runs r ON r.run_id=p.run_id
      WHERE r.upload_id=${uploadId}::uuid AND p.player_id=${playerId}
        AND r.created_at<p.kickoff AND r.as_of_at<p.kickoff
      ORDER BY r.created_at DESC,r.run_id DESC LIMIT 1`);
    const row=result.rows[0];if(!row)return null;
    return matchupExplanation(row.projection as FrozenMatchupProjection, new Date(String(row.as_of_at)).toISOString(), String(row.baseline_run_id));
  } catch { return null; } // Optional research feed cannot disable the Why panel.
}
