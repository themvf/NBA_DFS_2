import 'server-only';
import { sql } from 'drizzle-orm';
import { db } from '@/db';
import type { DefensiveCapture, CaptureProfile, FrozenDefensiveCandidate } from '@/lib/nfl-dfs/defensive-projection';

/** One slate manifest is selected before any player rows are read. */
export async function readDefensiveCaptures(uploadId: string, baselineRunId: string,
  profile: CaptureProfile, decisionAt: Date): Promise<Map<number, DefensiveCapture>> {
  const modelVersion=profile==='pfr-efficiency'?'nfl-matchup-shadow-v1':'nfl-allowed-rushing-volume-v1';
  const run = await db.execute(sql`SELECT run_id,baseline_run_id,as_of_at,created_at,artifact_digest
    FROM nfl_matchup_forecast_runs
    WHERE upload_id=${uploadId}::uuid AND baseline_run_id=${baselineRunId}::uuid
      AND model_version=${modelVersion}
      AND as_of_at<=${decisionAt.toISOString()}::timestamptz
      AND created_at<=${decisionAt.toISOString()}::timestamptz
    ORDER BY as_of_at DESC,created_at DESC,run_id DESC LIMIT 1`);
  const selected = run.rows[0];
  if (!selected) return new Map();
  const rows = await db.execute(sql`SELECT p.player_id,p.game_id,p.kickoff,p.projection
    FROM nfl_matchup_player_forecasts p
    WHERE p.run_id=${String(selected.run_id)} AND p.kickoff>${decisionAt.toISOString()}::timestamptz
    ORDER BY p.player_id LIMIT 2000`);
  const captures = new Map<number, DefensiveCapture>();
  for (const row of rows.rows) {
    const candidate = row.projection as FrozenDefensiveCandidate;
    if (Date.parse(String(selected.as_of_at)) >= Date.parse(String(row.kickoff))
      || Date.parse(String(selected.created_at)) >= Date.parse(String(row.kickoff))) continue;
    const id = Number(row.player_id);
    if (!Number.isSafeInteger(id) || captures.has(id)) continue;
    captures.set(id, { runId: String(selected.run_id), baselineRunId: String(selected.baseline_run_id),
      capturedAt: new Date(String(selected.as_of_at)).toISOString(), artifactDigest: String(selected.artifact_digest), candidate });
  }
  return captures;
}
