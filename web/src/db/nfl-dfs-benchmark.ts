import 'server-only';
import {sql} from 'drizzle-orm';
import {db} from '@/db';
import {gradeBenchmark,type BenchmarkSnapshot,type BenchmarkOutcome} from '@/lib/nfl-dfs/competitor-benchmark';

export const BENCHMARK_DDL=`CREATE TABLE IF NOT EXISTS nfl_dfs_competitor_benchmarks (
  digest TEXT PRIMARY KEY, upload_id UUID NOT NULL REFERENCES nfl_dfs_slate_uploads(upload_id),
  captured_at TIMESTAMPTZ NOT NULL, payload JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW())`;

export async function saveNflBenchmark(digest:string,payload:BenchmarkSnapshot) {
  await db.execute(sql.raw(BENCHMARK_DDL));
  await db.execute(sql`INSERT INTO nfl_dfs_competitor_benchmarks(digest,upload_id,captured_at,payload)
    SELECT ${digest},${payload.uploadId}::uuid,${payload.capturedAt}::timestamptz,${JSON.stringify(payload)}::jsonb
    WHERE clock_timestamp()<${payload.rows.map(r=>r.kickoff).sort()[0]}::timestamptz
    ON CONFLICT DO NOTHING`);
  const stored=await db.execute(sql`SELECT digest FROM nfl_dfs_competitor_benchmarks WHERE digest=${digest}`);
  if(!stored.rows.length)throw new Error('Kickoff passed before the benchmark could be saved. No benchmark was recorded.');
}

export async function readNflBenchmarks(uploadId:string) {
  // Table creation only on explicit freeze, not on a page view.
  const exists=await db.execute(sql`SELECT to_regclass('nfl_dfs_competitor_benchmarks') name`);
  if(!exists.rows[0]?.name)return [];
  const runs=await db.execute(sql`SELECT digest,payload FROM nfl_dfs_competitor_benchmarks WHERE upload_id=${uploadId}::uuid ORDER BY captured_at DESC,digest DESC LIMIT 12`);
  if(!runs.rows.length)return [];
  const payloads=runs.rows.map(r=>r.payload as BenchmarkSnapshot);
  const kickoffs=payloads.flatMap(p=>p.rows.map(r=>r.kickoff)).sort();
  const results=await db.execute(sql`SELECT DISTINCT ON (r.player_id,r.game_id,r.team,r.position)
    r.id,r.player_id,r.team,r.position,r.actual_dk_fpts,r.input_digest,g.kickoff,r.scoring_status
    FROM nfl_dfs_player_week_results r JOIN nfl_season_games g ON g.id=r.game_id
    WHERE g.completed AND g.kickoff BETWEEN ${kickoffs[0]}::timestamptz AND ${kickoffs.at(-1)}::timestamptz
    ORDER BY r.player_id,r.game_id,r.team,r.position,r.computed_at DESC,r.id DESC`);
  const outcomes:BenchmarkOutcome[]=results.rows.filter(r=>r.scoring_status==='exact'&&r.actual_dk_fpts!==null).map(r=>({playerId:Number(r.player_id),team:String(r.team),position:String(r.position),kickoff:new Date(r.kickoff as string).toISOString(),actual:Number(r.actual_dk_fpts),resultId:String(r.id),digest:String(r.input_digest)}));
  return runs.rows.map((r,i)=>({digest:String(r.digest),snapshot:payloads[i],grade:gradeBenchmark(payloads[i],outcomes)}));
}
