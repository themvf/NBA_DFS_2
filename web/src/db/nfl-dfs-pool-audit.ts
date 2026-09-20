import 'server-only';
import { createHash } from 'node:crypto';
import { sql } from 'drizzle-orm';
import { db } from '@/db';
import { canonicalAuditJson } from '@/lib/nfl-dfs/audit-json';
import { gradePool, type AuditGame, type PoolAuditPayload, type PoolCapture, type PoolOutcome } from '@/lib/nfl-dfs/pool-audit';

export const POOL_AUDIT_DDL = [
  `CREATE TABLE IF NOT EXISTS nfl_dfs_pool_captures (
    digest TEXT PRIMARY KEY, upload_id UUID NOT NULL REFERENCES nfl_dfs_slate_uploads(upload_id),
    game_id BIGINT NOT NULL REFERENCES nfl_season_games(id), kickoff TIMESTAMPTZ NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL, captured_at TIMESTAMPTZ NOT NULL, capture_key TEXT NOT NULL,
    payload JSONB NOT NULL, UNIQUE(upload_id,game_id,capture_key), CHECK(observed_at<=captured_at))`,
  `CREATE INDEX IF NOT EXISTS idx_nfl_pool_capture_upload ON nfl_dfs_pool_captures(upload_id,game_id,observed_at)`,
  `CREATE OR REPLACE FUNCTION reject_nfl_pool_capture_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN RAISE EXCEPTION 'NFL pool captures are append-only'; END $$`,
  `DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname='nfl_pool_capture_immutable') THEN
    CREATE TRIGGER nfl_pool_capture_immutable BEFORE UPDATE OR DELETE ON nfl_dfs_pool_captures
    FOR EACH ROW EXECUTE FUNCTION reject_nfl_pool_capture_mutation(); END IF; END $$`,
  `CREATE TABLE IF NOT EXISTS nfl_dfs_pool_capture_runs (
    id BIGSERIAL PRIMARY KEY, started_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    finished_at TIMESTAMPTZ, summary JSONB NOT NULL DEFAULT '{}'::jsonb)`
];
export async function installPoolAudit() { for(const ddl of POOL_AUDIT_DDL) await db.execute(sql.raw(ddl)); }
export async function poolAuditInstalled() {
  return Boolean((await db.execute(sql`SELECT to_regclass('nfl_dfs_pool_captures') AS name`)).rows[0]?.name);
}
export async function auditGames(season:number,week:number):Promise<AuditGame[]> {
  const rows=await db.execute(sql`SELECT g.id,g.season,g.week,g.kickoff,g.completed,h.abbreviation home,a.abbreviation away
    FROM nfl_season_games g JOIN nfl_teams h ON h.team_id=g.home_team_id JOIN nfl_teams a ON a.team_id=g.away_team_id
    WHERE g.season=${season} AND g.week=${week} AND g.game_type='REG'`);
  return rows.rows.map(g=>({id:Number(g.id),season:Number(g.season),week:Number(g.week),home:String(g.home),away:String(g.away),
    kickoff:new Date(g.kickoff as string).toISOString(),completed:Boolean(g.completed)}));
}
/** The SQL clock is authoritative. Archived time comes only from a verified saved run. */
export async function savePoolCapture(payload:PoolAuditPayload, archive?:{observedAt:string;runId:string}) {
  if(!payload.players.length || new Set(payload.players.map(p=>p.dkPlayerId)).size!==payload.players.length) throw new Error('Empty or duplicate pool capture');
  if((payload.origin==='saved_optimizer')!==Boolean(archive)) throw new Error('Missing archived run provenance');
  const content=canonicalAuditJson(payload);
  const hash=createHash('sha256').update(content).digest('hex');
  // One capture per minute per game; archived runs have a permanent idempotency key.
  const rows=await db.execute(sql`WITH stamp AS MATERIALIZED (SELECT clock_timestamp() AS at),
    input AS (SELECT at,COALESCE(${archive?.observedAt??null}::timestamptz,at) AS observed,
      COALESCE(${archive?`optimizer:${archive.runId}`:null}::text,'live:'||date_trunc('minute',at)::text) AS key FROM stamp)
    INSERT INTO nfl_dfs_pool_captures(digest,upload_id,game_id,kickoff,observed_at,captured_at,capture_key,payload)
    SELECT ${hash}||':'||input.key,${payload.uploadId}::uuid,${payload.game.id},${payload.game.kickoff}::timestamptz,
      input.observed,input.at,input.key,${content}::jsonb FROM input
    ON CONFLICT(upload_id,game_id,capture_key) DO NOTHING RETURNING digest,observed_at,captured_at`);
  return rows.rows;
}
export async function readPoolCaptures(uploadId:string,latest=false):Promise<PoolCapture[]> {
  const rows=await db.execute(sql`SELECT digest,observed_at,captured_at,payload FROM (
    SELECT *,row_number() OVER(PARTITION BY game_id ORDER BY CASE WHEN ${latest} THEN observed_at END DESC,(observed_at<kickoff) DESC,
      CASE WHEN observed_at<kickoff THEN observed_at END DESC,
      CASE WHEN observed_at>=kickoff THEN observed_at END ASC,digest) AS rank
    FROM nfl_dfs_pool_captures WHERE upload_id=${uploadId}::uuid) ranked WHERE rank=1 ORDER BY game_id`);
  return rows.rows.map(r=>{
    if(String(r.digest).split(':')[0]!==createHash('sha256').update(canonicalAuditJson(r.payload)).digest('hex'))
      throw new Error('Pool capture digest mismatch');
    return {digest:String(r.digest),observedAt:new Date(r.observed_at as string).toISOString(),
      capturedAt:new Date(r.captured_at as string).toISOString(),payload:r.payload as PoolAuditPayload};
  });
}
export async function poolAuditIndex() {
  if(!await poolAuditInstalled())return {uploads:[],health:null};
  const [uploads,health]=await Promise.all([
    db.execute(sql`SELECT u.upload_id,u.file_name,u.player_count,u.created_at,r.season,r.week,
      count(DISTINCT c.game_id)::int captured_games,
      count(DISTINCT c.game_id) FILTER(WHERE c.observed_at<c.kickoff)::int pregame_games
      FROM nfl_dfs_slate_uploads u JOIN nfl_dfs_projection_runs r ON r.run_id=u.projection_run_id
      LEFT JOIN nfl_dfs_pool_captures c ON c.upload_id=u.upload_id
      WHERE u.player_count=(SELECT count(*) FROM nfl_dfs_slate_players p WHERE p.upload_id=u.upload_id)
      GROUP BY u.upload_id,r.season,r.week
      ORDER BY r.season DESC,r.week DESC,pregame_games DESC,u.created_at DESC`),
    db.execute(sql`SELECT started_at,finished_at,summary FROM nfl_dfs_pool_capture_runs ORDER BY id DESC LIMIT 1`)
  ]);
  return {uploads:uploads.rows.map(u=>({id:String(u.upload_id),fileName:String(u.file_name),players:Number(u.player_count),
    createdAt:new Date(u.created_at as string).toISOString(),season:Number(u.season),week:Number(u.week),
    capturedGames:Number(u.captured_games),pregameGames:Number(u.pregame_games)})),
    health:health.rows[0]?{startedAt:new Date(health.rows[0].started_at as string).toISOString(),
      finishedAt:health.rows[0].finished_at?new Date(health.rows[0].finished_at as string).toISOString():null,
      summary:health.rows[0].summary as {errors?:{uploadId:string;error:string}[]}}:null};
}
export async function readPoolReview(uploadId:string,latest=false) {
  const captures=await readPoolCaptures(uploadId,latest);
  if(!captures.length)return {captures,rows:[],games:[],evaluatedAt:new Date().toISOString()};
  const first=captures[0].payload.game;
  const games=await auditGames(first.season,first.week);
  const results=await db.execute(sql`SELECT r.* FROM nfl_dfs_player_week_results r
    WHERE r.season=${first.season} AND r.week=${first.week} AND r.computed_at<=clock_timestamp()`);
  const outcomes:PoolOutcome[]=results.rows.map(r=>({id:String(r.id),playerId:Number(r.player_id),gameId:Number(r.game_id),
    team:String(r.team),position:String(r.position),actual:r.actual_dk_fpts==null?null:Number(r.actual_dk_fpts),
    status:String(r.scoring_status),digest:String(r.input_digest),computedAt:new Date(r.computed_at as string).toISOString(),
    scoringVersion:String(r.scoring_version),evidence:r.scoring_evidence as Record<string,unknown>}));
  return {captures,rows:gradePool(captures,outcomes,games),games,evaluatedAt:new Date().toISOString()};
}
