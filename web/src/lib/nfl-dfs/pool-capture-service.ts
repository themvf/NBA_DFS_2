import 'server-only';
import { createHash } from 'node:crypto';
import { sql } from 'drizzle-orm';
import { db } from '@/db';
import { loadSavedNflWorkspace, type NflWorkspacePlayer } from '@/app/dfs/nfl/actions';
import { auditGames, installPoolAudit, poolAuditInstalled, savePoolCapture } from '@/db/nfl-dfs-pool-audit';
import { canonicalAuditJson } from './audit-json';
import { matchPoolGame, poolCaptureDue, POOL_AUDIT_VERSION, type AuditGame, type PoolAuditPayload, type PoolAuditPlayer } from './pool-audit';

const number=(v:unknown):number|null=>typeof v==='number'&&Number.isFinite(v)?v:null;
function livePlayer(p:NflWorkspacePlayer):PoolAuditPlayer {
  return {dkPlayerId:p.dkPlayerId,playerId:p.ffPlayerId,name:p.name,team:p.team,position:p.position,
    salary:p.salary,gameInfo:p.gameInfo,gameKey:p.gameKey,isOut:p.isOut||p.projectionStatus==='out',projection:p.ourProj,
    floor:p.floorFpts,ceiling:p.ceilingFpts,median:p.medianFpts??null,boom:p.boomRate,
    scenario:p.projectionScenario??'unknown',stats:p.statMeans??{},evidence:p};
}
async function uploadInfo(uploadId:string) {
  const rows=await db.execute(sql`SELECT u.*,r.season,r.week FROM nfl_dfs_slate_uploads u
    JOIN nfl_dfs_projection_runs r ON r.run_id=u.projection_run_id WHERE u.upload_id=${uploadId}::uuid`);
  if(!rows.rows.length)throw new Error('Upload must have a matched projection run');
  return rows.rows[0];
}
function groupPlayers(players:PoolAuditPlayer[],games:AuditGame[]) {
  const grouped=new Map<number,{game:AuditGame;players:PoolAuditPlayer[]}>();
  for(const p of players) {
    const game=matchPoolGame(p,games);
    const group=grouped.get(game.id)??{game,players:[]};group.players.push(p);grouped.set(game.id,group);
  }
  return [...grouped.values()];
}
export async function capturePool(uploadId:string, dueOnly=false) {
  if(!/^[a-f0-9-]{36}$/.test(uploadId))throw new Error('Invalid upload');
  if(!await poolAuditInstalled())await installPoolAudit();
  const info=await uploadInfo(uploadId);
  const weekGames=await auditGames(Number(info.season),Number(info.week));
  const salaryGames=await db.execute(sql`SELECT DISTINCT team,game_key,game_info FROM nfl_dfs_slate_players WHERE upload_id=${uploadId}::uuid`);
  const gameIds=new Set(salaryGames.rows.map(p=>matchPoolGame({team:String(p.team),gameKey:p.game_key as string|null,gameInfo:p.game_info as string|null},weekGames).id));
  const games=weekGames.filter(g=>gameIds.has(g.id));
  const existing=await db.execute(sql`SELECT game_id,bool_or(observed_at<kickoff) AS pregame
    FROM nfl_dfs_pool_captures WHERE upload_id=${uploadId}::uuid GROUP BY game_id`);
  const has=new Map(existing.rows.map(r=>[Number(r.game_id),Boolean(r.pregame)]));
  const now=Date.now();
  const due=(g:AuditGame)=>!dueOnly || poolCaptureDue(g.kickoff,now,has.has(g.id));
  if(dueOnly&&!games.some(due))return {uploadId,saved:0,players:0,skipped:true};
  const {slate}=await loadSavedNflWorkspace(uploadId);
  if(slate.players.length!==Number(info.player_count))throw new Error('Incomplete salary pool; capture refused');
  const groups=groupPlayers(slate.players.map(livePlayer),games);
  let saved=0,players=0;
  for(const group of groups.filter(g=>due(g.game))) {
    const payload:PoolAuditPayload={version:POOL_AUDIT_VERSION,uploadId,fileName:String(info.file_name),
      fileDigest:String(info.file_digest),projectionRunId:slate.projectionRunId,modelAsOf:slate.modelAsOf,
      codeRevision:process.env.VERCEL_GIT_COMMIT_SHA??process.env.NFL_POOL_CODE_REVISION??'local-uncommitted',
      origin:'live_pool',game:group.game,players:group.players,
      context:{modelVersion:slate.modelVersion,format:slate.format,redistribution:slate.redistribution,
        injuryCoverage:slate.injuryCoverage,warnings:slate.warnings,situationTeams:slate.situationTeams,
        totalSalaryPool:slate.players.length,projectionSource:'our',resolvedAt:new Date().toISOString()}};
    const inserted=await savePoolCapture(payload);saved+=inserted.length;if(inserted.length)players+=group.players.length;
  }
  return {uploadId,saved,players,skipped:false};
}

/** Preserve genuine older full-pool observations; never backdate a newly computed pool. */
export async function archiveOptimizerPool(runId:string) {
  if(!await poolAuditInstalled())await installPoolAudit();
  const runs=await db.execute(sql`SELECT * FROM nfl_dfs_optimizer_runs WHERE run_id=${runId}::uuid`);
  const run=runs.rows[0];if(!run)throw new Error('Saved optimizer run missing');
  const raw=run.input_snapshot as Record<string,unknown>[];
  const hash=createHash('sha256').update(canonicalAuditJson({settings:run.settings,inputSnapshot:raw,optimizerVersion:run.optimizer_version})).digest('hex');
  if(hash!==run.input_digest)throw new Error('Saved optimizer input digest mismatch; archive refused');
  const info=await uploadInfo(String(run.upload_id));
  if(raw.length!==Number(info.player_count))throw new Error('This optimizer run did not save the full salary pool');
  const players:PoolAuditPlayer[]=raw.map(p=>({dkPlayerId:Number(p.dkPlayerId),playerId:number(p.ffPlayerId),name:String(p.name),
    team:String(p.team),position:String(p.position),salary:Number(p.salary),gameInfo:p.gameInfo as string|null,
    gameKey:p.gameKey as string|null,isOut:p.isOut===true||p.projectionStatus==='out',projection:number(p.ourProj),
    floor:number(p.floor),ceiling:number(p.ceiling),median:null,boom:number(p.boomRate),
    scenario:String(p.projectionScenario??'legacy_saved_estimate'),stats:(p.statMeans??{}) as Record<string,number>,evidence:p}));
  const games=await auditGames(Number(info.season),Number(info.week));
  let saved=0;
  for(const group of groupPlayers(players,games)) {
    const payload:PoolAuditPayload={version:POOL_AUDIT_VERSION,uploadId:String(run.upload_id),fileName:String(info.file_name),
      fileDigest:String(info.file_digest),projectionRunId:String(run.projection_run_id),modelAsOf:null,
      codeRevision:'not recorded by legacy optimizer',origin:'saved_optimizer',sourceRunId:runId,sourceDigest:hash,
      game:group.game,players:group.players,context:{optimizerVersion:run.optimizer_version,settings:run.settings,
        totalSalaryPool:raw.length,distributionNote:'Legacy saved metrics may mix baseline simulations and workload estimates; retained verbatim.'}};
    saved+=(await savePoolCapture(payload,{observedAt:new Date(run.created_at as string).toISOString(),runId})).length;
  }
  return {runId,saved,players:players.length};
}

export async function captureDuePools() {
  if(!await poolAuditInstalled())await installPoolAudit();
  const start=await db.execute(sql`INSERT INTO nfl_dfs_pool_capture_runs DEFAULT VALUES RETURNING id`);
  const id=Number(start.rows[0].id);
  const summary:{codeRevision:string;captured:Awaited<ReturnType<typeof capturePool>>[];errors:{uploadId:string;error:string}[]}={
    codeRevision:process.env.VERCEL_GIT_COMMIT_SHA??process.env.NFL_POOL_CODE_REVISION??'local-uncommitted',captured:[],errors:[]};
  try {
    // Latest complete revision of each salary file; no player-table display filters.
    const uploads=await db.execute(sql`SELECT DISTINCT ON(u.file_digest) u.upload_id FROM nfl_dfs_slate_uploads u
      JOIN nfl_dfs_projection_runs r ON r.run_id=u.projection_run_id
      WHERE u.player_count=(SELECT count(*) FROM nfl_dfs_slate_players p WHERE p.upload_id=u.upload_id)
      AND EXISTS(SELECT 1 FROM nfl_season_games g WHERE g.season=r.season AND g.week=r.week
        AND g.kickoff BETWEEN clock_timestamp()-INTERVAL '15 minutes' AND clock_timestamp()+INTERVAL '24 hours')
      ORDER BY u.file_digest,u.created_at DESC,u.upload_id`);
    for(const row of uploads.rows) {
      const uploadId=String(row.upload_id);
      try {summary.captured.push(await capturePool(uploadId,true));}
      catch(error){summary.errors.push({uploadId,error:error instanceof Error?error.message:'Capture failed'});}
    }
  } catch(error) {summary.errors.push({uploadId:'scheduler',error:error instanceof Error?error.message:'Scheduler failed'});}
  await db.execute(sql`UPDATE nfl_dfs_pool_capture_runs SET finished_at=clock_timestamp(),summary=${JSON.stringify(summary)}::jsonb WHERE id=${id}`);
  return summary;
}
