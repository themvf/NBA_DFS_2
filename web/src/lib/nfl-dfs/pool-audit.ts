import { benchmarkTeam, salaryMatchesKickoff } from './competitor-benchmark';

export const POOL_AUDIT_VERSION = 'nfl-dfs-pool-audit-v1';
export type AuditGame = { id: number; season: number; week: number; home: string; away: string; kickoff: string; completed: boolean };
export type PoolAuditPlayer = {
  dkPlayerId: number; playerId: number | null; name: string; team: string; position: string;
  salary: number; gameInfo: string | null; gameKey: string | null; isOut: boolean;
  projection: number | null; floor: number | null; ceiling: number | null;
  median: number | null; boom: number | null; scenario: string; stats: Record<string, number>;
  /** Entire resolved input, including alternate sources, roles and availability. */
  evidence: unknown;
};
export type PoolAuditPayload = {
  version: string; uploadId: string; fileName: string; fileDigest: string; projectionRunId: string | null;
  modelAsOf: string | null; codeRevision: string; origin: 'live_pool' | 'saved_optimizer';
  sourceRunId?: string; sourceDigest?: string; game: AuditGame;
  players: PoolAuditPlayer[]; context: unknown;
};
export type PoolCapture = { digest: string; capturedAt: string; observedAt: string; payload: PoolAuditPayload };
export type PoolOutcome = { id: string; playerId: number; gameId: number; team: string; position: string;
  actual: number | null; status: string; digest: string; computedAt: string; scoringVersion: string;
  evidence: Record<string, unknown> };
const finite = (v: unknown): v is number => typeof v === 'number' && Number.isFinite(v);
export function poolCaptureDue(kickoff:string,now:number,hasSnapshot:boolean) {
  const remaining=Date.parse(kickoff)-now;
  return remaining>0 && remaining<=86400000 && (!hasSnapshot||remaining<=20*60000)
    || remaining<=0 && remaining> -15*60000 && !hasSnapshot;
}

export function matchPoolGame(player: Pick<PoolAuditPlayer,'team'|'gameKey'|'gameInfo'>, games: AuditGame[]) {
  const matches = games.filter(g => salaryMatchesKickoff(player.gameInfo, g.kickoff)
    && `${benchmarkTeam(g.away)}@${benchmarkTeam(g.home)}` === player.gameKey?.split('@').map(benchmarkTeam).join('@')
    && [benchmarkTeam(g.home),benchmarkTeam(g.away)].includes(benchmarkTeam(player.team)));
  if (matches.length !== 1) throw new Error(`Cannot uniquely match salary game for ${player.team}: ${player.gameInfo}`);
  return matches[0];
}

/** Prefer the last preserved pregame observation, never a newer postgame pool. */
export function selectPoolCaptures(captures: PoolCapture[]) {
  const selected = new Map<number, PoolCapture>();
  for (const c of captures) {
    const game = c.payload.game;
    if (![c.observedAt,c.capturedAt,game.kickoff].every(v=>Number.isFinite(Date.parse(v))) || Date.parse(c.observedAt) > Date.parse(c.capturedAt)) continue;
    const previous = selected.get(game.id);
    const pre = Date.parse(c.observedAt) < Date.parse(game.kickoff);
    const oldPre = previous && Date.parse(previous.observedAt) < Date.parse(previous.payload.game.kickoff);
    // When no pregame evidence exists, show the earliest late record honestly.
    if (!previous || pre && !oldPre || pre === oldPre &&
      (pre ? Date.parse(c.observedAt) > Date.parse(previous.observedAt) : Date.parse(c.observedAt) < Date.parse(previous.observedAt))) selected.set(game.id,c);
  }
  return [...selected.values()].sort((a,b)=>a.payload.game.id-b.payload.game.id);
}

export function gradePool(captures: PoolCapture[], results: PoolOutcome[], games: AuditGame[]) {
  return selectPoolCaptures(captures).flatMap(c => c.payload.players.map(p => {
    const game = games.find(g=>g.id===c.payload.game.id);
    const scheduleMatches = game && Date.parse(game.kickoff) === Date.parse(c.payload.game.kickoff);
    const pregame = Date.parse(c.observedAt) < Date.parse(c.payload.game.kickoff);
    const matches = results.filter(r=>r.playerId===p.playerId && r.gameId===c.payload.game.id
      && benchmarkTeam(r.team)===benchmarkTeam(p.team) && r.position===p.position)
      .sort((a,b)=>Date.parse(b.computedAt)-Date.parse(a.computedAt) || Number(b.id)-Number(a.id));
    const result = matches[0];
    const actual = game?.completed && result?.status==='exact' && finite(result.actual) ? result.actual : null;
    const status = !scheduleMatches ? 'schedule_changed' : !pregame ? 'late_capture' : !p.playerId ? 'identity_unmatched'
      : !finite(p.projection) ? 'projection_missing' : !game.completed ? 'pending_result'
      : !result ? 'result_missing' : actual===null ? 'result_unscorable' : 'scored';
    const error = status==='scored' ? actual! - p.projection! : null;
    const intervalHit = error!==null && p.scenario==='baseline_simulation' && finite(p.floor) && finite(p.ceiling)
      && p.floor<=p.ceiling ? actual!>=p.floor && actual!<=p.ceiling : null;
    return {player:p, gameId:c.payload.game.id, kickoff:c.payload.game.kickoff, captureDigest:c.digest,
      observedAt:c.observedAt, capturedAt:c.capturedAt, origin:c.payload.origin, sourceRunId:c.payload.sourceRunId,
      pregame, status, actual, error, absoluteError:error===null?null:Math.abs(error), intervalHit,
      result:result ?? null, resultRevisions:matches.length};
  }));
}
export type PoolReviewRow = ReturnType<typeof gradePool>[number];
export function summarizePool(rows: PoolReviewRow[]) {
  const scored=rows.filter(r=>r.error!==null), intervals=scored.filter(r=>r.intervalHit!==null);
  const mean=(xs:number[])=>xs.length?xs.reduce((a,b)=>a+b,0)/xs.length:null;
  return {players:rows.length,scored:scored.length,late:rows.filter(r=>!r.pregame).length,
    mae:mean(scored.map(r=>r.absoluteError!)),bias:mean(scored.map(r=>r.error!)),
    coverage:mean(intervals.map(r=>Number(r.intervalHit))),intervals:intervals.length,
    statuses:Object.fromEntries([...new Set(rows.map(r=>r.status))].map(s=>[s,rows.filter(r=>r.status===s).length]))};
}
