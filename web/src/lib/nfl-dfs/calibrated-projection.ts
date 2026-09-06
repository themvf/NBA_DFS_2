import release from "./calibrated-release.json";

export { release as calibratedRelease };
export const CALIBRATED_MAX_AGE_HOURS = 72;
export type CalibrationTerm = {name:string; input:number; center:number; scale:number; coefficient:number; points:number};
export type CalibratedProjection = {
  mean: number; p10: number; p50: number; p90: number; boom: number;
  priorOpportunity?: number;
  explanationTerms?: CalibrationTerm[];
  baselineMean: number; baselineP10: number; baselineP90: number;
  snapshotId: string; capturedAt: string; kickoff: string;
  recipeDigest: string; studyDigest: string; releaseVersion: string;
};
export type CalibrationTarget = { ffPlayerId: number | null; position: string; team: string; opponent: string | null; gameInfo: string | null };
export type CalibrationSnapshot = { id: string; playerId: number; season: number; week: number; capturedAt: string; kickoff: string; payload: unknown; explanationTerms?:CalibrationTerm[] };
type RecordValue = Record<string, unknown>;
const record = (v: unknown): RecordValue => v !== null && typeof v === "object" && !Array.isArray(v) ? v as RecordValue : {};
const finite = (v: unknown): v is number => typeof v === "number" && Number.isFinite(v);

/** No inferred missing values, fuzzy identity or cross-week candidate reuse. */
export function readCalibratedProjection(snapshot: CalibrationSnapshot | undefined, target: CalibrationTarget, season: number, week: number, now: number): { projection: CalibratedProjection | null; reason: string } {
  return readPinnedProjection(snapshot,target,season,week,now,false);
}

/** Explicit experimental access for RB/TE; the calibrated release gate stays unchanged. */
export function readPositionWorkloadProjection(snapshot: CalibrationSnapshot | undefined, target: CalibrationTarget, season:number, week:number, now:number) {
  if(!['QB','RB','TE'].includes(target.position))return {projection:null,reason:'This position uses a different workload recipe or the historical baseline.'};
  const result=readPinnedProjection(snapshot,target,season,week,now,true);
  return result.projection ? {...result,projection:{...result.projection,releaseVersion:"nfl-dfs-position-workload-opt-in-v1"},reason:target.position==='QB'?'Pinned QB workload candidate; experimental.':'Experimental candidate: historical range quality worsened; enable this position explicitly.'} : result;
}

function readPinnedProjection(snapshot: CalibrationSnapshot | undefined, target: CalibrationTarget, season:number, week:number, now:number, experimental:boolean): {projection:CalibratedProjection|null;reason:string} {
  const no = (reason: string) => ({ projection: null, reason });
  const policy = release.positions[target.position as keyof typeof release.positions];
  if (!policy || (!experimental && !policy.enabledForOptIn)) return no("Position retains baseline: candidate interval scores did not qualify (or unsupported).");
  if (!snapshot) return no("No frozen candidate for this player and week.");
  if (snapshot.playerId !== target.ffPlayerId || snapshot.season !== season || snapshot.week !== week) return no("Candidate identity/week mismatch.");
  const p = record(snapshot.payload), candidate = record(p.candidate);
  if (p.position !== target.position || p.team !== target.team || !target.opponent || p.opponent !== target.opponent) return no("Candidate matchup mismatch.");
  if (p.source_study_digest !== release.studyDigest || candidate.recipe_digest !== policy.recipeDigest) return no("Candidate recipe is not pinned to this release.");
  const captured = Date.parse(snapshot.capturedAt), kickoff = Date.parse(snapshot.kickoff);
  if (![captured, kickoff, now].every(Number.isFinite) || captured > now || captured >= kickoff || now >= kickoff) return no("Candidate is not usable before kickoff.");
  if (now - captured > CALIBRATED_MAX_AGE_HOURS * 3600000) return no("Candidate is older than 72 hours; refresh the daily forecast.");
  const date = target.gameInfo?.match(/\b(\d{2}\/\d{2}\/\d{4})\b/)?.[1];
  if (!date || date !== new Intl.DateTimeFormat("en-US", { timeZone: "America/New_York", month: "2-digit", day: "2-digit", year: "numeric" }).format(kickoff)) return no("Salary game date does not match the candidate kickoff.");
  const time = target.gameInfo?.match(/\b(\d{1,2}:\d{2})\s*(AM|PM)\s+ET\b/i);
  const easternTime = new Intl.DateTimeFormat("en-US", { timeZone: "America/New_York", hour: "2-digit", minute: "2-digit", hour12: true }).format(kickoff).replace(/\s/g, "");
  if (!time || `${time[1].padStart(5, "0")}${time[2].toUpperCase()}` !== easternTime) return no("Salary kickoff time does not match the candidate forecast.");
  const cutoff = p.history_cutoff;
  if (!Array.isArray(cutoff) || cutoff.length !== 2 || !cutoff.every(Number.isInteger) || !(cutoff[0] < season || cutoff[0] === season && cutoff[1] < week)) return no("Historical cutoff is not strictly before the target week.");
  if (![candidate.prediction, candidate.p10, candidate.median, candidate.p90, candidate.boom_probability, p.baseline, p.p10, p.p90].every(finite)) return no("Candidate distribution is incomplete.");
  const mean = candidate.prediction as number, p10 = candidate.p10 as number, p50 = candidate.median as number, p90 = candidate.p90 as number, boom = candidate.boom_probability as number;
  if (p10 > p50 || p50 > p90 || boom < 0 || boom > 1 || mean <= 0) return no("Invalid candidate distribution.");
  const terms=snapshot.explanationTerms;
  const validTerms=terms?.length&&terms.every(t=>[t.input,t.center,t.scale,t.coefficient,t.points].every(finite)&&t.scale>0&&Math.abs((t.input-t.center)/t.scale*t.coefficient-t.points)<1e-8)&&Math.abs((p.baseline as number)+terms.reduce((s,t)=>s+t.points,0)-mean)<1e-8;
  return { projection: { mean, p10, p50, p90, boom, ...(validTerms?{explanationTerms:terms}:{}), ...(finite(p.prior_opportunity)?{priorOpportunity:p.prior_opportunity}:{}), baselineMean: p.baseline as number, baselineP10: p.p10 as number, baselineP90: p.p90 as number, snapshotId: snapshot.id, capturedAt: snapshot.capturedAt, kickoff: snapshot.kickoff, recipeDigest: policy.recipeDigest, studyDigest: release.studyDigest, releaseVersion: release.version }, reason: "Pinned pregame candidate · experimental" };
}
