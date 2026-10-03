import { writeFileSync } from 'node:fs';
import { loadSavedNflWorkspace } from '../src/app/dfs/nfl/actions';
import { simulateAirMatchupOpportunity } from '../src/lib/nfl-dfs/air-matchup-shadow-sim';

async function main(){
const uploadId=process.argv[2];
const output=process.argv[3];
if(!uploadId || !output) throw new Error('Usage: simulate-nfl-air-matchup <upload-id> <output-json>');
const {slate}=await loadSavedNflWorkspace(uploadId);
const receivers=slate.players.filter(p=>p.position==='WR'||p.position==='TE');
const ready=receivers.filter(p=>p.airMatchupEvidence?.state==='ready');
const rows=ready.map((p,i)=>{const evidence=p.airMatchupEvidence!;const simulations=simulateAirMatchupOpportunity(evidence,20261004+i,10000);return {
  name:p.name,team:p.team,opponent:p.opponent,position:p.position,airMatchupChip:p.playerSignals?.some(s=>s.code==='AIR_MATCHUP')??false,
  evidence,simulations,neutralMeanDelta:simulations.neutral.matchup.mean-simulations.neutral.neutral.mean,
};}).sort((a,b)=>b.neutralMeanDelta-a.neutralMeanDelta);
const report={version:'nfl-air-matchup-shadow-v1',generatedAt:new Date().toISOString(),uploadId,projectionRunId:slate.projectionRunId,
  modelAsOf:slate.modelAsOf,format:slate.format,coverage:{receivers:receivers.length,ready:ready.length,unavailable:receivers.length-ready.length,
    reasons:receivers.filter(p=>p.airMatchupEvidence?.state!=='ready').reduce((acc,p)=>{const reason=p.airMatchupEvidence?.reason??'No evidence';acc[reason]=(acc[reason]??0)+1;return acc;},{} as Record<string,number>)},
  method:'10,000 paired draws per player and script. Team attempts have 15% normal relative dispersion; targets are binomial; per-target depth is lognormal with 0.3 log standard deviation. Leading/trailing multiply attempts by 0.9/1.1. Matchup multiplies the same draw by the regressed defense factor. These are assumed sensitivities, not fitted forecasts or coherent game/lineup outcomes.',
  limitations:['Participant rows have no as-of timestamp; historical reconstruction can include later corrections.','No catches, YAC, touchdowns, fantasy points, ownership, field, ROI, or lineup dependence.','Market quote is descriptive and does not alter script probabilities.'],rows};
writeFileSync(output,JSON.stringify(report,null,2));
console.log(JSON.stringify({output,coverage:report.coverage,top:rows.slice(0,10).map(r=>({name:r.name,team:r.team,opponent:r.opponent,chip:r.airMatchupChip,factor:r.evidence.matchupFactor,neutralMeanDelta:r.neutralMeanDelta,over100Delta:r.simulations.neutral.matchup.over100-r.simulations.neutral.neutral.over100}))},null,2));
}
main().catch(error=>{console.error(error);process.exitCode=1;});
