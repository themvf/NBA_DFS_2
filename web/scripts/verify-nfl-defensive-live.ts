/** Run only before kickoff with an explicitly selected saved salary upload. */
import { runNflOptimizer, loadSavedNflLineups, readNflOptimizerAudit } from '../src/app/dfs/nfl/actions';
import type { NflOptimizerSettings } from '../src/app/dfs/nfl/nfl-optimizer';

async function main() {
  const uploadId=process.argv[2];
  const profile=process.argv[3]==='allowed-rushing-volume'?'allowed-rushing-volume':'pfr-efficiency';
  const nLineups=Number(process.argv[4]??1);
  if(!uploadId)throw new Error('Supply a saved salary upload ID.');
  const settings:NflOptimizerSettings={format:'classic',mode:'gpp',projectionSource:'our',allowDkFallback:false,
    nLineups,minSalary:45000,maxExposure:1,minUnique:1,stackPassCatchers:1,bringBack:true,randomness:0,
    lockedPlayerIds:[],excludedPlayerIds:[],minExposureByPlayer:{},maxExposureByPlayer:{}};
  const off=await runNflOptimizer(uploadId,{...settings,defensiveAdjustments:{mode:'off',profile}});
  const experimental=await runNflOptimizer(uploadId,{...settings,defensiveAdjustments:{mode:'experimental',profile}});
  const saved=await loadSavedNflLineups(uploadId,experimental.runId);
  const audit=await readNflOptimizerAudit(experimental.runId);
  const selected=experimental.result.lineups[0];
  const applied=(audit.run.inputSnapshot as Array<{defensiveForecast?:{status:string;digest:string}}>)
    .filter(p=>p.defensiveForecast?.status==='applied');
  if(!selected||!saved.lineups[0]||!applied.length)throw new Error('Experimental run did not save an adjusted lineup.');
  if(off.result.lineups.length!==nLineups||experimental.result.lineups.length!==nLineups||saved.lineups.length!==nLineups)
    throw new Error(`Requested ${nLineups}; generated ${off.result.lineups.length} Off and ${experimental.result.lineups.length} Experimental.`);
  if(JSON.stringify(selected.playerIds)!==JSON.stringify(saved.lineups[0].playerIds))throw new Error('Saved roster changed on reload.');
  console.log(JSON.stringify({profile,nLineups,generated:experimental.result.lineups.length,offRun:off.runId,experimentalRun:experimental.runId,
    offRoster:off.result.lineups[0]?.playerIds,selectedRoster:selected.playerIds,
    appliedPlayers:applied.length,selectedAdjustedSlots:selected.slots.filter(s=>s.player.defensiveForecast?.status==='applied').length,
    savedDigest:audit.run.inputDigest,reloaded:JSON.stringify(selected.playerIds)===JSON.stringify(saved.lineups[0].playerIds),
    warnings:experimental.result.warnings},null,2));
}
main().catch(error=>{console.error(error);process.exitCode=1});
