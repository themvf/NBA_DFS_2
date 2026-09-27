import assert from 'node:assert/strict';
import { runNflOptimizer, exportSavedNflDefensiveEntries } from '../src/app/dfs/nfl/actions';
import type { NflOptimizerSettings } from '../src/app/dfs/nfl/nfl-optimizer';

async function main() {
  const [uploadId,runId]=process.argv.slice(2);
  if(!uploadId||!runId)throw new Error('Supply upload and saved run IDs.');
  const settings:NflOptimizerSettings={format:'classic',mode:'gpp',projectionSource:'our',allowDkFallback:false,
    defensiveAdjustments:{mode:'experimental',profile:'pfr-efficiency'},nLineups:1,minSalary:45000,
    maxExposure:1,minUnique:1,stackPassCatchers:1,bringBack:true,randomness:0,
    lockedPlayerIds:[],excludedPlayerIds:[],minExposureByPlayer:{},maxExposureByPlayer:{}};
  await assert.rejects(runNflOptimizer(uploadId,settings),/slate has started/i);
  await assert.rejects(exportSavedNflDefensiveEntries(runId,'Entry ID,QB,RB,RB,WR,WR,WR,TE,FLEX,DST\nE1,,,,,,,,,'),/slate has started/i);
  console.log('Started slate rejects new adjusted generation and saved entry rewrites.');
}
main().catch(error=>{console.error(error);process.exitCode=1});
