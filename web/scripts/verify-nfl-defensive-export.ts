import assert from 'node:assert/strict';
import { exportSavedNflDefensiveEntries, loadSavedNflLineups } from '../src/app/dfs/nfl/actions';

async function main() {
  const [uploadId,runId]=process.argv.slice(2);
  if(!uploadId||!runId)throw new Error('Supply upload and saved run IDs.');
  const saved=await loadSavedNflLineups(uploadId,runId);
  const header='Entry ID,Contest Name,Contest ID,Entry Fee,QB,RB,RB,WR,WR,WR,TE,FLEX,DST';
  const template=`${header}\n${saved.lineups.map((_,i)=>`TEST-ENTRY-${i+1},Test contest,TEST-CONTEST,0,,,,,,,,,`).join('\n')}\n`;
  const csv=await exportSavedNflDefensiveEntries(runId,template);
  for(let i=0;i<saved.lineups.length;i++) {
    assert.ok(csv.includes(`TEST-ENTRY-${i+1}`));
    for(const slot of saved.lineups[i].slots)assert.ok(csv.includes(`${slot.player.name} (${slot.player.dkPlayerId})`));
  }
  console.log(JSON.stringify({runId,rows:csv.trim().split(/\r?\n/).length,
    entriesPreserved:saved.lineups.length,firstRosterIds:saved.lineups[0].playerIds},null,2));
}
main().catch(error=>{console.error(error);process.exitCode=1});
