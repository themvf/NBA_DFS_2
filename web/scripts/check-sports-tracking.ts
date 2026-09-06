// Read-only integration check; no schema initialization or ingestion.
import assert from "node:assert/strict";
import {getSportsTracking} from "../src/db/queries-sports-tracking";
import {trackingFilters,trackingTotals} from "../src/lib/sports-tracking";
async function main(){
 for(const sport of ["all","mlb","nfl","cfb","tennis"]){
  const f=trackingFilters({sport});const data=await getSportsTracking(f);const t=trackingTotals(data.groups);
  assert.equal(t.total,t.wins+t.losses+t.pushes+t.draws+t.voids+t.pending+t.unavailable);
  assert.ok(data.entries.length<=100);assert.ok(data.groups.every(g=>sport==="all"||g.sport===sport));
  console.log(JSON.stringify({sport,groups:data.groups.length,total:t.total,ledger:data.entries.length}));
  if(sport==="all"&&t.total>100){const next=await getSportsTracking({...f,page:2});assert.ok(next.entries.every(e=>!data.entries.some(first=>first.id===e.id)));}
 }
 const won=await getSportsTracking(trackingFilters({result:"won"}));assert.ok(won.entries.every(e=>e.result==="won"));
}
main().catch(()=>{console.error("Tracking read-only integration check failed");process.exitCode=1;});
