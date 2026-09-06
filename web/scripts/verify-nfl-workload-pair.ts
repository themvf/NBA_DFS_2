/** Read-only verification of two saved optimizer runs; never generates contest entries. */
import assert from 'node:assert/strict';
import {sql} from 'drizzle-orm';
import {db} from '../src/db';
const [baselineId,candidateId]=process.argv.slice(2);
if(![baselineId,candidateId].every(x=>/^[0-9a-f-]{36}$/.test(x??'')))throw new Error('Provide baseline and workload run UUIDs.');
async function main(){
  const rows=(await db.execute(sql`SELECT * FROM nfl_dfs_optimizer_runs WHERE run_id IN (${baselineId}::uuid,${candidateId}::uuid)`)).rows;
  const baseline=rows.find(r=>r.run_id===baselineId)!,candidate=rows.find(r=>r.run_id===candidateId)!;
  assert.ok(baseline&&candidate);assert.equal(baseline.projection_source,'our');assert.equal(candidate.projection_source,'workload');
  assert.equal(baseline.upload_id,candidate.upload_id);assert.equal(baseline.optimizer_version,candidate.optimizer_version);
  assert.deepEqual(baseline.input_snapshot,candidate.input_snapshot,'both runs must freeze the identical cohort');
  const bs={...(baseline.settings as Record<string,unknown>)},cs={...(candidate.settings as Record<string,unknown>)};delete bs.projectionSource;delete cs.projectionSource;assert.deepEqual(bs,cs);assert.equal(bs.randomness,0);
  type Slot={slot:string;dkPlayerId:number;source:string;salary:number};
  const lineups=(await db.execute(sql`SELECT run_id,lineup_number,slots,total_salary FROM nfl_dfs_lineups WHERE run_id IN (${baselineId}::uuid,${candidateId}::uuid) ORDER BY lineup_number`)).rows;
  const a=lineups.filter(l=>l.run_id===baselineId),b=lineups.filter(l=>l.run_id===candidateId);
  assert.equal(a.length,baseline.generated_lineups);assert.equal(b.length,candidate.generated_lineups);assert.equal(a.length,b.length);assert.ok(a.length>0);
  const snapshot=candidate.input_snapshot as {dkPlayerId:number;position:string;availability:{role:string};workload:unknown}[];
  const size=bs.format==='classic'?9:6;
  for(const l of lineups){const slots=l.slots as Slot[];assert.equal(slots.length,size);assert.equal(new Set(slots.map(s=>s.dkPlayerId)).size,size);assert.ok(Number(l.total_salary)<=50000&&Number(l.total_salary)>=Number(bs.minSalary));for(const slot of slots){const p=snapshot.find(p=>p.dkPlayerId===slot.dkPlayerId)!;assert.ok(p);if(p.position==='QB')assert.equal(p.availability.role,'Expected starter · QB1');if(slot.source==='workload')assert.ok(p.workload);}}
  const key=(slots:Slot[])=>slots.map(s=>`${s.slot==='CPT'?'CPT':'FLEX'}:${s.dkPlayerId}`).sort().join('|');
  const direct=b.reduce((n,l)=>n+(l.slots as Slot[]).filter(s=>s.source==='workload').length,0);assert.ok(direct>0,'saved candidate must actually select workload players');
  console.log(JSON.stringify({baselineId,candidateId,identicalFrozenInputs:true,identicalControls:true,baselineLineups:a.length,workloadLineups:b.length,changedLineups:b.filter((l,i)=>key(l.slots as Slot[])!==key(a[i].slots as Slot[])).length,workloadRosterSlots:direct,legalAndStarterVerified:true,performance:'Pending actual results'},null,2));
}
main().catch(()=>{console.error('Saved workload pair failed verification.');process.exitCode=1;});
