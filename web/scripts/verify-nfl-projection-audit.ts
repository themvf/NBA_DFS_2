/** Read-only integrity check of a saved audit, including JSONB-stable fingerprint. */
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {readNflOptimizerAudit} from '../src/app/dfs/nfl/actions';
import {canonicalAuditJson} from '../src/lib/nfl-dfs/audit-json';
import type {ProjectionAudit} from '../src/lib/nfl-dfs/projection-audit';
const id=process.argv[2];
async function main(){
  const {run,lineups}=await readNflOptimizerAudit(id);
  assert.equal(run.optimizerVersion,'nfl-dfs-ilp-v5-audited-situations');
  const digest=createHash('sha256').update(canonicalAuditJson({settings:run.settings,inputSnapshot:run.inputSnapshot,optimizerVersion:run.optimizerVersion})).digest('hex');
  assert.equal(digest,run.inputDigest,'fingerprint must survive JSONB key reordering');
  const rows=run.inputSnapshot as {dkPlayerId:number;name:string;projectionAudit:ProjectionAudit}[];
  for(const p of rows){const a=p.projectionAudit;assert.ok(a);if(a.baseline!==null&&a.final!==null)assert.ok(Math.abs(a.baseline+a.steps.reduce((s,t)=>s+t.points,0)-a.final)<1e-8,`${p.name}: ledger reconciliation`);}
  assert.equal(lineups.length,run.generatedLineups);
  for(const l of lineups){const slots=l.slots as {dkPlayerId:number;projection:number;multiplier:number;source:string;projectionAudit:ProjectionAudit}[];
    assert.equal(new Set(slots.map(s=>s.dkPlayerId)).size,slots.length);
    for(const s of slots){const p=rows.find(p=>p.dkPlayerId===s.dkPlayerId)!;assert.ok(p);assert.equal(s.projection,p.projectionAudit.final!*s.multiplier);assert.equal(s.source,p.projectionAudit.source);assert.equal(s.projectionAudit.final,p.projectionAudit.final);}
    assert.ok(Math.abs(slots.reduce((s,p)=>s+p.projection,0)-l.projectedFpts)<1e-8);
  }
  console.log(JSON.stringify({runId:id,fingerprintVerified:true,playerLedgers:rows.length,savedLineups:lineups.length,slotEstimatesReconciled:true,situationChanges:rows.filter(p=>p.projectionAudit.steps.some(s=>s.calculation&&s.points!==0)).map(p=>({name:p.name,baseline:p.projectionAudit.baseline,final:p.projectionAudit.final}))},null,2));
}
main().catch(e=>{console.error(e instanceof Error?e.message:'Audit verification failed.');process.exitCode=1;});
