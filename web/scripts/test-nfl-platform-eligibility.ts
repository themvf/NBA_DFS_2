import assert from 'node:assert/strict';
import {buildDraftKingsEligibilityManifest} from '../src/lib/nfl-dfs/platform-eligibility';

const input={slateId:'slate-1',decisionAt:'2026-09-25T16:00:00.000Z',fileDigest:'file-a',players:[
  {dkPlayerId:2,status:null,isOut:false},{dkPlayerId:1,status:'OUT',isOut:true},
]};
const one=buildDraftKingsEligibilityManifest(input);
const reordered=buildDraftKingsEligibilityManifest({...input,players:[...input.players].reverse()});
assert.equal(one.digest,reordered.digest,'manifest identity is independent of input row order');
assert.deepEqual(one.decisions.map(d=>[d.sourceRecordId,d.state]),[[1,'INELIGIBLE'],[2,'ELIGIBLE']]);
assert(one.decisions.every(d=>d.platform==='draftkings'&&d.slateId==='slate-1'&&d.manifestDigest===one.digest));
assert.notEqual(buildDraftKingsEligibilityManifest({...input,decisionAt:'2026-09-25T16:01:00.000Z'}).digest,one.digest);
assert.notEqual(buildDraftKingsEligibilityManifest({...input,fileDigest:'file-b'}).digest,one.digest);
console.log('NFL platform eligibility: slate scope, immutable evidence, order-independent digest and OUT separation passed');
