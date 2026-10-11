import assert from 'node:assert/strict';
import { applyLatestEligibility } from '../src/lib/nfl-dfs/latest-eligibility';
import type { PinnedGameAvailabilityDecision } from '../src/lib/nfl-dfs/availability';
const player={ffPlayerId:1,team:'NO',gameInfo:'ATL@NO 10/05/2026 08:15PM ET',isOut:false,
  ourProj:8,floorFpts:0,medianFpts:4,ceilingFpts:20,boomRate:.2};
const decision:PinnedGameAvailabilityDecision={version:'test-resolver',state:'OUT_CONFIRMED',projection_status:'out',source:'nfl_official',
  observation_id:1,source_snapshot_id:1,available_at:'2026-10-05T23:00:00Z',as_of_at:'2026-10-05T23:05:00Z',
  kickoff:'2026-10-06T00:15:00Z',reason:'Official inactive',qualifying_observation_ids:[1],display_only_observation_ids:[]};
const row={playerId:1,team:'NO',decision};
const clock={baselineAt:'2026-10-05T21:42:00Z',reviewRunId:'new',reviewAt:decision.as_of_at,now:Date.parse('2026-10-05T23:10:00Z')};
const result=applyLatestEligibility([player],[row],clock);
assert.equal(result[0].isOut,true);
assert.equal(result[0].ourProj,0);
assert.equal(result[0].ceilingFpts,0);
assert.equal(result[0].latestEligibilityReview?.runId,'new');
assert.equal(player.ourProj,8,'Original forecast object stays unchanged');
for(const bad of [{...row,team:'ATL'},{...row,playerId:2},
  {...row,decision:{...decision,available_at:'2026-10-05T23:06:00Z'}},
  {...row,decision:{...decision,as_of_at:'invalid'}},
  {...row,decision:{...decision,kickoff:'2026-10-06T01:15:00Z'}},
  {...row,decision:{...decision,qualifying_observation_ids:[]}},
  {...row,decision:{...decision,source_snapshot_id:null}},
  {...row,decision:{...decision,available_at:'2026-10-01T23:00:00Z'}}]) {
  assert.equal(applyLatestEligibility([player],[bad],clock)[0].isOut,false);
}
assert.equal(applyLatestEligibility([player],[row],{...clock,now:Date.parse(decision.kickoff!)})[0].isOut,false,'Archives do not get current-health overlays');
assert.equal(applyLatestEligibility([player],[row],{...clock,reviewAt:clock.baselineAt})[0].isOut,false);
assert.throws(()=>applyLatestEligibility([player],[row,row],clock),/Duplicate/);
assert.equal(applyLatestEligibility([player],[row],{...clock,now:NaN})[0].isOut,false);
const q=applyLatestEligibility([player],[{...row,decision:{...decision,state:'QUESTIONABLE',projection_status:null}}],clock)[0];
assert.equal(q.isOut,false);assert.equal(q.ourProj,8);
assert.equal(q.availability?.status,'QUESTIONABLE');
const alreadyOut={...player,isOut:true};
assert.equal(applyLatestEligibility([alreadyOut],[{...row,decision:{...decision,state:'EXPECTED_ACTIVE'}}],clock)[0].isOut,true);
console.log('NFL latest eligibility: frozen late OUT/Q decisions, exact game/team/time/provenance, no clearance, no projection artifact rewrite and archive boundaries passed.');
