import assert from 'node:assert/strict';
import { presentPinnedGameAvailability, resolveAvailability, type RosterEvidence } from '../src/lib/nfl-dfs/availability';
const now = Date.parse('2026-09-06T12:00:00Z');
const e: RosterEvidence = {team:'NO',position:'QB',fetchedAt:'2026-09-06T10:00:00Z',sleeper:{team:'NO',position:'QB',status:'Active',depth_chart_order:1}};
assert.equal(resolveAvailability(e,'NO','QB',now).role,'Expected starter · QB1');
assert.equal(resolveAvailability(e,'NO','QB',now).blockedReason,null);
for (const status of ['Out','IR','PUP','NFI','Suspended','Inactive']) assert.ok(resolveAvailability({...e,sleeper:{...(e.sleeper as object),injury_status:status}},'NO','QB',now).blockedReason);
assert.match(resolveAvailability({...e,sleeper:{...(e.sleeper as object),depth_chart_order:2}},'NO','QB',now).blockedReason!,/QB2/);
assert.equal(resolveAvailability({...e,sleeper:{...(e.sleeper as object),injury_status:'Questionable'}},'NO','QB',now).blockedReason,null);
for (const bad of [undefined,{...e,team:'BUF'},{...e,fetchedAt:'2025-01-01'},{...e,fetchedAt:'2027-01-01'},{...e,sleeper:{team:'BUF',position:'QB'}}]) {
 assert.equal(resolveAvailability(bad,'NO','QB',now).fresh,false);
 assert.equal(resolveAvailability(bad,'NO','QB',now).blockedReason,null);
}
assert.equal(resolveAvailability({...e,sleeper:{team:'NO',position:'QB',status:'Active'}},'NO','QB',now).role,'QB role unresolved');
console.log('NFL availability: starter, backup, unavailable, questionable, stale, future and identity checks passed');
for (const [alias, canonical] of [['WAS','WSH'],['LA','LAR'],['AZ','ARI'],['JAC','JAX']]) {
 const aliased = {...e,team:canonical,sleeper:{team:alias,position:'QB',status:'Active',injury_status:'Out'}};
 assert.equal(resolveAvailability(aliased,canonical,'QB',now).blockedReason,'Unavailable: OUT');
 assert.equal(resolveAvailability(aliased,'BUF','QB',now).fresh,false);
}
assert.equal(resolveAvailability({...e,sleeper:{team:'NO',position:'QB',injury_status:'Questionable',status:'IR'}},'NO','QB',now).blockedReason,'Unavailable: IR');
const pinned=presentPinnedGameAvailability({version:'player-game-availability-v1',state:'OUT_CONFIRMED',projection_status:'OUT',source:'sleeper',
 observation_id:10,source_snapshot_id:20,available_at:'2026-09-06T10:00:00Z',as_of_at:'2026-09-06T12:00:00Z',
 kickoff:'2026-09-06T17:00:00Z',reason:'Qualified status.',qualifying_observation_ids:[10],display_only_observation_ids:[11]},'Expected starter · QB1');
assert.equal(pinned.pinned,true);assert.equal(pinned.status,'OUT');assert.match(pinned.blockedReason!,/OUT/);
assert.equal(pinned.decisionId,'player-game-availability-v1:20:10:2026-09-06T12:00:00Z');
// A pinned health decision must not drop the depth-chart block on a backup QB (live on every pinned run 2026-09-26..28).
const active={version:'player-game-availability-v1',state:'EXPECTED_ACTIVE',projection_status:'EXPECTED_ACTIVE',source:'sleeper',
 observation_id:12,source_snapshot_id:21,available_at:'2026-09-06T10:00:00Z',as_of_at:'2026-09-06T12:00:00Z',
 kickoff:'2026-09-06T17:00:00Z',reason:'Qualified status.',qualifying_observation_ids:[12],display_only_observation_ids:[]};
const backup=resolveAvailability({...e,sleeper:{team:'NO',position:'QB',status:'Active',depth_chart_order:2}},'NO','QB',now);
assert.match(backup.roleBlockedReason!,/Listed QB2/);
assert.match(presentPinnedGameAvailability(active,backup.role,backup.roleBlockedReason).blockedReason!,/Listed QB2/,'pinned backup stays blocked');
assert.equal(presentPinnedGameAvailability(active,'Expected starter · QB1',null).blockedReason,null,'pinned starter stays eligible');
