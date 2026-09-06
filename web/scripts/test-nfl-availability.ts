import assert from 'node:assert/strict';
import { resolveAvailability, type RosterEvidence } from '../src/lib/nfl-dfs/availability';
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
