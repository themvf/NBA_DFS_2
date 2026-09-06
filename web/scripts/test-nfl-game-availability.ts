import assert from 'node:assert/strict';
import { resolveGameAvailability, type RosterEvidence, type InjuryEvidence } from '../src/lib/nfl-dfs/availability';
const now=Date.parse('2026-09-13T16:00:00Z'), kickoff='2026-09-13T17:00:00Z';
const injury: InjuryEvidence={id:'1',source:'fantasypros',status:'OUT',practice:'DNP',observedAt:'2026-09-13T15:00:00Z',updatedAt:'2026-09-13T14:30:00Z',team:'NO',week:1,hash:'hash'};
const roster: RosterEvidence={team:'NO',position:'WR',fetchedAt:'2026-09-13T15:00:00Z',sleeper:{team:'NO',position:'WR',status:'Active',depth_chart_order:2},injuries:[injury]};
const resolve=(r: RosterEvidence)=>resolveGameAvailability(r,'NO','WR',now,1,kickoff);
assert.match(resolve(roster).blockedReason!,/FantasyPros/);
assert.ok(resolve(roster).warnings!.some(w=>w.includes('Sources differ')));
assert.equal(resolve(roster).officialConfirmed,false);
for(const patch of [{week:0},{team:'BUF'},{observedAt:'2026-09-13T18:00:00Z'},{updatedAt:'2026-09-12T15:00:00Z'},{observedAt:'2026-09-12T15:00:00Z'}]) {
 const result=resolve({...roster,injuries:[{...injury,...patch}]});
 assert.equal(result.blockedReason,null);
 assert.ok(result.warnings!.some(w=>w.includes('No fresh')));
}
assert.equal(resolve({...roster,injuries:[{...injury,status:'QUESTIONABLE'}]}).blockedReason,null);
assert.equal(resolve({...roster,sleeper:{team:'NO',position:'WR',status:'IR'},injuries:[{...injury,status:'HEALTHY'}]}).blockedReason,'Unavailable: IR');
assert.ok(resolve({...roster,injuries:[],injuryReadFailed:true}).warnings!.some(w=>w.includes('could not')));
assert.ok(resolveGameAvailability(roster,'NO','WR',now,1,null).warnings!.some(w=>w.includes('Kickoff unresolved')));
const frozen=JSON.stringify(resolve(roster)); roster.injuries=[]; assert.match(frozen,/hash/);
console.log('Game-week availability: freshness, identity, conflicts, exclusions and frozen provenance passed');
const official={...injury,source:'nfl_official',status:'INACTIVE',reportType:'inactive_list',kickoff,url:'https://www.nfl.com/news/inactives'};
assert.equal(resolve({...roster,injuries:[official]}).officialConfirmed,true);
assert.match(resolve({...roster,injuries:[official]}).blockedReason!,/Official list/);
assert.equal(resolve({...roster,injuries:[{...official,kickoff:'2026-09-14T17:00:00Z'}]}).officialConfirmed,false);
assert.equal(resolve({...roster,injuries:[{...official,status:'ACTIVE'}]}).blockedReason,null);
assert.equal(resolve({...roster,injuries:[{...injury,updatedAt:null}]}).blockedReason,null);
assert.equal(resolve({...roster,injuries:[{...injury,updatedAt:null}]}).freshFantasyPros,false);
