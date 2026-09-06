import assert from 'node:assert/strict';
import fs from 'node:fs';
import { previewAbsence } from '../src/lib/nfl-dfs/absence-preview';
import { resolveGameAvailability, type RosterEvidence } from '../src/lib/nfl-dfs/availability';
import type { PlayerContext } from '../src/lib/nfl-dfs/player-context';
const original=JSON.parse(fs.readFileSync('src/data/nfl-player-context-2025.json','utf8')) as PlayerContext;
const stats={...original.rows.find(r=>r.stats)!.stats!};
for(const key of Object.keys(stats)) stats[key as keyof typeof stats]=0;
const data:PlayerContext={...original,players:[{id:'wr',name:'Receiver',position:'WR'},{id:'mate',name:'Teammate',position:'WR'}],games:{},rows:[]};
for(let week=1;week<=8;week++) {
 data.games[String(week)]={week,date:'2025-11-01',team:'NO',opponent:'BUF',plays:50,covered:50,roster:[{id:'mate',name:'Teammate',position:'WR',status:'ACT',recordedPlays:week<=4?30:0}]};
 data.rows.push({playerId:'wr',gameKey:String(week),stats:{...stats,receptions:week<=4?4:10},targets:week<=4?5:12,attempts:null});
}
const now=Date.parse('2026-09-13T16:00:00Z'),kickoff='2026-09-13T17:00:00Z';
const roster:RosterEvidence={team:'NO',position:'WR',fetchedAt:'2026-09-13T15:00:00Z',sleeper:{team:'NO',position:'WR',status:'Active',depth_chart_order:2},injuries:[]};
const official={id:'1',source:'nfl_official',status:'INACTIVE',practice:null,observedAt:'2026-09-13T15:30:00Z',updatedAt:'2026-09-13T15:20:00Z',team:'NO',week:1,hash:'source-hash',reportType:'inactive_list',kickoff,url:'https://www.nfl.com/news/inactives'};
const receiver={dkPlayerId:1,name:'Receiver',position:'WR',team:'NO',gameKey:'game',salary:5000,isOut:false,availability:resolveGameAvailability(roster,'NO','WR',now,1,kickoff)};
const teammate={...receiver,dkPlayerId:2,name:'Teammate',isOut:true,availability:resolveGameAvailability({...roster,injuries:[official]},'NO','WR',now,1,kickoff)};
const before=JSON.stringify({data,receiver,teammate});
const preview=previewAbsence(data,receiver,teammate,now);
assert.equal(preview.optimizerEnabled,false);
assert.ok(preview.estimate.available);
assert.ok(preview.estimate.scenario.mean>preview.estimate.baseline.mean);
assert.equal(preview.estimate.baseline.salaryHits![0].probability,.5,'inclusive 2x threshold must include 10 DK');
assert.equal(preview.estimate.scenario.salaryHits![1].probability,0);
assert.equal(preview.estimate.scenario.salaryHits![2].probability,0);
assert.equal(preview.teammate.availability!.evidence![0].hash,'source-hash');
assert.equal(JSON.stringify({data,receiver,teammate}),before,'no input mutation');
assert.deepEqual(previewAbsence(data,receiver,teammate,now),preview,'same evidence and clock reproduce preview');
for(const patch of [{source:'fantasypros'},{status:'QUESTIONABLE'},{week:2},{updatedAt:null},{kickoff:'2026-09-14T17:00:00Z'},{updatedAt:'2026-09-12T16:00:00Z'}]) {
 const unavailable={...teammate,availability:resolveGameAvailability({...roster,injuries:[{...official,...patch}]},'NO','WR',now,1,kickoff)};
 assert.throws(()=>previewAbsence(data,receiver,unavailable,now));
}
assert.throws(()=>previewAbsence(data,receiver,teammate,now+61000));
assert.throws(()=>previewAbsence(data,{...receiver,isOut:true},teammate,now));
assert.throws(()=>previewAbsence(data,receiver,{...teammate,team:'BUF'},now));
assert.throws(()=>previewAbsence(data,receiver,{...teammate,gameKey:'other'},now));
assert.throws(()=>previewAbsence(data,{...receiver,salary:NaN},teammate,now));
assert.throws(()=>previewAbsence({...data,season:2026},receiver,teammate,now));
assert.throws(()=>previewAbsence({...data,players:[...data.players,{...data.players[0],id:'duplicate'}]},receiver,teammate,now));
const moved=structuredClone(data);Object.values(moved.games).forEach(g=>g.team='BUF');
assert.equal(previewAbsence(moved,receiver,teammate,now).estimate.available,false,'new-team history is not copied');
const incomplete=structuredClone(data);Object.values(incomplete.games).forEach(g=>g.covered=49);
assert.equal(previewAbsence(incomplete,receiver,teammate,now).estimate.available,false);
console.log('Verified absence preview: evidence gates, identity, movement, conservation of empirical mass, salary thresholds and reproducibility passed');

const lower=structuredClone(data); for(const row of lower.rows) { row.stats!.receptions=Number(row.gameKey)<=4?10:2; row.targets=Number(row.gameKey)<=4?12:3; }
const downside=previewAbsence(lower,receiver,teammate,now).estimate;
assert.ok(downside.available); assert.ok(downside.scenario.mean<downside.baseline.mean,'absence may lower scoring, never force an uplift');
for(const range of [preview.estimate.baseline,preview.estimate.scenario]) { const hits=range.salaryHits!.map(h=>h.probability); assert.ok(hits.every(p=>p>=0&&p<=1)); assert.ok(hits[0]>=hits[1]&&hits[1]>=hits[2]); }
