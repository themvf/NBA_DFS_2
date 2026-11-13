import assert from "node:assert/strict";
import {dueJobs,nbaGhostFreezeDue,nbaGhostGradeDue,nbaInjuryRefreshDue,nbaTipPickupDue} from "../src/lib/cron-dispatch";
for(const [yes,no] of [["2026-10-31T15:37:00Z","2026-10-31T16:37:00Z"],["2026-11-02T16:37:00Z","2026-11-02T15:37:00Z"]]){assert(nbaGhostFreezeDue(new Date(yes)));assert(!nbaGhostFreezeDue(new Date(no)));}
assert(nbaGhostGradeDue(new Date("2026-11-02T12:37:00Z")));assert(nbaInjuryRefreshDue(new Date("2026-11-02T23:52:00Z")));assert(!nbaInjuryRefreshDue(new Date("2026-11-02T22:52:00Z")));assert(!nbaGhostFreezeDue(new Date("2026-07-02T15:37:00Z")));
// Tip-timed injury report reads: every tick in the 50 minutes before the night's first tip, none inside 5 minutes of it.
const at=(s:string)=>new Date(s), night=[at("2026-11-14T00:10:00Z"),at("2026-11-14T00:40:00Z"),at("2026-11-14T03:10:00Z")];   // 7:10, 7:40 and 10:10 PM ET
const reads=(tips:Date[]|null,from:string,to:string)=>{const out:string[]=[];for(let t=at(from).getTime();t<=at(to).getTime();t+=15*60_000){const now=new Date(t);if(nbaTipPickupDue(now,tips))out.push(now.toISOString().slice(11,16));}return out;};
assert.deepEqual(reads(night,"2026-11-13T22:07:00Z","2026-11-14T03:22:00Z"),["23:22","23:37","23:52","00:22","02:52"]);
// 23:22-23:52 are the three reads before the 7:10 lock (48, 33 and 18 minutes out); 00:07 is 3 minutes before it and skipped;
// 00:22 is 18 minutes before the 7:40 tip; 02:52 is 18 minutes before the 10:10 tip. A tick 35 minutes before a LATER tip is not read.
assert(!nbaTipPickupDue(at("2026-11-14T02:37:00Z"),night));
// A tip on the hour: 6:22, 6:37 and 6:52 PM ET (38, 23 and 8 minutes out).
assert.deepEqual(reads([at("2026-11-14T00:00:00Z")],"2026-11-13T22:07:00Z","2026-11-14T00:22:00Z"),["23:22","23:37","23:52"]);
// A weekend afternoon slate gets its reads too, which the fixed evening ticks never gave it.
assert.deepEqual(reads([at("2026-11-14T20:40:00Z"),at("2026-11-15T00:10:00Z")],"2026-11-14T19:07:00Z","2026-11-14T20:52:00Z"),["19:52","20:07","20:22"]);
assert(nbaInjuryRefreshDue(at("2026-11-14T20:07:00Z"),{nflKickoffs:null,nbaTips:[at("2026-11-14T20:40:00Z")]}));
// No tips, or tips that could not be read: only the two fixed evening reads remain.
assert.deepEqual(reads([],"2026-11-13T22:07:00Z","2026-11-14T03:22:00Z"),[]);assert.deepEqual(reads(null,"2026-11-13T22:07:00Z","2026-11-14T03:22:00Z"),[]);
assert(nbaInjuryRefreshDue(at("2026-11-13T23:52:00Z"),{nflKickoffs:null,nbaTips:null}));assert(!nbaInjuryRefreshDue(at("2026-11-13T23:22:00Z"),{nflKickoffs:null,nbaTips:null}));
// Off-season: nothing, whatever the context says.
assert(!nbaInjuryRefreshDue(at("2026-08-14T00:00:00Z"),{nflKickoffs:null,nbaTips:[at("2026-08-14T00:30:00Z")]}));
// The dispatcher fires the one job once on a tick where both rules are due, with the availability action.
const fired=dueJobs(at("2026-11-13T23:52:00Z"),{nflKickoffs:[],nbaTips:night}).filter(j=>j.key==="nba-injury-refresh");
assert.equal(fired.length,1);assert.equal(fired[0].inputs?.action,"availability");
assert(dueJobs(at("2026-11-13T23:22:00Z"),{nflKickoffs:[],nbaTips:night}).some(j=>j.key==="nba-injury-refresh"));
assert(!dueJobs(at("2026-11-13T23:22:00Z"),{nflKickoffs:[]}).some(j=>j.key==="nba-injury-refresh"));
console.log("PASS: Eastern freeze/grade, the fixed and tip-timed injury report reads across DST and off-season");
