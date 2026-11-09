import assert from "node:assert/strict";
import {nbaResearchCheckDue,nbaResearchReviewDue,dueJobs} from "../src/lib/cron-dispatch";
import {summarizePoolObservations} from "../src/lib/nba-pool-summary";
for (const day of ["2026-10-30", "2026-11-09"]) {
  const offset=day.includes("10-30")?4:5;
  assert(nbaResearchCheckDue(new Date(`${day}T${11+offset}:52:00Z`)));
  assert(nbaResearchCheckDue(new Date(`${day}T${12+offset}:22:00Z`)));
  assert(!nbaResearchCheckDue(new Date(`${day}T${12+offset}:37:00Z`)));
  assert(nbaResearchReviewDue(new Date(`${day}T${8+offset}:37:00Z`)));
}
assert(!nbaResearchCheckDue(new Date("2026-07-09T15:52:00Z")));
const jobs=dueJobs(new Date("2026-11-09T16:52:00Z"),{nflKickoffs:[]});
assert(jobs.some(j=>j.key==="nba-research-check" && j.inputs?.action==="check"));
const summary=summarizePoolObservations([
  {game_date:"2026-11-04",record_sha256:"one",provisional:false,pool_gap:0},
  {game_date:"2026-11-05",record_sha256:"two",provisional:true,pool_gap:8},
]);
assert.deepEqual(summary,{complete:{n:1,positive:0,mean:0},provisional:{n:1,positive:1,mean:8}});
assert.equal(summarizePoolObservations([]).complete.mean,null);
assert.throws(()=>summarizePoolObservations([
  {game_date:"same",record_sha256:"one",provisional:false,pool_gap:0},
  {game_date:"same",record_sha256:"two",provisional:false,pool_gap:1},
]));
console.log("PASS: Eastern pre-lock checks, morning review, exact unique cohorts and provisional separation");
