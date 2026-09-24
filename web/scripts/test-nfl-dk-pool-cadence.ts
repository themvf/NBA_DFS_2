import assert from "node:assert/strict";
import { dkPoolDispatchDue } from "../src/lib/nfl-dfs/dk-pool-cadence";

const at = (iso: string) => dkPoolDispatchDue(new Date(iso));

// Thursday 2026-09-24: the slots GitHub skipped are all due.
for (const t of ["16:07", "16:37", "17:07", "17:37", "23:37"]) assert.ok(at(`2026-09-24T${t}:00Z`), `Thu ${t}`);
// The 8:15pm ET kickoff is 00:15 UTC FRIDAY. The old cron put this window on
// Thursday's 00:00-01:59, which is Wednesday evening in the US.
assert.ok(at("2026-09-25T00:07:00Z"), "the half-hour before a Thursday-night kickoff");
assert.ok(!at("2026-09-24T00:07:00Z"), "Thursday 00:07 UTC is Wednesday evening -- no game");
// Sunday through the late window, which is Monday UTC.
assert.ok(at("2026-09-27T12:07:00Z") && at("2026-09-27T20:37:00Z") && at("2026-09-28T01:37:00Z"));
// Monday night.
assert.ok(at("2026-09-28T16:07:00Z") && at("2026-09-29T00:37:00Z"));
// Quiet days: sparse, and only in the first half-hour of the chosen hours.
assert.ok(at("2026-09-26T06:07:00Z") && !at("2026-09-26T06:37:00Z") && !at("2026-09-26T07:07:00Z"));
assert.ok(at("2026-09-30T12:07:00Z") && at("2026-09-30T22:07:00Z") && !at("2026-09-30T15:07:00Z"));

// Budget: half-hour ticks for a whole week -> dispatches.
let due = 0;
for (let m = 0; m < 7 * 48; m += 1) {
  if (dkPoolDispatchDue(new Date(Date.UTC(2026, 8, 27, 0, 7 + 30 * m)))) due += 1;
}
assert.ok(due > 60 && due < 90, `about 70 dispatches a week, got ${due}`);
console.log(`DK pool cadence: game windows every 30 min, quiet days sparse, ${due} dispatches/week`);
