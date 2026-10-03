/** NFL projection rebuild cadence: exactly the registered slots, nothing else. */
import assert from "node:assert/strict";
import { nflProjectionDispatchDue, nextNflProjectionSlot } from "../src/lib/nfl-dfs/projection-cadence";

const at = (iso: string) => nflProjectionDispatchDue(new Date(iso));
// Saturday 2026-09-26: the 21:35 slot GitHub skipped fires; 21:05 does not.
assert.equal(at("2026-09-26T21:35:00Z"), true);
assert.equal(at("2026-09-26T21:05:00Z"), false);
assert.equal(at("2026-09-26T13:35:00Z"), true);
assert.equal(at("2026-09-26T16:05:00Z"), false, "the 16:05 pass is Sunday only");
// Sunday 2026-09-27: the two extra game-day passes.
assert.equal(at("2026-09-27T16:05:00Z"), true);
assert.equal(at("2026-09-27T19:05:00Z"), true);
assert.equal(at("2026-09-27T19:35:00Z"), false);
// Every other Vercel tick in the 13:05-21:35 window is skipped.
let fired = 0;
for (let h = 13; h <= 21; h += 1) for (const m of ["05", "35"]) if (at(`2026-09-30T${String(h).padStart(2, "0")}:${m}:00Z`)) fired += 1;
assert.equal(fired, 2, "a Wednesday dispatches twice");
fired = 0;
for (let h = 13; h <= 21; h += 1) for (const m of ["05", "35"]) if (at(`2026-09-27T${String(h).padStart(2, "0")}:${m}:00Z`)) fired += 1;
assert.equal(fired, 4, "a Sunday dispatches four times");
// The next slot: what the Slate Check quotes for a slate uploaded between runs.
const next = (iso: string) => nextNflProjectionSlot(new Date(iso)).toISOString();
// PIT@CLE 2026-10-01: uploaded after the 21:35 UTC pass; the next is Friday morning, after kickoff.
assert.equal(next("2026-10-01T22:00:00Z"), "2026-10-02T13:35:00.000Z");
assert.equal(next("2026-10-01T21:35:00Z"), "2026-10-02T13:35:00.000Z", "strictly after now");
assert.equal(next("2026-10-01T21:34:00Z"), "2026-10-01T21:35:00.000Z");
// Sunday 2026-10-04 picks up the extra game-day passes; Saturday night rolls to Sunday morning.
assert.equal(next("2026-10-04T14:00:00Z"), "2026-10-04T16:05:00.000Z");
assert.equal(next("2026-10-04T16:30:00Z"), "2026-10-04T19:05:00.000Z");
assert.equal(next("2026-10-03T23:00:00Z"), "2026-10-04T13:35:00.000Z");
console.log("NFL projection cadence: 2 passes a day, 4 on Sundays; next slot is strictly ahead.");
