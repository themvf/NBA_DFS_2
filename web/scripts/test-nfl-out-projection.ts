// A DK-flagged OUT player must be STORED at zero, not only displayed at zero.
// Until 2026-09-22 the slate write copied the run's number and the read layer
// hid it; the stored row and every report card reading it were wrong.
import assert from "node:assert/strict";
import { OUT_PROJECTION_STATUS, storedSlateProjection, zeroOutProjection } from "../src/lib/nfl-dfs/out-projection";

const run = { projectionStatus: "historical", modelProjFpts: 14.2, floorFpts: 6.1, medianFpts: 13.0, ceilingFpts: 24.5, boomRate: 0.12 };

// Playing: the run's numbers pass through untouched.
assert.deepEqual(storedSlateProjection(run, false), {
  projectionStatus: "historical", ourProj: 14.2, floorFpts: 6.1, medianFpts: 13.0, ceilingFpts: 24.5, boomRate: 0.12,
});

// OUT: stored at zero with the status saying why, regardless of the run's number.
const out = storedSlateProjection(run, true);
assert.deepEqual(out, { projectionStatus: OUT_PROJECTION_STATUS, ourProj: 0, floorFpts: 0, medianFpts: 0, ceilingFpts: 0, boomRate: 0 });

// The write-time rule and the read-time rule agree on every shared field.
const readSide = zeroOutProjection({ projectionStatus: "historical", ourProj: 14.2, floorFpts: 6.1, ceilingFpts: 24.5, boomRate: 0.12 }, true);
for (const key of ["projectionStatus", "ourProj", "floorFpts", "ceilingFpts", "boomRate"] as const) {
  assert.equal(out[key], readSide[key], key);
}

// No projection is absence, never zero.
assert.deepEqual(storedSlateProjection(null, false), {
  projectionStatus: "unmatched", ourProj: null, floorFpts: null, medianFpts: null, ceilingFpts: null, boomRate: null,
});
// ...but an OUT player with no projection is still stored at zero: DK pays him zero either way.
assert.equal(storedSlateProjection(null, true).ourProj, 0);

console.log("test-nfl-out-projection: 5 assertions passed");
