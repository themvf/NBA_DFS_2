import assert from "node:assert/strict";
import {
  zonedWallClockToUtcMs,
  getMlbSnapshotAtTime,
  MLB_SNAPSHOT_TIMES_ET,
} from "../src/lib/mlb-movement-snapshot";

// Regression test for the 2026-08-06 bug: a Client Component ("use client")
// runs in the VIEWER's browser timezone, not a fixed one. The original
// implementation assumed the ambient timezone was UTC and manually added a
// +4h EDT offset on top of an ambiguous `new Date(string)` parse - which
// silently double-shifted every checkpoint by 4 hours for any Eastern-based
// viewer (the "10am" column was actually matching captures near 2pm).
//
// zonedWallClockToUtcMs must give the SAME correct answer no matter what
// process.env.TZ happens to be when this script runs - that's the whole
// point of the fix, so we don't need multiple TZ subprocesses to prove it;
// a single run is either ambient-independent or it isn't.
console.log(`(running under TZ=${process.env.TZ ?? "<unset>"})`);

// 2026-08-06 is during EDT (UTC-4): 1:10 PM ET must resolve to 17:10 UTC.
const ms = zonedWallClockToUtcMs("2026-08-06", 13, 10, "America/New_York");
assert.equal(new Date(ms).toISOString(), "2026-08-06T17:10:00.000Z");

// A January date is during EST (UTC-5): 1:10 PM ET must resolve to 18:10 UTC.
const msWinter = zonedWallClockToUtcMs("2026-01-06", 13, 10, "America/New_York");
assert.equal(new Date(msWinter).toISOString(), "2026-01-06T18:10:00.000Z");

// getMlbSnapshotAtTime: a capture at 17:07 UTC (1:07 PM EDT) is within ±20min
// of the "1:10p" (13:10 ET) checkpoint and must be picked up.
const trail = [
  { capturedAt: "2026-08-06T14:07:00.000Z", homeProb: 0.55 }, // 10:07 AM ET
  { capturedAt: "2026-08-06T17:07:00.000Z", homeProb: 0.58 }, // 1:07 PM ET
  { capturedAt: "2026-08-06T21:07:00.000Z", homeProb: 0.60 }, // 5:07 PM ET (not near any checkpoint below)
];
const tenAm = MLB_SNAPSHOT_TIMES_ET.find((t) => t.label === "10am")!;
const onePTen = MLB_SNAPSHOT_TIMES_ET.find((t) => t.label === "1:10p")!;
assert.equal(getMlbSnapshotAtTime(trail, "2026-08-06", tenAm.hour, tenAm.minute), 0.55);
assert.equal(getMlbSnapshotAtTime(trail, "2026-08-06", onePTen.hour, onePTen.minute), 0.58);

// Nothing captured near 6:20p (18:20 ET) in this trail -> null, not a wrong match.
const sixTwenty = MLB_SNAPSHOT_TIMES_ET.find((t) => t.label === "6:20p")!;
assert.equal(getMlbSnapshotAtTime(trail, "2026-08-06", sixTwenty.hour, sixTwenty.minute), null);

console.log("test-mlb-movement-snapshot: all assertions passed");
