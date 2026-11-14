import assert from "node:assert/strict";
import { INJURY_READ_GRACE_MIN, missedInjuryRead, nbaInjuryReportIndexUrl } from "../src/lib/nba-injury-read-watch";

// Winter (Eastern = UTC-5). A weeknight: tips 7:10, 7:40 and 10:10 PM Eastern on 2026-11-13.
const at = (s: string) => new Date(s);
const tips = [at("2026-11-14T00:10:00Z"), at("2026-11-14T00:40:00Z"), at("2026-11-14T03:10:00Z")];
const MORNING = at("2026-11-13T16:37:30Z");          // the 11:37 AM Eastern freeze read

// 5:00 PM Eastern: no read has been due yet, so the morning report is not a missed read.
assert.equal(missedInjuryRead(at("2026-11-13T22:00:00Z"), tips, MORNING), null);
assert.equal(missedInjuryRead(at("2026-11-13T22:00:00Z"), tips, null), null);

// The first tip-timed read is the 6:22 PM tick (48 minutes before the 7:10 tip). Inside the grace
// it is not yet called missed; after it, with only the morning report on hand, it is.
assert.equal(missedInjuryRead(at("2026-11-13T23:30:00Z"), tips, MORNING), null, "8 minutes after the tick is inside the grace");
assert.equal(INJURY_READ_GRACE_MIN, 10);
assert.deepEqual(missedInjuryRead(at("2026-11-13T23:33:00Z"), tips, MORNING), { dueAt: "2026-11-13T23:22:00.000Z", lastReadAt: MORNING.toISOString() });
assert.deepEqual(missedInjuryRead(at("2026-11-13T23:33:00Z"), tips, null), { dueAt: "2026-11-13T23:22:00.000Z", lastReadAt: null });

// The read arrived: no warning. A read one tick old still counts (the 6:22 read at 6:50 PM,
// when the 6:37 read is the newest due one).
assert.equal(missedInjuryRead(at("2026-11-13T23:33:00Z"), tips, at("2026-11-13T23:23:10Z")), null);
assert.equal(missedInjuryRead(at("2026-11-13T23:50:00Z"), tips, at("2026-11-13T23:23:10Z")), null);
// Two missed in a row: at 7:05 PM the 6:52 read is the newest due, and a 6:23 read is too old.
assert.deepEqual(missedInjuryRead(at("2026-11-14T00:05:00Z"), tips, at("2026-11-13T23:23:10Z")),
  { dueAt: "2026-11-13T23:52:00.000Z", lastReadAt: "2026-11-13T23:23:10.000Z" });

// The 7:40 PM tip gets one read, at the 7:22 PM tick. A late-games-only contest: the one read
// before the 10:10 PM tip is the 9:52 PM tick. At 9:30 PM the 7:22 read is two hours old and
// nothing newer was due, so there is no warning; had 7:22 failed, there still is one.
const EVENING = at("2026-11-14T00:23:00Z");
assert.equal(missedInjuryRead(at("2026-11-14T02:30:00Z"), tips, EVENING), null);
assert.deepEqual(missedInjuryRead(at("2026-11-14T02:30:00Z"), tips, at("2026-11-13T23:53:00Z")),
  { dueAt: "2026-11-14T00:22:00.000Z", lastReadAt: "2026-11-13T23:53:00.000Z" });
assert.deepEqual(missedInjuryRead(at("2026-11-14T03:04:00Z"), tips, EVENING), { dueAt: "2026-11-14T02:52:00.000Z", lastReadAt: EVENING.toISOString() });
assert.equal(missedInjuryRead(at("2026-11-14T03:04:00Z"), tips, at("2026-11-14T02:53:00Z")), null);

// An afternoon slate (first tip 3:30 PM Eastern): reads are due at 2:52, 3:07 and 3:22 PM.
const sunday = [at("2026-11-08T20:30:00Z"), at("2026-11-09T00:00:00Z")];
assert.deepEqual(missedInjuryRead(at("2026-11-08T20:05:00Z"), sunday, at("2026-11-08T16:37:20Z")),
  { dueAt: "2026-11-08T19:52:00.000Z", lastReadAt: "2026-11-08T16:37:20.000Z" });
// The fixed 6:37 and 6:52 PM Eastern reads are due whatever the tips are.
assert.deepEqual(missedInjuryRead(at("2026-11-08T23:50:00Z"), [], null), { dueAt: "2026-11-08T23:37:00.000Z", lastReadAt: null });

// Off-season: nothing is due.
assert.equal(missedInjuryRead(at("2026-08-13T23:50:00Z"), [], null), null);

assert.equal(nbaInjuryReportIndexUrl("2026-11-13"), "https://official.nba.com/nba-injury-report-2026-27-season/");
assert.equal(nbaInjuryReportIndexUrl("2027-03-02"), "https://official.nba.com/nba-injury-report-2026-27-season/");

console.log("nba injury read watch: ok");
