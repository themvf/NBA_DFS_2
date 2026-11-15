import assert from "node:assert/strict";
import { createPatternWatchSession, patternWatchSessionValid } from "../src/lib/cfb-pattern-watch-access";
import { cfbDay, nbaDay, nflDay, nflSeason, shiftDate, studyDate } from "../src/lib/daily-market-studies";

const today = "2026-11-14";
assert.equal(studyDate("2026-02-31", today), today);
assert.equal(studyDate("2026-11-15", today), today);
assert.equal(studyDate(["2026-11-08"], today), today);
assert.equal(studyDate("2026-11-08", today), "2026-11-08");
assert.equal(shiftDate("2026-11-01", -1), "2026-10-31");
assert.equal(nflSeason("2027-01-03"), 2026);

assert.equal(nbaDay([]).totals.games, 0);
assert.equal(nbaDay([]).paper.pending, 0);
assert.equal(nflDay([]).over.wins, 0);
assert.equal(nflDay([]).moneyline, 0);
const row = { Date: today, Game: "Away at Home", Pattern: "T4", Pick: "Over", Price: "-110", Final: "30-21" };
assert.deepEqual(cfbDay([
  { ...row, Result: "won", Units: 0.91 },
  { ...row, Result: "push", Units: 0 },
  { ...row, Pattern: "T5", Result: "lost", Units: -1 },
  { ...row, Pattern: "T6", Result: "pending", Units: 0 },
]), [
  ["T4", { wins: 1, losses: 0, pushes: 1, units: 0.91 }],
  ["T5", { wins: 0, losses: 1, pushes: 0, units: -1 }],
]);

const key = "a-local-test-key-only";
const now = Date.parse("2026-11-15T01:00:00Z");
assert.equal(createPatternWatchSession("incorrect", now, key), null);
const session = createPatternWatchSession(key, now, key)!;
assert.ok(!session.value.includes(key));
assert.ok(patternWatchSessionValid(session.value, now, key));
assert.equal(patternWatchSessionValid(session.value, session.expiresAt, key), false);
assert.equal(patternWatchSessionValid(session.value, now, "another-local-test-key"), false);
assert.equal(patternWatchSessionValid(session.value.replace(/^\d+/, String(session.expiresAt - 1)), now, key), false);
assert.equal(patternWatchSessionValid(undefined, now, key), false);
console.log("Daily date selection, market rollups and protected CFB access passed.");
