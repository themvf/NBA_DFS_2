import assert from "node:assert/strict";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { AllStudies } from "../src/app/sports/market-studies/all-studies";
import type { PatternWatchDataset } from "../src/db/cfb-pattern-watch";
import { studyHeading, studyRegistry, type StudyId } from "../src/lib/study-registry";
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

const cfb = {
  patterns: [],
  results: [
    { ...row, Result: "won", Units: 0.91 },
    { ...row, Date: "2026-11-13", Result: "lost", Units: -1 },
    { ...row, Date: "2026-11-15", Result: "won", Units: 1 },
  ],
} as unknown as PatternWatchDataset;
const html = renderToStaticMarkup(createElement(AllStudies, { nba: [], nfl: [], cfb, cfbUnavailable: false, date: today }));
for (const id of Object.keys(studyRegistry) as StudyId[]) assert.ok(html.includes(studyHeading(id)), id);
assert.ok(html.includes("1–0–0 · 100% · +0.91 u"));
assert.ok(html.includes("1–1–0 · 50% · -0.09 u"));
assert.ok(html.includes("Awaiting original point-in-time totals forecasts"));
const unavailable = renderToStaticMarkup(createElement(AllStudies, { nba: null, nfl: null, cfb: null, cfbUnavailable: true, date: today }));
assert.ok(unavailable.includes("CFB study data is temporarily unavailable."));
console.log("Daily date selection, market rollups and all registered study sections passed.");
