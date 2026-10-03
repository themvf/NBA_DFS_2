import assert from "node:assert/strict";
import { coverageIssues, marketCoverage } from "../src/lib/cfb-coverage";

const capturedAt = "2026-10-03T15:55:00Z";
const complete = { spread_home: -3, spread_away: 3, spread_home_price: -110,
  spread_away_price: -110, total_line: 48.5, over: -110, under: -110,
  ml_home: -150, ml_away: 130, last_update: "2026-10-03T15:54:00Z" };
const old = { ...complete, last_update: "2026-10-03T15:40:00Z" };
const result = marketCoverage({ draftkings: complete, fanduel: complete, betmgm: old },
  { draftkings: complete, fanduel: complete }, capturedAt);
assert.deepEqual(result.spread, { books: 3, freshAtCapture: 2, sameBooksAsPrevious: 2 });
assert.deepEqual(result.total, { books: 3, freshAtCapture: 2, sameBooksAsPrevious: 2 });
assert.deepEqual(result.moneyline, { books: 3, freshAtCapture: 2, sameBooksAsPrevious: 2 });
assert.equal(marketCoverage(null, null, null).spread.books, 0);

const issues = coverageIssues({ mapped: true, kickoff: "2026-10-03T16:00:00Z",
  asOf: "2026-10-03T15:55:00Z", capturedAt, markets: result,
  dueCheckpoint: true, missedCheckpoint: false });
assert(issues.includes("Checkpoint due now"));
assert(issues.includes("spread has fewer than 3 fresh books"));
assert(!issues.includes("Capture overdue"));
console.log("CFB coverage checks passed");
