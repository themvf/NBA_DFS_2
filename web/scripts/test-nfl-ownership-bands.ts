/**
 * Ownership ranges (`nfl-ownership-bands-v1`): the FantasyPros page parses
 * section by section (Flex skipped, status letters stripped), players match on
 * team + position + salary + name (a defense on its nickname), and the prior
 * is held inside each range without renormalising the slate.
 */
import assert from "node:assert/strict";
import { bandReport, clampToBand, matchBands, parseFantasyProsOwnership } from "../src/lib/nfl-dfs/ownership-bands";

const PAGE = [
  "DraftKings Projected Roster %",
  "Quarterbacks - Projected Roster %",
  "Quarterbacks Projected Roster %",
  "Player\tSalary\tProj. Roster %",
  "Matthew Stafford", "Matthew Stafford", "LAR at LV\t$6,400\t", "10-20%",
  "Lamar Jackson", "Lamar JacksonO", "BAL vs. CIN\t$6,700\t", "5-10%",
  "Michael Penix Jr.", "Michael Penix Jr.", "ATL vs. SF\t$5,300\t", "5-10%",
  "Running Backs - Projected Roster %",
  "Kyren Williams", "Kyren Williams", "LAR at LV\t$7,000\t", "20-30%",
  "Flex Position (RB/WR/TE) - Projected Roster %",
  "Kyren Williams", "Kyren Williams", "LAR at LV\t$7,000\t", "20-30%",
  "DST - Projected Roster %",
  "Defense and Special Teams Projected Roster %",
  "Houston Texans", "Houston Texans", "HOU vs. NYG\t$3,400\t", "10-20%",
  "Minnesota Vikings", "Minnesota Vikings", "MIA at NYJ\t$2,300\t", "1-5%",
].join("\r\n");

const parsed = parseFantasyProsOwnership(PAGE);
assert.deepEqual(parsed.warnings, []);
assert.equal(parsed.rows.length, 6, "the Flex repeat is skipped");
assert.deepEqual(parsed.rows.map((r) => [r.name, r.position, r.team, r.salary, r.low, r.high]), [
  ["Matthew Stafford", "QB", "LAR", 6400, 10, 20],
  ["Lamar Jackson", "QB", "BAL", 6700, 5, 10],
  ["Michael Penix Jr.", "QB", "ATL", 5300, 5, 10],
  ["Kyren Williams", "RB", "LAR", 7000, 20, 30],
  ["Houston Texans", "DST", "HOU", 3400, 10, 20],
  ["Minnesota Vikings", "DST", "MIA", 2300, 1, 5],
]);

assert.equal(clampToBand(3, { low: 5, high: 10 }), 5, "raised to the low end");
assert.equal(clampToBand(14, { low: 5, high: 10 }), 10, "lowered to the high end");
assert.equal(clampToBand(7.2, { low: 5, high: 10 }), 7.2, "inside is kept");

const players = [
  { dkPlayerId: 1, name: "Matthew Stafford", position: "QB", team: "LAR", salary: 6400 },
  { dkPlayerId: 2, name: "Michael Penix", position: "QB", team: "ATL", salary: 5300 },
  { dkPlayerId: 3, name: "Kyren Williams", position: "RB", team: "LAR", salary: 7100 },
  { dkPlayerId: 4, name: "Texans", position: "DST", team: "HOU", salary: 3400 },
  { dkPlayerId: 5, name: "Dolphins", position: "DST", team: "MIA", salary: 2300 },
  { dkPlayerId: 6, name: "Puka Nacua", position: "WR", team: "LAR", salary: 8000 },
];
const matches = matchBands(players, parsed.rows);
assert.deepEqual([...matches.keys()].sort(), [1, 2, 4], "salary change, wrong defense name and no row stay unmatched");
assert.equal(matches.get(2)?.name, "Michael Penix Jr.", "suffixes are ignored");

const before = new Map([[1, 4], [2, 7], [4, 25], [3, 18], [5, 2], [6, 30]]);
const after = new Map([...matches].map(([id, band]) => [id, clampToBand(before.get(id)!, band)]));
const report = bandReport("fantasypros", null, parsed.rows, matches, before, after);
assert.equal(report.matched, 3);
assert.equal(report.raised, 1);
assert.equal(report.lowered, 1);
assert.equal(report.kept, 1);
assert.equal(report.uncovered, 3);
assert.equal(report.unmatched, 3);
assert.equal(report.totalBefore, 86);
assert.equal(report.totalAfter, 86 - 4 + 10 - 25 + 20);
console.log("nfl ownership bands: ok");
