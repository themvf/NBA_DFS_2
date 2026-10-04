// Season and "today" defaults must follow Eastern time and the NFL calendar,
// not UTC and not a literal year. Regression checks for the helpers every
// page default now goes through.
import assert from "node:assert/strict";
import { easternDateString, easternYear } from "../src/lib/eastern-date";
import { currentNflSeason, resolveNflSeason, selectableNflSeasons } from "../src/lib/nfl/season";

// 8:30pm ET on Sept 29 is 00:30 UTC on Sept 30. The slate is still Sept 29.
assert.equal(easternDateString(new Date("2026-09-30T00:30:00Z")), "2026-09-29");
assert.equal(new Date("2026-09-30T00:30:00Z").toISOString().slice(0, 10), "2026-09-30", "UTC would have rolled over");
// Standard time: 7:30pm ET on Jan 5 is 00:30 UTC on Jan 6.
assert.equal(easternDateString(new Date("2027-01-06T00:30:00Z")), "2027-01-05");
// Noon ET agrees with UTC.
assert.equal(easternDateString(new Date("2026-09-29T16:00:00Z")), "2026-09-29");
assert.equal(easternYear(new Date("2027-01-01T02:00:00Z")), 2026, "New Year's Eve evening in ET is still 2026");

// NFL season: the year it kicks off in, through the following February.
assert.equal(currentNflSeason(new Date("2026-09-29T20:00:00Z")), 2026);
assert.equal(currentNflSeason(new Date("2027-01-15T20:00:00Z")), 2026, "January playoffs belong to the 2026 season");
assert.equal(currentNflSeason(new Date("2027-02-14T20:00:00Z")), 2026, "Super Bowl month too");
assert.equal(currentNflSeason(new Date("2027-03-01T12:00:00Z")), 2027, "league year starts in March");
assert.equal(currentNflSeason(new Date("2027-03-01T03:00:00Z")), 2026, "Feb 28 at 10pm ET is still February");

assert.equal(resolveNflSeason("2024", new Date("2026-09-29T20:00:00Z")), 2024);
assert.equal(resolveNflSeason(undefined, new Date("2026-09-29T20:00:00Z")), 2026);
assert.equal(resolveNflSeason("nope", new Date("2027-01-15T20:00:00Z")), 2026);
assert.equal(resolveNflSeason("1999", new Date("2026-09-29T20:00:00Z")), 2026, "implausible years fall back");

assert.deepEqual(selectableNflSeasons(new Date("2026-09-29T20:00:00Z")), [2026, 2025, 2024, 2023, 2022, 2021, 2020]);
assert.deepEqual(selectableNflSeasons(new Date("2027-09-29T20:00:00Z"))[0], 2027, "the picker follows the calendar");

console.log("RESULT: season-defaults checks passed");
