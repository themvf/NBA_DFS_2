/**
 * NBA season strings come from the date (2026-11-15). A hard-coded "2025-26"
 * fed 2026-27 slates last season's final ten games. Mirrors config.py
 * nba_season_for; tests/test_nba_season.py pins the same cases.
 */
import { currentNbaSeason, nbaSeasonFor } from "../src/lib/nba-season";

function eq(actual: unknown, expected: unknown, label: string) {
  if (actual !== expected) throw new Error(`${label}: expected ${String(expected)}, got ${String(actual)}`);
}

eq(nbaSeasonFor("2026-10-24"), "2026-27", "October is the new season");
eq(nbaSeasonFor("2026-04-10"), "2025-26", "April is the old season");
eq(nbaSeasonFor("2026-07-31"), "2025-26", "July still the old season");
eq(nbaSeasonFor("2026-08-01"), "2026-27", "turns over in August");
eq(nbaSeasonFor("2027-01-05"), "2026-27", "January belongs to the season that started in the fall");
eq(nbaSeasonFor("2099-12-01"), "2099-00", "century wraps to two digits");
eq(nbaSeasonFor(new Date("2026-11-15T12:00:00Z")), "2026-27", "accepts a Date");
// 03:00 UTC on Aug 1 is still July 31 in New York.
eq(currentNbaSeason(new Date("2026-08-01T03:00:00Z")), "2025-26", "uses the Eastern calendar day");
eq(currentNbaSeason(new Date("2026-08-01T16:00:00Z")), "2026-27", "Eastern Aug 1");
let threw = false;
try { nbaSeasonFor("not a date"); } catch { threw = true; }
eq(threw, true, "rejects a non-date");
console.log("nba-season: 10 checks passed");
