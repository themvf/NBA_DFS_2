import assert from "node:assert/strict";
import { cfbForecastFreshness as freshness } from "../src/lib/cfb-forecast-freshness";
import { cfbForecastDue } from "../src/lib/cron-dispatch";
const kickoff = "2026-10-15T23:30:00Z";
const ctx = { nflKickoffs: [], cfbKickoffs: [new Date(kickoff)] };
assert(cfbForecastDue(new Date("2026-10-15T20:22:00Z"), ctx));
assert(!cfbForecastDue(new Date("2026-10-15T20:37:00Z"), ctx));
assert(!cfbForecastDue(new Date("2026-10-15T20:07:00Z"), ctx));
for (const day of [13,14,15,16,17,18,19]) {
  assert(cfbForecastDue(new Date(`2026-10-${day}T10:07:00Z`), { nflKickoffs: null }));
  assert(!cfbForecastDue(new Date(`2026-10-${day}T10:22:00Z`), { nflKickoffs: null }));
}
assert(!cfbForecastDue(new Date("2026-07-15T10:07:00Z"), ctx));
assert(cfbForecastDue(new Date("2027-01-15T10:07:00Z"), ctx));
assert(freshness("2026-10-11T16:22:00Z", kickoff, "2026-10-15T22:00:00Z").stale);
assert(!freshness("2026-10-15T20:25:00Z", kickoff, "2026-10-15T22:00:00Z").stale);
assert(freshness("2026-10-15T10:25:00Z", kickoff, "2026-10-15T22:00:00Z").stale);
assert(!freshness("2026-10-15T20:25:00Z", kickoff, "2026-11-15T22:00:00Z").stale);
assert(freshness(null, kickoff, "2026-10-15T22:00:00Z").stale);
assert(freshness("2026-10-15T23:31:00Z", kickoff, "2026-10-16T22:00:00Z").stale);
console.log("CFB forecast cadence and freshness checks passed");
