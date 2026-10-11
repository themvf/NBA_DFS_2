import assert from "node:assert/strict";
import { classifyRuledOut, outPlayersInForecast } from "../src/lib/nfl/game-leaders-availability";

// A Sleeper reserve-list designation rules a player out on its own.
for (const reserve of ["IR", "PUP", "NFI", "SUSPENDED", "ir"]) {
  assert.equal(classifyRuledOut(reserve, "UNKNOWN", null), "Sleeper reserve list", reserve);
}
// A weekly OUT still needs FantasyPros to agree.
assert.equal(classifyRuledOut("OUT", "UNKNOWN", null), null);
assert.equal(classifyRuledOut("OUT", "OUT", null), "Sleeper and FantasyPros");
// FantasyPros alone is not enough; the official report always is.
assert.equal(classifyRuledOut("QUESTIONABLE", "IR", null), null);
assert.equal(classifyRuledOut(null, null, "INACTIVE"), "Official");
assert.equal(classifyRuledOut("IR", "OUT", "OUT"), "Official");
assert.equal(classifyRuledOut("ACTIVE", "HEALTHY", null), null);

const families = { receptions: { players: [{ identity: "a", residual: false }, { identity: "OTHER:X", residual: true }] } };
const out = [{ identity: "a", name: "A", team: "X", observedAt: "2026-10-11T00:00:00Z", source: "Sleeper reserve list" as const },
  { identity: "b", name: "B", team: "X", observedAt: "2026-10-11T00:00:00Z", source: "Official" as const }];
assert.deepEqual(outPlayersInForecast(families, out).map((p) => p.identity), ["a"]);

console.log("Game leaders availability: reserve-list rule matches the weekly capture.");
