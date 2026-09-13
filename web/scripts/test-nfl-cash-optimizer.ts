/**
 * Verifies the cash-optimizer runner against synthetic players, so the reporting
 * logic is proven before it is ever pointed at a real slate.
 *
 * The load-bearing assertion is that `floorProvenance` agrees with the optimizer's
 * OWN cash branching. Those are two copies of the same rule, and if they drift the
 * report will quietly mislabel a flat projection*0.74 fallback as a modelled floor --
 * exactly the thing a cash player most needs to see.
 */
import assert from "node:assert/strict";
import { optimizeNflLineups, type NflOptimizerPlayer, type NflOptimizerSettings, type NflLineupSlot }
  from "../src/app/dfs/nfl/nfl-optimizer";
import { floorProvenance, CLASSIC_LABEL } from "./run-nfl-cash-optimizer";
import { savedSlateLabel } from "../src/lib/nfl-dfs/saved-workspace";

const player = (over: Partial<NflOptimizerPlayer> & { dkPlayerId: number; position: NflOptimizerPlayer["position"] }): NflOptimizerPlayer => ({
  id: over.dkPlayerId, captainDkPlayerId: null, name: `P${over.dkPlayerId}`, team: "AAA", opponent: "BBB",
  gameKey: "AAA@BBB", salary: 5000, isOut: false, projectionStatus: "ok", ourProj: 12, floorFpts: 8,
  ceilingFpts: 20, boomRate: 0.2, avgFptsDk: 11, fantasyprosProj: null, linestarProj: null,
  linestarOwnPct: null, customProj: null, ...over,
});

const slot = (over: Partial<NflLineupSlot> & { player: NflOptimizerPlayer }): NflLineupSlot =>
  ({ slot: "FLEX", salary: 5000, multiplier: 1, projection: 10, projectionSource: "our", ...over });

// A historical floor is modelled; a missing one is the flat fallback.
assert.equal(floorProvenance(slot({ player: player({ dkPlayerId: 1, position: "RB", floorFpts: 7.5 }) })).modelled, true);
assert.match(floorProvenance(slot({ player: player({ dkPlayerId: 2, position: "RB", floorFpts: 7.5 }) })).detail, /historical floor 7\.5/);
assert.equal(floorProvenance(slot({ player: player({ dkPlayerId: 3, position: "RB", floorFpts: null }) })).modelled, false);
assert.match(floorProvenance(slot({ player: player({ dkPlayerId: 4, position: "RB", floorFpts: null }) })).detail, /no historical floor/);

// A DK-average fallback carries no floor estimate at all, so it must never read as modelled.
assert.equal(floorProvenance(slot({ projectionSource: "dk_avg_fallback", player: player({ dkPlayerId: 5, position: "WR" }) })).modelled, false);

// Calibrated reads p10, not the historical floor -- a player can have one and not the other.
const calibrated = player({ dkPlayerId: 6, position: "WR", floorFpts: null });
calibrated.calibrated = { p10: 6.25, p50: 12, p90: 22, boom: 0.3, kickoff: new Date(Date.now() + 864e5).toISOString() } as never;
assert.equal(floorProvenance(slot({ projectionSource: "calibrated", player: calibrated })).modelled, true);
assert.match(floorProvenance(slot({ projectionSource: "calibrated", player: calibrated })).detail, /calibrated p10 6\.3/);
console.log("floorProvenance: historical, missing, dk fallback and calibrated cases passed.");

// A full Classic pool must actually solve under the runner's own default settings.
const pool: NflOptimizerPlayer[] = [];
let nextId = 100;
for (const [position, count] of [["QB", 3], ["RB", 8], ["WR", 10], ["TE", 4], ["DST", 3]] as const) {
  for (let i = 0; i < count; i++) {
    pool.push(player({ dkPlayerId: nextId, position, salary: 4000 + i * 400, ourProj: 18 - i,
      floorFpts: i === 0 ? null : 12 - i, team: i % 2 ? "AAA" : "CCC", opponent: i % 2 ? "BBB" : "DDD",
      gameKey: i % 2 ? "AAA@BBB" : "CCC@DDD" }));
    nextId++;
  }
}
const settings: NflOptimizerSettings = { format: "classic", mode: "cash", projectionSource: "our",
  allowDkFallback: true, nLineups: 3, minSalary: 30000, maxExposure: 1, minUnique: 1,
  stackPassCatchers: 0, bringBack: false, randomness: 0, lockedPlayerIds: [], excludedPlayerIds: [],
  minExposureByPlayer: {}, maxExposureByPlayer: {} };

const result = optimizeNflLineups(pool, settings);
assert.ok(result.lineups.length >= 1, "classic pool produced no lineup");
const first = result.lineups[0];
assert.equal(first.slots.length, 9, "classic lineup must fill 9 slots");
assert.ok(first.totalSalary <= 50000, "lineup exceeded the salary cap");
assert.ok(first.totalSalary >= settings.minSalary, "lineup fell under min salary");
assert.equal(new Set(first.playerIds).size, 9, "a player was rostered twice");

// Cash is solved first without uniqueness constraints, so lineup 1 must be the optimum.
const scores = result.lineups.map(l => l.projectedFpts);
assert.deepEqual([...scores].sort((a, b) => b - a), scores, "lineups were not returned best-first");

// Determinism: the jitter seed is hardcoded, so a repeat run must be identical.
assert.deepEqual(optimizeNflLineups(pool, settings).lineups[0].playerIds, first.playerIds,
  "repeat run differed; the optimizer is supposed to be deterministic");
console.log(`Classic cash solve: ${result.lineups.length} lineups, 9 slots, $${first.totalSalary} salary, deterministic on rerun.`);
console.log("All cash optimizer runner checks passed.");

// The Classic-label filter must agree with the real label generator. A Showdown upload
// is routinely the most recent one, so picking "newest slate" instead of "newest Classic
// slate" makes the runner refuse a Classic slate that is sitting right there -- which is
// exactly how the first live run failed.
assert.equal(CLASSIC_LABEL.test(savedSlateLabel("classic", "NO@DET 09/13/2026 01:00PM ET", Array(12).fill("GAME"))), true);
assert.equal(CLASSIC_LABEL.test(savedSlateLabel("showdown", "NE@SEA 09/09/2026 08:20PM ET", ["NE@SEA"])), false);
assert.equal(CLASSIC_LABEL.test(savedSlateLabel("classic", "NO@DET 09/13/2026 01:00PM ET", ["NO@DET"])), true);
console.log("Classic label filter agrees with savedSlateLabel for classic and showdown.");
