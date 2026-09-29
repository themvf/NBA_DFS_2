/**
 * A lock or an exposure range on a player the build can't use is said before
 * generating, never dropped (2026-09-29 audit, findings 4 and 11).
 *
 * Before: a Showdown captain range on a ruled-out or blocked player was skipped
 * and QA said "All exposure ranges satisfied"; a lock on one made every lineup
 * infeasible with a solver message that named nobody.
 */
import assert from "node:assert/strict";
import { optimizeNflLineups, type NflOptimizerPlayer, type NflOptimizerSettings, type PlayerExposurePolicy } from "../src/app/dfs/nfl/nfl-optimizer";

function player(over: Partial<NflOptimizerPlayer> & { dkPlayerId: number; salary: number }): NflOptimizerPlayer {
  return {
    id: over.dkPlayerId, dkPlayerId: over.dkPlayerId, captainDkPlayerId: over.dkPlayerId + 100_000,
    name: over.name ?? `P${over.dkPlayerId}`, position: over.position ?? "WR", team: over.team ?? "PHI",
    opponent: over.team === "CHI" ? "PHI" : "CHI", gameKey: "PHI@CHI", salary: over.salary,
    captainSalary: Math.round(over.salary * 1.5), isOut: over.isOut ?? false, projectionStatus: over.projectionStatus ?? "historical",
    historyGames: 6, ourProj: over.ourProj === undefined ? 10 : over.ourProj, floorFpts: 7, ceilingFpts: 14, boomRate: 0.2,
    avgFptsDk: over.avgFptsDk ?? null, fantasyprosProj: null, linestarProj: null, linestarOwnPct: null, customProj: null,
    availability: over.availability,
  };
}

const pool = (): NflOptimizerPlayer[] => [
  player({ dkPlayerId: 1, salary: 10000, position: "QB", team: "PHI", ourProj: 20, name: "Jalen Hurts" }),
  player({ dkPlayerId: 2, salary: 9000, position: "WR", team: "PHI", ourProj: 16 }),
  player({ dkPlayerId: 3, salary: 8000, position: "RB", team: "PHI", ourProj: 14 }),
  player({ dkPlayerId: 4, salary: 7000, position: "WR", team: "CHI", ourProj: 12 }),
  player({ dkPlayerId: 5, salary: 6000, position: "TE", team: "CHI", ourProj: 10 }),
  player({ dkPlayerId: 6, salary: 5000, position: "RB", team: "CHI", ourProj: 9 }),
  player({ dkPlayerId: 7, salary: 4000, position: "WR", team: "PHI", ourProj: 8 }),
  player({ dkPlayerId: 8, salary: 3800, position: "QB", team: "CHI", ourProj: 15, name: "Case Keenum" }),
  // A listed backup QB the depth chart blocks, the way the workspace marks him.
  player({ dkPlayerId: 9, salary: 5200, position: "QB", team: "CHI", ourProj: 3.5, name: "Tyson Bagent", isOut: true,
    availability: { blockedReason: "Listed QB2; starter workload not supported" } }),
  // Ruled out by DraftKings.
  player({ dkPlayerId: 10, salary: 11000, position: "QB", team: "CHI", ourProj: 0, projectionStatus: "out", name: "Caleb Williams", isOut: true }),
  // Matched to no projection.
  player({ dkPlayerId: 11, salary: 1000, position: "WR", team: "CHI", ourProj: null, name: "No Projection" }),
];

const settings = (over: Partial<NflOptimizerSettings> = {}): NflOptimizerSettings => ({
  format: "showdown", mode: "gpp", projectionSource: "our", allowDkFallback: false,
  nLineups: 5, minSalary: 0, maxExposure: 1, minUnique: 1, stackPassCatchers: 0, bringBack: false, randomness: 0,
  lockedPlayerIds: [], excludedPlayerIds: [], minExposureByPlayer: {}, maxExposureByPlayer: {}, ...over,
});
const captainRange = (playerId: number, minPct: number | null, maxPct: number | null): PlayerExposurePolicy =>
  ({ playerId, overall: { minPct: null, maxPct: 1 }, captain: { minPct, maxPct }, flex: { minPct: null, maxPct: null }, exactTargetMode: false });

// --- Captain range on a blocked backup (the PHI@CHI Bagent shape) ---
assert.throws(() => optimizeNflLineups(pool(), settings({ exposurePolicies: [captainRange(9, 0.15, 0.3)] })),
  /^Error: Tyson Bagent has a captain minimum but can't be used: not available: Listed QB2; starter workload not supported\. Clear his CPT range to build without him\.$/);
// Captain range on a DraftKings OUT player.
assert.throws(() => optimizeNflLineups(pool(), settings({ exposurePolicies: [captainRange(10, 0.2, null)] })),
  /Caleb Williams has a captain minimum but can't be used: inactive \(OUT\/IR\)/);
// A cap alone costs nothing to honour, but it is still said, not dropped.
const capped = optimizeNflLineups(pool(), settings({ exposurePolicies: [captainRange(9, null, 0.2)] }));
assert.equal(capped.lineups.length, 5);
assert.ok(capped.warnings.includes("Tyson Bagent's range was ignored: not available: Listed QB2; starter workload not supported."));
// An overall minimum on a player with no projection names the reason.
assert.throws(() => optimizeNflLineups(pool(), settings({ minExposureByPlayer: { "11": 0.4 } })),
  /No Projection has a minimum exposure but can't be used: no usable projection in the selected source\./);
const overallCap = optimizeNflLineups(pool(), settings({ maxExposureByPlayer: { "10": 0.2 } }));
assert.ok(overallCap.warnings.includes("Caleb Williams's exposure cap was ignored: inactive (OUT/IR)."));

// --- Locks ---
assert.throws(() => optimizeNflLineups(pool(), settings({ lockedPlayerIds: [10] })),
  /^Error: Caleb Williams is locked but can't be used: inactive \(OUT\/IR\)\. Remove the lock to build without him\.$/);
assert.throws(() => optimizeNflLineups(pool(), settings({ lockedPlayerIds: [9] })), /Tyson Bagent is locked but can't be used: not available: Listed QB2/);
assert.throws(() => optimizeNflLineups(pool(), settings({ lockedPlayerIds: [11] })), /No Projection is locked but can't be used: no usable projection/);
assert.throws(() => optimizeNflLineups(pool(), settings({ lockedPlayerIds: [2], excludedPlayerIds: [2] })), /P2 is locked but can't be used: manually excluded for this run/);
// A lock on a usable player still builds.
const locked = optimizeNflLineups(pool(), settings({ lockedPlayerIds: [8] }));
assert.equal(locked.lineups.length, 5);
assert.ok(locked.lineups.every((l) => l.playerIds.includes(8)));
// The eligibility reason names the real block, not "Inactive" for a backup.
assert.equal(locked.eligibility!.find((e) => e.dkPlayerId === 9)!.reason, "Not available: Listed QB2; starter workload not supported.");

// --- The quota plan is reported so QA can check it; absent without a plan ---
assert.equal(locked.archetypePlan, undefined);
const planned = optimizeNflLineups(pool(), settings({ archetypeMode: "chalk_leverage" }));
assert.ok(planned.archetypePlan && planned.archetypePlan.length === 1);
assert.equal(planned.archetypePlan[0].requested, 5);
assert.equal(planned.archetypePlan[0].realized, planned.lineups.length);

console.log("Unusable players: locks and exposure minimums on players outside the pool fail with the player and reason; caps are reported; the quota plan is returned.");
