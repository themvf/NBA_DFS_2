/**
 * GPP objective: multiplicative ownership leverage and the ceiling cap.
 *
 * Before 2026-09-27 the ownership term was a flat 0.025 points per ownership
 * point against a P90 objective — 1.3 points on a 47-point ceiling — and a
 * leverage-on build matched a leverage-off build player for player. The
 * ceiling cap exists because a 5.5-point back carried a 30.9 P90 (5.6× his
 * projection) and was filling RB slots on it.
 */
import assert from "node:assert/strict";
import {
  cappedCeiling, DEFAULT_LEVERAGE_EXPONENT, DEFAULT_MAX_CEILING_MULTIPLE, leverageFactor, optimizeNflLineups,
  type NflOptimizerPlayer, type NflOptimizerSettings,
} from "../src/app/dfs/nfl/nfl-optimizer";

// --- Units ---
assert.ok(Math.abs(leverageFactor(53.5, 0.5) - Math.sqrt(0.465)) < 1e-9, "53.5% owned -> sqrt(0.465)");
assert.equal(leverageFactor(null, 0.5), 1, "unknown ownership is factor 1 (never a penalty, never a bonus)");
assert.equal(leverageFactor(undefined, 0.5), 1);
assert.equal(leverageFactor(40, 0), 1, "exponent 0 disables the factor");
assert.ok(leverageFactor(100, 0.5) > 0, "100% owned is clamped, not zeroed");
assert.ok(leverageFactor(20, 0.5) > leverageFactor(20, 1.0), "a larger exponent penalises harder");
assert.equal(cappedCeiling(30.9, 5.5, 2.5), 13.75, "the lottery ceiling is capped at 2.5x projection");
assert.equal(cappedCeiling(54.8, 26.9, 2.5), 54.8, "a starter's 2.0x ceiling is untouched");
assert.equal(cappedCeiling(20, 0, 2.5), 20, "no projection: nothing to cap against, ceiling stands");
assert.equal(DEFAULT_LEVERAGE_EXPONENT, 0.5); assert.equal(DEFAULT_MAX_CEILING_MULTIPLE, 2.5);

// --- Integration: a legal Showdown pool where only one of two rivals fits ---
function player(over: Partial<NflOptimizerPlayer> & { dkPlayerId: number; salary: number }): NflOptimizerPlayer {
  return {
    id: over.dkPlayerId, dkPlayerId: over.dkPlayerId, captainDkPlayerId: over.dkPlayerId + 100_000,
    name: over.name ?? `P${over.dkPlayerId}`, position: over.position ?? "WR", team: over.team ?? "AAA",
    opponent: over.team === "BBB" ? "AAA" : "BBB", gameKey: "AAA@BBB", salary: over.salary,
    captainSalary: Math.round(over.salary * 1.5), isOut: false, projectionStatus: "historical",
    historyGames: 6, ourProj: over.ourProj ?? 10, floorFpts: 7, ceilingFpts: over.ceilingFpts ?? 14, boomRate: 0.2,
    avgFptsDk: 10, fantasyprosProj: null, linestarProj: null, linestarOwnPct: null, customProj: null,
    ownPct: over.ownPct ?? null, ownSource: over.ownPct != null ? "test" : null,
  };
}
function settings(over: Partial<NflOptimizerSettings> = {}): NflOptimizerSettings {
  return {
    format: "showdown", mode: "gpp", projectionSource: "our", allowDkFallback: true,
    nLineups: 1, minSalary: 0, maxExposure: 1, minUnique: 1, stackPassCatchers: 1, bringBack: true, randomness: 0,
    lockedPlayerIds: [], excludedPlayerIds: [], minExposureByPlayer: {}, maxExposureByPlayer: {},
    ownershipCapability: "heuristic_uncalibrated", ownershipLeverageEnabled: true, ...over,
  };
}
const names = (r: ReturnType<typeof optimizeNflLineups>) => new Set(r.lineups[0].slots.map((s) => s.player.name));

// Chalk A: slightly higher ceiling, 60% owned. Pivot B: 5% owned. Priced so both cannot fit.
const rivals = [
  player({ dkPlayerId: 1, name: "QB", salary: 6000, position: "QB", team: "AAA", ourProj: 20, ceilingFpts: 32 }),
  player({ dkPlayerId: 2, name: "ChalkA", salary: 20000, position: "WR", team: "AAA", ourProj: 16, ceilingFpts: 30, ownPct: 60 }),
  player({ dkPlayerId: 3, name: "PivotB", salary: 20000, position: "WR", team: "BBB", ourProj: 15, ceilingFpts: 28, ownPct: 5 }),
  player({ dkPlayerId: 4, name: "RB1", salary: 5000, position: "RB", team: "AAA", ourProj: 12, ceilingFpts: 22, ownPct: 20 }),
  player({ dkPlayerId: 5, name: "TE1", salary: 4000, position: "TE", team: "BBB", ourProj: 9, ceilingFpts: 18, ownPct: 10 }),
  player({ dkPlayerId: 6, name: "RB2", salary: 3500, position: "RB", team: "BBB", ourProj: 8, ceilingFpts: 16, ownPct: 8 }),
  player({ dkPlayerId: 7, name: "WR3", salary: 3200, position: "WR", team: "AAA", ourProj: 7, ceilingFpts: 15, ownPct: 6 }),
  player({ dkPlayerId: 8, name: "QB2", salary: 3100, position: "QB", team: "BBB", ourProj: 9, ceilingFpts: 17, ownPct: 3 }),
];
const off = names(optimizeNflLineups(rivals, settings({ ownershipLeverageEnabled: false, ownershipCapability: "unavailable" })));
assert.ok(off.has("ChalkA") && !off.has("PivotB"), "leverage off: the higher ceiling wins");
const on = names(optimizeNflLineups(rivals, settings()));
assert.ok(on.has("PivotB") && !on.has("ChalkA"), "leverage on: 60% owned loses to 5% owned at a similar ceiling");
const cash = names(optimizeNflLineups(rivals, settings({ mode: "cash" })));
assert.ok(cash.has("ChalkA"), "cash mode ignores ownership entirely");
const flat = names(optimizeNflLineups(rivals, settings({ leverageExponent: 0 })));
assert.ok(flat.has("ChalkA"), "exponent 0 reproduces the leverage-off choice");

// Lottery L: 5-point projection with a 31-point ceiling. Solid S: 12 projected, 20 ceiling. Same price.
const lottery = rivals.map((p) => p.name === "ChalkA" ? player({ dkPlayerId: 2, name: "Lottery", salary: 20000, position: "WR", team: "AAA", ourProj: 5, ceilingFpts: 31, ownPct: 1 })
  : p.name === "PivotB" ? player({ dkPlayerId: 3, name: "Solid", salary: 20000, position: "WR", team: "BBB", ourProj: 12, ceilingFpts: 20, ownPct: 1 }) : p);
const capped = names(optimizeNflLineups(lottery, settings()));
assert.ok(capped.has("Solid") && !capped.has("Lottery"), "with the cap, a 6x ceiling on a 5-point projection loses to a real 12");
const uncapped = names(optimizeNflLineups(lottery, settings({ maxCeilingMultiple: 100 })));
assert.ok(uncapped.has("Lottery"), "without the cap the lottery ticket wins on raw P90 — the behaviour being fixed");

console.log("NFL GPP leverage: multiplicative factor flips chalk at similar ceilings, cash ignores it, ceiling cap ends the lottery-ticket fill.");
