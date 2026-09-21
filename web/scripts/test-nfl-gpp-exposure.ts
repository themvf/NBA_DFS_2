/**
 * Phase 3 tests (spec §10): role-specific exposure ranges.
 * Covers P3-AC1..P3-AC5 plus derivation/infeasibility units.
 */
import assert from "node:assert/strict";
import { optimizeNflLineups, type NflOptimizerPlayer, type NflOptimizerSettings, type PlayerExposurePolicy } from "../src/app/dfs/nfl/nfl-optimizer";
import { deriveExposureCounts, detectExposureInfeasibility, validateExposurePolicy, ceilMin, floorMax } from "../src/lib/nfl-dfs/exposure-plan";

function player(over: Partial<NflOptimizerPlayer> & { dkPlayerId: number; salary: number }): NflOptimizerPlayer {
  return {
    id: over.dkPlayerId, dkPlayerId: over.dkPlayerId, captainDkPlayerId: over.dkPlayerId + 100_000,
    name: over.name ?? `P${over.dkPlayerId}`, position: over.position ?? "WR", team: over.team ?? "AAA",
    opponent: over.team === "BBB" ? "AAA" : "BBB", gameKey: "AAA@BBB", salary: over.salary,
    captainSalary: Math.round(over.salary * 1.5), isOut: false, projectionStatus: "historical",
    historyGames: 6, ourProj: over.ourProj ?? 10, floorFpts: 7, ceilingFpts: 14, boomRate: 0.2,
    avgFptsDk: 10, fantasyprosProj: null, linestarProj: null, linestarOwnPct: null, customProj: null,
  };
}

function pool(): NflOptimizerPlayer[] {
  return [
    player({ dkPlayerId: 1, salary: 10000, position: "QB", team: "AAA", ourProj: 20 }),
    player({ dkPlayerId: 2, salary: 9000, position: "WR", team: "AAA", ourProj: 16 }),
    player({ dkPlayerId: 3, salary: 8000, position: "RB", team: "AAA", ourProj: 14 }),
    player({ dkPlayerId: 4, salary: 7000, position: "WR", team: "BBB", ourProj: 12 }),
    player({ dkPlayerId: 5, salary: 6000, position: "TE", team: "BBB", ourProj: 10 }),
    player({ dkPlayerId: 6, salary: 5000, position: "RB", team: "BBB", ourProj: 9 }),
    player({ dkPlayerId: 7, salary: 4000, position: "WR", team: "AAA", ourProj: 8 }),
    player({ dkPlayerId: 8, salary: 3800, position: "QB", team: "BBB", ourProj: 15 }),
  ];
}

function settings(over: Partial<NflOptimizerSettings> = {}): NflOptimizerSettings {
  return {
    format: "showdown", mode: "gpp", projectionSource: "our", allowDkFallback: true,
    nLineups: 20, minSalary: 0, maxExposure: 1, minUnique: 1, stackPassCatchers: 1, bringBack: true, randomness: 0,
    lockedPlayerIds: [], excludedPlayerIds: [], minExposureByPlayer: {}, maxExposureByPlayer: {}, ...over,
  };
}

function range(min: number | null, max: number | null) { return { minPct: min, maxPct: max }; }
function policy(playerId: number, over: Partial<PlayerExposurePolicy> = {}): PlayerExposurePolicy {
  return { playerId, overall: range(null, null), captain: range(null, null), flex: range(null, null), exactTargetMode: false, ...over };
}

function main() {
  // --- Units: count derivation with ceil(min)/floor(max) ---
  assert.equal(ceilMin(20, 0.12), 3, "ceil(20*0.12)=ceil(2.4)=3");
  assert.equal(floorMax(20, 0.12), 2, "floor(20*0.12)=floor(2.4)=2");
  const c = deriveExposureCounts(policy(1, { overall: range(0.12, 0.5) }), 20);
  assert.equal(c.overallMin, 3);
  assert.equal(c.overallMax, 10);

  // Validation rejects min>max and out-of-range.
  assert.throws(() => validateExposurePolicy(policy(1, { overall: range(0.6, 0.4) })), /exceeds its maximum/);
  assert.throws(() => validateExposurePolicy(policy(1, { flex: range(null, 1.5) })), /between 0% and 100%/);

  // --- P3-AC3: exact-target mode is only via the explicit flag, never a single field ---
  const plain = deriveExposureCounts(policy(1, { overall: range(0.25, 0.75) }), 20);
  assert.notEqual(plain.overallMin, plain.overallMax, "without the flag, min and max differ");
  const exact = deriveExposureCounts(policy(1, { overall: range(0.25, 0.75), exactTargetMode: true }), 20);
  assert.equal(exact.overallMin, exact.overallMax, "exact-target pins max down to min");

  // --- P3-AC1: 0% CPT + 20–50% Flex never places the player at Captain ---
  const p2Policy = policy(2, { captain: range(null, 0), flex: range(0.2, 0.5) });
  const aced = optimizeNflLineups(pool(), settings({ exposurePolicies: [p2Policy] }));
  assert.ok(aced.lineups.length > 0, "produces lineups");
  assert.ok(aced.lineups.every((l) => l.slots.find((s) => s.slot === "CPT")!.player.dkPlayerId !== 2), "player 2 never captains");
  const p2Report = aced.exposureReport!.find((r) => r.dkPlayerId === 2)!;
  assert.equal(p2Report.captain, 0, "zero captain appearances");

  // --- P3-AC2: a 12% minimum yields >=3 appearances; a 12% maximum permits <=2 ---
  const minRun = optimizeNflLineups(pool(), settings({ exposurePolicies: [policy(3, { overall: range(0.12, null) })] }));
  const minReport = minRun.exposureReport!.find((r) => r.dkPlayerId === 3)!;
  assert.ok(minReport.overall >= 3, `12% min over 20 lineups -> >=3 (got ${minReport.overall})`);

  const maxRun = optimizeNflLineups(pool(), settings({ exposurePolicies: [policy(3, { overall: range(null, 0.12) })] }));
  const maxReport = maxRun.exposureReport!.find((r) => r.dkPlayerId === 3)!;
  assert.ok(maxReport.overall <= 2, `12% max over 20 lineups -> <=2 (got ${maxReport.overall})`);

  // --- P3-AC4: infeasible plans are rejected before generation with conflicts named ---
  // Aggregate captain minimums exceed the number of lineups (only one CPT/lineup).
  const overCaptain = [policy(1, { captain: range(0.6, null) }), policy(2, { captain: range(0.6, null) })];
  const problems = detectExposureInfeasibility(overCaptain.map((p) => ({ ...p, overall: p.overall })), 20, new Set([1, 2]));
  assert.ok(problems.some((x) => x.reason === "CAPTAIN_MIN_AGGREGATE"), "aggregate captain min is detected");
  assert.throws(
    () => optimizeNflLineups(pool(), settings({ exposurePolicies: overCaptain })),
    /infeasible before generation/i,
    "the optimizer rejects the plan up front",
  );

  // A per-player overall min that exceeds captain+flex capacity is caught.
  const unreachable = detectExposureInfeasibility([policy(1, { overall: range(1, null), captain: range(null, 0), flex: range(null, 0) })], 20, new Set([1]));
  assert.ok(unreachable.some((x) => x.reason === "PLAYER_OVERALL_UNREACHABLE"));

  // --- P3-AC5: report preserves both requested and realized slot-specific exposures ---
  const r = maxRun.exposureReport!.find((x) => x.dkPlayerId === 3)!;
  assert.equal(typeof r.overallMax, "number");
  assert.equal(typeof r.captainMax, "number");
  assert.equal(typeof r.flexMax, "number");
  assert.equal(typeof r.overall, "number");

  console.log("NFL GPP Phase 3 (exposure ranges): P3-AC1..AC5 and derivation/infeasibility units passed.");
}

main();
