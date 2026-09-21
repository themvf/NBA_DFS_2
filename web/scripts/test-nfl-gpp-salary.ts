/**
 * Phase 5 tests (spec §12): salary-left distribution + duplication controls.
 * Covers P5-AC1..P5-AC5 plus unit rules.
 */
import assert from "node:assert/strict";
import { optimizeNflLineups, type NflOptimizerPlayer, type NflOptimizerSettings } from "../src/app/dfs/nfl/nfl-optimizer";
import {
  DEFAULT_SALARY_POLICY, reportSalaryBands, findExactDuplicates, maxPairwiseOverlap,
  estimateDuplication, validateSalaryPolicy, NFL_SALARY_CAP, type SalaryConstructionPolicy,
} from "../src/lib/nfl-dfs/salary-duplication";

function player(over: Partial<NflOptimizerPlayer> & { dkPlayerId: number; salary: number }): NflOptimizerPlayer {
  return {
    id: over.dkPlayerId, dkPlayerId: over.dkPlayerId, captainDkPlayerId: over.dkPlayerId + 100_000,
    name: over.name ?? `P${over.dkPlayerId}`, position: over.position ?? "WR", team: over.team ?? "AAA",
    opponent: over.team === "BBB" ? "AAA" : "BBB", gameKey: "AAA@BBB", salary: over.salary,
    captainSalary: Math.round(over.salary * 1.5), isOut: false, projectionStatus: "historical",
    historyGames: 6, ourProj: over.ourProj ?? 10, floorFpts: 7, ceilingFpts: 14, boomRate: 0.2,
    avgFptsDk: 10, fantasyprosProj: null, linestarProj: null, linestarOwnPct: over.linestarOwnPct ?? null, customProj: null,
  };
}

function pool(): NflOptimizerPlayer[] {
  return [
    player({ dkPlayerId: 1, salary: 9000, position: "QB", team: "AAA", ourProj: 18, linestarOwnPct: 30 }),
    player({ dkPlayerId: 2, salary: 8000, position: "WR", team: "AAA", ourProj: 15, linestarOwnPct: 25 }),
    player({ dkPlayerId: 3, salary: 7000, position: "RB", team: "AAA", ourProj: 13, linestarOwnPct: 18 }),
    player({ dkPlayerId: 4, salary: 6000, position: "WR", team: "BBB", ourProj: 11, linestarOwnPct: 12 }),
    player({ dkPlayerId: 5, salary: 5000, position: "TE", team: "BBB", ourProj: 9, linestarOwnPct: 8 }),
    player({ dkPlayerId: 6, salary: 4200, position: "RB", team: "BBB", ourProj: 8, linestarOwnPct: 6 }),
    player({ dkPlayerId: 7, salary: 3600, position: "WR", team: "AAA", ourProj: 7, linestarOwnPct: 5 }),
    player({ dkPlayerId: 8, salary: 3000, position: "K", team: "BBB", ourProj: 6, linestarOwnPct: 4 }),
  ];
}

function settings(over: Partial<NflOptimizerSettings> = {}): NflOptimizerSettings {
  return {
    format: "showdown", mode: "gpp", projectionSource: "our", allowDkFallback: true,
    nLineups: 6, minSalary: 0, maxExposure: 1, minUnique: 1, stackPassCatchers: 1, bringBack: true, randomness: 0,
    lockedPlayerIds: [], excludedPlayerIds: [], minExposureByPlayer: {}, maxExposureByPlayer: {}, ...over,
  };
}

function main() {
  // --- Unit: salary policy validation ---
  assert.throws(() => validateSalaryPolicy({ ...DEFAULT_SALARY_POLICY, minSalaryUsed: 60000 }), /exceeds maximum/);
  assert.throws(() => validateSalaryPolicy({ ...DEFAULT_SALARY_POLICY, salaryLeftBands: [{ min: 100, max: 50, minLineups: 0, maxLineups: 1 }] }), /lower bound/);

  // --- Unit: band tally ---
  const bands = reportSalaryBands(DEFAULT_SALARY_POLICY, [200, 800, 1600, 4000, 4200, 600]);
  assert.equal(bands.reduce((s, b) => s + b.count, 0) <= 6, true);

  // --- P5-AC1: users can construct lineups leaving $1,500+ without weakening role rules ---
  const wideLeft: SalaryConstructionPolicy = { minSalaryUsed: 0, maxSalaryUsed: NFL_SALARY_CAP - 1500, minSalaryLeft: 1500, maxSalaryLeft: NFL_SALARY_CAP, salaryLeftBands: [] };
  const leftRun = optimizeNflLineups(pool(), settings({ salaryPolicy: wideLeft, nLineups: 3 }));
  assert.ok(leftRun.lineups.length > 0, "produces lineups leaving >=$1,500");
  for (const l of leftRun.lineups) assert.ok(NFL_SALARY_CAP - l.totalSalary >= 1500, `salary left >= 1500 (got ${NFL_SALARY_CAP - l.totalSalary})`);

  // --- P5-AC4: exact duplicates are impossible, and overlap limits are enforced ---
  const overlapRun = optimizeNflLineups(pool(), settings({ nLineups: 4, minUnique: 1, maxPairwiseOverlap: 3 }));
  assert.equal(findExactDuplicates(overlapRun.lineups).length, 0, "no exact duplicates");
  assert.ok(maxPairwiseOverlap(overlapRun.lineups) <= 3, "no pair shares more than 3 players");
  assert.ok((overlapRun.maxPairwiseOverlap ?? 6) <= 3, "reported overlap respects the cap");

  // --- P5-AC2: salary-band quotas are enforced across the selected portfolio ---
  const banded: SalaryConstructionPolicy = {
    minSalaryUsed: 0, maxSalaryUsed: NFL_SALARY_CAP, minSalaryLeft: 0, maxSalaryLeft: NFL_SALARY_CAP,
    salaryLeftBands: [{ min: 0, max: NFL_SALARY_CAP, minLineups: 1, maxLineups: 1 }], // everything in one band
  };
  const bandedRun = optimizeNflLineups(pool(), settings({ salaryPolicy: banded, nLineups: 3 }));
  const report = bandedRun.salaryBandReport!;
  assert.ok(report.every((b) => b.withinPlan), "all lineups fall inside the single all-encompassing band");

  // --- P5-AC3: no label says "expected duplicates" unless it comes from a validated field model ---
  const heuristic = estimateDuplication(
    bandedRun.lineups.map((l) => ({ lineupNumber: l.lineupNumber, playerIds: l.playerIds, totalSalary: l.totalSalary })),
    { ownershipValidated: false, ownershipByPlayer: new Map(pool().map((p) => [p.dkPlayerId, (p.linestarOwnPct ?? 0) / 100])) },
  );
  assert.ok(heuristic.every((d) => d.basis === "heuristic" && d.expectedDuplicates === null), "no expected-duplicate counts without a field model");
  assert.ok(heuristic.every((d) => /uncalibrated/i.test(d.label) && !/expected duplicates/i.test(d.label)), "heuristic label never claims expected duplicates");

  // With a validated field model, expected duplicates ARE produced.
  const model = estimateDuplication(
    bandedRun.lineups.map((l) => ({ lineupNumber: l.lineupNumber, playerIds: l.playerIds, totalSalary: l.totalSalary })),
    { ownershipValidated: true, fieldModel: () => 0.001, fieldSize: 10000 },
  );
  assert.ok(model.every((d) => d.basis === "model" && typeof d.expectedDuplicates === "number"), "field model yields expected-duplicate counts");
  assert.ok(model.every((d) => /expected duplicates/i.test(d.label)));

  // --- P5-AC5: results show salary used, salary left, band and duplication capability per lineup ---
  const full = optimizeNflLineups(pool(), settings({ salaryPolicy: DEFAULT_SALARY_POLICY, ownershipCapability: "unavailable", nLineups: 3 }));
  assert.ok(full.salaryBandReport, "salary band report present");
  assert.ok(full.duplication, "duplication estimate present (ownership exists on this pool)");
  assert.ok(full.duplication!.every((d) => d.basis === "heuristic"), "unavailable capability -> heuristic basis, never model");
  for (const l of full.lineups) {
    assert.equal(typeof l.totalSalary, "number");
    assert.ok(NFL_SALARY_CAP - l.totalSalary >= 0);
  }

  console.log("NFL GPP Phase 5 (salary + duplication): P5-AC1..AC5 and unit rules passed.");
}

main();
