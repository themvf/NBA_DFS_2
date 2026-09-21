/**
 * Phase 1 tests (spec §8): role-aware no-punt policy.
 * Covers P1-AC1..P1-AC5 plus the punt-policy unit rules.
 */
import assert from "node:assert/strict";
import { optimizeNflLineups, DEFAULT_NFL_PUNT_POLICY, type NflOptimizerPlayer, type NflOptimizerSettings, type NflPuntPolicy } from "../src/app/dfs/nfl/nfl-optimizer";
import { evaluatePuntEligibility, validateNflPuntPolicy, type NflPlayerRoleEvidence, type PuntOverride } from "../src/lib/nfl-dfs/punt-policy";

function player(over: Partial<NflOptimizerPlayer> & { dkPlayerId: number; salary: number }): NflOptimizerPlayer {
  return {
    id: over.dkPlayerId, dkPlayerId: over.dkPlayerId, captainDkPlayerId: over.dkPlayerId + 100_000,
    name: over.name ?? `P${over.dkPlayerId}`, position: over.position ?? "WR", team: over.team ?? "AAA",
    opponent: over.team === "BBB" ? "AAA" : "BBB", gameKey: "AAA@BBB", salary: over.salary,
    captainSalary: Math.round(over.salary * 1.5), isOut: over.isOut ?? false, projectionStatus: "historical",
    historyGames: over.historyGames ?? 6, ourProj: over.ourProj ?? 10, floorFpts: 7, ceilingFpts: 14, boomRate: 0.2,
    avgFptsDk: 10, fantasyprosProj: null, linestarProj: null, linestarOwnPct: null, customProj: null,
    depthRole: over.depthRole, roleConfidence: over.roleConfidence, projectedOpportunities: over.projectedOpportunities,
    availabilityState: over.availabilityState,
  };
}

function evidence(over: Partial<NflPlayerRoleEvidence> & { playerId: number }): NflPlayerRoleEvidence {
  // "in" checks preserve an explicit null (unknown role) rather than coalescing
  // it back to a confident default.
  return {
    playerId: over.playerId,
    verifiedActive: "verifiedActive" in over ? over.verifiedActive! : true,
    availabilityState: over.availabilityState ?? "confirmed",
    depthRole: "depthRole" in over ? over.depthRole! : "WR1",
    roleConfidence: "roleConfidence" in over ? over.roleConfidence! : 0.8,
    projectedOpportunities: "projectedOpportunities" in over ? over.projectedOpportunities! : 6,
    opportunityUnit: "target", observedGameCount: over.observedGameCount ?? 6, sourceIds: [], evidenceAsOf: null,
  };
}

function showdownSettings(over: Partial<NflOptimizerSettings> = {}): NflOptimizerSettings {
  return {
    format: "showdown", mode: "gpp", projectionSource: "our", allowDkFallback: true,
    nLineups: 3, minSalary: 0, maxExposure: 1, minUnique: 1, stackPassCatchers: 1, bringBack: true, randomness: 0,
    lockedPlayerIds: [], excludedPlayerIds: [], minExposureByPlayer: {}, maxExposureByPlayer: {},
    puntPolicy: DEFAULT_NFL_PUNT_POLICY, ...over,
  };
}

/** A legal Showdown pool of full-priced, role-qualified players. */
function corePool(): NflOptimizerPlayer[] {
  return [
    player({ dkPlayerId: 1, salary: 10000, position: "QB", team: "AAA", ourProj: 20 }),
    player({ dkPlayerId: 2, salary: 9000, position: "WR", team: "AAA", ourProj: 16 }),
    player({ dkPlayerId: 3, salary: 8000, position: "RB", team: "AAA", ourProj: 14 }),
    player({ dkPlayerId: 4, salary: 7000, position: "WR", team: "BBB", ourProj: 12 }),
    player({ dkPlayerId: 5, salary: 6000, position: "TE", team: "BBB", ourProj: 10 }),
    player({ dkPlayerId: 6, salary: 5000, position: "RB", team: "BBB", ourProj: 9 }),
    player({ dkPlayerId: 7, salary: 4200, position: "WR", team: "AAA", ourProj: 8 }),
  ];
}

function main() {
  // --- Policy validation ---
  assert.throws(() => validateNflPuntPolicy({ ...DEFAULT_NFL_PUNT_POLICY, minimumRoleConfidence: 2 }), /confidence/i);
  assert.throws(() => validateNflPuntPolicy({ ...DEFAULT_NFL_PUNT_POLICY, mode: "bogus" as unknown as NflPuntPolicy["mode"] }), /mode/i);

  // --- Unit: reason precedence ---
  const cheap = { dkPlayerId: 99, salary: 400, isOut: false };
  // P1-AC1: a $400 unknown-role player is blocked under the default preset.
  const d1 = evaluatePuntEligibility(cheap, evidence({ playerId: 99, roleConfidence: null, depthRole: null }), DEFAULT_NFL_PUNT_POLICY);
  assert.equal(d1.eligible, false);
  assert.equal(d1.eligible === false && d1.reason, "ABSOLUTE_SALARY_BLOCK");

  // Inactivity outranks an allowlist entry.
  const d2 = evaluatePuntEligibility({ dkPlayerId: 99, salary: 2000, isOut: true }, evidence({ playerId: 99 }), { ...DEFAULT_NFL_PUNT_POLICY, allowlistedPlayerIds: [99] });
  assert.equal(d2.eligible === false && d2.reason, "INACTIVE");

  // P1-AC2: a sub-$3,000 player with validated opportunity is role-qualified without override.
  const d3 = evaluatePuntEligibility({ dkPlayerId: 50, salary: 2500, isOut: false }, evidence({ playerId: 50, roleConfidence: 0.6, projectedOpportunities: 4 }), DEFAULT_NFL_PUNT_POLICY);
  assert.equal(d3.eligible, true);
  assert.equal(d3.eligible === true && d3.salaryRelief, true, "sub-threshold player is salary relief");

  // A cheap unknown-role player fails closed (ROLE_UNKNOWN), not silently admitted.
  const d4 = evaluatePuntEligibility({ dkPlayerId: 51, salary: 2500, isOut: false }, evidence({ playerId: 51, roleConfidence: null, depthRole: null }), DEFAULT_NFL_PUNT_POLICY);
  assert.equal(d4.eligible === false && d4.reason, "ROLE_UNKNOWN");

  // --- P1-AC1 in the optimizer: a $400 body cannot enter a lineup ---
  const withBody = optimizeNflLineups([...corePool(), player({ dkPlayerId: 200, salary: 400, position: "WR", team: "AAA", historyGames: 0 })], showdownSettings());
  assert.ok(withBody.lineups.every((l) => !l.playerIds.includes(200)), "the $400 body never enters a lineup");
  const bodyDecision = withBody.eligibility!.find((e) => e.dkPlayerId === 200)!;
  assert.equal(bodyDecision.eligible, false);
  assert.equal(bodyDecision.reasonCode, "ABSOLUTE_SALARY_BLOCK");

  // --- P1-AC4: salary-relief cap is a lineup constraint ---
  // Two cheap-but-qualified salary-relief players; default cap is 1 per lineup.
  const reliefPool = [
    ...corePool(),
    player({ dkPlayerId: 300, salary: 2200, position: "WR", team: "AAA", roleConfidence: 0.7, projectedOpportunities: 5, ourProj: 6 }),
    player({ dkPlayerId: 301, salary: 2400, position: "RB", team: "BBB", roleConfidence: 0.7, projectedOpportunities: 5, ourProj: 6 }),
  ];
  const capped = optimizeNflLineups(reliefPool, showdownSettings({ nLineups: 4 }));
  for (const lineup of capped.lineups) {
    const relief = lineup.playerIds.filter((id) => id === 300 || id === 301).length;
    assert.ok(relief <= DEFAULT_NFL_PUNT_POLICY.maxSalaryReliefPlayersPerLineup, "no lineup exceeds the salary-relief cap");
  }

  // --- P1-AC3/§8.3: an allowlisted cheap player is admitted, Flex-only by default ---
  const overrides: PuntOverride[] = [{ playerId: 400, reason: "Active as returner/RB3 with a verified package", user: "tester", at: "2026-09-20T12:00:00Z", slot: "FLEX" }];
  const allowPool = [...corePool(), player({ dkPlayerId: 400, salary: 600, position: "RB", team: "AAA", historyGames: 0, ourProj: 5 })];
  const allowed = optimizeNflLineups(allowPool, showdownSettings({ puntPolicy: { ...DEFAULT_NFL_PUNT_POLICY, allowlistedPlayerIds: [400] }, puntOverrides: overrides }));
  const allowDecision = allowed.eligibility!.find((e) => e.dkPlayerId === 400)!;
  assert.equal(allowDecision.eligible, true, "allowlisted cheap player is eligible");
  assert.equal(allowDecision.overridden, true);
  assert.equal(allowDecision.captainEligible, false, "override is Flex-only without a CPT override");
  // He must never appear at Captain.
  assert.ok(allowed.lineups.every((l) => l.slots.find((s) => s.slot === "CPT")!.player.dkPlayerId !== 400), "overridden cheap player never captains");

  // A CPT override lifts the captain restriction.
  const cptOverrides: PuntOverride[] = [{ playerId: 400, reason: "Verified featured role", user: "tester", at: "2026-09-20T12:00:00Z", slot: "CPT" }];
  const cptAllowed = optimizeNflLineups(allowPool, showdownSettings({ puntPolicy: { ...DEFAULT_NFL_PUNT_POLICY, allowlistedPlayerIds: [400] }, puntOverrides: cptOverrides }));
  assert.equal(cptAllowed.eligibility!.find((e) => e.dkPlayerId === 400)!.captainEligible, true, "CPT override makes the player captain-eligible");

  // --- P1-AC5: a locked but ineligible player yields a readable infeasibility error ---
  assert.throws(
    () => optimizeNflLineups([...corePool(), player({ dkPlayerId: 500, salary: 300, position: "WR", team: "AAA", historyGames: 0 })], showdownSettings({ lockedPlayerIds: [500] })),
    /locked but ineligible/i,
    "locking a $300 body produces a readable error, not zero lineups",
  );

  console.log("NFL GPP Phase 1 (punt policy): P1-AC1..AC5 and unit precedence passed.");
}

main();
