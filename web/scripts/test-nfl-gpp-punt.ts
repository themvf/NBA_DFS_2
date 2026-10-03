/**
 * Phase 1 tests (spec §8): role-aware no-punt policy.
 * Covers P1-AC1..P1-AC5 plus the punt-policy unit rules.
 */
import assert from "node:assert/strict";
import { optimizeNflLineups, DEFAULT_NFL_PUNT_POLICY, type NflOptimizerPlayer, type NflOptimizerSettings, type NflPuntPolicy } from "../src/app/dfs/nfl/nfl-optimizer";
import { evaluatePuntEligibility, validateNflPuntPolicy, type NflPlayerRoleEvidence, type PuntOverride } from "../src/lib/nfl-dfs/punt-policy";
import { rolePolicyEvidence, ROSTER_FRESH_MS, type Availability } from "../src/lib/nfl-dfs/availability";
import { deriveReceiverRoleChanges } from "../src/lib/nfl-dfs/receiver-role";

function player(over: Partial<NflOptimizerPlayer> & { dkPlayerId: number; salary: number }): NflOptimizerPlayer {
  return {
    id: over.dkPlayerId, dkPlayerId: over.dkPlayerId, captainDkPlayerId: over.dkPlayerId + 100_000,
    name: over.name ?? `P${over.dkPlayerId}`, position: over.position ?? "WR", team: over.team ?? "AAA",
    opponent: over.team === "BBB" ? "AAA" : "BBB", gameKey: "AAA@BBB", salary: over.salary,
    captainSalary: Math.round(over.salary * 1.5), isOut: over.isOut ?? false, projectionStatus: "historical",
    historyGames: over.historyGames ?? 6, ourProj: over.ourProj ?? 10, floorFpts: 7, ceilingFpts: 14, boomRate: 0.2,
    avgFptsDk: 10, fantasyprosProj: null, linestarProj: null, linestarOwnPct: null, customProj: null,
    depthRole: over.depthRole, roleConfidence: over.roleConfidence, projectedOpportunities: over.projectedOpportunities,
    availabilityState: over.availabilityState, dkStatus: over.dkStatus, availability: over.availability,
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
    player({ dkPlayerId: 300, salary: 2200, position: "WR", team: "AAA", depthRole: "Listed WR3",
      availabilityState: "probable", roleConfidence: 0.7, projectedOpportunities: 5, ourProj: 6 }),
    player({ dkPlayerId: 301, salary: 2400, position: "RB", team: "BBB", roleConfidence: 0.7, projectedOpportunities: 5, ourProj: 6 }),
  ];
  const capped = optimizeNflLineups(reliefPool, showdownSettings({ nLineups: 4 }));
  // The cap test must not pass vacuously: both relief players are ELIGIBLE.
  for (const id of [300, 301]) {
    const d = capped.eligibility!.find((e) => e.dkPlayerId === id)!;
    assert.equal(d.eligible, true, `relief player ${id} is eligible`);
    assert.equal(d.salaryRelief, true, `relief player ${id} counts as salary relief`);
  }
  for (const lineup of capped.lineups) {
    const relief = lineup.playerIds.filter((id) => id === 300 || id === 301).length;
    assert.ok(relief <= DEFAULT_NFL_PUNT_POLICY.maxSalaryReliefPlayersPerLineup, "no lineup exceeds the salary-relief cap");
  }

  // --- Regression (found in review): a cheap veteran in PRODUCTION shape ---
  // Observed history can still serve as weak role evidence for a back. It
  // cannot establish a receiver's current route share or depth-chart role.
  const productionShape = optimizeNflLineups(
    [...corePool(),
      player({ dkPlayerId: 600, salary: 2800, position: "RB", team: "BBB", historyGames: 6, ourProj: 7 }),
      player({ dkPlayerId: 601, salary: 2800, position: "RB", team: "BBB", historyGames: 0, ourProj: 7 })],
    showdownSettings());
  const veteran = productionShape.eligibility!.find((e) => e.dkPlayerId === 600)!;
  assert.equal(veteran.eligible, true, "cheap veteran with observed games is eligible without a feed");
  assert.equal(veteran.salaryRelief, true);
  const noHistory = productionShape.eligibility!.find((e) => e.dkPlayerId === 601)!;
  assert.equal(noHistory.eligible, false, "cheap no-history body still fails closed");
  assert.equal(noHistory.reasonCode, "ROLE_UNRESOLVED");

  // The $3,000 boundary previously skipped the role gate entirely. A fresh
  // WR4 and an unresolved receiver both fail despite old scoring history.
  const cheapReceivers = optimizeNflLineups(
    [...corePool(),
      player({ dkPlayerId: 610, name: "Tory Horton", salary: 3000, position: "WR", team: "BBB", historyGames: 9,
        depthRole: "Listed WR4", availabilityState: "probable", roleConfidence: 0.75, ourProj: 9.18 }),
      player({ dkPlayerId: 611, name: "Theo Wease Jr.", salary: 3000, position: "WR", team: "BBB", historyGames: 3,
        depthRole: null, availabilityState: "probable", roleConfidence: 0.5, ourProj: 7.78 }),
      player({ dkPlayerId: 612, name: "Current WR3", salary: 3000, position: "WR", team: "BBB", historyGames: 9,
        depthRole: "Listed WR3", availabilityState: "probable", roleConfidence: 0.75, ourProj: 8 })],
    showdownSettings());
  const decision = (id: number) => cheapReceivers.eligibility!.find((entry) => entry.dkPlayerId === id)!;
  assert.equal(decision(610).reasonCode, "ROLE_UNRESOLVED");
  assert.equal(decision(611).reasonCode, "ROLE_UNKNOWN");
  assert.equal(decision(612).eligible, true);
  assert.equal(decision(612).salaryRelief, true, "$3,000 now counts toward the cheap-player cap");
  assert.equal(cheapReceivers.lineups.some((lineup) => lineup.playerIds.includes(610) || lineup.playerIds.includes(611)), false);

  const replacementPool = [...corePool(),
    player({ dkPlayerId: 615, salary: 3000, position: "WR", team: "BBB", depthRole: "Listed WR4",
      availabilityState: "probable", historyGames: 9, ourProj: 9 }),
    player({ dkPlayerId: 616, salary: 6000, position: "WR", team: "BBB", depthRole: "Listed WR1",
      availabilityState: "probable", isOut: true, dkStatus: "OUT" })];
  const promoted = optimizeNflLineups(replacementPool, showdownSettings());
  const roleChanges = deriveReceiverRoleChanges(replacementPool);
  assert.deepEqual(roleChanges.get(615), {
    listedRank: 4, effectiveRank: 3,
    absentAhead: [{ playerId: 616, name: "P616", rank: 1, status: "OUT", source: "DraftKings status", capturedAt: null }],
  }, "the player-pool facet and gate share the same named absence evidence");
  assert.equal(promoted.eligibility!.find((entry) => entry.dkPlayerId === 615)?.eligible, true,
    "a documented WR1 absence promotes a listed WR4 to effective WR3");
  const officialInactive = optimizeNflLineups(replacementPool.map(p => p.dkPlayerId === 616
    ? { ...p, dkStatus: null, availability: { status: "INACTIVE" }, availabilityState: "confirmed" as const } : p), showdownSettings());
  assert.equal(officialInactive.eligibility!.find((entry) => entry.dkPlayerId === 615)?.eligible, true,
    "a current inactive report also promotes the next receiver");
  const uncertain = optimizeNflLineups(replacementPool.map(p => p.dkPlayerId === 616
    ? { ...p, isOut: false, dkStatus: "Q" } : p), showdownSettings());
  assert.equal(uncertain.eligibility!.find((entry) => entry.dkPlayerId === 615)?.reasonCode, "ROLE_UNRESOLVED",
    "questionable is not a verified absence");
  const unresolvedReplacement = optimizeNflLineups(replacementPool.map(p => p.dkPlayerId === 615
    ? { ...p, depthRole: null } : p), showdownSettings());
  assert.equal(unresolvedReplacement.eligibility!.find((entry) => entry.dkPlayerId === 615)?.reasonCode, "ROLE_UNKNOWN",
    "an unknown chart position cannot be promoted by subtraction");

  const unsupportedAllowlist = optimizeNflLineups(
    [...corePool(), player({ dkPlayerId: 613, salary: 3000, position: "WR", team: "BBB", historyGames: 9,
      depthRole: "Listed WR4", availabilityState: "probable" })],
    showdownSettings({ puntPolicy: { ...DEFAULT_NFL_PUNT_POLICY, allowlistedPlayerIds: [613] } }));
  assert.equal(unsupportedAllowlist.eligibility!.find((entry) => entry.dkPlayerId === 613)?.eligible, false,
    "an allowlist ID without a recorded role reason cannot bypass the gate");

  const verifiedPromotion = optimizeNflLineups(
    [...corePool(), player({ dkPlayerId: 614, salary: 3000, position: "WR", team: "BBB", historyGames: 9,
      depthRole: "Listed WR3", availabilityState: "stale", roleConfidence: 0.75 })],
    showdownSettings());
  assert.equal(verifiedPromotion.eligibility!.find((entry) => entry.dkPlayerId === 614)?.reasonCode, "EVIDENCE_STALE",
    "a former WR3 must have a current role decision");

  const roleOverride: PuntOverride[] = [{ playerId: 613, reason: "Verified injury replacement with first-team routes",
    user: "tester", at: "2026-10-03T12:00:00Z", slot: "FLEX" }];
  const admittedDepthReceiver = optimizeNflLineups(
    [...corePool(), player({ dkPlayerId: 613, salary: 3000, position: "WR", team: "BBB", historyGames: 9,
      depthRole: "Listed WR4", availabilityState: "probable" })],
    showdownSettings({ puntPolicy: { ...DEFAULT_NFL_PUNT_POLICY, allowlistedPlayerIds: [613] }, puntOverrides: roleOverride }));
  assert.equal(admittedDepthReceiver.eligibility!.find((entry) => entry.dkPlayerId === 613)?.eligible, true,
    "a recorded role-based override admits a verified replacement");

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

  // --- The stale-role gate the preset text promises actually fires (2026-09-29 audit) ---
  // Nothing set availabilityState before, so "stale role evidence fails closed" never ran.
  const at = Date.parse("2026-09-28T18:00:00Z");
  const roster = (capturedAt: string, over: Partial<Availability> = {}): Availability =>
    ({ role: "Listed WR3", status: "ACTIVE", source: "Sleeper roster", capturedAt, blockedReason: null, fresh: at - Date.parse(capturedAt) <= ROSTER_FRESH_MS, ...over });
  assert.deepEqual(rolePolicyEvidence(roster("2026-09-28T12:00:00Z"), at), { availabilityState: "probable", depthRole: "Listed WR3" });
  assert.deepEqual(rolePolicyEvidence(roster("2026-09-24T12:00:00Z"), at), { availabilityState: "stale", depthRole: "Listed WR3" });
  assert.equal(rolePolicyEvidence(roster("2026-09-28T12:00:00Z", { officialConfirmed: true }), at).availabilityState, "confirmed");
  assert.deepEqual(rolePolicyEvidence(roster("2026-09-24T12:00:00Z", { pinned: true, role: "Role unresolved" }), at), { availabilityState: "unknown", depthRole: null }, "an unresolved pinned decision is unknown, not stale");
  assert.equal(rolePolicyEvidence(undefined, at).availabilityState, "unknown");
  const staleCheap = player({ dkPlayerId: 60, salary: 2500, ourProj: 7, ...rolePolicyEvidence(roster("2026-09-24T12:00:00Z"), at) });
  const staleRun = optimizeNflLineups([...corePool(), staleCheap], showdownSettings());
  const staleDecision = staleRun.eligibility!.find((e) => e.dkPlayerId === 60)!;
  assert.equal(staleDecision.eligible, false);
  assert.equal(staleDecision.reasonCode, "EVIDENCE_STALE");
  const freshCheap = player({ dkPlayerId: 61, salary: 2500, ourProj: 7, ...rolePolicyEvidence(roster("2026-09-28T12:00:00Z"), at) });
  assert.equal(optimizeNflLineups([...corePool(), freshCheap], showdownSettings()).eligibility!.find((e) => e.dkPlayerId === 61)!.eligible, true, "fresh evidence clears the same player");

  console.log("NFL GPP Phase 1 (punt policy): P1-AC1..AC5, unit precedence and the stale-role gate passed.");
}

main();
