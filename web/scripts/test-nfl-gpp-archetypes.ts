/**
 * Phase 4 tests (spec §11): fade and game-script archetypes.
 * Covers P4-AC1..P4-AC5 plus compiler and quota-allocation units.
 */
import assert from "node:assert/strict";
import { optimizeNflLineups, type NflOptimizerPlayer, type NflOptimizerSettings } from "../src/app/dfs/nfl/nfl-optimizer";
import { compileArchetype, allocateArchetypeQuotas, ARCHETYPE_LABELS, isFadeArchetype, type ArchetypeSlateContext, type ArchetypeQuota } from "../src/lib/nfl-dfs/archetypes";

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
    player({ dkPlayerId: 1, salary: 11000, position: "QB", team: "AAA", ourProj: 22, linestarOwnPct: 40 }),
    player({ dkPlayerId: 2, salary: 9800, position: "WR", team: "AAA", ourProj: 18, linestarOwnPct: 35 }),
    player({ dkPlayerId: 3, salary: 8600, position: "RB", team: "AAA", ourProj: 15, linestarOwnPct: 20 }),
    player({ dkPlayerId: 4, salary: 7400, position: "WR", team: "BBB", ourProj: 13, linestarOwnPct: 12 }),
    player({ dkPlayerId: 5, salary: 6200, position: "TE", team: "BBB", ourProj: 11, linestarOwnPct: 8 }),
    player({ dkPlayerId: 6, salary: 5200, position: "RB", team: "BBB", ourProj: 10, linestarOwnPct: 6 }),
    player({ dkPlayerId: 7, salary: 4200, position: "WR", team: "AAA", ourProj: 9, linestarOwnPct: 5 }),
    player({ dkPlayerId: 8, salary: 3600, position: "QB", team: "BBB", ourProj: 16, linestarOwnPct: 4 }),
    player({ dkPlayerId: 9, salary: 3400, position: "K", team: "AAA", ourProj: 7, linestarOwnPct: 3 }),
    player({ dkPlayerId: 10, salary: 3000, position: "DST", team: "BBB", ourProj: 6, linestarOwnPct: 3 }),
  ];
}

function settings(over: Partial<NflOptimizerSettings> = {}): NflOptimizerSettings {
  return {
    format: "showdown", mode: "gpp", projectionSource: "our", allowDkFallback: true,
    nLineups: 5, minSalary: 0, maxExposure: 1, minUnique: 1, stackPassCatchers: 1, bringBack: true, randomness: 0,
    lockedPlayerIds: [], excludedPlayerIds: [], minExposureByPlayer: {}, maxExposureByPlayer: {},
    favoriteTeam: "AAA", underdogTeam: "BBB", ...over,
  };
}

function ctx(): ArchetypeSlateContext {
  return {
    players: pool().map((p) => ({ dkPlayerId: p.dkPlayerId, position: p.position, team: p.team, opponent: p.opponent, ownership: (p.linestarOwnPct ?? 0) / 100, captainEligible: true })),
    favoriteTeam: "AAA", underdogTeam: "BBB", ownershipValidated: true,
  };
}

function quota(id: ArchetypeQuota["archetypeId"], min: number, max: number): ArchetypeQuota {
  return { archetypeId: id, minLineups: min, maxLineups: max, enabled: true };
}

function main() {
  // --- Compiler unit: a fade MUST carry a beneficiary path (§11.2) ---
  const singleFade = compileArchetype("single_chalk_fade", ctx(), { fadePlayerIds: [2] });
  assert.ok(singleFade.beneficiaries.length >= 1, "single fade derives a beneficiary path");
  assert.deepEqual(singleFade.fadePlayerIds, [2]);
  assert.ok(isFadeArchetype("single_chalk_fade"));
  assert.ok(!isFadeArchetype("standard_ceiling"));
  assert.throws(() => compileArchetype("single_chalk_fade", ctx(), {}), /requires one player/);

  // Contrarian captain uses an ownership ceiling.
  const contrarian = compileArchetype("contrarian_captain", ctx(), { contrarianCaptainCeiling: 0.1 });
  assert.ok(contrarian.eligibleCaptainIds && contrarian.eligibleCaptainIds.every((id) => (ctx().players.find((p) => p.dkPlayerId === id)!.ownership ?? 1) <= 0.1));

  // --- Quota allocation unit ---
  const alloc = allocateArchetypeQuotas([quota("standard_ceiling", 1, 3), quota("single_chalk_fade", 1, 3), quota("contrarian_captain", 1, 3)], 5);
  assert.ok(alloc.ok);
  if (alloc.ok) assert.equal(alloc.allocation.reduce((s, a) => s + a.count, 0), 5, "allocation sums to the request");
  // Minimums over the request -> infeasible with a named reason.
  const bad = allocateArchetypeQuotas([quota("standard_ceiling", 4, 4), quota("single_chalk_fade", 4, 4)], 5);
  assert.equal(bad.ok, false);

  // --- P4-AC1: a five-lineup plan requesting distinct archetypes returns the requested counts ---
  const quotas = [quota("standard_ceiling", 2, 2), quota("single_chalk_fade", 2, 2), quota("contrarian_captain", 1, 1)];
  const run = optimizeNflLineups(pool(), settings({ ownershipCapability: "validated", archetypeQuotas: quotas, archetypeConfigs: { single_chalk_fade: { fadePlayerIds: [2] } } }));
  const labelCounts = new Map<string, number>();
  for (const l of run.lineups) labelCounts.set(l.archetype!.id, (labelCounts.get(l.archetype!.id) ?? 0) + 1);
  assert.equal(labelCounts.get("standard_ceiling"), 2, "2 standard-ceiling lineups");
  assert.equal(labelCounts.get("single_chalk_fade"), 2, "2 single-chalk-fade lineups");
  assert.equal(labelCounts.get("contrarian_captain"), 1, "1 contrarian-captain lineup");

  // Every lineup carries exactly one primary archetype label (P4-AC3 — no mislabeling).
  for (const l of run.lineups) {
    assert.ok(l.archetype, "every lineup has an archetype label");
    assert.ok(ARCHETYPE_LABELS[l.archetype!.id], "label resolves");
  }

  // --- P4-AC2: a single-chalk-fade lineup records the faded player and a satisfied beneficiary ---
  for (const l of run.lineups.filter((x) => x.archetype!.id === "single_chalk_fade")) {
    assert.ok(!l.playerIds.includes(2), "faded player 2 is absent");
    assert.deepEqual(l.archetype!.fadedPlayerIds, [2]);
    assert.ok(l.archetype!.beneficiariesSatisfied.length >= 1, "at least one beneficiary rule is satisfied and recorded");
  }

  // --- P4-AC3: a plain standard-ceiling lineup that merely omits a popular player is NOT labeled a fade ---
  const plain = optimizeNflLineups(pool(), settings({ nLineups: 3 }));
  assert.ok(plain.lineups.every((l) => l.archetype!.id === "standard_ceiling"), "no quota -> all standard ceiling, never a fade label");

  // --- P4-AC4: labels survive a serialize/deserialize round-trip (save/reload proxy) ---
  const roundTrip = JSON.parse(JSON.stringify(run.lineups));
  assert.equal(roundTrip[0].archetype.id, run.lineups[0].archetype!.id);

  // --- P4-AC5: every preset compiles to a deterministic constraint spec ---
  for (const id of ["standard_ceiling", "double_fade", "favorite_onslaught", "underdog_comeback", "low_scoring_k_dst"] as const) {
    const config = id === "double_fade" ? { fadePlayerIds: [2, 3] } : {};
    const a = compileArchetype(id, ctx(), config);
    const b = compileArchetype(id, ctx(), config);
    assert.deepEqual(a, b, `${id} compiles deterministically`);
  }
  // Regression (found in review): the team-count range is bound to the team the
  // archetype is ABOUT — underdog for comeback, favorite for onslaught — never
  // implicitly to settings.favoriteTeam.
  assert.equal(compileArchetype("underdog_comeback", ctx(), {}).teamCountRange?.team, "BBB", "comeback range binds to the underdog");
  assert.equal(compileArchetype("favorite_onslaught", ctx(), {}).teamCountRange?.team, "AAA", "onslaught range binds to the favorite");
  const comeback = optimizeNflLineups(pool(), settings({ nLineups: 2, archetypeQuotas: [quota("underdog_comeback", 2, 2)] }));
  for (const l of comeback.lineups) {
    assert.ok(l.playerIds.filter((id) => pool().find((p) => p.dkPlayerId === id)!.team === "BBB").length >= 2, "comeback lineup carries at least two underdog players");
  }

  // Low-scoring requires K/DST presence.
  const kdst = optimizeNflLineups(pool(), settings({ nLineups: 2, archetypeQuotas: [quota("low_scoring_k_dst", 2, 2)] }));
  for (const l of kdst.lineups) {
    assert.ok(l.slots.some((s) => s.player.position === "K" || s.player.position === "DST"), "low-scoring lineup includes a kicker or defense");
  }

  console.log("NFL GPP Phase 4 (archetypes): P4-AC1..AC5 and compiler/quota units passed.");
}

main();
