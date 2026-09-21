/**
 * Balanced archetype plan: the default "just build me a portfolio" mode.
 * The generator allocates the archetype mix, auto-selects fade targets
 * (ownership when present, projection as the chalk proxy otherwise), and
 * folds any archetype whose prerequisite the slate cannot satisfy into
 * Standard ceiling — it never throws for a missing input the user was not
 * asked for. Explicit quotas always win, and Classic never auto-plans.
 */
import assert from "node:assert/strict";
import { balancedArchetypePlan, autoFadeCandidates, type ArchetypeSlateContext } from "../src/lib/nfl-dfs/archetypes";
import { optimizeNflLineups, type NflOptimizerPlayer, type NflOptimizerSettings } from "../src/app/dfs/nfl/nfl-optimizer";

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

function ctx(over: Partial<ArchetypeSlateContext> = {}): ArchetypeSlateContext {
  return {
    players: pool().map((p) => ({ dkPlayerId: p.dkPlayerId, position: p.position, team: p.team, opponent: p.opponent, ownership: (p.linestarOwnPct ?? 0) / 100, projection: p.ourProj, captainEligible: true })),
    favoriteTeam: "AAA", underdogTeam: "BBB", ownershipValidated: false, ...over,
  };
}

function settings(over: Partial<NflOptimizerSettings> = {}): NflOptimizerSettings {
  return {
    format: "showdown", mode: "gpp", projectionSource: "our", allowDkFallback: true,
    nLineups: 8, minSalary: 0, maxExposure: 1, minUnique: 1, stackPassCatchers: 1, bringBack: true, randomness: 0,
    lockedPlayerIds: [], excludedPlayerIds: [], minExposureByPlayer: {}, maxExposureByPlayer: {},
    archetypeMode: "balanced", favoriteTeam: "AAA", underdogTeam: "BBB", ...over,
  };
}

function labelCounts(run: ReturnType<typeof optimizeNflLineups>): Map<string, number> {
  const m = new Map<string, number>();
  for (const l of run.lineups) m.set(l.archetype!.id, (m.get(l.archetype!.id) ?? 0) + 1);
  return m;
}

function main() {
  // --- Auto-fade selection ---
  // With ownership present, the chalkiest players win; K/DST are never fades.
  assert.deepEqual(autoFadeCandidates(ctx(), 2), [1, 2], "fades = top-owned skill players");
  // Without any ownership, projection is the chalk proxy.
  const noOwn = ctx({ players: ctx().players.map((p) => ({ ...p, ownership: null })) });
  assert.deepEqual(autoFadeCandidates(noOwn, 2), [1, 2], "projection proxy picks the same studs here");
  assert.ok(!autoFadeCandidates(ctx(), 10).some((id) => id === 9 || id === 10), "K/DST never fade targets");

  // --- Balanced plan allocation ---
  const full = balancedArchetypePlan(ctx(), 20);
  assert.equal(full.quotas.reduce((s, q) => s + q.minLineups, 0), 20, "counts sum to the request");
  assert.deepEqual(full, balancedArchetypePlan(ctx(), 20), "plan is deterministic");
  const ids = full.quotas.map((q) => q.archetypeId);
  for (const id of ["standard_ceiling", "single_chalk_fade", "double_fade", "favorite_onslaught", "underdog_comeback", "low_scoring_k_dst"]) {
    assert.ok(ids.includes(id as typeof ids[number]), `${id} present at n=20`);
  }
  assert.ok(!ids.includes("contrarian_captain"), "contrarian captain is excluded (no-op without validated ownership)");
  assert.deepEqual(full.configs.single_chalk_fade?.fadePlayerIds, [1]);
  assert.deepEqual(full.configs.double_fade?.fadePlayerIds, [1, 2]);

  // Missing favorite: game-script archetypes fold into Standard, with a note.
  const noFav = balancedArchetypePlan(ctx({ favoriteTeam: null, underdogTeam: null }), 20);
  assert.equal(noFav.quotas.reduce((s, q) => s + q.minLineups, 0), 20);
  assert.ok(!noFav.quotas.some((q) => q.archetypeId === "favorite_onslaught" || q.archetypeId === "underdog_comeback"));
  assert.ok(noFav.notes.some((n) => /favorite unknown/i.test(n)), "the fold is disclosed");

  // Tiny request degrades to Standard ceiling.
  const one = balancedArchetypePlan(ctx(), 1);
  assert.deepEqual(one.quotas.map((q) => [q.archetypeId, q.minLineups]), [["standard_ceiling", 1]]);

  // --- Optimizer integration: balanced mode with zero manual configuration ---
  const run = optimizeNflLineups(pool(), settings());
  assert.equal(run.lineups.length, 8, "balanced plan generates the full request");
  const counts = labelCounts(run);
  assert.ok(counts.size >= 3, `mixed portfolio, got: ${[...counts.keys()].join(", ")}`);
  for (const l of run.lineups.filter((x) => x.archetype!.id === "single_chalk_fade" || x.archetype!.id === "double_fade")) {
    assert.ok(!l.playerIds.includes(1), "auto-faded player 1 is absent from fade lineups");
    assert.ok(l.archetype!.beneficiariesSatisfied.length >= 1, "fade lineups satisfy a beneficiary path");
  }
  assert.ok(run.warnings.some((w) => /^Balanced plan:/.test(w)), "auto choices are disclosed in the run warnings");
  const replay = optimizeNflLineups(pool(), settings());
  assert.deepEqual(replay.lineups.map((l) => l.playerIds), run.lineups.map((l) => l.playerIds), "balanced runs are reproducible");

  // Without a favorite, balanced still runs — no game-script labels, no throw.
  const noFavRun = optimizeNflLineups(pool(), settings({ favoriteTeam: undefined, underdogTeam: undefined }));
  assert.equal(noFavRun.lineups.length, 8);
  assert.ok(![...labelCounts(noFavRun).keys()].some((id) => id === "favorite_onslaught" || id === "underdog_comeback"));

  // Explicit quotas always win over balanced mode.
  const explicit = optimizeNflLineups(pool(), settings({ nLineups: 3, archetypeQuotas: [{ archetypeId: "standard_ceiling", minLineups: 3, maxLineups: 3, enabled: true }] }));
  assert.ok(explicit.lineups.every((l) => l.archetype!.id === "standard_ceiling"), "explicit quotas override the balanced mix");

  // Classic never auto-plans: game-script constraints do not exist there.
  const classicPool = [
    ...["QB","RB","RB","WR","WR","WR","TE","DST"].map((pos, i) => player({ dkPlayerId: 100 + i, salary: 6000 - i * 200, position: pos as NflOptimizerPlayer["position"], team: i % 2 ? "AAA" : "BBB", ourProj: 15 - i })),
    ...["QB","RB","WR","TE","DST","RB","WR","WR"].map((pos, i) => player({ dkPlayerId: 200 + i, salary: 5000 - i * 200, position: pos as NflOptimizerPlayer["position"], team: i % 2 ? "BBB" : "AAA", ourProj: 12 - i })),
  ];
  const classic = optimizeNflLineups(classicPool, settings({ format: "classic", nLineups: 2, minUnique: 1, stackPassCatchers: 0, bringBack: false }));
  assert.ok(classic.lineups.every((l) => l.archetype!.id === "standard_ceiling"), "classic + balanced stays Standard ceiling");

  console.log("NFL balanced plan: auto-fades, allocation, folding, optimizer integration, precedence and classic gate passed.");
}

main();
