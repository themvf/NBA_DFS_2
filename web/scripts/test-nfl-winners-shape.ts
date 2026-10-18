/**
 * Winners shape (`nfl-winners-shape-v1`..`v8`): the preset resolves into ordinary
 * optimizer settings and pool exclusions, and the optimizer enforces each of
 * them: the bring-back SHARE alternates lineups, the quarterback cap binds,
 * kickers leave a showdown pool, a showdown lineup is 5-1 or 3-3 and never
 * 4-2, and the captain cap holds. Nothing here changes a projection.
 */
import assert from "node:assert/strict";
import { dstFacesOwnOffense, optimizeNflLineups, type NflOptimizerPlayer, type NflOptimizerSettings } from "../src/app/dfs/nfl/nfl-optimizer";
import { applyWinnersShape, qbExposureCap, winnersShapeLimits, WINNERS_SHAPE, WINNERS_SHAPE_VERSION } from "../src/lib/nfl-dfs/winners-shape";

// --- Units: the quarterback cap follows the lineup count ---
assert.equal(qbExposureCap(20), 0.35, "20 lineups -> 3 quarterbacks -> 7 lineups each -> 35%");
assert.equal(qbExposureCap(100), 0.15, "100 lineups -> 16 quarterbacks -> 7 each -> 7%, floored at 15%");
assert.equal(qbExposureCap(10), 0.5, "10 lineups -> 2 quarterbacks -> 50%");
assert.equal(qbExposureCap(1), 1, "one lineup: the cap cannot bind");

function player(over: Partial<NflOptimizerPlayer> & { dkPlayerId: number; position: NflOptimizerPlayer["position"]; team: string }): NflOptimizerPlayer {
  const opponent = over.opponent ?? ({ AAA: "BBB", BBB: "AAA", CCC: "DDD", DDD: "CCC" } as Record<string, string>)[over.team];
  const gameKey = over.gameKey ?? ([over.team, opponent].sort().join("@"));
  return {
    id: over.dkPlayerId, dkPlayerId: over.dkPlayerId, captainDkPlayerId: over.dkPlayerId + 100_000,
    name: over.name ?? `${over.position}${over.dkPlayerId}`, position: over.position, team: over.team, opponent, gameKey,
    salary: over.salary ?? 5000, captainSalary: Math.round((over.salary ?? 5000) * 1.5), isOut: false, projectionStatus: "historical",
    historyGames: 6, ourProj: over.ourProj ?? 10, floorFpts: 6, ceilingFpts: over.ceilingFpts ?? (over.ourProj ?? 10) * 1.8, boomRate: 0.2,
    avgFptsDk: 10, fantasyprosProj: null, linestarProj: null, linestarOwnPct: null, customProj: null,
    priorOpportunity: over.priorOpportunity ?? null, ...(over.availabilityStatus ? { availabilityStatus: over.availabilityStatus } : {}),
    ...(over.depthRole ? { depthRole: over.depthRole } : {}), ...(over.availability ? { availability: over.availability } : {}),
  };
}
function settings(over: Partial<NflOptimizerSettings> = {}): NflOptimizerSettings {
  return {
    format: "classic", mode: "gpp", projectionSource: "our", allowDkFallback: true,
    nLineups: 4, minSalary: 45000, maxExposure: 0.6, minUnique: 2, stackPassCatchers: 0, bringBack: true, randomness: 0,
    lockedPlayerIds: [], excludedPlayerIds: [], minExposureByPlayer: {}, maxExposureByPlayer: {}, ...over,
  };
}
// --- The page's Lineup rules summary reads the same limits the build enforces ---
for (const over of [{}, { format: "showdown" as const }, { minSalary: 49_800, maxExposure: 0.4, minUnique: 5 }, { nLineups: 100 }]) {
  const s = settings(over);
  const built = applyWinnersShape(s, []).settings;
  const shown = winnersShapeLimits(s);
  assert.equal(shown.minSalary, built.minSalary, "summary min salary = enforced");
  assert.equal(shown.maxExposure, built.maxExposure, "summary max exposure = enforced");
  assert.equal(shown.minUnique, built.minUnique, "summary min unique = enforced");
  assert.equal(shown.maxQbExposure ?? undefined, built.maxQbExposure, "summary QB cap = enforced");
  assert.equal(shown.maxCaptainExposure ?? undefined, built.maxCaptainExposure, "summary captain cap = enforced");
}

const prior = (games: number, targets: number, targetShare: number | null = null, carries = games * 10) => ({ games, targets, carries, targetShare });

// --- The preset's pool rules name every player they leave out, and skip a rule that would leave a position too thin ---
{
  const pool = [
    ...[0, 1, 2, 3, 4].map(i => player({ dkPlayerId: 10 + i, position: "RB", team: "AAA", priorOpportunity: prior(3, 15) })),   // 5.0 targets/g
    ...[0, 1, 2].map(i => player({ dkPlayerId: 20 + i, position: "RB", team: "BBB", priorOpportunity: prior(3, 6) })),          // 2.0 targets/g
    player({ dkPlayerId: 23, position: "RB", team: "BBB", priorOpportunity: null }),                                            // no record
    player({ dkPlayerId: 24, position: "RB", team: "BBB", priorOpportunity: prior(3, 6, null, 60) }),                            // 2 targets but 22 touches/g: a bell-cow
    ...[0, 1, 2, 3, 4, 5].map(i => player({ dkPlayerId: 30 + i, position: "WR", team: "AAA", salary: 7000, priorOpportunity: prior(3, 30, 0.3) })),
    player({ dkPlayerId: 36, position: "WR", team: "BBB", salary: 4500, priorOpportunity: prior(3, 6, 0.08) }),                 // cheap: kept
    player({ dkPlayerId: 37, position: "WR", team: "BBB", salary: 6200, priorOpportunity: prior(3, 9, 0.12) }),                 // out
    player({ dkPlayerId: 40, position: "TE", team: "AAA", salary: 3500, priorOpportunity: prior(3, 15, 0.2) }),                 // the only qualifying TE
    player({ dkPlayerId: 41, position: "TE", team: "BBB", salary: 5500, priorOpportunity: prior(3, 18, 0.2) }),
    player({ dkPlayerId: 42, position: "TE", team: "BBB", salary: 3200, priorOpportunity: prior(3, 3, 0.05) }),
    player({ dkPlayerId: 50, position: "DST", team: "AAA", salary: 2500 }), player({ dkPlayerId: 51, position: "DST", team: "BBB", salary: 2900 }),
    player({ dkPlayerId: 52, position: "DST", team: "CCC", salary: 3600 }),
  ];
  const shaped = applyWinnersShape(settings({ lockedPlayerIds: [20], buildShape: "winners" }), pool);
  assert.equal(shaped.settings.minUnique, WINNERS_SHAPE.minUnique);
  assert.equal(shaped.settings.maxExposure, WINNERS_SHAPE.maxExposure, "the flat cap tightens to 55%");
  assert.equal(shaped.settings.minSalary, WINNERS_SHAPE.minSalary, "$45,000 rises to the preset floor");
  assert.equal(shaped.settings.stackPassCatchers, 1, "a build with no stack rule gets one stacked pass catcher");
  assert.equal(shaped.settings.bringBackShare, 0.5);
  assert.equal(shaped.settings.maxQbExposure, qbExposureCap(4));
  assert.equal(shaped.settings.buildShape, "winners", "the preset stays on the saved settings");
  const out = [...shaped.excluded.keys()].sort((a, b) => a - b);
  assert.deepEqual(out, [21, 22, 23, 37, 52], "two low-target backs, the back with no record, the thin receiver and the dear defense");
  assert.ok(!shaped.excluded.has(24), "a bell-cow with 2 targets a game stays on top-quartile touches (v2)");
  assert.match(shaped.excluded.get(21)!, /12\.0 touches and 2\.0 targets a game/);
  assert.ok(!shaped.excluded.has(20), "a locked back below the rule is kept: locks always win");
  assert.match(shaped.excluded.get(21)!, /over the prior 3 game\(s\)/);
  assert.match(shaped.excluded.get(23)!, /no prior-week opportunity record/);
  assert.match(shaped.excluded.get(37)!, /12% target share at \$6,200/);
  assert.match(shaped.excluded.get(52)!, /\$3,600; the preset keeps defenses under \$3,000/);
  assert.ok(shaped.notes.some(n => /only 1 TE\(s\) meet the rule/.test(n) && /TE rule was not applied/.test(n)), "a rule that leaves one tight end is skipped and said");
  assert.ok(!shaped.excluded.has(41) && !shaped.excluded.has(42), "the skipped TE rule leaves every tight end in");
  assert.ok(shaped.notes[0].startsWith(`Winners shape (${WINNERS_SHAPE_VERSION}, classic)`));
  assert.ok(shaped.notes.some(n => /RB pool kept 7 of 10/.test(n)), `the locked back and the bell-cow count as kept: ${shaped.notes.join(" | ")}`);
  assert.equal(settings().minUnique, 2, "the caller's settings object is not mutated");

  const noData = applyWinnersShape(settings(), pool.map(p => ({ ...p, priorOpportunity: null })));
  assert.ok(noData.notes.some(n => /no prior-week opportunity data for any RB/.test(n)), "no data at all: the rule is not applied, and that is said");
  assert.ok(!noData.excluded.has(21));
  assert.ok(noData.excluded.has(52), "the salary-only DST rule still applies");

  const cash = applyWinnersShape(settings({ mode: "cash" }), pool);
  assert.equal(cash.settings.minUnique, 2); assert.equal(cash.excluded.size, 0);
  assert.match(cash.notes[0], /GPP builds only/);

  const showdown = applyWinnersShape(settings({ format: "showdown" }), pool);
  assert.equal(showdown.settings.excludeKickers, true); assert.equal(showdown.settings.forbidFourTwoSplit, true);
  assert.equal(showdown.settings.maxCaptainExposure, WINNERS_SHAPE.captainMaxExposure);
  assert.deepEqual(showdown.settings.captainPositions, ["QB", "RB", "WR"]);
  assert.equal(showdown.settings.minUnique, 2, "showdown keeps the user's min unique");
  assert.equal(showdown.excluded.size, 0, "the classic pool rules do not run on a showdown");
}
console.log("Winners shape: the preset resolves to named settings and named exclusions.");

// --- Classic integration: half the lineups carry a bring-back, each quarterback is capped, every DST over $3,000 is out ---
{
  const pool: NflOptimizerPlayer[] = [];
  let id = 100;
  const teams = ["AAA", "BBB", "CCC", "DDD"];
  for (const team of teams) {
    const t = teams.indexOf(team);
    pool.push(player({ dkPlayerId: id++, position: "QB", team, salary: 5800 + 350 * t, ourProj: 20 + t }));
    for (let i = 0; i < 4; i++) pool.push(player({ dkPlayerId: id++, position: "RB", team, salary: 3900 + 650 * i + 50 * t, ourProj: 9 + 2.5 * i }));
    for (let i = 0; i < 5; i++) pool.push(player({ dkPlayerId: id++, position: "WR", team, salary: 3700 + 850 * i + 50 * t, ourProj: 8 + 3 * i }));
    for (let i = 0; i < 3; i++) pool.push(player({ dkPlayerId: id++, position: "TE", team, salary: 2800 + 900 * i + 50 * t, ourProj: 6 + 3 * i }));
    pool.push(player({ dkPlayerId: id++, position: "DST", team, salary: team === "DDD" ? 3400 : 2400 + 100 * teams.indexOf(team), ourProj: 7 }));
  }
  const n = 4;
  const result = optimizeNflLineups(pool, settings({ nLineups: n, buildShape: "winners" }));
  assert.equal(result.lineups.length, n, `built ${result.lineups.length}: ${result.warnings.join(" | ")}`);
  const bringBacks = result.lineups.map(l => l.stackSummary.bringBack != null);
  assert.deepEqual(bringBacks, [true, false, true, false], "a 50% share alternates: required, forbidden, required, forbidden");
  for (const lineup of result.lineups) {
    const qb = lineup.slots.find(s => s.player.position === "QB")!.player;
    assert.ok(lineup.slots.some(s => ["WR", "TE"].includes(s.player.position) && s.player.team === qb.team), "every lineup stacks the quarterback");
    if (!lineup.stackSummary.bringBack) assert.ok(!lineup.slots.some(s => ["RB", "WR", "TE"].includes(s.player.position) && s.player.team === qb.opponent), "a forbidden lineup has no opposing skill player");
    assert.ok(lineup.totalSalary >= WINNERS_SHAPE.minSalary, `salary ${lineup.totalSalary} under the preset's floor`);
    assert.ok(!lineup.slots.some(s => s.player.position === "DST" && s.player.salary > WINNERS_SHAPE.dstMaxSalary), "no dear defense");
  }
  for (let i = 0; i < n; i++) for (let j = i + 1; j < n; j++) {
    const shared = result.lineups[i].playerIds.filter(p => result.lineups[j].playerIds.includes(p)).length;
    assert.ok(9 - shared >= WINNERS_SHAPE.minUnique, `lineups ${i + 1} and ${j + 1} differ by ${9 - shared}`);
  }
  const qbUse = new Map<number, number>();
  for (const l of result.lineups) { const qb = l.slots.find(s => s.player.position === "QB")!.player.dkPlayerId; qbUse.set(qb, (qbUse.get(qb) ?? 0) + 1); }
  assert.ok([...qbUse.values()].every(c => c <= Math.floor(qbExposureCap(n) * n)), `quarterback use ${[...qbUse.values()]} exceeds ${qbExposureCap(n)}`);
  assert.ok(qbUse.size >= 2, "the cap spreads the quarterbacks");
  assert.ok(result.eligibility!.some(e => e.reasonCode === "WINNERS_SHAPE" && /\$3,400/.test(e.reason ?? "")), "the dear defense is named in eligibility");
  assert.ok(result.warnings.some(w => /Bring-back share: 2 of 4 lineups/.test(w)), result.warnings.join(" | "));
  assert.ok(result.warnings.some(w => /no prior-week opportunity data for any RB/.test(w)), "a pool without prior data says the RB rule was skipped");

  // v8 Classic distributions, on a 10-lineup run of the same pool.
  const dist = optimizeNflLineups(pool, settings({ nLineups: 10, minUnique: 2, maxExposure: 1, minSalary: 0, stackPassCatchers: 1,
    qbDoubleStackShare: 0.4, maxFourWrShare: 0.2, maxOtherGameStackShare: 0.3 }));
  assert.equal(dist.lineups.length, 10, dist.warnings.join(" | "));
  const stackOf = (l: typeof dist.lineups[number]) => { const qb = l.slots.find(s => s.player.position === "QB")!.player;
    return { qb, mates: l.slots.filter(s => ["WR", "TE"].includes(s.player.position) && s.player.team === qb.team).length }; };
  assert.equal(dist.lineups.filter(l => stackOf(l).mates >= 2).length, 4, "qbDoubleStackShare 0.4 of 10: four double stacks");
  assert.ok(dist.lineups.every(l => stackOf(l).mates >= 1), "every lineup still stacks one");
  assert.equal(dist.lineups.filter(l => stackOf(l).mates === 1).length, 6, "the rest stack exactly one");
  assert.ok(dist.lineups.filter(l => l.slots.filter(s => s.player.position === "WR").length === 4).length <= 2, "4th WR capped at 20% of 10");
  const otherGame = (l: typeof dist.lineups[number]) => { const { qb } = stackOf(l); const c = new Map<string, number>();
    for (const s of l.slots) if (["RB", "WR", "TE"].includes(s.player.position)) c.set(s.player.team, (c.get(s.player.team) ?? 0) + 1);
    return [...c].some(([t, k]) => k >= 2 && t !== qb.team && t !== qb.opponent); };
  assert.ok(dist.lineups.filter(otherGame).length <= 3, "other-game 2-stacks capped at 30% of 10");
  assert.ok(dist.warnings.some(w => w.startsWith("Classic distributions: 4 of 10")), dist.warnings.join(" | "));
  assert.throws(() => optimizeNflLineups(pool, settings({ format: "showdown", qbDoubleStackShare: 0.4 })), /Classic GPP rule/);
  const shapedCl = applyWinnersShape(settings({ nLineups: 20 }), pool);
  assert.equal(shapedCl.settings.qbDoubleStackShare, WINNERS_SHAPE.qbDoubleStackShare);
  assert.equal(shapedCl.settings.maxFourWrShare, WINNERS_SHAPE.maxFourWrShare);
  assert.equal(shapedCl.settings.maxOtherGameStackShare, WINNERS_SHAPE.maxOtherGameStackShare);

  const plain = optimizeNflLineups(pool, settings({ nLineups: n, stackPassCatchers: 1 }));
  assert.ok(plain.lineups.every(l => l.stackSummary.bringBack != null), "without the preset, the hard bring-back rule is every lineup");
  assert.ok(!plain.warnings.some(w => /Winners shape/.test(w)));

  const dearDst = id - 1;
  const lockedRun = optimizeNflLineups(pool, settings({ buildShape: "winners", lockedPlayerIds: [dearDst], nLineups: 1 }));
  assert.ok(lockedRun.lineups[0].playerIds.includes(dearDst), "a locked defense over $3,000 is kept: the lock outranks the pool rule");
  assert.ok(!lockedRun.eligibility!.some(e => e.dkPlayerId === dearDst && !e.eligible));
}
console.log("Winners shape (classic): bring-back share alternates, quarterbacks are capped, pool rules are enforced and named.");

// --- Showdown integration: no kicker, 5-1 or 3-3 only, captain cap ---
{
  const pool: NflOptimizerPlayer[] = [];
  let id = 200;
  // AAA has four strong players, BBB two: the natural best lineup is 4-2.
  const positions: NflOptimizerPlayer["position"][] = ["QB", "RB", "RB", "WR", "WR", "WR", "TE", "TE", "RB", "WR"];
  // Strong players are priced low so the best lineup fits the cap; the dear
  // filler lets six distinct lineups reach the preset's salary floor.
  // AAA's strongest player is a tight end: the natural captain without the preset.
  for (let i = 0; i < 10; i++) pool.push(player({ dkPlayerId: id++, position: positions[i], team: "AAA", salary: 6000 + 450 * i, ourProj: i < 3 ? 22 - i : i === 6 ? 26 : 6 - i * 0.2 }));
  for (let i = 0; i < 10; i++) pool.push(player({ dkPlayerId: id++, position: positions[i], team: "BBB", salary: 6050 + 450 * i, ourProj: i < 2 ? 20 - i : 5 - i * 0.2 }));
  for (const team of ["AAA", "BBB"]) pool.push(player({ dkPlayerId: id++, position: "K", team, salary: 3200, ourProj: 9, ceilingFpts: 14, depthRole: "Listed K1", availabilityStatus: "ACTIVE" }));
  const teamSplit = (l: { slots: { player: NflOptimizerPlayer }[] }) => l.slots.filter(s => s.player.team === "AAA").length;

  const plain = optimizeNflLineups(pool, settings({ format: "showdown", nLineups: 1, minSalary: 0, stackPassCatchers: 0 }));
  assert.equal(teamSplit(plain.lineups[0]), 4, "without the preset the best lineup is 4-2 (the ban has something to bind on)");
  assert.equal(plain.lineups[0].slots.find(s => s.slot === "CPT")!.player.position, "TE", "without the preset the tight end is the captain (the position rule has something to bind on)");

  const n = 6;
  const result = optimizeNflLineups(pool, settings({ format: "showdown", nLineups: n, minSalary: 0, stackPassCatchers: 0, buildShape: "winners", maxExposure: 1 }));
  assert.equal(result.lineups.length, n, `built ${result.lineups.length}: ${result.warnings.join(" | ")}`);
  for (const lineup of result.lineups) {
    assert.ok([1, 3, 5].includes(teamSplit(lineup)), `split ${teamSplit(lineup)}-${6 - teamSplit(lineup)} is 4-2`);
    assert.ok(!lineup.slots.some(s => s.player.position === "K"), "no kicker");
    assert.ok(lineup.totalSalary >= WINNERS_SHAPE.minSalary, `salary ${lineup.totalSalary}`);
    assert.ok(lineup.slots.some(s => s.player.position === "QB"), "v5: every lineup has a quarterback");
    const cpt = lineup.slots.find(s => s.slot === "CPT")!.player;
    if (cpt.position === "WR" || cpt.position === "TE") assert.ok(lineup.slots.some(s => s.player.position === "QB" && s.player.team === cpt.team), "v6: a WR/TE captain has his own QB");
    if (cpt.position === "QB") assert.ok(lineup.slots.some(s => s.slot !== "CPT" && ["WR", "TE"].includes(s.player.position) && s.player.team === cpt.team), "v6: a QB captain has his own WR/TE");
  }
  for (const lineup of result.lineups) assert.ok(["QB", "RB", "WR"].includes(lineup.slots.find(s => s.slot === "CPT")!.player.position), "the captain is a QB, RB or WR");
  assert.ok(result.lineups.some(l => l.slots.some(s => s.slot !== "CPT" && s.player.position === "TE" && s.player.ourProj === 26)), "the strong tight end still plays flex");
  const captains = result.lineups.map(l => l.slots.find(s => s.slot === "CPT")!.player.dkPlayerId);
  assert.equal(new Set(captains).size, n, "30% of 6 lineups is one captain slot each: six different captains");
  assert.equal(result.eligibility!.filter(e => e.reasonCode === "KICKER_EXCLUDED").length, 2, "both kickers are named as left out");
  assert.ok(result.warnings.some(w => w.startsWith(`Winners shape (${WINNERS_SHAPE_VERSION}, showdown)`)));

  // A captain range the user set outranks the cap.
  const ranged = optimizeNflLineups(pool, settings({ format: "showdown", nLineups: n, minSalary: 0, stackPassCatchers: 0, buildShape: "winners", maxExposure: 1,
    exposurePolicies: [{ playerId: 200, overall: { minPct: null, maxPct: 1 }, captain: { minPct: 0.5, maxPct: 1 }, flex: { minPct: null, maxPct: null }, exactTargetMode: false }] }));
  assert.ok(ranged.lineups.filter(l => l.slots.find(s => s.slot === "CPT")!.player.dkPlayerId === 200).length >= 3, "a 50% captain minimum the user set is honoured over the 30% cap");

  assert.throws(() => optimizeNflLineups(pool, settings({ format: "showdown", nLineups: 1, buildShape: "winners", lockedPlayerIds: [id - 1] })), /leaves kickers out/, "a locked kicker is refused by name");
  assert.throws(() => optimizeNflLineups(pool, settings({ format: "classic", captainPositions: ["QB"] })), /Showdown rules/, "showdown-only rules are rejected on a classic slate");
  assert.throws(() => optimizeNflLineups(pool, settings({ format: "showdown", bringBackShare: 0.5 })), /Classic GPP/);
  assert.throws(() => optimizeNflLineups(pool, settings({ format: "showdown", maxDstVsOffenseShare: 0.05 })), /Classic rule/);

  // v5 quarterback minimum, on a pool whose quarterbacks are weak enough that
  // the plain build leaves them out.
  const weakQb = pool.map(p => p.position === "QB" ? { ...p, ourProj: 2, ceilingFpts: 3 } : p);
  const noQb = optimizeNflLineups(weakQb, settings({ format: "showdown", nLineups: 4, minSalary: 0, stackPassCatchers: 0, maxExposure: 1 }));
  assert.ok(noQb.lineups.every(l => !l.slots.some(s => s.player.position === "QB")), "without the minimum the weak quarterbacks sit (the rule has something to bind on)");
  const oneQb = optimizeNflLineups(weakQb, settings({ format: "showdown", nLineups: 4, minSalary: 0, stackPassCatchers: 0, maxExposure: 1, minQbPerLineup: 1 }));
  assert.equal(oneQb.lineups.length, 4);
  assert.ok(oneQb.lineups.every(l => l.slots.filter(s => s.player.position === "QB").length >= 1), "minQbPerLineup 1: a quarterback in every lineup");
  const twoQb = optimizeNflLineups(weakQb, settings({ format: "showdown", nLineups: 2, minSalary: 0, stackPassCatchers: 0, maxExposure: 1, minQbPerLineup: 2 }));
  assert.ok(twoQb.lineups.every(l => l.slots.filter(s => s.player.position === "QB").length === 2), "minQbPerLineup 2: both quarterbacks");
  const shapedQb = optimizeNflLineups(weakQb, settings({ format: "showdown", nLineups: 4, minSalary: 0, stackPassCatchers: 0, buildShape: "winners", maxExposure: 1 }));
  assert.ok(shapedQb.lineups.every(l => l.slots.some(s => s.player.position === "QB")), "winners shape v5 forces the quarterback even when he projects poorly");
  // Two strong quarterbacks capped at 50% of 6 lineups = 6 appearances: an
  // early two-QB lineup would leave a later one with none, so each lineup
  // gets exactly one (the reservation), never a quarterback-less lineup.
  const strongQb = pool.map(p => p.position === "QB" ? { ...p, ourProj: 30, ceilingFpts: 45 } : p);
  const qbIds = strongQb.filter(p => p.position === "QB").map(p => p.dkPlayerId);
  const reserve = optimizeNflLineups(strongQb, settings({ format: "showdown", nLineups: 6, minSalary: 0, stackPassCatchers: 0, maxExposure: 1, minQbPerLineup: 1,
    exposurePolicies: qbIds.map(playerId => ({ playerId, overall: { minPct: null, maxPct: 0.5 }, captain: { minPct: null, maxPct: null }, flex: { minPct: null, maxPct: null }, exactTargetMode: false })) }));
  assert.equal(reserve.lineups.length, 6, reserve.warnings.join(" | "));
  assert.ok(reserve.lineups.every(l => l.slots.filter(s => s.player.position === "QB").length === 1), "reserved: one quarterback in each lineup, none left without");
  const uncapped = optimizeNflLineups(strongQb, settings({ format: "showdown", nLineups: 6, minSalary: 0, stackPassCatchers: 0, maxExposure: 1, minQbPerLineup: 1 }));
  assert.ok(uncapped.lineups.some(l => l.slots.filter(s => s.player.position === "QB").length === 2), "with room to spare, two-QB lineups are still allowed");
  // v6 captain stack, on a pool where the best captain is BBB's receiver and
  // BBB's quarterback is too weak to roster on his own merits.
  const stackPool = pool.map(p => p.team === "BBB" && p.position === "QB" ? { ...p, ourProj: 1, ceilingFpts: 2 }
    : p.team === "BBB" && p.position === "WR" && p.dkPlayerId === pool.find(q => q.team === "BBB" && q.position === "WR")!.dkPlayerId ? { ...p, ourProj: 40, ceilingFpts: 60 } : p);
  const loneWr = optimizeNflLineups(stackPool, settings({ format: "showdown", nLineups: 1, minSalary: 0, stackPassCatchers: 0, maxExposure: 1 }));
  const loneCpt = loneWr.lineups[0].slots.find(s => s.slot === "CPT")!.player;
  assert.ok(loneCpt.position === "WR" && loneCpt.team === "BBB" && !loneWr.lineups[0].slots.some(s => s.player.position === "QB" && s.player.team === "BBB"),
    "without the stack the BBB receiver captains with no BBB quarterback (the rule has something to bind on)");
  const stacked = optimizeNflLineups(stackPool, settings({ format: "showdown", nLineups: 3, minSalary: 0, stackPassCatchers: 0, maxExposure: 1, captainStack: true }));
  for (const l of stacked.lineups) {
    const c = l.slots.find(s => s.slot === "CPT")!.player;
    if (c.position === "WR" || c.position === "TE") assert.ok(l.slots.some(s => s.player.position === "QB" && s.player.team === c.team), "captainStack: receiver captain brings his QB");
    if (c.position === "QB") assert.ok(l.slots.some(s => s.slot !== "CPT" && ["WR", "TE"].includes(s.player.position) && s.player.team === c.team), "captainStack: QB captain brings a pass catcher");
  }
  assert.throws(() => optimizeNflLineups(pool, settings({ format: "classic", captainStack: true })), /Showdown rule/);

  // v7 distributions: scheduled across the run, two-sided for the RB captain.
  const dist = optimizeNflLineups(pool, settings({ format: "showdown", nLineups: 10, minSalary: 0, stackPassCatchers: 0, maxExposure: 1,
    minQbPerLineup: 1, twoQbShare: 0.4, maxLoneQbCaptainShare: 0.1, rbCaptainShare: 0.2 }));
  assert.equal(dist.lineups.length, 10, dist.warnings.join(" | "));
  const qbsIn = (l: { slots: { player: NflOptimizerPlayer }[] }) => l.slots.filter(s => s.player.position === "QB").length;
  const cptOf = (l: { slots: { slot: string; player: NflOptimizerPlayer }[] }) => l.slots.find(s => s.slot === "CPT")!.player;
  assert.equal(dist.lineups.filter(l => qbsIn(l) === 2).length, 4, "twoQbShare 0.4 of 10: four two-QB lineups");
  assert.ok(dist.lineups.every(l => qbsIn(l) >= 1), "the QB minimum still holds");
  assert.ok(dist.lineups.filter(l => cptOf(l).position === "QB" && qbsIn(l) === 1).length <= 1, "lone-QB captains capped at 10% of 10");
  assert.equal(dist.lineups.filter(l => cptOf(l).position === "RB").length, 2, "rbCaptainShare 0.2 of 10: exactly two RB captains");
  assert.ok(dist.warnings.some(w => w.startsWith("Showdown distributions: 4 of 10")), "the run reports what it built");
  assert.throws(() => optimizeNflLineups(pool, settings({ format: "classic", twoQbShare: 0.4 })), /Showdown rules/);
  // The preset raises a QB's overall cap to 75% unless he has his own overall range.
  const shapedSd = applyWinnersShape(settings({ format: "showdown", nLineups: 20 }), pool);
  assert.equal(shapedSd.settings.twoQbShare, WINNERS_SHAPE.showdownTwoQbShare);
  assert.equal(shapedSd.settings.showdownQbMinCap, 0.75);
  assert.throws(() => optimizeNflLineups(pool, settings({ format: "classic", minQbPerLineup: 1 })), /Showdown rule/);
  assert.throws(() => optimizeNflLineups(pool, settings({ format: "showdown", minQbPerLineup: 3 })), /0, 1 or 2/);
}
console.log("Winners shape (showdown): no kicker, 5-1 or 3-3 only, captains capped, user ranges still win.");

// --- DST against own players (v4): capped at 5% of lineups, rounded down ---
{
  const pool: NflOptimizerPlayer[] = [];
  let id = 300;
  // The best defense (AAA) faces BBB, whose quarterback and top two receivers
  // are the best values: unconstrained, lineups play AAA DST against them (the
  // game-script rule still allows up to three opposing players).
  for (const team of ["AAA", "BBB", "CCC", "DDD"]) {
    const strong = team === "BBB";
    pool.push(player({ dkPlayerId: id++, position: "QB", team, salary: 6000, ourProj: strong ? 28 : 18 }));
    for (let i = 0; i < 4; i++) pool.push(player({ dkPlayerId: id++, position: "RB", team, salary: 5000 + 300 * i, ourProj: 11 + i }));
    for (let i = 0; i < 5; i++) pool.push(player({ dkPlayerId: id++, position: "WR", team, salary: 5000 + 300 * i, ourProj: (strong && i >= 3 ? 22 : 11) + i }));
    for (let i = 0; i < 2; i++) pool.push(player({ dkPlayerId: id++, position: "TE", team, salary: 3500 + 300 * i, ourProj: 7 + i }));
    pool.push(player({ dkPlayerId: id++, position: "DST", team, salary: 3000, ourProj: team === "AAA" ? 16 : 5 }));
  }
  const n = 20;
  const base = { nLineups: n, minSalary: 0, maxExposure: 1, minUnique: 1 };
  const plain = optimizeNflLineups(pool, settings(base));
  const plainCount = plain.lineups.filter(dstFacesOwnOffense).length;
  assert.ok(plainCount > 1, `the rule needs something to bind on: ${plainCount} of ${n} unconstrained`);
  const capped = optimizeNflLineups(pool, settings({ ...base, maxDstVsOffenseShare: 0.05 }));
  assert.equal(capped.lineups.length, n, capped.warnings.join(" | "));
  const cappedCount = capped.lineups.filter(dstFacesOwnOffense).length;
  assert.ok(cappedCount <= 1, `5% of 20 is one lineup, built ${cappedCount}`);
  assert.ok(capped.warnings.some(w => w.startsWith(`DST against own players: ${cappedCount} of ${n}`) && /cap 1, 5%/.test(w)), capped.warnings.join(" | "));
  const none = optimizeNflLineups(pool, settings({ ...base, maxDstVsOffenseShare: 0 }));
  assert.equal(none.lineups.filter(dstFacesOwnOffense).length, 0, "a 0% cap forbids it in every lineup");
  const shaped = applyWinnersShape(settings({ nLineups: 100 }), pool);
  assert.equal(shaped.settings.maxDstVsOffenseShare, WINNERS_SHAPE.dstVsOffenseMaxShare);
  assert.ok(shaped.notes[0].includes("a DST against its own players in at most 5% of lineups (5 of 100)"), shaped.notes[0]);
  assert.equal(applyWinnersShape(settings({ maxDstVsOffenseShare: 0 }), pool).settings.maxDstVsOffenseShare, 0, "a tighter cap you set is kept");
  assert.throws(() => optimizeNflLineups(pool, settings({ maxDstVsOffenseShare: 1.5 })), /between 0 and 100%/);
}
console.log("Winners shape (classic): a DST against the lineup's own players is capped at 5% of lineups.");
