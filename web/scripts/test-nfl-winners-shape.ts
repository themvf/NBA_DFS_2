/**
 * Winners shape (`nfl-winners-shape-v1`): the preset resolves into ordinary
 * optimizer settings and pool exclusions, and the optimizer enforces each of
 * them: the bring-back SHARE alternates lineups, the quarterback cap binds,
 * kickers leave a showdown pool, a showdown lineup is 5-1 or 3-3 and never
 * 4-2, and the captain cap holds. Nothing here changes a projection.
 */
import assert from "node:assert/strict";
import { optimizeNflLineups, type NflOptimizerPlayer, type NflOptimizerSettings } from "../src/app/dfs/nfl/nfl-optimizer";
import { applyWinnersShape, qbExposureCap, WINNERS_SHAPE, WINNERS_SHAPE_VERSION } from "../src/lib/nfl-dfs/winners-shape";

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
  assert.equal(shaped.settings.minSalary, WINNERS_SHAPE.minSalary, "$45,000 rises to $49,500");
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
  // filler lets six distinct lineups reach the preset's $49,500 floor.
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
  }
  for (const lineup of result.lineups) assert.ok(["QB", "RB", "WR"].includes(lineup.slots.find(s => s.slot === "CPT")!.player.position), "the captain is a QB, RB or WR");
  assert.ok(result.lineups.some(l => l.slots.some(s => s.slot !== "CPT" && s.player.position === "TE" && s.player.ourProj === 26)), "the strong tight end still plays flex");
  const captains = result.lineups.map(l => l.slots.find(s => s.slot === "CPT")!.player.dkPlayerId);
  assert.equal(new Set(captains).size, n, "30% of 6 lineups is one captain slot each: six different captains");
  assert.equal(result.eligibility!.filter(e => e.reasonCode === "KICKER_EXCLUDED").length, 2, "both kickers are named as left out");
  assert.ok(result.warnings.some(w => /Winners shape \(nfl-winners-shape-v3, showdown\)/.test(w)));

  // A captain range the user set outranks the cap.
  const ranged = optimizeNflLineups(pool, settings({ format: "showdown", nLineups: n, minSalary: 0, stackPassCatchers: 0, buildShape: "winners", maxExposure: 1,
    exposurePolicies: [{ playerId: 200, overall: { minPct: null, maxPct: 1 }, captain: { minPct: 0.5, maxPct: 1 }, flex: { minPct: null, maxPct: null }, exactTargetMode: false }] }));
  assert.ok(ranged.lineups.filter(l => l.slots.find(s => s.slot === "CPT")!.player.dkPlayerId === 200).length >= 3, "a 50% captain minimum the user set is honoured over the 30% cap");

  assert.throws(() => optimizeNflLineups(pool, settings({ format: "showdown", nLineups: 1, buildShape: "winners", lockedPlayerIds: [id - 1] })), /leaves kickers out/, "a locked kicker is refused by name");
  assert.throws(() => optimizeNflLineups(pool, settings({ format: "classic", captainPositions: ["QB"] })), /Showdown rules/, "showdown-only rules are rejected on a classic slate");
  assert.throws(() => optimizeNflLineups(pool, settings({ format: "showdown", bringBackShare: 0.5 })), /Classic GPP/);
}
console.log("Winners shape (showdown): no kicker, 5-1 or 3-3 only, captains capped, user ranges still win.");
