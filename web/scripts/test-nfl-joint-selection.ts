/**
 * Joint portfolio selection (nfl-joint-selection-v1).
 *
 * 1. The selector finds the true optimum (objective minus target penalties),
 *    checked against brute force over every subset of small random pools.
 * 2. It never returns a portfolio worse than the incumbent, and keeps the
 *    incumbent when nothing improves on it.
 * 3. Candidate order does not change the answer when the optimum is unique.
 * 4. In the optimizer: the joint build respects every exposure cap and the
 *    minimum-unique rule, is deterministic, and the sequential default is untouched.
 */
import assert from "node:assert/strict";
import { featureTotals, portfolioValue, selectJointPortfolio, targetMisses, type JointCandidate, type JointTarget } from "../src/lib/nfl-dfs/joint-selection";
import { optimizeNflLineups, type NflOptimizerPlayer, type NflOptimizerSettings } from "../src/app/dfs/nfl/nfl-optimizer";
import { nflOverlapCap } from "../src/lib/nfl-dfs/pre-export-qa";

let seed = 12345;
const rand = () => { seed = (seed * 1103515245 + 12345) % 2147483648; return seed / 2147483648; };

function* subsets(size: number, k: number, start = 0, picked: number[] = []): Generator<number[]> {
  if (picked.length === k) { yield [...picked]; return; }
  for (let i = start; i < size; i++) { picked.push(i); yield* subsets(size, k, i + 1, picked); picked.pop(); }
}

// 1-3: brute force on random small pools.
for (let trial = 0; trial < 60; trial++) {
  const size = 7 + Math.floor(rand() * 4), count = 3;
  const candidates: JointCandidate[] = Array.from({ length: size }, (_, i) => {
    const features: Record<string, number> = {};
    for (const name of ["p1", "p2", "p3", "flag"]) if (rand() < 0.45) features[name] = 1;
    return { key: `k${i}`, score: Math.round(rand() * 10000) / 100, features };
  });
  const targets: Record<string, JointTarget> = { p1: { max: 1 }, p2: { max: 2 }, p3: { min: 1 }, flag: { min: 1, max: 1 } };
  let best = -Infinity, bestKeys: string[] = [], ties = 0;
  for (const subset of subsets(size, count)) {
    const keys = subset.map((i) => candidates[i].key);
    const value = portfolioValue(candidates, keys, targets);
    if (value > best + 1e-9) { best = value; bestKeys = keys; ties = 1; } else if (Math.abs(value - best) < 1e-9) ties++;
  }
  const incumbent = candidates.slice(0, count).map((c) => c.key);
  const result = selectJointPortfolio({ candidates, count, targets, incumbent });
  assert.ok(result.value >= result.incumbentValue - 1e-9, `trial ${trial}: never worse than the incumbent`);
  assert.ok(Math.abs(result.value - best) < 1e-6, `trial ${trial}: selector ${result.value} matches brute force ${best}`);
  assert.deepEqual(result.misses, targetMisses(featureTotals(candidates, result.selected), targets), "misses are recounted from the chosen lineups");
  if (result.solver === "incumbent") assert.ok(Math.abs(result.incumbentValue - best) < 1e-6, "the incumbent is kept only when it is optimal");
  if (ties === 1) {
    const shuffled = [...candidates].sort(() => rand() - 0.5);
    const again = selectJointPortfolio({ candidates: shuffled, count, targets, incumbent });
    assert.deepEqual([...again.selected].sort(), [...bestKeys].sort(), `trial ${trial}: candidate order does not change a unique optimum`);
  }
}

// A rule the incumbent breaks is repaired when a candidate allows it, even at some cost in score.
{
  const candidates: JointCandidate[] = [
    { key: "a", score: 50, features: { lone: 1 } }, { key: "b", score: 49, features: { lone: 1 } },
    { key: "c", score: 45, features: {} },
  ];
  const result = selectJointPortfolio({ candidates, count: 2, targets: { lone: { max: 1 } }, incumbent: ["a", "b"] });
  assert.equal(result.solver, "ilp");
  assert.deepEqual([...result.selected].sort(), ["a", "c"]);
  assert.deepEqual(result.misses, {});
}
// A hard cap (a player's exposure cap) is never traded for a softer target or for score.
{
  const candidates: JointCandidate[] = [
    { key: "a", score: 50, features: { qb: 1 } }, { key: "b", score: 40, features: {} },
    { key: "c", score: 60, features: { qb: 1, two: 1 } },
  ];
  const result = selectJointPortfolio({ candidates, count: 2, incumbent: ["a", "b"],
    targets: { qb: { max: 1, hard: true }, two: { min: 1 } } });
  assert.ok(featureTotals(candidates, result.selected).qb <= 1, "the hard cap holds");
  assert.deepEqual([...result.selected].sort(), ["b", "c"], "the target is met only in a way the cap allows");
  assert.throws(() => selectJointPortfolio({ candidates, count: 2, incumbent: ["a", "c"], targets: { qb: { max: 1, hard: true } } }),
    /hard bounds/, "an incumbent that breaks a hard bound is refused");
}
// Malformed input is refused rather than guessed at.
assert.throws(() => selectJointPortfolio({ candidates: [{ key: "a", score: 1, features: {} }], count: 1, targets: {}, incumbent: ["zz"] }));

// 4: the optimizer.
const player = (id: number, name: string, position: NflOptimizerPlayer["position"], team: string, salary: number, proj: number): NflOptimizerPlayer =>
  ({ dkPlayerId: id, captainDkPlayerId: 100 + id, name, position, depthRole: position === "K" ? "Listed K1" : null, team, opponent: team === "NE" ? "CHI" : "NE",
     gameKey: "NE@CHI", salary, captainSalary: Math.round(salary * 1.5), ourProj: proj, floorFpts: proj * 0.3,
     ceilingFpts: proj * 2.2, isOut: false, projectionStatus: "historical", historyGames: 20 } as NflOptimizerPlayer);
const pool = [
  player(1, "Maye", "QB", "NE", 10600, 21.7), player(2, "Williams", "QB", "CHI", 10400, 21.3),
  player(3, "Brown", "WR", "NE", 9000, 18.5), player(4, "Swift", "RB", "CHI", 9800, 17.2),
  player(5, "Burden", "WR", "CHI", 8400, 14.9), player(6, "Stevenson", "RB", "NE", 7800, 13.4),
  player(7, "Loveland", "TE", "CHI", 6400, 12.0), player(8, "Odunze", "WR", "CHI", 7400, 11.5),
  player(9, "Douglas", "WR", "NE", 6600, 10.0), player(10, "Henry", "TE", "NE", 6200, 9.8),
  player(11, "Raymond", "WR", "CHI", 3400, 7.0), player(12, "Chism", "WR", "NE", 2800, 4.8),
  player(13, "Raridon", "TE", "NE", 2200, 4.1), player(14, "Kmet", "TE", "CHI", 2400, 3.2),
];
const base: NflOptimizerSettings = {
  format: "showdown", mode: "gpp", projectionSource: "our", allowDkFallback: false, nLineups: 12, minSalary: 0,
  maxExposure: 0.5, minUnique: 2, stackPassCatchers: 0, bringBack: false, randomness: 0.08,
  lockedPlayerIds: [], excludedPlayerIds: [], minExposureByPlayer: {}, maxExposureByPlayer: {},
  maxCaptainExposure: 0.3, minQbPerLineup: 1, twoQbShare: 0.4, maxLoneQbCaptainShare: 0.07, rbCaptainShare: 0.2,
};
const sequential = optimizeNflLineups(pool, base);
assert.ok(!sequential.warnings.some((w) => /Joint selection/.test(w)), "sequential stays the default");
const joint = optimizeNflLineups(pool, { ...base, portfolioSelection: "joint" });
const again = optimizeNflLineups(pool, { ...base, portfolioSelection: "joint" });
assert.equal(joint.lineups.length, sequential.lineups.length, "same number of lineups");
assert.ok(joint.warnings.some((w) => /Joint selection \(nfl-joint-selection-v1\)/.test(w)), "the joint step reports what it did");
assert.deepEqual(joint.lineups.map((l) => l.playerIds), again.lineups.map((l) => l.playerIds), "deterministic");
for (const row of joint.exposureReport ?? []) {
  assert.ok(row.overall <= row.overallMax, `${row.name} overall ${row.overall} <= ${row.overallMax}`);
  assert.ok(row.captain <= row.captainMax, `${row.name} captain ${row.captain} <= ${row.captainMax}`);
}
const cap = nflOverlapCap("showdown", base.minUnique, undefined);
for (let i = 0; i < joint.lineups.length; i++) for (let j = i + 1; j < joint.lineups.length; j++) {
  const shared = joint.lineups[i].playerIds.filter((id) => joint.lineups[j].playerIds.includes(id)).length;
  assert.ok(shared <= cap, `lineups ${i + 1} and ${j + 1} share ${shared} players (cap ${cap})`);
}
assert.deepEqual(joint.lineups.map((l) => l.lineupNumber), joint.lineups.map((_, i) => i + 1), "lineups are renumbered");
const loneQbCaptains = (lineups: typeof joint.lineups) => lineups.filter((l) => l.slots.find((s) => s.slot === "CPT")!.player.position === "QB"
  && l.slots.filter((s) => s.player.position === "QB").length === 1).length;
assert.ok(loneQbCaptains(joint.lineups) <= Math.max(Math.floor(0.07 * 12), loneQbCaptains(sequential.lineups)), "the lone-QB captain cap is never worse");

console.log(`Joint selection: 60 brute-force pools matched, rule repair, order invariance, and a ${joint.lineups.length}-lineup optimizer build all pass.`);
