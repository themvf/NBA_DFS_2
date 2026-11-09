/**
 * nfl-lineup-draws-v1: GPP lineups solved on correlated simulated games
 * (v2: the same on a fitted role-pair correlation table, opt-in).
 *
 * Pins (1) the draws themselves: reproducible by seed, mean equal to the
 * projection, teammates positively correlated and a DST negatively correlated
 * with the offense it faces; (2) the lineup-level leverage unit; and (3) the
 * reason the mode exists: on a Showdown pool shaped like CHI@SEA 2026-11-02,
 * ceiling scoring puts a cheap boom-or-bust back (median a quarter of his
 * mean) exactly at the exposure cap, while simulated-game scoring uses him
 * about as often as he belongs in the best lineup.
 */
import assert from "node:assert/strict";
import { buildDrawModel, drawOutcome, slotLeverageBonus, type DrawPlayer } from "../src/lib/nfl-dfs/lineup-draws";
import { optimizeNflLineups, type NflOptimizerPlayer, type NflOptimizerSettings } from "../src/app/dfs/nfl/nfl-optimizer";

// --- 1. Draws ---
const drawPlayers: DrawPlayer[] = [
  { id: 1, position: "QB", team: "AAA", opponent: "BBB", gameKey: "AAA@BBB", mean: 18, p10: 9, median: 17, p90: 29 },
  { id: 2, position: "WR", team: "AAA", opponent: "BBB", gameKey: "AAA@BBB", mean: 14, p10: 4, median: 12, p90: 27 },
  { id: 3, position: "DST", team: "BBB", opponent: "AAA", gameKey: "AAA@BBB", mean: 6, p10: 0, median: 5, p90: 13 },
  { id: 4, position: "RB", team: "BBB", opponent: "AAA", gameKey: "AAA@BBB", mean: 7.4, p10: 0, median: 1.9, p90: 21.5 },
  { id: 5, position: "K", team: "BBB", opponent: "AAA", gameKey: "AAA@BBB", mean: 8, p10: null, median: null, p90: null },
];
const model = buildDrawModel(drawPlayers, 7);
assert.deepEqual(model.fixed, [5], "a player with no range is drawn at his projection");
assert.deepEqual([...drawOutcome(model, 3)], [...drawOutcome(buildDrawModel(drawPlayers, 7), 3)], "same seed and index, same game");
assert.notDeepEqual([...drawOutcome(model, 3)], [...drawOutcome(model, 4)], "different index, different game");
const N = 6000;
const series = new Map<number, number[]>(drawPlayers.map((p) => [p.id, []]));
for (let i = 1; i <= N; i++) for (const [id, v] of drawOutcome(model, i)) series.get(id)!.push(v);
const mean = (xs: number[]) => xs.reduce((a, b) => a + b, 0) / xs.length;
const corr = (a: number[], b: number[]) => {
  const ma = mean(a), mb = mean(b);
  let num = 0, da = 0, db = 0;
  a.forEach((x, i) => { num += (x - ma) * (b[i] - mb); da += (x - ma) ** 2; db += (b[i] - mb) ** 2; });
  return num / Math.sqrt(da * db);
};
for (const p of drawPlayers) {
  const m = mean(series.get(p.id)!);
  assert.ok(Math.abs(m - p.mean) / p.mean < 0.04, `${p.position} draws average ${m.toFixed(2)} against a ${p.mean} projection`);
}
assert.equal(new Set(series.get(5)).size, 1, "the fixed player never varies");
assert.ok(corr(series.get(1)!, series.get(2)!) > 0.2, "QB and his receiver move together");
assert.ok(corr(series.get(3)!, series.get(1)!) < -0.1, "a DST moves against the offense it faces");
// Fitted role pairs (nfl-player-correlations-v1): a QB rises with his pass
// catchers, two receivers sharing one team's targets do not rise together,
// and his RB barely moves with him. v1's stated priors put all three at 0.40.
const room: DrawPlayer[] = [
  { id: 11, position: "QB", team: "AAA", opponent: "BBB", gameKey: "AAA@BBB", mean: 20, p10: 10, median: 19, p90: 31 },
  { id: 12, position: "WR", team: "AAA", opponent: "BBB", gameKey: "AAA@BBB", mean: 16, p10: 5, median: 14, p90: 29 },
  { id: 13, position: "WR", team: "AAA", opponent: "BBB", gameKey: "AAA@BBB", mean: 12, p10: 3, median: 10, p90: 24 },
  { id: 14, position: "RB", team: "AAA", opponent: "BBB", gameKey: "AAA@BBB", mean: 15, p10: 5, median: 14, p90: 27 },
  { id: 15, position: "QB", team: "BBB", opponent: "AAA", gameKey: "AAA@BBB", mean: 17, p10: 8, median: 16, p90: 28 },
  { id: 16, position: "DST", team: "BBB", opponent: "AAA", gameKey: "AAA@BBB", mean: 7, p10: 1, median: 6, p90: 14 },
];
const roomSeries = (m: ReturnType<typeof buildDrawModel>) => {
  const s = new Map<number, number[]>(room.map((p) => [p.id, []]));
  for (let i = 1; i <= N; i++) for (const [id, v] of drawOutcome(m, i)) s.get(id)!.push(v);
  return s;
};
const fitted = roomSeries(buildDrawModel(room, 5, "fitted"));
const stated = roomSeries(buildDrawModel(room, 5));
assert.ok(corr(fitted.get(11)!, fitted.get(12)!) > 0.2, "QB1 with WR1: fitted 0.31");
assert.ok(Math.abs(corr(fitted.get(12)!, fitted.get(13)!)) < 0.08, "WR1 with WR2 on one team: fitted about 0");
assert.ok(corr(fitted.get(11)!, fitted.get(14)!) < 0.17, "QB1 with RB1: fitted 0.09");
assert.ok(Math.abs(corr(fitted.get(11)!, fitted.get(15)!)) < 0.2, "the two QBs: weak");
assert.ok(corr(fitted.get(16)!, fitted.get(11)!) < -0.2, "a DST against the QB it faces: fitted -0.32");
assert.ok(corr(stated.get(12)!, stated.get(13)!) > 0.3, "the default, v1's stated priors: every teammate pair 0.40");
const roomModel = buildDrawModel(room, 5, "fitted");
assert.deepEqual(roomModel.entries.map((e) => e.role), ["QB1", "WR1", "WR2", "RB1", "QB1", "DST"], "roles rank each team's players at a position by projection");
assert.deepEqual(roomModel.repairedGames, [], "the fitted table gives a valid matrix for a normal room");
// The default reproduces nfl-lineup-draws-v1 exactly: these are the draws the
// v1 code on main produced for this pool (seed 7) before the fitted tables existed.
{
  const v1Pool: DrawPlayer[] = [
    { id: 1, position: "QB", team: "AAA", opponent: "BBB", gameKey: "AAA@BBB", mean: 18, p10: 9, median: 17, p90: 29 },
    { id: 2, position: "WR", team: "AAA", opponent: "BBB", gameKey: "AAA@BBB", mean: 14, p10: 4, median: 12, p90: 27, notPlay: 0.1 },
    { id: 3, position: "DST", team: "BBB", opponent: "AAA", gameKey: "AAA@BBB", mean: 6, p10: 0, median: 5, p90: 13 },
    { id: 4, position: "RB", team: "CCC", opponent: "DDD", gameKey: "CCC@DDD", mean: 7.4, p10: 0, median: 1.9, p90: 21.5 },
    { id: 5, position: "K", team: "DDD", opponent: "CCC", gameKey: "CCC@DDD", mean: 8, p10: null, median: null, p90: null },
  ];
  const v1 = buildDrawModel(v1Pool, 7);
  assert.equal(v1.version, "nfl-lineup-draws-v1");
  const got = [1, 2, 3, 50].map((i) => [...drawOutcome(v1, i)].map(([id, v]) => [id, Math.round(v * 1e6) / 1e6]));
  assert.deepEqual(got, [[[1,20.091742],[2,11.309452],[3,12.392366],[4,0.710605],[5,8]],[[1,15.07981],[2,5.481524],[3,3.021173],[4,0.91422],[5,8]],[[1,13.425831],[2,23.196823],[3,-0.195383],[4,12.294176],[5,8]],[[1,8.996089],[2,8.884738],[3,3.37774],[4,-0.44843],[5,8]]], "default draws are v1, bit for bit");
}
const lotto = series.get(4)!.slice().sort((a, b) => a - b);
assert.ok(Math.abs(lotto[Math.floor(N / 2)] - 1.9) < 0.6, "the boom-or-bust back's draws keep his median");

// --- 2. Lineup-level leverage unit: points per halving of ownership ---
assert.equal(slotLeverageBonus(50, 1), 1);
assert.equal(slotLeverageBonus(25, 1), 2);
assert.ok(Math.abs(slotLeverageBonus(12.5, 2) - 6) < 1e-9);
assert.equal(slotLeverageBonus(null, 1), 0, "unknown ownership is neutral");
assert.equal(slotLeverageBonus(40, 0), 0, "0 disables it");

// --- 3. The CHI@SEA case ---
function player(over: Partial<NflOptimizerPlayer> & { dkPlayerId: number; salary: number; ourProj: number; medianFpts: number; ceilingFpts: number }): NflOptimizerPlayer {
  return {
    id: over.dkPlayerId, dkPlayerId: over.dkPlayerId, captainDkPlayerId: over.dkPlayerId + 100_000,
    name: over.name ?? `P${over.dkPlayerId}`, position: over.position ?? "WR", team: over.team ?? "SEA",
    opponent: over.team === "CHI" ? "SEA" : "CHI", gameKey: "CHI@SEA", salary: over.salary,
    captainSalary: Math.round(over.salary * 1.5), isOut: false, projectionStatus: "historical",
    historyGames: 6, ourProj: over.ourProj, floorFpts: over.floorFpts ?? over.ourProj * 0.35, medianFpts: over.medianFpts,
    ceilingFpts: over.ceilingFpts, boomRate: over.boomRate ?? 0.05, avgFptsDk: over.ourProj, fantasyprosProj: null, linestarProj: null,
    linestarOwnPct: null, customProj: null, ownPct: null, ownSource: null,
  };
}
const pool: NflOptimizerPlayer[] = [
  player({ dkPlayerId: 1, name: "Smith-Njigba", position: "WR", team: "SEA", salary: 12000, ourProj: 25.3, medianFpts: 26.0, ceilingFpts: 41.7 }),
  player({ dkPlayerId: 2, name: "Caleb", position: "QB", team: "CHI", salary: 10200, ourProj: 18.6, medianFpts: 17.7, ceilingFpts: 33.4 }),
  player({ dkPlayerId: 3, name: "Swift", position: "RB", team: "CHI", salary: 9800, ourProj: 16.4, medianFpts: 16.8, ceilingFpts: 25.3 }),
  player({ dkPlayerId: 4, name: "Darnold", position: "QB", team: "SEA", salary: 9400, ourProj: 17.9, medianFpts: 16.4, ceilingFpts: 32.0 }),
  player({ dkPlayerId: 5, name: "Burden", position: "WR", team: "CHI", salary: 9000, ourProj: 13.7, medianFpts: 10.5, ceilingFpts: 30.7 }),
  player({ dkPlayerId: 6, name: "Loveland", position: "TE", team: "CHI", salary: 7800, ourProj: 11.8, medianFpts: 11.5, ceilingFpts: 24.1 }),
  player({ dkPlayerId: 7, name: "Odunze", position: "WR", team: "CHI", salary: 7600, ourProj: 10.3, medianFpts: 8.4, ceilingFpts: 20.8 }),
  player({ dkPlayerId: 8, name: "Myers", position: "K", team: "SEA", salary: 5400, ourProj: 10.3, medianFpts: 8.0, ceilingFpts: 23.0 }),
  player({ dkPlayerId: 9, name: "Kupp", position: "WR", team: "SEA", salary: 4800, ourProj: 7.8, medianFpts: 4.7, ceilingFpts: 15.5 }),
  player({ dkPlayerId: 10, name: "Barner", position: "TE", team: "SEA", salary: 4600, ourProj: 8.8, medianFpts: 7.2, ceilingFpts: 17.4 }),
  player({ dkPlayerId: 11, name: "Raymond", position: "WR", team: "CHI", salary: 3600, ourProj: 5.3, medianFpts: 3.2, ceilingFpts: 15.1 }),
  player({ dkPlayerId: 12, name: "Roschon", position: "RB", team: "CHI", salary: 3400, ourProj: 7.37, medianFpts: 1.95, ceilingFpts: 21.53, floorFpts: 0, boomRate: 0.003 }),
  player({ dkPlayerId: 13, name: "Holani", position: "RB", team: "SEA", salary: 3000, ourProj: 4.4, medianFpts: 3.1, ceilingFpts: 11.3 }),
  player({ dkPlayerId: 14, name: "Horton", position: "WR", team: "SEA", salary: 2800, ourProj: 5.4, medianFpts: 2.8, ceilingFpts: 14.4 }),
  player({ dkPlayerId: 15, name: "Charbonnet", position: "RB", team: "SEA", salary: 7000, ourProj: 8.7, medianFpts: 8.5, ceilingFpts: 15.1 }),
  player({ dkPlayerId: 16, name: "Seahawks", position: "DST", team: "SEA", salary: 4200, ourProj: 8.4, medianFpts: 8.0, ceilingFpts: 13.0 }),
  player({ dkPlayerId: 17, name: "Bears", position: "DST", team: "CHI", salary: 3800, ourProj: 6.6, medianFpts: 6.0, ceilingFpts: 12.0 }),
  player({ dkPlayerId: 18, name: "Kmet", position: "TE", team: "CHI", salary: 2400, ourProj: 3.2, medianFpts: 2.2, ceilingFpts: 8.5 }),
];
const settings = (over: Partial<NflOptimizerSettings>): NflOptimizerSettings => ({
  format: "showdown", mode: "gpp", projectionSource: "our", allowDkFallback: true,
  nLineups: 20, minSalary: 0, maxExposure: 0.5, minUnique: 1, stackPassCatchers: 0, bringBack: false, randomness: 0.08,
  lockedPlayerIds: [], excludedPlayerIds: [], minExposureByPlayer: {}, maxExposureByPlayer: {}, ...over,
});
const uses = (r: ReturnType<typeof optimizeNflLineups>, id: number) => r.lineups.filter((l) => l.playerIds.includes(id)).length;
const both = (r: ReturnType<typeof optimizeNflLineups>, a: number, b: number) => r.lineups.filter((l) => l.playerIds.includes(a) && l.playerIds.includes(b)).length;

const ceiling = optimizeNflLineups(pool, settings({ gppScoring: "ceiling" }));
const draws = optimizeNflLineups(pool, settings({ gppScoring: "draws", drawSeed: 11 }));
assert.equal(ceiling.lineups.length, 20); assert.equal(draws.lineups.length, 20);
const pinned = (r: ReturnType<typeof optimizeNflLineups>) => pool.filter((p) => uses(r, p.dkPlayerId) === 10).length;
console.log(`ceiling: Roschon ${uses(ceiling, 12)}/20, players at the 10-lineup cap ${pinned(ceiling)}, JSN+Swift ${both(ceiling, 1, 3)}`);
console.log(`draws:   Roschon ${uses(draws, 12)}/20, players at the 10-lineup cap ${pinned(draws)}, JSN+Swift ${both(draws, 1, 3)}`);
assert.equal(uses(ceiling, 12), 10, "ceiling scoring rides the boom-or-bust back to his cap");
assert.ok(uses(draws, 12) < 10, "simulated-game scoring does not");
assert.ok(pinned(draws) < pinned(ceiling), "fewer players sit exactly on the cap");
assert.ok(draws.warnings.some((w) => /simulated games \(nfl-lineup-draws-v1, seed 11\)/.test(w)), "the run says how it was scored");
const again = optimizeNflLineups(pool, settings({ gppScoring: "draws", drawSeed: 11 }));
assert.deepEqual(again.lineups.map((l) => l.playerIds), draws.lineups.map((l) => l.playerIds), "same seed rebuilds the same run");
const cash = optimizeNflLineups(pool, settings({ mode: "cash", gppScoring: "draws", nLineups: 1 }));
const cashCeiling = optimizeNflLineups(pool, settings({ mode: "cash", nLineups: 1 }));
assert.deepEqual(cash.lineups[0].playerIds, cashCeiling.lineups[0].playerIds, "cash mode ignores GPP scoring");

console.log("Lineup draws: reproducible correlated draws, lineup-level leverage, exposure off the caps, cash unchanged passed.");

// The generate action returns these lineups to a client component, and
// React's server-action serializer refuses any function-valued field. The
// first draws version attached `drawAt` to every pool player and every
// production GPP build failed (2026-11-05, JAX @ BAL). The result and the
// input pool must both survive a serializing walk.
const functionPaths = (value: unknown, path = "$", seen = new Set<object>()): string[] => {
  if (typeof value === "function") return [path];
  if (!value || typeof value !== "object" || seen.has(value)) return [];
  seen.add(value);
  return Object.entries(value as Record<string, unknown>).flatMap(([k, v]) => functionPaths(v, `${path}.${k}`, seen));
};
assert.deepEqual(functionPaths(draws), [], "a draws build carries no function-valued field");
assert.deepEqual(functionPaths(pool), [], "the input pool is not mutated with functions");
console.log("serializable: no function-valued fields on the draws result or its pool");
