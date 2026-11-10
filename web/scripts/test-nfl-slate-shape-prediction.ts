/**
 * Slate shape prediction (lib/nfl-dfs/slate-shape-prediction.ts): the exact
 * Showdown solver against brute force, the both-teams rule, the shape counts,
 * and a speed check on a full-size slate.
 */
import assert from "node:assert/strict";
import { tallyShowdownTypes, type HistoryPlayer } from "../src/lib/nfl-dfs/showdown-type-history";
import { actualBestShowdown, predictShowdownShapes, ShowdownSolver, showdownShape, type PredictionPlayer, type SolverPlayer } from "../src/lib/nfl-dfs/slate-shape-prediction";

let state = 12345;
const rand = () => { state = (Math.imul(state, 1103515245) + 12345) >>> 0; return state / 4294967296; };

function brute(players: SolverPlayer[], points: number[]): number {
  let best = -Infinity;
  const n = players.length;
  for (let c = 0; c < n; c++) {
    const rest = [...Array(n).keys()].filter((i) => i !== c);
    const pick = (start: number, left: number, chosen: number[]) => {
      if (left === 0) {
        const salary = players[c].captainSalary + chosen.reduce((s, i) => s + players[i].salary, 0);
        if (salary > 50000) return;
        const teams = new Set([players[c].team, ...chosen.map((i) => players[i].team)]);
        if (teams.size < 2) return;
        best = Math.max(best, 1.5 * points[c] + chosen.reduce((s, i) => s + points[i], 0));
        return;
      }
      for (let j = start; j < rest.length; j++) pick(j + 1, left - 1, [...chosen, rest[j]]);
    };
    pick(0, 5, []);
  }
  return best;
}

for (let trial = 0; trial < 40; trial++) {
  const n = 9 + Math.floor(rand() * 4);
  const players: SolverPlayer[] = Array.from({ length: n }, (_, i) => {
    const salary = 100 * (20 + Math.floor(rand() * 100));
    return { id: 1000 + i, team: rand() < (trial % 5 === 0 ? 0.85 : 0.5) ? "AAA" : "BBB", salary, captainSalary: Math.round(salary * 1.5 / 100) * 100 };
  });
  if (new Set(players.map((p) => p.team)).size < 2) players[0].team = players[0].team === "AAA" ? "BBB" : "AAA";
  const points = players.map(() => Math.round(rand() * 300) / 10);
  const solver = new ShowdownSolver(players, ["AAA", "BBB"]);
  const got = solver.solve(Float64Array.from(points));
  const expected = brute(players, points);
  if (!Number.isFinite(expected)) { assert.equal(got, null, `trial ${trial}: no legal lineup`); continue; }
  assert.ok(got, `trial ${trial}: a lineup exists`);
  assert.ok(Math.abs(got.score - expected) < 1e-9, `trial ${trial}: solver ${got.score} vs brute force ${expected}`);
  // The recovered lineup is legal and scores what the solver said.
  const idx = new Map(players.map((p, i) => [p.id, i]));
  const ids = [got.captain, ...got.flex];
  assert.equal(new Set(ids).size, 6, "six different players");
  const salary = players[idx.get(got.captain)!].captainSalary + got.flex.reduce((s, id) => s + players[idx.get(id)!].salary, 0);
  assert.ok(salary <= 50000, "under the cap");
  assert.equal(new Set(ids.map((id) => players[idx.get(id)!].team)).size, 2, "both teams");
  const rescored = 1.5 * points[idx.get(got.captain)!] + got.flex.reduce((s, id) => s + points[idx.get(id)!], 0);
  assert.ok(Math.abs(rescored - got.score) < 1e-9, "recovered players score the solver's total");
}

// Both-teams rule binds: one team's players dominate.
{
  const players: SolverPlayer[] = [
    ...Array.from({ length: 6 }, (_, i) => ({ id: i + 1, team: "AAA", salary: 5000, captainSalary: 7500 })),
    { id: 99, team: "BBB", salary: 1000, captainSalary: 1500 },
  ];
  const points = Float64Array.from([20, 20, 20, 20, 20, 20, 1]);
  const got = new ShowdownSolver(players, ["AAA", "BBB"]).solve(points)!;
  assert.ok(got.flex.includes(99), "a 6-0 lineup is never chosen");
}

// Prediction on a synthetic slate: shares add up, and a full-size slate runs fast enough.
const slate: PredictionPlayer[] = [];
const positions = ["QB", "RB", "RB", "WR", "WR", "WR", "WR", "TE", "TE", "K", "DST", "RB", "WR", "WR", "TE", "WR", "RB", "WR"];
for (const [t, team] of ["BUF", "MIN"].entries()) positions.forEach((position, i) => {
  const proj = Math.max(0.5, (position === "QB" ? 20 : position === "DST" || position === "K" ? 7 : 14) - i * 0.6 - t);
  const salary = Math.max(200, Math.round((proj * 550) / 100) * 100);
  slate.push({ dkPlayerId: 100 * (t + 1) + i, name: `${team} ${position}${i}`, team, opponent: t ? "BUF" : "MIN", gameKey: "BUF@MIN", position,
    salary, captainSalary: salary * 1.5, ourProj: proj, floorFpts: proj * 0.3, medianFpts: proj * 0.85, ceilingFpts: proj * 2.1, isOut: false });
});
async function main() {
const started = Date.now();
const prediction = await predictShowdownShapes(slate, { draws: 300, seed: 7 });
const ms = Date.now() - started;
assert.equal(prediction.solved, 300);
const sum = (rows: { pct: number }[]) => rows.reduce((s, r) => s + r.pct, 0);
assert.ok(Math.abs(sum(prediction.splitKinds) - 100) < 1e-6, "split kinds sum to 100%");
assert.ok(Math.abs(sum(prediction.captains) - 100) < 1e-6, "captains sum to 100%");
assert.ok(Math.abs(sum(prediction.quarterbacks) - 100) < 1e-6);
assert.ok(prediction.splitKinds.every((r) => ["3-3", "4-2", "5-1"].includes(r.key)), "no 6-0");
assert.ok(prediction.score.p10 <= prediction.score.median && prediction.score.median <= prediction.score.p90);
const again = await predictShowdownShapes(slate, { draws: 300, seed: 7 });
assert.deepEqual(again.splitKinds, prediction.splitKinds, "same seed, same answer");
assert.ok(ms < 8000, `300 simulated games on 36 players took ${ms} ms`);

// Grading uses the same solver on real points.
const actual = new Map(slate.map((p) => [p.dkPlayerId, p.ourProj ?? 0]));
const real = actualBestShowdown(slate, actual)!;
assert.equal(real.flex.length, 5);
const byId = new Map(slate.map((p) => [p.dkPlayerId, p]));
assert.deepEqual(real.shape, showdownShape(real.captain, real.flex, byId));

// The running tally: winner and best-possible types per slate, counted.
const hp = (name: string, team: string, position: string, salary: number, ourProj: number): HistoryPlayer =>
  ({ normalizedName: name.toLowerCase(), name, team, position, salary, captainSalary: salary * 1.5, ourProj });
const histPlayers = [hp("QbA", "AAA", "QB", 11000, 20), hp("WrA", "AAA", "WR", 9000, 15), hp("RbA", "AAA", "RB", 8000, 14), hp("TeA", "AAA", "TE", 5000, 8),
  hp("QbB", "BBB", "QB", 10000, 17), hp("WrB", "BBB", "WR", 7000, 12), hp("RbB", "BBB", "RB", 6000, 10), hp("KB", "BBB", "K", 4000, 7)];
const entry = (rank: number, cpt: string, flex: string[]) => ({ rank, points: 100 - rank, players: [{ normalizedName: cpt, slot: "CPT" }, ...flex.map((n) => ({ normalizedName: n, slot: "FLEX" }))] });
const tallied = tallyShowdownTypes([{ key: "s1", label: "Wk 1", season: 2026, week: 1, contestName: "c", entryCount: 100, winningScore: 99, players: histPlayers,
  fpts: new Map([["wra", 30], ["qba", 20], ["rba", 18], ["tea", 2], ["qbb", 15], ["wrb", 4], ["rbb", 3], ["kb", 9]]),
  top: [entry(1, "wra", ["qba", "rba", "tea", "qbb", "kb"]), entry(2, "qba", ["wra", "rba", "qbb", "wrb", "rbb"])] }]);
const row = tallied.slates[0];
assert.equal(row.favored, "AAA", "the team with more projected points");
assert.equal(row.winner!.side, "Favored 4-2");
assert.equal(row.winner!.captainPosition, "WR");
assert.equal(row.winner!.captainRank, "2nd-3rd", "WrA is the 3rd-highest projection");
assert.equal(row.winner!.quarterbacks, "Both QBs");
assert.equal(row.winner!.kicker, true);
assert.deepEqual(row.topShares, { "3-3": 50, "4-2": 50, "5-1": 0 });
assert.ok(row.best, "best possible lineup solved from the real points");
assert.equal(tallied.splitKinds.find((r) => r.key === "4-2")!.winners, 1);
assert.equal(tallied.splitKinds.find((r) => r.key === "4-2")!.topShare, 50);

console.log(`Slate shape prediction: solver matches brute force on 40 slates; 300 simulated games in ${ms} ms.`);
}

main().catch((error) => { console.error(error); process.exit(1); });
