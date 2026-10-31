/**
 * CFB contest results: standings parsing (per-slot rows summed), lineup
 * scoring, ranks, set summaries and projection error. Synthetic file in
 * DraftKings' layout; the real one holds other entrants' usernames.
 */
import assert from "node:assert/strict";
import {
  cfbCaptainTable, cfbPerfectLineup, cfbProjectionError, estimateRank, parseCfbContestStandings, scoreCfbLineups, summarizeCfbSet,
} from "../src/lib/cfb-dfs/results";
import type { CfbLineup, CfbPoolPlayer } from "../src/lib/cfb-dfs/settings";

const header = "﻿Rank,EntryId,EntryName,TimeRemaining,Points,Lineup,,Player,Roster Position,%Drafted,FPTS";
const players: Array<[string, string, string, number]> = [
  ["Jackson Gutierrez", "QB", "40.00%", 25.56], ["Jackson Gutierrez", "S-FLEX", "7.39%", 25.56],
  ["Aidan Chiles", "S-FLEX", "30.00%", 24.1], ["Charlie Becker", "WR", "45.00%", 16.8],
  ["Lee Beebe Jr.", "RB", "28.72%", 30.5], ["Rod Robinson II", "RB", "41.85%", 3.8],
];
const rows = Array.from({ length: 200 }, (_, i) => {
  const block = players[i] ? `,,${players[i][0]},${players[i][1]},${players[i][2]},${players[i][3]}` : ",,,,,";
  return `${i + 1},${5269374147 + i},user${i} (1/1),0,${(182.08 - i * 0.5).toFixed(2)},FLEX X QB Y${block}`;
});
const contest = parseCfbContestStandings([header, ...rows].join("\r\n"));
assert.equal(contest.entryCount, 200);
assert.equal(contest.winningScore, 182.08);
const gutierrez = contest.players.find((p) => p.name === "Jackson Gutierrez")!;
assert.equal(gutierrez.draftedPct, 47.39, "a player's slots are summed");
assert.deepEqual(gutierrez.draftedBySlot, { QB: 40, "S-FLEX": 7.39 });
assert.equal(contest.players.length, 5, "one entry per player, not per slot");

assert.throws(() => parseCfbContestStandings("Position,Name\nQB,A"), /not a DraftKings contest standings file/);
assert.equal(contest.format, "classic");

// Rank: 182.08 is first; 150 sits between entries 64 (150.58) and 65 (150.08) -> 65 scored more.
assert.deepEqual(estimateRank(200, contest.scoreCurve, 200), { rank: 1, beatShare: 1, exact: true });
const mid = estimateRank(150, contest.scoreCurve, 200)!;
assert.equal(mid.rank, 66); assert.equal(mid.exact, true);

// Lineup scoring: every player known -> a score; one unknown -> unknown, never zero.
const p = (name: string, position: "QB" | "RB" | "WR", proj: number): CfbPoolPlayer =>
  ({ dkId: name.length, name, position, team: "T", game: "A@B", salary: 5000, proj });
const lineup = (n: number, names: Array<[string, "QB" | "RB" | "WR"]>): CfbLineup => ({
  lineupNumber: n, salary: 40000, projection: 100,
  slots: names.map(([name, pos], i) => ({ slot: (["QB", "RB", "RB", "WR", "WR", "WR", "FLEX", "S-FLEX"] as const)[i], player: p(name, pos, 10) })),
});
const fpts = new Map(contest.players.map((x) => [x.key, x.fpts] as const));
const known = lineup(1, [["Jackson Gutierrez", "QB"], ["Lee Beebe Jr.", "RB"], ["Rod Robinson II", "RB"], ["Charlie Becker", "WR"],
  ["Charlie Becker", "WR"], ["Charlie Becker", "WR"], ["Lee Beebe Jr.", "RB"], ["Aidan Chiles", "QB"]]);
const unknown = lineup(2, [["Jackson Gutierrez", "QB"], ["Nobody Drafted", "RB"], ["Rod Robinson II", "RB"], ["Charlie Becker", "WR"],
  ["Charlie Becker", "WR"], ["Charlie Becker", "WR"], ["Lee Beebe Jr.", "RB"], ["Aidan Chiles", "QB"]]);
const scored = scoreCfbLineups([known, unknown], fpts, contest.scoreCurve, contest.entryCount);
assert.equal(scored[0].actual, Math.round((25.56 + 30.5 + 3.8 + 16.8 * 3 + 30.5 + 24.1) * 100) / 100);
assert.deepEqual(scored[0].qbs, ["Jackson Gutierrez", "Aidan Chiles"]);
assert.equal(scored[1].actual, null); assert.deepEqual(scored[1].missing, ["Nobody Drafted"]);
const summary = summarizeCfbSet(scored, contest.medianScore);
assert.equal(summary.scored, 1); assert.equal(summary.lineups, 2); assert.equal(summary.best?.lineupNumber, 1);

// Projection error: bias = actual - projected; undrafted players are not graded.
const errors = cfbProjectionError([
  { name: "Jackson Gutierrez", position: "QB", proj: 13.2 }, { name: "Rod Robinson II", position: "RB", proj: 12.25 },
  { name: "Nobody Drafted", position: "WR", proj: 9 }, { name: "Zero Proj", position: "WR", proj: 0 },
], fpts);
assert.deepEqual(errors.find((e) => e.position === "QB"), { position: "QB", n: 1, mae: 12.36, bias: 12.36 });
assert.deepEqual(errors.find((e) => e.position === "RB"), { position: "RB", n: 1, mae: 8.45, bias: -8.45 });
assert.equal(errors.find((e) => e.position === "All")!.n, 2);

// Showdown standings: a player appears at UTIL with his base score and at CPT with 1.5x it.
// Base must come from the UTIL row whichever order the rows arrive in; a CPT-only player is divided back.
const sdRows: Array<[string, string, string, number]> = [
  ["Star Wr", "CPT", "30.00%", 45], ["Star Wr", "UTIL", "50.00%", 30],
  ["Good Qb", "UTIL", "70.00%", 20], ["Good Qb", "CPT", "20.00%", 30],
  ["Only Cpt", "CPT", "1.00%", 15],
  ["Cheap Rb", "UTIL", "40.00%", 12], ["Mid Wr", "UTIL", "35.00%", 9], ["Back Rb", "UTIL", "25.00%", 6], ["Slot Wr", "UTIL", "10.00%", 2],
];
const sdLines = Array.from({ length: 40 }, (_, i) => {
  const block = sdRows[i] ? `,,${sdRows[i][0]},${sdRows[i][1]},${sdRows[i][2]},${sdRows[i][3]}` : ",,,,,";
  return `${i + 1},${900 + i},user${i},0,${(110 - i).toFixed(2)},CPT X UTIL Y${block}`;
});
const sd = parseCfbContestStandings([header, ...sdLines].join("\n"));
assert.equal(sd.format, "showdown");
const base = new Map(sd.players.map((x) => [x.name, x] as const));
assert.equal(base.get("Star Wr")!.fpts, 30, "CPT row first: base still from UTIL");
assert.equal(base.get("Good Qb")!.fpts, 20, "UTIL row first: CPT row does not overwrite it");
assert.equal(base.get("Only Cpt")!.fpts, 10, "captained only: CPT points / 1.5");
assert.deepEqual(base.get("Star Wr")!.draftedBySlot, { CPT: 30, UTIL: 50 });
assert.equal(base.get("Star Wr")!.draftedPct, 80);

// Captain scoring: the CPT slot counts 1.5x.
const sdFpts = new Map(sd.players.map((x) => [x.key, x.fpts] as const));
const sp = (dkId: number, name: string, position: "QB" | "RB" | "WR", team: string, salary: number): CfbPoolPlayer =>
  ({ dkId, captainId: dkId + 100, captainSalary: salary * 1.5, name, position, team, game: "A@B", salary, proj: 0 });
const pool = [sp(1, "Star Wr", "WR", "A", 11000), sp(2, "Good Qb", "QB", "B", 10000), sp(3, "Only Cpt", "WR", "A", 9000),
  sp(4, "Cheap Rb", "RB", "B", 6000), sp(5, "Mid Wr", "WR", "A", 5000), sp(6, "Back Rb", "RB", "A", 4000),
  sp(7, "Slot Wr", "WR", "B", 2000), sp(8, "Nobody Drafted", "WR", "B", 1000)];
const sdLineup: CfbLineup = { lineupNumber: 1, salary: 0, projection: 0,
  slots: [{ slot: "CPT", player: pool[1] }, ...[0, 3, 4, 5, 6].map((i) => ({ slot: "FLEX" as const, player: pool[i] }))] };
const sdScored = scoreCfbLineups([sdLineup], sdFpts, sd.scoreCurve, sd.entryCount)[0];
assert.equal(sdScored.actual, 20 * 1.5 + 30 + 12 + 9 + 6 + 2);
assert.equal(sdScored.captain, "Good Qb");

// Perfect lineup: the solver on actual points equals brute force over every 6-player set and Captain.
const perfect = cfbPerfectLineup(pool, sdFpts, "showdown", sd.scoreCurve, sd.entryCount)!;
let oracle = -Infinity;
const scoredPool = pool.filter((x) => base.has(x.name));
for (let mask = 0; mask < 1 << scoredPool.length; mask += 1) {
  const pick = scoredPool.filter((_, i) => mask & (1 << i));
  if (pick.length !== 6 || new Set(pick.map((x) => x.team)).size !== 2) continue;
  for (const c of pick) {
    const salary = pick.reduce((n, x) => n + x.salary, 0) + c.salary * 0.5;
    const pts = pick.reduce((n, x) => n + base.get(x.name)!.fpts, 0) + base.get(c.name)!.fpts * 0.5;
    if (salary <= 50000) oracle = Math.max(oracle, pts);
  }
}
assert.equal(perfect.actual, oracle);
assert.equal(perfect.unscored, 1, "the undrafted player has no score and is left out");
assert.ok(perfect.lineup.slots.every((s) => s.player.name !== "Nobody Drafted"));
assert.equal(perfect.lineup.slots[0].slot, "CPT");
assert.equal(perfect.rank, estimateRank(oracle, sd.scoreCurve, sd.entryCount)!.rank, "placed in the field like any lineup");

// Captain table: ours beside the field's, misses included, Captain points 1.5x.
const captains = cfbCaptainTable([sdLineup], sd.players.map((x) => ({ key: x.key, name: x.name, cpt: x.draftedBySlot.CPT ?? null, fpts: x.fpts })));
assert.deepEqual(captains.find((c) => c.name === "Good Qb"), { name: "Good Qb", ours: 100, field: 20, fpts: 20, captainPoints: 30 });
assert.deepEqual(captains.find((c) => c.name === "Star Wr"), { name: "Star Wr", ours: 0, field: 30, fpts: 30, captainPoints: 45 });

// Classic perfect lineup uses the Classic rules (8 slots, two games).
const cp = (dkId: number, name: string, position: "QB" | "RB" | "WR", game: string): CfbPoolPlayer =>
  ({ dkId, name, position, team: game.slice(0, 1) + dkId, game, salary: 5000, proj: 0 });
const classicPool = [cp(1, "Q1", "QB", "A@B"), cp(2, "Q2", "QB", "C@D"), cp(3, "R1", "RB", "A@B"), cp(4, "R2", "RB", "C@D"),
  cp(5, "R3", "RB", "A@B"), cp(6, "W1", "WR", "A@B"), cp(7, "W2", "WR", "C@D"), cp(8, "W3", "WR", "A@B"), cp(9, "W4", "WR", "C@D")];
const classicFpts = new Map(classicPool.map((x, i) => [x.name.toLowerCase(), 10 + i] as const));
const classicPerfect = cfbPerfectLineup(classicPool, classicFpts, "classic", contest.scoreCurve, contest.entryCount)!;
assert.equal(classicPerfect.lineup.slots.length, 8);
assert.equal(classicPerfect.actual, [11, 12, 13, 14, 15, 16, 17, 18].reduce((a, b) => a + b, 0), "drops the lowest scorer (Q1)");

console.log("CFB results: slots summed, unknown scores stay unknown, ranks and projection error correct; Showdown standings, Captain scoring, perfect lineup and Captain table hold.");
