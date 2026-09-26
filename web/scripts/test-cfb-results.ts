/**
 * CFB contest results: standings parsing (per-slot rows summed), lineup
 * scoring, ranks, set summaries and projection error. Synthetic file in
 * DraftKings' layout; the real one holds other entrants' usernames.
 */
import assert from "node:assert/strict";
import {
  cfbProjectionError, estimateRank, parseCfbContestStandings, scoreCfbLineups, summarizeCfbSet,
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
assert.throws(() => parseCfbContestStandings([header, "1,1,u,0,100,CPT A,,A,CPT,10%,30"].join("\n")), /Showdown/);

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

console.log("CFB results: slots summed, unknown scores stay unknown, ranks and projection error correct.");
