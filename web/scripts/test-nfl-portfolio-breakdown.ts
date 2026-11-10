/**
 * Portfolio breakdown and lineup filters (lib/nfl-dfs/portfolio-breakdown.ts):
 * captain counts with field captain ownership, Showdown team splits by kind
 * (3-3 / 4-2 / 5-1) and by team, the Classic stack label, quarterbacks and
 * kickers, and filters that combine.
 */
import assert from "node:assert/strict";
import { breakdownPortfolio, comparePortfolios, fieldOwnershipSource, isFilterActive, lineupShape, matchesFilter, pairBreakdown, rosterKey, scoreByShape, type BreakdownLineup, type BreakdownPlayer } from "../src/lib/nfl-dfs/portfolio-breakdown";
import { addToPool, MAX_POOL_LINEUPS, parsePool, poolOverlap, removeFromPool, validatePicks } from "../src/lib/nfl-dfs/lineup-pool";

const p = (dkPlayerId: number, name: string, team: string, position: string, captainOwnPct: number | null = null): BreakdownPlayer =>
  ({ dkPlayerId, name, team, position, ownPct: null, captainOwnPct });
const allen = p(1, "Josh Allen", "BUF", "QB", 21.6), cook = p(2, "James Cook III", "BUF", "RB", 17.1), moore = p(3, "DJ Moore", "BUF", "WR"),
  bass = p(4, "Tyler Bass", "BUF", "K"), wentz = p(5, "Carson Wentz", "MIN", "QB", 5.9), jj = p(6, "Justin Jefferson", "MIN", "WR", 13.4),
  hock = p(7, "T.J. Hockenson", "MIN", "TE"), reich = p(8, "Will Reichard", "MIN", "K");
const showdown = (n: number, cpt: BreakdownPlayer, flex: BreakdownPlayer[]): BreakdownLineup =>
  ({ lineupNumber: n, slots: [{ slot: "CPT", player: cpt }, ...flex.map((player) => ({ slot: "FLEX", player }))] });

const lineups = [
  showdown(1, allen, [cook, moore, bass, wentz, jj]),     // BUF 4-2, both QBs, kicker
  showdown(2, allen, [cook, moore, jj, wentz, hock]),     // 3-3, both QBs
  showdown(3, jj, [wentz, hock, reich, allen, cook]),     // MIN 4-2, both QBs, kicker
  showdown(4, cook, [allen, moore, bass, jj, hock]),      // BUF 4-2, one QB, kicker
  showdown(5, allen, [cook, moore, bass, jj, reich]),     // BUF 4-2, one QB, two kickers' teams
];

assert.equal(lineupShape(lineups[0], "showdown").split, "BUF 4-2");
assert.equal(lineupShape(lineups[1], "showdown").split, "3-3", "an even split carries no team");
assert.equal(lineupShape(lineups[2], "showdown").split, "MIN 4-2");

const b = breakdownPortfolio(lineups, "showdown");
assert.equal(b.total, 5);
assert.deepEqual(b.captains.map((c) => [c.label, c.count]), [["Josh Allen", 3], ["James Cook III", 1], ["Justin Jefferson", 1]]);
assert.equal(b.captains[0].fieldPct, 21.6, "field captain ownership travels with the captain row");
assert.deepEqual(b.splitKinds.map((s) => [s.key, s.count]), [["3-3", 1], ["4-2", 4]], "kinds in 3-3 / 4-2 / 5-1 order");
assert.deepEqual(b.splits.map((s) => [s.key, s.count]), [["BUF 4-2", 3], ["3-3", 1], ["MIN 4-2", 1]]);
assert.deepEqual(b.quarterbacks.map((q) => [q.key, q.count]), [["Both QBs", 3], ["One QB", 2]]);
assert.equal(b.kicker, 4);
assert.equal(b.teamSlots.reduce((sum, t) => sum + t.count, 0), 30, "every roster spot counted once");

const shapes = lineups.map((l) => lineupShape(l, "showdown"));
const pick = (f: Parameters<typeof matchesFilter>[1]) => shapes.flatMap((s, i) => (matchesFilter(s, f) ? [lineups[i].lineupNumber] : []));
assert.deepEqual(pick({}), [1, 2, 3, 4, 5]);
assert.equal(isFilterActive({}), false);
assert.equal(isFilterActive({ captainId: null, split: null }), false, "cleared fields are not an active filter");
assert.deepEqual(pick({ captainId: 1 }), [1, 2, 5]);
assert.deepEqual(pick({ splitKind: "4-2" }), [1, 3, 4, 5]);
assert.deepEqual(pick({ split: "BUF 4-2" }), [1, 4, 5]);
assert.deepEqual(pick({ captainId: 1, splitKind: "4-2" }), [1, 5], "filters combine");
assert.deepEqual(pick({ quarterbacks: "One QB" }), [4, 5]);
assert.deepEqual(pick({ kicker: false }), [2]);
assert.deepEqual(pick({ includes: 8 }), [3, 5]);
assert.deepEqual(pick({ excludes: 5, captainId: 1 }), [5]);

// Classic: the stack label comes from the lineup's stack summary.
const classic: BreakdownLineup = { lineupNumber: 1, slots: [{ slot: "QB", player: allen }, { slot: "WR", player: moore }], stackSummary: { quarterback: "Josh Allen", passCatchers: ["DJ Moore", "Dalton Kincaid"], bringBack: "Justin Jefferson" } };
assert.equal(lineupShape(classic, "classic").split, "QB+2 + bring-back");
assert.equal(lineupShape({ ...classic, stackSummary: { quarterback: null, passCatchers: [], bringBack: null } }, "classic").split, "No QB stack");
assert.deepEqual(breakdownPortfolio([classic], "classic").captains, [], "Classic has no captains");

// Pairs: captain-with counts are ordered; together counts are not.
const pairs = pairBreakdown(lineups, 2);
const capCook = pairs.captainWith.find((r) => r.a.name === "Josh Allen" && r.b.name === "James Cook III")!;
assert.equal(capCook.count, 3, "Allen captained 3 times, Cook in all three");
assert.equal(capCook.pct, 60);
assert.equal(capCook.independentPct, (60 * 80) / 100, "chance = captain share x flex share");
const allenCook = pairs.together.find((r) => r.key === "1|2")!;
assert.equal(allenCook.count, 5, "Allen and Cook are together in every lineup, in either slot");
assert.ok(pairs.together.every((r) => r.count >= 2), "pairs in one lineup only are left out");
assert.ok(pairs.together.every((r, i, all) => i === 0 || all[i - 1].count >= r.count), "most common first");

// Comparison: exposure change sorted by size; identical rosters counted.
const cmp = comparePortfolios(lineups.slice(0, 2), lineups, "showdown");
assert.equal(cmp.sharedLineups, 2, "lineups 1 and 2 are in both");
assert.equal(cmp.players.find((x) => x.dkPlayerId === 7)!.pctA, 50, "Hockenson in 1 of the first 2");
assert.ok(Math.abs(cmp.players[0].pctB - cmp.players[0].pctA) >= Math.abs(cmp.players.at(-1)!.pctB - cmp.players.at(-1)!.pctA));
assert.notEqual(rosterKey(showdown(9, allen, [cook, moore, bass, wentz, jj])), rosterKey(showdown(9, cook, [allen, moore, bass, wentz, jj])),
  "captain and flex of the same players are different rosters");

// Scores by shape: averages, best, beat-median, and lineups missing from the scores skipped.
const actual = new Map<number, number | null>([[1, 100], [2, 80], [3, 120], [4, null]]);
const scores = scoreByShape(lineups, "showdown", actual, 90);
const allenCpt = scores.captains.find((c) => c.label === "Josh Allen")!;
assert.deepEqual([allenCpt.lineups, allenCpt.scored, allenCpt.average, allenCpt.best, allenCpt.aboveMedian], [2, 2, 90, 100, 1], "lineup 5 has no score row, so it is skipped");
const fourTwo = scores.splitKinds.find((k) => k.key === "4-2")!;
assert.deepEqual([fourTwo.lineups, fourTwo.scored, fourTwo.average], [3, 2, 110], "an unknown score counts as a lineup but not as scored");
assert.deepEqual(scores.splits.map((x) => x.key).sort(), ["BUF 4-2", "MIN 4-2"]);
assert.equal(scoreByShape(lineups, "showdown", actual, null).captains[0].aboveMedian, null, "no median, no beat count");

// Where the field numbers come from.
assert.equal(fieldOwnershipSource([{ ownSource: "linestar", ownPct: 10 }], "showdown").short, "LineStar");
const est = fieldOwnershipSource([{ ownSource: "nfl-ownership-prior-v3", ownPct: 10 }, { ownSource: "linestar", ownPct: 4 }], "showdown");
assert.equal(est.short, "est.");
assert.match(est.note, /not DraftKings' numbers/);
assert.match(est.note, /LineStar for 1 players/);
assert.match(fieldOwnershipSource([{ ownSource: null, ownPct: 3 }], "classic").note, /about 1 point/);

// The final set.
const runA = "11111111-1111-1111-1111-111111111111", runB = "22222222-2222-2222-2222-222222222222";
const rosterOf = (pick: { runId: string; lineupNumber: number }) => {
  const source = pick.runId === runA ? lineups : [showdown(1, allen, [cook, moore, bass, wentz, jj]), showdown(2, jj, [allen, cook, moore, wentz, hock])];
  const l = source.find((x) => x.lineupNumber === pick.lineupNumber); return l ? rosterKey(l) : null;
};
const first = addToPool([], runA, lineups.slice(0, 3), rosterOf);
assert.deepEqual([first.added, first.duplicates], [3, 0]);
const again = addToPool(first.picks, runA, lineups.slice(0, 4), rosterOf);
assert.deepEqual([again.added, again.duplicates, again.picks.length], [1, 3, 4], "lineups already in the pool are skipped");
const fromB = addToPool(again.picks, runB, [showdown(1, allen, [cook, moore, bass, wentz, jj]), showdown(2, jj, [allen, cook, moore, wentz, hock])], rosterOf);
assert.deepEqual([fromB.added, fromB.duplicates], [1, 1], "run B's lineup 1 repeats run A's lineup 1 roster");
assert.deepEqual(fromB.picks.map((x) => `${x.runId.slice(0, 1)}${x.lineupNumber}`), ["11", "12", "13", "14", "22"], "picks keep the order added");
assert.deepEqual(removeFromPool(fromB.picks, [{ runId: runA, lineupNumber: 2 }]).length, 4);
const full = addToPool(Array.from({ length: MAX_POOL_LINEUPS - 1 }, (_, i) => ({ runId: runB, lineupNumber: 100 + i })), runA, lineups.slice(0, 3), () => null);
assert.deepEqual([full.added, full.overLimit, full.picks.length], [1, 2, MAX_POOL_LINEUPS], "the pool stops at the DraftKings limit");
assert.deepEqual(parsePool(JSON.stringify([{ runId: runA, lineupNumber: 1 }, { runId: "x", lineupNumber: 2 }, { runId: runB, lineupNumber: 0 }, "junk"])), [{ runId: runA, lineupNumber: 1 }], "stored picks are validated");
assert.deepEqual(parsePool("{not json"), []);
assert.match(validatePicks([])!, /empty/);
assert.match(validatePicks([{ runId: runA, lineupNumber: 1 }, { runId: runA, lineupNumber: 1 }])!, /twice/);
assert.equal(validatePicks(fromB.picks), null);
const overlap = poolOverlap([lineups[0], lineups[4]], 5);
assert.deepEqual([overlap.maxShared, overlap.closePairs], [5, 1], "lineups 1 and 5 share five of six players");

console.log("Portfolio breakdown: captains, splits, structure, filters, pairs, comparison, scores by shape, field source and the final set passed.");
