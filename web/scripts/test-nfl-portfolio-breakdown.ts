/**
 * Portfolio breakdown and lineup filters (lib/nfl-dfs/portfolio-breakdown.ts):
 * captain counts with field captain ownership, Showdown team splits by kind
 * (3-3 / 4-2 / 5-1) and by team, the Classic stack label, quarterbacks and
 * kickers, and filters that combine.
 */
import assert from "node:assert/strict";
import { breakdownPortfolio, isFilterActive, lineupShape, matchesFilter, type BreakdownLineup, type BreakdownPlayer } from "../src/lib/nfl-dfs/portfolio-breakdown";

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

console.log("Portfolio breakdown: captains, splits, structure and combined filters passed.");
