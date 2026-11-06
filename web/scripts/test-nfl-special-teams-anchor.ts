/**
 * Special-teams anchor (`nfl-special-teams-anchor-v1`) on the DraftKings lines
 * captured for CIN @ ATL on 2026-11-06 (the probe in CLAUDE.md), so the numbers
 * here are the ones quoted there: McPherson 10.1, Folk 9.0, Falcons -1.01.
 */
import assert from "node:assert/strict";
import { anchorDst, anchorKicker, distanceBonusPerFg, expectedCount, kickerPoints, pointsAllowedFromLadder, pointsAllowedFromMean,
  poissonRateForTail, rungsFromOutcomes, survivalCurve, type Rung } from "../src/lib/nfl-dfs/special-teams-anchor";

const near = (a: number, b: number, tol: number, msg: string) => assert.ok(Math.abs(a - b) <= tol, `${msg}: ${a} vs ${b}`);
const over = (point: number, price: number): Rung => ({ point, side: "over", price });
const under = (point: number, price: number): Rung => ({ point, side: "under", price });

// Evan McPherson, DraftKings, 2026-11-06 ~13:00 ET.
const mcphersonFg = [over(1.5, -162), under(1.5, 127), over(0.5, -810), over(2.5, 207), over(3.5, 650)];
const mcphersonPat = [over(2.5, -152), under(2.5, 119), over(0.5, -3000), over(1.5, -477), over(3.5, 162), over(4.5, 386), over(5.5, 920)];
const fg = expectedCount(mcphersonFg)!;
near(fg.expected, 1.86, 0.02, "McPherson expected field goals");
assert.deepEqual(fg.atLeast.map((r) => r.k), [1, 2, 3, 4]);
near(fg.atLeast[1].p, 0.58, 0.01, "P(2+ FG) from the two-sided main line");
const pat = expectedCount(mcphersonPat)!;
near(pat.expected, 2.91, 0.02, "McPherson expected extra points");
// His own distance mix from the projection's stat means (0.12 of 1.02 made from 40-49, 0.39 from 50+).
const bonus = distanceBonusPerFg({ fg_made: 1.02, fg_made_40_49: 0.12, fg_made_50_59: 0.39, fg_made_60_: 0 });
assert.equal(bonus.source, "history");
near(bonus.bonus, 0.88, 0.01, "distance bonus per FG");
const read = kickerPoints({ fieldGoals: mcphersonFg, pats: mcphersonPat }, bonus.bonus)!;
near(read.points, 10.1, 0.1, "McPherson book-implied DK points");
const mcpherson = anchorKicker({ before: { mean: 7.4, p10: 4.0, median: 7.0, p90: 12.0 }, lines: { fieldGoals: mcphersonFg, pats: mcphersonPat },
  statMeans: { fg_made: 1.02, fg_made_40_49: 0.12, fg_made_50_59: 0.39 }, book: "draftkings", capturedAt: "2026-11-06T18:07:48Z" })!;
near(mcpherson.after.mean, 10.1, 0.1, "anchored mean");
near(mcpherson.after.p90!, 12.0 * mcpherson.after.mean / 7.4, 1e-9, "range rescaled to the new mean");
assert.equal(mcpherson.before.mean, 7.4);
assert.equal(mcpherson.mode, "kicker_lines");

// Nick Folk: no distance history -> league prior; 9.0 quoted used his own 0.38, so the prior lands a little higher.
const folkFg = [over(1.5, -161), under(1.5, 126), over(0.5, -690), over(2.5, 177), over(3.5, 452), over(4.5, 1140)];
const folkPat = [over(2.5, 126), under(2.5, -161), over(0.5, -1440), over(1.5, -263), over(3.5, 365), over(4.5, 1040)];
const folk = anchorKicker({ before: { mean: 6.2, p10: 0, median: 5.5, p90: 12 }, lines: { fieldGoals: folkFg, pats: folkPat }, statMeans: null, book: "draftkings", capturedAt: "2026-11-06T18:07:48Z" })!;
assert.equal(folk.kicker!.distanceSource, "league");
near(folk.kicker!.efg, 1.99, 0.02, "Folk expected field goals");
near(folk.kicker!.epat, 2.27, 0.02, "Folk expected extra points");
near(folk.after.mean, 3 * 1.99 + 2.27 + 0.55 * 1.99, 0.1, "Folk anchored mean with the league distance prior");

// A kicker with only a main line and no ladder gets a Poisson fit of that line (P(2+ FG) = 0.58 -> about 1.9); no PAT market leaves him on the model.
const mainOnly = kickerPoints({ fieldGoals: [over(1.5, -162), under(1.5, 127)], pats: mcphersonPat }, 0.5)!;
assert.equal(mainOnly.method, "poisson_main_line");
near(mainOnly.efg, poissonRateForTail(2, 0.58), 0.02, "Poisson rate from the main line");
near(mainOnly.efg, 1.9, 0.15, "the Poisson fit lands near the ladder's 1.86");
assert.equal(read.method, "ladder");
assert.equal(anchorKicker({ before: { mean: 7.4, p10: 4, median: 7, p90: 12 }, lines: { fieldGoals: mcphersonFg, pats: [] }, book: "draftkings", capturedAt: "x" }), null);
assert.equal(anchorKicker({ before: { mean: 0, p10: 0, median: 0, p90: 0 }, lines: { fieldGoals: mcphersonFg, pats: mcphersonPat }, book: "draftkings", capturedAt: "x" }), null, "a zeroed kicker is never revived by lines");

// Outcomes as the capture stores them (a pre-fix alternate_team_totals row carries both `line` and `outcomes`; only outcomes are read).
const rungs = rungsFromOutcomes([{ name: "Over", side: "over", point: 1.5, price: -162, description: "x" }, { name: "Under", side: "under", point: 1.5, price: 127 }, { name: "Yes", side: "yes", price: 400 }]);
assert.deepEqual(rungs, [over(1.5, -162), under(1.5, 127)]);

// Survival curve: one-sided rungs de-vigged by the nearest pair's overround and forced non-increasing.
const curve = survivalCurve([over(0.5, -810), over(1.5, -162), under(1.5, 127), over(2.5, 207)]);
assert.deepEqual(curve.map((c) => c.point), [0.5, 1.5, 2.5]);
assert.ok(curve[0].p >= curve[1].p && curve[1].p >= curve[2].p, "non-increasing");
near(curve[1].p, 0.58, 0.01, "main line de-vigged by its own pair");

// Atlanta Falcons alternate team totals, DraftKings, 2026-11-06 (a 42-rung ladder; the first and last rungs here).
const falconsLadder: Rung[] = [];
const falconsPrices: [number, number, number][] = [[14.5, -700, 450], [17.5, -400, 300], [20.5, -210, 170], [23.5, -125, 102], [24.5, -108, -112], [26.5, 120, -145],
  [27.5, 135, -165], [30.5, 215, -270], [33.5, 330, -430], [34.5, 370, -500]];
for (const [point, o, u] of falconsPrices) falconsLadder.push(over(point, o), under(point, u));
const falcons = pointsAllowedFromLadder(falconsLadder)!;
near(falcons.expectedPoints, 24.8, 0.6, "Falcons expected points from the ladder");
assert.equal(falcons.tiers.length, 7);
near(falcons.tiers.reduce((s, t) => s + t.p, 0), 1, 0.02, "tier probabilities sum to one");
assert.ok(falcons.expectation < 0, "a 24.8-point opponent costs the DST points-allowed points");
const model = pointsAllowedFromMean(24.75);
near(model.tiers.reduce((s, t) => s + t.p, 0), 1, 1e-6, "normal tiers sum to one");
const bengals = anchorDst({ before: { mean: 4.5, p10: 2.0, median: 3.7, p90: 7.2 }, opponent: "Falcons", opponentLadder: falconsLadder, modelImpliedTotal: 24.75, book: "draftkings", capturedAt: "2026-11-06T18:07:48Z" })!;
near(bengals.dst!.shift, falcons.expectation - model.expectation, 1e-9, "the shift is ladder minus normal at the model's mean");
assert.ok(Math.abs(bengals.dst!.shift) < 1.5, `a same-mean ladder moves a DST by its shape only, got ${bengals.dst!.shift}`);
near(bengals.after.mean - bengals.before.mean, bengals.dst!.shift, 1e-9, "additive shift");
near(bengals.after.p90! - bengals.before.p90!, bengals.dst!.shift, 1e-9, "range shifted, not scaled");
assert.equal(bengals.mode, "dst_points_allowed");
// Three rungs are not a distribution; a missing model mean anchors nothing.
assert.equal(pointsAllowedFromLadder(falconsLadder.slice(0, 6)), null);
assert.equal(anchorDst({ before: { mean: 4.5, p10: 2, median: 3.7, p90: 7.2 }, opponent: "Falcons", opponentLadder: falconsLadder, modelImpliedTotal: NaN, book: "draftkings", capturedAt: "x" }), null);
// A ladder centred lower than the model's mean moves the DST up; higher moves it down.
const low = anchorDst({ before: { mean: 4.5, p10: 2, median: 3.7, p90: 7.2 }, opponent: "Falcons", opponentLadder: falconsLadder, modelImpliedTotal: 28, book: "draftkings", capturedAt: "x" })!;
const high = anchorDst({ before: { mean: 4.5, p10: 2, median: 3.7, p90: 7.2 }, opponent: "Falcons", opponentLadder: falconsLadder, modelImpliedTotal: 21, book: "draftkings", capturedAt: "x" })!;
assert.ok(low.dst!.shift > bengals.dst!.shift && high.dst!.shift < bengals.dst!.shift, "the shift follows the gap between the ladder and the model's mean");

console.log("NFL special-teams anchor: McPherson 10.1, Folk with the league distance prior, ladder survival, Falcons tiers, DST shift direction, refusals passed");
