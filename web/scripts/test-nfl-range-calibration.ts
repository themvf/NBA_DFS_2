/**
 * nfl-range-calibration-v1: chance of no stat line, means pulled toward the
 * middle (ranges rescaled with them, never widened),
 * and simulated games that score a 0 for a player's not-playing share.
 */
import assert from "node:assert/strict";
import { calibratePlayer, calibratePlayers, depthRank, notPlayChance, MEAN_CALIBRATION, type CalibrationInput } from "../src/lib/nfl-dfs/range-calibration";
import { buildDrawModel, drawOutcome, type DrawPlayer } from "../src/lib/nfl-dfs/lineup-draws";

const base = (over: Partial<CalibrationInput>): CalibrationInput => ({
  position: "WR", isOut: false, projectionStatus: "projected", ourProj: 10, floorFpts: 3, medianFpts: 9, ceilingFpts: 20,
  boomRate: 0.05, depthRole: "Listed WR1", availabilityState: "probable", marketAnchor: null, ...over,
});
const close = (a: number, b: number, tol: number, msg: string) => assert.ok(Math.abs(a - b) <= tol, `${msg}: ${a} vs ${b}`);

// --- 1. Depth rank and the chance of a 0 ---
assert.equal(depthRank("WR", "Listed WR3", "probable"), 3);
assert.equal(depthRank("QB", "Expected starter — QB1", "probable"), 1);
assert.equal(depthRank("WR", null, "probable"), "unlisted", "current roster evidence and no rank: not on the chart");
assert.equal(depthRank("WR", null, "stale"), null, "stale evidence says nothing");
assert.equal(notPlayChance(base({ depthRole: null })).notPlay, 0.96, "an unlisted WR almost never plays");
assert.equal(notPlayChance(base({ depthRole: null, availabilityState: "unknown" })).notPlay, 0, "no depth chart, no adjustment");
assert.equal(notPlayChance(base({ depthRole: "Listed WR4", marketAnchor: { mean: 5 } })).notPlay, 0.15, "books with lines cap it at 15%");
assert.equal(notPlayChance(base({ position: "RB", depthRole: "Listed RB3", absenceCommit: { kind: "x" } })).notPlay, 0.05, "a promoted backup plays");
assert.equal(notPlayChance(base({ position: "QB", depthRole: null })).notPlay, 0, "an unlisted QB is left alone");
assert.equal(notPlayChance(base({ position: "DST" })).notPlay, 0);

// --- 2. Quez Watkins, weeks 5-8: 7.4 projected at $3,000, not on the chart, zero every week ---
const quez = calibratePlayer(base({ ourProj: 7.4, floorFpts: 1.5, medianFpts: 6, ceilingFpts: 16, depthRole: null }))!;
const quezPlays = MEAN_CALIBRATION.WR.a + MEAN_CALIBRATION.WR.b * 7.4;
close(quez.rangeCalibration.ifPlays.mean, quezPlays, 0.01, "if-he-plays mean follows the WR line");
close(quez.ourProj!, 0.04 * quezPlays, 0.01, "projection is 4% of his if-he-plays number");
assert.equal(quez.ceilingFpts, 0, "a 96% chance of a 0 means his P90 is 0");
assert.equal(quez.medianFpts, 0);
assert.ok(quez.boomRate! < 0.005, "boom shrinks with the chance he plays");

// --- 3. Starters: stars come down, backups up, ranges only rescaled ---
const star = calibratePlayer(base({ ourProj: 22, floorFpts: 11, medianFpts: 21, ceilingFpts: 35 }))!;
assert.ok(star.ourProj! < 22 && star.ourProj! > 18.5, `a 22-point WR1 comes down (${star.ourProj})`);
const backup = calibratePlayer(base({ ourProj: 4, floorFpts: 0.5, medianFpts: 3, ceilingFpts: 10, depthRole: "Listed WR2" }))!;
assert.ok(backup.rangeCalibration.ifPlays.mean > 4, "a 4-point WR2 goes up if he plays");
const s = star.rangeCalibration.ifPlays.mean / 22;
close(star.rangeCalibration.ifPlays.p90, 35 * s, 0.02, "the range is rescaled to the new mean, not widened");
close(star.rangeCalibration.ifPlays.p10, 11 * s, 0.02, "same for the floor");

// --- 4. Not calibrated ---
assert.equal(calibratePlayer(base({ isOut: true })), null);
assert.equal(calibratePlayer(base({ position: "K" })), null);
assert.equal(calibratePlayer(base({ ourProj: 0 })), null);
assert.equal(calibratePlayer(base({ medianFpts: null })), null, "no median, no range to calibrate");
const many = calibratePlayers([base({}), base({ depthRole: null }), base({ position: "DST" })]);
assert.equal(many.applied, 2);
assert.equal(many.notPlaying, 1);
assert.equal(many.players[2].rangeCalibration, undefined);

// --- 5. Simulated games: the 0 comes from its own coin ---
const players: DrawPlayer[] = [
  { id: 1, position: "QB", team: "AAA", opponent: "BBB", gameKey: "AAA@BBB", mean: 18, p10: 9, median: 17, p90: 29 },
  { id: 2, position: "WR", team: "AAA", opponent: "BBB", gameKey: "AAA@BBB", mean: 8.4, p10: 1.4, median: 6.1, p90: 17.6, notPlay: 0.96 },
  { id: 3, position: "WR", team: "BBB", opponent: "AAA", gameKey: "AAA@BBB", mean: 12, p10: 4, median: 11, p90: 22 },
];
const withChance = buildDrawModel(players, 11);
const without = buildDrawModel(players.map((p) => ({ ...p, notPlay: 0 })), 11);
let zeros = 0, sum = 0;
const N = 5000;
for (let i = 1; i <= N; i++) {
  const a = drawOutcome(withChance, i), b = drawOutcome(without, i);
  assert.equal(a.get(1), b.get(1), "players with no chance of a 0 draw exactly what they drew before");
  assert.equal(a.get(3), b.get(3));
  const v = a.get(2)!;
  if (v === 0) zeros += 1;
  sum += v;
}
close(zeros / N, 0.96, 0.01, "he scores 0 in 96% of games");
close(sum / N, 0.04 * 8.4, 0.08, "his draws average his projection");

console.log("nfl range calibration tests passed");
