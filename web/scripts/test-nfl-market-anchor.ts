/**
 * Market anchor (`nfl-market-anchor-v2`): the TypeScript port reproduces
 * model/nfl_prop_implied.py (CALIBRATION_V1) on four real-shaped players, and
 * the anchor rescales the model's range around the books' mean.
 */
import assert from "node:assert/strict";
import { anchorPlayer, dkPoints, inverseNormal, statLine, type PropQuote } from "../src/lib/nfl-dfs/market-anchor";

const near = (a: number, b: number, tol = 0.01, msg = "") => assert.ok(Math.abs(a - b) <= tol, `${msg} ${a} vs ${b}`);

assert.ok(Math.abs(inverseNormal(0.975) - 1.959964) < 1e-6, "inverse normal");
assert.ok(Math.abs(inverseNormal(0.5)) < 1e-9);

// Expected values printed by the Python module (stat_line(..., calibration=CALIBRATION_V1), dk_points).
const FIXTURES: Record<string, { lines: Record<string, PropQuote>; points: number; stats: Record<string, number> }> = {
  QB: { lines: { player_pass_yds: { line: 245.5, over: -118, under: -108 }, player_pass_tds: { line: 1.5, over: -130, under: 102 },
    player_pass_interceptions: { line: 0.5, over: -140, under: 110 }, player_rush_yds: { line: 18.5, over: -112, under: -114 },
    player_anytime_td: { yes: 450 } }, points: 20.311,
    stats: { passing_yards: 252.7464, passing_tds: 1.8221, passing_interceptions: 0.7878, rushing_yards: 19.5036, scrimmage_tds: 0.184 } },
  RB: { lines: { player_rush_yds: { line: 69.5, over: -113, under: -111 }, player_receptions: { line: 2.5, over: -156, under: 122 },
    player_anytime_td: { yes: -125 } }, points: 17.4756,
    stats: { rushing_yards: 73.6554, receptions: 2.8988, receiving_yards: 21.741, scrimmage_tds: 0.7209 } },
  WR: { lines: { player_receptions: { line: 5.5, over: -129, under: 104 }, player_reception_yds: { line: 65.5, over: -111, under: -115 },
    player_anytime_td: { yes: 155 } }, points: 17.285,
    stats: { receptions: 6.0229, receiving_yards: 76.4815, scrimmage_tds: 0.4503 } },
  TE: { lines: { player_receptions: { line: 3.5, over: 123, under: -155 }, player_reception_yds: { line: 28.5, over: -109, under: -117 },
    player_anytime_td: { yes: 300 } }, points: 8.4524,
    stats: { receptions: 3.5679, receiving_yards: 33.0687, scrimmage_tds: 0.2628 } },
};
for (const [pos, f] of Object.entries(FIXTURES)) {
  const s = statLine(pos, f.lines);
  assert.ok(s, `${pos} has a full line set`);
  for (const [k, v] of Object.entries(f.stats)) near(s![k as keyof typeof s], v, 0.002, `${pos} ${k}`);
  near(dkPoints(pos, s!), f.points, 0.01, `${pos} points`);
}

// Missing a required market: no anchor, never a zero.
assert.equal(statLine("WR", { player_receptions: { line: 4.5, over: -110, under: -110 } }), null);
assert.equal(statLine("DST", {}), null, "defenses are never anchored");

// The anchor rescales the model's range to the books' mean, keeping its shape and the model's number.
const a = anchorPlayer({ position: "RB", ourProj: 15, floorFpts: 6, medianFpts: 14, ceilingFpts: 28, lines: FIXTURES.RB.lines,
  book: "draftkings", capturedAt: "2026-10-11T15:23:16Z" })!;
near(a.after.mean, 17.4756, 0.01, "anchored mean");
near(a.after.p90!, 28 * 17.4756 / 15, 0.02, "range scaled");
assert.equal(a.before.mean, 15, "model's own number kept");
assert.equal(anchorPlayer({ position: "RB", ourProj: 0, floorFpts: 0, medianFpts: 0, ceilingFpts: 0, lines: FIXTURES.RB.lines,
  book: "draftkings", capturedAt: "x" }), null, "no positive model mean to rescale: left alone");

// v2: a QB without anytime-TD / rushing-yards lines takes the model's own
// rushing means for the missing ones (v1 read them as 0). Values from Python:
// stat_line("QB", ..., calibration=CALIBRATION_V1, qb_rush_fallback=...).
const QB_PASS_ONLY: Record<string, PropQuote> = { player_pass_yds: FIXTURES.QB.lines.player_pass_yds,
  player_pass_tds: FIXTURES.QB.lines.player_pass_tds, player_pass_interceptions: FIXTURES.QB.lines.player_pass_interceptions };
const model = { rushing_yards: 38.2, rushing_tds: 0.31 };
const filled: string[] = [];
const qb = statLine("QB", QB_PASS_ONLY, model, filled)!;
near(qb.rushing_yards, 38.2, 1e-9, "QB rush yards from model"); near(qb.scrimmage_tds, 0.31, 1e-9, "QB rush TDs from model");
near(dkPoints("QB", qb), 22.9387, 0.01, "QB fallback points");
assert.deepEqual(filled, ["player_anytime_td", "player_rush_yds"]);
near(dkPoints("QB", statLine("QB", QB_PASS_ONLY)!), 17.2568, 0.01, "no fallback: research behaviour unchanged");
const partial: string[] = [];
const qb2 = statLine("QB", { ...QB_PASS_ONLY, player_rush_yds: FIXTURES.QB.lines.player_rush_yds }, model, partial)!;
near(qb2.rushing_yards, 19.5036, 0.002, "a posted line wins over the model"); near(dkPoints("QB", qb2), 21.0672, 0.01);
assert.deepEqual(partial, ["player_anytime_td"]);
near(statLine("QB", FIXTURES.QB.lines, model)!.scrimmage_tds, 0.184, 0.002, "full line set ignores the fallback");
assert.equal(statLine("WR", FIXTURES.WR.lines, model)!.rushing_yards, 0, "fallback is QB-only");
const qa = anchorPlayer({ position: "QB", ourProj: 21, floorFpts: 12, medianFpts: 20, ceilingFpts: 31, lines: QB_PASS_ONLY,
  book: "draftkings", capturedAt: "x", qbRushFallback: model })!;
near(qa.after.mean, 22.9387, 0.01, "QB anchor uses the fallback"); assert.deepEqual(qa.modelFilled, ["player_anytime_td", "player_rush_yds"]);
assert.equal(a.modelFilled.length, 0, "RB anchor fills nothing");

console.log("Market anchor: the TypeScript port matches model/nfl_prop_implied.py and rescales ranges around the books' mean.");
