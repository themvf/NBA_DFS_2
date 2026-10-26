/**
 * Market anchor (`nfl-market-anchor-v3`): the TypeScript port reproduces
 * model/nfl_prop_implied.py (CALIBRATION_V1, td_only_delta) on real-shaped
 * players, and the anchor rescales the model's range around the books' mean.
 */
import assert from "node:assert/strict";
import { anchorPlayer, dkPoints, inverseNormal, modelTouchdowns, statLine, tdOnlyDelta, type PropQuote } from "../src/lib/nfl-dfs/market-anchor";

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
assert.equal(a.mode, "full");
assert.deepEqual(a.fromMarket, ["player_rush_yds", "player_receptions", "player_anytime_td"], "full RB: the markets it read");
assert.deepEqual(qa.fromMarket, ["player_pass_yds", "player_pass_tds", "player_pass_interceptions"], "QB fallback: model-filled markets are not from the market");

// v3: an RB/WR/TE without a full set but with an anytime-TD line moves only the
// touchdown term. Deltas from Python: td_only_delta(pos, lines, model_tds).
for (const [pos, yes, modelTds, delta] of [["RB", 400, 0.25, -0.273691], ["WR", 650, 0.08, 0.309034], ["TE", 155, 0.30, 0.901519]] as const) {
  near(tdOnlyDelta(pos, { player_anytime_td: { yes } }, modelTds)!, delta, 1e-4, `${pos} TD-only delta`);
}
assert.equal(tdOnlyDelta("QB", { player_anytime_td: { yes: 400 } }, 0.25), null, "QBs are never TD-only");
assert.equal(tdOnlyDelta("RB", {}, 0.25), null, "no anytime-TD line: nothing");
assert.equal(tdOnlyDelta("RB", { player_anytime_td: { yes: 400 } }, null), null, "no model TDs: nothing");
assert.equal(modelTouchdowns({ rushing_yards: 30 }), null, "a model with no TD means is not anchored");
const backupMeans = { rushing_yards: 22.0, receptions: 1.4, receiving_yards: 9.5, rushing_tds: 0.18, receiving_tds: 0.07 };
const td = anchorPlayer({ position: "RB", ourProj: 6.2, floorFpts: 1.0, medianFpts: 5.1, ceilingFpts: 14.8,
  lines: { player_anytime_td: { yes: 400 }, player_rush_yds: { line: 24.5, over: -115, under: -115 } },
  book: "draftkings", capturedAt: "x", modelStatMeans: backupMeans })!;
assert.equal(td.mode, "td_only", "partial set without receptions: TD-only, the rush-yards line is not used");
near(td.after.mean, 6.2 - 0.273691, 1e-4, "TD-only mean = model + delta");
near(td.after.p90!, 14.8 * td.after.mean / 6.2, 1e-9, "range rescaled as in v2");
near(td.statLine.scrimmage_tds, 0.25 - 0.273691 / 6, 1e-5, "TDs from the line");
near(td.statLine.rushing_yards, 22.0, 1e-9, "every other stat is the model's");
assert.deepEqual(td.fromMarket, ["player_anytime_td"]);
assert.equal(td.modelTds, 0.25);
assert.equal(td.before.mean, 6.2, "model's own number kept");
assert.equal(anchorPlayer({ position: "WR", ourProj: 6, floorFpts: 1, medianFpts: 5, ceilingFpts: 14,
  lines: { player_anytime_td: { yes: 400 } }, book: "draftkings", capturedAt: "x" }), null, "no model stat means: not anchored");
assert.equal(anchorPlayer({ position: "QB", ourProj: 6, floorFpts: 1, medianFpts: 5, ceilingFpts: 14,
  lines: { player_anytime_td: { yes: 400 } }, book: "draftkings", capturedAt: "x", modelStatMeans: backupMeans }), null,
  "a QB without passing lines stays on the model");
const fullWithMeans = anchorPlayer({ position: "RB", ourProj: 15, floorFpts: 6, medianFpts: 14, ceilingFpts: 28, lines: FIXTURES.RB.lines,
  book: "draftkings", capturedAt: "x", modelStatMeans: backupMeans })!;
assert.equal(fullWithMeans.mode, "full"); near(fullWithMeans.after.mean, 17.4756, 0.01, "a full set is v2, whatever the model says");

console.log("Market anchor: the TypeScript port matches model/nfl_prop_implied.py and rescales ranges around the books' mean.");
