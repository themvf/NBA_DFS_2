/**
 * Long-memory ranges (`nfl-dfs-range-split-v1`): floor/ceiling = projection x
 * the 17-game ratios, the projection never moves, the median stays inside the
 * new range, and the replaced range is kept in `before`. Ghost arms re-apply
 * the ratios to whichever projection they use. The defensive capture's
 * reproduction check reads `before` and an applied candidate takes the ratios.
 */
import assert from "node:assert/strict";
import { rangeFromRatio, rangeSplitFor, RANGE_SPLIT_VERSION } from "../src/lib/nfl-dfs/range-split";
import { armProjection } from "../src/lib/nfl-dfs/ghost/arm";
import { resolveDefensiveForecast } from "../src/lib/nfl-dfs/defensive-projection";

const ratio = { ratioP10: 0.4327, ratioP90: 2.0275, m17: 11.17, historyGames: 57, computedAt: "2026-10-26T21:00:00.000Z" };

// Pollard, week 7: projection 14.34, model range 7.67-20.07.
const pollard = { position: "RB", ourProj: 14.34, floorFpts: 7.67, medianFpts: 11.22, ceilingFpts: 20.07 };
const out = rangeSplitFor(pollard, ratio)!;
assert.equal(out.split.version, RANGE_SPLIT_VERSION);
assert.equal(out.p10, Math.round(14.34 * 0.4327 * 1000) / 1000);
assert.equal(out.p90, Math.round(14.34 * 2.0275 * 1000) / 1000);
assert.equal(out.median, 11.22, "median inside the new range is kept");
assert.deepEqual(out.split.before, { p10: 7.67, median: 11.22, p90: 20.07 });

// Positions, missing ratio, no projection.
assert.equal(rangeSplitFor({ ...pollard, position: "QB" }, ratio), null, "QB keeps the model's range");
assert.equal(rangeSplitFor(pollard, null), null);
assert.equal(rangeSplitFor({ ...pollard, ourProj: 0 }, ratio), null);
assert.equal(rangeFromRatio(10, 5, { ratioP10: 1.2, ratioP90: 0.8 }), null, "inverted ratios are refused");

// Median is clamped inside a range that moved past it.
assert.deepEqual(rangeFromRatio(10, 30, { ratioP10: 0.5, ratioP90: 2 }), { p10: 5, median: 20, p90: 20 });
assert.deepEqual(rangeFromRatio(10, 2, { ratioP10: 0.5, ratioP90: 2 }), { p10: 5, median: 5, p90: 20 });

// Ghost arms: the house player is unchanged; a reverted projection gets the ratios again.
const split = out.split;
const frozen = { position: "RB", ourProj: 14.34, floor: out.p10, median: out.median, ceiling: out.p90, rangeSplit: split,
  marketAnchor: { before: { mean: 8.99, p10: 4.81, median: 7.03, p90: 12.58 }, after: {} }, absenceCommit: null };
assert.equal(armProjection(frozen, "anchored", true), frozen, "house arm is the frozen row");
const model = armProjection(frozen, "model", true);
assert.equal(model.ourProj, 8.99);
assert.equal(model.floor, Math.round(8.99 * ratio.ratioP10 * 1000) / 1000, "model arm carries the ratios too");
assert.equal(model.ceiling, Math.round(8.99 * ratio.ratioP90 * 1000) / 1000);
const plain = { position: "WR", ourProj: 10, floor: 4, median: 9, ceiling: 18 };
assert.equal(armProjection(plain, "model", true), plain, "a row without the overlay is untouched");

// Defensive capture: baseline matches the pre-split range; the candidate takes the ratios.
const stats = { carries: 15 };
const baseline = { mean: 14.34, p10: 7.67, p50: 11.22, p90: 20.07, boom: 0.03, stat_means: stats };
const candidate = { mean: 15.0, p10: 8.0, p50: 12.0, p90: 21.0, boom: 0.04, stat_means: stats };
const capture = { runId: "c", baselineRunId: "run", capturedAt: "2026-10-25T13:00:00Z", artifactDigest: "d",
  candidate: { player_id: 256, dk_player_id: 1, game_id: "2026_07_CLE_TEN",
    shadow: { reproduction: { passed: true }, status: "under_evaluation", baseline, candidate, ledger: [] } } } as never;
const input = { dkPlayerId: 1, ffPlayerId: 256, gameKey: "CLE@TEN", isOut: false, ourProj: 14.34, floorFpts: out.p10,
  medianFpts: 11.22, ceilingFpts: out.p90, boomRate: 0.03, statMeans: stats, rangeSplit: split };
const settings = { mode: "experimental", profile: "gpp-integrated" } as never;
const applied = resolveDefensiveForecast(input, "run", settings, capture);
assert.equal(applied.status, "applied", `defensive stays on with the overlay (${applied.reason})`);
assert.ok(Math.abs(applied.selected.p10 - 15 * ratio.ratioP10) < 1e-9);
assert.ok(Math.abs(applied.selected.p90 - 15 * ratio.ratioP90) < 1e-9);
const without = resolveDefensiveForecast({ ...input, rangeSplit: null }, "run", settings, capture);
assert.equal(without.reason, "saved_baseline_distribution_mismatch", "without `before` the changed range is refused");

console.log("nfl range split: all assertions passed");
