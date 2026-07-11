import assert from "node:assert/strict";

import {
  describeMlbTotalEdge,
  evaluateMlbActionability,
  isMlbGameLineActionable,
  MLB_GAME_LINES_TRUST,
} from "../src/lib/mlb-vegas-trust";
import { canWebSurfaceWriteMlbOdds } from "../src/lib/mlb-odds-writer-policy";

assert.equal(MLB_GAME_LINES_TRUST.state, "research");
assert.equal(isMlbGameLineActionable(), false);
assert.equal(canWebSurfaceWriteMlbOdds("vegas_action"), false);
assert.equal(canWebSurfaceWriteMlbOdds("dfs_slate_fallback"), false);

const evidence = {
  market: "moneyline" as const,
  modelVersion: "test-v1",
  ledgerRows: 150,
  settledUniqueGames: 150,
  settledBets: 150,
  roi: 0.03,
  roiConfidenceLowerBound: 0.005,
  clvN: 150,
  avgClvPp: 1.2,
  exactPriceCoverage: 1,
  missingCommence: 0,
  postCommenceWrites: 0,
  invalidPrices: 0,
  duplicateActiveRecommendations: 0,
  prospectiveTrackingAvailable: true,
  immutableFeatureSnapshotsAvailable: true,
};

const actionable = evaluateMlbActionability(evidence);
assert.equal(actionable.state, "actionable");
assert.equal(isMlbGameLineActionable(actionable), true);

const research = evaluateMlbActionability({
  ...evidence,
  prospectiveTrackingAvailable: false,
  immutableFeatureSnapshotsAvailable: false,
});
assert.equal(research.state, "research");
assert.equal(isMlbGameLineActionable(research), false);

const watch = evaluateMlbActionability({
  ...evidence,
  settledUniqueGames: 20,
  roiConfidenceLowerBound: null,
});
assert.equal(watch.state, "watch");

const blocked = evaluateMlbActionability({
  ...evidence,
  postCommenceWrites: 1,
});
assert.equal(blocked.state, "blocked");

assert.deepEqual(describeMlbTotalEdge(1.24), {
  side: "Over",
  signed: "+1.2",
  magnitude: 1.24,
});
assert.deepEqual(describeMlbTotalEdge(-0.76), {
  side: "Under",
  signed: "-0.8",
  magnitude: 0.76,
});
assert.deepEqual(describeMlbTotalEdge(0), {
  side: "At market",
  signed: "0.0",
  magnitude: 0,
});
assert.equal(describeMlbTotalEdge(null), null);
assert.equal(describeMlbTotalEdge(Number.NaN), null);

console.log("MLB Vegas trust-policy tests passed");
