import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

import {
  americanToDecimal,
  buildMlbDecisionBoard,
  buildMoneylineDecisions,
  buildTotalDecisions,
  isValidAmericanPrice,
  minimumAmericanPrice,
  removeTwoWayVig,
  type MlbDecisionMatchup,
} from "../src/lib/mlb-vegas-decisions";
import type { MlbActionabilityDecision } from "../src/lib/mlb-vegas-trust";

const evaluatedAt = "2026-07-11T20:01:00.000Z";

function trust(market: "moneyline" | "total", state: MlbActionabilityDecision["state"] = "actionable"): MlbActionabilityDecision {
  return {
    market,
    policyVersion: "mlb-actionability-v1",
    modelVersion: market === "moneyline" ? "mlb-ml-v2" : "mlb-total-v2",
    canonicalHorizon: "t-180",
    trustEvaluationId: `trust:${market}:v2:t-180`,
    state,
    passed: state === "actionable" ? 10 : 7,
    total: 10,
    gates: [],
    summary: state === "actionable" ? "All gates pass." : "Three gates remain.",
  };
}

const base: MlbDecisionMatchup = {
  matchupId: 42,
  gameDate: "2026-07-11",
  gameId: "777",
  gameStatus: "Scheduled",
  doubleheaderGameNumber: null,
  awayAbbrev: "NYY",
  homeAbbrev: "BOS",
  awaySpName: "Away Starter",
  homeSpName: "Home Starter",
  ballpark: "Fenway Park",
  commenceTime: "2026-07-11T23:10:00.000Z",
  oddsSnapshotId: 7,
  oddsCaptureKey: "capture-7",
  oddsCapturedAt: "2026-07-11T20:00:00.000Z",
  oddsBooks: {
    draftkings: {
      ml_home: -130,
      ml_away: 110,
      total_line: 8.5,
      over: -105,
      under: -115,
      last_update: "2026-07-11T19:59:00.000Z",
    },
  },
  openingOddsBooks: {
    draftkings: {
      ml_home: -125,
      ml_away: 105,
      total_line: 8.0,
      over: -110,
      under: -110,
    },
  },
  moneylinePredictionSnapshotId: 9,
  moneylineReferenceOddsSnapshotId: 7,
  moneylinePredictionEventCommence: "2026-07-11T23:10:00.000Z",
  moneylineFeatureAvailableAt: "2026-07-11T19:58:00.000Z",
  moneylineReferenceMarketProbability: removeTwoWayVig(-130, 110),
  moneylinePrediction: 0.61,
  moneylineCalibratedProbability: 0.61,
  moneylinePredictionAt: "2026-07-11T20:00:30.000Z",
  moneylineModelVersion: "mlb-ml-v2",
  moneylineRunConfig: {
    missingness_policy: "source-aware-v1",
    calibration_method: "isotonic-oof-v1",
    canonical_horizon: "t-180",
  },
  moneylineFeatureValues: {
    probability_resamples: Array.from({ length: 10 }, (_, index) => 0.59 + index * 0.004),
    contributions: { sp_xfip_adv: 0.2, bullpen_adv: -0.05 },
  },
  moneylineMissingness: {},
  totalPredictionSnapshotId: 10,
  totalReferenceOddsSnapshotId: 7,
  totalPredictionEventCommence: "2026-07-11T23:10:00.000Z",
  totalFeatureAvailableAt: "2026-07-11T19:58:00.000Z",
  totalReferenceMarketLine: 8.5,
  totalPrediction: 9.2,
  totalPredictionAt: "2026-07-11T20:00:30.000Z",
  totalModelVersion: "mlb-total-v2",
  totalRunConfig: {
    missingness_policy: "source-aware-v1",
    distribution_method: "oof-residual-v1",
    canonical_horizon: "t-180",
  },
  totalFeatureValues: {
    total_distribution: {
      line: 8.5,
      p_over: 0.56,
      p_push: 0,
      p_under: 0.44,
      resamples: Array.from({ length: 10 }, () => [0.56, 0, 0.44]),
    },
    contributions: { wind_component: 0.3, park_runs_factor: 0.1 },
  },
  totalMissingness: {},
};

assert.equal(americanToDecimal(+100), 2);
assert.equal(americanToDecimal(-100), 2);
for (const invalid of [-99, 0, 99, -110.5, Number.NaN, Number.POSITIVE_INFINITY]) {
  assert.equal(isValidAmericanPrice(invalid), false, `${invalid} must be rejected`);
}
assert.equal(minimumAmericanPrice(1.02 / 0.58), -131, "displayed integer must actually clear +2% ROI");

const marketHome = removeTwoWayVig(-130, 110);
assert.ok(marketHome > 0.54 && marketHome < 0.55, "vig removal should normalize both sides");

const actionableOptions = {
  evaluatedAt,
  trustDecisions: [trust("moneyline"), trust("total")],
};
const moneyline = buildMoneylineDecisions(base, actionableOptions)[0];
assert.equal(moneyline.primaryStatus, "take_now");
assert.equal(moneyline.selection, "BOS");
assert.equal(moneyline.bookLabel, "DraftKings");
assert.equal(moneyline.price, -130);
assert.equal(moneyline.relationship, "agree_model_stronger");
assert.equal(moneyline.blockers.length, 0);

const bothSides = buildMoneylineDecisions({
  ...base,
  oddsBooks: {
    draftkings: {
      ml_home: -140,
      ml_away: 130,
      last_update: "2026-07-11T19:59:00.000Z",
    },
  },
  moneylineReferenceMarketProbability: removeTwoWayVig(-140, 130),
  moneylinePrediction: 0.55,
  moneylineCalibratedProbability: 0.55,
  moneylineFeatureValues: { probability_resamples: Array.from({ length: 10 }, () => 0.55) },
}, actionableOptions)[0];
assert.equal(bothSides.selection, "NYY", "the engine must evaluate both prices, not merely the model favorite");
assert.equal(bothSides.price, 130);
assert.ok((bothSides.estimatedRoi ?? 0) > 0.034 && (bothSides.estimatedRoi ?? 0) < 0.036);

const rawOnly = buildMoneylineDecisions({
  ...base,
  moneylineRunConfig: { missingness_policy: "source-aware-v1", canonical_horizon: "t-180" },
}, actionableOptions)[0];
assert.equal(rawOnly.primaryStatus, "blocked");
assert.ok(rawOnly.blockers.some((reason) => reason.includes("calibrated")));

const noResamples = buildMoneylineDecisions({
  ...base,
  moneylineFeatureValues: { contributions: {} },
}, actionableOptions)[0];
assert.equal(noResamples.primaryStatus, "blocked");
assert.ok(noResamples.blockers.some((reason) => reason.includes("resamples")));

const watchTrust = buildMoneylineDecisions(base, {
  evaluatedAt,
  trustDecisions: [trust("moneyline", "watch")],
})[0];
assert.equal(watchTrust.primaryStatus, "watch");

const stale = buildMoneylineDecisions({
  ...base,
  oddsCapturedAt: "2026-07-11T19:50:59.999Z",
  oddsBooks: {
    draftkings: { ml_home: -130, ml_away: 110, last_update: "2026-07-11T19:50:59.999Z" },
  },
}, actionableOptions)[0];
assert.equal(stale.primaryStatus, "blocked");
assert.ok(stale.blockers.some((reason) => reason.includes("policy maximum")));

const exactlyFresh = buildMoneylineDecisions({
  ...base,
  oddsCapturedAt: "2026-07-11T19:51:00.000Z",
  oddsBooks: {
    draftkings: { ml_home: -130, ml_away: 110, last_update: "2026-07-11T19:51:00.000Z" },
  },
}, actionableOptions)[0];
assert.notEqual(exactlyFresh.primaryStatus, "blocked", "exactly ten minutes old is allowed");

const priceChanged = buildMoneylineDecisions({ ...base, oddsSnapshotId: 8 }, actionableOptions)[0];
assert.equal(priceChanged.primaryStatus, "watch");
assert.match(priceChanged.primaryReason, /newer than the market input/i);

const closed = buildMoneylineDecisions({
  ...base,
  gameStatus: "Final",
}, actionableOptions)[0];
assert.equal(closed.primaryStatus, "closed");

const total = buildTotalDecisions(base, actionableOptions)[0];
assert.equal(total.primaryStatus, "take_now");
assert.equal(total.selection, "Over 8.5");
assert.equal(total.relationship, "model_above_line");

const pushTotal = buildTotalDecisions({
  ...base,
  oddsBooks: {
    draftkings: {
      total_line: 8,
      over: -110,
      under: -110,
      last_update: "2026-07-11T19:59:00.000Z",
    },
  },
  totalReferenceMarketLine: 8,
  totalPrediction: 8.2,
  totalFeatureValues: {
    total_distribution: {
      line: 8,
      p_over: 0.48,
      p_push: 0.10,
      p_under: 0.42,
      resamples: Array.from({ length: 10 }, () => [0.48, 0.10, 0.42]),
    },
  },
}, actionableOptions)[0];
assert.ok((pushTotal.estimatedRoi ?? 0) > 0.016 && (pushTotal.estimatedRoi ?? 0) < 0.017);
assert.equal(pushTotal.primaryStatus, "watch", "+1.64% does not clear the +2% rule");

const missingDistribution = buildTotalDecisions({
  ...base,
  totalFeatureValues: { contributions: {} },
}, actionableOptions)[0];
assert.equal(missingDistribution.primaryStatus, "blocked");
assert.ok(missingDistribution.blockers.some((reason) => reason.includes("distribution")));

const board = buildMlbDecisionBoard([base], actionableOptions);
assert.equal(board.length, 2, "one exact-book moneyline and total decision should be returned");

const serverSource = readFileSync(new URL("../src/app/vegas/mlb-vegas-content.tsx", import.meta.url), "utf8");
const clientSource = readFileSync(new URL("../src/app/vegas/mlb-vegas-client.tsx", import.meta.url), "utf8");
const querySource = readFileSync(new URL("../src/db/queries.ts", import.meta.url), "utf8");
assert.doesNotMatch(serverSource, /buildMlbDecisionBoard/, "the simplified MLB movement board must not be gated by the legacy decision service");
assert.match(serverSource, /getMlbLineMovement\(7\)/);
assert.match(serverSource, /matchups=\{matchups\}/);
assert.doesNotMatch(clientSource, /buildMlbDecisionBoard/);
assert.doesNotMatch(clientSource, /MLB_MIN_MEAN_ROI|MLB_MIN_POSITIVE_RESAMPLE_RATE/);
assert.match(querySource, /latest_odds\.id AS "oddsSnapshotId"/);
assert.match(querySource, /ml_pred\.odds_snapshot_id AS "moneylineReferenceOddsSnapshotId"/);
assert.match(querySource, /total_pred\.odds_snapshot_id AS "totalReferenceOddsSnapshotId"/);

console.log("MLB Vegas v3 decision-contract tests passed");
