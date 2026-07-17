import assert from "node:assert/strict";
import { buildMlbMovementSignal } from "../src/lib/mlb-movement-signals";

const homeMove = buildMlbMovementSignal({
  openHomeProbability: 0.51,
  currentHomeProbability: 0.54,
  modelHomeProbability: 0.572,
  homeTeam: "CLE",
  awayTeam: "PIT",
});
assert.equal(homeMove.movementTeam, "CLE");
assert.ok(Math.abs(homeMove.movementPp - 3) < 1e-9);
assert.ok(Math.abs((homeMove.modelGapPp ?? 0) - 3.2) < 1e-9);
assert.equal(homeMove.agreement, "agree");
assert.equal(homeMove.combinedSignal, "strong_confirmation");

const awayMove = buildMlbMovementSignal({
  openHomeProbability: 0.58,
  currentHomeProbability: 0.54,
  modelHomeProbability: 0.57,
  homeTeam: "NYY",
  awayTeam: "LAD",
});
assert.equal(awayMove.movementTeam, "LAD");
assert.ok(Math.abs((awayMove.openProbability ?? 0) - 0.42) < 1e-9);
assert.ok(Math.abs((awayMove.currentProbability ?? 0) - 0.46) < 1e-9);
assert.equal(awayMove.agreement, "disagree");
assert.equal(awayMove.combinedSignal, "contrarian");

const quiet = buildMlbMovementSignal({
  openHomeProbability: 0.5,
  currentHomeProbability: 0.5049,
  modelHomeProbability: 0.53,
  homeTeam: "BOS",
  awayTeam: "TOR",
});
assert.equal(quiet.movementSide, null);
assert.equal(quiet.agreement, "unavailable");
assert.equal(quiet.combinedSignal, "quiet");

const neutral = buildMlbMovementSignal({
  openHomeProbability: 0.5,
  currentHomeProbability: 0.53,
  modelHomeProbability: 0.534,
  homeTeam: "SEA",
  awayTeam: "HOU",
});
assert.equal(neutral.agreement, "neutral");
assert.equal(neutral.combinedSignal, "market_only");

const implausible = buildMlbMovementSignal({
  openHomeProbability: 0.5,
  currentHomeProbability: 0.54,
  modelHomeProbability: 0.999,
  homeTeam: "CLE",
  awayTeam: "PIT",
});
assert.equal(implausible.modelProbability, null);
assert.equal(implausible.modelGapPp, null);
assert.equal(implausible.agreement, "unavailable");
assert.equal(implausible.combinedSignal, "market_only");
assert.equal(implausible.suppressionReason, "probability_out_of_range");
assert.equal(implausible.evaluatedModelProbability, 0.999);
assert.ok(Math.abs((implausible.evaluatedModelGapPp ?? 0) - 45.9) < 1e-9);

const implausibleGap = buildMlbMovementSignal({
  openHomeProbability: 0.49,
  currentHomeProbability: 0.5,
  modelHomeProbability: 0.8,
  homeTeam: "OAK",
  awayTeam: "WSH",
});
assert.equal(implausibleGap.modelProbability, null);
assert.equal(implausibleGap.modelGapPp, null);
assert.equal(implausibleGap.agreement, "unavailable");
assert.equal(implausibleGap.combinedSignal, "market_only");
assert.equal(implausibleGap.suppressionReason, "gap_exceeds_limit");
assert.equal(implausibleGap.evaluatedModelProbability, 0.8);
assert.ok(Math.abs((implausibleGap.evaluatedModelGapPp ?? 0) - 30) < 1e-9);

console.log("MLB movement signal tests passed");
