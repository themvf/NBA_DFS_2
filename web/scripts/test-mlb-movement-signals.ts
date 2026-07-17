import assert from "node:assert/strict";
import { buildMlbMovementSignal } from "../src/lib/mlb-movement-signals";
import { classifyMlbMovementShape, findMovementStart } from "../src/lib/mlb-movement-shape";

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

const shapeBase = {
  openProbability: 0.5,
  currentProbability: 0.53,
  maxJumpPp: 0.8,
  confirmingBooks: 4,
  trackedBooks: 5,
  closeCapturedAt: "2026-07-17T20:30:00Z",
  nowIso: "2026-07-17T20:40:00Z",
  trail: [
    { capturedAt: "2026-07-17T18:00:00Z", homeProb: 0.5 },
    { capturedAt: "2026-07-17T18:30:00Z", homeProb: 0.506 },
    { capturedAt: "2026-07-17T20:30:00Z", homeProb: 0.53 },
  ],
};
assert.equal(classifyMlbMovementShape(shapeBase), "steady");
assert.equal(findMovementStart(shapeBase), "2026-07-17T18:30:00Z");
assert.equal(classifyMlbMovementShape({ ...shapeBase, maxJumpPp: 1.8 }), "steam");
assert.equal(classifyMlbMovementShape({ ...shapeBase, confirmingBooks: 1 }), "one_book");
assert.equal(classifyMlbMovementShape({
  ...shapeBase,
  trail: [
    { capturedAt: "2026-07-17T18:00:00Z", homeProb: 0.5 },
    { capturedAt: "2026-07-17T19:00:00Z", homeProb: 0.485 },
    { capturedAt: "2026-07-17T20:30:00Z", homeProb: 0.53 },
  ],
}), "reversal");
assert.equal(classifyMlbMovementShape({ ...shapeBase, currentProbability: 0.504 }), "quiet");
assert.equal(classifyMlbMovementShape({ ...shapeBase, nowIso: "2026-07-17T22:00:00Z" }), "stale");

console.log("MLB movement signal tests passed");
