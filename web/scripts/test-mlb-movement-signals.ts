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

const quiet = buildMlbMovementSignal({
  openHomeProbability: 0.5,
  currentHomeProbability: 0.5049,
  modelHomeProbability: 0.53,
  homeTeam: "BOS",
  awayTeam: "TOR",
});
assert.equal(quiet.movementSide, null);
assert.equal(quiet.agreement, "unavailable");

const neutral = buildMlbMovementSignal({
  openHomeProbability: 0.5,
  currentHomeProbability: 0.53,
  modelHomeProbability: 0.534,
  homeTeam: "SEA",
  awayTeam: "HOU",
});
assert.equal(neutral.agreement, "neutral");

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

console.log("MLB movement signal tests passed");
