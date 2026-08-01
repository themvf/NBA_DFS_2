import assert from "node:assert/strict";
import { buildSnakeSlots, nextControlledPick, picksUntilControlled } from "../src/lib/fantasy-football/draft-engine";
import { recommendPlayers } from "../src/lib/fantasy-football/recommendations";

const slots = buildSnakeSlots(4, 3);
assert.deepEqual(slots.map((slot) => slot.teamSlot), [1, 2, 3, 4, 4, 3, 2, 1, 1, 2, 3, 4]);
assert.equal(nextControlledPick(3, 2, 4, 3), 7);
assert.equal(picksUntilControlled(3, 2, 4, 3), 4);

const recommendations = recommendPlayers([
  { playerId: 1, position: "RB", ourRank: 12, ecr: 15, adp: 25, projectedPoints: 250, tier: 2, confidence: 0.8 },
  { playerId: 2, position: "QB", ourRank: 8, ecr: 8, adp: 8, projectedPoints: 330, tier: 1, confidence: 0.9 },
], ["QB"], 10);
assert.equal(recommendations[0].playerId, 1);
assert.equal(recommendations[0].adpDelta, 13);
console.log("fantasy-football tests passed");
