import assert from "node:assert/strict";
import { classifyNflPlayerSignals } from "../src/lib/nfl-dfs/player-signals";

const base = {
  games: 3, targets: 0, catches: 0, targetAirYards: 0, caughtAirYards: 0,
  deepTargets: 0, yardsAfterCatch: 0, expectedYac: 0, carries: 0,
  carriesInsideFive: 0, targetsInsideTen: 0,
};

assert.deepEqual(classifyNflPlayerSignals("WR", { ...base, targets: 30, catches: 15,
  targetAirYards: 416, caughtAirYards: 156, deepTargets: 8,
  yardsAfterCatch: 97, expectedYac: 67.8 }).map(x => x.code),
  ["AIR_VOLUME", "YAC_RUNWAY"]);
assert.deepEqual(classifyNflPlayerSignals("RB", { ...base, carries: 32,
  carriesInsideFive: 6 }).map(x => x.code), ["INSIDE_FIVE"]);
assert.deepEqual(classifyNflPlayerSignals("WR", { ...base, targetsInsideTen: 3 })
  .map(x => x.code), ["CLOSE_TARGET"]);
assert.deepEqual(classifyNflPlayerSignals("RB", { ...base, targetsInsideTen: 2 })
  .map(x => x.code), ["CLOSE_TARGET"]);
assert.deepEqual(classifyNflPlayerSignals("WR", { ...base, games: 1, targets: 30,
  targetAirYards: 416, deepTargets: 8 }), []);
assert.deepEqual(classifyNflPlayerSignals("WR", null), []);
console.log("NFL player opportunity signals: OK");
