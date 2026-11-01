/** Shared chart helpers: tick steps and nearest-point hit testing. */
import assert from "node:assert/strict";
import { nearestPoint, niceTicks } from "../src/lib/charts";

assert.deepEqual(niceTicks(0, 43.7, 5), [0, 10, 20, 30, 40, 50]);
assert.deepEqual(niceTicks(0, 22.5, 5), [0, 5, 10, 15, 20, 25]);
assert.deepEqual(niceTicks(-0.1, 26.2, 5), [-10, 0, 10, 20, 30], "ticks start at the step at or below lo");
assert.deepEqual(niceTicks(3, 3, 5), [0, 1], "an empty span falls back instead of looping");

const points = [{ cx: 10, cy: 10 }, { cx: 30, cy: 10 }, { cx: 100, cy: 100 }];
assert.equal(nearestPoint(points, 12, 11, 18), 0);
assert.equal(nearestPoint(points, 24, 10, 18), 1, "the closer of two candidates wins");
assert.equal(nearestPoint(points, 60, 60, 18), null, "nothing inside the radius");
assert.equal(nearestPoint([], 0, 0, 18), null);

console.log("charts: all assertions passed");
