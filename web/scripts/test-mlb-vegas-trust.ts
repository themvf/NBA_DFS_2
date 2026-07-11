import assert from "node:assert/strict";

import {
  describeMlbTotalEdge,
  isMlbGameLineActionable,
  MLB_GAME_LINES_TRUST,
} from "../src/lib/mlb-vegas-trust";

assert.equal(MLB_GAME_LINES_TRUST.state, "research");
assert.equal(isMlbGameLineActionable(), false);

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
