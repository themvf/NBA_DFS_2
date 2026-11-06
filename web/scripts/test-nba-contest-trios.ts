import assert from "node:assert/strict";
import { sharePct, tallyContest } from "../src/lib/nba-contest-trios";

const side = (best: number, percentile: number, realizedShare: number, inContention = false) => ({ best, percentile, realizedShare, inContention });
const night = (o: ReturnType<typeof side>, b: ReturnType<typeof side>) => ({ byN: { "5": { ours: o, baseline: b } } });

const t = tallyContest([
  night(side(110, 0.99, 1, true), side(100, 0.95, 0)),
  night(side(90, 0.9, 0), side(105, 0.98, 0.25, true)),
  night(side(95, 0.93, 0), side(95, 0.93, 0)),
]);
assert.equal(t.nights, 3);
assert.equal(t.oursPrizes, 1);
assert.equal(t.baselinePrizes, 0.25);
assert.equal(t.oursAhead, 1);
assert.equal(t.baselineAhead, 1);           // a 0-0 night is neither
assert.ok(Math.abs((t.oursPercentile ?? 0) - (0.99 + 0.9 + 0.93) / 3) < 1e-12);
assert.equal(t.oursContention, 1);
assert.equal(t.baselineContention, 1);

const empty = tallyContest([]);
assert.equal(empty.nights, 0);
assert.equal(empty.oursPercentile, null);
assert.equal(tallyContest([{ byN: {} }]).nights, 0);   // a grade without the 5-entry cell is skipped

assert.equal(sharePct(0), "0%");
assert.equal(sharePct(0.0004), "<0.1%");
assert.equal(sharePct(0.0623), "6.2%");

console.log("test-nba-contest-trios: all assertions passed");
