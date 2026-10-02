import assert from "node:assert/strict";
import { settleNflIdentityMarket } from "../src/lib/nfl/team-identity";
import type { NflIdentityGame } from "../src/db/nfl-team-identity";

function score(teamScore: number | null, opponentScore: number | null, spread: number | null, total: number | null) {
  return settleNflIdentityMarket({
    teamScore, opponentScore,
    market: { source: "captured", moneyline: -101, winProbability: 0.483,
      spread, total, impliedPoints: 19.25, observedAt: "2026-09-13T16:59:35Z", snapshotId: 1 },
  } satisfies Pick<NflIdentityGame, "teamScore" | "opponentScore" | "market">);
}

// The Jets' actual first three 2026 scores exercise cover, under, over, and exact-line pushes.
const week1 = score(23, 10, 1, 39.5);
assert.equal(week1.winner, "Won");
assert.equal(week1.spreadResult, "Covered");
assert.equal(week1.spreadEdge, 14);
assert.equal(week1.totalResult, "Under");
assert.equal(week1.totalEdge, -6.5);

const week2 = score(17, 20, 3, 44);
assert.equal(week2.winner, "Lost");
assert.equal(week2.spreadResult, "Push");
assert.equal(week2.spreadEdge, 0);
assert.equal(week2.totalResult, "Under");

const week3 = score(24, 31, 7, 49.5);
assert.equal(week3.spreadResult, "Push");
assert.equal(week3.totalResult, "Over");
assert.equal(week3.totalEdge, 5.5);

const missingQuote = score(10, 7, null, null);
assert.equal(missingQuote.winner, "Won");
assert.equal(missingQuote.spreadResult, null);
assert.equal(missingQuote.totalResult, null);

const pending = score(null, null, -3, 42);
assert.equal(pending.winner, null);
assert.equal(pending.spreadResult, null);
assert.equal(pending.totalResult, null);

console.log("NFL Team Identity market settlement checks passed");
