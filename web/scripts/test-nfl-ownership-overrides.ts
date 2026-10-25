/**
 * Manual ownership (`nfl-ownership-manual-v1`): a typed number replaces the
 * prior for that player only, is labelled as typed, and on Showdown scales
 * the captain/flex split (which slotOwnershipPct reads per slot) to the typed
 * total within the 95% cap.
 */
import assert from "node:assert/strict";
import { applyOwnershipOverrides, sanitizeOwnershipOverrides, NFL_OWNERSHIP_MANUAL_SOURCE } from "../src/lib/nfl-dfs/ownership-overrides";

assert.deepEqual(sanitizeOwnershipOverrides({ "1": 25, "2": "12.34", "3": 150, "4": -1, "5": "", "6": "abc", x: 9, "7": null }),
  { "1": 25, "2": 12.3, "3": 100, "4": 0 });
assert.deepEqual(sanitizeOwnershipOverrides(null), {});

const classic = [
  { dkPlayerId: 1, ownPct: 3.1, ownSource: "nfl-ownership-prior-v5+nfl-ownership-bands-v1" },
  { dkPlayerId: 2, ownPct: 18, ownSource: "nfl-ownership-prior-v5" },
  { dkPlayerId: 3, ownPct: null, ownSource: null },
];
const c = applyOwnershipOverrides(classic, { "1": 25, "3": 4 }, "classic");
assert.equal(c.applied, 2);
assert.deepEqual(c.players[0], { dkPlayerId: 1, ownPct: 25, ownSource: NFL_OWNERSHIP_MANUAL_SOURCE }, "typed replaces the bounded prior");
assert.equal(c.players[1], classic[1], "an untyped player is the same object");
assert.equal(c.players[2].ownPct, 4, "a player with no prior can be given one");
assert.equal(applyOwnershipOverrides(classic, {}, "classic").players, classic, "no edits, no copy");

const showdown = [{ dkPlayerId: 9, ownPct: 40, captainOwnPct: 10, flexOwnPct: 30, ownSource: "nfl-ownership-prior-v5" }];
const s = applyOwnershipOverrides(showdown, { "9": 60 }, "showdown").players[0];
assert.equal(s.ownPct, 60);
assert.equal(s.captainOwnPct, 15, "captain share kept at a quarter of the total");
assert.equal(s.flexOwnPct, 45);
const capped = applyOwnershipOverrides(showdown, { "9": 100 }, "showdown").players[0];
assert.equal(capped.ownPct, 95, "one slot per lineup: never above 95%");
console.log("nfl ownership overrides: ok");
