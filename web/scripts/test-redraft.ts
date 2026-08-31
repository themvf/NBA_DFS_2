import assert from "node:assert/strict";
import { buildDraftPool } from "../src/lib/fantasy-football/draft-pool";
import {
  REDRAFT_BENCH_SLOTS,
  REDRAFT_ROSTER_SIZE,
  REDRAFT_ROUNDS,
  REDRAFT_SLOTS,
  REDRAFT_STARTER_COUNT,
  REDRAFT_TEAM_COUNT,
  canAddRedraftPlayer,
  countFilledStarterSlots,
  getRedraftRosterStatus,
  parseRedraftState,
  type RedraftRosterPlayer,
} from "../src/lib/fantasy-football/redraft";

// Yahoo standard defaults, verified against Yahoo's own express-settings page:
// 10 teams, QB1/WR2/RB2/TE1/FLEX1/K1/DEF1 starters, 6 bench, 2 (undrafted) IR.
assert.equal(REDRAFT_TEAM_COUNT, 10);
assert.equal(REDRAFT_STARTER_COUNT, 9);
assert.equal(REDRAFT_BENCH_SLOTS, 6);
assert.equal(REDRAFT_ROSTER_SIZE, 15);
assert.equal(REDRAFT_ROUNDS, 15);
assert.equal(REDRAFT_SLOTS.length, 150);

// Snake order: round 1 runs 1..10, round 2 reverses to 10..1.
assert.equal(REDRAFT_SLOTS[0].teamSlot, 1);
assert.equal(REDRAFT_SLOTS[9].teamSlot, 10);
assert.equal(REDRAFT_SLOTS[10].teamSlot, 10);
assert.equal(REDRAFT_SLOTS[19].teamSlot, 1);

const player = (playerId: number, position: string): RedraftRosterPlayer => ({ playerId, position, team: "SF" });

// The flex must absorb exactly one spare RB/WR/TE -- no more.
assert.equal(countFilledStarterSlots({ QB: 1, RB: 2, WR: 2, TE: 1, K: 1, DST: 1 }), 8);   // no flex filled
assert.equal(countFilledStarterSlots({ QB: 1, RB: 3, WR: 2, TE: 1, K: 1, DST: 1 }), 9);   // spare RB -> flex
assert.equal(countFilledStarterSlots({ QB: 1, RB: 5, WR: 5, TE: 3, K: 1, DST: 1 }), 9);   // extras do not over-count
assert.equal(countFilledStarterSlots({ QB: 3, RB: 2, WR: 2, TE: 1, K: 1, DST: 1 }), 8);   // extra QB is NOT flex-eligible

// A legal lineup needs every dedicated slot plus one flex body.
const legal = [
  player(1, "QB"), player(2, "RB"), player(3, "RB"), player(4, "WR"),
  player(5, "WR"), player(6, "TE"), player(7, "K"), player(8, "DST"), player(9, "WR"),
];
assert.equal(getRedraftRosterStatus(legal).canFieldLegalLineup, true);
assert.equal(getRedraftRosterStatus(legal.slice(0, 8)).canFieldLegalLineup, false);

// Missing a kicker fails the lineup gate even with plenty of skill players.
const noKicker = [
  player(1, "QB"), player(2, "RB"), player(3, "RB"), player(4, "WR"),
  player(5, "WR"), player(6, "TE"), player(7, "WR"), player(8, "DST"),
];
const noKickerStatus = getRedraftRosterStatus(noKicker);
assert.equal(noKickerStatus.canFieldLegalLineup, false);
assert.equal(noKickerStatus.gates.find((gate) => gate.code === "MIN_K")?.pass, false);

// Yahoo does not block an unbalanced roster, so neither do we: gates are
// advisory. Only a full roster or a duplicate pick actually blocks an Add.
const allQbs = Array.from({ length: 5 }, (_, index) => player(index + 1, "QB"));
assert.equal(canAddRedraftPlayer(allQbs, player(99, "QB")), true);
assert.equal(getRedraftRosterStatus(allQbs).canFieldLegalLineup, false);
assert.equal(canAddRedraftPlayer(allQbs, player(1, "QB")), false, "duplicate pick must be blocked");
const full = Array.from({ length: REDRAFT_ROSTER_SIZE }, (_, index) => player(index + 1, "WR"));
assert.equal(canAddRedraftPlayer(full, player(99, "RB")), false, "full roster must be blocked");
// Positions outside the redraft universe are not draftable here.
assert.equal(canAddRedraftPlayer([], player(1, "P")), false);

// Corrupt/hostile localStorage must never crash the room.
assert.deepEqual(parseRedraftState("not json"), { userSlot: 1, playerIds: [], cpuEnabled: false });
assert.deepEqual(parseRedraftState(JSON.stringify({ userSlot: 99, playerIds: [3, 3, -1, "x"] })), { userSlot: 1, playerIds: [3], cpuEnabled: false });
assert.deepEqual(parseRedraftState(JSON.stringify({ userSlot: 7, playerIds: [5, 6] })), { userSlot: 7, playerIds: [5, 6], cpuEnabled: false });
// cpuEnabled round-trips, and non-boolean values fall back to off rather than crashing.
assert.deepEqual(parseRedraftState(JSON.stringify({ userSlot: 3, playerIds: [], cpuEnabled: true })), { userSlot: 3, playerIds: [], cpuEnabled: true });
assert.deepEqual(parseRedraftState(JSON.stringify({ userSlot: 3, playerIds: [], cpuEnabled: "yes" })), { userSlot: 3, playerIds: [], cpuEnabled: false });

console.log("redraft tests passed");

// --- draft pool -------------------------------------------------------------
// Regression: the rooms used to slice the board by OUR rank alone, which cut
// players the market drafts early but our model ranks low. Bhayshul Tuten went
// at ADP 52 -- round 6 of a 10-team draft -- while sitting at our #285, so he
// could not be picked in a mock of the draft he is certain to be in.
{
  const board = Array.from({ length: 300 }, (_, i) => ({
    playerId: i + 1,
    position: i % 2 === 0 ? "RB" : "WR",
    adp: null as number | null,
  }));
  // a market darling our board buries, exactly Tuten's shape
  board[284] = { playerId: 285, position: "RB", adp: 52.4 };
  // and one the market also ignores, who should stay cut
  board[290] = { playerId: 291, position: "RB", adp: 400 };
  const pool = buildDraftPool(board, ["QB", "RB", "WR", "TE"], 260, 150);
  const ids = new Set(pool.map((p) => p.playerId));

  assert.ok(ids.has(285), "a player drafted inside the format's picks must be reachable");
  assert.ok(!ids.has(291), "a player nobody drafts stays off the board");
  assert.equal(pool.length, 261, "exactly one player is added back");
  assert.ok(
    pool.findIndex((p) => p.playerId === 285) > 250,
    "the added player keeps his board position and is not promoted",
  );
  // ordering is otherwise untouched
  assert.deepEqual(pool.slice(0, 5).map((p) => p.playerId), [1, 2, 3, 4, 5]);

  // positions outside the format are excluded regardless of ADP
  const withK = buildDraftPool(
    [{ playerId: 999, position: "K", adp: 10 }, ...board],
    ["QB", "RB", "WR", "TE"], 260, 150,
  );
  assert.ok(!withK.some((p) => p.playerId === 999), "position filter still applies");
}

console.log("draft-pool tests passed");
