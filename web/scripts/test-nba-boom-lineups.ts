/**
 * NBA boom lineups (`nba-boom-exposure-v1`): a ranked-10 rising-volume player
 * moves into more lineups, lineup 1 stays the frozen baseline trio, every lineup
 * respects the points limit, requests that cannot fit are refused, and the board
 * reads the same pool and boom score as nba-boom-trios-v1.
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { buildBoomLineups, readBoomBoard, type BoomBoard } from "../src/lib/nba-boom-lineups";
import { selectBoomTrios, type BoomSnapshot } from "../src/lib/nba-boom-trios";

const board: BoomBoard = { date: "2026-10-31", snapshot: "fixture", model: "fixture", frozenAt: "2026-10-31T12:00:00Z", lockAt: "2026-10-31T19:00:00Z", excluded: 0,
  players: Array.from({ length: 12 }, (_, i) => ({ id: `p${i + 1}`, name: `Player ${i + 1}`, points: 30 - i, pointRank: i + 1,
    boom: 12 - i, boomRank: 12 - i, shotsLast3: 20, shotsLast20: 15, availability: "unverified" })) };
const original = JSON.stringify(board);
const r = buildBoomLineups(board, "p10", 2, 10);
assert.equal(r.feasible, true); assert.equal(r.achieved, 2); assert.equal(r.baseline.filter((t) => t.ids.includes("p10")).length, 0);
assert.deepEqual(r.adjusted[0].ids, ["p1", "p2", "p3"]); assert.deepEqual(r.adjusted[0], r.baseline[0]);
assert.equal(new Set(r.adjusted.map((t) => t.key)).size, 5);
assert.ok(r.adjusted.every((t) => r.baseline[0].total - t.total <= 10)); assert.equal(JSON.stringify(board), original);
assert.equal(buildBoomLineups(board, "p10", 5, 20).feasible, false, "lineup 1 is fixed, so 5 of 5 is impossible");
assert.equal(buildBoomLineups(board, "p10", 2, 3).feasible, false, "no silent relaxation of the points limit");
assert.throws(() => buildBoomLineups(board, "missing", 2, 5));
assert.throws(() => buildBoomLineups({ ...board, players: board.players.map((p) => (p.id === "p4" ? { ...p, boomRank: null } : p)) }, "p4", 1, 5), /no frozen boom/);
assert.throws(() => buildBoomLineups(board, null, 2, NaN));
assert.equal(buildBoomLineups({ ...board, players: board.players.slice(0, 2) }, null, 0, 5).feasible, false);

// On a real frozen slate the board ranks the same pool as the boom trios, and lineup 1 is their baseline trio.
const fixture = JSON.parse(readFileSync(new URL("../../tests/fixtures/nba_boom_trios_fixture.json", import.meta.url), "utf-8")) as { snapshot: BoomSnapshot };
const snap = { ...fixture.snapshot, version: "nba-points-ghost-v2", lock_at: "2026-10-30T23:00:00+00:00" };
const real = readBoomBoard(snap, "2026-10-30")!;
assert.ok(real, "real snapshot reads");
const trios = selectBoomTrios(fixture.snapshot);
const lineup1 = buildBoomLineups(real, null, 0, 5).adjusted[0].ids.map((id) => id.split(":")[1]);
assert.deepEqual(lineup1, trios.baseline!.members.map((m) => m.player_key));
const boomPick = trios.boom!.members.find((m) => m.role === "boom")!.player_key;
assert.ok(real.players.find((p) => p.id.endsWith(`:${boomPick}`))?.boomRank != null, "the trio's boom pick is selectable here");
assert.equal(readBoomBoard({ ...snap, created_at: snap.lock_at }, "2026-10-30"), null, "frozen after lock is refused");
assert.equal(readBoomBoard({ ...snap, candidates: [snap.candidates[0], snap.candidates[0]] }, "2026-10-30"), null, "duplicate identity is refused");
console.log("NBA boom lineups: inclusion control, fixed first lineup, points limit, refusals, and the same pool as the boom trios.");
