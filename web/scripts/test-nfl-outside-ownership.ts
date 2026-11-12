/**
 * Outside ownership (`nfl-ownership-outside-v1`): stat-api's projected
 * ownership replaces our model for the players it covers; a player it leaves
 * at 0% keeps our number when ours is real, and the rest scale down to make
 * room; a ruled-out player gets nothing; our own split stays on the player.
 */
import assert from "node:assert/strict";
import { applyOutsideOwnership, parseStatApiOwnership, slateOverlap, NFL_OWNERSHIP_STATAPI_SOURCE } from "../src/lib/nfl-dfs/outside-ownership";

// --- reading stat-api's two payloads -------------------------------------------
const projections = { projections: [
  { player: { player_id: 1, position: "CPT", operator_player_export_id: "9001" } },
  { player: { player_id: 1, position: "QB", operator_player_export_id: "101" } },
  { player: { player_id: 2, position: "CPT", operator_player_export_id: "9002" } },
  { player: { player_id: 2, position: "WR", operator_player_export_id: "102" } },
  { player: { player_id: 3, position: "RB", operator_player_export_id: "103" } },
] };
const ownership = { slate: { id: 77, name: "AAA @ BBB Showdown" }, model: "coupled_field", as_of: "2026-11-12T13:05:01Z",
  contests: [{ id: 5, name: "Big Showdown" }],
  contest: { id: 5, players: [
    { player_id: 1, captain_pct: 27, flex_pct: 65, total_pct: 92 },
    { player_id: 2, captain_pct: 14, flex_pct: 54, total_pct: 68 },
    { player_id: 3, captain_pct: 0, flex_pct: 0, total_pct: 0 },
    { player_id: 4, captain_pct: 1, flex_pct: 2, total_pct: 3 },
  ] } };
const parsed = parseStatApiOwnership(ownership, projections, "showdown");
assert.deepEqual(parsed.rows.map((r) => r.dkPlayerId), [101, 102, 103], "the flex row's DraftKings id is the key, never the captain row's");
assert.deepEqual(parsed.rows[0], { dkPlayerId: 101, captainPct: 27, flexPct: 65, totalPct: 92 });
assert.equal(parsed.meta.contestName, "Big Showdown");
assert.equal(parsed.meta.asOf, "2026-11-12T13:05:01Z");
assert.match(parsed.problems.join(" "), /1 stat-api player\(s\) had no DraftKings id/);
assert.equal(slateOverlap([101, 102, 103, 104], projections), 0.75);
assert.equal(slateOverlap([], projections), 0);

const classicParsed = parseStatApiOwnership({ contest: { id: 1, players: [{ player_id: 3, captain_pct: null, flex_pct: 40.7, total_pct: 40.7 }] } }, projections, "classic");
assert.deepEqual(classicParsed.rows, [{ dkPlayerId: 103, captainPct: null, flexPct: null, totalPct: 40.7 }]);
assert.match(parseStatApiOwnership({ contest: { players: [] } }, projections, "showdown").problems[0], /no ownership/);
assert.match(parseStatApiOwnership({ contest: { players: [{ player_id: 1, captain_pct: 140, flex_pct: 10, total_pct: 150 }] } }, projections, "showdown").problems[0], /outside 0-100%/);

// --- Showdown: replace, keep ours for a 0% player, scale the rest ---------------
const pool = [
  { dkPlayerId: 101, name: "Quarterback", isOut: false, ownPct: 73, captainOwnPct: 15, flexOwnPct: 58, ownSource: "nfl-ownership-fit-showdown-v1" },
  { dkPlayerId: 102, name: "Receiver", isOut: false, ownPct: 50, captainOwnPct: 10, flexOwnPct: 40, ownSource: "nfl-ownership-fit-showdown-v1" },
  { dkPlayerId: 103, name: "Back", isOut: false, ownPct: 30, captainOwnPct: 5, flexOwnPct: 25, ownSource: "nfl-ownership-fit-showdown-v1" },
  { dkPlayerId: 104, name: "Deep Bench", isOut: false, ownPct: 0.4, captainOwnPct: 0.1, flexOwnPct: 0.3, ownSource: "nfl-ownership-fit-showdown-v1" },
  { dkPlayerId: 105, name: "Injured", isOut: true, ownPct: null, captainOwnPct: null, flexOwnPct: null, ownSource: null },
  { dkPlayerId: 106, name: "Unlisted", isOut: false, ownPct: 0.5, captainOwnPct: 0.1, flexOwnPct: 0.4, ownSource: "nfl-ownership-fit-showdown-v1" },
];
const rows = [
  { dkPlayerId: 101, captainPct: 60, flexPct: 30, totalPct: 90 },
  { dkPlayerId: 102, captainPct: 40, flexPct: 70, totalPct: 110 - 0 }, // captain + flex over the cap: flex is held to 95 - captain
  { dkPlayerId: 103, captainPct: 0, flexPct: 0, totalPct: 0 },
  { dkPlayerId: 104, captainPct: 0, flexPct: 0, totalPct: 0 },
  { dkPlayerId: 105, captainPct: 2, flexPct: 8, totalPct: 10 },
];
const { players, report } = applyOutsideOwnership(pool, rows, "showdown");
const by = new Map(players.map((p) => [p.dkPlayerId, p]));
assert.equal(report.applied, 2);
assert.deepEqual(report.keptOurs, [{ name: "Back", ourPct: 30 }]);
assert.deepEqual(report.ruledOutWithOwnership, [{ name: "Injured", theirPct: 10 }]);
// Captain: stat-api had 100 on two players; the kept back holds 5, so theirs scale by 95/100.
assert.equal(by.get(101)!.captainOwnPct, 57);
assert.equal(by.get(102)!.captainOwnPct, 38);
// Flex: stat-api had 100; the kept back holds 25, so theirs scale by 75/100.
assert.equal(by.get(101)!.flexOwnPct, 22.5);
assert.equal(by.get(102)!.flexOwnPct, 52.5);
assert.equal(by.get(101)!.ownPct, 79.5);
assert.equal(by.get(101)!.ownSource, NFL_OWNERSHIP_STATAPI_SOURCE);
assert.deepEqual([by.get(101)!.modelCaptainOwnPct, by.get(101)!.modelFlexOwnPct], [15, 58], "our own split stays on the player");
// The back keeps our numbers and our source.
assert.deepEqual([by.get(103)!.ownPct, by.get(103)!.captainOwnPct, by.get(103)!.flexOwnPct, by.get(103)!.ownSource], [30, 5, 25, "nfl-ownership-fit-showdown-v1"]);
// stat-api's 0% stands when ours is under the keep-ours line.
assert.deepEqual([by.get(104)!.ownPct, by.get(104)!.captainOwnPct, by.get(104)!.flexOwnPct, by.get(104)!.ownSource], [0, 0, 0, NFL_OWNERSHIP_STATAPI_SOURCE]);
// A ruled-out player gets nothing, and a player stat-api never listed keeps ours.
assert.equal(by.get(105)!.ownPct, null);
assert.equal(by.get(106)!.ownPct, 0.5);
assert.equal(report.captainTotal, 57 + 38 + 5 + 0.1);
assert.equal(report.flexTotal, 22.5 + 52.5 + 25 + 0.4);

// One slot per lineup: captain plus flex never passes 95%.
const capped = applyOutsideOwnership([pool[0]], [{ dkPlayerId: 101, captainPct: 40, flexPct: 70, totalPct: 110 }], "showdown").players[0];
assert.deepEqual([capped.captainOwnPct, capped.flexOwnPct, capped.ownPct], [40, 55, 95]);

// Nothing kept: stat-api's numbers pass through unscaled.
const plain = applyOutsideOwnership(pool.slice(0, 2), [{ dkPlayerId: 101, captainPct: 27, flexPct: 64.8, totalPct: 91.8 }, { dkPlayerId: 102, captainPct: 14, flexPct: 54, totalPct: 68 }], "showdown");
assert.deepEqual(plain.players.map((p) => [p.captainOwnPct, p.flexOwnPct]), [[27, 64.8], [14, 54]]);
assert.deepEqual(plain.report.keptOurs, []);

// --- Classic: totals only -------------------------------------------------------
const classicPool: { dkPlayerId: number; name: string; isOut: boolean; ownPct: number; ownSource: string; captainOwnPct?: number | null }[] = [
  { dkPlayerId: 1, name: "Star", isOut: false, ownPct: 38, ownSource: "nfl-ownership-fit-classic-v1" },
  { dkPlayerId: 2, name: "Value", isOut: false, ownPct: 6, ownSource: "nfl-ownership-fit-classic-v1" },
  { dkPlayerId: 3, name: "Missing", isOut: false, ownPct: 10, ownSource: "nfl-ownership-fit-classic-v1" },
];
const classic = applyOutsideOwnership(classicPool, [{ dkPlayerId: 1, captainPct: null, flexPct: null, totalPct: 5 }, { dkPlayerId: 2, captainPct: null, flexPct: null, totalPct: 35 }], "classic");
// stat-api's 40 makes room for the kept player's 10: scale 30/40.
assert.deepEqual(classic.players.map((p) => [p.ownPct, p.captainOwnPct ?? null, p.ownSource]),
  [[3.8, null, "stat-api"], [26.3, null, "stat-api"], [10, null, "nfl-ownership-fit-classic-v1"]]);
assert.equal(classic.report.captainTotal, null);

console.log("nfl outside ownership: ok");
