/**
 * Lineups-only DraftKings export: one roster per row under DraftKings' slot
 * headers with `Name (id)` cells (the captain by his CPT id), no entry file,
 * and the same legality checks as the entry-template export.
 */
import assert from "node:assert/strict";
import { parseCsvLine } from "../src/app/dfs/csv";
import { optimizeNflLineups, type NflOptimizerPlayer, type NflOptimizerSettings } from "../src/app/dfs/nfl/nfl-optimizer";
import { exportNflDkLineups, NFL_DK_LINEUP_HEADERS } from "../src/lib/nfl-dfs/entry-export";

function player(over: Partial<NflOptimizerPlayer> & { dkPlayerId: number; position: NflOptimizerPlayer["position"]; team: string }): NflOptimizerPlayer {
  const opponent = ({ AAA: "BBB", BBB: "AAA", CCC: "DDD", DDD: "CCC" } as Record<string, string>)[over.team];
  return {
    id: over.dkPlayerId, dkPlayerId: over.dkPlayerId, captainDkPlayerId: over.dkPlayerId + 100_000,
    name: over.name ?? `${over.position} ${over.dkPlayerId}`, position: over.position, team: over.team, opponent, gameKey: [over.team, opponent].sort().join("@"),
    salary: over.salary ?? 5000, captainSalary: Math.round((over.salary ?? 5000) * 1.5), isOut: false, projectionStatus: "historical",
    historyGames: 6, ourProj: over.ourProj ?? 10, floorFpts: 6, ceilingFpts: (over.ourProj ?? 10) * 1.8, boomRate: 0.2,
    avgFptsDk: 10, fantasyprosProj: null, linestarProj: null, linestarOwnPct: null, customProj: null,
  };
}
const settings = (over: Partial<NflOptimizerSettings>): NflOptimizerSettings => ({
  format: "classic", mode: "gpp", projectionSource: "our", allowDkFallback: true, nLineups: 2, minSalary: 0, maxExposure: 1, minUnique: 1,
  stackPassCatchers: 0, bringBack: false, randomness: 0, lockedPlayerIds: [], excludedPlayerIds: [], minExposureByPlayer: {}, maxExposureByPlayer: {}, ...over,
});

// Classic: four teams in two games, enough of every position.
const classic: NflOptimizerPlayer[] = [];
let id = 1;
for (const team of ["AAA", "BBB", "CCC", "DDD"]) {
  classic.push(player({ dkPlayerId: id++, position: "QB", team, ourProj: 18 }));
  for (let i = 0; i < 3; i++) classic.push(player({ dkPlayerId: id++, position: "RB", team, salary: 4500 + 600 * i, ourProj: 9 + 2 * i }));
  for (let i = 0; i < 4; i++) classic.push(player({ dkPlayerId: id++, position: "WR", team, salary: 4000 + 800 * i, ourProj: 8 + 3 * i }));
  for (let i = 0; i < 2; i++) classic.push(player({ dkPlayerId: id++, position: "TE", team, salary: 3000 + 900 * i, ourProj: 6 + 3 * i }));
  classic.push(player({ dkPlayerId: id++, position: "DST", team, salary: 2800, ourProj: 7 }));
}
const classicRun = optimizeNflLineups(classic, settings({}));
assert.equal(classicRun.lineups.length, 2);
const classicCsv = exportNflDkLineups("classic", classicRun.lineups);
const classicRows = classicCsv.trimEnd().split("\r\n").map(parseCsvLine);
assert.deepEqual(classicRows[0], [...NFL_DK_LINEUP_HEADERS.classic], "DraftKings' classic slot headers");
assert.equal(classicRows.length, 3, "a header and one row per lineup");
for (const [index, lineup] of classicRun.lineups.entries()) {
  const row = classicRows[index + 1];
  assert.equal(row.length, 9);
  lineup.slots.forEach((slot, column) => assert.equal(row[column], `${slot.player.name} (${slot.player.dkPlayerId})`, "name (dk id), in slot order"));
  assert.equal(row[8].startsWith("DST"), true, "the DST column is last");
}
assert.ok(classicCsv.endsWith("\r\n"), "CRLF line endings, as DraftKings writes them");

// Showdown: the captain is written with his CPT id, not his flex id.
const showdown: NflOptimizerPlayer[] = [];
id = 500;
const positions: NflOptimizerPlayer["position"][] = ["QB", "RB", "RB", "WR", "WR", "WR", "TE", "TE"];
for (const team of ["AAA", "BBB"]) for (let i = 0; i < positions.length; i++) showdown.push(player({ dkPlayerId: id++, position: positions[i], team, salary: 5000 + 400 * i, ourProj: 14 - i }));
const showdownRun = optimizeNflLineups(showdown, settings({ format: "showdown", nLineups: 3 }));
assert.equal(showdownRun.lineups.length, 3);
const showdownRows = exportNflDkLineups("showdown", showdownRun.lineups).trimEnd().split("\r\n").map(parseCsvLine);
assert.deepEqual(showdownRows[0], [...NFL_DK_LINEUP_HEADERS.showdown]);
for (const [index, lineup] of showdownRun.lineups.entries()) {
  const captain = lineup.slots.find(s => s.slot === "CPT")!;
  assert.equal(showdownRows[index + 1][0], `${captain.player.name} (${captain.player.captainDkPlayerId})`, "the captain cell carries the CPT id");
  assert.equal(showdownRows[index + 1].length, 6);
}

// The format must match the lineups, and an empty run is refused, not written as a header alone.
assert.throws(() => exportNflDkLineups("showdown", classicRun.lineups), /does not match/);
assert.throws(() => exportNflDkLineups("classic", []), /no lineups/);

console.log("Lineup export: DraftKings slot headers, name (id) cells, CPT ids on captains, no entry file needed.");
