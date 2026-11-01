import assert from "node:assert/strict";
import { parseCbbSalaries } from "../src/lib/cbb-dfs/salary-csv";
import { cbbScore, importCbbEstimates, projectCbbHistory, type CbbEstimates } from "../src/lib/cbb-dfs/projection";
import { buildCbbLineups } from "../src/lib/cbb-dfs/optimizer";

const header = "Position,Name,ID,Roster Position,Salary,Game Info,TeamAbbrev,AvgPointsPerGame,Status";
function salaryCsv(showdown = false) {
  const rows: string[] = [];
  for (let i = 1; i <= 10; i++) {
    const game = showdown || i <= 5 ? "DUKE@UNC" : "UK@KU";
    const team = game.split("@")[i % 2];
    const position = i <= 4 ? "G" : i <= 8 ? "F" : "G/F";
    const salary = 4000 + i * 200;
    rows.push(`${position},Player ${i},${i},${showdown ? "UTIL" : `${position}/UTIL`},${salary},${game} 11/15/2026 07:00PM ET,${team},10,${i === 10 ? "OUT" : ""}`);
    if (showdown) rows.push(`${position},Player ${i},${i + 100},CPT,${salary * 1.5},${game} 11/15/2026 07:00PM ET,${team},15,`);
  }
  return `${header}\n${rows.join("\n")}`;
}
const classicCsv = salaryCsv();
const slate = parseCbbSalaries(classicCsv, "classic");
assert.equal(slate.firstTipoff, "2026-11-16T00:00:00.000Z");
assert.equal(parseCbbSalaries(classicCsv.replaceAll("11/15/2026", "07/15/2026"), "classic").firstTipoff, "2026-07-15T23:00:00.000Z");
assert.throws(() => parseCbbSalaries(classicCsv.replaceAll("11/15/2026", "02/31/2026"), "classic"), /Invalid tipoff/);
assert.throws(() => parseCbbSalaries(classicCsv.replace("G/UTIL", "PG/G/UTIL"), "classic"), /Wrong contest/);
assert.throws(() => parseCbbSalaries(classicCsv, "showdown"), /Wrong contest/);
const showdownCsv = salaryCsv(true);
const showdown = parseCbbSalaries(showdownCsv, "showdown");
assert.equal(showdown.players[0].captainId, "101");
assert.throws(() => parseCbbSalaries(showdownCsv.replace(",6300,", ",6200,"), "showdown"), /Captain/);

assert.deepEqual(cbbScore({ points: 20, threes: 2, rebounds: 10, assists: 10, steals: 1, blocks: 1, turnovers: 4 }), { base: 50.5, bonus: 4.5, total: 55 });
const estimates: CbbEstimates = {};
for (const p of slate.players) estimates[p.id] = { projection: 20 + Number(p.id), minutes: 30, rate: 1, bonus: 0, source: "test-only", asOf: "2026-11-01T12:00:00Z", method: "import", games: 0, ownership: null, captainOwnership: null, excluded: false };
const now = Date.parse("2026-11-01T12:00:00Z");
const lineups = buildCbbLineups(classicCsv, "classic", estimates, 2, now);
assert.equal(lineups.length, 2);
for (const l of lineups) {
  assert.equal(l.slots.length, 8); assert.equal(new Set(l.slots.map(p => p.player.id)).size, 8);
  assert.ok(l.salary <= 50000); assert.equal(new Set(l.slots.map(p => p.player.game)).size, 2);
  assert.ok(!l.slots.some(p => p.player.id === "10"), "out player excluded");
  assert.equal(l.slots.filter(s => s.slot === "G").length, 3);
  assert.equal(l.slots.filter(s => s.slot === "F").length, 3);
}
// Exhaustively enumerate this small fixture to verify the optimization objective independently.
let best = 0;
const eligible = slate.players.filter(p => p.status !== "OUT");
for (let mask = 0; mask < 2 ** eligible.length; mask++) {
  const chosen = eligible.filter((_, i) => mask & (1 << i));
  if (chosen.length !== 8 || chosen.reduce((a, p) => a + p.salary, 0) > 50000 || new Set(chosen.map(p => p.game)).size < 2) continue;
  if (chosen.filter(p => p.positions.includes("G")).length < 3 || chosen.filter(p => p.positions.includes("F")).length < 3) continue;
  best = Math.max(best, chosen.reduce((a, p) => a + estimates[p.id].projection!, 0));
}
assert.equal(lineups[0].projection, best);
const single = buildCbbLineups(showdownCsv, "showdown", estimates, 2, now);
assert.equal(single.length, 2);
let bestShowdown = 0;
for (let mask = 0; mask < 2 ** eligible.length; mask++) {
  const chosen = eligible.filter((_, i) => mask & (1 << i));
  if (chosen.length !== 6) continue;
  for (const captain of chosen) {
    if (chosen.reduce((a, p) => a + p.salary, 0) + captain.salary * .5 > 50000) continue;
    bestShowdown = Math.max(bestShowdown, chosen.reduce((a, p) => a + estimates[p.id].projection!, 0) + estimates[captain.id].projection! * .5);
  }
}
assert.equal(single[0].projection, bestShowdown);
for (const l of single) {
  const captain = l.slots[0]; assert.equal(captain.slot, "CPT");
  assert.equal(captain.id, String(Number(captain.player.id) + 100));
  assert.equal(captain.projection, estimates[captain.player.id].projection! * 1.5);
  assert.equal(captain.salary, captain.player.salary * 1.5);
  assert.equal(new Set(l.slots.map(p => p.player.team)).size, 2);
  assert.equal(new Set(l.slots.map(p => p.player.id)).size, 6);
}
assert.throws(() => buildCbbLineups(classicCsv, "classic", estimates, 1, Date.parse(slate.firstTipoff)), /started/);
assert.throws(() => buildCbbLineups(classicCsv, "classic", {}, 1, now), /No legal/);
assert.throws(() => buildCbbLineups(classicCsv, "classic", estimates, 21, now), /1 and 20/);
const projectionHeader = "ID,Projection,Minutes,Fppm,Bonus,Ownership,CaptainOwnership,Source,AsOf";
const imported = importCbbEstimates(`${projectionHeader}\n1,,30,1,1.5,0,,test,2026-11-01T12:00:00Z`, slate);
assert.equal(imported[1].projection, 31.5); assert.equal(imported[1].ownership, 0); assert.equal(imported[1].captainOwnership, null);
assert.throws(() => importCbbEstimates(`${projectionHeader}\n1,30,,,,,,,2026-11-01T12:00:00Z`, slate), /Source/);
assert.throws(() => importCbbEstimates(`${projectionHeader}\n1,30,,,,,,test,${slate.firstTipoff}`, slate), /before slate lock/);
assert.throws(() => importCbbEstimates(`${projectionHeader}\n101,30,,,,,,test,2026-11-01T12:00:00Z`, showdown), /Unmatched/);
assert.throws(() => importCbbEstimates(`${projectionHeader}\n1,30,,,,80,30,test,2026-11-01T12:00:00Z`, showdown), /cannot exceed 100/);
assert.throws(() => importCbbEstimates(`${projectionHeader}\n1,30,,,,,30,test,2026-11-01T12:00:00Z`, slate), /belongs to Showdown/);
assert.throws(() => importCbbEstimates(`${projectionHeader}\n1,30,,,,,,test,2026-11-04T12:00:00Z`, slate, now), /decision time/);
const historyHeader = "ID,Team,GameID,PlayedAt,CapturedAt,Minutes,PTS,3PM,REB,AST,STL,BLK,TO,Source";
const historyRow = "1,UNC,game-1,2026-11-03T00:00:00Z,2026-11-04T00:00:00Z,30,20,2,10,10,1,1,4,test";
const historyDecisionTime = Date.parse("2026-11-10T12:00:00Z");
const history = projectCbbHistory(`${historyHeader}\n${historyRow}`, slate, historyDecisionTime);
assert.equal(history[1].projection, 55); assert.equal(history[1].bonus, 4.5); assert.equal(history[1].games, 1);
assert.equal(history[1].ownership, null);
assert.throws(() => projectCbbHistory(`${historyHeader}\n${historyRow}\n${historyRow}`, slate, historyDecisionTime), /Repeated/);
assert.throws(() => projectCbbHistory(`${historyHeader}\n${historyRow.replace("1,UNC,", "1,DUKE,")}`, slate, historyDecisionTime), /Unresolved/);
assert.throws(() => projectCbbHistory(`${historyHeader}\n${historyRow.replace("2026-11-04", "2026-11-17")}`, slate, historyDecisionTime), /before slate lock/);
assert.throws(() => projectCbbHistory(`${historyHeader}\n${historyRow.replaceAll("2026-11-03", "2026-03-03").replaceAll("2026-11-04", "2026-03-04")}`, slate), /No played games/);
console.log("CBB DFS: scoring, source/time boundaries, identity matching, Classic and Showdown legality, exhaustive optimum, exclusions, and import semantics passed.");
