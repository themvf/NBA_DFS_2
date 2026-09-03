import assert from "node:assert/strict";
import { optimizeNflLineups, type NflOptimizerPlayer, type NflOptimizerSettings } from "../src/app/dfs/nfl/nfl-optimizer";
import { parseNflComparisonCsv } from "../src/lib/nfl-dfs/comparison-csv";
import { exportNflDkEntries } from "../src/lib/nfl-dfs/entry-export";
import { averagePairwiseUnique, buildLineupInsight, lineupOverlap } from "../src/lib/nfl-dfs/lineup-insights";

let id = 100;
const player = (name: string, position: NflOptimizerPlayer["position"], team: string, opponent: string, salary: number, proj: number): NflOptimizerPlayer => ({
  id: ++id, dkPlayerId: id, captainDkPlayerId: id + 1000, name, position, team, opponent,
  gameKey: [team, opponent].sort().join("@"), salary, captainSalary: Math.round(salary * 1.5), isOut: false,
  projectionStatus: "historical", ourProj: proj, floorFpts: proj * .7, ceilingFpts: proj * 1.3,
  boomRate: .2, avgFptsDk: proj - 1, fantasyprosProj: proj + .2, linestarProj: proj - .2,
  linestarOwnPct: 10, customProj: null,
});
const pool: NflOptimizerPlayer[] = [];
for (const [game, teams] of [["A", ["BUF", "MIA"]], ["B", ["KC", "DEN"]], ["C", ["DAL", "NYG"]]] as const) {
  void game;
  for (const [team, opp] of [[teams[0], teams[1]], [teams[1], teams[0]]] as const) {
    pool.push(player(`${team} QB`, "QB", team, opp, 6200, 20));
    pool.push(player(`${team} RB1`, "RB", team, opp, 6000, 16), player(`${team} RB2`, "RB", team, opp, 4800, 11));
    pool.push(player(`${team} WR1`, "WR", team, opp, 6200, 17), player(`${team} WR2`, "WR", team, opp, 5000, 13), player(`${team} WR3`, "WR", team, opp, 4000, 9));
    pool.push(player(`${team} TE`, "TE", team, opp, 4200, 11), player(`${team} DST`, "DST", team, opp, 3200, 8));
  }
}
const settings: NflOptimizerSettings = { format: "classic", mode: "gpp", projectionSource: "our", allowDkFallback: false, nLineups: 3, minSalary: 45000, maxExposure: 1, minUnique: 2, stackPassCatchers: 1, bringBack: true, randomness: 0, lockedPlayerIds: [], excludedPlayerIds: [], minExposureByPlayer: {}, maxExposureByPlayer: {} };
const result = optimizeNflLineups(pool, settings);
assert.equal(result.lineups.length, 3);
for (const lineup of result.lineups) {
  assert.equal(lineup.slots.length, 9);
  assert.equal(new Set(lineup.playerIds).size, 9);
  assert.ok(lineup.totalSalary <= 50000 && lineup.totalSalary >= 45000);
  assert.ok(lineup.stackSummary.passCatchers.length >= 1);
  assert.ok(lineup.stackSummary.bringBack);
}
assert.ok(result.lineups[0].playerIds.filter((value) => result.lineups[1].playerIds.includes(value)).length <= 7);
assert.equal(lineupOverlap(result.lineups[0], result.lineups[0]), 9);
assert.ok(averagePairwiseUnique(result.lineups) >= 2);
const insight = buildLineupInsight(result.lineups[1], result.lineups, "gpp", "our");
assert.ok(insight.reasons.some((reason) => reason.tone === "ceiling"));
assert.ok(insight.reasons.some((reason) => reason.tone === "correlation"));
assert.equal(insight.sourceCounts.our, 9);
assert.ok(insight.gameCounts.length >= 2);

const targetId = pool.find((entry) => entry.name === "BUF QB")!.dkPlayerId;
const exactExposure = optimizeNflLineups(pool, {
  ...settings,
  nLineups: 5,
  stackPassCatchers: 0,
  bringBack: false,
  minExposureByPlayer: { [String(targetId)]: 0.4 },
  maxExposureByPlayer: { [String(targetId)]: 0.4 },
});
assert.equal(exactExposure.lineups.length, 5);
assert.equal(exactExposure.lineups.filter((lineup) => lineup.playerIds.includes(targetId)).length, 2);
const zeroExposure = optimizeNflLineups(pool, {
  ...settings,
  nLineups: 1,
  stackPassCatchers: 0,
  bringBack: false,
  minExposureByPlayer: { [String(targetId)]: 0 },
  maxExposureByPlayer: { [String(targetId)]: 0 },
});
assert.equal(zeroExposure.lineups[0].playerIds.includes(targetId), false);

const comparison = parseNflComparisonCsv("Player,Team,Proj,Own%\nJosh Allen,BUF,24.5,12.3%\nTyreek Hill,MIA,19.2,0.18\n");
assert.equal(comparison.rows[0].projection, 24.5);
assert.equal(comparison.rows[1].ownership, 18);

const entries = "Entry ID,Contest Name,Contest ID,Entry Fee,QB,RB,RB,WR,WR,WR,TE,FLEX,DST\n1,Test,2,$1,,,,,,,,,\n";
const exported = exportNflDkEntries(entries, [result.lineups[0]]);
assert.match(exported, /BUF QB \(101\)|MIA QB \(/);
assert.equal(exported.split(/\r?\n/)[1].split(",").filter(Boolean).length >= 10, true);
console.log("NFL DFS workspace tests passed");
