import assert from "node:assert/strict";
import { optimizeNflLineups, type NflOptimizerPlayer, type NflOptimizerSettings } from "../src/app/dfs/nfl/nfl-optimizer";
import { formFromSettings } from "../src/lib/nfl-dfs/generation-settings";
import { summarizeRunRisks } from "../src/lib/nfl-dfs/run-risk-summary";

const specs: Array<[number, string, NflOptimizerPlayer["position"], number, number, number, number]> = [
  [1, "Top QB", "QB", 22, 35, 5000, 10], [2, "Other QB", "QB", 20, 32, 5000, 10],
  [3, "Top RB", "RB", 20, 22, 5000, 10], [4, "Second RB", "RB", 18, 21, 5000, 10],
  [5, "Upside RB", "RB", 12, 28, 5000, 10], [6, "Other RB", "RB", 11, 25, 5000, 10],
  [7, "Leading WR", "WR", 25, 26, 9000, 60], [8, "Upside WR", "WR", 14, 32, 5000, 5],
  [9, "Second WR", "WR", 13, 30, 5000, 5], [10, "Third WR", "WR", 12, 28, 5000, 5],
  [11, "Other WR", "WR", 11, 27, 5000, 5],
  [12, "Top TE", "TE", 10, 20, 5000, 5], [13, "Other TE", "TE", 9, 18, 5000, 5],
  [14, "Top DST", "DST", 8, 15, 5000, 5], [15, "Other DST", "DST", 7, 14, 5000, 5],
];
const pool: NflOptimizerPlayer[] = specs.map(([id, name, position, mean, p90, salary, own]) => ({
  id, dkPlayerId: id, captainDkPlayerId: null, name, position,
  team: position === "DST" ? "CCC" : "AAA", opponent: position === "DST" ? "DDD" : "BBB",
  gameKey: position === "DST" ? "CCC@DDD" : "AAA@BBB", salary, captainSalary: null,
  isOut: false, projectionStatus: "historical", historyGames: 4,
  ourProj: mean, floorFpts: mean * .7, ceilingFpts: p90, boomRate: 0, avgFptsDk: mean,
  fantasyprosProj: null, linestarProj: null, linestarOwnPct: own, customProj: null,
}));
const settings: NflOptimizerSettings = {
  format: "classic", mode: "gpp", projectionSource: "our", allowDkFallback: false,
  nLineups: 6, minSalary: 0, maxExposure: 1, minUnique: 1, stackPassCatchers: 0,
  bringBack: false, randomness: 0, ownershipLeverageEnabled: true,
  lockedPlayerIds: [], excludedPlayerIds: [], minExposureByPlayer: {}, maxExposureByPlayer: {},
};
assert.throws(() => optimizeNflLineups(pool.map(player => ({ ...player,
  fantasyprosProj: player.dkPlayerId === 7 ? player.ourProj : null })),
  { ...settings, projectionSource: "fantasypros", allowDkFallback: true }),
  /mixed objective sources/, "a direct source cannot compete with a different fallback scale");
assert.ok(optimizeNflLineups(pool.map(player => ({ ...player, fantasyprosProj: player.ourProj })),
  { ...settings, projectionSource: "fantasypros", allowDkFallback: false, nLineups: 1 }).lineups.length,
"a single selected source can still build");

const without = optimizeNflLineups(pool, settings);
assert.equal(without.lineups.length, 6);
assert.ok(without.lineups.every(lineup => !lineup.playerIds.includes(7)),
  "the expensive leading WR can disappear under ceiling, salary, and ownership scoring");

const withCoverage = optimizeNflLineups(pool, { ...settings, topProjectedCoverage: true });
assert.equal(withCoverage.lineups.length, 6);
for (const id of [1, 3, 4, 7, 8, 12]) {
  assert.ok(withCoverage.lineups.some(lineup => lineup.playerIds.includes(id)),
    `top projected player ${id} appears without a manual min/max`);
}
assert.ok(withCoverage.warnings.some(warning => warning.includes("Top projected coverage:") && warning.includes("Leading WR")));
assert.equal(withCoverage.lineups.flatMap(lineup => lineup.slots).find(slot => slot.player.dkPlayerId === 7)?.projection, 25,
  "coverage changes selection, not the player's forecast");

const excluded = optimizeNflLineups(pool, { ...settings, topProjectedCoverage: true, excludedPlayerIds: [7] });
assert.ok(excluded.lineups.every(lineup => !lineup.playerIds.includes(7)), "manual exclusion wins");
assert.ok(!excluded.warnings.some(warning => warning.includes("Top projected coverage:") && warning.includes("Leading WR")));
const capped = optimizeNflLineups(pool, { ...settings, topProjectedCoverage: true, maxExposureByPlayer: { "7": 0 } });
assert.ok(capped.lineups.every(lineup => !lineup.playerIds.includes(7)), "a manual zero maximum wins");
const impossible = optimizeNflLineups(pool.map(player => player.dkPlayerId === 7
  ? { ...player, salary: 50_000 } : player), { ...settings, topProjectedCoverage: true });
assert.ok(impossible.warnings.some(warning => warning.includes("skipped Leading WR")),
  "an unaffordable leader is disclosed rather than silently promised");
assert.ok(impossible.lineups.some(lineup => lineup.playerIds.includes(9)),
  "the next feasible WR receives coverage");

assert.equal(formFromSettings(settings, { topProjectedCoverage: true }).settings.topProjectedCoverage, false,
  "restoring a legacy run does not claim it used the new default");

const savedLineup = withCoverage.lineups[0];
const reviewedLineup = { ...savedLineup, slots: savedLineup.slots.map(slot => ({
  ...slot,
  projectionSource: slot.player.position === "WR" ? "workload" as const : slot.projectionSource,
  player: slot.player.position === "DST" ? { ...slot.player, opponent: "AAA" } : slot.player,
})) };
const riskSummary = summarizeRunRisks([reviewedLineup]);
assert.deepEqual(riskSummary.sourceFamilies, ["historical", "workload"]);
assert.deepEqual(riskSummary.dstOpponentLineups, [savedLineup.lineupNumber]);

console.log("NFL top projected portfolio coverage: OK");
