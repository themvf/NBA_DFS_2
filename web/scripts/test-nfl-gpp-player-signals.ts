import assert from "node:assert/strict";
import { optimizeNflLineups, type NflOptimizerPlayer, type NflOptimizerSettings } from "../src/app/dfs/nfl/nfl-optimizer";
import { airMatchupSignals, type NflAirDefenseEvidence } from "../src/lib/nfl-dfs/player-signals";

const spec: Array<[number, string, NflOptimizerPlayer["position"], number]> = [
  [1, "QB", "QB", 10], [2, "RB one", "RB", 9], [3, "RB two", "RB", 8],
  [4, "RB three", "RB", 7], [5, "WR one", "WR", 9], [6, "WR two", "WR", 8],
  [7, "WR three", "WR", 7], [8, "WR four", "WR", 6],
  [9, "WR with air-yard signal", "WR", 2], [10, "TE", "TE", 6], [11, "DST", "DST", 5],
];
const pool: NflOptimizerPlayer[] = spec.map(([id, name, position, projection]) => ({
  id, dkPlayerId: id, captainDkPlayerId: null, name, position, team: "AAA",
  opponent: "BBB", gameKey: null, salary: 5000, captainSalary: null,
  isOut: false, projectionStatus: "historical", historyGames: 3,
  ourProj: projection, floorFpts: projection * 0.7, ceilingFpts: projection * 1.4,
  boomRate: 0, avgFptsDk: projection, fantasyprosProj: null, linestarProj: null,
  linestarOwnPct: null, customProj: null,
  playerSignals: id === 9 ? [{ code: "AIR_VOLUME", label: "Air-yard volume",
    detail: "Observed deep targets.", evidence: { targetAirYards: 250 } }] : [],
}));
const settings: NflOptimizerSettings = {
  format: "classic", mode: "gpp", projectionSource: "our", allowDkFallback: false,
  nLineups: 1, minSalary: 0, maxExposure: 1, minUnique: 1,
  stackPassCatchers: 0, bringBack: false, randomness: 0,
  lockedPlayerIds: [], excludedPlayerIds: [], minExposureByPlayer: {}, maxExposureByPlayer: {},
};
const baseline = optimizeNflLineups(pool, settings).lineups[0];
assert.ok(baseline && !baseline.playerIds.includes(9));
const signaled = optimizeNflLineups(pool, { ...settings, gppSignalMinPerLineup: 1 }).lineups[0];
assert.ok(signaled?.playerIds.includes(9));
assert.equal(signaled.slots.find(slot => slot.player.dkPlayerId === 9)?.projection, 2,
  "the signal changes construction, not player fantasy points");
const defenses = new Map<string, NflAirDefenseEvidence>(Array.from({ length: 32 }, (_, index) => [
  `T${index}`, { games: 3, targets: 90, targetAirYards: 400 + index * 20 },
]));
const matchup = airMatchupSignals("WR", pool[8].playerSignals ?? [], "T31", defenses);
assert.equal(matchup[0]?.code, "AIR_MATCHUP");
assert.equal(airMatchupSignals("WR", pool[8].playerSignals ?? [], "T0", defenses).length, 0);
assert.equal(airMatchupSignals("WR", [], "T31", defenses).length, 0);
const matchupPool = pool.map(player => player.dkPlayerId === 9 ? { ...player, playerSignals: matchup } : player);
const matchupLineup = optimizeNflLineups(matchupPool, { ...settings, gppSignalMinPerLineup: 1, gppSignalCodes: ["AIR_MATCHUP"] }).lineups[0];
assert.ok(matchupLineup?.playerIds.includes(9));
assert.equal(matchupLineup.slots.find(slot => slot.player.dkPlayerId === 9)?.projection, 2);
const percentRun = optimizeNflLineups(matchupPool, { ...settings, nLineups: 4, gppAirMatchupMinPct: 25 });
assert.equal(percentRun.lineups.length, 4);
assert.ok(percentRun.lineups.filter(lineup => lineup.playerIds.includes(9)).length >= 1);
assert.ok(percentRun.warnings.some(warning => warning.includes("minimum 1/4 requested")));
const fullMatchupRun = optimizeNflLineups(matchupPool, { ...settings, nLineups: 2, gppAirMatchupMinPct: 100 });
assert.ok(fullMatchupRun.lineups.every(lineup => lineup.playerIds.includes(9)));
assert.throws(() => optimizeNflLineups(matchupPool, { ...settings, nLineups: 2, maxExposure: 0.5, gppAirMatchupMinPct: 100 }), /Could not meet the air-yard matchup minimum/);
assert.throws(() => optimizeNflLineups(pool, { ...settings, gppAirMatchupMinPct: 25 }), /No eligible player has an air-yard matchup tag/);
assert.throws(() => optimizeNflLineups(matchupPool, { ...settings, mode: "cash", gppAirMatchupMinPct: 25 }), /Classic GPP/);
assert.throws(() => optimizeNflLineups(matchupPool, { ...settings, gppAirMatchupMinPct: 101 }), /between 0 and 100/);
const goalLinePool = matchupPool.map(player => player.dkPlayerId === 4 ? { ...player, ourProj: 1, floorFpts: .7, ceilingFpts: 1.4, avgFptsDk: 1,
  playerSignals: [{ code: "INSIDE_FIVE" as const, label: "Inside-5 work", detail: "Three prior carries inside five.", evidence: { games: 3, carries: 25, carriesInsideFive: 3 } }] } : player);
assert.ok(!optimizeNflLineups(goalLinePool, settings).lineups[0]?.playerIds.includes(4));
const goalLineRun = optimizeNflLineups(goalLinePool, { ...settings, nLineups: 4, gppGoalLineMinPct: 25 });
assert.equal(goalLineRun.lineups.length, 4);
assert.ok(goalLineRun.lineups.filter(lineup => lineup.playerIds.includes(4)).length >= 1);
assert.ok(goalLineRun.warnings.some(warning => warning.includes("Goal-line RB coverage") && warning.includes("minimum 1/4 requested")));
const fullGoalLineRun = optimizeNflLineups(goalLinePool, { ...settings, nLineups: 2, gppGoalLineMinPct: 100 });
assert.ok(fullGoalLineRun.lineups.every(lineup => lineup.playerIds.includes(4)));
const bothRun = optimizeNflLineups(goalLinePool, { ...settings, gppAirMatchupMinPct: 100, gppGoalLineMinPct: 100 });
assert.ok(bothRun.lineups[0]?.playerIds.includes(4) && bothRun.lineups[0]?.playerIds.includes(9));
assert.equal(bothRun.lineups[0]?.slots.find(slot => slot.player.dkPlayerId === 4)?.projection, 1,
  "goal-line selection must not increase projected points");
assert.throws(() => optimizeNflLineups(goalLinePool, { ...settings, nLineups: 2, maxExposure: 0.5, gppGoalLineMinPct: 100 }), /Could not meet the goal-line RB minimum/);
assert.throws(() => optimizeNflLineups(pool, { ...settings, gppGoalLineMinPct: 25 }), /No eligible RB has an inside-5 work tag/);
assert.throws(() => optimizeNflLineups(goalLinePool, { ...settings, mode: "cash", gppGoalLineMinPct: 25 }), /Classic GPP/);
assert.throws(() => optimizeNflLineups(goalLinePool, { ...settings, gppGoalLineMinPct: 101 }), /between 0 and 100/);
const limited = optimizeNflLineups(pool, { ...settings, nLineups: 2, maxExposure: 0.5,
  gppSignalMinPerLineup: 1 });
assert.ok(limited.lineups.length < 2);
assert.ok(limited.lineups.every(lineup => lineup.playerIds.includes(9)),
  "the rule cannot silently disappear after the tagged player's exposure is spent");
assert.throws(() => optimizeNflLineups(pool,
  { ...settings, gppSignalMinPerLineup: 1, gppSignalCodes: ["INSIDE_FIVE"] }),
  /No eligible player has a selected opportunity signal/);
assert.throws(() => optimizeNflLineups(pool.map(player => ({ ...player, playerSignals: [] })),
  { ...settings, gppSignalMinPerLineup: 1 }), /No eligible player has a selected opportunity signal/);
assert.throws(() => optimizeNflLineups(pool, { ...settings, mode: "cash", gppSignalMinPerLineup: 1 }),
  /Classic GPP/);
console.log("NFL GPP opportunity signal rule: OK");
