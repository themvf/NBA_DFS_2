/**
 * A captain / flex MINIMUM must be honored while lineups are built, not merely
 * reported afterward. Regression: a low-scoring player with a captain minimum
 * was never picked (the optimizer scored him out), then export QA blocked the
 * run with "captain min missed (0/1)".
 */
import assert from "node:assert/strict";
import { optimizeNflLineups, type NflOptimizerPlayer, type NflOptimizerSettings } from "../src/app/dfs/nfl/nfl-optimizer";

const player = (id: number, name: string, position: NflOptimizerPlayer["position"], team: string, salary: number, proj: number): NflOptimizerPlayer =>
  ({ dkPlayerId: id, captainDkPlayerId: 100 + id, name, position, depthRole: position === "K" ? "Listed K1" : null, team, opponent: team === "ATL" ? "GB" : "ATL",
     gameKey: "ATL@GB", salary, captainSalary: Math.round(salary * 1.5), ourProj: proj, floorFpts: proj * 0.3,
     ceilingFpts: proj * 2.2, isOut: false, projectionStatus: "historical", historyGames: 20 } as NflOptimizerPlayer);
const pool = [
  player(1, "Bijan", "RB", "ATL", 11800, 20.8), player(2, "Watson", "WR", "GB", 9800, 17.4),
  player(3, "Love", "QB", "GB", 10000, 16.5), player(4, "Kraft", "TE", "GB", 7000, 12.2),
  player(5, "London", "WR", "ATL", 8800, 12.2), player(6, "Penix", "QB", "ATL", 9000, 12.1),
  player(7, "Pitts", "TE", "ATL", 6600, 9.1), player(8, "Golden", "WR", "GB", 7800, 6.5),
  player(9, "Lloyd", "RB", "GB", 7400, 5.8), player(10, "Smack", "K", "GB", 5200, 9.7),
  player(11, "Falcons", "DST", "ATL", 3400, 6.1), player(12, "Hooper", "TE", "ATL", 1600, 4.4),
];
const base: NflOptimizerSettings = {
  format: "showdown", mode: "gpp", projectionSource: "our", allowDkFallback: false, nLineups: 8, minSalary: 0,
  maxExposure: 1, minUnique: 1, stackPassCatchers: 0, bringBack: false, randomness: 0,
  lockedPlayerIds: [], excludedPlayerIds: [], minExposureByPlayer: {}, maxExposureByPlayer: {},
};
const policy = (playerId: number, captain: { minPct: number | null; maxPct: number | null }, flex = { minPct: null, maxPct: null } as { minPct: number | null; maxPct: number | null }) =>
  ({ playerId, overall: { minPct: null, maxPct: 1 }, captain, flex, exactTargetMode: false });

// Without the minimum the cheap, low-projection TE never captains.
const free = optimizeNflLineups(pool, base);
const freeCpt = free.lineups.filter((l) => l.slots.find((s) => s.slot === "CPT")!.player.name === "Hooper").length;
assert.ok(freeCpt < 4, `premise: optimizer under-uses him unforced (got ${freeCpt})`);

// With a 50% captain minimum (4 of 8) he captains at least four times.
const withMin = optimizeNflLineups(pool, { ...base, exposurePolicies: [policy(12, { minPct: 0.5, maxPct: 1 })] });
assert.equal(withMin.lineups.length, 8, "forcing a minimum must not shorten the run");
const cpt = withMin.lineups.filter((l) => l.slots.find((s) => s.slot === "CPT")!.player.name === "Hooper").length;
assert.ok(cpt >= 4, `captain minimum honored (got ${cpt})`);
assert.equal(withMin.exposureReport?.find((r) => r.dkPlayerId === 12)?.binding?.startsWith("captain min missed") ?? false, false);

// An overall cap must leave room for the Captain minimum rather than get
// exhausted by cheap FLEX appearances early in the run.
const reserved = optimizeNflLineups(pool, { ...base, exposurePolicies: [{ ...policy(12, { minPct: 0.5, maxPct: 1 }),
  overall: { minPct: null, maxPct: 0.5 } }] });
const reservedCounts = reserved.exposureReport!.find(r => r.dkPlayerId === 12)!;
assert.equal(reservedCounts.overall, 4);
assert.equal(reservedCounts.captain, 4);
assert.equal(reservedCounts.flex, 0);

// Later K/DST archetypes cannot captain a TE. Meet his four commitments
// during the earlier standard lineups rather than waiting until too late.
const restricted = optimizeNflLineups(pool, { ...base,
  archetypeQuotas: [{archetypeId:'standard_ceiling',minLineups:4,maxLineups:4,enabled:true},
    {archetypeId:'low_scoring_k_dst',minLineups:4,maxLineups:4,enabled:true}],
  exposurePolicies:[policy(12,{minPct:0.5,maxPct:1})] });
assert.equal(restricted.lineups.length,8);
assert.ok(restricted.exposureReport!.find(r=>r.dkPlayerId===12)!.captain>=4);

// A flex minimum is honored the same way.
const flexMin = optimizeNflLineups(pool, { ...base, exposurePolicies: [policy(9, { minPct: null, maxPct: null }, { minPct: 0.5, maxPct: 1 })] });
const flexCount = flexMin.lineups.filter((l) => l.slots.some((s) => s.slot.startsWith("FLEX") && s.player.name === "Lloyd")).length;
assert.ok(flexCount >= 4, `flex minimum honored (got ${flexCount})`);
console.log("captain/flex minimum tests passed");
