/** Weakest lineups and their upgrade (nfl-lineup-upgrade-v1). */
import assert from "node:assert/strict";
import type { NflGeneratedLineup, NflLineupSlot, NflOptimizerPlayer } from "../src/app/dfs/nfl/nfl-optimizer";
import type { SwapCandidate, SwapRules } from "../src/lib/nfl-dfs/inactive-swap";
import { MIN_GAIN, upgradeLineups, weakestLineups } from "../src/lib/nfl-dfs/lineup-upgrade";
import { assertShowdownLineup } from "../src/lib/nfl-dfs/showdown-legality";

type Pos = NflOptimizerPlayer["position"];
function player(id: number, name: string, position: Pos, team: string, salary: number, proj: number): NflOptimizerPlayer {
  const opponent = team === "AAA" ? "BBB" : "AAA";
  return { id, dkPlayerId: id, captainDkPlayerId: id + 1000, name, position, team, opponent, gameKey: "AAA@BBB", salary, captainSalary: Math.round(salary * 1.5),
    rosterPositions: ["CPT", "FLEX"], isOut: false, projectionStatus: "historical", ourProj: proj, floorFpts: proj * 0.5, ceilingFpts: proj * 2,
    boomRate: null, avgFptsDk: null, fantasyprosProj: null, linestarProj: null, linestarOwnPct: null, customProj: null } as NflOptimizerPlayer;
}
const cand = (p: NflOptimizerPlayer): SwapCandidate => ({ player: p, projection: p.ourProj!, source: "our", floor: p.floorFpts!, ceiling: p.ceilingFpts!, flexEligible: true, captainEligible: true });
function showdown(n: number, captain: NflOptimizerPlayer, flex: NflOptimizerPlayer[]): NflGeneratedLineup {
  const slots: NflLineupSlot[] = [
    { slot: "CPT", player: captain, salary: captain.captainSalary!, multiplier: 1.5, projection: captain.ourProj! * 1.5, projectionSource: "our" },
    ...flex.map((p, i) => ({ slot: `FLEX${i + 1}`, player: p, salary: p.salary, multiplier: 1, projection: p.ourProj!, projectionSource: "our" as const })),
  ];
  return { lineupNumber: n, slots, playerIds: slots.map((s) => s.player.dkPlayerId), totalSalary: slots.reduce((s, x) => s + x.salary, 0),
    projectedFpts: slots.reduce((s, x) => s + x.projection, 0), floorFpts: 50, ceilingFpts: 150, projectedOwnership: null,
    stackSummary: { quarterback: null, passCatchers: [], bringBack: null } };
}

const qbA = player(1, "QB A", "QB", "AAA", 10000, 20);
const wrA = player(2, "WR A", "WR", "AAA", 8000, 15);
const rbB = player(3, "RB B", "RB", "BBB", 8000, 14);
const wrB = player(4, "WR B", "WR", "BBB", 6000, 10);
const teA = player(5, "TE A", "TE", "AAA", 4000, 6);
const kB = player(6, "K B", "K", "BBB", 3000, 5);
const puntA = player(7, "Punt A", "WR", "AAA", 2000, 1);
const midA = player(8, "Mid A", "WR", "AAA", 5000, 9);
const midB = player(9, "Mid B", "WR", "BBB", 5000, 9.5);
const dstB = player(10, "DST B", "DST", "BBB", 3000, 4);
const pool = [qbA, wrA, rbB, wrB, teA, kB, puntA, midA, midB, dstB];
const rules: SwapRules = { format: "showdown", mode: "gpp", minSalary: 0, overlapCap: 5, maxLineupsFor: () => 10 };

const strong1 = showdown(1, qbA, [wrA, rbB, wrB, teA, kB]);
const strong2 = showdown(2, wrA, [qbA, rbB, wrB, teA, kB]);
const strong3 = showdown(3, rbB, [qbA, wrA, wrB, teA, dstB]);
const weak = showdown(4, qbA, [rbB, wrB, kB, puntA, teA]);       // 3 AAA (QB, Punt, TE), 3 BBB
const set = [strong1, strong2, strong3, weak];

// --- the list: lowest projected first, only lineups below the median, with the player who drags it
const list = weakestLineups(set);
assert.equal(list[0].lineupNumber, 4);
assert.equal(list[0].weakestPlayer.name, "Punt A");
assert.ok(list[0].belowMedian > 0);
assert.ok(list.every((w) => w.belowMedian > 0), "a lineup at or above the median is never called weak");
assert.deepEqual(weakestLineups(set.slice(0, 3)), [], "too few lineups to rank");

// --- the upgrade keeps the team split: Punt A (AAA) can only become another AAA player
const res = upgradeLineups({ lineups: set, targets: [4], candidates: pool.map(cand), rules });
assert.equal(res.upgrades.length, 1);
const after = res.lineups[3];
assert.equal(after.lineupNumber, 4);
assert.ok(after.projectedFpts >= weak.projectedFpts + MIN_GAIN, "projects higher");
assert.ok(after.totalSalary <= 50000, "salary cap");
assertShowdownLineup(after);
const teams = (l: NflGeneratedLineup) => l.slots.filter((s) => s.player.team === "AAA").length;
assert.equal(teams(after), teams(weak), "same team split");
assert.equal(after.slots[0].player, qbA, "captain kept when no better legal captain exists");
assert.ok(!after.playerIds.includes(puntA.dkPlayerId), "the punt is gone");
assert.equal(res.lineups[0], strong1, "lineups not asked for are returned as they were");
for (const other of res.lineups.slice(0, 3)) assert.ok(after.playerIds.filter((id) => other.playerIds.includes(id)).length <= rules.overlapCap, "overlap cap against every other lineup");
assert.match(res.summary[0], /^Lineup 4: .*Punt A out.*Projected/);

// --- exposure limit: a player already at his limit is not brought in
const capped = upgradeLineups({ lineups: set, targets: [4], candidates: pool.map(cand), rules: { ...rules, maxLineupsFor: (id) => (id === wrA.dkPlayerId || id === midA.dkPlayerId ? 0 : 10) } });
assert.ok(!capped.lineups[3].playerIds.includes(wrA.dkPlayerId) && !capped.lineups[3].playerIds.includes(midA.dkPlayerId));

// --- overlap cap that leaves nothing legal: left alone and said
const tight = upgradeLineups({ lineups: set, targets: [4], candidates: pool.map(cand), rules: { ...rules, overlapCap: 0 } });
assert.deepEqual(tight.unchanged, [4]);
assert.equal(tight.lineups[3], weak);
assert.match(tight.summary[0], /no legal swap/);

// --- salary floor of the run is kept when the lineup met it
const floored = upgradeLineups({ lineups: set, targets: [4], candidates: pool.map(cand), rules: { ...rules, minSalary: weak.totalSalary } });
for (const l of floored.lineups) assert.ok(l.lineupNumber !== 4 || l.totalSalary >= weak.totalSalary);

console.log("nfl lineup upgrade: all assertions passed");
