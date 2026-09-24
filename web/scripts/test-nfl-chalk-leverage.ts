/**
 * The CPT control and the chalk-captain, rotating-leverage lineup model.
 */
import assert from "node:assert/strict";
import { captainExposurePolicies, generationSettings } from "../src/lib/nfl-dfs/generation-settings";
import { chalkLeveragePlan, LEVERAGE_ROTATION, type ArchetypeSlateContext } from "../src/lib/nfl-dfs/archetypes";
import { optimizeNflLineups, type NflOptimizerPlayer, type NflOptimizerSettings } from "../src/app/dfs/nfl/nfl-optimizer";

// ── CPT control ─────────────────────────────────────────────────────────────
{
  const [policy] = captainExposurePolicies({ "7": { min: 10, max: 20 } }, {}, 0.6);
  assert.deepEqual(policy.captain, { minPct: 0.1, maxPct: 0.2 });
  // The regression this exists for: a per-player policy REPLACES the flat max,
  // so a captain range alone once took a player from 60% to 94-100% overall.
  assert.deepEqual(policy.overall, { minPct: null, maxPct: 0.6 }, "the global cap is carried across");

  const [targeted] = captainExposurePolicies({ "7": { min: null, max: 30 } }, { "7": 0.5 }, 0.6);
  assert.deepEqual(targeted.overall, { minPct: 0.5, maxPct: 0.5 }, "an explicit overall target wins");
  assert.deepEqual(captainExposurePolicies({ "7": { min: null, max: null } }, {}, 0.6), [], "blank is no policy");
  assert.equal(captainExposurePolicies({ "7": { min: -5, max: 250 } }, {}, 0.6)[0].captain.maxPct, 1, "clamped");

  const base = { mode: "gpp", projectionSource: "our", allowDkFallback: false, nLineups: 20, minSalary: 0,
    maxExposure: 0.6, minUnique: 1, stackPassCatchers: 0, bringBack: false, randomness: 0 } as never;
  assert.equal(generationSettings(base, "classic", [], [], {}, { "7": { min: 10, max: 20 } }).exposurePolicies, undefined,
    "no captain on Classic, so no captain policy");
  assert.equal(generationSettings(base, "showdown", [], [], {}, { "7": { min: 10, max: 20 } }).exposurePolicies?.length, 1);
}

// ── The model's definitions ─────────────────────────────────────────────────
const P = (id: number, position: NflOptimizerPlayer["position"], team: string, projection: number, captainEligible = true) =>
  ({ dkPlayerId: id, position, team, opponent: team === "ATL" ? "GB" : "ATL", ownership: null, projection, captainEligible });
const ctx: ArchetypeSlateContext = {
  players: [
    P(1, "RB", "ATL", 20.8), P(2, "WR", "GB", 17.4), P(3, "QB", "GB", 16.5), P(4, "TE", "GB", 12.2),
    P(5, "WR", "ATL", 12.2), P(6, "QB", "ATL", 12.1), P(7, "TE", "ATL", 9.1), P(8, "WR", "GB", 6.5),
    P(9, "RB", "GB", 5.8), P(10, "K", "GB", 9.7), P(11, "DST", "ATL", 6.1), P(12, "WR", "ATL", 0),
  ],
  favoriteTeam: "GB", underdogTeam: "ATL", ownershipValidated: false,
};
{
  const plan = chalkLeveragePlan(ctx, 10);
  assert.deepEqual(plan.chalkCaptainIds, [1, 2, 3], "top three by projection: the obvious captains");
  assert.deepEqual(plan.coreIds, [1, 2, 3, 4, 5, 6], "the six chalkiest skill players");
  assert.equal(plan.basis, "projection", "no ownership feed -> projection proxy, disclosed");
  // No QB outside the core has a projection here, so QB drops out of the rotation.
  assert.deepEqual(plan.rotation, ["WR", "TE", "RB", "K/DST"]);
  assert.deepEqual(plan.lineups.map((l) => l.leveragePosition),
    ["WR", "TE", "RB", "K/DST", "WR", "TE", "RB", "K/DST", "WR", "TE"], "the leverage position rotates");
  const wr = plan.lineups[0].compiled;
  assert.deepEqual(wr.eligibleCaptainIds, [1, 2, 3]);
  assert.deepEqual(wr.beneficiaries[0].playerIds, [8], "leverage excludes the core and zero projections");
  assert.deepEqual(plan.lineups[3].compiled.beneficiaries[0].playerIds, [10, 11], "K and DST share one leverage slot");
  assert.equal(wr.fadePlayerIds.length, 0, "leverage is not a fade");

  // A captain MINIMUM the user set must stay reachable.
  assert.deepEqual(chalkLeveragePlan(ctx, 4, { extraCaptainIds: [4] }).chalkCaptainIds, [1, 2, 3, 4]);
  assert.ok(LEVERAGE_ROTATION.includes("K/DST"));
}

// ── End to end through the optimizer ────────────────────────────────────────
{
  const player = (id: number, name: string, position: NflOptimizerPlayer["position"], team: string, salary: number, proj: number): NflOptimizerPlayer =>
    ({ dkPlayerId: id, captainDkPlayerId: 100 + id, name, position, team, opponent: team === "ATL" ? "GB" : "ATL",
       gameKey: "ATL@GB", salary, captainSalary: Math.round(salary * 1.5), ourProj: proj, floorFpts: proj * 0.3,
       ceilingFpts: proj * 2.2, isOut: false, projectionStatus: "historical", historyGames: 20 } as NflOptimizerPlayer);
  const pool = [
    player(1, "Bijan", "RB", "ATL", 11800, 20.8), player(2, "Watson", "WR", "GB", 9800, 17.4),
    player(3, "Love", "QB", "GB", 10000, 16.5), player(4, "Kraft", "TE", "GB", 7000, 12.2),
    player(5, "London", "WR", "ATL", 8800, 12.2), player(6, "Penix", "QB", "ATL", 9000, 12.1),
    player(7, "Pitts", "TE", "ATL", 6600, 9.1), player(8, "Golden", "WR", "GB", 7800, 6.5),
    player(9, "Lloyd", "RB", "GB", 7400, 5.8), player(10, "Smack", "K", "GB", 5200, 9.7),
    player(11, "Falcons", "DST", "ATL", 3400, 6.1), player(12, "Hooper", "TE", "ATL", 1600, 4.4),
    player(13, "Zaccheaus", "WR", "ATL", 2000, 3.9), player(14, "Brian R", "RB", "ATL", 4400, 4.4),
  ];
  const settings: NflOptimizerSettings = {
    format: "showdown", mode: "gpp", projectionSource: "our", allowDkFallback: false, nLineups: 8, minSalary: 0,
    maxExposure: 1, minUnique: 1, stackPassCatchers: 0, bringBack: false, randomness: 0,
    lockedPlayerIds: [], excludedPlayerIds: [], minExposureByPlayer: {}, maxExposureByPlayer: {},
    archetypeMode: "chalk_leverage",
  };
  const result = optimizeNflLineups(pool, settings);
  assert.equal(result.lineups.length, 8);
  const chalkCaptains = new Set(["Bijan", "Watson", "Love"]);
  for (const lineup of result.lineups) {
    const captain = lineup.slots.find((s) => s.slot === "CPT")!.player.name;
    assert.ok(chalkCaptains.has(captain), `captain ${captain} must be chalk`);
    assert.equal(lineup.archetype?.id, "chalk_captain_leverage");
    assert.equal(lineup.archetype?.beneficiariesSatisfied.length, 1, "every lineup carries its leverage player");
  }
  assert.ok(result.warnings.some((w) => w.startsWith("Chalk captain model:")), "the run discloses what it chose");
}

console.log("Chalk captain model + CPT control:");
console.log("  - a captain range keeps the overall cap instead of lifting it");
console.log("  - captains limited to the chalk set plus any user captain minimum");
console.log("  - one leverage player per lineup, and its position rotates");
