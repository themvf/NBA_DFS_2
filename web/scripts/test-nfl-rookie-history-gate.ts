/**
 * Season-aware observed-history gate.
 *
 * The flat >=2-game rule over-blocks early season: in week 2 EVERY rookie
 * starter has exactly one career game, so "one real game" and "zero games"
 * were indistinguishable (found live: Dominic Zvada, NYG's rookie K1, blocked
 * with his week-1 results on file). The requirement is now capped at the games
 * the player's TEAM has completed this season, never below 1, and falls back
 * to the flat minimum when the team's season game count is unknown.
 */
import assert from "node:assert/strict";
import { observedHistoryRequirement, MIN_OBSERVED_GAMES } from "../src/lib/nfl-dfs/opportunity-redistribution";
import { optimizeNflLineups, DEFAULT_NFL_PUNT_POLICY, type NflOptimizerPlayer, type NflOptimizerSettings } from "../src/app/dfs/nfl/nfl-optimizer";

function player(over: Partial<NflOptimizerPlayer> & { dkPlayerId: number; salary: number }): NflOptimizerPlayer {
  return {
    id: over.dkPlayerId, dkPlayerId: over.dkPlayerId, captainDkPlayerId: over.dkPlayerId + 100_000,
    name: over.name ?? `P${over.dkPlayerId}`, position: over.position ?? "WR", team: over.team ?? "AAA",
    opponent: over.team === "BBB" ? "AAA" : "BBB", gameKey: "AAA@BBB", salary: over.salary,
    captainSalary: Math.round(over.salary * 1.5), isOut: false, projectionStatus: "historical",
    historyGames: over.historyGames ?? 6, teamSeasonGames: over.teamSeasonGames,
    ourProj: over.ourProj ?? 10, floorFpts: 7, ceilingFpts: 14, boomRate: 0.2,
    avgFptsDk: 10, fantasyprosProj: null, linestarProj: null, linestarOwnPct: null, customProj: null,
  };
}

/** Veterans with deep history so the roster always solves. */
function corePool(teamSeasonGames?: number | null): NflOptimizerPlayer[] {
  return [
    player({ dkPlayerId: 1, salary: 10000, position: "QB", team: "AAA", ourProj: 20, teamSeasonGames }),
    player({ dkPlayerId: 2, salary: 9000, position: "WR", team: "AAA", ourProj: 16, teamSeasonGames }),
    player({ dkPlayerId: 3, salary: 8000, position: "RB", team: "AAA", ourProj: 14, teamSeasonGames }),
    player({ dkPlayerId: 4, salary: 7000, position: "WR", team: "BBB", ourProj: 12, teamSeasonGames }),
    player({ dkPlayerId: 5, salary: 6000, position: "TE", team: "BBB", ourProj: 10, teamSeasonGames }),
    player({ dkPlayerId: 6, salary: 5000, position: "RB", team: "BBB", ourProj: 9, teamSeasonGames }),
    player({ dkPlayerId: 7, salary: 4200, position: "WR", team: "AAA", ourProj: 8, teamSeasonGames }),
  ];
}

function settings(over: Partial<NflOptimizerSettings> = {}): NflOptimizerSettings {
  return {
    format: "showdown", mode: "gpp", projectionSource: "our", allowDkFallback: true,
    nLineups: 2, minSalary: 0, maxExposure: 1, minUnique: 1, stackPassCatchers: 1, bringBack: true, randomness: 0,
    requireObservedHistory: true,
    lockedPlayerIds: [], excludedPlayerIds: [], minExposureByPlayer: {}, maxExposureByPlayer: {}, ...over,
  };
}

function decisionFor(pool: NflOptimizerPlayer[], id: number, over: Partial<NflOptimizerSettings> = {}) {
  const run = optimizeNflLineups(pool, settings(over));
  const d = run.eligibility!.find((e) => e.dkPlayerId === id)!;
  return { run, d };
}

function main() {
  // --- Unit: the requirement itself ---
  assert.equal(observedHistoryRequirement(null), MIN_OBSERVED_GAMES, "unknown team games -> flat minimum");
  assert.equal(observedHistoryRequirement(undefined), MIN_OBSERVED_GAMES);
  assert.equal(observedHistoryRequirement(Number.NaN), MIN_OBSERVED_GAMES, "non-finite -> flat minimum");
  assert.equal(observedHistoryRequirement(0), 1, "week 1: never drops below one game");
  assert.equal(observedHistoryRequirement(1), 1, "week 2: one available game suffices");
  assert.equal(observedHistoryRequirement(2), MIN_OBSERVED_GAMES, "week 3+: flat minimum returns");
  assert.equal(observedHistoryRequirement(10), MIN_OBSERVED_GAMES, "cap, not a ramp");

  // --- Legacy path (no punt policy): week-2 rookie starter is eligible ---
  const rookieWk2 = player({ dkPlayerId: 90, salary: 4800, position: "K", team: "BBB", historyGames: 1, teamSeasonGames: 1, ourProj: 7.5 });
  const backupWk2 = player({ dkPlayerId: 91, salary: 5200, position: "QB", team: "BBB", historyGames: 0, teamSeasonGames: 1, ourProj: 15 });
  {
    const { d } = decisionFor([...corePool(1), rookieWk2], 90);
    assert.equal(d.eligible, true, "week 2: rookie with his one available game is eligible");
  }
  {
    const { d } = decisionFor([...corePool(1), backupWk2], 91);
    assert.equal(d.eligible, false, "week 2: zero-game backup still fails closed");
    assert.equal(d.reasonCode, "ROLE_UNKNOWN");
  }

  // --- Week 3+: one game is no longer every available game; strictness returns ---
  {
    const oneGameWk3 = player({ dkPlayerId: 92, salary: 4800, position: "K", team: "BBB", historyGames: 1, teamSeasonGames: 2, ourProj: 7.5 });
    const { d } = decisionFor([...corePool(2), oneGameWk3], 92);
    assert.equal(d.eligible, false, "week 3: a one-game player is behind his team and stays blocked");
  }

  // --- Unknown season context: conservative flat fallback ---
  {
    const oneGameUnknown = player({ dkPlayerId: 93, salary: 4800, position: "K", team: "BBB", historyGames: 1, teamSeasonGames: null, ourProj: 7.5 });
    const { d } = decisionFor([...corePool(null), oneGameUnknown], 93);
    assert.equal(d.eligible, false, "unknown team games must not loosen the gate");
  }

  // --- A lock still outranks the gate ---
  {
    const { run } = decisionFor([...corePool(1), backupWk2], 91, { lockedPlayerIds: [91] });
    assert.ok(run.lineups.length > 0 && run.lineups.every((l) => l.playerIds.includes(91)), "locked zero-game player is kept");
  }

  // --- Punt-policy path: the same rule applies with the policy active ---
  {
    const { d } = decisionFor([...corePool(1), rookieWk2], 90, { puntPolicy: DEFAULT_NFL_PUNT_POLICY });
    assert.equal(d.eligible, true, "policy path: week-2 rookie above the cheap threshold is eligible");
  }
  {
    const { d } = decisionFor([...corePool(1), backupWk2], 91, { puntPolicy: DEFAULT_NFL_PUNT_POLICY });
    assert.equal(d.eligible, false, "policy path: zero-game backup still fails");
  }

  // --- Punt-policy path, CHEAP rookie: the weak-evidence confidence fallback
  //     is season-aware too, so a $2,800 week-2 starter passes the role gate
  //     as capped salary relief while a week-3 one-game player does not. ---
  {
    const cheapRookieWk2 = player({ dkPlayerId: 94, salary: 2800, position: "WR", team: "BBB", historyGames: 1, teamSeasonGames: 1, ourProj: 6 });
    const { d } = decisionFor([...corePool(1), cheapRookieWk2], 94, { puntPolicy: DEFAULT_NFL_PUNT_POLICY });
    assert.equal(d.eligible, true, "cheap week-2 rookie clears the role gate on his own game");
    assert.equal(d.salaryRelief, true, "he is still capped salary relief, not a free pass");
  }
  {
    const cheapOneGameWk3 = player({ dkPlayerId: 95, salary: 2800, position: "WR", team: "BBB", historyGames: 1, teamSeasonGames: 2, ourProj: 6 });
    const { d } = decisionFor([...corePool(2), cheapOneGameWk3], 95, { puntPolicy: DEFAULT_NFL_PUNT_POLICY });
    assert.equal(d.eligible, false, "cheap one-game player in week 3 still fails closed");
    assert.equal(d.reasonCode, "ROLE_UNKNOWN");
  }

  console.log("NFL rookie history gate: season-aware requirement, week-1 floor, fallback, lock, and punt-policy paths passed.");
}

main();
