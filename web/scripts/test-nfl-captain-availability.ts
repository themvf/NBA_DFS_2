/**
 * The Showdown Captain slot may not go to a player whose availability is in
 * doubt.
 *
 * Captain pays 1.5x points for 1.5x salary, so the slot is value-neutral and
 * a linear objective is indifferent about where a player goes: given a chosen
 * six, the solver puts the multiplier on whichever scores highest under the
 * objective, which in GPP mode is the highest ceiling. Nothing caps that --
 * `captainMax` falls back to `nLineups` -- so one ceiling estimate can legally
 * own every captain slot in the portfolio.
 *
 * Measured on the 2026 week-2 Monday showdown (DK contest 195786073, 47,562
 * entries): Puka Nacua carried a fresh QUESTIONABLE tag, our highest ceiling
 * on the slate (42.7) and 0.64% field ownership. He took 14 of 40 captain
 * slots and scored 0.0. Davante Adams, the correct captain, was our SEVENTH
 * ceiling; he was in 20 of 40 lineups and captained none.
 *
 * The rule here gates on evidence, not quota. Uniform captain exposure caps
 * were measured on the same slate and made it worse (best 117.8 -> 97.3),
 * because a cap pushes the multiplier onto genuinely weaker players.
 */
import assert from "node:assert/strict";
import {
  optimizeNflLineups, captainBlockedByAvailability,
  type NflOptimizerPlayer, type NflOptimizerSettings,
} from "../src/app/dfs/nfl/nfl-optimizer";

function player(over: Partial<NflOptimizerPlayer> & { dkPlayerId: number; salary: number }): NflOptimizerPlayer {
  return {
    id: over.dkPlayerId, dkPlayerId: over.dkPlayerId, captainDkPlayerId: over.dkPlayerId + 100_000,
    name: over.name ?? `P${over.dkPlayerId}`, position: over.position ?? "WR", team: over.team ?? "AAA",
    opponent: over.team === "BBB" ? "AAA" : "BBB", gameKey: "AAA@BBB", salary: over.salary,
    captainSalary: Math.round(over.salary * 1.5),
    isOut: over.isOut ?? false, projectionStatus: over.projectionStatus ?? "historical",
    historyGames: over.historyGames ?? 8, teamSeasonGames: over.teamSeasonGames ?? 8,
    availabilityStatus: over.availabilityStatus,
    ourProj: over.ourProj === undefined ? 10 : over.ourProj,
    floorFpts: over.floorFpts ?? 6,
    ceilingFpts: over.ceilingFpts ?? 16,
    boomRate: 0.2, avgFptsDk: over.avgFptsDk ?? 10,
    fantasyprosProj: null, linestarProj: null, linestarOwnPct: null, customProj: null,
  };
}

/** Six-plus players so a Showdown roster always solves. */
function pool(): NflOptimizerPlayer[] {
  return [
    player({ dkPlayerId: 1, salary: 9000, position: "QB", team: "AAA", ourProj: 20, ceilingFpts: 30 }),
    player({ dkPlayerId: 2, salary: 7000, position: "RB", team: "AAA", ourProj: 14, ceilingFpts: 22 }),
    player({ dkPlayerId: 3, salary: 6000, position: "WR", team: "BBB", ourProj: 12, ceilingFpts: 20 }),
    player({ dkPlayerId: 4, salary: 5000, position: "TE", team: "BBB", ourProj: 10, ceilingFpts: 17 }),
    player({ dkPlayerId: 5, salary: 4000, position: "WR", team: "AAA", ourProj: 8, ceilingFpts: 14 }),
    player({ dkPlayerId: 6, salary: 3000, position: "RB", team: "BBB", ourProj: 6, ceilingFpts: 11 }),
    player({ dkPlayerId: 7, salary: 2500, position: "DST", team: "AAA", ourProj: 5, ceilingFpts: 10 }),
  ];
}

function settings(over: Partial<NflOptimizerSettings> = {}): NflOptimizerSettings {
  return {
    format: "showdown", mode: "gpp", projectionSource: "our", allowDkFallback: true,
    nLineups: 5, minSalary: 0, maxExposure: 1, minUnique: 1, stackPassCatchers: 1,
    bringBack: false, randomness: 0, requireObservedHistory: true,
    lockedPlayerIds: [], excludedPlayerIds: [], minExposureByPlayer: {}, maxExposureByPlayer: {}, ...over,
  };
}

const captainsOf = (run: ReturnType<typeof optimizeNflLineups>) =>
  run.lineups.map((l) => l.slots.find((s) => s.multiplier === 1.5)!.player.dkPlayerId);

function main() {
  // --- The unit ---------------------------------------------------------------
  assert.equal(captainBlockedByAvailability(player({ dkPlayerId: 1, salary: 5000, availabilityStatus: "QUESTIONABLE" })), true);
  assert.equal(captainBlockedByAvailability(player({ dkPlayerId: 1, salary: 5000, availabilityStatus: "DOUBTFUL" })), true);
  assert.equal(captainBlockedByAvailability(player({ dkPlayerId: 1, salary: 5000, availabilityStatus: "questionable" })), true, "casing");
  assert.equal(captainBlockedByAvailability(player({ dkPlayerId: 1, salary: 5000, availabilityStatus: "ACTIVE" })), false);
  // Unknown is NOT doubt: absence of a tag is not a tag.
  assert.equal(captainBlockedByAvailability(player({ dkPlayerId: 1, salary: 5000, availabilityStatus: "UNKNOWN" })), false);
  assert.equal(captainBlockedByAvailability(player({ dkPlayerId: 1, salary: 5000 })), false, "absent status");
  assert.equal(captainBlockedByAvailability(player({ dkPlayerId: 1, salary: 5000, availabilityStatus: null })), false);

  // --- The Nacua shape: highest ceiling on the slate, QUESTIONABLE ------------
  const nacua = player({
    dkPlayerId: 99, salary: 9500, position: "WR", team: "AAA", name: "Questionable WR",
    ourProj: 25, ceilingFpts: 42, availabilityStatus: "QUESTIONABLE",
  });

  const healthy = optimizeNflLineups([...pool(), { ...nacua, availabilityStatus: "ACTIVE" }], settings());
  assert.ok(captainsOf(healthy).includes(99),
    "sanity: with an ACTIVE tag the highest ceiling IS captained, so the gate is what changes it");

  const run = optimizeNflLineups([...pool(), nacua], settings());
  assert.ok(run.lineups.length > 0, "the slate still solves");
  assert.ok(!captainsOf(run).includes(99), "a QUESTIONABLE player may never take the 1.5x multiplier");

  // He is NOT removed from the pool -- only from the captain slot.
  const decision = run.eligibility!.find((d) => d.dkPlayerId === 99)!;
  assert.equal(decision.eligible, true, "still rosterable at FLEX; QUESTIONABLE usually plays");
  assert.equal(decision.captainEligible, false);
  assert.ok(run.lineups.some((l) => l.slots.some((s) => s.player.dkPlayerId === 99 && s.multiplier === 1)),
    "and he does appear at FLEX");

  // --- The gate is about the multiplier, so Classic is untouched --------------
  const classic = optimizeNflLineups(
    [...pool(), nacua,
     player({ dkPlayerId: 8, salary: 4000, position: "WR", team: "BBB" }),
     player({ dkPlayerId: 9, salary: 4000, position: "RB", team: "AAA" }),
     player({ dkPlayerId: 10, salary: 3500, position: "WR", team: "BBB" }),
     player({ dkPlayerId: 11, salary: 3500, position: "TE", team: "AAA" })],
    settings({ format: "classic", minSalary: 0 }),
  );
  // Classic has no Captain slot, so the gate must not touch eligibility there.
  // Asserted on the decision rather than on roster membership: whether the
  // solver happens to want him is a different question from whether he is allowed.
  assert.equal(classic.eligibility!.find((d) => d.dkPlayerId === 99)!.eligible, true,
    "Classic has no 1.5x slot, so a QUESTIONABLE player is unaffected there");

  // --- A ruled-out player is still refused outright, not merely Flex-only -----
  const out = optimizeNflLineups(
    [...pool(), { ...nacua, isOut: true, availabilityStatus: "OUT" }], settings(),
  );
  assert.equal(out.eligibility!.find((d) => d.dkPlayerId === 99)!.eligible, false);

  console.log("Captain availability gate:");
  console.log("  - QUESTIONABLE/DOUBTFUL cannot take the 1.5x slot, but stay rosterable at FLEX");
  console.log("  - unknown is not doubt; an empty feed changes nothing");
  console.log("  - Classic is untouched, and a ruled-out player is still refused outright");
}

main();
