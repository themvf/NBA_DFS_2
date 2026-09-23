/**
 * A policy zero must never be re-read as a missing value.
 *
 * Found in the 2026 week-2 classic post-mortem (DK contest 195648006,
 * 317,082 entries). Two sources decide availability and they do not agree:
 *
 *  - DraftKings' `Status` column -> `isOut`, which covers OUT/IR only.
 *  - our own availability feed, which zeroes the projection and stamps
 *    `projectionStatus = "out"`, and can rule out a player DK lists
 *    Doubtful or Questionable.
 *
 * The optimizer honoured the first and not the second, and `projectionFor`
 * fell back to the DK season average whenever the selected source produced
 * no POSITIVE number -- and a deliberate 0 is not positive. So a player our
 * own feed had ruled out was restored at somebody else's number.
 *
 * Live cost: 11 players on that slate carried a policy zero and a non-zero DK
 * average. All 11 scored exactly 0. The largest, Zay Flowers at 29.0, became
 * the single highest projection on the board and reached 3 of 20 lineups
 * against 0.11% field ownership.
 */
import assert from "node:assert/strict";
import { optimizeNflLineups, type NflOptimizerPlayer, type NflOptimizerSettings }
  from "../src/app/dfs/nfl/nfl-optimizer";
import { OUT_PROJECTION_STATUS, zeroOutProjection, storedSlateProjection }
  from "../src/lib/nfl-dfs/out-projection";

function player(over: Partial<NflOptimizerPlayer> & { dkPlayerId: number; salary: number }): NflOptimizerPlayer {
  return {
    id: over.dkPlayerId, dkPlayerId: over.dkPlayerId, captainDkPlayerId: over.dkPlayerId + 100_000,
    name: over.name ?? `P${over.dkPlayerId}`, position: over.position ?? "WR", team: over.team ?? "AAA",
    opponent: over.team === "BBB" ? "AAA" : "BBB", gameKey: "AAA@BBB", salary: over.salary,
    captainSalary: Math.round(over.salary * 1.5),
    isOut: over.isOut ?? false, projectionStatus: over.projectionStatus ?? "historical",
    historyGames: over.historyGames ?? 6, teamSeasonGames: over.teamSeasonGames ?? 6,
    // `??` would turn an explicit null (no projection at all) into 10.
    ourProj: over.ourProj === undefined ? 10 : over.ourProj, floorFpts: 7, ceilingFpts: 14, boomRate: 0.2,
    avgFptsDk: over.avgFptsDk ?? 10,
    fantasyprosProj: null, linestarProj: null, linestarOwnPct: null, customProj: null,
  };
}

function corePool(): NflOptimizerPlayer[] {
  return [
    player({ dkPlayerId: 1, salary: 10000, position: "QB", team: "AAA", ourProj: 20 }),
    player({ dkPlayerId: 2, salary: 9000, position: "WR", team: "AAA", ourProj: 16 }),
    player({ dkPlayerId: 3, salary: 8000, position: "RB", team: "AAA", ourProj: 14 }),
    player({ dkPlayerId: 4, salary: 7000, position: "WR", team: "BBB", ourProj: 12 }),
    player({ dkPlayerId: 5, salary: 6000, position: "TE", team: "BBB", ourProj: 10 }),
    player({ dkPlayerId: 6, salary: 5000, position: "RB", team: "BBB", ourProj: 9 }),
    player({ dkPlayerId: 7, salary: 4200, position: "WR", team: "AAA", ourProj: 8 }),
  ];
}

function settings(over: Partial<NflOptimizerSettings> = {}): NflOptimizerSettings {
  return {
    format: "showdown", mode: "gpp", projectionSource: "our", allowDkFallback: true,
    nLineups: 2, minSalary: 0, maxExposure: 1, minUnique: 1, stackPassCatchers: 1,
    bringBack: true, randomness: 0, requireObservedHistory: true,
    lockedPlayerIds: [], excludedPlayerIds: [], minExposureByPlayer: {}, maxExposureByPlayer: {}, ...over,
  };
}

function decision(pool: NflOptimizerPlayer[], id: number, over: Partial<NflOptimizerSettings> = {}) {
  const run = optimizeNflLineups(pool, settings(over));
  return { run, d: run.eligibility!.find((e) => e.dkPlayerId === id)! };
}

function main() {
  // --- The Zay Flowers shape: DK says Doubtful (not OUT), our feed zeroed him,
  //     and his DK average is the biggest number on the board. ------------------
  const flowers = player({
    dkPlayerId: 99, salary: 6700, position: "WR", team: "AAA", name: "Zeroed WR",
    isOut: false, projectionStatus: OUT_PROJECTION_STATUS, ourProj: 0, avgFptsDk: 29,
  });
  const pool = [...corePool(), flowers];

  const { run, d } = decision(pool, 99);
  assert.equal(d.eligible, false, "a player our own feed ruled out must not be eligible");
  assert.equal(d.reasonCode, "INACTIVE");
  assert.match(d.reason ?? "", /availability feed/i, "the reason must name the feed, not invent an OUT tag");
  assert.ok(
    run.lineups.every((l) => l.slots.every((s) => s.player.dkPlayerId !== 99)),
    "a ruled-out player must not appear in any lineup",
  );


  // --- The same player with the fallback switched OFF behaves identically.
  //     The gate must not depend on a user setting. -----------------------------
  const off = decision(pool, 99, { allowDkFallback: false });
  assert.equal(off.d.eligible, false, "the gate holds regardless of allowDkFallback");
  assert.equal(off.d.reasonCode, "INACTIVE");

  // --- DK's own OUT flag still works, and still reads as OUT/IR. ---------------
  const dkOut = player({
    dkPlayerId: 98, salary: 7000, position: "WR", team: "AAA",
    isOut: true, projectionStatus: OUT_PROJECTION_STATUS, ourProj: 0, avgFptsDk: 21,
  });
  const dk = decision([...corePool(), dkOut], 98);
  assert.equal(dk.d.eligible, false);
  assert.match(dk.d.reason ?? "", /OUT\/IR/, "DK's flag keeps its own wording");

  // --- A genuinely missing projection is NOT a policy zero, so the fallback
  //     still does its intended job for a playing player. ------------------------
  const missing = player({
    dkPlayerId: 97, salary: 4600, position: "WR", team: "BBB",
    isOut: false, projectionStatus: "unmatched", ourProj: null, avgFptsDk: 11,
  });
  const m = decision([...corePool(), missing], 97);
  assert.equal(m.d.eligible, true, "absence is not a decision: the DK fallback still applies");
  assert.equal(m.run.sourceCoverage.fallback, 1, "he is the one player resolved by fallback");
  assert.ok(m.run.warnings.some((w) => /DK Avg fallback/.test(w)), "and the run says so");

  // ...and turning the fallback off excludes him, as before.
  assert.equal(decision([...corePool(), missing], 97, { allowDkFallback: false }).d.eligible, false);

  // --- A playing player projected at exactly 0 by the model (not by policy)
  //     is still allowed to use the fallback: only `out` is a decision. ---------
  const trueZero = player({
    dkPlayerId: 96, salary: 4000, position: "WR", team: "BBB",
    isOut: false, projectionStatus: "historical", ourProj: 0, avgFptsDk: 9,
  });
  const z = decision([...corePool(), trueZero], 96);
  assert.equal(z.d.eligible, true, "a model zero on a playing player is not the policy zero");

  // --- The two writers that produce the policy zero agree with the gate. -------
  const zeroed = zeroOutProjection(
    { projectionStatus: "historical", ourProj: 18.4, floorFpts: 9, ceilingFpts: 26, boomRate: 0.3 },
    true,
  );
  assert.equal(zeroed.projectionStatus, OUT_PROJECTION_STATUS);
  assert.equal(zeroed.ourProj, 0);
  const stored = storedSlateProjection(
    { projectionStatus: "historical", modelProjFpts: 18.4, floorFpts: 9, medianFpts: 17, ceilingFpts: 26, boomRate: 0.3 },
    true,
  );
  assert.equal(stored.projectionStatus, OUT_PROJECTION_STATUS);
  assert.equal(stored.ourProj, 0);
  // Both statuses are exactly what the optimizer now gates on.
  assert.equal(
    decision([...corePool(), player({ dkPlayerId: 95, salary: 6000, position: "WR", team: "AAA",
      projectionStatus: stored.projectionStatus, ourProj: stored.ourProj, avgFptsDk: 25 })], 95).d.eligible,
    false,
    "the status the slate writer stores is the status the optimizer refuses",
  );

  // --- A lock is the user's own instruction and still outranks the gate?
  //     No: an inactive player is refused before locks, as before this change. --
  const locked = decision(pool, 99, { lockedPlayerIds: [99] });
  assert.equal(locked.d.eligible, false, "inactivity is not overridable by a lock");

  console.log("Out-fallback gate: a policy zero is a decision, not a missing value.");
  console.log("  - our feed's ruling is honoured even when DK says only Doubtful");
  console.log("  - the DK average can no longer restore a player we ruled out");
  console.log("  - a genuinely missing projection still falls back as intended");
}

main();
