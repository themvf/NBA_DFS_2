/**
 * The per-player salary floor exists because a Showdown pool contains $200 third-string
 * bodies the solver will happily use as free roster filler to afford studs. Those players
 * are not cheap production, they are non-participants, so the fix removes them from the
 * pool rather than trying to price them.
 *
 * The load-bearing assertions are that (a) the floor actually removes them, (b) a LOCK
 * outranks the floor -- the floor is a default, never an override of an explicit
 * instruction -- and (c) the removal is reported rather than silent.
 */
import assert from "node:assert/strict";
import { optimizeNflLineups, type NflOptimizerPlayer, type NflOptimizerSettings }
  from "../src/app/dfs/nfl/nfl-optimizer";

const player = (id: number, salary: number, proj: number, team: string): NflOptimizerPlayer => ({
  id, dkPlayerId: id, captainDkPlayerId: id + 1000, captainSalary: Math.round(salary * 1.5),
  name: `P${id}`, position: id % 2 ? "WR" : "RB", team, opponent: team === "AAA" ? "BBB" : "AAA",
  gameKey: "AAA@BBB", salary, isOut: false, projectionStatus: "ok", ourProj: proj, floorFpts: proj * 0.7,
  ceilingFpts: proj * 1.6, boomRate: 0.2, avgFptsDk: proj, fantasyprosProj: null, linestarProj: null,
  linestarOwnPct: null, customProj: null,
});

// Two elites and six mid-priced players, plus one $200 body per side. The cap binds hard
// enough that the solver genuinely WANTS the punts: it cannot fit both elites otherwise.
const pool: NflOptimizerPlayer[] = [
  player(10, 11000, 25, "AAA"), player(20, 11000, 25, "BBB"),
  player(11, 7000, 10, "AAA"), player(12, 7000, 10, "AAA"), player(13, 7000, 10, "AAA"),
  player(21, 7000, 10, "BBB"), player(22, 7000, 10, "BBB"), player(23, 7000, 10, "BBB"),
];
const punts = [player(90, 200, 1.5, "AAA"), player(91, 200, 1.4, "BBB")];
pool.push(...punts);

const base: NflOptimizerSettings = {
  format: "showdown", mode: "gpp", projectionSource: "our", allowDkFallback: false, nLineups: 3,
  minSalary: 0, minPlayerSalary: 1000, maxExposure: 1, minUnique: 1, stackPassCatchers: 0,
  bringBack: false, randomness: 0, lockedPlayerIds: [], excludedPlayerIds: [],
  minExposureByPlayer: {}, maxExposureByPlayer: {},
};

const floored = optimizeNflLineups(pool, base);
assert.ok(floored.lineups.length > 0, "a floored Showdown pool must still solve");
for (const lineup of floored.lineups) {
  for (const id of lineup.playerIds) assert.ok(id !== 90 && id !== 91, `$200 filler ${id} survived the floor`);
}
assert.ok(floored.warnings.some((w) => /2 player\(s\) priced under the \$1,000/.test(w)),
  "the removal must be reported, not silent");

// With the floor off, the solver does reach for the $200 bodies -- proving the test pool
// is one where the floor is what changes the outcome, not an incidental constraint.
const unfloored = optimizeNflLineups(pool, { ...base, minPlayerSalary: 0 });
assert.ok(unfloored.lineups.some((l) => l.playerIds.some((id) => id === 90 || id === 91)),
  "control case: without a floor the $200 filler should be used");

// An explicit lock is the user's own instruction and outranks the floor.
const locked = optimizeNflLineups(pool, { ...base, lockedPlayerIds: [90] });
assert.ok(locked.lineups.every((l) => l.playerIds.includes(90)), "a locked sub-floor player must be kept");
assert.ok(locked.lineups.every((l) => !l.playerIds.includes(91)), "the unlocked sub-floor player stays removed");

// A target exposure is equally explicit and must not throw for a sub-floor player.
const targeted = optimizeNflLineups(pool, { ...base, minExposureByPlayer: { "91": 1 } });
assert.ok(targeted.lineups.every((l) => l.playerIds.includes(91)), "a targeted sub-floor player must be kept");

// The floor is format-agnostic but a no-op where nothing is priced under it.
const classicPool = pool.filter((p) => p.salary >= 1000);
assert.equal(optimizeNflLineups(classicPool, base).warnings.filter((w) => /salary floor/.test(w)).length, 0);

console.log("Showdown per-player salary floor: removal, control, lock, target and no-op cases passed.");
