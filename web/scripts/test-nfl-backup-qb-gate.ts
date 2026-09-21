/**
 * Two independent defects let backup quarterbacks into a Showdown pool at starter
 * projections. Both are covered here because fixing either alone leaves the pool wrong.
 *
 * 1. Stale roster evidence failed OPEN. The Sleeper depth chart correctly listed the
 *    player QB3, but the capture was older than the 72h freshness window, so the whole
 *    record was discarded and the depth-chart block never ran. A stale note saying
 *    "QB3" is still evidence he is not the starter; only its HEALTH claim decays.
 * 2. A zero-game player's projection is drawn entirely from position peers, so a
 *    third-stringer is handed the average NFL start -- above real starters.
 */
import assert from "node:assert/strict";
import { resolveAvailability, ROSTER_FRESH_MS, type RosterEvidence } from "../src/lib/nfl-dfs/availability";
import { optimizeNflLineups, type NflOptimizerPlayer, type NflOptimizerSettings }
  from "../src/app/dfs/nfl/nfl-optimizer";

const now = Date.parse("2026-09-20T22:58:00Z");
const evidence = (ageMs: number, depth: number | null, status = "Active"): RosterEvidence => ({
  team: "KC", position: "QB", fetchedAt: new Date(now - ageMs).toISOString(),
  sleeper: { team: "KC", position: "QB", depth_chart_order: depth, status },
});

// --- 1. Staleness ------------------------------------------------------------------
const fresh = resolveAvailability(evidence(3600e3, 3), "KC", "QB", now);
assert.match(fresh.blockedReason ?? "", /Listed QB3/);
assert.equal(fresh.fresh, true);

// The real failure: six days old, correct depth chart, previously unblocked.
const stale = resolveAvailability(evidence(6 * 864e5, 3), "KC", "QB", now);
assert.match(stale.blockedReason ?? "", /Listed QB3/, "a stale depth chart must still block");
assert.match(stale.blockedReason ?? "", /blocks still apply, clearances do not/);
assert.equal(stale.fresh, false, "it must not be reported as fresh evidence");
assert.equal(stale.role, "Backup \u00b7 QB3");

// A stale starter is blocked by nothing -- staleness adds blocks, it never removes them.
assert.equal(resolveAvailability(evidence(6 * 864e5, 1), "KC", "QB", now).blockedReason, null);
// A stale OUT still blocks: that is also a block, not a clearance.
assert.match(resolveAvailability(evidence(6 * 864e5, 1, "IR"), "KC", "QB", now).blockedReason ?? "", /Unavailable: IR/);
// Corrupt is not the same as old, and still resolves to nothing known.
assert.equal(resolveAvailability(evidence(-3600e3, 3), "KC", "QB", now).blockedReason, null);
assert.equal(resolveAvailability(undefined, "KC", "QB", now).blockedReason, null);
// The boundary is exactly the documented window.
assert.equal(resolveAvailability(evidence(ROSTER_FRESH_MS - 1, 1), "KC", "QB", now).fresh, true);
assert.equal(resolveAvailability(evidence(ROSTER_FRESH_MS + 1, 1), "KC", "QB", now).fresh, false);
console.log("Stale roster evidence: blocks apply, clearances do not, corrupt stays unknown.");

// --- 2. Zero-history players -------------------------------------------------------
const p = (id: number, salary: number, proj: number, games: number, team: string): NflOptimizerPlayer => ({
  id, dkPlayerId: id, captainDkPlayerId: id + 1000, captainSalary: Math.round(salary * 1.5),
  name: `P${id}`, position: id % 2 ? "WR" : "RB", team, opponent: team === "AAA" ? "BBB" : "AAA",
  gameKey: "AAA@BBB", salary, isOut: false, projectionStatus: games ? "historical" : "position_prior",
  historyGames: games, ourProj: proj, floorFpts: proj * 0.7, ceilingFpts: proj * 1.6, boomRate: 0.2,
  avgFptsDk: proj, fantasyprosProj: null, linestarProj: null, linestarOwnPct: null, customProj: null,
});

// Six established players, plus two zero-game bodies carrying a position-average number
// that outranks half the real pool -- the Nussmeier/Ott shape.
const pool = [
  p(10, 7000, 18, 34, "AAA"), p(11, 6000, 14, 34, "AAA"), p(12, 5000, 12, 20, "AAA"),
  p(20, 7000, 18, 34, "BBB"), p(21, 6000, 14, 34, "BBB"), p(22, 5000, 12, 20, "BBB"),
  p(90, 6000, 15.8, 0, "AAA"), p(91, 3600, 15.5, 0, "BBB"),
];

const base: NflOptimizerSettings = {
  format: "showdown", mode: "gpp", projectionSource: "our", allowDkFallback: false, nLineups: 3,
  minSalary: 0, minPlayerSalary: 0, requireObservedHistory: true, maxExposure: 1, minUnique: 1,
  stackPassCatchers: 0, bringBack: false, randomness: 0, lockedPlayerIds: [], excludedPlayerIds: [],
  minExposureByPlayer: {}, maxExposureByPlayer: {},
};

const gated = optimizeNflLineups(pool, base);
assert.ok(gated.lineups.length > 0, "the gated pool must still solve");
for (const l of gated.lineups) for (const id of l.playerIds) {
  assert.ok(id !== 90 && id !== 91, `zero-game player ${id} survived the history gate`);
}
assert.ok(gated.warnings.some((w) => /2 player\(s\) with too few games of their own were removed/.test(w)), "removal must be reported");

// Control: with the gate off they ARE used, so the gate is what changes the outcome.
const ungated = optimizeNflLineups(pool, { ...base, requireObservedHistory: false });
assert.ok(ungated.lineups.some((l) => l.playerIds.some((id) => id === 90 || id === 91)));

// An explicit lock still outranks the gate, exactly as it does the salary floor.
const locked = optimizeNflLineups(pool, { ...base, lockedPlayerIds: [90] });
assert.ok(locked.lineups.every((l) => l.playerIds.includes(90)));

// A one-game player with UNKNOWN team season context is still gated: without
// knowing how many games his team has completed, the flat MIN_OBSERVED_GAMES
// applies (the season-aware cap is tested in test-nfl-rookie-history-gate.ts).
assert.equal(optimizeNflLineups([...pool.slice(0, 6), p(92, 5000, 15, 1, "AAA")], base)
  .lineups.some((l) => l.playerIds.includes(92)), false);

console.log("Zero-history gate: removal, control, lock and one-game cases passed.");

// --- 3. Unlisted depth beside an identified QB1 -------------------------------------
// Jake Haener, NYG, 2026 week 2: Sleeper had no depth number for him, so he resolved
// to "QB role unresolved" and was NOT blocked, at a position-prior 13.3 points, while
// Jaxson Dart was listed QB1 in the same capture. A team with a QB1 makes an unlisted
// QB a backup; a team without one is left alone.
import { applyTeamQbContext, identifyTeamQb1s } from "../src/lib/nfl-dfs/availability";
const nyg = (depth: number | null) => resolveAvailability({ team: "NYG", position: "QB", fetchedAt: new Date(now - 3600e3).toISOString(),
  sleeper: { team: "NYG", position: "QB", depth_chart_order: depth, status: "Active" } }, "NYG", "QB", now);
const dart = nyg(1), haener = nyg(null), winston = nyg(2);
assert.equal(haener.blockedReason, null, "precondition: alone, an unlisted QB is not blocked");
const qb1s = identifyTeamQb1s([
  { team: "NYG", position: "QB", name: "Jaxson Dart", availability: dart },
  { team: "NYG", position: "QB", name: "Jake Haener", availability: haener },
  { team: "NYG", position: "QB", name: "Jameis Winston", availability: winston },
  { team: "NYG", position: "WR", name: "Malik Nabers", availability: resolveAvailability(undefined, "NYG", "WR", now) },
]);
assert.equal(qb1s.get("NYG")?.name, "Jaxson Dart");
const gatedHaener = applyTeamQbContext(haener, "QB", qb1s.get("NYG"));
assert.match(gatedHaener.blockedReason ?? "", /unresolved while Jaxson Dart is listed QB1/);
// The starter, the listed backup and a non-QB are untouched.
assert.equal(applyTeamQbContext(dart, "QB", qb1s.get("NYG")).blockedReason, null);
assert.match(applyTeamQbContext(winston, "QB", qb1s.get("NYG")).blockedReason ?? "", /Listed QB2/);
assert.equal(applyTeamQbContext(resolveAvailability(undefined, "NYG", "WR", now), "WR", qb1s.get("NYG")).blockedReason, null);
// No identified QB1 on the team: nothing is inferred, nobody is blocked.
assert.equal(applyTeamQbContext(haener, "QB", undefined).blockedReason, null);
// Two listed QB1s is conflicting evidence and blocks nobody.
assert.equal(identifyTeamQb1s([
  { team: "CLE", position: "QB", name: "A", availability: nyg(1) }, { team: "CLE", position: "QB", name: "B", availability: nyg(1) },
]).has("CLE"), false);
// A stale QB1 listing still blocks the unlisted teammate: blocks apply, clearances do not.
const staleDart = resolveAvailability({ team: "NYG", position: "QB", fetchedAt: new Date(now - 6 * 864e5).toISOString(),
  sleeper: { team: "NYG", position: "QB", depth_chart_order: 1, status: "Active" } }, "NYG", "QB", now);
assert.ok(applyTeamQbContext(haener, "QB", identifyTeamQb1s([{ team: "NYG", position: "QB", name: "Jaxson Dart", availability: staleDart }]).get("NYG")).blockedReason);
console.log("Unlisted-depth QB beside an identified QB1: blocked; starter, listed backup, non-QB and QB1-less teams untouched.");
