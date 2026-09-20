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
assert.ok(gated.warnings.some((w) => /2 player\(s\) with fewer than 2 games/.test(w)), "removal must be reported");

// Control: with the gate off they ARE used, so the gate is what changes the outcome.
const ungated = optimizeNflLineups(pool, { ...base, requireObservedHistory: false });
assert.ok(ungated.lineups.some((l) => l.playerIds.some((id) => id === 90 || id === 91)));

// An explicit lock still outranks the gate, exactly as it does the salary floor.
const locked = optimizeNflLineups(pool, { ...base, lockedPlayerIds: [90] });
assert.ok(locked.lineups.every((l) => l.playerIds.includes(90)));

// A one-game player is still gated; the documented cliff is the zero-game group, but
// MIN_OBSERVED_GAMES is shared with opportunity redistribution and stays that one value.
assert.equal(optimizeNflLineups([...pool.slice(0, 6), p(92, 5000, 15, 1, "AAA")], base)
  .lineups.some((l) => l.playerIds.includes(92)), false);

console.log("Zero-history gate: removal, control, lock and one-game cases passed.");
