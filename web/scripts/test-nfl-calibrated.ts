import assert from "node:assert/strict";
import { calibratedRelease, readCalibratedProjection, type CalibrationSnapshot, type CalibrationTarget } from "../src/lib/nfl-dfs/calibrated-projection";
import { optimizeNflLineups, type NflOptimizerPlayer, type NflOptimizerSettings } from "../src/app/dfs/nfl/nfl-optimizer";

const now = Date.parse("2026-09-12T12:00:00Z");
const target: CalibrationTarget = { ffPlayerId: 1, position: "QB", team: "BUF", opponent: "MIA", gameInfo: "BUF@MIA 09/13/2026 01:00PM ET" };
const snapshot: CalibrationSnapshot = { id: "42", playerId: 1, season: 2026, week: 1, capturedAt: "2026-09-12T10:00:00Z", kickoff: "2026-09-13T17:00:00Z", payload: { position: "QB", team: "BUF", opponent: "MIA", source_study_digest: calibratedRelease.studyDigest, history_cutoff: [2025, 18], baseline: 20, p10: 8, p90: 30, candidate: { prediction: 24, p10: 10, median: 22, p90: 37, boom_probability: .3, recipe_digest: calibratedRelease.positions.QB.recipeDigest } } };
const decoded = readCalibratedProjection(snapshot, target, 2026, 1, now).projection!;
assert.equal(decoded.mean, 24); assert.equal(decoded.p90, 37);
assert.notEqual(decoded.p90 - decoded.baselineP90, decoded.mean - decoded.baselineMean, "range is not a translated baseline");
for (const changed of [
  { ...snapshot, playerId: 2 }, { ...snapshot, week: 2 },
  { ...snapshot, capturedAt: "2026-09-01T00:00:00Z" }, { ...snapshot, capturedAt: "2026-09-14T00:00:00Z" },
  { ...snapshot, payload: { ...(snapshot.payload as object), history_cutoff: [2026, 1] } },
  { ...snapshot, payload: { ...(snapshot.payload as object), candidate: null } },
  { ...snapshot, payload: { ...(snapshot.payload as object), source_study_digest: "wrong" } },
]) assert.equal(readCalibratedProjection(changed, target, 2026, 1, now).projection, null);
for (const changed of [{ ...target, position: "WR" }, { ...target, opponent: "NYJ" }, { ...target, gameInfo: "BUF@MIA 09/13/2026 04:00PM ET" }, { ...target, gameInfo: null }]) assert.equal(readCalibratedProjection(snapshot, changed, 2026, 1, now).projection, null);
assert.equal(readCalibratedProjection(snapshot, target, 2026, 1, Date.parse(snapshot.kickoff)).projection, null);

let id = 0;
const pool: NflOptimizerPlayer[] = [];
for (const [team, opponent] of [["BUF", "MIA"], ["MIA", "BUF"], ["KC", "DEN"], ["DEN", "KC"]]) {
  for (const position of ["QB", "RB", "RB", "WR", "WR", "WR", "TE", "DST"] as const) {
    id++;
    const baseline = position === "QB" ? team === "BUF" ? 25 : 15 : 10;
    pool.push({ id, dkPlayerId: id, captainDkPlayerId: id + 1000, name: `${team} ${position} ${id}`, position, team, opponent, gameKey: [team, opponent].sort().join("@"), salary: 5000, captainSalary: 7500, isOut: false, projectionStatus: "historical", ourProj: baseline, floorFpts: baseline * .5, ceilingFpts: baseline * 1.5, boomRate: .1, avgFptsDk: 10, fantasyprosProj: null, linestarProj: null, linestarOwnPct: null, customProj: null,
      calibrated: position === "QB" && team === "MIA" ? { ...decoded, mean: 40, p10: 30, p50: 40, p90: 60 } : null });
  }
}
const settings: NflOptimizerSettings = { format: "classic", mode: "gpp", projectionSource: "our", allowDkFallback: false, nLineups: 1, minSalary: 0, maxExposure: 1, minUnique: 1, stackPassCatchers: 0, bringBack: false, randomness: 0, lockedPlayerIds: [], excludedPlayerIds: [], minExposureByPlayer: {}, maxExposureByPlayer: {} };
for (const mode of ["cash", "gpp"] as const) {
  const baseline = optimizeNflLineups(pool, { ...settings, mode }).lineups[0];
  const result = optimizeNflLineups(pool, { ...settings, mode, projectionSource: "calibrated" });
  const lineup = result.lineups[0];
  assert.equal(baseline.slots.find(s => s.slot === "QB")!.player.team, "BUF");
  assert.equal(lineup.slots.find(s => s.slot === "QB")!.player.team, "MIA", "source changes real optimizer choice");
  assert.equal(lineup.projectedFpts, 120);
  assert.equal(lineup.floorFpts, 70); assert.equal(lineup.ceilingFpts, 180);
  assert.equal(lineup.slots.filter(s => s.projectionSource === "our_fallback").length, 8);
  assert.equal(result.sourceCoverage.direct, 1);
  assert.equal(new Set(lineup.playerIds).size, 9);
}
const sd = optimizeNflLineups(pool.filter(p => ["BUF", "MIA"].includes(p.team)), { ...settings, format: "showdown", projectionSource: "calibrated" }).lineups[0];
const captain = sd.slots.find(s => s.slot === "CPT")!;
assert.equal(captain.projectionSource, "calibrated"); assert.equal(captain.projection, 60);
assert.equal(sd.ceilingFpts, 187.5); assert.equal(sd.floorFpts, 77.5); // 45 CPT + BUF QB 12.5 + four 5-point floors
assert.ok(sd.totalSalary <= 50000);
assert.throws(() => optimizeNflLineups(pool.map(p => ({ ...p, calibrated: null })), { ...settings, projectionSource: "calibrated" }), /No qualified/);
const dk = optimizeNflLineups(pool, { ...settings, projectionSource: "dk_avg" }).lineups[0];
assert.ok(Math.abs(dk.floorFpts - 9 * 10 * .74) < 1e-8, "external source never inherits historical tails");
console.log("Calibrated source: identity/time/recipe gates, real cash/GPP selection changes, Showdown CPT and source-consistent ranges passed.");
