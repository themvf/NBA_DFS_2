import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { calibratedRelease, readCalibratedProjection, type CalibratedRelease, type CalibrationSnapshot, type CalibrationTarget } from "../src/lib/nfl-dfs/calibrated-projection";
import { optimizeNflLineups, type NflOptimizerPlayer, type NflOptimizerSettings } from "../src/app/dfs/nfl/nfl-optimizer";

// The release names the study the shadow job is pinned to (generated from it), never a stale one.
const shadowConfig = JSON.parse(readFileSync(new URL("../../artifacts/nfl_dfs_shadow_config.json", import.meta.url), "utf8"));
assert.equal(calibratedRelease.studyId, shadowConfig.study_run_id, "calibrated release must pin the shadow study");
assert.equal(calibratedRelease.studyDigest, shadowConfig.output_digest);
assert.equal(calibratedRelease.version, "nfl-dfs-calibrated-opt-in-v2");
// Under that study only DST qualifies; QB is disabled with the study's own status, not silently empty.
assert.equal(calibratedRelease.positions.DST.enabledForOptIn, true);
assert.equal(calibratedRelease.positions.QB.enabledForOptIn, false);

const now = Date.parse("2026-09-12T12:00:00Z");
const target: CalibrationTarget = { ffPlayerId: 1, position: "QB", team: "BUF", opponent: "MIA", gameInfo: "BUF@MIA 09/13/2026 01:00PM ET" };
const snapshot: CalibrationSnapshot = { id: "42", playerId: 1, season: 2026, week: 1, capturedAt: "2026-09-12T10:00:00Z", kickoff: "2026-09-13T17:00:00Z", payload: { position: "QB", team: "BUF", opponent: "MIA", source_study_digest: calibratedRelease.studyDigest, history_cutoff: [2025, 18], baseline: 20, p10: 8, p90: 30, candidate: { prediction: 24, p10: 10, median: 22, p90: 37, boom_probability: .3, recipe_digest: calibratedRelease.positions.QB.recipeDigest } } };
assert.match(readCalibratedProjection(snapshot, target, 2026, 1, now).reason, /release gate did not qualify QB in study 7ff4d404/);
// Reader logic against a release that qualifies QB.
const qbRelease: CalibratedRelease = { ...calibratedRelease, positions: { ...calibratedRelease.positions, QB: { ...calibratedRelease.positions.QB, enabledForOptIn: true, shadowCandidate: true } } };
const decoded = readCalibratedProjection(snapshot, target, 2026, 1, now, qbRelease).projection!;
assert.equal(decoded.mean, 24); assert.equal(decoded.p90, 37);
assert.notEqual(decoded.p90 - decoded.baselineP90, decoded.mean - decoded.baselineMean, "range is not a translated baseline");
for (const changed of [
  { ...snapshot, playerId: 2 }, { ...snapshot, week: 2 },
  { ...snapshot, capturedAt: "2026-09-01T00:00:00Z" }, { ...snapshot, capturedAt: "2026-09-14T00:00:00Z" },
  { ...snapshot, payload: { ...(snapshot.payload as object), history_cutoff: [2026, 1] } },
  { ...snapshot, payload: { ...(snapshot.payload as object), candidate: null } },
  { ...snapshot, payload: { ...(snapshot.payload as object), source_study_digest: "wrong" } },
]) assert.equal(readCalibratedProjection(changed, target, 2026, 1, now, qbRelease).projection, null);
for (const changed of [{ ...target, position: "WR" }, { ...target, opponent: "NYJ" }, { ...target, gameInfo: "BUF@MIA 09/13/2026 04:00PM ET" }, { ...target, gameInfo: null }]) assert.equal(readCalibratedProjection(snapshot, changed, 2026, 1, now, qbRelease).projection, null);
assert.equal(readCalibratedProjection(snapshot, target, 2026, 1, Date.parse(snapshot.kickoff), qbRelease).projection, null);

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
  assert.equal(baseline.slots.find(s => s.slot === "QB")!.player.team, "BUF");
  assert.throws(() => optimizeNflLineups(pool, { ...settings, mode, projectionSource: "calibrated" }), /mixed objective sources/);
}
assert.throws(() => optimizeNflLineups(pool.filter(p => ["BUF", "MIA"].includes(p.team)),
  { ...settings, format: "showdown", projectionSource: "calibrated" }), /mixed objective sources/);
assert.throws(() => optimizeNflLineups(pool.map(p => ({ ...p, calibrated: null })), { ...settings, projectionSource: "calibrated" }), /No qualified/);
const dk = optimizeNflLineups(pool, { ...settings, projectionSource: "dk_avg" }).lineups[0];
assert.ok(Math.abs(dk.floorFpts - 9 * 10 * .74) < 1e-8, "external source never inherits historical tails");
console.log("Calibrated source: identity/time/recipe gates, uniform-source build guard and external-source ranges passed.");
