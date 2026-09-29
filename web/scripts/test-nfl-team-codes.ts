/**
 * Teams are compared by franchise across sources, never by string (2026-09-29
 * audit, finding 8). DraftKings writes WAS and LAR; nfl_teams writes WSH and
 * LAR; nflverse game ids write WAS and LA. A string comparison gave the week-3
 * classic "WAS: 0 games", would have found no favorite for IND@WAS, refused
 * every Washington calibrated candidate and every Rams defensive capture.
 */
import assert from "node:assert/strict";
import { nflTeamKey, sameNflTeam, showdownGame } from "../src/lib/nfl-dfs/availability";
import { calibratedRelease, readCalibratedProjection, type CalibratedRelease, type CalibrationSnapshot, type CalibrationTarget } from "../src/lib/nfl-dfs/calibrated-projection";
import { resolveDefensiveForecast, type DefensiveCapture, type DefensivePlayerInput } from "../src/lib/nfl-dfs/defensive-projection";

// One key per franchise.
assert.ok(sameNflTeam("WAS", "WSH")); assert.ok(sameNflTeam("LA", "LAR")); assert.ok(sameNflTeam("AZ", "ARI")); assert.ok(sameNflTeam("JAC", "JAX"));
assert.ok(!sameNflTeam("WAS", "LAR"));
assert.equal(nflTeamKey("WAS"), nflTeamKey("WSH"));

// The Showdown favorite: nfl_teams says WSH, the slate says WAS; the favorite comes back in the slate's code.
const rows = [
  { home: "KC", away: "BAL", home_ml: -150, away_ml: 130 },
  { home: "WSH", away: "IND", home_ml: -120, away_ml: 100 },
  { home: "WSH", away: "IND", home_ml: 110, away_ml: -130 },
];
const game = showdownGame(rows, ["IND", "WAS"])!;
assert.deepEqual(game, { home: "WAS", away: "IND", home_ml: -120, away_ml: 100 }, "earliest matching game, in DraftKings codes");
assert.equal(showdownGame(rows, ["IND", "NYG"]), null, "no game for teams that don't meet");
assert.deepEqual(showdownGame([{ home: "SF", away: "LA", home_ml: "-200", away_ml: "170" }], ["LAR", "SF"]), { home: "SF", away: "LAR", home_ml: -200, away_ml: 170 });

// A calibrated candidate written WSH applies to the DraftKings WAS player.
const release: CalibratedRelease = { ...calibratedRelease, positions: { ...calibratedRelease.positions, QB: { ...calibratedRelease.positions.QB, enabledForOptIn: true, shadowCandidate: true } } };
const target: CalibrationTarget = { ffPlayerId: 1, position: "QB", team: "WAS", opponent: "IND", gameInfo: "IND@WAS 10/04/2026 01:00PM ET" };
const snapshot: CalibrationSnapshot = { id: "7", playerId: 1, season: 2026, week: 4, capturedAt: "2026-10-03T10:00:00Z", kickoff: "2026-10-04T17:00:00Z",
  payload: { position: "QB", team: "WSH", opponent: "IND", source_study_digest: calibratedRelease.studyDigest, history_cutoff: [2026, 3], baseline: 20, p10: 8, p90: 30,
    candidate: { prediction: 21, p10: 9, median: 20, p90: 33, boom_probability: .2, recipe_digest: calibratedRelease.positions.QB.recipeDigest } } };
const read = readCalibratedProjection(snapshot, target, 2026, 4, Date.parse("2026-10-03T12:00:00Z"), release);
assert.equal(read.projection?.mean, 21, `WSH candidate applies to WAS: ${read.reason}`);
assert.equal(readCalibratedProjection(snapshot, { ...target, team: "NYG" }, 2026, 4, Date.parse("2026-10-03T12:00:00Z"), release).reason, "Candidate matchup mismatch.");

// A defensive capture for nflverse game 2026_04_LA_SF applies to the DraftKings LAR@SF player.
const input: DefensivePlayerInput = { dkPlayerId: 5, ffPlayerId: 50, gameKey: "LAR@SF", isOut: false, ourProj: 12, floorFpts: 8, medianFpts: 12,
  ceilingFpts: 18, boomRate: .2, statMeans: { carries: 15 } };
const capture = (gameId: string): DefensiveCapture => ({ runId: "c", baselineRunId: "b", capturedAt: "2026-10-03T12:00:00Z", artifactDigest: "d",
  candidate: { player_id: 50, dk_player_id: 5, game_id: gameId, kickoff: "2026-10-04T20:05:00Z",
    shadow: { status: "under_evaluation", reproduction: { passed: true },
      baseline: { mean: 12, p10: 8, p50: 12, p90: 18, boom: .2, stat_means: { carries: 15 } },
      candidate: { mean: 13, p10: 8, p50: 13, p90: 20, boom: .25, stat_means: { carries: 16 } } } } });
const settings = { mode: "experimental" as const, profile: "pfr-efficiency" as const };
assert.equal(resolveDefensiveForecast(input, "b", settings, capture("2026_04_LA_SF")).status, "applied", "LA in the game id is the Rams");
assert.equal(resolveDefensiveForecast(input, "b", settings, capture("2026_04_SF_LA")).reason, "game_identity_mismatch", "home/away still matter");
assert.equal(resolveDefensiveForecast({ ...input, gameKey: "SEA@WAS" }, "b", settings, capture("2026_04_SEA_WAS")).status, "applied");

console.log("Team codes: WAS/WSH and LA/LAR match by franchise in the Showdown favorite, calibrated candidates and defensive captures.");
