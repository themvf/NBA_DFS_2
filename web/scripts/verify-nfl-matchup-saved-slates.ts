/** Mechanical release gate using three distinct archived pre-lock input snapshots. */
import assert from "node:assert/strict";
import { readFileSync, writeFileSync } from "node:fs";
import { createHash } from "node:crypto";
import { optimizeNflLineups, DEFAULT_NFL_PUNT_POLICY, type NflOptimizerPlayer, type NflOptimizerSettings } from "../src/app/dfs/nfl/nfl-optimizer";
import { evaluateNflOptimizerShadowIfAvailable, toScenarioLineup } from "../src/app/dfs/nfl/nfl-optimizer-shadow";
import { validateNflLineup } from "../src/lib/nfl-dfs/lineups";
import type { NflDkSlate } from "../src/lib/nfl-dfs/dk-salary-csv";
const [source, target] = process.argv.slice(2);
const text = readFileSync(source, "utf8"), saved = JSON.parse(text);
assert.equal(new Set(saved.map((row: any) => row.upload.upload_id)).size, 3);
const results = [];
for (const row of saved) {
  const salary = new Map<number, any>(row.salary.map((p: any) => [p.dk_player_id, p]));
  const players: NflOptimizerPlayer[] = row.run.input_snapshot.map((p: any) => ({ ...p,
    rosterPositions: salary.get(p.dkPlayerId)?.roster_positions, floorFpts: p.floor, ceilingFpts: p.ceiling,
    avgFptsDk: p.dkAvg, fantasyprosProj: p.fantasypros, linestarProj: p.linestar, linestarOwnPct: p.ownership,
    customProj: p.custom, historyGames: null, availabilityState: p.availability?.fresh ? "current" : "stale" }));
  const slate: NflDkSlate = { format: row.upload.format, games: row.upload.games, teams: row.upload.teams, warnings: [],
    players: row.salary.map((p: any) => ({ dkPlayerId: p.dk_player_id, name: p.name, position: p.position,
      rosterPositions: p.roster_positions, teamAbbrev: p.team, opponent: p.opponent, homeAway: null, gameKey: p.game_key,
      gameInfo: p.game_info, salary: p.salary, avgFptsDk: p.avg_fpts_dk, status: p.dk_status, isOut: p.is_out,
      captain: p.captain_dk_player_id === null ? null : { dkPlayerId: p.captain_dk_player_id, salary: p.captain_salary } })) };
  const settings: NflOptimizerSettings = { ...row.run.settings, nLineups: 20, minSalary: 45000,
    maxExposure: .6, minUnique: 2, stackPassCatchers: 1, bringBack: true, randomness: .08,
    puntPolicy: { ...DEFAULT_NFL_PUNT_POLICY }, puntOverrides: [] };
  const baseline = optimizeNflLineups(players, settings);
  const before = JSON.stringify({ players, baseline });
  const optional = evaluateNflOptimizerShadowIfAvailable({ slate, baseline, candidates: baseline, settings,
    contest: { id: "regression-only", platform: "draftkings", scoringVersion: "nfl-dk-scenario-v1", slateId: row.upload.upload_id,
      format: slate.format, mode: "multi_entry", entryCount: 20, maxEntriesPerUser: 150, fieldSize: null, entryFee: null, payouts: null,
      tieRule: "split_occupied_prizes", decisionAt: "2026-09-20T12:00:00Z", lockAt: "2026-09-20T17:00:00Z", lateSwap: true, ownershipCapability: "missing" },
    selection: null, evaluation: null, target: 180 });
  assert.equal(optional.status, "unavailable");
  assert.strictEqual(optional.baseline, baseline);
  assert.equal(JSON.stringify({ players, baseline }), before, "optional research mutated production results");
  const legal = baseline.lineups.map((lineup) => validateNflLineup(slate, toScenarioLineup(lineup)));
  // validateNflLineup throws on illegal construction and returns key/salary only.
  assert.equal(new Set(legal.map((lineup) => lineup.key)).size, legal.length);
  const fingerprint = createHash("sha256").update(JSON.stringify(baseline.lineups)).digest("hex");
  results.push({ uploadId: row.upload.upload_id, savedRunId: row.run.run_id, frozenBeforeLock: true,
    format: row.upload.format, players: players.length, generated: baseline.lineups.length, requested: settings.nLineups,
    legal: legal.length, productionUnchanged: true, missingResearchFallback: optional.status, fingerprint,
    warnings: baseline.warnings, limitation: "Archived snapshots predate own-history/depth evidence fields; unsupported cheap-player roles fail closed. Mechanical gate, not a performance holdout." });
  console.log(JSON.stringify(results.at(-1)));
}
writeFileSync(target, JSON.stringify({ version: "nfl-matchup-three-slate-gate-v1", inputDigest: createHash("sha256").update(text).digest("hex"),
  passed: true, status: "mechanical_regression_only", noProspectivePerformanceClaim: true, results }, null, 2));
