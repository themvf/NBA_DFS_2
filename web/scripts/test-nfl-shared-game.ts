import assert from 'node:assert/strict';
import { analyzePartialGame, analyzeCompleteDfs, correlation, type SharedLeaderBank } from '../src/lib/nfl-dfs/shared-game-model';
import { emptyNflStats, type NflScenarioBank } from '../src/lib/nfl-dfs/scenarios';
import type { NflDkSlate, NflDkPlayer } from '../src/lib/nfl-dfs/dk-salary-csv';

const partial: SharedLeaderBank = { schema_version: 1, scope: 'partial_offense_not_full_dfs',
  game: { game_id: 'test', home: 'A', away: 'B', kickoff: '2026-10-09T00:15:00Z' },
  decision_at: '2026-10-08T22:00:00Z', implementation_sha256: 'test-fixture',
  scenario_ids: ['one', 'two'], modeled_fields: ['rushYds', 'recYds', 'receptions', 'targets', 'carries'],
  missing_fields: ['passYds', 'passTds', 'rushTds', 'recTds', 'interceptions'], players: [
    { identity: 'a', name: 'A', team: 'A', residual: false, draws: { rushYds: [90, 110], recYds: [0, 0], receptions: [0, 0], targets: [0, 0], carries: [10, 10] } },
    { identity: 'b', name: 'B', team: 'B', residual: false, draws: { rushYds: [0, 0], recYds: [110, 90], receptions: [5, 5], targets: [6, 6], carries: [0, 0] } } ] };
const report = analyzePartialGame(partial);
assert.equal(report.fullDfsReady, false);
assert.equal(report.players.find(p => p.identity === 'a')!.productionPoints.mean, 11.5); // E[bonus]=1.5; bonus(mean)=3 is WRONG.
assert.equal(report.players[0].fullDfsPoints, null);
assert.equal(correlation([1, 2, 3], [3, 2, 1]), -1);
assert.equal(correlation([1, 1], [2, 3]), null);
const broken = structuredClone(partial); broken.players[0].draws.targets = [1];
assert.throws(() => analyzePartialGame(broken), /misaligned/);
const later = structuredClone(partial); later.decision_at = later.game.kickoff;
assert.throws(() => analyzePartialGame(later), /boundary/);

const players: NflDkPlayer[] = Array.from({ length: 6 }, (_, i) => ({ dkPlayerId: i + 1, name: `P${i+1}`,
  position: i === 0 ? 'QB' : 'WR', rosterPositions: ['FLEX'], teamAbbrev: i < 3 ? 'A' : 'B', opponent: i < 3 ? 'B' : 'A',
  homeAway: i < 3 ? 'home' : 'away', gameKey: 'B@A', gameInfo: null, salary: 5000, avgFptsDk: null,
  status: null, isOut: false, captain: { dkPlayerId: 100+i, salary: 7500 } }));
const slate: NflDkSlate = { format: 'showdown', players, games: ['B@A'], teams: ['A', 'B'], warnings: [] };
const bank: NflScenarioBank = { schemaVersion: 1, runId: 'fixture', modelVersion: 'test', snapshotId: 'frozen',
  decisionAt: partial.decision_at, inputsCapturedAt: partial.decision_at, source: 'synthetic', sampling: 'iid', seed: 1,
  streamId: 'evaluation-fixture', scenarios: ['one', 'two'].map((id, i) => ({ id, weight: 1,
    stats: Object.fromEntries(players.map(p => [p.dkPlayerId, { ...emptyNflStats(p.position),
      passYds: p.position === 'QB' ? (i === 0 ? 300 : 100) : 0,
      recYds: p.dkPlayerId === 2 ? (i === 0 ? 100 : 20) : 0,
      receptions: p.dkPlayerId === 2 ? 5 : 0 }])) })) };
const lineup = players.map((p, i) => ({ playerId: p.dkPlayerId, slot: i === 0 ? 'CPT' as const : 'FLEX' as const }));
const full = analyzeCompleteDfs(slate, bank, [lineup]);
assert.equal(full.players[0].points.mean, 9.5); // (15 + 4)/2
assert.equal(full.lineups[0].points.mean, 26.75); // same draw scores + CPT, not sum of percentiles
for (const leader of full.leaders) assert.ok(Math.abs(leader.players.reduce((s, p) => s + p.share, 0) - 1) < 1e-12);
assert.equal(full.leaders[0].fieldScope, 'modeled_slate_players_only_not_full_game');
const missing = structuredClone(bank); delete missing.scenarios[0].stats['1'].passTds;
assert.throws(() => analyzeCompleteDfs(slate, missing), /exactly/);
const illegal = lineup.map(p => ({ ...p, playerId: 1 }));
assert.throws(() => analyzeCompleteDfs(slate, bank, [illegal]));
console.log('Shared game / DFS scoring, bonus, identity, correlation, field-scope and legal-lineup checks passed.');
