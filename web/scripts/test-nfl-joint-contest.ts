import assert from 'node:assert/strict';
import { analyzeNflContest, applyJointScenarioWeights } from '../src/lib/nfl-dfs/joint-contest';
import { emptyNflStats, type NflScenarioBank } from '../src/lib/nfl-dfs/scenarios';
import { analyzePartialGame, type SharedLeaderBank } from '../src/lib/nfl-dfs/shared-game-model';
import type { NflDkSlate, NflDkPlayer } from '../src/lib/nfl-dfs/dk-salary-csv';

const players: NflDkPlayer[] = Array.from({ length: 6 }, (_, i) => ({ dkPlayerId: i + 1, name: `P${i}`,
  position: 'WR', rosterPositions: ['FLEX'], teamAbbrev: i < 3 ? 'A' : 'B', opponent: i < 3 ? 'B' : 'A',
  homeAway: i < 3 ? 'home' : 'away', gameKey: 'B@A', gameInfo: null, salary: 5000, avgFptsDk: null,
  status: null, isOut: false, captain: { dkPlayerId: 100 + i, salary: 7500 } }));
const slate: NflDkSlate = { format: 'showdown', players, games: ['B@A'], teams: ['A', 'B'], warnings: [] };
const bank: NflScenarioBank = { schemaVersion: 1, runId: 'test', modelVersion: 'joint', snapshotId: 'test',
  decisionAt: '2026-10-08T22:00:00Z', inputsCapturedAt: '2026-10-08T21:00:00Z', source: 'synthetic', sampling: 'iid',
  seed: 1, streamId: 'test', scenarios: ['a', 'b'].map(id => ({ id, weight: 1,
    stats: Object.fromEntries(players.map(p => [p.dkPlayerId, { ...emptyNflStats(p.position), recYds: 100, receptions: 5 }])) })) };
const lineup = players.map((p, i) => ({ playerId: p.dkPlayerId, slot: i === 0 ? 'CPT' as const : 'FLEX' as const }));
const contest = analyzeNflContest(slate, bank, [{ id: 'one', lineup }, { id: 'two', lineup }], [20, 0], 5);
assert.equal(contest.entries[0].net.mean, 5);
assert.equal(contest.entries[1].net.mean, 5);
assert.equal(contest.entries[0].profitProbability, 1);
assert.throws(() => applyJointScenarioWeights(bank, ['b', 'a'], [.7, .3]), /identities/);
assert.equal(applyJointScenarioWeights(bank, ['a', 'b'], [.7, .3]).scenarios[0].weight, .7);
const partial: SharedLeaderBank = { schema_version: 1, scope: 'partial_offense_not_full_dfs',
  game: { game_id: 'test', home: 'A', away: 'B', kickoff: '2026-10-09T00:00:00Z' },
  decision_at: '2026-10-08T22:00:00Z', implementation_sha256: 'test', scenario_ids: ['a', 'b'],
  weights: [.9, .1], modeled_fields: ['rushYds', 'recYds', 'receptions', 'targets', 'carries'], missing_fields: ['passYds'],
  players: [{ identity: 'a', team: 'A', name: 'A', residual: false,
    draws: { rushYds: [0, 0], recYds: [0, 100], receptions: [0, 1], targets: [0, 1], carries: [0, 0] } }] };
assert.ok(Math.abs(analyzePartialGame(partial).players[0].productionPoints.mean - 1.4) < 1e-12);
assert.equal(analyzePartialGame(partial).players[0].receivingBonusProbability, .1);
console.log('Joint weighting, canonical contest scoring and duplicate tie payouts passed.');
