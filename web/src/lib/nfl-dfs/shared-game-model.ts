/** Shared draw analysis. Partial yardage scoring never becomes full DFS points. */
import type { NflDkSlate } from './dk-salary-csv';
import type { NflLineup } from './lineups';
import { scoreNflOffense } from './scoring';
import { prepareNflScenarios, scoreNflLineupDraws, summarizeNflDraws, type NflScenarioBank } from './scenarios';

type PartialPlayer = { identity: string; name: string; team: string; residual: boolean;
  draws: Record<'rushYds' | 'recYds' | 'receptions' | 'targets' | 'carries', number[]> };
export type SharedLeaderBank = { schema_version: number; scope: string;
  game: { game_id: string; kickoff: string; home: string; away: string };
  decision_at: string; implementation_sha256: string; scenario_ids: string[];
  weights?: number[];
  modeled_fields: string[]; missing_fields: string[]; players: PartialPlayer[] };
const partialFields = ['rushYds', 'recYds', 'receptions', 'targets', 'carries'] as const;

export function correlation(a: number[], b: number[], suppliedWeights?: number[]) {
  if (a.length !== b.length || !a.length || [...a, ...b].some(v => !Number.isFinite(v))) throw new Error('Invalid paired draws');
  const weights = normalizedScenarioWeights(a.length, suppliedWeights);
  const ma = a.reduce((s, v, i) => s + v * weights[i], 0), mb = b.reduce((s, v, i) => s + v * weights[i], 0);
  const va = a.reduce((s, v, i) => s + weights[i] * (v - ma) ** 2, 0), vb = b.reduce((s, v, i) => s + weights[i] * (v - mb) ** 2, 0);
  return va && vb ? a.reduce((s, v, i) => s + weights[i] * (v - ma) * (b[i] - mb), 0) / Math.sqrt(va * vb) : null;
}

export function normalizedScenarioWeights(count: number, supplied?: number[]) {
  const weights = supplied ?? Array(count).fill(1);
  if (weights.length !== count || weights.some(v => !Number.isFinite(v) || v < 0)) throw new Error('Invalid scenario weights');
  const sum = weights.reduce((a, b) => a + b, 0);
  if (!(sum > 0)) throw new Error('Empty scenario weight support');
  return weights.map(v => v / sum);
}

export function analyzePartialGame(bank: SharedLeaderBank) {
  if (bank.schema_version !== 1 || bank.scope !== 'partial_offense_not_full_dfs' || !bank.implementation_sha256 ||
      !Number.isFinite(Date.parse(bank.decision_at)) || !Number.isFinite(Date.parse(bank.game.kickoff)) ||
      Date.parse(bank.decision_at) >= Date.parse(bank.game.kickoff)) throw new Error('Invalid shared bank boundary');
  const count = bank.scenario_ids.length;
  if (count < 2 || new Set(bank.scenario_ids).size !== count || bank.scenario_ids.some(id => !id)) throw new Error('Invalid scenario identities');
  if (!bank.players.length || new Set(bank.players.map(p => p.identity)).size !== bank.players.length) throw new Error('Invalid player identities');
  if (partialFields.some(k => !bank.modeled_fields.includes(k)) || !bank.missing_fields.length) throw new Error('Partial coverage must be explicit');
  for (const p of bank.players) {
    if (![bank.game.home, bank.game.away].includes(p.team)) throw new Error('Player outside game');
    for (const k of partialFields) {
      const values = p.draws[k];
      if (!Array.isArray(values) || values.length !== count || values.some(v => !Number.isSafeInteger(v) ||
          (!k.endsWith('Yds') && v < 0))) throw new Error(`Missing/misaligned ${k} draws`);
    }
    if (p.draws.receptions.some((n, i) => n > p.draws.targets[i])) throw new Error('Catches exceed targets');
  }
  const weights = normalizedScenarioWeights(count, bank.weights);
  const scores = new Map(bank.players.map(p => [p.identity, p.draws.rushYds.map((rushYds, i) =>
    scoreNflOffense({ rushYds, recYds: p.draws.recYds[i], receptions: p.draws.receptions[i] }))]));
  const players = bank.players.filter(p => !p.residual).map(p => {
    const values = scores.get(p.identity)!;
    return { identity: p.identity, name: p.name, team: p.team,
      productionPoints: summarizeNflDraws(values, weights, 20),
      rushBonusProbability: p.draws.rushYds.reduce((s, v, i) => s + (v >= 100 ? weights[i] : 0), 0),
      receivingBonusProbability: p.draws.recYds.reduce((s, v, i) => s + (v >= 100 ? weights[i] : 0), 0),
      fullDfsPoints: null };
  }).sort((a, b) => b.productionPoints.mean - a.productionPoints.mean);
  const relevant = players.slice(0, 12);
  const correlations = relevant.flatMap((p, i) => relevant.slice(i + 1).map(q => ({
    first: p.name, second: q.name, sameTeam: p.team === q.team,
    correlation: correlation(scores.get(p.identity)!, scores.get(q.identity)!, weights) })));
  return { version: 'nfl-shared-game-analysis-v1', authority: 'exploratory', game: bank.game,
    decisionAt: bank.decision_at, implementationSha256: bank.implementation_sha256, draws: count,
    scoringScope: 'Receptions + rushing/receiving yards + their separate DraftKings bonuses',
    modeledFields: bank.modeled_fields, missingFields: bank.missing_fields,
    players, correlations, fullDfsReady: false, optimizerEnabled: false,
    limits: ['These are partial production points, not full fantasy projections or a lower bound.',
      'Passing, touchdowns, turnovers and other missing fields are excluded, not modeled as zero.',
      'Correlations describe this simulator; they have not been validated against real games.'] };
}

/** Complete existing event-ledger banks: same draws for DFS and slate-field leaders. */
export function analyzeCompleteDfs(slate: NflDkSlate, input: NflScenarioBank, lineups: NflLineup[] = []) {
  const bank = prepareNflScenarios(slate, input);
  if (slate.players.some(p => !p.gameKey || !slate.games.includes(p.gameKey))) throw new Error('Missing canonical slate game assignment');
  const players = slate.players.map(p => ({ playerId: p.dkPlayerId, name: p.name, team: p.teamAbbrev,
    position: p.position, salary: p.salary,
    points: summarizeNflDraws(bank.scores[p.dkPlayerId], bank.weights, p.salary > 0 ? 3 * p.salary / 1000 : 20),
    targetDefinition: p.salary > 0 ? '3x salary points' : '20 points; salary unavailable',
    pointsPerThousand: p.salary > 0 ? bank.scores[p.dkPlayerId].reduce((s, v, i) => s + v * bank.weights[i], 0) / (p.salary / 1000) : null }));
  const leaders: Array<{ game: string; metric: string; fieldScope: string; players: Array<{ playerId: number; share: number }> }> = [];
  for (const game of slate.games) {
    const pool = slate.players.filter(p => p.gameKey === game && p.position !== 'DST' && p.position !== 'K');
    if (!pool.length) continue;
    for (const metric of ['rushYds', 'receptions', 'recYds', 'totalYards']) {
      const credit = new Map(pool.map(p => [p.dkPlayerId, 0]));
      input.scenarios.forEach((draw, i) => {
        const values = pool.map(p => { const s = draw.stats[p.dkPlayerId]; return metric === 'totalYards' ? s.rushYds! + s.recYds! : s[metric as 'rushYds' | 'recYds' | 'receptions']!; });
        const best = Math.max(...values), winners = pool.filter((_, j) => values[j] === best);
        for (const p of winners) credit.set(p.dkPlayerId, credit.get(p.dkPlayerId)! + bank.weights[i] / winners.length);
      });
      leaders.push({ game, metric, fieldScope: 'modeled_slate_players_only_not_full_game',
        players: [...credit].map(([playerId, share]) => ({ playerId, share })).sort((a, b) => b.share - a.share) });
    }
  }
  return { version: 'nfl-shared-dfs-analysis-v1', authority: 'exploratory', optimizerEnabled: false,
    provenance: bank.metadata, draws: bank.scenarioIds.length, dependence: bank.dependence,
    players, leaders, lineups: lineups.map(lineup => ({ lineup,
      points: summarizeNflDraws(scoreNflLineupDraws(slate, lineup, bank), bank.weights, slate.format === 'showdown' ? 150 : 200) })),
    limits: ['Full scoring uses the existing canonical DraftKings scorer; accuracy depends on the supplied bank.',
      'Slate-player leader shares omit contributors outside that pool and are not sportsbook full-field probabilities.',
      'Salary multiples and lineup thresholds are descriptive, not contest-winning or profitability probabilities.'] };
}
