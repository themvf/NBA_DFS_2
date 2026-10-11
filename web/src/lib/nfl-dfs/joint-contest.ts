import type { NflDkSlate } from './dk-salary-csv';
import type { NflLineup } from './lineups';
import { prepareNflScenarios, scoreNflLineupDraws, summarizeNflDraws, type NflScenarioBank } from './scenarios';
import { normalizedScenarioWeights } from './shared-game-model';

export function applyJointScenarioWeights(bank: NflScenarioBank, scenarioIds: string[], weights: number[]) {
  if (bank.scenarios.length !== scenarioIds.length || bank.scenarios.some((s, i) => s.id !== scenarioIds[i])) {
    throw new Error('Joint banks must share exact scenario identities and order');
  }
  const normalized = normalizedScenarioWeights(scenarioIds.length, weights);
  if (normalized.some(w => w === 0)) throw new Error('Complete DFS scorer requires positive scenario support');
  return { ...bank, sampling: 'weighted' as const, scenarios: bank.scenarios.map((s, i) => ({ ...s, weight: normalized[i] })) };
}

export function analyzeNflContest(slate: NflDkSlate, input: NflScenarioBank,
  entrants: Array<{ id: string; lineup: NflLineup }>, payouts: number[], entryFee: number) {
  if (!entrants.length || new Set(entrants.map(e => e.id)).size !== entrants.length || entrants.some(e => !e.id)) {
    throw new Error('Unique contest entries required');
  }
  if (!Number.isFinite(entryFee) || entryFee < 0 || payouts.length > entrants.length ||
    payouts.some(p => !Number.isFinite(p) || p < 0) || payouts.some((p, i) => i > 0 && p > payouts[i - 1])) {
    throw new Error('Invalid contest payout schedule');
  }
  const bank = prepareNflScenarios(slate, input);
  const scores = entrants.map(e => scoreNflLineupDraws(slate, e.lineup, bank));
  const net = entrants.map(() => Array(bank.weights.length).fill(-entryFee) as number[]);
  for (let draw = 0; draw < bank.weights.length; draw++) {
    const sorted = entrants.map((_, i) => i).sort((a, b) => scores[b][draw] - scores[a][draw]);
    for (let rank = 0; rank < sorted.length;) {
      let end = rank + 1;
      while (end < sorted.length && scores[sorted[end]][draw] === scores[sorted[rank]][draw]) end++;
      let pool = 0;
      for (let r = rank; r < end; r++) pool += payouts[r] ?? 0;
      for (let r = rank; r < end; r++) net[sorted[r]][draw] += pool / (end - rank);
      rank = end;
    }
  }
  return { authority: 'exploratory_supplied_field_not_validated_ownership',
    fieldSize: entrants.length, entryFee, draws: bank.weights.length,
    entries: entrants.map((e, i) => ({ id: e.id, net: summarizeNflDraws(net[i], bank.weights, 0),
      profitProbability: net[i].reduce((s, value, j) => s + (value > 0 ? bank.weights[j] : 0), 0),
      points: summarizeNflDraws(scores[i], bank.weights, 0) })),
    limits: ['Supplied field determines ownership and duplication; no fitted contest field is implied.',
      'Tied entries share the payouts allocated to their occupied finishing positions.'] };
}
