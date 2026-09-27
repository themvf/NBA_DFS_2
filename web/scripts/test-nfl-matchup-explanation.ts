import assert from 'node:assert/strict';
import { matchupExplanation } from '../src/lib/nfl-dfs/matchup-explanation';

const fallback = matchupExplanation({ baseline: { model_proj_fpts: 0, ceiling_fpts: 0 }, shadow: {
  status: 'not_applied', reason: 'ineligible_or_out', baseline: { mean: 20, p90: 30 }, candidate: { mean: 20, p90: 30 }, delta: 0,
}}, '2026-09-27T14:30:00Z', 'baseline');
assert.equal(fallback?.baseline, 0);
assert.equal(fallback?.candidate, 0);
assert.equal(fallback?.candidateP90, 0);
assert.equal(fallback?.opportunity, null);
const applied = matchupExplanation({ baseline: { model_proj_fpts: 20, ceiling_fpts: 30 }, shadow: {
  status: 'under_evaluation', baseline: { mean: 20, p90: 30, stat_means: { carries: 15 } },
  candidate: { mean: 21, p90: 33, p10: 4, p50: 18, boom: .3 }, delta: 1,
  ledger: [{ component: 'rushing_yards', before: 4, after: 4.2, features: { own_ybc: 2, opp_ybc: 1.5 } }],
}}, '2026-09-27T14:30:00Z', 'baseline');
assert.equal(applied?.delta, 1);
assert.equal(applied?.efficiencyAfter, 4.2);
assert.equal(applied?.opportunity, 15);
assert.equal(applied?.evidence.length, 2);
assert.equal(applied?.candidateBoom, .3);
console.log('Matchup explanation checks passed: applied ledger and saved-zero fallback.');
