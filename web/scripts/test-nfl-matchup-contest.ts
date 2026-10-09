import assert from "node:assert/strict";
import { evaluateNflMatchupPortfolio, scoreNflContestOutcome, type NflContestConfig } from "../src/lib/nfl-dfs/matchup-contest";
import { generateNflCandidates } from "../src/lib/nfl-dfs/lineups";
import { nflDemoBank, nflDemoSlate } from "../src/lib/nfl-dfs/synthetic";
import { prepareNflScenarios } from "../src/lib/nfl-dfs/scenarios";
import type { NflMarginalScoreBank } from "../src/lib/nfl-dfs/marginal-scenario-bank";

// Two of our entries tie two rivals for ranks 1--4: each receives (100+50+20+0)/4.
const ties = scoreNflContestOutcome([200, 200], [{ score: 200, multiplicity: 2 }, { score: 190, multiplicity: 1 }], [100, 50, 20], 5);
assert.equal(ties.grossPayout, 85);
assert.equal(ties.entries[0].tied, 4);
assert.equal(ties.firstOrFirstTie, true);
assert.throws(() => scoreNflContestOutcome([200], [], [100], 2), /field size/);
assert.equal(scoreNflContestOutcome([90], [{ score: 100, multiplicity: 10 }], [100], 11).grossPayout, 0);

const slate = nflDemoSlate("showdown");
const candidates = generateNflCandidates(slate, { count: 15, seed: 81 }).lineups;
const selection = { ...nflDemoBank(slate, 42, 100, "selection"), source: "model" as const };
const evaluation = { ...nflDemoBank(slate, 43, 100, "evaluation"), source: "model" as const };
const config: NflContestConfig = { id: "test", platform: "draftkings", scoringVersion: "nfl-dk-scenario-v1", slateId: "test-slate", format: "showdown",
  mode: "three_entry", entryCount: 3, maxEntriesPerUser: 3, fieldSize: null, entryFee: null, payouts: null,
  tieRule: "split_occupied_prizes", decisionAt: selection.decisionAt, lockAt: "2030-01-01T00:00:00Z", lateSwap: false, ownershipCapability: "missing" };
const input = { slate, candidates, baseline: candidates.slice(0, 3), selection, evaluation, contest: config, target: 100, rules: { maxPairwiseOverlap: 6 } };
const report = evaluateNflMatchupPortfolio(input);
assert.equal(report.capability, "construction_only");
assert.equal(report.status, "shadow_comparison");
assert.equal(report.selected.length, 3);
assert.equal(report.challenger?.contest, null);
assert.equal(report.productionChanged, false);
assert.deepEqual(evaluateNflMatchupPortfolio(input), report);
assert.throws(() => evaluateNflMatchupPortfolio({ ...input, evaluation: selection }), /Separate/);
assert.throws(() => evaluateNflMatchupPortfolio({ ...input, evaluation: { ...evaluation, snapshotId: "different" } }), /snapshotId/);
assert.throws(() => evaluateNflMatchupPortfolio({ ...input, contest: { ...config, mode: "single_entry" } }), /Single-entry/);
assert.throws(() => evaluateNflMatchupPortfolio({ ...input, candidates: [...candidates, candidates[0]] }), /Duplicate/);
assert.throws(() => evaluateNflMatchupPortfolio({ ...input, baseline: candidates.slice(0, 2) }), /equal-entry-count/);
assert.throws(() => evaluateNflMatchupPortfolio({ ...input, rules: { maxPairwiseOverlap: 6, excludedPlayerIds: [candidates[0][0].playerId] } }), /Baseline/);

const contestInput = { ...input, contest: { ...config, fieldSize: 10, entryFee: 10, payouts: [60, 30, 10], ownershipCapability: "validated" as const },
  field: { modelVersion: "fixture-only", snapshotId: "fixture-field", capturedAt: selection.inputsCapturedAt, qualification: "validated" as const, qualificationId: "fixture-q", entries: [{ lineup: candidates[3], multiplicity: 7 }] } };
const withField = evaluateNflMatchupPortfolio(contestInput);
assert.equal(withField.capability, "validated_field_conditional");
assert.equal(withField.challenger?.contest?.entryCost, 30);
assert.ok(withField.challenger!.contest!.expectedGrossPayout >= 0);
assert.ok(withField.challenger!.contest!.expectedGrossPayout <= 100);
assert.throws(() => evaluateNflMatchupPortfolio({ ...contestInput, field: { ...contestInput.field, capturedAt: "2031-01-01T00:00:00Z" } }), /after the cutoff/);
assert.throws(() => evaluateNflMatchupPortfolio({ ...contestInput, field: { ...contestInput.field, entries: [{ lineup: candidates[3], multiplicity: 8 }] } }), /rivals/);

const marginal = (bank: typeof selection): NflMarginalScoreBank => {
  const prepared = prepareNflScenarios(slate, bank);
  return { schemaVersion: "nfl-marginal-score-bank-v1", metadata: prepared.metadata, scenarioIds: prepared.scenarioIds, scores: prepared.scores,
    provenance: { generator: "fixture-only", sourceManifestHash: "fixture-hash", productionRunId: "fixture-run", marginalAuditPassed: true, historyMode: "current_source_replay", maximumMeanDifference: 0 } };
};
const fallback = evaluateNflMatchupPortfolio({ ...contestInput, selection: marginal(selection), evaluation: marginal(evaluation) });
assert.equal(fallback.capability, "construction_only", "Independent fallback may never claim field/ROI metrics.");
assert.equal(fallback.challenger?.contest, null);
assert.equal(fallback.dependence, "independent-ablation");
assert.throws(() => evaluateNflMatchupPortfolio({ ...input, selection: { ...marginal(selection), provenance: { ...marginal(selection).provenance, marginalAuditPassed: false } }, evaluation: marginal(evaluation) }), /provenance/);
console.log("NFL matchup contest: ties, portfolio accounting, independent streams, legality, field capability and honest marginal fallback passed.");
