/** Shared browser/Node research engine. No server or filesystem dependencies. */
import type { NflDkSlate } from "./dk-salary-csv";
import { validateNflLineup, type NflLineup } from "./lineups";
import { NFL_PRNG_VERSION } from "./random";
import {
  NFL_SCORER_VERSION, independentNflAblation, prepareNflScenarios, scoreNflLineupDraws, summarizeNflDraws,
  type PreparedNflScenarios, type NflDistributionSummary,
} from "./scenarios";

export function rankNflCandidates(input: {
  slate: NflDkSlate; candidates: NflLineup[]; selection: unknown; evaluation: unknown;
  target: number; ablationSeed: number;
}) {
  const { slate, candidates, target, ablationSeed } = input;
  if (!candidates.length) throw new Error("At least one legal candidate required");
  if (!Number.isFinite(target)) throw new Error("Target must be finite");
  const selection = prepareNflScenarios(slate, input.selection);
  const evaluation = prepareNflScenarios(slate, input.evaluation);
  for (const field of ["snapshotId", "modelVersion", "source", "decisionAt", "inputsCapturedAt"] as const) {
    if (selection.metadata[field] !== evaluation.metadata[field]) throw new Error(`Selection/evaluation ${field} mismatch`);
  }
  if (selection.metadata.runId === evaluation.metadata.runId || selection.metadata.streamId === evaluation.metadata.streamId
      || selection.metadata.seed === evaluation.metadata.seed) throw new Error("Selection/evaluation require separate runs, streams, and seeds");
  const selectionIds = new Set(selection.scenarioIds);
  if (evaluation.scenarioIds.some((id) => selectionIds.has(id))) throw new Error("Selection/evaluation scenario IDs overlap");
  const independentSelection = independentNflAblation(selection, ablationSeed);
  const evaluationAblationSeed = (ablationSeed ^ 0x9e3779b9) >>> 0;
  const independentEvaluation = independentNflAblation(evaluation, evaluationAblationSeed);
  const keys = new Set<string>();
  const summarize = (lineup: NflLineup, bank: PreparedNflScenarios) => summarizeNflDraws(
    scoreNflLineupDraws(slate, lineup, bank), bank.weights, target,
    bank.metadata.sampling === "iid" && bank.dependence === "supplied-joint",
  );
  const playerP90 = Object.fromEntries(selection.playerIds.map((id) => [id,
    summarizeNflDraws(selection.scores[id], selection.weights, target).p90]));
  const rows = candidates.map((lineup) => {
    const { key, salary } = validateNflLineup(slate, lineup);
    if (keys.has(key)) throw new Error(`Duplicate canonical candidate: ${key}`);
    keys.add(key);
    return {
      key, salary, lineup,
      additivePlayerP90: lineup.reduce((sum, entry) => sum + playerP90[entry.playerId] * (entry.slot === "CPT" ? 1.5 : 1), 0),
      selection: { joint: summarize(lineup, selection), independent: summarize(lineup, independentSelection) },
      evaluation: { joint: summarize(lineup, evaluation), independent: summarize(lineup, independentEvaluation) },
    };
  });
  type Row = typeof rows[number];
  const policies: Array<[string, (row: Row) => number]> = [
    ["additive-player-p90", (row) => row.additivePlayerP90],
    ["independent-lineup-p90", (row) => row.selection.independent.p90],
    ["joint-lineup-p90", (row) => row.selection.joint.p90],
    ["joint-target-probability", (row) => row.selection.joint.targetProbability],
  ];
  const selected = policies.map(([policy, score]) => {
    const ranked = [...rows].sort((a, b) => score(b) - score(a) || (a.key < b.key ? -1 : a.key > b.key ? 1 : 0));
    const best = ranked[0];
    return { policy, candidateKey: best.key, selectionObjective: score(best),
      /** Compare every selected policy under the same joint evaluation distribution. */
      evaluation: best.evaluation.joint as NflDistributionSummary };
  });
  return {
    schemaVersion: 1,
    kind: "nfl-scenario-ranking-experiment",
    source: selection.metadata.source,
    limitations: [
      "Research scorer only: no contest field, payout, cash-hit, top-1% or Kelly claim.",
      "Supplied scenario coherence/calibration and independent streams require generator evidence; this boundary validates identity, stats and provenance.",
      "Monte Carlo intervals cover IID simulation noise only, not model uncertainty or selection bias.",
      "The candidate set bounds the search; selected lineups are not a jointly optimized portfolio.",
    ],
    manifest: {
      scorerVersion: NFL_SCORER_VERSION, prngVersion: NFL_PRNG_VERSION,
      target, candidateCount: rows.length, ablationSeed, evaluationAblationSeed,
      selection: selection.metadata, evaluation: evaluation.metadata,
      selectionDraws: selection.scenarioIds.length, evaluationDraws: evaluation.scenarioIds.length,
    },
    selected, candidates: rows,
  };
}

export type NflExperimentInput = Parameters<typeof rankNflCandidates>[0];
export type NflExperimentReport = ReturnType<typeof rankNflCandidates>;
