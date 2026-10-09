/** A clearly separate fallback: actual model score draws, independently sampled.
 * It cannot certify football-event coherence or support contest/ROI claims. */
import type { NflDkSlate } from "./dk-salary-csv";
import { nflPoolIndex } from "./lineups";
import type { NflScenarioBank, PreparedNflScenarios } from "./scenarios";

export type NflMarginalScoreBank = {
  schemaVersion: "nfl-marginal-score-bank-v1";
  metadata: Omit<NflScenarioBank, "scenarios">;
  scenarioIds: string[];
  scores: Record<string, number[]>;
  provenance: {
    generator: string; sourceManifestHash: string;
    productionRunId: string; marginalAuditPassed: boolean;
    historyMode: "frozen_source_replay" | "current_source_replay";
    maximumMeanDifference: number;
  };
};

export function prepareNflMarginalScores(slate: NflDkSlate, input: NflMarginalScoreBank): PreparedNflScenarios {
  if (!input || input.schemaVersion !== "nfl-marginal-score-bank-v1") throw new Error("Unsupported marginal-score schema.");
  const bank = input.metadata;
  if (bank.schemaVersion !== 1 || bank.source !== "model" || bank.sampling !== "iid") throw new Error("Independent fallback requires IID model draws.");
  for (const key of ["runId", "modelVersion", "snapshotId", "streamId", "decisionAt", "inputsCapturedAt"] as const) if (!bank[key]) throw new Error(`Missing marginal ${key}.`);
  for (const timestamp of [bank.decisionAt, bank.inputsCapturedAt]) if (!/(Z|[+-]\d\d:\d\d)$/.test(timestamp) || !Number.isFinite(Date.parse(timestamp))) throw new Error("Marginal times need explicit timezones.");
  if (Date.parse(bank.inputsCapturedAt) > Date.parse(bank.decisionAt)) throw new Error("Marginal inputs arrived after cutoff.");
  if (!Number.isSafeInteger(bank.seed) || bank.seed < 0 || bank.seed > 0xffff_ffff) throw new Error("Invalid marginal seed.");
  const p = input.provenance;
  if (!p || !p.generator || !p.sourceManifestHash || !p.productionRunId || !p.marginalAuditPassed || !Number.isFinite(p.maximumMeanDifference) || p.maximumMeanDifference < 0
    || !["frozen_source_replay", "current_source_replay"].includes(p.historyMode)) throw new Error("Audited source/model marginal provenance required.");
  const ids = input.scenarioIds;
  if (!Array.isArray(ids) || ids.length < 2 || ids.length > 10000 || ids.some((id) => typeof id !== "string" || !id) || new Set(ids).size !== ids.length) throw new Error("Invalid marginal scenario IDs.");
  const playerIds = [...nflPoolIndex(slate).keys()].sort((a, b) => a - b);
  if (Object.keys(input.scores).length !== playerIds.length || Object.keys(input.scores).some((id) => !playerIds.includes(Number(id)) || String(Number(id)) !== id)) throw new Error("Marginal bank must exactly match its declared slate pool.");
  for (const id of playerIds) if (!Array.isArray(input.scores[id]) || input.scores[id].length !== ids.length || input.scores[id].some((score) => !Number.isFinite(score) || !Number.isSafeInteger(Math.round(score * 100)))) throw new Error(`Missing/invalid model scores for ${id}.`);
  return { metadata: bank, scenarioIds: ids, playerIds, weights: ids.map(() => 1 / ids.length),
    scores: input.scores, dependence: "independent-ablation" };
}
