import type { NflDkSlate, NflPosition } from "./dk-salary-csv";
import { nflPoolIndex, validateNflLineup, type NflLineup } from "./lineups";
import { nflRandom, nflShuffle } from "./random";
import { scoreNflStatLine, type NflStatLine } from "./scoring";

export const NFL_SCENARIO_SCHEMA = 1;
export const NFL_SCORER_VERSION = "nfl-dk-scenario-v1";
export const NFL_STAT_KEYS = {
  offense: ["passYds", "passTds", "interceptions", "rushYds", "rushTds", "recYds", "recTds", "receptions",
    "fumblesLost", "twoPointConversions", "returnTds", "offensiveFumbleRecoveryTds"],
  K: ["extraPointsMade", "fgMade0to39", "fgMade40to49", "fgMade50Plus"],
  DST: ["sacks", "dstInterceptions", "fumbleRecoveries", "safeties", "blockedKicks", "dstTds", "twoPointReturns", "pointsAllowed"],
} as const;

export type NflScenarioBank = {
  schemaVersion: 1;
  runId: string;
  modelVersion: string;
  snapshotId: string;
  decisionAt: string;
  inputsCapturedAt: string;
  source: "synthetic" | "model";
  sampling: "iid" | "weighted";
  seed: number;
  /** Explicit stream identity; selection and evaluation must be separate runs. */
  streamId: string;
  scenarios: Array<{ id: string; weight: number; stats: Record<string, NflStatLine> }>;
};

export type PreparedNflScenarios = {
  metadata: Omit<NflScenarioBank, "scenarios">;
  scenarioIds: string[];
  weights: number[];
  playerIds: number[];
  /** Player-major columns, always in the same scenario order. */
  scores: Record<string, number[]>;
  dependence: "supplied-joint" | "independent-ablation";
};

function object(value: unknown, label: string): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error(`${label} must be an object`);
  return value as Record<string, unknown>;
}
function text(value: unknown, label: string): string {
  if (typeof value !== "string" || !value.trim()) throw new Error(`${label} must be a nonempty string`);
  return value;
}

export function emptyNflStats(position: NflPosition): NflStatLine {
  const keys = position === "DST" || position === "K" ? NFL_STAT_KEYS[position] : NFL_STAT_KEYS.offense;
  return Object.fromEntries(keys.map((key) => [key, 0])) as NflStatLine;
}

/** Strict at the scenario boundary: omitted values must not become invented zeros. */
export function validateNflScenarioStats(position: NflPosition, input: unknown): NflStatLine {
  const values = object(input, "Stat line");
  const expected = Object.keys(emptyNflStats(position));
  if (Object.keys(values).length !== expected.length || Object.keys(values).some((k) => !expected.includes(k))) {
    throw new Error(`Stat line for ${position} must contain exactly: ${expected.join(", ")}`);
  }
  for (const key of expected) {
    const value = values[key];
    if (typeof value !== "number" || !Number.isSafeInteger(value)) throw new Error(`Invalid integer stat ${key}`);
    if (!key.endsWith("Yds") && value < 0) throw new Error(`Negative count stat ${key}`);
  }
  return values as NflStatLine;
}

/** Validates/scores supplied draws. Does not certify that their generator is calibrated or coherent. */
export function prepareNflScenarios(slate: NflDkSlate, input: unknown): PreparedNflScenarios {
  const bank = object(input, "Scenario bank");
  if (bank.schemaVersion !== NFL_SCENARIO_SCHEMA) throw new Error("Unsupported scenario schema");
  const index = nflPoolIndex(slate);
  if (!index.size) throw new Error("Empty NFL player pool");
  const playerIds = [...index.keys()].sort((a, b) => a - b);
  const runId = text(bank.runId, "runId");
  const modelVersion = text(bank.modelVersion, "modelVersion");
  const snapshotId = text(bank.snapshotId, "snapshotId");
  const streamId = text(bank.streamId, "streamId");
  const decisionAt = text(bank.decisionAt, "decisionAt");
  const inputsCapturedAt = text(bank.inputsCapturedAt, "inputsCapturedAt");
  for (const timestamp of [decisionAt, inputsCapturedAt]) {
    if (!/(Z|[+-]\d\d:\d\d)$/.test(timestamp) || !Number.isFinite(Date.parse(timestamp))) {
      throw new Error("Scenario timestamps require an explicit timezone and valid date");
    }
  }
  if (Date.parse(inputsCapturedAt) > Date.parse(decisionAt)) throw new Error("Inputs captured after decision cutoff");
  if (bank.source !== "synthetic" && bank.source !== "model") throw new Error("Unknown scenario source");
  if (bank.sampling !== "iid" && bank.sampling !== "weighted") throw new Error("Unknown sampling method");
  if (typeof bank.seed !== "number") throw new Error("Scenario seed must be a number");
  nflRandom(bank.seed);
  if (!Array.isArray(bank.scenarios) || bank.scenarios.length < 2) throw new Error("At least two scenarios required");
  const scores: Record<string, number[]> = Object.fromEntries(playerIds.map((id) => [id, []]));
  const scenarioIds: string[] = [];
  const ids = new Set<string>();
  const weights: number[] = [];
  for (const raw of bank.scenarios) {
    const scenario = object(raw, "Scenario");
    const id = text(scenario.id, "Scenario ID");
    if (ids.has(id)) throw new Error(`Duplicate scenario ID ${id}`);
    ids.add(id);
    scenarioIds.push(id);
    if (typeof scenario.weight !== "number" || !Number.isFinite(scenario.weight) || scenario.weight <= 0) {
      throw new Error("Scenario weights must be finite and positive");
    }
    weights.push(scenario.weight);
    const stats = object(scenario.stats, "Scenario stats");
    if (Object.keys(stats).length !== playerIds.length || Object.keys(stats).some((id) => !index.has(Number(id)) || String(Number(id)) !== id)) {
      throw new Error("Every scenario must match the complete underlying player pool exactly");
    }
    for (const playerId of playerIds) {
      const player = index.get(playerId)!;
      const statLine = validateNflScenarioStats(player.position, stats[playerId]);
      const score = scoreNflStatLine(player.position, statLine);
      const hundredths = Math.round(score * 100);
      if (!Number.isSafeInteger(hundredths)) throw new Error("Fantasy score exceeds exact scoring range");
      scores[playerId].push(hundredths / 100);
    }
  }
  const totalWeight = weights.reduce((a, b) => a + b, 0);
  if (!Number.isFinite(totalWeight)) throw new Error("Scenario weight total overflow");
  if (bank.sampling === "iid" && weights.some((w) => w !== weights[0])) throw new Error("IID scenarios must have equal weights");
  return {
    metadata: { schemaVersion: 1, runId, modelVersion, snapshotId, streamId, decisionAt, inputsCapturedAt,
      source: bank.source, sampling: bank.sampling, seed: bank.seed },
    scenarioIds, weights: weights.map((w) => w / totalWeight), playerIds, scores, dependence: "supplied-joint",
  };
}

export type NflDistributionSummary = {
  mean: number; p10: number; p50: number; p90: number;
  target: number; targetProbability: number;
  /** Wilson 95% interval for IID simulation noise only; never model uncertainty. */
  monteCarlo95: [number, number] | null;
};

export function summarizeNflDraws(draws: number[], weights: number[], target: number, iid = false): NflDistributionSummary {
  if (!Number.isFinite(target) || draws.length < 2 || draws.length !== weights.length || draws.some((v) => !Number.isFinite(v))) {
    throw new Error("Invalid draws, target, or weight dimensions");
  }
  if (weights.some((w) => !Number.isFinite(w) || w <= 0)) throw new Error("Invalid weights");
  const total = weights.reduce((a, b) => a + b, 0);
  if (!Number.isFinite(total) || total <= 0) throw new Error("Invalid total weight");
  if (iid && weights.some((w) => w !== weights[0])) throw new Error("IID interval requires equal weights");
  const normalized = weights.map((w) => w / total);
  const ordered = draws.map((value, i) => ({ value, weight: normalized[i] })).sort((a, b) => a.value - b.value);
  const quantile = (p: number) => {
    let cumulative = 0;
    for (const item of ordered) { cumulative += item.weight; if (cumulative + 1e-12 >= p) return item.value; }
    return ordered[ordered.length - 1].value;
  };
  const probability = Math.min(1, Math.max(0, draws.reduce((sum, value, i) => sum + (value >= target ? normalized[i] : 0), 0)));
  let monteCarlo95: [number, number] | null = null;
  if (iid) {
    const n = draws.length;
    const z = 1.959963984540054;
    const denominator = 1 + z * z / n;
    const center = (probability + z * z / (2 * n)) / denominator;
    const radius = z * Math.sqrt(probability * (1 - probability) / n + z * z / (4 * n * n)) / denominator;
    monteCarlo95 = [Math.max(0, center - radius), Math.min(1, center + radius)];
  }
  return { mean: draws.reduce((sum, value, i) => sum + value * normalized[i], 0),
    p10: quantile(.1), p50: quantile(.5), p90: quantile(.9), target, targetProbability: probability, monteCarlo95 };
}

export function scoreNflLineupDraws(slate: NflDkSlate, lineup: NflLineup, bank: PreparedNflScenarios): number[] {
  validateNflLineup(slate, lineup);
  const result = bank.weights.map(() => 0);
  for (const entry of lineup) {
    const draws = bank.scores[entry.playerId];
    if (!draws || draws.length !== result.length) throw new Error(`Missing/misaligned draws for ${entry.playerId}`);
    const multiplier = entry.slot === "CPT" ? 1.5 : 1;
    // DK NFL scores have exact hundredth-point representation, including CPT.
    // Accumulating integers avoids scoring 149.99999999999 below a 150 target.
    draws.forEach((score, i) => {
      result[i] += Math.round(score * 100) * multiplier;
      if (!Number.isSafeInteger(result[i])) throw new Error("Lineup score exceeds exact scoring range");
    });
  }
  return result.map((hundredths) => hundredths / 100);
}

/** Diagnostic ablation only. Permutation preserves marginals, deliberately breaks event accounting. */
export function independentNflAblation(bank: PreparedNflScenarios, seed: number): PreparedNflScenarios {
  if (bank.weights.some((w) => w !== bank.weights[0])) throw new Error("Permutation ablation requires equal weights");
  const random = nflRandom(seed);
  return { ...bank, dependence: "independent-ablation",
    scores: Object.fromEntries(bank.playerIds.map((id) => [id, nflShuffle(bank.scores[id], random)])) };
}
