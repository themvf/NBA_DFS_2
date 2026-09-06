import { parseNflDkSalaryCsv, type NflSlateFormat } from "./dk-salary-csv";
import { rankNflCandidates } from "./experiment-core";
import { generateNflCandidates } from "./lineups";
import { independentNflAblation, prepareNflScenarios, scoreNflLineupDraws } from "./scenarios";
import { nflDemoBank, nflDemoSlate } from "./synthetic";

export type ScenarioLabRequest = {
  mode: "demo" | "files";
  format: NflSlateFormat;
  seed: number;
  count: number;
  draws: number;
  target: number;
  files?: { salary: string; selection: string; evaluation: string };
};

export function histogramNflDraws(joint: number[], independent: number[], weights: number[]) {
  const low = Math.floor(Math.min(joint.reduce((a, b) => Math.min(a, b), Infinity), independent.reduce((a, b) => Math.min(a, b), Infinity)) / 10) * 10;
  const high = Math.max(joint.reduce((a, b) => Math.max(a, b), -Infinity), independent.reduce((a, b) => Math.max(a, b), -Infinity));
  const width = Math.max(1, Math.ceil((high - low) / 20));
  const bins = Array.from({ length: 20 }, (_, i) => ({ start: low + i * width, end: low + (i + 1) * width, joint: 0, independent: 0 }));
  joint.forEach((score, i) => { bins[Math.min(19, Math.floor((score - low) / width))].joint += weights[i]; });
  independent.forEach((score, i) => { bins[Math.min(19, Math.floor((score - low) / width))].independent += weights[i]; });
  return bins;
}

async function sha256(value: string) {
  const bytes = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(value));
  return Array.from(new Uint8Array(bytes), (b) => b.toString(16).padStart(2, "0")).join("");
}

/** Bounded browser research runner. No uploads, database calls, or live lineup mutations. */
export async function runNflScenarioLab(request: ScenarioLabRequest) {
  if (!request || !["demo", "files"].includes(request.mode)) throw new Error("Choose demo or uploaded inputs");
  if (!["classic", "showdown"].includes(request.format)) throw new Error("Unknown format");
  if (!Number.isInteger(request.count) || request.count < 1 || request.count > 150) throw new Error("Choose 1–150 candidates");
  if (!Number.isInteger(request.draws) || request.draws < 100 || request.draws > 3000) throw new Error("Choose 100–3,000 draws per bank");
  if (!Number.isFinite(request.target) || request.target < -100 || request.target > 1000) throw new Error("Score target must be between -100 and 1,000");
  if (!Number.isInteger(request.seed) || request.seed < 0 || request.seed > 0xffff_ffff) throw new Error("Seed must be an unsigned 32-bit integer");
  if (request.mode === "files" && (!request.files || [request.files.salary, request.files.selection, request.files.evaluation].some((v) => typeof v !== "string" || !v.trim()))) {
    throw new Error("Choose a salary CSV and separate selection and evaluation JSON files");
  }
  if (request.files && Object.values(request.files).reduce((sum, s) => sum + s.length, 0) > 40 * 1024 * 1024) throw new Error("Combined files exceed the 40 MB browser limit");
  const started = performance.now();
  const slate = request.mode === "demo" ? nflDemoSlate(request.format) : parseNflDkSalaryCsv(request.files!.salary);
  if (slate.players.length > 1000) throw new Error("Browser limit is 1,000 players");
  const selection = request.mode === "demo" ? nflDemoBank(slate, request.seed, request.draws, "selection") : JSON.parse(request.files!.selection);
  const evaluation = request.mode === "demo" ? nflDemoBank(slate, (request.seed ^ 0xa5a5a5a5) >>> 0, request.draws, "evaluation") : JSON.parse(request.files!.evaluation);
  for (const bank of [selection, evaluation]) {
    if (!bank || !Array.isArray(bank.scenarios) || bank.scenarios.length > 3000) throw new Error("Each bank must contain at most 3,000 scenarios");
  }
  const searchSeed = (request.seed ^ 0x12345678) >>> 0;
  const search = generateNflCandidates(slate, { count: request.count, seed: searchSeed });
  if (!search.lineups.length) throw new Error("No legal candidates found within the search limit. This does not prove infeasibility.");
  const report = rankNflCandidates({ slate, candidates: search.lineups, selection, evaluation, target: request.target, ablationSeed: (request.seed ^ 0x87654321) >>> 0 });
  const joint = prepareNflScenarios(slate, evaluation);
  const independent = independentNflAblation(joint, report.manifest.evaluationAblationSeed);
  const histograms = Object.fromEntries(report.candidates.map((candidate) => [candidate.key, histogramNflDraws(
    scoreNflLineupDraws(slate, candidate.lineup, joint), scoreNflLineupDraws(slate, candidate.lineup, independent), joint.weights,
  )]));
  const digests = {
    encoding: "sha256-json-serialization-v1",
    slate: await sha256(JSON.stringify(slate)), candidates: await sha256(JSON.stringify(search.lineups)),
    selection: await sha256(JSON.stringify(selection)), evaluation: await sha256(JSON.stringify(evaluation)),
  };
  return { report, slate, histograms, digests,
    execution: { elapsedMs: performance.now() - started, requestedCount: request.count, searchSeed, searchStatus: search.status, attempts: search.attempts, nodes: search.nodes } };
}

export type ScenarioLabResult = Awaited<ReturnType<typeof runNflScenarioLab>>;
