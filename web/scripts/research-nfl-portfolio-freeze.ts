/**
 * Freeze gate for nfl-portfolio-objective-v3 (docs/nfl-portfolio-objective-study.md, "v3").
 *
 * For one format, over its registered gate slates (fresh week 6+ slates the
 * v1/v2 gate never checked), applies the rule registered before this ran:
 *
 *   u        share of the 20,000 evaluation draws whose implied within-draw
 *            top-1% score is below the realized one (a tie counts half);
 *            uniform for a calibrated model.
 *   coverage at most k slates with u outside [0.05, 0.95], k the smallest
 *            number with P(Binomial(n, 0.10) > k) <= 0.05 (k = 1 at n = 3).
 *   bias     mean of z = normal quantile of u (u clamped half a draw from 0
 *            and 1) within +/- 1.96 / sqrt(n).
 *   stability on every slate, arm B's pick from the study's own candidate pool
 *            under selection seeds 20261016 and 20261019 overlaps at a Jaccard
 *            of at least 0.80.
 *
 * The format passes only if all three hold. Realized values are read for the
 * test and never used to tune anything.
 *
 * usage: research-nfl-portfolio-freeze.ts SPEC.json OUT.json
 */
import { createHash } from "node:crypto";
import { readFileSync, writeFileSync } from "node:fs";
import { gunzipSync } from "node:zlib";
import { drawJointBank, sampleField, fieldThresholdsMany, summarizeFieldThresholds, SUMMARY_FRACTIONS, slateFromSnapshot, PORTFOLIO_OBJECTIVE, PORTFOLIO_OBJECTIVE_VERSION,
  JOINT_COPULA_VERSION, FIELD_MODEL_VERSION, FIELD_STACKING } from "../src/lib/nfl-dfs/portfolio-objective";
import { portfolioSelectionStability } from "./nfl-pool-rule-study-core";

type Spec = {
  version: string; format: "classic" | "showdown";
  slates: Array<{ label: string; contestId: string; entries: number; sourceRunId: string; input: string; inputSha256: string;
    realized: { winning: number; top01Pct: number; top1Pct: number; median: number } }>;
  moduleFiles: Record<string, string>;
};
export const GATE = { pitLow: 0.05, pitHigh: 0.95, outsideRate: 0.10, coverageAlpha: 0.05, biasZ: 1.96, minJaccard: 0.80, minSlates: 3 } as const;

/** Inverse standard normal CDF (Acklam's rational approximation, |error| < 1.2e-9). */
export function normalQuantile(p: number): number {
  const a = [-3.969683028665376e1, 2.209460984245205e2, -2.759285104469687e2, 1.38357751867269e2, -3.066479806614716e1, 2.506628277459239];
  const b = [-5.447609879822406e1, 1.615858368580409e2, -1.556989798598866e2, 6.680131188771972e1, -1.328068155288572e1];
  const c = [-7.784894002430293e-3, -3.223964580411365e-1, -2.400758277161838, -2.549732539343734, 4.374664141464968, 2.938163982698783];
  const d = [7.784695709041462e-3, 3.224671290700398e-1, 2.445134137142996, 3.754408661907416];
  const lo = 0.02425, hi = 1 - lo;
  if (p < lo) { const q = Math.sqrt(-2 * Math.log(p)); return (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1); }
  if (p > hi) { const q = Math.sqrt(-2 * Math.log(1 - p)); return -(((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1); }
  const q = p - 0.5, r = q * q;
  return (((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5]) * q / (((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1);
}

/** Smallest k with P(Binomial(n, rate) > k) <= alpha. */
export function allowedOutside(n: number, rate = GATE.outsideRate, alpha = GATE.coverageAlpha): number {
  const choose = (m: number, j: number) => { let v = 1; for (let i = 1; i <= j; i++) v = (v * (m - j + i)) / i; return v; };
  let cdf = 0;
  for (let k = 0; k <= n; k++) {
    cdf += choose(n, k) * rate ** k * (1 - rate) ** (n - k);
    if (1 - cdf <= alpha + 1e-12) return k;
  }
  return n;
}

/** Where the realized value falls among the implied draws (ties count half). */
export function pit(implied: Float64Array, realized: number): number {
  let below = 0, tied = 0;
  for (const v of implied) { if (v < realized) below++; else if (v === realized) tied++; }
  return (below + 0.5 * tied) / implied.length;
}

/** The registered verdict for one format from its slates' u values and stability overlaps. */
export function verdict(us: number[], jaccards: number[]) {
  const n = us.length;
  if (n < GATE.minSlates) return { status: "unchecked" as const, n, reason: `${n} gate slates; the rule needs ${GATE.minSlates}` };
  const outside = us.filter(u => u < GATE.pitLow || u > GATE.pitHigh).length;
  const k = allowedOutside(n);
  const D = PORTFOLIO_OBJECTIVE.draws;
  const zs = us.map(u => normalQuantile(Math.min(1 - 0.5 / D, Math.max(0.5 / D, u))));
  const meanZ = zs.reduce((s, z) => s + z, 0) / n;
  const biasLimit = GATE.biasZ / Math.sqrt(n);
  const coverage = outside <= k, bias = Math.abs(meanZ) <= biasLimit, stability = jaccards.every(j => j >= GATE.minJaccard);
  return { status: coverage && bias && stability ? "pass" as const : "fail" as const, n,
    coverage: { pass: coverage, outside, allowed: k }, bias: { pass: bias, meanZ: Math.round(meanZ * 1000) / 1000, limit: Math.round(biasLimit * 1000) / 1000, z: zs.map(z => Math.round(z * 1000) / 1000) },
    stability: { pass: stability, minJaccard: GATE.minJaccard, jaccards } };
}

const main = () => {
  const [specPath, outPath] = process.argv.slice(2);
  if (!specPath || !outPath) throw new Error("usage: research-nfl-portfolio-freeze.ts SPEC.json OUT.json");
  const spec = JSON.parse(readFileSync(specPath, "utf8")) as Spec;
  if (spec.version !== PORTFOLIO_OBJECTIVE_VERSION) throw new Error(`Spec is ${spec.version}, module is ${PORTFOLIO_OBJECTIVE_VERSION}`);
  const sha = (path: string) => createHash("sha256").update(readFileSync(path)).digest("hex");
  const round = (x: number) => Math.round(x * 100) / 100;
  const rows = spec.slates.map(s => {
    const raw = gunzipSync(readFileSync(s.input));
    if (createHash("sha256").update(raw).digest("hex") !== s.inputSha256) throw new Error(`${s.label}: archived input changed`);
    const input = JSON.parse(raw.toString());
    const started = Date.now();
    const slate = slateFromSnapshot(spec.format, input.players);
    const bank = drawJointBank(input.players, PORTFOLIO_OBJECTIVE.evaluationSeed);
    const field = sampleField(slate, input.players, PORTFOLIO_OBJECTIVE.fieldSeed);
    const thresholds = fieldThresholdsMany(field, bank, SUMMARY_FRACTIONS);
    const top1 = thresholds[0];
    const implied = summarizeFieldThresholds(thresholds);
    const u = pit(top1, s.realized.top1Pct);
    const stability = portfolioSelectionStability(input, [PORTFOLIO_OBJECTIVE.selectionSeed, PORTFOLIO_OBJECTIVE.stabilitySeed]);
    console.error(`${s.label}: gate slate evaluated in ${Math.round((Date.now() - started) / 1000)} s`);
    return { label: s.label, contestId: s.contestId, entries: s.entries, sourceRunId: s.sourceRunId, inputSha256: s.inputSha256,
      players: input.players.length, fallbackPlayers: bank.fallbackPlayerIds.length, meanNotMatched: bank.meanNotMatchedIds.length,
      fieldCalibration: field.calibration, fieldShapes: field.shapes,
      implied, realized: s.realized, u: Math.round(u * 10000) / 10000, top1PctMiss: round(s.realized.top1Pct - implied.top1Pct.p50),
      stability };
  });
  const result = verdict(rows.map(r => r.u), rows.map(r => r.stability.jaccard));
  const out = { version: PORTFOLIO_OBJECTIVE_VERSION, jointVersion: JOINT_COPULA_VERSION, fieldVersion: FIELD_MODEL_VERSION,
    constants: PORTFOLIO_OBJECTIVE, fieldStacking: FIELD_STACKING, gate: GATE, frozenAt: new Date().toISOString(),
    rule: "Registered 2026-10-30 (docs/nfl-portfolio-objective-study.md, v3). u = share of 20,000 evaluation draws with implied top-1% below realized (ties half). Pass only if: at most k slates with u outside [0.05, 0.95] (k smallest with P(Bin(n,0.1) > k) <= 0.05); |mean normal-quantile(u)| <= 1.96/sqrt(n); and arm B's selection under seeds 20261016 and 20261019 overlaps at Jaccard >= 0.80 on every slate.",
    moduleHashes: Object.fromEntries(Object.entries(spec.moduleFiles).map(([rel, abs]) => [rel, sha(abs)])),
    formats: { [spec.format]: result }, slates: rows };
  writeFileSync(outPath, JSON.stringify(out, null, 1), { flag: "wx" });
  for (const r of rows) console.log(`${r.label}: realized top1% ${r.realized.top1Pct} vs implied p50 ${r.implied.top1Pct.p50} (p5-p95 ${r.implied.top1Pct.p5}-${r.implied.top1Pct.p95}); u ${r.u}; selection Jaccard ${r.stability.jaccard} (${r.stability.candidates} candidates, ${r.stability.requested} lineups)`);
  console.log(JSON.stringify({ [spec.format]: result }));
};

if (process.argv[1]?.endsWith("research-nfl-portfolio-freeze.ts")) main();
