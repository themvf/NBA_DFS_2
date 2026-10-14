/**
 * Freeze gate for nfl-portfolio-objective-v1 (docs/nfl-portfolio-objective-study.md).
 *
 * Runs the challenger's joint draws and field model on frozen discovery
 * inputs (the attribution studies' week-4/5 snapshots, the only ones carrying
 * an ownership prior) and compares the IMPLIED field thresholds with the
 * REALIZED ones from the stored contest score curves. Per format, the
 * challenger is frozen only if, on every checked slate, the realized top-1%
 * score lies within the format's week-to-week spread of the implied median.
 * The spread is the standard deviation of the realized top-1% across the
 * stored flagships of that format, supplied by the Python wrapper. Nothing
 * is fitted to outcomes here; the inputs are checked, not tuned.
 *
 * usage: research-nfl-portfolio-freeze.ts SPEC.json OUT.json
 */
import { createHash } from "node:crypto";
import { readFileSync, writeFileSync } from "node:fs";
import { gunzipSync } from "node:zlib";
import { drawJointBank, sampleField, fieldSummary, slateFromSnapshot, PORTFOLIO_OBJECTIVE, PORTFOLIO_OBJECTIVE_VERSION,
  JOINT_COPULA_VERSION, FIELD_MODEL_VERSION } from "../src/lib/nfl-dfs/portfolio-objective";

type Spec = {
  slates: Array<{ file: string; format: "classic" | "showdown"; label: string; contestId: string; entries: number;
    realized: { winning: number; top01Pct: number; top1Pct: number; median: number } }>;
  spreads: Record<"classic" | "showdown", { top1PctSd: number; contests: number }>;
  moduleFiles: string[];
};
const [specPath, outPath] = process.argv.slice(2);
if (!specPath || !outPath) throw new Error("usage: research-nfl-portfolio-freeze.ts SPEC.json OUT.json");
const spec = JSON.parse(readFileSync(specPath, "utf8")) as Spec;
const sha = (path: string) => createHash("sha256").update(readFileSync(path)).digest("hex");

const rows = spec.slates.map(s => {
  const d = JSON.parse(gunzipSync(readFileSync(s.file)).toString());
  const slate = slateFromSnapshot(s.format, d.players);
  const bank = drawJointBank(d.players, PORTFOLIO_OBJECTIVE.evaluationSeed);
  const field = sampleField(slate, d.players, PORTFOLIO_OBJECTIVE.fieldSeed);
  const implied = fieldSummary(field, bank);
  const sd = spec.spreads[s.format].top1PctSd;
  const miss = s.realized.top1Pct - implied.top1Pct.p50;
  return { label: s.label, format: s.format, contestId: s.contestId, entries: s.entries, players: d.players.length,
    fallbackPlayers: bank.fallbackPlayerIds.length, meanNotMatched: bank.meanNotMatchedIds.length,
    fieldCalibration: field.calibration, implied, realized: s.realized,
    top1PctMiss: Math.round(miss * 100) / 100, allowedMiss: Math.round(sd * 100) / 100,
    pass: Math.abs(miss) <= sd,
    inside90: s.realized.top1Pct >= implied.top1Pct.p5 && s.realized.top1Pct <= implied.top1Pct.p95 };
});
const formats = Object.fromEntries((["classic", "showdown"] as const).map(f => {
  const mine = rows.filter(r => r.format === f);
  return [f, { checked: mine.length, passed: mine.filter(r => r.pass).length,
    status: mine.length && mine.every(r => r.pass) ? "pass" : mine.length ? "fail" : "unchecked",
    spread: spec.spreads[f] }];
}));
const out = { version: PORTFOLIO_OBJECTIVE_VERSION, jointVersion: JOINT_COPULA_VERSION, fieldVersion: FIELD_MODEL_VERSION,
  constants: PORTFOLIO_OBJECTIVE, frozenAt: new Date().toISOString(),
  rule: "Per format: frozen only if on every checked slate |realized top-1% - implied median top-1%| <= the format's week-to-week SD of realized top-1% across stored flagships. Realized values are never used to tune anything.",
  moduleHashes: Object.fromEntries(spec.moduleFiles.map(f => [f, sha(f)])), formats, slates: rows };
writeFileSync(outPath, JSON.stringify(out, null, 1), { flag: "wx" });
for (const r of rows) console.log(`${r.pass ? "PASS" : "FAIL"} ${r.label}: realized top1% ${r.realized.top1Pct} vs implied ${r.implied.top1Pct.p50} (p5-p95 ${r.implied.top1Pct.p5}-${r.implied.top1Pct.p95}); miss ${r.top1PctMiss} allowed ${r.allowedMiss}; field calibration max dev ${r.fieldCalibration.maxAbsDeviationPct}pp`);
console.log(JSON.stringify(formats));
