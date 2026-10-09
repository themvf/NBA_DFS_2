/** Reuse fixed legal candidates only after proving every supplied stat draw identical. */
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFileSync, writeFileSync } from "node:fs";
import { prepareNflScenarios, scoreNflLineupDraws } from "../src/lib/nfl-dfs/scenarios";
import { toScenarioLineup } from "../src/app/dfs/nfl/nfl-optimizer-shadow";
const [oldInputPath, newInputPath, oldReportPath, outputPath] = process.argv.slice(2);
const old = JSON.parse(readFileSync(oldInputPath, "utf8"));
const newText = readFileSync(newInputPath, "utf8"), current = JSON.parse(newText);
const report = JSON.parse(readFileSync(oldReportPath, "utf8"));
assert.deepEqual(current.slate, old.slate);
assert.deepEqual(current.optimizerPlayers, old.optimizerPlayers);
assert.equal(current.manifest.sources.comparison_file_sha256, report.comparisonDigest);
for (const key of ["selection", "evaluation"]) {
  assert.equal(current[key].seed, old[key].seed);
  assert.deepEqual(current[key].scenarios.map((d: any) => d.stats), old[key].scenarios.map((d: any) => d.stats));
}
const selection = prepareNflScenarios(current.slate, current.selection), evaluation = prepareNflScenarios(current.slate, current.evaluation);
for (const row of report.records) {
  for (const [lineups, expected] of [[row.baselineGenerated, row.baselinePortfolioUnderShadowModel.construction.mean],
    [row.shadowComparison.selectedGenerated, row.shadowComparison.challenger.construction.mean]] as const) {
    const scored = lineups.map((lineup: any) => scoreNflLineupDraws(current.slate, toScenarioLineup(lineup), evaluation));
    const actual = evaluation.scenarioIds.reduce((sum, _, i) => sum + Math.max(...scored.map((values: number[]) => values[i])), 0) / evaluation.scenarioIds.length;
    assert.ok(Math.abs(actual - expected) < 1e-8, "Fixed portfolio independent score changed");
  }
  row.shadowComparison.selectionManifest = selection.metadata;
  row.shadowComparison.evaluationManifest = evaluation.metadata;
  row.shadowComparison.contest.decisionAt = current.selection.decisionAt;
}
report.createdAt = new Date().toISOString();
report.sourceDigest = createHash("sha256").update(newText).digest("hex");
report.inputAudit = current.audit;
report.evaluationModel = current.version;
report.scenarioManifest = current.manifest;
report.replayProof = { method: "all per-player stat draws, slate and optimizer inputs exactly equal across both streams; every baseline/selected portfolio independently rescored",
  oldBankDigest: createHash("sha256").update(readFileSync(oldInputPath)).digest("hex"), oldReportDigest: createHash("sha256").update(readFileSync(oldReportPath)).digest("hex") };
report.assumptions.push("Candidate lineups were retained from the prior freeze after exact equality of all slate inputs and stat draws was verified; each portfolio was scored again under the new registered manifest.");
writeFileSync(outputPath, JSON.stringify(report, null, 2));
console.log(JSON.stringify({ status: "exact_draw_replay_verified", model: current.version, modes: report.records.map((r: any) => ({ count: r.settings.nLineups, baseline: r.baselinePortfolioUnderShadowModel.construction.mean, selected: r.shadowComparison.challenger.construction.mean })) }));
