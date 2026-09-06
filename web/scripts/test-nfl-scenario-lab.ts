import assert from "node:assert/strict";
import { histogramNflDraws, runNflScenarioLab } from "../src/lib/nfl-dfs/scenario-lab";

async function main() {
  const bins = histogramNflDraws([-10, 0, 10], [10, -10, 0], [.2, .3, .5]);
  assert.equal(bins[0].start, -10);
  assert.equal(bins.at(-1)!.joint, .5, "maximum belongs to final bin");
  assert.equal(bins.reduce((s, b) => s + b.joint, 0), 1);
  assert.equal(bins.reduce((s, b) => s + b.independent, 0), 1);
  for (const format of ["classic", "showdown"] as const) {
    const request = { mode: "demo" as const, format, seed: 42, count: 12, draws: 100, target: 100 };
    const first = await runNflScenarioLab(request);
    const replay = await runNflScenarioLab(request);
    assert.deepEqual(first.report, replay.report);
    assert.deepEqual(first.histograms, replay.histograms);
    assert.deepEqual(first.digests, replay.digests);
    assert.equal(first.report.candidates.length, 12);
    assert.notEqual(first.digests.selection, first.digests.evaluation);
    for (const histogram of Object.values(first.histograms)) {
      assert.ok(Math.abs(histogram.reduce((s, b) => s + b.joint, 0) - 1) < 1e-10);
    }
    await assert.rejects(runNflScenarioLab({ ...request, seed: -1 }), /Seed/);
    await assert.rejects(runNflScenarioLab({ ...request, count: 151 }), /candidates/);
    await assert.rejects(runNflScenarioLab({ ...request, mode: "files" }), /salary CSV/);
  }
  console.log("NFL Scenario Lab: replay, histogram mass/boundaries, formats and input limits passed.");
}
main().catch((error) => { console.error(error); process.exitCode = 1; });
