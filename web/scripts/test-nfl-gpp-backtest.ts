/**
 * Phase 8 tests (spec §15): point-in-time contest backtesting.
 * Covers P8-AC1..P8-AC5 with synthetic point-in-time packages.
 */
import assert from "node:assert/strict";
import {
  detectLeakage, backtestCapability, evaluateBacktestRow, summarizeBacktest,
  promotionDecision, assertDisjointWindow, type PointInTimeSlatePackage,
} from "../src/lib/nfl-dfs/backtest";

function pkg(over: Partial<PointInTimeSlatePackage> = {}): PointInTimeSlatePackage {
  return {
    season: 2025, week: 3, slateType: "showdown", contestId: "C1", lockAt: "2025-09-21T17:00:00Z",
    inputs: [
      { id: "salaries", asOf: "2025-09-20T12:00:00Z", digest: "a" },
      { id: "projections", asOf: "2025-09-21T09:00:00Z", digest: "b" },
    ],
    actualPlayerScores: { 1: 30, 2: 20, 3: 15, 4: 12, 5: 10, 6: 8 },
    contest: {
      fieldSize: 100, maxEntries: 20, entryFee: 5,
      payoutTable: [{ rankFrom: 1, rankTo: 1, payout: 200 }, { rankFrom: 2, rankTo: 10, payout: 20 }],
      tieRule: "split",
      fieldLineups: [
        { playerIds: [1, 2, 3, 4, 5, 6], score: 95 },
        { playerIds: [1, 2, 3, 4, 5, 7], score: 80 },
        { playerIds: [2, 3, 4, 5, 6, 7], score: 60 },
      ],
    },
    codeVersion: "nfl-dfs-ilp-v6-punt-policy", modelVersion: "model-x",
    ...over,
  };
}

const portfolio = [
  { playerIds: [1, 2, 3, 4, 5, 6], legal: true },
  { playerIds: [1, 2, 3, 4, 5, 7], legal: true },
];

function main() {
  // --- P8-AC2: a source timestamp after lock is leakage ---
  const leaked = pkg({ inputs: [{ id: "late-injury", asOf: "2025-09-21T18:00:00Z", digest: "z" }] }); // after 17:00 lock
  const findings = detectLeakage(leaked);
  assert.equal(findings.length, 1);
  assert.ok(/AFTER contest lock/.test(findings[0].detail));
  // Clean package has no leakage.
  assert.equal(detectLeakage(pkg()).length, 0);

  // --- P8-AC1: every row traces to a pre-lock snapshot and code/model version ---
  const row = evaluateBacktestRow(pkg(), portfolio);
  assert.equal(row.season, 2025);
  assert.equal(row.codeVersion, "nfl-dfs-ilp-v6-punt-policy");
  assert.equal(row.modelVersion, "model-x");
  assert.equal(row.leakage.length, 0);

  // --- P8-AC3: separate contests with complete field/payout data from projection-only ---
  assert.equal(backtestCapability(pkg()), "field_relative");
  const projOnly = pkg({ contest: null });
  assert.equal(backtestCapability(projOnly), "construction_only");
  const projRow = evaluateBacktestRow(projOnly, portfolio);
  assert.equal(projRow.field, null, "no field metrics without contest data");
  assert.ok(projRow.construction.legalityRate === 1, "construction metrics still available");

  // Field-relative row DOES produce ROI when data is present and clean.
  assert.ok(row.field, "field metrics present with contest data");
  assert.equal(typeof row.field!.roi, "number");
  assert.equal(typeof row.field!.netPayout, "number");

  // A leaked field-relative package still refuses field metrics (no ROI on leaked data).
  const leakedField = evaluateBacktestRow(leaked, portfolio);
  assert.equal(leakedField.field, null, "leaked data yields no field metrics");

  // --- P8-AC4: model selection and holdout evaluation use disjoint periods ---
  assertDisjointWindow({ trainSeasons: [{ season: 2025, week: 1 }, { season: 2025, week: 2 }], holdout: { season: 2025, week: 3 } });
  assert.throws(() => assertDisjointWindow({ trainSeasons: [{ season: 2025, week: 3 }], holdout: { season: 2025, week: 3 } }), /Leakage/);

  // --- P8-AC5: promotion decision cites experiment/sample/uncertainty; no ROI claim without field data ---
  const summaryProjOnly = summarizeBacktest([projRow, evaluateBacktestRow(pkg({ week: 4, contest: null }), portfolio)]);
  assert.equal(summaryProjOnly.fieldRelativeRows, 0);
  assert.equal(summaryProjOnly.meanRoi, null, "no ROI without field data");
  const decisionProjOnly = promotionDecision(summaryProjOnly, { minSlates: 2, requireFieldForRoiClaim: true });
  assert.ok(decisionProjOnly.reasons.some((r) => /ROI improvement cannot be claimed/i.test(r)));
  assert.ok(decisionProjOnly.reasons.some((r) => /legality/i.test(r)), "cites sample legality");

  // Leaked rows are excluded from the usable sample and gate promotion.
  const summaryLeaked = summarizeBacktest([leakedField, row]);
  assert.equal(summaryLeaked.leakedRows, 1);
  const gated = promotionDecision(summaryLeaked, { minSlates: 5, requireFieldForRoiClaim: true });
  assert.equal(gated.approved, false, "insufficient clean slates blocks promotion");

  // A field-relative summary reports ROI honestly.
  const fieldSummary = summarizeBacktest([row, evaluateBacktestRow(pkg({ week: 5 }), portfolio)]);
  assert.equal(fieldSummary.fieldRelativeRows, 2);
  assert.equal(typeof fieldSummary.meanRoi, "number");

  console.log("NFL GPP Phase 8 (point-in-time backtest): P8-AC1..AC5 passed.");
}

main();
