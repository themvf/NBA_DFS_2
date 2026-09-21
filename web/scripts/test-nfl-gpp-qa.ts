/**
 * Phase 6 tests (spec §13): pre-export portfolio QA.
 * Covers P6-AC1..P6-AC5 plus severity/override units.
 */
import assert from "node:assert/strict";
import { runNflPreExportQa, NFL_QA_RULESET_VERSION, type QaInput, type QaOverride } from "../src/lib/nfl-dfs/pre-export-qa";

function legalLineup(n: number, ids: number[]): QaInput["lineups"][number] {
  return { lineupNumber: n, playerIds: ids, totalSalary: 49000, slots: ids.map((id, i) => ({ slot: i === 0 ? "CPT" : `FLEX${i}`, playerId: id })), archetype: { id: "standard_ceiling", label: "Standard ceiling", fadedPlayerIds: [], beneficiariesSatisfied: [] } };
}

function baseInput(over: Partial<QaInput> = {}): QaInput {
  return {
    format: "showdown", requestedLineups: 2,
    lineups: [legalLineup(1, [1, 2, 3, 4, 5, 6]), legalLineup(2, [1, 2, 3, 4, 5, 7])],
    eligibility: [], ...over,
  };
}

function main() {
  // --- A clean portfolio is Ready ---
  const clean = runNflPreExportQa(baseInput());
  assert.equal(clean.decision, "ready");
  assert.equal(clean.rulesetVersion, NFL_QA_RULESET_VERSION);
  assert.equal(clean.openBlockers.length, 0);

  // --- P6-AC1: an unapproved $400 player in a lineup blocks export ---
  const puntInput = baseInput({
    eligibility: [{ dkPlayerId: 6, name: "$400 body", eligible: false, reasonCode: "ABSOLUTE_SALARY_BLOCK", overridden: false }],
  });
  const puntQa = runNflPreExportQa(puntInput);
  assert.equal(puntQa.decision, "blocked");
  assert.ok(puntQa.openBlockers.includes("no_unapproved_punt"));

  // Overriding the punt check clears it (it is overridable).
  const override: QaOverride[] = [{ checkId: "no_unapproved_punt", reason: "verified returner role", user: "t", at: "2026-09-20T00:00:00Z", rulesetVersion: NFL_QA_RULESET_VERSION, runId: "r" }];
  const puntOverridden = runNflPreExportQa(puntInput, override);
  assert.ok(!puntOverridden.openBlockers.includes("no_unapproved_punt"), "override clears the punt blocker");

  // --- Inactive is a blocker that is NEVER overridable ---
  const inactiveInput = baseInput({ eligibility: [{ dkPlayerId: 6, name: "OUT guy", eligible: false, reasonCode: "INACTIVE", overridden: false }] });
  const inactiveQa = runNflPreExportQa(inactiveInput, [{ checkId: "no_inactive", reason: "nope", user: "t", at: "x", rulesetVersion: NFL_QA_RULESET_VERSION, runId: "r" }]);
  assert.ok(inactiveQa.openBlockers.includes("no_inactive"), "inactive cannot be overridden");

  // --- P6-AC2: exposure/archetype shortfalls name the exact unsatisfied constraint ---
  const exposureQa = runNflPreExportQa(baseInput({ exposureReport: [{ dkPlayerId: 3, name: "Player 3", binding: "captain min missed (0/2)" }] }));
  assert.ok(exposureQa.openBlockers.includes("exposure_ranges"));
  const exposureCheck = exposureQa.checks.find((c) => c.id === "exposure_ranges")!;
  assert.ok(/Player 3/.test(exposureCheck.detail) && /captain min missed/.test(exposureCheck.detail));

  const quotaQa = runNflPreExportQa(baseInput({ archetypePlan: [{ archetypeId: "single_chalk_fade", label: "Single-chalk fade", requested: 2, realized: 1 }] }));
  const quotaCheck = quotaQa.checks.find((c) => c.id === "archetype_quotas")!;
  assert.ok(/Single-chalk fade 1\/2/.test(quotaCheck.detail));

  // --- Fade without a beneficiary is blocked ---
  const fadeInput = baseInput();
  fadeInput.lineups[0].archetype = { id: "single_chalk_fade", label: "Single-chalk fade", fadedPlayerIds: [9], beneficiariesSatisfied: [] };
  const fadeQa = runNflPreExportQa(fadeInput);
  assert.ok(fadeQa.openBlockers.includes("fade_beneficiary"));

  // --- Illegal roster is a non-overridable blocker ---
  const illegal = baseInput();
  illegal.lineups[0].totalSalary = 51000;
  const illegalQa = runNflPreExportQa(illegal, [{ checkId: "legal_roster", reason: "x", user: "t", at: "x", rulesetVersion: NFL_QA_RULESET_VERSION, runId: "r" }]);
  assert.ok(illegalQa.openBlockers.includes("legal_roster"), "illegal roster never overridable");

  // --- Ownership: invalid + leverage on is a blocker; unavailable is info ---
  const invalidLev = runNflPreExportQa(baseInput({ ownership: { capability: "validated", errors: ["bad captain total"], features: { leverage: true } } }));
  assert.ok(invalidLev.openBlockers.includes("ownership_leverage_valid"));
  const projOnly = runNflPreExportQa(baseInput({ ownership: { capability: "unavailable", errors: [], features: { leverage: false } } }));
  assert.ok(projOnly.checks.some((c) => c.id === "ownership_unavailable" && c.severity === "info"));

  // --- P6-AC3: changing inputs re-runs QA (pure fn: different input -> different report) ---
  const before = runNflPreExportQa(baseInput());
  const after = runNflPreExportQa(baseInput({ exposureReport: [{ dkPlayerId: 1, name: "P1", binding: "overall min missed (0/1)" }] }));
  assert.notEqual(before.decision, after.decision, "a settings change yields a different QA decision");

  // --- P6-AC4: the report is fully reconstructable from persisted inputs (deterministic) ---
  const a = runNflPreExportQa(puntInput, override);
  const b = runNflPreExportQa(puntInput, override);
  assert.deepEqual(a, b, "QA is deterministic and reconstructable");

  // --- P6-AC5: projection-only cannot claim leverage (info check present, no leverage blocker) ---
  assert.ok(!projOnly.openBlockers.includes("ownership_leverage_valid"));

  // Warning-only portfolio is "ready_with_warnings".
  const warnOnly = runNflPreExportQa(baseInput({ salaryBandReport: [{ band: { min: 0, max: 400 }, withinPlan: false, count: 5, minCount: 0, maxCount: 1 }] }));
  assert.equal(warnOnly.decision, "ready_with_warnings");

  console.log("NFL GPP Phase 6 (pre-export QA): P6-AC1..AC5 and severity/override units passed.");
}

main();
