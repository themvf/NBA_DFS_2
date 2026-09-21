/**
 * Phase 2 tests (spec §9): ownership capability and missing-data safeguards.
 * Covers P2-AC1..P2-AC5 plus validation unit rules.
 */
import assert from "node:assert/strict";
import { assessOwnership, objectiveLabel, type NflOwnershipInput, type EligibleOwnershipPlayer } from "../src/lib/nfl-dfs/ownership-capability";

function eligiblePool(n: number): EligibleOwnershipPlayer[] {
  return Array.from({ length: n }, (_, i) => ({ playerId: i + 1, medianProjection: 10 }));
}

/** A validated-looking Showdown feed: full coverage, CPT ~100%, FLEX ~500%. */
function validatedFeed(n: number): NflOwnershipInput[] {
  return Array.from({ length: n }, (_, i) => ({
    playerId: i + 1, flexPct: 5 / n, captainPct: 1 / n, source: "validated-feed", asOf: "2026-09-20T12:00:00Z",
  }));
}

function main() {
  const pool = eligiblePool(12);

  // --- P2-AC1: null ownership gets neither a zero-ownership bonus nor a leverage label ---
  const unavailable = assessOwnership(pool, []);
  assert.equal(unavailable.capability, "unavailable");
  assert.equal(unavailable.features.leverage, false);
  assert.equal(objectiveLabel("unavailable"), "Projection-only GPP");

  // --- validated feed enables leverage ---
  const validated = assessOwnership(pool, validatedFeed(12));
  assert.equal(validated.capability, "validated", `expected validated, got ${validated.capability}: ${validated.errors.join("; ")}`);
  assert.equal(validated.features.leverage, true);
  assert.equal(validated.features.duplicationModel, true);
  assert.equal(objectiveLabel("validated"), "GPP leverage");

  // --- P2-AC2: a malformed upload with Captain ownership totaling 35% fails validation ---
  const badCaptain: NflOwnershipInput[] = validatedFeed(12).map((r, i) => ({ ...r, captainPct: i < 5 ? 0.07 : 0 })); // sums to 0.35
  const badAssessment = assessOwnership(pool, badCaptain);
  assert.notEqual(badAssessment.capability, "validated");
  assert.ok(badAssessment.errors.some((e) => /Captain ownership totals/i.test(e)), "captain total error is reported");

  // --- P2-AC4: captain and flex are never combined into one percentage ---
  // The assessment reports captainTotal and flexTotal separately and they differ.
  assert.ok(Math.abs(validated.captainTotal - 1) < 0.15);
  assert.ok(Math.abs(validated.flexTotal - 5) < 0.75);
  assert.notEqual(validated.captainTotal, validated.flexTotal);

  // --- P2-AC5: a heuristic estimate can never be mistaken for a validated feed ---
  const heuristic = assessOwnership(pool, validatedFeed(12), { heuristic: true, optIntoHeuristic: true });
  assert.equal(heuristic.capability, "heuristic_uncalibrated");
  assert.ok(heuristic.warnings.some((w) => /Uncalibrated estimate/i.test(w)));
  assert.equal(heuristic.features.duplicationModel, false, "heuristic never enables the duplication model");
  // Without opt-in, heuristic features stay off.
  const heuristicNoOptIn = assessOwnership(pool, validatedFeed(12), { heuristic: true });
  assert.equal(heuristicNoOptIn.features.leverage, false);

  // --- value bounds and duplicates are caught ---
  const outOfRange = assessOwnership(pool, [{ playerId: 1, flexPct: 1.5, captainPct: null, source: "x", asOf: null }]);
  assert.ok(outOfRange.errors.some((e) => /0–100%/.test(e)));
  const dup = assessOwnership(pool, [validatedFeed(1)[0], validatedFeed(1)[0]]);
  assert.ok(dup.errors.some((e) => /Duplicate/i.test(e)));

  // --- partial validated ownership below coverage -> projection-only, not validated ---
  const partial = assessOwnership(eligiblePool(20), validatedFeed(10)); // only 50% coverage
  assert.notEqual(partial.capability, "validated");
  assert.equal(partial.features.leverage, false);

  console.log("NFL GPP Phase 2 (ownership capability): P2-AC1..AC5 and validation units passed.");
}

main();
