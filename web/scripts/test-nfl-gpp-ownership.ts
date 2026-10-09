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

  // Structure/coverage do not prove accuracy on independent games.
  const validated = assessOwnership(pool, validatedFeed(12));
  assert.equal(validated.capability, "unavailable");
  assert.equal(validated.features.leverage, false);
  assert.equal(validated.features.duplicationModel, false);
  assert.ok(validated.warnings.some(w => /accuracy/.test(w)));
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

  // --- a combined feed (no captain data) can never validate for Showdown ---
  const combined: NflOwnershipInput[] = validatedFeed(12).map((r) => ({ ...r, flexPct: 0.42, captainPct: null }));
  const combinedStrict = assessOwnership(pool, combined);
  assert.notEqual(combinedStrict.capability, "validated");
  assert.ok(combinedStrict.errors.some((e) => /no Captain-slot ownership/i.test(e)), "missing captain data is named, not misreported as a 0% captain sum");
  assert.ok(!combinedStrict.errors.some((e) => /Captain ownership totals 0%/.test(e)), "no impossible captain-sum error for a feed with no captain data");
  // The same combined feed DECLARED heuristic carries no slot-sum errors at all:
  // the invariants test a structure the feed never claimed to have.
  const combinedHeuristic = assessOwnership(pool, combined, { heuristic: true, optIntoHeuristic: true });
  assert.equal(combinedHeuristic.capability, "heuristic_uncalibrated");
  assert.equal(combinedHeuristic.errors.length, 0, "declared-heuristic combined feed has no structural errors");
  assert.equal(combinedHeuristic.features.leverage, true, "explicit opt-in enables labeled heuristic leverage");
  assert.equal(combinedHeuristic.features.duplicationModel, false);

  // --- classic format expects ~900% across 9 roster slots, no captain check ---
  const classicFeed: NflOwnershipInput[] = Array.from({ length: 12 }, (_, i) => ({ playerId: i + 1, flexPct: 9 / 12, captainPct: null, source: "slot-feed", asOf: null }));
  const classic = assessOwnership(pool, classicFeed, { format: "classic" });
  assert.equal(classic.capability, "unavailable", 'Coverage alone cannot validate a Classic feed');
  const calibration = {format:'classic' as const,modelVersion:'slot-feed',registration:'nfl-ownership-classic-phase2',
    sourceDigest:'a'.repeat(64),heldOutSlateIds:['s1','s2','s3','s4'],spearman:.8,maePp:1.5,biasPp:.1};
  const qualified = assessOwnership(pool,classicFeed,{format:'classic',calibration});
  assert.equal(qualified.capability,'validated');
  assert.equal(qualified.features.leverage,true);
  assert.equal(qualified.features.duplicationModel,false,'Marginal ownership accuracy does not qualify a joint contest field');
  for (const bad of [{...calibration,heldOutSlateIds:['s1','s1','s1','s1']},
    {...calibration,maePp:2.01},{...calibration,biasPp:.51},{...calibration,spearman:NaN},
    {...calibration,modelVersion:'other-model'},{...calibration,sourceDigest:'missing'}]) {
    assert.equal(assessOwnership(pool,classicFeed,{format:'classic',calibration:bad}).features.leverage,false);
  }
  assert.equal(assessOwnership(pool,validatedFeed(12),{calibration}).features.leverage,false,'Classic cannot qualify Showdown');

  console.log("NFL GPP Phase 2 (ownership capability): P2-AC1..AC5 and validation units passed.");
}

main();
