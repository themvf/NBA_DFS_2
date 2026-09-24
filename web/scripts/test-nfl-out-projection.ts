// A DK-flagged OUT player must be STORED at zero, not only displayed at zero.
// Until 2026-09-22 the slate write copied the run's number and the read layer
// hid it; the stored row and every report card reading it were wrong.
import assert from "node:assert/strict";
import { OUT_PROJECTION_STATUS, POSITION_PRIOR_STATUS, UNSUPPORTED_PROJECTION_STATUS,
  isUnsupportedProjection, storedSlateProjection, zeroOutProjection } from "../src/lib/nfl-dfs/out-projection";

const run = { projectionStatus: "historical", modelProjFpts: 14.2, floorFpts: 6.1, medianFpts: 13.0, ceilingFpts: 24.5, boomRate: 0.12 };

// Playing: the run's numbers pass through untouched.
assert.deepEqual(storedSlateProjection(run, false), {
  projectionStatus: "historical", ourProj: 14.2, floorFpts: 6.1, medianFpts: 13.0, ceilingFpts: 24.5, boomRate: 0.12,
});

// OUT: stored at zero with the status saying why, regardless of the run's number.
const out = storedSlateProjection(run, true);
assert.deepEqual(out, { projectionStatus: OUT_PROJECTION_STATUS, ourProj: 0, floorFpts: 0, medianFpts: 0, ceilingFpts: 0, boomRate: 0 });

// The write-time rule and the read-time rule agree on every shared field.
const readSide = zeroOutProjection({ projectionStatus: "historical", ourProj: 14.2, floorFpts: 6.1, ceilingFpts: 24.5, boomRate: 0.12 }, true);
for (const key of ["projectionStatus", "ourProj", "floorFpts", "ceilingFpts", "boomRate"] as const) {
  assert.equal(out[key], readSide[key], key);
}

// No projection is absence, never zero.
assert.deepEqual(storedSlateProjection(null, false), {
  projectionStatus: "unmatched", ourProj: null, floorFpts: null, medianFpts: null, ceilingFpts: null, boomRate: null,
});
// ...but an OUT player with no projection is still stored at zero: DK pays him zero either way.
assert.equal(storedSlateProjection(null, true).ourProj, 0);

console.log("test-nfl-out-projection: 5 assertions passed");

// --- A position average is not a projection of this player -----------------
// Measured on 2026 weeks 1-2 (n=435 position-prior rows, one per player-week):
// projected 7.15, scored 0.70, 92% scored <= 3; quarterbacks 13.97 -> 0.04
// with 52 of 52 at or under 3. `historical` rows on the same slates project
// 7.00 and score 5.28.
{
  const prior = { projectionStatus: POSITION_PRIOR_STATUS, ourProj: 13.97,
                  floorFpts: 6, ceilingFpts: 24, boomRate: 0.2 };

  const read = zeroOutProjection(prior, false);
  assert.equal(read.projectionStatus, UNSUPPORTED_PROJECTION_STATUS);
  assert.equal(read.ourProj, null, "absence, not zero: we are not claiming he scores nothing");
  assert.equal(read.floorFpts, null);
  assert.equal(read.ceilingFpts, null);
  assert.equal(read.boomRate, null);

  const stored = storedSlateProjection(
    { projectionStatus: POSITION_PRIOR_STATUS, modelProjFpts: 13.97, floorFpts: 6,
      medianFpts: 12, ceilingFpts: 24, boomRate: 0.2 }, false);
  assert.equal(stored.projectionStatus, UNSUPPORTED_PROJECTION_STATUS);
  assert.equal(stored.ourProj, null);
  assert.equal(stored.medianFpts, null);

  // Ruled out still wins: not playing is a stronger statement than "unknown".
  const out = zeroOutProjection(prior, true);
  assert.equal(out.projectionStatus, OUT_PROJECTION_STATUS);
  assert.equal(out.ourProj, 0);

  // A player with games of his own is untouched. Silencing hist_1_5 would
  // remove a genuine week-1 rookie starter along with the scratches.
  const real = { projectionStatus: "historical", ourProj: 13.97, floorFpts: 6, ceilingFpts: 24, boomRate: 0.2 };
  assert.deepEqual(zeroOutProjection(real, false), real);
  assert.equal(storedSlateProjection(
    { projectionStatus: "historical", modelProjFpts: 13.97, floorFpts: 6, medianFpts: 12,
      ceilingFpts: 24, boomRate: 0.2 }, false).ourProj, 13.97);

  // The seam for the frozen v4 study: this gates on the STATUS, never on the
  // game count, so a future prior that produces a number genuinely about the
  // player stamps its own status and is not silenced by this rule.
  assert.equal(isUnsupportedProjection(POSITION_PRIOR_STATUS), true);
  assert.equal(isUnsupportedProjection("historical"), false);
  assert.equal(isUnsupportedProjection("first_appearance_cohort"), false,
    "a differently-named future prior must not inherit this refusal");
  assert.equal(isUnsupportedProjection(OUT_PROJECTION_STATUS), false);

  console.log("Unsupported projection: a position average is withheld, not published.");
}
