/**
 * Pick'em pool policy: the constants, the defaults, and -- the part that
 * matters most -- an explicit statement of what has been validated and what
 * has not.
 *
 * This repo's standing discipline is that a tool which cannot be graded is a
 * toy, and that a number nobody measured must not be rendered like a number
 * somebody did. Applied honestly here, that means:
 *
 *   PROVED (no simulation, no data, cannot be wrong):
 *     - the EV-optimal entry is the probability-sorted one (rearrangement
 *       inequality)
 *     - the exact expected-points cost of any swap or flip
 *     - the closed-form prize share for R rivals given one rival's score
 *       distribution
 *
 *   MODELED (correct given its assumptions, assumptions never measured HERE):
 *     - that a bigger pool rewards more deviation. This is Clair & Letscher's
 *       central result and it reproduces cleanly in the simulator, but the
 *       simulator's field is a stated prior, not an observation.
 *
 *   UNMEASURED (the honest gap):
 *     - FIELD_FAVORITE_BIAS and FIELD_SKILL_SIGMA. There is no pick'em pick-
 *       share feed in this repo. The survivor popularity feed exists but is a
 *       DIFFERENT distribution -- survivor share is concentrated on a handful
 *       of teams and warped by future-team value -- so using it here would be
 *       exactly the silent substitution the MLB point-in-time work banned.
 *     - whether any of this beats naive EV-max in a real pool. Zero settled
 *       pick'em entries exist. No backtest has been run.
 *
 * So the page ships as decision support, badged RESEARCH, and it never tells
 * anyone a deviation is correct -- only what it costs and what the simulator
 * thinks it buys.
 */

import type { FieldModel, PoolFormat } from "./pickem-strategy";

export const MODEL_VERSION = "pickem-v1";

/**
 * Public over-backing of favourites, on the logit scale.
 *
 * 1.30 maps a 60% favourite to ~65% of the field and an 85% favourite to
 * ~90%. That shape matches the direction every published pool-strategy source
 * describes, and the magnitude is a judgement call, not a fit. It is exposed
 * as a UI control precisely because it is the weakest link: set it to 1.00 and
 * the field becomes a mirror of the market, which is the conservative case in
 * which contrarian value nearly vanishes.
 */
export const FIELD_FAVORITE_BIAS = 1.3;

/**
 * Opponent skill spread, in logits. 0.35 gives opponents materially different
 * confidence orderings on near-coin-flip games while leaving the top and
 * bottom of the slate largely agreed -- which is what real pool entries look
 * like. Also unmeasured.
 */
export const FIELD_SKILL_SIGMA = 0.35;

/**
 * Fraction of rivals assumed to submit the exact all-favourites card.
 *
 * The previous implicit value was 0 -- not by choice but by omission, because
 * the field had no chalk concept at all. That is a worse prior than any
 * non-zero number: drawing each game independently gives a rival about a one
 * in a thousand chance of landing on chalk over a 16-game slate, while in a
 * real pool taking every favourite is the most common entry there is.
 *
 * 0.25 is a stated prior and remains unmeasured. It is exposed in the UI
 * because the recommendation genuinely turns on it: `analyze-nfl-how-many-dogs`
 * found that at 0% chalk the best card at a 50-entry pool is zero flips, while
 * at 25% it is one flip worth 5.6x as much. Set it to 0 for the conservative
 * case.
 */
export const FIELD_CHALK_FRACTION = 0.25;

export const DEFAULT_FIELD: FieldModel = {
  favoriteBias: FIELD_FAVORITE_BIAS,
  skillSigma: FIELD_SKILL_SIGMA,
  chalkFraction: FIELD_CHALK_FRACTION,
};

/**
 * The odd/even sawtooth is NOT hard-coded, deliberately.
 *
 * An even number of side flips can land you exactly level with the chalk block
 * and split the prize with all of it; an odd number cannot, because
 *
 *     yourScore - chalkScore = 2 * (dogs that hit) - k
 *
 * is zero only for even k. Once the field model carries a chalk block the
 * simulator reproduces this on its own -- measured on a 2025-shaped slate at a
 * 50-entry pool with 25% chalk: k=0 0.65%, k=1 3.84%, k=2 3.47%, k=3 4.62%,
 * k=4 3.86%. A constant asserting the same thing would be a second source of
 * truth that could silently drift from the model, so there isn't one.
 *
 * What the sawtooth DOES require is a search that can cross it: a purely
 * greedy hill-climb stops at one flip because the step to two looks like a
 * loss, and never reaches three. That is why `optimizeEntry` falls back to a
 * two-move lookahead before giving up.
 */
export const LOOKAHEAD_PAIRS = true;

/** Monte Carlo size. 4,000 keeps a full re-optimize under ~1s in the browser. */
export const DEFAULT_SIMS = 4000;

/**
 * Opponents actually simulated per world. Beyond this the rival count is
 * handled by the closed-form prize share rather than by drawing more entries,
 * so a 5,000-person pool costs the same as a 100-person one.
 */
export const SAMPLE_OPPONENTS = 240;

/** Most deviations the optimizer will stack before it stops. */
export const MAX_DEVIATIONS = 4;

/**
 * Below this pool size, deviation is not worth considering.
 *
 * The survivor study measured its own analogue of this and found the original
 * guess wrong by an order of magnitude, so this number is deliberately stated
 * as a floor from theory rather than a fitted threshold: with few enough
 * rivals the field max sits close to your own score, and giving up expected
 * points to add variance moves you away from a lead you already have. In a
 * 6-person pool, submit the EV-optimal entry.
 */
export const DEVIATION_MIN_POOL = 12;

/** No component of this page has been graded against a settled pool. */
export const PICKEM_SEASONS_OBSERVED = 0;
export const PICKEM_SEASONS_REQUIRED = 2;
export const PICKEM_IS_VALIDATED = PICKEM_SEASONS_OBSERVED >= PICKEM_SEASONS_REQUIRED;

export type Objective = "ev" | "win";

/**
 * The default objective.
 *
 * `ev` -- maximize expected points. Provably optimal, and the right answer in
 * a small pool or a season-long cumulative standings pool.
 * `win` -- maximize expected share of a winner-take-all prize. The right
 * QUESTION in a weekly-prize pool of any size, answered by a simulator whose
 * field model is unmeasured.
 *
 * Defaults to `ev` at every pool size. Nothing here has been validated, and a
 * tool that defaults to its unvalidated mode is asserting something it has not
 * earned. The switch exists so the user chooses it deliberately.
 */
export function defaultObjective(): Objective {
  return "ev";
}

export function poolAdvisory(poolEntries: number | null, format: PoolFormat): string {
  if (poolEntries == null || !Number.isFinite(poolEntries)) {
    return "Enter your pool size — how much deviation is worth paying for depends almost entirely on it.";
  }
  if (poolEntries <= 2) {
    return "Head-to-head: maximize expected points. With one rival there is no field to differentiate from.";
  }
  if (poolEntries < DEVIATION_MIN_POOL) {
    return (
      `${poolEntries} entries is small. The field's best score will usually land near your own, ` +
      `so points given up to buy variance are mostly just points given up. Take the EV-optimal entry.`
    );
  }
  if (poolEntries < 60) {
    return (
      `${poolEntries} entries. Someone will beat the EV-optimal score fairly often, so cheap ` +
      `differentiation starts to pay — but only the cheap kind. Deviate in the flat middle of the ` +
      `slate, not on the games you are most sure of.`
    );
  }
  return (
    `${poolEntries} entries. To win you must beat the best of ${poolEntries - 1} others, which ` +
    `usually takes a near-perfect card. Submitting the same entry as everyone else means splitting ` +
    `the same coin flips${format === "confidence" ? " and the same confidence order" : ""}. ` +
    `This is where deviation earns its cost — and where the field model below is doing the most work, ` +
    `so treat the numbers as an ordering, not a forecast.`
  );
}

/**
 * Season-long cumulative pools are a different game and the page has to say so.
 *
 * Over 18 weeks your total is a sum of ~270 weighted Bernoullis; both your
 * score and the field's tighten around their means relative to the spread of a
 * single week, so week-by-week variance-seeking mostly cancels while its EV
 * cost accumulates every week. The correct dynamic is the standard one: play
 * near-EV while in contention, and buy variance in proportion to how far
 * behind you are with how few weeks left -- which is a sequential problem this
 * page does not solve and does not pretend to.
 */
export const SEASON_LONG_NOTE =
  "In a season-long cumulative pool, play close to the EV-optimal entry. Weekly variance largely " +
  "averages out over 18 weeks while the EV you paid for it does not. Buy variance only when you are " +
  "behind late — this page optimizes one week at a time and does not model that sequence.";
