/**
 * Availability odds: the probability a player will still be on the board
 * when a specific future pick is on the clock.
 *
 * This is the direct analog of the "availability odds" signal on
 * stackedfantasy.com's War Room. It reuses ADP variance we already ingest
 * from Fantasy Football Calculator (`ingest/ff_independent.py`) but never
 * used: FFC's API returns `stdev`/`high`/`low`/`times_drafted` alongside the
 * mean `adp`, and the full raw row is already persisted in
 * `ff_player_rankings.source_row->'adp'`. No new ingestion is required.
 *
 * Model: treat a player's real draft position as approximately
 * Normal(mean, sd), where mean/sd come from FFC's 12-team ADP rescaled to
 * the draft's actual team count. We then condition on what the draft room
 * already knows -- the player was still undrafted as of the current pick --
 * by dividing survival probabilities. That conditioning is what makes this
 * more than a lookup table: a player who has already fallen well past their
 * expected range gets pushed toward "very likely still available soon"
 * rather than reusing a stale unconditional number.
 *
 * This is a decision aid, not a validated model -- there is no backtest
 * behind it the way the sports-betting side of this repo requires before a
 * signal can be called an edge. Treat probabilities as directional
 * estimates, and always show the underlying sample size so a thin-sample
 * player doesn't read as more certain than it is.
 */

// FFC ADP is captured for a 12-team snake format regardless of the target
// draft's team count (`ingest/ff_independent.py`'s FFC_ADP_URL hardcodes
// teams=12).
const FFC_REFERENCE_TEAM_COUNT = 12;

// Below this many observed real drafts, FFC's own stdev is noisy -- widen it
// rather than let a 3-draft sample produce a falsely tight, falsely
// confident probability.
const MIN_SAMPLE_FOR_FULL_TRUST = 30;
const MIN_SAMPLE_FLOOR = 5;

// No matter how tight FFC's reported stdev is (elite consensus picks can be
// sub-1.0), never let the model claim near-certainty at the pick level --
// real drafts still have single-pick variance the reported stdev alone can
// understate.
const ABSOLUTE_STDEV_FLOOR_PICKS = 1.25;

export type AvailabilityOddsInput = {
  adp: number | null;
  adpStdev: number | null;
  adpSampleSize: number | null;
};

export type AvailabilityOddsContext = {
  /** The next pick to be made; the player is known to be undrafted through this point. */
  currentPick: number;
  /** The future pick we're asking "will they still be here" for. */
  targetPick: number;
  /** The actual draft's team count -- used to rescale FFC's 12-team ADP. */
  teamCount: number;
};

export type AvailabilityOdds = {
  /** P(still available at targetPick | still available at currentPick), 0..1. */
  probability: number;
  adjustedAdp: number;
  adjustedStdev: number;
  sampleSize: number | null;
  confidence: "low" | "medium" | "high";
};

// Abramowitz & Stegun 7.1.26 erf approximation, |error| <= 1.5e-7 -- no stats
// dependency in this repo, and this is the standard closed-form choice.
function erf(x: number): number {
  const sign = x < 0 ? -1 : 1;
  const ax = Math.abs(x);
  const a1 = 0.254829592;
  const a2 = -0.284496736;
  const a3 = 1.421413741;
  const a4 = -1.453152027;
  const a5 = 1.061405429;
  const p = 0.3275911;
  const t = 1 / (1 + p * ax);
  const y = 1 - (((((a5 * t + a4) * t) + a3) * t + a2) * t + a1) * t * Math.exp(-ax * ax);
  return sign * y;
}

function normalCdf(z: number): number {
  return 0.5 * (1 + erf(z / Math.SQRT2));
}

// P(actual draft position >= pick) -- i.e. the player is still on the board
// when `pick` is on the clock. Continuity-corrected: pick N is treated as
// covering the continuous interval [N-0.5, N+0.5).
function survivalProbability(pick: number, mean: number, sd: number): number {
  if (sd <= 0) return pick <= mean ? 1 : 0;
  const z = (pick - 0.5 - mean) / sd;
  return 1 - normalCdf(z);
}

export function computeAvailabilityOdds(
  input: AvailabilityOddsInput,
  context: AvailabilityOddsContext,
): AvailabilityOdds | null {
  const { adp, adpStdev, adpSampleSize } = input;
  const { currentPick, targetPick, teamCount } = context;
  if (adp === null || !Number.isFinite(adp)) return null;
  // Require a real observed stdev -- fabricating one for players FFC didn't
  // report variance for would manufacture false confidence.
  if (adpStdev === null || !Number.isFinite(adpStdev) || adpStdev <= 0) return null;
  if (!Number.isFinite(currentPick) || !Number.isFinite(targetPick) || targetPick < currentPick) return null;
  if (!Number.isInteger(teamCount) || teamCount < 2) return null;

  // Round-preserving rescale: a player taken in round R of a 12-team draft
  // is treated as a round-R player in an N-team draft, not the same literal
  // overall pick number.
  const scale = teamCount / FFC_REFERENCE_TEAM_COUNT;
  const adjustedAdp = 1 + (adp - 1) * scale;

  const sampleSize = adpSampleSize !== null && Number.isFinite(adpSampleSize) && adpSampleSize > 0
    ? Math.round(adpSampleSize)
    : null;
  const shrinkMultiplier = sampleSize
    ? Math.max(1, Math.sqrt(MIN_SAMPLE_FOR_FULL_TRUST / Math.max(sampleSize, MIN_SAMPLE_FLOOR)))
    : 1.6; // sample size unknown -- treat as thin, same as a ~12-draft sample
  const adjustedStdev = Math.max(ABSOLUTE_STDEV_FLOOR_PICKS * scale, adpStdev * scale * shrinkMultiplier);

  const denominator = Math.max(survivalProbability(currentPick, adjustedAdp, adjustedStdev), 1e-6);
  const numerator = survivalProbability(targetPick, adjustedAdp, adjustedStdev);
  const probability = Math.min(1, Math.max(0, numerator / denominator));

  const confidence: AvailabilityOdds["confidence"] = sampleSize === null
    ? "low"
    : sampleSize < 10 ? "low" : sampleSize < 50 ? "medium" : "high";

  return { probability, adjustedAdp, adjustedStdev, sampleSize, confidence };
}
