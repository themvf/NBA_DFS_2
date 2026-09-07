/**
 * Grading a frozen pick'em recommendation.
 *
 * The design decision that makes this ledger worth having: it freezes BOTH
 * entries, not just the one recommended.
 *
 * A ledger that stores only what we advised can answer "did that card score
 * well", which is not the question this page exists to ask. The question is
 * whether deviating from the provably-optimal max-points entry was worth its
 * price. That is a PAIRED comparison, and the counterfactual has to be frozen
 * at the same moment as the recommendation or it is not a counterfactual at
 * all -- it is a card reconstructed after the results were known, from a model
 * that may since have changed. So every row carries the baseline entry, the
 * recommended entry, and the exact expected-points gap between them, all
 * stamped before the first kickoff.
 *
 * Three things are gradable here, and they are NOT equally strong. The UI has
 * to keep them apart:
 *
 *   1. PROBABILITY CALIBRATION (fully automatic, genuinely informative).
 *      Brier score of the win probabilities against real results. This grades
 *      nfl_game_win_probs, which is shared with the survivor page, so it is
 *      worth having regardless of anything else on this page.
 *
 *   2. PAIRED POINTS DELTA (fully automatic, weak on its own).
 *      Recommended score minus baseline score. Deviation is EXPECTED to lose
 *      points -- that is what it costs -- so a negative mean here confirms the
 *      arithmetic rather than refuting the strategy. Reporting it as if it were
 *      the verdict would be the mirror image of the mistake this repo has made
 *      before, where a favourable-looking aggregate was read as evidence.
 *
 *   3. POOL OUTCOME (manual, and the only real test).
 *      Whether the entry actually won. We cannot observe your pool's field, so
 *      this cannot be automated and is never inferred. A week with no reported
 *      finish stays unreported rather than being quietly treated as a loss.
 *
 * With at most 18 weeks a season, the settled sample will stay far below the
 * 30-observation floor this repo uses elsewhere before quoting a rate. So the
 * summary reports raw counts and is explicitly descriptive; it must never
 * present a win rate as though it settled anything.
 */

/** One frozen game inside a recommendation, joined to its result. */
export type GradedGameRow = {
  gameId: number;
  /** P(home wins) as frozen at recommendation time -- never recomputed. */
  pHome: number;
  provenance: string;
  /** Null until the game is final; null forever on a tie. */
  homeWon: boolean | null;
  baselinePickHome: boolean;
  baselineConfidence: number;
  recommendedPickHome: boolean;
  recommendedConfidence: number;
};

export type RecommendationGrade = {
  gamesTotal: number;
  gamesGraded: number;
  complete: boolean;
  baselinePoints: number;
  recommendedPoints: number;
  baselineCorrect: number;
  recommendedCorrect: number;
  /** Points a perfect card would have scored on the graded games. */
  maxPossiblePoints: number;
  /** Mean squared error of pHome against the result. Null with nothing graded. */
  brier: number | null;
  /**
   * Brier of a "no information" forecaster that says 50% on everything, over
   * the SAME graded games. The only fair reference: comparing our Brier to a
   * constant 0.25 would be comparing across different game sets.
   */
  coinflipBrier: number | null;
};

/**
 * Grade one recommendation.
 *
 * Ungraded games (not final, or a tie) are excluded from every total rather
 * than counted as losses, and `complete` says whether that happened. A pool
 * usually voids a tie; scoring it as a miss would silently penalise both
 * entries and corrupt the paired delta, which is the one number here that has
 * to be exact.
 */
export function gradeRecommendation(rows: GradedGameRow[]): RecommendationGrade {
  let gamesGraded = 0;
  let baselinePoints = 0;
  let recommendedPoints = 0;
  let baselineCorrect = 0;
  let recommendedCorrect = 0;
  let maxPossiblePoints = 0;
  let brierSum = 0;
  let coinflipSum = 0;

  for (const row of rows) {
    if (row.homeWon == null) continue;
    gamesGraded += 1;

    if (row.baselinePickHome === row.homeWon) {
      baselinePoints += row.baselineConfidence;
      baselineCorrect += 1;
    }
    if (row.recommendedPickHome === row.homeWon) {
      recommendedPoints += row.recommendedConfidence;
      recommendedCorrect += 1;
    }
    // A perfect card scores every weight it assigned, whichever entry we use --
    // both are permutations of the same multiset, so this is well defined.
    maxPossiblePoints += row.recommendedConfidence;

    const outcome = row.homeWon ? 1 : 0;
    brierSum += (row.pHome - outcome) ** 2;
    coinflipSum += (0.5 - outcome) ** 2;
  }

  return {
    gamesTotal: rows.length,
    gamesGraded,
    complete: gamesGraded === rows.length && rows.length > 0,
    baselinePoints,
    recommendedPoints,
    baselineCorrect,
    recommendedCorrect,
    maxPossiblePoints,
    brier: gamesGraded > 0 ? brierSum / gamesGraded : null,
    coinflipBrier: gamesGraded > 0 ? coinflipSum / gamesGraded : null,
  };
}

// ---------------------------------------------------------------------------
// Cross-week summary
// ---------------------------------------------------------------------------

export type SettledWeek = {
  week: number;
  objective: "ev" | "win";
  baselinePoints: number;
  recommendedPoints: number;
  /** Expected-points gap priced BEFORE kickoff. Negative = we paid for variance. */
  expectedPointsDelta: number;
  gamesGraded: number;
  brier: number | null;
  coinflipBrier: number | null;
  /** 1 = won the pool, 0 = did not, null = not reported. */
  wonPool: boolean | null;
  finishRank: number | null;
};

export type CalibrationBin = {
  label: string;
  lower: number;
  upper: number;
  n: number;
  meanForecast: number;
  hitRate: number;
};

export type LedgerSummary = {
  settledWeeks: number;
  gamesGraded: number;

  /** Realised points delta, recommended minus baseline. */
  meanPointsDelta: number | null;
  totalPointsDelta: number;
  weeksRecommendedBeatBaseline: number;
  weeksBaselineBeatRecommended: number;
  weeksLevel: number;

  /**
   * What the deviation was PRICED at before kickoff, summed. The realised
   * delta should scatter around this, not around zero -- deviation costs
   * expected points by construction. Comparing realised to zero would be
   * grading the strategy against a claim it never made.
   */
  totalExpectedPointsDelta: number;

  brier: number | null;
  coinflipBrier: number | null;
  calibration: CalibrationBin[];

  reportedFinishes: number;
  poolsWon: number;
  /** Only meaningful once finishes are reported; null otherwise. */
  poolWinRate: number | null;

  /** True while the sample is too small to support any rate claim. */
  descriptiveOnly: boolean;
};

/** Below this, the summary refuses to present a rate as a conclusion. */
export const MIN_SETTLED_FOR_RATES = 30;

const CALIBRATION_EDGES = [0.5, 0.55, 0.6, 0.65, 0.7, 0.75, 0.8, 1.0001];

/**
 * Reliability table over the FAVOURITE's probability.
 *
 * Folded to the favourite side (p mapped to max(p, 1-p)) because a bin holding
 * both 20% and 80% forecasts would average to 50% and look calibrated no
 * matter how wrong both halves were.
 */
export function calibrationBins(rows: GradedGameRow[]): CalibrationBin[] {
  const bins: Array<{ lower: number; upper: number; n: number; f: number; hits: number }> = [];
  for (let i = 0; i < CALIBRATION_EDGES.length - 1; i += 1) {
    bins.push({ lower: CALIBRATION_EDGES[i], upper: CALIBRATION_EDGES[i + 1], n: 0, f: 0, hits: 0 });
  }
  for (const row of rows) {
    if (row.homeWon == null) continue;
    const homeIsFavourite = row.pHome >= 0.5;
    const p = homeIsFavourite ? row.pHome : 1 - row.pHome;
    const favouriteWon = homeIsFavourite ? row.homeWon : !row.homeWon;
    const bin = bins.find((b) => p >= b.lower && p < b.upper);
    if (!bin) continue;
    bin.n += 1;
    bin.f += p;
    if (favouriteWon) bin.hits += 1;
  }
  return bins
    .filter((b) => b.n > 0)
    .map((b) => ({
      label: b.upper > 1 ? "80%+" : `${Math.round(b.lower * 100)}–${Math.round(b.upper * 100)}%`,
      lower: b.lower,
      upper: b.upper,
      n: b.n,
      meanForecast: b.f / b.n,
      hitRate: b.hits / b.n,
    }));
}

export function summarizeLedger(weeks: SettledWeek[], allRows: GradedGameRow[]): LedgerSummary {
  const settled = weeks.filter((w) => w.gamesGraded > 0);
  let totalDelta = 0;
  let beat = 0;
  let lost = 0;
  let level = 0;
  let expectedDelta = 0;
  let gamesGraded = 0;

  for (const w of settled) {
    const delta = w.recommendedPoints - w.baselinePoints;
    totalDelta += delta;
    expectedDelta += w.expectedPointsDelta;
    gamesGraded += w.gamesGraded;
    if (delta > 0) beat += 1;
    else if (delta < 0) lost += 1;
    else level += 1;
  }

  const graded = allRows.filter((r) => r.homeWon != null);
  let brierSum = 0;
  let coinflipSum = 0;
  for (const r of graded) {
    const outcome = r.homeWon ? 1 : 0;
    brierSum += (r.pHome - outcome) ** 2;
    coinflipSum += (0.5 - outcome) ** 2;
  }

  const reported = settled.filter((w) => w.wonPool != null);
  const won = reported.filter((w) => w.wonPool === true).length;

  return {
    settledWeeks: settled.length,
    gamesGraded,
    meanPointsDelta: settled.length > 0 ? totalDelta / settled.length : null,
    totalPointsDelta: totalDelta,
    weeksRecommendedBeatBaseline: beat,
    weeksBaselineBeatRecommended: lost,
    weeksLevel: level,
    totalExpectedPointsDelta: expectedDelta,
    brier: graded.length > 0 ? brierSum / graded.length : null,
    coinflipBrier: graded.length > 0 ? coinflipSum / graded.length : null,
    calibration: calibrationBins(allRows),
    reportedFinishes: reported.length,
    poolsWon: won,
    poolWinRate: reported.length > 0 ? won / reported.length : null,
    descriptiveOnly: settled.length < MIN_SETTLED_FOR_RATES,
  };
}

/**
 * The verdict line.
 *
 * Deliberately refuses to produce one in almost every realistic state. An NFL
 * season is 18 weeks, so a single season's ledger cannot clear a 30-week
 * floor no matter how it goes -- which means the honest output here is
 * "descriptive" essentially always, and a function that quietly started
 * declaring winners at n=6 would be the exact failure this repo keeps
 * catching in its own history.
 */
export function ledgerVerdict(summary: LedgerSummary): string {
  if (summary.settledWeeks === 0) {
    return "Nothing settled yet. Freeze a card before kickoff, then settle it once the games are final.";
  }
  const parts: string[] = [];

  if (summary.brier != null && summary.coinflipBrier != null) {
    const better = summary.brier < summary.coinflipBrier;
    parts.push(
      `Win probabilities: Brier ${summary.brier.toFixed(4)} over ${summary.gamesGraded} games, ` +
      `against ${summary.coinflipBrier.toFixed(4)} for a coin flip on the same games — ` +
      `${better ? "better than no information" : "no better than no information"}.`,
    );
  }

  if (summary.meanPointsDelta != null) {
    parts.push(
      `Deviation cost ${(-summary.totalExpectedPointsDelta).toFixed(2)} expected points across ` +
      `${summary.settledWeeks} week${summary.settledWeeks === 1 ? "" : "s"} and realised ` +
      `${summary.totalPointsDelta >= 0 ? "+" : ""}${summary.totalPointsDelta.toFixed(0)}. ` +
      `A negative realised delta is the expected result, not a failure — that is what the ` +
      `variance was bought with.`,
    );
  }

  if (summary.reportedFinishes === 0) {
    parts.push(
      "No pool finishes reported, so the only question that matters — did the entry win — is " +
      "unanswered. Points and calibration cannot substitute for it.",
    );
  } else {
    parts.push(
      `Pool finishes: ${summary.poolsWon} win${summary.poolsWon === 1 ? "" : "s"} in ` +
      `${summary.reportedFinishes} reported week${summary.reportedFinishes === 1 ? "" : "s"}. ` +
      (summary.descriptiveOnly
        ? `Far below the ${MIN_SETTLED_FOR_RATES}-week floor this repo uses before quoting a rate — ` +
          "descriptive only, and an 18-week season cannot reach that floor in one year."
        : "Sample is large enough to quote a rate."),
    );
  }

  return parts.join(" ");
}
