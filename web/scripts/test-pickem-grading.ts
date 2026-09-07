/**
 * Tests for pick'em ledger grading.
 *
 * The load-bearing checks here are the refusals, not the arithmetic:
 *
 *   - a tie is EXCLUDED from both cards rather than scored as a miss, because
 *     scoring it as a loss would penalise both entries and corrupt the paired
 *     delta, which is the one number that has to be exact;
 *   - an unreported pool finish stays unreported and never becomes a loss;
 *   - the verdict refuses to quote a rate below the sample floor, and says so
 *     even when the record looks good.
 *
 * Run: npm run test:pickem-grading
 */

import {
  MIN_SETTLED_FOR_RATES,
  calibrationBins,
  gradeRecommendation,
  ledgerVerdict,
  summarizeLedger,
  type GradedGameRow,
  type SettledWeek,
} from "../src/lib/nfl/pickem-grading";

let passed = 0;
let failed = 0;

function check(name: string, condition: boolean, detail?: string) {
  if (condition) {
    passed += 1;
    console.log(`  PASS  ${name}`);
  } else {
    failed += 1;
    console.error(`  FAIL  ${name}${detail ? ` -- ${detail}` : ""}`);
  }
}

function close(a: number, b: number, tol = 1e-9) {
  return Math.abs(a - b) < tol;
}

function row(over: Partial<GradedGameRow> & { gameId: number }): GradedGameRow {
  return {
    pHome: 0.6,
    provenance: "market_ml_novig",
    homeWon: null,
    baselinePickHome: true,
    baselineConfidence: 1,
    recommendedPickHome: true,
    recommendedConfidence: 1,
    ...over,
  };
}

// ---------------------------------------------------------------------------
console.log("\nScoring a card");
// ---------------------------------------------------------------------------

{
  const rows: GradedGameRow[] = [
    row({ gameId: 1, pHome: 0.8, homeWon: true, baselineConfidence: 3, recommendedConfidence: 3 }),
    row({ gameId: 2, pHome: 0.6, homeWon: false, baselineConfidence: 2, recommendedConfidence: 1 }),
    row({ gameId: 3, pHome: 0.55, homeWon: true, baselineConfidence: 1, recommendedConfidence: 2 }),
  ];
  const g = gradeRecommendation(rows);
  // Baseline: game1 hit (3) + game2 miss + game3 hit (1) = 4
  // Recommended: game1 hit (3) + game2 miss + game3 hit (2) = 5
  check("baseline points are summed from its own weights", g.baselinePoints === 4, String(g.baselinePoints));
  check("recommended points are summed from its own weights", g.recommendedPoints === 5, String(g.recommendedPoints));
  check("hit counts are independent of the weights", g.baselineCorrect === 2 && g.recommendedCorrect === 2);
  check("max possible is the full weight multiset", g.maxPossiblePoints === 6, String(g.maxPossiblePoints));
  check("all three games graded", g.gamesGraded === 3 && g.complete);
  check(
    "the two entries are permutations of the same weights",
    rows.reduce((s, r) => s + r.baselineConfidence, 0) ===
      rows.reduce((s, r) => s + r.recommendedConfidence, 0),
  );
}

{
  // A picked-away game must score when the AWAY side wins.
  const rows = [
    row({ gameId: 1, pHome: 0.4, homeWon: false, baselinePickHome: false, recommendedPickHome: false, baselineConfidence: 5, recommendedConfidence: 5 }),
  ];
  const g = gradeRecommendation(rows);
  check("an away pick scores when the away team wins", g.baselinePoints === 5 && g.recommendedPoints === 5);
}

// ---------------------------------------------------------------------------
console.log("\nTies and incomplete weeks");
// ---------------------------------------------------------------------------

{
  const rows: GradedGameRow[] = [
    row({ gameId: 1, homeWon: true, baselineConfidence: 2, recommendedConfidence: 2 }),
    // A tie: homeWon stays null.
    row({ gameId: 2, homeWon: null, baselineConfidence: 3, recommendedConfidence: 1 }),
    row({ gameId: 3, homeWon: null, baselineConfidence: 1, recommendedConfidence: 3 }),
  ];
  const g = gradeRecommendation(rows);
  check("ungraded games are excluded, not counted as misses", g.gamesGraded === 1);
  check("an incomplete week is flagged incomplete", !g.complete);
  check("points come only from graded games", g.baselinePoints === 2 && g.recommendedPoints === 2);
  check(
    "max possible covers only graded games",
    g.maxPossiblePoints === 2,
    String(g.maxPossiblePoints),
  );
  check(
    "a tie does not skew the paired delta",
    g.recommendedPoints - g.baselinePoints === 0,
  );
}

{
  const g = gradeRecommendation([row({ gameId: 1, homeWon: null })]);
  check("nothing graded yields null Brier rather than 0", g.brier === null && g.coinflipBrier === null);
  check("nothing graded is not 'complete'", !g.complete);
}

// ---------------------------------------------------------------------------
console.log("\nBrier and its reference");
// ---------------------------------------------------------------------------

{
  const rows = [
    row({ gameId: 1, pHome: 1, homeWon: true }),
    row({ gameId: 2, pHome: 0, homeWon: false }),
  ];
  const g = gradeRecommendation(rows);
  check("a perfect forecaster scores Brier 0", close(g.brier!, 0));
  check("the coin-flip reference is 0.25 on the same games", close(g.coinflipBrier!, 0.25));
}

{
  const rows = [
    row({ gameId: 1, pHome: 0.75, homeWon: true }),
    row({ gameId: 2, pHome: 0.75, homeWon: false }),
  ];
  const g = gradeRecommendation(rows);
  // (0.25^2 + 0.75^2) / 2 = (0.0625 + 0.5625)/2 = 0.3125
  check("Brier is the mean squared error", close(g.brier!, 0.3125), String(g.brier));
  check(
    "a badly calibrated forecaster loses to the coin flip",
    g.brier! > g.coinflipBrier!,
  );
}

{
  // The reference must be computed over the SAME games, not a constant 0.25.
  // With every graded game landing the same way, the coin-flip Brier is still
  // 0.25 -- but the point is that it is derived, not assumed.
  const g = gradeRecommendation([
    row({ gameId: 1, pHome: 0.9, homeWon: true }),
    row({ gameId: 2, pHome: 0.9, homeWon: true }),
    row({ gameId: 3, pHome: 0.9, homeWon: null }),
  ]);
  check("the reference ignores ungraded games too", close(g.coinflipBrier!, 0.25) && g.gamesGraded === 2);
  check("a confident correct forecaster beats the coin flip", g.brier! < g.coinflipBrier!);
}

// ---------------------------------------------------------------------------
console.log("\nCalibration bins");
// ---------------------------------------------------------------------------

{
  const rows: GradedGameRow[] = [];
  // 10 home favourites at 80%, 8 win.
  for (let i = 0; i < 10; i += 1) rows.push(row({ gameId: i, pHome: 0.82, homeWon: i < 8 }));
  // 10 AWAY favourites at 80% (pHome 0.18), 8 of the favourites win.
  for (let i = 0; i < 10; i += 1) rows.push(row({ gameId: 100 + i, pHome: 0.18, homeWon: !(i < 8) }));

  const bins = calibrationBins(rows);
  check("bins are folded onto the favourite side", bins.length === 1, `${bins.length} bins`);
  check("all 20 games land in the 80%+ bin", bins[0].n === 20);
  check("hit rate is measured on the favourite", close(bins[0].hitRate, 0.8), String(bins[0].hitRate));
  check("mean forecast is the favourite's probability", close(bins[0].meanForecast, 0.82, 1e-6));
  check(
    "folding matters -- unfolded, these would average to 50%",
    Math.abs(rows.reduce((s, r) => s + r.pHome, 0) / rows.length - 0.5) < 1e-9,
  );
}

{
  const bins = calibrationBins([row({ gameId: 1, pHome: 0.6, homeWon: true })]);
  check("a 60% forecast lands in the 60-65% bin", bins.length === 1 && bins[0].label === "60–65%");
  check("empty bins are dropped rather than shown as 0/0", bins.every((b) => b.n > 0));
}

// ---------------------------------------------------------------------------
console.log("\nCross-week summary");
// ---------------------------------------------------------------------------

function week(over: Partial<SettledWeek> & { week: number }): SettledWeek {
  return {
    objective: "win",
    baselinePoints: 90,
    recommendedPoints: 88,
    expectedPointsDelta: -3,
    gamesGraded: 16,
    brier: 0.2,
    coinflipBrier: 0.25,
    wonPool: null,
    finishRank: null,
    ...over,
  };
}

{
  const weeks = [
    week({ week: 1, baselinePoints: 90, recommendedPoints: 95 }),
    week({ week: 2, baselinePoints: 90, recommendedPoints: 80 }),
    week({ week: 3, baselinePoints: 90, recommendedPoints: 90 }),
  ];
  const s = summarizeLedger(weeks, []);
  check("weeks are tallied win/loss/level on the paired delta", s.weeksRecommendedBeatBaseline === 1 && s.weeksBaselineBeatRecommended === 1 && s.weeksLevel === 1);
  check("total realised delta is summed", s.totalPointsDelta === -5, String(s.totalPointsDelta));
  check("mean realised delta divides by settled weeks", close(s.meanPointsDelta!, -5 / 3));
  check(
    "the pre-kickoff price is carried alongside, so the comparison is against it and not against zero",
    s.totalExpectedPointsDelta === -9,
    String(s.totalExpectedPointsDelta),
  );
}

{
  const weeks = [week({ week: 1, gamesGraded: 0 }), week({ week: 2 })];
  const s = summarizeLedger(weeks, []);
  check("weeks with nothing graded are not counted as settled", s.settledWeeks === 1);
}

{
  const weeks = [
    week({ week: 1, wonPool: true }),
    week({ week: 2, wonPool: false }),
    week({ week: 3, wonPool: null }),
  ];
  const s = summarizeLedger(weeks, []);
  check("an unreported finish is not counted as a loss", s.reportedFinishes === 2);
  check("wins are counted only among reported weeks", s.poolsWon === 1);
  check("the win rate divides by reported, not settled", close(s.poolWinRate!, 0.5));
}

{
  const s = summarizeLedger([week({ week: 1 })], []);
  check("with no finishes reported the win rate is null, never 0", s.poolWinRate === null);
  check("a one-week ledger is descriptive only", s.descriptiveOnly);
}

// ---------------------------------------------------------------------------
console.log("\nVerdict honesty");
// ---------------------------------------------------------------------------

{
  const empty = summarizeLedger([], []);
  check("an empty ledger says so plainly", ledgerVerdict(empty).includes("Nothing settled yet"));
}

{
  const weeks = [week({ week: 1, wonPool: true }), week({ week: 2, wonPool: true })];
  const v = ledgerVerdict(summarizeLedger(weeks, []));
  check(
    "a 2-for-2 record is still reported as descriptive, not as a result",
    v.includes("descriptive only"),
    v,
  );
  check(
    "the verdict names the floor it fails to clear",
    v.includes(String(MIN_SETTLED_FOR_RATES)),
  );
  check(
    "the verdict admits an 18-week season cannot reach that floor",
    v.includes("cannot reach that floor"),
  );
}

{
  const v = ledgerVerdict(summarizeLedger([week({ week: 1 })], []));
  check(
    "with no finish reported, the verdict says the real question is unanswered",
    v.includes("unanswered"),
    v,
  );
  check(
    "a negative realised delta is explained as expected, not as failure",
    v.includes("expected result"),
  );
}

{
  const rows = [row({ gameId: 1, pHome: 0.9, homeWon: true }), row({ gameId: 2, pHome: 0.9, homeWon: true })];
  const v = ledgerVerdict(summarizeLedger([week({ week: 1 })], rows));
  check("calibration is reported against the coin flip on the same games", v.includes("coin flip"));
  check("a good Brier is stated as better than no information", v.includes("better than no information"));
}

console.log(`\n${passed} passed, ${failed} failed\n`);
if (failed > 0) process.exit(1);
