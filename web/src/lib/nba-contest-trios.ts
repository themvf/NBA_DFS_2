// Plain tallies of the winner-take-all contest grades for the page. The registered
// paired test with its night-resampled interval is `python -m research.nba_wta_trios report`
// (docs/nba-wta-trios.md); this file only counts, so the two cannot disagree on a number
// the page shows without the interval.

export const REVIEW_AFTER_NIGHTS = 30;
export const PRIMARY_N = 5;

type Side = { best: number; percentile: number; inContention: boolean; realizedShare: number };
export type GradeCells = { byN: Record<string, { ours: Side; baseline: Side }> };

export type ContestTally = {
  nights: number;
  oursPrizes: number;
  baselinePrizes: number;
  oursAhead: number;
  baselineAhead: number;
  oursPercentile: number | null;
  baselinePercentile: number | null;
  oursContention: number;
  baselineContention: number;
};

export function tallyContest(grades: GradeCells[], n: number = PRIMARY_N): ContestTally {
  const cells = grades.map((g) => g.byN[String(n)]).filter((c): c is { ours: Side; baseline: Side } => Boolean(c));
  const sum = (f: (c: { ours: Side; baseline: Side }) => number) => cells.reduce((a, c) => a + f(c), 0);
  return {
    nights: cells.length,
    oursPrizes: sum((c) => c.ours.realizedShare),
    baselinePrizes: sum((c) => c.baseline.realizedShare),
    oursAhead: cells.filter((c) => c.ours.realizedShare > c.baseline.realizedShare).length,
    baselineAhead: cells.filter((c) => c.baseline.realizedShare > c.ours.realizedShare).length,
    oursPercentile: cells.length ? sum((c) => c.ours.percentile) / cells.length : null,
    baselinePercentile: cells.length ? sum((c) => c.baseline.percentile) / cells.length : null,
    oursContention: cells.filter((c) => c.ours.inContention).length,
    baselineContention: cells.filter((c) => c.baseline.inContention).length,
  };
}

/** "6.2%" for a prize share; shares under 0.1% read "<0.1%" rather than a misleading 0. */
export function sharePct(v: number): string {
  if (v <= 0) return "0%";
  if (v < 0.001) return "<0.1%";
  return `${(v * 100).toFixed(1)}%`;
}
