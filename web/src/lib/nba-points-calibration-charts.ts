/**
 * Chart data for the Top Scorers calibration panel, from the graded rows of
 * nba_points_calibration (model/nba_points_calibration.py). Pure.
 *
 * - Reliability diagram: for each reading (over the line, 20+ ... 40+), the
 *   graded rows are binned by the chance the books stated, and each bin is
 *   plotted at its MEAN stated chance (not the bin centre) against the share
 *   of rows where it happened, with n. A calibrated reading sits on the
 *   diagonal. Bins under MIN_BIN_N are kept but flagged thin.
 * - Actual-vs-line scatter: one point per graded player-game, the line on x
 *   and the points scored on y, split by how long before tip the graded
 *   capture was taken, so a stale morning read can be told from a close one.
 */

export type GradedRow = {
  gameDate: string;
  player: string;
  line: number;
  actual: number;
  minutesBeforeTip: number;
  /** The ladder's P10 and P90; null when the ladder did not reach them. */
  floor: number | null;
  ceiling: number | null;
  pOver: number | null;
  p20: number | null;
  p25: number | null;
  p30: number | null;
  p35: number | null;
  p40: number | null;
};

export type Reading = { key: string; label: string; stated: (r: GradedRow) => number | null; hit: (r: GradedRow) => boolean };

export const READINGS: Reading[] = [
  { key: "over", label: "Over the line", stated: (r) => r.pOver, hit: (r) => r.actual > r.line },
  { key: "20", label: "20+ points", stated: (r) => r.p20, hit: (r) => r.actual >= 20 },
  { key: "25", label: "25+ points", stated: (r) => r.p25, hit: (r) => r.actual >= 25 },
  { key: "30", label: "30+ points", stated: (r) => r.p30, hit: (r) => r.actual >= 30 },
  { key: "35", label: "35+ points", stated: (r) => r.p35, hit: (r) => r.actual >= 35 },
  { key: "40", label: "40+ points", stated: (r) => r.p40, hit: (r) => r.actual >= 40 },
];

export const BIN_WIDTH = 0.1;
/** Below this many rows a bin is drawn hollow and the tooltip says so. */
export const MIN_BIN_N = 20;
/** The scatter shows the newest nights only; the reliability bins use every night. */
export const SCATTER_NIGHTS = 14;
/** A capture inside this many minutes of tip counts as a closing read. */
export const CLOSE_MINUTES = 120;

export type ReliabilityBin = {
  lo: number;
  hi: number;
  stated: number;
  observed: number;
  n: number;
  thin: boolean;
};

export type ReliabilitySeries = { key: string; label: string; n: number; bins: ReliabilityBin[] };

export function reliabilityBins(rows: GradedRow[], reading: Reading, binWidth = BIN_WIDTH): ReliabilityBin[] {
  const bins = new Map<number, { sum: number; hits: number; n: number }>();
  for (const row of rows) {
    const p = reading.stated(row);
    if (p == null || !Number.isFinite(p)) continue;
    // 1.0 lands in the top bin, not a bin of its own.
    const index = Math.min(Math.floor(p / binWidth + 1e-9), Math.round(1 / binWidth) - 1);
    const b = bins.get(index) ?? { sum: 0, hits: 0, n: 0 };
    b.sum += p;
    b.n += 1;
    if (reading.hit(row)) b.hits += 1;
    bins.set(index, b);
  }
  return [...bins.entries()]
    .sort((a, b) => a[0] - b[0])
    .map(([index, b]) => ({
      // Rounded so 3 * 0.1 is 0.3, not 0.30000000000000004, in keys and labels.
      lo: Math.round(index * binWidth * 1000) / 1000,
      hi: Math.round((index + 1) * binWidth * 1000) / 1000,
      stated: b.sum / b.n,
      observed: b.hits / b.n,
      n: b.n,
      thin: b.n < MIN_BIN_N,
    }));
}

export function reliabilitySeries(rows: GradedRow[]): ReliabilitySeries[] {
  return READINGS.map((reading) => {
    const bins = reliabilityBins(rows, reading);
    return { key: reading.key, label: reading.label, n: bins.reduce((s, b) => s + b.n, 0), bins };
  });
}

export type ScatterPoint = {
  line: number;
  actual: number;
  player: string;
  gameDate: string;
  minutesBeforeTip: number;
  /** "close": captured within CLOSE_MINUTES of tip; "early": before that. */
  group: "close" | "early";
};

export type ScatterData = {
  points: ScatterPoint[];
  nights: number;
  firstDate: string | null;
  lastDate: string | null;
  /** Mean of actual minus line over the shown points. */
  bias: number | null;
  /** Mean absolute difference over the shown points. */
  mae: number | null;
  maxValue: number;
};

export function scatterData(rows: GradedRow[], nights = SCATTER_NIGHTS): ScatterData {
  const dates = [...new Set(rows.map((r) => r.gameDate))].sort().slice(-nights);
  const keep = new Set(dates);
  const points = rows
    .filter((r) => keep.has(r.gameDate))
    .map((r) => ({
      line: r.line,
      actual: r.actual,
      player: r.player,
      gameDate: r.gameDate,
      minutesBeforeTip: r.minutesBeforeTip,
      group: (r.minutesBeforeTip <= CLOSE_MINUTES ? "close" : "early") as ScatterPoint["group"],
    }));
  const n = points.length;
  const bias = n ? points.reduce((s, p) => s + (p.actual - p.line), 0) / n : null;
  const mae = n ? points.reduce((s, p) => s + Math.abs(p.actual - p.line), 0) / n : null;
  const maxValue = points.reduce((m, p) => Math.max(m, p.line, p.actual), 0);
  return {
    points,
    nights: dates.length,
    firstDate: dates[0] ?? null,
    lastDate: dates[dates.length - 1] ?? null,
    bias,
    mae,
    maxValue: Math.ceil((maxValue + 1) / 5) * 5,
  };
}

/**
 * Interval coverage: where the actual landed against the ladder's floor (P10)
 * and ceiling (P90), over rows that carry both. The ladder claims 10% below,
 * 80% inside, 10% above. "Inside" is floor <= actual <= ceiling.
 */
export type CoverageData = {
  n: number;
  below: number;
  inside: number;
  above: number;
  target: { below: number; inside: number; above: number };
};

export const COVERAGE_TARGET = { below: 0.1, inside: 0.8, above: 0.1 };

export function coverageData(rows: GradedRow[]): CoverageData {
  let below = 0;
  let inside = 0;
  let above = 0;
  for (const r of rows) {
    if (r.floor == null || r.ceiling == null) continue;
    if (r.actual < r.floor) below += 1;
    else if (r.actual > r.ceiling) above += 1;
    else inside += 1;
  }
  const n = below + inside + above;
  return {
    n,
    below: n ? below / n : 0,
    inside: n ? inside / n : 0,
    above: n ? above / n : 0,
    target: COVERAGE_TARGET,
  };
}

/**
 * Tail calibration over time: for each reading, one point per graded night
 * with that night's mean stated chance and hit share (noisy, small n), plus the
 * running means over every night so far (what the pooled table shows, as a
 * line that should converge).
 */
export type TailPoint = {
  date: string;
  n: number;
  statedNight: number;
  observedNight: number;
  nCum: number;
  statedCum: number;
  observedCum: number;
};

export type TailSeries = { key: string; label: string; n: number; points: TailPoint[] };

export function tailTrend(rows: GradedRow[]): TailSeries[] {
  const dates = [...new Set(rows.map((r) => r.gameDate))].sort();
  return READINGS.map((reading) => {
    const points: TailPoint[] = [];
    let nCum = 0;
    let statedSum = 0;
    let hits = 0;
    for (const date of dates) {
      let n = 0;
      let stated = 0;
      let hit = 0;
      for (const r of rows) {
        if (r.gameDate !== date) continue;
        const p = reading.stated(r);
        if (p == null || !Number.isFinite(p)) continue;
        n += 1;
        stated += p;
        if (reading.hit(r)) hit += 1;
      }
      if (n === 0) continue;
      nCum += n;
      statedSum += stated;
      hits += hit;
      points.push({
        date,
        n,
        statedNight: stated / n,
        observedNight: hit / n,
        nCum,
        statedCum: statedSum / nCum,
        observedCum: hits / nCum,
      });
    }
    return { key: reading.key, label: reading.label, n: nCum, points };
  });
}
