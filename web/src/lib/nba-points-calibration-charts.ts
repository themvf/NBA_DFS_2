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
