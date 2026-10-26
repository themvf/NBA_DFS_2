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
  /** Minutes played (box score); null if the box score did not carry it. */
  minutes: number | null;
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
/**
 * A graded player who played fewer minutes than this is a "short night"
 * (early exit, foul trouble, blowout rest). Books settle his line as played,
 * so he stays graded; he is counted and drawn hollow, never removed, because
 * minutes are known only after the game. A stated threshold, not fitted.
 */
export const SHORT_MINUTES = 15;

export function isShortMinutes(row: Pick<GradedRow, "minutes">): boolean {
  return row.minutes != null && row.minutes < SHORT_MINUTES;
}

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
  minutes: number | null;
  /** Played under SHORT_MINUTES; drawn hollow. */
  short: boolean;
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
      minutes: r.minutes,
      short: isShortMinutes(r),
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

/**
 * Does the books' reading beat simple guesses? Brier of each reading against
 * two references, over the same rows:
 *
 * - Base rate: everyone gets the pooled hit share of these rows. It is
 *   computed in-sample (it knows the outcomes it is graded on), so it is the
 *   best any constant can do; beating it is a low bar.
 * - Line only: a player gets the hit share of players whose consensus line
 *   sat in the same LINE_BAND-point band on OTHER nights, shrunk toward the
 *   other nights' pooled share with BASELINE_PRIOR_ROWS pseudo-rows. This is
 *   "stars score more" without the ladder; it never sees its own night, so a
 *   row on the only graded night has no line-only guess.
 *
 * Skill = 1 - ladder Brier / reference Brier: positive means the books'
 * reading beat the reference, 0 means no better, negative means worse.
 * Line-only skill is paired: the ladder is scored on exactly the rows that
 * have a line-only guess.
 */
export const LINE_BAND = 3;
export const BASELINE_PRIOR_ROWS = 10;

export type SkillRow = {
  key: string;
  label: string;
  n: number;
  nights: number;
  brier: number | null;
  baseRate: number | null;
  brierBase: number | null;
  skillVsBase: number | null;
  /** Rows that have a line-only guess (graded on a night with at least one other night). */
  lineN: number;
  brierPaired: number | null;
  brierLine: number | null;
  skillVsLine: number | null;
};

const band = (line: number) => Math.floor(line / LINE_BAND);

export function skillRow(rows: GradedRow[], reading: Reading): SkillRow {
  const scored = rows
    .map((r) => ({ r, p: reading.stated(r), y: reading.hit(r) ? 1 : 0 }))
    .filter((x): x is { r: GradedRow; p: number; y: 0 | 1 } => x.p != null && Number.isFinite(x.p));
  const n = scored.length;
  const nights = new Set(scored.map((x) => x.r.gameDate)).size;
  const empty: SkillRow = {
    key: reading.key, label: reading.label, n, nights, brier: null, baseRate: null, brierBase: null,
    skillVsBase: null, lineN: 0, brierPaired: null, brierLine: null, skillVsLine: null,
  };
  if (n === 0) return empty;

  const brier = scored.reduce((s, x) => s + (x.p - x.y) ** 2, 0) / n;
  const baseRate = scored.reduce((s, x) => s + x.y, 0) / n;
  const brierBase = baseRate * (1 - baseRate);

  // Counts for leave-one-night-out: totals, per band, per night, per (night, band).
  let hitsAll = 0;
  const bandAll = new Map<number, { n: number; h: number }>();
  const nightAll = new Map<string, { n: number; h: number }>();
  const nightBand = new Map<string, { n: number; h: number }>();
  const bump = <K,>(m: Map<K, { n: number; h: number }>, k: K, y: number) => {
    const c = m.get(k) ?? { n: 0, h: 0 };
    c.n += 1;
    c.h += y;
    m.set(k, c);
  };
  for (const x of scored) {
    hitsAll += x.y;
    bump(bandAll, band(x.r.line), x.y);
    bump(nightAll, x.r.gameDate, x.y);
    bump(nightBand, `${x.r.gameDate}|${band(x.r.line)}`, x.y);
  }
  let lineN = 0;
  let sumPaired = 0;
  let sumLine = 0;
  for (const x of scored) {
    const night = nightAll.get(x.r.gameDate)!;
    const nOther = n - night.n;
    if (nOther === 0) continue;
    const pool = (hitsAll - night.h) / nOther;
    const b = band(x.r.line);
    const all = bandAll.get(b)!;
    const own = nightBand.get(`${x.r.gameDate}|${b}`)!;
    const nb = all.n - own.n;
    const hb = all.h - own.h;
    const guess = (hb + BASELINE_PRIOR_ROWS * pool) / (nb + BASELINE_PRIOR_ROWS);
    lineN += 1;
    sumPaired += (x.p - x.y) ** 2;
    sumLine += (guess - x.y) ** 2;
  }
  const brierPaired = lineN ? sumPaired / lineN : null;
  const brierLine = lineN ? sumLine / lineN : null;
  return {
    ...empty,
    brier,
    baseRate,
    brierBase,
    skillVsBase: brierBase > 0 ? 1 - brier / brierBase : null,
    lineN,
    brierPaired,
    brierLine,
    skillVsLine: brierPaired != null && brierLine != null && brierLine > 0 ? 1 - brierPaired / brierLine : null,
  };
}

export function skillTable(rows: GradedRow[]): SkillRow[] {
  return READINGS.map((reading) => skillRow(rows, reading));
}

/** How many graded rows were short nights, for the panel header. */
export function shortMinutesSummary(rows: GradedRow[]): { n: number; short: number; withMinutes: number } {
  const withMinutes = rows.filter((r) => r.minutes != null).length;
  return { n: rows.length, short: rows.filter(isShortMinutes).length, withMinutes };
}
