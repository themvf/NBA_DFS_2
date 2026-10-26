/**
 * Market anchor (`nfl-market-anchor-v1`, owner's decision 2026-10-12): for a
 * QB, RB, WR or TE with a full set of pregame prop lines, the projection the
 * optimizer builds from is the sportsbooks' number, not the model's.
 *
 * Why: over weeks 1-4 the books' calibrated stat line missed actual DK points
 * by 5.61 a player against our 5.83 (game-clustered CI on the gap
 * [+0.04, +0.41]); the lines already carry the opponent, game script and the
 * injury news our model cannot see (CLAUDE.md "Reading the books' player
 * lines"). The model's number stays on the row (`before`) so every week can be
 * graded anchored-versus-model.
 *
 * The conversion is a port of `model/nfl_prop_implied.py` (pinned by
 * `scripts/test-nfl-market-anchor.ts` against values the Python computes):
 * de-vig each two-way market, read yards and catches as a normal around the
 * line, TDs and interceptions as Poisson, remove the anytime-TD margin, then
 * multiply each stat by its median-to-mean factor (CALIBRATION, fitted on
 * weeks 1-4) and score with DraftKings rules, the yardage bonuses as
 * probabilities. The model's P10/median/P90 are rescaled to the new mean (same
 * shape); its boom rate is kept. Change a constant here and bump the version.
 *
 * v2 (2026-10-17): a QB's anytime-TD and rushing-yards lines are optional, and
 * v1 read a missing one as 0 rushing TDs / 0 rushing yards, which cut every
 * running quarterback without those lines. v2 fills a missing one from the
 * model's own mean for that stat (`QbRushFallback`) and records which it used.
 *
 * v3 (2026-10-25, owner's priority: lines on more players): an RB, WR or TE
 * without a full set whose books post an anytime-TD line (most backups get only
 * touchdown markets) is anchored on touchdowns alone (`mode: 'td_only'`): the
 * model's projection plus 6 x (the line's calibrated TD rate - the model's own
 * rushing + receiving TDs); every other stat stays the model's. Graded on 2026
 * weeks 1-6 (docs/nfl-market-anchor-v3.md, 665 player-games): MAE 0.20 better
 * than the model (game-clustered CI [-0.25, -0.14]), better at RB, WR and TE,
 * in every week, and better than shrinking every model TD by a constant. A
 * partial set of volume or yardage lines (mostly RBs) graded no better than the
 * model, so those lines are still used only as part of a full set. Full-set
 * players are unchanged from v2. Python twin: `td_only_delta`.
 */

export const MARKET_ANCHOR_VERSION = 'nfl-market-anchor-v3';
export const ANYTIME_TD_MARGIN = 1.08;
export const RB_YARDS_PER_RECEPTION = 7.5;
/** Mean actual / implied over weeks 1-4 (research/nfl_prop_implied.py). */
export const CALIBRATION: Record<string, number> = {
  player_pass_yds: 1.023, player_pass_tds: 1.020, player_pass_interceptions: 0.985,
  player_rush_yds: 1.057, player_receptions: 1.078, player_reception_yds: 1.175, player_anytime_td: 0.998,
};

type MarketKind = 'normal' | 'poisson';
const MARKETS: Record<string, { stat: StatKey; kind: MarketKind; spread: number; ratio: number }> = {
  player_pass_yds: { stat: 'passing_yards', kind: 'normal', spread: 60, ratio: 0 },
  player_pass_tds: { stat: 'passing_tds', kind: 'poisson', spread: 0, ratio: 0 },
  player_pass_interceptions: { stat: 'passing_interceptions', kind: 'poisson', spread: 0, ratio: 0 },
  player_rush_yds: { stat: 'rushing_yards', kind: 'normal', spread: 8, ratio: 0.5 },
  player_receptions: { stat: 'receptions', kind: 'normal', spread: 1, ratio: 0 },
  player_reception_yds: { stat: 'receiving_yards', kind: 'normal', spread: 6, ratio: 0.6 },
};
export const ANCHOR_MARKETS = [...Object.keys(MARKETS), 'player_anytime_td'] as const;
const REQUIRED: Record<string, string[]> = {
  QB: ['player_pass_yds', 'player_pass_tds'],
  RB: ['player_rush_yds', 'player_receptions', 'player_anytime_td'],
  WR: ['player_receptions', 'player_reception_yds', 'player_anytime_td'],
  TE: ['player_receptions', 'player_reception_yds', 'player_anytime_td'],
};

type StatKey = 'passing_yards' | 'passing_tds' | 'passing_interceptions' | 'rushing_yards' | 'receptions' | 'receiving_yards' | 'scrimmage_tds';
export type StatLine = Record<StatKey, number>;
export interface PropQuote { line?: number | null; over?: number | null; under?: number | null; yes?: number | null }
/** The model's own QB rushing means, used only where the books posted no line. */
export interface QbRushFallback { rushing_yards?: number | null; rushing_tds?: number | null }
/** The model's own stat means a TD-only anchor keeps (and whose TDs it replaces). */
export interface ModelStatMeans {
  rushing_yards?: number | null; receptions?: number | null; receiving_yards?: number | null;
  rushing_tds?: number | null; receiving_tds?: number | null;
}
export const TD_ONLY_POSITIONS = ['RB', 'WR', 'TE'];

export function impliedProb(american: number): number {
  return american < 0 ? -american / (-american + 100) : 100 / (american + 100);
}

export function pOver(over: number, under: number): number {
  const a = impliedProb(over), b = impliedProb(under);
  return a / (a + b);
}

/** Inverse standard normal CDF (Acklam), |error| < 1.2e-9. */
export function inverseNormal(p: number): number {
  const a = [-39.69683028665376, 220.9460984245205, -275.9285104469687, 138.3577518672690, -30.66479806614716, 2.506628277459239];
  const b = [-54.47609879822406, 161.5858368580409, -155.6989798598866, 66.80131188771972, -13.28068155288572];
  const c = [-0.007784894002430293, -0.3223964580411365, -2.400758277161838, -2.549732539343734, 4.374664141464968, 2.938163982698783];
  const d = [0.007784695709041462, 0.3224671290700398, 2.445134137142996, 3.754408661907416];
  const lo = 0.02425, hi = 1 - lo;
  if (p < lo) { const q = Math.sqrt(-2 * Math.log(p)); return (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1); }
  if (p > hi) { const q = Math.sqrt(-2 * Math.log(1 - p)); return -(((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1); }
  const q = p - 0.5, r = q * q;
  return (((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5]) * q / (((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1);
}

function normalCdf(x: number): number {
  // Abramowitz-Stegun 7.1.26 via erf; ample for a bonus probability.
  const t = 1 / (1 + 0.3275911 * Math.abs(x) / Math.SQRT2);
  const y = 1 - (((((1.061405429 * t - 1.453152027) * t) + 1.421413741) * t - 0.284496736) * t + 0.254829592) * t * Math.exp(-(x * x) / 2);
  return x >= 0 ? 0.5 * (1 + y) : 0.5 * (1 - y);
}

function poissonRateForTail(k: number, p: number): number {
  let lo = 0, hi = 12;
  for (let i = 0; i < 80; i++) {
    const m = (lo + hi) / 2;
    let cdf = 0, term = Math.exp(-m);
    for (let j = 0; j < k; j++) { cdf += term; term *= m / (j + 1); }
    if (1 - cdf < p) lo = m; else hi = m;
  }
  return (lo + hi) / 2;
}

/** A two-way market's expected value BEFORE calibration, or null when a price is missing. */
export function marketMean(market: string, q: PropQuote | undefined): number | null {
  const md = MARKETS[market];
  if (!md || !q || q.line == null || q.over == null || q.under == null) return null;
  const p = Math.min(Math.max(pOver(q.over, q.under), 0.02), 0.98);
  if (md.kind === 'poisson') return poissonRateForTail(Math.floor(q.line) + 1, p);
  return q.line + Math.max(md.spread, md.ratio * q.line) * inverseNormal(p);
}

export function anytimeTdRate(yes: number | null | undefined): number | null {
  if (yes == null) return null;
  return -Math.log(1 - Math.min(impliedProb(yes) / ANYTIME_TD_MARGIN, 0.95));
}

function present(market: string, q: PropQuote | undefined): boolean {
  if (!q) return false;
  return market === 'player_anytime_td' ? q.yes != null : q.line != null && q.over != null && q.under != null;
}

/** The calibrated expected stat line, or null when a required market is missing. */
export function statLine(position: string, lines: Record<string, PropQuote>, fallback?: QbRushFallback | null,
  used?: string[]): StatLine | null {
  const required = REQUIRED[position];
  if (!required || required.some((m) => !present(m, lines[m]))) return null;
  const cal = (market: string, value: number | null) => value == null ? null : value * (CALIBRATION[market] ?? 1);
  const mean = (market: string) => cal(market, marketMean(market, lines[market]));
  const fill = (value: number | null, market: string, model: number | null | undefined): number => {
    if (value != null) return value;
    if (position === 'QB' && model != null && Number.isFinite(model) && model > 0) { used?.push(market); return model; }
    return 0;
  };
  const td = fill(cal('player_anytime_td', anytimeTdRate(lines.player_anytime_td?.yes)), 'player_anytime_td', fallback?.rushing_tds);
  const s: StatLine = { passing_yards: 0, passing_tds: 0, passing_interceptions: 0, rushing_yards: 0, receptions: 0, receiving_yards: 0, scrimmage_tds: td };
  if (position === 'QB') {
    const py = mean('player_pass_yds'), ptd = mean('player_pass_tds');
    if (py == null || ptd == null) return null;
    s.passing_yards = py; s.passing_tds = ptd;
    s.passing_interceptions = mean('player_pass_interceptions') ?? 0.8;
    s.rushing_yards = fill(mean('player_rush_yds'), 'player_rush_yds', fallback?.rushing_yards);
  } else if (position === 'RB') {
    const ry = mean('player_rush_yds'), rec = mean('player_receptions');
    if (ry == null || rec == null) return null;
    s.rushing_yards = ry; s.receptions = rec;
    s.receiving_yards = mean('player_reception_yds') ?? rec * RB_YARDS_PER_RECEPTION;
  } else {
    const rec = mean('player_receptions'), yds = mean('player_reception_yds');
    if (rec == null || yds == null) return null;
    s.receptions = rec; s.receiving_yards = yds; s.rushing_yards = mean('player_rush_yds') ?? 0;
  }
  return s;
}

/** The model's rushing + receiving TDs, or null when it carries neither. */
export function modelTouchdowns(m: ModelStatMeans | null | undefined): number | null {
  if (!m || (m.rushing_tds == null && m.receiving_tds == null)) return null;
  const tds = (Number(m.rushing_tds) || 0) + (Number(m.receiving_tds) || 0);
  return Number.isFinite(tds) ? tds : null;
}

/**
 * DK points a TD-only anchor adds to the model's projection, or null (v3). Only
 * the touchdown term moves, so no yardage bonus changes.
 */
export function tdOnlyDelta(position: string, lines: Record<string, PropQuote>, modelTds: number | null): number | null {
  if (!TD_ONLY_POSITIONS.includes(position) || modelTds == null || !Number.isFinite(modelTds)) return null;
  const rate = anytimeTdRate(lines.player_anytime_td?.yes);
  return rate == null ? null : 6 * (rate * CALIBRATION.player_anytime_td - modelTds);
}

function tail(mean: number, spread: number, threshold: number): number {
  return spread > 0 ? 1 - normalCdf((threshold - mean) / spread) : Number(mean >= threshold);
}

/** DraftKings points of an expected stat line; bonuses as probabilities, fumbles and 2-pt conversions left out. */
export function dkPoints(position: string, s: StatLine): number {
  let pts = 0.04 * s.passing_yards + 4 * s.passing_tds - s.passing_interceptions + 0.1 * s.rushing_yards
    + s.receptions + 0.1 * s.receiving_yards + 6 * s.scrimmage_tds;
  if (position === 'QB') pts += 3 * tail(s.passing_yards, MARKETS.player_pass_yds.spread, 300);
  if (s.rushing_yards > 0) pts += 3 * tail(s.rushing_yards, Math.max(8, 0.5 * s.rushing_yards), 100);
  if (s.receiving_yards > 0) pts += 3 * tail(s.receiving_yards, Math.max(6, 0.6 * s.receiving_yards), 100);
  return pts;
}

export interface MarketAnchor {
  version: string;
  /** 'full': the whole stat line from the books (v1/v2). 'td_only': touchdowns from the anytime-TD line, the rest the model's (v3). */
  mode: 'full' | 'td_only';
  /** Markets whose calibrated value the projection used. */
  fromMarket: string[];
  /** td_only: the model's own rushing + receiving TDs the line replaced. */
  modelTds?: number;
  /** The model's own range, kept for grading. */
  before: { mean: number; p10: number | null; median: number | null; p90: number | null };
  after: { mean: number; p10: number | null; median: number | null; p90: number | null };
  statLine: StatLine;
  /** Markets whose stat came from the model's own mean: a QB's missing rushing lines (full), every non-TD stat (td_only). */
  modelFilled: string[];
  book: string;
  capturedAt: string;
  lines: Record<string, PropQuote>;
}

const PASS_MARKETS = ['player_pass_yds', 'player_pass_tds', 'player_pass_interceptions'];
const RECEIVING_MARKETS = ['player_receptions', 'player_reception_yds'];

/**
 * Anchor one player, or null when the model has no positive mean to rescale
 * his range from, or he has neither a full set of lines nor (RB/WR/TE) an
 * anytime-TD line plus the model's own TD means. Either way the model's range
 * is rescaled to the new mean.
 */
export function anchorPlayer(input: {
  position: string; ourProj: number | null; floorFpts: number | null; medianFpts: number | null; ceilingFpts: number | null;
  lines: Record<string, PropQuote>; book: string; capturedAt: string; qbRushFallback?: QbRushFallback | null;
  /** The model's own stat means, for a TD-only anchor (v3). */
  modelStatMeans?: ModelStatMeans | null;
}): MarketAnchor | null {
  if (input.ourProj == null || !(input.ourProj > 0.5)) return null;
  const ourProj = input.ourProj;
  const rescale = (mean: number) => {
    const k = mean / ourProj;
    const scale = (v: number | null) => v == null ? null : v * k;
    return { before: { mean: ourProj, p10: input.floorFpts, median: input.medianFpts, p90: input.ceilingFpts },
      after: { mean, p10: scale(input.floorFpts), median: scale(input.medianFpts), p90: scale(input.ceilingFpts) } };
  };
  const common = { version: MARKET_ANCHOR_VERSION, book: input.book, capturedAt: input.capturedAt, lines: input.lines };
  const modelFilled: string[] = [];
  const s = statLine(input.position, input.lines, input.qbRushFallback, modelFilled);
  if (s) {
    const unread = input.position === 'QB' ? RECEIVING_MARKETS : PASS_MARKETS;
    const fromMarket = (ANCHOR_MARKETS as readonly string[]).filter((m) => present(m, input.lines[m]) && !unread.includes(m));
    return { ...common, mode: 'full', fromMarket, ...rescale(dkPoints(input.position, s)), statLine: s, modelFilled };
  }
  const modelTds = modelTouchdowns(input.modelStatMeans);
  const delta = tdOnlyDelta(input.position, input.lines, modelTds);
  if (delta == null || modelTds == null || !(ourProj + delta > 0)) return null;
  const m = input.modelStatMeans!;
  const num = (v: number | null | undefined) => (v != null && Number.isFinite(Number(v)) ? Number(v) : 0);
  const tdLine: StatLine = { passing_yards: 0, passing_tds: 0, passing_interceptions: 0, rushing_yards: num(m.rushing_yards),
    receptions: num(m.receptions), receiving_yards: num(m.receiving_yards), scrimmage_tds: modelTds + delta / 6 };
  return { ...common, mode: 'td_only', fromMarket: ['player_anytime_td'], modelTds, ...rescale(ourProj + delta), statLine: tdLine,
    modelFilled: ['player_rush_yds', ...RECEIVING_MARKETS] };
}
