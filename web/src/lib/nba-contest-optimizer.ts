// Browser port of research/nba_wta_trios.py (nba-wta-trios-v1) for custom contest runs:
// the owner picks which games a contest includes, and the same rules build entries from
// tonight's latest lines. Same constants, same pool rule, same payout and greedy selection.
//
// Pinned to the Python by tests/fixtures/nba_wta_fixture.json (survival knots, sitting
// rates, correlations, trio order, baseline order, prize-share selection), checked by
// web/scripts/test-nba-contest-optimizer.ts. The random draws are not pinned: numpy's
// stream and this one differ, so an all-games custom run can differ slightly from the
// frozen entries by simulation noise. Custom runs are never graded; the frozen
// all-games entries are the tracked record. Since 2026-11-06 custom runs also thin each
// player's top end (nba-points-sim-recal-v1); the frozen entries do not.

export const WTA = {
  POOL_SIZE: 20,
  N_ENTRIES: 10,
  FIELD_SIZE: 300,
  CHALK_SHARE: 0.1,
  POPULARITY_TAU: 3.0,
  SIMS: 6000,
  SIM_CHUNK: 500,
  TEAMMATE_RHO: -0.06,
  OPPONENT_RHO: 0.02,
  DNP_BY_BOOKS: [[1, 0.35], [3, 0.03], [1e6, 0.002]] as [number, number][],
  FALLBACK_SD_INTERCEPT: 3.9,
  FALLBACK_SD_SLOPE: 0.18,
  TAIL_FLOOR: 0.0005,
  // nba-points-sim-recal-v1: thins each player's top end (research/nba_wta_trios.py RECAL_G).
  RECAL_KNOTS: [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 0.99, 1.0],
  RECAL_G: [0.0, 0.0964, 0.2309, 0.3347, 0.4317, 0.5254, 0.6276, 0.7368, 0.8379, 0.9306, 0.9762, 0.9974, 1.0],
} as const;

export const RECAL_VERSION = "nba-points-sim-recal-v1";

export type ContestPlayer = {
  key: string;
  player: string;
  eventId: string;
  team: string | null;
  line: number;
  pOver: number | null;
  books: number;
  /** S(x) = P(points > x) points from the books' ladder; empty when none. */
  curve: { point: number; p: number }[];
};

export type OptimizerEntry = {
  rank: number;
  players: string[];
  names: string[];
  lineSum: number;
  simMean: number;
  simP90: number;
  marginalShare: number;
  portfolioShare: number;
  standaloneShare: number;
};

export type OptimizerResult = {
  pool: ContestPlayer[];
  entries: OptimizerEntry[];
  baseline: OptimizerEntry[];
  winningScore: { p25: number; median: number; p75: number };
  sims: number;
  fieldSize: number;
};

export function dnpProb(books: number): number {
  for (const [upper, p] of WTA.DNP_BY_BOOKS) if (books <= upper) return p;
  return WTA.DNP_BY_BOOKS[WTA.DNP_BY_BOOKS.length - 1][1];
}

/** (survival levels ascending, points) for inverse sampling; null when unusable. Mirrors survival_knots. */
export function survivalKnots(curve: { point: number; p: number }[] | [number, number][]): { s: number[]; x: number[] } | null {
  const pts = (curve as unknown[])
    .map((c) => (Array.isArray(c) ? [Number(c[0]), Number(c[1])] : [Number((c as { point: number }).point), Number((c as { p: number }).p)]))
    .filter(([x, p]) => Number.isFinite(x) && Number.isFinite(p) && p > 0 && p < 1)
    .sort((a, b) => a[0] - b[0] || a[1] - b[1]);
  if (pts.length < 2) return null;
  const xs: number[] = [];
  const ss: number[] = [];
  const [firstX, firstP] = pts[0];
  if (firstX > 0 && firstP < 0.995) { xs.push(0); ss.push(0.995); }
  let lastS = 1.0;
  for (const [x, p0] of pts) {
    const p = Math.min(p0, lastS);
    if (xs.length && x <= xs[xs.length - 1]) continue;
    xs.push(x); ss.push(p); lastS = p;
  }
  if (xs.length < 2) return null;
  const x1 = xs[xs.length - 2], p1 = ss[ss.length - 2], x2 = xs[xs.length - 1], p2 = ss[ss.length - 1];
  let rate = p1 > p2 && p2 > 0 && x2 > x1 ? Math.log(p1 / p2) / (x2 - x1) : 0;
  rate = Math.min(1.0, Math.max(0.05, rate));
  let x = x2, p = p2;
  while (p > WTA.TAIL_FLOOR) {
    x += 1.0;
    p = p2 * Math.exp(-rate * (x - x2));
    xs.push(x); ss.push(Math.max(p, WTA.TAIL_FLOOR * 0.5));
  }
  const outS: number[] = [], outX: number[] = [];
  for (let i = 0; i < xs.length; i++) {
    let s = ss[i];
    if (outS.length && s >= outS[outS.length - 1]) s = outS[outS.length - 1] - 1e-9;
    outS.push(s); outX.push(xs[i]);
  }
  return { s: outS.reverse(), x: outX.reverse() };
}

export function correlationMatrix(pool: { eventId: string; team: string | null }[]): number[][] {
  const n = pool.length;
  const c: number[][] = Array.from({ length: n }, (_, i) => Array.from({ length: n }, (_, j) => (i === j ? 1 : 0)));
  for (let i = 0; i < n; i++) for (let j = i + 1; j < n; j++) {
    const a = pool[i], b = pool[j];
    if (a.eventId !== b.eventId || a.team == null || b.team == null) continue;
    c[i][j] = c[j][i] = a.team === b.team ? WTA.TEAMMATE_RHO : WTA.OPPONENT_RHO;
  }
  return c;
}

/** Lower Cholesky factor; off-diagonals shrink toward zero until positive definite (never with the measured values). */
export function cholesky(c: number[][]): number[][] {
  let m = c.map((r) => r.slice());
  for (let attempt = 0; attempt < 50; attempt++) {
    const n = m.length, l = Array.from({ length: n }, () => new Array<number>(n).fill(0));
    let ok = true;
    for (let i = 0; i < n && ok; i++) for (let j = 0; j <= i; j++) {
      let s = m[i][j];
      for (let k = 0; k < j; k++) s -= l[i][k] * l[j][k];
      if (i === j) { if (s <= 1e-12) { ok = false; break; } l[i][i] = Math.sqrt(s); }
      else l[i][j] = s / l[j][j];
    }
    if (ok) return l;
    m = m.map((r, i) => r.map((v, j) => (i === j ? 1 : v * 0.9)));
  }
  throw new Error("correlation matrix could not be factored");
}

/** Deterministic PRNG (mulberry32), so a selection gives the same entries every time. */
export function rng(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

function normal(r: () => number): number {
  let u = 0;
  while (u === 0) u = r();
  return Math.sqrt(-2 * Math.log(u)) * Math.cos(2 * Math.PI * r());
}

function gumbel(r: () => number): number {
  let u = 0;
  while (u === 0) u = r();
  return -Math.log(-Math.log(u));
}

/** Standard normal CDF (Abramowitz-Stegun 7.1.26 erf, error under 1.5e-7). */
export function normCdf(z: number): number {
  const x = Math.abs(z) / Math.SQRT2, t = 1 / (1 + 0.3275911 * x);
  const erf = 1 - (((((1.061405429 * t - 1.453152027) * t) + 1.421413741) * t - 0.284496736) * t + 0.254829592) * t * Math.exp(-x * x);
  return z >= 0 ? 0.5 * (1 + erf) : 0.5 * (1 - erf);
}

function interp(u: number, s: number[], x: number[]): number {
  if (u <= s[0]) return x[0];
  const last = s.length - 1;
  if (u >= s[last]) return x[last];
  let lo = 0, hi = last;
  while (hi - lo > 1) { const mid = (lo + hi) >> 1; if (s[mid] <= u) lo = mid; else hi = mid; }
  return x[lo] + ((u - s[lo]) / (s[hi] - s[lo])) * (x[hi] - x[lo]);
}

/** Round half to even, like numpy's rint. */
function rint(v: number): number {
  const f = Math.floor(v), d = v - f;
  if (d > 0.5) return f + 1;
  if (d < 0.5) return f;
  return f % 2 === 0 ? f : f + 1;
}

/** Inverse standard normal CDF (Acklam, relative error under 1.2e-9). */
export function normInv(p: number): number {
  const a = [-39.69683028665376, 220.9460984245205, -275.9285104469687, 138.357751867269, -30.66479806614716, 2.506628277459239];
  const b = [-54.47609879822406, 161.5858368580409, -155.6989798598866, 66.80131188771972, -13.28068155288572];
  const c = [-0.007784894002430293, -0.3223964580411365, -2.400758277161838, -2.549732539343734, 4.374664141464968, 2.938163982698783];
  const d = [0.007784695709041462, 0.3224671290700398, 2.445134137142996, 3.754408661907416];
  const q0 = Math.min(Math.max(p, 1e-12), 1 - 1e-12);
  if (q0 < 0.02425) {
    const q = Math.sqrt(-2 * Math.log(q0));
    return (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1);
  }
  if (q0 > 1 - 0.02425) return -normInv(1 - q0);
  const q = q0 - 0.5, t = q * q;
  return (((((a[0] * t + a[1]) * t + a[2]) * t + a[3]) * t + a[4]) * t + a[5]) * q / (((((b[0] * t + b[1]) * t + b[2]) * t + b[3]) * t + b[4]) * t + 1);
}

/** Survival level -> recalibrated survival level: F-level u becomes G^-1(u) (RECAL_VERSION). */
export function recalibrateSurvival(u: number): number {
  const f = 1 - u, g = WTA.RECAL_G, k = WTA.RECAL_KNOTS;
  if (f <= g[0]) return 1 - k[0];
  if (f >= g[g.length - 1]) return 1 - k[k.length - 1];
  let i = 1;
  while (g[i] < f) i++;
  return 1 - (k[i - 1] + ((f - g[i - 1]) / (g[i] - g[i - 1])) * (k[i] - k[i - 1]));
}

/**
 * sims x players whole points, row-major (sim * n + player). A player who sits scores 0.
 * `recal` thins each player's top end (RECAL_VERSION); the registered v1 entries never use it.
 */
export function simulatePoints(pool: ContestPlayer[], sims: number, r: () => number, recal = false): Float64Array {
  const n = pool.length, l = cholesky(correlationMatrix(pool));
  const knots = pool.map((p) => survivalKnots(p.curve));
  const sit = pool.map((p) => dnpProb(p.books));
  const out = new Float64Array(sims * n), e = new Float64Array(n);
  for (let s = 0; s < sims; s++) {
    for (let j = 0; j < n; j++) e[j] = normal(r);
    for (let i = 0; i < n; i++) {
      let z = 0;
      for (let j = 0; j <= i; j++) z += l[i][j] * e[j];
      let u = normCdf(-z);
      if (recal) { u = recalibrateSurvival(u); z = -normInv(u); }
      const k = knots[i];
      let v: number;
      if (k == null) v = Math.max(0, pool[i].line + 0.5 + (WTA.FALLBACK_SD_INTERCEPT + WTA.FALLBACK_SD_SLOPE * pool[i].line) * z);
      else v = interp(Math.min(Math.max(u, k.s[0]), k.s[k.s.length - 1]), k.s, k.x);
      if (r() < sit[i]) v = 0;
      out[s * n + i] = rint(Math.max(v, 0));
    }
  }
  return out;
}

export function candidateTrios(n: number): [number, number, number][] {
  const out: [number, number, number][] = [];
  for (let a = 0; a < n; a++) for (let b = a + 1; b < n; b++) for (let c = b + 1; c < n; c++) out.push([a, b, c]);
  return out;
}

/** Per simulated night: the field's best total and how many field entries share it. */
export function fieldMax(points: Float64Array, sims: number, lines: number[], r: () => number,
  fieldSize: number = WTA.FIELD_SIZE, chalkShare: number = WTA.CHALK_SHARE, tau: number = WTA.POPULARITY_TAU,
): { best: Float64Array; count: Float64Array } {
  const n = lines.length;
  const chalk = lines.map((v, i) => [v, i] as const).sort((a, b) => b[0] - a[0] || a[1] - b[1]).slice(0, 3).map(([, i]) => i);
  const nChalk = Math.round(fieldSize * chalkShare);
  const logw = lines.map((v) => v / tau);
  const best = new Float64Array(sims), count = new Float64Array(sims);
  const g = new Float64Array(n);
  for (let start = 0; start < sims; start += WTA.SIM_CHUNK) {
    const stop = Math.min(sims, start + WTA.SIM_CHUNK);
    const picks: number[][] = [];
    for (let e = 0; e < nChalk; e++) picks.push(chalk);
    for (let e = nChalk; e < fieldSize; e++) {
      for (let i = 0; i < n; i++) g[i] = logw[i] + gumbel(r);
      const idx = Array.from({ length: n }, (_, i) => i).sort((a, b) => g[b] - g[a]).slice(0, 3);
      picks.push(idx);
    }
    for (let s = start; s < stop; s++) {
      let m = -Infinity, c = 0;
      const row = s * n;
      for (const p of picks) {
        const t = points[row + p[0]] + points[row + p[1]] + points[row + p[2]];
        if (t > m) { m = t; c = 1; } else if (t === m) c++;
      }
      best[s] = m; count[s] = c;
    }
  }
  return { best, count };
}

/** Candidate-major totals: totals[k * sims + s]. */
export function trioTotals(points: Float64Array, sims: number, n: number, trios: [number, number, number][]): Int16Array {
  const out = new Int16Array(trios.length * sims);
  trios.forEach(([a, b, c], k) => {
    for (let s = 0; s < sims; s++) out[k * sims + s] = points[s * n + a] + points[s * n + b] + points[s * n + c];
  });
  return out;
}

export type PortfolioPick = { trio: number; marginalShare: number; portfolioShare: number; standaloneShare: number };

/** Greedy prize-share selection; with `order`, scores fixed picks the same way. Mirrors build_portfolio. */
export function buildPortfolio(totals: ArrayLike<number>, sims: number, nTrios: number, fmax: ArrayLike<number>,
  fcount: ArrayLike<number>, nEntries: number, order?: number[]): PortfolioPick[] {
  const top = Float64Array.from(fmax), cf = Float64Array.from(fcount), cp = new Float64Array(sims), old = new Float64Array(sims);
  const standalone = new Float64Array(nTrios);
  for (let k = 0; k < nTrios; k++) {
    let sum = 0;
    for (let s = 0; s < sims; s++) {
      const t = totals[k * sims + s];
      sum += t > fmax[s] ? 1 : t === fmax[s] ? 1 / (fcount[s] + 1) : 0;
    }
    standalone[k] = sum / sims;
  }
  const chosen = new Set<number>();
  const out: PortfolioPick[] = [];
  let current = 0;
  for (let step = 0; step < nEntries; step++) {
    for (let s = 0; s < sims; s++) old[s] = cp[s] > 0 ? cp[s] / (cf[s] + cp[s]) : 0;
    const gainOf = (k: number) => {
      let sum = 0;
      for (let s = 0; s < sims; s++) {
        const t = totals[k * sims + s];
        const v = t > top[s] ? 1 : t === top[s] ? (cp[s] + 1) / (cf[s] + cp[s] + 1) : old[s];
        sum += v - old[s];
      }
      return sum / sims;
    };
    let k: number, gain: number;
    if (order) {
      if (step >= order.length) break;
      k = order[step]; gain = gainOf(k);
    } else {
      k = -1; gain = -Infinity;
      for (let c = 0; c < nTrios; c++) {
        if (chosen.has(c)) continue;
        const v = gainOf(c);
        if (v > gain + 1e-12) { gain = v; k = c; }
      }
    }
    chosen.add(k);
    for (let s = 0; s < sims; s++) {
      const t = totals[k * sims + s];
      if (t > top[s]) { cf[s] = 0; cp[s] = 1; top[s] = t; } else if (t === top[s]) cp[s] += 1;
    }
    current += gain;
    out.push({ trio: k, marginalShare: gain, portfolioShare: current, standaloneShare: standalone[k] });
  }
  return out;
}

/** The n trios with the largest line sums (ties: higher P(over) sum, then player keys). Mirrors baseline_order. */
export function baselineOrder(pool: { line: number; pOver: number | null; key: string }[], trios: [number, number, number][], n: number): number[] {
  const keyOf = (k: number) => {
    const t = trios[k];
    return { line: t.reduce((a, i) => a + pool[i].line, 0), p: t.reduce((a, i) => a + (pool[i].pOver ?? 0.5), 0), keys: t.map((i) => pool[i].key) };
  };
  return trios.map((_, k) => k).sort((a, b) => {
    const x = keyOf(a), y = keyOf(b);
    if (x.line !== y.line) return y.line - x.line;
    if (x.p !== y.p) return y.p - x.p;
    for (let i = 0; i < 3; i++) if (x.keys[i] !== y.keys[i]) return x.keys[i] < y.keys[i] ? -1 : 1;
    return 0;
  }).slice(0, n);
}

/** numpy-style linear percentile. */
export function percentile(values: ArrayLike<number>, q: number): number {
  const v = Array.from(values).sort((a, b) => a - b);
  if (!v.length) return NaN;
  const pos = (q / 100) * (v.length - 1), lo = Math.floor(pos), hi = Math.ceil(pos);
  return v[lo] + (v[hi] - v[lo]) * (pos - lo);
}

/** The pool rule: top lines, a name quoted in two games dropped. */
export function contestPool(players: ContestPlayer[], size: number = WTA.POOL_SIZE): ContestPlayer[] {
  const seen = new Map<string, number>();
  for (const p of players) seen.set(p.key, (seen.get(p.key) ?? 0) + 1);
  return players.filter((p) => seen.get(p.key) === 1)
    .sort((a, b) => b.line - a.line || (b.pOver ?? 0.5) - (a.pOver ?? 0.5) || (a.key < b.key ? -1 : a.key > b.key ? 1 : 0))
    .slice(0, size);
}

export function seedOf(text: string): number {
  let h = 2166136261;
  for (let i = 0; i < text.length; i++) { h ^= text.charCodeAt(i); h = Math.imul(h, 16777619); }
  return h >>> 0;
}

/**
 * Custom contest runs use the corrected simulation by default (`recal`); the frozen tracked
 * entries keep the registered v1 rules, so an all-games run can differ from them.
 */
export function optimizeContest(players: ContestPlayer[], opts: { fieldSize?: number; entries?: number; sims?: number; recal?: boolean } = {}): OptimizerResult {
  const pool = contestPool(players);
  if (pool.length < 3) throw new Error("Pick games with at least three quoted players.");
  const sims = opts.sims ?? WTA.SIMS, fieldSize = opts.fieldSize ?? WTA.FIELD_SIZE;
  const trios = candidateTrios(pool.length);
  const nEntries = Math.min(opts.entries ?? WTA.N_ENTRIES, trios.length);
  const r = rng(seedOf(`${pool.map((p) => `${p.key}:${p.line}`).join("|")}|${fieldSize}`));
  const points = simulatePoints(pool, sims, r, opts.recal ?? true);
  const n = pool.length;
  const totals = trioTotals(points, sims, n, trios);
  const { best, count } = fieldMax(points, sims, pool.map((p) => p.line), r, fieldSize);
  const picks = buildPortfolio(totals, sims, trios.length, best, count, nEntries);
  const base = buildPortfolio(totals, sims, trios.length, best, count, nEntries, baselineOrder(pool, trios, nEntries));
  const entry = (rank: number, p: PortfolioPick): OptimizerEntry => {
    const t = trios[p.trio];
    const col = totals.subarray(p.trio * sims, (p.trio + 1) * sims);
    return {
      rank, players: t.map((i) => pool[i].key), names: t.map((i) => pool[i].player),
      lineSum: Math.round(t.reduce((a, i) => a + pool[i].line, 0) * 10) / 10,
      simMean: col.reduce((a, v) => a + v, 0) / sims, simP90: percentile(col, 90),
      marginalShare: p.marginalShare, portfolioShare: p.portfolioShare, standaloneShare: p.standaloneShare,
    };
  };
  return {
    pool, sims, fieldSize,
    entries: picks.map((p, i) => entry(i + 1, p)),
    baseline: base.map((p, i) => entry(i + 1, p)),
    winningScore: { p25: percentile(best, 25), median: percentile(best, 50), p75: percentile(best, 75) },
  };
}
