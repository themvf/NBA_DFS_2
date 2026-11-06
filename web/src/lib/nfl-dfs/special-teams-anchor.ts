/**
 * Special-teams anchor (`nfl-special-teams-anchor-v1`, owner's decision
 * 2026-11-06: "incorporated now, not graded first"). Two rules, applied in the
 * workspace read after the skill-player market anchor and the projection
 * corrections, with the model's own number kept on the row as `before` so
 * every week can be graded anchored-versus-model the way the other anchors are.
 *
 * KICKER. DraftKings posts field-goal and extra-point lines, each with an
 * alternate ladder (over 0.5, 1.5, 2.5 ...), on every kicker it expects to
 * kick. Summing the de-vigged ladder gives the expected count directly:
 * E[X] = sum over k of P(X >= k). The two-sided main line is de-vigged by its
 * own pair; a one-sided rung is de-vigged by the main line's overround (the
 * NBA points ladder's method). DraftKings DFS scores a field goal 3, 4 or 5
 * by distance and an extra point 1, so the kicker's points are
 * 3 x E[FG] + E[PAT] + (distance bonus per made FG) x E[FG], the bonus from
 * his own history (40-49 yards +1, 50+ +2), else a league prior. The model's
 * range is rescaled to the new mean (the market anchor's method). Measured on
 * CIN @ ATL, 2026-11-06: McPherson 10.1 against the model's 7.4, Folk 9.0
 * against 6.2.
 *
 * DST. The special-teams model takes the opponent's implied total as a MEAN
 * and resamples history around it. DraftKings' alternate team-total ladder
 * (42 rungs) de-vigs to the opponent's whole points distribution, which maps
 * onto DraftKings' points-allowed tiers (0: +10, 1-6: +7, 7-13: +4, 14-20: +1,
 * 21-27: 0, 28-34: -1, 35+: -4). The anchor shifts the DST's projection by
 * (the ladder's tier expectation) minus (the same tier expectation under a
 * normal at the model's own opponent implied total, SD 10.2, the NFL team-
 * score spread), so only what the ladder knows beyond the mean moves the
 * number. Applied after the Showdown DST scale, as an additive shift, since
 * that scale corrects the model's level and this corrects the shape. No
 * market covers fumbles, defensive touchdowns, safeties or blocked kicks.
 *
 * Both keep the special-teams candidate block the optimizer builds from in
 * step with the row. Unvalidated: review after six weeks against `before`.
 */

export const SPECIAL_TEAMS_ANCHOR_VERSION = 'nfl-special-teams-anchor-v1';
export const SPECIAL_TEAMS_ANCHOR_MARKETS = [
  'player_field_goals', 'player_field_goals_alternate', 'player_pats', 'player_pats_alternate', 'alternate_team_totals',
] as const;
/** Extra DK points per made field goal from distance when a kicker has no history: ~35% from 40-49 (+1), ~12% from 50+ (+2). */
export const LEAGUE_DISTANCE_BONUS_PER_FG = 0.55;
/** Standard deviation of an NFL team's points around its implied total (stated prior). */
export const TEAM_POINTS_SD = 10.2;
/** DraftKings DST points by opponent points: [low, high, points]. */
export const POINTS_ALLOWED_TIERS: readonly [number, number, number][] = [
  [0, 0, 10], [1, 6, 7], [7, 13, 4], [14, 20, 1], [21, 27, 0], [28, 34, -1], [35, 200, -4],
];

export interface Rung { point: number; side: 'over' | 'under'; price: number }
export interface Range { mean: number; p10: number | null; median: number | null; p90: number | null }

export function impliedProb(american: number): number {
  return american < 0 ? -american / (-american + 100) : 100 / (american + 100);
}

/** Outcome rows as the capture stores them (`books[book].outcomes`), any market shape. */
export function rungsFromOutcomes(outcomes: ReadonlyArray<{ side?: string | null; name?: string | null; point?: number | null; price?: number | null; description?: string | null }> | null | undefined): Rung[] {
  const out: Rung[] = [];
  for (const o of outcomes ?? []) {
    const side = (o.side ?? o.name ?? '').toLowerCase();
    if ((side !== 'over' && side !== 'under') || o.point == null || o.price == null || !Number.isFinite(o.point) || !Number.isFinite(o.price)) continue;
    out.push({ point: o.point, side, price: o.price });
  }
  return out;
}

/**
 * P(X > point) at each rung, de-vigged. A rung with both sides uses its own
 * pair; a one-sided rung is scaled by the overround of the nearest two-sided
 * rung (else taken raw). Forced non-increasing in the point.
 */
export function survivalCurve(rungs: readonly Rung[]): { point: number; p: number }[] {
  const byPoint = new Map<number, { over?: number; under?: number }>();
  for (const r of rungs) {
    const slot = byPoint.get(r.point) ?? {};
    slot[r.side] = r.price;
    byPoint.set(r.point, slot);
  }
  const paired = [...byPoint.entries()].filter(([, s]) => s.over != null && s.under != null)
    .map(([point, s]) => ({ point, overround: impliedProb(s.over!) + impliedProb(s.under!) }));
  const overroundNear = (point: number) => {
    if (!paired.length) return 1;
    return paired.reduce((best, c) => Math.abs(c.point - point) < Math.abs(best.point - point) ? c : best).overround;
  };
  const curve = [...byPoint.entries()].map(([point, s]) => {
    if (s.over != null && s.under != null) {
      const po = impliedProb(s.over), pu = impliedProb(s.under);
      return { point, p: po / (po + pu) };
    }
    if (s.over != null) return { point, p: Math.min(1, impliedProb(s.over) / overroundNear(point)) };
    return { point, p: Math.max(0, 1 - impliedProb(s.under!) / overroundNear(point)) };
  }).sort((a, b) => a.point - b.point);
  let last = 1;
  for (const c of curve) { c.p = Math.min(c.p, last); last = c.p; }
  return curve;
}

function poissonTail(k: number, lambda: number): number {
  // P(X >= k) for X ~ Poisson(lambda).
  let term = Math.exp(-lambda), cdf = term;
  for (let i = 1; i < k; i += 1) { term *= lambda / i; cdf += term; }
  return Math.max(0, 1 - cdf);
}

/** The Poisson rate whose P(X >= k) is p (bisection). */
export function poissonRateForTail(k: number, p: number): number {
  let lo = 0, hi = 20;
  for (let i = 0; i < 60; i += 1) {
    const mid = (lo + hi) / 2;
    if (poissonTail(k, mid) < p) lo = mid; else hi = mid;
  }
  return (lo + hi) / 2;
}

/**
 * Expected count for an integer-valued market on half-point rungs. With a
 * ladder (two or more rungs): E[X] = sum_k P(X >= k), truncated at the top
 * rung. With the main line alone: the Poisson rate whose tail matches it (a
 * book that posts no ladder, or a capture taken before the ladder shipped).
 */
export function expectedCount(rungs: readonly Rung[]): { expected: number; atLeast: { k: number; p: number }[]; method: 'ladder' | 'poisson_main_line' } | null {
  const curve = survivalCurve(rungs).filter((c) => Math.abs(c.point - Math.floor(c.point) - 0.5) < 1e-9);
  if (!curve.length) return null;
  const atLeast = curve.map((c) => ({ k: Math.ceil(c.point), p: c.p }));
  if (atLeast.length >= 2) return { expected: atLeast.reduce((s, c) => s + c.p, 0), atLeast, method: 'ladder' };
  const [{ k, p }] = atLeast;
  if (!(p > 0 && p < 1)) return null;
  return { expected: poissonRateForTail(k, p), atLeast, method: 'poisson_main_line' };
}

/** Extra DK points per made field goal from this kicker's own distance mix, else the league prior. */
export function distanceBonusPerFg(statMeans: Record<string, number | null | undefined> | null | undefined): { bonus: number; source: 'history' | 'league' } {
  const n = (k: string) => { const v = statMeans?.[k]; return typeof v === 'number' && Number.isFinite(v) ? v : 0; };
  const made = n('fg_made');
  if (made > 0.2) {
    const bonus = (n('fg_made_40_49') + 2 * (n('fg_made_50_59') + n('fg_made_60_'))) / made;
    if (Number.isFinite(bonus) && bonus >= 0 && bonus <= 2) return { bonus, source: 'history' };
  }
  return { bonus: LEAGUE_DISTANCE_BONUS_PER_FG, source: 'league' };
}

export interface KickerLines { fieldGoals: Rung[]; pats: Rung[] }

export function kickerPoints(lines: KickerLines, distanceBonus: number): { points: number; efg: number; epat: number; fgAtLeast: { k: number; p: number }[]; patAtLeast: { k: number; p: number }[]; method: 'ladder' | 'poisson_main_line' } | null {
  const fg = expectedCount(lines.fieldGoals), pat = expectedCount(lines.pats);
  if (!fg || !pat) return null;
  return { points: 3 * fg.expected + pat.expected + distanceBonus * fg.expected, efg: fg.expected, epat: pat.expected, fgAtLeast: fg.atLeast, patAtLeast: pat.atLeast,
    method: fg.method === 'ladder' && pat.method === 'ladder' ? 'ladder' : 'poisson_main_line' };
}

function normalCdf(x: number): number {
  const t = 1 / (1 + 0.2316419 * Math.abs(x));
  const d = 0.3989422804014327 * Math.exp(-x * x / 2);
  const p = d * t * (0.3193815 + t * (-0.3565638 + t * (1.781478 + t * (-1.821256 + t * 1.330274))));
  return x >= 0 ? 1 - p : p;
}

/** DK points-allowed expectation and tier probabilities from P(points > x) at integer x (x = -1 means everything). */
function tierExpectation(survival: (x: number) => number): { expectation: number; tiers: { tier: string; p: number }[] } {
  let expectation = 0;
  const tiers = POINTS_ALLOWED_TIERS.map(([lo, hi, pts]) => {
    const p = Math.max(0, (lo === 0 ? 1 : survival(lo - 1)) - survival(hi));
    expectation += p * pts;
    return { tier: hi >= 200 ? `${lo}+` : lo === hi ? `${lo}` : `${lo}-${hi}`, p };
  });
  return { expectation, tiers };
}

/** The ladder's points-allowed read for the DST facing this team total. */
export function pointsAllowedFromLadder(rungs: readonly Rung[]): { expectedPoints: number; expectation: number; tiers: { tier: string; p: number }[]; rungCount: number } | null {
  const curve = survivalCurve(rungs);
  if (curve.length < 4) return null;
  const survival = (x: number): number => {
    const at = x + 0.5;
    const exact = curve.find((c) => Math.abs(c.point - at) < 1e-9);
    if (exact) return exact.p;
    const lo = curve.filter((c) => c.point < at).at(-1), hi = curve.find((c) => c.point > at);
    if (!lo) return 1;
    if (!hi) return 0;
    return lo.p + (hi.p - lo.p) * ((at - lo.point) / (hi.point - lo.point));
  };
  let expectedPoints = 0;
  for (let x = 0; x < 80; x += 1) expectedPoints += survival(x);
  return { expectedPoints, ...tierExpectation(survival), rungCount: curve.length };
}

/** The same tier expectation the model implies: a normal at its own opponent implied total. */
export function pointsAllowedFromMean(mean: number, sd = TEAM_POINTS_SD): { expectation: number; tiers: { tier: string; p: number }[] } {
  return tierExpectation((x) => 1 - normalCdf((x + 0.5 - mean) / sd));
}

export interface SpecialTeamsAnchor {
  version: string;
  position: 'K' | 'DST';
  mode: 'kicker_lines' | 'dst_points_allowed';
  book: string;
  capturedAt: string;
  before: Range;
  after: Range;
  /** K: the ladder read. */
  kicker?: { efg: number; epat: number; distanceBonus: number; distanceSource: 'history' | 'league'; method: 'ladder' | 'poisson_main_line'; fgAtLeast: { k: number; p: number }[]; patAtLeast: { k: number; p: number }[] };
  /** DST: the opponent's distribution read against the model's mean. */
  dst?: { opponent: string; expectedPoints: number; modelImpliedTotal: number; bookTierExpectation: number; modelTierExpectation: number; shift: number;
    tiers: { tier: string; p: number }[]; rungCount: number };
  note: string;
}

const rescale = (before: Range, mean: number): Range => {
  const k = before.mean > 0 ? mean / before.mean : 0;
  const s = (v: number | null) => (v == null ? null : v * k);
  return { mean, p10: s(before.p10), median: s(before.median), p90: s(before.p90) };
};
const shifted = (before: Range, shift: number): Range => {
  const s = (v: number | null) => (v == null ? null : v + shift);
  return { mean: before.mean + shift, p10: s(before.p10), median: s(before.median), p90: s(before.p90) };
};

export function anchorKicker(input: { before: Range; lines: KickerLines; statMeans?: Record<string, number | null | undefined> | null; book: string; capturedAt: string }): SpecialTeamsAnchor | null {
  if (!(input.before.mean > 0.5)) return null;
  const { bonus, source } = distanceBonusPerFg(input.statMeans);
  const read = kickerPoints(input.lines, bonus);
  if (!read || !(read.points > 0)) return null;
  const after = rescale(input.before, read.points);
  return { version: SPECIAL_TEAMS_ANCHOR_VERSION, position: 'K', mode: 'kicker_lines', book: input.book, capturedAt: input.capturedAt, before: input.before, after,
    kicker: { efg: read.efg, epat: read.epat, distanceBonus: bonus, distanceSource: source, method: read.method, fgAtLeast: read.fgAtLeast, patAtLeast: read.patAtLeast },
    note: `${input.book}'s field-goal and extra-point ${read.method === 'ladder' ? 'ladders' : 'lines (no ladder captured; Poisson fit of the main line)'}: ${read.efg.toFixed(2)} field goals and ${read.epat.toFixed(2)} extra points expected, ${bonus.toFixed(2)} distance points a field goal (${source}), ${read.points.toFixed(1)} DK points in place of the model's ${input.before.mean.toFixed(1)}.` };
}

export function anchorDst(input: { before: Range; opponent: string; opponentLadder: Rung[]; modelImpliedTotal: number; book: string; capturedAt: string }): SpecialTeamsAnchor | null {
  if (!Number.isFinite(input.modelImpliedTotal) || input.modelImpliedTotal <= 0) return null;
  const book = pointsAllowedFromLadder(input.opponentLadder);
  if (!book) return null;
  const model = pointsAllowedFromMean(input.modelImpliedTotal);
  const shift = book.expectation - model.expectation;
  if (!Number.isFinite(shift)) return null;
  const after = shifted(input.before, shift);
  return { version: SPECIAL_TEAMS_ANCHOR_VERSION, position: 'DST', mode: 'dst_points_allowed', book: input.book, capturedAt: input.capturedAt, before: input.before, after,
    dst: { opponent: input.opponent, expectedPoints: book.expectedPoints, modelImpliedTotal: input.modelImpliedTotal, bookTierExpectation: book.expectation,
      modelTierExpectation: model.expectation, shift, tiers: book.tiers, rungCount: book.rungCount },
    note: `${input.book}'s ${input.opponent} team-total ladder (${book.rungCount} rungs, ${book.expectedPoints.toFixed(1)} points expected) puts the points-allowed tiers at ${book.expectation >= 0 ? '+' : ''}${book.expectation.toFixed(2)} against ${model.expectation >= 0 ? '+' : ''}${model.expectation.toFixed(2)} under the model's ${input.modelImpliedTotal.toFixed(2)} implied total, so the model's ${input.before.mean.toFixed(1)} moves ${shift >= 0 ? '+' : ''}${shift.toFixed(2)} to ${after.mean.toFixed(1)}.` };
}
