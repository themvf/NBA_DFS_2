/**
 * A player's points distribution, read off the books' alternate points ladder.
 *
 * Pure. Each rung is a price on "more than N.5 points". From the rungs we build
 * S(x) = P(points > x), the chance of beating x, and read off:
 *   floor   = P10, the x the player beats 90% of the time  (S = 0.9)
 *   ceiling = P90, the x he beats only 10% of the time      (S = 0.1)
 *   P(N+)   = S(N - 0.5)
 *
 * Removing the vig:
 * - a rung quoted on both sides at one book is de-vigged as a pair;
 * - an over-only rung (the usual ladder) is divided by that book's overround
 *   on its main points line. Books shade longshot rungs harder, so this
 *   leaves a little margin in the top rungs: high-end chances read slightly
 *   high. A rung from a book with no main two-sided line is skipped, never
 *   guessed.
 *
 * Across books each point takes the median, then the curve is forced
 * non-increasing (pool adjacent violators, weighted by book count), because
 * the chance of beating 30 cannot exceed the chance of beating 25.
 * Nothing is extrapolated past the lowest or highest rung: a floor below the
 * ladder or a chance past its top is null, not a guess.
 *
 * `{ devig: "power" }` is the candidate nba-points-ladder-v2 reader registered in
 * docs/nba-points-ladder-v2-registration.md, opt-in only until that study's
 * verdict (the page never passes it). It changes ONLY one-sided rungs: per book
 * it solves k with implied(over)^k + implied(under)^k = 1 on the main line; an
 * over-only rung becomes implied(over)^k, an under-only rung 1 - implied(under)^k,
 * and a book whose k cannot be solved has its one-sided rungs skipped. Mirrored
 * in model/nba_points_ladder.py and pinned by the shared fixture.
 */

export type Rung = { point: number; side: string; price: number };

export type MainQuote = { line: number; over: number | null; under: number | null };

export type ExceedancePoint = { point: number; p: number; books: number };

export type PointsDistribution = {
  /** S(x) = P(points > x) at each rung point, ascending x, non-increasing p. */
  curve: ExceedancePoint[];
  floor: number | null;
  ceiling: number | null;
  /** P(at least N points) for the thresholds the page shows. */
  atLeast: Record<20 | 25 | 30 | 35 | 40, number | null>;
  /** Books whose ladder contributed at least one rung. */
  ladderBooks: number;
  /** Rungs dropped because their book had no main two-sided line to de-vig with. */
  skippedRungs: number;
};

const MIN_CURVE_POINTS = 3;

export type Devig = "proportional" | "power";

/** Bisection bracket and tolerance for the power exponent, fixed by the registration. */
export const POWER_K_LOW = 0.25;
export const POWER_K_HIGH = 4;
export const POWER_K_TOL = 1e-10;

/** The k with over^k + under^k = 1 (implied chances), by bisection on [0.25, 4]; null outside it. */
export function powerExponent(overImplied: number, underImplied: number): number | null {
  const f = (k: number) => overImplied ** k + underImplied ** k - 1;
  let lo = POWER_K_LOW;
  let hi = POWER_K_HIGH;
  if (!(overImplied > 0 && overImplied < 1 && underImplied > 0 && underImplied < 1) || f(lo) < 0 || f(hi) > 0) return null;
  while (hi - lo > POWER_K_TOL) {
    const mid = (lo + hi) / 2;
    if (f(mid) > 0) lo = mid;
    else hi = mid;
  }
  return (lo + hi) / 2;
}

function validAmerican(price: unknown): price is number {
  return typeof price === "number" && Number.isFinite(price) && (price <= -100 || price >= 100);
}

function implied(american: number): number {
  return american < 0 ? -american / (-american + 100) : 100 / (american + 100);
}

function median(values: number[]): number {
  const s = [...values].sort((a, b) => a - b);
  const m = Math.floor(s.length / 2);
  return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2;
}

const clamp = (p: number) => Math.min(0.999, Math.max(0.001, p));

/** Pool adjacent violators for a non-increasing sequence (ascending x). */
export function nonIncreasing(points: ExceedancePoint[]): ExceedancePoint[] {
  type Block = { p: number; w: number; members: ExceedancePoint[] };
  const blocks: Block[] = [];
  for (const pt of points) {
    blocks.push({ p: pt.p, w: Math.max(1, pt.books), members: [pt] });
    while (blocks.length > 1 && blocks[blocks.length - 2].p < blocks[blocks.length - 1].p) {
      const b = blocks.pop()!;
      const a = blocks.pop()!;
      const w = a.w + b.w;
      blocks.push({ p: (a.p * a.w + b.p * b.w) / w, w, members: [...a.members, ...b.members] });
    }
  }
  return blocks.flatMap((b) => b.members.map((m) => ({ ...m, p: b.p })));
}

/** S(x) by linear interpolation between curve points; null outside the curve. */
export function exceedanceAt(curve: ExceedancePoint[], x: number): number | null {
  if (curve.length === 0 || x < curve[0].point || x > curve[curve.length - 1].point) return null;
  for (let i = 0; i < curve.length; i++) {
    if (curve[i].point === x) return curve[i].p;
    if (curve[i].point > x) {
      const a = curve[i - 1];
      const b = curve[i];
      return a.p + ((x - a.point) / (b.point - a.point)) * (b.p - a.p);
    }
  }
  return null;
}

/** The x where S(x) falls to q; null when the ladder does not reach q. */
export function quantileAt(curve: ExceedancePoint[], q: number): number | null {
  if (curve.length === 0 || curve[0].p < q || curve[curve.length - 1].p > q) return null;
  for (let i = 1; i < curve.length; i++) {
    const a = curve[i - 1];
    const b = curve[i];
    if (a.p >= q && b.p <= q) {
      if (a.p === b.p) return a.point;
      return a.point + ((a.p - q) / (a.p - b.p)) * (b.point - a.point);
    }
  }
  return null;
}

export function buildDistribution(
  main: Record<string, MainQuote>,
  ladders: Record<string, { ladder?: Rung[] | null }>,
  options: { devig?: Devig } = {},
): PointsDistribution | null {
  const power = options.devig === "power";
  const byPoint = new Map<number, number[]>();
  const add = (point: number, p: number) => {
    const list = byPoint.get(point) ?? [];
    list.push(clamp(p));
    byPoint.set(point, list);
  };

  // Each book's main line is a rung too, and its overround de-vigs that book's ladder.
  const overround = new Map<string, number>();
  const bookK = new Map<string, number | null>();
  for (const [book, q] of Object.entries(main)) {
    if (!Number.isFinite(q.line) || !validAmerican(q.over) || !validAmerican(q.under)) continue;
    const o = implied(q.over);
    const u = implied(q.under);
    overround.set(book, o + u);
    if (power) bookK.set(book, powerExponent(o, u));
    add(q.line, o / (o + u));
  }

  let skippedRungs = 0;
  let ladderBooks = 0;
  for (const [book, entry] of Object.entries(ladders)) {
    const rungs = (entry?.ladder ?? []).filter((r) => Number.isFinite(r.point) && validAmerican(r.price));
    if (rungs.length === 0) continue;
    const sides = new Map<number, { over?: number; under?: number }>();
    for (const r of rungs) {
      const s = sides.get(r.point) ?? {};
      if (r.side === "over") s.over = r.price;
      else if (r.side === "under") s.under = r.price;
      sides.set(r.point, s);
    }
    const m = overround.get(book);
    const k = power ? bookK.get(book) ?? null : null;
    const oneSidedOk = power ? k != null : m != null;
    let used = 0;
    for (const [point, s] of sides) {
      if (s.over != null && s.under != null) {
        const o = implied(s.over);
        add(point, o / (o + implied(s.under)));
        used++;
      } else if (oneSidedOk && s.over != null) {
        add(point, power ? implied(s.over) ** k! : implied(s.over) / m!);
        used++;
      } else if (oneSidedOk && s.under != null) {
        add(point, 1 - (power ? implied(s.under) ** k! : implied(s.under) / m!));
        used++;
      } else {
        skippedRungs++;
      }
    }
    if (used > 0) ladderBooks++;
  }

  if (ladderBooks === 0) return null;
  const raw = [...byPoint.entries()]
    .map(([point, ps]) => ({ point, p: median(ps), books: ps.length }))
    .sort((a, b) => a.point - b.point);
  if (raw.length < MIN_CURVE_POINTS) return null;
  const curve = nonIncreasing(raw);
  const at = (n: number) => exceedanceAt(curve, n - 0.5);
  return {
    curve,
    floor: quantileAt(curve, 0.9),
    ceiling: quantileAt(curve, 0.1),
    atLeast: { 20: at(20), 25: at(25), 30: at(30), 35: at(35), 40: at(40) },
    ladderBooks,
    skippedRungs,
  };
}
