/**
 * Which kind of Showdown lineup is likely to be the best one on this slate
 * (`nfl-slate-shape-v1`).
 *
 * Simulate the game many times with the optimizer's own correlated draw model
 * (`lineup-draws.ts`, same ranges and same chance of a 0), find the best legal
 * lineup in each simulated game (captain at 1.5x, $50,000, players from both
 * teams), and count its shape: captain, team split, quarterbacks, kicker. The
 * shares are this model's answer to "which lineup type wins", not a measured
 * rate; the Results step grades them against the real best lineup.
 *
 * The best lineup per game is exact: a knapsack over salary in $100 units,
 * with prefix and suffix tables so each captain is excluded from the flex in
 * O(1) tables, and a team mask so 6-0 lineups are never chosen.
 */
import { buildDrawModel, drawOutcome, DEFAULT_DRAW_SEED, type DrawPlayer } from "./lineup-draws";

export const SLATE_SHAPE_PREDICTION_VERSION = "nfl-slate-shape-v1";
export const DEFAULT_SHAPE_DRAWS = 1000;
const CAP_UNITS = 500; // $50,000 in $100 units
const FLEX = 5;

export interface PredictionPlayer {
  dkPlayerId: number;
  name: string;
  team: string;
  opponent: string | null;
  gameKey: string | null;
  position: string;
  salary: number;
  captainSalary: number | null;
  ourProj: number | null;
  floorFpts: number | null;
  medianFpts?: number | null;
  ceilingFpts: number | null;
  rangeCalibration?: { notPlay: number; ifPlays: { mean: number; p10: number; median: number; p90: number } } | null;
  isOut: boolean;
}

export interface ShowdownLineup { captain: number; flex: number[]; score: number }

/** The range the optimizer draws a player from on our own projection (mirrors `rangeFor` in nfl-optimizer.ts for source "our"). */
export function drawPlayerFor(p: PredictionPlayer): DrawPlayer {
  const proj = p.ourProj ?? 0;
  const cal = p.rangeCalibration;
  if (cal && cal.ifPlays.mean > 0 && proj > 0) {
    const notPlay = Math.min(Math.max(cal.notPlay, 0), 0.99);
    const mean = proj / (1 - notPlay), k = mean / cal.ifPlays.mean;
    return { id: p.dkPlayerId, position: p.position, team: p.team, opponent: p.opponent, gameKey: p.gameKey,
      mean, p10: cal.ifPlays.p10 * k, median: cal.ifPlays.median * k, p90: cal.ifPlays.p90 * k, notPlay };
  }
  return { id: p.dkPlayerId, position: p.position, team: p.team, opponent: p.opponent, gameKey: p.gameKey,
    mean: proj, p10: p.floorFpts, median: p.medianFpts ?? null, p90: p.ceilingFpts, notPlay: 0 };
}

export interface SolverPlayer { id: number; team: string; salary: number; captainSalary: number }

/**
 * Exact best Showdown lineup for one set of player points. Null when no legal
 * lineup exists. `players` must hold exactly two teams.
 */
export class ShowdownSolver {
  private readonly n: number;
  private readonly sal: Int32Array;
  private readonly capSal: Int32Array;
  private readonly bit: Int32Array;
  private readonly pre: Float64Array;
  private readonly suf: Float64Array;
  private readonly stride = (CAP_UNITS + 1);

  constructor(readonly players: readonly SolverPlayer[], readonly teams: readonly [string, string]) {
    this.n = players.length;
    this.sal = Int32Array.from(players, (p) => Math.round(p.salary / 100));
    this.capSal = Int32Array.from(players, (p) => Math.round(p.captainSalary / 100));
    this.bit = Int32Array.from(players, (p) => (p.team === teams[0] ? 1 : 2));
    const size = (this.n + 1) * 4 * (FLEX + 1) * this.stride;
    this.pre = new Float64Array(size);
    this.suf = new Float64Array(size);
  }

  private at(i: number, m: number, k: number, s: number) { return ((i * 4 + m) * (FLEX + 1) + k) * this.stride + s; }

  /** Fill one table: `forward` = prefix over players 0..i-1, else suffix over i..n-1. */
  private fill(table: Float64Array, value: Float64Array, forward: boolean) {
    const { n, stride } = this;
    const block = 4 * (FLEX + 1) * stride;
    const start = forward ? 0 : n;
    table.fill(-Infinity, start * block, (start + 1) * block);
    for (let s = 0; s <= CAP_UNITS; s++) table[this.at(start, 0, 0, s)] = 0;
    for (let step = 0; step < n; step++) {
      const from = forward ? step : n - step, to = forward ? step + 1 : n - step - 1;
      const p = forward ? step : n - step - 1;
      const v = value[p], c = this.sal[p], b = this.bit[p];
      table.copyWithin(to * block, from * block, (from + 1) * block);
      for (let m = 1; m < 4; m++) {
        if (!(m & b)) continue;
        for (let k = 1; k <= FLEX; k++) {
          const dst = this.at(to, m, k, 0);
          const srcSame = this.at(from, m, k - 1, 0), srcWithout = this.at(from, m & ~b, k - 1, 0);
          for (let s = c; s <= CAP_UNITS; s++) {
            const a = table[srcSame + s - c], w = table[srcWithout + s - c];
            const best = (a > w ? a : w) + v;
            if (best > table[dst + s]) table[dst + s] = best;
          }
        }
      }
    }
  }

  solve(points: Float64Array): ShowdownLineup | null {
    const { n } = this;
    this.fill(this.pre, points, true);
    this.fill(this.suf, points, false);
    const { pre, suf } = this;
    let best = -Infinity, found: { c: number; k1: number; s1: number } | null = null;
    for (let c = 0; c < n; c++) {
      const budget = CAP_UNITS - this.capSal[c];
      if (budget < 0) continue;
      // The flex must include the other team; masks: 1 = first team, 2 = second, 3 = both.
      const other = this.bit[c] === 1 ? 2 : 1, own = 3 - other;
      const cap = 1.5 * points[c];
      for (let k1 = 0; k1 <= FLEX; k1++) {
        const k2 = FLEX - k1;
        const l0 = this.at(c, 0, k1, 0), lOwn = this.at(c, own, k1, 0), lOther = this.at(c, other, k1, 0), lBoth = this.at(c, 3, k1, 0);
        const r0 = this.at(c + 1, 0, k2, 0), rOwn = this.at(c + 1, own, k2, 0), rOther = this.at(c + 1, other, k2, 0), rBoth = this.at(c + 1, 3, k2, 0);
        for (let s1 = 0; s1 <= budget; s1++) {
          const s2 = budget - s1;
          const lWith = Math.max(pre[lOther + s1], pre[lBoth + s1]);
          const lAny = Math.max(lWith, pre[l0 + s1], pre[lOwn + s1]);
          const rWith = Math.max(suf[rOther + s2], suf[rBoth + s2]);
          const rAny = Math.max(rWith, suf[r0 + s2], suf[rOwn + s2]);
          const total = Math.max(lWith + rAny, lAny + rWith) + cap;
          if (total > best) { best = total; found = { c, k1, s1 }; }
        }
      }
    }
    if (!found) return null;
    // Recover the masks behind the best total.
    let pick: { c: number; k1: number; s1: number; m1: number; m2: number } | null = null;
    {
      const { c, k1, s1 } = found, budget = CAP_UNITS - this.capSal[c], other = this.bit[c] === 1 ? 2 : 1;
      const cap = 1.5 * points[c];
      for (let m1 = 0; m1 < 4 && !pick; m1++) for (let m2 = 0; m2 < 4 && !pick; m2++) {
        if (!((m1 | m2) & other)) continue;
        if (pre[this.at(c, m1, k1, s1)] + suf[this.at(c + 1, m2, FLEX - k1, budget - s1)] + cap === best) pick = { c, k1, s1, m1, m2 };
      }
    }
    if (!pick) return null;
    const flex = [
      ...this.trace(this.pre, points, true, pick.c, pick.m1, pick.k1, pick.s1),
      ...this.trace(this.suf, points, false, pick.c + 1, pick.m2, FLEX - pick.k1, CAP_UNITS - this.capSal[pick.c] - pick.s1),
    ];
    return { captain: this.players[pick.c].id, flex: flex.map((i) => this.players[i].id), score: best };
  }

  /** Recover the chosen players behind table cell (i, m, k, s). */
  private trace(table: Float64Array, value: Float64Array, forward: boolean, i: number, m: number, k: number, s: number): number[] {
    const chosen: number[] = [];
    while (k > 0) {
      const prev = forward ? i - 1 : i + 1;
      const p = forward ? i - 1 : i;
      const here = table[this.at(i, m, k, s)];
      if (table[this.at(prev, m, k, s)] === here) { i = prev; continue; }
      const b = this.bit[p], c = this.sal[p];
      const same = table[this.at(prev, m, k - 1, s - c)] + value[p];
      const nextM = same === here ? m : m & ~b;
      chosen.push(p); i = prev; m = nextM; k -= 1; s -= c;
    }
    return chosen;
  }
}

export interface ShareRow { key: string; label: string; pct: number }

export interface SlateShapePrediction {
  version: string;
  draws: number;
  solved: number;
  seed: number;
  splitKinds: ShareRow[];
  splits: ShareRow[];
  captains: (ShareRow & { dkPlayerId: number; position: string; team: string })[];
  quarterbacks: ShareRow[];
  kicker: number;
  dst: number;
  /** Best-lineup score across simulated games. */
  score: { p10: number; median: number; p90: number };
  /** Eligible players drawn at their projection (no usable range). */
  fixed: string[];
}

/** Shape of a Showdown lineup, by the same rules as `lineupShape`. */
export function showdownShape(captain: number, flex: readonly number[], byId: ReadonlyMap<number, PredictionPlayer>) {
  const ids = [captain, ...flex];
  const teams = new Map<string, number>();
  for (const id of ids) { const t = byId.get(id)!.team; teams.set(t, (teams.get(t) ?? 0) + 1); }
  const sides = [...teams].sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]));
  const big = sides[0][1], small = sides[1]?.[1] ?? 0, kind = `${big}-${small}`;
  const positions = ids.map((id) => byId.get(id)!.position);
  const qbs = positions.filter((p) => p === "QB").length;
  return { splitKind: kind, split: big === small ? kind : `${sides[0][0]} ${kind}`,
    quarterbacks: qbs >= 2 ? "Both QBs" : qbs === 1 ? "One QB" : "No QB",
    kicker: positions.includes("K"), dst: positions.includes("DST") };
}

export function eligiblePredictionPlayers(players: readonly PredictionPlayer[]): PredictionPlayer[] {
  return players.filter((p) => !p.isOut && (p.ourProj ?? 0) > 0 && p.captainSalary != null && p.salary > 0);
}

/**
 * Run the prediction. `onProgress` lets the page yield between chunks; the
 * result is the same with or without it.
 */
export async function predictShowdownShapes(players: readonly PredictionPlayer[],
  { draws = DEFAULT_SHAPE_DRAWS, seed = DEFAULT_DRAW_SEED, chunk = 50, onProgress }: { draws?: number; seed?: number; chunk?: number; onProgress?: (done: number) => void } = {}): Promise<SlateShapePrediction> {
  const pool = eligiblePredictionPlayers(players);
  const teams = [...new Set(pool.map((p) => p.team))].sort();
  if (teams.length !== 2) throw new Error(`A Showdown prediction needs players from exactly two teams; found ${teams.length}.`);
  const byId = new Map(pool.map((p) => [p.dkPlayerId, p]));
  const model = buildDrawModel(pool.map(drawPlayerFor), seed);
  const solver = new ShowdownSolver(pool.map((p) => ({ id: p.dkPlayerId, team: p.team, salary: p.salary, captainSalary: p.captainSalary! })), [teams[0], teams[1]]);
  const points = new Float64Array(pool.length);
  const count = { kind: new Map<string, number>(), split: new Map<string, number>(), captain: new Map<number, number>(), qb: new Map<string, number>() };
  const bump = <K,>(m: Map<K, number>, k: K) => m.set(k, (m.get(k) ?? 0) + 1);
  let kicker = 0, dst = 0, solved = 0;
  const scores: number[] = [];
  for (let d = 0; d < draws; d++) {
    const outcome = drawOutcome(model, d);
    pool.forEach((p, i) => { points[i] = outcome.get(p.dkPlayerId) ?? 0; });
    const best = solver.solve(points);
    if (best) {
      solved += 1; scores.push(best.score);
      const shape = showdownShape(best.captain, best.flex, byId);
      bump(count.kind, shape.splitKind); bump(count.split, shape.split); bump(count.captain, best.captain); bump(count.qb, shape.quarterbacks);
      if (shape.kicker) kicker += 1;
      if (shape.dst) dst += 1;
    }
    if (onProgress && (d + 1) % chunk === 0) { onProgress(d + 1); await new Promise((r) => setTimeout(r, 0)); }
  }
  const pct = (n: number) => (solved ? (100 * n) / solved : 0);
  const rows = (m: Map<string, number>, order?: string[]) => [...m].map(([key, n]) => ({ key, label: key, pct: pct(n) }))
    .sort((a, b) => order ? (order.indexOf(a.key) + 1 || 99) - (order.indexOf(b.key) + 1 || 99) : b.pct - a.pct);
  scores.sort((a, b) => a - b);
  const q = (f: number) => (scores.length ? scores[Math.min(scores.length - 1, Math.floor(f * scores.length))] : 0);
  return {
    version: SLATE_SHAPE_PREDICTION_VERSION, draws, solved, seed,
    splitKinds: rows(count.kind, ["3-3", "4-2", "5-1"]),
    splits: rows(count.split).filter((r) => r.key.includes(" ")),
    captains: [...count.captain].map(([id, n]) => { const p = byId.get(id)!; return { key: String(id), label: p.name, pct: pct(n), dkPlayerId: id, position: p.position, team: p.team }; })
      .sort((a, b) => b.pct - a.pct),
    quarterbacks: rows(count.qb, ["Both QBs", "One QB", "No QB"]),
    kicker: pct(kicker), dst: pct(dst),
    score: { p10: q(0.1), median: q(0.5), p90: q(0.9) },
    fixed: model.fixed.map((id) => byId.get(id)!.name),
  };
}

/** The real best lineup, from the points DraftKings paid. Players with no reported points count as 0. */
export function actualBestShowdown(players: readonly PredictionPlayer[], actual: ReadonlyMap<number, number>) {
  const pool = players.filter((p) => p.captainSalary != null && p.salary > 0);
  const teams = [...new Set(pool.map((p) => p.team))].sort();
  if (teams.length !== 2) return null;
  const solver = new ShowdownSolver(pool.map((p) => ({ id: p.dkPlayerId, team: p.team, salary: p.salary, captainSalary: p.captainSalary! })), [teams[0], teams[1]]);
  const best = solver.solve(Float64Array.from(pool, (p) => actual.get(p.dkPlayerId) ?? 0));
  if (!best) return null;
  const byId = new Map(pool.map((p) => [p.dkPlayerId, p]));
  return { ...best, shape: showdownShape(best.captain, best.flex, byId), names: { captain: byId.get(best.captain)!.name, flex: best.flex.map((id) => byId.get(id)!.name) } };
}
