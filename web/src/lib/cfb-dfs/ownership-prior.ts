/**
 * CFB field-ownership estimate, `cfb-ownership-prior-v1`. Spec:
 * docs/cfb-dfs-ownership-model.md. A stated prior, not a fit: every constant
 * is NFL v5's (web/src/lib/nfl-dfs/ownership-prior.ts) except the three the
 * college roster forces, marked CFB below. Pure, so the player pool can
 * recompute it in the browser when a manual projection changes.
 *
 * Field points = 0.5 x our projection + 0.5 x DraftKings' average [CFB], where
 * "our projection" is the owner's manual number when set. Kickers have no
 * model projection, so without a manual number they draw on the DK average
 * alone. The blend is handed to the NFL functions as the projection (with no
 * DK average), so the NFL scoring and allocation run unchanged on it.
 */
import {
  allocateBudgetDetailed, CLASSIC_FLATTEN_ALPHA_V5, CLASSIC_MAX_PCT, ownershipScore, projectOwnershipPriorV5,
  type OwnershipPriorPlayer,
} from "../nfl-dfs/ownership-prior";
import type { CfbMode } from "./salary-csv";

export const CFB_OWNERSHIP_PRIOR_VERSION = "cfb-ownership-prior-v1";
/** [CFB] Field points = OURS_WEIGHT x our projection + (1 - OURS_WEIGHT) x DK average. */
export const CFB_OURS_WEIGHT = 0.5;
/**
 * [CFB] Classic budgets, summing to the 8 roster slots. From the 2026-09-26
 * contest (196055904): QB 185.5 / RB 253.2 / WR 361.2, 85.5 of the 100 Super
 * Flex points on a second QB. One design contest, rounded.
 */
export const CFB_CLASSIC_BUDGETS: Readonly<Record<string, number>> = { QB: 185, RB: 255, WR: 360 };
/**
 * [CFB] Showdown Captain shares are NFL v5's with TE and DST absent; the v5
 * allocation renormalises over the positions present, giving about
 * QB 26.7 / RB 33.8 / WR 37.4 / K 2.1.
 */

/** Imported contests before this instant are design contests: reported, never counted. */
export const CFB_OWNERSHIP_COUNT_FROM = "2026-11-01T12:19:59Z";

export interface CfbOwnershipInput {
  dkId: number;
  position: string;
  salary: number;
  captainSalary?: number | null;
  /** Model projection; null when the model has none. Kickers are not projected. */
  proj: number | null;
  /** The owner's manual projection, when set. */
  manual?: number | null;
  dkAvg: number | null;
  /** Effective status, canonical (O, D, Q, IR, ...). */
  status: string;
}

export interface CfbOwnershipRow { dkId: number; ownPct: number; captainPct: number | null; utilPct: number | null }
export interface CfbOwnershipResult {
  version: string; mode: CfbMode;
  /** What ownPct sums to across the pool: 800 Classic, 600 Showdown. */
  budgetPct: number;
  /** Budget the caps left unplaced (a pool too thin to hold it). */
  unallocatedPct: number;
  players: CfbOwnershipRow[];
}

const OUT = new Set(["O", "OUT", "IR"]);
const round = (v: number) => Math.round(v * 100) / 100;

/** The number the field is assumed to draft on; null when there is nothing to draft on. */
export function cfbFieldPoints(p: Pick<CfbOwnershipInput, "position" | "proj" | "manual" | "dkAvg">): number | null {
  const ours = p.manual != null && Number.isFinite(p.manual) ? p.manual : p.position === "K" ? null : p.proj;
  const avg = p.dkAvg != null && Number.isFinite(p.dkAvg) ? p.dkAvg : null;
  if (ours == null && avg == null) return null;
  if (ours == null) return avg;
  if (avg == null) return ours;
  return CFB_OURS_WEIGHT * ours + (1 - CFB_OURS_WEIGHT) * avg;
}

function asNfl(p: CfbOwnershipInput): OwnershipPriorPlayer {
  const status = (p.status ?? "").trim().toUpperCase();
  return { dkPlayerId: p.dkId, position: p.position, salary: p.salary, projection: cfbFieldPoints(p), dkAvg: null,
    isOut: OUT.has(status), dkStatus: status, captainSalary: p.captainSalary ?? null };
}

export function projectCfbOwnership(players: readonly CfbOwnershipInput[], mode: CfbMode): CfbOwnershipResult {
  const nfl = players.map(asNfl);
  if (mode === "showdown") {
    const r = projectOwnershipPriorV5(nfl, "showdown");
    return { version: CFB_OWNERSHIP_PRIOR_VERSION, mode, budgetPct: r.budgetPct, unallocatedPct: r.unallocatedPct,
      players: r.players.map((p) => ({ dkId: p.dkPlayerId, ownPct: p.ownPct, captainPct: p.captainPct, utilPct: p.flexPct })) };
  }
  const own = new Map<number, number>();
  let unallocated = 0;
  for (const [position, budget] of Object.entries(CFB_CLASSIC_BUDGETS)) {
    const group = nfl.filter((p) => p.position.toUpperCase() === position);
    const weights = new Map(group.map((p) => { const s = ownershipScore(p); return [p.dkPlayerId, s > 0 ? Math.pow(s, CLASSIC_FLATTEN_ALPHA_V5) : 0]; }));
    const alloc = allocateBudgetDetailed(weights, budget, CLASSIC_MAX_PCT);
    for (const [id, pct] of alloc.shares) own.set(id, pct);
    unallocated += alloc.unallocated;
  }
  return { version: CFB_OWNERSHIP_PRIOR_VERSION, mode, budgetPct: Object.values(CFB_CLASSIC_BUDGETS).reduce((a, b) => a + b, 0),
    unallocatedPct: round(unallocated),
    players: players.map((p) => ({ dkId: p.dkId, ownPct: round(own.get(p.dkId) ?? 0), captainPct: null, utilPct: null })) };
}

export interface CfbOwnershipGradeCell { n: number; spearman: number | null; mae: number; bias: number }
export interface CfbOwnershipGrade {
  /** Classic: every slate player. Showdown: UTIL and Captain separately. */
  total: CfbOwnershipGradeCell | null; util: CfbOwnershipGradeCell | null; captain: CfbOwnershipGradeCell | null;
}

/** Average ranks, ties shared. */
function ranks(xs: readonly number[]): number[] {
  const order = xs.map((v, i) => [v, i] as const).sort((a, b) => a[0] - b[0]);
  const out = new Array<number>(xs.length);
  for (let i = 0; i < order.length;) {
    let j = i;
    while (j + 1 < order.length && order[j + 1][0] === order[i][0]) j += 1;
    for (let k = i; k <= j; k += 1) out[order[k][1]] = (i + j) / 2 + 1;
    i = j + 1;
  }
  return out;
}

function pearson(a: readonly number[], b: readonly number[]): number | null {
  const n = a.length, ma = a.reduce((s, v) => s + v, 0) / n, mb = b.reduce((s, v) => s + v, 0) / n;
  let num = 0, da = 0, db = 0;
  for (let i = 0; i < n; i += 1) { num += (a[i] - ma) * (b[i] - mb); da += (a[i] - ma) ** 2; db += (b[i] - mb) ** 2; }
  return da > 0 && db > 0 ? num / Math.sqrt(da * db) : null;
}

/** Spearman, MAE and bias (forecast minus actual), in percentage points. */
export function gradeCell(pairs: ReadonlyArray<readonly [number, number]>): CfbOwnershipGradeCell | null {
  if (pairs.length < 2) return null;
  const f = pairs.map((p) => p[0]), a = pairs.map((p) => p[1]);
  const rho = pearson(ranks(f), ranks(a));
  return { n: pairs.length, spearman: rho == null ? null : Math.round(rho * 1000) / 1000,
    mae: round(pairs.reduce((s, [x, y]) => s + Math.abs(x - y), 0) / pairs.length),
    bias: round(pairs.reduce((s, [x, y]) => s + (x - y), 0) / pairs.length) };
}

/**
 * Grade a forecast against a contest's actual ownership. Every slate player
 * counts: a player missing from DraftKings' ownership block was drafted by
 * nobody, so his actual is 0%.
 */
export function gradeCfbOwnership(mode: CfbMode, forecast: ReadonlyArray<{ key: string; ownPct: number; captainPct: number | null; utilPct: number | null }>,
  actual: ReadonlyMap<string, { total: number; bySlot: Record<string, number> }>): CfbOwnershipGrade {
  const act = (k: string) => actual.get(k);
  if (mode === "classic") return { total: gradeCell(forecast.map((p) => [p.ownPct, act(p.key)?.total ?? 0] as const)), util: null, captain: null };
  return {
    total: gradeCell(forecast.map((p) => [p.ownPct, act(p.key)?.total ?? 0] as const)),
    util: gradeCell(forecast.map((p) => [p.utilPct ?? 0, act(p.key)?.bySlot.UTIL ?? act(p.key)?.bySlot.FLEX ?? 0] as const)),
    captain: gradeCell(forecast.map((p) => [p.captainPct ?? 0, act(p.key)?.bySlot.CPT ?? 0] as const)),
  };
}
