/**
 * NFL field-ownership prior — `nfl-ownership-prior-v1`.
 *
 * A STATED PRIOR, not a fitted model. It exists so the optimizer's leverage,
 * fade and duplication features have an input at all: with no ownership the
 * server resolves capability `unavailable` and every one of them is off, and
 * the optimizer maximises raw projection (which is how it handed back Josh
 * Allen chalk or a Jameis Winston punt with nothing in between).
 *
 * Shape, in plain terms: the field drafts value and points, anchored on the
 * number DraftKings prints next to the player (his season average), and it
 * concentrates hard on the top few at each position. So
 *
 *   blend  = 0.6 * our projection + 0.4 * DK season average   (DK avg alone if we have no projection)
 *   value  = blend per $1,000 of salary
 *   score  = blend^2 * value^1.5 * status multiplier   (Q 0.6, D 0.25)
 *   share  = score / sum(score) within the position, times that position's
 *            roster budget (QB 100%, RB 255%, WR 340%, TE 105%, DST 100% —
 *            the 9 Classic slots with FLEX split 55/40/5 RB/WR/TE), capped at
 *            60% per player with the excess redistributed.
 *
 * Every constant above is a judgement. `scripts/nfl-ownership-calibration.ts`
 * grades the prior against imported contest ownership; the fitted replacement
 * and its promotion gate are registered in docs/nfl-ownership-model.md. This
 * source declares itself heuristic, so `assessOwnership` can never rate it
 * `validated` — leverage runs only through the user's explicit opt-in.
 *
 * Deterministic and pure: same inputs, same output, no clock, no randomness.
 */

/**
 * v2 (2026-09-29, after PHI@CHI 2026-09-28):
 *   - Showdown: a player's captain + flex ownership is at most
 *     SHOWDOWN_TOTAL_MAX_PCT. A player fills one slot per lineup, so the two
 *     can never sum past 100%; v1 capped them separately (90 + 50) and read
 *     Swift at 111% and Hurts at 100%, which the leverage factor then crushed.
 *   - Value is computed on at least VALUE_SALARY_FLOOR of salary. Points per
 *     $1,000 explodes near a $200 Showdown salary; v1 put Salvon Ahmed ($200,
 *     5.8 projected) at 93%.
 * Both are structural corrections, not fitted constants.
 */
export const NFL_OWNERSHIP_PRIOR_VERSION = "nfl-ownership-prior-v2";

/** Roster budget per position for a 9-slot Classic lineup, in percent. Sums to 900. */
export const CLASSIC_POSITION_BUDGETS: Readonly<Record<string, number>> = { QB: 100, RB: 255, WR: 340, TE: 105, DST: 100 };
export const CLASSIC_MAX_PCT = 60;
/** Showdown: one Captain slot (100%) and five Flex slots (500%). */
export const SHOWDOWN_FLEX_BUDGET = 500;
export const SHOWDOWN_CAPTAIN_BUDGET = 100;
export const SHOWDOWN_FLEX_MAX_PCT = 90;
export const SHOWDOWN_CAPTAIN_MAX_PCT = 50;
/** A player is in at most one slot per lineup, so captain + flex never passes this. */
export const SHOWDOWN_TOTAL_MAX_PCT = 95;
/**
 * Value (points per $1,000) is computed on at least this salary: DraftKings'
 * Classic minimum for a skill player. Below it the ratio measures the price
 * floor, not how attractive the player is to the field.
 */
export const VALUE_SALARY_FLOOR = 3000;
/** Captain ownership concentrates harder than flex: same scores, steeper exponent. */
export const CAPTAIN_EXPONENT = 1.5;
export const STATUS_MULTIPLIER: Readonly<Record<string, number>> = { Q: 0.6, D: 0.25 };
const PROJECTION_WEIGHT = 0.6;
const POINTS_EXPONENT = 2;
const VALUE_EXPONENT = 1.5;

export interface OwnershipPriorPlayer {
  dkPlayerId: number;
  position: string;
  salary: number;
  /** Our projection; null when the model has none. */
  projection: number | null;
  /** DraftKings' printed season average; null when absent. */
  dkAvg: number | null;
  isOut: boolean;
  /** DraftKings' Status column ("Q", "D", "" ...). */
  dkStatus?: string | null;
  captainSalary?: number | null;
}

export interface OwnershipPriorRow {
  dkPlayerId: number;
  /** Total projected ownership across all slots, percent. */
  ownPct: number;
  /** Showdown only: Captain-slot ownership, percent. */
  captainPct: number | null;
  /** Showdown only: Flex-slot ownership, percent. */
  flexPct: number | null;
}

export interface OwnershipPriorResult {
  version: string;
  format: "classic" | "showdown";
  /** What the ownPct column is meant to sum to across the pool. */
  budgetPct: number;
  /** Budget the cap left unplaced (a pool too thin to hold it). 0 on any real slate. */
  unallocatedPct: number;
  players: OwnershipPriorRow[];
}

/** The number the field is drafting on. Null when neither source exists. */
export function fieldPoints(player: Pick<OwnershipPriorPlayer, "projection" | "dkAvg">): number | null {
  const proj = player.projection != null && Number.isFinite(player.projection) ? player.projection : null;
  const avg = player.dkAvg != null && Number.isFinite(player.dkAvg) ? player.dkAvg : null;
  if (proj == null && avg == null) return null;
  if (proj == null) return avg;
  if (avg == null) return proj;
  return PROJECTION_WEIGHT * proj + (1 - PROJECTION_WEIGHT) * avg;
}

/** Unnormalised attractiveness to the field; 0 for anyone who is out or has nothing to draft on. */
export function ownershipScore(player: OwnershipPriorPlayer, salary = player.salary): number {
  if (player.isOut || !(salary > 0)) return 0;
  const points = fieldPoints(player);
  if (points == null || points <= 0) return 0;
  const value = points / (Math.max(salary, VALUE_SALARY_FLOOR) / 1000);
  const status = STATUS_MULTIPLIER[(player.dkStatus ?? "").trim().toUpperCase()] ?? 1;
  return Math.pow(Math.max(points, 0.5), POINTS_EXPONENT) * Math.pow(Math.max(value, 0.2), VALUE_EXPONENT) * status;
}

/**
 * Scale scores to a budget with a per-player cap. A capped player keeps the
 * cap and the excess is re-shared among the uncapped, repeated until no one
 * is over the cap. Returns percents keyed by dkPlayerId.
 */
export function allocateBudget(scores: ReadonlyMap<number, number>, budget: number, maxPct: number): Map<number, number> {
  return allocateBudgetDetailed(scores, budget, maxPct).shares;
}

/** `maxPct` may be one cap for everyone or a per-player cap. */
export function allocateBudgetDetailed(scores: ReadonlyMap<number, number>, budget: number, maxPct: number | ((id: number) => number)): { shares: Map<number, number>; unallocated: number } {
  const capFor = typeof maxPct === "number" ? () => maxPct : maxPct;
  const out = new Map<number, number>();
  const open = new Map([...scores].filter(([, s]) => s > 0));
  let remaining = budget;
  for (let pass = 0; pass < 16 && open.size; pass += 1) {
    const total = [...open.values()].reduce((a, b) => a + b, 0);
    if (total <= 0) break;
    let capped = false;
    for (const [id, s] of open) {
      const share = remaining * (s / total), cap = Math.max(0, capFor(id));
      if (share > cap) { out.set(id, cap); open.delete(id); remaining -= cap; capped = true; }
    }
    if (!capped) { for (const [id, s] of open) out.set(id, remaining * (s / total)); remaining = 0; open.clear(); }
  }
  for (const id of scores.keys()) if (!out.has(id)) out.set(id, 0);
  return { shares: out, unallocated: Math.max(0, remaining) };
}

export function projectOwnershipPrior(players: readonly OwnershipPriorPlayer[], format: "classic" | "showdown"): OwnershipPriorResult {
  const round = (v: number) => Math.round(v * 100) / 100;
  if (format === "showdown") {
    const flexScores = new Map(players.map((p) => [p.dkPlayerId, ownershipScore(p)]));
    const captainScores = new Map(players.map((p) => [p.dkPlayerId,
      p.captainSalary == null ? 0 : Math.pow(ownershipScore(p, p.captainSalary), CAPTAIN_EXPONENT)]));
    // Captain first; each player's flex cap is what his captain share leaves
    // under the one-slot-per-lineup total.
    const captainAlloc = allocateBudgetDetailed(captainScores, SHOWDOWN_CAPTAIN_BUDGET, SHOWDOWN_CAPTAIN_MAX_PCT);
    const captain = captainAlloc.shares;
    const flexAlloc = allocateBudgetDetailed(flexScores, SHOWDOWN_FLEX_BUDGET,
      (id) => Math.min(SHOWDOWN_FLEX_MAX_PCT, SHOWDOWN_TOTAL_MAX_PCT - (captain.get(id) ?? 0)));
    const flex = flexAlloc.shares;
    return { version: NFL_OWNERSHIP_PRIOR_VERSION, format, budgetPct: SHOWDOWN_FLEX_BUDGET + SHOWDOWN_CAPTAIN_BUDGET,
      unallocatedPct: round(flexAlloc.unallocated + captainAlloc.unallocated),
      players: players.map((p) => { const f = flex.get(p.dkPlayerId) ?? 0, c = captain.get(p.dkPlayerId) ?? 0;
        return { dkPlayerId: p.dkPlayerId, ownPct: round(f + c), captainPct: round(c), flexPct: round(f) }; }) };
  }
  const own = new Map<number, number>();
  let unallocated = 0;
  for (const [position, budget] of Object.entries(CLASSIC_POSITION_BUDGETS)) {
    const group = players.filter((p) => p.position === position);
    const alloc = allocateBudgetDetailed(new Map(group.map((p) => [p.dkPlayerId, ownershipScore(p)])), budget, CLASSIC_MAX_PCT);
    for (const [id, pct] of alloc.shares) own.set(id, pct);
    unallocated += alloc.unallocated;
  }
  return { version: NFL_OWNERSHIP_PRIOR_VERSION, format, budgetPct: Object.values(CLASSIC_POSITION_BUDGETS).reduce((a, b) => a + b, 0), unallocatedPct: round(unallocated),
    players: players.map((p) => ({ dkPlayerId: p.dkPlayerId, ownPct: round(own.get(p.dkPlayerId) ?? 0), captainPct: null, flexPct: null })) };
}
