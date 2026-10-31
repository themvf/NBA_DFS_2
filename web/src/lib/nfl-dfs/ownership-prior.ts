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
 *            (Showdown uses value^0.5 since v3)
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
import { NFL_OWNERSHIP_FIT_SHOWDOWN_VERSION, projectShowdownFit } from "./showdown-ownership-fit";

/**
 * v3 (2026-10-04, Showdown only; Classic is unchanged):
 *   - The Showdown value exponent drops from 1.5 to SHOWDOWN_VALUE_EXPONENT (0.5).
 *     On a six-player, one-game pool the cap is not the binding constraint it is
 *     in Classic, and the field drafts the stars on points far more than on
 *     points per dollar; the heavy value term sent a kicker to 74% owned (actual
 *     28.5%) and over-ranked cheap role players.
 *   - Evidence, stated at its real strength: scored against the two imported
 *     2026 Showdown contests (weeks 2 and 3), flex MAE fell 7.18 -> 6.30 and
 *     5.73 -> 5.48, captain MAE 1.63 -> 1.26 and 1.68 -> 1.52, chalk rank
 *     0.69 -> 0.76 and 0.79 -> 0.80, and the exponent picked on either week
 *     alone was 0.5. The constant was read off those two slates; the two week 4
 *     Showdown contests, imported afterwards, are the holdout, and v3 beat v2
 *     on both (flex MAE 4.35 -> 3.82 and 5.23 -> 4.61, captain MAE 1.63 -> 1.49
 *     and 1.38 -> 1.04, chalk rank 0.90 -> 0.93 and 0.83 -> 0.85). Four
 *     contests, one format: improved, NOT validated. Judged on what the forecast does to a
 *     built portfolio (model/nfl_showdown_ownership_eval.py) it is roughly
 *     neutral: more projected points kept, slightly less uniqueness, on about
 *     the same trade-off line. It does NOT fix true punts (a $1,800 TE owned
 *     19% against 0.8% predicted); those are role and news effects with no
 *     feature here.
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
/**
 * v4 (2026-10-19, before the WAS@SF Monday night Showdown; owner's decision to
 * commit and grade rather than wait for a fitted model). Every constant below is
 * a STATED prior read off the graded misses recorded in
 * docs/nfl-ownership-review-2026-10-19.md; none is fitted, and the scorecard
 * (docs/nfl-ownership-scorecard-v1.md) grades v4 against v3 on frozen forecasts.
 *   Classic
 *   - Position budgets: TE 105 -> 135, RB 255 -> 230, WR 340 -> 335. The
 *     recorded TE ownership total of three full-field Millionaires was 137.71 /
 *     134.09 / 137.12% (weeks 3-5), i.e. a 34-38% TE FLEX share against the 5%
 *     the v1 budgets implied. RB/WR split of the remaining FLEX is a judgement.
 *   Showdown (flex)
 *   - Punt tier: players under SHOWDOWN_PUNT_SALARY_MAX share a reserved
 *     SHOWDOWN_PUNT_FLEX_BUDGET of the 500% flex budget in proportion to
 *     points^1 (no value term, no squaring), capped at SHOWDOWN_PUNT_MAX_PCT.
 *     The field drafts the two or three cheapest viable players at 15-30%
 *     (Ferguson $1,800 19-21.7% actual vs 0.8-1.9% projected; Lance 23.87% vs
 *     1.45%; Treadwell $3,200 19.3% vs 2.4%), and points^2 x value^0.5 cannot
 *     reach that from a 4-point projection. Budget the tier cannot absorb
 *     returns to the main pool, so a slate with no punts is unchanged.
 *   - Kicker flex score x SHOWDOWN_KICKER_MULTIPLIER (Shrader projected 67%
 *     against 33.3% actual after v3).
 *   - DST flex and captain score x the game-script multiplier when the slate
 *     knows the favorite (favorite 1.3, underdog 0.7); the field drafts the
 *     favored defense.
 *   Showdown (captain)
 *   - Captain score x CAPTAIN_POSITION_MULTIPLIER: TE 0.7, K 0.25, DST 0.25.
 *     Direction only; the magnitudes are judgements. Full-field captain shares
 *     (`nfl_dfs_field_ownership` drafted_by_slot.CPT) are the data to fit these
 *     on; winners' captain choices are outcome-selected and were NOT used.
 *   The v3 algorithm stays callable (`variant: "v3"`) so the Python mirror
 *   (model/nfl_showdown_ownership_eval.prior_showdown) and its pins still hold.
 */
export const NFL_OWNERSHIP_PRIOR_V4_VERSION = "nfl-ownership-prior-v4";
export const NFL_OWNERSHIP_PRIOR_V3_VERSION = "nfl-ownership-prior-v3";
export const NFL_OWNERSHIP_PRIOR_V5_VERSION = "nfl-ownership-prior-v5";
/** The version projectOwnershipPrior returns by default. */
export const NFL_OWNERSHIP_PRIOR_VERSION = NFL_OWNERSHIP_PRIOR_V5_VERSION;
export type OwnershipPriorVariant = "v3" | "v4" | "v5" | "fit";

/**
 * v5 (drafted 2026-10-20, NOT the default; the owner switches the default after
 * review). Registration: docs/nfl-ownership-prior-v5.md. Every constant in this
 * block was chosen on the 2026 weeks 2-6 slates (11 Showdown, 5 Classic), which
 * are therefore v5 DESIGN slates; v5 is graded for real only on week 7 onward.
 *   Shape: v3 (points^2 x value^exponent x status), with no kicker cut and no DST
 *   game-script term. Changes:
 *   - Flattening. Each allocation weight is score^alpha before the budget is
 *     shared. Under v3/v4 the top five predicted players of every slate and slot
 *     were over-predicted (+7 to +24 points) and everyone else under. One alpha
 *     per format, chosen on a 0.05 grid by full-pool MAE (scorecard v3, latest
 *     capture at the canonical cutoff): Classic 0.75 (3 complete-label slates,
 *     MAE 1.258 -> 1.205; 0.45 would give 1.36), Showdown 0.45 (11 slates, flex
 *     MAE v3 5.47 -> 4.30; 0.75 would give 5.01). The two optima differ clearly, so
 *     the formats are fitted separately. In Showdown the same alpha applies to the
 *     flex main pool and to the captain weight within a position (one parameter);
 *     a captain-only optimum (~0.65, captain MAE 1.23 vs 1.32) was NOT taken.
 *     The transform is monotone, so it keeps the order within one allocation
 *     group; it can move cross-group order (Classic positions, Showdown captain
 *     positions, the punt tier), and does: Classic pooled Spearman 0.808 -> 0.800.
 *   - Showdown captain by position. The 100% captain budget is split into
 *     position shares CAPTAIN_POSITION_SHARES_V5, the mean over the 11 design
 *     slates of each slate's OBSERVED full-field captain labels summed by
 *     position and normalised to 100 (raw means QB 22.69 / RB 28.63 / WR 31.76 /
 *     TE 11.34 / DST 3.23 / K 1.79; slate SDs 4.6-8.6 points for QB/RB/WR/TE).
 *     Within a position the share goes by the v3 captain score (score^1.5 at
 *     captain salary) raised to alpha, capped at 50%. A position with no eligible
 *     player (every one out, or none listed) gives up its share, and the
 *     remaining positions are renormalised to 100. A position outside the table
 *     gets no captain share. This replaces v4's hand multipliers.
 *   - Punt tier kept as v4 (salary < $3,000 shares 60 of the 500 flex points on
 *     points^1, capped at 30): with it the Showdown optimum is flex MAE 4.30 and
 *     Spearman 0.855; without it 4.52 and 0.845. No kicker multiplier inside it.
 *   - Kickers: no cut (v4's x0.6 under-predicted kicker flex ownership).
 *   - Classic budgets: v4's (QB100/RB230/WR335/TE135/DST100). v3 budgets were
 *     0.007 points better pooled at alpha 0.75, all of it from week 2; v4 budgets
 *     were better on 4 of the 5 Classic slates and match the recorded TE totals.
 *   Deliberately not addressed: news- and hype-driven chalk (no input carries it).
 */
export const CLASSIC_FLATTEN_ALPHA_V5 = 0.75;
export const SHOWDOWN_FLATTEN_ALPHA_V5 = 0.45;
export const CAPTAIN_POSITION_SHARES_V5: Readonly<Record<string, number>> = { QB: 22.82, RB: 28.82, WR: 31.92, TE: 11.39, DST: 3.25, K: 1.80 };

/** v1-v3 roster budget per position for a 9-slot Classic lineup, in percent. Sums to 900. */
export const CLASSIC_POSITION_BUDGETS: Readonly<Record<string, number>> = { QB: 100, RB: 255, WR: 340, TE: 105, DST: 100 };
/** v4: FLEX split 30/35/35 RB/WR/TE, from the recorded TE totals of weeks 3-5. Sums to 900. */
export const CLASSIC_POSITION_BUDGETS_V4: Readonly<Record<string, number>> = { QB: 100, RB: 230, WR: 335, TE: 135, DST: 100 };
/** v4 Showdown constants. All stated; see the v4 note. */
export const SHOWDOWN_PUNT_SALARY_MAX = 3000;
export const SHOWDOWN_PUNT_FLEX_BUDGET = 60;
export const SHOWDOWN_PUNT_MAX_PCT = 30;
export const SHOWDOWN_KICKER_MULTIPLIER = 0.6;
export const CAPTAIN_POSITION_MULTIPLIER: Readonly<Record<string, number>> = { TE: 0.7, K: 0.25, DST: 0.25 };
export const DST_GAME_SCRIPT_MULTIPLIER = { favorite: 1.3, underdog: 0.7 } as const;
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
/** Showdown only; see the v3 note above. Classic keeps VALUE_EXPONENT. */
export const SHOWDOWN_VALUE_EXPONENT = 0.5;

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
  /** DraftKings team abbreviation; v4 Showdown uses it for the DST game-script term. */
  team?: string | null;
}

export interface OwnershipPriorOptions {
  /** Defaults to v4. "v3" reproduces the previous algorithm exactly. */
  variant?: OwnershipPriorVariant;
  /** Showdown game script, when the slate resolved it (null = unknown, no DST adjustment). */
  favoriteTeam?: string | null;
  underdogTeam?: string | null;
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
  /** v4 provenance: what the stated terms did on this slate. Absent on v3. */
  notes?: {
    variant: OwnershipPriorVariant;
    /** Showdown: players in the punt tier and the flex budget they actually took. */
    puntTierPlayers?: number;
    puntTierPct?: number;
    /** Showdown: whether the DST game-script term had a favorite to read. */
    gameScriptKnown?: boolean;
    /** v5 Showdown: captain positions with no eligible player, whose share was renormalised away. */
    captainPositionsMissing?: string[];
  };
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
export function ownershipScore(player: OwnershipPriorPlayer, salary = player.salary, valueExponent = VALUE_EXPONENT): number {
  if (player.isOut || !(salary > 0)) return 0;
  const points = fieldPoints(player);
  if (points == null || points <= 0) return 0;
  const value = points / (Math.max(salary, VALUE_SALARY_FLOOR) / 1000);
  const status = STATUS_MULTIPLIER[(player.dkStatus ?? "").trim().toUpperCase()] ?? 1;
  return Math.pow(Math.max(points, 0.5), POINTS_EXPONENT) * Math.pow(Math.max(value, 0.2), valueExponent) * status;
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

export function projectOwnershipPrior(players: readonly OwnershipPriorPlayer[], format: "classic" | "showdown", options: OwnershipPriorOptions = {}): OwnershipPriorResult {
  // Default v5 from 2026-10-20 (owner): docs/nfl-ownership-prior-v5.md. Showdown
  // defaults to the fitted model from 2026-10-31 (owner): showdown-ownership-fit.ts.
  const variant = options.variant ?? (format === "showdown" ? "fit" : "v5");
  if (variant === "fit" && format === "showdown") return projectOwnershipShowdownFit(players);
  if (variant === "v3") return projectOwnershipPriorV3(players, format);
  if (variant === "v5" || variant === "fit") return projectOwnershipPriorV5(players, format);
  const round = (v: number) => Math.round(v * 100) / 100;
  const pos = (p: OwnershipPriorPlayer) => (p.position ?? "").toUpperCase();
  if (format === "showdown") {
    const fav = options.favoriteTeam ?? null, dog = options.underdogTeam ?? null;
    const gameScript = (p: OwnershipPriorPlayer) => {
      if (pos(p) !== "DST" || !p.team || !fav) return 1;
      if (p.team === fav) return DST_GAME_SCRIPT_MULTIPLIER.favorite;
      if (dog == null || p.team === dog) return DST_GAME_SCRIPT_MULTIPLIER.underdog;
      return 1;
    };
    const positional = (p: OwnershipPriorPlayer) => (pos(p) === "K" ? SHOWDOWN_KICKER_MULTIPLIER : 1) * gameScript(p);
    const isPunt = (p: OwnershipPriorPlayer) => p.salary > 0 && p.salary < SHOWDOWN_PUNT_SALARY_MAX && !p.isOut;
    // Captain first (v3 shape, plus the positional prior), so each player's flex
    // cap is what his captain share leaves under the one-slot total.
    const captainScores = new Map(players.map((p) => [p.dkPlayerId,
      p.captainSalary == null ? 0
        : Math.pow(ownershipScore(p, p.captainSalary, SHOWDOWN_VALUE_EXPONENT), CAPTAIN_EXPONENT)
          * (CAPTAIN_POSITION_MULTIPLIER[pos(p)] ?? 1) * gameScript(p)]));
    const captainAlloc = allocateBudgetDetailed(captainScores, SHOWDOWN_CAPTAIN_BUDGET, SHOWDOWN_CAPTAIN_MAX_PCT);
    const captain = captainAlloc.shares;
    const flexCap = (id: number) => Math.min(SHOWDOWN_FLEX_MAX_PCT, SHOWDOWN_TOTAL_MAX_PCT - (captain.get(id) ?? 0));
    // Punt tier: a reserved slice of the flex budget, shared on points alone.
    const puntScores = new Map(players.filter(isPunt).map((p) => {
      const pts = fieldPoints(p);
      const status = STATUS_MULTIPLIER[(p.dkStatus ?? "").trim().toUpperCase()] ?? 1;
      return [p.dkPlayerId, pts == null || pts <= 0 ? 0 : Math.max(pts, 0.5) * status * positional(p)];
    }));
    const puntAlloc = allocateBudgetDetailed(puntScores, SHOWDOWN_PUNT_FLEX_BUDGET, (id) => Math.min(SHOWDOWN_PUNT_MAX_PCT, flexCap(id)));
    const puntTaken = SHOWDOWN_PUNT_FLEX_BUDGET - puntAlloc.unallocated;
    // Everyone else shares the rest, v3 shape plus the positional terms.
    const mainScores = new Map(players.filter((p) => !isPunt(p)).map((p) => [p.dkPlayerId, ownershipScore(p, p.salary, SHOWDOWN_VALUE_EXPONENT) * positional(p)]));
    const mainAlloc = allocateBudgetDetailed(mainScores, SHOWDOWN_FLEX_BUDGET - puntTaken, flexCap);
    const flex = new Map<number, number>([...puntAlloc.shares, ...mainAlloc.shares]);
    return { version: NFL_OWNERSHIP_PRIOR_V4_VERSION, format, budgetPct: SHOWDOWN_FLEX_BUDGET + SHOWDOWN_CAPTAIN_BUDGET,
      unallocatedPct: round(mainAlloc.unallocated + captainAlloc.unallocated),
      notes: { variant, puntTierPlayers: puntScores.size, puntTierPct: round(puntTaken), gameScriptKnown: fav != null },
      players: players.map((p) => { const f = flex.get(p.dkPlayerId) ?? 0, c = captain.get(p.dkPlayerId) ?? 0;
        return { dkPlayerId: p.dkPlayerId, ownPct: round(f + c), captainPct: round(c), flexPct: round(f) }; }) };
  }
  const own = new Map<number, number>();
  let unallocated = 0;
  for (const [position, budget] of Object.entries(CLASSIC_POSITION_BUDGETS_V4)) {
    const group = players.filter((p) => pos(p) === position);
    const alloc = allocateBudgetDetailed(new Map(group.map((p) => [p.dkPlayerId, ownershipScore(p)])), budget, CLASSIC_MAX_PCT);
    for (const [id, pct] of alloc.shares) own.set(id, pct);
    unallocated += alloc.unallocated;
  }
  return { version: NFL_OWNERSHIP_PRIOR_V4_VERSION, format, budgetPct: Object.values(CLASSIC_POSITION_BUDGETS_V4).reduce((a, b) => a + b, 0), unallocatedPct: round(unallocated),
    notes: { variant },
    players: players.map((p) => ({ dkPlayerId: p.dkPlayerId, ownPct: round(own.get(p.dkPlayerId) ?? 0), captainPct: null, flexPct: null })) };
}

/** The v3 algorithm, unchanged, mirrored by model/nfl_showdown_ownership_eval.prior_showdown. */
export function projectOwnershipPriorV3(players: readonly OwnershipPriorPlayer[], format: "classic" | "showdown"): OwnershipPriorResult {
  const round = (v: number) => Math.round(v * 100) / 100;
  if (format === "showdown") {
    const flexScores = new Map(players.map((p) => [p.dkPlayerId, ownershipScore(p, p.salary, SHOWDOWN_VALUE_EXPONENT)]));
    const captainScores = new Map(players.map((p) => [p.dkPlayerId,
      p.captainSalary == null ? 0 : Math.pow(ownershipScore(p, p.captainSalary, SHOWDOWN_VALUE_EXPONENT), CAPTAIN_EXPONENT)]));
    // Captain first; each player's flex cap is what his captain share leaves
    // under the one-slot-per-lineup total.
    const captainAlloc = allocateBudgetDetailed(captainScores, SHOWDOWN_CAPTAIN_BUDGET, SHOWDOWN_CAPTAIN_MAX_PCT);
    const captain = captainAlloc.shares;
    const flexAlloc = allocateBudgetDetailed(flexScores, SHOWDOWN_FLEX_BUDGET,
      (id) => Math.min(SHOWDOWN_FLEX_MAX_PCT, SHOWDOWN_TOTAL_MAX_PCT - (captain.get(id) ?? 0)));
    const flex = flexAlloc.shares;
    return { version: NFL_OWNERSHIP_PRIOR_V3_VERSION, format, budgetPct: SHOWDOWN_FLEX_BUDGET + SHOWDOWN_CAPTAIN_BUDGET,
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
  return { version: NFL_OWNERSHIP_PRIOR_V3_VERSION, format, budgetPct: Object.values(CLASSIC_POSITION_BUDGETS).reduce((a, b) => a + b, 0), unallocatedPct: round(unallocated),
    players: players.map((p) => ({ dkPlayerId: p.dkPlayerId, ownPct: round(own.get(p.dkPlayerId) ?? 0), captainPct: null, flexPct: null })) };
}

/** score^alpha, with 0 kept at 0 (an ineligible player stays ineligible). */
function flatten(score: number, alpha: number): number {
  return score > 0 ? Math.pow(score, alpha) : 0;
}

/** Showdown with the fitted model (`nfl-ownership-fit-showdown-v1`), same result shape as the priors. */
export function projectOwnershipShowdownFit(players: readonly OwnershipPriorPlayer[]): OwnershipPriorResult {
  const round = (v: number) => Math.round(v * 100) / 100;
  const fit = projectShowdownFit(players.map((p) => ({ id: p.dkPlayerId, position: p.position, salary: p.salary, team: p.team ?? null,
    dkStatus: p.dkStatus ?? null, pts: fieldPoints(p) ?? 0, isOut: p.isOut })));
  const placed = [...fit.values()].reduce((t, v) => t + v.flex + v.captain, 0);
  return { version: NFL_OWNERSHIP_FIT_SHOWDOWN_VERSION, format: "showdown", budgetPct: SHOWDOWN_FLEX_BUDGET + SHOWDOWN_CAPTAIN_BUDGET,
    unallocatedPct: round(Math.max(0, SHOWDOWN_FLEX_BUDGET + SHOWDOWN_CAPTAIN_BUDGET - placed)), notes: { variant: "fit" },
    players: players.map((p) => { const v = fit.get(p.dkPlayerId) ?? { flex: 0, captain: 0 };
      return { dkPlayerId: p.dkPlayerId, ownPct: round(v.flex + v.captain), captainPct: round(v.captain), flexPct: round(v.flex) }; }) };
}

/**
 * The v5 algorithm (see the v5 note at the top). Mirrored by
 * model/nfl_showdown_ownership_eval.prior_showdown_v5 (Showdown) and
 * model/nfl_ownership_scorecard.prior_classic(..., "v5") (Classic); the shared
 * fixtures in tests/fixtures pin both languages to 0.02.
 */
export function projectOwnershipPriorV5(players: readonly OwnershipPriorPlayer[], format: "classic" | "showdown"): OwnershipPriorResult {
  const round = (v: number) => Math.round(v * 100) / 100;
  const pos = (p: OwnershipPriorPlayer) => (p.position ?? "").toUpperCase();
  if (format === "showdown") {
    const alpha = SHOWDOWN_FLATTEN_ALPHA_V5;
    // Captain: a fixed share of the 100% per position, split within the position.
    const captainWeight = new Map(players.map((p) => [p.dkPlayerId,
      p.captainSalary == null ? 0 : flatten(Math.pow(ownershipScore(p, p.captainSalary, SHOWDOWN_VALUE_EXPONENT), CAPTAIN_EXPONENT), alpha)]));
    const positions = Object.keys(CAPTAIN_POSITION_SHARES_V5);
    const present = positions.filter((position) => players.some((p) => pos(p) === position && (captainWeight.get(p.dkPlayerId) ?? 0) > 0));
    const shareTotal = present.reduce((t, position) => t + CAPTAIN_POSITION_SHARES_V5[position], 0);
    const captain = new Map<number, number>();
    let captainUnallocated = present.length ? 0 : SHOWDOWN_CAPTAIN_BUDGET;
    for (const position of present) {
      const group = players.filter((p) => pos(p) === position);
      const alloc = allocateBudgetDetailed(new Map(group.map((p) => [p.dkPlayerId, captainWeight.get(p.dkPlayerId) ?? 0])),
        SHOWDOWN_CAPTAIN_BUDGET * CAPTAIN_POSITION_SHARES_V5[position] / shareTotal, SHOWDOWN_CAPTAIN_MAX_PCT);
      for (const [id, pct] of alloc.shares) captain.set(id, pct);
      captainUnallocated += alloc.unallocated;
    }
    const flexCap = (id: number) => Math.min(SHOWDOWN_FLEX_MAX_PCT, SHOWDOWN_TOTAL_MAX_PCT - (captain.get(id) ?? 0));
    // Punt tier exactly as v4, without the kicker or DST terms.
    const isPunt = (p: OwnershipPriorPlayer) => p.salary > 0 && p.salary < SHOWDOWN_PUNT_SALARY_MAX && !p.isOut;
    const puntScores = new Map(players.filter(isPunt).map((p) => {
      const pts = fieldPoints(p);
      const status = STATUS_MULTIPLIER[(p.dkStatus ?? "").trim().toUpperCase()] ?? 1;
      return [p.dkPlayerId, pts == null || pts <= 0 ? 0 : Math.max(pts, 0.5) * status];
    }));
    const puntAlloc = allocateBudgetDetailed(puntScores, SHOWDOWN_PUNT_FLEX_BUDGET, (id) => Math.min(SHOWDOWN_PUNT_MAX_PCT, flexCap(id)));
    const puntTaken = SHOWDOWN_PUNT_FLEX_BUDGET - puntAlloc.unallocated;
    const mainScores = new Map(players.filter((p) => !isPunt(p)).map((p) => [p.dkPlayerId, flatten(ownershipScore(p, p.salary, SHOWDOWN_VALUE_EXPONENT), alpha)]));
    const mainAlloc = allocateBudgetDetailed(mainScores, SHOWDOWN_FLEX_BUDGET - puntTaken, flexCap);
    const flex = new Map<number, number>([...puntAlloc.shares, ...mainAlloc.shares]);
    return { version: NFL_OWNERSHIP_PRIOR_V5_VERSION, format, budgetPct: SHOWDOWN_FLEX_BUDGET + SHOWDOWN_CAPTAIN_BUDGET,
      unallocatedPct: round(mainAlloc.unallocated + captainUnallocated),
      notes: { variant: "v5", puntTierPlayers: puntScores.size, puntTierPct: round(puntTaken),
        captainPositionsMissing: positions.filter((position) => !present.includes(position)) },
      players: players.map((p) => { const f = flex.get(p.dkPlayerId) ?? 0, c = captain.get(p.dkPlayerId) ?? 0;
        return { dkPlayerId: p.dkPlayerId, ownPct: round(f + c), captainPct: round(c), flexPct: round(f) }; }) };
  }
  const own = new Map<number, number>();
  let unallocated = 0;
  for (const [position, budget] of Object.entries(CLASSIC_POSITION_BUDGETS_V4)) {
    const group = players.filter((p) => pos(p) === position);
    const alloc = allocateBudgetDetailed(new Map(group.map((p) => [p.dkPlayerId, flatten(ownershipScore(p), CLASSIC_FLATTEN_ALPHA_V5)])), budget, CLASSIC_MAX_PCT);
    for (const [id, pct] of alloc.shares) own.set(id, pct);
    unallocated += alloc.unallocated;
  }
  return { version: NFL_OWNERSHIP_PRIOR_V5_VERSION, format, budgetPct: Object.values(CLASSIC_POSITION_BUDGETS_V4).reduce((a, b) => a + b, 0), unallocatedPct: round(unallocated),
    notes: { variant: "v5" },
    players: players.map((p) => ({ dkPlayerId: p.dkPlayerId, ownPct: round(own.get(p.dkPlayerId) ?? 0), captainPct: null, flexPct: null })) };
}
