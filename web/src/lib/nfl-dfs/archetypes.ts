/**
 * Phase 4 (spec §11): fade and game-script archetypes.
 *
 * An archetype is a NAMED set of lineup-level constraints expressing a game
 * script or leverage thesis. Names describe strategy, not expected profit.
 *
 * A fade is never just "exclude player X": it MUST specify at least one
 * alternate scoring path (a beneficiary group). Each generated lineup carries
 * exactly one primary archetype label, plus its faded players and the
 * beneficiary rules it satisfied.
 *
 * This module is pure: it defines archetypes and compiles them to a constraint
 * spec the optimizer consumes. It does not itself run the solver.
 */

import type { PlayerExposurePolicy } from "./exposure-plan";

export type ArchetypeId =
  | "standard_ceiling"
  | "single_chalk_fade"
  | "double_fade"
  | "contrarian_captain"
  | "favorite_onslaught"
  | "underdog_comeback"
  | "low_scoring_k_dst"
  | "chalk_captain_leverage";

export interface ArchetypeQuota {
  archetypeId: ArchetypeId;
  minLineups: number;
  maxLineups: number;
  enabled: boolean;
}

/** A beneficiary group: an alternate scoring path a fade must provide. */
export interface BeneficiaryGroup {
  /** Human description of the alternate path, e.g. "replacement WR gains targets". */
  label: string;
  /** At least this many players from `playerIds` must appear in the lineup. */
  minFromGroup: number;
  playerIds: number[];
}

/** The compiled, solver-ready constraint spec for one archetype instance. */
export interface CompiledArchetype {
  archetypeId: ArchetypeId;
  /** Players that must NOT appear (the faded players). */
  fadePlayerIds: number[];
  /** Captain must be drawn from this set when non-empty. */
  eligibleCaptainIds: number[] | null;
  /** Captain must NOT be one of these (e.g. contrarian: not the chalk captain). */
  forbiddenCaptainIds: number[];
  /** Team-count range across the 6 Showdown slots, bound to the team it constrains. */
  teamCountRange: { team: string; min: number; max: number } | null;
  /** Alternate scoring paths a fade provides; at least one must be satisfiable. */
  beneficiaries: BeneficiaryGroup[];
  /** Optional required minimum K/DST count (low-scoring script). */
  minKickerDst: number | null;
  /** Human summary for the UI and audit. */
  summary: string;
}

/** Inputs the compiler needs about the slate to resolve a preset to concrete ids. */
export interface ArchetypeSlateContext {
  players: Array<{
    dkPlayerId: number;
    position: "QB" | "RB" | "WR" | "TE" | "K" | "DST";
    team: string;
    opponent: string | null;
    /** Flex ownership decimal when validated; null otherwise. */
    ownership: number | null;
    /** Resolved projection, used as the chalk PROXY when ownership is absent. */
    projection?: number | null;
    captainEligible: boolean;
  }>;
  /** Team abbreviations, favorite first when known. */
  favoriteTeam: string | null;
  underdogTeam: string | null;
  /** Ownership capability gates ownership-based archetype logic. */
  ownershipValidated: boolean;
}

export interface ArchetypeConfig {
  /** For single/double fade: which high-owned players to fade. */
  fadePlayerIds?: number[];
  /** For contrarian captain: ownership ceiling (decimal) for an eligible captain. */
  contrarianCaptainCeiling?: number;
  /** For favorite onslaught: 4-2 or 5-1 favorite skew. */
  favoriteSkew?: "4-2" | "5-1";
  /** Explicit beneficiary groups supplied by the user for a fade. */
  beneficiaries?: BeneficiaryGroup[];
  /** Chalk-captain model: the captains this lineup may use. */
  chalkCaptainIds?: number[];
  /** Chalk-captain model: this lineup's leverage group and its label. */
  leveragePlayerIds?: number[];
  leveragePosition?: string;
}

export const ARCHETYPE_LABELS: Record<ArchetypeId, string> = {
  standard_ceiling: "Standard ceiling",
  single_chalk_fade: "Single-chalk fade",
  double_fade: "Double fade",
  contrarian_captain: "Contrarian Captain",
  favorite_onslaught: "Favorite onslaught",
  underdog_comeback: "Underdog comeback",
  low_scoring_k_dst: "Low-scoring K/DST",
  chalk_captain_leverage: "Chalk captain, rotating leverage",
};

/** Whether an archetype is a fade (and therefore requires a beneficiary path). */
export function isFadeArchetype(id: ArchetypeId): boolean {
  return id === "single_chalk_fade" || id === "double_fade";
}

function teammatesOf(ctx: ArchetypeSlateContext, playerId: number): number[] {
  const target = ctx.players.find((p) => p.dkPlayerId === playerId);
  if (!target) return [];
  return ctx.players.filter((p) => p.team === target.team && p.dkPlayerId !== playerId).map((p) => p.dkPlayerId);
}

/**
 * Compile an archetype preset + config into a solver-ready constraint spec.
 * Throws when a fade is requested without any alternate scoring path — a fade
 * MUST NOT be represented only as "exclude player X" (spec §11.2).
 */
export function compileArchetype(id: ArchetypeId, ctx: ArchetypeSlateContext, config: ArchetypeConfig = {}): CompiledArchetype {
  const base: CompiledArchetype = {
    archetypeId: id, fadePlayerIds: [], eligibleCaptainIds: null, forbiddenCaptainIds: [],
    teamCountRange: null, beneficiaries: [], minKickerDst: null, summary: ARCHETYPE_LABELS[id],
  };

  const requireBeneficiaries = (fades: number[]): BeneficiaryGroup[] => {
    const supplied = config.beneficiaries ?? [];
    if (supplied.length && supplied.every((g) => g.minFromGroup > 0 && g.playerIds.length >= g.minFromGroup)) return supplied;
    // Derive a default beneficiary path: teammates of the faded players plus the
    // opposing offense, so the fade always carries an alternate scoring route.
    const derived: BeneficiaryGroup[] = [];
    for (const fadeId of fades) {
      const mates = teammatesOf(ctx, fadeId);
      if (mates.length) derived.push({ label: `Teammates absorb ${fadeId}'s share`, minFromGroup: 1, playerIds: mates });
    }
    if (!derived.length) {
      throw new Error(`A ${ARCHETYPE_LABELS[id]} lineup must declare at least one beneficiary (alternate scoring path). Faded players have no resolvable teammates; supply an explicit beneficiary group.`);
    }
    return derived;
  };

  switch (id) {
    case "standard_ceiling":
      return { ...base, summary: "Strongest evaluated lineups; no forced chalk fade." };

    case "single_chalk_fade": {
      const fades = (config.fadePlayerIds ?? []).slice(0, 1);
      if (!fades.length) throw new Error("Single-chalk fade requires one player to fade.");
      return { ...base, fadePlayerIds: fades, beneficiaries: requireBeneficiaries(fades),
        summary: `Fades 1 chalk player; requires a declared beneficiary path.` };
    }

    case "double_fade": {
      const fades = (config.fadePlayerIds ?? []).slice(0, 2);
      if (fades.length < 2) throw new Error("Double fade requires two players to fade.");
      return { ...base, fadePlayerIds: fades, beneficiaries: requireBeneficiaries(fades),
        summary: `Fades 2 chalk players; requires a coherent alternate allocation.` };
    }

    case "contrarian_captain": {
      const ceiling = config.contrarianCaptainCeiling ?? 0.1;
      if (!ctx.ownershipValidated) {
        // Without validated ownership we cannot honestly restrict by ownership;
        // fall back to a captain set excluding the highest-projected chalk is not
        // possible either, so leave captain open but label the limitation.
        return { ...base, summary: "Contrarian Captain requires validated ownership; ran without an ownership ceiling." };
      }
      const eligible = ctx.players.filter((p) => p.captainEligible && (p.ownership ?? 1) <= ceiling).map((p) => p.dkPlayerId);
      if (!eligible.length) throw new Error(`No captain-eligible player is under the ${(ceiling * 100).toFixed(0)}% ownership ceiling.`);
      return { ...base, eligibleCaptainIds: eligible, summary: `Captain owned ≤ ${(ceiling * 100).toFixed(0)}% with correlated teammates.` };
    }

    case "favorite_onslaught": {
      if (!ctx.favoriteTeam) throw new Error("Favorite onslaught requires a known favorite team.");
      const favShare = config.favoriteSkew === "5-1" ? 5 : 4;
      return { ...base, teamCountRange: { team: ctx.favoriteTeam, min: favShare, max: 6 },
        summary: `${config.favoriteSkew ?? "4-2"} favorite skew (${ctx.favoriteTeam} wins decisively).` };
    }

    case "underdog_comeback": {
      if (!ctx.underdogTeam) throw new Error("Underdog comeback requires a known underdog team.");
      // Prefer an underdog captain and require opponent bring-back presence.
      const underdogCaptains = ctx.players.filter((p) => p.captainEligible && p.team === ctx.underdogTeam).map((p) => p.dkPlayerId);
      // The count range is bound to the UNDERDOG team: at least two underdog
      // players regardless of whether an underdog captain is available.
      return { ...base, eligibleCaptainIds: underdogCaptains.length ? underdogCaptains : null,
        teamCountRange: { team: ctx.underdogTeam, min: 2, max: 6 }, summary: `Underdog (${ctx.underdogTeam}) passing-volume response with bring-back.` };
    }

    case "low_scoring_k_dst":
      return { ...base, minKickerDst: 1, summary: "Reduced-touchdown environment; requires kicker/defense presence." };

    case "chalk_captain_leverage": {
      const captains = config.chalkCaptainIds ?? [];
      const leverage = config.leveragePlayerIds ?? [];
      if (!captains.length) throw new Error("Chalk captain model requires at least one chalk captain.");
      if (!leverage.length) throw new Error("Chalk captain model requires a leverage group for this lineup.");
      // The leverage requirement reuses the beneficiary mechanism: at least one
      // player from the group, in any slot. It is NOT a fade, so it carries no
      // faded players.
      return { ...base, eligibleCaptainIds: captains,
        beneficiaries: [{ label: `Leverage at ${config.leveragePosition ?? "flex"}`, minFromGroup: 1, playerIds: leverage }],
        summary: `Chalk captain; leverage taken at ${config.leveragePosition ?? "flex"}.` };
    }
  }
}

/**
 * The chalk-captain, rotating-leverage lineup model.
 *
 * WHY: the week-2 field audit found the winning entries were CHALKIER than
 * ours -- 106% cumulative ownership for the winner and 119% across the top
 * 1,000, against our 96%. Contrarian captains were not where the edge came
 * from. This model takes the captain from the obvious plays, where the field
 * is, and takes its differentiation in the FLEX slots instead, rotating WHICH
 * position supplies that differentiation from lineup to lineup so the
 * portfolio is not one leverage bet repeated.
 *
 * Definitions, all deterministic and disclosed in the run notes:
 *   chalk rank   skill players (not K/DST) by projected ownership when any is
 *                supplied, else by projection -- the field gravitates to the
 *                obvious studs. Same proxy the fade archetypes already use.
 *   captains     the top `captainCount` captain-eligible names in that rank,
 *                plus anyone the user gave a captain MINIMUM (the user's
 *                explicit captain intent always stays reachable).
 *   core         the top `coreCount` names in that rank -- the plays everyone
 *                has. Leverage is anything outside it with a real projection.
 *   rotation     lineup i takes its leverage at position rotation[i % k],
 *                cycling through the positions that have a leverage option.
 *
 * Names describe strategy, not expected profit, and none of it is validated:
 * it is a portfolio shape, graded afterward by the field audit.
 */
export const CHALK_CAPTAIN_COUNT = 3;
export const CHALK_CORE_COUNT = 6;
export const LEVERAGE_ROTATION = ["WR", "TE", "RB", "K/DST", "QB"] as const;

/**
 * How a chalk captain's exposure is read in the chalk-captain model.
 *
 * The flat max exposure (60% by default) counts FLEX appearances too, and the
 * optimizer likes all three chalk captains in most lineups as filler. Measured
 * on the Thursday ATL@GB slate: they spent their 60% as FLEX, and at 50
 * lineups the run stopped at 49 because no chalk captain had capacity left.
 * So in this model the cap limits how often a chalk captain is FLEX, and his
 * CAPTAINCY is set by the captain plan (the CPT control, or the objective).
 *
 * An overall TARGET the user set explicitly (min == max) is left untouched --
 * that is their instruction, not a default.
 */
export function chalkCaptainPolicy(policy: PlayerExposurePolicy): PlayerExposurePolicy {
  if (policy.overall.minPct != null) return policy;
  return {
    ...policy,
    overall: { minPct: null, maxPct: 1 },
    flex: { minPct: policy.flex.minPct, maxPct: policy.flex.maxPct ?? policy.overall.maxPct },
  };
}

export interface ChalkLeveragePlan {
  lineups: Array<{ compiled: CompiledArchetype; leveragePosition: string }>;
  chalkCaptainIds: number[];
  coreIds: number[];
  rotation: string[];
  basis: "projected ownership" | "projection";
}

export function chalkLeveragePlan(
  ctx: ArchetypeSlateContext,
  n: number,
  opts: { captainCount?: number; coreCount?: number; extraCaptainIds?: number[] } = {},
): ChalkLeveragePlan {
  const anyOwnership = ctx.players.some((p) => p.ownership != null);
  const rank = ctx.players
    .filter((p) => p.position !== "K" && p.position !== "DST" && (p.projection ?? 0) > 0)
    .sort((a, b) =>
      (anyOwnership ? (b.ownership ?? -1) - (a.ownership ?? -1) : 0)
      || (b.projection ?? 0) - (a.projection ?? 0)
      || a.dkPlayerId - b.dkPlayerId);
  const captainCount = opts.captainCount ?? CHALK_CAPTAIN_COUNT;
  const coreCount = opts.coreCount ?? CHALK_CORE_COUNT;
  const chalkCaptainIds = [...new Set([
    ...rank.filter((p) => p.captainEligible).slice(0, captainCount).map((p) => p.dkPlayerId),
    ...(opts.extraCaptainIds ?? []).filter((id) => ctx.players.some((p) => p.dkPlayerId === id && p.captainEligible)),
  ])];
  const coreIds = rank.slice(0, coreCount).map((p) => p.dkPlayerId);
  const core = new Set([...coreIds, ...chalkCaptainIds]);

  const groupOf = (position: string) => (position === "K" || position === "DST" ? "K/DST" : position);
  const leverageByGroup = new Map<string, number[]>();
  for (const p of ctx.players) {
    if (core.has(p.dkPlayerId) || !((p.projection ?? 0) > 0)) continue;
    const group = groupOf(p.position);
    leverageByGroup.set(group, [...(leverageByGroup.get(group) ?? []), p.dkPlayerId]);
  }
  const rotation = LEVERAGE_ROTATION.filter((g) => (leverageByGroup.get(g)?.length ?? 0) > 0);
  if (!chalkCaptainIds.length) throw new Error("Chalk captain model: no captain-eligible player has a projection.");
  if (!rotation.length) throw new Error("Chalk captain model: no leverage option exists outside the chalk core.");

  const lineups = Array.from({ length: n }, (_, i) => {
    const leveragePosition = rotation[i % rotation.length];
    return {
      leveragePosition,
      compiled: compileArchetype("chalk_captain_leverage", ctx, {
        chalkCaptainIds, leveragePlayerIds: leverageByGroup.get(leveragePosition)!, leveragePosition,
      }),
    };
  });
  return { lineups, chalkCaptainIds, coreIds, rotation: [...rotation], basis: anyOwnership ? "projected ownership" : "projection" };
}

/**
 * Auto-select fade targets: the most CHALK-like skill players. Ranked by
 * ownership when any is supplied (validated or heuristic — this is a
 * selection heuristic, not an ownership claim), else by projection as the
 * chalk proxy (the field gravitates to the obvious studs). K/DST are never
 * fade targets — nobody chalk-fades a kicker — and ties break by player id
 * so the pick is deterministic.
 */
export function autoFadeCandidates(ctx: ArchetypeSlateContext, count: number): number[] {
  const anyOwnership = ctx.players.some((p) => p.ownership != null);
  return ctx.players
    .filter((p) => p.position !== "K" && p.position !== "DST")
    .sort((a, b) =>
      (anyOwnership ? (b.ownership ?? -1) - (a.ownership ?? -1) : 0)
      || (b.projection ?? 0) - (a.projection ?? 0)
      || a.dkPlayerId - b.dkPlayerId)
    .slice(0, count)
    .map((p) => p.dkPlayerId);
}

/** Balanced-mix weights, in deterministic priority order. */
const BALANCED_WEIGHTS: Array<{ id: ArchetypeId; weight: number }> = [
  { id: "standard_ceiling", weight: 0.40 },
  { id: "single_chalk_fade", weight: 0.15 },
  { id: "favorite_onslaught", weight: 0.15 },
  { id: "double_fade", weight: 0.10 },
  { id: "underdog_comeback", weight: 0.10 },
  { id: "low_scoring_k_dst", weight: 0.10 },
];

export interface BalancedArchetypePlan {
  quotas: ArchetypeQuota[];
  configs: Partial<Record<ArchetypeId, ArchetypeConfig>>;
  /** Plain-language disclosures about what was auto-chosen and what was skipped. */
  notes: string[];
}

/**
 * Build the default "balanced mix" plan: allocate n lineups across the
 * archetypes whose prerequisites the slate actually satisfies, auto-selecting
 * fade targets. An archetype whose prerequisite is missing is SKIPPED with a
 * note and its share folds into Standard ceiling — the plan never throws for
 * a missing input the user was not asked for.
 *
 * Deliberately excluded: contrarian_captain (without validated ownership it
 * compiles to a no-op, and mixing a no-op into a default plan would label
 * ordinary lineups with a strategy they do not express).
 */
export function balancedArchetypePlan(ctx: ArchetypeSlateContext, n: number): BalancedArchetypePlan {
  const notes: string[] = [];
  const fades = autoFadeCandidates(ctx, 2);
  const hasGameScript = Boolean(ctx.favoriteTeam && ctx.underdogTeam);
  const hasKdst = ctx.players.some((p) => p.position === "K" || p.position === "DST");
  if (!hasGameScript) notes.push("Vegas favorite unknown — favorite-onslaught and underdog-comeback lineups were folded into Standard ceiling.");
  if (!hasKdst) notes.push("No kicker or defense in the pool — low-scoring K/DST lineups were folded into Standard ceiling.");
  if (fades.length) {
    const basis = ctx.players.some((p) => p.ownership != null) ? "projected ownership" : "projection (chalk proxy — no ownership feed)";
    notes.push(`Fade targets auto-selected by ${basis}.`);
  }

  const eligible = BALANCED_WEIGHTS.filter(({ id }) => {
    if (id === "single_chalk_fade") return fades.length >= 1;
    if (id === "double_fade") return fades.length >= 2;
    if (id === "favorite_onslaught" || id === "underdog_comeback") return hasGameScript;
    if (id === "low_scoring_k_dst") return hasKdst;
    return true;
  });
  // Largest-remainder allocation over the eligible weights so counts sum to n
  // exactly and small requests degrade gracefully toward Standard ceiling.
  const totalWeight = eligible.reduce((s, e) => s + e.weight, 0);
  const raw = eligible.map((e) => ({ id: e.id, exact: (e.weight / totalWeight) * n }));
  const counts = raw.map((r) => ({ id: r.id, count: Math.floor(r.exact), frac: r.exact - Math.floor(r.exact) }));
  let remainder = n - counts.reduce((s, c) => s + c.count, 0);
  for (const c of [...counts].sort((a, b) => b.frac - a.frac || (a.id === "standard_ceiling" ? -1 : b.id === "standard_ceiling" ? 1 : 0))) {
    if (remainder <= 0) break;
    c.count += 1; remainder -= 1;
  }
  const quotas: ArchetypeQuota[] = counts.filter((c) => c.count > 0)
    .map((c) => ({ archetypeId: c.id, minLineups: c.count, maxLineups: c.count, enabled: true }));
  const configs: Partial<Record<ArchetypeId, ArchetypeConfig>> = {};
  if (fades.length >= 1) configs.single_chalk_fade = { fadePlayerIds: fades.slice(0, 1) };
  if (fades.length >= 2) configs.double_fade = { fadePlayerIds: fades.slice(0, 2) };
  return { quotas, configs, notes };
}

/**
 * Allocate a total lineup count across enabled archetype quotas. Returns the
 * per-archetype target counts, or an infeasibility when minimums exceed total
 * or an archetype's min>max (spec P4-AC1).
 */
export function allocateArchetypeQuotas(quotas: ArchetypeQuota[], total: number):
  { ok: true; allocation: Array<{ archetypeId: ArchetypeId; count: number }> }
  | { ok: false; reason: string } {
  const enabled = quotas.filter((q) => q.enabled);
  if (!enabled.length) return { ok: true, allocation: [{ archetypeId: "standard_ceiling", count: total }] };
  for (const q of enabled) {
    if (q.minLineups > q.maxLineups) return { ok: false, reason: `${ARCHETYPE_LABELS[q.archetypeId]}: min ${q.minLineups} exceeds max ${q.maxLineups}.` };
    if (q.minLineups < 0 || q.maxLineups < 0) return { ok: false, reason: `${ARCHETYPE_LABELS[q.archetypeId]}: negative quota.` };
  }
  const minTotal = enabled.reduce((s, q) => s + q.minLineups, 0);
  const maxTotal = enabled.reduce((s, q) => s + q.maxLineups, 0);
  if (minTotal > total) return { ok: false, reason: `Archetype minimums sum to ${minTotal}, exceeding the ${total} requested lineups.` };
  if (maxTotal < total) return { ok: false, reason: `Archetype maximums sum to ${maxTotal}, below the ${total} requested lineups. Raise a maximum or reduce the lineup count.` };

  // Start each at its minimum, then distribute the remainder up to each maximum
  // in declared order (deterministic).
  const allocation = enabled.map((q) => ({ archetypeId: q.archetypeId, count: q.minLineups, max: q.maxLineups }));
  let remaining = total - minTotal;
  for (const a of allocation) {
    if (remaining <= 0) break;
    const room = a.max - a.count;
    const add = Math.min(room, remaining);
    a.count += add; remaining -= add;
  }
  return { ok: true, allocation: allocation.map(({ archetypeId, count }) => ({ archetypeId, count })) };
}
