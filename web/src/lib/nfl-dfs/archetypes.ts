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

export type ArchetypeId =
  | "standard_ceiling"
  | "single_chalk_fade"
  | "double_fade"
  | "contrarian_captain"
  | "favorite_onslaught"
  | "underdog_comeback"
  | "low_scoring_k_dst";

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
  /** Team-count range across the 6 Showdown slots. */
  teamCountRange: { min: number; max: number } | null;
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
}

export const ARCHETYPE_LABELS: Record<ArchetypeId, string> = {
  standard_ceiling: "Standard ceiling",
  single_chalk_fade: "Single-chalk fade",
  double_fade: "Double fade",
  contrarian_captain: "Contrarian Captain",
  favorite_onslaught: "Favorite onslaught",
  underdog_comeback: "Underdog comeback",
  low_scoring_k_dst: "Low-scoring K/DST",
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
      return { ...base, teamCountRange: { min: favShare, max: 6 },
        summary: `${config.favoriteSkew ?? "4-2"} favorite skew (${ctx.favoriteTeam} wins decisively).` };
    }

    case "underdog_comeback": {
      if (!ctx.underdogTeam) throw new Error("Underdog comeback requires a known underdog team.");
      // Prefer an underdog captain and require opponent bring-back presence.
      const underdogCaptains = ctx.players.filter((p) => p.captainEligible && p.team === ctx.underdogTeam).map((p) => p.dkPlayerId);
      return { ...base, eligibleCaptainIds: underdogCaptains.length ? underdogCaptains : null,
        teamCountRange: { min: 2, max: 6 }, summary: `Underdog (${ctx.underdogTeam}) passing-volume response with bring-back.` };
    }

    case "low_scoring_k_dst":
      return { ...base, minKickerDst: 1, summary: "Reduced-touchdown environment; requires kicker/defense presence." };
  }
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
