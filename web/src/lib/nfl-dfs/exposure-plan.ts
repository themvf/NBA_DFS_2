/**
 * Phase 3 (spec §10): role-specific exposure ranges.
 *
 * Replaces the single max-exposure model with independent Overall / Captain /
 * Flex min+max ranges. Counts derive from percentages with ceil(n·minPct) for
 * minimums and floor(n·maxPct) for maximums (spec §7). Captain and Flex are
 * counted independently; Overall is their union. Exact-target mode is off by
 * default and can only be enabled explicitly (never via a single target field).
 *
 * Pure and deterministic. The optimizer uses derived counts; this module also
 * detects impossible plans BEFORE a solve so failures are explained, not silent.
 */

export interface ExposureRange {
  /** Decimal 0..1, or null for "no minimum". */
  minPct: number | null;
  /** Decimal 0..1, or null for "no maximum". */
  maxPct: number | null;
}

export interface PlayerExposurePolicy {
  playerId: number;
  overall: ExposureRange;
  captain: ExposureRange;
  flex: ExposureRange;
  /** When true, min and max counts are forced equal (an exact target). Off by default. */
  exactTargetMode: boolean;
}

export interface ExposureCounts {
  overallMin: number; overallMax: number;
  captainMin: number; captainMax: number;
  flexMin: number; flexMax: number;
}

export function ceilMin(n: number, pct: number | null): number {
  return pct === null ? 0 : Math.ceil(pct * n - 1e-9);
}
export function floorMax(n: number, pct: number | null): number {
  return pct === null ? n : Math.floor(pct * n + 1e-9);
}

/** Validate a single policy's ranges (spec §10.2). */
export function validateExposurePolicy(policy: PlayerExposurePolicy): void {
  for (const [label, range] of [["overall", policy.overall], ["captain", policy.captain], ["flex", policy.flex]] as const) {
    for (const [bound, value] of [["min", range.minPct], ["max", range.maxPct]] as const) {
      if (value !== null && (!Number.isFinite(value) || value < 0 || value > 1)) {
        throw new Error(`Player ${policy.playerId} ${label} ${bound} exposure must be between 0% and 100%.`);
      }
    }
    if (range.minPct !== null && range.maxPct !== null && range.minPct > range.maxPct) {
      throw new Error(`Player ${policy.playerId} ${label} minimum exposure exceeds its maximum.`);
    }
  }
}

/** Derive integer counts for a portfolio of n lineups. */
export function deriveExposureCounts(policy: PlayerExposurePolicy, n: number): ExposureCounts {
  const counts: ExposureCounts = {
    overallMin: ceilMin(n, policy.overall.minPct), overallMax: floorMax(n, policy.overall.maxPct),
    captainMin: ceilMin(n, policy.captain.minPct), captainMax: floorMax(n, policy.captain.maxPct),
    flexMin: ceilMin(n, policy.flex.minPct), flexMax: floorMax(n, policy.flex.maxPct),
  };
  if (policy.exactTargetMode) {
    // Exact target: pin max down to min for each slot that has a minimum.
    if (policy.overall.minPct !== null) counts.overallMax = counts.overallMin;
    if (policy.captain.minPct !== null) counts.captainMax = counts.captainMin;
    if (policy.flex.minPct !== null) counts.flexMax = counts.flexMin;
  }
  return counts;
}

export interface ExposureInfeasibility {
  reason: string;
  detail: string;
}

/**
 * Detect impossible plans before the solve (spec §10.2, P3-AC4):
 * - a player whose captain+flex maxima cannot reach his overall minimum;
 * - aggregate captain minimums that exceed the number of lineups (only one
 *   captain per lineup, so total captain appearances ≤ n);
 * - insufficient captain maximum capacity to fill n captain slots.
 */
export function detectExposureInfeasibility(
  policies: PlayerExposurePolicy[],
  n: number,
  captainEligibleIds: Set<number>,
): ExposureInfeasibility[] {
  const problems: ExposureInfeasibility[] = [];
  let captainMinTotal = 0;
  let captainMaxTotal = 0;
  let anyCaptainCapPolicy = false;

  for (const policy of policies) {
    const c = deriveExposureCounts(policy, n);
    // Per-player: overall min cannot exceed captain-max + flex-max capacity.
    if (c.overallMin > c.captainMax + c.flexMax) {
      problems.push({ reason: "PLAYER_OVERALL_UNREACHABLE", detail: `Player ${policy.playerId}: overall minimum ${c.overallMin} exceeds captain(${c.captainMax}) + flex(${c.flexMax}) capacity.` });
    }
    if (c.captainMin > 0 && !captainEligibleIds.has(policy.playerId)) {
      problems.push({ reason: "CAPTAIN_MIN_ON_INELIGIBLE", detail: `Player ${policy.playerId} has a captain minimum but is not captain-eligible.` });
    }
    captainMinTotal += c.captainMin;
    if (policy.captain.maxPct !== null || policy.exactTargetMode) { anyCaptainCapPolicy = true; captainMaxTotal += c.captainMax; }
  }

  // Only one captain per lineup.
  if (captainMinTotal > n) {
    problems.push({ reason: "CAPTAIN_MIN_AGGREGATE", detail: `Captain minimums sum to ${captainMinTotal} across players, but only ${n} captain slots exist (one per lineup).` });
  }
  // If every captain-eligible player has a max, they must jointly cover n captains.
  if (anyCaptainCapPolicy && captainMaxTotal < n && policies.length && captainMaxTotal > 0) {
    // Only a problem when the capped players are the ONLY captain-eligible pool.
    const cappedCaptainEligible = policies.filter((p) => (p.captain.maxPct !== null || p.exactTargetMode) && captainEligibleIds.has(p.playerId));
    if (cappedCaptainEligible.length === captainEligibleIds.size && captainEligibleIds.size > 0) {
      problems.push({ reason: "CAPTAIN_MAX_CAPACITY", detail: `Captain maximums cap total captain appearances at ${captainMaxTotal}, below the ${n} captain slots to fill.` });
    }
  }
  return problems;
}

/** A default, unconstrained policy for a player (used when no per-player rule is set). */
export function defaultExposurePolicy(playerId: number, overallMaxPct: number | null): PlayerExposurePolicy {
  return {
    playerId,
    overall: { minPct: null, maxPct: overallMaxPct },
    captain: { minPct: null, maxPct: null },
    flex: { minPct: null, maxPct: null },
    exactTargetMode: false,
  };
}
