/**
 * Phase 2 (spec §9): ownership capability and missing-data safeguards.
 *
 * Core rule: missing ownership is `null`, never `0`. The optimizer must not
 * reward unknown ownership as if it were low ownership, and leverage / fade /
 * duplication features are only enabled when ownership is validated.
 *
 * Showdown carries Captain and Flex ownership SEPARATELY. They are never
 * combined into a single per-player percentage for scoring or reporting.
 */

export type OwnershipCapability = "validated" | "heuristic_uncalibrated" | "unavailable";

export interface NflOwnershipInput {
  playerId: number;
  /** Flex-slot ownership as a decimal 0..1, or null when unknown. */
  flexPct: number | null;
  /** Captain-slot ownership as a decimal 0..1, or null when unknown. */
  captainPct: number | null;
  source: string | null;
  asOf: string | null;
}

export interface OwnershipValidationThresholds {
  /** Fraction of eligible players that must carry validated ownership. */
  minCoverage: number;
  /** Fraction of median-projection mass that must be covered. */
  minMassCoverage: number;
  /** Captain totals should sum near 100% (1.0) within tolerance. */
  captainTotalTolerance: number;
  /** Flex totals should sum near 500% (5.0) within tolerance. */
  flexTotalTolerance: number;
}

export const DEFAULT_OWNERSHIP_THRESHOLDS: OwnershipValidationThresholds = {
  minCoverage: 0.95,
  minMassCoverage: 0.99,
  captainTotalTolerance: 0.15,   // Captain sum within 85%–115%.
  flexTotalTolerance: 0.75,      // Flex sum within 425%–575% (5.0 ± 0.75).
};

export interface OwnershipAssessment {
  capability: OwnershipCapability;
  source: string | null;
  asOf: string | null;
  /** Coverage of eligible players (0..1). */
  coverage: number;
  /** Coverage of median-projection mass (0..1). */
  massCoverage: number;
  captainTotal: number;
  flexTotal: number;
  errors: string[];
  warnings: string[];
  /** Which downstream features this capability permits. */
  features: { leverage: boolean; ownershipFade: boolean; duplicationModel: boolean };
}

export interface EligibleOwnershipPlayer {
  playerId: number;
  /** Median projection, used to weight mass coverage. */
  medianProjection: number | null;
}

/**
 * Assess ownership for a slate. `optIntoHeuristic` lets the caller display and
 * use uncalibrated estimates AFTER an explicit opt-in — every affected metric
 * must still be labeled "Uncalibrated estimate" in the UI.
 */
export function assessOwnership(
  eligible: EligibleOwnershipPlayer[],
  ownership: NflOwnershipInput[],
  options: { thresholds?: OwnershipValidationThresholds; optIntoHeuristic?: boolean; heuristic?: boolean; format?: "classic" | "showdown" } = {},
): OwnershipAssessment {
  const thresholds = options.thresholds ?? DEFAULT_OWNERSHIP_THRESHOLDS;
  const format = options.format ?? "showdown";
  const byId = new Map(ownership.map((o) => [o.playerId, o]));
  const source = ownership.find((o) => o.source)?.source ?? null;
  const asOf = ownership.find((o) => o.asOf)?.asOf ?? null;

  const errors: string[] = [];
  const warnings: string[] = [];

  if (!ownership.length) {
    return {
      capability: "unavailable", source, asOf, coverage: 0, massCoverage: 0, captainTotal: 0, flexTotal: 0,
      errors: ["No ownership supplied. Running projection-only: leverage, ownership fade and duplication estimates are disabled."],
      warnings, features: { leverage: false, ownershipFade: false, duplicationModel: false },
    };
  }

  // Value bounds and duplicate detection.
  const seen = new Set<number>();
  for (const row of ownership) {
    if (seen.has(row.playerId)) errors.push(`Duplicate ownership row for player ${row.playerId}.`);
    seen.add(row.playerId);
    for (const [label, value] of [["flex", row.flexPct], ["captain", row.captainPct]] as const) {
      if (value !== null && (!Number.isFinite(value) || value < 0 || value > 1)) {
        errors.push(`${label} ownership for player ${row.playerId} is out of the 0–100% range.`);
      }
    }
  }

  // Coverage: fraction of eligible players with any validated ownership value.
  const covered = eligible.filter((p) => {
    const row = byId.get(p.playerId);
    return row && (row.flexPct !== null || row.captainPct !== null);
  });
  const coverage = eligible.length ? covered.length / eligible.length : 0;

  // Mass coverage: fraction of median-projection mass that is covered.
  const totalMass = eligible.reduce((s, p) => s + Math.max(0, p.medianProjection ?? 0), 0);
  const coveredMass = covered.reduce((s, p) => s + Math.max(0, p.medianProjection ?? 0), 0);
  const massCoverage = totalMass > 0 ? coveredMass / totalMass : 0;

  // Slot totals — kept separate. Missing values contribute nothing (null≠0).
  const captainTotal = ownership.reduce((s, o) => s + (o.captainPct ?? 0), 0);
  const flexTotal = ownership.reduce((s, o) => s + (o.flexPct ?? 0), 0);

  if (coverage < thresholds.minCoverage) warnings.push(`Only ${(coverage * 100).toFixed(0)}% of eligible players have ownership (need ${(thresholds.minCoverage * 100).toFixed(0)}%).`);
  if (massCoverage < thresholds.minMassCoverage) warnings.push(`Only ${(massCoverage * 100).toFixed(0)}% of projection mass is covered (need ${(thresholds.minMassCoverage * 100).toFixed(0)}%).`);

  // If the caller declares the feed heuristic (e.g. a single combined
  // percentage that is not slot-level ownership at all), it can never be
  // validated — and slot-sum invariants do not apply to it, because they test
  // a structure the feed never claimed to have.
  if (options.heuristic) {
    const enabled = Boolean(options.optIntoHeuristic);
    return {
      capability: "heuristic_uncalibrated", source, asOf, coverage, massCoverage, captainTotal, flexTotal,
      errors, warnings: [...warnings, "Ownership is a heuristic estimate, not a validated feed. Every dependent metric is labeled 'Uncalibrated estimate'."],
      features: { leverage: enabled, ownershipFade: enabled, duplicationModel: false },
    };
  }

  // Slot-sum invariants for a feed claiming real slot-level ownership. Captain
  // invariants only exist in Showdown; the flex slot count depends on format.
  const hasCaptainData = ownership.some((o) => o.captainPct !== null);
  let captainOk = true;
  if (format === "showdown") {
    if (!hasCaptainData) { captainOk = false; errors.push("Feed supplies no Captain-slot ownership. A Showdown feed must carry Captain separately, or be declared a heuristic."); }
    else {
      captainOk = Math.abs(captainTotal - 1) <= thresholds.captainTotalTolerance;
      if (!captainOk) errors.push(`Captain ownership totals ${(captainTotal * 100).toFixed(0)}%, outside the expected ~100% (a valid Showdown captain field sums near one lineup's worth).`);
    }
  }
  const flexSlots = format === "classic" ? 9 : 5;
  // Tolerance was calibrated for 5 Showdown flex slots; scale it to the format.
  const flexOk = Math.abs(flexTotal - flexSlots) <= thresholds.flexTotalTolerance * (flexSlots / 5);
  if (!flexOk) errors.push(`Flex ownership totals ${(flexTotal * 100).toFixed(0)}%, outside the expected ~${flexSlots * 100}% (${flexSlots} roster slots).`);

  const validated = errors.length === 0
    && coverage >= thresholds.minCoverage
    && massCoverage >= thresholds.minMassCoverage
    && captainOk && flexOk;

  if (validated) {
    return {
      capability: "validated", source, asOf, coverage, massCoverage, captainTotal, flexTotal,
      errors, warnings, features: { leverage: true, ownershipFade: true, duplicationModel: true },
    };
  }

  // Ownership exists but does not clear the bar: projection-only, no leverage.
  return {
    capability: "unavailable", source, asOf, coverage, massCoverage, captainTotal, flexTotal,
    errors, warnings: [...warnings, "Ownership did not meet validation thresholds; running projection-only. Leverage, ownership fade and duplication estimates are disabled."],
    features: { leverage: false, ownershipFade: false, duplicationModel: false },
  };
}

/** The objective label to show given capability — never claims leverage it cannot support. */
export function objectiveLabel(capability: OwnershipCapability): string {
  switch (capability) {
    case "validated": return "GPP leverage";
    case "heuristic_uncalibrated": return "GPP (uncalibrated ownership estimate)";
    case "unavailable": return "Projection-only GPP";
  }
}
