/**
 * Phase 5 (spec §12): salary-left distribution and duplication controls.
 *
 * Replaces reliance on a single high minimum salary with explicit construction
 * controls: min/max salary used, min/max salary left, and portfolio quotas by
 * salary-left band. Adds duplication estimation that is HONEST about its basis:
 *  - model-based duplication requires validated slot ownership + a field model;
 *  - heuristic duplication is labeled uncalibrated and is NEVER presented as an
 *    expected duplicate COUNT.
 *
 * It is explicitly forbidden to estimate full-lineup probability by naively
 * multiplying marginal player ownership. Heuristic scoring here is an ordinal
 * concentration signal, not a probability.
 */

export const NFL_SALARY_CAP = 50000;

export interface SalaryLeftBand {
  /** Inclusive salary-left lower bound (dollars). */
  min: number;
  /** Inclusive salary-left upper bound (dollars). */
  max: number;
  /** Portfolio share bounds as decimals 0..1. */
  minLineups: number;
  maxLineups: number;
}

export interface SalaryConstructionPolicy {
  minSalaryUsed: number;
  maxSalaryUsed: number;
  minSalaryLeft: number;
  maxSalaryLeft: number;
  salaryLeftBands: SalaryLeftBand[];
}

/** Recommended EXPERIMENTAL Showdown default (spec §12.1, §22 — pending backtest). */
export const DEFAULT_SALARY_POLICY: SalaryConstructionPolicy = {
  minSalaryUsed: 45000,
  maxSalaryUsed: NFL_SALARY_CAP,
  minSalaryLeft: 0,
  maxSalaryLeft: NFL_SALARY_CAP - 45000,
  salaryLeftBands: [
    { min: 0, max: 400, minLineups: 0, maxLineups: 0.35 },
    { min: 500, max: 1400, minLineups: 0.25, maxLineups: 0.70 },
    { min: 1500, max: 3000, minLineups: 0.10, maxLineups: 0.50 },
    { min: 3001, max: NFL_SALARY_CAP, minLineups: 0, maxLineups: 0.20 },
  ],
};

export function validateSalaryPolicy(policy: SalaryConstructionPolicy): void {
  if (policy.minSalaryUsed > policy.maxSalaryUsed) throw new Error("Minimum salary used exceeds maximum salary used.");
  if (policy.minSalaryLeft > policy.maxSalaryLeft) throw new Error("Minimum salary left exceeds maximum salary left.");
  for (const band of policy.salaryLeftBands) {
    if (band.min > band.max) throw new Error("Salary-left band lower bound exceeds its upper bound.");
    if (band.minLineups > band.maxLineups) throw new Error("Salary-left band minimum share exceeds its maximum share.");
    for (const share of [band.minLineups, band.maxLineups]) {
      if (share < 0 || share > 1) throw new Error("Salary-left band shares must be decimals between 0 and 1.");
    }
  }
}

/** Which band a given salary-left value falls into (first match), or -1. */
export function bandIndexForSalaryLeft(policy: SalaryConstructionPolicy, salaryLeft: number): number {
  return policy.salaryLeftBands.findIndex((b) => salaryLeft >= b.min && salaryLeft <= b.max);
}

export interface SalaryBandReport {
  band: SalaryLeftBand;
  count: number;
  share: number;
  minCount: number;
  maxCount: number;
  withinPlan: boolean;
}

/** Tally realized salary-left bands across a portfolio against the plan (P5-AC2/AC5). */
export function reportSalaryBands(policy: SalaryConstructionPolicy, salaryLefts: number[]): SalaryBandReport[] {
  const total = salaryLefts.length || 1;
  return policy.salaryLeftBands.map((band) => {
    const count = salaryLefts.filter((s) => s >= band.min && s <= band.max).length;
    const minCount = Math.ceil(band.minLineups * salaryLefts.length - 1e-9);
    const maxCount = Math.floor(band.maxLineups * salaryLefts.length + 1e-9);
    return { band, count, share: count / total, minCount, maxCount, withinPlan: count >= minCount && count <= maxCount };
  });
}

// --- Duplication ---------------------------------------------------------

export type DuplicationBasis = "model" | "heuristic" | "unavailable";

export interface LineupDuplication {
  lineupNumber: number;
  basis: DuplicationBasis;
  /** Model-based EXPECTED duplicate count. Present ONLY when basis === "model". */
  expectedDuplicates: number | null;
  /** Heuristic ordinal concentration score 0..1 (higher = more chalk-concentrated). Never a count. */
  concentrationScore: number | null;
  /** Human label that never claims "expected duplicates" for a heuristic. */
  label: string;
}

/** Exact-duplicate detection across a portfolio (P5-AC4). */
export function findExactDuplicates(lineups: Array<{ lineupNumber: number; playerIds: number[] }>): Array<[number, number]> {
  const seen = new Map<string, number>();
  const dups: Array<[number, number]> = [];
  for (const l of lineups) {
    const key = [...l.playerIds].sort((a, b) => a - b).join(",");
    if (seen.has(key)) dups.push([seen.get(key)!, l.lineupNumber]);
    else seen.set(key, l.lineupNumber);
  }
  return dups;
}

/** Maximum pairwise overlap (shared players) across a portfolio. */
export function maxPairwiseOverlap(lineups: Array<{ playerIds: number[] }>): number {
  let worst = 0;
  for (let i = 0; i < lineups.length; i++) {
    for (let j = i + 1; j < lineups.length; j++) {
      const shared = lineups[i].playerIds.filter((id) => lineups[j].playerIds.includes(id)).length;
      if (shared > worst) worst = shared;
    }
  }
  return worst;
}

/**
 * Estimate duplication for each lineup.
 *
 * When `fieldModel` is supplied (requires validated ownership), it returns a
 * model-based expected duplicate count computed on COMPLETE lineup probabilities
 * — never a product of marginal player ownership. Otherwise it returns a
 * heuristic concentration score that is explicitly NOT a duplicate count.
 */
export function estimateDuplication(
  lineups: Array<{ lineupNumber: number; playerIds: number[]; totalSalary: number }>,
  options: {
    ownershipValidated: boolean;
    /** Optional field model: given a lineup's player ids, returns P(field entry = this exact lineup). */
    fieldModel?: (playerIds: number[]) => number;
    fieldSize?: number;
    /** Per-player flex ownership decimals for the heuristic concentration signal. */
    ownershipByPlayer?: Map<number, number>;
  },
): LineupDuplication[] {
  const modelAvailable = options.ownershipValidated && typeof options.fieldModel === "function" && typeof options.fieldSize === "number";
  return lineups.map((l) => {
    if (modelAvailable) {
      const p = Math.max(0, Math.min(1, options.fieldModel!(l.playerIds)));
      return { lineupNumber: l.lineupNumber, basis: "model" as const, expectedDuplicates: p * options.fieldSize!, concentrationScore: null, label: `~${(p * options.fieldSize!).toFixed(1)} expected duplicates (field model)` };
    }
    if (options.ownershipByPlayer && options.ownershipByPlayer.size) {
      // Ordinal concentration: mean ownership of the lineup's players, scaled.
      // This is a RELATIVE chalkiness signal, not a probability or a count.
      const vals = l.playerIds.map((id) => options.ownershipByPlayer!.get(id) ?? 0);
      const mean = vals.reduce((s, v) => s + v, 0) / (vals.length || 1);
      return { lineupNumber: l.lineupNumber, basis: "heuristic" as const, expectedDuplicates: null, concentrationScore: Math.max(0, Math.min(1, mean)), label: `Concentration ${(Math.max(0, Math.min(1, mean)) * 100).toFixed(0)}% (uncalibrated estimate — not a duplicate count)` };
    }
    return { lineupNumber: l.lineupNumber, basis: "unavailable" as const, expectedDuplicates: null, concentrationScore: null, label: "Duplication unavailable (needs ownership)" };
  });
}
