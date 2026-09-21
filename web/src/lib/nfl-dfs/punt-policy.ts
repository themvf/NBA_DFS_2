/**
 * Phase 1 (spec §8): role-aware no-punt policy.
 *
 * A cheap player is admitted by verified role, not by price. This module is the
 * single source of truth for WHY a player is eligible or blocked. It fails
 * closed: unknown role state below the evidence threshold blocks rather than
 * silently admitting a min-priced body. Salary alone is never a valid reason to
 * keep a player — that requires a recorded manual override.
 *
 * Pure and deterministic: no I/O, no clock. Callers supply the player's
 * resolved role evidence and the run's policy.
 */

export type EvidenceState = "confirmed" | "probable" | "unknown" | "stale";

export type PuntReasonCode =
  | "ABSOLUTE_SALARY_BLOCK"
  | "ROLE_UNKNOWN"
  | "ROLE_UNRESOLVED"
  | "NO_PROJECTED_OPPORTUNITY"
  | "EVIDENCE_STALE"
  | "INACTIVE"
  | "MANUAL_EXCLUSION";

export type PuntMode = "no_punts" | "role_qualified" | "custom";

export interface NflPuntPolicy {
  mode: PuntMode;
  /** Players at or below this salary are hard-blocked regardless of evidence. */
  absoluteMinSalary: number;
  /** Below this salary, fresh role evidence is required to be eligible. */
  roleEvidenceRequiredBelowSalary: number;
  /** 0..1. Minimum role confidence required below the evidence threshold. */
  minimumRoleConfidence: number;
  /** Minimum projected opportunities required below the evidence threshold; null disables the check. */
  minimumProjectedOpportunities: number | null;
  /** At most this many salary-relief players may appear in a single lineup. */
  maxSalaryReliefPlayersPerLineup: number;
  /** Cheap players explicitly admitted with a recorded reason (see overrides). */
  allowlistedPlayerIds: number[];
  /** Players explicitly removed by the user. */
  denylistedPlayerIds: number[];
}

export interface NflPlayerRoleEvidence {
  playerId: number;
  /** DK Status column verdict; null when unknown. */
  verifiedActive: boolean | null;
  availabilityState: EvidenceState;
  depthRole: string | null;
  /** 0..1; null is unknown, NOT zero. */
  roleConfidence: number | null;
  /** Projected opportunities (touches/targets/etc); null is unknown, NOT zero. */
  projectedOpportunities: number | null;
  opportunityUnit: "touch" | "target" | "attempt" | "kick" | "defense" | null;
  /** The player's own observed games; null is unknown. */
  observedGameCount: number | null;
  sourceIds: string[];
  evidenceAsOf: string | null;
}

/** A recorded manual admission of a cheap player. Salary is never a valid reason. */
export interface PuntOverride {
  playerId: number;
  reason: string;
  user: string;
  at: string;
  /** Captain admissions require their own override; Flex-only by default. */
  slot: "FLEX" | "CPT";
}

export type PuntEligibility =
  | { playerId: number; eligible: true; salaryRelief: boolean; overridden: boolean }
  | { playerId: number; eligible: false; reason: PuntReasonCode; detail: string };

/** The shipped-default no-punts preset. Values are configuration, not constants. */
export const DEFAULT_NFL_PUNT_POLICY: NflPuntPolicy = {
  mode: "no_punts",
  absoluteMinSalary: 1000,               // $200–$800 hard-blocked; $1,000 is the first admissible tier.
  roleEvidenceRequiredBelowSalary: 3000,
  minimumRoleConfidence: 0.4,
  minimumProjectedOpportunities: 1,
  maxSalaryReliefPlayersPerLineup: 1,
  allowlistedPlayerIds: [],
  denylistedPlayerIds: [],
};

export function validateNflPuntPolicy(policy: NflPuntPolicy): void {
  if (!["no_punts", "role_qualified", "custom"].includes(policy.mode)) throw new Error("Unknown punt policy mode.");
  for (const [label, value] of [["absoluteMinSalary", policy.absoluteMinSalary], ["roleEvidenceRequiredBelowSalary", policy.roleEvidenceRequiredBelowSalary]] as const) {
    if (!Number.isFinite(value) || value < 0 || value > 50000) throw new Error(`${label} must be between $0 and $50,000.`);
  }
  if (!Number.isFinite(policy.minimumRoleConfidence) || policy.minimumRoleConfidence < 0 || policy.minimumRoleConfidence > 1) {
    throw new Error("minimumRoleConfidence must be between 0 and 1.");
  }
  if (policy.minimumProjectedOpportunities !== null && (!Number.isFinite(policy.minimumProjectedOpportunities) || policy.minimumProjectedOpportunities < 0)) {
    throw new Error("minimumProjectedOpportunities must be null or a non-negative number.");
  }
  if (!Number.isInteger(policy.maxSalaryReliefPlayersPerLineup) || policy.maxSalaryReliefPlayersPerLineup < 0) {
    throw new Error("maxSalaryReliefPlayersPerLineup must be a non-negative integer.");
  }
}

const REASON_TEXT: Record<PuntReasonCode, string> = {
  ABSOLUTE_SALARY_BLOCK: "Priced at or below the absolute salary block — a punt by price, not a role.",
  ROLE_UNKNOWN: "Cheap player with unknown role and no fresh evidence — fails closed.",
  ROLE_UNRESOLVED: "Cheap player whose role confidence is below the required threshold.",
  NO_PROJECTED_OPPORTUNITY: "Cheap player with no projected opportunity — salary is not a role.",
  EVIDENCE_STALE: "Role evidence is stale; a cheap player cannot be cleared on stale evidence.",
  INACTIVE: "Player is inactive; the allowlist cannot override inactive status.",
  MANUAL_EXCLUSION: "Player is on the denylist for this run.",
};

/**
 * Decide a single player's eligibility under the policy.
 *
 * Precedence (most authoritative first): manual denylist, inactive status,
 * absolute salary block, then — only for players below the evidence threshold —
 * stale evidence, unknown role, insufficient confidence, and missing
 * opportunity. An allowlisted player clears the cheap-player role gate but never
 * clears inactivity; a Captain admission requires an explicit CPT override.
 */
export function evaluatePuntEligibility(
  player: { dkPlayerId: number; salary: number; isOut: boolean },
  evidence: NflPlayerRoleEvidence,
  policy: NflPuntPolicy,
  overrides: PuntOverride[] = [],
): PuntEligibility {
  const id = player.dkPlayerId;
  const deny = new Set(policy.denylistedPlayerIds);
  const allow = new Set(policy.allowlistedPlayerIds);
  const override = overrides.find((o) => o.playerId === id);
  const overridden = allow.has(id) || Boolean(override);

  const blocked = (reason: PuntReasonCode, extra = ""): PuntEligibility => ({
    playerId: id, eligible: false, reason, detail: `${REASON_TEXT[reason]}${extra ? ` ${extra}` : ""}`,
  });

  if (deny.has(id)) return blocked("MANUAL_EXCLUSION");
  // Inactivity outranks everything, including any allowlist entry.
  if (player.isOut || evidence.verifiedActive === false) return blocked("INACTIVE");

  const absoluteBlock = player.salary <= policy.absoluteMinSalary;
  const belowEvidenceThreshold = player.salary < policy.roleEvidenceRequiredBelowSalary;

  // The absolute block is a floor: below it there is no legitimate role at all.
  // Only a recorded override may lift it (spec §8.1: reason required).
  if (absoluteBlock && !overridden) return blocked("ABSOLUTE_SALARY_BLOCK", `$${player.salary.toLocaleString()} ≤ $${policy.absoluteMinSalary.toLocaleString()}.`);

  const salaryRelief = belowEvidenceThreshold;
  if (!salaryRelief) return { playerId: id, eligible: true, salaryRelief: false, overridden };

  // Below the evidence threshold. An override clears the ROLE gate (but the
  // player is still counted as salary-relief and still cannot be inactive).
  if (overridden) return { playerId: id, eligible: true, salaryRelief: true, overridden: true };

  if (policy.mode === "custom") {
    // Custom mode still enforces the absolute block and inactivity above, but
    // leaves the role gate to explicit thresholds only.
  }

  if (evidence.availabilityState === "stale") return blocked("EVIDENCE_STALE");
  if (evidence.roleConfidence === null || evidence.depthRole === null) return blocked("ROLE_UNKNOWN");
  if (evidence.roleConfidence < policy.minimumRoleConfidence) {
    return blocked("ROLE_UNRESOLVED", `confidence ${(evidence.roleConfidence * 100).toFixed(0)}% < ${(policy.minimumRoleConfidence * 100).toFixed(0)}%.`);
  }
  if (policy.minimumProjectedOpportunities !== null) {
    if (evidence.projectedOpportunities === null) return blocked("NO_PROJECTED_OPPORTUNITY", "opportunity unknown.");
    if (evidence.projectedOpportunities < policy.minimumProjectedOpportunities) {
      return blocked("NO_PROJECTED_OPPORTUNITY", `${evidence.projectedOpportunities} < ${policy.minimumProjectedOpportunities}.`);
    }
  }

  return { playerId: id, eligible: true, salaryRelief: true, overridden: false };
}

/** Whether a recorded override authorizes the player at Captain. Flex-only by default. */
export function captainAdmissible(playerId: number, overrides: PuntOverride[]): boolean {
  return overrides.some((o) => o.playerId === playerId && o.slot === "CPT");
}
