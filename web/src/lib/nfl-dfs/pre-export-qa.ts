/**
 * Phase 6 (spec §13): pre-export portfolio QA.
 *
 * A single, versioned ruleset produces one decision — Ready, Ready with
 * warnings, or Blocked — plus a per-check report. Export is gated on the
 * absence of un-overridden blockers. Rules declare whether they are
 * overridable; an override records who, when, ruleset version, run id and the
 * before/after state (handled by the caller/persistence layer).
 *
 * Pure and deterministic: given the same run inputs it returns the same report.
 */

export const NFL_QA_RULESET_VERSION = "nfl-gpp-qa-v1";

export type QaSeverity = "blocker" | "warning" | "info";
export type QaDecision = "ready" | "ready_with_warnings" | "blocked";

export interface QaCheck {
  id: string;
  title: string;
  severity: QaSeverity;
  passed: boolean;
  /** Human explanation shown in the report. */
  detail: string;
  /** Whether a recorded override can clear this check. Inactive/illegal are never overridable. */
  overridable: boolean;
  /** Ids of affected lineups or players, for direct links. */
  affected: Array<string | number>;
}

export interface QaOverride {
  checkId: string;
  reason: string;
  user: string;
  at: string;
  rulesetVersion: string;
  runId: string;
}

export interface QaReport {
  rulesetVersion: string;
  decision: QaDecision;
  checks: QaCheck[];
  counts: { blocker: number; warning: number; info: number };
  /** Blockers still open after applying overrides. Export is allowed only when empty. */
  openBlockers: string[];
}

/** The subset of run data the QA ruleset evaluates. */
export interface QaInput {
  format: "classic" | "showdown";
  requestedLineups: number;
  lineups: Array<{
    lineupNumber: number;
    playerIds: number[];
    totalSalary: number;
    slots: Array<{ slot: string; playerId: number }>;
    archetype?: { id: string; label: string; fadedPlayerIds: number[]; beneficiariesSatisfied: string[] } | null;
  }>;
  eligibility?: Array<{ dkPlayerId: number; name: string; eligible: boolean; reasonCode: string | null; overridden: boolean }>;
  exposureReport?: Array<{ dkPlayerId: number; name: string; binding: string | null }>;
  salaryBandReport?: Array<{ band: { min: number; max: number }; withinPlan: boolean; count: number; minCount: number; maxCount: number }>;
  duplication?: Array<{ lineupNumber: number; basis: string }>;
  maxPairwiseOverlap?: number;
  ownership?: { capability: string; errors: string[]; features: { leverage: boolean } };
  /** Archetype quota targets vs realized, when a plan was used. */
  archetypePlan?: Array<{ archetypeId: string; label: string; requested: number; realized: number }>;
  /** Projection freshness. */
  projectionStale?: boolean;
  newerRunAvailable?: boolean;
  projectionAgeHours?: number | null;
  /** Configured overlap cap (rosterSize when unset). */
  overlapCap?: number;
  /** Selection/evaluation scenario digests (Phase 7); a collision is a blocker. */
  selectionDigest?: string | null;
  evaluationDigest?: string | null;
  /**
   * Availability coverage for the slate these lineups were built on. Absent
   * means the caller did not supply it, which is not the same as blind and is
   * therefore not checked.
   */
  availabilityCoverage?: { state: "blind" | "thin" | "adequate"; resolved: number; considered: number; fresh: number };
}

const STALE_BLOCK_HOURS = 48;

/** Build the QA report. Overrides clear only overridable blockers. */
export function runNflPreExportQa(input: QaInput, overrides: QaOverride[] = []): QaReport {
  const overrideIds = new Set(overrides.map((o) => o.checkId));
  const checks: QaCheck[] = [];
  const add = (c: Omit<QaCheck, "overridable"> & { overridable?: boolean }) => checks.push({ overridable: false, ...c });

  const rosterSize = input.format === "showdown" ? 6 : 9;

  // --- Did we know who was playing? ---
  // Overridable: exporting a slate we are blind on is a legitimate choice as
  // long as it is a choice. On 2026 week 2 it was not one -- the information
  // was in a different tab and nothing asked.
  const coverage = input.availabilityCoverage;
  if (coverage && coverage.state !== "adequate") {
    add({
      id: "availability_coverage",
      title: "Availability was known for this slate",
      severity: coverage.state === "blind" ? "blocker" : "warning",
      passed: false,
      overridable: true,
      detail: coverage.state === "blind"
        ? `Only ${coverage.resolved} of ${coverage.considered} players had any availability status (${coverage.fresh} fresh). These lineups may contain inactive players.`
        : `${coverage.resolved} of ${coverage.considered} players had an availability status, ${coverage.fresh} of them fresh.`,
      affected: [],
    });
  }

  // --- Illegal roster or salary (blocker, never overridable) ---
  const illegal = input.lineups.filter((l) => l.playerIds.length !== rosterSize || new Set(l.playerIds).size !== rosterSize || l.totalSalary > 50000);
  add({ id: "legal_roster", title: "Legal roster and salary", severity: "blocker", passed: illegal.length === 0,
    detail: illegal.length ? `${illegal.length} lineup(s) have an illegal roster size, duplicate player, or salary over the cap.` : "All lineups are legal.",
    affected: illegal.map((l) => l.lineupNumber) });

  // --- Ineligible / inactive players in lineups (blocker) ---
  const ineligibleById = new Map((input.eligibility ?? []).filter((e) => !e.eligible).map((e) => [e.dkPlayerId, e]));
  const usedIneligible = input.lineups.flatMap((l) => l.playerIds.filter((id) => ineligibleById.has(id)).map((id) => ({ lineup: l.lineupNumber, e: ineligibleById.get(id)! })));
  const usedInactive = usedIneligible.filter((x) => x.e.reasonCode === "INACTIVE");
  add({ id: "no_inactive", title: "No inactive players", severity: "blocker", passed: usedInactive.length === 0, overridable: false,
    detail: usedInactive.length ? `${usedInactive.length} lineup slot(s) use an inactive player.` : "No inactive players used.",
    affected: usedInactive.map((x) => x.e.name) });
  const usedPunt = usedIneligible.filter((x) => x.e.reasonCode !== "INACTIVE" && !x.e.overridden);
  add({ id: "no_unapproved_punt", title: "No unapproved cheap players", severity: "blocker", passed: usedPunt.length === 0, overridable: true,
    detail: usedPunt.length ? `${usedPunt.length} lineup slot(s) use a punt/unknown-role player not allowed for this run. Approve via the cheap-player flow.` : "No unapproved cheap players used.",
    affected: usedPunt.map((x) => x.e.name) });

  // --- Projection freshness (warning; blocker past age) ---
  const ageBlock = (input.projectionAgeHours ?? 0) > STALE_BLOCK_HOURS;
  add({ id: "projection_freshness", title: "Projection freshness", severity: input.projectionStale && ageBlock ? "blocker" : "warning",
    passed: !input.projectionStale && !input.newerRunAvailable, overridable: true,
    detail: input.newerRunAvailable ? "A newer projection run is available." : input.projectionStale ? `Projections are stale${input.projectionAgeHours ? ` (${Math.round(input.projectionAgeHours)}h)` : ""}.` : "Projections are current.",
    affected: [] });

  // --- Ownership state (info in projection-only; blocker if invalid + leverage on) ---
  if (input.ownership) {
    const invalidWithLeverage = input.ownership.errors.length > 0 && input.ownership.features.leverage;
    add({ id: "ownership_leverage_valid", title: "Ownership validity for leverage", severity: "blocker", passed: !invalidWithLeverage, overridable: false,
      detail: invalidWithLeverage ? "Leverage is enabled but ownership failed validation. Disable the feature or repair the input." : input.ownership.capability === "unavailable" ? "Projection-only: no ownership leverage claimed." : "Ownership state is consistent with enabled features.",
      affected: [] });
    if (input.ownership.capability === "unavailable") {
      add({ id: "ownership_unavailable", title: "Ownership unavailable", severity: "info", passed: true, overridable: false,
        detail: "Running projection-only. Leverage and duplication claims are disabled and excluded from the export audit.", affected: [] });
    }
  }

  // --- Exposure range violations (blocker) ---
  const exposureMisses = (input.exposureReport ?? []).filter((r) => r.binding && /missed/.test(r.binding));
  add({ id: "exposure_ranges", title: "Exposure ranges satisfied", severity: "blocker", passed: exposureMisses.length === 0, overridable: true,
    detail: exposureMisses.length ? `${exposureMisses.length} player(s) missed a slot exposure minimum: ${exposureMisses.slice(0, 5).map((r) => `${r.name} (${r.binding})`).join("; ")}.` : "All exposure ranges satisfied.",
    affected: exposureMisses.map((r) => r.name) });

  // --- Archetype quota violations (blocker) ---
  const quotaMisses = (input.archetypePlan ?? []).filter((a) => a.realized !== a.requested);
  add({ id: "archetype_quotas", title: "Archetype quotas met", severity: "blocker", passed: quotaMisses.length === 0, overridable: true,
    detail: quotaMisses.length ? `Quota shortfall: ${quotaMisses.map((a) => `${a.label} ${a.realized}/${a.requested}`).join("; ")}.` : "Archetype quotas met.",
    affected: quotaMisses.map((a) => a.label) });

  // --- Fade without a satisfied beneficiary (blocker) ---
  const fadeNoBenef = input.lineups.filter((l) => l.archetype && l.archetype.fadedPlayerIds.length > 0 && l.archetype.beneficiariesSatisfied.length === 0);
  add({ id: "fade_beneficiary", title: "Fades carry a beneficiary", severity: "blocker", passed: fadeNoBenef.length === 0, overridable: true,
    detail: fadeNoBenef.length ? `${fadeNoBenef.length} fade lineup(s) satisfy no beneficiary rule. Fix the lineup or remove the fade label.` : "Every fade lineup satisfies a beneficiary path.",
    affected: fadeNoBenef.map((l) => l.lineupNumber) });

  // --- Salary-left concentration (warning) ---
  const bandMiss = (input.salaryBandReport ?? []).filter((b) => !b.withinPlan);
  add({ id: "salary_left_concentration", title: "Salary-left within plan", severity: "warning", passed: bandMiss.length === 0, overridable: true,
    detail: bandMiss.length ? `${bandMiss.length} salary-left band(s) are outside the plan.` : "Salary-left distribution is within plan.",
    affected: bandMiss.map((b) => `$${b.band.min}-$${b.band.max}`) });

  // --- Excess overlap (blocker) ---
  const overlapCap = input.overlapCap ?? rosterSize;
  const overlapBad = (input.maxPairwiseOverlap ?? 0) > overlapCap;
  add({ id: "overlap", title: "Pairwise overlap within cap", severity: "blocker", passed: !overlapBad, overridable: false,
    detail: overlapBad ? `Maximum pairwise overlap ${input.maxPairwiseOverlap} exceeds the cap ${overlapCap}.` : "Pairwise overlap within the cap.", affected: [] });

  // --- Excess model-based duplication (warning; only meaningful with a field model) ---
  const modelDup = (input.duplication ?? []).some((d) => d.basis === "model");
  if (modelDup) {
    add({ id: "duplication_model", title: "Model-based duplication reviewed", severity: "warning", passed: true, overridable: true,
      detail: "Model-based duplication estimates are available; review high-duplicate lineups.", affected: [] });
  }

  // --- Selection/evaluation bank collision (Phase 7 blocker) ---
  if (input.selectionDigest && input.evaluationDigest) {
    const collision = input.selectionDigest === input.evaluationDigest;
    add({ id: "bank_collision", title: "Selection/evaluation banks differ", severity: "blocker", passed: !collision, overridable: false,
      detail: collision ? "Selection and evaluation scenario banks share a digest; evaluation would not be independent." : "Selection and evaluation banks are independent.", affected: [] });
  }

  // Decision.
  const failed = checks.filter((c) => !c.passed);
  const openBlockers = failed.filter((c) => c.severity === "blocker" && !(c.overridable && overrideIds.has(c.id))).map((c) => c.id);
  const openWarnings = failed.filter((c) => c.severity === "warning");
  const counts = {
    blocker: failed.filter((c) => c.severity === "blocker").length,
    warning: openWarnings.length,
    info: checks.filter((c) => c.severity === "info").length,
  };
  const decision: QaDecision = openBlockers.length ? "blocked" : openWarnings.length ? "ready_with_warnings" : "ready";
  return { rulesetVersion: NFL_QA_RULESET_VERSION, decision, checks, counts, openBlockers };
}
