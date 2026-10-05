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

import { assertDstGameScript, assertShowdownLineup, type ShowdownPlayer } from './showdown-legality';
import { kickerRoleBlockedReason } from './availability';
import { maxPairwiseOverlap as realizedMaxOverlap } from './salary-duplication';

export const NFL_QA_RULESET_VERSION = "nfl-gpp-qa-v5-kicker-role";

/**
 * Evidence a run saved (or failed to save) for one QA check. `undefined` means
 * the caller did not supply it and the check is left out entirely; `null`
 * means it should exist but does not (a run saved before the evidence was
 * recorded), and the check says it could not be evaluated instead of passing.
 * Before 2026-09-29 a missing list read as an empty one, so 40 of 51 saved runs
 * showed "No inactive players used" without anything having been checked.
 */
type Evidence<T> = T | null | undefined;

/**
 * The most players any two lineups may share: the stricter of min-unique and
 * an explicit overlap cap, and never a whole roster (no exact duplicates).
 * The optimizer builds with this rule and QA checks against the same one.
 */
export function nflOverlapCap(format: "classic" | "showdown", minUnique: number, maxPairwiseOverlap?: number | null): number {
  const rosterSize = format === "showdown" ? 6 : 9;
  return Math.min(rosterSize - minUnique, maxPairwiseOverlap ?? rosterSize, rosterSize - 1);
}

/** A player as the page's current slate describes him, for the current-pool check. */
export interface CurrentPoolPlayer {
  dkPlayerId: number;
  name: string;
  isOut: boolean;
  /** Not playing (the workspace's `ruledOut`); a depth-chart block alone is not this. */
  ruledOut?: boolean;
  projectionStatus?: string | null;
  dkStatus?: string | null;
  position?: string;
  depthRole?: string | null;
  availability?: { blockedReason?: string | null; role?: string; chartRole?: string; fresh?: boolean } | null;
}

/**
 * A depth-chart block (a quarterback now listed behind the starter) is a
 * judgement the chart can get wrong; an injury or DraftKings ruling is a fact.
 * The first may be overridden with a reason; the second never.
 */
const ROLE_BLOCK = /starter workload not supported/;
// Only when the slate says outright he is NOT ruled out: the read layer zeroes
// a blocked backup's projection too, so projectionStatus can't tell them apart.
function roleOnlyBlock(player: CurrentPoolPlayer, reason: string): boolean {
  return ROLE_BLOCK.test(reason) && player.ruledOut === false;
}

/** Why a player cannot be used right now, in words, or null when he can. */
export function currentUnavailableReason(player: CurrentPoolPlayer): string | null {
  const blocked = player.availability?.blockedReason?.trim();
  if (blocked) return blocked;
  const kickerBlock = kickerRoleBlockedReason({ ...player, position: player.position ?? '' });
  if (kickerBlock) return kickerBlock;
  if (player.isOut) return player.dkStatus ? `DraftKings lists him ${player.dkStatus.trim().toUpperCase()}` : "ruled out";
  if (player.projectionStatus === "out") return "ruled out by our injury feed";
  return null;
}

/** Whether a run's settings shaped the portfolio with an archetype plan (the optimizer's own rule). */
export function usedPortfolioPlan(settings: { format?: string; archetypeMode?: string; archetypeQuotas?: Array<{ enabled?: boolean }> }): boolean {
  if (settings.archetypeQuotas?.some((quota) => quota.enabled)) return true;
  return settings.format === "showdown" && (settings.archetypeMode === "balanced" || settings.archetypeMode === "chalk_leverage");
}

/**
 * The evidence-backed QA inputs for a saved run: what it recorded, or null
 * ("couldn't be checked") for what it should have recorded but did not.
 */
export function savedRunQaEvidence(
  evidence: { eligibility?: QaInput["eligibility"]; exposureReport?: QaInput["exposureReport"]; archetypePlan?: QaInput["archetypePlan"] } | null | undefined,
  settings: Parameters<typeof usedPortfolioPlan>[0],
): Pick<QaInput, "eligibility" | "exposureReport" | "archetypePlan"> {
  const plan = usedPortfolioPlan(settings);
  return {
    eligibility: evidence?.eligibility ?? null,
    exposureReport: evidence?.exposureReport ?? null,
    archetypePlan: evidence?.archetypePlan ?? (plan ? null : undefined),
  };
}

/** The current pool, reduced to what the current-pool QA check reads. */
export function currentPoolForQa(players: readonly CurrentPoolPlayer[]): NonNullable<QaInput["currentPool"]> {
  return { players: players.map((p) => {
    const unavailable = currentUnavailableReason(p);
    return { dkPlayerId: p.dkPlayerId, name: p.name, unavailable, roleOnly: unavailable != null && roleOnlyBlock(p, unavailable) };
  }) };
}

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
    slots: Array<{ slot: string; playerId: number; salary?: number; player?: ShowdownPlayer }>;
    archetype?: { id: string; label: string; fadedPlayerIds: number[]; beneficiariesSatisfied: string[] } | null;
  }>;
  /** Generation-time eligibility. See `Evidence` for undefined vs null. */
  eligibility?: Evidence<Array<{ dkPlayerId: number; name: string; eligible: boolean; reasonCode: string | null; overridden: boolean }>>;
  exposureReport?: Evidence<Array<{ dkPlayerId: number; name: string; binding: string | null }>>;
  salaryBandReport?: Array<{ band: { min: number; max: number }; withinPlan: boolean; count: number; minCount: number; maxCount: number }>;
  duplication?: Array<{ lineupNumber: number; basis: string }>;
  /** Recorded overlap; QA also measures it from the lineups themselves and uses the larger. */
  maxPairwiseOverlap?: number;
  ownership?: { capability: string; errors: string[]; features: { leverage: boolean } };
  /** Archetype quota targets vs realized, when a plan was used. See `Evidence`. */
  archetypePlan?: Evidence<Array<{ archetypeId: string; label: string; requested: number; realized: number }>>;
  /** Projection freshness. */
  projectionStale?: boolean;
  newerRunAvailable?: boolean;
  projectionAgeHours?: number | null;
  /** Configured overlap cap (see `nflOverlapCap`; one less than a roster when unset). */
  overlapCap?: number;
  /**
   * Every player on the slate as it reads NOW (not when the lineups were
   * built), with the reason he cannot be used, if any. A lineup player who is
   * out now, or who is no longer on the slate, blocks export. `null` means the
   * current slate could not be read.
   */
  currentPool?: Evidence<{ players: Array<{ dkPlayerId: number; name: string; unavailable: string | null; roleOnly?: boolean }> }>;
  /** Entries in the DraftKings entry file, once one is chosen. */
  entryRows?: number | null;
  /** Selection/evaluation scenario digests (Phase 7); a collision is a blocker. */
  selectionDigest?: string | null;
  evaluationDigest?: string | null;
  /**
   * Availability coverage for the slate these lineups were built on. Absent
   * means the caller did not supply it, which is not the same as blind and is
   * therefore not checked.
   */
  availabilityCoverage?: { state: "blind" | "thin" | "adequate"; resolved: number; considered: number; fresh: number };
  /**
   * The code that built these lineups (the run's `build_info.commitSha`).
   * Absent means the caller did not supply it and is not checked.
   */
  build?: { commitSha: string | null };
}

/** A commit SHA that identifies deployed code, not a local working copy. */
export function isDeployedBuild(commitSha: string | null | undefined): boolean {
  return typeof commitSha === "string" && /^[0-9a-f]{7,40}$/i.test(commitSha);
}

const STALE_BLOCK_HOURS = 48;

/** Build the QA report. Overrides clear only overridable blockers. */
export function runNflPreExportQa(input: QaInput, overrides: QaOverride[] = []): QaReport {
  const overrideIds = new Set(overrides.map((o) => o.checkId));
  const checks: QaCheck[] = [];
  const add = (c: Omit<QaCheck, "overridable"> & { overridable?: boolean }) => checks.push({ overridable: false, ...c });

  const rosterSize = input.format === "showdown" ? 6 : 9;

  // --- Was this built by the live site? ---
  // Not overridable. The 2026-09-27 contest lineups were built from a local
  // checkout 132 commits behind main (commitSha "local-uncommitted"), missing
  // fixes the live site had; a rule to "use the live site" was broken again
  // the next evening. A mechanical guard replaces the rule.
  if (input.build && !isDeployedBuild(input.build.commitSha)) {
    add({
      id: "live_build",
      title: "Built by the live site",
      severity: "blocker",
      passed: false,
      detail: `These lineups were built from a local copy of the code (${input.build.commitSha ?? "no version recorded"}), not the live site, so they may be missing fixes. Rebuild them on the live site to export.`,
      affected: input.lineups.map((l) => l.lineupNumber),
    });
  }

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
  const illegal = input.lineups.filter((l) => {
    if (l.playerIds.length !== rosterSize || new Set(l.playerIds).size !== rosterSize || !Number.isFinite(l.totalSalary) || l.totalSalary <= 0 || l.totalSalary > 50000) return true;
    if (l.slots.length !== rosterSize || new Set(l.slots.map(s => s.playerId)).size !== rosterSize || l.slots.some(s => !l.playerIds.includes(s.playerId))) return true;
    if (input.format === 'showdown' && (l.slots[0].slot !== 'CPT' || l.slots.slice(1).some(s => !/^FLEX[1-5]?$/.test(s.slot)))) return true;
    if (input.format === 'showdown' && l.slots.some(s => s.player !== undefined)) {
      try {
        assertShowdownLineup({ ...l, slots: l.slots.map(s => {
          if (!s.player || s.salary === undefined) throw new Error('Missing slot salary data.');
          return { slot: s.slot, salary: s.salary, player: s.player };
        }) });
      } catch { return true; }
    }
    if (input.format === 'classic' && l.slots.some(s => s.player !== undefined)) {
      try {
        if (l.slots.some(s => !s.player)) throw new Error('Missing player data.');
        assertDstGameScript('classic', l.slots.map(s => ({slot:s.slot,player:s.player!})));
      } catch { return true; }
    }
    return false;
  });
  add({ id: "legal_roster", title: "Roster, salary, and DST game script", severity: "blocker", passed: illegal.length === 0,
    detail: illegal.length ? `${illegal.length} lineup(s) break a roster, salary, or DST game-script rule. Regenerate before exporting.` : "All lineups pass roster and game-script rules.",
    affected: illegal.map((l) => l.lineupNumber) });

  // --- Every requested lineup built, and every entry filled ---
  // A partial run exports fine and leaves the rest of the entry file as it was,
  // so a short set reached DraftKings unnoticed. Overridable: entering fewer
  // lineups than asked for is a legitimate choice, as long as it is a choice.
  const built = input.lineups.length;
  add({ id: "lineup_count", title: "Every requested lineup was built", severity: "blocker", passed: built >= input.requestedLineups, overridable: true,
    detail: built >= input.requestedLineups ? `All ${input.requestedLineups} requested lineups were built.`
      : `Only ${built} of the ${input.requestedLineups} lineups you asked for were built; the build notes say why. Export would enter ${built}.`,
    affected: [] });
  if (input.entryRows != null) {
    if (input.entryRows < built) {
      add({ id: "entry_rows", title: "Entry file has room for every lineup", severity: "blocker", passed: false, overridable: false,
        detail: `The entry file has ${input.entryRows} entr${input.entryRows === 1 ? "y" : "ies"} for ${built} lineups. Download an entry file with at least ${built} entries, or build fewer lineups.`,
        affected: [] });
    } else {
      add({ id: "entry_rows", title: "Every entry gets a lineup", severity: "blocker", passed: input.entryRows === built, overridable: true,
        detail: input.entryRows === built ? `All ${built} entries in the file get a lineup.`
          : `The entry file has ${input.entryRows} entries but there are ${built} lineups, so ${input.entryRows - built} entr${input.entryRows - built === 1 ? "y" : "ies"} would keep whatever lineup DraftKings already has.`,
        affected: [] });
    }
  }

  // --- Lineup players who are out NOW (blocker, never overridable) ---
  // Eligibility below is what was known when the lineups were BUILT. A player
  // ruled out since then, or a run saved before eligibility was recorded, used
  // to pass straight through to export.
  if (input.currentPool !== undefined) {
    if (input.currentPool === null) {
      add({ id: "current_pool", title: "Lineup players checked against the current pool", severity: "warning", passed: false, overridable: true,
        detail: "The current player pool could not be read, so these lineups were not re-checked for players ruled out since they were built.", affected: [] });
    } else {
      const now = new Map(input.currentPool.players.map((p) => [p.dkPlayerId, p]));
      const problems = new Map<number, { name: string; reason: string; roleOnly: boolean; lineups: number[] }>();
      for (const lineup of input.lineups) {
        for (const slot of lineup.slots) {
          const current = now.get(slot.playerId);
          const reason = current ? current.unavailable : "no longer on this slate's player pool";
          if (!reason) continue;
          const entry = problems.get(slot.playerId) ?? { name: current?.name ?? slot.player?.name ?? `Player ${slot.playerId}`, reason, roleOnly: current?.roleOnly === true, lineups: [] };
          entry.lineups.push(lineup.lineupNumber);
          problems.set(slot.playerId, entry);
        }
      }
      const describe = (list: { name: string; reason: string; lineups: number[] }[]) =>
        list.map((p) => `${p.name} (${p.reason}) is in ${p.lineups.length === 1 ? "lineup" : "lineups"} ${p.lineups.join(", ")}`).join("; ");
      const out = [...problems.values()].filter((p) => !p.roleOnly);
      const backups = [...problems.values()].filter((p) => p.roleOnly);
      add({ id: "current_pool", title: "No player who is out now", severity: "blocker", passed: out.length === 0, overridable: false,
        detail: out.length ? `${describe(out)}. Build again so these lineups use players who are playing.`
          : "Every lineup player is still available on the current slate.",
        affected: out.map((p) => p.name) });
      // Overridable: the depth chart can be wrong at the last minute.
      if (backups.length) {
        add({ id: "current_pool_role", title: "No quarterback now listed as a backup", severity: "blocker", passed: false, overridable: true,
          detail: `${describe(backups)}. Confirm the starter and build again, or override with a reason if the depth chart is wrong.`,
          affected: backups.map((p) => p.name) });
      }
    }
  }

  // --- Ineligible / inactive players in lineups (blocker) ---
  if (input.eligibility === null) {
    add({ id: "no_inactive", title: "No inactive players (when built)", severity: "warning", passed: false, overridable: true,
      detail: "This run was saved before eligibility was recorded, so who was inactive when it was built can't be checked. The current-pool check covers who is out now.",
      affected: [] });
    add({ id: "no_unapproved_punt", title: "No unapproved cheap players", severity: "warning", passed: false, overridable: true,
      detail: "This run was saved before eligibility was recorded, so cheap-player approvals can't be checked.", affected: [] });
  } else if (input.eligibility !== undefined) {
    const ineligibleById = new Map(input.eligibility.filter((e) => !e.eligible).map((e) => [e.dkPlayerId, e]));
    const usedIneligible = input.lineups.flatMap((l) => l.playerIds.filter((id) => ineligibleById.has(id)).map((id) => ({ lineup: l.lineupNumber, e: ineligibleById.get(id)! })));
    const usedInactive = usedIneligible.filter((x) => x.e.reasonCode === "INACTIVE");
    add({ id: "no_inactive", title: "No inactive players", severity: "blocker", passed: usedInactive.length === 0, overridable: false,
      detail: usedInactive.length ? `${usedInactive.length} lineup slot(s) use an inactive player.` : "No inactive players used.",
      affected: usedInactive.map((x) => x.e.name) });
    const usedPunt = usedIneligible.filter((x) => x.e.reasonCode !== "INACTIVE" && !x.e.overridden);
    add({ id: "no_unapproved_punt", title: "No unapproved cheap players", severity: "blocker", passed: usedPunt.length === 0, overridable: true,
      detail: usedPunt.length ? `${usedPunt.length} lineup slot(s) use a punt/unknown-role player not allowed for this run. Approve via the cheap-player flow.` : "No unapproved cheap players used.",
      affected: usedPunt.map((x) => x.e.name) });
  }

  // --- Projection freshness (warning; blocker past age) ---
  const ageBlock = (input.projectionAgeHours ?? 0) > STALE_BLOCK_HOURS;
  const age = input.projectionAgeHours != null && Number.isFinite(input.projectionAgeHours) ? Math.round(input.projectionAgeHours) : null;
  add({ id: "projection_freshness", title: "Projection freshness", severity: input.projectionStale && ageBlock ? "blocker" : "warning",
    passed: !input.projectionStale && !input.newerRunAvailable, overridable: true,
    detail: input.newerRunAvailable ? "A newer projection run is available than the one these lineups were built on."
      : input.projectionStale ? `The projections behind these lineups are ${age ?? "many"} hours old. Update data and build again for the latest injuries and roles.`
      : age != null ? `Projections are current (built ${age} hour${age === 1 ? "" : "s"} ago).` : "Projections are current.",
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
  if (input.exposureReport === null) {
    add({ id: "exposure_ranges", title: "Exposure ranges satisfied", severity: "warning", passed: false, overridable: true,
      detail: "This run was saved before exposure results were recorded, so its exposure ranges can't be checked.", affected: [] });
  } else if (input.exposureReport !== undefined) {
    const exposureMisses = input.exposureReport.filter((r) => r.binding && /missed/.test(r.binding));
    add({ id: "exposure_ranges", title: "Exposure ranges satisfied", severity: "blocker", passed: exposureMisses.length === 0, overridable: true,
      detail: exposureMisses.length ? `${exposureMisses.length} player(s) missed a slot exposure minimum: ${exposureMisses.slice(0, 5).map((r) => `${r.name} (${r.binding})`).join("; ")}.` : "All exposure ranges satisfied.",
      affected: exposureMisses.map((r) => r.name) });
  }

  // --- Archetype quota violations (blocker) ---
  if (input.archetypePlan === null) {
    add({ id: "archetype_quotas", title: "Archetype quotas met", severity: "warning", passed: false, overridable: true,
      detail: "This run used a portfolio plan but was saved before the plan's quotas were recorded, so they can't be checked.", affected: [] });
  } else if (input.archetypePlan !== undefined) {
    const quotaMisses = input.archetypePlan.filter((a) => a.realized !== a.requested);
    add({ id: "archetype_quotas", title: "Archetype quotas met", severity: "blocker", passed: quotaMisses.length === 0, overridable: true,
      detail: quotaMisses.length ? `Quota shortfall: ${quotaMisses.map((a) => `${a.label} ${a.realized}/${a.requested}`).join("; ")}.` : "Archetype quotas met.",
      affected: quotaMisses.map((a) => a.label) });
  }

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
  // Measured from the lineups themselves, so the check is always evaluated.
  const overlapCap = input.overlapCap ?? rosterSize - 1;
  const overlap = Math.max(realizedMaxOverlap(input.lineups), input.maxPairwiseOverlap ?? 0);
  const overlapBad = overlap > overlapCap;
  add({ id: "overlap", title: "Pairwise overlap within cap", severity: "blocker", passed: !overlapBad, overridable: false,
    detail: overlapBad ? `Two lineups share ${overlap} players; the cap is ${overlapCap}.` : `No two lineups share more than ${overlap} of ${rosterSize} players (cap ${overlapCap}).`, affected: [] });

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
