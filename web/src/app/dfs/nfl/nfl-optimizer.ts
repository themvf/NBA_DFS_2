import "server-only";
import {selectedWorkload,validateWorkloadPositions,WORKLOAD_POSITIONS,type WorkloadPositions} from "@/lib/nfl-dfs/workload-selection";
import type { WorkloadProjection } from "@/lib/nfl-dfs/workload-projection";
import type { CalibratedProjection } from "@/lib/nfl-dfs/calibrated-projection";
import type { SituationSettings, ProjectionAudit, SituationEvidence } from '@/lib/nfl-dfs/projection-audit';
import { hasObservedOpportunity, MIN_OBSERVED_GAMES } from '@/lib/nfl-dfs/opportunity-redistribution';
import {
  DEFAULT_NFL_PUNT_POLICY,
  evaluatePuntEligibility,
  captainAdmissible,
  validateNflPuntPolicy,
  type NflPuntPolicy,
  type PuntOverride,
  type NflPlayerRoleEvidence,
  type EvidenceState,
} from '@/lib/nfl-dfs/punt-policy';
import type { OwnershipCapability } from '@/lib/nfl-dfs/ownership-capability';
import {
  deriveExposureCounts,
  detectExposureInfeasibility,
  validateExposurePolicy,
  type PlayerExposurePolicy,
  type ExposureCounts,
} from '@/lib/nfl-dfs/exposure-plan';
import {
  allocateArchetypeQuotas,
  compileArchetype,
  ARCHETYPE_LABELS,
  isFadeArchetype,
  type ArchetypeId,
  type ArchetypeQuota,
  type ArchetypeConfig,
  type CompiledArchetype,
  type ArchetypeSlateContext,
} from '@/lib/nfl-dfs/archetypes';
import {
  validateSalaryPolicy,
  reportSalaryBands,
  findExactDuplicates,
  maxPairwiseOverlap as computeMaxOverlap,
  estimateDuplication,
  NFL_SALARY_CAP,
  type SalaryConstructionPolicy,
  type SalaryBandReport,
  type LineupDuplication,
} from '@/lib/nfl-dfs/salary-duplication';

// Phase 0/1: bumped from v5 to record that role-aware eligibility now gates the
// pool. Legacy runs keep their own recorded version and are not reinterpreted.
export const NFL_OPTIMIZER_VERSION = "nfl-dfs-ilp-v6-punt-policy";

export type NflProjectionSource = "our" | "workload" | "calibrated" | "dk_avg" | "fantasypros" | "linestar" | "custom";
export type NflOptimizerMode = "cash" | "gpp";
export type NflSlateFormat = "classic" | "showdown";

export type NflOptimizerPlayer = {
  id: number;
  dkPlayerId: number;
  captainDkPlayerId: number | null;
  name: string;
  position: "QB" | "RB" | "WR" | "TE" | "K" | "DST";
  team: string;
  opponent: string | null;
  gameKey: string | null;
  salary: number;
  captainSalary: number | null;
  isOut: boolean;
  projectionStatus: string;
  /** The player's OWN games behind his projection. 0 means the number is his position's average, not his. */
  historyGames?: number | null;
  /** Depth-chart role label when known; null is unknown, not "no role". */
  depthRole?: string | null;
  /** 0..1 role confidence when known; null is unknown, not zero. */
  roleConfidence?: number | null;
  /** Projected opportunities (touches/targets/etc); null is unknown, not zero. */
  projectedOpportunities?: number | null;
  /** Availability evidence freshness, used to fail cheap players closed on stale data. */
  availabilityState?: EvidenceState;
  ourProj: number | null;
  floorFpts: number | null;
  ceilingFpts: number | null;
  boomRate: number | null;
  avgFptsDk: number | null;
  fantasyprosProj: number | null;
  linestarProj: number | null;
  linestarOwnPct: number | null;
  customProj: number | null;
  workload?: WorkloadProjection | null;
  workloadReason?: string;
  positionWorkload?: CalibratedProjection | null;
  positionWorkloadReason?: string;
  calibrated?: CalibratedProjection | null;
  calibrationReason?: string;
  situationEvidence?: SituationEvidence;
  projectionAudit?: ProjectionAudit;
};

export type NflOptimizerSettings = {
  format: NflSlateFormat;
  mode: NflOptimizerMode;
  projectionSource: NflProjectionSource;
  allowDkFallback: boolean;
  workloadPositions?: WorkloadPositions;
  situations?: SituationSettings;
  nLineups: number;
  minSalary: number;
  /** Per-player salary floor. Drops minimum-priced roster filler. Absent/0 = no floor. Superseded by puntPolicy when present. */
  minPlayerSalary?: number;
  /** Drop players with no observed games of their own, whose projection is a position average. */
  requireObservedHistory?: boolean;
  /** Phase 1: role-aware no-punt policy. Absent = no policy (legacy behavior). */
  puntPolicy?: NflPuntPolicy;
  /** Phase 1: recorded cheap-player admissions. Salary alone is never a valid reason. */
  puntOverrides?: PuntOverride[];
  /** Phase 2: resolved ownership capability. When not "validated", leverage is disabled. */
  ownershipCapability?: OwnershipCapability;
  /**
   * Phase 2: whether the ownership penalty may be applied, resolved by the
   * SERVER from the assessment's features (validated feeds, or a declared
   * heuristic the user explicitly opted into — always labeled uncalibrated).
   * Absent = legacy rule: leverage only when capability is "validated".
   */
  ownershipLeverageEnabled?: boolean;
  /** User opt-in for uncalibrated heuristic ownership leverage (client → server; the server resolves the final bit). */
  useHeuristicOwnershipLeverage?: boolean;
  /** Phase 3: per-player Overall/Captain/Flex exposure ranges. Overrides the flat maxExposure per player. */
  exposurePolicies?: PlayerExposurePolicy[];
  /** Phase 4: per-archetype portfolio quotas. Absent = single Standard-ceiling quota. */
  archetypeQuotas?: ArchetypeQuota[];
  /** Phase 4: per-archetype config (fades, beneficiaries, skews, captain ceilings). */
  archetypeConfigs?: Partial<Record<ArchetypeId, ArchetypeConfig>>;
  /** Phase 4: favorite/underdog teams for game-script archetypes. */
  favoriteTeam?: string | null;
  underdogTeam?: string | null;
  /** Phase 5: salary-used and salary-left construction controls. Overrides the flat minSalary. */
  salaryPolicy?: SalaryConstructionPolicy;
  /** Phase 5: maximum shared players allowed between any two lineups (0..rosterSize). */
  maxPairwiseOverlap?: number;
  maxExposure: number;
  minUnique: number;
  stackPassCatchers: 0 | 1 | 2;
  bringBack: boolean;
  randomness: number;
  lockedPlayerIds: number[];
  excludedPlayerIds: number[];
  minExposureByPlayer: Record<string, number>;
  maxExposureByPlayer: Record<string, number>;
};

/** Re-exported so callers can build policy-aware settings from one import site. */
export { DEFAULT_NFL_PUNT_POLICY };
export type { NflPuntPolicy, PuntOverride };
export type { PlayerExposurePolicy };

/** Phase 3: realized vs requested exposure for a player, surfaced to the UI. */
export type NflExposureReport = {
  dkPlayerId: number;
  name: string;
  overall: number;
  captain: number;
  flex: number;
  overallMin: number;
  overallMax: number;
  captainMin: number;
  captainMax: number;
  flexMin: number;
  flexMax: number;
  binding: string | null;
};

export type NflLineupSlot = {
  slot: string;
  player: NflOptimizerPlayer;
  salary: number;
  multiplier: number;
  projection: number;
  projectionSource: NflProjectionSource | "dk_avg_fallback" | "our_fallback";
};

export type NflGeneratedLineup = {
  lineupNumber: number;
  slots: NflLineupSlot[];
  playerIds: number[];
  totalSalary: number;
  projectedFpts: number;
  floorFpts: number;
  ceilingFpts: number;
  projectedOwnership: number | null;
  stackSummary: { quarterback: string | null; passCatchers: string[]; bringBack: string | null };
  /** Phase 4: exactly one primary archetype label, its faded players, and beneficiary rules satisfied. */
  archetype?: {
    id: ArchetypeId;
    label: string;
    fadedPlayerIds: number[];
    fadedPlayerNames: string[];
    beneficiariesSatisfied: string[];
  };
};

/** Per-player eligibility decision surfaced to the UI so the pool can say WHY. */
export type NflEligibilityDecision = {
  dkPlayerId: number;
  name: string;
  salary: number;
  eligible: boolean;
  salaryRelief: boolean;
  captainEligible: boolean;
  overridden: boolean;
  reason: string | null;
  reasonCode: string | null;
};

export type NflOptimizerResult = {
  lineups: NflGeneratedLineup[];
  warnings: string[];
  sourceCoverage: { requested: number; direct: number; fallback: number; excluded: number };
  /** Phase 1: eligibility decisions for every input player (present when a punt policy is applied). */
  eligibility?: NflEligibilityDecision[];
  /** Phase 3: realized vs requested slot-specific exposures (present when exposure policies apply). */
  exposureReport?: NflExposureReport[];
  /** Phase 5: realized salary-left band distribution against the plan. */
  salaryBandReport?: SalaryBandReport[];
  /** Phase 5: per-lineup duplication estimate, honestly labeled by basis. */
  duplication?: LineupDuplication[];
  /** Phase 5: maximum shared players between any two lineups in the portfolio. */
  maxPairwiseOverlap?: number;
};

type ResolvedPlayer = NflOptimizerPlayer & {
  projection: number;
  resolvedSource: NflProjectionSource | "dk_avg_fallback" | "our_fallback";
  /** Phase 1: whether this player counts against the per-lineup salary-relief cap. */
  salaryRelief: boolean;
  /** Phase 1: whether this player may be used at Captain (Flex-only for overridden cheap players by default). */
  captainEligible: boolean;
};

type SolverModel = {
  optimize: "score";
  opType: "max";
  constraints: Record<string, { max?: number; min?: number; equal?: number }>;
  variables: Record<string, Record<string, number>>;
  binaries: Record<string, 1>;
};
type SolverResult = Record<string, number | boolean> & { feasible?: boolean; result?: number };

const CLASSIC_SLOTS = ["QB", "RB1", "RB2", "WR1", "WR2", "WR3", "TE", "FLEX", "DST"] as const;
const SHOWDOWN_SLOTS = ["CPT", "FLEX1", "FLEX2", "FLEX3", "FLEX4", "FLEX5"] as const;

function finite(value: number | null | undefined): number | null {
  return value != null && Number.isFinite(value) ? value : null;
}

/** Build role evidence for the punt policy from a slate player, keeping unknowns as null. */
function roleEvidenceFor(player: NflOptimizerPlayer): NflPlayerRoleEvidence {
  const observed = player.historyGames ?? null;
  return {
    playerId: player.dkPlayerId,
    verifiedActive: player.isOut ? false : null,
    availabilityState: player.availabilityState ?? "unknown",
    depthRole: player.depthRole ?? null,
    // If role confidence is not supplied but the player has observed games of
    // his own, treat observed history as weak role evidence rather than unknown.
    roleConfidence: player.roleConfidence ?? (observed != null && observed >= MIN_OBSERVED_GAMES ? 0.5 : observed === 0 ? 0 : null),
    projectedOpportunities: player.projectedOpportunities ?? null,
    opportunityUnit: null,
    observedGameCount: observed,
    sourceIds: [],
    evidenceAsOf: null,
  };
}

function projectionFor(player: NflOptimizerPlayer, settings: NflOptimizerSettings): { value: number; source: ResolvedPlayer["resolvedSource"] } | null {
  if (settings.projectionSource === "workload") {
    const candidate=selectedWorkload(player,settings.workloadPositions);
    if (candidate && finite(candidate.mean) !== null && candidate.mean > 0) return { value: candidate.mean, source: "workload" };
    if (finite(player.ourProj) !== null && player.ourProj! > 0) return { value: player.ourProj!, source: "our_fallback" };
  }
  if (settings.projectionSource === "calibrated") {
    if (player.calibrated && finite(player.calibrated.mean) !== null && player.calibrated.mean > 0) return { value: player.calibrated.mean, source: "calibrated" };
    if (finite(player.ourProj) !== null && player.ourProj! > 0) return { value: player.ourProj!, source: "our_fallback" };
  }
  const direct = settings.projectionSource === "our" ? finite(player.ourProj)
    : settings.projectionSource === "calibrated" || settings.projectionSource === "workload" ? null
    : settings.projectionSource === "dk_avg" ? finite(player.avgFptsDk)
    : settings.projectionSource === "fantasypros" ? finite(player.fantasyprosProj)
    : settings.projectionSource === "linestar" ? finite(player.linestarProj)
    : finite(player.customProj);
  if (direct != null && direct > 0) return { value: direct, source: settings.projectionSource };
  const fallback = finite(player.avgFptsDk);
  return settings.allowDkFallback && fallback != null && fallback > 0
    ? { value: fallback, source: "dk_avg_fallback" }
    : null;
}

export function resolveProjectionAudit(player:NflOptimizerPlayer,settings:NflOptimizerSettings):ProjectionAudit {
  const resolved=projectionFor(player,settings);
  if(resolved?.source==='workload'&&player.projectionAudit)return {...player.projectionAudit,excluded:player.isOut||settings.excludedPlayerIds.includes(player.dkPlayerId)};
  const baseline=finite(player.ourProj),final=resolved?.value??null,delta=baseline!=null&&final!=null?final-baseline:0;
  return {version:'nfl-projection-audit-v1',baseline,final,source:resolved?.source??'unavailable',excluded:player.isOut||settings.excludedPlayerIds.includes(player.dkPlayerId)||!resolved,modelSnapshot:resolved?.source==='calibrated'?player.calibrated:null,evidence:player.projectionAudit?.evidence??null,assumption:null,rangeMethod:resolved?.source==='calibrated'?'Pinned calibrated player ranges.':resolved?.source==='our'||resolved?.source==='our_fallback'?'Historical player ranges.':'Source supplies a mean only; optimizer uses 0.74 × mean / 1.28 × mean range heuristics.',steps:[{label:'Selected projection source',status:delta?'applied':'not_applied',points:delta,reason:resolved?`${resolved.source}: ${baseline===null?'historical baseline unavailable; no comparative delta claimed':final===baseline?'historical estimate retained':'source replacement, not an inferred injury or matchup effect'}.`:'No usable projection; excluded.'},...(player.projectionAudit?.steps.filter(s=>s.label==='Situation adjustments')??[])]};
}

function safe(value: string): string {
  return value.replace(/[^a-zA-Z0-9]/g, "_");
}

function jitter(seed: number, lineup: number, playerId: number): number {
  let value = (seed ^ (lineup * 2654435761) ^ (playerId * 1597334677)) >>> 0;
  value = Math.imul(value ^ (value >>> 16), 2246822507) >>> 0;
  return (value / 4294967295) * 2 - 1;
}

function objective(player: ResolvedPlayer, settings: NflOptimizerSettings, lineupNumber: number): number {
  const historical = player.resolvedSource === "our" || player.resolvedSource === "our_fallback";
  const base = settings.mode === "cash"
    ? (player.resolvedSource === "workload" ? selectedWorkload(player,settings.workloadPositions)!.p10 : player.resolvedSource === "calibrated" ? player.calibrated!.p10 : historical ? finite(player.floorFpts) : null) ?? player.projection * 0.74
    : (player.resolvedSource === "workload" ? selectedWorkload(player,settings.workloadPositions)!.p90 : player.resolvedSource === "calibrated" ? player.calibrated!.p90 : historical ? finite(player.ceilingFpts) : null) ?? player.projection * 1.28;
  // Phase 2: leverage (ownership penalty) applies when the server resolved it
  // as permitted — a validated feed, or a declared-heuristic feed the user
  // explicitly opted into (labeled "uncalibrated" everywhere). Missing
  // ownership is null and contributes no penalty — it is never rewarded as low
  // ownership. Without a resolved bit, only "validated" enables it.
  const leverageEnabled = settings.mode === "gpp"
    && (settings.ownershipLeverageEnabled ?? ((settings.ownershipCapability ?? "unavailable") === "validated"));
  const ownershipPenalty = leverageEnabled ? (finite(player.linestarOwnPct) ?? 0) * 0.025 : 0;
  const workload=player.resolvedSource === "workload"?selectedWorkload(player,settings.workloadPositions):null;
  const boomBonus = settings.mode === "gpp" ? (workload && "boom" in workload ? workload.boom : player.resolvedSource === "calibrated" ? player.calibrated!.boom : historical ? finite(player.boomRate) ?? 0 : 0) * 2 : 0;
  return base + boomBonus - ownershipPenalty + jitter(20260902, lineupNumber, player.dkPlayerId) * settings.randomness * player.projection;
}

function validateSettings(settings: NflOptimizerSettings): void {
  if(settings.projectionSource === "workload")validateWorkloadPositions(settings.workloadPositions);
  if (!["our", "workload", "calibrated", "dk_avg", "fantasypros", "linestar", "custom"].includes(settings.projectionSource)) throw new Error("Unknown projection source.");
  if (!Number.isInteger(settings.nLineups) || settings.nLineups < 1 || settings.nLineups > 150) throw new Error("Lineup count must be between 1 and 150.");
  if (settings.minSalary < 0 || settings.minSalary > 50000) throw new Error("Minimum salary must be between $0 and $50,000.");
  if (settings.maxExposure <= 0 || settings.maxExposure > 1) throw new Error("Maximum exposure must be greater than 0 and at most 100%.");
  const floor = settings.minPlayerSalary ?? 0;
  if (!Number.isFinite(floor) || floor < 0 || floor > 50000) throw new Error("Minimum player salary must be between $0 and $50,000.");
  if (settings.puntPolicy) validateNflPuntPolicy(settings.puntPolicy);
  const rosterSize = settings.format === "classic" ? 9 : 6;
  if (settings.minUnique < 1 || settings.minUnique > rosterSize) throw new Error(`Minimum unique players must be 1-${rosterSize}.`);
}

function buildOne(
  pool: ResolvedPlayer[],
  settings: NflOptimizerSettings,
  lineupNumber: number,
  previous: NflGeneratedLineup[],
  exposureCounts: Map<number, number>,
  forcedIds: Set<number>,
  countsById: Map<number, ExposureCounts>,
  captainCounts: Map<number, number>,
  flexCounts: Map<number, number>,
  compiled: CompiledArchetype | null = null,
): NflGeneratedLineup | null {
  // eslint-disable-next-line @typescript-eslint/no-require-imports
  const solver = require("javascript-lp-solver") as { Solve: (model: SolverModel) => SolverResult };
  const slots = settings.format === "classic" ? [...CLASSIC_SLOTS] : [...SHOWDOWN_SLOTS];
  const rosterSize = slots.length;
  // A player drops out of the pool once his OVERALL maximum is reached (locked
  // players are always eligible so a lock cannot deadlock generation).
  const maxCount = (player: ResolvedPlayer) => Math.max(forcedIds.has(player.dkPlayerId) ? 1 : 0, countsById.get(player.dkPlayerId)?.overallMax ?? settings.nLineups);
  // Phase 4: faded players are removed from the pool entirely for this lineup.
  const faded = new Set(compiled?.fadePlayerIds ?? []);
  const available = pool.filter((player) => !faded.has(player.dkPlayerId) && ((exposureCounts.get(player.dkPlayerId) ?? 0) < maxCount(player) || forcedIds.has(player.dkPlayerId)));
  // Phase 5: salary-used window. A salary policy sets an explicit min/max used;
  // salary-left is the cap minus salary used, so these bound salary left too.
  const salaryMin = settings.salaryPolicy ? Math.max(settings.salaryPolicy.minSalaryUsed, NFL_SALARY_CAP - settings.salaryPolicy.maxSalaryLeft) : settings.minSalary;
  const salaryMax = settings.salaryPolicy ? Math.min(settings.salaryPolicy.maxSalaryUsed, NFL_SALARY_CAP - settings.salaryPolicy.minSalaryLeft) : NFL_SALARY_CAP;
  // Slot capacity: whether a player may still take a Captain or a Flex slot in
  // THIS lineup given his per-slot maxima already spent (P3-AC1/AC2).
  const captainFull = (player: ResolvedPlayer) => (captainCounts.get(player.dkPlayerId) ?? 0) >= (countsById.get(player.dkPlayerId)?.captainMax ?? settings.nLineups);
  const flexFull = (player: ResolvedPlayer) => (flexCounts.get(player.dkPlayerId) ?? 0) >= (countsById.get(player.dkPlayerId)?.flexMax ?? settings.nLineups);
  const constraints: SolverModel["constraints"] = { salary: { max: salaryMax, min: salaryMin } };
  if (settings.format === "classic") {
    constraints.roster = { equal: 9 };
    constraints.qb = { equal: 1 };
    constraints.rb = { min: 2, max: 3 };
    constraints.wr = { min: 3, max: 4 };
    constraints.te = { min: 1, max: 2 };
    constraints.dst = { equal: 1 };
    constraints.flex = { equal: 7 };
  } else {
    constraints.cpt = { equal: 1 };
    constraints.flex = { equal: 5 };
  }
  for (const player of available) constraints[`player_${player.dkPlayerId}`] = { max: 1 };
  for (const game of new Set(available.map((player) => player.gameKey).filter(Boolean))) {
    constraints[`game_${safe(game!)}`] = { max: settings.format === "classic" ? 8 : 6 };
  }
  if (settings.format === "showdown") {
    for (const team of new Set(available.map((player) => player.team))) constraints[`team_${safe(team)}`] = { max: 5 };
  }
  // Phase 1 (P1-AC4): the salary-relief cap is a lineup CONSTRAINT, not a
  // post-generation filter. At most this many cheap salary-relief players may
  // appear in any single lineup.
  if (settings.puntPolicy && available.some((player) => player.salaryRelief)) {
    constraints.salary_relief = { max: settings.puntPolicy.maxSalaryReliefPlayersPerLineup };
  }
  // Phase 4: archetype constraints. Team-count skew (favorite onslaught), K/DST
  // presence (low-scoring), and beneficiary minimums (fades' alternate paths).
  // The range applies to the team the ARCHETYPE compiled it for (favorite for
  // onslaught, underdog for comeback) — never implicitly to settings.favoriteTeam.
  const archetypeTeam = compiled?.teamCountRange?.team ?? null;
  if (compiled?.teamCountRange && settings.format === "showdown") {
    if (available.some((p) => p.team === archetypeTeam)) {
      constraints[`arch_team_${safe(archetypeTeam!)}`] = { min: compiled.teamCountRange.min, max: compiled.teamCountRange.max };
    } else if (compiled.teamCountRange.min > 0) {
      // The archetype's required team has no available players: the lineup
      // cannot honor its label. Refuse rather than mislabel a generic lineup.
      return null;
    }
  }
  if (compiled?.minKickerDst) {
    if (available.some((p) => p.position === "K" || p.position === "DST")) {
      constraints.arch_kdst = { min: compiled.minKickerDst };
    }
  }
  const beneficiaryConstraints = (compiled?.beneficiaries ?? []).map((group, index) => ({ key: `arch_benef_${index}`, group }));
  for (const { key, group } of beneficiaryConstraints) {
    if (available.some((p) => group.playerIds.includes(p.dkPlayerId))) constraints[key] = { min: group.minFromGroup };
  }
  for (const playerId of forcedIds) constraints[`force_${playerId}`] = { equal: 1 };
  // Phase 5: the shared-player cap between any two lineups is the stricter of the
  // min-unique rule and an explicit maxPairwiseOverlap. Exact duplicates are
  // impossible because at least one player must differ (overlap < rosterSize).
  const overlapCap = Math.min(rosterSize - settings.minUnique, settings.maxPairwiseOverlap ?? rosterSize, rosterSize - 1);
  previous.forEach((lineup, index) => { constraints[`prior_${index}`] = { max: overlapCap }; });

  if (settings.format === "classic" && settings.mode === "gpp" && settings.stackPassCatchers > 0) {
    for (const quarterback of available.filter((player) => player.position === "QB")) {
      constraints[`stack_${quarterback.dkPlayerId}`] = { min: 0 };
      if (settings.bringBack) constraints[`bring_${quarterback.dkPlayerId}`] = { min: 0 };
      constraints[`anti_${quarterback.dkPlayerId}`] = { max: 1 };
    }
  }

  const variables: SolverModel["variables"] = {};
  const binaries: SolverModel["binaries"] = {};
  for (const player of available) {
    const purchaseTypes = settings.format === "classic" ? ["CLASSIC"] : ["CPT", "FLEX"];
    for (const purchaseType of purchaseTypes) {
      if (purchaseType === "CPT" && (player.captainDkPlayerId == null || player.captainSalary == null)) continue;
      // Phase 1 (P1-AC1/§8.3): a cheap player admitted only by override is
      // Flex-only unless a Captain override is recorded. Skip his CPT variable.
      if (purchaseType === "CPT" && !player.captainEligible) continue;
      // Phase 3: skip a slot variable once that slot's per-player maximum is
      // spent, so Captain and Flex maxima are enforced independently.
      if (purchaseType === "CPT" && captainFull(player)) continue;
      if (purchaseType === "FLEX" && flexFull(player)) continue;
      // Phase 4: archetype captain restrictions (contrarian / underdog captain
      // sets, forbidden chalk captains).
      if (purchaseType === "CPT" && compiled?.eligibleCaptainIds && !compiled.eligibleCaptainIds.includes(player.dkPlayerId)) continue;
      if (purchaseType === "CPT" && compiled?.forbiddenCaptainIds.includes(player.dkPlayerId)) continue;
      const key = `${purchaseType === "CLASSIC" ? "x" : purchaseType === "CPT" ? "c" : "f"}_${player.dkPlayerId}`;
      const slot = purchaseType === "CPT" ? "CPT" : purchaseType === "FLEX" ? "FLEX" : "CLASSIC";
      const multiplier = slot === "CPT" ? 1.5 : 1;
      const salary = slot === "CPT" ? player.captainSalary! : player.salary;
      const variable: Record<string, number> = {
        score: objective(player, settings, lineupNumber) * multiplier,
        salary,
        [`player_${player.dkPlayerId}`]: 1,
      };
      // Count this pick against the per-lineup salary-relief cap.
      if (settings.puntPolicy && player.salaryRelief) variable.salary_relief = 1;
      if (settings.format === "classic") {
        variable.roster = 1;
        variable[player.position.toLowerCase()] = 1;
        if (["RB", "WR", "TE"].includes(player.position)) variable.flex = 1;
      } else {
        variable[slot === "CPT" ? "cpt" : "flex"] = 1;
      }
      if (settings.format === "showdown") variable[`team_${safe(player.team)}`] = 1;
      if (player.gameKey) variable[`game_${safe(player.gameKey)}`] = 1;
      // Phase 4: archetype constraint coefficients.
      if (compiled) {
        if (archetypeTeam && constraints[`arch_team_${safe(archetypeTeam)}`] && player.team === archetypeTeam) variable[`arch_team_${safe(archetypeTeam)}`] = 1;
        if (constraints.arch_kdst && (player.position === "K" || player.position === "DST")) variable.arch_kdst = 1;
        for (const { key, group } of beneficiaryConstraints) {
          if (constraints[key] && group.playerIds.includes(player.dkPlayerId)) variable[key] = 1;
        }
      }
      if (forcedIds.has(player.dkPlayerId)) variable[`force_${player.dkPlayerId}`] = 1;
      previous.forEach((lineup, index) => { if (lineup.playerIds.includes(player.dkPlayerId)) variable[`prior_${index}`] = 1; });
      if (settings.format === "classic" && settings.mode === "gpp" && settings.stackPassCatchers > 0) {
        for (const quarterback of available.filter((candidate) => candidate.position === "QB")) {
          if (player.dkPlayerId === quarterback.dkPlayerId) {
            variable[`stack_${quarterback.dkPlayerId}`] = -settings.stackPassCatchers;
            if (settings.bringBack) variable[`bring_${quarterback.dkPlayerId}`] = -1;
            variable[`anti_${quarterback.dkPlayerId}`] = 1;
          }
          if (["WR", "TE"].includes(player.position) && player.team === quarterback.team) variable[`stack_${quarterback.dkPlayerId}`] = 1;
          if (settings.bringBack && ["RB", "WR", "TE"].includes(player.position) && player.team === quarterback.opponent) variable[`bring_${quarterback.dkPlayerId}`] = 1;
          if (player.position === "DST" && player.team === quarterback.opponent) variable[`anti_${quarterback.dkPlayerId}`] = 1;
        }
      }
      variables[key] = variable;
      binaries[key] = 1;
    }
  }
  const solved = solver.Solve({ optimize: "score", opType: "max", constraints, variables, binaries });
  if (solved.feasible === false) return null;
  const purchases: { player: ResolvedPlayer; slot: "CPT" | "FLEX" | "CLASSIC" }[] = [];
  for (const [key, raw] of Object.entries(solved)) {
    if (!/^[xcf]_/.test(key) || typeof raw !== "number" || raw < 0.5) continue;
    const match = key.match(/^([xcf])_(\d+)$/);
    if (!match) continue;
    const player = available.find((candidate) => candidate.dkPlayerId === Number(match[2]));
    if (!player) continue;
    const slot = match[1] === "c" ? "CPT" : match[1] === "f" ? "FLEX" : "CLASSIC";
    purchases.push({ player, slot });
  }
  const chosen: NflLineupSlot[] = [];
  if (settings.format === "showdown") {
    let flexIndex = 0;
    for (const purchase of purchases) {
      const slot = purchase.slot === "CPT" ? "CPT" : `FLEX${++flexIndex}`;
      const multiplier = purchase.slot === "CPT" ? 1.5 : 1;
      chosen.push({ slot, player: purchase.player, salary: purchase.slot === "CPT" ? purchase.player.captainSalary! : purchase.player.salary, multiplier, projection: purchase.player.projection * multiplier, projectionSource: purchase.player.resolvedSource });
    }
  } else {
    const byPosition = (position: NflOptimizerPlayer["position"]) => purchases.filter((entry) => entry.player.position === position).map((entry) => entry.player);
    const qb = byPosition("QB"), rb = byPosition("RB"), wr = byPosition("WR"), te = byPosition("TE"), dst = byPosition("DST");
    const assigned = new Set<number>();
    const assign = (slot: string, player: ResolvedPlayer) => { assigned.add(player.dkPlayerId); chosen.push({ slot, player, salary: player.salary, multiplier: 1, projection: player.projection, projectionSource: player.resolvedSource }); };
    assign("QB", qb[0]); assign("RB1", rb[0]); assign("RB2", rb[1]); assign("WR1", wr[0]); assign("WR2", wr[1]); assign("WR3", wr[2]); assign("TE", te[0]); assign("DST", dst[0]);
    const flex = purchases.map((entry) => entry.player).find((player) => !assigned.has(player.dkPlayerId));
    if (!flex) return null;
    assign("FLEX", flex);
  }
  const slotOrder = new Map<string, number>(slots.map((slot, index) => [slot, index]));
  chosen.sort((a, b) => (slotOrder.get(a.slot) ?? 99) - (slotOrder.get(b.slot) ?? 99));
  if (chosen.length !== rosterSize) return null;
  const qb = chosen.find((entry) => entry.player.position === "QB")?.player ?? null;
  const passCatchers = qb ? chosen.filter((entry) => ["WR", "TE"].includes(entry.player.position) && entry.player.team === qb.team).map((entry) => entry.player.name) : [];
  const bringBack = qb ? chosen.find((entry) => ["RB", "WR", "TE"].includes(entry.player.position) && entry.player.team === qb.opponent)?.player.name ?? null : null;
  return {
    lineupNumber,
    slots: chosen,
    playerIds: chosen.map((entry) => entry.player.dkPlayerId),
    totalSalary: chosen.reduce((sum, entry) => sum + entry.salary, 0),
    projectedFpts: chosen.reduce((sum, entry) => sum + entry.projection, 0),
    floorFpts: chosen.reduce((sum, entry) => sum + (entry.projectionSource === "workload" ? selectedWorkload(entry.player,settings.workloadPositions)!.p10 : entry.projectionSource === "calibrated" ? entry.player.calibrated!.p10 : entry.projectionSource === "our" || entry.projectionSource === "our_fallback" ? entry.player.floorFpts ?? entry.projection / entry.multiplier * .74 : entry.projection / entry.multiplier * .74) * entry.multiplier, 0),
    ceilingFpts: chosen.reduce((sum, entry) => sum + (entry.projectionSource === "workload" ? selectedWorkload(entry.player,settings.workloadPositions)!.p90 : entry.projectionSource === "calibrated" ? entry.player.calibrated!.p90 : entry.projectionSource === "our" || entry.projectionSource === "our_fallback" ? entry.player.ceilingFpts ?? entry.projection / entry.multiplier * 1.28 : entry.projection / entry.multiplier * 1.28) * entry.multiplier, 0),
    projectedOwnership: chosen.some((entry) => entry.player.linestarOwnPct != null)
      ? chosen.reduce((sum, entry) => sum + (entry.player.linestarOwnPct ?? 0), 0)
      : null,
    stackSummary: { quarterback: qb?.name ?? null, passCatchers, bringBack },
    // §11.4: every selected lineup carries exactly one primary archetype label.
    // With no compiled archetype it is Standard ceiling — never a fade.
    archetype: compiled ? {
      id: compiled.archetypeId,
      label: ARCHETYPE_LABELS[compiled.archetypeId],
      fadedPlayerIds: compiled.fadePlayerIds,
      fadedPlayerNames: compiled.fadePlayerIds.map((id) => pool.find((p) => p.dkPlayerId === id)?.name ?? `#${id}`),
      // A beneficiary rule is "satisfied" when the lineup actually contains the
      // required players (P4-AC2). This is checked, not assumed.
      beneficiariesSatisfied: compiled.beneficiaries
        .filter((group) => chosen.filter((entry) => group.playerIds.includes(entry.player.dkPlayerId)).length >= group.minFromGroup)
        .map((group) => group.label),
    } : { id: "standard_ceiling", label: ARCHETYPE_LABELS.standard_ceiling, fadedPlayerIds: [], fadedPlayerNames: [], beneficiariesSatisfied: [] },
  };
}

export function optimizeNflLineups(players: NflOptimizerPlayer[], settings: NflOptimizerSettings): NflOptimizerResult {
  validateSettings(settings);
  const excluded = new Set(settings.excludedPlayerIds);
  const locked = new Set(settings.lockedPlayerIds);
  const policy = settings.puntPolicy;
  const overrides = settings.puntOverrides ?? [];
  // Legacy fallbacks (branch reconciliation): a bare minPlayerSalary floor and
  // observed-history gate still work when no full policy is supplied. Locked and
  // explicit-exposure players are the user's own instruction and outrank both.
  const salaryFloor = settings.minPlayerSalary ?? 0;
  const floorExempt = new Set([...settings.lockedPlayerIds,
    ...Object.entries(settings.minExposureByPlayer).filter(([, target]) => target > 0).map(([id]) => Number(id))]);

  const coverage = { requested: players.length, direct: 0, fallback: 0, excluded: 0 };
  const pool: ResolvedPlayer[] = [];
  const eligibility: NflEligibilityDecision[] = [];
  const warnings: string[] = [];
  let belowSalaryFloor = 0;
  let withoutHistory = 0;
  let puntBlocked = 0;

  for (const player of players) {
    const named = { dkPlayerId: player.dkPlayerId, name: player.name, salary: player.salary };
    // Manual exclusion and inactivity are handled first so they always win.
    if (player.isOut) {
      eligibility.push({ ...named, eligible: false, salaryRelief: false, captainEligible: false, overridden: false, reason: "Inactive (OUT/IR).", reasonCode: "INACTIVE" });
      coverage.excluded++; continue;
    }
    if (excluded.has(player.dkPlayerId)) {
      eligibility.push({ ...named, eligible: false, salaryRelief: false, captainEligible: false, overridden: false, reason: "Manually excluded for this run.", reasonCode: "MANUAL_EXCLUSION" });
      coverage.excluded++; continue;
    }

    let salaryRelief = false;
    let captainEligible = true;
    let overridden = false;

    if (policy) {
      // Role-aware no-punt policy (spec §8) is the single eligibility authority
      // when present. It fully supersedes the bare salary floor.
      const decision = evaluatePuntEligibility(player, roleEvidenceFor(player), policy, overrides);
      if (!decision.eligible) {
        eligibility.push({ ...named, eligible: false, salaryRelief: false, captainEligible: false, overridden: false, reason: decision.detail, reasonCode: decision.reason });
        // A locked player who is ineligible must produce a readable error, not a
        // silent drop that yields a confusing zero-lineup failure (P1-AC5).
        if (locked.has(player.dkPlayerId)) {
          throw new Error(`${player.name} is locked but ineligible under the ${policy.mode} punt policy: ${decision.detail} Allow this player for the run, raise the policy thresholds, or remove the lock.`);
        }
        puntBlocked++; coverage.excluded++; continue;
      }
      salaryRelief = decision.salaryRelief;
      overridden = decision.overridden;
      // A cheap player admitted only by override is Flex-only unless a CPT
      // override is recorded (spec §8.3).
      captainEligible = !overridden || captainAdmissible(player.dkPlayerId, overrides);
    } else {
      // Legacy path: bare salary floor + observed-history gate.
      if (salaryFloor > 0 && player.salary < salaryFloor && !floorExempt.has(player.dkPlayerId)) {
        eligibility.push({ ...named, eligible: false, salaryRelief: false, captainEligible: false, overridden: false, reason: `Below the $${salaryFloor.toLocaleString()} per-player salary floor.`, reasonCode: "ABSOLUTE_SALARY_BLOCK" });
        belowSalaryFloor++; coverage.excluded++; continue;
      }
      if (settings.requireObservedHistory && !hasObservedOpportunity(player) && !floorExempt.has(player.dkPlayerId)) {
        eligibility.push({ ...named, eligible: false, salaryRelief: false, captainEligible: false, overridden: false, reason: `Fewer than ${MIN_OBSERVED_GAMES} observed games; projection is a position average.`, reasonCode: "ROLE_UNKNOWN" });
        withoutHistory++; coverage.excluded++; continue;
      }
    }

    const resolved = projectionFor(player, settings);
    if (!resolved) {
      eligibility.push({ ...named, eligible: false, salaryRelief: false, captainEligible: false, overridden, reason: "No usable projection in the selected source.", reasonCode: "NO_PROJECTED_OPPORTUNITY" });
      coverage.excluded++; continue;
    }
    if (resolved.source === "dk_avg_fallback" || resolved.source === "our_fallback") coverage.fallback++; else coverage.direct++;
    eligibility.push({ ...named, eligible: true, salaryRelief, captainEligible, overridden, reason: null, reasonCode: null });
    pool.push({ ...player, projectionAudit:resolveProjectionAudit(player,settings), projection: resolved.value, resolvedSource: resolved.source, salaryRelief, captainEligible });
  }

  for (const [rawId, target] of Object.entries(settings.minExposureByPlayer)) {
    if (target > 0 && !pool.some((player) => player.dkPlayerId === Number(rawId))) {
      const named = players.find((player) => player.dkPlayerId === Number(rawId));
      throw new Error(`${named?.name ?? `Player ${rawId}`} has a target exposure but is unavailable in the selected projection source.`);
    }
  }
  if (puntBlocked) warnings.push(`${puntBlocked} player(s) blocked by the ${policy!.mode} punt policy. See the cheap-player review for the reason on each; allow a player for the run to keep him.`);
  if (withoutHistory) warnings.push(`${withoutHistory} player(s) with fewer than ${MIN_OBSERVED_GAMES} games of their own were removed: their projection is their position's average, not theirs. Lock a player to keep him regardless.`);
  if (belowSalaryFloor) warnings.push(`${belowSalaryFloor} player(s) priced under the $${salaryFloor.toLocaleString()} per-player salary floor were removed. Lock a player to keep him regardless.`);
  const missingTails = pool.filter(p => (p.resolvedSource === 'our' || p.resolvedSource === 'our_fallback')
    && (finite(p.floorFpts) === null || finite(p.ceilingFpts) === null)).length;
  if (missingTails) warnings.push(`${missingTails} historical-source players have no usable scenario distribution. Search uses 0.74×/1.28× point-estimate heuristics for missing lower/upper tails; these are not simulated percentiles. Missing boom rates receive no boom bonus.`);
  const dkFallback = pool.filter(p => p.resolvedSource === "dk_avg_fallback").length;
  const ourFallback = pool.filter(p => p.resolvedSource === "our_fallback").length;
  if (dkFallback) warnings.push(`${dkFallback} players used DK Avg fallback.`);
  if (ourFallback) warnings.push(`${ourFallback} players retained historical baseline projections.`);
  if (settings.projectionSource === "calibrated") {
    if (!coverage.direct) throw new Error("No qualified pregame calibrated projections are available. Refresh forecasts or choose the historical model.");
    warnings.push("Calibrated QB/DST is experimental; forward validation is pending.");
  }
  if (settings.projectionSource === "workload") {
    if (!coverage.direct) throw new Error("No eligible pregame forecasts for the enabled workload positions. Refresh the workload snapshot or select the historical source.");
    const counts=WORKLOAD_POSITIONS.map(pos=>`${pos}: ${pool.filter(p=>p.resolvedSource==='workload'&&p.position===pos).length}`).join(', ');
    warnings.push(`Workload coverage — ${counts}. Other players retain disclosed fallback. RB/WR/TE candidate ranges worsened historical interval scores. Situation effects, when enabled, are listed in each player audit; WR has no invented boom bonus.`);
  }
  warnings.push("Lineup floor/ceiling sums are player-level search heuristics, not lineup P10/P90. Use Scenario Lab for joint distributions.");

  // Phase 3: resolve the effective exposure policy per pooled player. An
  // explicit per-player policy wins; otherwise fall back to the flat global
  // maxExposure and any legacy min/maxExposureByPlayer target.
  const policyById = new Map((settings.exposurePolicies ?? []).map((p) => [p.playerId, p]));
  const effectivePolicy = (player: ResolvedPlayer): PlayerExposurePolicy => {
    const explicit = policyById.get(player.dkPlayerId);
    if (explicit) return explicit;
    const legacyMax = settings.maxExposureByPlayer[String(player.dkPlayerId)] ?? settings.maxExposure;
    const legacyMin = settings.minExposureByPlayer[String(player.dkPlayerId)] ?? null;
    return {
      playerId: player.dkPlayerId,
      overall: { minPct: legacyMin, maxPct: legacyMax },
      captain: { minPct: null, maxPct: null },
      flex: { minPct: null, maxPct: null },
      exactTargetMode: false,
    };
  };
  const exposurePolicies = settings.exposurePolicies?.length ? settings.exposurePolicies : null;
  const countsById = new Map<number, ExposureCounts>(pool.map((p) => {
    const counts = deriveExposureCounts(effectivePolicy(p), settings.nLineups);
    // The GLOBAL flat max-exposure default always permits at least one
    // appearance (floor(maxExposure × n) can round to 0 for small n — with 1
    // lineup at 60% max, every player would vanish and generation returns
    // nothing). An explicit per-player policy or per-player max override of 0
    // is the user's own instruction and is honored exactly.
    const hasExplicit = policyById.has(p.dkPlayerId) || settings.maxExposureByPlayer[String(p.dkPlayerId)] != null;
    if (!hasExplicit) counts.overallMax = Math.max(1, counts.overallMax);
    return [p.dkPlayerId, counts];
  }));

  if (settings.salaryPolicy) validateSalaryPolicy(settings.salaryPolicy);

  // P3-AC4: reject impossible plans BEFORE generation, naming the conflicts.
  if (exposurePolicies) {
    for (const p of exposurePolicies) validateExposurePolicy(p);
    const captainEligibleIds = new Set(pool.filter((p) => p.captainEligible).map((p) => p.dkPlayerId));
    const problems = detectExposureInfeasibility(
      pool.map((p) => effectivePolicy(p)),
      settings.nLineups,
      settings.format === "showdown" ? captainEligibleIds : new Set(),
    );
    if (problems.length) {
      throw new Error(`Exposure plan is infeasible before generation:\n${problems.map((x) => `• ${x.detail}`).join("\n")}`);
    }
  }

  // Phase 4: build the per-archetype generation plan. Allocate the requested
  // lineup count across enabled quotas, then compile each archetype once.
  const archetypeQuotas = settings.archetypeQuotas?.filter((q) => q.enabled);
  const archetypeContext: ArchetypeSlateContext = {
    players: pool.map((p) => ({ dkPlayerId: p.dkPlayerId, position: p.position, team: p.team, opponent: p.opponent, ownership: finite(p.linestarOwnPct) != null ? (p.linestarOwnPct as number) / 100 : null, captainEligible: p.captainEligible })),
    favoriteTeam: settings.favoriteTeam ?? null,
    underdogTeam: settings.underdogTeam ?? null,
    ownershipValidated: (settings.ownershipCapability ?? "unavailable") === "validated",
  };
  let plan: Array<{ archetypeId: ArchetypeId; compiled: CompiledArchetype | null }> = [];
  if (archetypeQuotas?.length) {
    const allocation = allocateArchetypeQuotas(settings.archetypeQuotas!, settings.nLineups);
    if (!allocation.ok) throw new Error(`Archetype plan is infeasible: ${allocation.reason}`);
    for (const slot of allocation.allocation) {
      const compiled = compileArchetype(slot.archetypeId, archetypeContext, settings.archetypeConfigs?.[slot.archetypeId] ?? {});
      // A fade with no satisfiable beneficiary is rejected up front (§11.2).
      if (isFadeArchetype(slot.archetypeId) && !compiled.beneficiaries.length) {
        throw new Error(`${ARCHETYPE_LABELS[slot.archetypeId]} declared no beneficiary path. A fade must specify at least one alternate scoring route.`);
      }
      for (let k = 0; k < slot.count; k++) plan.push({ archetypeId: slot.archetypeId, compiled });
    }
  } else {
    plan = Array.from({ length: settings.nLineups }, () => ({ archetypeId: "standard_ceiling" as ArchetypeId, compiled: null }));
  }

  const exposureCounts = new Map<number, number>();
  const captainCounts = new Map<number, number>();
  const flexCounts = new Map<number, number>();
  const lineups: NflGeneratedLineup[] = [];
  for (let lineupNumber = 1; lineupNumber <= plan.length; lineupNumber++) {
    const remaining = plan.length - lineupNumber + 1;
    const forced = new Set(locked);
    for (const player of pool) {
      // Force a player in when his remaining overall-minimum need equals the
      // remaining lineups. Uses the derived overall minimum (policy or legacy).
      const target = countsById.get(player.dkPlayerId)!.overallMin;
      const current = exposureCounts.get(player.dkPlayerId) ?? 0;
      if (target - current >= remaining) forced.add(player.dkPlayerId);
    }
    const lineup = buildOne(pool, settings, lineupNumber, lineups, exposureCounts, forced, countsById, captainCounts, flexCounts, plan[lineupNumber - 1].compiled);
    if (!lineup) {
      warnings.push(`Stopped after ${lineups.length} lineup(s): the ${ARCHETYPE_LABELS[plan[lineupNumber - 1].archetypeId]} quota or remaining exposure/uniqueness/salary constraints are infeasible.`);
      break;
    }
    lineups.push(lineup);
    // Track overall and slot-specific appearances. Captain and Flex are counted
    // independently; overall is their union (each player appears once per lineup).
    lineup.playerIds.forEach((id) => exposureCounts.set(id, (exposureCounts.get(id) ?? 0) + 1));
    for (const slot of lineup.slots) {
      const id = slot.player.dkPlayerId;
      if (slot.slot === "CPT") captainCounts.set(id, (captainCounts.get(id) ?? 0) + 1);
      else flexCounts.set(id, (flexCounts.get(id) ?? 0) + 1);
    }
  }

  // Report realized vs requested per slot and flag missed minimums (P3-AC2/AC5).
  const exposureReport: NflExposureReport[] = pool.map((player) => {
    const c = countsById.get(player.dkPlayerId)!;
    const overall = exposureCounts.get(player.dkPlayerId) ?? 0;
    const captain = captainCounts.get(player.dkPlayerId) ?? 0;
    const flex = flexCounts.get(player.dkPlayerId) ?? 0;
    let binding: string | null = null;
    if (overall < c.overallMin) binding = `overall min missed (${overall}/${c.overallMin})`;
    else if (settings.format === "showdown" && captain < c.captainMin) binding = `captain min missed (${captain}/${c.captainMin})`;
    else if (settings.format === "showdown" && flex < c.flexMin) binding = `flex min missed (${flex}/${c.flexMin})`;
    else if (overall >= c.overallMax) binding = `overall max reached (${overall}/${c.overallMax})`;
    if (binding && (overall < c.overallMin || (settings.format === "showdown" && (captain < c.captainMin || flex < c.flexMin)))) {
      warnings.push(`${player.name}: ${binding}; constraints were infeasible for the remaining lineups.`);
    }
    return { dkPlayerId: player.dkPlayerId, name: player.name, overall, captain, flex,
      overallMin: c.overallMin, overallMax: c.overallMax, captainMin: c.captainMin, captainMax: c.captainMax, flexMin: c.flexMin, flexMax: c.flexMax, binding };
  });

  // Phase 5: salary-band distribution, exact-duplicate/overlap check, and
  // capability-gated duplication estimate.
  const salaryBandReport = settings.salaryPolicy
    ? reportSalaryBands(settings.salaryPolicy, lineups.map((l) => NFL_SALARY_CAP - l.totalSalary))
    : undefined;
  if (salaryBandReport) {
    for (const b of salaryBandReport) {
      if (!b.withinPlan) warnings.push(`Salary-left band $${b.band.min.toLocaleString()}–$${b.band.max.toLocaleString()}: ${b.count} lineup(s) (plan ${b.minCount}–${b.maxCount}).`);
    }
  }
  const exactDuplicates = findExactDuplicates(lineups);
  if (exactDuplicates.length) warnings.push(`${exactDuplicates.length} exact-duplicate lineup pair(s) detected — this should not happen; report the run.`);
  const overlap = computeMaxOverlap(lineups);
  const ownershipValidated = (settings.ownershipCapability ?? "unavailable") === "validated";
  const ownershipByPlayer = new Map(pool.filter((p) => finite(p.linestarOwnPct) != null).map((p) => [p.dkPlayerId, (p.linestarOwnPct as number) / 100]));
  const duplication = ownershipByPlayer.size
    ? estimateDuplication(lineups.map((l) => ({ lineupNumber: l.lineupNumber, playerIds: l.playerIds, totalSalary: l.totalSalary })), { ownershipValidated, ownershipByPlayer })
    : undefined;

  return { lineups, warnings, sourceCoverage: coverage, eligibility, exposureReport, salaryBandReport, duplication, maxPairwiseOverlap: overlap };
}
