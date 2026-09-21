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
  // Phase 2: leverage (ownership penalty) only applies when ownership is
  // validated. Missing ownership is null and contributes no penalty — it is
  // never rewarded as low ownership. When capability is not validated the
  // penalty is disabled entirely so the run is honestly projection-only.
  const leverageEnabled = settings.mode === "gpp" && (settings.ownershipCapability ?? "unavailable") === "validated";
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
): NflGeneratedLineup | null {
  // eslint-disable-next-line @typescript-eslint/no-require-imports
  const solver = require("javascript-lp-solver") as { Solve: (model: SolverModel) => SolverResult };
  const slots = settings.format === "classic" ? [...CLASSIC_SLOTS] : [...SHOWDOWN_SLOTS];
  const rosterSize = slots.length;
  const maxCount = (player: ResolvedPlayer) => {
    const override = settings.maxExposureByPlayer[String(player.dkPlayerId)];
    return override == null
      ? Math.max(1, Math.floor(settings.maxExposure * settings.nLineups + 1e-9))
      : Math.max(0, Math.floor(override * settings.nLineups + 1e-9));
  };
  const available = pool.filter((player) => (exposureCounts.get(player.dkPlayerId) ?? 0) < maxCount(player));
  const constraints: SolverModel["constraints"] = { salary: { max: 50000, min: settings.minSalary } };
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
  for (const playerId of forcedIds) constraints[`force_${playerId}`] = { equal: 1 };
  previous.forEach((lineup, index) => { constraints[`prior_${index}`] = { max: rosterSize - settings.minUnique }; });

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
  const exposureCounts = new Map<number, number>();
  const lineups: NflGeneratedLineup[] = [];
  for (let lineupNumber = 1; lineupNumber <= settings.nLineups; lineupNumber++) {
    const remaining = settings.nLineups - lineupNumber + 1;
    const forced = new Set(locked);
    for (const player of pool) {
      const minPct = settings.minExposureByPlayer[String(player.dkPlayerId)] ?? 0;
      const target = Math.ceil(minPct * settings.nLineups - 1e-9);
      const current = exposureCounts.get(player.dkPlayerId) ?? 0;
      if (target - current >= remaining) forced.add(player.dkPlayerId);
    }
    const lineup = buildOne(pool, settings, lineupNumber, lineups, exposureCounts, forced);
    if (!lineup) {
      warnings.push(`Stopped after ${lineups.length} lineup(s): remaining exposure, uniqueness, salary, or stacking constraints are infeasible.`);
      break;
    }
    lineups.push(lineup);
    lineup.playerIds.forEach((id) => exposureCounts.set(id, (exposureCounts.get(id) ?? 0) + 1));
  }
  for (const player of pool) {
    const target = Math.ceil((settings.minExposureByPlayer[String(player.dkPlayerId)] ?? 0) * settings.nLineups - 1e-9);
    const actual = exposureCounts.get(player.dkPlayerId) ?? 0;
    if (actual < target) warnings.push(`${player.name} minimum exposure missed (${actual}/${target}); constraints were infeasible.`);
  }
  return { lineups, warnings, sourceCoverage: coverage, eligibility };
}
