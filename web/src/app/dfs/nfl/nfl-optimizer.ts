import "server-only";
import type { DefensiveForecastBundle, DefensiveSettings } from '@/lib/nfl-dfs/defensive-projection';
import { captureProfileFor } from '@/lib/nfl-dfs/defensive-display';
import { assertShowdownLineup, showdownSalary, showdownFlexEligible } from '@/lib/nfl-dfs/showdown-legality';
import {selectedWorkload,validateWorkloadPositions,WORKLOAD_POSITIONS,type WorkloadPositions} from "@/lib/nfl-dfs/workload-selection";
import type { WorkloadProjection } from "@/lib/nfl-dfs/workload-projection";
import type { CalibratedProjection } from "@/lib/nfl-dfs/calibrated-projection";
import type { SituationSettings, ProjectionAudit, SituationEvidence } from '@/lib/nfl-dfs/projection-audit';
import { MIN_OBSERVED_GAMES, observedHistoryRequirement } from '@/lib/nfl-dfs/opportunity-redistribution';
import { OUT_PROJECTION_STATUS } from '@/lib/nfl-dfs/out-projection';
import { deriveReceiverRoleChanges } from '@/lib/nfl-dfs/receiver-role';
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
  balancedArchetypePlan,
  chalkLeveragePlan,
  chalkCaptainPolicy,
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
import { nflOverlapCap } from '@/lib/nfl-dfs/pre-export-qa';
import { isNflGppSignalPlayer, type NflPlayerSignal, type NflPlayerSignalCode } from '@/lib/nfl-dfs/player-signals';
import type { NflAirMatchupEvidence } from '@/lib/nfl-dfs/air-matchup-evidence';

// Record the strict Showdown purchase and completed-roster validation.
// v8: DK-average fallback honoured in defensive mode; a lock or exposure
// minimum on a player outside the pool is an error instead of being dropped.
export const NFL_OPTIMIZER_VERSION = "nfl-dfs-ilp-v8-defensive-dk-fallback";

export type NflProjectionSource = "our" | "workload" | "calibrated" | "dk_avg" | "fantasypros" | "linestar" | "custom";
export type NflOptimizerMode = "cash" | "gpp";
export type NflSlateFormat = "classic" | "showdown";

export type NflOptimizerPlayer = {
  playerSignals?: NflPlayerSignal[];
  airMatchupEvidence?: NflAirMatchupEvidence | null;
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
  rosterPositions?: string[];
  isOut: boolean;
  projectionStatus: string;
  /** The player's OWN games behind his projection. 0 means the number is his position's average, not his. */
  historyGames?: number | null;
  /**
   * Completed games the player's TEAM has this season. Caps the observed-
   * history requirement so a rookie who has played every available week is
   * not blocked in week 2 (min 1 game always required). Null = unknown, and
   * the flat MIN_OBSERVED_GAMES requirement applies.
   */
  teamSeasonGames?: number | null;
  /** Depth-chart role label when known; null is unknown, not "no role". */
  depthRole?: string | null;
  /** 0..1 role confidence when known; null is unknown, not zero. */
  roleConfidence?: number | null;
  /** Projected opportunities (touches/targets/etc); null is unknown, not zero. */
  projectedOpportunities?: number | null;
  /** Availability evidence freshness, used to fail cheap players closed on stale data. */
  availabilityState?: EvidenceState;
  /**
   * Resolved availability status ("ACTIVE", "QUESTIONABLE", ...). Null/absent
   * is unknown, not healthy. Gates the Captain slot only; see
   * `captainBlockedByAvailability`.
   */
  availabilityStatus?: string | null;
  /** The resolved availability; only its block reason is read here, to say why a player is out. */
  availability?: { blockedReason?: string | null; status?: string | null } | null;
  /**
   * DraftKings' own Status column, verbatim ("Q", "D", "OUT", "IR", ...).
   * `dk-salary-csv.ts` maps only OUT/IR-family codes to `isOut` and leaves the
   * risk codes here on purpose, because which risk is acceptable is a
   * contest-type judgement that belongs to the optimizer. See `listedDoubtful`.
   */
  dkStatus?: string | null;
  ourProj: number | null;
  floorFpts: number | null;
  ceilingFpts: number | null;
  boomRate: number | null;
  avgFptsDk: number | null;
  fantasyprosProj: number | null;
  linestarProj: number | null;
  linestarOwnPct: number | null;
  /** Our stated-prior ownership (nfl-ownership-prior-v1), percent. Null when it could not be computed. */
  ourOwnPct?: number | null;
  /** Showdown only: the prior's Captain and Flex slot ownership, percent. */
  captainOwnPct?: number | null;
  flexOwnPct?: number | null;
  /**
   * The projected ownership the optimizer reads, percent: LineStar when the
   * feed is present, otherwise our prior. `ownSource` names which. Null means
   * unknown, and unknown is never scored as low.
   */
  ownPct?: number | null;
  ownSource?: string | null;
  customProj: number | null;
  workload?: WorkloadProjection | null;
  workloadReason?: string;
  positionWorkload?: CalibratedProjection | null;
  positionWorkloadReason?: string;
  calibrated?: CalibratedProjection | null;
  calibrationReason?: string;
  situationEvidence?: SituationEvidence;
  projectionAudit?: ProjectionAudit;
  defensiveForecast?: DefensiveForecastBundle | null;
};

export type NflOptimizerSettings = {
  format: NflSlateFormat;
  mode: NflOptimizerMode;
  /** Classic GPP: give eligible leaders by selected mean projection a lineup without manual exposure targets. */
  topProjectedCoverage?: boolean;
  /** Optional Classic GPP lineup rule; chips are descriptive and never modify projections. */
  gppSignalMinPerLineup?: 0 | 1;
  gppSignalCodes?: NflPlayerSignalCode[];
  /** Minimum share of Classic GPP lineups with an AIR_MATCHUP player. 0 disables it. */
  gppAirMatchupMinPct?: number;
  /** Minimum share of Classic GPP lineups with an INSIDE_FIVE RB. 0 disables it. */
  gppGoalLineMinPct?: number;
  projectionSource: NflProjectionSource;
  defensiveAdjustments?: DefensiveSettings;
  /** Team -> DK id of a user-confirmed starting QB; see `confirmed-starter.ts`. */
  confirmedStartingQbs?: Record<string, number>;
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
  /**
   * GPP leverage exponent k in `(ceiling + boom) × (1 − own)^k`. 0.5 is a
   * stated prior; 0 disables the factor. GPP mode only, and only when
   * leverage is enabled. Replaced a flat 0.025 points per ownership point on
   * 2026-09-27: against a P90 objective that penalty was ~3% of a chalk
   * player's ceiling and never flipped a single choice.
   */
  leverageExponent?: number;
  /**
   * Cap on the ceiling the GPP search may credit a player: `min(P90,
   * projection × this)`. Starters run 1.7–2.1× their projection; a 5.5-point
   * back with a 30.9 P90 (5.6×) carries a ceiling from a role he no longer
   * has, and the search was filling RB slots with him. 2.5 is a stated prior.
   */
  maxCeilingMultiple?: number;
  /** User opt-in for uncalibrated heuristic ownership leverage (client → server; the server resolves the final bit). */
  useHeuristicOwnershipLeverage?: boolean;
  /** Phase 3: per-player Overall/Captain/Flex exposure ranges. Overrides the flat maxExposure per player. */
  exposurePolicies?: PlayerExposurePolicy[];
  /**
   * Phase 4 simplification: how the archetype plan is chosen.
   * - "balanced": auto-allocate the balanced mix and auto-select fade targets
   *   (archetypes with missing prerequisites fold into Standard ceiling).
   * - "custom": use archetypeQuotas/archetypeConfigs exactly as supplied.
   * - "standard" or absent: Standard ceiling only (legacy behavior) unless
   *   explicit archetypeQuotas are supplied, which always win.
   */
  archetypeMode?: "balanced" | "custom" | "standard" | "chalk_leverage";
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
  projectionSource: NflProjectionSource | "dk_avg_fallback" | "our_fallback" | "defensive";
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
  /** Phase 4: the portfolio plan's quota per archetype against what was built; absent without a plan. */
  archetypePlan?: Array<{ archetypeId: ArchetypeId; label: string; requested: number; realized: number }>;
};

type ResolvedPlayer = NflOptimizerPlayer & {
  projection: number;
  resolvedSource: NflProjectionSource | "dk_avg_fallback" | "our_fallback" | "defensive";
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

/**
 * Season-aware observed-history gate: a player fails when he has fewer games
 * of his own than the requirement — MIN_OBSERVED_GAMES, capped at the games
 * his team has completed this season (never below 1). A week-2 rookie starter
 * with his one available game passes; a zero-game backup never does.
 */
function missingObservedHistory(player: NflOptimizerPlayer): boolean {
  return (player.historyGames ?? 0) < observedHistoryRequirement(player.teamSeasonGames);
}

function observedHistoryReason(player: NflOptimizerPlayer): string {
  const required = observedHistoryRequirement(player.teamSeasonGames);
  return `Fewer than ${required} observed game${required === 1 ? "" : "s"} of his own (team has completed ${player.teamSeasonGames ?? "an unknown number of"} game(s) this season); projection is a position average.`;
}

/** Build role evidence for the punt policy from a slate player, keeping unknowns as null. */
function roleEvidenceFor(player: NflOptimizerPlayer, verifiedReceiversOutAhead = 0): NflPlayerRoleEvidence {
  const observed = player.historyGames ?? null;
  return {
    playerId: player.dkPlayerId,
    verifiedActive: player.isOut ? false : null,
    availabilityState: player.availabilityState ?? "unknown",
    depthRole: player.depthRole ?? null,
    verifiedReceiversOutAhead,
    // If role confidence is not supplied but the player has observed games of
    // his own, treat observed history as weak role evidence rather than unknown.
    // Season-aware: a rookie starter who has played every game his team has
    // played carries the same weak evidence a 2-game veteran does.
    roleConfidence: player.roleConfidence ?? (observed != null && observed >= observedHistoryRequirement(player.teamSeasonGames) && observed > 0 ? 0.5 : observed === 0 ? 0 : null),
    projectedOpportunities: player.projectedOpportunities ?? null,
    opportunityUnit: null,
    observedGameCount: observed,
    sourceIds: [],
    evidenceAsOf: null,
  };
}

/**
 * A zero written by the availability policy is a DECISION, not a missing value.
 *
 * `zeroOutProjection`/`storedSlateProjection` set `projectionStatus = "out"`
 * with `ourProj = 0` for a player ruled out. Every fallback below fires on
 * "no usable number", and `0 > 0` is false, so without this guard the
 * deliberate zero reads as absence and the player is restored at somebody
 * else's number -- the loudest one available, his DK season average.
 *
 * Measured on the 2026 week-2 classic slate: 11 players carried a policy zero
 * and a non-zero DK average. All 11 scored exactly 0. The fallback's entire
 * effect was to overwrite correct zeros; the largest, Zay Flowers at 29.0,
 * became the highest projection on the board and reached 3 of 20 lineups.
 *
 * Note this is NOT the same as `isOut`. DK's own flag covers OUT/IR only, so
 * a player DK lists Doubtful whom OUR feed ruled out has `isOut === false` and
 * survives the inactive check; his status is the only thing that says so.
 */
function ruledOut(player: NflOptimizerPlayer): boolean {
  return player.isOut || player.projectionStatus === OUT_PROJECTION_STATUS;
}

/**
 * Showdown Captain pays 1.5x points for 1.5x salary, so the slot is
 * value-neutral and a linear objective is indifferent about WHERE a player
 * goes: given a chosen six, the solver puts the multiplier on whichever of
 * them scores highest, which in GPP mode is the highest `ceilingFpts`. The
 * captain is therefore never chosen on its own merits -- it is the model's
 * top ceiling estimate, amplified, with no diversification and no cap
 * (`captainMax` falls back to `nLineups`).
 *
 * That makes the slot uniquely unforgiving of an availability doubt. A
 * QUESTIONABLE player in FLEX costs his own points; the same player at Captain
 * costs 1.5x and takes the lineup with him.
 *
 * Measured on the 2026 week-2 Monday showdown (contest 195786073): Puka Nacua
 * carried a fresh QUESTIONABLE tag, our highest ceiling on the slate (42.7),
 * and 0.64% field ownership -- the market had resolved him and we had not. He
 * took 14 of 40 captain slots and scored 0.0.
 *
 * So availability doubt makes a player Flex-only. This is deliberately NOT a
 * uniform captain exposure cap: caps were measured on the same slate and made
 * it worse, because they push the multiplier onto genuinely weaker players.
 * This gates on evidence instead of quota.
 *
 * Unknown is not doubt. A player with no availability evidence is unchanged --
 * absence of a tag is not a tag, and treating it as one would make the gate
 * fire on every slate where the feed is simply empty.
 */
const CAPTAIN_DOUBTFUL_STATES: ReadonlySet<string> = new Set(["QUESTIONABLE", "DOUBTFUL"]);

/**
 * DraftKings lists him Doubtful.
 *
 * `dk-salary-csv.ts` deliberately keeps `D` out of its OUT set and says the
 * call belongs here: "which of them is acceptable is a contest-type judgement
 * (a GPP lineup may want a cheap doubtful player others fade)". This is that
 * call, and it comes out as: not by default.
 *
 * Measured on the 2026 slates, one observation per player per week:
 *
 *   status   n     took the field   mean scored
 *   D        4          0%             0.00
 *   Q       98         39%             4.10     (unflagged players: 52%, 3.79)
 *
 * Four is a small number and the direction is not carried by it alone. The
 * NFL's own injury report defines Doubtful as roughly a 25% chance to play and
 * in practice it is lower; a 317,000-entry field owned the largest of these
 * four (Brock Bowers, projected 13.1) at 0.01%, meaning essentially everyone
 * else read D as out. The cost is asymmetric too: excluding a doubtful player
 * who does play loses one option out of several hundred, while rostering one
 * who does not is a zero in the lineup.
 *
 * Questionable is explicitly NOT included. It is genuinely ambiguous -- 39%
 * play, and those who do score slightly BETTER than unflagged players -- so
 * excluding it would throw away real players (Chris Olave 22.6, Zay Flowers
 * 29.0 in this sample). Q remains rostered, and only loses the Captain slot.
 *
 * An explicit lock overrides this: it is a default about risk, not a statement
 * of fact, which is the distinction `isOut` carries and this does not.
 */
export function listedDoubtful(player: NflOptimizerPlayer): boolean {
  return (player.dkStatus ?? "").trim().toUpperCase() === "D";
}

export function captainBlockedByAvailability(player: NflOptimizerPlayer): boolean {
  const status = (player.availabilityStatus ?? "").trim().toUpperCase();
  return CAPTAIN_DOUBTFUL_STATES.has(status);
}

function projectionFor(player: NflOptimizerPlayer, settings: NflOptimizerSettings): { value: number; source: ResolvedPlayer["resolvedSource"] } | null {
  // Fail closed before any source is consulted: no projection exists for a
  // player we have decided is not playing, in any source.
  if (ruledOut(player)) return null;
  if (settings.defensiveAdjustments?.mode !== undefined && settings.defensiveAdjustments.mode !== 'off') {
    const bundle = player.defensiveForecast;
    if (!bundle || bundle.profile !== captureProfileFor(settings.defensiveAdjustments.profile, player.position) || bundle.mode !== settings.defensiveAdjustments.mode)
      throw new Error('Defensive forecast bundle does not match optimizer settings.');
    if (bundle.status === 'applied') return bundle.selected.mean > 0
      ? {value:bundle.selected.mean,source:'defensive'}:null;
    if (finite(player.ourProj)!==null && player.ourProj!>0) return {value:player.ourProj!,source:'our_fallback'};
    // Defensive mode is the default, and it used to return here, so "fall
    // back to DK's season average" did nothing: 17 players on the 2026 week-3
    // classic silently left the pool. The same opt-in applies in every mode,
    // and `ruledOut` above still wins, so it never restores an absent player.
    const dkAvg = finite(player.avgFptsDk);
    return settings.allowDkFallback && dkAvg != null && dkAvg > 0 ? { value: dkAvg, source: "dk_avg_fallback" } : null;
  }
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
  if (settings.defensiveAdjustments?.mode !== undefined && settings.defensiveAdjustments.mode !== 'off' && player.defensiveForecast) {
    const bundle=player.defensiveForecast;
    return {version:'nfl-projection-audit-v1',baseline:bundle.baseline.mean,final:bundle.selected.mean,
      source:resolved?.source??'unavailable',excluded:ruledOut(player)||settings.excludedPlayerIds.includes(player.dkPlayerId)||!resolved,
      modelSnapshot:bundle,evidence:{digest:bundle.digest,candidateRunId:bundle.candidateRunId,capturedAt:bundle.capturedAt},assumption:null,
      rangeMethod:'Frozen player distribution, rescored from adjusted stat draws.',
      steps:[{label:'Defensive adjustments',status:bundle.status==='applied'?'applied':'not_applied',
        points:bundle.selected.mean-bundle.baseline.mean,reason:`${bundle.profile}: ${bundle.reason}; bundle ${bundle.digest}.`}]};
  }
  if(resolved?.source==='workload'&&player.projectionAudit)return {...player.projectionAudit,excluded:ruledOut(player)||settings.excludedPlayerIds.includes(player.dkPlayerId)};
  const baseline=finite(player.ourProj),final=resolved?.value??null,delta=baseline!=null&&final!=null?final-baseline:0;
  return {version:'nfl-projection-audit-v1',baseline,final,source:resolved?.source??'unavailable',excluded:ruledOut(player)||settings.excludedPlayerIds.includes(player.dkPlayerId)||!resolved,modelSnapshot:resolved?.source==='calibrated'?player.calibrated:null,evidence:player.projectionAudit?.evidence??null,assumption:null,rangeMethod:resolved?.source==='calibrated'?'Pinned calibrated player ranges.':resolved?.source==='our'||resolved?.source==='our_fallback'?'Historical player ranges.':'Source supplies a mean only; optimizer uses 0.74 × mean / 1.28 × mean range heuristics.',steps:[{label:'Selected projection source',status:delta?'applied':'not_applied',points:delta,reason:resolved?`${resolved.source}: ${baseline===null?'historical baseline unavailable; no comparative delta claimed':final===baseline?'historical estimate retained':'source replacement, not an inferred injury or matchup effect'}.`:'No usable projection; excluded.'},...(player.projectionAudit?.steps.filter(s=>s.label==='Situation adjustments')??[])]};
}

function safe(value: string): string {
  return value.replace(/[^a-zA-Z0-9]/g, "_");
}

function jitter(seed: number, lineup: number, playerId: number): number {
  let value = (seed ^ (lineup * 2654435761) ^ (playerId * 1597334677)) >>> 0;
  value = Math.imul(value ^ (value >>> 16), 2246822507) >>> 0;
  return (value / 4294967295) * 2 - 1;
}

/**
 * The projected ownership the optimizer consumes: `ownPct` (LineStar or our
 * prior, set by the workspace), falling back to a bare `linestarOwnPct` for
 * callers that predate `ownPct`. Reading `ownPct` alone (2026-09-26) silently
 * blinded the contrarian-captain archetype for any such caller.
 */
export function ownershipPct(player: Pick<NflOptimizerPlayer, "ownPct" | "linestarOwnPct">): number | null {
  return finite(player.ownPct ?? null) ?? finite(player.linestarOwnPct);
}

/**
 * Ownership for one Showdown slot. Captain and flex are different fields: a
 * chalk flex play can be a quiet captain. Applying the combined figure to both
 * faded the same player twice (PHI@CHI 2026-09-28). A single combined feed
 * (LineStar) has no slot split, so it is used as is.
 */
export function slotOwnershipPct(player: Pick<NflOptimizerPlayer, "ownPct" | "linestarOwnPct" | "captainOwnPct" | "flexOwnPct" | "ownSource">, slot: string): number | null {
  if (player.ownSource !== "linestar") {
    if (slot === "CPT" && finite(player.captainOwnPct ?? null) !== null) return finite(player.captainOwnPct ?? null);
    if (slot === "FLEX" && finite(player.flexOwnPct ?? null) !== null) return finite(player.flexOwnPct ?? null);
  }
  return ownershipPct(player);
}

export const DEFAULT_LEVERAGE_EXPONENT = 0.5;
export const DEFAULT_MAX_CEILING_MULTIPLE = 2.5;
/** Ownership above this is clamped so the factor never reaches zero. */
const LEVERAGE_OWNERSHIP_CLAMP = 0.95;

/**
 * `(1 − own)^k`. Unknown ownership is 1: it is neither penalised nor
 * rewarded, which is the same neutrality the flat penalty had (null → 0).
 */
export function leverageFactor(ownPct: number | null | undefined, exponent: number): number {
  const own = finite(ownPct ?? null);
  if (own == null || exponent <= 0) return 1;
  return Math.pow(1 - Math.min(LEVERAGE_OWNERSHIP_CLAMP, Math.max(0, own) / 100), exponent);
}

/** The ceiling the search may credit: never more than `maxMultiple` × the projection. */
export function cappedCeiling(ceiling: number, projection: number, maxMultiple: number): number {
  if (!(projection > 0) || !(maxMultiple > 0)) return ceiling;
  return Math.min(ceiling, projection * maxMultiple);
}

function objective(player: ResolvedPlayer, settings: NflOptimizerSettings, lineupNumber: number, slot = "CLASSIC"): number {
  const defensive=player.defensiveForecast?.status==='applied' && settings.defensiveAdjustments?.mode!=='off' ? player.defensiveForecast.selected:null;
  const historical = player.resolvedSource === "our" || player.resolvedSource === "our_fallback";
  const rawBase = settings.mode === "cash"
    ? (defensive ? defensive.p10 : player.resolvedSource === "workload" ? selectedWorkload(player,settings.workloadPositions)!.p10 : player.resolvedSource === "calibrated" ? player.calibrated!.p10 : historical ? finite(player.floorFpts) : null) ?? player.projection * 0.74
    : (defensive ? defensive.p90 : player.resolvedSource === "workload" ? selectedWorkload(player,settings.workloadPositions)!.p90 : player.resolvedSource === "calibrated" ? player.calibrated!.p90 : historical ? finite(player.ceilingFpts) : null) ?? player.projection * 1.28;
  const base = settings.mode === "gpp" ? cappedCeiling(rawBase, player.projection, settings.maxCeilingMultiple ?? DEFAULT_MAX_CEILING_MULTIPLE) : rawBase;
  // Phase 2: leverage (ownership penalty) applies when the server resolved it
  // as permitted — a validated feed, or a declared-heuristic feed the user
  // explicitly opted into (labeled "uncalibrated" everywhere). Missing
  // ownership is null and contributes no penalty — it is never rewarded as low
  // ownership. Without a resolved bit, only "validated" enables it.
  const leverageEnabled = settings.mode === "gpp"
    && (settings.ownershipLeverageEnabled ?? ((settings.ownershipCapability ?? "unavailable") === "validated"));
  // Ownership scales the whole ceiling term: (1 − own)^k. A flat points
  // penalty could not move a P90 objective; a factor can (53.5% owned at
  // k = 0.5 is ×0.68). Cash mode never fades chalk.
  const leverage = leverageEnabled && settings.mode === "gpp"
    ? leverageFactor(slotOwnershipPct(player, slot), settings.leverageExponent ?? DEFAULT_LEVERAGE_EXPONENT) : 1;
  const workload=player.resolvedSource === "workload"?selectedWorkload(player,settings.workloadPositions):null;
  const boomBonus = settings.mode === "gpp" ? (defensive ? defensive.boom : workload && "boom" in workload ? workload.boom : player.resolvedSource === "calibrated" ? player.calibrated!.boom : historical ? finite(player.boomRate) ?? 0 : 0) * 2 : 0;
  return (base + boomBonus) * leverage + jitter(20260902, lineupNumber, player.dkPlayerId) * settings.randomness * player.projection;
}

function validateSettings(settings: NflOptimizerSettings): void {
  if (![0, 1].includes(settings.gppSignalMinPerLineup ?? 0)) throw new Error("GPP signal minimum must be 0 or 1.");
  if (settings.gppSignalMinPerLineup && (settings.format !== "classic" || settings.mode !== "gpp")) throw new Error("Opportunity signals can only be required in Classic GPP.");
  if (settings.gppSignalMinPerLineup && settings.gppSignalCodes?.length === 0) throw new Error("Select at least one opportunity signal.");
  if (settings.gppSignalCodes?.some(code => !["AIR_VOLUME", "AIR_MATCHUP", "YAC_RUNWAY", "INSIDE_FIVE", "CLOSE_TARGET"].includes(code))) throw new Error("Unknown opportunity signal.");
  const airPct = settings.gppAirMatchupMinPct ?? 0;
  if (!Number.isFinite(airPct) || airPct < 0 || airPct > 100) throw new Error("Air-yard matchup lineup percentage must be between 0 and 100.");
  if (airPct > 0 && (settings.format !== "classic" || settings.mode !== "gpp")) throw new Error("Air-yard matchup lineup percentage is only available in Classic GPP.");
  const goalLinePct = settings.gppGoalLineMinPct ?? 0;
  if (!Number.isFinite(goalLinePct) || goalLinePct < 0 || goalLinePct > 100) throw new Error("Goal-line RB lineup percentage must be between 0 and 100.");
  if (goalLinePct > 0 && (settings.format !== "classic" || settings.mode !== "gpp")) throw new Error("Goal-line RB lineup percentage is only available in Classic GPP.");
  if (settings.defensiveAdjustments?.mode !== undefined && settings.defensiveAdjustments.mode !== 'off') {
    if (settings.projectionSource !== 'our') throw new Error('Defensive adjustments require the historical projection source.');
    if (settings.defensiveAdjustments.mode !== 'experimental' && settings.defensiveAdjustments.mode !== 'approved') throw new Error('Unknown defensive mode.');
    if (!['pfr-efficiency', 'allowed-rushing-volume', 'gpp-integrated'].includes(settings.defensiveAdjustments.profile)) throw new Error('Unknown defensive profile.');
    if (settings.defensiveAdjustments.profile === 'gpp-integrated' && settings.defensiveAdjustments.mode !== 'experimental') throw new Error('The integrated profile requires experimental mode.');
  }
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
  remaining: number = settings.nLineups - lineupNumber + 1,
  enforceSlotMinimums = true,
  requireAirMatchup = false,
  requireGoalLine = false,
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
  if (settings.gppSignalMinPerLineup) constraints.gpp_opportunity_signal = { min: settings.gppSignalMinPerLineup };
  if (requireAirMatchup) constraints.air_matchup = { min: 1 };
  if (requireGoalLine) constraints.goal_line_rb = { min: 1 };
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
  const overlapCap = nflOverlapCap(settings.format, settings.minUnique, settings.maxPairwiseOverlap);
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
  let slotForced = false;
  for (const player of available) {
    const purchaseTypes = settings.format === "classic" ? ["CLASSIC"] : ["CPT", "FLEX"];
    for (const purchaseType of purchaseTypes) {
      if (purchaseType === "CPT" && (player.captainDkPlayerId == null || player.captainSalary == null)) continue;
      if (purchaseType === "FLEX" && !showdownFlexEligible(player)) continue;
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
      const salary = settings.format === "showdown" ? showdownSalary(player, slot === "CPT") : player.salary;
      const variable: Record<string, number> = {
        score: objective(player, settings, lineupNumber, slot) * multiplier,
        salary,
        [`player_${player.dkPlayerId}`]: 1,
      };
      // Count this pick against the per-lineup salary-relief cap.
      if (settings.puntPolicy && player.salaryRelief) variable.salary_relief = 1;
      if (constraints.gpp_opportunity_signal && isNflGppSignalPlayer(player.playerSignals, settings.gppSignalCodes)) variable.gpp_opportunity_signal = 1;
      if (constraints.air_matchup && isNflGppSignalPlayer(player.playerSignals, ["AIR_MATCHUP"])) variable.air_matchup = 1;
      if (constraints.goal_line_rb && player.position === "RB" && isNflGppSignalPlayer(player.playerSignals, ["INSIDE_FIVE"])) variable.goal_line_rb = 1;
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
      // A captain/flex MINIMUM is a promise, not a report line: once the
      // remaining lineups equal the slot appearances still owed, force that
      // slot variable in. The constraint is only created alongside a variable
      // that can satisfy it, so it can never be unsatisfiable by absence.
      if (enforceSlotMinimums && settings.format === "showdown" && slot !== "CLASSIC") {
        const counts = countsById.get(player.dkPlayerId);
        const owed = slot === "CPT"
          ? (counts?.captainMin ?? 0) - (captainCounts.get(player.dkPlayerId) ?? 0)
          : (counts?.flexMin ?? 0) - (flexCounts.get(player.dkPlayerId) ?? 0);
        if (owed >= remaining) {
          const forceKey = `${slot === "CPT" ? "forcecpt" : "forceflex"}_${player.dkPlayerId}`;
          constraints[forceKey] = { equal: 1 };
          variable[forceKey] = 1;
          slotForced = true;
        }
      }
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
  if (solved.feasible === false) {
    // Forcing slot minimums made this lineup impossible (salary, team caps,
    // overlap). Build it without them rather than ending the run early; the
    // exposure report still flags the missed minimum honestly.
    if (slotForced) return buildOne(pool, settings, lineupNumber, previous, exposureCounts, forcedIds, countsById, captainCounts, flexCounts, compiled, remaining, false, requireAirMatchup, requireGoalLine);
    return null;
  }
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
      chosen.push({ slot, player: purchase.player, salary: showdownSalary(purchase.player, purchase.slot === "CPT"), multiplier, projection: purchase.player.projection * multiplier, projectionSource: purchase.player.resolvedSource });
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
  if (settings.format === "showdown") assertShowdownLineup({ slots: chosen, playerIds: chosen.map(s => s.player.dkPlayerId), totalSalary: chosen.reduce((sum, s) => sum + s.salary, 0) });
  const qb = chosen.find((entry) => entry.player.position === "QB")?.player ?? null;
  const passCatchers = qb ? chosen.filter((entry) => ["WR", "TE"].includes(entry.player.position) && entry.player.team === qb.team).map((entry) => entry.player.name) : [];
  const bringBack = qb ? chosen.find((entry) => ["RB", "WR", "TE"].includes(entry.player.position) && entry.player.team === qb.opponent)?.player.name ?? null : null;
  return {
    lineupNumber,
    slots: chosen,
    playerIds: chosen.map((entry) => entry.player.dkPlayerId),
    totalSalary: chosen.reduce((sum, entry) => sum + entry.salary, 0),
    projectedFpts: chosen.reduce((sum, entry) => sum + entry.projection, 0),
    floorFpts: chosen.reduce((sum, entry) => sum + (entry.projectionSource==='defensive' ? entry.player.defensiveForecast!.selected.p10 : entry.projectionSource === "workload" ? selectedWorkload(entry.player,settings.workloadPositions)!.p10 : entry.projectionSource === "calibrated" ? entry.player.calibrated!.p10 : entry.projectionSource === "our" || entry.projectionSource === "our_fallback" ? entry.player.floorFpts ?? entry.projection / entry.multiplier * .74 : entry.projection / entry.multiplier * .74) * entry.multiplier, 0),
    ceilingFpts: chosen.reduce((sum, entry) => sum + (entry.projectionSource==='defensive' ? entry.player.defensiveForecast!.selected.p90 : entry.projectionSource === "workload" ? selectedWorkload(entry.player,settings.workloadPositions)!.p90 : entry.projectionSource === "calibrated" ? entry.player.calibrated!.p90 : entry.projectionSource === "our" || entry.projectionSource === "our_fallback" ? entry.player.ceilingFpts ?? entry.projection / entry.multiplier * 1.28 : entry.projection / entry.multiplier * 1.28) * entry.multiplier, 0),
    projectedOwnership: chosen.some((entry) => ownershipPct(entry.player) != null)
      ? chosen.reduce((sum, entry) => sum + (ownershipPct(entry.player) ?? 0), 0)
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
  const receiverRoleChanges = deriveReceiverRoleChanges(players);
  let belowSalaryFloor = 0;
  let withoutHistory = 0;
  let puntBlocked = 0;

  for (const player of players) {
    const named = { dkPlayerId: player.dkPlayerId, name: player.name, salary: player.salary };
    // Manual exclusion and inactivity are handled first so they always win.
    if (ruledOut(player)) {
      // Two sources, one gate. DK's Status column covers OUT/IR; our own
      // availability feed can rule out a player DK lists Doubtful/Questionable,
      // and says so by stamping the projection `out`. Honouring only the first
      // let the DK-average fallback restore the second (see `ruledOut`).
      // `isOut` also carries availability blocks (a listed backup QB), which
      // are not OUT/IR; name the block rather than calling him inactive.
      const blocked = player.availability?.blockedReason?.trim();
      const reason = player.isOut
        ? blocked ? `Not available: ${blocked}.` : "Inactive (OUT/IR)."
        : "Ruled out by our availability feed; projection zeroed. DraftKings did not flag him OUT/IR, so only the projection status records it.";
      eligibility.push({ ...named, eligible: false, salaryRelief: false, captainEligible: false, overridden: false, reason, reasonCode: "INACTIVE" });
      coverage.excluded++; continue;
    }
    if (excluded.has(player.dkPlayerId)) {
      eligibility.push({ ...named, eligible: false, salaryRelief: false, captainEligible: false, overridden: false, reason: "Manually excluded for this run.", reasonCode: "MANUAL_EXCLUSION" });
      coverage.excluded++; continue;
    }
    // A lock is the user's own instruction and outranks a default about risk.
    if (listedDoubtful(player) && !locked.has(player.dkPlayerId)) {
      eligibility.push({ ...named, eligible: false, salaryRelief: false, captainEligible: false, overridden: false,
        reason: "DraftKings lists this player Doubtful. Lock him to use him anyway.", reasonCode: "DOUBTFUL" });
      coverage.excluded++; continue;
    }

    let salaryRelief = false;
    // Applies on both the policy path and the legacy path: the Captain rule is
    // about the 1.5x multiplier, not about how cheap players are screened.
    let captainEligible = !captainBlockedByAvailability(player);
    let overridden = false;

    if (policy) {
      // Role-aware no-punt policy (spec §8) is the single eligibility authority
      // when present. It fully supersedes the bare salary floor.
      const outAhead = receiverRoleChanges.get(player.dkPlayerId)?.absentAhead.length ?? 0;
      const decision = evaluatePuntEligibility(player, roleEvidenceFor(player, outAhead), policy, overrides);
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
      captainEligible = (!overridden || captainAdmissible(player.dkPlayerId, overrides))
        && !captainBlockedByAvailability(player);
      // Main's observed-history gate (#217) still applies alongside the policy:
      // the policy scrutinizes CHEAP roles, while this removes any-priced
      // players whose projection is a position average, not theirs (the backup
      // QB handed the average NFL start). Locks, exposure targets and recorded
      // overrides are the user's own instruction and outrank it.
      if (settings.requireObservedHistory && missingObservedHistory(player) && !floorExempt.has(player.dkPlayerId) && !overridden) {
        eligibility.push({ ...named, eligible: false, salaryRelief: false, captainEligible: false, overridden: false, reason: observedHistoryReason(player), reasonCode: "ROLE_UNKNOWN" });
        withoutHistory++; coverage.excluded++; continue;
      }
    } else {
      // Legacy path: bare salary floor + observed-history gate.
      if (salaryFloor > 0 && player.salary < salaryFloor && !floorExempt.has(player.dkPlayerId)) {
        eligibility.push({ ...named, eligible: false, salaryRelief: false, captainEligible: false, overridden: false, reason: `Below the $${salaryFloor.toLocaleString()} per-player salary floor.`, reasonCode: "ABSOLUTE_SALARY_BLOCK" });
        belowSalaryFloor++; coverage.excluded++; continue;
      }
      if (settings.requireObservedHistory && missingObservedHistory(player) && !floorExempt.has(player.dkPlayerId)) {
        eligibility.push({ ...named, eligible: false, salaryRelief: false, captainEligible: false, overridden: false, reason: observedHistoryReason(player), reasonCode: "ROLE_UNKNOWN" });
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

  // A lock, or any minimum exposure, on a player outside the pool used to be
  // dropped: a lock made every lineup infeasible with a solver message that
  // named nobody, and a captain range was skipped while QA said "All exposure
  // ranges satisfied". Say who and why before generating anything.
  const inPool = new Set(pool.map((player) => player.dkPlayerId));
  if (settings.gppSignalMinPerLineup && !pool.some(player => isNflGppSignalPlayer(player.playerSignals, settings.gppSignalCodes))) {
    throw new Error("No eligible player has a selected opportunity signal. Change the selected signals or turn off the lineup rule.");
  }
  const airMatchupTarget = Math.ceil(settings.nLineups * (settings.gppAirMatchupMinPct ?? 0) / 100);
  if (airMatchupTarget && !pool.some(player => isNflGppSignalPlayer(player.playerSignals, ["AIR_MATCHUP"]))) {
    throw new Error("No eligible player has an air-yard matchup tag. Lower the requested percentage or turn off the air-yard matchup rule.");
  }
  const goalLineTarget = Math.ceil(settings.nLineups * (settings.gppGoalLineMinPct ?? 0) / 100);
  if (goalLineTarget && !pool.some(player => player.position === "RB" && isNflGppSignalPlayer(player.playerSignals, ["INSIDE_FIVE"]))) {
    throw new Error("No eligible RB has an inside-5 work tag. Lower the requested percentage or turn off the goal-line RB rule.");
  }
  const decisionById = new Map(eligibility.map((decision) => [decision.dkPlayerId, decision]));
  const nameOf = (id: number) => players.find((player) => player.dkPlayerId === id)?.name ?? `Player ${id}`;
  const whyOut = (id: number): string => {
    const decision = decisionById.get(id);
    if (!decision) return "he is not on this slate";
    const reason = (decision.reason ?? "he is not in the player pool").replace(/\.$/, "");
    return reason.charAt(0).toLowerCase() + reason.slice(1);
  };
  for (const id of settings.lockedPlayerIds) {
    if (!inPool.has(id)) throw new Error(`${nameOf(id)} is locked but can't be used: ${whyOut(id)}. Remove the lock to build without him.`);
  }
  for (const [rawId, target] of Object.entries(settings.minExposureByPlayer)) {
    if (target > 0 && !inPool.has(Number(rawId))) {
      throw new Error(`${nameOf(Number(rawId))} has a minimum exposure but can't be used: ${whyOut(Number(rawId))}. Clear his exposure range to build without him.`);
    }
  }
  for (const policy of settings.exposurePolicies ?? []) {
    if (inPool.has(policy.playerId)) continue;
    const minimum = (policy.captain.minPct ?? 0) > 0 ? "captain" : (policy.flex.minPct ?? 0) > 0 ? "flex" : (policy.overall.minPct ?? 0) > 0 ? "overall" : null;
    if (minimum) throw new Error(`${nameOf(policy.playerId)} has a ${minimum} minimum but can't be used: ${whyOut(policy.playerId)}. Clear his ${minimum === "captain" ? "CPT " : ""}range to build without him.`);
    if (policy.captain.maxPct != null || policy.flex.maxPct != null || policy.overallFromUser) {
      warnings.push(`${nameOf(policy.playerId)}'s range was ignored: ${whyOut(policy.playerId)}.`);
    }
  }
  for (const rawId of Object.keys(settings.maxExposureByPlayer)) {
    const id = Number(rawId);
    if (!inPool.has(id) && !(settings.minExposureByPlayer[rawId] > 0) && !(settings.exposurePolicies ?? []).some((p) => p.playerId === id)) {
      warnings.push(`${nameOf(id)}'s exposure cap was ignored: ${whyOut(id)}.`);
    }
  }
  if (puntBlocked) warnings.push(`${puntBlocked} player(s) blocked by the ${policy!.mode} punt policy. See the cheap-player review for the reason on each; allow a player for the run to keep him.`);
  if (withoutHistory) warnings.push(`${withoutHistory} player(s) with too few games of their own were removed (${MIN_OBSERVED_GAMES} required, capped at the games their team has completed this season): their projection is their position's average, not theirs. Lock a player to keep him regardless.`);
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
  // Phase 4: build the per-archetype generation plan. Explicit quotas always
  // win; "balanced" mode auto-allocates the mix and auto-selects fade targets.
  const archetypeContext: ArchetypeSlateContext = {
    players: pool.map((p) => ({ dkPlayerId: p.dkPlayerId, position: p.position, team: p.team, opponent: p.opponent, ownership: ownershipPct(p) != null ? (ownershipPct(p) as number) / 100 : null, projection: p.projection, captainEligible: p.captainEligible })),
    favoriteTeam: settings.favoriteTeam ?? null,
    underdogTeam: settings.underdogTeam ?? null,
    ownershipValidated: (settings.ownershipCapability ?? "unavailable") === "validated",
  };
  // Chalk-captain model (Showdown only), resolved BEFORE exposure policies:
  // its captains need a different reading of the flat cap. See below.
  const chalkPlan = settings.archetypeMode === "chalk_leverage" && settings.format === "showdown"
    && !settings.archetypeQuotas?.some((q) => q.enabled)
    ? chalkLeveragePlan(archetypeContext, settings.nLineups, {
        extraCaptainIds: (settings.exposurePolicies ?? [])
          .filter((policy) => (policy.captain.minPct ?? 0) > 0).map((policy) => policy.playerId),
      })
    : null;
  const chalkCaptainSet = new Set(chalkPlan?.chalkCaptainIds ?? []);

  const policyById = new Map((settings.exposurePolicies ?? []).map((p) => [p.playerId, p]));
  const effectivePolicy = (player: ResolvedPlayer): PlayerExposurePolicy => {
    const resolved = baseEffectivePolicy(player);
    const userBound = resolved.overallFromUser ?? (settings.maxExposureByPlayer[String(player.dkPlayerId)] != null
      || settings.minExposureByPlayer[String(player.dkPlayerId)] != null);
    return chalkCaptainSet.has(player.dkPlayerId) ? chalkCaptainPolicy(resolved, userBound) : resolved;
  };
  const baseEffectivePolicy = (player: ResolvedPlayer): PlayerExposurePolicy => {
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

  let archetypeQuotas = settings.archetypeQuotas?.filter((q) => q.enabled);
  let archetypeConfigs = settings.archetypeConfigs ?? {};
  // Balanced auto-planning is Showdown-only: the game-script team constraints
  // only exist there, and labeling an unconstrained Classic lineup with a
  // script it does not enforce would be a mislabel. Classic stays Standard.
  if (!archetypeQuotas?.length && settings.archetypeMode === "balanced" && settings.format === "showdown") {
    const balanced = balancedArchetypePlan(archetypeContext, settings.nLineups);
    archetypeQuotas = balanced.quotas;
    archetypeConfigs = { ...balanced.configs, ...archetypeConfigs };
    for (const note of balanced.notes) warnings.push(`Balanced plan: ${note}`);
  }
  let plan: Array<{ archetypeId: ArchetypeId; compiled: CompiledArchetype | null }> = [];
  // Chalk-captain, rotating-leverage model (Showdown only). Every lineup gets
  // its own compiled instance, because the leverage POSITION rotates.
  if (chalkPlan) {
    const chalk = chalkPlan;
    const nameOf = (id: number) => pool.find((p) => p.dkPlayerId === id)?.name ?? `#${id}`;
    warnings.push(`Chalk captain model: captains limited to ${chalk.chalkCaptainIds.map(nameOf).join(", ")} `
      + `(chalk ranked by ${chalk.basis === "projection" ? "projection -- no ownership feed" : "projected ownership"}). `
      + `Leverage rotates through ${chalk.rotation.join(" → ")}, outside the core of ${chalk.coreIds.map(nameOf).join(", ")}. `
      + `Chalk captains may pass the ${Math.round(settings.maxExposure * 100)}% default through their captain slots (it still caps their flex use); a cap you set on a player is always exact.`);
    plan = chalk.lineups.map((lineup) => ({ archetypeId: "chalk_captain_leverage" as ArchetypeId, compiled: lineup.compiled }));
  } else if (archetypeQuotas?.length) {
    const allocation = allocateArchetypeQuotas(archetypeQuotas, settings.nLineups);
    if (!allocation.ok) throw new Error(`Archetype plan is infeasible: ${allocation.reason}`);
    for (const slot of allocation.allocation) {
      const compiled = compileArchetype(slot.archetypeId, archetypeContext, archetypeConfigs[slot.archetypeId] ?? {});
      // A fade with no satisfiable beneficiary is rejected up front (§11.2).
      if (isFadeArchetype(slot.archetypeId) && !compiled.beneficiaries.length) {
        throw new Error(`${ARCHETYPE_LABELS[slot.archetypeId]} declared no beneficiary path. A fade must specify at least one alternate scoring route.`);
      }
      for (let k = 0; k < slot.count; k++) plan.push({ archetypeId: slot.archetypeId, compiled });
    }
  } else {
    plan = Array.from({ length: settings.nLineups }, () => ({ archetypeId: "standard_ceiling" as ArchetypeId, compiled: null }));
  }

  // Coverage is a portfolio rule, not a change to a player's projection or
  // GPP score. Rank by the SELECTED source's mean so ownership and salary
  // cannot silently remove every appearance of a leading scorer. Only a
  // player who can make a legal lineup under the current rules is promised.
  const topProjectedTargets: ResolvedPlayer[] = [];
  if (settings.topProjectedCoverage && settings.format === "classic" && settings.mode === "gpp") {
    const emptyCounts = new Map<number, number>();
    const preflightSteps = plan.filter((step, index) => plan.findIndex(other => other.archetypeId === step.archetypeId) === index);
    for (const [position, count] of [["QB", 1], ["RB", 2], ["WR", 2], ["TE", 1]] as const) {
      const ranked = pool.filter(player => player.position === position
        && (countsById.get(player.dkPlayerId)?.overallMax ?? 0) > 0)
        .sort((a, b) => b.projection - a.projection || a.dkPlayerId - b.dkPlayerId);
      for (const candidate of ranked.slice(0, count + 3)) {
        if (topProjectedTargets.filter(player => player.position === position).length >= count) break;
        const canRoster = preflightSteps.some(step => buildOne(pool, settings, 1, [], emptyCounts,
          new Set([...locked, candidate.dkPlayerId]), countsById, emptyCounts, emptyCounts,
          step.compiled, plan.length, true, false, false));
        if (canRoster) topProjectedTargets.push(candidate);
        else warnings.push(`Top projected coverage skipped ${candidate.name}: no legal lineup with the current salary, stack, exposure, and lineup rules.`);
      }
    }
    topProjectedTargets.sort((a, b) => b.projection - a.projection || a.dkPlayerId - b.dkPlayerId);
    topProjectedTargets.splice(Math.max(1, settings.nLineups));
  }

  const exposureCounts = new Map<number, number>();
  const captainCounts = new Map<number, number>();
  const flexCounts = new Map<number, number>();
  const lineups: NflGeneratedLineup[] = [];
  let airMatchupLineups = 0;
  let goalLineLineups = 0;
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
    const requireAirMatchup = airMatchupTarget > 0 && airMatchupTarget - airMatchupLineups >= remaining;
    const requireGoalLine = goalLineTarget > 0 && goalLineTarget - goalLineLineups >= remaining;
    let lineup: NflGeneratedLineup | null = null;
    for (const candidate of topProjectedTargets) {
      if ((exposureCounts.get(candidate.dkPlayerId) ?? 0) > 0) continue;
      lineup = buildOne(pool, settings, lineupNumber, lineups, exposureCounts,
        new Set([...forced, candidate.dkPlayerId]), countsById, captainCounts, flexCounts,
        plan[lineupNumber - 1].compiled, remaining, true, requireAirMatchup, requireGoalLine);
      if (lineup) break;
    }
    lineup ??= buildOne(pool, settings, lineupNumber, lineups, exposureCounts, forced, countsById, captainCounts, flexCounts, plan[lineupNumber - 1].compiled, remaining, true, requireAirMatchup, requireGoalLine);
    if (!lineup) {
      if (requireAirMatchup && requireGoalLine) throw new Error(`Could not meet the air-yard matchup and goal-line RB minimums together with the remaining exposure, salary, and roster constraints. Lower a percentage or adjust player limits.`);
      if (requireAirMatchup) throw new Error(`Could not meet the air-yard matchup minimum of ${airMatchupTarget}/${settings.nLineups} lineups with the remaining exposure, salary, and roster constraints. Lower the percentage or adjust player limits.`);
      if (requireGoalLine) throw new Error(`Could not meet the goal-line RB minimum of ${goalLineTarget}/${settings.nLineups} lineups with the remaining exposure, salary, and roster constraints. Lower the percentage or adjust player limits.`);
      warnings.push(`Stopped after ${lineups.length} lineup(s): the ${ARCHETYPE_LABELS[plan[lineupNumber - 1].archetypeId]} quota or remaining exposure/uniqueness/salary constraints are infeasible.`);
      break;
    }
    lineups.push(lineup);
    if (lineup.slots.some(({ player }) => isNflGppSignalPlayer(player.playerSignals, ["AIR_MATCHUP"]))) airMatchupLineups++;
    if (lineup.slots.some(({ player }) => player.position === "RB" && isNflGppSignalPlayer(player.playerSignals, ["INSIDE_FIVE"]))) goalLineLineups++;
    // Track overall and slot-specific appearances. Captain and Flex are counted
    // independently; overall is their union (each player appears once per lineup).
    lineup.playerIds.forEach((id) => exposureCounts.set(id, (exposureCounts.get(id) ?? 0) + 1));
    for (const slot of lineup.slots) {
      const id = slot.player.dkPlayerId;
      if (slot.slot === "CPT") captainCounts.set(id, (captainCounts.get(id) ?? 0) + 1);
      else flexCounts.set(id, (flexCounts.get(id) ?? 0) + 1);
    }
  }
  if (airMatchupTarget) {
    if (airMatchupLineups < airMatchupTarget) throw new Error(`Air-yard matchup minimum missed: ${airMatchupLineups}/${airMatchupTarget} required lineups. Adjust the percentage or player limits.`);
    warnings.push(`Air-yard matchup coverage: ${airMatchupLineups}/${lineups.length} generated lineups; minimum ${airMatchupTarget}/${settings.nLineups} requested (${settings.gppAirMatchupMinPct}%).`);
  }
  if (goalLineTarget) {
    if (goalLineLineups < goalLineTarget) throw new Error(`Goal-line RB minimum missed: ${goalLineLineups}/${goalLineTarget} required lineups. Adjust the percentage or player limits.`);
    warnings.push(`Goal-line RB coverage: ${goalLineLineups}/${lineups.length} generated lineups; minimum ${goalLineTarget}/${settings.nLineups} requested (${settings.gppGoalLineMinPct}%).`);
  }
  if (topProjectedTargets.length) {
    const missed = topProjectedTargets.filter(player => !exposureCounts.get(player.dkPlayerId));
    if (missed.length) throw new Error(`Top projected coverage could not be completed for ${missed.map(player => player.name).join(", ")}. The lineup rules conflict with covering these players; adjust the rules or turn off top projected coverage.`);
    warnings.push(`Top projected coverage: ${topProjectedTargets.map(player => player.name).join(", ")} each appeared in at least one lineup (ranked by selected mean projection).`);
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
  const ownershipByPlayer = new Map(pool.filter((p) => ownershipPct(p) != null).map((p) => [p.dkPlayerId, (ownershipPct(p) as number) / 100]));
  const duplication = ownershipByPlayer.size
    ? estimateDuplication(lineups.map((l) => ({ lineupNumber: l.lineupNumber, playerIds: l.playerIds, totalSalary: l.totalSalary })), { ownershipValidated, ownershipByPlayer })
    : undefined;

  // The plan's quotas against what was built, so QA checks them instead of
  // showing "Archetype quotas met" unevaluated. Only when a plan shaped the
  // portfolio; plain Standard ceiling has no quota to miss.
  const planned = chalkPlan || archetypeQuotas?.length ? plan : [];
  const requestedById = new Map<ArchetypeId, number>();
  for (const slot of planned) requestedById.set(slot.archetypeId, (requestedById.get(slot.archetypeId) ?? 0) + 1);
  const archetypePlan = planned.length ? [...requestedById].map(([archetypeId, requested]) => ({ archetypeId, label: ARCHETYPE_LABELS[archetypeId], requested,
    realized: lineups.filter((lineup) => lineup.archetype?.id === archetypeId).length })) : undefined;

  return { lineups, warnings, sourceCoverage: coverage, eligibility, exposureReport, salaryBandReport, duplication, maxPairwiseOverlap: overlap, archetypePlan };
}
