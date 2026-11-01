import { diversifiedComposition, diversifiedCompositionValid, applyDiversified, diversifiedPolicy, diversifiedCount, diversifiedPositionMax, diversifiedGroups, repeatedGroupCounts, groupLimit, concentrationCost, improveDiversifiedPortfolio, DIVERSIFIED_VERSION, type DiversifiedPolicy, type DiversifiedReport } from "@/lib/nfl-dfs/diversified";
import "server-only";
import { kickerRoleBlockedReason } from '@/lib/nfl-dfs/availability';
import type { DefensiveForecastBundle, DefensiveSettings } from '@/lib/nfl-dfs/defensive-projection';
import { captureProfileFor } from '@/lib/nfl-dfs/defensive-display';
import { assertDstGameScript, assertShowdownLineup, showdownSalary, showdownFlexEligible } from '@/lib/nfl-dfs/showdown-legality';
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
import { applyWinnersShape, BUILD_SHAPES, type BuildShape, type PriorOpportunity } from '@/lib/nfl-dfs/winners-shape';
import type { SpecialTeamsProjection } from '@/lib/nfl-dfs/special-teams-projection';
import { featureTotals, JOINT_TARGET_PENALTY, NFL_JOINT_SELECTION_VERSION, selectJointPortfolio, type JointCandidate, type JointTarget } from '@/lib/nfl-dfs/joint-selection';

// Record the strict Showdown purchase and completed-roster validation.
// v8: DK-average fallback honoured in defensive mode; a lock or exposure
// minimum on a player outside the pool is an error instead of being dropped.
export const NFL_OPTIMIZER_VERSION = "nfl-dfs-ilp-v14-winners-diversified";

export type NflProjectionSource = "our" | "workload" | "calibrated" | "dk_avg" | "fantasypros" | "linestar" | "custom";
export type NflOptimizerMode = "cash" | "gpp";
export type NflSlateFormat = "classic" | "showdown";

export type NflOptimizerPlayer = {
  playerSignals?: NflPlayerSignal[];
  airMatchupEvidence?: NflAirMatchupEvidence | null;
  /** Prior-week targets/carries (three weeks of play-by-play) for the winners-shape pool rules; null = no record. */
  priorOpportunity?: PriorOpportunity | null;
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
  availability?: { blockedReason?: string | null; status?: string | null; role?: string; chartRole?: string; fresh?: boolean } | null;
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
  specialTeams?: SpecialTeamsProjection | null;
  specialTeamsReason?: string | null;
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
  /** DK id -> field ownership percent typed by the owner (nfl-ownership-manual-v1); replaces the prior for that player. */
  ownershipOverrides?: Record<string, number>;
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
  /** Calibrated marginal ownership alone does not qualify a joint field/duplication model. */
  duplicationModelValidated?: boolean;
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
  /**
   * Points the GPP search credits per unit of boom rate: `(capped ceiling +
   * boomRate × this) × leverage`. 2 is a stated prior, never fitted. Absent
   * means 2; 0 removes the boom term. The ghost factorial's boom-weight arm
   * pair (`nfl-ghost-extra-arms-v1`) builds at 1 and 4 against the house's 2.
   */
  boomWeight?: number;
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
  /**
   * "winners": the winners-shape preset (`lib/nfl-dfs/winners-shape.ts`)
   * tightens the settings below and filters the pool before generation; the
   * run's notes list every number it enforced. Absent/"standard": no preset.
   */
  buildShape?: BuildShape;
  /** Classic construction risk preferences. No effect on Standard, Winners or ghost arms. */
  diversified?: Partial<DiversifiedPolicy>;
  /** Internal candidate-generation objective. Never persisted as the final build objective. */
  diversifiedMeanSearch?: boolean;
  diversifiedCandidateSearch?: boolean;
  /**
   * Classic GPP: the share of lineups that carry an opposing bring-back,
   * 0..1. When set it replaces `bringBack`: lineups are built alternately
   * with the bring-back required and with it forbidden so the run lands on
   * the share, instead of every lineup carrying one.
   */
  bringBackShare?: number;
  /** Classic: no quarterback in more than this share of lineups (never below one lineup). A per-player range you set wins. */
  maxQbExposure?: number;
  /**
   * Classic: at most this share of lineups may play a DST against one of the
   * lineup's own QB/RB/WR/TE (rounded down, so 5% of 19 lineups is none).
   * Once the run reaches the cap, every later lineup forbids it.
   */
  maxDstVsOffenseShare?: number;
  /** Showdown: no player captains more than this share of lineups unless his own CPT range says otherwise. */
  maxCaptainExposure?: number;
  /** Showdown: leave every kicker out of the pool (locks still throw, by name). */
  excludeKickers?: boolean;
  /** Showdown: each lineup is 5-1 or 3-3 by team, never 4-2. */
  forbidFourTwoSplit?: boolean;
  /** Showdown: only these positions may take the Captain slot (a user's CPT minimum on another position is reported as infeasible). */
  captainPositions?: NflOptimizerPlayer["position"][];
  /** Showdown: every lineup rosters at least this many quarterbacks (Captain or Flex). Ignored when the pool has fewer. */
  minQbPerLineup?: number;
  /**
   * Showdown: a WR or TE captain needs his own team's quarterback in the
   * lineup, and a QB captain at least one of his own WR/TE.
   */
  captainStack?: boolean;
  /** Showdown: share of lineups that roster both quarterbacks (scheduled like the bring-back share). */
  twoQbShare?: number;
  /** Showdown: at most this share of lineups may captain a quarterback with no second quarterback. */
  maxLoneQbCaptainShare?: number;
  /** Showdown: share of lineups with a running-back captain. */
  rbCaptainShare?: number;
  /** Showdown: each quarterback may appear in at least this share of lineups unless his own overall range says otherwise. */
  showdownQbMinCap?: number;
  /** Internal, per lineup: exact quarterback count, captain restrictions. Set by the run loop, never by callers. */
  lineupQbExact?: 1 | 2;
  lineupForbidQbCaptain?: boolean;
  lineupRequireRbCaptain?: boolean;
  lineupForbidRbCaptain?: boolean;
  /** Classic GPP: share of lineups stacking the QB with 2+ of his own WR/TE; the rest stack exactly one. */
  qbDoubleStackShare?: number;
  /** Classic: at most this share of lineups may roster a 4th WR. */
  maxFourWrShare?: number;
  /** Internal, per lineup (Classic): stack size bounds and the 4th-WR ban. Set by the run loop, never by callers. */
  lineupStackMin?: 1 | 2;
  lineupStackMax?: number;
  lineupForbidFourWr?: boolean;
  /** Classic: at most this share of lineups may roster 2+ non-DST players from a team outside the QB's game. */
  maxOtherGameStackShare?: number;
  lineupForbidOtherGameStack?: boolean;
  /** Classic: share of lineups rostering a 2nd TE (two-sided: the rest roster exactly one). */
  twoTeShare?: number;
  lineupTeExact?: 1 | 2;
  /**
   * How the portfolio is assembled. `sequential` (the default, and what the
   * ghost factorial and research studies pin) builds lineup by lineup, spending
   * exposure as it goes. `joint` (nfl-joint-selection-v1) then chooses the final
   * set from that build plus extra candidates in one integer program, so an
   * early lineup cannot use up a player a later captain combination needs.
   */
  portfolioSelection?: "sequential" | "joint";
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
  projectionSource: NflProjectionSource | "dk_avg_fallback" | "our_fallback" | "defensive" | "special_teams";
};

export type NflGeneratedLineup = {
  lineupNumber: number;
  /** Concrete construction rules, frozen for constraint-preserving research replay. */
  constructionContract?: CompiledArchetype | null;
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
  diversifiedReport?: DiversifiedReport;
  lineups: NflGeneratedLineup[];
  /** Frozen leader identities: reselection must preserve their automatic coverage. */
  topProjectedPlayerIds?: number[];
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
  resolvedSource: NflProjectionSource | "dk_avg_fallback" | "our_fallback" | "defensive" | "special_teams";
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
  options?: { timeout: number };
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

/**
 * A captain minimum the user set on a Questionable player is his own
 * instruction and outranks the default, as a lock outranks the Doubtful rule.
 * Used after inactives post, when the availability feed still says Q for a
 * player the team has listed active. Doubtful stays blocked.
 */
export function userCaptainOverridesAvailability(player: NflOptimizerPlayer, settings: Pick<NflOptimizerSettings, "exposurePolicies">): boolean {
  if ((player.availabilityStatus ?? "").trim().toUpperCase() !== "QUESTIONABLE") return false;
  return (settings.exposurePolicies ?? []).some((p) => p.playerId === player.dkPlayerId && (p.captain.minPct ?? 0) > 0);
}

function captainBlockedForRun(player: NflOptimizerPlayer, settings: Pick<NflOptimizerSettings, "exposurePolicies">): boolean {
  return captainBlockedByAvailability(player) && !userCaptainOverridesAvailability(player, settings);
}

/** Why a pool player cannot be captain, completing "has a captain minimum, but ...". */
function captainIneligibleReason(player: NflOptimizerPlayer, settings: NflOptimizerSettings): string {
  if (settings.captainPositions && !settings.captainPositions.includes(player.position)) {
    const positions = settings.captainPositions.join("/");
    return settings.buildShape === "winners"
      ? `the Winners shape captains only ${positions}. Clear his CPT range or switch the build shape to Standard`
      : `this build captains only ${positions}. Clear his CPT range`;
  }
  if (captainBlockedForRun(player, settings)) return `his status (${player.availabilityStatus}) keeps him out of the captain slot. Clear his CPT range`;
  return "he cannot be captain in this build. Clear his CPT range";
}

function projectionFor(player: NflOptimizerPlayer, settings: NflOptimizerSettings): { value: number; source: ResolvedPlayer["resolvedSource"] } | null {
  // Fail closed before any source is consulted: no projection exists for a
  // player we have decided is not playing, in any source.
  if (ruledOut(player)) return null;
  if (settings.projectionSource === 'our' && player.specialTeams &&
      (player.position === 'DST' || player.position === 'K')) {
    return { value: player.specialTeams.mean, source: 'special_teams' };
  }
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
  if (resolved?.source === 'special_teams' && player.specialTeams) {
    const baseline=finite(player.ourProj);
    return {version:'nfl-projection-audit-v1',baseline,final:resolved.value,source:resolved.source,
      excluded:settings.excludedPlayerIds.includes(player.dkPlayerId),modelSnapshot:player.specialTeams,
      evidence:player.specialTeams.feature_snapshot,assumption:null,
      rangeMethod:'Saved historical whole-game draws conditioned on a pregame team total and, for DST, opponent history.',
      steps:[{label:'DST/kicker game context',status:'applied',
        points:baseline===null?0:resolved.value-baseline,
        reason:`${player.specialTeams.version}; saved with the projection run.`}]};
  }
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
/** Points per unit of boom rate in the GPP objective (a stated prior; see `boomWeight`). */
export const DEFAULT_BOOM_WEIGHT = 2;
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
  const special=player.resolvedSource === 'special_teams' ? player.specialTeams : null;
  const historical = player.resolvedSource === "our" || player.resolvedSource === "our_fallback";
  const rawBase = settings.mode === "cash"
    ? (special ? special.p10 : defensive ? defensive.p10 : player.resolvedSource === "workload" ? selectedWorkload(player,settings.workloadPositions)!.p10 : player.resolvedSource === "calibrated" ? player.calibrated!.p10 : historical ? finite(player.floorFpts) : null) ?? player.projection * 0.74
    : (special ? special.p90 : defensive ? defensive.p90 : player.resolvedSource === "workload" ? selectedWorkload(player,settings.workloadPositions)!.p90 : player.resolvedSource === "calibrated" ? player.calibrated!.p90 : historical ? finite(player.ceilingFpts) : null) ?? player.projection * 1.28;
  const base = settings.buildShape === "diversified" && settings.diversifiedMeanSearch ? player.projection
    : settings.mode === "gpp" ? cappedCeiling(rawBase, player.projection, settings.maxCeilingMultiple ?? DEFAULT_MAX_CEILING_MULTIPLE) : rawBase;
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
  const boomBonus = settings.mode === "gpp" ? (special ? special.boom : defensive ? defensive.boom : workload && "boom" in workload ? workload.boom : player.resolvedSource === "calibrated" ? player.calibrated!.boom : historical ? finite(player.boomRate) ?? 0 : 0) * (settings.boomWeight ?? DEFAULT_BOOM_WEIGHT) : 0;
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
  if (settings.buildShape != null && !BUILD_SHAPES.includes(settings.buildShape)) throw new Error("Unknown build shape.");
  const share = (value: number | undefined, label: string, allowZero: boolean) => {
    if (value == null) return;
    if (!Number.isFinite(value) || value > 1 || value < 0 || (!allowZero && value === 0)) throw new Error(`${label} must be ${allowZero ? "between 0 and 100%" : "above 0 and at most 100%"}.`);
  };
  share(settings.bringBackShare, "Bring-back share", true);
  share(settings.maxQbExposure, "Quarterback exposure cap", false);
  share(settings.maxCaptainExposure, "Captain exposure cap", false);
  share(settings.maxDstVsOffenseShare, "DST-against-own-players cap", true);
  if (settings.maxDstVsOffenseShare != null && settings.format !== "classic") throw new Error("The DST-against-own-players cap is a Classic rule.");
  if (settings.bringBackShare != null && (settings.format !== "classic" || settings.mode !== "gpp")) throw new Error("A bring-back share is only available in Classic GPP.");
  if ((settings.excludeKickers || settings.forbidFourTwoSplit || settings.maxCaptainExposure != null || settings.captainPositions) && settings.format !== "showdown") throw new Error("Kicker exclusion, the 4-2 ban, the captain cap and captain positions are Showdown rules.");
  if (settings.captainPositions && !settings.captainPositions.length) throw new Error("Captain positions must name at least one position.");
  if (settings.minQbPerLineup != null) {
    if (settings.format !== "showdown") throw new Error("A per-lineup quarterback minimum is a Showdown rule (Classic always has one QB).");
    if (!Number.isInteger(settings.minQbPerLineup) || settings.minQbPerLineup < 0 || settings.minQbPerLineup > 2) throw new Error("The quarterback minimum must be 0, 1 or 2.");
  }
  if (settings.captainStack && settings.format !== "showdown") throw new Error("The captain stack is a Showdown rule.");
  share(settings.twoQbShare, "Two-QB share", true);
  share(settings.maxLoneQbCaptainShare, "Lone-QB captain share", true);
  share(settings.rbCaptainShare, "RB captain share", true);
  share(settings.showdownQbMinCap, "Showdown QB exposure floor", false);
  share(settings.qbDoubleStackShare, "QB double-stack share", true);
  share(settings.maxFourWrShare, "4th-WR share", true);
  if (settings.qbDoubleStackShare != null && (settings.format !== "classic" || settings.mode !== "gpp" || settings.stackPassCatchers < 1))
    throw new Error("A QB double-stack share is a Classic GPP rule and needs at least one stacked pass catcher.");
  if (settings.maxFourWrShare != null && settings.format !== "classic") throw new Error("The 4th-WR share is a Classic rule.");
  share(settings.maxOtherGameStackShare, "Other-game stack share", true);
  if (settings.maxOtherGameStackShare != null && settings.format !== "classic") throw new Error("The other-game stack share is a Classic rule.");
  share(settings.twoTeShare, "Two-TE share", true);
  if (settings.twoTeShare != null && settings.format !== "classic") throw new Error("The two-TE share is a Classic rule.");
  if ((settings.twoQbShare != null || settings.maxLoneQbCaptainShare != null || settings.rbCaptainShare != null || settings.showdownQbMinCap != null) && settings.format !== "showdown")
    throw new Error("The two-QB share, lone-QB captain share, RB captain share and QB exposure floor are Showdown rules.");
}

/**
 * How this lineup treats the opposing bring-back: `default` follows
 * `settings.bringBack`; `require`/`forbid` come from a bring-back SHARE, which
 * alternates them so the run lands on the share rather than on every lineup.
 */
type BringBackRule = "default" | "require" | "forbid";
/** Whether a lineup plays a DST against one of its own QB/RB/WR/TE. */
export function dstFacesOwnOffense(lineup: Pick<NflGeneratedLineup, "slots">): boolean {
  return lineup.slots.some(({ player: defense }) => {
    if (defense.position !== "DST") return false;
    const opponent = defense.gameKey?.split("@").find(team => team !== defense.team);
    return lineup.slots.some(({ player }) => player.team === opponent && ["QB", "RB", "WR", "TE"].includes(player.position));
  });
}
/** Flex slots a lineup can give one quarterback's opponents; the big-M of the forbid constraint. */
const BRING_BACK_BIG_M = 7;

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
  bringBackRule: BringBackRule = "default",
  forbidDstVsOffense = false,
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
  const defenseOpponents = new Map(available.filter(player => player.position === "DST").map(defense => {
    const opponent = defense.gameKey?.split("@").find(team => team !== defense.team);
    if (!opponent) throw new Error(`${defense.name} has no matching game opponent. Refresh the salary slate before building.`);
    return [defense.dkPlayerId, opponent] as const;
  }));
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
    constraints.wr = { min: 3, max: settings.lineupForbidFourWr ? 3 : 4 };
    // No two non-DST players from a team outside the QB's game: for each team,
    // its RB/WR/TE count minus M x (that team's or its opponent's QB rostered) <= 1.
    if (settings.lineupForbidOtherGameStack) for (const team of new Set(available.map((p) => p.team))) constraints[`og_${team}`] = { max: 1 };
    constraints.te = settings.lineupTeExact != null ? { equal: settings.lineupTeExact } : { min: 1, max: 2 };
    constraints.dst = { equal: 1 };
    constraints.flex = { equal: 7 };
  } else {
    constraints.cpt = { equal: 1 };
    constraints.flex = { equal: 5 };
  }
  const saturatedGroups = settings.buildShape === "diversified" && !settings.diversifiedCandidateSearch
    ? [...repeatedGroupCounts(previous).values()].filter(({group,count}) => count >= groupLimit(group, diversifiedPolicy(settings), settings.nLineups)) : [];
  saturatedGroups.forEach(({group}, i) => { constraints[`div_group_${i}`] = { max: group.ids.length - 1 }; });
  if (settings.buildShape === "diversified" && !settings.diversifiedCandidateSearch) {
    const p = diversifiedPolicy(settings);
    const usedQbs = new Set(previous.flatMap(l => l.slots.filter(s => s.player.position === "QB").map(s => s.player.dkPlayerId)));
    if (p.minimumQbs - usedQbs.size >= remaining && p.minimumQbs > usedQbs.size)
      constraints.div_new_qb = { min: 1 };
  }
  for (const player of available) constraints[`player_${player.dkPlayerId}`] = { max: 1 };
  for (const game of new Set(available.map((player) => player.gameKey).filter(Boolean))) {
    constraints[`game_${safe(game!)}`] = { max: settings.format === "classic" ? 8 : 6 };
  }
  if (settings.format === "showdown") {
    for (const team of new Set(available.map((player) => player.team))) constraints[`team_${safe(team)}`] = { max: 5 };
    // A DST may coexist with a small opposing bring-back. It cannot accompany
    // four opposing offensive players, or an opposing offensive Captain plus
    // two teammates. With six slots, 4*DST + offense + offensive CPT <= 7
    // leaves ordinary lineups unrestricted when that DST is absent.
    for (const id of defenseOpponents.keys()) constraints[`dst_script_${id}`] = { max: 7 };
  }
  if (settings.format === "classic") {
    // Nine roster slots: 6*DST + opposing offense <= 9 permits at most three
    // opposing offensive players when the DST is present.
    for (const id of defenseOpponents.keys()) constraints[`dst_script_${id}`] = { max: 9 };
    // DST cap: 9*DST + opposing offense <= 9 allows no opposing QB/RB/WR/TE
    // when the DST is present and leaves lineups without it unrestricted.
    if (forbidDstVsOffense) for (const id of defenseOpponents.keys()) constraints[`dst_block_${id}`] = { max: 9 };
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

  const bringRequired = bringBackRule === "require" || (bringBackRule === "default" && settings.bringBack);
  const bringForbidden = bringBackRule === "forbid";
  if (settings.format === "classic" && settings.mode === "gpp" && settings.stackPassCatchers > 0) {
    for (const quarterback of available.filter((player) => player.position === "QB")) {
      constraints[`stack_${quarterback.dkPlayerId}`] = { min: 0 };
      // own WR/TE + M*qb <= cap + M: with this quarterback rostered, at most `cap` of his pass catchers.
      if (settings.lineupStackMax != null) constraints[`stackmax_${quarterback.dkPlayerId}`] = { max: settings.lineupStackMax + STACK_MAX_BIG_M };
      if (bringRequired) constraints[`bring_${quarterback.dkPlayerId}`] = { min: 0 };
      // opponents + M*qb <= M: with the quarterback rostered, no opposing RB/WR/TE.
      if (bringForbidden) constraints[`nobring_${quarterback.dkPlayerId}`] = { max: BRING_BACK_BIG_M };
      constraints[`anti_${quarterback.dkPlayerId}`] = { max: 1 };
    }
  }
  // Winners shape (Showdown): the lineup is 5-1 or 3-3 by team, never 4-2.
  // One binary per allowed count of the first team (the other team is 6 minus
  // it); exactly one is on and the team's roster count must equal it.
  const splitTeam = settings.format === "showdown" && settings.forbidFourTwoSplit
    ? [...new Set(available.map((player) => player.team))].sort()[0] ?? null : null;
  if (splitTeam) {
    constraints.split_pick = { equal: 1 };
    constraints.split_match = { equal: 0 };
  }
  // Showdown: at least N quarterbacks per lineup, Captain or Flex. A lineup
  // may carry an extra quarterback only while enough quarterback appearances
  // remain under the exposure caps for every later lineup to have its
  // minimum (otherwise early two-QB lineups spend the caps and the last
  // lineups have none). An archetype that fades the quarterbacks away is
  // refused (null), so the run falls back to a standard lineup; if the caps
  // themselves leave too few appearances the rule is skipped here and the
  // run's warnings count the short lineups.
  // Showdown captain stack, one pair of rows per team: own QBs rostered minus
  // that team's WR/TE captain >= 0, and own WR/TE in Flex minus that team's
  // QB captain >= 0 (only one captain exists, so each row binds only when
  // its captain is chosen).
  const captainStack = settings.format === "showdown" && settings.captainStack === true;
  if (captainStack) {
    for (const team of new Set(available.map((player) => player.team))) {
      constraints[`cstack_qb_${team}`] = { min: 0 };
      constraints[`cstack_pc_${team}`] = { min: 0 };
    }
  }
  const minQb = settings.format === "showdown" ? settings.minQbPerLineup ?? 0 : 0;
  if (minQb > 0) {
    const qbSlotsLeft = pool.filter((player) => player.position === "QB")
      .reduce((sum, qb) => sum + Math.max(0, maxCount(qb) - (exposureCounts.get(qb.dkPlayerId) ?? 0)), 0);
    const playableQbs = available.filter((player) => player.position === "QB").length;
    if (settings.lineupQbExact != null) {
      if (playableQbs < settings.lineupQbExact) return null;
      constraints.min_qb = { equal: settings.lineupQbExact };
    } else if (playableQbs >= minQb) {
      // Two spare appearances are kept back so the last lineups still have a
      // choice of quarterback; with exactly one left, uniqueness and salary can
      // leave the final lineup unbuildable.
      const extraAllowed = qbSlotsLeft - (minQb + 1) >= (remaining - 1) * minQb + QB_RESERVE_SLACK;
      constraints.min_qb = extraAllowed ? { min: minQb } : { min: minQb, max: minQb };
    } else if (qbSlotsLeft >= remaining * minQb) {
      return null;
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
      // Reserve overall appearances for the other slot's outstanding minimum.
      // Spending Olave's overall cap on FLEX used to make his Captain target
      // unreachable before the end-of-run minimum constraints were attempted.
      if (settings.format === 'showdown') {
        const counts = countsById.get(player.dkPlayerId);
        const availableOverall = maxCount(player) - (exposureCounts.get(player.dkPlayerId) ?? 0);
        const otherSlotOwed = purchaseType === 'FLEX'
          ? (counts?.captainMin ?? 0) - (captainCounts.get(player.dkPlayerId) ?? 0)
          : (counts?.flexMin ?? 0) - (flexCounts.get(player.dkPlayerId) ?? 0);
        if (otherSlotOwed > 0 && availableOverall <= otherSlotOwed) continue;
      }
      // Phase 4: archetype captain restrictions (contrarian / underdog captain
      // sets, forbidden chalk captains).
      if (purchaseType === "CPT" && compiled?.eligibleCaptainIds && !compiled.eligibleCaptainIds.includes(player.dkPlayerId)) continue;
      if (purchaseType === "CPT" && compiled?.forbiddenCaptainIds.includes(player.dkPlayerId)) continue;
      // A captain minimum the user set on this quarterback outranks the lone-QB cap.
      if (purchaseType === "CPT" && settings.lineupForbidQbCaptain && player.position === "QB"
        && (captainCounts.get(player.dkPlayerId) ?? 0) >= (countsById.get(player.dkPlayerId)?.captainMin ?? 0)) continue;
      if (purchaseType === "CPT" && settings.lineupRequireRbCaptain && player.position !== "RB") continue;
      if (purchaseType === "CPT" && settings.lineupForbidRbCaptain && player.position === "RB") continue;
      const key = `${purchaseType === "CLASSIC" ? "x" : purchaseType === "CPT" ? "c" : "f"}_${player.dkPlayerId}`;
      const slot = purchaseType === "CPT" ? "CPT" : purchaseType === "FLEX" ? "FLEX" : "CLASSIC";
      const multiplier = slot === "CPT" ? 1.5 : 1;
      const salary = settings.format === "showdown" ? showdownSalary(player, slot === "CPT") : player.salary;
      const variable: Record<string, number> = {
        score: objective(player, settings, lineupNumber, slot) * multiplier - (settings.buildShape === "diversified"
          ? concentrationCost((exposureCounts.get(player.dkPlayerId) ?? 0) + 1, settings) - concentrationCost(exposureCounts.get(player.dkPlayerId) ?? 0, settings) : 0),
        salary,
        [`player_${player.dkPlayerId}`]: 1,
      };
      saturatedGroups.forEach(({group}, i) => {
        const membership = group.ids.includes(player.dkPlayerId) ? 1 : 0;
        const exemption = group.exemptTeam === player.team && player.position === "QB" ? group.ids.length : 0;
        if (membership || exemption) variable[`div_group_${i}`] = membership - exemption;
      });
      if (constraints.div_new_qb && player.position === "QB" && !previous.some(l => l.playerIds.includes(player.dkPlayerId))) variable.div_new_qb = 1;
      // Count this pick against the per-lineup salary-relief cap.
      if (settings.puntPolicy && player.salaryRelief) variable.salary_relief = 1;
      if (constraints.gpp_opportunity_signal && isNflGppSignalPlayer(player.playerSignals, settings.gppSignalCodes)) variable.gpp_opportunity_signal = 1;
      if (constraints.air_matchup && isNflGppSignalPlayer(player.playerSignals, ["AIR_MATCHUP"])) variable.air_matchup = 1;
      if (constraints.goal_line_rb && player.position === "RB" && isNflGppSignalPlayer(player.playerSignals, ["INSIDE_FIVE"])) variable.goal_line_rb = 1;
      if (constraints.min_qb && player.position === "QB") variable.min_qb = 1;
      if (captainStack) {
        const passCatcher = player.position === "WR" || player.position === "TE";
        if (player.position === "QB") variable[`cstack_qb_${player.team}`] = 1;
        if (slot === "CPT" && passCatcher) variable[`cstack_qb_${player.team}`] = -1;
        if (slot === "FLEX" && passCatcher) variable[`cstack_pc_${player.team}`] = 1;
        if (slot === "CPT" && player.position === "QB") variable[`cstack_pc_${player.team}`] = -1;
      }
      if (settings.format === "classic" && settings.lineupForbidOtherGameStack) {
        if (["RB", "WR", "TE"].includes(player.position)) variable[`og_${player.team}`] = 1;
        if (player.position === "QB") {
          variable[`og_${player.team}`] = -OTHER_GAME_BIG_M;
          if (player.opponent) variable[`og_${player.opponent}`] = -OTHER_GAME_BIG_M;
        }
      }
      if (settings.format === "classic") {
        variable.roster = 1;
        variable[player.position.toLowerCase()] = 1;
        if (["RB", "WR", "TE"].includes(player.position)) variable.flex = 1;
      } else {
        variable[slot === "CPT" ? "cpt" : "flex"] = 1;
      }
      if (settings.format === "showdown") variable[`team_${safe(player.team)}`] = 1;
      if (splitTeam && player.team === splitTeam) variable.split_match = 1;
      for (const [defenseId, opponent] of defenseOpponents) {
        const key = `dst_script_${defenseId}`;
        if (player.dkPlayerId === defenseId) variable[key] = settings.format === "showdown" ? 4 : 6;
        else if (player.team === opponent && ["QB", "RB", "WR", "TE"].includes(player.position))
          variable[key] = slot === "CPT" ? 2 : 1;
        const block = `dst_block_${defenseId}`;
        if (constraints[block]) {
          if (player.dkPlayerId === defenseId) variable[block] = 9;
          else if (player.team === opponent && ["QB", "RB", "WR", "TE"].includes(player.position)) variable[block] = 1;
        }
      }
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
        // Make progress on Captain commitments while compatible archetypes
        // remain, instead of deferring them to the final restricted lineups.
        // If this archetype cannot accommodate one, the existing retry builds
        // it without this progress constraint and the final audit stays honest.
        if (slot === 'CPT' && owed > 0) {
          constraints.captain_target_progress = { min: 1 };
          variable.captain_target_progress = 1;
          slotForced = true;
        }
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
            variable[`stack_${quarterback.dkPlayerId}`] = -(settings.lineupStackMin ?? settings.stackPassCatchers);
            if (settings.lineupStackMax != null) variable[`stackmax_${quarterback.dkPlayerId}`] = STACK_MAX_BIG_M;
            if (bringRequired) variable[`bring_${quarterback.dkPlayerId}`] = -1;
            if (bringForbidden) variable[`nobring_${quarterback.dkPlayerId}`] = BRING_BACK_BIG_M;
            variable[`anti_${quarterback.dkPlayerId}`] = 1;
          }
          if (["WR", "TE"].includes(player.position) && player.team === quarterback.team) {
            variable[`stack_${quarterback.dkPlayerId}`] = 1;
            if (settings.lineupStackMax != null) variable[`stackmax_${quarterback.dkPlayerId}`] = 1;
          }
          if (bringRequired && ["RB", "WR", "TE"].includes(player.position) && player.team === quarterback.opponent) variable[`bring_${quarterback.dkPlayerId}`] = 1;
          if (bringForbidden && ["RB", "WR", "TE"].includes(player.position) && player.team === quarterback.opponent) variable[`nobring_${quarterback.dkPlayerId}`] = 1;
          if (player.position === "DST" && player.team === quarterback.opponent) variable[`anti_${quarterback.dkPlayerId}`] = 1;
        }
      }
      variables[key] = variable;
      binaries[key] = 1;
    }
  }
  if (splitTeam) {
    for (const count of [1, 3, 5]) {
      variables[`split_${count}`] = { score: 0, split_pick: 1, split_match: -count };
      binaries[`split_${count}`] = 1;
    }
  }
  const solved = solver.Solve({ optimize: "score", opType: "max", constraints, variables, binaries,
    ...(settings.buildShape === "diversified" ? { options: { timeout: settings.diversifiedCandidateSearch ? 150 : 1000 } } : {}) });
  if (solved.feasible === false) {
    // Forcing slot minimums made this lineup impossible (salary, team caps,
    // overlap). Build it without them rather than ending the run early; the
    // exposure report still flags the missed minimum honestly.
    // A Showdown distribution directive yields first: return null so the run
    // retries this lineup with the directive relaxed and the user's slot
    // minimums still forced. Only the plain build drops the minimums.
    const directed = settings.lineupQbExact != null || settings.lineupForbidQbCaptain || settings.lineupRequireRbCaptain || settings.lineupForbidRbCaptain
      || settings.lineupStackMin != null || settings.lineupStackMax != null || settings.lineupForbidFourWr || settings.lineupForbidOtherGameStack
      || settings.lineupTeExact != null;
    if (slotForced && directed) return null;
    if (slotForced) return buildOne(pool, settings, lineupNumber, previous, exposureCounts, forcedIds, countsById, captainCounts, flexCounts, compiled, remaining, false, requireAirMatchup, requireGoalLine, bringBackRule, forbidDstVsOffense);
    return null;
  }
  if (settings.buildShape === "diversified" && Object.entries(solved).some(([key,value]) => /^[xcf]_/.test(key)
    && typeof value === 'number' && Math.abs(value-Math.round(value))>1e-6)) return null;
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
    if (settings.buildShape === "diversified" && (purchases.length!==9 || qb.length!==1 || rb.length<2 || wr.length<3 || te.length<1 || dst.length!==1)) return null;
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
  else assertDstGameScript("classic", chosen);
  const qb = chosen.find((entry) => entry.player.position === "QB")?.player ?? null;
  const passCatchers = qb ? chosen.filter((entry) => ["WR", "TE"].includes(entry.player.position) && entry.player.team === qb.team).map((entry) => entry.player.name) : [];
  const bringBack = qb ? chosen.find((entry) => ["RB", "WR", "TE"].includes(entry.player.position) && entry.player.team === qb.opponent)?.player.name ?? null : null;
  return {
    lineupNumber,
    constructionContract: compiled,
    slots: chosen,
    playerIds: chosen.map((entry) => entry.player.dkPlayerId),
    totalSalary: chosen.reduce((sum, entry) => sum + entry.salary, 0),
    projectedFpts: chosen.reduce((sum, entry) => sum + entry.projection, 0),
    floorFpts: chosen.reduce((sum, entry) => sum + (entry.projectionSource==='special_teams' ? entry.player.specialTeams!.p10 : entry.projectionSource==='defensive' ? entry.player.defensiveForecast!.selected.p10 : entry.projectionSource === "workload" ? selectedWorkload(entry.player,settings.workloadPositions)!.p10 : entry.projectionSource === "calibrated" ? entry.player.calibrated!.p10 : entry.projectionSource === "our" || entry.projectionSource === "our_fallback" ? entry.player.floorFpts ?? entry.projection / entry.multiplier * .74 : entry.projection / entry.multiplier * .74) * entry.multiplier, 0),
    ceilingFpts: chosen.reduce((sum, entry) => sum + (entry.projectionSource==='special_teams' ? entry.player.specialTeams!.p90 : entry.projectionSource==='defensive' ? entry.player.defensiveForecast!.selected.p90 : entry.projectionSource === "workload" ? selectedWorkload(entry.player,settings.workloadPositions)!.p90 : entry.projectionSource === "calibrated" ? entry.player.calibrated!.p90 : entry.projectionSource === "our" || entry.projectionSource === "our_fallback" ? entry.player.ceilingFpts ?? entry.projection / entry.multiplier * 1.28 : entry.projection / entry.multiplier * 1.28) * entry.multiplier, 0),
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

/** Big-M for the per-lineup stack maximum: larger than any team's WR/TE count in one lineup. */
const STACK_MAX_BIG_M = 9;
/** Big-M for the other-game stack rule: larger than any team's RB/WR/TE count in one lineup. */
const OTHER_GAME_BIG_M = 9;
/** Spare quarterback appearances held back for the last lineups under a Showdown QB minimum. */
const QB_RESERVE_SLACK = 2;

/**
 * The portfolio-rule features of one lineup: each player's appearance (and, in
 * Showdown, his slot), the distribution flags the run schedules, and the
 * archetype label. The joint selector constrains their totals; the same
 * definitions recount the final portfolio, so the two cannot disagree.
 */
export function portfolioFeatures(lineup: Pick<NflGeneratedLineup, "slots" | "archetype" | "stackSummary">, format: "classic" | "showdown"): Record<string, number> {
  const f: Record<string, number> = {};
  for (const { slot, player } of lineup.slots) {
    f[`o_${player.dkPlayerId}`] = 1;
    if (format === "showdown") f[`${slot === "CPT" ? "c" : "f"}_${player.dkPlayerId}`] = 1;
  }
  if (format === "showdown") {
    const qbCount = lineup.slots.filter(({ player }) => player.position === "QB").length;
    const captain = lineup.slots.find(({ slot }) => slot === "CPT")?.player;
    if (qbCount >= 2) f.twoQb = 1;
    if (captain?.position === "QB" && qbCount === 1) f.loneQbCpt = 1;
    if (captain?.position === "RB") f.rbCpt = 1;
  } else {
    const qb = lineup.slots.find(({ player }) => player.position === "QB")?.player;
    if (qb && lineup.slots.filter(({ player }) => ["WR", "TE"].includes(player.position) && player.team === qb.team).length >= 2) f.dbl = 1;
    if (lineup.slots.filter(({ player }) => player.position === "WR").length >= 4) f.fourWr = 1;
    if (lineup.slots.filter(({ player }) => player.position === "TE").length >= 2) f.twoTe = 1;
    if (qb) {
      const counts = new Map<string, number>();
      for (const { player } of lineup.slots) if (["RB", "WR", "TE"].includes(player.position)) counts.set(player.team, (counts.get(player.team) ?? 0) + 1);
      if ([...counts].some(([team, k]) => k >= 2 && team !== qb.team && team !== qb.opponent)) f.otherGame = 1;
    }
    if (lineup.stackSummary.bringBack) f.bringBack = 1;
  }
  if (dstFacesOwnOffense(lineup)) f.dstVsOff = 1;
  if (lineup.slots.some(({ player }) => isNflGppSignalPlayer(player.playerSignals, ["AIR_MATCHUP"]))) f.air = 1;
  if (lineup.slots.some(({ player }) => player.position === "RB" && isNflGppSignalPlayer(player.playerSignals, ["INSIDE_FIVE"]))) f.goal = 1;
  if (lineup.archetype) f[`arch_${lineup.archetype.id}`] = 1;
  return f;
}

/** Time spent building extra candidates, and the selection solve's limit. */
const JOINT_CANDIDATE_BUDGET_MS = 8000;
const JOINT_SOLVE_TIMEOUT_MS = 6000;

type JointTargets = {
  twoQbShare: number | null; rbCaptainShare: number | null; loneQbCaptainCap: number | null;
  doubleStackShare: number | null; fourWrCap: number | null; otherGameCap: number | null; twoTeShare: number | null;
  bringBackShare: number | null; dstVsOffenseCap: number | null; airMatchupTarget: number; goalLineTarget: number;
};

/**
 * Build extra candidates without spending exposure, then choose the portfolio
 * from them and the sequential build together. Returns null lineups when the
 * sequential build is kept, with the reason in the note.
 */
function jointSelect(ctx: {
  pool: ResolvedPlayer[]; settings: NflOptimizerSettings; plan: Array<{ archetypeId: ArchetypeId; compiled: CompiledArchetype | null }>;
  lineups: NflGeneratedLineup[]; countsById: Map<number, ExposureCounts>; locked: Set<number>;
  topProjectedTargets: ResolvedPlayer[]; targets: JointTargets;
}): { lineups: NflGeneratedLineup[] | null; note: string } {
  const { pool, settings, plan, lineups, countsById, locked, targets: t } = ctx;
  const n = lineups.length;
  const showdownFormat = settings.format === "showdown";
  const started = Date.now();
  const maxCandidates = Math.min(400, Math.max(60, n * 8));
  const prior = [...lineups];
  const candidates: NflGeneratedLineup[] = [];
  const empty = new Map<number, number>();
  // Each candidate is anchored on one captain (Showdown) or quarterback
  // (Classic), cycling through all of them, so every combination the rules
  // allow gets its own chance rather than whatever exposure is left over.
  const anchors = pool.filter((p) => showdownFormat ? p.captainEligible : p.position === "QB")
    .sort((a, b) => b.projection - a.projection || a.dkPlayerId - b.dkPlayerId);
  let timedOut = false;
  for (let k = 0; anchors.length && candidates.length < maxCandidates && k < maxCandidates * 3; k++) {
    if (Date.now() - started > JOINT_CANDIDATE_BUDGET_MS) { timedOut = true; break; }
    const anchor = anchors[k % anchors.length];
    const step = plan[k % plan.length];
    const bit = (d: number) => jitter(20261023, k, d) > 0;
    const directive: Partial<NflOptimizerSettings> = {};
    if (showdownFormat) {
      if (t.twoQbShare != null && (settings.minQbPerLineup ?? 0) >= 1) directive.lineupQbExact = bit(0) ? 2 : 1;
    } else {
      if (t.doubleStackShare != null) Object.assign(directive, bit(0) ? { lineupStackMin: 2 } : { lineupStackMin: 1, lineupStackMax: 1 });
      if (t.twoTeShare != null) directive.lineupTeExact = bit(1) ? 2 : 1;
      if (t.fourWrCap != null && bit(2)) directive.lineupForbidFourWr = true;
      if (t.otherGameCap != null && bit(3)) directive.lineupForbidOtherGameStack = true;
    }
    const bringRule: BringBackRule = t.bringBackShare == null ? "default" : bit(4) ? "require" : "forbid";
    const forbidDst = t.dstVsOffenseCap != null && bit(5);
    const ls: NflOptimizerSettings = { ...settings, ...directive };
    // Showdown: every other captain's slot is marked full, so only the anchor can captain.
    const captainCounts = showdownFormat
      ? new Map(pool.filter((p) => p.dkPlayerId !== anchor.dkPlayerId).map((p) => [p.dkPlayerId, Number.MAX_SAFE_INTEGER]))
      : empty;
    const forced = showdownFormat ? new Set(locked) : new Set([...locked, anchor.dkPlayerId]);
    const build = (compiled: CompiledArchetype | null) => buildOne(pool, ls, 1_000_000 + k, prior, empty, forced, countsById,
      captainCounts, empty, compiled, 1, false, false, false, bringRule, forbidDst);
    const built = build(step.compiled) ?? (step.compiled ? build(null) : null);
    if (built) { candidates.push(built); prior.push(built); }
  }
  const considered = n + candidates.length;
  if (!candidates.length) return { lineups: null, note: `Joint selection (${NFL_JOINT_SELECTION_VERSION}) kept the first-pass portfolio: no extra lineup could be built under the rules.` };

  // One scale for every candidate: the build objective without per-lineup jitter.
  const byId = new Map(pool.map((p) => [p.dkPlayerId, p]));
  const scoreSettings = { ...settings, randomness: 0 };
  const scoreOf = (lineup: NflGeneratedLineup) => lineup.slots.reduce((sum, { slot, player, multiplier }) =>
    sum + objective(byId.get(player.dkPlayerId)!, scoreSettings, 0, showdownFormat ? (slot === "CPT" ? "CPT" : "FLEX") : "CLASSIC") * multiplier, 0);
  const all = [...lineups.map((l, i) => ({ key: `b_${i}`, lineup: l })), ...candidates.map((l, i) => ({ key: `n_${i}`, lineup: l }))];
  const jointCandidates: JointCandidate[] = all.map(({ key, lineup }) => ({ key, score: scoreOf(lineup), features: portfolioFeatures(lineup, settings.format) }));

  // A player's exposure caps are hard, as in the sequential build, which never
  // exceeds one (a lock or forced minimum can; the cap then widens to what the
  // first pass used, so it stays a valid answer). Minimums are targets.
  const targets: Record<string, JointTarget> = {};
  const used = featureTotals(jointCandidates, jointCandidates.slice(0, n).map((c) => c.key));
  const capAndFloor = (feature: string, min: number, max: number) => {
    targets[feature] = { max: Math.max(max, used[feature] ?? 0), hard: true };
    if (min > 0) targets[`min_${feature}`] = { feature, min };
  };
  for (const p of pool) {
    const id = p.dkPlayerId;
    const c = countsById.get(id)!;
    if (locked.has(id)) { targets[`o_${id}`] = { min: n, max: n, hard: true }; continue; }
    capAndFloor(`o_${id}`, c.overallMin, c.overallMax);
    if (showdownFormat) { capAndFloor(`c_${id}`, c.captainMin, c.captainMax); capAndFloor(`f_${id}`, c.flexMin, c.flexMax); }
  }
  for (const p of ctx.topProjectedTargets) {
    const id = p.dkPlayerId;
    if (!locked.has(id)) targets[`min_o_${id}`] = { feature: `o_${id}`, min: Math.max(1, targets[`min_o_${id}`]?.min ?? 0) };
  }
  const exact = (name: string, share: number | null) => { if (share != null) { const r = Math.round(share * n); targets[name] = { min: r, max: r }; } };
  exact("twoQb", t.twoQbShare); exact("rbCpt", t.rbCaptainShare); exact("dbl", t.doubleStackShare); exact("twoTe", t.twoTeShare); exact("bringBack", t.bringBackShare);
  if (t.loneQbCaptainCap != null) targets.loneQbCpt = { max: t.loneQbCaptainCap };
  if (t.fourWrCap != null) targets.fourWr = { max: t.fourWrCap };
  if (t.otherGameCap != null) targets.otherGame = { max: t.otherGameCap };
  if (t.dstVsOffenseCap != null) targets.dstVsOff = { max: t.dstVsOffenseCap };
  if (t.airMatchupTarget) targets.air = { min: t.airMatchupTarget };
  if (t.goalLineTarget) targets.goal = { min: t.goalLineTarget };
  if (plan.some((step) => step.compiled)) {
    const requested = new Map<string, number>();
    for (const step of plan) requested.set(step.archetypeId, (requested.get(step.archetypeId) ?? 0) + 1);
    for (const [id, count] of requested) targets[`arch_${id}`] = { min: count, max: count };
  }

  const result = selectJointPortfolio({ candidates: jointCandidates, count: n, targets,
    incumbent: jointCandidates.slice(0, n).map((c) => c.key), timeoutMs: JOINT_SOLVE_TIMEOUT_MS });
  const scoreByKey = new Map(jointCandidates.map((c) => [c.key, c.score]));
  const scoreSum = (keys: string[]) => keys.reduce((sum, key) => sum + scoreByKey.get(key)!, 0);
  const nameOf = (id: string) => byId.get(Number(id))?.name ?? `#${id}`;
  const describe = (name: string): string => {
    const [kind, id] = name.replace(/^min_/, "").split(/_(.+)/);
    if (kind === "o") return `${nameOf(id)} exposure`;
    if (kind === "c") return `${nameOf(id)} captain exposure`;
    if (kind === "f") return `${nameOf(id)} flex exposure`;
    if (kind === "arch") return `${ARCHETYPE_LABELS[id as ArchetypeId] ?? id} quota`;
    return ({ twoQb: "both-QB share", loneQbCpt: "lone-QB captain cap", rbCpt: "RB captain share", dbl: "QB double-stack share",
      fourWr: "4th-WR cap", otherGame: "other-game stack cap", twoTe: "2nd-TE share", bringBack: "bring-back share",
      dstVsOff: "DST-against-own-players cap", air: "air-yard matchup minimum", goal: "goal-line RB minimum" } as Record<string, string>)[name] ?? name;
  };
  const misses = Object.entries(result.misses);
  const missText = misses.length ? ` Still missed: ${misses.map(([name, miss]) => `${describe(name)} by ${miss} lineup${miss === 1 ? "" : "s"}`).join("; ")}.` : "";
  const timeText = timedOut ? " The candidate search stopped at its time limit." : "";
  if (result.solver === "incumbent") {
    return { lineups: null, note: `Joint selection (${NFL_JOINT_SELECTION_VERSION}) kept the first-pass portfolio: ${result.reason}. ${considered} valid lineups considered.${timeText}${missText}` };
  }
  const chosen = result.selected
    .map((key) => ({ key, lineup: all.find((entry) => entry.key === key)!.lineup, score: scoreByKey.get(key)! }))
    .sort((a, b) => b.score - a.score || a.key.localeCompare(b.key));
  const replaced = chosen.filter(({ key }) => key.startsWith("n_")).length;
  const incumbentKeys = jointCandidates.slice(0, n).map((c) => c.key);
  return {
    lineups: chosen.map(({ lineup }, index) => ({ ...lineup, lineupNumber: index + 1 })),
    note: `Joint selection (${NFL_JOINT_SELECTION_VERSION}): chose the ${n} lineups together from ${considered} valid lineups; ${replaced} replaced first-pass lineups. `
      + `Model score ${scoreSum(incumbentKeys).toFixed(1)} -> ${scoreSum(result.selected).toFixed(1)}`
      + `${result.incumbentValue < scoreSum(incumbentKeys) - 1e-6 ? `, and portfolio-rule misses fell from ${Math.round((scoreSum(incumbentKeys) - result.incumbentValue) / JOINT_TARGET_PENALTY)} to ${Math.round((scoreSum(result.selected) - result.value) / JOINT_TARGET_PENALTY)} lineups` : ""}.${timeText}${missText}`,
  };
}

function diversifiedSelect(ctx: {pool: ResolvedPlayer[]; settings: NflOptimizerSettings; lineups: NflGeneratedLineup[];
  countsById: Map<number, ExposureCounts>; locked: Set<number>; topProjectedTargets: ResolvedPlayer[]}) {
  const { pool, settings, countsById, locked, topProjectedTargets } = ctx;
  const policy = diversifiedPolicy(settings), n = settings.nLineups, empty = new Map<number, number>();
  const overlapLimit = Math.min(9-settings.minUnique, settings.maxPairwiseOverlap ?? 9);
  const scoreSettings = {...settings, randomness: 0, diversifiedMeanSearch: false};
  const scores = new WeakMap<NflGeneratedLineup, number>();
  const score = (l: NflGeneratedLineup) => {
    if (!scores.has(l)) scores.set(l, l.slots.reduce((s,e) => s+objective(pool.find(p=>p.dkPlayerId===e.player.dkPlayerId)!,scoreSettings,0),0));
    return scores.get(l)!;
  };
  const lockedQb = pool.find(p=>p.position==='QB' && locked.has(p.dkPlayerId));
  const qbs = pool.filter(p=>p.position==='QB' && (!lockedQb || p.dkPlayerId===lockedQb.dkPlayerId) && (countsById.get(p.dkPlayerId)?.overallMax??0)>0).sort((a,b)=>b.projection-a.projection||a.dkPlayerId-b.dkPlayerId);
  const valid = (ls: NflGeneratedLineup[]) => {
    if (ls.length!==n || !diversifiedCompositionValid(ls, settings)) return false;
    if (ls.some(l=>l.playerIds.length!==9 || new Set(l.playerIds).size!==9 || l.slots.length!==9
      || l.totalSalary>50_000 || l.totalSalary<settings.minSalary || l.slots.some(s=>s.player.isOut)
      || l.totalSalary!==l.slots.reduce((sum,s)=>sum+s.salary,0))) return false;
    const counts = new Map<number,number>();
    for (const l of ls) for (const id of l.playerIds) counts.set(id,(counts.get(id)??0)+1);
    for (const [id,c] of countsById) {
      const count=counts.get(id)??0;
      if (count<c.overallMin || (!locked.has(id) && count>c.overallMax)) return false;
    }
    if ([...locked].some(id=>counts.get(id)!==n) || topProjectedTargets.some(p=>!counts.get(p.dkPlayerId))) return false;
    if (qbs.filter(p=>counts.has(p.dkPlayerId)).length<policy.minimumQbs) return false;
    for (let i=0;i<ls.length;i++) for (let j=i+1;j<ls.length;j++)
      if (ls[i].playerIds.filter(id=>ls[j].playerIds.includes(id)).length>overlapLimit) return false;
    if ([...repeatedGroupCounts(ls).values()].some(({group,count})=>count>groupLimit(group,policy,n))) return false;
    if (ls.some(dstFacesOwnOffense)) return false;
    if (settings.salaryPolicy && reportSalaryBands(settings.salaryPolicy,ls.map(l=>NFL_SALARY_CAP-l.totalSalary)).some(b=>!b.withinPlan)) return false;
    const tagged=(l:NflGeneratedLineup,codes: Parameters<typeof isNflGppSignalPlayer>[1],rb=false)=>l.slots.some(s=>(!rb||s.player.position==='RB')&&isNflGppSignalPlayer(s.player.playerSignals,codes));
    return ls.filter(l=>tagged(l,['AIR_MATCHUP'])).length>=Math.ceil(n*(settings.gppAirMatchupMinPct??0)/100)
      && ls.filter(l=>tagged(l,['INSIDE_FIVE'],true)).length>=Math.ceil(n*(settings.gppGoalLineMinPct??0)/100);
  };
  const result = improveDiversifiedPortfolio({initial:ctx.lineups,settings,score,valid,
    generate:(attempt,known)=>{
      const qb=qbs[attempt%qbs.length];
      if (!qb) return null;
      const forced=new Set([...locked,qb.dkPlayerId]);
      const variant=Math.floor(attempt/qbs.length)%4;
      const search={...settings,minUnique:1,maxPairwiseOverlap:8,randomness:Math.max(settings.randomness,.25),
        diversifiedCandidateSearch:true, diversifiedMeanSearch:variant===0, lineupStackMin:variant===2?2 as const:undefined};
      // Exposure and repeated-group budgets belong to the final 50, not the candidate bank.
      return buildOne(pool,search,100_000+attempt,known,empty,forced,countsById,empty,empty,null,n,true,false,false,
        variant===1?'require':'default',true);
    }});
  if (!valid(result.lineups)) throw new Error(`Diversified could not produce a complete portfolio satisfying every Winners composition target, exposure, combination, uniqueness and coverage rule (${result.lineups.length}/${n} lineups). Adjust limits or exclusions and retry.`);
  const counts=new Map<number,number>();
  for (const l of result.lineups) for (const id of l.playerIds) counts.set(id,(counts.get(id)??0)+1);
  const byId=new Map(pool.map(p=>[p.dkPlayerId,p]));
  const report: DiversifiedReport={version:DIVERSIFIED_VERSION,candidateTarget:Math.max(n,policy.candidateTarget),candidates:result.candidates,
    attempts:result.attempts,candidateKeys:result.candidateKeys,stopReason:result.stopReason,swaps:result.swaps,requested:n,generated:result.lineups.length,
    composition:diversifiedComposition(result.lineups),minimumQbs:policy.minimumQbs,distinctQbs:qbs.filter(p=>counts.has(p.dkPlayerId)).length,
    meanProjection:result.lineups.reduce((s,l)=>s+l.projectedFpts,0)/n,playersAt40Pct:[...counts.values()].filter(c=>c/n>=.4).length,
    repeated:[...repeatedGroupCounts(result.lineups).values()].sort((a,b)=>b.count-a.count||a.group.key.localeCompare(b.group.key)).slice(0,12)
      .map(({group,count})=>({names:group.ids.map(id=>byId.get(id)!.name),kind:group.kind,count,limit:groupLimit(group,policy,n)})),
    caps:pool.filter(p=>counts.has(p.dkPlayerId)).map(p=>({name:p.name,position:p.position,count:counts.get(p.dkPlayerId)!,
      limit:locked.has(p.dkPlayerId)?n:countsById.get(p.dkPlayerId)!.overallMax,
      override:locked.has(p.dkPlayerId)||settings.maxExposureByPlayer[String(p.dkPlayerId)]!=null||!!settings.exposurePolicies?.some(e=>e.playerId===p.dkPlayerId)}))};
  const top=report.caps.sort((a,b)=>b.count-a.count||a.name.localeCompare(b.name)).slice(0,8);
  return {lineups:result.lineups,report,note:`Diversified (${DIVERSIFIED_VERSION}): ${result.candidates}/${report.candidateTarget} distinct legal candidates, ${result.attempts} attempts; stopped at ${result.stopReason.replaceAll('_',' ')}. ${result.swaps} feasible roster swaps. ${report.distinctQbs} QBs; ${report.playersAt40Pct} players at 40%+; mean projection ${report.meanProjection.toFixed(2)}. Ownership influence off; concentration penalty starts above ${Math.round(policy.softStart*100)}%. Risk controls, not a validated performance improvement. Leading exposures: ${top.map(p=>`${p.name} ${p.count}/${n} (cap ${p.limit}${p.override?', player override':''})`).join('; ')}. Repeated combinations: ${report.repeated.slice(0,4).map(g=>`${g.names.join(' + ')} ${g.count}/${n} (cap ${g.limit})`).join('; ')}.`};
}

export function optimizeNflLineups(players: NflOptimizerPlayer[], requested: NflOptimizerSettings): NflOptimizerResult {
  validateSettings(requested);
  // The winners-shape preset is resolved into ordinary settings here, so the
  // rest of the build (and the saved run) sees only numbers it can report.
  requested = applyDiversified(requested);
  const shape = requested.buildShape === "winners" ? applyWinnersShape(requested, players) : null;
  const settings = shape ? shape.settings : requested;
  if (shape) validateSettings(settings);
  const shapeExcluded = shape?.excluded ?? new Map<number, string>();
  const excluded = new Set(settings.excludedPlayerIds);
  const locked = new Set(settings.lockedPlayerIds);
  const policy = settings.puntPolicy;
  const overrides = settings.puntOverrides ?? [];
  if (settings.buildShape === "diversified") {
    const policy = diversifiedPolicy(settings);
    const lockedGroups = diversifiedGroups({slots:players.filter(p=>locked.has(p.dkPlayerId)).map(player=>({player}))});
    const conflict = lockedGroups.find(group=>groupLimit(group,policy,settings.nLineups)<settings.nLineups);
    if (conflict) throw new Error(`Locked combination ${conflict.ids.map(id=>players.find(p=>p.dkPlayerId===id)?.name??id).join(' + ')} would appear in every lineup, exceeding its Diversified cap of ${groupLimit(conflict,policy,settings.nLineups)}. Unlock a player or raise the combination cap.`);
  }

  // Legacy fallbacks (branch reconciliation): a bare minPlayerSalary floor and
  // observed-history gate still work when no full policy is supplied. Locked and
  // explicit-exposure players are the user's own instruction and outrank both.
  const salaryFloor = settings.minPlayerSalary ?? 0;
  const floorExempt = new Set([...settings.lockedPlayerIds,
    ...Object.entries(settings.minExposureByPlayer).filter(([, target]) => target > 0).map(([id]) => Number(id))]);

  const coverage = { requested: players.length, direct: 0, fallback: 0, excluded: 0 };
  const pool: ResolvedPlayer[] = [];
  const eligibility: NflEligibilityDecision[] = [];
  const warnings: string[] = [...(shape?.notes ?? [])];
  const receiverRoleChanges = deriveReceiverRoleChanges(players);
  let belowSalaryFloor = 0;
  let withoutHistory = 0;
  let puntBlocked = 0;

  for (const player of players) {
    const named = { dkPlayerId: player.dkPlayerId, name: player.name, salary: player.salary };
    if (settings.excludeKickers && player.position === "K") {
      const reason = "Kickers are left out of this build (winners shape: 6% of winning showdown lineups carried one).";
      eligibility.push({ ...named, eligible: false, salaryRelief: false, captainEligible: false, overridden: false, reason, reasonCode: "KICKER_EXCLUDED" });
      if (locked.has(player.dkPlayerId)) throw new Error(`${player.name} is locked, but this build leaves kickers out. Remove the lock or choose the standard build shape.`);
      coverage.excluded++; continue;
    }
    const kickerBlock = kickerRoleBlockedReason(player);
    if (kickerBlock) {
      eligibility.push({ ...named, eligible: false, salaryRelief: false, captainEligible: false, overridden: false,
        reason: kickerBlock, reasonCode: 'KICKER_ROLE' });
      if (locked.has(player.dkPlayerId)) throw new Error(`${player.name}: ${kickerBlock}`);
      coverage.excluded++; continue;
    }
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
    // Winners-shape pool rules never apply to a locked player (the preset
    // computes its exclusions without him), so this cannot reach a lock.
    const shapeReason = shapeExcluded.get(player.dkPlayerId);
    if (shapeReason) {
      eligibility.push({ ...named, eligible: false, salaryRelief: false, captainEligible: false, overridden: false, reason: shapeReason, reasonCode: "WINNERS_SHAPE" });
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
    let captainEligible = !captainBlockedForRun(player, settings);
    if (settings.format === "showdown" && userCaptainOverridesAvailability(player, settings)) {
      warnings.push(`${player.name} is listed Questionable; his CPT range was applied anyway because you set a captain minimum.`);
    }
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
        && !captainBlockedForRun(player, settings);
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
    // Winners shape (Showdown): the Captain slot is open only to the listed
    // positions; the player still plays Flex, so this is not an exclusion.
    const captainPositionAllowed = !settings.captainPositions || settings.captainPositions.includes(player.position);
    pool.push({ ...player, projectionAudit:resolveProjectionAudit(player,settings), projection: resolved.value, resolvedSource: resolved.source, salaryRelief, captainEligible: captainEligible && captainPositionAllowed });
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
  assertDstGameScript(settings.format, pool.filter(player => settings.lockedPlayerIds.includes(player.dkPlayerId))
    .map(player => ({slot:"FLEX",player})));
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
  // A workload P90 without boom and a historical P90 with boom are not one
  // comparable objective. Do not let the solver hide that mismatch behind a
  // fallback label. A qualified defensive adjustment remains in the historical
  // family because it modifies that same saved distribution.
  const objectiveSources = new Map<string, number>();
  for (const player of pool) {
    const source = ["our", "our_fallback", "defensive", "special_teams"].includes(player.resolvedSource)
      ? "historical" : player.resolvedSource;
    objectiveSources.set(source, (objectiveSources.get(source) ?? 0) + 1);
  }
  if (objectiveSources.size > 1) {
    const mix = [...objectiveSources].map(([source, count]) => `${count} ${source}`).join(" and ");
    throw new Error(`Cannot build lineups from mixed objective sources (${mix}). Their ceilings and boom bonuses are not on one scale. Choose Our projections, or a source that covers the full eligible player pool. Experimental forecasts can still be reviewed without building lineups.`);
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
  if (settings.projectionSource === 'our') {
    const dst = pool.filter(p => p.resolvedSource === 'special_teams' && p.position === 'DST').length;
    const kicker = pool.filter(p => p.resolvedSource === 'special_teams' && p.position === 'K').length;
    const fallback = pool.filter(p => (p.position === 'DST' || p.position === 'K') && p.resolvedSource !== 'special_teams');
    warnings.push(`Special teams forecasts: ${dst} DST used opponent context and ${kicker} kickers used team scoring context.`);
    if (fallback.length) warnings.push(`${fallback.length} DST/kicker players used historical baselines because game-context projections were unavailable: ${fallback.slice(0, 3).map(p => `${p.name} (${p.specialTeamsReason ?? 'no saved candidate'})`).join('; ')}${fallback.length > 3 ? '; …' : ''}. Refresh projections for current game context.`);
  }
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
    if (settings.buildShape === "diversified" && !hasExplicit) {
      counts.overallMax = Math.min(counts.overallMax, diversifiedCount(diversifiedPositionMax(p.position, diversifiedPolicy(settings)), settings.nLineups));
    }
    // Winners shape: a quarterback cap below the flat default, and a captain
    // cap, both yielding to a range the user set on that player himself.
    if (!hasExplicit && settings.format === "classic" && p.position === "QB" && settings.maxQbExposure != null) {
      counts.overallMax = Math.max(1, Math.min(counts.overallMax, Math.floor(settings.maxQbExposure * settings.nLineups)));
    }
    if (settings.format === "showdown" && p.position === "QB" && settings.showdownQbMinCap != null
      && policyById.get(p.dkPlayerId)?.overallFromUser !== true && settings.maxExposureByPlayer[String(p.dkPlayerId)] == null) {
      counts.overallMax = Math.max(counts.overallMax, Math.floor(settings.showdownQbMinCap * settings.nLineups + 1e-9));
    }
    if (settings.format === "showdown" && settings.maxCaptainExposure != null && policyById.get(p.dkPlayerId)?.captain.maxPct == null) {
      counts.captainMax = Math.max(1, counts.captainMin, Math.min(counts.captainMax, Math.floor(settings.maxCaptainExposure * settings.nLineups)));
    }
    if (settings.buildShape === "diversified") {
      if (locked.has(p.dkPlayerId)) counts.overallMin = counts.overallMax = settings.nLineups;
      if (counts.overallMin > counts.overallMax) throw new Error(`${p.name}: minimum ${counts.overallMin} exceeds the Diversified maximum ${counts.overallMax}. Set an explicit player maximum or lower the minimum.`);
    }
    return [p.dkPlayerId, counts];
  }));

  if (settings.buildShape === "diversified") {
    const policy = diversifiedPolicy(settings);
    if (policy.minimumQbs>1 && pool.some(p=>p.position==='QB'&&locked.has(p.dkPlayerId))) throw new Error('Diversified minimum QBs conflicts with a locked QB. Unlock him or set minimum different QBs to 0 or 1.');
    const playable = pool.filter(p=>(countsById.get(p.dkPlayerId)?.overallMax??0)>0);
    if (playable.filter(p=>p.position==='QB').length<policy.minimumQbs)
      throw new Error(`Diversified requires ${policy.minimumQbs} different QBs, but only ${playable.filter(p=>p.position==='QB').length} are eligible under your limits.`);
    for (const [position, slots] of [['QB',1],['RB',2],['WR',3],['TE',1],['DST',1]] as const) {
      const capacity=playable.filter(p=>p.position===position).reduce((sum,p)=>sum+countsById.get(p.dkPlayerId)!.overallMax,0);
      if(capacity<slots*settings.nLineups) throw new Error(`Diversified ${position} limits cover only ${capacity}/${slots*settings.nLineups} required appearances. Raise a player or position maximum, or remove exclusions.`);
    }
  }

  if (settings.salaryPolicy) validateSalaryPolicy(settings.salaryPolicy);

  // P3-AC4: reject impossible plans BEFORE generation, naming the conflicts.
  if (exposurePolicies) {
    for (const p of exposurePolicies) validateExposurePolicy(p);
    const captainEligibleIds = new Set(pool.filter((p) => p.captainEligible).map((p) => p.dkPlayerId));
    const problems = detectExposureInfeasibility(
      pool.map((p) => effectivePolicy(p)),
      settings.nLineups,
      settings.format === "showdown" ? captainEligibleIds : new Set(),
      new Map(pool.map((p) => [p.dkPlayerId, { name: `${p.name} (${p.position})`, captainIneligibleReason: captainIneligibleReason(p, settings) }])),
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
  let bringBackLineups = 0;
  const bringBackShare = settings.format === "classic" && settings.mode === "gpp" && settings.stackPassCatchers > 0 ? settings.bringBackShare ?? null : null;
  const dstVsOffenseCap = settings.format === "classic" && settings.maxDstVsOffenseShare != null
    ? Math.floor(settings.maxDstVsOffenseShare * plan.length + 1e-9) : null;
  let dstVsOffenseLineups = 0;
  const showdown = settings.format === "showdown" && settings.mode === "gpp";
  const twoQbShare = showdown ? settings.twoQbShare ?? null : null;
  const rbCaptainShare = showdown ? settings.rbCaptainShare ?? null : null;
  const loneQbCaptainCap = showdown && settings.maxLoneQbCaptainShare != null ? Math.floor(settings.maxLoneQbCaptainShare * plan.length + 1e-9) : null;
  const showdownShares = twoQbShare != null || rbCaptainShare != null || loneQbCaptainCap != null;
  let twoQbLineups = 0, loneQbCaptainLineups = 0, rbCaptainLineups = 0, distributionRelaxed = 0;
  const classicGpp = settings.format === "classic" && settings.mode === "gpp" && settings.stackPassCatchers > 0;
  const doubleStackShare = classicGpp ? settings.qbDoubleStackShare ?? null : null;
  const fourWrCap = settings.format === "classic" && settings.maxFourWrShare != null ? Math.floor(settings.maxFourWrShare * plan.length + 1e-9) : null;
  const otherGameCap = settings.format === "classic" && settings.maxOtherGameStackShare != null ? Math.floor(settings.maxOtherGameStackShare * plan.length + 1e-9) : null;
  const twoTeShare = settings.format === "classic" ? settings.twoTeShare ?? null : null;
  const classicShares = doubleStackShare != null || fourWrCap != null || otherGameCap != null || twoTeShare != null;
  let doubleStackLineups = 0, fourWrLineups = 0, otherGameLineups = 0, twoTeLineups = 0;
  const foldedToStandard = new Map<ArchetypeId, number>();
  for (let lineupNumber = 1; lineupNumber <= plan.length; lineupNumber++) {
    const forbidDstVsOffense = dstVsOffenseCap != null && dstVsOffenseLineups >= dstVsOffenseCap;
    const remaining = plan.length - lineupNumber + 1;
    // A bring-back share alternates: required while the run is behind the
    // share, forbidden once it is level, so 50% lands on every other lineup.
    const bringBackRule: BringBackRule = bringBackShare == null ? "default"
      : Math.round(bringBackShare * lineupNumber) - bringBackLineups > 0 ? "require" : "forbid";
    const forced = new Set(locked);
    for (const player of pool) {
      // Force a player in when his remaining overall-minimum need equals the
      // remaining lineups. Uses the derived overall minimum (policy or legacy).
      const target = countsById.get(player.dkPlayerId)!.overallMin;
      const current = exposureCounts.get(player.dkPlayerId) ?? 0;
      if (target - current >= remaining) forced.add(player.dkPlayerId);
    }
    // Classic one-slot positions (QB, DST): players with unmet overall
    // minimums cannot share a lineup, so forcing each only when his own need
    // equals the lineups left makes several come due together and the run
    // stops. Once their combined need fills the remaining lineups, force one
    // per lineup (the furthest from his minimum) and none of the others.
    if (settings.format === "classic") {
      for (const position of ["QB", "DST"] as const) {
        const owed = pool.filter((player) => player.position === position)
          .map((player) => ({ id: player.dkPlayerId, need: countsById.get(player.dkPlayerId)!.overallMin - (exposureCounts.get(player.dkPlayerId) ?? 0) }))
          .filter((row) => row.need > 0);
        if (!owed.length || owed.reduce((sum, row) => sum + row.need, 0) < remaining) continue;
        if (owed.some((row) => locked.has(row.id))) continue;
        owed.sort((x, y) => y.need - x.need || x.id - y.id);
        for (const row of owed) forced.delete(row.id);
        forced.add(owed[0].id);
      }
    }
    const requireAirMatchup = airMatchupTarget > 0 && airMatchupTarget - airMatchupLineups >= remaining;
    const requireGoalLine = goalLineTarget > 0 && goalLineTarget - goalLineLineups >= remaining;
    // Showdown winner distributions: this lineup's quarterback count and
    // captain restrictions, scheduled like the bring-back share (required while
    // the run is behind the share). A lineup that cannot meet them is retried
    // with them relaxed, never dropped; relaxations are counted in the notes.
    const wantTwoQb = twoQbShare != null && Math.round(twoQbShare * lineupNumber) - twoQbLineups > 0;
    const directives: Partial<NflOptimizerSettings>[] = [];
    if (classicShares) {
      const wantDouble = doubleStackShare != null && Math.round(doubleStackShare * lineupNumber) - doubleStackLineups > 0;
      const full: Partial<NflOptimizerSettings> = {
        ...(doubleStackShare != null ? (wantDouble ? { lineupStackMin: 2 as const } : { lineupStackMin: 1 as const, lineupStackMax: 1 }) : {}),
        ...(fourWrCap != null && fourWrLineups >= fourWrCap ? { lineupForbidFourWr: true } : {}),
        ...(otherGameCap != null && otherGameLineups >= otherGameCap ? { lineupForbidOtherGameStack: true } : {}),
        ...(twoTeShare != null ? { lineupTeExact: (Math.round(twoTeShare * lineupNumber) - twoTeLineups > 0 ? 2 : 1) as 1 | 2 } : {}),
      };
      // Relax in order: the TE count, then the stack size, then the other-game cap; the 4th-WR cap last.
      const relaxations: Partial<NflOptimizerSettings>[] = [
        full,
        { ...full, lineupTeExact: undefined },
        { lineupForbidFourWr: full.lineupForbidFourWr, lineupForbidOtherGameStack: full.lineupForbidOtherGameStack },
        { lineupForbidFourWr: full.lineupForbidFourWr },
      ];
      const seen = new Set<string>();
      for (const r of (settings.buildShape === "diversified" ? [full] : relaxations)) {
        const key = JSON.stringify(r, Object.keys(r).sort());
        if (!seen.has(key)) { seen.add(key); directives.push(r); }
      }
    }
    if (showdownShares) {
      const full: Partial<NflOptimizerSettings> = {
        ...(twoQbShare != null && (settings.minQbPerLineup ?? 0) >= 1 ? { lineupQbExact: (wantTwoQb ? 2 : 1) as 1 | 2 } : {}),
        ...(loneQbCaptainCap != null && !wantTwoQb && loneQbCaptainLineups >= loneQbCaptainCap ? { lineupForbidQbCaptain: true } : {}),
        ...(rbCaptainShare != null ? (Math.round(rbCaptainShare * lineupNumber) - rbCaptainLineups > 0
          ? { lineupRequireRbCaptain: true } : { lineupForbidRbCaptain: true }) : {}),
      };
      directives.push(full);
      if (full.lineupRequireRbCaptain || full.lineupForbidRbCaptain) directives.push({ ...full, lineupRequireRbCaptain: undefined, lineupForbidRbCaptain: undefined });
      if (full.lineupQbExact != null) directives.push({ lineupForbidQbCaptain: full.lineupForbidQbCaptain });
    }
    if (settings.buildShape !== "diversified") directives.push({});
    let lineup: NflGeneratedLineup | null = null;
    for (const [attempt, directive] of directives.entries()) {
      const ls: NflOptimizerSettings = { ...settings, ...directive };
      for (const candidate of topProjectedTargets) {
        if ((exposureCounts.get(candidate.dkPlayerId) ?? 0) > 0) continue;
        lineup = buildOne(pool, ls, lineupNumber, lineups, exposureCounts,
          new Set([...forced, candidate.dkPlayerId]), countsById, captainCounts, flexCounts,
          plan[lineupNumber - 1].compiled, remaining, true, requireAirMatchup, requireGoalLine, bringBackRule, forbidDstVsOffense);
        if (lineup) break;
      }
      lineup ??= buildOne(pool, ls, lineupNumber, lineups, exposureCounts, forced, countsById, captainCounts, flexCounts, plan[lineupNumber - 1].compiled, remaining, true, requireAirMatchup, requireGoalLine, bringBackRule, forbidDstVsOffense);
      // A plan strategy the remaining rules cannot satisfy folds this lineup into
      // Standard ceiling instead of ending the run, as the plan promises; later
      // strategies still get their turn. The fold is named in the run's notes.
      if (!lineup && plan[lineupNumber - 1].compiled) {
        lineup = buildOne(pool, ls, lineupNumber, lineups, exposureCounts, forced, countsById, captainCounts, flexCounts, null, remaining, true, requireAirMatchup, requireGoalLine, bringBackRule, forbidDstVsOffense);
        if (lineup) foldedToStandard.set(plan[lineupNumber - 1].archetypeId, (foldedToStandard.get(plan[lineupNumber - 1].archetypeId) ?? 0) + 1);
      }
      if (lineup) {
        if (attempt > 0) distributionRelaxed++;
        break;
      }
    }
    if (!lineup) {
      if (requireAirMatchup && requireGoalLine) throw new Error(`Could not meet the air-yard matchup and goal-line RB minimums together with the remaining exposure, salary, and roster constraints. Lower a percentage or adjust player limits.`);
      if (requireAirMatchup) throw new Error(`Could not meet the air-yard matchup minimum of ${airMatchupTarget}/${settings.nLineups} lineups with the remaining exposure, salary, and roster constraints. Lower the percentage or adjust player limits.`);
      if (requireGoalLine) throw new Error(`Could not meet the goal-line RB minimum of ${goalLineTarget}/${settings.nLineups} lineups with the remaining exposure, salary, and roster constraints. Lower the percentage or adjust player limits.`);
      warnings.push(`Stopped after ${lineups.length} lineup(s): the ${ARCHETYPE_LABELS[plan[lineupNumber - 1].archetypeId]} quota or remaining exposure, uniqueness, salary, and DST game-script rules cannot all be met.`);
      break;
    }
    lineups.push(lineup);
    if (lineup.stackSummary.bringBack) bringBackLineups++;
    if (settings.format === "classic") {
      const qb = lineup.slots.find(({ player }) => player.position === "QB")?.player;
      if (qb && lineup.slots.filter(({ player }) => ["WR", "TE"].includes(player.position) && player.team === qb.team).length >= 2) doubleStackLineups++;
      if (lineup.slots.filter(({ player }) => player.position === "WR").length >= 4) fourWrLineups++;
      if (lineup.slots.filter(({ player }) => player.position === "TE").length >= 2) twoTeLineups++;
      if (qb) {
        const counts = new Map<string, number>();
        for (const { player } of lineup.slots) if (["RB", "WR", "TE"].includes(player.position)) counts.set(player.team, (counts.get(player.team) ?? 0) + 1);
        if ([...counts].some(([team, k]) => k >= 2 && team !== qb.team && team !== qb.opponent)) otherGameLineups++;
      }
    }
    if (showdown) {
      const qbCount = lineup.slots.filter(({ player }) => player.position === "QB").length;
      const captain = lineup.slots.find(({ slot }) => slot === "CPT")?.player;
      if (qbCount >= 2) twoQbLineups++;
      if (captain?.position === "QB" && qbCount === 1) loneQbCaptainLineups++;
      if (captain?.position === "RB") rbCaptainLineups++;
    }
    if (dstFacesOwnOffense(lineup)) dstVsOffenseLineups++;
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

  // Joint selection (nfl-joint-selection-v1): choose the final set from the
  // sequential build plus extra candidates in one integer program. The
  // sequential build is always a feasible answer, so this can only keep it or
  // improve on it; every tally below is recounted from the final lineups.
  let diversifiedReport: DiversifiedReport | undefined;
  if ((settings.portfolioSelection === "joint" || settings.buildShape === "diversified") && lineups.length) {
    const joint = settings.buildShape === "diversified" ? diversifiedSelect({pool, settings, lineups, countsById, locked, topProjectedTargets}) : jointSelect({ pool, settings, plan, lineups, countsById, locked, topProjectedTargets,
      targets: { twoQbShare, rbCaptainShare, loneQbCaptainCap, doubleStackShare, fourWrCap, otherGameCap, twoTeShare,
        bringBackShare, dstVsOffenseCap, airMatchupTarget, goalLineTarget } });
    warnings.push(joint.note);
    if ("report" in joint) diversifiedReport = joint.report as DiversifiedReport;
    if (joint.lineups) {
      lineups.splice(0, lineups.length, ...joint.lineups);
      // The sequential pass's relaxation and fold counts describe lineups that
      // may no longer be in the portfolio; the joint note reports misses instead.
      distributionRelaxed = 0;
      foldedToStandard.clear();
      exposureCounts.clear(); captainCounts.clear(); flexCounts.clear();
      airMatchupLineups = goalLineLineups = bringBackLineups = dstVsOffenseLineups = 0;
      twoQbLineups = loneQbCaptainLineups = rbCaptainLineups = 0;
      doubleStackLineups = fourWrLineups = otherGameLineups = twoTeLineups = 0;
      for (const lineup of lineups) {
        const f = portfolioFeatures(lineup, settings.format);
        if (lineup.stackSummary.bringBack) bringBackLineups++;
        doubleStackLineups += f.dbl ?? 0; fourWrLineups += f.fourWr ?? 0; twoTeLineups += f.twoTe ?? 0; otherGameLineups += f.otherGame ?? 0;
        if (showdown) { twoQbLineups += f.twoQb ?? 0; loneQbCaptainLineups += f.loneQbCpt ?? 0; rbCaptainLineups += f.rbCpt ?? 0; }
        dstVsOffenseLineups += f.dstVsOff ?? 0; airMatchupLineups += f.air ?? 0; goalLineLineups += f.goal ?? 0;
        lineup.playerIds.forEach((id) => exposureCounts.set(id, (exposureCounts.get(id) ?? 0) + 1));
        for (const slot of lineup.slots) {
          const id = slot.player.dkPlayerId;
          if (slot.slot === "CPT") captainCounts.set(id, (captainCounts.get(id) ?? 0) + 1);
          else flexCounts.set(id, (flexCounts.get(id) ?? 0) + 1);
        }
      }
    }
  }
  if (classicShares && lineups.length) {
    warnings.push(`Classic distributions: ${doubleStackLineups} of ${lineups.length} lineups stack the QB with 2+ of his pass catchers${doubleStackShare != null ? ` (target ${Math.round(doubleStackShare * 100)}%)` : ""}, `
      + `${fourWrLineups} roster a 4th WR${fourWrCap != null ? ` (cap ${fourWrCap})` : ""}, `
      + `${otherGameLineups} carry a 2-player stack from another game${otherGameCap != null ? ` (cap ${otherGameCap})` : ""}, `
      + `${twoTeLineups} roster a 2nd TE${twoTeShare != null ? ` (target ${Math.round(twoTeShare * 100)}%)` : ""}`
      + (distributionRelaxed ? `; ${distributionRelaxed} lineup(s) could not meet their target under the other rules and were built with it relaxed.` : "."));
  }
  if (showdownShares && lineups.length) {
    warnings.push(`Showdown distributions: ${twoQbLineups} of ${lineups.length} lineups have both quarterbacks${twoQbShare != null ? ` (target ${Math.round(twoQbShare * 100)}%)` : ""}, `
      + `${loneQbCaptainLineups} captain a quarterback with no second quarterback${loneQbCaptainCap != null ? ` (cap ${loneQbCaptainCap})` : ""}, `
      + `${rbCaptainLineups} captain a running back${rbCaptainShare != null ? ` (target ${Math.round(rbCaptainShare * 100)}%)` : ""}`
      + (distributionRelaxed ? `; ${distributionRelaxed} lineup(s) could not meet their target under the other rules and were built with it relaxed.` : "."));
  }
  if (bringBackShare != null && lineups.length) {
    warnings.push(`Bring-back share: ${bringBackLineups} of ${lineups.length} lineups carry an opposing bring-back (target ${Math.round(bringBackShare * 100)}%).`);
  }
  if (dstVsOffenseCap != null && lineups.length) {
    warnings.push(`DST against own players: ${dstVsOffenseLineups} of ${lineups.length} lineups play a defense against one of their own QB/RB/WR/TE (cap ${dstVsOffenseCap}, ${Math.round(settings.maxDstVsOffenseShare! * 100)}%).`);
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
    warnings.push(settings.buildShape === 'diversified'
      ? `Leading projection exposure: ${topProjectedTargets.map(p=>`${p.name} ${exposureCounts.get(p.dkPlayerId)??0}/${settings.nLineups}`).join('; ')}. One appearance is coverage, not evidence that the allocation is sufficient.`
      : `Top projected coverage: ${topProjectedTargets.map(player => player.name).join(", ")} each appeared in at least one lineup (ranked by selected mean projection).`);
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
      warnings.push(`${player.name}: ${binding}; this build did not satisfy the requested target. Adjust targets or construction rules and generate again.`);
    }
    return { dkPlayerId: player.dkPlayerId, name: player.name, overall, captain, flex,
      overallMin: c.overallMin, overallMax: c.overallMax, captainMin: c.captainMin, captainMax: c.captainMax, flexMin: c.flexMin, flexMax: c.flexMax, binding };
  });
  const blockedKickers = eligibility.filter(p => p.reasonCode === 'KICKER_ROLE');
  if (blockedKickers.length) warnings.push(`Kickers excluded without a supported starting role: ${blockedKickers.map(p => p.name).join(', ')}. Healthy status and salary inclusion do not establish kicking duties.`);

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
  if (settings.format === "showdown" && (settings.minQbPerLineup ?? 0) > 0) {
    const short = lineups.filter((l) => l.slots.filter((slot) => slot.player.position === "QB").length < settings.minQbPerLineup!).length;
    if (short) warnings.push(`${short} lineup(s) have fewer than ${settings.minQbPerLineup} quarterback(s): the quarterbacks' exposure caps ran out. Raise their maximum exposure to keep the minimum in every lineup.`);
  }
  if (exactDuplicates.length) warnings.push(`${exactDuplicates.length} exact-duplicate lineup pair(s) detected — this should not happen; report the run.`);
  const overlap = computeMaxOverlap(lineups);
  const ownershipValidated = settings.duplicationModelValidated === true && settings.ownershipCapability === "validated";
  const ownershipByPlayer = new Map(pool.filter((p) => ownershipPct(p) != null).map((p) => [p.dkPlayerId, (ownershipPct(p) as number) / 100]));
  const duplication = ownershipByPlayer.size
    ? estimateDuplication(lineups.map((l) => ({ lineupNumber: l.lineupNumber, playerIds: l.playerIds, totalSalary: l.totalSalary })), { ownershipValidated, ownershipByPlayer })
    : undefined;

  // The plan's quotas against what was built, so QA checks them instead of
  // showing "Archetype quotas met" unevaluated. Only when a plan shaped the
  // portfolio; plain Standard ceiling has no quota to miss.
  for (const [archetypeId, count] of foldedToStandard) warnings.push(`${count} ${ARCHETYPE_LABELS[archetypeId]} lineup(s) could not be built under the remaining exposure, uniqueness, salary and roster rules, so they were built as Standard ceiling instead.`);
  const planned = chalkPlan || archetypeQuotas?.length ? plan : [];
  const requestedById = new Map<ArchetypeId, number>();
  for (const slot of planned) requestedById.set(slot.archetypeId, (requestedById.get(slot.archetypeId) ?? 0) + 1);
  const archetypePlan = planned.length ? [...requestedById].map(([archetypeId, requested]) => ({ archetypeId, label: ARCHETYPE_LABELS[archetypeId], requested,
    realized: lineups.filter((lineup) => lineup.archetype?.id === archetypeId).length })) : undefined;

  if (settings.buildShape === "diversified") {
    if (lineups.length !== settings.nLineups) throw new Error(`Diversified could build only ${lineups.length}/${settings.nLineups} lineups under the exposure, combination and uniqueness limits. No partial portfolio was saved. Adjust limits or exclusions and retry.`);
    if (exposureReport.some(r => /missed/.test(r.binding ?? ""))) throw new Error("Diversified could not satisfy all player minimums. Adjust player targets and retry.");
  }
  return { lineups, warnings, diversifiedReport, sourceCoverage: coverage, eligibility, exposureReport, salaryBandReport, duplication, maxPairwiseOverlap: overlap, archetypePlan, topProjectedPlayerIds: topProjectedTargets.map(player => player.dkPlayerId) };
}
