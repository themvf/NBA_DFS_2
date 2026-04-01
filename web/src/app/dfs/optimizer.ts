import "server-only";

/**
 * DraftKings NBA lineup optimizer using Integer Linear Programming.
 *
 * Lineup structure: PG / SG / SF / PF / C / G / F / UTIL (8 players, $50k cap)
 *   - PG slot: player with "PG" in eligible_positions
 *   - SG slot: player with "SG" in eligible_positions
 *   - SF slot: player with "SF" in eligible_positions
 *   - PF slot: player with "PF" in eligible_positions
 *   - C slot:  player with "C" in eligible_positions
 *   - G slot:  player with "G" in eligible_positions (PG or SG flex)
 *   - F slot:  player with "F" in eligible_positions (SF or PF flex)
 *   - UTIL:    any player
 *
 * Uses javascript-lp-solver for binary ILP.
 */

import type { DkPlayerRow } from "@/db/queries";
import { parseCsvLine, stringifyCsvLine } from "./csv";
import type { OptimizerDebugInfo, OptimizerLineupAttemptDebug } from "./optimizer-debug";
import type { NbaPreparedOptimizerRun } from "./optimizer-job-types";
import {
  normalizeNbaRuleSelections,
  validateNbaRuleSelections,
  type NbaTeamStackRule,
  type NormalizedNbaRuleSelections,
} from "./nba-optimizer-rules";

// eslint-disable-next-line @typescript-eslint/no-require-imports
const solver = require("javascript-lp-solver") as {
  Solve: (model: SolverModel) => SolverResult;
};

type SolverModel = {
  optimize: string;
  opType: "max" | "min";
  constraints: Record<string, { min?: number; max?: number; equal?: number }>;
  variables: Record<string, Record<string, number>>;
  binaries: Record<string, number>;
};

type SolverResult = Record<string, number> & { feasible: boolean; result: number };

export type OptimizerPlayer = Pick<
  DkPlayerRow,
  | "id"
  | "dkPlayerId"
  | "name"
  | "teamAbbrev"
  | "teamId"
  | "matchupId"
  | "eligiblePositions"
  | "salary"
  | "ourProj"
  | "ourLeverage"
  | "linestarProj"
  | "projOwnPct"
  | "isOut"
  | "gameInfo"
  | "teamLogo"
  | "teamName"
> & {
  /** Home team ID for the player's matchup — used for bring-back enforcement. */
  homeTeamId: number | null;
};

export type LineupSlot = "PG" | "SG" | "SF" | "PF" | "C" | "G" | "F" | "UTIL";

export type GeneratedLineup = {
  players: OptimizerPlayer[];
  slots: Record<LineupSlot, OptimizerPlayer>;
  totalSalary: number;
  projFpts: number;
  leverageScore: number;
};

export type OptimizerSettings = {
  mode: "cash" | "gpp";
  nLineups: number;
  minStack: number;
  maxExposure: number;
  /**
   * Bring-back threshold (GPP only).
   * If a team contributes ≥ this many players to a lineup, the optimizer
   * must include ≥ 1 player from their opponent in that matchup.
   * 0 = disabled; 3 = standard GPP construction (3+1 or 4+1 stacks).
   */
  bringBackThreshold: number;
  /** Per-player salary filters — exclude players outside this range from the eligible pool. */
  minSalaryFilter?: number | null;
  maxSalaryFilter?: number | null;
  playerLocks?: number[];
  playerBlocks?: number[];
  blockedTeamIds?: number[];
  requiredTeamStacks?: NbaTeamStackRule[];
};

const SALARY_CAP = 50000;
const SALARY_FLOOR = 49000;   // DK lineups must spend close to the cap
const ROSTER_SIZE = 8;
const LINEUP_SLOTS: LineupSlot[] = ["PG", "SG", "SF", "PF", "C", "G", "F", "UTIL"];

type NextNbaLineupResult = {
  lineup: GeneratedLineup | null;
  summary: OptimizerDebugInfo["lineupSummaries"][number];
};

function finiteOrNull(value: number | null | undefined): number | null {
  return value != null && Number.isFinite(value) ? value : null;
}

function getPlayerProjection(p: OptimizerPlayer): number | null {
  return finiteOrNull(p.ourProj);
}

function getPlayerLeverage(p: OptimizerPlayer): number {
  return finiteOrNull(p.ourLeverage) ?? 0;
}

function getPlayerScore(p: OptimizerPlayer, mode: "cash" | "gpp"): number {
  if (mode === "gpp") {
    const leverage = finiteOrNull(p.ourLeverage);
    if (leverage != null) return leverage;
  }
  return getPlayerProjection(p) ?? 0;
}

function canFillSlot(slot: LineupSlot, pos: string): boolean {
  switch (slot) {
    case "PG":   return pos.includes("PG");
    case "SG":   return pos.includes("SG");
    case "SF":   return pos.includes("SF");
    case "PF":   return pos.includes("PF");
    case "C":    return pos.includes("C");
    case "G":    return pos.includes("G");
    case "F":    return pos.includes("F");
    case "UTIL": return true;
  }
}

function filterEligibleNbaPool(
  pool: OptimizerPlayer[],
  settings: OptimizerSettings,
  ruleSelections: NormalizedNbaRuleSelections,
): OptimizerPlayer[] {
  const blockedPlayers = new Set(ruleSelections.playerBlocks);
  const blockedTeams = new Set(ruleSelections.blockedTeamIds);
  const { minSalaryFilter, maxSalaryFilter } = settings;

  return pool.filter((player) => {
    if (player.isOut) return false;
    if (blockedPlayers.has(player.id)) return false;
    if (player.teamId != null && blockedTeams.has(player.teamId)) return false;
    const ourProj = getPlayerProjection(player);
    if (!(ourProj != null && ourProj > 0 && player.salary > 0)) return false;
    if (minSalaryFilter != null && player.salary < minSalaryFilter) return false;
    if (maxSalaryFilter != null && player.salary > maxSalaryFilter) return false;
    return true;
  });
}

export function optimizeLineups(
  pool: OptimizerPlayer[],
  settings: OptimizerSettings
): GeneratedLineup[] {
  return optimizeLineupsWithDebug(pool, settings).lineups;
}

export function optimizeLineupsWithDebug(
  pool: OptimizerPlayer[],
  settings: OptimizerSettings
): { lineups: GeneratedLineup[]; debug: OptimizerDebugInfo } {
  const { mode, nLineups, minStack, maxExposure, bringBackThreshold } = settings;
  const totalStart = Date.now();
  const ruleValidation = validateNbaRuleSelections(pool, settings);
  const ruleSelections = ruleValidation.normalized;
  const eligible = filterEligibleNbaPool(pool, settings, ruleSelections);

  const debug: OptimizerDebugInfo = {
    sport: "nba",
    mode,
    eligibleCount: eligible.length,
    requestedLineups: nLineups,
    builtLineups: 0,
    totalMs: 0,
    probeMs: 0,
    maxExposureCount: Math.ceil(nLineups * maxExposure),
    relaxedConstraints: [],
    probeSummary: [],
    lineupSummaries: [],
    terminationReason: "completed",
    effectiveSettings: {
      minStack,
      bringBackThreshold,
      maxExposure,
      minChanges: mode === "gpp" ? 3 : 2,
      salaryFloor: SALARY_FLOOR,
    },
  };

  if (!ruleValidation.ok) {
    debug.terminationReason = "probe_infeasible";
    debug.totalMs = Date.now() - totalStart;
    return { lineups: [], debug };
  }

  if (eligible.length < ROSTER_SIZE) {
    debug.terminationReason = "insufficient_pool";
    debug.totalMs = Date.now() - totalStart;
    return { lineups: [], debug };
  }

  // Probe for feasibility — run a single test solve (no history, no exposure cap)
  // to discover which constraints the current pool can satisfy. Relax progressively:
  //   1st try: full constraints (stack + bring-back)
  //   2nd try: no bring-back  (bring-back requires an opponent player that may be expensive)
  //   3rd try: no stack either (matchupId missing or pool too thin per game)
  //   4th try: no salary floor (eligible pool filtered to ourProj>0 may exclude cheap fillers,
  //            causing all 8 cheapest eligible players to collectively exceed the $50k cap or
  //            sit below the $49k floor — remove the floor rather than return 0 lineups)
  const freshCount = () => new Map<number, number>(eligible.map((p) => [p.id, 0]));
  const timedProbe = (label: string, ms: number, bb: number, salaryFloor = SALARY_FLOOR): boolean => {
    const start = Date.now();
    const success = !!solveOneLineup(eligible, mode, ms, nLineups, freshCount(), [], bb, undefined, salaryFloor, ruleSelections);
    const durationMs = Date.now() - start;
    debug.probeMs += durationMs;
    debug.probeSummary.push({ label, success, durationMs });
    return success;
  };

  let effectiveMinStack   = minStack;
  let effectiveBringBack  = bringBackThreshold;
  let effectiveSalaryFloor = SALARY_FLOOR;
  let relaxed = false;

  if (!timedProbe(`stack=${effectiveMinStack},bringBack=${effectiveBringBack},floor=${effectiveSalaryFloor}`, effectiveMinStack, effectiveBringBack, effectiveSalaryFloor)) {
    effectiveBringBack = 0;           // disable bring-back
    relaxed = true;
    debug.relaxedConstraints.push("bring-back disabled");
  }
  if (!timedProbe(`stack=${effectiveMinStack},bringBack=${effectiveBringBack},floor=${effectiveSalaryFloor}`, effectiveMinStack, effectiveBringBack, effectiveSalaryFloor)) {
    effectiveMinStack  = 0;           // disable stacking too
    effectiveBringBack = 0;           // bring-back needs gamePlayers, empty when minStack=0
    relaxed = true;
    debug.relaxedConstraints.push("stacking disabled");
  }
  // Third probe: verify even the first lineup (no history, no exposure caps)
  // is feasible after full relaxation. Does NOT guarantee subsequent lineups
  // succeed — exposure caps and diversity can still exhaust the pool mid-run.
  if (relaxed && !timedProbe(`stack=${effectiveMinStack},bringBack=${effectiveBringBack},floor=${effectiveSalaryFloor}`, effectiveMinStack, effectiveBringBack, effectiveSalaryFloor)) {
    // Fourth probe: the $49k salary floor may be blocking when all projection-eligible
    // players happen to be high-salary (cheap fillers filtered out by ourProj > 0).
    // Disable the floor rather than returning zero lineups.
    if (!timedProbe("stack=0,bringBack=0,floor=0", 0, 0, 0)) {
      debug.terminationReason = "probe_infeasible";
      debug.totalMs = Date.now() - totalStart;
      return { lineups: [], debug }; // genuinely infeasible — bail early
    }
    effectiveSalaryFloor = 0;
    effectiveMinStack    = 0;
    effectiveBringBack   = 0;
    debug.relaxedConstraints.push("salary floor disabled");
  }

  // Adaptive diversity: small or thin pools can't sustain 3-player diff between
  // every pair of lineups — drop to 2 when eligible < 55 or constraints were relaxed.
  const effectiveMinChanges = mode === "gpp" && eligible.length >= 55 && !relaxed ? 3 : 2;
  debug.effectiveSettings = {
    minStack: effectiveMinStack,
    bringBackThreshold: effectiveBringBack,
    maxExposure,
    minChanges: effectiveMinChanges,
    salaryFloor: effectiveSalaryFloor,
  };

  const exposureCount = new Map<number, number>(eligible.map((p) => [p.id, 0]));
  const lineups: GeneratedLineup[] = [];
  const previousLineupSets: Set<number>[] = [];

  for (let i = 0; i < nLineups; i++) {
    const maxExp = Math.ceil(nLineups * maxExposure);
    const attempts: OptimizerLineupAttemptDebug[] = [];
    const runAttempt = (
      stage: string,
      stack: number,
      bringBack: number,
      minChanges: number,
      salaryFloor: number,
    ): GeneratedLineup | null => {
      const start = Date.now();
      const result = solveOneLineup(
        eligible, mode, stack, maxExp,
        exposureCount, previousLineupSets, bringBack, minChanges, salaryFloor, ruleSelections,
      );
      attempts.push({
        stage,
        success: result != null,
        durationMs: Date.now() - start,
      });
      return result;
    };

    let lineup = runAttempt("base", effectiveMinStack, effectiveBringBack, effectiveMinChanges, effectiveSalaryFloor);
    // If still infeasible, relax diversity to 1 (just prevent exact duplicates)
    if (!lineup && effectiveMinChanges > 1) {
      lineup = runAttempt("diversity=1", effectiveMinStack, effectiveBringBack, 1, effectiveSalaryFloor);
    }
    // 3rd fallback: disable bring-back — exposure/diversity exhaustion often
    // makes team-pairing infeasible for later lineups even when pool is large.
    if (!lineup && effectiveBringBack > 0) {
      lineup = runAttempt("bringBack=0", effectiveMinStack, 0, 1, effectiveSalaryFloor);
    }
    // 4th fallback: disable stack — when effectiveBringBack was already 0 from
    // the probe phase, the 3rd fallback above is skipped entirely. If minStack
    // combined with the salary floor and diversity still blocks lineup N,
    // retry without any stacking requirement. A non-stacked lineup is better
    // than stopping short of nLineups.
    if (!lineup && effectiveMinStack > 0) {
      lineup = runAttempt("stack=0", 0, 0, 1, effectiveSalaryFloor);
    }
    const durationMs = attempts.reduce((sum, attempt) => sum + attempt.durationMs, 0);
    debug.lineupSummaries.push({
      lineupNumber: i + 1,
      status: lineup ? "built" : "failed",
      durationMs,
      winningStage: attempts.find((attempt) => attempt.success)?.stage,
      attempts,
    });
    if (!lineup) {
      debug.terminationReason = "lineup_failed";
      debug.stoppedAtLineup = i + 1;
      break;
    }

    lineups.push(lineup);
    const lineupSet = new Set(lineup.players.map((p) => p.id));
    previousLineupSets.push(lineupSet);
    for (const p of lineup.players) {
      exposureCount.set(p.id, (exposureCount.get(p.id) ?? 0) + 1);
    }
  }

  debug.builtLineups = lineups.length;
  debug.totalMs = Date.now() - totalStart;
  if (lineups.length === nLineups) debug.terminationReason = "completed";

  return { lineups, debug };
}

export function prepareNbaOptimizerRun(
  pool: OptimizerPlayer[],
  settings: OptimizerSettings,
): { prepared?: NbaPreparedOptimizerRun; debug: OptimizerDebugInfo; error?: string } {
  const totalStart = Date.now();
  const { mode, nLineups, minStack, maxExposure, bringBackThreshold } = settings;
  const ruleValidation = validateNbaRuleSelections(pool, settings);
  const ruleSelections = ruleValidation.normalized;
  const eligible = filterEligibleNbaPool(pool, settings, ruleSelections);

  const debug: OptimizerDebugInfo = {
    sport: "nba",
    mode,
    eligibleCount: eligible.length,
    requestedLineups: nLineups,
    builtLineups: 0,
    totalMs: 0,
    probeMs: 0,
    maxExposureCount: Math.ceil(nLineups * maxExposure),
    relaxedConstraints: [],
    probeSummary: [],
    lineupSummaries: [],
    terminationReason: "completed",
    effectiveSettings: {
      minStack,
      bringBackThreshold,
      maxExposure,
      minChanges: mode === "gpp" ? 3 : 2,
      salaryFloor: SALARY_FLOOR,
    },
  };

  if (!ruleValidation.ok) {
    debug.terminationReason = "probe_infeasible";
    debug.totalMs = Date.now() - totalStart;
    return { debug, error: ruleValidation.error };
  }

  if (eligible.length < ROSTER_SIZE) {
    debug.terminationReason = "insufficient_pool";
    debug.totalMs = Date.now() - totalStart;
    return { debug };
  }

  const freshCount = () => new Map<number, number>(eligible.map((p) => [p.id, 0]));
  const timedProbe = (label: string, ms: number, bb: number, salaryFloor = SALARY_FLOOR): boolean => {
    const start = Date.now();
    const success = !!solveOneLineup(eligible, mode, ms, nLineups, freshCount(), [], bb, undefined, salaryFloor, ruleSelections);
    const durationMs = Date.now() - start;
    debug.probeMs += durationMs;
    debug.probeSummary.push({ label, success, durationMs });
    return success;
  };

  let effectiveMinStack = minStack;
  let effectiveBringBack = bringBackThreshold;
  let effectiveSalaryFloor = SALARY_FLOOR;
  let relaxed = false;

  if (!timedProbe(`stack=${effectiveMinStack},bringBack=${effectiveBringBack},floor=${effectiveSalaryFloor}`, effectiveMinStack, effectiveBringBack, effectiveSalaryFloor)) {
    effectiveBringBack = 0;
    relaxed = true;
    debug.relaxedConstraints.push("bring-back disabled");
  }
  if (!timedProbe(`stack=${effectiveMinStack},bringBack=${effectiveBringBack},floor=${effectiveSalaryFloor}`, effectiveMinStack, effectiveBringBack, effectiveSalaryFloor)) {
    effectiveMinStack = 0;
    effectiveBringBack = 0;
    relaxed = true;
    debug.relaxedConstraints.push("stacking disabled");
  }
  if (relaxed && !timedProbe(`stack=${effectiveMinStack},bringBack=${effectiveBringBack},floor=${effectiveSalaryFloor}`, effectiveMinStack, effectiveBringBack, effectiveSalaryFloor)) {
    if (!timedProbe("stack=0,bringBack=0,floor=0", 0, 0, 0)) {
      debug.terminationReason = "probe_infeasible";
      debug.totalMs = Date.now() - totalStart;
      return { debug };
    }
    effectiveSalaryFloor = 0;
    effectiveMinStack = 0;
    effectiveBringBack = 0;
    debug.relaxedConstraints.push("salary floor disabled");
  }

  const effectiveMinChanges = mode === "gpp" && eligible.length >= 55 && !relaxed ? 3 : 2;
  debug.effectiveSettings = {
    minStack: effectiveMinStack,
    bringBackThreshold: effectiveBringBack,
    maxExposure,
    minChanges: effectiveMinChanges,
    salaryFloor: effectiveSalaryFloor,
  };
  debug.totalMs = Date.now() - totalStart;

  return {
    prepared: {
      sport: "nba",
      mode,
      requestedLineups: nLineups,
      maxExposureCount: debug.maxExposureCount,
      eligibleCount: eligible.length,
      pool: eligible,
      ruleSelections,
      effectiveSettings: {
        minStack: effectiveMinStack,
        bringBackThreshold: effectiveBringBack,
        maxExposure,
        minChanges: effectiveMinChanges,
        salaryFloor: effectiveSalaryFloor,
      },
      relaxedConstraints: [...debug.relaxedConstraints],
      probeSummary: [...debug.probeSummary],
    },
    debug,
  };
}

export function buildNextNbaLineup(
  prepared: NbaPreparedOptimizerRun,
  priorLineupPlayerIds: number[][],
): NextNbaLineupResult {
  const exposureCount = new Map<number, number>(prepared.pool.map((p) => [p.id, 0]));
  const previousLineupSets = priorLineupPlayerIds.map((ids) => new Set(ids));

  for (const lineup of priorLineupPlayerIds) {
    for (const playerId of lineup) {
      exposureCount.set(playerId, (exposureCount.get(playerId) ?? 0) + 1);
    }
  }

  const attempts: OptimizerLineupAttemptDebug[] = [];
  const runAttempt = (
    stage: string,
    stack: number,
    bringBack: number,
    minChanges: number,
    salaryFloor: number,
  ): GeneratedLineup | null => {
    const start = Date.now();
    const result = solveOneLineup(
      prepared.pool,
      prepared.mode,
      stack,
      prepared.maxExposureCount,
      exposureCount,
      previousLineupSets,
      bringBack,
      minChanges,
      salaryFloor,
      prepared.ruleSelections,
    );
    attempts.push({
      stage,
      success: result != null,
      durationMs: Date.now() - start,
    });
    return result;
  };

  const { minStack, bringBackThreshold, minChanges, salaryFloor } = prepared.effectiveSettings;
  let lineup = runAttempt("base", minStack, bringBackThreshold, minChanges, salaryFloor);
  if (!lineup && minChanges > 1) {
    lineup = runAttempt("diversity=1", minStack, bringBackThreshold, 1, salaryFloor);
  }
  if (!lineup && bringBackThreshold > 0) {
    lineup = runAttempt("bringBack=0", minStack, 0, 1, salaryFloor);
  }
  if (!lineup && minStack > 0) {
    lineup = runAttempt("stack=0", 0, 0, 1, salaryFloor);
  }

  const durationMs = attempts.reduce((sum, attempt) => sum + attempt.durationMs, 0);
  return {
    lineup,
    summary: {
      lineupNumber: priorLineupPlayerIds.length + 1,
      status: lineup ? "built" : "failed",
      durationMs,
      winningStage: attempts.find((attempt) => attempt.success)?.stage,
      attempts,
    },
  };
}

function solveOneLineup(
  pool: OptimizerPlayer[],
  mode: "cash" | "gpp",
  minStack: number,
  maxExposureCount: number,
  exposureCount: Map<number, number>,
  previousLineupSets: Set<number>[],
  bringBackThreshold = 3,
  minChanges = mode === "gpp" ? 3 : 2,
  salaryFloor = SALARY_FLOOR,
  ruleSelections: NormalizedNbaRuleSelections = normalizeNbaRuleSelections({}),
): GeneratedLineup | null {
  const lockedPlayers = new Set(ruleSelections.playerLocks);
  const requiredTeamStacks = ruleSelections.requiredTeamStacks;
  const availablePool = pool.filter((p) =>
    lockedPlayers.has(p.id) || (exposureCount.get(p.id) ?? 0) < maxExposureCount,
  );
  if (availablePool.length < ROSTER_SIZE) return null;

  // Group by matchupId for game stacking.
  // When minStack <= 0 stacking is effectively disabled — skip entirely to avoid
  // polluting the model with trivial helper variables that bloat the B&B tree.
  const gamePlayers = new Map<number, OptimizerPlayer[]>();
  if (minStack > 0) {
    for (const p of availablePool) {
      if (p.matchupId == null) continue;
      if (!gamePlayers.has(p.matchupId)) gamePlayers.set(p.matchupId, []);
      gamePlayers.get(p.matchupId)!.push(p);
    }
  }
  const stackableGames = Array.from(gamePlayers.entries())
    .filter(([, players]) => players.length >= minStack)
    .map(([mid]) => mid);

  const availableTeamCounts = new Map<number, number>();
  for (const player of availablePool) {
    if (player.teamId == null) continue;
    availableTeamCounts.set(player.teamId, (availableTeamCounts.get(player.teamId) ?? 0) + 1);
  }
  const effectiveRequiredTeamStacks = requiredTeamStacks.filter(
    (rule) => (availableTeamCounts.get(rule.teamId) ?? 0) >= rule.stackSize,
  );
  const useRequiredTeamStacks = effectiveRequiredTeamStacks.length > 0;
  if (requiredTeamStacks.length > 0 && !useRequiredTeamStacks) return null;

  // Bring-back: matchups where both teams have players in the pool
  // (threshold 0 = disabled; only applies in GPP mode)
  const bringBackGames: number[] = [];
  if (mode === "gpp" && bringBackThreshold >= 2) {
    for (const [mid, players] of gamePlayers) {
      const teams = new Set(players.map((p) => p.teamId).filter(Boolean));
      if (teams.size === 2) bringBackGames.push(mid);
    }
  }

  const stackableSet = new Set(stackableGames);
  const bringBackSet = new Set(bringBackGames);

  const constraints: SolverModel["constraints"] = {
    salary: { min: salaryFloor, max: SALARY_CAP },
    total:  { equal: ROSTER_SIZE },
    // Stack constraint only applies when games with enough players exist.
    // If matchupId is null for all players (no schedule data), omitting this
    // prevents the solver from being immediately infeasible.
    ...(useRequiredTeamStacks
      ? { required_team_stack_count: { min: 1 } }
      : stackableGames.length > 0
        ? { stack_count: { min: 1 } }
        : {}),
  };

  for (const slot of LINEUP_SLOTS) {
    constraints[`slot_${slot}`] = { equal: 1 };
  }

  for (const p of availablePool) {
    constraints[`use_${p.id}`] = lockedPlayers.has(p.id) ? { equal: 1 } : { max: 1 };
  }

  // Bring-back constraints: if team A has ≥ threshold players in the lineup,
  // team B (their opponent) must have ≥ 1.
  //
  // Formulated as two symmetric max-constraints per matchup:
  //   home_net = Σ home_players - Σ away_players  → home_net ≤ threshold - 1
  //   away_net = Σ away_players - Σ home_players  → away_net ≤ threshold - 1
  //
  // Example (threshold=3): 3 home + 0 away → home_net=3 > 2 → infeasible.
  // Optimizer must include ≥1 away player (home_net drops to 2 → feasible).
  const bringBackMax = bringBackThreshold - 1;
  for (const mid of bringBackGames) {
    constraints[`bb_home_${mid}`] = { max: bringBackMax };
    constraints[`bb_away_${mid}`] = { max: bringBackMax };
  }

  // Only enforce diversity against the last DIV_WINDOW lineups.
  // Applying it to ALL prior lineups causes the ILP to become infeasible
  // after ~5-7 lineups on a small pool (each new lineup must simultaneously
  // differ from every previous one).
  const DIV_WINDOW = 5;
  const divWindow = previousLineupSets.slice(-DIV_WINDOW);
  for (let i = 0; i < divWindow.length; i++) {
    constraints[`div_${i}`] = { max: ROSTER_SIZE - minChanges };
  }

  for (const mid of stackableGames) {
    constraints[`game_${mid}`] = { min: 0 };
  }
  for (const rule of effectiveRequiredTeamStacks) {
    constraints[`team_${rule.teamId}`] = { min: 0 };
  }

  const variables: SolverModel["variables"] = {};
  const binaries: SolverModel["binaries"] = {};
  const variableMeta = new Map<string, { player: OptimizerPlayer; slot: LineupSlot }>();

  for (const p of availablePool) {
    const score = getPlayerScore(p, mode);
    const pos = p.eligiblePositions;

    for (const slot of LINEUP_SLOTS) {
      if (!canFillSlot(slot, pos)) continue;
      const key = `s_${slot}_${p.id}`;
      const entry: Record<string, number> = {
        score,
        salary: p.salary,
        total: 1,
        [`slot_${slot}`]: 1,
        [`use_${p.id}`]: 1,
      };

      if (p.matchupId != null && stackableSet.has(p.matchupId)) {
        entry[`game_${p.matchupId}`] = 1;
      }
      if (p.teamId != null && effectiveRequiredTeamStacks.some((rule) => rule.teamId === p.teamId)) {
        entry[`team_${p.teamId}`] = 1;
      }

      if (p.matchupId != null && bringBackSet.has(p.matchupId) && p.teamId != null) {
        const isHome = p.teamId === p.homeTeamId;
        entry[`bb_home_${p.matchupId}`] = isHome ? 1 : -1;
        entry[`bb_away_${p.matchupId}`] = isHome ? -1 : 1;
      }

      for (let i = 0; i < divWindow.length; i++) {
        if (divWindow[i].has(p.id)) {
          entry[`div_${i}`] = 1;
        }
      }

      variables[key] = entry;
      binaries[key] = 1;
      variableMeta.set(key, { player: p, slot });
    }
  }

  // Stack helper variables
  if (useRequiredTeamStacks) {
    for (const rule of effectiveRequiredTeamStacks) {
      const key = `z_team_${rule.teamId}`;
      variables[key] = {
        required_team_stack_count: 1,
        [`team_${rule.teamId}`]: -rule.stackSize,
      };
      binaries[key] = 1;
    }
  } else {
    for (const mid of stackableGames) {
      const key = `z_game_${mid}`;
      variables[key] = {
        stack_count: 1,
        [`game_${mid}`]: -minStack,
      };
      binaries[key] = 1;
    }
  }

  const model: SolverModel = {
    optimize: "score",
    opType: "max",
    constraints,
    variables,
    binaries,
  };

  const result = solver.Solve(model);
  if (!result.feasible) return null;

  const slots = {} as Record<LineupSlot, OptimizerPlayer>;
  for (const slot of LINEUP_SLOTS) {
    const key = Array.from(variableMeta.keys()).find((varKey) => {
      const meta = variableMeta.get(varKey)!;
      return meta.slot === slot && (result[varKey] ?? 0) >= 0.5;
    });
    if (!key) return null;
    slots[slot] = variableMeta.get(key)!.player;
  }

  const selectedPlayers = LINEUP_SLOTS.map((slot) => slots[slot]);
  if (new Set(selectedPlayers.map((p) => p.id)).size !== ROSTER_SIZE) return null;

  const totalSalary = selectedPlayers.reduce((s, p) => s + p.salary, 0);
  const projFpts = selectedPlayers.reduce((s, p) => s + (getPlayerProjection(p) ?? 0), 0);
  const leverageScore = selectedPlayers.reduce((s, p) => s + getPlayerLeverage(p), 0);

  return { players: selectedPlayers, slots, totalSalary, projFpts, leverageScore };
}

/**
 * Build DK NBA multi-entry upload CSV.
 * Header: Entry ID,Contest Name,Contest ID,Entry Fee,PG,SG,SF,PF,C,G,F,UTIL
 * Player cell format: "Name (dkPlayerId)"
 */
export function buildMultiEntryCSV(
  lineups: GeneratedLineup[],
  entryRows: string[]
): string {
  if (lineups.length === 0) return "";
  if (entryRows.length < 2) {
    throw new Error("Entry template must include a header row and at least one entry row.");
  }
  if (entryRows.length - 1 < lineups.length) {
    throw new Error(`Entry template has ${entryRows.length - 1} entries for ${lineups.length} lineups.`);
  }

  const rows = [stringifyCsvLine(parseCsvLine(entryRows[0]))];

  for (let i = 0; i < lineups.length; i++) {
    const lineup = lineups[i];
    const cols = parseCsvLine(entryRows[i + 1]);
    if (cols.length < 4 + LINEUP_SLOTS.length) {
      throw new Error(`Entry row ${i + 2} is missing required DraftKings columns.`);
    }
    for (let j = 0; j < LINEUP_SLOTS.length; j++) {
      const player = lineup.slots[LINEUP_SLOTS[j]];
      cols[4 + j] = player ? `${player.name} (${player.dkPlayerId})` : "";
    }
    rows.push(stringifyCsvLine(cols));
  }

  return rows.join("\n");
}

/**
 * Diagnostic probe — runs the three progressive probes and returns a debug
 * string array so the caller can surface results in an error message.
 * Called only when optimizeLineups returns [] (zero lineups generated).
 */
export function probeOptimizerAll(
  pool: OptimizerPlayer[],
  settings: OptimizerSettings,
): string[] {
  const { mode, nLineups, minStack, bringBackThreshold } = settings;
  const ruleValidation = validateNbaRuleSelections(pool, settings);
  if (!ruleValidation.ok) {
    return [ruleValidation.error];
  }
  const eligible = filterEligibleNbaPool(pool, settings, ruleValidation.normalized);

  if (eligible.length < ROSTER_SIZE) {
    return [`eligible=${eligible.length} (< ${ROSTER_SIZE})`];
  }

  const freshCount = () => new Map<number, number>(eligible.map((p) => [p.id, 0]));
  const probe = (ms: number, bb: number) =>
    !!solveOneLineup(eligible, mode, ms, nLineups, freshCount(), [], bb, undefined, SALARY_FLOOR, ruleValidation.normalized);
  const probeNoFloor = (ms: number, bb: number) =>
    !!solveOneLineup(eligible, mode, ms, nLineups, freshCount(), [], bb, undefined, 0, ruleValidation.normalized);

  const scoreOf = (p: OptimizerPlayer) => getPlayerScore(p, mode);

  const withScore    = eligible.filter((p) => scoreOf(p) > 0).length;
  const withNegScore = eligible.filter((p) => scoreOf(p) < 0).length;
  // Separate NaN from zero: NaN is neither > 0 nor < 0, so without explicit tracking
  // it silently inflates the "zero" bucket and obscures data quality problems.
  const withNanScore  = eligible.filter((p) => !Number.isFinite(scoreOf(p))).length;
  const withZeroScore = eligible.length - withScore - withNegScore - withNanScore;

  const p1 = probe(minStack, bringBackThreshold);
  const p2 = probe(minStack, 0);
  const p3 = probe(0, 0);
  // 4th probe: same as p3 but with salary floor removed — confirms whether the
  // $49k floor is the blocking constraint (eligible pool filtered to ourProj > 0
  // may exclude cheap fillers, leaving only high-salary players that can't sum to ≤$50k)
  const p4 = !p3 ? probeNoFloor(0, 0) : null;

  // Salary range: min and sum-of-8-cheapest to surface salary floor issues
  const salaries = eligible.map((p) => p.salary).sort((a, b) => a - b);
  const salMin = salaries[0];
  const salMax = salaries[salaries.length - 1];
  const min8 = salaries.slice(0, 8).reduce((s, v) => s + v, 0);

  // Per-slot position counts — the aggregate G/F/C counts don't reveal if a specific
  // required slot (PG, SG, SF, PF, or C) has 0 eligible players, which would make
  // the corresponding min:1 constraint infeasible regardless of total pool size.
  const pgCount = eligible.filter((p) => p.eligiblePositions.includes("PG")).length;
  const sgCount = eligible.filter((p) => p.eligiblePositions.includes("SG")).length;
  const sfCount = eligible.filter((p) => p.eligiblePositions.includes("SF")).length;
  const pfCount = eligible.filter((p) => p.eligiblePositions.includes("PF")).length;
  const cCount  = eligible.filter((p) => p.eligiblePositions.includes("C")).length;

  // Sample the top 3 players by score so we can see what values the ILP received
  const sorted = [...eligible].sort((a, b) => scoreOf(b) - scoreOf(a));
  const top3 = sorted.slice(0, 3).map((p) => {
    const s = scoreOf(p);
    return `${p.name}(${Number.isFinite(s) ? s.toFixed(1) : "NaN"})`;
  }).join(", ");

  return [
    `eligible=${eligible.length} slots: ${pgCount}PG/${sgCount}SG/${sfCount}SF/${pfCount}PF/${cCount}C`,
    `salary: min=$${salMin?.toLocaleString()} max=$${salMax?.toLocaleString()} cheapest8=$${min8.toLocaleString()} (floor=$${SALARY_FLOOR.toLocaleString()})`,
    `scores: ${withScore}+ / ${withNegScore}- / ${withZeroScore}zero${withNanScore > 0 ? ` / ${withNanScore}NaN — ILP objective poisoned!` : ""}`,
    `top3: ${top3}`,
    `probe(stack=${minStack},bb=${bringBackThreshold}): ${p1 ? "PASS" : "FAIL"}`,
    `probe(stack=${minStack},bb=0): ${p2 ? "PASS" : "FAIL"}`,
    `probe(0,0): ${p3 ? "PASS" : "FAIL"}`,
    ...(p4 !== null ? [`probe(0,0,no-floor): ${p4 ? "PASS — salary floor was blocking" : `FAIL — check slot counts above for 0-player positions`}`] : []),
  ];
}
