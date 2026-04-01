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
  /** Home team ID for the player's matchup - used for bring-back enforcement. */
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
  /** Minimum players in each counted team stack. */
  minStack: number;
  /** Distinct team stacks required in each lineup. */
  teamStackCount?: number;
  maxExposure: number;
  /**
   * Explicit opponent bring-back configuration.
   * When enabled, every counted team stack must include this many players
   * from the opponent in the same matchup.
   */
  bringBackEnabled?: boolean;
  bringBackSize?: number;
  /** Legacy field kept for backward compatibility with persisted jobs. */
  bringBackThreshold?: number;
  /** Per-player salary filters - exclude players outside this range from the eligible pool. */
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

function normalizeTeamStackCount(value: number | null | undefined): number {
  if (value == null || !Number.isFinite(value)) return 1;
  return Math.max(1, Math.min(3, Math.floor(value)));
}

function normalizeStackSize(value: number | null | undefined): number {
  if (value == null || !Number.isFinite(value)) return 2;
  return Math.max(2, Math.min(5, Math.floor(value)));
}

function normalizeBringBackSize(
  enabled: boolean,
  value: number | null | undefined,
  legacyThreshold?: number | null,
): number {
  if (!enabled) return 0;
  if (value != null && Number.isFinite(value)) {
    return Math.max(1, Math.min(3, Math.floor(value)));
  }
  return legacyThreshold != null && legacyThreshold > 0 ? 1 : 1;
}

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
  const { mode, nLineups, maxExposure } = settings;
  const minStack = normalizeStackSize(settings.minStack);
  const teamStackCount = normalizeTeamStackCount(settings.teamStackCount);
  const bringBackEnabled = settings.bringBackEnabled ?? ((settings.bringBackThreshold ?? 0) > 0);
  const bringBackSize = normalizeBringBackSize(bringBackEnabled, settings.bringBackSize, settings.bringBackThreshold);
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
      teamStackCount,
      bringBackEnabled,
      bringBackSize,
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

  // Probe for feasibility - run a single test solve (no history, no exposure cap)
  // to discover whether the requested stack / bring-back / salary floor constraints
  // are feasible on the current pool.
  const freshCount = () => new Map<number, number>(eligible.map((p) => [p.id, 0]));
  const timedProbe = (
    label: string,
    stackSize: number,
    stackCount: number,
    bringBackSizeForProbe: number,
    salaryFloor = SALARY_FLOOR,
  ): boolean => {
    const start = Date.now();
    const success = !!solveOneLineup(
      eligible,
      mode,
      stackSize,
      stackCount,
      Math.ceil(nLineups * maxExposure),
      freshCount(),
      [],
      bringBackSizeForProbe,
      undefined,
      salaryFloor,
      ruleSelections,
    );
    const durationMs = Date.now() - start;
    debug.probeMs += durationMs;
    debug.probeSummary.push({ label, success, durationMs });
    return success;
  };

  let effectiveMinStack = minStack;
  let effectiveTeamStackCount = teamStackCount;
  let effectiveBringBackSize = bringBackSize;
  let effectiveSalaryFloor = SALARY_FLOOR;
  if (!timedProbe(
    `teamStacks=${effectiveTeamStackCount},stackSize=${effectiveMinStack},bringBack=${effectiveBringBackSize},floor=${effectiveSalaryFloor}`,
    effectiveMinStack,
    effectiveTeamStackCount,
    effectiveBringBackSize,
    effectiveSalaryFloor,
  )) {
    if (!timedProbe(
      `teamStacks=${effectiveTeamStackCount},stackSize=${effectiveMinStack},bringBack=${effectiveBringBackSize},floor=0`,
      effectiveMinStack,
      effectiveTeamStackCount,
      effectiveBringBackSize,
      0,
    )) {
      debug.terminationReason = "probe_infeasible";
      debug.totalMs = Date.now() - totalStart;
      return { lineups: [], debug };
    }
    effectiveSalaryFloor = 0;
    debug.relaxedConstraints.push("salary floor disabled");
  }

  // Adaptive diversity: small or thin pools can't sustain 3-player diff between
  // every pair of lineups - drop to 2 when eligible < 55 or constraints were relaxed.
  const relaxed = effectiveSalaryFloor === 0;
  const effectiveMinChanges = mode === "gpp" && eligible.length >= 55 && !relaxed ? 3 : 2;
  debug.effectiveSettings = {
    minStack: effectiveMinStack,
    teamStackCount: effectiveTeamStackCount,
    bringBackEnabled: effectiveBringBackSize > 0,
    bringBackSize: effectiveBringBackSize,
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
      stackSize: number,
      stackCount: number,
      bringBackSizeForAttempt: number,
      minChanges: number,
      salaryFloor: number,
    ): GeneratedLineup | null => {
      const start = Date.now();
      const result = solveOneLineup(
        eligible,
        mode,
        stackSize,
        stackCount,
        maxExp,
        exposureCount,
        previousLineupSets,
        bringBackSizeForAttempt,
        minChanges,
        salaryFloor,
        ruleSelections,
      );
      attempts.push({
        stage,
        success: result != null,
        durationMs: Date.now() - start,
      });
      return result;
    };

    let lineup = runAttempt("base", effectiveMinStack, effectiveTeamStackCount, effectiveBringBackSize, effectiveMinChanges, effectiveSalaryFloor);
    // If still infeasible, relax diversity to 1 (just prevent exact duplicates)
    if (!lineup && effectiveMinChanges > 1) {
      lineup = runAttempt("diversity=1", effectiveMinStack, effectiveTeamStackCount, effectiveBringBackSize, 1, effectiveSalaryFloor);
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
  const { mode, nLineups, maxExposure } = settings;
  const minStack = normalizeStackSize(settings.minStack);
  const teamStackCount = normalizeTeamStackCount(settings.teamStackCount);
  const bringBackEnabled = settings.bringBackEnabled ?? ((settings.bringBackThreshold ?? 0) > 0);
  const bringBackSize = normalizeBringBackSize(bringBackEnabled, settings.bringBackSize, settings.bringBackThreshold);
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
      teamStackCount,
      bringBackEnabled,
      bringBackSize,
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
  const timedProbe = (
    label: string,
    stackSize: number,
    stackCount: number,
    bringBackSizeForProbe: number,
    salaryFloor = SALARY_FLOOR,
  ): boolean => {
    const start = Date.now();
    const success = !!solveOneLineup(
      eligible,
      mode,
      stackSize,
      stackCount,
      Math.ceil(nLineups * maxExposure),
      freshCount(),
      [],
      bringBackSizeForProbe,
      undefined,
      salaryFloor,
      ruleSelections,
    );
    const durationMs = Date.now() - start;
    debug.probeMs += durationMs;
    debug.probeSummary.push({ label, success, durationMs });
    return success;
  };

  let effectiveMinStack = minStack;
  let effectiveTeamStackCount = teamStackCount;
  let effectiveBringBackSize = bringBackSize;
  let effectiveSalaryFloor = SALARY_FLOOR;
  if (!timedProbe(
    `teamStacks=${effectiveTeamStackCount},stackSize=${effectiveMinStack},bringBack=${effectiveBringBackSize},floor=${effectiveSalaryFloor}`,
    effectiveMinStack,
    effectiveTeamStackCount,
    effectiveBringBackSize,
    effectiveSalaryFloor,
  )) {
    if (!timedProbe(
      `teamStacks=${effectiveTeamStackCount},stackSize=${effectiveMinStack},bringBack=${effectiveBringBackSize},floor=0`,
      effectiveMinStack,
      effectiveTeamStackCount,
      effectiveBringBackSize,
      0,
    )) {
      debug.terminationReason = "probe_infeasible";
      debug.totalMs = Date.now() - totalStart;
      return { debug };
    }
    effectiveSalaryFloor = 0;
    debug.relaxedConstraints.push("salary floor disabled");
  }

  const relaxed = effectiveSalaryFloor === 0;
  const effectiveMinChanges = mode === "gpp" && eligible.length >= 55 && !relaxed ? 3 : 2;
  debug.effectiveSettings = {
    minStack: effectiveMinStack,
    teamStackCount: effectiveTeamStackCount,
    bringBackEnabled: effectiveBringBackSize > 0,
    bringBackSize: effectiveBringBackSize,
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
        teamStackCount: effectiveTeamStackCount,
        bringBackEnabled: effectiveBringBackSize > 0,
        bringBackSize: effectiveBringBackSize,
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
    stackSize: number,
    stackCount: number,
    bringBackSizeForAttempt: number,
    minChanges: number,
    salaryFloor: number,
  ): GeneratedLineup | null => {
    const start = Date.now();
    const result = solveOneLineup(
      prepared.pool,
      prepared.mode,
      stackSize,
      stackCount,
      prepared.maxExposureCount,
      exposureCount,
      previousLineupSets,
      bringBackSizeForAttempt,
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

  const { minStack, teamStackCount, bringBackSize, minChanges, salaryFloor } = prepared.effectiveSettings;
  let lineup = runAttempt("base", minStack, teamStackCount, bringBackSize, minChanges, salaryFloor);
  if (!lineup && minChanges > 1) {
    lineup = runAttempt("diversity=1", minStack, teamStackCount, bringBackSize, 1, salaryFloor);
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
  teamStackCount: number,
  maxExposureCount: number,
  exposureCount: Map<number, number>,
  previousLineupSets: Set<number>[],
  bringBackSize = 0,
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

  const teamPlayers = new Map<number, OptimizerPlayer[]>();
  for (const player of availablePool) {
    if (player.teamId == null) continue;
    const existing = teamPlayers.get(player.teamId) ?? [];
    existing.push(player);
    teamPlayers.set(player.teamId, existing);
  }

  const availableTeamCounts = new Map<number, number>();
  for (const player of availablePool) {
    if (player.teamId == null) continue;
    availableTeamCounts.set(player.teamId, (availableTeamCounts.get(player.teamId) ?? 0) + 1);
  }

  const teamsByMatchup = new Map<number, Set<number>>();
  for (const player of availablePool) {
    if (player.matchupId == null || player.teamId == null) continue;
    const existing = teamsByMatchup.get(player.matchupId) ?? new Set<number>();
    existing.add(player.teamId);
    teamsByMatchup.set(player.matchupId, existing);
  }
  const opponentByTeamId = new Map<number, number>();
  for (const teamIds of teamsByMatchup.values()) {
    const matchupTeams = Array.from(teamIds);
    if (matchupTeams.length !== 2) continue;
    opponentByTeamId.set(matchupTeams[0], matchupTeams[1]);
    opponentByTeamId.set(matchupTeams[1], matchupTeams[0]);
  }

  const effectiveRequiredTeamStacks = requiredTeamStacks.filter((rule) => {
    if ((availableTeamCounts.get(rule.teamId) ?? 0) < rule.stackSize) return false;
    if (bringBackSize <= 0) return true;
    const opponentTeamId = opponentByTeamId.get(rule.teamId);
    return opponentTeamId != null && (availableTeamCounts.get(opponentTeamId) ?? 0) >= bringBackSize;
  });
  const bringBackActive = bringBackSize > 0;
  const requiredStackSizeByTeam = new Map(
    effectiveRequiredTeamStacks.map((rule) => [rule.teamId, rule.stackSize] as const),
  );
  const countableTeamIds = Array.from(availableTeamCounts.keys()).filter((teamId) => {
    const stackThreshold = requiredStackSizeByTeam.get(teamId) ?? minStack;
    if ((availableTeamCounts.get(teamId) ?? 0) < stackThreshold) return false;
    if (!bringBackActive) return true;
    const opponentTeamId = opponentByTeamId.get(teamId);
    if (opponentTeamId == null) return false;
    return (availableTeamCounts.get(opponentTeamId) ?? 0) >= bringBackSize;
  });
  if (teamStackCount > 0 && countableTeamIds.length < teamStackCount) return null;
  const useRequiredTeamStacks = effectiveRequiredTeamStacks.length > 0;
  if (requiredTeamStacks.length > 0 && effectiveRequiredTeamStacks.length !== requiredTeamStacks.length) return null;

  const countableTeamSet = new Set(countableTeamIds);

  const constraints: SolverModel["constraints"] = {
    salary: { min: salaryFloor, max: SALARY_CAP },
    total:  { equal: ROSTER_SIZE },
    ...(teamStackCount > 0 ? { team_stack_count: { min: teamStackCount } } : {}),
    ...(useRequiredTeamStacks ? { required_team_stack_count: { min: 1 } } : {}),
  };

  for (const slot of LINEUP_SLOTS) {
    constraints[`slot_${slot}`] = { equal: 1 };
  }

  for (const p of availablePool) {
    constraints[`use_${p.id}`] = lockedPlayers.has(p.id) ? { equal: 1 } : { max: 1 };
  }

  for (const teamId of countableTeamIds) {
    constraints[`team_${teamId}`] = { min: 0 };
    if (bringBackActive) {
      constraints[`bringback_${teamId}`] = { min: 0 };
    }
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

      if (p.teamId != null && countableTeamSet.has(p.teamId)) {
        entry[`team_${p.teamId}`] = 1;
      }
      if (p.teamId != null && effectiveRequiredTeamStacks.some((rule) => rule.teamId === p.teamId)) {
        entry[`team_${p.teamId}`] = 1;
      }
      if (bringBackActive && p.teamId != null) {
        const opponentTeamId = opponentByTeamId.get(p.teamId);
        if (opponentTeamId != null && constraints[`bringback_${opponentTeamId}`]) {
          entry[`bringback_${opponentTeamId}`] = 1;
        }
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

  for (const teamId of countableTeamIds) {
    const requiredRule = effectiveRequiredTeamStacks.find((rule) => rule.teamId === teamId);
    const stackThreshold = requiredRule?.stackSize ?? minStack;
    const key = `z_team_${teamId}`;
    variables[key] = {
      team_stack_count: 1,
      [`team_${teamId}`]: -stackThreshold,
      ...(requiredRule ? { required_team_stack_count: 1 } : {}),
      ...(bringBackActive ? { [`bringback_${teamId}`]: -bringBackSize } : {}),
    };
    binaries[key] = 1;
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
 * Diagnostic probe - runs the three progressive probes and returns a debug
 * string array so the caller can surface results in an error message.
 * Called only when optimizeLineups returns [] (zero lineups generated).
 */
export function probeOptimizerAll(
  pool: OptimizerPlayer[],
  settings: OptimizerSettings,
): string[] {
  const { mode, nLineups } = settings;
  const minStack = normalizeStackSize(settings.minStack);
  const teamStackCount = normalizeTeamStackCount(settings.teamStackCount);
  const bringBackEnabled = settings.bringBackEnabled ?? ((settings.bringBackThreshold ?? 0) > 0);
  const bringBackSize = normalizeBringBackSize(bringBackEnabled, settings.bringBackSize, settings.bringBackThreshold);
  const ruleValidation = validateNbaRuleSelections(pool, settings);
  if (!ruleValidation.ok) {
    return [ruleValidation.error];
  }
  const eligible = filterEligibleNbaPool(pool, settings, ruleValidation.normalized);

  if (eligible.length < ROSTER_SIZE) {
    return [`eligible=${eligible.length} (< ${ROSTER_SIZE})`];
  }

  const freshCount = () => new Map<number, number>(eligible.map((p) => [p.id, 0]));
  const probe = (stackSize: number, stackCount: number, bbSize: number) =>
    !!solveOneLineup(eligible, mode, stackSize, stackCount, nLineups, freshCount(), [], bbSize, undefined, SALARY_FLOOR, ruleValidation.normalized);
  const probeNoFloor = (stackSize: number, stackCount: number, bbSize: number) =>
    !!solveOneLineup(eligible, mode, stackSize, stackCount, nLineups, freshCount(), [], bbSize, undefined, 0, ruleValidation.normalized);

  const scoreOf = (p: OptimizerPlayer) => getPlayerScore(p, mode);

  const withScore    = eligible.filter((p) => scoreOf(p) > 0).length;
  const withNegScore = eligible.filter((p) => scoreOf(p) < 0).length;
  // Separate NaN from zero: NaN is neither > 0 nor < 0, so without explicit tracking
  // it silently inflates the "zero" bucket and obscures data quality problems.
  const withNanScore  = eligible.filter((p) => !Number.isFinite(scoreOf(p))).length;
  const withZeroScore = eligible.length - withScore - withNegScore - withNanScore;

  const p1 = probe(minStack, teamStackCount, bringBackSize);
  const p2 = probe(minStack, teamStackCount, 0);
  const p3 = probe(minStack, 1, 0);
  const bringBackThreshold = bringBackSize;
  // 4th probe: same as p3 but with salary floor removed - confirms whether the
  // $49k floor is the blocking constraint (eligible pool filtered to ourProj > 0
  // may exclude cheap fillers, leaving only high-salary players that can't sum to <= $50k)
  const p4 = !p3 ? probeNoFloor(minStack, 1, 0) : null;

  // Salary range: min and sum-of-8-cheapest to surface salary floor issues
  const salaries = eligible.map((p) => p.salary).sort((a, b) => a - b);
  const salMin = salaries[0];
  const salMax = salaries[salaries.length - 1];
  const min8 = salaries.slice(0, 8).reduce((s, v) => s + v, 0);

  // Per-slot position counts - the aggregate G/F/C counts don't reveal if a specific
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
    `scores: ${withScore}+ / ${withNegScore}- / ${withZeroScore}zero${withNanScore > 0 ? ` / ${withNanScore}NaN - ILP objective poisoned!` : ""}`,
    `top3: ${top3}`,
    `probe(teamStacks=${teamStackCount},stackSize=${minStack},bringBack=${bringBackThreshold}): ${p1 ? "PASS" : "FAIL"}`,
    `probe(teamStacks=${teamStackCount},stackSize=${minStack},bringBack=0): ${p2 ? "PASS" : "FAIL"}`,
    `probe(teamStacks=1,stackSize=${minStack},bringBack=0): ${p3 ? "PASS" : "FAIL"}`,
    ...(p4 !== null ? [`probe(teamStacks=1,stackSize=${minStack},bringBack=0,no-floor): ${p4 ? "PASS - salary floor was blocking" : `FAIL - check slot counts above for 0-player positions`}`] : []),
  ];
}
