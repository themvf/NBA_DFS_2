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
import { stringifyCsvLine } from "./csv";
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
  | "projCeiling"
  | "boomRate"
  | "propPts"
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
  ceilingBoost?: boolean;
  ceilingCount?: number;
  playerLocks?: number[];
  playerBlocks?: number[];
  blockedTeamIds?: number[];
  requiredTeamStacks?: NbaTeamStackRule[];
};

const SALARY_CAP = 50000;
const SALARY_FLOOR = 49000;   // DK lineups must spend close to the cap
const ROSTER_SIZE = 8;
const LINEUP_SLOTS: LineupSlot[] = ["PG", "SG", "SF", "PF", "C", "G", "F", "UTIL"];
const DIV_WINDOW = 5;
const GLOBAL_KEEP_COUNT = 42;
const SLOT_KEEP_COUNT = 18;
const CHEAP_KEEP_COUNT = 14;
const TEAM_KEEP_COUNT = 6;
const REQUIRED_TEAM_KEEP_COUNT = 10;
const TEMPLATE_FILL_KEEP_COUNT = 30;
const TEMPLATE_CANDIDATE_LIMIT = 80;
const MAX_REPAIR_ATTEMPTS = 8;
const SLOT_BRANCH_LIMIT = 24;
const BEAM_WIDTH = 160;
const NBA_CEILING_BONUSES = [2.5, 1.75, 1.0, 0.5, 0.25] as const;

type NbaHeuristicFailureReason =
  | "exposure_exhausted"
  | "no_valid_templates"
  | "salary_feasible_fill_not_found"
  | "diversity_repair_exhausted";

type NbaHeuristicAttemptMeta = {
  prunedCandidateCount: number;
  templateCount: number;
  templatesTried: number;
  repairAttempts: number;
  rejectedByReason: Record<string, number>;
  failureReason?: string;
};

type NbaStackTemplate = {
  id: string;
  stackTeamIds: number[];
  minCountsByTeam: Map<number, number>;
  score: number;
};

type NbaLineupValidationResult =
  | { ok: true; slots: Record<LineupSlot, OptimizerPlayer> }
  | { ok: false; reason: string };

type DetailedSolveResult = {
  lineup: GeneratedLineup | null;
  meta: NbaHeuristicAttemptMeta;
};

function createAttemptMeta(prunedCandidateCount: number): NbaHeuristicAttemptMeta {
  return {
    prunedCandidateCount,
    templateCount: 0,
    templatesTried: 0,
    repairAttempts: 0,
    rejectedByReason: {},
  };
}

function incrementReject(meta: NbaHeuristicAttemptMeta, reason: string) {
  meta.rejectedByReason[reason] = (meta.rejectedByReason[reason] ?? 0) + 1;
}

function mergeHeuristicMeta(
  target: NbaHeuristicAttemptMeta,
  source: NbaHeuristicAttemptMeta,
) {
  target.prunedCandidateCount = Math.max(target.prunedCandidateCount, source.prunedCandidateCount);
  target.templateCount = Math.max(target.templateCount, source.templateCount);
  target.templatesTried += source.templatesTried;
  target.repairAttempts += source.repairAttempts;
  for (const [reason, count] of Object.entries(source.rejectedByReason)) {
    target.rejectedByReason[reason] = (target.rejectedByReason[reason] ?? 0) + count;
  }
  if (!target.failureReason && source.failureReason) {
    target.failureReason = source.failureReason;
  }
}

function pickFailureReason(meta: NbaHeuristicAttemptMeta): string {
  if (meta.failureReason) return meta.failureReason;
  if (meta.rejectedByReason.diversity_repair_exhausted) return "diversity_repair_exhausted";
  if (meta.rejectedByReason.salary_feasible_fill_not_found) return "salary_feasible_fill_not_found";
  if (meta.templateCount === 0) return "no_valid_templates";
  return "salary_feasible_fill_not_found";
}

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

// GPP leverage weight — how much leverage adds on top of projection in search score.
// Projection is the primary signal; leverage is a differentiator.
// Calibrated to live_leverage quartile data (Q4 avg_lev ≈ +6.0, avg_proj ≈ 25.0):
// at weight=0.6, a Q4 player scores ~3.6 pts above a neutral player at the same projection,
// which is enough to prefer the contrarian play without overriding a 5+ FPTS projection gap.
const NBA_GPP_LEVERAGE_WEIGHT = 0.6;
const NBA_CASH_LEVERAGE_WEIGHT = 0.1;
const NBA_GPP_CEILING_EDGE_WEIGHT = 0.18;
const NBA_GPP_BOOM_WEIGHT = 5.0;
const NBA_GPP_CHALK_PENALTY_START = 18.0;
const NBA_GPP_CHALK_PENALTY_WEIGHT = 0.035;
const NBA_GPP_LINEUP_OWNERSHIP_TARGET = 125.0;
const NBA_GPP_LINEUP_OWNERSHIP_WEIGHT = 0.045;
const NBA_GPP_HIGH_OWNED_PLAYER_THRESHOLD = 20.0;
const NBA_GPP_HIGH_OWNED_PLAYER_ALLOWANCE = 3;
const NBA_GPP_HIGH_OWNED_PLAYER_PENALTY = 0.9;
const NBA_GPP_STACK_OWNERSHIP_THRESHOLD = 48.0;
const NBA_GPP_STACK_OWNERSHIP_WEIGHT = 0.03;

function getNbaGppBonus(p: OptimizerPlayer): number {
  const projection = getPlayerProjection(p) ?? 0;
  if (projection <= 0) return 0;

  const ceiling = finiteOrNull(p.projCeiling) ?? (projection * 1.18);
  const ceilingEdge = Math.max(0, ceiling - projection);
  const boomRate = Math.max(0, finiteOrNull(p.boomRate) ?? 0);
  const projectedOwnership = Math.max(0, finiteOrNull(p.projOwnPct) ?? 0);
  const chalkPenalty = Math.max(0, projectedOwnership - NBA_GPP_CHALK_PENALTY_START) * NBA_GPP_CHALK_PENALTY_WEIGHT;

  return (ceilingEdge * NBA_GPP_CEILING_EDGE_WEIGHT)
    + (boomRate * NBA_GPP_BOOM_WEIGHT)
    - chalkPenalty;
}

function getProjectedOwnership(p: OptimizerPlayer): number {
  return Math.max(0, finiteOrNull(p.projOwnPct) ?? 0);
}

function isHighOwnedNbaPlayer(p: OptimizerPlayer): boolean {
  return getProjectedOwnership(p) >= NBA_GPP_HIGH_OWNED_PLAYER_THRESHOLD;
}

function getMaxTeamOwnership(teamOwnership: Map<number, number>): number {
  let maxOwnership = 0;
  for (const ownership of teamOwnership.values()) {
    if (ownership > maxOwnership) maxOwnership = ownership;
  }
  return maxOwnership;
}

function getNbaLineupDuplicationPenalty(
  totalOwnership: number,
  highOwnedCount: number,
  teamOwnership: Map<number, number>,
  mode: "cash" | "gpp",
): number {
  if (mode !== "gpp") return 0;

  const totalOwnershipPenalty = Math.max(0, totalOwnership - NBA_GPP_LINEUP_OWNERSHIP_TARGET) * NBA_GPP_LINEUP_OWNERSHIP_WEIGHT;
  const highOwnedPenalty = Math.max(0, highOwnedCount - NBA_GPP_HIGH_OWNED_PLAYER_ALLOWANCE) * NBA_GPP_HIGH_OWNED_PLAYER_PENALTY;
  const stackOwnershipPenalty = Math.max(0, getMaxTeamOwnership(teamOwnership) - NBA_GPP_STACK_OWNERSHIP_THRESHOLD) * NBA_GPP_STACK_OWNERSHIP_WEIGHT;

  return totalOwnershipPenalty + highOwnedPenalty + stackOwnershipPenalty;
}

function getPlayerScore(p: OptimizerPlayer, mode: "cash" | "gpp"): number {
  const projection = getPlayerProjection(p) ?? 0;
  const leverage = finiteOrNull(p.ourLeverage) ?? 0;
  const weight = mode === "gpp" ? NBA_GPP_LEVERAGE_WEIGHT : NBA_CASH_LEVERAGE_WEIGHT;
  return projection + leverage * weight + (mode === "gpp" ? getNbaGppBonus(p) : 0);
}

function getSearchScore(
  p: OptimizerPlayer,
  mode: "cash" | "gpp",
  scoreAdjustments?: Map<number, number>,
): number {
  return getPlayerScore(p, mode) + (scoreAdjustments?.get(p.id) ?? 0);
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

function comparePlayersForSearch(
  a: OptimizerPlayer,
  b: OptimizerPlayer,
  mode: "cash" | "gpp",
  scoreAdjustments?: Map<number, number>,
): number {
  const scoreDiff = getSearchScore(b, mode, scoreAdjustments) - getSearchScore(a, mode, scoreAdjustments);
  if (Math.abs(scoreDiff) > 1e-9) return scoreDiff;
  const projDiff = (getPlayerProjection(b) ?? 0) - (getPlayerProjection(a) ?? 0);
  if (Math.abs(projDiff) > 1e-9) return projDiff;
  const levDiff = getPlayerLeverage(b) - getPlayerLeverage(a);
  if (Math.abs(levDiff) > 1e-9) return levDiff;
  const salaryDiff = a.salary - b.salary;
  if (salaryDiff !== 0) return salaryDiff;
  return a.id - b.id;
}

function sortPlayersForSearch(
  players: OptimizerPlayer[],
  mode: "cash" | "gpp",
  scoreAdjustments?: Map<number, number>,
): OptimizerPlayer[] {
  return [...players].sort((a, b) => comparePlayersForSearch(a, b, mode, scoreAdjustments));
}

function rankMetric(
  value: number | null | undefined,
  values: Array<number | null | undefined>,
  higherIsBetter = true,
): number {
  if (value == null) return 0.5;
  const numeric = values.filter((entry): entry is number => entry != null && Number.isFinite(entry));
  if (numeric.length === 0) return 0.5;

  let below = 0;
  let equal = 0;
  for (const entry of numeric) {
    if (entry < value) below++;
    else if (entry === value) equal++;
  }
  const percentile = (below + equal * 0.5) / numeric.length;
  return higherIsBetter ? percentile : 1 - percentile;
}

export function computeNbaCeilingBonusMap(
  players: OptimizerPlayer[],
  topCount: number,
): Map<number, number> {
  const activePlayers = players.filter((player) => !player.isOut && (getPlayerProjection(player) ?? 0) > 0);
  const ceilingCount = Math.max(0, Math.min(topCount, activePlayers.length));
  if (ceilingCount === 0) return new Map<number, number>();

  const contexts = activePlayers.map((player) => {
    const projection = getPlayerProjection(player);
    const ceiling = finiteOrNull(player.projCeiling)
      ?? (projection != null ? projection * 1.18 : null);
    const boomRate = finiteOrNull(player.boomRate);
    const pointsProp = finiteOrNull(player.propPts);
    const value = projection != null ? projection / Math.max(1, player.salary / 1000) : null;
    return {
      player,
      projection,
      ceiling,
      boomRate,
      pointsProp,
      value,
      score: 0,
    };
  });

  const ceilingValues = contexts.map((context) => context.ceiling);
  const boomValues = contexts.map((context) => context.boomRate);
  const pointsPropValues = contexts.map((context) => context.pointsProp);
  const projectionValues = contexts.map((context) => context.projection);
  const valueValues = contexts.map((context) => context.value);

  for (const context of contexts) {
    context.score =
      rankMetric(context.ceiling, ceilingValues, true) * 0.46 +
      rankMetric(context.boomRate, boomValues, true) * 0.24 +
      rankMetric(context.pointsProp, pointsPropValues, true) * 0.12 +
      rankMetric(context.projection, projectionValues, true) * 0.10 +
      rankMetric(context.value, valueValues, true) * 0.08;
  }

  const sorted = [...contexts].sort((a, b) => {
    const diff = b.score - a.score;
    return diff !== 0 ? diff : a.player.name.localeCompare(b.player.name);
  });

  const result = new Map<number, number>();
  for (const [index, context] of sorted.slice(0, ceilingCount).entries()) {
    result.set(context.player.id, NBA_CEILING_BONUSES[index] ?? 0.25);
  }
  return result;
}

function buildOpponentByTeamId(pool: OptimizerPlayer[]): Map<number, number> {
  const teamsByMatchup = new Map<number, Set<number>>();
  for (const player of pool) {
    if (player.matchupId == null || player.teamId == null) continue;
    const existing = teamsByMatchup.get(player.matchupId) ?? new Set<number>();
    existing.add(player.teamId);
    teamsByMatchup.set(player.matchupId, existing);
  }

  const opponentByTeamId = new Map<number, number>();
  for (const teams of teamsByMatchup.values()) {
    const matchupTeams = Array.from(teams);
    if (matchupTeams.length !== 2) continue;
    opponentByTeamId.set(matchupTeams[0], matchupTeams[1]);
    opponentByTeamId.set(matchupTeams[1], matchupTeams[0]);
  }
  return opponentByTeamId;
}

function buildTeamPlayersMap(
  pool: OptimizerPlayer[],
  mode: "cash" | "gpp",
  scoreAdjustments?: Map<number, number>,
): Map<number, OptimizerPlayer[]> {
  const byTeam = new Map<number, OptimizerPlayer[]>();
  for (const player of pool) {
    if (player.teamId == null) continue;
    const existing = byTeam.get(player.teamId) ?? [];
    existing.push(player);
    byTeam.set(player.teamId, existing);
  }
  for (const [teamId, players] of byTeam) {
    byTeam.set(teamId, sortPlayersForSearch(players, mode, scoreAdjustments));
  }
  return byTeam;
}

function addTopPlayers(
  keepIds: Set<number>,
  players: OptimizerPlayer[],
  count: number,
) {
  for (const player of players.slice(0, count)) {
    keepIds.add(player.id);
  }
}

function addCheapestPlayers(
  keepIds: Set<number>,
  players: OptimizerPlayer[],
  count: number,
) {
  const cheapest = [...players]
    .sort((a, b) => a.salary - b.salary || comparePlayersForSearch(a, b, "cash"))
    .slice(0, count);
  for (const player of cheapest) {
    keepIds.add(player.id);
  }
}

function getStackThreshold(
  teamId: number,
  minStack: number,
  requiredStackSizeByTeam: Map<number, number>,
): number {
  return requiredStackSizeByTeam.get(teamId) ?? minStack;
}

function pruneNbaCandidatePool(
  pool: OptimizerPlayer[],
  mode: "cash" | "gpp",
  minStack: number,
  bringBackSize: number,
  ruleSelections: NormalizedNbaRuleSelections,
): OptimizerPlayer[] {
  if (pool.length <= 180) {
    return sortPlayersForSearch(pool, mode);
  }

  const keepIds = new Set<number>(ruleSelections.playerLocks);
  const sorted = sortPlayersForSearch(pool, mode);
  const byTeam = buildTeamPlayersMap(pool, mode);
  const opponentByTeamId = buildOpponentByTeamId(pool);
  const requiredStackSizeByTeam = new Map(
    ruleSelections.requiredTeamStacks.map((rule) => [rule.teamId, rule.stackSize] as const),
  );

  addTopPlayers(keepIds, sorted, GLOBAL_KEEP_COUNT);
  addTopPlayers(
    keepIds,
    [...pool].sort(
      (a, b) => (getPlayerProjection(b) ?? 0) - (getPlayerProjection(a) ?? 0) || comparePlayersForSearch(a, b, mode),
    ),
    GLOBAL_KEEP_COUNT,
  );
  if (mode === "gpp") {
    addTopPlayers(
      keepIds,
      [...pool].sort((a, b) => getPlayerLeverage(b) - getPlayerLeverage(a) || comparePlayersForSearch(a, b, mode)),
      Math.max(18, Math.floor(GLOBAL_KEEP_COUNT / 2)),
    );
  }
  addCheapestPlayers(keepIds, pool, CHEAP_KEEP_COUNT);

  for (const slot of LINEUP_SLOTS) {
    addTopPlayers(
      keepIds,
      sorted.filter((player) => canFillSlot(slot, player.eligiblePositions)),
      SLOT_KEEP_COUNT,
    );
  }

  for (const [teamId, teamPlayers] of byTeam) {
    const requiredCount = getStackThreshold(teamId, minStack, requiredStackSizeByTeam);
    const keepCount = requiredStackSizeByTeam.has(teamId)
      ? Math.max(REQUIRED_TEAM_KEEP_COUNT, requiredCount + bringBackSize + 4)
      : Math.max(TEAM_KEEP_COUNT, requiredCount + bringBackSize + 2);
    addTopPlayers(keepIds, teamPlayers, keepCount);

    if (bringBackSize > 0) {
      const opponentTeamId = opponentByTeamId.get(teamId);
      if (opponentTeamId != null) {
        addTopPlayers(
          keepIds,
          byTeam.get(opponentTeamId) ?? [],
          Math.max(TEAM_KEEP_COUNT, bringBackSize + 3),
        );
      }
    }
  }

  return sortPlayersForSearch(
    pool.filter((player) => keepIds.has(player.id)),
    mode,
  );
}

function sumTopTeamScores(
  players: OptimizerPlayer[],
  count: number,
  mode: "cash" | "gpp",
  scoreAdjustments?: Map<number, number>,
): number {
  const selected = players.slice(0, count);
  const baseScore = selected
    .reduce((total, player) => total + getSearchScore(player, mode, scoreAdjustments) + ((getPlayerProjection(player) ?? 0) * 0.01), 0);
  if (mode !== "gpp") return baseScore;

  const teamOwnership = new Map<number, number>();
  for (const player of selected) {
    if (player.teamId == null) continue;
    teamOwnership.set(player.teamId, (teamOwnership.get(player.teamId) ?? 0) + getProjectedOwnership(player));
  }

  return baseScore - getNbaLineupDuplicationPenalty(
    selected.reduce((sum, player) => sum + getProjectedOwnership(player), 0),
    selected.filter((player) => isHighOwnedNbaPlayer(player)).length,
    teamOwnership,
    mode,
  );
}

function enumerateTeamCombinations(teamIds: number[], size: number): number[][] {
  if (size <= 0) return [[]];
  if (teamIds.length < size) return [];

  const combinations: number[][] = [];
  const current: number[] = [];
  const visit = (start: number) => {
    if (current.length === size) {
      combinations.push([...current]);
      return;
    }
    for (let i = start; i <= teamIds.length - (size - current.length); i++) {
      current.push(teamIds[i]);
      visit(i + 1);
      current.pop();
    }
  };
  visit(0);
  return combinations;
}

function enumerateStackTemplates(
  pool: OptimizerPlayer[],
  mode: "cash" | "gpp",
  minStack: number,
  teamStackCount: number,
  bringBackSize: number,
  ruleSelections: NormalizedNbaRuleSelections,
  scoreAdjustments?: Map<number, number>,
): NbaStackTemplate[] {
  const byTeam = buildTeamPlayersMap(pool, mode, scoreAdjustments);
  const availableTeamCounts = new Map<number, number>();
  for (const [teamId, teamPlayers] of byTeam) {
    availableTeamCounts.set(teamId, teamPlayers.length);
  }
  const opponentByTeamId = buildOpponentByTeamId(pool);
  const requiredStackSizeByTeam = new Map(
    ruleSelections.requiredTeamStacks.map((rule) => [rule.teamId, rule.stackSize] as const),
  );
  const requiredTeamIds = new Set(ruleSelections.requiredTeamStacks.map((rule) => rule.teamId));

  const countableTeamIds = Array.from(availableTeamCounts.keys()).filter((teamId) => {
    const stackThreshold = getStackThreshold(teamId, minStack, requiredStackSizeByTeam);
    if ((availableTeamCounts.get(teamId) ?? 0) < stackThreshold) return false;
    if (bringBackSize <= 0) return true;
    const opponentTeamId = opponentByTeamId.get(teamId);
    return opponentTeamId != null && (availableTeamCounts.get(opponentTeamId) ?? 0) >= bringBackSize;
  });

  if (countableTeamIds.length < teamStackCount) {
    return [];
  }

  const templates: NbaStackTemplate[] = [];
  for (const combination of enumerateTeamCombinations(countableTeamIds, teamStackCount)) {
    if (requiredTeamIds.size > 0 && !combination.some((teamId) => requiredTeamIds.has(teamId))) {
      continue;
    }

    const minCountsByTeam = new Map<number, number>();
    let feasible = true;
    for (const teamId of combination) {
      const stackThreshold = getStackThreshold(teamId, minStack, requiredStackSizeByTeam);
      minCountsByTeam.set(teamId, Math.max(minCountsByTeam.get(teamId) ?? 0, stackThreshold));

      if (bringBackSize > 0) {
        const opponentTeamId = opponentByTeamId.get(teamId);
        if (opponentTeamId == null) {
          feasible = false;
          break;
        }
        minCountsByTeam.set(opponentTeamId, Math.max(minCountsByTeam.get(opponentTeamId) ?? 0, bringBackSize));
      }
    }
    if (!feasible) continue;

    let totalRequiredPlayers = 0;
    let templateScore = 0;
    for (const [teamId, requiredCount] of minCountsByTeam) {
      const teamPlayers = byTeam.get(teamId) ?? [];
      if (teamPlayers.length < requiredCount) {
        feasible = false;
        break;
      }
      totalRequiredPlayers += requiredCount;
      templateScore += sumTopTeamScores(teamPlayers, requiredCount, mode, scoreAdjustments);
    }
    if (!feasible || totalRequiredPlayers > ROSTER_SIZE) continue;

    templates.push({
      id: combination.join("-"),
      stackTeamIds: [...combination],
      minCountsByTeam,
      score: templateScore,
    });
  }

  return templates
    .sort((a, b) => b.score - a.score || a.id.localeCompare(b.id))
    .slice(0, TEMPLATE_CANDIDATE_LIMIT);
}

function buildTemplatePool(
  prunedPool: OptimizerPlayer[],
  template: NbaStackTemplate,
  mode: "cash" | "gpp",
  lockedPlayers: Set<number>,
  scoreAdjustments?: Map<number, number>,
): OptimizerPlayer[] {
  const keepIds = new Set<number>(lockedPlayers);
  const templateTeams = new Set(template.minCountsByTeam.keys());
  const sorted = sortPlayersForSearch(prunedPool, mode, scoreAdjustments);

  for (const player of prunedPool) {
    if (player.teamId != null && templateTeams.has(player.teamId)) {
      keepIds.add(player.id);
    }
  }

  addTopPlayers(keepIds, sorted, TEMPLATE_FILL_KEEP_COUNT);
  addCheapestPlayers(keepIds, prunedPool, 8);

  return sorted.filter((player) => keepIds.has(player.id));
}

function assignPlayersToSlots(players: OptimizerPlayer[]): Record<LineupSlot, OptimizerPlayer> | null {
  const slotOptions = players
    .map((player) => ({
      player,
      slots: LINEUP_SLOTS.filter((slot) => canFillSlot(slot, player.eligiblePositions)),
    }))
    .sort((a, b) => a.slots.length - b.slots.length || a.player.id - b.player.id);

  const assignment = {} as Record<LineupSlot, OptimizerPlayer>;
  const usedSlots = new Set<LineupSlot>();
  const visit = (index: number): boolean => {
    if (index >= slotOptions.length) return true;
    const { player, slots } = slotOptions[index];
    for (const slot of slots) {
      if (usedSlots.has(slot)) continue;
      usedSlots.add(slot);
      assignment[slot] = player;
      if (visit(index + 1)) return true;
      usedSlots.delete(slot);
      delete assignment[slot];
    }
    return false;
  };

  return visit(0) ? assignment : null;
}

function buildExactLineupFromPlayers(players: OptimizerPlayer[]): GeneratedLineup | null {
  const slots = assignPlayersToSlots(players);
  if (!slots) return null;
  return {
    players: [...players],
    slots,
    totalSalary: players.reduce((sum, player) => sum + player.salary, 0),
    projFpts: players.reduce((sum, player) => sum + (getPlayerProjection(player) ?? 0), 0),
    leverageScore: players.reduce((sum, player) => sum + getPlayerLeverage(player), 0),
  };
}

function calculateSharedCount(players: OptimizerPlayer[], previousLineup: Set<number>): number {
  let shared = 0;
  for (const player of players) {
    if (previousLineup.has(player.id)) shared++;
  }
  return shared;
}

function validateLineupExact(
  players: OptimizerPlayer[],
  minStack: number,
  teamStackCount: number,
  previousLineupSets: Set<number>[],
  bringBackSize: number,
  minChanges: number,
  salaryFloor: number,
  ruleSelections: NormalizedNbaRuleSelections,
): NbaLineupValidationResult {
  if (players.length !== ROSTER_SIZE) {
    return { ok: false, reason: "invalid_roster_size" };
  }
  if (new Set(players.map((player) => player.id)).size !== ROSTER_SIZE) {
    return { ok: false, reason: "duplicate_player" };
  }

  const totalSalary = players.reduce((sum, player) => sum + player.salary, 0);
  if (totalSalary > SALARY_CAP) return { ok: false, reason: "salary_cap_exceeded" };
  if (totalSalary < salaryFloor) return { ok: false, reason: "salary_floor_missed" };

  const lockedPlayers = new Set(ruleSelections.playerLocks);
  for (const playerId of lockedPlayers) {
    if (!players.some((player) => player.id === playerId)) {
      return { ok: false, reason: "locked_player_missing" };
    }
  }

  const slots = assignPlayersToSlots(players);
  if (!slots) {
    return { ok: false, reason: "slot_assignment_failed" };
  }

  const teamCounts = new Map<number, number>();
  for (const player of players) {
    if (player.teamId == null) continue;
    teamCounts.set(player.teamId, (teamCounts.get(player.teamId) ?? 0) + 1);
  }

  const opponentByTeamId = buildOpponentByTeamId(players);
  const requiredStackSizeByTeam = new Map(
    ruleSelections.requiredTeamStacks.map((rule) => [rule.teamId, rule.stackSize] as const),
  );
  const countableTeamIds = Array.from(teamCounts.keys()).filter((teamId) => {
    const stackThreshold = getStackThreshold(teamId, minStack, requiredStackSizeByTeam);
    if ((teamCounts.get(teamId) ?? 0) < stackThreshold) return false;
    if (bringBackSize <= 0) return true;
    const opponentTeamId = opponentByTeamId.get(teamId);
    if (opponentTeamId == null) return false;
    return (teamCounts.get(opponentTeamId) ?? 0) >= bringBackSize;
  });

  if (countableTeamIds.length < teamStackCount) {
    return { ok: false, reason: "stack_count_insufficient" };
  }
  if (
    ruleSelections.requiredTeamStacks.length > 0
    && !ruleSelections.requiredTeamStacks.some((rule) => countableTeamIds.includes(rule.teamId))
  ) {
    return { ok: false, reason: "required_team_stack_missing" };
  }

  const recentLineups = previousLineupSets.slice(-DIV_WINDOW);
  const maxShared = ROSTER_SIZE - minChanges;
  for (const previousLineup of recentLineups) {
    if (calculateSharedCount(players, previousLineup) > maxShared) {
      return { ok: false, reason: "diversity_violation" };
    }
  }

  return { ok: true, slots };
}

function solveReducedLineup(
  pool: OptimizerPlayer[],
  mode: "cash" | "gpp",
  minCountsByTeam: Map<number, number>,
  lockedPlayers: Set<number>,
  salaryFloor: number,
  scoreAdjustments?: Map<number, number>,
): GeneratedLineup | null {
  if (pool.length < ROSTER_SIZE) return null;
  const slotCandidates = new Map<LineupSlot, OptimizerPlayer[]>();
  for (const slot of LINEUP_SLOTS) {
    const candidates = sortPlayersForSearch(
      pool.filter((player) => canFillSlot(slot, player.eligiblePositions)),
      mode,
      scoreAdjustments,
    );
    if (candidates.length === 0) return null;
    slotCandidates.set(slot, candidates);
  }

  const orderedSlots = [...LINEUP_SLOTS].sort((a, b) => {
    const diff = (slotCandidates.get(a)?.length ?? 0) - (slotCandidates.get(b)?.length ?? 0);
    return diff !== 0 ? diff : LINEUP_SLOTS.indexOf(a) - LINEUP_SLOTS.indexOf(b);
  });

  type SearchState = {
    slotAssignment: Partial<Record<LineupSlot, OptimizerPlayer>>;
    selectedIds: Set<number>;
    teamCounts: Map<number, number>;
    teamOwnership: Map<number, number>;
    salary: number;
    score: number;
    projection: number;
    leverage: number;
    ownership: number;
    highOwnedCount: number;
  };

  function canMeetTeamMinimums(
    selectedIds: Set<number>,
    teamCounts: Map<number, number>,
    remainingSlots: readonly LineupSlot[],
  ): boolean {
    for (const [teamId, minCount] of minCountsByTeam) {
      const current = teamCounts.get(teamId) ?? 0;
      if (current >= minCount) continue;
      const remainingNeeded = minCount - current;
      let available = 0;
      for (const player of pool) {
        if (player.teamId !== teamId || selectedIds.has(player.id)) continue;
        if (remainingSlots.some((slot) => canFillSlot(slot, player.eligiblePositions))) {
          available++;
        }
      }
      if (available < remainingNeeded) return false;
    }
    return true;
  }

  function canPlaceLockedPlayers(selectedIds: Set<number>, remainingSlots: readonly LineupSlot[]): boolean {
    const missingLocks = Array.from(lockedPlayers).filter((playerId) => !selectedIds.has(playerId));
    if (missingLocks.length > remainingSlots.length) return false;
    for (const playerId of missingLocks) {
      const player = pool.find((candidate) => candidate.id === playerId);
      if (!player) return false;
      if (!remainingSlots.some((slot) => canFillSlot(slot, player.eligiblePositions))) {
        return false;
      }
    }
    return true;
  }

  function salaryBounds(selectedIds: Set<number>, remainingCount: number): { min: number; max: number } {
    const unused = pool.filter((player) => !selectedIds.has(player.id));
    const bySalaryAsc = [...unused].sort((a, b) => a.salary - b.salary);
    const bySalaryDesc = [...unused].sort((a, b) => b.salary - a.salary);
    return {
      min: bySalaryAsc.slice(0, remainingCount).reduce((sum, player) => sum + player.salary, 0),
      max: bySalaryDesc.slice(0, remainingCount).reduce((sum, player) => sum + player.salary, 0),
    };
  }

  function scoreUpperBound(selectedIds: Set<number>, remainingCount: number): number {
    return sortPlayersForSearch(
      pool.filter((player) => !selectedIds.has(player.id)),
      mode,
      scoreAdjustments,
    )
      .slice(0, remainingCount)
      .reduce((sum, player) => sum + getSearchScore(player, mode, scoreAdjustments), 0);
  }

  function estimateStateScore(state: SearchState, remainingCount: number): number {
    const missingLocks = Array.from(lockedPlayers).filter((playerId) => !state.selectedIds.has(playerId)).length;
    let teamShortfall = 0;
    for (const [teamId, minCount] of minCountsByTeam) {
      teamShortfall += Math.max(0, minCount - (state.teamCounts.get(teamId) ?? 0));
    }
    const duplicationPenalty = getNbaLineupDuplicationPenalty(
      state.ownership,
      state.highOwnedCount,
      state.teamOwnership,
      mode,
    );
    return state.score
      + scoreUpperBound(state.selectedIds, remainingCount)
      - duplicationPenalty
      - (missingLocks * 1000)
      - (teamShortfall * 250);
  }

  function branchCandidatesForSlot(
    slot: LineupSlot,
    state: SearchState,
  ): OptimizerPlayer[] {
    const baseCandidates = (slotCandidates.get(slot) ?? []).filter((player) => !state.selectedIds.has(player.id));
    const nextIds = new Set<number>();
    const selected: OptimizerPlayer[] = [];

    const pushCandidate = (player: OptimizerPlayer) => {
      if (nextIds.has(player.id)) return;
      nextIds.add(player.id);
      selected.push(player);
    };

    for (const player of baseCandidates.filter((candidate) => lockedPlayers.has(candidate.id))) {
      pushCandidate(player);
    }
    for (const player of baseCandidates.slice(0, SLOT_BRANCH_LIMIT)) {
      pushCandidate(player);
    }
    for (const player of [...baseCandidates]
      .sort((a, b) => a.salary - b.salary || comparePlayersForSearch(a, b, mode, scoreAdjustments))
      .slice(0, Math.max(6, Math.floor(SLOT_BRANCH_LIMIT / 3)))) {
      pushCandidate(player);
    }
    for (const [teamId, minCount] of minCountsByTeam) {
      if ((state.teamCounts.get(teamId) ?? 0) >= minCount) continue;
      for (const player of baseCandidates.filter((candidate) => candidate.teamId === teamId).slice(0, 4)) {
        pushCandidate(player);
      }
    }

    return selected;
  }

  let states: SearchState[] = [{
    slotAssignment: {},
    selectedIds: new Set<number>(),
    teamCounts: new Map<number, number>(),
    teamOwnership: new Map<number, number>(),
    salary: 0,
    score: 0,
    projection: 0,
    leverage: 0,
    ownership: 0,
    highOwnedCount: 0,
  }];

  for (let depth = 0; depth < orderedSlots.length; depth++) {
    const slot = orderedSlots[depth];
    const remainingSlots = orderedSlots.slice(depth + 1);
    const remainingCount = remainingSlots.length;
    const nextStates: Array<SearchState & { estimate: number }> = [];

    for (const state of states) {
      const candidates = branchCandidatesForSlot(slot, state);

      for (const player of candidates) {
        const selectedIds = new Set(state.selectedIds);
        selectedIds.add(player.id);
        const teamCounts = new Map(state.teamCounts);
        const teamOwnership = new Map(state.teamOwnership);
        if (player.teamId != null) {
          teamCounts.set(player.teamId, (teamCounts.get(player.teamId) ?? 0) + 1);
          teamOwnership.set(player.teamId, (teamOwnership.get(player.teamId) ?? 0) + getProjectedOwnership(player));
        }

        const salary = state.salary + player.salary;
        if (salary > SALARY_CAP) continue;
        if (!canMeetTeamMinimums(selectedIds, teamCounts, remainingSlots)) continue;
        if (!canPlaceLockedPlayers(selectedIds, remainingSlots)) continue;

        const salaryRange = salaryBounds(selectedIds, remainingCount);
        if (salary + salaryRange.min > SALARY_CAP) continue;
        if (salary + salaryRange.max < salaryFloor) continue;

        const nextState: SearchState = {
          slotAssignment: {
            ...state.slotAssignment,
            [slot]: player,
          },
          selectedIds,
          teamCounts,
          teamOwnership,
          salary,
          score: state.score + getSearchScore(player, mode, scoreAdjustments),
          projection: state.projection + (getPlayerProjection(player) ?? 0),
          leverage: state.leverage + getPlayerLeverage(player),
          ownership: state.ownership + getProjectedOwnership(player),
          highOwnedCount: state.highOwnedCount + (isHighOwnedNbaPlayer(player) ? 1 : 0),
        };
        nextStates.push({
          ...nextState,
          estimate: estimateStateScore(nextState, remainingCount),
        });
      }
    }

    if (nextStates.length === 0) return null;

    nextStates.sort((a, b) => {
      const estimateDiff = b.estimate - a.estimate;
      if (Math.abs(estimateDiff) > 1e-9) return estimateDiff;
      const scoreDiff = b.score - a.score;
      if (Math.abs(scoreDiff) > 1e-9) return scoreDiff;
      return a.salary - b.salary;
    });
    states = nextStates.slice(0, BEAM_WIDTH).map(({ estimate: _estimate, ...state }) => state);
  }

  let bestState: SearchState | null = null;
  let bestScore = Number.NEGATIVE_INFINITY;
  for (const state of states) {
    if (state.salary < salaryFloor) continue;
    if (Array.from(lockedPlayers).some((playerId) => !state.selectedIds.has(playerId))) continue;
    if (!canMeetTeamMinimums(state.selectedIds, state.teamCounts, [])) continue;
    if (!canPlaceLockedPlayers(state.selectedIds, [])) continue;
    const penalizedScore = state.score - getNbaLineupDuplicationPenalty(
      state.ownership,
      state.highOwnedCount,
      state.teamOwnership,
      mode,
    );
    if (!bestState || penalizedScore > bestScore) {
      bestState = state;
      bestScore = penalizedScore;
    }
  }

  if (!bestState) return null;

  const slots = {} as Record<LineupSlot, OptimizerPlayer>;
  for (const slot of LINEUP_SLOTS) {
    const player = bestState.slotAssignment[slot];
    if (!player) return null;
    slots[slot] = player;
  }

  return {
    players: LINEUP_SLOTS.map((slot) => slots[slot]),
    slots,
    totalSalary: bestState.salary,
    projFpts: bestState.projection,
    leverageScore: bestState.leverage,
  };
}

function attemptRepairForDiversity(
  lineup: GeneratedLineup,
  pool: OptimizerPlayer[],
  mode: "cash" | "gpp",
  minStack: number,
  teamStackCount: number,
  previousLineupSets: Set<number>[],
  bringBackSize: number,
  minChanges: number,
  salaryFloor: number,
  ruleSelections: NormalizedNbaRuleSelections,
): { lineup: GeneratedLineup | null; attempts: number } {
  const lockedPlayers = new Set(ruleSelections.playerLocks);
  const selectedIds = new Set(lineup.players.map((player) => player.id));
  const recentLineups = previousLineupSets.slice(-DIV_WINDOW);
  const overlapIds = new Set<number>();
  const maxShared = ROSTER_SIZE - minChanges;
  for (const previousLineup of recentLineups) {
    if (calculateSharedCount(lineup.players, previousLineup) > maxShared) {
      for (const player of lineup.players) {
        if (previousLineup.has(player.id)) overlapIds.add(player.id);
      }
    }
  }

  const replaceablePlayers = lineup.players
    .filter((player) => !lockedPlayers.has(player.id) && overlapIds.has(player.id))
    .sort((a, b) => comparePlayersForSearch(a, b, mode));
  const replacementPool = sortPlayersForSearch(
    pool.filter((player) => !selectedIds.has(player.id)),
    mode,
  );

  let attempts = 0;
  for (const currentPlayer of replaceablePlayers) {
    for (const replacement of replacementPool) {
      if (attempts >= MAX_REPAIR_ATTEMPTS) {
        return { lineup: null, attempts };
      }
      attempts++;
      const nextPlayers = lineup.players.map((player) => player.id === currentPlayer.id ? replacement : player);
      const validation = validateLineupExact(
        nextPlayers,
        minStack,
        teamStackCount,
        previousLineupSets,
        bringBackSize,
        minChanges,
        salaryFloor,
        ruleSelections,
      );
      if (!validation.ok) continue;
      return {
        lineup: {
          players: nextPlayers,
          slots: validation.slots,
          totalSalary: nextPlayers.reduce((sum, player) => sum + player.salary, 0),
          projFpts: nextPlayers.reduce((sum, player) => sum + (getPlayerProjection(player) ?? 0), 0),
          leverageScore: nextPlayers.reduce((sum, player) => sum + getPlayerLeverage(player), 0),
        },
        attempts,
      };
    }
  }

  return { lineup: null, attempts };
}

function buildRecentScoreAdjustments(
  previousLineupSets: Set<number>[],
  mode: "cash" | "gpp",
): Map<number, number> {
  const recentCounts = new Map<number, number>();
  for (const lineup of previousLineupSets.slice(-DIV_WINDOW)) {
    for (const playerId of lineup) {
      recentCounts.set(playerId, (recentCounts.get(playerId) ?? 0) + 1);
    }
  }

  const weight = mode === "gpp" ? 2.5 : 1.5;
  return new Map(
    Array.from(recentCounts.entries()).map(([playerId, count]) => [playerId, -(count * weight)]),
  );
}

function mergeScoreAdjustments(...maps: Array<Map<number, number>>): Map<number, number> {
  const merged = new Map<number, number>();
  for (const map of maps) {
    for (const [playerId, adjustment] of map) {
      merged.set(playerId, (merged.get(playerId) ?? 0) + adjustment);
    }
  }
  return merged;
}

function solveOneLineupDetailed(
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
  staticScoreAdjustments: Map<number, number> = new Map<number, number>(),
): DetailedSolveResult {
  const lockedPlayers = new Set(ruleSelections.playerLocks);
  const availablePool = pool.filter((player) =>
    lockedPlayers.has(player.id) || (exposureCount.get(player.id) ?? 0) < maxExposureCount,
  );
  const prunedPool = pruneNbaCandidatePool(availablePool, mode, minStack, bringBackSize, ruleSelections);
  const meta = createAttemptMeta(prunedPool.length);
  const scoreAdjustments = mergeScoreAdjustments(
    buildRecentScoreAdjustments(previousLineupSets, mode),
    staticScoreAdjustments,
  );

  if (availablePool.length < ROSTER_SIZE || prunedPool.length < ROSTER_SIZE) {
    meta.failureReason = "exposure_exhausted";
    incrementReject(meta, "exposure_exhausted");
    return { lineup: null, meta };
  }

  const templates = enumerateStackTemplates(
    prunedPool,
    mode,
    minStack,
    teamStackCount,
    bringBackSize,
    ruleSelections,
    scoreAdjustments,
  );
  meta.templateCount = templates.length;
  if (templates.length === 0) {
    meta.failureReason = "no_valid_templates";
    incrementReject(meta, "no_valid_templates");
    return { lineup: null, meta };
  }

  for (const template of templates) {
    meta.templatesTried++;
    const templatePool = buildTemplatePool(prunedPool, template, mode, lockedPlayers, scoreAdjustments);
    const lineup = solveReducedLineup(
      templatePool,
      mode,
      template.minCountsByTeam,
      lockedPlayers,
      salaryFloor,
      scoreAdjustments,
    );
    if (!lineup) {
      incrementReject(meta, "salary_feasible_fill_not_found");
      continue;
    }

    const validation = validateLineupExact(
      lineup.players,
      minStack,
      teamStackCount,
      previousLineupSets,
      bringBackSize,
      minChanges,
      salaryFloor,
      ruleSelections,
    );
    if (validation.ok) {
      return {
        lineup: {
          ...lineup,
          slots: validation.slots,
        },
        meta,
      };
    }

    if (validation.reason === "diversity_violation") {
      const repaired = attemptRepairForDiversity(
        lineup,
        templatePool,
        mode,
        minStack,
        teamStackCount,
        previousLineupSets,
        bringBackSize,
        minChanges,
        salaryFloor,
        ruleSelections,
      );
      meta.repairAttempts += repaired.attempts;
      if (repaired.lineup) {
        return { lineup: repaired.lineup, meta };
      }
      incrementReject(meta, "diversity_repair_exhausted");
      continue;
    }

    incrementReject(meta, validation.reason);
  }

  meta.failureReason = pickFailureReason(meta);
  return { lineup: null, meta };
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
  const ceilingBoost = settings.ceilingBoost === true;
  const ceilingCount = Math.max(1, Math.min(5, Math.floor(settings.ceilingCount ?? 3)));
  const ceilingBonusMap = ceilingBoost
    ? computeNbaCeilingBonusMap(eligible, ceilingCount)
    : new Map<number, number>();

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
      ceilingBoost,
      ceilingCount,
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
      ceilingBonusMap,
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
  const prunedPool = pruneNbaCandidatePool(eligible, mode, effectiveMinStack, effectiveBringBackSize, ruleSelections);
  debug.effectiveSettings = {
    minStack: effectiveMinStack,
    teamStackCount: effectiveTeamStackCount,
    bringBackEnabled: effectiveBringBackSize > 0,
    bringBackSize: effectiveBringBackSize,
    maxExposure,
    ceilingBoost,
    ceilingCount,
    minChanges: effectiveMinChanges,
    salaryFloor: effectiveSalaryFloor,
  };
  debug.heuristic = {
    prunedCandidateCount: prunedPool.length,
    templateCount: 0,
    templatesTried: 0,
    repairAttempts: 0,
    rejectedByReason: {},
  };

  const exposureCount = new Map<number, number>(prunedPool.map((p) => [p.id, 0]));
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
      const result = solveOneLineupDetailed(
        prunedPool,
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
        ceilingBonusMap,
      );
      mergeHeuristicMeta(debug.heuristic!, result.meta);
      attempts.push({
        stage,
        success: result.lineup != null,
        durationMs: Date.now() - start,
        prunedCandidateCount: result.meta.prunedCandidateCount,
        templateCount: result.meta.templateCount,
        templatesTried: result.meta.templatesTried,
        repairAttempts: result.meta.repairAttempts,
        rejectedByReason: result.meta.rejectedByReason,
        failureReason: result.meta.failureReason,
      });
      return result.lineup;
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
  const ceilingBoost = settings.ceilingBoost === true;
  const ceilingCount = Math.max(1, Math.min(5, Math.floor(settings.ceilingCount ?? 3)));
  const ceilingBonusMap = ceilingBoost
    ? computeNbaCeilingBonusMap(eligible, ceilingCount)
    : new Map<number, number>();

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
      ceilingBoost,
      ceilingCount,
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
      ceilingBonusMap,
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
  const prunedPool = pruneNbaCandidatePool(eligible, mode, effectiveMinStack, effectiveBringBackSize, ruleSelections);
  debug.effectiveSettings = {
    minStack: effectiveMinStack,
    teamStackCount: effectiveTeamStackCount,
    bringBackEnabled: effectiveBringBackSize > 0,
    bringBackSize: effectiveBringBackSize,
    maxExposure,
    ceilingBoost,
    ceilingCount,
    minChanges: effectiveMinChanges,
    salaryFloor: effectiveSalaryFloor,
  };
  debug.heuristic = {
    prunedCandidateCount: prunedPool.length,
    templateCount: 0,
    templatesTried: 0,
    repairAttempts: 0,
    rejectedByReason: {},
  };
  debug.totalMs = Date.now() - totalStart;

  return {
    prepared: {
      sport: "nba",
      mode,
      requestedLineups: nLineups,
      maxExposureCount: debug.maxExposureCount,
      eligibleCount: eligible.length,
      pool: prunedPool,
      ruleSelections,
      effectiveSettings: {
        minStack: effectiveMinStack,
        teamStackCount: effectiveTeamStackCount,
        bringBackEnabled: effectiveBringBackSize > 0,
        bringBackSize: effectiveBringBackSize,
        ceilingBoost,
        ceilingCount,
        maxExposure,
        minChanges: effectiveMinChanges,
        salaryFloor: effectiveSalaryFloor,
      },
      ceilingBonusRecord: Object.fromEntries(ceilingBonusMap),
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
  const ceilingBonusMap = new Map<number, number>(
    Object.entries(prepared.ceilingBonusRecord).map(([playerId, bonus]) => [Number(playerId), bonus]),
  );

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
      const result = solveOneLineupDetailed(
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
        ceilingBonusMap,
      );
      attempts.push({
        stage,
        success: result.lineup != null,
        durationMs: Date.now() - start,
        prunedCandidateCount: result.meta.prunedCandidateCount,
        templateCount: result.meta.templateCount,
        templatesTried: result.meta.templatesTried,
        repairAttempts: result.meta.repairAttempts,
        rejectedByReason: result.meta.rejectedByReason,
        failureReason: result.meta.failureReason,
      });
      return result.lineup;
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
  staticScoreAdjustments: Map<number, number> = new Map<number, number>(),
): GeneratedLineup | null {
  return solveOneLineupDetailed(
    pool,
    mode,
    minStack,
    teamStackCount,
    maxExposureCount,
    exposureCount,
    previousLineupSets,
    bringBackSize,
    minChanges,
    salaryFloor,
    ruleSelections,
    staticScoreAdjustments,
  ).lineup;
}

/**
 * Build DK NBA multi-entry upload CSV.
 * Header: Entry ID,Contest Name,Contest ID,Entry Fee,PG,SG,SF,PF,C,G,F,UTIL
 * Player cell format: "Name (dkPlayerId)"
 */
export function buildMultiEntryCSV(
  lineups: GeneratedLineup[],
): string {
  if (lineups.length === 0) return "";

  const rows = [stringifyCsvLine([
    "Lineup",
    ...LINEUP_SLOTS,
    "Salary",
    "Projection",
    "Leverage",
  ])];

  for (let i = 0; i < lineups.length; i++) {
    const lineup = lineups[i];
    rows.push(stringifyCsvLine([
      String(i + 1),
      ...LINEUP_SLOTS.map((slot) => {
        const player = lineup.slots[slot];
        return player ? `${player.name} (${player.dkPlayerId})` : "";
      }),
      String(lineup.totalSalary),
      lineup.projFpts.toFixed(2),
      lineup.leverageScore.toFixed(2),
    ]));
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
