import "server-only";

import type { DkPlayerRow } from "@/db/queries";
import { stringifyCsvLine } from "./csv";
import { applyMlbPendingLineupPolicy, normalizeMlbPendingLineupPolicy, type MlbPendingLineupPolicy } from "./mlb-lineup";
import {
  MLB_MAX_HITTERS_PER_TEAM,
  normalizeMlbRuleSelections,
  validateMlbRuleSelections,
  type MlbRuleSettings,
  type NormalizedMlbRuleSelections,
  type MlbTeamStackRule,
} from "./mlb-optimizer-rules";
import type { OptimizerDebugInfo, OptimizerLineupAttemptDebug } from "./optimizer-debug";
import type { MlbPreparedOptimizerRun } from "./optimizer-job-types";

export type MlbOptimizerPlayer = Pick<
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
  | "dkInStartingLineup"
  | "dkStartingLineupOrder"
  | "dkTeamLineupConfirmed"
  | "isOut"
  | "gameInfo"
  | "teamLogo"
  | "teamName"
  | "homeTeamId"
  | "awayTeamId"
  | "vegasTotal"
  | "homeImplied"
  | "awayImplied"
  | "hrProb1Plus"
  | "propPts"
  | "propReb"
  | "propAst"
>;

export type MlbLineupSlot = "P1" | "P2" | "C" | "1B" | "2B" | "3B" | "SS" | "OF1" | "OF2" | "OF3";

export type MlbGeneratedLineup = {
  players: MlbOptimizerPlayer[];
  slots: Record<MlbLineupSlot, MlbOptimizerPlayer>;
  totalSalary: number;
  projFpts: number;
  leverageScore: number;
  templateId?: string;
};

export type MlbOptimizerSettings = MlbRuleSettings & {
  mode: "cash" | "gpp";
  nLineups: number;
  minStack: number;
  maxExposure: number;
  bringBackThreshold: number;
  antiCorrMax: number;
  pendingLineupPolicy: MlbPendingLineupPolicy;
  hrCorrelation: boolean;
  hrCorrelationThreshold: number;
  pitcherCeilingBoost: boolean;
  pitcherCeilingCount: number;
};

const MLB_SALARY_CAP = 50000;
const MLB_ROSTER_SIZE = 10;
const MLB_DIV_WINDOW = 5;
const MLB_PITCHER_PAIR_LIMIT = 24;
const MLB_TEMPLATE_LIMIT = 16;
const MLB_BATTER_BEAM_WIDTH = 120;
const MLB_BATTER_BRANCH_LIMIT = 12;
const MLB_GLOBAL_BATTER_KEEP_COUNT = 48;
const MLB_TEAM_BATTER_KEEP_COUNT = 10;
const MLB_CHEAP_BATTER_KEEP_COUNT = 10;
const MLB_GPP_LEVERAGE_WEIGHT = 0.6;
const MLB_CASH_LEVERAGE_WEIGHT = 0.1;
const MLB_GPP_HITTER_CEILING_EDGE_WEIGHT = 0.16;
const MLB_GPP_PITCHER_CEILING_EDGE_WEIGHT = 0.18;
const MLB_GPP_HITTER_BOOM_WEIGHT = 5.5;
const MLB_GPP_PITCHER_BOOM_WEIGHT = 4.25;
const MLB_GPP_HR_WEIGHT = 2.75;
const MLB_GPP_STRIKEOUT_PROP_WEIGHT = 0.16;
const MLB_GPP_CHALK_PENALTY_START = 10.0;
const MLB_GPP_CHALK_PENALTY_WEIGHT = 0.045;
const MLB_GPP_LINEUP_OWNERSHIP_TARGET = 115.0;
const MLB_GPP_LINEUP_OWNERSHIP_WEIGHT = 0.055;
const MLB_GPP_HIGH_OWNED_HITTER_THRESHOLD = 18.0;
const MLB_GPP_HIGH_OWNED_PITCHER_THRESHOLD = 24.0;
const MLB_GPP_HIGH_OWNED_PLAYER_ALLOWANCE = 3;
const MLB_GPP_HIGH_OWNED_PLAYER_PENALTY = 0.85;
const MLB_GPP_STACK_OWNERSHIP_THRESHOLD = 42.0;
const MLB_GPP_STACK_OWNERSHIP_WEIGHT = 0.05;
const MLB_LEAGUE_AVG_TEAM_TOTAL = 4.5;
const MLB_TEMPLATE_USAGE_PENALTY = 1.25;
const MLB_TEMPLATE_ROTATION_PENALTY = 0.6;
const MLB_PITCHER_CEILING_BONUSES = [2.5, 1.75, 1.0, 0.5, 0.25] as const;
const MLB_BATTER_SLOTS: readonly MlbLineupSlot[] = ["C", "1B", "2B", "3B", "SS", "OF1", "OF2", "OF3"];
const MLB_ALL_SLOTS: readonly MlbLineupSlot[] = ["P1", "P2", ...MLB_BATTER_SLOTS];

type NextMlbLineupResult = {
  lineup: MlbGeneratedLineup | null;
  summary: OptimizerDebugInfo["lineupSummaries"][number];
};

type MlbStackTemplate = {
  id: string;
  minCountsByTeam: Map<number, number>;
  score: number;
  baseRank: number;
};

type MlbValidationResult =
  | { ok: true; slots: Record<MlbLineupSlot, MlbOptimizerPlayer> }
  | { ok: false };

type BatterSearchState = {
  slotAssignment: Partial<Record<MlbLineupSlot, MlbOptimizerPlayer>>;
  selectedIds: Set<number>;
  teamCounts: Map<number, number>;
  hitterTeamOwnership: Map<number, number>;
  salary: number;
  score: number;
  projection: number;
  leverage: number;
  ownership: number;
  highOwnedCount: number;
};

function finiteOrNull(value: number | null | undefined): number | null {
  return value != null && Number.isFinite(value) ? value : null;
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

export function isPitcher(pos: string): boolean {
  return pos.includes("SP") || pos.includes("RP");
}

function filterEligibleMlbPool(
  pool: MlbOptimizerPlayer[],
  ruleSelections: NormalizedMlbRuleSelections,
): MlbOptimizerPlayer[] {
  const blockedPlayers = new Set(ruleSelections.playerBlocks);
  const blockedTeams = new Set(ruleSelections.blockedTeamIds);

  return pool.filter((player) => {
    if (player.isOut) return false;
    if (blockedPlayers.has(player.id)) return false;
    if (player.teamId != null && blockedTeams.has(player.teamId)) return false;
    return getMlbProjection(player) > 0 && player.salary > 0;
  });
}

function getMlbProjection(player: MlbOptimizerPlayer): number {
  return finiteOrNull(player.ourProj) ?? 0;
}

function getMlbLeverage(player: MlbOptimizerPlayer): number {
  return finiteOrNull(player.ourLeverage) ?? 0;
}

function getMlbProjectedOwnership(player: MlbOptimizerPlayer): number {
  return Math.max(0, finiteOrNull(player.projOwnPct) ?? 0);
}

function isHighOwnedMlbPlayer(player: MlbOptimizerPlayer): boolean {
  const threshold = isPitcher(player.eligiblePositions)
    ? MLB_GPP_HIGH_OWNED_PITCHER_THRESHOLD
    : MLB_GPP_HIGH_OWNED_HITTER_THRESHOLD;
  return getMlbProjectedOwnership(player) >= threshold;
}

function getMaxMlbTeamOwnership(teamOwnership: Map<number, number>): number {
  let maxOwnership = 0;
  for (const ownership of teamOwnership.values()) {
    if (ownership > maxOwnership) maxOwnership = ownership;
  }
  return maxOwnership;
}

function getMlbLineupDuplicationPenalty(
  totalOwnership: number,
  highOwnedCount: number,
  hitterTeamOwnership: Map<number, number>,
  mode: "cash" | "gpp",
): number {
  if (mode !== "gpp") return 0;

  const totalOwnershipPenalty = Math.max(0, totalOwnership - MLB_GPP_LINEUP_OWNERSHIP_TARGET) * MLB_GPP_LINEUP_OWNERSHIP_WEIGHT;
  const highOwnedPenalty = Math.max(0, highOwnedCount - MLB_GPP_HIGH_OWNED_PLAYER_ALLOWANCE) * MLB_GPP_HIGH_OWNED_PLAYER_PENALTY;
  const stackOwnershipPenalty = Math.max(0, getMaxMlbTeamOwnership(hitterTeamOwnership) - MLB_GPP_STACK_OWNERSHIP_THRESHOLD) * MLB_GPP_STACK_OWNERSHIP_WEIGHT;

  return totalOwnershipPenalty + highOwnedPenalty + stackOwnershipPenalty;
}

function getMlbGppBonus(player: MlbOptimizerPlayer): number {
  const projection = getMlbProjection(player);
  if (projection <= 0) return 0;

  const projectedOwnership = Math.max(0, finiteOrNull(player.projOwnPct) ?? 0);
  const chalkPenalty = Math.max(0, projectedOwnership - MLB_GPP_CHALK_PENALTY_START) * MLB_GPP_CHALK_PENALTY_WEIGHT;
  const boomRate = Math.max(0, finiteOrNull(player.boomRate) ?? 0);

  if (isPitcher(player.eligiblePositions)) {
    const ceiling = finiteOrNull(player.projCeiling) ?? (projection * 1.55);
    const ceilingEdge = Math.max(0, ceiling - projection);
    const strikeoutProp = Math.max(0, (finiteOrNull(player.propPts) ?? 0) - 5);
    return (ceilingEdge * MLB_GPP_PITCHER_CEILING_EDGE_WEIGHT)
      + (boomRate * MLB_GPP_PITCHER_BOOM_WEIGHT)
      + (strikeoutProp * MLB_GPP_STRIKEOUT_PROP_WEIGHT)
      - chalkPenalty;
  }

  const ceiling = finiteOrNull(player.projCeiling) ?? (projection * 1.7);
  const ceilingEdge = Math.max(0, ceiling - projection);
  const hrProb = Math.max(0, finiteOrNull(player.hrProb1Plus) ?? 0);
  const impliedRuns = getMlbTeamImpliedRuns(player);
  const runEnvironmentBoost = impliedRuns != null
    ? Math.max(0, impliedRuns - MLB_LEAGUE_AVG_TEAM_TOTAL) * 0.22
    : 0;

  return (ceilingEdge * MLB_GPP_HITTER_CEILING_EDGE_WEIGHT)
    + (boomRate * MLB_GPP_HITTER_BOOM_WEIGHT)
    + (hrProb * MLB_GPP_HR_WEIGHT)
    + runEnvironmentBoost
    - chalkPenalty;
}

function getMlbSearchScore(player: MlbOptimizerPlayer, mode: "cash" | "gpp"): number {
  const projection = getMlbProjection(player);
  const leverage = getMlbLeverage(player);
  return projection
    + leverage * (mode === "gpp" ? MLB_GPP_LEVERAGE_WEIGHT : MLB_CASH_LEVERAGE_WEIGHT)
    + (mode === "gpp" ? getMlbGppBonus(player) : 0);
}

function getMlbTeamImpliedRuns(player: MlbOptimizerPlayer): number | null {
  if (player.teamId != null) {
    if (player.homeTeamId != null && player.teamId === player.homeTeamId) {
      return finiteOrNull(player.homeImplied)
        ?? (finiteOrNull(player.vegasTotal) != null ? player.vegasTotal! / 2 : null);
    }
    if (player.awayTeamId != null && player.teamId === player.awayTeamId) {
      return finiteOrNull(player.awayImplied)
        ?? (finiteOrNull(player.vegasTotal) != null ? player.vegasTotal! / 2 : null);
    }
  }
  const vegasTotal = finiteOrNull(player.vegasTotal);
  return vegasTotal != null ? vegasTotal / 2 : null;
}

function getMlbStackEnvironmentFactor(player: MlbOptimizerPlayer | null | undefined): number {
  if (!player) return 1;
  const impliedRuns = getMlbTeamImpliedRuns(player);
  if (impliedRuns == null || impliedRuns <= 0) return 1;
  const ratio = Math.max(0.7, Math.min(1.4, impliedRuns / MLB_LEAGUE_AVG_TEAM_TOTAL));
  return Math.sqrt(ratio);
}

// HR correlation bonus: for each batter above hrThreshold with a confirmed batting order,
// bonus the batter immediately preceding them (+5) and two spots back (+2).
// Rationale: if batter #3 hits a HR, batter #2 (on base) likely scores — high RBI correlation.
export function computeHrBonusMap(
  batters: MlbOptimizerPlayer[],
  threshold: number,
): Map<number, number> {
  const bonuses = new Map<number, number>();
  const byTeamOrder = new Map<string, MlbOptimizerPlayer>();
  for (const batter of batters) {
    if (batter.dkStartingLineupOrder != null && batter.teamId != null) {
      byTeamOrder.set(`${batter.teamId}:${batter.dkStartingLineupOrder}`, batter);
    }
  }
  for (const batter of batters) {
    const hrProb = batter.hrProb1Plus ?? 0;
    if (hrProb < threshold) continue;
    const order = batter.dkStartingLineupOrder;
    const teamId = batter.teamId;
    if (order == null || teamId == null) continue;
    const prevOrder = order === 1 ? 9 : order - 1;
    const prev2Order = prevOrder === 1 ? 9 : prevOrder - 1;
    const preceding1 = byTeamOrder.get(`${teamId}:${prevOrder}`);
    const preceding2 = byTeamOrder.get(`${teamId}:${prev2Order}`);
    if (preceding1) bonuses.set(preceding1.id, (bonuses.get(preceding1.id) ?? 0) + 5.0);
    if (preceding2) bonuses.set(preceding2.id, (bonuses.get(preceding2.id) ?? 0) + 2.0);
  }
  return bonuses;
}

function getPitcherOpponentTeamTotal(player: MlbOptimizerPlayer): number | null {
  const teamId = finiteOrNull(player.teamId);
  const homeTeamId = finiteOrNull(player.homeTeamId);
  const awayTeamId = finiteOrNull(player.awayTeamId);
  if (teamId == null || homeTeamId == null || awayTeamId == null) return null;
  if (teamId === homeTeamId) {
    return finiteOrNull(player.awayImplied)
      ?? (finiteOrNull(player.vegasTotal) != null ? player.vegasTotal! / 2 : null);
  }
  if (teamId === awayTeamId) {
    return finiteOrNull(player.homeImplied)
      ?? (finiteOrNull(player.vegasTotal) != null ? player.vegasTotal! / 2 : null);
  }
  return finiteOrNull(player.vegasTotal) != null ? player.vegasTotal! / 2 : null;
}

export function computePitcherCeilingBonusMap(
  pitchers: MlbOptimizerPlayer[],
  topCount: number,
): Map<number, number> {
  const activePitchers = pitchers.filter((player) => !player.isOut);
  const ceilingCount = Math.max(0, Math.min(topCount, activePitchers.length));
  if (ceilingCount === 0) return new Map<number, number>();

  const contexts = activePitchers.map((player) => {
    const projection = getMlbProjection(player) || finiteOrNull(player.linestarProj);
    const value = projection != null ? projection / Math.max(1, player.salary / 1000) : null;
    return {
      player,
      projection,
      value,
      strikeouts: finiteOrNull(player.propPts),
      outs: finiteOrNull(player.propReb),
      earnedRuns: finiteOrNull(player.propAst),
      oppTeamTotal: getPitcherOpponentTeamTotal(player),
      score: 0,
    };
  });

  const strikeoutValues = contexts.map((context) => context.strikeouts);
  const outsValues = contexts.map((context) => context.outs);
  const erValues = contexts.map((context) => context.earnedRuns);
  const oppTotalValues = contexts.map((context) => context.oppTeamTotal);
  const projectionValues = contexts.map((context) => context.projection);
  const valueValues = contexts.map((context) => context.value);

  for (const context of contexts) {
    context.score =
      rankMetric(context.strikeouts, strikeoutValues, true) * 0.34 +
      rankMetric(context.outs, outsValues, true) * 0.22 +
      rankMetric(context.earnedRuns, erValues, false) * 0.10 +
      rankMetric(context.oppTeamTotal, oppTotalValues, false) * 0.14 +
      rankMetric(context.projection, projectionValues, true) * 0.12 +
      rankMetric(context.value, valueValues, true) * 0.08;
  }

  const sorted = [...contexts].sort((a, b) => {
    const diff = b.score - a.score;
    return Math.abs(diff) > 1e-9 ? diff : a.player.id - b.player.id;
  });

  const bonuses = new Map<number, number>();
  for (let index = 0; index < ceilingCount; index++) {
    const context = sorted[index];
    if (!context) break;
    bonuses.set(context.player.id, MLB_PITCHER_CEILING_BONUSES[index] ?? MLB_PITCHER_CEILING_BONUSES[MLB_PITCHER_CEILING_BONUSES.length - 1]);
  }
  return bonuses;
}

function orderMlbStackTemplates(
  templates: readonly MlbStackTemplate[],
  templateUsageCount: Map<string, number>,
  lineupIteration: number,
): MlbStackTemplate[] {
  const rotationWindow = Math.min(4, templates.length);
  const rotationOffset = rotationWindow > 1 ? lineupIteration % rotationWindow : 0;

  return [...templates].sort((a, b) => {
    const usagePenaltyA = (templateUsageCount.get(a.id) ?? 0) * MLB_TEMPLATE_USAGE_PENALTY;
    const usagePenaltyB = (templateUsageCount.get(b.id) ?? 0) * MLB_TEMPLATE_USAGE_PENALTY;
    const rotationPenaltyA = a.baseRank < rotationWindow
      ? ((a.baseRank - rotationOffset + rotationWindow) % rotationWindow) * MLB_TEMPLATE_ROTATION_PENALTY
      : 0;
    const rotationPenaltyB = b.baseRank < rotationWindow
      ? ((b.baseRank - rotationOffset + rotationWindow) % rotationWindow) * MLB_TEMPLATE_ROTATION_PENALTY
      : 0;
    const adjustedA = a.score - usagePenaltyA - rotationPenaltyA;
    const adjustedB = b.score - usagePenaltyB - rotationPenaltyB;
    const diff = adjustedB - adjustedA;
    return Math.abs(diff) > 1e-9 ? diff : a.baseRank - b.baseRank || a.id.localeCompare(b.id);
  });
}

function compareMlbPlayers(a: MlbOptimizerPlayer, b: MlbOptimizerPlayer, mode: "cash" | "gpp"): number {
  const scoreDiff = getMlbSearchScore(b, mode) - getMlbSearchScore(a, mode);
  if (Math.abs(scoreDiff) > 1e-9) return scoreDiff;
  const projDiff = getMlbProjection(b) - getMlbProjection(a);
  if (Math.abs(projDiff) > 1e-9) return projDiff;
  const levDiff = getMlbLeverage(b) - getMlbLeverage(a);
  if (Math.abs(levDiff) > 1e-9) return levDiff;
  const salaryDiff = a.salary - b.salary;
  if (salaryDiff !== 0) return salaryDiff;
  return a.id - b.id;
}

function sortMlbPlayersForSearch(players: MlbOptimizerPlayer[], mode: "cash" | "gpp"): MlbOptimizerPlayer[] {
  return [...players].sort((a, b) => compareMlbPlayers(a, b, mode));
}

function canFillMlbSlot(slot: MlbLineupSlot, pos: string): boolean {
  switch (slot) {
    case "P1":
    case "P2":
      return isPitcher(pos);
    case "C":
      return pos.includes("C") && !isPitcher(pos);
    case "1B":
      return pos.includes("1B");
    case "2B":
      return pos.includes("2B");
    case "3B":
      return pos.includes("3B");
    case "SS":
      return pos.includes("SS");
    case "OF1":
    case "OF2":
    case "OF3":
      return pos.includes("OF");
  }
}

function buildMlbOpponentByTeamId(pool: MlbOptimizerPlayer[]): Map<number, number> {
  const teamsByMatchup = new Map<number, Set<number>>();
  for (const player of pool) {
    if (player.matchupId == null || player.teamId == null) continue;
    const teams = teamsByMatchup.get(player.matchupId) ?? new Set<number>();
    teams.add(player.teamId);
    teamsByMatchup.set(player.matchupId, teams);
  }

  const opponentByTeamId = new Map<number, number>();
  for (const teams of teamsByMatchup.values()) {
    const values = Array.from(teams);
    if (values.length !== 2) continue;
    opponentByTeamId.set(values[0], values[1]);
    opponentByTeamId.set(values[1], values[0]);
  }
  return opponentByTeamId;
}

function enumerateCombinations<T>(items: T[], size: number): T[][] {
  if (size <= 0) return [[]];
  if (items.length < size) return [];
  const combinations: T[][] = [];
  const current: T[] = [];
  const visit = (start: number) => {
    if (current.length === size) {
      combinations.push([...current]);
      return;
    }
    for (let i = start; i <= items.length - (size - current.length); i++) {
      current.push(items[i]);
      visit(i + 1);
      current.pop();
    }
  };
  visit(0);
  return combinations;
}

// additional helpers and optimizer implementation follow below
function addTopMlbPlayers(keepIds: Set<number>, players: MlbOptimizerPlayer[], count: number) {
  for (const player of players.slice(0, count)) {
    keepIds.add(player.id);
  }
}

function addCheapestMlbPlayers(keepIds: Set<number>, players: MlbOptimizerPlayer[], count: number) {
  const cheapest = [...players]
    .sort((a, b) => a.salary - b.salary || compareMlbPlayers(a, b, "cash"))
    .slice(0, count);
  for (const player of cheapest) {
    keepIds.add(player.id);
  }
}

function pruneMlbBatterPool(
  batters: MlbOptimizerPlayer[],
  mode: "cash" | "gpp",
  minStack: number,
  bringBackThreshold: number,
  lockedIds: Set<number>,
  requiredTeamStacks: MlbTeamStackRule[],
): MlbOptimizerPlayer[] {
  if (batters.length <= 70) {
    return sortMlbPlayersForSearch(batters, mode);
  }

  const keepIds = new Set<number>(lockedIds);
  const sorted = sortMlbPlayersForSearch(batters, mode);
  addTopMlbPlayers(keepIds, sorted, MLB_GLOBAL_BATTER_KEEP_COUNT);
  addCheapestMlbPlayers(keepIds, batters, MLB_CHEAP_BATTER_KEEP_COUNT);

  for (const slot of MLB_BATTER_SLOTS) {
    addTopMlbPlayers(
      keepIds,
      sorted.filter((player) => canFillMlbSlot(slot, player.eligiblePositions)),
      14,
    );
  }

  const byTeam = new Map<number, MlbOptimizerPlayer[]>();
  for (const batter of batters) {
    if (batter.teamId == null) continue;
    const teamPlayers = byTeam.get(batter.teamId) ?? [];
    teamPlayers.push(batter);
    byTeam.set(batter.teamId, teamPlayers);
  }

  for (const teamPlayers of byTeam.values()) {
    const sortedTeam = sortMlbPlayersForSearch(teamPlayers, mode);
    addTopMlbPlayers(
      keepIds,
      sortedTeam,
      Math.max(MLB_TEAM_BATTER_KEEP_COUNT, minStack + Math.max(0, bringBackThreshold - 1) + 3),
    );
  }

  for (const rule of requiredTeamStacks) {
    const teamPlayers = byTeam.get(rule.teamId);
    if (!teamPlayers) continue;
    addTopMlbPlayers(
      keepIds,
      sortMlbPlayersForSearch(teamPlayers, mode),
      Math.max(MLB_TEAM_BATTER_KEEP_COUNT, rule.stackSize + Math.max(0, bringBackThreshold - 1) + 3),
    );
  }

  return sortMlbPlayersForSearch(
    batters.filter((player) => keepIds.has(player.id)),
    mode,
  );
}

function enumeratePitcherPairs(
  pitchers: MlbOptimizerPlayer[],
  mode: "cash" | "gpp",
  pitcherBonusById: Map<number, number> = new Map(),
): Array<{ players: [MlbOptimizerPlayer, MlbOptimizerPlayer]; score: number }> {
  const sorted = sortMlbPlayersForSearch(pitchers, mode).slice(0, Math.max(8, Math.min(18, pitchers.length)));
  return enumerateCombinations(sorted, 2)
    .map(([a, b]) => {
      const players = [a, b] as [MlbOptimizerPlayer, MlbOptimizerPlayer];
      const hitterTeamOwnership = new Map<number, number>();
      return {
        players,
        score:
          getMlbSearchScore(a, mode)
          + getMlbSearchScore(b, mode)
          + (pitcherBonusById.get(a.id) ?? 0)
          + (pitcherBonusById.get(b.id) ?? 0)
          - getMlbLineupDuplicationPenalty(
            players.reduce((sum, player) => sum + getMlbProjectedOwnership(player), 0),
            players.filter((player) => isHighOwnedMlbPlayer(player)).length,
            hitterTeamOwnership,
            mode,
          ),
      };
    })
    .sort((a, b) => b.score - a.score || a.players[0].id - b.players[0].id || a.players[1].id - b.players[1].id)
    .slice(0, MLB_PITCHER_PAIR_LIMIT);
}

function enumerateMlbStackTemplates(
  batters: MlbOptimizerPlayer[],
  mode: "cash" | "gpp",
  minStack: number,
  bringBackThreshold: number,
  requiredTeamStacks: MlbTeamStackRule[],
): MlbStackTemplate[] {
  const effectiveRules = requiredTeamStacks.filter((rule) => rule.teamId != null);
  if (minStack <= 0) {
    if (effectiveRules.length > 0) {
      return [];
    }
    return [{ id: "no-stack", minCountsByTeam: new Map(), score: 0, baseRank: 0 }];
  }

  const byTeam = new Map<number, MlbOptimizerPlayer[]>();
  for (const batter of batters) {
    if (batter.teamId == null) continue;
    const teamPlayers = byTeam.get(batter.teamId) ?? [];
    teamPlayers.push(batter);
    byTeam.set(batter.teamId, teamPlayers);
  }

  const opponentByTeamId = buildMlbOpponentByTeamId(batters);
  const templates: MlbStackTemplate[] = [];
  const templateSeeds = effectiveRules.length > 0
    ? effectiveRules.map((rule) => ({ teamId: rule.teamId, stackSize: Math.max(minStack, rule.stackSize) }))
    : Array.from(byTeam.keys()).map((teamId) => ({ teamId, stackSize: minStack }));

  for (const { teamId, stackSize } of templateSeeds) {
    const teamPlayers = byTeam.get(teamId) ?? [];
    const sorted = sortMlbPlayersForSearch(teamPlayers, mode);
    if (sorted.length < stackSize) continue;
    const minCountsByTeam = new Map<number, number>([[teamId, stackSize]]);
    if (bringBackThreshold > 0 && stackSize >= bringBackThreshold) {
      const opponentTeamId = opponentByTeamId.get(teamId);
      if (opponentTeamId == null) continue;
      minCountsByTeam.set(opponentTeamId, 1);
    }
    const score = sorted
      .slice(0, stackSize)
      .reduce((sum, player) => sum + getMlbSearchScore(player, mode) + (getMlbProjection(player) * 0.01), 0);
    const selectedStackPlayers = sorted.slice(0, stackSize);
    const hitterTeamOwnership = new Map<number, number>();
    hitterTeamOwnership.set(
      teamId,
      selectedStackPlayers.reduce((sum, player) => sum + getMlbProjectedOwnership(player), 0),
    );
    templates.push({
      id: `stack-${teamId}-${stackSize}`,
      minCountsByTeam,
      score:
        (score * getMlbStackEnvironmentFactor(sorted[0] ?? null))
        - getMlbLineupDuplicationPenalty(
          selectedStackPlayers.reduce((sum, player) => sum + getMlbProjectedOwnership(player), 0),
          selectedStackPlayers.filter((player) => isHighOwnedMlbPlayer(player)).length,
          hitterTeamOwnership,
          mode,
        ),
      baseRank: 0,
    });
  }

  return templates
    .sort((a, b) => b.score - a.score || a.id.localeCompare(b.id))
    .slice(0, MLB_TEMPLATE_LIMIT)
    .map((template, index) => ({
      ...template,
      baseRank: index,
    }));
}

function calculateMlbSharedCount(players: MlbOptimizerPlayer[], previousLineup: Set<number>): number {
  let shared = 0;
  for (const player of players) {
    if (previousLineup.has(player.id)) shared++;
  }
  return shared;
}

function assignMlbPlayersToSlots(
  players: MlbOptimizerPlayer[],
  slots: readonly MlbLineupSlot[],
): Partial<Record<MlbLineupSlot, MlbOptimizerPlayer>> | null {
  if (players.length > slots.length) return null;
  const slotOptions = players
    .map((player) => ({
      player,
      slots: slots.filter((slot) => canFillMlbSlot(slot, player.eligiblePositions)),
    }))
    .sort((a, b) => a.slots.length - b.slots.length || a.player.id - b.player.id);

  if (slotOptions.some((entry) => entry.slots.length === 0)) return null;

  const assignment: Partial<Record<MlbLineupSlot, MlbOptimizerPlayer>> = {};
  const usedSlots = new Set<MlbLineupSlot>();

  const visit = (index: number): boolean => {
    if (index >= slotOptions.length) return true;
    const { player, slots: candidateSlots } = slotOptions[index];
    for (const slot of candidateSlots) {
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

function assignMlbPositions(
  players: MlbOptimizerPlayer[],
): Record<MlbLineupSlot, MlbOptimizerPlayer> | null {
  const assignment = assignMlbPlayersToSlots(players, MLB_ALL_SLOTS);
  return assignment ? assignment as Record<MlbLineupSlot, MlbOptimizerPlayer> : null;
}

function validateMlbLineupExact(
  players: MlbOptimizerPlayer[],
  minStack: number,
  previousLineupSets: Set<number>[],
  bringBackThreshold: number,
  antiCorrMax: number,
  minChanges: number,
): MlbValidationResult {
  if (players.length !== MLB_ROSTER_SIZE) return { ok: false };
  if (new Set(players.map((player) => player.id)).size !== MLB_ROSTER_SIZE) return { ok: false };
  if (players.reduce((sum, player) => sum + player.salary, 0) > MLB_SALARY_CAP) return { ok: false };

  const slots = assignMlbPositions(players);
  if (!slots) return { ok: false };

  const batterTeamCounts = new Map<number, number>();
  for (const player of players) {
    if (isPitcher(player.eligiblePositions) || player.teamId == null) continue;
    batterTeamCounts.set(player.teamId, (batterTeamCounts.get(player.teamId) ?? 0) + 1);
  }
  if (Array.from(batterTeamCounts.values()).some((count) => count > MLB_MAX_HITTERS_PER_TEAM)) {
    return { ok: false };
  }
  if (minStack > 0 && !Array.from(batterTeamCounts.values()).some((count) => count >= minStack)) {
    return { ok: false };
  }

  const opponentByTeamId = buildMlbOpponentByTeamId(players);
  if (bringBackThreshold >= 2) {
    for (const [teamId, count] of batterTeamCounts) {
      if (count < bringBackThreshold) continue;
      const opponentTeamId = opponentByTeamId.get(teamId);
      if (opponentTeamId == null) return { ok: false };
      if ((batterTeamCounts.get(opponentTeamId) ?? 0) < 1) return { ok: false };
    }
  }

  if (antiCorrMax < MLB_ROSTER_SIZE) {
    for (const player of players) {
      if (!isPitcher(player.eligiblePositions) || player.teamId == null) continue;
      const opponentTeamId = opponentByTeamId.get(player.teamId);
      if (opponentTeamId == null) continue;
      if ((batterTeamCounts.get(opponentTeamId) ?? 0) > antiCorrMax) return { ok: false };
    }
  }

  const maxShared = MLB_ROSTER_SIZE - minChanges;
  for (const previousLineup of previousLineupSets.slice(-MLB_DIV_WINDOW)) {
    if (calculateMlbSharedCount(players, previousLineup) > maxShared) return { ok: false };
  }

  return { ok: true, slots };
}

function solveMlbLineup(
  pool: MlbOptimizerPlayer[],
  mode: "cash" | "gpp",
  minStack: number,
  maxExposureCount: number,
  exposureCount: Map<number, number>,
  previousLineupSets: Set<number>[],
  bringBackThreshold = 4,
  antiCorrMax = 1,
  minChanges = 3,
  ruleSelections: NormalizedMlbRuleSelections = normalizeMlbRuleSelections({}),
  templateUsageCount: Map<string, number> = new Map(),
  baseTemplates: MlbStackTemplate[] | null = null,
  lineupIndex = 0,
  hrBonusById: Map<number, number> = new Map(),
  pitcherCeilingBonusById: Map<number, number> = new Map(),
): MlbGeneratedLineup | null {
  const eligible = pool
    .filter((player) => (exposureCount.get(player.id) ?? 0) < maxExposureCount)
    .filter((player) => !player.isOut && getMlbProjection(player) > 0 && player.salary > 0);
  if (eligible.length < MLB_ROSTER_SIZE) return null;

  const lockedIds = new Set(ruleSelections.playerLocks);
  const lockedPlayers = eligible.filter((player) => lockedIds.has(player.id));
  if (lockedPlayers.length !== lockedIds.size) return null;

  const lockedPitchers = lockedPlayers.filter((player) => isPitcher(player.eligiblePositions));
  const lockedBatters = lockedPlayers.filter((player) => !isPitcher(player.eligiblePositions));
  if (lockedPitchers.length > 2 || lockedBatters.length > MLB_BATTER_SLOTS.length) return null;

  const lockedBatterAssignments = assignMlbPlayersToSlots(lockedBatters, MLB_BATTER_SLOTS);
  if (lockedBatters.length > 0 && !lockedBatterAssignments) return null;
  const lockedBatterSlotAssignments = lockedBatterAssignments ?? {};
  const lockedBatterIds = new Set<number>(lockedBatters.map((player) => player.id));
  const lockedBatterSalary = lockedBatters.reduce((sum, player) => sum + player.salary, 0);

  // Batter score includes HR correlation bonus for preceding batters.
  // Pitchers are scored with getMlbSearchScore directly (no batting-order bonus).
  const getScoreForBatter = (player: MlbOptimizerPlayer) =>
    getMlbSearchScore(player, mode) + (hrBonusById.get(player.id) ?? 0);
  const getScoreForPitcher = (player: MlbOptimizerPlayer) =>
    getMlbSearchScore(player, mode) + (pitcherCeilingBonusById.get(player.id) ?? 0);

  const lockedBatterScore = lockedBatters.reduce((sum, player) => sum + getScoreForBatter(player), 0);
  const lockedBatterProjection = lockedBatters.reduce((sum, player) => sum + getMlbProjection(player), 0);
  const lockedBatterLeverage = lockedBatters.reduce((sum, player) => sum + getMlbLeverage(player), 0);
  const lockedBatterTeamCounts = new Map<number, number>();
  const lockedBatterTeamOwnership = new Map<number, number>();
  for (const player of lockedBatters) {
    if (player.teamId == null) continue;
    lockedBatterTeamCounts.set(player.teamId, (lockedBatterTeamCounts.get(player.teamId) ?? 0) + 1);
    lockedBatterTeamOwnership.set(
      player.teamId,
      (lockedBatterTeamOwnership.get(player.teamId) ?? 0) + getMlbProjectedOwnership(player),
    );
  }

  const pitchers = sortMlbPlayersForSearch(
    eligible.filter((player) => isPitcher(player.eligiblePositions)),
    mode,
  );
  const batters = pruneMlbBatterPool(
    eligible.filter((player) => !isPitcher(player.eligiblePositions)),
    mode,
    minStack,
    bringBackThreshold,
    lockedBatterIds,
    ruleSelections.requiredTeamStacks,
  );
  if (pitchers.length < 2 || batters.length < MLB_BATTER_SLOTS.length) return null;

  // Bring-back quality floor: bring-back candidates must project at or above
  // the 40th percentile of the pruned batter pool.  minCount === 1 in the
  // template exclusively identifies bring-back slots (primary stacks are 2–5).
  const bringBackMinProj = (() => {
    if (bringBackThreshold <= 0) return 0;
    const projs = batters.map(getMlbProjection).filter((p) => p > 0).sort((a, b) => a - b);
    return projs.length > 0 ? projs[Math.floor(projs.length * 0.4)] : 0;
  })();

  const opponentByTeamId = buildMlbOpponentByTeamId(eligible);
  const pitcherPairs = enumeratePitcherPairs(pitchers, mode, pitcherCeilingBonusById).filter(({ players }) =>
    lockedPitchers.every((lockedPitcher) => players.some((player) => player.id === lockedPitcher.id)),
  );
  const templates = orderMlbStackTemplates(
    baseTemplates ?? enumerateMlbStackTemplates(
      batters,
      mode,
      minStack,
      bringBackThreshold,
      ruleSelections.requiredTeamStacks,
    ),
    templateUsageCount,
    previousLineupSets.length,
  );
  if (pitcherPairs.length === 0 || templates.length === 0) return null;

  // Taper beam width for later lineups — early lineups need full width for quality;
  // later lineups are diversity-constrained so extra states rarely survive anyway.
  const beamWidth = lineupIndex < 3 ? MLB_BATTER_BEAM_WIDTH
    : lineupIndex < 8 ? Math.ceil(MLB_BATTER_BEAM_WIDTH * 0.67)
    : Math.ceil(MLB_BATTER_BEAM_WIDTH * 0.5);

  // minSalaryForRemaining is defined inside buildLineupForTemplate to use pre-sorted per-slot arrays.

  // Pre-group batters by team once — used by canMeetTeamMinimums instead of scanning all batters.
  const battersByTeam = new Map<number, MlbOptimizerPlayer[]>();
  for (const batter of batters) {
    if (batter.teamId == null) continue;
    const group = battersByTeam.get(batter.teamId) ?? [];
    group.push(batter);
    battersByTeam.set(batter.teamId, group);
  }

  function canMeetTeamMinimums(
    selectedIds: Set<number>,
    teamCounts: Map<number, number>,
    remainingSlots: readonly MlbLineupSlot[],
    minCountsByTeam: Map<number, number>,
  ): boolean {
    for (const [teamId, minCount] of minCountsByTeam) {
      const current = teamCounts.get(teamId) ?? 0;
      if (current >= minCount) continue;
      const needed = minCount - current;
      let available = 0;
      for (const player of battersByTeam.get(teamId) ?? []) {
        if (selectedIds.has(player.id)) continue;
        if (remainingSlots.some((slot) => canFillMlbSlot(slot, player.eligiblePositions))) {
          available++;
          if (available >= needed) break;
        }
      }
      if (available < needed) return false;
    }
    return true;
  }

  function buildLineupForTemplate(
    pitcherPair: [MlbOptimizerPlayer, MlbOptimizerPlayer],
    template: MlbStackTemplate,
  ): MlbGeneratedLineup | null {
    const pitcherIds = new Set<number>(pitcherPair.map((player) => player.id));
    const pitcherSalary = pitcherPair[0].salary + pitcherPair[1].salary;
    if (pitcherSalary >= MLB_SALARY_CAP) return null;
    if (lockedPitchers.some((player) => !pitcherIds.has(player.id))) return null;

    const pitcherPairOwnership = pitcherPair.reduce((sum, player) => sum + getMlbProjectedOwnership(player), 0);
    const pitcherPairHighOwnedCount = pitcherPair.filter((player) => isHighOwnedMlbPlayer(player)).length;

    const maxBattersByTeam = new Map<number, number>();
    for (const pitcher of pitcherPair) {
      if (pitcher.teamId == null) continue;
      const opponentTeamId = opponentByTeamId.get(pitcher.teamId);
      if (opponentTeamId == null) continue;
      maxBattersByTeam.set(
        opponentTeamId,
        Math.min(
          maxBattersByTeam.get(opponentTeamId) ?? MLB_MAX_HITTERS_PER_TEAM,
          antiCorrMax,
          MLB_MAX_HITTERS_PER_TEAM,
        ),
      );
    }
    for (const [teamId, minCount] of template.minCountsByTeam) {
      const maxAllowed = maxBattersByTeam.get(teamId);
      if (maxAllowed != null && minCount > maxAllowed) return null;
    }

    for (const [teamId, count] of lockedBatterTeamCounts) {
      if (count > (maxBattersByTeam.get(teamId) ?? MLB_MAX_HITTERS_PER_TEAM)) return null;
    }

    const remainingBatterSlots = MLB_BATTER_SLOTS.filter((slot) => !lockedBatterSlotAssignments[slot]);
    const slotCandidates = new Map<MlbLineupSlot, MlbOptimizerPlayer[]>();
    for (const slot of remainingBatterSlots) {
      const candidates = batters
        .filter((player) => !pitcherIds.has(player.id) && canFillMlbSlot(slot, player.eligiblePositions))
        .sort((a, b) => {
          const diff = getScoreForBatter(b) - getScoreForBatter(a);
          return Math.abs(diff) > 1e-9 ? diff : compareMlbPlayers(a, b, mode);
        });
      if (candidates.length === 0) return null;
      slotCandidates.set(slot, candidates);
    }

    const orderedSlots = [...remainingBatterSlots].sort((a, b) => {
      const diff = (slotCandidates.get(a)?.length ?? 0) - (slotCandidates.get(b)?.length ?? 0);
      return diff !== 0 ? diff : MLB_BATTER_SLOTS.indexOf(a) - MLB_BATTER_SLOTS.indexOf(b);
    });

    // Pre-sort each slot's candidates by salary once — used by minSalaryForRemaining
    // and the cheap-batter branch in branchCandidates, avoiding repeated inner-loop sorts.
    const slotCandidatesBySalary = new Map<MlbLineupSlot, MlbOptimizerPlayer[]>();
    for (const slot of remainingBatterSlots) {
      slotCandidatesBySalary.set(
        slot,
        (slotCandidates.get(slot) ?? [])
          .slice()
          .sort((a, b) => a.salary - b.salary || compareMlbPlayers(a, b, mode)),
      );
    }

    function minSalaryForRemaining(selectedIds: Set<number>, remainingSlots: readonly MlbLineupSlot[]): number {
      let total = 0;
      for (const slot of remainingSlots) {
        const cheapest = (slotCandidatesBySalary.get(slot) ?? []).find((p) => !selectedIds.has(p.id));
        if (!cheapest) return Infinity;
        total += cheapest.salary;
      }
      return total;
    }

    const branchCandidates = (slot: MlbLineupSlot, state: BatterSearchState): MlbOptimizerPlayer[] => {
      const base = (slotCandidates.get(slot) ?? []).filter((player) => !state.selectedIds.has(player.id));
      const chosen = new Set<number>();
      const result: MlbOptimizerPlayer[] = [];
      const push = (player: MlbOptimizerPlayer) => {
        if (chosen.has(player.id)) return;
        chosen.add(player.id);
        result.push(player);
      };

      for (const [teamId, minCount] of template.minCountsByTeam) {
        if ((state.teamCounts.get(teamId) ?? 0) >= minCount) continue;
        const isBringBack = minCount === 1;
        const teamCandidates = base.filter((candidate) =>
          candidate.teamId === teamId &&
          (!isBringBack || getMlbProjection(candidate) >= bringBackMinProj),
        );
        for (const player of teamCandidates.slice(0, 5)) {
          push(player);
        }
      }
      for (const player of base.slice(0, MLB_BATTER_BRANCH_LIMIT)) {
        push(player);
      }
      for (const player of (slotCandidatesBySalary.get(slot) ?? [])
        .filter((p) => !state.selectedIds.has(p.id))
        .slice(0, Math.max(4, Math.floor(MLB_BATTER_BRANCH_LIMIT / 3)))) {
        push(player);
      }
      return result;
    };

    let states: BatterSearchState[] = [{
      slotAssignment: { ...lockedBatterSlotAssignments },
      selectedIds: new Set<number>(lockedBatterIds),
      teamCounts: new Map<number, number>(lockedBatterTeamCounts),
      hitterTeamOwnership: new Map<number, number>(lockedBatterTeamOwnership),
      salary: pitcherSalary + lockedBatterSalary,
      score: pitcherPair.reduce((sum, player) => sum + getScoreForPitcher(player), 0) + lockedBatterScore,
      projection: pitcherPair.reduce((sum, player) => sum + getMlbProjection(player), 0) + lockedBatterProjection,
      leverage: pitcherPair.reduce((sum, player) => sum + getMlbLeverage(player), 0) + lockedBatterLeverage,
      ownership: pitcherPairOwnership + lockedBatters.reduce((sum, player) => sum + getMlbProjectedOwnership(player), 0),
      highOwnedCount: pitcherPairHighOwnedCount + lockedBatters.filter((player) => isHighOwnedMlbPlayer(player)).length,
    }];

    for (let depth = 0; depth < orderedSlots.length; depth++) {
      const slot = orderedSlots[depth];
      const remainingSlots = orderedSlots.slice(depth + 1);
      const nextStates: Array<BatterSearchState & { estimate: number }> = [];

      for (const state of states) {
        for (const player of branchCandidates(slot, state)) {
          const selectedIds = new Set(state.selectedIds);
          selectedIds.add(player.id);
          const teamCounts = new Map(state.teamCounts);
          const hitterTeamOwnership = new Map(state.hitterTeamOwnership);
          if (player.teamId != null) {
            const nextCount = (teamCounts.get(player.teamId) ?? 0) + 1;
            if (nextCount > (maxBattersByTeam.get(player.teamId) ?? MLB_MAX_HITTERS_PER_TEAM)) {
              continue;
            }
            teamCounts.set(player.teamId, nextCount);
            hitterTeamOwnership.set(
              player.teamId,
              (hitterTeamOwnership.get(player.teamId) ?? 0) + getMlbProjectedOwnership(player),
            );
          }

          const salary = state.salary + player.salary;
          if (salary > MLB_SALARY_CAP) continue;
          if (!canMeetTeamMinimums(selectedIds, teamCounts, remainingSlots, template.minCountsByTeam)) continue;

          const minRemainingSalary = minSalaryForRemaining(selectedIds, remainingSlots);
          if (!Number.isFinite(minRemainingSalary) || salary + minRemainingSalary > MLB_SALARY_CAP) continue;

          const nextState: BatterSearchState = {
            slotAssignment: {
              ...state.slotAssignment,
              [slot]: player,
            },
            selectedIds,
            teamCounts,
            hitterTeamOwnership,
            salary,
            score: state.score + getScoreForBatter(player),
            projection: state.projection + getMlbProjection(player),
            leverage: state.leverage + getMlbLeverage(player),
            ownership: state.ownership + getMlbProjectedOwnership(player),
            highOwnedCount: state.highOwnedCount + (isHighOwnedMlbPlayer(player) ? 1 : 0),
          };
          // batters is pre-sorted by score; iterate once instead of filter+sort per state.
          let estimateBonus = 0;
          let slotsNeeded = remainingSlots.length;
          for (const candidate of batters) {
            if (slotsNeeded <= 0) break;
            if (!selectedIds.has(candidate.id)) {
              estimateBonus += getScoreForBatter(candidate);
              slotsNeeded--;
            }
          }
          const estimate = nextState.score + estimateBonus - getMlbLineupDuplicationPenalty(
            nextState.ownership,
            nextState.highOwnedCount,
            nextState.hitterTeamOwnership,
            mode,
          );
          nextStates.push({ ...nextState, estimate });
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
      states = nextStates.slice(0, beamWidth).map(({ estimate: _estimate, ...state }) => state);
    }

    let bestLineup: MlbGeneratedLineup | null = null;
    let bestScore = Number.NEGATIVE_INFINITY;
    for (const state of states) {
      if (!canMeetTeamMinimums(state.selectedIds, state.teamCounts, [], template.minCountsByTeam)) continue;
      const players = [
        ...pitcherPair,
        ...MLB_BATTER_SLOTS.map((slot) => state.slotAssignment[slot]).filter((player): player is MlbOptimizerPlayer => !!player),
      ];
      const validation = validateMlbLineupExact(
        players,
        minStack,
        previousLineupSets,
        bringBackThreshold,
        antiCorrMax,
        minChanges,
      );
      if (!validation.ok) continue;
      const penalizedScore = state.score - getMlbLineupDuplicationPenalty(
        state.ownership,
        state.highOwnedCount,
        state.hitterTeamOwnership,
        mode,
      );
      if (penalizedScore <= bestScore) continue;
      bestScore = penalizedScore;
      bestLineup = {
        players,
        slots: validation.slots,
        totalSalary: state.salary,
        projFpts: state.projection,
        leverageScore: state.leverage,
        templateId: template.id,
      };
    }

    return bestLineup;
  }

  for (const pitcherPair of pitcherPairs) {
    for (const template of templates) {
      const lineup = buildLineupForTemplate(pitcherPair.players, template);
      if (lineup) {
        templateUsageCount.set(template.id, (templateUsageCount.get(template.id) ?? 0) + 1);
        return lineup;
      }
    }
  }

  return null;
}

function prepareMlbSearchPool(
  pool: MlbOptimizerPlayer[],
  mode: "cash" | "gpp",
  minStack: number,
  bringBackThreshold: number,
  ruleSelections: NormalizedMlbRuleSelections,
): {
  pool: MlbOptimizerPlayer[];
  eligibleCount: number;
  prunedCandidateCount: number;
  templateCount: number;
  pitcherCount: number;
  batterCount: number;
} {
  const eligible = filterEligibleMlbPool(pool, ruleSelections);
  const pitchers = sortMlbPlayersForSearch(
    eligible.filter((player) => isPitcher(player.eligiblePositions)),
    mode,
  );
  const batters = pruneMlbBatterPool(
    eligible.filter((player) => !isPitcher(player.eligiblePositions)),
    mode,
    minStack,
    bringBackThreshold,
    new Set<number>(ruleSelections.playerLocks),
    ruleSelections.requiredTeamStacks,
  );

  return {
    pool: [...pitchers, ...batters],
    eligibleCount: eligible.length,
    prunedCandidateCount: pitchers.length + batters.length,
    templateCount: enumerateMlbStackTemplates(
      batters,
      mode,
      minStack,
      bringBackThreshold,
      ruleSelections.requiredTeamStacks,
    ).length,
    pitcherCount: pitchers.length,
    batterCount: batters.length,
  };
}

function getMlbFailureReasonFromSearch(search: {
  eligibleCount: number;
  pitcherCount: number;
  batterCount: number;
  templateCount: number;
}): string {
  if (search.eligibleCount < MLB_ROSTER_SIZE || search.pitcherCount < 2 || search.batterCount < MLB_BATTER_SLOTS.length) {
    return "exposure_exhausted";
  }
  if (search.templateCount === 0) {
    return "no_valid_templates";
  }
  return "salary_feasible_fill_not_found";
}

export function optimizeMlbLineups(
  pool: MlbOptimizerPlayer[],
  settings: MlbOptimizerSettings,
): MlbGeneratedLineup[] {
  return optimizeMlbLineupsWithDebug(pool, settings).lineups;
}

export function optimizeMlbLineupsWithDebug(
  pool: MlbOptimizerPlayer[],
  settings: MlbOptimizerSettings,
): { lineups: MlbGeneratedLineup[]; debug: OptimizerDebugInfo } {
  const {
    mode,
    nLineups,
    minStack,
    maxExposure,
    bringBackThreshold,
    antiCorrMax,
    hrCorrelation,
    hrCorrelationThreshold,
    pitcherCeilingBoost,
    pitcherCeilingCount,
  } = settings;
  const pendingLineupPolicy = normalizeMlbPendingLineupPolicy(settings.pendingLineupPolicy);
  const totalStart = Date.now();
  const candidatePool = applyMlbPendingLineupPolicy(pool, pendingLineupPolicy);
  const ruleValidation = validateMlbRuleSelections(candidatePool, settings);
  const ruleSelections = ruleValidation.normalized;

  const initialSearch = prepareMlbSearchPool(candidatePool, mode, minStack, bringBackThreshold, ruleSelections);
  const debug: OptimizerDebugInfo = {
    sport: "mlb",
    mode,
    eligibleCount: initialSearch.eligibleCount,
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
      antiCorrMax,
      pendingLineupPolicy,
    },
  };

  if (!ruleValidation.ok) {
    debug.terminationReason = "probe_infeasible";
    debug.totalMs = Date.now() - totalStart;
    return { lineups: [], debug };
  }

  if (initialSearch.eligibleCount < MLB_ROSTER_SIZE) {
    debug.terminationReason = "insufficient_pool";
    debug.totalMs = Date.now() - totalStart;
    return { lineups: [], debug };
  }

  const freshCount = (players: MlbOptimizerPlayer[]) => new Map<number, number>(players.map((player) => [player.id, 0]));
  const timedProbe = (label: string, stack: number, bringBack: number, antiCorr: number) => {
    const start = Date.now();
    const search = prepareMlbSearchPool(candidatePool, mode, stack, bringBack, ruleSelections);
    const success = !!solveMlbLineup(
      search.pool,
      mode,
      stack,
      nLineups,
      freshCount(search.pool),
      [],
      bringBack,
      antiCorr,
      undefined,
      ruleSelections,
    );
    const durationMs = Date.now() - start;
    debug.probeMs += durationMs;
    debug.probeSummary.push({ label, success, durationMs });
    return success;
  };

  let effectiveMinStack = minStack;
  let effectiveBringBack = bringBackThreshold;
  let effectiveAntiCorr = antiCorrMax;
  let relaxed = false;

  if (!timedProbe(`stack=${effectiveMinStack},bringBack=${effectiveBringBack},antiCorr=${effectiveAntiCorr}`, effectiveMinStack, effectiveBringBack, effectiveAntiCorr)) {
    effectiveBringBack = 0;
    relaxed = true;
    debug.relaxedConstraints.push("bring-back disabled");
  }
  if (!timedProbe(`stack=${effectiveMinStack},bringBack=${effectiveBringBack},antiCorr=${effectiveAntiCorr}`, effectiveMinStack, effectiveBringBack, effectiveAntiCorr)) {
    effectiveAntiCorr = MLB_ROSTER_SIZE;
    relaxed = true;
    debug.relaxedConstraints.push("anti-correlation disabled");
  }
  if (!timedProbe(`stack=${effectiveMinStack},bringBack=${effectiveBringBack},antiCorr=${effectiveAntiCorr}`, effectiveMinStack, effectiveBringBack, effectiveAntiCorr)) {
    effectiveMinStack = 0;
    relaxed = true;
    debug.relaxedConstraints.push("stacking disabled");
  }
  if (relaxed && !timedProbe(`stack=${effectiveMinStack},bringBack=${effectiveBringBack},antiCorr=${effectiveAntiCorr}`, effectiveMinStack, effectiveBringBack, effectiveAntiCorr)) {
    debug.terminationReason = "probe_infeasible";
    debug.totalMs = Date.now() - totalStart;
    return { lineups: [], debug };
  }

  const effectiveMinChanges = mode === "gpp" && initialSearch.eligibleCount >= 60 && !relaxed ? 3 : 2;
  const preparedSearch = prepareMlbSearchPool(candidatePool, mode, effectiveMinStack, effectiveBringBack, ruleSelections);
  const preparedPool = preparedSearch.pool;
  debug.effectiveSettings = {
    minStack: effectiveMinStack,
    bringBackThreshold: effectiveBringBack,
    maxExposure,
    minChanges: effectiveMinChanges,
    antiCorrMax: effectiveAntiCorr,
    pendingLineupPolicy,
  };
  debug.heuristic = {
    prunedCandidateCount: preparedSearch.prunedCandidateCount,
    templateCount: preparedSearch.templateCount,
    templatesTried: 0,
    repairAttempts: 0,
    rejectedByReason: {},
  };

  const maxExp = Math.ceil(nLineups * maxExposure);
  const exposureCount = new Map<number, number>(preparedPool.map((player) => [player.id, 0]));
  const templateUsageCount = new Map<string, number>();
  const lineups: MlbGeneratedLineup[] = [];
  const previousLineupSets: Set<number>[] = [];

  // Compute HR correlation bonus map once for the run — preceding batters of high-HR players
  // get a score bonus so the beam search prefers to co-include them.
  const preparedBatters = preparedPool.filter((p) => !isPitcher(p.eligiblePositions));
  const preparedPitchers = preparedPool.filter((p) => isPitcher(p.eligiblePositions));
  const hrBonusById: Map<number, number> = hrCorrelation
    ? computeHrBonusMap(preparedBatters, hrCorrelationThreshold)
    : new Map();
  const pitcherCeilingBonusById: Map<number, number> = pitcherCeilingBoost
    ? computePitcherCeilingBonusMap(preparedPitchers, pitcherCeilingCount)
    : new Map();

  // Compute base templates once for the entire run — batter pool doesn't change lineup-to-lineup.
  const sharedBaseTemplates = enumerateMlbStackTemplates(
    preparedBatters,
    mode,
    effectiveMinStack,
    effectiveBringBack,
    ruleSelections.requiredTeamStacks,
  );

  for (let i = 0; i < nLineups; i++) {
    const attempts: OptimizerLineupAttemptDebug[] = [];
    const runAttempt = (
      stage: string,
      stack: number,
      bringBack: number,
      antiCorr: number,
      minChanges: number,
    ): MlbGeneratedLineup | null => {
      const start = Date.now();
      const lineup = solveMlbLineup(
        preparedPool,
        mode,
        stack,
        maxExp,
        exposureCount,
        previousLineupSets,
        bringBack,
        antiCorr,
        minChanges,
        ruleSelections,
        templateUsageCount,
        sharedBaseTemplates,
        i,
        hrBonusById,
        pitcherCeilingBonusById,
      );
      const durationMs = Date.now() - start;
      const failureReason = lineup
        ? undefined
        : getMlbFailureReasonFromSearch(preparedSearch);
      attempts.push({
        stage,
        success: lineup != null,
        durationMs,
        prunedCandidateCount: preparedSearch.prunedCandidateCount,
        templateCount: preparedSearch.templateCount,
        failureReason,
      });
      if (debug.heuristic) {
        debug.heuristic.templatesTried += 1;
        if (failureReason) {
          debug.heuristic.rejectedByReason[failureReason] =
            (debug.heuristic.rejectedByReason[failureReason] ?? 0) + 1;
        }
      }
      return lineup;
    };

    let lineup = runAttempt("base", effectiveMinStack, effectiveBringBack, effectiveAntiCorr, effectiveMinChanges);
    if (!lineup && effectiveMinChanges > 1) {
      lineup = runAttempt("diversity=1", effectiveMinStack, effectiveBringBack, effectiveAntiCorr, 1);
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
    previousLineupSets.push(new Set(lineup.players.map((player) => player.id)));
    for (const player of lineup.players) {
      exposureCount.set(player.id, (exposureCount.get(player.id) ?? 0) + 1);
    }
  }

  debug.builtLineups = lineups.length;
  debug.totalMs = Date.now() - totalStart;
  if (lineups.length === nLineups) {
    debug.terminationReason = "completed";
  }

  return { lineups, debug };
}

export function prepareMlbOptimizerRun(
  pool: MlbOptimizerPlayer[],
  settings: MlbOptimizerSettings,
): { prepared?: MlbPreparedOptimizerRun; debug: OptimizerDebugInfo; error?: string } {
  const totalStart = Date.now();
  const {
    mode,
    nLineups,
    minStack,
    maxExposure,
    bringBackThreshold,
    antiCorrMax,
    hrCorrelation,
    hrCorrelationThreshold,
    pitcherCeilingBoost,
    pitcherCeilingCount,
  } = settings;
  const pendingLineupPolicy = normalizeMlbPendingLineupPolicy(settings.pendingLineupPolicy);
  const candidatePool = applyMlbPendingLineupPolicy(pool, pendingLineupPolicy);
  const ruleValidation = validateMlbRuleSelections(candidatePool, settings);
  const ruleSelections = ruleValidation.normalized;

  const initialSearch = prepareMlbSearchPool(candidatePool, mode, minStack, bringBackThreshold, ruleSelections);
  const debug: OptimizerDebugInfo = {
    sport: "mlb",
    mode,
    eligibleCount: initialSearch.eligibleCount,
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
      antiCorrMax,
      pendingLineupPolicy,
    },
  };

  if (!ruleValidation.ok) {
    debug.terminationReason = "probe_infeasible";
    debug.totalMs = Date.now() - totalStart;
    return { debug, error: ruleValidation.error };
  }

  if (initialSearch.eligibleCount < MLB_ROSTER_SIZE) {
    debug.terminationReason = "insufficient_pool";
    debug.totalMs = Date.now() - totalStart;
    return { debug };
  }

  const freshCount = (players: MlbOptimizerPlayer[]) => new Map<number, number>(players.map((player) => [player.id, 0]));
  const timedProbe = (label: string, stack: number, bringBack: number, antiCorr: number) => {
    const start = Date.now();
    const search = prepareMlbSearchPool(candidatePool, mode, stack, bringBack, ruleSelections);
    const success = !!solveMlbLineup(
      search.pool,
      mode,
      stack,
      nLineups,
      freshCount(search.pool),
      [],
      bringBack,
      antiCorr,
      undefined,
      ruleSelections,
    );
    const durationMs = Date.now() - start;
    debug.probeMs += durationMs;
    debug.probeSummary.push({ label, success, durationMs });
    return success;
  };

  let effectiveMinStack = minStack;
  let effectiveBringBack = bringBackThreshold;
  let effectiveAntiCorr = antiCorrMax;
  let relaxed = false;

  if (!timedProbe(`stack=${effectiveMinStack},bringBack=${effectiveBringBack},antiCorr=${effectiveAntiCorr}`, effectiveMinStack, effectiveBringBack, effectiveAntiCorr)) {
    effectiveBringBack = 0;
    relaxed = true;
    debug.relaxedConstraints.push("bring-back disabled");
  }
  if (!timedProbe(`stack=${effectiveMinStack},bringBack=${effectiveBringBack},antiCorr=${effectiveAntiCorr}`, effectiveMinStack, effectiveBringBack, effectiveAntiCorr)) {
    effectiveAntiCorr = MLB_ROSTER_SIZE;
    relaxed = true;
    debug.relaxedConstraints.push("anti-correlation disabled");
  }
  if (!timedProbe(`stack=${effectiveMinStack},bringBack=${effectiveBringBack},antiCorr=${effectiveAntiCorr}`, effectiveMinStack, effectiveBringBack, effectiveAntiCorr)) {
    effectiveMinStack = 0;
    relaxed = true;
    debug.relaxedConstraints.push("stacking disabled");
  }
  if (relaxed && !timedProbe(`stack=${effectiveMinStack},bringBack=${effectiveBringBack},antiCorr=${effectiveAntiCorr}`, effectiveMinStack, effectiveBringBack, effectiveAntiCorr)) {
    debug.terminationReason = "probe_infeasible";
    debug.totalMs = Date.now() - totalStart;
    return { debug };
  }

  const effectiveMinChanges = mode === "gpp" && initialSearch.eligibleCount >= 60 && !relaxed ? 3 : 2;
  const preparedSearch = prepareMlbSearchPool(candidatePool, mode, effectiveMinStack, effectiveBringBack, ruleSelections);
  debug.effectiveSettings = {
    minStack: effectiveMinStack,
    bringBackThreshold: effectiveBringBack,
    maxExposure,
    minChanges: effectiveMinChanges,
    antiCorrMax: effectiveAntiCorr,
    pendingLineupPolicy,
  };
  debug.heuristic = {
    prunedCandidateCount: preparedSearch.prunedCandidateCount,
    templateCount: preparedSearch.templateCount,
    templatesTried: 0,
    repairAttempts: 0,
    rejectedByReason: {},
  };
  debug.totalMs = Date.now() - totalStart;

  const preparedBattersForBonus = preparedSearch.pool.filter((p) => !isPitcher(p.eligiblePositions));
  const preparedPitchersForBonus = preparedSearch.pool.filter((p) => isPitcher(p.eligiblePositions));
  const hrBonusMap = hrCorrelation
    ? computeHrBonusMap(preparedBattersForBonus, hrCorrelationThreshold)
    : new Map<number, number>();
  const pitcherCeilingBonusMap = pitcherCeilingBoost
    ? computePitcherCeilingBonusMap(preparedPitchersForBonus, pitcherCeilingCount)
    : new Map<number, number>();
  const hrBonusRecord: Record<number, number> = Object.fromEntries(hrBonusMap);
  const pitcherCeilingBonusRecord: Record<number, number> = Object.fromEntries(pitcherCeilingBonusMap);

  return {
    prepared: {
      sport: "mlb",
      mode,
      requestedLineups: nLineups,
      maxExposureCount: debug.maxExposureCount,
      eligibleCount: initialSearch.eligibleCount,
      pool: preparedSearch.pool,
      ruleSelections,
      effectiveSettings: {
        minStack: effectiveMinStack,
        bringBackThreshold: effectiveBringBack,
        maxExposure,
        minChanges: effectiveMinChanges,
        antiCorrMax: effectiveAntiCorr,
        pendingLineupPolicy,
      },
      hrBonusRecord,
      pitcherCeilingBonusRecord,
      relaxedConstraints: [...debug.relaxedConstraints],
      probeSummary: [...debug.probeSummary],
    },
    debug,
  };
}

export function buildNextMlbLineup(
  prepared: MlbPreparedOptimizerRun,
  priorLineupPlayerIds: number[][],
): NextMlbLineupResult {
  const exposureCount = new Map<number, number>(prepared.pool.map((player) => [player.id, 0]));
  const previousLineupSets = priorLineupPlayerIds.map((ids) => new Set(ids));

  for (const lineup of priorLineupPlayerIds) {
    for (const playerId of lineup) {
      exposureCount.set(playerId, (exposureCount.get(playerId) ?? 0) + 1);
    }
  }

  // Reconstruct the HR bonus Map from the serialized record (Maps aren't JSON-serializable).
  const hrBonusById = new Map<number, number>(
    Object.entries(prepared.hrBonusRecord ?? {}).map(([k, v]) => [Number(k), v]),
  );
  const pitcherCeilingBonusById = new Map<number, number>(
    Object.entries(prepared.pitcherCeilingBonusRecord ?? {}).map(([k, v]) => [Number(k), v]),
  );

  const preparedSearch = prepareMlbSearchPool(
    prepared.pool,
    prepared.mode,
    prepared.effectiveSettings.minStack,
    prepared.effectiveSettings.bringBackThreshold,
    prepared.ruleSelections,
  );
  const attempts: OptimizerLineupAttemptDebug[] = [];
  const runAttempt = (
    stage: string,
    stack: number,
    bringBack: number,
    antiCorr: number,
    minChanges: number,
  ): MlbGeneratedLineup | null => {
    const start = Date.now();
    const lineup = solveMlbLineup(
      prepared.pool,
      prepared.mode,
      stack,
      prepared.maxExposureCount,
      exposureCount,
      previousLineupSets,
      bringBack,
      antiCorr,
      minChanges,
      prepared.ruleSelections,
      undefined,
      undefined,
      priorLineupPlayerIds.length,
      hrBonusById,
      pitcherCeilingBonusById,
    );
    attempts.push({
      stage,
      success: lineup != null,
      durationMs: Date.now() - start,
      prunedCandidateCount: preparedSearch.prunedCandidateCount,
      templateCount: preparedSearch.templateCount,
      failureReason: lineup ? undefined : getMlbFailureReasonFromSearch(preparedSearch),
    });
    return lineup;
  };

  const { minStack, bringBackThreshold, antiCorrMax, minChanges } = prepared.effectiveSettings;
  let lineup = runAttempt("base", minStack, bringBackThreshold, antiCorrMax, minChanges);
  if (!lineup && minChanges > 1) {
    lineup = runAttempt("diversity=1", minStack, bringBackThreshold, antiCorrMax, 1);
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

export function buildMlbMultiEntryCSV(
  lineups: MlbGeneratedLineup[],
): string {
  if (lineups.length === 0) return "";
  const slotOrder: MlbLineupSlot[] = ["P1", "P2", "C", "1B", "2B", "3B", "SS", "OF1", "OF2", "OF3"];
  const rows = [stringifyCsvLine([
    "Lineup",
    ...slotOrder,
    "Salary",
    "Projection",
    "Leverage",
  ])];

  for (let i = 0; i < lineups.length; i++) {
    const lineup = lineups[i];
    const batterTeamCounts = new Map<string, number>();
    for (const player of lineup.players) {
      if (isPitcher(player.eligiblePositions)) continue;
      const teamKey = player.teamAbbrev || (player.teamId != null ? String(player.teamId) : "");
      if (!teamKey) continue;
      const nextCount = (batterTeamCounts.get(teamKey) ?? 0) + 1;
      if (nextCount > MLB_MAX_HITTERS_PER_TEAM) {
        throw new Error(
          `Lineup ${i + 1} has ${nextCount} hitters from ${teamKey}. DraftKings allows at most ${MLB_MAX_HITTERS_PER_TEAM}. Regenerate the MLB lineups before exporting.`,
        );
      }
      batterTeamCounts.set(teamKey, nextCount);
    }
    rows.push(stringifyCsvLine([
      String(i + 1),
      ...slotOrder.map((slot) => {
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
