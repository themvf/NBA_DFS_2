"use client";

import { memo, useCallback, useDeferredValue, useEffect, useMemo, useRef, useState, useTransition } from "react";
import type { UIEvent } from "react";
import type { DfsPagePlayerRow as DkPlayerRow, MlbGameEnvironmentCard, MlbPitcherSlateSignal, Sport } from "@/db/queries";
import { DK_SLATE_TIMING_OPTIONS } from "@/lib/dk-slate-timing";
import type { DkSlateTiming } from "@/lib/dk-slate-timing";
import MlbGameCardStrip from "./mlb-game-card-strip";
import type { GeneratedLineup, OptimizerSettings } from "./optimizer";
import type { MlbGeneratedLineup, MlbOptimizerSettings } from "./mlb-optimizer";
import type { OptimizerDebugInfo } from "./optimizer-debug";
import type { CreateOptimizerJobResponse, OptimizerJobStatusResponse, PersistedOptimizerJobLineup } from "./optimizer-job-types";
import type { OptimizerMode } from "./optimizer-mode";
import { buildMlbBlowupCandidates } from "./mlb-blowup";
import { getMlbLineupStatus, isMlbRowUnavailable, type MlbPendingLineupPolicy } from "./mlb-lineup";
import {
  normalizeNbaRuleSelections,
  validateNbaRuleSelections,
  type NbaTeamStackRule,
} from "./nba-optimizer-rules";
import {
  normalizeMlbRuleSelections,
  validateMlbRuleSelections,
  type MlbTeamStackRule,
} from "./mlb-optimizer-rules";
import { processDkSlate, loadSlateFromContestId, loadMlbSlateFromContestId, saveLineups, exportLineups, exportMlbLineups, refreshPlayerStatus, checkLinestarCookie, uploadLinestarCsv, applyLinestarPaste, fetchPlayerProps, clearSlate, recomputeProjections, auditNbaPropCoverage, auditMlbPropCoverage } from "./actions";

type Props = {
  players: DkPlayerRow[];
  slateDate: string | null;
  mlbPitcherSignals: MlbPitcherSlateSignal[];
  mlbGameCards?: MlbGameEnvironmentCard[];
  sport: Sport;
};

type SortCol =
  | "name"
  | "salary"
  | "avgFptsDk"
  | "linestarProj"
  | "linestarOwnPct"
  | "ourProj"
  | "delta"
  | "projOwnPct"
  | "ourOwnPct"
  | "liveOwnPct"
  | "ourLeverage"
  | "hrProb"
  | "value";
type SortDir = "asc" | "desc";
type TeamStackRule = NbaTeamStackRule | MlbTeamStackRule;
type StackSize = TeamStackRule["stackSize"];
type MlbPlayerPoolFilter = "all" | "p" | "c" | "1b" | "2b" | "3b" | "ss" | "of" | "hitters";
type RuleState = {
  playerLocks: number[];
  playerBlocks: number[];
  blockedTeamIds: number[];
  requiredTeamStacks: TeamStackRule[];
};

type NbaPropCoverageAuditResult = {
  ok: boolean;
  message: string;
  selectedGames: string[];
  playerPoolCount: number;
  bookmakerCount?: number;
  books?: Array<{
    bookmakerKey: string;
    bookmakerTitle: string;
    uniquePlayers: number;
    stats: {
      pts: number;
      reb: number;
      ast: number;
      blk: number;
      stl: number;
    };
  }>;
  leaders?: Array<{
    stat: "pts" | "reb" | "ast" | "blk" | "stl";
    bookmakerKey: string;
    bookmakerTitle: string;
    count: number;
  }>;
};

type MlbPropCoverageAuditResult = {
  ok: boolean;
  message: string;
  selectedGames: string[];
  playerPoolCount: number;
  bookmakerCount?: number;
  books?: Array<{
    bookmakerKey: string;
    bookmakerTitle: string;
    uniquePlayers: number;
    stats: {
      hits: number;
      tb: number;
      runs: number;
      rbis: number;
      hr: number;
      ks: number;
      outs: number;
      er: number;
    };
  }>;
  leaders?: Array<{
    stat: "hits" | "tb" | "runs" | "rbis" | "hr" | "ks" | "outs" | "er";
    bookmakerKey: string;
    bookmakerTitle: string;
    count: number;
  }>;
};

type FilteredTeam = {
  teamId: number;
  teamAbbrev: string;
  teamName: string | null;
  teamLogo: string | null;
};

type TeamOddsSummary = {
  vegasTotal: number | null;
  teamTotal: number | null;
  moneyline: number | null;
};

type MlbPitcherDecisionBadge = {
  label: string;
  className: string;
  title: string;
};

type MlbPitcherCeilingBadge = {
  label: string;
  className: string;
  title: string;
};

type NbaCeilingBadge = {
  label: string;
  className: string;
  title: string;
};

type MlbLineupSummary = {
  confirmedIn: number;
  pending: number;
  confirmedOut: number;
};

type AnyGeneratedLineup = GeneratedLineup | MlbGeneratedLineup;

type SortHeaderProps = {
  col: SortCol;
  label: string;
  sortCol?: SortCol;
  sortDir?: SortDir;
  onToggleSort?: (col: SortCol) => void;
};

type RuleControlsSectionProps = {
  sport: Sport;
  filteredTeams: FilteredTeam[];
  blockedTeamSet: Set<number>;
  requiredTeamStackMap: Map<number, StackSize>;
  teamOddsById: Map<number, TeamOddsSummary>;
  hideOutInactivePlayers: boolean;
  unavailablePlayerCount: number;
  lockedCount: number;
  blockedCount: number;
  requiredTeamStackCount: number;
  onToggleHideOutInactive: () => void;
  onClearLocks: () => void;
  onClearBlocks: () => void;
  onClearTeamRules: () => void;
  onToggleTeamBlock: (teamId: number) => void;
  onUpdateTeamStackRule: (teamId: number, value: string) => void;
};

type PlayerPoolTableProps = {
  sport: Sport;
  visiblePlayers: DkPlayerRow[];
  playerPoolSourceCount: number;
  unavailablePlayerCount: number;
  hideOutInactivePlayers: boolean;
  mlbLineupSummary: MlbLineupSummary;
  mlbPlayerPoolFilter: MlbPlayerPoolFilter;
  onChangeMlbPlayerPoolFilter: (filter: MlbPlayerPoolFilter) => void;
  onToggleHideOutInactive: () => void;
  supportsRuleControls: boolean;
  sortCol: SortCol;
  sortDir: SortDir;
  onToggleSort: (col: SortCol) => void;
  lockedPlayerSet: Set<number>;
  blockedPlayerSet: Set<number>;
  blockedTeamSet: Set<number>;
  requiredTeamStackMap: Map<number, StackSize>;
  nbaTopScorerRanks: Map<number, number>;
  nbaCeilingBadges: Map<number, NbaCeilingBadge>;
  mlbPitcherDecisionBadges: Map<number, MlbPitcherDecisionBadge>;
  mlbPitcherCeilingBadges: Map<number, MlbPitcherCeilingBadge>;
  manuallyOutSet: Set<number>;
  manuallyOutAdjustments: Map<number, number>;
  onTogglePlayerLock: (player: DkPlayerRow) => void;
  onTogglePlayerBlock: (player: DkPlayerRow) => void;
  onToggleManuallyOut: (player: DkPlayerRow) => void;
};

type GeneratedLineupsSectionProps = {
  sport: Sport;
  activeLineups: AnyGeneratedLineup[];
  lastRequestedLineupCount: number | null;
  strategy: string;
  onStrategyChange: (value: string) => void;
  onSave: () => void;
  saveMsg: string | null;
  onExport: () => void;
  isExporting: boolean;
  exportError: string | null;
};

type OptimizerStatusPanelProps = {
  isOptimizing: boolean;
  optimizeStartedAt: number | null;
  builtLineupCount: number;
  lastRequestedLineupCount: number | null;
};

type TimedSpinnerMessageProps = {
  active: boolean;
  text: string;
};

function parseGameKey(gameInfo: string | null): string {
  if (!gameInfo) return "Unknown";
  return gameInfo.split(" ")[0] ?? "Unknown";
}

function fmt1(v: number | null | undefined): string {
  return v != null ? v.toFixed(1) : "—";
}

function fmtSalary(v: number): string {
  return `$${v.toLocaleString()}`;
}

function fmtAmericanOdds(v: number | null | undefined): string {
  if (v == null) return "—";
  return v > 0 ? `+${v}` : `${v}`;
}

function mlToProb(ml: number): number {
  return ml >= 0 ? 100 / (ml + 100) : Math.abs(ml) / (Math.abs(ml) + 100);
}

function getPlayerHrMarketProb(player: DkPlayerRow): number | null {
  if (player.propStlPrice == null) return null;
  return mlToProb(player.propStlPrice);
}

function getPlayerHrEdgePct(player: DkPlayerRow): number | null {
  const marketProb = getPlayerHrMarketProb(player);
  if (marketProb == null || player.hrProb1Plus == null) return null;
  return (player.hrProb1Plus - marketProb) * 100;
}

function fmtSignedPctPoint(v: number | null | undefined): string {
  if (v == null) return "—";
  return `${v >= 0 ? "+" : ""}${v.toFixed(1)} pts`;
}

function computeTeamImpliedTotal(
  vegasTotal: number,
  homeMl: number | null,
  awayMl: number | null,
  isHome: boolean,
): number {
  if (homeMl == null || awayMl == null) return vegasTotal / 2;
  const rawHome = mlToProb(homeMl);
  const rawAway = mlToProb(awayMl);
  const vig = rawHome + rawAway;
  const homeProbClean = rawHome / vig;
  const impliedSpread = Math.max(-15, Math.min(15, (homeProbClean - 0.5) / 0.025));
  const homeImplied = vegasTotal / 2 + impliedSpread / 2;
  return isHome ? homeImplied : vegasTotal - homeImplied;
}

function fmtDuration(ms: number): string {
  if (ms < 1000) return `${ms}ms`;
  const totalSeconds = Math.floor(ms / 1000);
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;
  return minutes > 0 ? `${minutes}:${String(seconds).padStart(2, "0")}` : `${seconds}s`;
}

function formatPendingLineupPolicy(policy: MlbPendingLineupPolicy): string {
  switch (policy) {
    case "ignore":
      return "Maintain";
    case "exclude":
      return "Exclude";
    case "downgrade":
    default:
      return "Downgrade";
  }
}

const OPTIMIZER_CLIENT_TOKEN_KEY = "dfsOptimizerClientToken";
const RULE_STORAGE_PREFIX = "dfsOptimizerRules";
const LEGACY_NBA_RULE_STORAGE_PREFIX = "dfsOptimizerNbaRules";
const NBA_SLOT_NAMES = ["PG","SG","SF","PF","C","G","F","UTIL"] as const;
const MLB_SLOT_NAMES = ["P1","P2","C","1B","2B","3B","SS","OF1","OF2","OF3"] as const;
const EMPTY_RULE_STATE: RuleState = {
  playerLocks: [],
  playerBlocks: [],
  blockedTeamIds: [],
  requiredTeamStacks: [],
};
const PLAYER_POOL_VIEWPORT_HEIGHT_PX = 720;
const PLAYER_POOL_OVERSCAN_ROWS = 8;

function getOrCreateOptimizerClientToken(): string {
  const existing = window.localStorage.getItem(OPTIMIZER_CLIENT_TOKEN_KEY);
  if (existing) return existing;
  const next = window.crypto?.randomUUID?.() ?? `client-${Date.now()}`;
  window.localStorage.setItem(OPTIMIZER_CLIENT_TOKEN_KEY, next);
  return next;
}

function optimizerJobStorageKey(sport: Sport, slateId: number): string {
  return `dfsOptimizerJob:${sport}:${slateId}`;
}

function optimizerRuleStorageKey(sport: Sport, slateId: number): string {
  return `${RULE_STORAGE_PREFIX}:${sport}:${slateId}`;
}

function legacyNbaRuleStorageKey(slateId: number): string {
  return `${LEGACY_NBA_RULE_STORAGE_PREFIX}:${slateId}`;
}

function readStoredRuleState(sport: Sport, slateId: number): RuleState {
  const keys = sport === "nba"
    ? [optimizerRuleStorageKey(sport, slateId), legacyNbaRuleStorageKey(slateId)]
    : [optimizerRuleStorageKey(sport, slateId)];

  for (const key of keys) {
    const raw = window.localStorage.getItem(key);
    if (!raw) continue;
    try {
      const parsed = JSON.parse(raw) as Partial<RuleState>;
      const baseState = {
        playerLocks: Array.isArray(parsed.playerLocks) ? parsed.playerLocks : [],
        playerBlocks: Array.isArray(parsed.playerBlocks) ? parsed.playerBlocks : [],
        blockedTeamIds: Array.isArray(parsed.blockedTeamIds) ? parsed.blockedTeamIds : [],
        requiredTeamStacks: Array.isArray(parsed.requiredTeamStacks) ? parsed.requiredTeamStacks : [],
      };
      return sport === "mlb"
        ? normalizeMlbRuleSelections(baseState)
        : normalizeNbaRuleSelections(baseState);
    } catch {
      continue;
    }
  }
  return EMPTY_RULE_STATE;
}

// Position badge color — sport-aware
function posBadgeColor(pos: string, sport: Sport): string {
  if (sport === "mlb") {
    if (pos.includes("SP")) return "bg-orange-100 text-orange-800";
    if (pos.includes("RP")) return "bg-amber-100 text-amber-800";
    if (pos.includes("OF")) return "bg-green-100 text-green-800";
    if (pos.includes("SS")) return "bg-blue-100 text-blue-800";
    if (pos.includes("3B")) return "bg-indigo-100 text-indigo-800";
    if (pos.includes("2B")) return "bg-sky-100 text-sky-800";
    if (pos.includes("1B")) return "bg-cyan-100 text-cyan-800";
    if (pos.includes("C"))  return "bg-red-100 text-red-800";
    return "bg-gray-100 text-gray-700";
  }
  // NBA
  if (pos.includes("PG")) return "bg-blue-100 text-blue-800";
  if (pos.includes("SG")) return "bg-indigo-100 text-indigo-800";
  if (pos.includes("SF")) return "bg-green-100 text-green-800";
  if (pos.includes("PF")) return "bg-yellow-100 text-yellow-800";
  if (pos.includes("C"))  return "bg-red-100 text-red-800";
  return "bg-gray-100 text-gray-700";
}

// Simplified display position — sport-aware
function displayPos(eligiblePositions: string, sport: Sport): string {
  const parts = eligiblePositions.split("/");
  if (sport === "mlb") {
    const primary = parts.find((p) => ["SP","RP","C","1B","2B","3B","SS","OF"].includes(p));
    return primary ?? parts[0] ?? "UTIL";
  }
  const primary = parts.find((p) => ["PG","SG","SF","PF","C"].includes(p));
  return primary ?? parts[0] ?? "UTIL";
}

function shortBookName(value: string | null | undefined): string {
  switch ((value ?? "").toLowerCase()) {
    case "fanduel": return "FD";
    case "draftkings": return "DK";
    case "betmgm": return "MGM";
    case "caesars": return "CZR";
    case "betrivers": return "BR";
    case "betonline.ag": return "BO";
    case "betonlineag": return "BO";
    case "fanatics": return "FAN";
    case "bovada": return "BOV";
    default: return value ?? "";
  }
}

type PlayerPropToken = {
  stat: string;
  line: number;
  price: number | null;
  book: string | null;
};

function isMlbPitcherPlayer(player: DkPlayerRow): boolean {
  return player.eligiblePositions.includes("SP") || player.eligiblePositions.includes("RP");
}

function matchesMlbPlayerPoolFilter(player: DkPlayerRow, filter: MlbPlayerPoolFilter): boolean {
  if (filter === "all") return true;
  const positions = player.eligiblePositions.split("/");
  if (filter === "p") return positions.includes("SP") || positions.includes("RP");
  if (filter === "hitters") return !isMlbPitcherPlayer(player);
  return positions.includes(filter.toUpperCase());
}

const MLB_PLAYER_POOL_FILTER_OPTIONS: Array<{ value: MlbPlayerPoolFilter; label: string }> = [
  { value: "all", label: "All" },
  { value: "p", label: "P" },
  { value: "c", label: "C" },
  { value: "1b", label: "1B" },
  { value: "2b", label: "2B" },
  { value: "3b", label: "3B" },
  { value: "ss", label: "SS" },
  { value: "of", label: "OF" },
  { value: "hitters", label: "Hitters" },
];
const MLB_HR_BADGE_THRESHOLD = 0.25;
const MLB_HR_STRONG_BADGE_THRESHOLD = 0.35;

function getMlbLineupBadge(player: DkPlayerRow): { label: string; className: string } {
  const status = getMlbLineupStatus(player);
  switch (status) {
    case "pitcher":
      return player.isOut
        ? { label: "Out", className: "border-red-200 bg-red-50 text-red-700" }
        : { label: "Starter", className: "border-emerald-200 bg-emerald-50 text-emerald-700" };
    case "confirmed_in":
      return player.dkStartingLineupOrder != null
        ? { label: `L${player.dkStartingLineupOrder}`, className: "border-blue-200 bg-blue-50 text-blue-700" }
        : { label: "IN", className: "border-blue-200 bg-blue-50 text-blue-700" };
    case "confirmed_out":
      return { label: "Out", className: "border-red-200 bg-red-50 text-red-700" };
    case "pending":
    default:
      return { label: "Pending", className: "border-amber-200 bg-amber-50 text-amber-700" };
  }
}

// Batting order value/fade signals derived from 2026 season data:
//   #1 over-owned  (+8.4% own, only 6.4 FPTS) → Fade badge
//   #2/#3 under-owned relative to production   → no badge (captured in leverage)
//   #7 under-owned relative to production (-2.4 bias, 5.4% own) → Value badge
function getMlbOrderBadge(player: DkPlayerRow): { label: string; className: string; title: string } | null {
  if (isMlbPitcherPlayer(player) || player.isOut) return null;
  const order = player.dkStartingLineupOrder;
  if (order == null) return null;
  if (order === 1) return {
    label: "Fade",
    className: "bg-orange-100 text-orange-700",
    title: "Leadoff hitters are systematically over-owned by the field relative to production",
  };
  if (order === 7) return {
    label: "Value",
    className: "bg-violet-100 text-violet-700",
    title: "#7 hitters produce above their ownership level — GPP value spot",
  };
  return null;
}

function getMlbHrBadge(player: DkPlayerRow): { label: string; className: string; title: string } | null {
  if (isMlbPitcherPlayer(player) || player.isOut || player.hrProb1Plus == null) return null;
  if (player.hrProb1Plus < MLB_HR_BADGE_THRESHOLD) return null;

  const pct = Math.round(player.hrProb1Plus * 100);
  const expectedHr = player.expectedHr != null ? player.expectedHr.toFixed(2) : "—";
  const strong = player.hrProb1Plus >= MLB_HR_STRONG_BADGE_THRESHOLD;
  return {
    label: `HR ${pct}%`,
    className: strong
      ? "bg-rose-100 text-rose-700"
      : "bg-orange-100 text-orange-700",
    title: `1+ HR ${pct}% | Expected HR ${expectedHr}`,
  };
}

function getNbaProjectedPoints(player: DkPlayerRow): number | null {
  return player.blendPoints ?? player.marketPoints ?? player.modelPoints ?? null;
}

function getNbaPointsBadge(
  player: DkPlayerRow,
  rank: number | undefined,
): { label: string; className: string; title: string } | null {
  if (rank == null || rank > 5 || player.isOut) return null;
  const projectedPoints = getNbaProjectedPoints(player);
  if (projectedPoints == null) return null;

  return {
    label: `PTS #${rank}`,
    className: rank === 1
      ? "bg-amber-100 text-amber-700"
      : "bg-violet-100 text-violet-700",
    title: `Projected points ${projectedPoints.toFixed(1)}`,
  };
}

function getPlayerPropTokens(player: DkPlayerRow, sport: Sport): PlayerPropToken[] {
  const tokens: PlayerPropToken[] = [];
  const fields = sport === "mlb"
    ? (isMlbPitcherPlayer(player)
      ? [
          { stat: "K", line: player.propPts, price: player.propPtsPrice, book: player.propPtsBook },
          { stat: "OUTS", line: player.propReb, price: player.propRebPrice, book: player.propRebBook },
          { stat: "ER", line: player.propAst, price: player.propAstPrice, book: player.propAstBook },
        ]
      : [
          { stat: "H", line: player.propPts, price: player.propPtsPrice, book: player.propPtsBook },
          { stat: "TB", line: player.propReb, price: player.propRebPrice, book: player.propRebBook },
          { stat: "R", line: player.propAst, price: player.propAstPrice, book: player.propAstBook },
          { stat: "RBI", line: player.propBlk, price: player.propBlkPrice, book: player.propBlkBook },
          { stat: "HR", line: player.propStl, price: player.propStlPrice, book: player.propStlBook },
        ])
    : [
        { stat: "PTS", line: player.propPts, price: player.propPtsPrice, book: player.propPtsBook },
        { stat: "REB", line: player.propReb, price: player.propRebPrice, book: player.propRebBook },
        { stat: "AST", line: player.propAst, price: player.propAstPrice, book: player.propAstBook },
        { stat: "BLK", line: player.propBlk, price: player.propBlkPrice, book: player.propBlkBook },
        { stat: "STL", line: player.propStl, price: player.propStlPrice, book: player.propStlBook },
      ];

  for (const field of fields) {
    if (field.line == null) continue;
    tokens.push({
      stat: field.stat,
      line: field.line,
      price: field.price ?? null,
      book: field.book ?? null,
    });
  }
  return tokens;
}

function getPlayerOddsContext(player: DkPlayerRow): {
  teamTotal: number | null;
  vegasTotal: number | null;
  moneyline: number | null;
} {
  const isHome = player.teamId != null && player.homeTeamId != null
    ? player.teamId === player.homeTeamId
    : null;
  const moneyline = isHome == null
    ? null
    : (isHome ? player.homeMl : player.awayMl);
  const explicitTeamTotal = isHome == null
    ? null
    : (isHome ? player.homeImplied : player.awayImplied);
  const derivedTeamTotal = isHome != null && player.vegasTotal != null
    ? computeTeamImpliedTotal(player.vegasTotal, player.homeMl, player.awayMl, isHome)
    : null;
  return {
    teamTotal: explicitTeamTotal ?? derivedTeamTotal ?? null,
    vegasTotal: player.vegasTotal ?? null,
    moneyline,
  };
}

function getNbaCeilingBadges(players: DkPlayerRow[]): Map<number, NbaCeilingBadge> {
  const activePlayers = players.filter((player) => !player.isOut);
  if (activePlayers.length === 0) return new Map<number, NbaCeilingBadge>();

  const contexts = activePlayers.map((player) => {
    const liveProjection = player.liveProj ?? player.blendProj ?? player.ourProj ?? player.linestarProj ?? null;
    const value = liveProjection != null ? liveProjection / Math.max(1, player.salary / 1000) : null;
    const projCeiling = player.projCeiling ?? (liveProjection != null ? liveProjection * 1.18 : null);
    return {
      player,
      projCeiling,
      boomRate: player.boomRate ?? null,
      propPts: player.propPts ?? null,
      liveProjection,
      value,
      score: 0,
    };
  });

  const ceilingValues = contexts.map((context) => context.projCeiling);
  const boomValues = contexts.map((context) => context.boomRate);
  const propPtsValues = contexts.map((context) => context.propPts);
  const projectionValues = contexts.map((context) => context.liveProjection);
  const valueValues = contexts.map((context) => context.value);

  for (const context of contexts) {
    context.score =
      rankMetric(context.projCeiling, ceilingValues, true) * 0.46 +
      rankMetric(context.boomRate, boomValues, true) * 0.24 +
      rankMetric(context.propPts, propPtsValues, true) * 0.12 +
      rankMetric(context.liveProjection, projectionValues, true) * 0.10 +
      rankMetric(context.value, valueValues, true) * 0.08;
  }

  const sorted = [...contexts].sort((a, b) => {
    const diff = b.score - a.score;
    return diff !== 0 ? diff : a.player.name.localeCompare(b.player.name);
  });

  const result = new Map<number, NbaCeilingBadge>();
  for (const [index, context] of sorted.slice(0, 3).entries()) {
    const scorePct = Math.round(context.score * 100);
    result.set(context.player.id, {
      label: `CEIL #${index + 1}`,
      className: index === 0 ? "bg-fuchsia-100 text-fuchsia-700" : "bg-violet-100 text-violet-700",
      title: [
        `Ceiling score ${scorePct}`,
        `Ceiling ${context.projCeiling != null ? context.projCeiling.toFixed(1) : "—"}`,
        `Boom ${context.boomRate != null ? `${(context.boomRate * 100).toFixed(0)}%` : "—"}`,
        `PTS prop ${context.propPts != null ? context.propPts.toFixed(1) : "—"}`,
        `Live proj ${context.liveProjection != null ? context.liveProjection.toFixed(1) : "—"}`,
        `Value ${context.value != null ? context.value.toFixed(2) : "—"}`,
      ].join(" | "),
    });
  }

  return result;
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

function getMlbPitcherDecisionBadges(players: DkPlayerRow[]): Map<number, MlbPitcherDecisionBadge> {
  const activePitchers = players.filter((player) => isMlbPitcherPlayer(player) && !isMlbRowUnavailable(player));
  if (activePitchers.length < 2) return new Map<number, MlbPitcherDecisionBadge>();

  const contexts = activePitchers.map((player) => {
    const odds = getPlayerOddsContext(player);
    const projection = player.ourProj ?? player.linestarProj ?? null;
    const value = projection != null ? projection / Math.max(1, player.salary / 1000) : null;
    return {
      player,
      odds,
      projection,
      value,
      strikeouts: player.propPts ?? null,
      outs: player.propReb ?? null,
      earnedRuns: player.propAst ?? null,
      leverage: player.ourLeverage ?? null,
      winProb: odds.moneyline != null ? mlToProb(odds.moneyline) : null,
      score: 0,
    };
  });

  const strikeoutValues = contexts.map((context) => context.strikeouts);
  const outsValues = contexts.map((context) => context.outs);
  const erValues = contexts.map((context) => context.earnedRuns);
  const teamTotalValues = contexts.map((context) => context.odds.teamTotal);
  const winProbValues = contexts.map((context) => context.winProb);
  const projectionValues = contexts.map((context) => context.projection);
  const valueValues = contexts.map((context) => context.value);
  const leverageValues = contexts.map((context) => context.leverage);

  for (const context of contexts) {
    context.score =
      rankMetric(context.strikeouts, strikeoutValues, true) * 0.24 +
      rankMetric(context.outs, outsValues, true) * 0.14 +
      rankMetric(context.earnedRuns, erValues, false) * 0.10 +
      rankMetric(context.odds.teamTotal, teamTotalValues, false) * 0.16 +
      rankMetric(context.winProb, winProbValues, true) * 0.10 +
      rankMetric(context.projection, projectionValues, true) * 0.10 +
      rankMetric(context.value, valueValues, true) * 0.08 +
      rankMetric(context.leverage, leverageValues, true) * 0.08;
  }

  const sorted = [...contexts].sort((a, b) => {
    const diff = b.score - a.score;
    return diff !== 0 ? diff : a.player.name.localeCompare(b.player.name);
  });

  const badgeCount = Math.max(1, Math.ceil(sorted.length * 0.2));
  const fadeStart = Math.max(sorted.length - badgeCount, 0);
  const result = new Map<number, MlbPitcherDecisionBadge>();

  for (const [index, context] of sorted.entries()) {
    const scorePct = Math.round(context.score * 100);
    const tooltipParts = [
      `Score ${scorePct}`,
      `K ${context.strikeouts != null ? context.strikeouts.toFixed(1) : "—"}`,
      `Outs ${context.outs != null ? context.outs.toFixed(1) : "—"}`,
      `ER ${context.earnedRuns != null ? context.earnedRuns.toFixed(1) : "—"}`,
      `Opp TT ${context.odds.teamTotal != null ? context.odds.teamTotal.toFixed(1) : "—"}`,
      `ML ${fmtAmericanOdds(context.odds.moneyline)}`,
      `Proj ${context.projection != null ? context.projection.toFixed(1) : "—"}`,
      `Value ${context.value != null ? context.value.toFixed(2) : "—"}`,
      `Lev ${context.leverage != null ? context.leverage.toFixed(1) : "—"}`,
    ];

    if (index < badgeCount && context.score >= 0.56) {
      result.set(context.player.id, {
        label: "Choose",
        className: "bg-emerald-100 text-emerald-700",
        title: tooltipParts.join(" | "),
      });
    } else if (index >= fadeStart && context.score <= 0.44) {
      result.set(context.player.id, {
        label: "Fade",
        className: "bg-rose-100 text-rose-700",
        title: tooltipParts.join(" | "),
      });
    }
  }

  return result;
}

function getMlbPitcherCeilingBadges(players: DkPlayerRow[]): Map<number, MlbPitcherCeilingBadge> {
  const activePitchers = players.filter((player) => isMlbPitcherPlayer(player) && !isMlbRowUnavailable(player));
  if (activePitchers.length === 0) return new Map<number, MlbPitcherCeilingBadge>();

  const contexts = activePitchers.map((player) => {
    const odds = getPlayerOddsContext(player);
    const projection = player.ourProj ?? player.linestarProj ?? null;
    const value = projection != null ? projection / Math.max(1, player.salary / 1000) : null;
    return {
      player,
      odds,
      projection,
      value,
      strikeouts: player.propPts ?? null,
      outs: player.propReb ?? null,
      earnedRuns: player.propAst ?? null,
      winProb: odds.moneyline != null ? mlToProb(odds.moneyline) : null,
      score: 0,
    };
  });

  const strikeoutValues = contexts.map((context) => context.strikeouts);
  const outsValues = contexts.map((context) => context.outs);
  const erValues = contexts.map((context) => context.earnedRuns);
  const teamTotalValues = contexts.map((context) => context.odds.teamTotal);
  const winProbValues = contexts.map((context) => context.winProb);
  const projectionValues = contexts.map((context) => context.projection);
  const valueValues = contexts.map((context) => context.value);

  for (const context of contexts) {
    context.score =
      rankMetric(context.strikeouts, strikeoutValues, true) * 0.34 +
      rankMetric(context.outs, outsValues, true) * 0.22 +
      rankMetric(context.earnedRuns, erValues, false) * 0.10 +
      rankMetric(context.odds.teamTotal, teamTotalValues, false) * 0.10 +
      rankMetric(context.winProb, winProbValues, true) * 0.10 +
      rankMetric(context.projection, projectionValues, true) * 0.09 +
      rankMetric(context.value, valueValues, true) * 0.05;
  }

  const sorted = [...contexts].sort((a, b) => {
    const diff = b.score - a.score;
    return diff !== 0 ? diff : a.player.name.localeCompare(b.player.name);
  });

  const result = new Map<number, MlbPitcherCeilingBadge>();
  for (const [index, context] of sorted.slice(0, 3).entries()) {
    const ceilingPct = Math.round(context.score * 100);
    result.set(context.player.id, {
      label: `CEIL #${index + 1}`,
      className: index === 0 ? "bg-fuchsia-100 text-fuchsia-700" : "bg-violet-100 text-violet-700",
      title: [
        `Ceiling score ${ceilingPct}`,
        `K ${context.strikeouts != null ? context.strikeouts.toFixed(1) : "—"}`,
        `Outs ${context.outs != null ? context.outs.toFixed(1) : "—"}`,
        `ER ${context.earnedRuns != null ? context.earnedRuns.toFixed(1) : "—"}`,
        `Opp TT ${context.odds.teamTotal != null ? context.odds.teamTotal.toFixed(1) : "—"}`,
        `ML ${fmtAmericanOdds(context.odds.moneyline)}`,
        `Proj ${context.projection != null ? context.projection.toFixed(1) : "—"}`,
        `Value ${context.value != null ? context.value.toFixed(2) : "—"}`,
      ].join(" | "),
    });
  }

  return result;
}

function buildNbaLineupsFromPersisted(
  persisted: PersistedOptimizerJobLineup[],
  playersById: Map<number, DkPlayerRow>,
): GeneratedLineup[] {
  return persisted.flatMap((lineup) => {
    const slots = {} as GeneratedLineup["slots"];
    for (const slot of NBA_SLOT_NAMES) {
      const playerId = lineup.slotPlayerIds[slot];
      const player = playerId != null ? playersById.get(playerId) : undefined;
      if (!player) return [];
      slots[slot] = player as unknown as GeneratedLineup["slots"][typeof slot];
    }

    const players = lineup.playerIds
      .map((id) => playersById.get(id))
      .filter((player): player is DkPlayerRow => !!player)
      .map((player) => player as unknown as GeneratedLineup["players"][number]);
    if (players.length !== lineup.playerIds.length) return [];

    return [{
      players,
      slots,
      totalSalary: lineup.totalSalary,
      projFpts: lineup.projFpts,
      leverageScore: lineup.leverageScore,
    }];
  });
}

function buildMlbLineupsFromPersisted(
  persisted: PersistedOptimizerJobLineup[],
  playersById: Map<number, DkPlayerRow>,
): MlbGeneratedLineup[] {
  return persisted.flatMap((lineup) => {
    const slots = {} as MlbGeneratedLineup["slots"];
    for (const slot of MLB_SLOT_NAMES) {
      const playerId = lineup.slotPlayerIds[slot];
      const player = playerId != null ? playersById.get(playerId) : undefined;
      if (!player) return [];
      slots[slot] = player as unknown as MlbGeneratedLineup["slots"][typeof slot];
    }

    const players = lineup.playerIds
      .map((id) => playersById.get(id))
      .filter((player): player is DkPlayerRow => !!player)
      .map((player) => player as unknown as MlbGeneratedLineup["players"][number]);
    if (players.length !== lineup.playerIds.length) return [];

    return [{
      players,
      slots,
      totalSalary: lineup.totalSalary,
      projFpts: lineup.projFpts,
      leverageScore: lineup.leverageScore,
    }];
  });
}

function getPersistedLineupSignature(lineups: PersistedOptimizerJobLineup[]): string {
  if (lineups.length === 0) return "0";
  const last = lineups[lineups.length - 1];
  return `${lineups.length}:${last?.lineupNumber ?? 0}:${last?.totalSalary ?? 0}:${last?.projFpts ?? 0}`;
}

function getOptimizerDebugSignature(
  debug: OptimizerDebugInfo | null,
  status: OptimizerJobStatusResponse["job"]["status"],
): string {
  if (!debug) return `none:${status}`;
  if (status === "queued" || status === "running") {
    return `${status}:${debug.builtLineups}:${debug.lineupSummaries.length}`;
  }
  return [
    status,
    debug.builtLineups,
    debug.totalMs,
    debug.lineupSummaries.length,
    debug.probeSummary.length,
    debug.terminationReason ?? "",
  ].join(":");
}

const SortHeader = memo(function SortHeader({ col, label, sortCol, sortDir, onToggleSort }: SortHeaderProps) {
  const active = sortCol === col;
  return (
    <th
      className={`px-3 py-2 text-left text-xs font-medium cursor-pointer select-none whitespace-nowrap ${
        active ? "text-blue-600" : "text-gray-500 hover:text-gray-700"
      }`}
      onClick={() => onToggleSort?.(col)}
    >
      {label}
      {active ? (sortDir === "desc" ? " ↓" : " ↑") : ""}
    </th>
  );
});

const TimedSpinnerMessage = memo(function TimedSpinnerMessage({ active, text }: TimedSpinnerMessageProps) {
  const [elapsedSeconds, setElapsedSeconds] = useState(0);

  useEffect(() => {
    if (!active) {
      setElapsedSeconds(0);
      return;
    }
    setElapsedSeconds(0);
    const startedAt = Date.now();
    const id = window.setInterval(() => {
      setElapsedSeconds(Math.floor((Date.now() - startedAt) / 1000));
    }, 1000);
    return () => window.clearInterval(id);
  }, [active]);

  if (!active) return null;

  return (
    <div className="flex items-center gap-2 text-sm text-emerald-700">
      <span className="inline-block h-4 w-4 rounded-full border-2 border-emerald-500 border-t-transparent animate-spin" />
      <span>{text} ({elapsedSeconds}s)</span>
    </div>
  );
});

const OptimizerStatusPanel = memo(function OptimizerStatusPanel({
  isOptimizing,
  optimizeStartedAt,
  builtLineupCount,
  lastRequestedLineupCount,
}: OptimizerStatusPanelProps) {
  const [elapsedMs, setElapsedMs] = useState(0);

  useEffect(() => {
    if (!isOptimizing || optimizeStartedAt == null) {
      setElapsedMs(0);
      return;
    }
    setElapsedMs(Date.now() - optimizeStartedAt);
    const id = window.setInterval(() => {
      setElapsedMs(Date.now() - optimizeStartedAt);
    }, 1000);
    return () => window.clearInterval(id);
  }, [isOptimizing, optimizeStartedAt]);

  const optimizeStatusText = !isOptimizing
    ? null
    : lastRequestedLineupCount != null && builtLineupCount > 0
      ? `Built ${builtLineupCount} of ${lastRequestedLineupCount} lineups so far.`
      : elapsedMs >= 60_000
        ? "Still solving. This is longer than expected and usually means the slate is highly constrained."
        : elapsedMs >= 20_000
          ? "Long solve in progress. Exposure, stacking, and diversity constraints can make the solver much slower."
          : "Optimizer job is queued or running on the server.";

  if (!isOptimizing) return null;

  return (
    <div className="mt-2 rounded border border-blue-200 bg-blue-50 p-3 text-sm text-blue-900">
      <p className="font-medium">Optimizer running: {fmtDuration(elapsedMs)}</p>
      {optimizeStatusText && <p className="mt-1 text-xs text-blue-800">{optimizeStatusText}</p>}
    </div>
  );
});

const RuleControlsSection = memo(function RuleControlsSection({
  sport,
  filteredTeams,
  blockedTeamSet,
  requiredTeamStackMap,
  teamOddsById,
  hideOutInactivePlayers,
  unavailablePlayerCount,
  lockedCount,
  blockedCount,
  requiredTeamStackCount,
  onToggleHideOutInactive,
  onClearLocks,
  onClearBlocks,
  onClearTeamRules,
  onToggleTeamBlock,
  onUpdateTeamStackRule,
}: RuleControlsSectionProps) {
  return (
    <div className="rounded-lg border bg-card p-4">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <h2 className="text-sm font-semibold">{sport.toUpperCase()} Rule Controls</h2>
          <p className="text-xs text-gray-500">
            Locks persist per slate. Blocked teams remove every player from that team, and team stack rules require at least one lineup stack on each selected team.
          </p>
        </div>
        <div className="flex flex-wrap gap-2">
          <button
            type="button"
            onClick={onToggleHideOutInactive}
            disabled={unavailablePlayerCount === 0}
            className={`rounded border px-3 py-1 text-xs font-medium ${
              hideOutInactivePlayers
                ? "border-blue-300 bg-blue-50 text-blue-700"
                : "text-gray-700 hover:bg-gray-50"
            } disabled:cursor-not-allowed disabled:opacity-50`}
          >
            {hideOutInactivePlayers ? "Show Out/Inactive" : "Hide Out/Inactive"}
          </button>
          <button
            onClick={onClearLocks}
            disabled={lockedCount === 0}
            className="rounded border px-3 py-1 text-xs text-blue-700 hover:bg-blue-50 disabled:opacity-50"
          >
            Clear Locks
          </button>
          <button
            onClick={onClearBlocks}
            disabled={blockedCount === 0}
            className="rounded border px-3 py-1 text-xs text-red-700 hover:bg-red-50 disabled:opacity-50"
          >
            Clear Blocks
          </button>
          <button
            onClick={onClearTeamRules}
            disabled={requiredTeamStackCount === 0}
            className="rounded border px-3 py-1 text-xs text-emerald-700 hover:bg-emerald-50 disabled:opacity-50"
          >
            Clear Team Rules
          </button>
        </div>
      </div>
      <div className="mt-3 flex flex-wrap gap-2 text-xs">
        <span className="rounded-full bg-blue-50 px-2 py-0.5 text-blue-700">{lockedCount} locked</span>
        <span className="rounded-full bg-red-50 px-2 py-0.5 text-red-700">{blockedCount} blocked rules</span>
        <span className="rounded-full bg-emerald-50 px-2 py-0.5 text-emerald-700">{requiredTeamStackCount} team stacks</span>
      </div>
      <div className="mt-3 grid gap-2 md:grid-cols-2 xl:grid-cols-4">
        {filteredTeams.length === 0 && (
          <div className="rounded border border-dashed p-4 text-sm text-gray-500 md:col-span-2 xl:col-span-4">
            No active teams are visible with the current filter.
          </div>
        )}
        {filteredTeams.map((team) => {
          const isBlocked = blockedTeamSet.has(team.teamId);
          const stackSize = requiredTeamStackMap.get(team.teamId);
          const odds = teamOddsById.get(team.teamId);
          return (
            <div
              key={team.teamId}
              className={`rounded border p-3 ${
                isBlocked
                  ? "border-red-200 bg-red-50"
                  : stackSize
                    ? "border-emerald-200 bg-emerald-50"
                    : "bg-white"
              }`}
            >
              <div className="flex items-center gap-2">
                {team.teamLogo && <img src={team.teamLogo} alt="" className="h-5 w-5" />}
                <div>
                  <p className="text-sm font-medium">{team.teamAbbrev}</p>
                  <p className="text-[11px] text-gray-500">{team.teamName ?? team.teamAbbrev}</p>
                </div>
              </div>
              {(odds?.teamTotal != null || odds?.vegasTotal != null || odds?.moneyline != null) && (
                <div className="mt-2 flex flex-wrap gap-1 text-[10px] text-gray-600">
                  {odds?.teamTotal != null && (
                    <span className="rounded border border-gray-200 bg-gray-50 px-1.5 py-0.5">
                      TT {odds.teamTotal.toFixed(1)}
                    </span>
                  )}
                  {odds?.vegasTotal != null && (
                    <span className="rounded border border-gray-200 bg-gray-50 px-1.5 py-0.5">
                      O/U {odds.vegasTotal.toFixed(1)}
                    </span>
                  )}
                  {odds?.moneyline != null && (
                    <span className="rounded border border-gray-200 bg-gray-50 px-1.5 py-0.5">
                      ML {fmtAmericanOdds(odds.moneyline)}
                    </span>
                  )}
                </div>
              )}
              <div className="mt-3 flex items-center gap-2">
                <button
                  onClick={() => onToggleTeamBlock(team.teamId)}
                  className={`rounded border px-2 py-1 text-xs ${
                    isBlocked ? "border-red-300 bg-red-100 text-red-700" : "text-gray-600 hover:bg-gray-50"
                  }`}
                >
                  {isBlocked ? "Blocked" : "Block Team"}
                </button>
                <select
                  value={stackSize ?? ""}
                  onChange={(e) => onUpdateTeamStackRule(team.teamId, e.target.value)}
                  disabled={isBlocked}
                  className="rounded border px-2 py-1 text-xs disabled:opacity-50"
                >
                  <option value="">No Stack</option>
                  <option value="2">Stack 2</option>
                  <option value="3">Stack 3</option>
                  <option value="4">Stack 4</option>
                  <option value="5">Stack 5</option>
                </select>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
});

const PlayerPoolTable = memo(function PlayerPoolTable({
  sport,
  visiblePlayers,
  playerPoolSourceCount,
  unavailablePlayerCount,
  hideOutInactivePlayers,
  mlbLineupSummary,
  mlbPlayerPoolFilter,
  onChangeMlbPlayerPoolFilter,
  onToggleHideOutInactive,
  supportsRuleControls,
  sortCol,
  sortDir,
  onToggleSort,
  lockedPlayerSet,
  blockedPlayerSet,
  blockedTeamSet,
  requiredTeamStackMap,
  nbaTopScorerRanks,
  nbaCeilingBadges,
  mlbPitcherDecisionBadges,
  mlbPitcherCeilingBadges,
  manuallyOutSet,
  manuallyOutAdjustments,
  onTogglePlayerLock,
  onTogglePlayerBlock,
  onToggleManuallyOut,
}: PlayerPoolTableProps) {
  const scrollContainerRef = useRef<HTMLDivElement>(null);
  const [scrollTop, setScrollTop] = useState(0);
  const rowEstimate = sport === "mlb" ? 74 : 52;
  const visibleCount = Math.ceil(PLAYER_POOL_VIEWPORT_HEIGHT_PX / rowEstimate) + PLAYER_POOL_OVERSCAN_ROWS * 2;
  const startIndex = Math.max(0, Math.floor(scrollTop / rowEstimate) - PLAYER_POOL_OVERSCAN_ROWS);
  const endIndex = Math.min(visiblePlayers.length, startIndex + visibleCount);
  const windowedPlayers = useMemo(
    () => visiblePlayers.slice(startIndex, endIndex),
    [endIndex, startIndex, visiblePlayers],
  );
  const topSpacerHeight = startIndex * rowEstimate;
  const bottomSpacerHeight = Math.max(0, (visiblePlayers.length - endIndex) * rowEstimate);
  const columnCount = sport === "mlb"
    ? (supportsRuleControls ? 18 : 17)
    : (supportsRuleControls ? 18 : 17);

  useEffect(() => {
    setScrollTop(0);
    if (scrollContainerRef.current) {
      scrollContainerRef.current.scrollTop = 0;
    }
  }, [sortCol, sortDir, sport, visiblePlayers]);

  const handleScroll = useCallback((event: UIEvent<HTMLDivElement>) => {
    setScrollTop(event.currentTarget.scrollTop);
  }, []);

  return (
    <div className="rounded-lg border bg-card overflow-hidden">
      <div className="flex items-start justify-between gap-3 px-4 py-3 border-b">
        <div>
          <h2 className="text-sm font-semibold">
            Player Pool - {hideOutInactivePlayers ? `${visiblePlayers.length} of ${playerPoolSourceCount}` : playerPoolSourceCount} players
            {unavailablePlayerCount > 0 && (
              <span className="ml-2 text-xs text-red-500">({unavailablePlayerCount} OUT)</span>
            )}
          </h2>
          {sport === "mlb" && (
            <div className="mt-2 flex flex-wrap gap-2 text-[11px] text-gray-600">
              <span className="rounded border border-blue-200 bg-blue-50 px-1.5 py-0.5 text-blue-700">
                {mlbLineupSummary.confirmedIn} confirmed hitters
              </span>
              <span className="rounded border border-amber-200 bg-amber-50 px-1.5 py-0.5 text-amber-700">
                {mlbLineupSummary.pending} pending
              </span>
              <span className="rounded border border-red-200 bg-red-50 px-1.5 py-0.5 text-red-700">
                {mlbLineupSummary.confirmedOut} out of lineup
              </span>
            </div>
          )}
          {sport === "mlb" && (
            <p className="mt-2 text-[11px] text-gray-600">
              Pitcher badges: <span className="font-semibold text-emerald-700">SP1</span> = strongest overall fit,
              {" "}
              <span className="font-semibold text-sky-700">SP2</span> = strong secondary fit,
              {" "}
              <span className="font-semibold text-amber-700">PIVOT</span> = best lower-owned lane,
              {" "}
              <span className="font-semibold text-fuchsia-700">CEIL</span> = top ceiling score.
            </p>
          )}
        </div>
        <div className="flex flex-col items-end gap-2">
          {sport === "mlb" && (
            <div className="flex flex-wrap justify-end gap-1">
              {MLB_PLAYER_POOL_FILTER_OPTIONS.map((option) => (
                <button
                  key={option.value}
                  type="button"
                  onClick={() => onChangeMlbPlayerPoolFilter(option.value)}
                  className={`rounded border px-2 py-1 text-[11px] font-medium ${
                    mlbPlayerPoolFilter === option.value
                      ? "border-slate-300 bg-slate-700 text-white"
                      : "border-gray-300 text-gray-700 hover:bg-gray-50"
                  }`}
                >
                  {option.label}
                </button>
              ))}
            </div>
          )}
          <button
            type="button"
            onClick={onToggleHideOutInactive}
            disabled={unavailablePlayerCount === 0}
            className={`rounded border px-3 py-1.5 text-xs font-medium ${
              hideOutInactivePlayers
                ? "border-blue-300 bg-blue-50 text-blue-700"
                : "border-gray-300 text-gray-700 hover:bg-gray-50"
            } disabled:cursor-not-allowed disabled:opacity-50`}
          >
            {hideOutInactivePlayers ? "Show Out/Inactive" : "Hide Out/Inactive"}
          </button>
        </div>
      </div>
      <div
        ref={scrollContainerRef}
        className="overflow-auto"
        style={{ maxHeight: `${PLAYER_POOL_VIEWPORT_HEIGHT_PX}px` }}
        onScroll={handleScroll}
      >
        <table className="w-full text-sm">
          <thead className="sticky top-0 z-10 border-b bg-gray-50">
            <tr>
              <th className="px-3 py-2 text-left text-xs font-medium text-gray-500">Pos</th>
              <SortHeader col="name" label="Player" sortCol={sortCol} sortDir={sortDir} onToggleSort={onToggleSort} />
              <th className="px-3 py-2 text-left text-xs font-medium text-gray-500">Team</th>
              {sport === "mlb" && <th className="px-3 py-2 text-left text-xs font-medium text-gray-500">Lineup</th>}
              {sport === "mlb" && <th className="px-3 py-2 text-left text-xs font-medium text-gray-500">Odds</th>}
              <th className="px-3 py-2 text-left text-xs font-medium text-gray-500">Props</th>
              {sport === "mlb" && <SortHeader col="hrProb" label="HR%" sortCol={sortCol} sortDir={sortDir} onToggleSort={onToggleSort} />}
              {supportsRuleControls && <th className="px-3 py-2 text-left text-xs font-medium text-gray-500">Rules</th>}
              <SortHeader col="salary" label="Salary" sortCol={sortCol} sortDir={sortDir} onToggleSort={onToggleSort} />
              <SortHeader col="avgFptsDk" label="DK Proj" sortCol={sortCol} sortDir={sortDir} onToggleSort={onToggleSort} />
              <SortHeader col="linestarProj" label="LS Proj" sortCol={sortCol} sortDir={sortDir} onToggleSort={onToggleSort} />
              {sport === "nba" && (
                <>
                  <th className="px-3 py-2 text-left text-xs font-medium text-gray-500">Model</th>
                  <th className="px-3 py-2 text-left text-xs font-medium text-gray-500">Props Mkt</th>
                </>
              )}
              <SortHeader
                col="ourProj"
                label={sport === "nba" ? "Live Proj" : "Our Proj"}
                sortCol={sortCol}
                sortDir={sortDir}
                onToggleSort={onToggleSort}
              />
              <SortHeader
                col="delta"
                label={sport === "nba" ? "Live Δ" : "Delta"}
                sortCol={sortCol}
                sortDir={sortDir}
                onToggleSort={onToggleSort}
              />
              <SortHeader col="linestarOwnPct" label="LS Own%" sortCol={sortCol} sortDir={sortDir} onToggleSort={onToggleSort} />
              <SortHeader col="projOwnPct" label="Field Own%" sortCol={sortCol} sortDir={sortDir} onToggleSort={onToggleSort} />
              <SortHeader col="ourOwnPct" label="Our Own%" sortCol={sortCol} sortDir={sortDir} onToggleSort={onToggleSort} />
              {sport === "nba" && <SortHeader col="liveOwnPct" label="Live Own%" sortCol={sortCol} sortDir={sortDir} onToggleSort={onToggleSort} />}
              <SortHeader
                col="ourLeverage"
                label={sport === "nba" ? "Live Lev" : "Leverage"}
                sortCol={sortCol}
                sortDir={sortDir}
                onToggleSort={onToggleSort}
              />
              <SortHeader col="value" label="Value" sortCol={sortCol} sortDir={sortDir} onToggleSort={onToggleSort} />
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-100">
            {visiblePlayers.length === 0 && (
              <tr>
                <td colSpan={columnCount} className="px-3 py-6 text-center text-sm text-gray-500">
                  No active players are visible with the current filter.
                </td>
              </tr>
            )}
            {topSpacerHeight > 0 && (
              <tr aria-hidden="true">
                <td colSpan={columnCount} style={{ height: `${topSpacerHeight}px`, padding: 0, border: 0 }} />
              </tr>
            )}
            {windowedPlayers.map((p) => {
              const ourProjDisplay = sport === "nba" ? (p.modelProj ?? p.ourProj) : p.ourProj;
              const liveProjDisplay = sport === "nba"
                ? (p.liveProj ?? p.blendProj ?? p.ourProj)
                : p.ourProj;
              const liveOwnDisplay = sport === "nba"
                ? (p.liveOwnPct ?? p.projOwnPct ?? p.ourOwnPct)
                : p.ourOwnPct;
              const leverageDisplay = sport === "nba"
                ? (p.liveLeverage ?? p.ourLeverage)
                : p.ourLeverage;
              const isManuallyOut = manuallyOutSet.has(p.id);
              const projBonus = manuallyOutAdjustments.get(p.id) ?? 0;
              const effectiveLiveProj = isManuallyOut ? 0 : (liveProjDisplay != null ? liveProjDisplay + projBonus : null);
              const delta = effectiveLiveProj != null && p.linestarProj != null ? effectiveLiveProj - p.linestarProj : null;
              const value = effectiveLiveProj != null ? effectiveLiveProj / (p.salary / 1000) : null;
              const propTokens = getPlayerPropTokens(p, sport);
              const odds = getPlayerOddsContext(p);
              const hrMarketProb = sport === "mlb" ? getPlayerHrMarketProb(p) : null;
              const hrEdgePct = sport === "mlb" ? getPlayerHrEdgePct(p) : null;
              const pos = displayPos(p.eligiblePositions, sport);
              const mlbLineupBadge = sport === "mlb" ? getMlbLineupBadge(p) : null;
              const mlbPitcherDecisionBadge = sport === "mlb" ? mlbPitcherDecisionBadges.get(p.id) : null;
              const mlbPitcherCeilingBadge = sport === "mlb" ? mlbPitcherCeilingBadges.get(p.id) : null;
              const mlbHrBadge = sport === "mlb" ? getMlbHrBadge(p) : null;
              const mlbOrderBadge = sport === "mlb" ? getMlbOrderBadge(p) : null;
              const nbaPointsBadge = sport === "nba" ? getNbaPointsBadge(p, nbaTopScorerRanks.get(p.id)) : null;
              const nbaCeilingBadge = sport === "nba" ? nbaCeilingBadges.get(p.id) : null;
              const rowUnavailable = sport === "mlb" ? isMlbRowUnavailable(p) : !!p.isOut;
              const isLocked = lockedPlayerSet.has(p.id);
              const isBlocked = blockedPlayerSet.has(p.id);
              const isTeamBlocked = p.teamId != null && blockedTeamSet.has(p.teamId);
              const stackSize = p.teamId != null ? requiredTeamStackMap.get(p.teamId) : undefined;
              return (
                <tr
                  key={p.id}
                  className={`hover:bg-gray-50 ${
                    rowUnavailable || isManuallyOut ? "opacity-40 line-through" : ""
                  } ${
                    isLocked ? "bg-blue-50" : isBlocked || isTeamBlocked ? "bg-red-50" : isManuallyOut ? "bg-orange-50" : ""
                  }`}
                >
                  <td className="px-3 py-1.5">
                    <span className={`inline-block rounded px-1.5 py-0.5 text-xs font-medium ${posBadgeColor(p.eligiblePositions, sport)}`}>
                      {pos}
                    </span>
                  </td>
                  <td className="px-3 py-1.5 font-medium">
                    {p.teamLogo && <img src={p.teamLogo} alt="" className="inline-block mr-1.5 h-4 w-4 align-middle" />}
                    {p.name}
                    {supportsRuleControls && (
                      <span className="ml-2 inline-flex flex-wrap gap-1 align-middle">
                        {isLocked && <span className="rounded bg-blue-100 px-1.5 py-0.5 text-[10px] font-medium text-blue-700">LOCK</span>}
                        {(isBlocked || isTeamBlocked) && <span className="rounded bg-red-100 px-1.5 py-0.5 text-[10px] font-medium text-red-700">BLOCK</span>}
                        {isManuallyOut && <span className="rounded bg-orange-200 px-1.5 py-0.5 text-[10px] font-medium text-orange-800">M-OUT</span>}
                        {!isManuallyOut && projBonus > 0 && <span title={`+${projBonus.toFixed(1)} from teammate scratch`} className="rounded bg-emerald-100 px-1.5 py-0.5 text-[10px] font-medium text-emerald-700">+{projBonus.toFixed(1)}</span>}
                        {!rowUnavailable && !isManuallyOut && (p.dkStatus === "Q" || p.dkStatus === "GTD") && (
                          <span title={`DK status: ${p.dkStatus} — no prop lines posted; projection floored at 75% of DK avg`} className="rounded bg-yellow-100 px-1.5 py-0.5 text-[10px] font-medium text-yellow-800">{p.dkStatus}</span>
                        )}
                        {stackSize != null && <span className="rounded bg-emerald-100 px-1.5 py-0.5 text-[10px] font-medium text-emerald-700">STACK {stackSize}</span>}
                        {mlbPitcherCeilingBadge && (
                          <span title={mlbPitcherCeilingBadge.title} className={`rounded px-1.5 py-0.5 text-[10px] font-medium ${mlbPitcherCeilingBadge.className}`}>
                            {mlbPitcherCeilingBadge.label}
                          </span>
                        )}
                        {mlbPitcherDecisionBadge && (
                          <span title={mlbPitcherDecisionBadge.title} className={`rounded px-1.5 py-0.5 text-[10px] font-medium ${mlbPitcherDecisionBadge.className}`}>
                            {mlbPitcherDecisionBadge.label}
                          </span>
                        )}
                        {nbaPointsBadge && (
                          <span title={nbaPointsBadge.title} className={`rounded px-1.5 py-0.5 text-[10px] font-medium ${nbaPointsBadge.className}`}>
                            {nbaPointsBadge.label}
                          </span>
                        )}
                        {nbaCeilingBadge && (
                          <span title={nbaCeilingBadge.title} className={`rounded px-1.5 py-0.5 text-[10px] font-medium ${nbaCeilingBadge.className}`}>
                            {nbaCeilingBadge.label}
                          </span>
                        )}
                        {mlbHrBadge && (
                          <span title={mlbHrBadge.title} className={`rounded px-1.5 py-0.5 text-[10px] font-medium ${mlbHrBadge.className}`}>
                            {mlbHrBadge.label}
                          </span>
                        )}
                        {mlbOrderBadge && (
                          <span title={mlbOrderBadge.title} className={`rounded px-1.5 py-0.5 text-[10px] font-medium ${mlbOrderBadge.className}`}>
                            {mlbOrderBadge.label}
                          </span>
                        )}
                      </span>
                    )}
                    {!supportsRuleControls && (
                      <>
                        {mlbPitcherCeilingBadge && (
                          <span title={mlbPitcherCeilingBadge.title} className={`ml-2 inline-flex rounded px-1.5 py-0.5 text-[10px] font-medium align-middle ${mlbPitcherCeilingBadge.className}`}>
                            {mlbPitcherCeilingBadge.label}
                          </span>
                        )}
                        {mlbPitcherDecisionBadge && (
                          <span title={mlbPitcherDecisionBadge.title} className={`ml-2 inline-flex rounded px-1.5 py-0.5 text-[10px] font-medium align-middle ${mlbPitcherDecisionBadge.className}`}>
                            {mlbPitcherDecisionBadge.label}
                          </span>
                        )}
                        {nbaPointsBadge && (
                          <span title={nbaPointsBadge.title} className={`ml-2 inline-flex rounded px-1.5 py-0.5 text-[10px] font-medium align-middle ${nbaPointsBadge.className}`}>
                            {nbaPointsBadge.label}
                          </span>
                        )}
                        {nbaCeilingBadge && (
                          <span title={nbaCeilingBadge.title} className={`ml-2 inline-flex rounded px-1.5 py-0.5 text-[10px] font-medium align-middle ${nbaCeilingBadge.className}`}>
                            {nbaCeilingBadge.label}
                          </span>
                        )}
                        {mlbHrBadge && (
                          <span title={mlbHrBadge.title} className={`ml-2 inline-flex rounded px-1.5 py-0.5 text-[10px] font-medium align-middle ${mlbHrBadge.className}`}>
                            {mlbHrBadge.label}
                          </span>
                        )}
                        {mlbOrderBadge && (
                          <span title={mlbOrderBadge.title} className={`ml-2 inline-flex rounded px-1.5 py-0.5 text-[10px] font-medium align-middle ${mlbOrderBadge.className}`}>
                            {mlbOrderBadge.label}
                          </span>
                        )}
                      </>
                    )}
                  </td>
                  <td className="px-3 py-1.5 text-xs text-gray-500">{p.teamAbbrev}</td>
                  {sport === "mlb" && (
                    <td className="px-3 py-1.5 text-xs">
                      {mlbLineupBadge && (
                        <span className={`inline-flex rounded border px-1.5 py-0.5 text-[10px] font-medium ${mlbLineupBadge.className}`}>
                          {mlbLineupBadge.label}
                        </span>
                      )}
                    </td>
                  )}
                  {sport === "mlb" && (
                    <td className="px-3 py-1.5 text-[11px] text-gray-500">
                      {(odds.teamTotal != null || odds.vegasTotal != null || odds.moneyline != null) ? (
                        <div className="flex max-w-[200px] flex-wrap gap-1">
                          {odds.teamTotal != null && (
                            <span className="rounded border border-gray-200 bg-gray-50 px-1.5 py-0.5 font-mono text-[10px] text-gray-600">
                              TT {odds.teamTotal.toFixed(1)}
                            </span>
                          )}
                          {odds.vegasTotal != null && (
                            <span className="rounded border border-gray-200 bg-gray-50 px-1.5 py-0.5 font-mono text-[10px] text-gray-600">
                              O/U {odds.vegasTotal.toFixed(1)}
                            </span>
                          )}
                          {odds.moneyline != null && (
                            <span className="rounded border border-gray-200 bg-gray-50 px-1.5 py-0.5 font-mono text-[10px] text-gray-600">
                              ML {fmtAmericanOdds(odds.moneyline)}
                            </span>
                          )}
                        </div>
                      ) : (
                        <span className="text-gray-400">—</span>
                      )}
                    </td>
                  )}
                  <td className="px-3 py-1.5 text-[11px] text-gray-500">
                    {propTokens.length > 0 ? (
                      <div className="flex max-w-[240px] gap-1 overflow-x-auto whitespace-nowrap [scrollbar-width:thin]">
                        {propTokens.map((prop) => (
                          <span
                            key={`${p.id}-${prop.stat}`}
                            className="shrink-0 rounded border border-gray-200 bg-gray-50 px-1.5 py-0.5 font-mono text-[10px] text-gray-600"
                          >
                            {prop.stat} {prop.line.toFixed(1)}
                            {prop.price != null ? ` (${fmtAmericanOdds(prop.price)})` : ""}
                            {prop.book ? ` ${shortBookName(prop.book)}` : ""}
                          </span>
                        ))}
                      </div>
                    ) : (
                      <span className="text-gray-400">—</span>
                    )}
                  </td>
                  {sport === "mlb" && (
                    <td className="px-3 py-1.5 text-right text-[11px]">
                      {!isMlbPitcherPlayer(p) && p.hrProb1Plus != null ? (
                        <div className="space-y-0.5">
                          <div className="font-semibold text-rose-700">{(p.hrProb1Plus * 100).toFixed(1)}%</div>
                          <div className={hrEdgePct == null ? "text-gray-400" : hrEdgePct >= 0 ? "text-emerald-700" : "text-red-500"}>
                            {fmtSignedPctPoint(hrEdgePct)}
                          </div>
                          <div className="text-gray-400">
                            Exp {p.expectedHr != null ? p.expectedHr.toFixed(2) : "—"}
                            {hrMarketProb != null ? ` | Mkt ${(hrMarketProb * 100).toFixed(1)}%` : ""}
                          </div>
                        </div>
                      ) : (
                        <span className="text-gray-400">—</span>
                      )}
                    </td>
                  )}
                  {supportsRuleControls && (
                    <td className="px-3 py-1.5 text-xs">
                      <div className="flex flex-wrap gap-1">
                        <button
                          onClick={() => onTogglePlayerLock(p)}
                          disabled={isTeamBlocked && !isLocked}
                          className={`rounded border px-2 py-0.5 ${
                            isLocked
                              ? "border-blue-300 bg-blue-100 text-blue-700"
                              : "text-gray-600 hover:bg-gray-50"
                          } disabled:cursor-not-allowed disabled:opacity-50`}
                        >
                          {isLocked ? "Locked" : "Lock"}
                        </button>
                        <button
                          onClick={() => onTogglePlayerBlock(p)}
                          className={`rounded border px-2 py-0.5 ${
                            isBlocked
                              ? "border-red-300 bg-red-100 text-red-700"
                              : "text-gray-600 hover:bg-gray-50"
                          }`}
                        >
                          {isBlocked ? "Blocked" : "Block"}
                        </button>
                        {sport === "nba" && !rowUnavailable && (
                          <button
                            onClick={() => onToggleManuallyOut(p)}
                            title={isManuallyOut ? "Remove manual scratch" : "Treat as OUT — redistributes 70% of projected FPTS to teammates"}
                            className={`rounded border px-2 py-0.5 ${
                              isManuallyOut
                                ? "border-orange-300 bg-orange-100 text-orange-700"
                                : "text-gray-400 hover:bg-gray-50"
                            }`}
                          >
                            {isManuallyOut ? "M-OUT" : "OUT?"}
                          </button>
                        )}
                      </div>
                    </td>
                  )}
                  <td className="px-3 py-1.5 font-mono text-xs">{fmtSalary(p.salary)}</td>
                  <td className="px-3 py-1.5 text-xs text-gray-500">{fmt1(p.avgFptsDk)}</td>
                  <td className="px-3 py-1.5 text-xs">{fmt1(p.linestarProj)}</td>
                  {sport === "nba" && (
                    <>
                      <td className="px-3 py-1.5 text-xs">{fmt1(ourProjDisplay)}</td>
                      <td className="px-3 py-1.5 text-xs">{propTokens.length > 0 ? fmt1(p.marketProj) : "—"}</td>
                    </>
                  )}
                  <td className="px-3 py-1.5 text-xs font-medium">
                    {fmt1(effectiveLiveProj)}
                    {projBonus > 0 && !isManuallyOut && (
                      <span className="ml-1 text-[10px] text-emerald-600">+{projBonus.toFixed(1)}</span>
                    )}
                  </td>
                  <td className={`px-3 py-1.5 text-xs font-medium ${
                    delta == null ? "text-gray-400" : delta >= 2 ? "text-green-600" : delta <= -2 ? "text-red-500" : ""
                  }`}>
                    {delta != null ? `${delta >= 0 ? "+" : ""}${delta.toFixed(1)}` : "—"}
                  </td>
                  <td className="px-3 py-1.5 text-xs">{(p.linestarOwnPct ?? p.projOwnPct) != null ? `${(p.linestarOwnPct ?? p.projOwnPct)!.toFixed(1)}%` : "—"}</td>
                  <td className="px-3 py-1.5 text-xs">{p.projOwnPct != null ? `${p.projOwnPct.toFixed(1)}%` : "—"}</td>
                  <td className="px-3 py-1.5 text-xs">{p.ourOwnPct != null ? `${p.ourOwnPct.toFixed(1)}%` : "—"}</td>
                  {sport === "nba" && (
                    <td className="px-3 py-1.5 text-xs">{liveOwnDisplay != null ? `${liveOwnDisplay.toFixed(1)}%` : "—"}</td>
                  )}
                  <td className={`px-3 py-1.5 text-xs font-medium ${
                    leverageDisplay == null ? "" : leverageDisplay > 0 ? "text-green-700" : "text-red-400"
                  }`}>
                    {fmt1(leverageDisplay)}
                  </td>
                  <td className="px-3 py-1.5 text-xs text-gray-500">{value != null ? value.toFixed(2) : "—"}</td>
                </tr>
              );
            })}
            {bottomSpacerHeight > 0 && (
              <tr aria-hidden="true">
                <td colSpan={columnCount} style={{ height: `${bottomSpacerHeight}px`, padding: 0, border: 0 }} />
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
});

const GeneratedLineupsSection = memo(function GeneratedLineupsSection({
  sport,
  activeLineups,
  lastRequestedLineupCount,
  strategy,
  onStrategyChange,
  onSave,
  saveMsg,
  onExport,
  isExporting,
  exportError,
}: GeneratedLineupsSectionProps) {
  const slotNames = sport === "nba" ? NBA_SLOT_NAMES : MLB_SLOT_NAMES;
  const [showCheapDebug, setShowCheapDebug] = useState(false);
  const hasCheapDebug = sport === "nba" && activeLineups.some((lineup) => (
    "cheapPlayerDebug" in lineup
    && Array.isArray(lineup.cheapPlayerDebug)
    && lineup.cheapPlayerDebug.length > 0
  ));

  return (
    <div className="rounded-lg border bg-card p-4">
      <div className="flex items-center justify-between mb-3">
        <h2 className="text-sm font-semibold">
          {activeLineups.length}
          {lastRequestedLineupCount != null ? ` / ${lastRequestedLineupCount}` : ""}
          {" "}Lineups Generated
        </h2>
        <div className="flex items-center gap-3">
          {hasCheapDebug && (
            <button
              type="button"
              onClick={() => setShowCheapDebug((value) => !value)}
              className={`rounded border px-3 py-1 text-xs ${
                showCheapDebug
                  ? "border-amber-600 bg-amber-50 text-amber-700"
                  : "border-gray-300 text-gray-600 hover:bg-gray-50"
              }`}
            >
              {showCheapDebug ? "Hide Cheap Debug" : "Show Cheap Debug"}
            </button>
          )}
          <div className="flex items-center gap-2">
            <label className="text-xs text-gray-500">Strategy name</label>
            <input
              value={strategy}
              onChange={(e) => onStrategyChange(e.target.value)}
              className="w-28 rounded border px-2 py-1 text-xs"
            />
          </div>
          <button
            onClick={onSave}
            className="rounded border border-blue-600 px-3 py-1 text-xs text-blue-600 hover:bg-blue-50"
          >
            Save Lineups
          </button>
        </div>
      </div>
      {saveMsg && <p className="mb-2 text-xs text-green-600">{saveMsg}</p>}

      <div className="space-y-2 max-h-[600px] overflow-y-auto pr-1">
        {activeLineups.map((lineup, i) => {
          const slots = lineup.slots as Record<string, { id: number; name: string; salary: number; teamLogo: string | null; ourProj: number | null }>;
          const cheapDebug = "cheapPlayerDebug" in lineup && Array.isArray(lineup.cheapPlayerDebug)
            ? new Map(lineup.cheapPlayerDebug.map((entry) => [entry.playerId, entry.reasons]))
            : new Map<number, string[]>();
          return (
            <div key={i} className="rounded border p-2 text-xs">
              <div className="flex items-center gap-4 mb-1 text-gray-500">
                <span className="font-medium text-gray-800">#{i + 1}</span>
                <span>Proj: <strong className="text-gray-800">{lineup.projFpts.toFixed(1)}</strong></span>
                <span>Sal: <strong className="text-gray-800">${lineup.totalSalary.toLocaleString()}</strong></span>
                <span>Lev: <strong className="text-gray-800">{lineup.leverageScore.toFixed(1)}</strong></span>
              </div>
              <div className="flex flex-wrap gap-1">
                {slotNames.map((slot) => {
                  const player = slots[slot];
                  const cheapReasons = player ? cheapDebug.get(player.id) ?? [] : [];
                  return player ? (
                    <span
                      key={slot}
                      className={`inline-flex items-center gap-1 rounded px-1.5 py-0.5 ${
                        cheapReasons.length > 0 && showCheapDebug
                          ? "bg-amber-50"
                          : "bg-gray-100"
                      }`}
                      title={cheapReasons.length > 0 ? cheapReasons.join(" | ") : undefined}
                    >
                      <span className="text-gray-400">{slot.replace(/\d$/, "")}</span>
                      {player.teamLogo && <img src={player.teamLogo} alt="" className="h-3 w-3" />}
                      <span className="font-medium">{player.name}</span>
                      <span className="font-mono text-gray-500">{fmtSalary(player.salary)}</span>
                      <span className="text-gray-400">{fmt1(player.ourProj)}</span>
                      {cheapReasons.length > 0 && showCheapDebug && (
                        <span className="rounded bg-amber-100 px-1 text-[10px] font-medium text-amber-700">
                          {player.salary < 3300 ? "Min-price OK" : "Cheap OK"}
                        </span>
                      )}
                    </span>
                  ) : null;
                })}
              </div>
              {showCheapDebug && cheapDebug.size > 0 && (
                <div className="mt-2 space-y-1 text-[11px] text-amber-800">
                  {lineup.players
                    .filter((player) => cheapDebug.has(player.id))
                    .map((player) => (
                      <div key={player.id} className="rounded bg-amber-50 px-2 py-1">
                        <span className="font-medium">{player.name}</span>
                        {" "}
                        <span className="text-amber-700">({fmtSalary(player.salary)})</span>
                        {": "}
                        {cheapDebug.get(player.id)?.join(", ")}
                      </div>
                    ))}
                </div>
              )}
            </div>
          );
        })}
      </div>

      <div className="mt-4 pt-4 border-t">
        <h3 className="text-xs font-semibold mb-2">Multi-Entry Export</h3>
        <p className="text-xs text-gray-500 mb-2">
          Exports the generated lineups directly as CSV with one row per lineup.
        </p>
        <button
          onClick={onExport}
          disabled={isExporting}
          className="mt-2 rounded bg-blue-600 px-4 py-1.5 text-sm text-white hover:bg-blue-700 disabled:opacity-50"
        >
          {isExporting ? "Exporting…" : "Export CSV"}
        </button>
        {exportError && <p className="mt-2 text-xs text-red-600">{exportError}</p>}
      </div>
    </div>
  );
});

export default function DfsClient({ players, slateDate, mlbPitcherSignals, mlbGameCards, sport }: Props) {
  const [isPending, startTransition] = useTransition();

  // ── Load state ────────────────────────────────────────────
  const [uploadMsg, setUploadMsg] = useState<{ ok: boolean; text: string } | null>(null);
  const dkFileRef = useRef<HTMLInputElement>(null);
  const lsFileRef = useRef<HTMLInputElement>(null);
  const [contestId, setContestId] = useState("");
  const [cashLineInput, setCashLineInput] = useState("");
  const [loadMode, setLoadMode] = useState<"api" | "csv">("api");

  // ── Contest metadata ──────────────────────────────────────
  const [contestTiming, setContestTiming] = useState<DkSlateTiming>("main");
  const [fieldSizeInput, setFieldSizeInput] = useState("");
  const [contestFormat, setContestFormat] = useState<"gpp" | "cash">("gpp");

  // ── Game filter ───────────────────────────────────────────
  const allGames = useMemo(() => {
    const keys = new Set<string>();
    for (const p of players) keys.add(parseGameKey(p.gameInfo));
    return Array.from(keys).sort();
  }, [players]);
  const allGamesKey = useMemo(() => allGames.join("|"), [allGames]);

  const [selectedGames, setSelectedGames] = useState<Set<string>>(() => new Set(allGames));

  useEffect(() => {
    setSelectedGames(new Set(allGames));
  }, [allGamesKey]);

  // ── Sort ──────────────────────────────────────────────────
  const [sortCol, setSortCol] = useState<SortCol>("ourLeverage");
  const [sortDir, setSortDir] = useState<SortDir>("desc");

  // ── Optimizer settings ────────────────────────────────────
  const [mode, setMode] = useState<OptimizerMode>("gpp");
  const [nLineups, setNLineups] = useState(20);
  const [teamStackCount, setTeamStackCount] = useState(1);
  const [minStack, setMinStack] = useState(() => sport === "nba" ? 2 : 1);
  const [maxExposure, setMaxExposure] = useState(0.6);
  const [mlbBringBackThreshold, setMlbBringBackThreshold] = useState(3);
  const [pendingLineupPolicy, setPendingLineupPolicy] = useState<MlbPendingLineupPolicy>("downgrade");
  const [hideOutInactivePlayers, setHideOutInactivePlayers] = useState(true);
  const [mlbPlayerPoolFilter, setMlbPlayerPoolFilter] = useState<MlbPlayerPoolFilter>("all");
  const [showHeavyPanels, setShowHeavyPanels] = useState(false);
  const [bringBackEnabled, setBringBackEnabled] = useState(false);
  const [bringBackSize, setBringBackSize] = useState(1);
  const [minSalaryFilter, setMinSalaryFilter] = useState("");
  const [maxSalaryFilter, setMaxSalaryFilter] = useState("");
  const [strategy, setStrategy] = useState("gpp");
  const [lockedPlayerIds, setLockedPlayerIds] = useState<number[]>([]);
  const [blockedPlayerIds, setBlockedPlayerIds] = useState<number[]>([]);
  const [blockedTeamIds, setBlockedTeamIds] = useState<number[]>([]);
  const [requiredTeamStacks, setRequiredTeamStacks] = useState<TeamStackRule[]>([]);
  const [manuallyOutIds, setManuallyOutIds] = useState<number[]>([]);

  // ── Lineups ───────────────────────────────────────────────
  const [lineups,    setLineups]    = useState<GeneratedLineup[]    | null>(null);
  const [mlbLineups, setMlbLineups] = useState<MlbGeneratedLineup[] | null>(null);
  const [optimizerJobId, setOptimizerJobId] = useState<number | null>(null);
  const [optimizerClientToken, setOptimizerClientToken] = useState<string | null>(null);
  const [isOptimizing, setIsOptimizing] = useState(false);
  const [optimizeError, setOptimizeError] = useState<string | null>(null);
  const [optimizeWarning, setOptimizeWarning] = useState<string | null>(null);
  const [optimizeDebug, setOptimizeDebug] = useState<OptimizerDebugInfo | null>(null);
  const [optimizeStartedAt, setOptimizeStartedAt] = useState<number | null>(null);
  const [antiCorrMax, setAntiCorrMax] = useState(1); // MLB: max batters facing your own SP
  const [hrCorrelation, setHrCorrelation] = useState(false);
  const [hrCorrelationThreshold, setHrCorrelationThreshold] = useState(0.12);
  const [pitcherCeilingBoost, setPitcherCeilingBoost] = useState(false);
  const [pitcherCeilingCount, setPitcherCeilingCount] = useState(3);
  const [nbaCeilingBoost, setNbaCeilingBoost] = useState(false);
  const [nbaCeilingCount, setNbaCeilingCount] = useState(3);
  const [saveMsg, setSaveMsg] = useState<string | null>(null);
  const [lastRequestedLineupCount, setLastRequestedLineupCount] = useState<number | null>(null);

  // ── Export ────────────────────────────────────────────────
  const [isExporting, setIsExporting] = useState(false);
  const [exportError, setExportError] = useState<string | null>(null);
  const optimizerRenderStateRef = useRef<{
    metaKey: string;
    lineupSignature: string;
    debugSignature: string;
  }>({
    metaKey: "",
    lineupSignature: "",
    debugSignature: "",
  });

  // ── Results upload (disabled) ─────────────────────────────

  // ── Status refresh ────────────────────────────────────────
  const [refreshMsg, setRefreshMsg] = useState<{ ok: boolean; text: string } | null>(null);
  const [isRefreshing, setIsRefreshing] = useState(false);

  // ── Player props ──────────────────────────────────────────
  const [propsMsg, setPropsMsg] = useState<{ ok: boolean; text: string } | null>(null);
  const [isFetchingProps, setIsFetchingProps] = useState(false);
  const [propAudit, setPropAudit] = useState<NbaPropCoverageAuditResult | null>(null);
  const [mlbPropAudit, setMlbPropAudit] = useState<MlbPropCoverageAuditResult | null>(null);
  const [isAuditingProps, setIsAuditingProps] = useState(false);

  // ── Clear Slate ───────────────────────────────────────────
  const [clearSlateConfirm, setClearSlateConfirm] = useState(false);
  const [isClearingSlate, setIsClearingSlate] = useState(false);

  // ── Props elapsed timer ───────────────────────────────────

  // ── Recompute projections ─────────────────────────────────
  const [projMsg, setProjMsg] = useState<{ ok: boolean; text: string } | null>(null);
  const [isRecomputing, setIsRecomputing] = useState(false);

  // Auto-reset clear confirmation after 3s of inactivity
  useEffect(() => {
    if (!clearSlateConfirm) return;
    const id = setTimeout(() => setClearSlateConfirm(false), 3000);
    return () => clearTimeout(id);
  }, [clearSlateConfirm]);

  // ── LineStar input: CSV file or paste ────────────────────
  const [lsMode, setLsMode] = useState<"csv" | "paste">("paste");
  const lsUploadRef = useRef<HTMLInputElement>(null);
  const [lsUploadMsg, setLsUploadMsg] = useState<{ ok: boolean; text: string } | null>(null);
  const [isUploadingLs, setIsUploadingLs] = useState(false);
  const [lsPasteText, setLsPasteText] = useState("");

  // ── LineStar cookie status ────────────────────────────────
  const [cookieStatus, setCookieStatus] = useState<{ ok: boolean; message: string } | null>(null);
  const [isCheckingCookie, setIsCheckingCookie] = useState(false);

  const playersById = useMemo(
    () => new Map(players.map((player) => [player.id, player])),
    [players],
  );
  const currentSlateId = players[0]?.slateId ?? null;
  const supportsRuleControls = sport === "nba" || sport === "mlb";
  const lockedPlayerSet = useMemo(() => new Set(lockedPlayerIds), [lockedPlayerIds]);
  const blockedPlayerSet = useMemo(() => new Set(blockedPlayerIds), [blockedPlayerIds]);
  const blockedTeamSet = useMemo(() => new Set(blockedTeamIds), [blockedTeamIds]);
  const manuallyOutSet = useMemo(() => new Set(manuallyOutIds), [manuallyOutIds]);
  const manuallyOutAdjustments = useMemo(() => {
    if (sport !== "nba" || manuallyOutIds.length === 0) return new Map<number, number>();
    const bonusMap = new Map<number, number>();
    for (const outId of manuallyOutIds) {
      const outPlayer = playersById.get(outId);
      if (!outPlayer || outPlayer.teamId == null) continue;
      const outProj = outPlayer.liveProj ?? outPlayer.blendProj ?? outPlayer.ourProj ?? 0;
      const projToRedist = outProj * 0.7;
      if (projToRedist <= 0) continue;
      const teammates = players.filter(
        (p) => p.teamId === outPlayer.teamId && !p.isOut && !manuallyOutSet.has(p.id) && p.id !== outId,
      );
      if (teammates.length === 0) continue;
      const totalSalary = teammates.reduce((sum, p) => sum + p.salary, 0);
      if (totalSalary <= 0) continue;
      for (const teammate of teammates) {
        const bonus = projToRedist * (teammate.salary / totalSalary);
        bonusMap.set(teammate.id, (bonusMap.get(teammate.id) ?? 0) + bonus);
      }
    }
    return bonusMap;
  }, [manuallyOutIds, manuallyOutSet, players, playersById, sport]);
  const requiredTeamStackMap = useMemo(
    () => new Map(requiredTeamStacks.map((rule) => [rule.teamId, rule.stackSize])),
    [requiredTeamStacks],
  );
  const slowestLineup = optimizeDebug?.lineupSummaries.reduce<OptimizerDebugInfo["lineupSummaries"][number] | null>(
    (best, current) => !best || current.durationMs > best.durationMs ? current : best,
    null,
  ) ?? null;
  const debugTotalMs = optimizeDebug?.totalMs ?? 0;
  const heuristicRejectSummary = optimizeDebug?.heuristic
    ? Object.entries(optimizeDebug.heuristic.rejectedByReason)
        .filter(([, count]) => count > 0)
        .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
    : [];

  function applyOptimizerJobResult(result: OptimizerJobStatusResponse) {
    const startedAt = result.job.startedAt ?? result.job.createdAt;
    const startedAtMs = startedAt ? Date.parse(startedAt) : null;
    const isActiveJob = result.job.status === "queued" || result.job.status === "running";
    const lineupSignature = getPersistedLineupSignature(result.lineups);
    const debugSignature = getOptimizerDebugSignature(result.debug ?? null, result.job.status);
    const metaKey = [
      result.job.id,
      result.job.status,
      result.job.requestedLineups,
      result.job.warning ?? "",
      result.job.error ?? "",
      startedAt ?? "",
      result.job.finishedAt ?? "",
    ].join("|");

    const previous = optimizerRenderStateRef.current;
    const shouldUpdateMeta = previous.metaKey !== metaKey;
    const shouldUpdateLineups = previous.lineupSignature !== lineupSignature;
    const shouldUpdateDebug = previous.debugSignature !== debugSignature;

    setOptimizerJobId(result.job.id);
    window.localStorage.setItem(
      optimizerJobStorageKey(result.job.sport, result.job.slateId),
      String(result.job.id),
    );

    if (shouldUpdateMeta) {
      setLastRequestedLineupCount(result.job.requestedLineups);
      setOptimizeWarning(result.job.warning ?? null);
      setOptimizeError(result.job.error ?? null);
      setOptimizeStartedAt(startedAtMs);
    }
    setIsOptimizing(isActiveJob);

    if (shouldUpdateDebug || shouldUpdateLineups) {
      startTransition(() => {
        if (shouldUpdateDebug) {
          setOptimizeDebug(result.debug ?? null);
        }

        if (shouldUpdateLineups) {
          if (sport === "mlb") {
            setMlbLineups(buildMlbLineupsFromPersisted(result.lineups, playersById));
            setLineups(null);
          } else {
            setLineups(buildNbaLineupsFromPersisted(result.lineups, playersById));
            setMlbLineups(null);
          }
        }
      });
    }

    optimizerRenderStateRef.current = {
      metaKey,
      lineupSignature,
      debugSignature,
    };
  }

  useEffect(() => {
    setOptimizerClientToken(getOrCreateOptimizerClientToken());
  }, []);

  useEffect(() => {
    optimizerRenderStateRef.current.lineupSignature = "";
  }, [playersById]);

  useEffect(() => {
    if (!supportsRuleControls || !currentSlateId) {
      setLockedPlayerIds([]);
      setBlockedPlayerIds([]);
      setBlockedTeamIds([]);
      setRequiredTeamStacks([]);
      return;
    }

    const stored = readStoredRuleState(sport, currentSlateId);
    setLockedPlayerIds(stored.playerLocks);
    setBlockedPlayerIds(stored.playerBlocks);
    setBlockedTeamIds(stored.blockedTeamIds);
    setRequiredTeamStacks(stored.requiredTeamStacks);
  }, [currentSlateId, sport, supportsRuleControls]);

  useEffect(() => {
    if (!supportsRuleControls || !currentSlateId) return;
    const nextState: RuleState = {
      playerLocks: lockedPlayerIds,
      playerBlocks: blockedPlayerIds,
      blockedTeamIds,
      requiredTeamStacks,
    };
    window.localStorage.setItem(
      optimizerRuleStorageKey(sport, currentSlateId),
      JSON.stringify(nextState),
    );
  }, [blockedPlayerIds, blockedTeamIds, currentSlateId, lockedPlayerIds, requiredTeamStacks, sport, supportsRuleControls]);

  useEffect(() => {
    if (!optimizerClientToken || !currentSlateId) return;
    const clientToken = optimizerClientToken;
    const slateId = currentSlateId;

    let cancelled = false;
    async function hydrateActiveJob() {
      const params = new URLSearchParams({
        clientToken,
        sport,
        slateId: String(slateId),
      });

      const resp = await fetch(`/api/optimizer/jobs?${params.toString()}`, { cache: "no-store" });
      const data = await resp.json() as { ok: boolean; job?: OptimizerJobStatusResponse["job"] | null; error?: string } & Partial<OptimizerJobStatusResponse>;
      if (cancelled || !data.ok) return;
      if (data.job) {
        applyOptimizerJobResult(data as OptimizerJobStatusResponse);
        return;
      }

      const lastJobId = window.localStorage.getItem(optimizerJobStorageKey(sport, slateId));
      if (!lastJobId) return;

      const lastResp = await fetch(`/api/optimizer/jobs/${lastJobId}`, { cache: "no-store" });
      const lastData = await lastResp.json() as OptimizerJobStatusResponse | { ok: false; error: string };
      if (
        cancelled
        || !lastResp.ok
        || !lastData.ok
        || lastData.job.sport !== sport
        || lastData.job.slateId !== slateId
        || lastData.job.clientToken !== clientToken
      ) {
        return;
      }
      applyOptimizerJobResult(lastData);
    }

    hydrateActiveJob().catch((error) => {
      if (!cancelled) {
        setOptimizeError(error instanceof Error ? error.message : String(error));
      }
    });

    return () => { cancelled = true; };
  }, [currentSlateId, optimizerClientToken, playersById, sport]);

  useEffect(() => {
    if (!optimizerJobId) return;

    let cancelled = false;
    let timeoutId: ReturnType<typeof setTimeout> | null = null;

    const poll = async () => {
      const resp = await fetch(`/api/optimizer/jobs/${optimizerJobId}`, { cache: "no-store" });
      const data = await resp.json() as OptimizerJobStatusResponse | { ok: false; error: string };
      if (cancelled) return;

      if (!resp.ok || !data.ok) {
        setOptimizeError(("error" in data && data.error) ? data.error : "Optimizer polling failed.");
        setIsOptimizing(false);
        return;
      }

      applyOptimizerJobResult(data);
      if (data.job.status === "queued" || data.job.status === "running") {
        const nextPollDelayMs = document.visibilityState === "visible" ? 2500 : 10_000;
        timeoutId = setTimeout(() => {
          poll().catch((error) => {
            if (!cancelled) {
              setOptimizeError(error instanceof Error ? error.message : String(error));
              setIsOptimizing(false);
            }
          });
        }, nextPollDelayMs);
      }
    };

    poll().catch((error) => {
      if (!cancelled) {
        setOptimizeError(error instanceof Error ? error.message : String(error));
        setIsOptimizing(false);
      }
    });

    return () => {
      cancelled = true;
      if (timeoutId) clearTimeout(timeoutId);
    };
  }, [optimizerJobId, playersById, sport]);

  // ── Filtered + sorted player pool ─────────────────────────
  const filteredPlayers = useMemo(() => {
    return players.filter((p) => selectedGames.has(parseGameKey(p.gameInfo)));
  }, [players, selectedGames]);

  const playerPoolSourcePlayers = useMemo(() => {
    if (sport !== "mlb") return filteredPlayers;
    return filteredPlayers.filter((player) => matchesMlbPlayerPoolFilter(player, mlbPlayerPoolFilter));
  }, [filteredPlayers, mlbPlayerPoolFilter, sport]);

  const mlbLineupSummary = useMemo(() => {
    if (sport !== "mlb") {
      return { confirmedIn: 0, pending: 0, confirmedOut: 0, unavailable: 0 };
    }

    let confirmedIn = 0;
    let pending = 0;
    let confirmedOut = 0;
    let unavailable = 0;
    for (const player of playerPoolSourcePlayers) {
      if (isMlbRowUnavailable(player)) unavailable++;
      if (isMlbPitcherPlayer(player)) continue;
      const status = getMlbLineupStatus(player);
      if (status === "confirmed_in") confirmedIn++;
      else if (status === "confirmed_out") confirmedOut++;
      else if (status === "pending") pending++;
    }

    return { confirmedIn, pending, confirmedOut, unavailable };
  }, [playerPoolSourcePlayers, sport]);

  const unavailablePlayerCount = useMemo(() => {
    if (sport === "mlb") return mlbLineupSummary.unavailable;
    return filteredPlayers.filter((player) => player.isOut).length;
  }, [filteredPlayers, mlbLineupSummary.unavailable, sport]);

  const teamVisiblePlayers = useMemo(() => {
    if (!hideOutInactivePlayers) return filteredPlayers;
    return filteredPlayers.filter((player) =>
      sport === "mlb" ? !isMlbRowUnavailable(player) : !player.isOut,
    );
  }, [filteredPlayers, hideOutInactivePlayers, sport]);

  const visiblePlayers = useMemo(() => {
    if (!hideOutInactivePlayers) return playerPoolSourcePlayers;
    return playerPoolSourcePlayers.filter((player) =>
      sport === "mlb" ? !isMlbRowUnavailable(player) : !player.isOut,
    );
  }, [hideOutInactivePlayers, playerPoolSourcePlayers, sport]);
  const deferredVisiblePlayers = useDeferredValue(visiblePlayers);
  const deferredTeamVisiblePlayers = useDeferredValue(teamVisiblePlayers);

  const sortedPlayers = useMemo(() => {
    return [...deferredVisiblePlayers].sort((a, b) => {
      let av: number, bv: number;
      const nbaProjA = a.liveProj ?? a.blendProj ?? a.ourProj ?? -99;
      const nbaProjB = b.liveProj ?? b.blendProj ?? b.ourProj ?? -99;
      const nbaLevA = a.liveLeverage ?? a.ourLeverage ?? -99;
      const nbaLevB = b.liveLeverage ?? b.ourLeverage ?? -99;
      switch (sortCol) {
        case "salary":      av = a.salary;          bv = b.salary;          break;
        case "avgFptsDk":   av = a.avgFptsDk ?? -99; bv = b.avgFptsDk ?? -99; break;
        case "linestarProj":av = a.linestarProj ?? -99; bv = b.linestarProj ?? -99; break;
        case "ourProj":     av = sport === "nba" ? nbaProjA : (a.ourProj ?? -99);
                            bv = sport === "nba" ? nbaProjB : (b.ourProj ?? -99); break;
        case "delta":       av = (sport === "nba" ? nbaProjA : (a.ourProj ?? 0)) - (a.linestarProj ?? 0);
                            bv = (sport === "nba" ? nbaProjB : (b.ourProj ?? 0)) - (b.linestarProj ?? 0); break;
        case "linestarOwnPct": av = a.linestarOwnPct ?? a.projOwnPct ?? -99; bv = b.linestarOwnPct ?? b.projOwnPct ?? -99; break;
        case "projOwnPct":  av = a.projOwnPct ?? -99; bv = b.projOwnPct ?? -99; break;
        case "ourOwnPct":   av = a.ourOwnPct ?? -99; bv = b.ourOwnPct ?? -99; break;
        case "liveOwnPct":  av = sport === "nba" ? (a.liveOwnPct ?? a.projOwnPct ?? a.ourOwnPct ?? -99) : (a.ourOwnPct ?? -99);
                            bv = sport === "nba" ? (b.liveOwnPct ?? b.projOwnPct ?? b.ourOwnPct ?? -99) : (b.ourOwnPct ?? -99); break;
        case "ourLeverage": av = sport === "nba" ? nbaLevA : (a.ourLeverage ?? -99);
                            bv = sport === "nba" ? nbaLevB : (b.ourLeverage ?? -99); break;
        case "hrProb":      av = sport === "mlb" && !isMlbPitcherPlayer(a) ? (a.hrProb1Plus ?? -99) : -99;
                            bv = sport === "mlb" && !isMlbPitcherPlayer(b) ? (b.hrProb1Plus ?? -99) : -99; break;
        case "value":       av = (sport === "nba" ? nbaProjA : (a.ourProj ?? 0)) / (a.salary / 1000);
                            bv = (sport === "nba" ? nbaProjB : (b.ourProj ?? 0)) / (b.salary / 1000);    break;
        default:            return a.name.localeCompare(b.name);
      }
      return sortDir === "desc" ? bv - av : av - bv;
    });
  }, [deferredVisiblePlayers, sortCol, sortDir]);

  const filteredTeams = useMemo(() => {
    const byId = new Map<number, { teamId: number; teamAbbrev: string; teamName: string | null; teamLogo: string | null }>();
    for (const player of deferredTeamVisiblePlayers) {
      if (player.teamId == null || byId.has(player.teamId)) continue;
      byId.set(player.teamId, {
        teamId: player.teamId,
        teamAbbrev: player.teamAbbrev,
        teamName: player.teamName,
        teamLogo: player.teamLogo,
      });
    }
    return Array.from(byId.values()).sort((a, b) => a.teamAbbrev.localeCompare(b.teamAbbrev));
  }, [deferredTeamVisiblePlayers]);

  const teamOddsById = useMemo(() => {
    const byId = new Map<number, { vegasTotal: number | null; teamTotal: number | null; moneyline: number | null }>();

    for (const player of deferredTeamVisiblePlayers) {
      if (player.teamId == null || byId.has(player.teamId)) continue;
      const isHome = player.homeTeamId != null ? player.teamId === player.homeTeamId : null;
      const moneyline = isHome == null ? null : (isHome ? player.homeMl : player.awayMl);
      const explicitTeamTotal = isHome == null ? null : (isHome ? player.homeImplied : player.awayImplied);
      const derivedTeamTotal = isHome != null && player.vegasTotal != null
        ? computeTeamImpliedTotal(player.vegasTotal, player.homeMl, player.awayMl, isHome)
        : null;
      byId.set(player.teamId, {
        vegasTotal: player.vegasTotal ?? null,
        teamTotal: explicitTeamTotal ?? derivedTeamTotal,
        moneyline: moneyline ?? null,
      });
    }

    return byId;
  }, [deferredTeamVisiblePlayers]);

  const nbaTopScorerRanks = useMemo(() => {
    if (sport !== "nba") return new Map<number, number>();

    const ranked = [...players]
      .filter((player) => !player.isOut && getNbaProjectedPoints(player) != null)
      .sort((a, b) => {
        const diff = (getNbaProjectedPoints(b) ?? -Infinity) - (getNbaProjectedPoints(a) ?? -Infinity);
        return diff !== 0 ? diff : a.name.localeCompare(b.name);
      })
      .slice(0, 5);

    return new Map(ranked.map((player, idx) => [player.id, idx + 1]));
  }, [players, sport]);

  const nbaCeilingBadges = useMemo(() => {
    if (sport !== "nba") return new Map<number, NbaCeilingBadge>();
    return getNbaCeilingBadges(players);
  }, [players, sport]);

  const mlbPitcherDecisionBadges = useMemo(() => {
    if (sport !== "mlb") return new Map<number, MlbPitcherDecisionBadge>();
    if (mlbPitcherSignals.length === 0) return getMlbPitcherDecisionBadges(filteredPlayers);
    return new Map(
      mlbPitcherSignals.flatMap((signal) => signal.decisionBadge ? [[signal.playerId, signal.decisionBadge]] : []),
    );
  }, [filteredPlayers, mlbPitcherSignals, sport]);

  const mlbPitcherCeilingBadges = useMemo(() => {
    if (sport !== "mlb") return new Map<number, MlbPitcherCeilingBadge>();
    if (mlbPitcherSignals.length === 0) return getMlbPitcherCeilingBadges(filteredPlayers);
    return new Map(
      mlbPitcherSignals.flatMap((signal) => signal.ceilingBadge ? [[signal.playerId, signal.ceilingBadge]] : []),
    );
  }, [filteredPlayers, mlbPitcherSignals, sport]);

  const mlbBlowupCandidates = useMemo(() => {
    if (sport !== "mlb") return [];
    return buildMlbBlowupCandidates(filteredPlayers, 12);
  }, [filteredPlayers, sport]);

  const mlbHrTargets = useMemo(() => {
    if (sport !== "mlb") return [];

    return filteredPlayers
      .filter((player) => getMlbHrBadge(player) != null)
      .map((player) => {
        const badge = getMlbHrBadge(player)!;
        const odds = getPlayerOddsContext(player);
        const marketProb = getPlayerHrMarketProb(player);
        const edgePct = getPlayerHrEdgePct(player);
        return {
          player,
          badge,
          hrPct: player.hrProb1Plus != null ? Math.round(player.hrProb1Plus * 100) : null,
          expectedHr: player.expectedHr ?? null,
          marketProb,
          edgePct,
          teamTotal: odds.teamTotal,
        };
      })
      .sort((a, b) =>
        (b.player.hrProb1Plus ?? -1) - (a.player.hrProb1Plus ?? -1)
        || ((b.edgePct ?? -999) - (a.edgePct ?? -999))
        || (b.expectedHr ?? -1) - (a.expectedHr ?? -1)
        || a.player.name.localeCompare(b.player.name)
      );
  }, [filteredPlayers, sport]);

  const visiblePlayerRows = useMemo(() => sortedPlayers.slice(0, 200), [sortedPlayers]);
  const activeLineups = useMemo(
    () => ((sport === "nba" ? lineups : mlbLineups) ?? []) as AnyGeneratedLineup[],
    [lineups, mlbLineups, sport],
  );
  const toggleHideOutInactivePlayers = useCallback(() => {
    setHideOutInactivePlayers((current) => !current);
  }, []);
  const handleMlbPlayerPoolFilterChange = useCallback((filter: MlbPlayerPoolFilter) => {
    setMlbPlayerPoolFilter(filter);
  }, []);
  const handleStrategyChange = useCallback((value: string) => {
    setStrategy(value);
  }, []);

  useEffect(() => {
    setShowHeavyPanels(false);
    const frameId = window.requestAnimationFrame(() => setShowHeavyPanels(true));
    return () => window.cancelAnimationFrame(frameId);
  }, [players, sport]);

  const toggleSort = useCallback((col: SortCol) => {
    if (sortCol === col) setSortDir((d) => d === "desc" ? "asc" : "desc");
    else {
      setSortCol(col);
      setSortDir("desc");
    }
  }, [sortCol]);

  const togglePlayerLock = useCallback((player: DkPlayerRow) => {
    setBlockedPlayerIds((current) => current.filter((id) => id !== player.id));
    setLockedPlayerIds((current) =>
      current.includes(player.id)
        ? current.filter((id) => id !== player.id)
        : [...current, player.id],
    );
  }, []);

  const togglePlayerBlock = useCallback((player: DkPlayerRow) => {
    setLockedPlayerIds((current) => current.filter((id) => id !== player.id));
    setBlockedPlayerIds((current) =>
      current.includes(player.id)
        ? current.filter((id) => id !== player.id)
        : [...current, player.id],
    );
  }, []);

  const toggleManuallyOut = useCallback((player: DkPlayerRow) => {
    setManuallyOutIds((current) =>
      current.includes(player.id)
        ? current.filter((id) => id !== player.id)
        : [...current, player.id],
    );
  }, []);

  const toggleTeamBlock = useCallback((teamId: number) => {
    setRequiredTeamStacks((current) => current.filter((rule) => rule.teamId !== teamId));
    setBlockedTeamIds((current) =>
      current.includes(teamId)
        ? current.filter((id) => id !== teamId)
        : [...current, teamId],
    );
  }, []);

  const updateTeamStackRule = useCallback((teamId: number, value: string) => {
    const stackSize = Number(value);
    setBlockedTeamIds((current) => current.filter((id) => id !== teamId));
    setRequiredTeamStacks((current) => {
      const remaining = current.filter((rule) => rule.teamId !== teamId);
      if (![2, 3, 4, 5].includes(stackSize)) return remaining;
      return [...remaining, { teamId, stackSize: stackSize as TeamStackRule["stackSize"] }]
        .sort((a, b) => a.teamId - b.teamId);
    });
  }, []);

  const clearLocks = useCallback(() => {
    setLockedPlayerIds([]);
  }, []);

  const clearBlocks = useCallback(() => {
    setBlockedPlayerIds([]);
    setBlockedTeamIds([]);
  }, []);

  const clearTeamRules = useCallback(() => {
    setRequiredTeamStacks([]);
  }, []);

  function _LocalSortHeader({ col, label }: { col: SortCol; label: string }) {
    const active = sortCol === col;
    return (
      <th
        className={`px-3 py-2 text-left text-xs font-medium cursor-pointer select-none whitespace-nowrap
          ${active ? "text-blue-600" : "text-gray-500 hover:text-gray-700"}`}
        onClick={() => toggleSort(col)}
      >
        {label}{active ? (sortDir === "desc" ? " ↓" : " ↑") : ""}
      </th>
    );
  }

  // ── Handlers ──────────────────────────────────────────────

  function handleClear() {
    setContestId("");
    setCashLineInput("");
    setFieldSizeInput("");
    setUploadMsg(null);
    if (dkFileRef.current) dkFileRef.current.value = "";
    if (lsFileRef.current) lsFileRef.current.value = "";
  }

  async function handleLoadApi() {
    if (!contestId.trim()) { setUploadMsg({ ok: false, text: "Enter a DK contest ID" }); return; }
    startTransition(async () => {
      const cashLine  = cashLineInput ? parseFloat(cashLineInput) : undefined;
      const fieldSize = fieldSizeInput ? parseInt(fieldSizeInput, 10) : undefined;
      const fn = sport === "mlb" ? loadMlbSlateFromContestId : loadSlateFromContestId;
      const res = await fn(
        contestId.trim(),
        isNaN(cashLine!) ? undefined : cashLine,
        contestTiming,
        fieldSize && !isNaN(fieldSize) ? fieldSize : undefined,
        contestFormat,
      );
      setUploadMsg({ ok: res.ok, text: res.message });
    });
  }

  async function handleUpload() {
    if (sport === "mlb") {
      setUploadMsg({ ok: false, text: "MLB CSV upload is not wired into the app yet. Use Contest ID while the MLB workflow is being ported." });
      return;
    }
    const dkFile = dkFileRef.current?.files?.[0];
    const lsFile = lsFileRef.current?.files?.[0];
    if (!dkFile) { setUploadMsg({ ok: false, text: "Select a DK CSV first" }); return; }
    const fd = new FormData();
    fd.append("dkFile", dkFile);
    if (lsFile) fd.append("lsFile", lsFile);
    if (cashLineInput) fd.append("cashLine", cashLineInput);
    fd.append("contestType", contestTiming);
    if (fieldSizeInput) fd.append("fieldSize", fieldSizeInput);
    fd.append("contestFormat", contestFormat);
    startTransition(async () => {
      const res = await processDkSlate(fd);
      setUploadMsg({ ok: res.ok, text: res.message });
    });
  }

  async function handleOptimize() {
    if (!players[0]?.slateId) { setOptimizeError("No slate loaded"); return; }
    if (!optimizerClientToken) { setOptimizeError("Optimizer client token unavailable."); return; }
    optimizerRenderStateRef.current = { metaKey: "", lineupSignature: "", debugSignature: "" };
    setIsOptimizing(true);
    setOptimizeStartedAt(Date.now());
    setOptimizeError(null);
    setOptimizeWarning(null);
    setOptimizeDebug(null);
    setExportError(null);
    setLineups(null);
    setMlbLineups(null);
    setOptimizerJobId(null);
    setLastRequestedLineupCount(nLineups);

    // Build game → matchupId map for filtering
    const gameToMatchup = new Map<string, number>();
    for (const p of players) {
      if (p.matchupId != null) gameToMatchup.set(parseGameKey(p.gameInfo), p.matchupId);
    }
    const gameFilter = Array.from(selectedGames)
      .map((g) => gameToMatchup.get(g))
      .filter((id): id is number => id != null);

    try {
      const settings = sport === "mlb"
        ? {
            mode, nLineups, minStack, maxExposure,
            bringBackThreshold: mlbBringBackThreshold, antiCorrMax, pendingLineupPolicy,
            hrCorrelation, hrCorrelationThreshold,
            pitcherCeilingBoost, pitcherCeilingCount,
            playerLocks: lockedPlayerIds,
            playerBlocks: blockedPlayerIds,
            blockedTeamIds,
            requiredTeamStacks,
          } satisfies MlbOptimizerSettings
        : {
            mode, nLineups, minStack, teamStackCount, maxExposure, bringBackEnabled, bringBackSize,
            ceilingBoost: nbaCeilingBoost,
            ceilingCount: nbaCeilingCount,
            minSalaryFilter: minSalaryFilter ? parseInt(minSalaryFilter, 10) : null,
            maxSalaryFilter: maxSalaryFilter ? parseInt(maxSalaryFilter, 10) : null,
            playerLocks: lockedPlayerIds,
            playerBlocks: [...blockedPlayerIds, ...manuallyOutIds],
            blockedTeamIds,
            requiredTeamStacks,
          } satisfies OptimizerSettings;

      if (sport === "nba" || sport === "mlb") {
        const validation = sport === "mlb"
          ? validateMlbRuleSelections(filteredPlayers, settings)
          : validateNbaRuleSelections(filteredPlayers, settings);
        if (!validation.ok) {
          window.alert(validation.error);
          setOptimizeError(validation.error);
          setIsOptimizing(false);
          return;
        }
      }

      const resp = await fetch("/api/optimizer/jobs", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          sport,
          slateId: players[0].slateId,
          clientToken: optimizerClientToken,
          selectedMatchupIds: gameFilter,
          settings,
        }),
      });
      const data = await resp.json() as CreateOptimizerJobResponse;
      if (!resp.ok || !data.ok || !data.jobId) {
        setOptimizeError(data.error ?? "Optimizer job creation failed.");
        setIsOptimizing(false);
        return;
      }

      setOptimizerJobId(data.jobId);
    } catch (e) {
      setOptimizeError(e instanceof Error ? e.message : String(e));
      setIsOptimizing(false);
    }
  }

  const handleSave = useCallback(async () => {
    const activeLineups = sport === "nba" ? lineups : mlbLineups;
    if (!activeLineups || !players[0]?.slateId) return;
    setSaveMsg(null);
    const res = await saveLineups(players[0].slateId, activeLineups, strategy);
    setSaveMsg(res.ok ? `Saved ${res.saved} lineups as "${strategy}"` : "Save failed");
  }, [lineups, mlbLineups, players, sport, strategy]);

  const handleExport = useCallback(async () => {
    const activeLineups = sport === "nba" ? lineups : mlbLineups;
    if (!activeLineups) return;
    setExportError(null);
    setIsExporting(true);
    const result = sport === "mlb"
      ? await exportMlbLineups(mlbLineups!)
      : await exportLineups(lineups!);
    setIsExporting(false);
    if (!result.ok || !result.csv) {
      setExportError(result.error ?? "Export failed");
      return;
    }
    const csvStr = result.csv;
    const blob = new Blob([csvStr], { type: "text/csv" });
    const url  = URL.createObjectURL(blob);
    const a    = document.createElement("a");
    a.href     = url;
    a.download = `dk_${sport}_lineups_${slateDate ?? "export"}.csv`;
    a.click();
    URL.revokeObjectURL(url);
  }, [lineups, mlbLineups, slateDate, sport]);

  async function handleRefreshStatus() {
    if (!players[0]?.slateId) { setRefreshMsg({ ok: false, text: "No slate loaded" }); return; }
    setIsRefreshing(true);
    setRefreshMsg(null);
    const res = await refreshPlayerStatus(players[0].slateId);
    setIsRefreshing(false);
    setRefreshMsg({ ok: res.ok, text: res.message });
  }

  async function handleClearSlate() {
    if (!clearSlateConfirm) { setClearSlateConfirm(true); return; }
    setClearSlateConfirm(false);
    setIsClearingSlate(true);
    await clearSlate(sport);
    setIsClearingSlate(false);
    // revalidatePath in the server action triggers a router refresh automatically
  }

  async function handleFetchProps() {
    setIsFetchingProps(true);
    setPropsMsg(null);
    const res = await fetchPlayerProps(sport);
    setIsFetchingProps(false);
    setPropsMsg({ ok: res.ok, text: res.message });
  }

  async function handleAuditProps() {
    setIsAuditingProps(true);
    setPropAudit(null);
    const res = await auditNbaPropCoverage(Array.from(selectedGames));
    setIsAuditingProps(false);
    setPropAudit(res);
  }

  async function handleAuditMlbProps() {
    setIsAuditingProps(true);
    setMlbPropAudit(null);
    const res = await auditMlbPropCoverage(Array.from(selectedGames));
    setIsAuditingProps(false);
    setMlbPropAudit(res);
  }

  async function handleRecomputeProjections() {
    setIsRecomputing(true);
    setProjMsg(null);
    const res = await recomputeProjections();
    setIsRecomputing(false);
    setProjMsg({ ok: res.ok, text: res.message });
  }

  async function handleUploadLinestarCsv() {
    const file = lsUploadRef.current?.files?.[0];
    if (!file) { setLsUploadMsg({ ok: false, text: "Select a LineStar CSV first" }); return; }
    setIsUploadingLs(true);
    setLsUploadMsg(null);
    const fd = new FormData();
    fd.append("lsFile", file);
    const res = await uploadLinestarCsv(fd, sport);
    setIsUploadingLs(false);
    setLsUploadMsg({ ok: res.ok, text: res.message });
  }

  async function handleApplyPaste() {
    if (!lsPasteText.trim()) { setLsUploadMsg({ ok: false, text: "Paste LineStar data first" }); return; }
    setIsUploadingLs(true);
    setLsUploadMsg(null);
    const res = await applyLinestarPaste(lsPasteText, sport);
    setIsUploadingLs(false);
    setLsUploadMsg({ ok: res.ok, text: res.message });
  }

  async function handleCheckCookie() {
    setIsCheckingCookie(true);
    setCookieStatus(null);
    const res = await checkLinestarCookie();
    setIsCheckingCookie(false);
    setCookieStatus(res);
  }

  // ── Render ────────────────────────────────────────────────
  return (
    <div className="space-y-6">
      {/* Header */}
      <div>
        <h1 className="text-2xl font-bold">{sport.toUpperCase()} DFS Optimizer</h1>
        {slateDate && (
          <div className="flex items-center gap-3 mt-0.5">
            <p className="text-sm text-gray-500">Latest slate: {slateDate} · {players.length} players</p>
            {players.length > 0 && (
              <button
                onClick={handleClearSlate}
                disabled={isClearingSlate}
                className={`text-xs px-2 py-0.5 rounded border transition-colors disabled:opacity-50 ${
                  clearSlateConfirm
                    ? "border-red-400 bg-red-50 text-red-600 hover:bg-red-100 font-medium"
                    : "border-gray-300 text-gray-400 hover:border-red-300 hover:text-red-500"
                }`}
              >
                {isClearingSlate ? "Clearing…" : clearSlateConfirm ? "Confirm Clear" : "Clear All Slate"}
              </button>
            )}
          </div>
        )}
      </div>

      {/* Load Slate Panel */}
      <div className="rounded-lg border bg-card p-4">
        <div className="flex items-center justify-between mb-3">
          <h2 className="text-sm font-semibold">Load Slate</h2>
          <div className="flex items-center gap-2">
            <button
              onClick={handleClear}
              className="text-xs text-gray-400 hover:text-gray-600 px-2 py-1 rounded border border-gray-200 hover:border-gray-300"
            >
              Clear
            </button>
            <div className="flex rounded border text-xs overflow-hidden">
              <button
                onClick={() => setLoadMode("api")}
                className={`px-3 py-1 ${loadMode === "api" ? "bg-blue-600 text-white" : "text-gray-500 hover:bg-gray-50"}`}
              >
                Contest ID
              </button>
              <button
                onClick={() => setLoadMode("csv")}
                className={`px-3 py-1 border-l ${loadMode === "csv" ? "bg-blue-600 text-white" : "text-gray-500 hover:bg-gray-50"}`}
              >
                CSV Upload
              </button>
            </div>
          </div>
        </div>

        {/* Contest metadata — shared across both load modes */}
        <div className="flex flex-wrap items-end gap-4 mb-3">
          <div>
            <label className="text-xs text-gray-500 block mb-1">Timing</label>
            <div className="flex rounded border text-xs overflow-hidden">
              {DK_SLATE_TIMING_OPTIONS.map((option, i) => (
                <button
                  key={option.value}
                  onClick={() => setContestTiming(option.value)}
                  className={`px-3 py-1 ${i > 0 ? "border-l" : ""} ${
                    contestTiming === option.value ? "bg-slate-700 text-white" : "text-gray-500 hover:bg-gray-50"
                  }`}
                >
                  {option.label}
                </button>
              ))}
            </div>
          </div>
          <div>
            <label className="text-xs text-gray-500 block mb-1">Format</label>
            <div className="flex rounded border text-xs overflow-hidden">
              {(["gpp", "cash"] as const).map((f, i) => (
                <button
                  key={f}
                  onClick={() => setContestFormat(f)}
                  className={`px-3 py-1 uppercase ${i > 0 ? "border-l" : ""} ${
                    contestFormat === f ? "bg-slate-700 text-white" : "text-gray-500 hover:bg-gray-50"
                  }`}
                >
                  {f}
                </button>
              ))}
            </div>
          </div>
          <div>
            <label className="text-xs text-gray-500 block mb-1">
              Field Size <span className="text-gray-400">(optional)</span>
            </label>
            <input
              type="number"
              value={fieldSizeInput}
              onChange={(e) => setFieldSizeInput(e.target.value)}
              placeholder="e.g. 12500"
              className="w-28 rounded border px-3 py-1.5 text-sm"
            />
          </div>
        </div>

        {loadMode === "api" ? (
          <div className="flex items-end gap-3">
            <div className="flex-1 max-w-xs">
              <label className="text-xs text-gray-500 block mb-1">
                DK Contest ID{" "}
                <span className="text-gray-400">(from the contest URL on DraftKings)</span>
              </label>
              <input
                type="text"
                value={contestId}
                onChange={(e) => setContestId(e.target.value)}
                placeholder="e.g. 189058648"
                className="w-full rounded border px-3 py-1.5 text-sm font-mono"
              />
            </div>
            <div>
              <label className="text-xs text-gray-500 block mb-1">
                Cash Line <span className="text-gray-400">(optional)</span>
              </label>
              <input
                type="number"
                value={cashLineInput}
                onChange={(e) => setCashLineInput(e.target.value)}
                placeholder="e.g. 285"
                className="w-24 rounded border px-3 py-1.5 text-sm"
              />
            </div>
            <button
              onClick={handleLoadApi}
              disabled={isPending || !contestId.trim()}
              className="rounded bg-blue-600 px-4 py-1.5 text-sm text-white hover:bg-blue-700 disabled:opacity-50"
            >
              {isPending ? "Loading…" : "Load from DK API"}
            </button>
          </div>
        ) : (
          <div className="flex flex-wrap gap-4 items-end">
            <div>
              <label className="text-xs text-gray-500 block mb-1">DK Salary CSV</label>
              <input ref={dkFileRef} type="file" accept=".csv" className="text-sm" />
            </div>
            <div>
              <label className="text-xs text-gray-500 block mb-1">LineStar CSV (optional)</label>
              <input ref={lsFileRef} type="file" accept=".csv" className="text-sm" />
            </div>
            <div>
              <label className="text-xs text-gray-500 block mb-1">
                Cash Line <span className="text-gray-400">(optional)</span>
              </label>
              <input
                type="number"
                value={cashLineInput}
                onChange={(e) => setCashLineInput(e.target.value)}
                placeholder="e.g. 285"
                className="w-24 rounded border px-3 py-1.5 text-sm"
              />
            </div>
            <button
              onClick={handleUpload}
              disabled={isPending}
              className="rounded bg-blue-600 px-4 py-1.5 text-sm text-white hover:bg-blue-700 disabled:opacity-50"
            >
              {isPending ? "Processing…" : "Load from CSV"}
            </button>
          </div>
        )}

        {uploadMsg && (
          <p className={`mt-2 text-sm ${uploadMsg.ok ? "text-green-700" : "text-red-600"}`}>
            {uploadMsg.text}
          </p>
        )}

        {/* Refresh player status — re-hits DK API to catch late scratches */}
        {players.length > 0 && (
          <div className="mt-3 pt-3 border-t flex items-center gap-3">
            <button
              onClick={handleRefreshStatus}
              disabled={isRefreshing}
              className="rounded bg-amber-500 px-3 py-1.5 text-sm text-white hover:bg-amber-600 disabled:opacity-50"
            >
              {isRefreshing ? "Refreshing…" : "Refresh Player Status"}
            </button>
            <span className="text-xs text-gray-400">
              Re-checks DK API for late scratches / GTD updates
            </span>
            {refreshMsg && (
              <span className={`text-sm ${refreshMsg.ok ? "text-green-700" : "text-red-600"}`}>
                {refreshMsg.text}
              </span>
            )}
          </div>
        )}

        {/* LineStar tools — manual CSV upload + cookie health check */}
        <div className="mt-3 pt-3 border-t space-y-3">
          <div className="flex items-center gap-2">
            <span className="text-xs font-medium text-gray-600">LineStar</span>
            <button
              onClick={handleCheckCookie}
              disabled={isCheckingCookie}
              className="rounded border px-2 py-0.5 text-xs text-gray-600 hover:bg-gray-50 disabled:opacity-50"
            >
              {isCheckingCookie ? "Checking…" : "Check Cookie"}
            </button>
            {cookieStatus && (
              <span className={`text-xs px-2 py-0.5 rounded-full font-medium ${
                cookieStatus.ok
                  ? "bg-green-100 text-green-800"
                  : "bg-red-100 text-red-700"
              }`}>
                {cookieStatus.ok ? "✓ Valid" : cookieStatus.message.includes("expired") ? "✗ Expired" : "✗ Error"} — {cookieStatus.message}
              </span>
            )}
          </div>

          {/* Input mode toggle */}
          <div className="flex items-center gap-2 mb-2">
            <span className="text-xs text-gray-500">Input:</span>
            <div className="flex rounded border text-xs overflow-hidden">
              <button
                onClick={() => setLsMode("paste")}
                className={`px-3 py-1 ${lsMode === "paste" ? "bg-purple-600 text-white" : "text-gray-500 hover:bg-gray-50"}`}
              >
                Paste
              </button>
              <button
                onClick={() => setLsMode("csv")}
                className={`px-3 py-1 border-l ${lsMode === "csv" ? "bg-purple-600 text-white" : "text-gray-500 hover:bg-gray-50"}`}
              >
                CSV File
              </button>
            </div>
          </div>

          {lsMode === "paste" ? (
            <div className="space-y-2">
              <label className="text-xs text-gray-500">
                Select all rows on the LineStar salary page and paste below
              </label>
              <textarea
                value={lsPasteText}
                onChange={(e) => setLsPasteText(e.target.value)}
                rows={4}
                placeholder={"Pos\tTeam\tPlayer\tSalary\tprojOwn%\t...\nC\t\tNikola Jokic\t$12500\t35.1%\t..."}
                className="w-full rounded border px-2 py-1.5 text-xs font-mono resize-y"
              />
              <div className="flex items-center gap-3">
                <button
                  onClick={handleApplyPaste}
                  disabled={isUploadingLs || !lsPasteText.trim()}
                  className="rounded bg-purple-600 px-3 py-1.5 text-sm text-white hover:bg-purple-700 disabled:opacity-50"
                >
                  {isUploadingLs ? "Applying…" : "Apply LineStar Data"}
                </button>
                {lsUploadMsg && (
                  <span className={`text-sm ${lsUploadMsg.ok ? "text-green-700" : "text-red-600"}`}>
                    {lsUploadMsg.text}
                  </span>
                )}
              </div>
            </div>
          ) : (
            <div className="flex flex-wrap items-end gap-3">
              <div>
                <label className="text-xs text-gray-500 block mb-1">LineStar salary CSV export</label>
                <input ref={lsUploadRef} type="file" accept=".csv" className="text-sm" />
              </div>
              <button
                onClick={handleUploadLinestarCsv}
                disabled={isUploadingLs}
                className="rounded bg-purple-600 px-3 py-1.5 text-sm text-white hover:bg-purple-700 disabled:opacity-50"
              >
                {isUploadingLs ? "Uploading…" : "Upload CSV"}
              </button>
              {lsUploadMsg && (
                <span className={`text-sm ${lsUploadMsg.ok ? "text-green-700" : "text-red-600"}`}>
                  {lsUploadMsg.text}
                </span>
              )}
            </div>
          )}
        </div>

        {/* Fetch Projections — run after LineStar to compute leverage scores */}
        {sport === "nba" && players.length > 0 && (
          <div className="mt-3 pt-3 border-t space-y-2">
            <div className="flex items-center gap-3">
              <button
                onClick={handleRecomputeProjections}
                disabled={isRecomputing}
                className="rounded bg-indigo-600 px-3 py-1.5 text-sm text-white hover:bg-indigo-700 disabled:opacity-50"
              >
                {isRecomputing ? "Computing…" : "Fetch Projections"}
              </button>
              <span className="text-xs text-gray-400">
                Run after applying LineStar · re-computes projections + leverage scores
              </span>
            </div>
            {projMsg && (
              <span className={`text-sm ${projMsg.ok ? "text-green-700" : "text-red-600"} whitespace-pre-wrap`}>
                {projMsg.text}
              </span>
            )}
          </div>
        )}

        {/* Player props from The Odds API — NBA only (pts/reb/ast/blk/stl) */}
        {sport === "nba" && players.length > 0 && (
          <div className="mt-3 pt-3 border-t space-y-2">
            <div className="flex items-center gap-3">
              <button
                onClick={handleFetchProps}
                disabled={isFetchingProps}
                className="rounded bg-emerald-600 px-3 py-1.5 text-sm text-white hover:bg-emerald-700 disabled:opacity-50"
              >
                Fetch Player Props
              </button>
              <button
                onClick={handleAuditProps}
                disabled={isAuditingProps}
                className="rounded bg-slate-700 px-3 py-1.5 text-sm text-white hover:bg-slate-800 disabled:opacity-50"
              >
                {isAuditingProps ? "Auditing…" : "Audit Prop Coverage"}
              </button>
              <span className="text-xs text-gray-400">
                Pulls pts/reb/ast/blk/stl lines from The Odds API · audit checks coverage by book
              </span>
            </div>
            <TimedSpinnerMessage active={isFetchingProps} text="Fetching props…" />
            {propsMsg && (
              <span className={`text-sm ${propsMsg.ok ? "text-green-700" : "text-red-600"}`}>
                {propsMsg.text}
              </span>
            )}
            {propAudit && (
              <div className="rounded border bg-gray-50 p-3 text-xs text-gray-700 space-y-2">
                <div className={propAudit.ok ? "text-green-700" : "text-red-600"}>{propAudit.message}</div>
                {propAudit.ok && propAudit.books && propAudit.books.length > 0 && (
                  <>
                    <div className="text-gray-500">
                      Games: {propAudit.selectedGames.join(", ") || "All selected"} · Player pool: {propAudit.playerPoolCount} · Books: {propAudit.bookmakerCount ?? propAudit.books.length}
                    </div>
                    {propAudit.leaders && propAudit.leaders.length > 0 && (
                      <div className="text-gray-600">
                        Leaders: {propAudit.leaders.map((leader) => `${leader.stat.toUpperCase()} ${leader.bookmakerTitle} (${leader.count})`).join(" · ")}
                      </div>
                    )}
                    <div className="overflow-x-auto">
                      <table className="min-w-full border-collapse">
                        <thead>
                          <tr className="text-left text-[11px] uppercase tracking-wide text-gray-500">
                            <th className="px-2 py-1">Book</th>
                            <th className="px-2 py-1 text-right">Any</th>
                            <th className="px-2 py-1 text-right">Pts</th>
                            <th className="px-2 py-1 text-right">Reb</th>
                            <th className="px-2 py-1 text-right">Ast</th>
                            <th className="px-2 py-1 text-right">Blk</th>
                            <th className="px-2 py-1 text-right">Stl</th>
                          </tr>
                        </thead>
                        <tbody>
                          {propAudit.books.slice(0, 12).map((book) => (
                            <tr key={book.bookmakerKey} className="border-t border-gray-200">
                              <td className="px-2 py-1 font-medium">{book.bookmakerTitle}</td>
                              <td className="px-2 py-1 text-right">{book.uniquePlayers}</td>
                              <td className="px-2 py-1 text-right">{book.stats.pts}</td>
                              <td className="px-2 py-1 text-right">{book.stats.reb}</td>
                              <td className="px-2 py-1 text-right">{book.stats.ast}</td>
                              <td className="px-2 py-1 text-right">{book.stats.blk}</td>
                              <td className="px-2 py-1 text-right">{book.stats.stl}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </>
                )}
              </div>
            )}
          </div>
        )}

        {/* MLB player props and coverage audit */}
        {sport === "mlb" && players.length > 0 && (
          <div className="mt-3 pt-3 border-t space-y-2">
            <div className="flex items-center gap-3">
              <button
                onClick={handleFetchProps}
                disabled={isFetchingProps}
                className="rounded bg-emerald-600 px-3 py-1.5 text-sm text-white hover:bg-emerald-700 disabled:opacity-50"
              >
                Fetch Player Props
              </button>
              <button
                onClick={handleAuditMlbProps}
                disabled={isAuditingProps}
                className="rounded bg-slate-700 px-3 py-1.5 text-sm text-white hover:bg-slate-800 disabled:opacity-50"
              >
                {isAuditingProps ? "Auditing…" : "Audit Prop Coverage"}
              </button>
              <span className="text-xs text-gray-400">
                Pulls H/TB/R/RBI/HR/K/OUTS/ER lines from The Odds API · audit checks coverage by book
              </span>
            </div>
            <TimedSpinnerMessage active={isFetchingProps} text="Fetching props…" />
            {propsMsg && (
              <span className={`text-sm ${propsMsg.ok ? "text-green-700" : "text-red-600"}`}>
                {propsMsg.text}
              </span>
            )}
            {mlbPropAudit && (
              <div className="rounded border bg-gray-50 p-3 text-xs text-gray-700 space-y-2">
                <div className={mlbPropAudit.ok ? "text-green-700" : "text-red-600"}>{mlbPropAudit.message}</div>
                {mlbPropAudit.ok && mlbPropAudit.books && mlbPropAudit.books.length > 0 && (
                  <>
                    <div className="text-gray-500">
                      Games: {mlbPropAudit.selectedGames.join(", ") || "All selected"} · Player pool: {mlbPropAudit.playerPoolCount} · Books: {mlbPropAudit.bookmakerCount ?? mlbPropAudit.books.length}
                    </div>
                    {mlbPropAudit.leaders && mlbPropAudit.leaders.length > 0 && (
                      <div className="text-gray-600">
                        Leaders: {mlbPropAudit.leaders.map((leader) => `${leader.stat.toUpperCase()} ${leader.bookmakerTitle} (${leader.count})`).join(" · ")}
                      </div>
                    )}
                    <div className="overflow-x-auto">
                      <table className="min-w-full border-collapse">
                        <thead>
                          <tr className="text-left text-[11px] uppercase tracking-wide text-gray-500">
                            <th className="px-2 py-1">Book</th>
                            <th className="px-2 py-1 text-right">Any</th>
                            <th className="px-2 py-1 text-right">H</th>
                            <th className="px-2 py-1 text-right">TB</th>
                            <th className="px-2 py-1 text-right">R</th>
                            <th className="px-2 py-1 text-right">RBI</th>
                            <th className="px-2 py-1 text-right">HR</th>
                            <th className="px-2 py-1 text-right">K</th>
                            <th className="px-2 py-1 text-right">Outs</th>
                            <th className="px-2 py-1 text-right">ER</th>
                          </tr>
                        </thead>
                        <tbody>
                          {mlbPropAudit.books.slice(0, 12).map((book) => (
                            <tr key={book.bookmakerKey} className="border-t border-gray-200">
                              <td className="px-2 py-1 font-medium">{book.bookmakerTitle}</td>
                              <td className="px-2 py-1 text-right">{book.uniquePlayers}</td>
                              <td className="px-2 py-1 text-right">{book.stats.hits}</td>
                              <td className="px-2 py-1 text-right">{book.stats.tb}</td>
                              <td className="px-2 py-1 text-right">{book.stats.runs}</td>
                              <td className="px-2 py-1 text-right">{book.stats.rbis}</td>
                              <td className="px-2 py-1 text-right">{book.stats.hr}</td>
                              <td className="px-2 py-1 text-right">{book.stats.ks}</td>
                              <td className="px-2 py-1 text-right">{book.stats.outs}</td>
                              <td className="px-2 py-1 text-right">{book.stats.er}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </>
                )}
              </div>
            )}
          </div>
        )}

      </div>

      {/* MLB Game Environment Cards */}
      {sport === "mlb" && mlbGameCards && mlbGameCards.length > 0 && (
        <MlbGameCardStrip
          cards={mlbGameCards}
          players={players}
          signals={mlbPitcherSignals}
          selectedGames={selectedGames}
          onToggleGame={(gameKey) => {
            const next = new Set(selectedGames);
            if (next.has(gameKey)) next.delete(gameKey); else next.add(gameKey);
            setSelectedGames(next);
          }}
          onSelectAll={() => setSelectedGames(new Set(allGames))}
          onSelectNone={() => setSelectedGames(new Set())}
        />
      )}

      {/* Game Filter */}
      {allGames.length > 0 && (
        <div className="rounded-lg border bg-card p-4">
          <div className="flex items-center justify-between mb-2">
            <h2 className="text-sm font-semibold">Games ({selectedGames.size}/{allGames.length} selected)</h2>
            <div className="flex gap-2">
              <button onClick={() => setSelectedGames(new Set(allGames))} className="text-xs text-blue-600 hover:underline">All</button>
              <button onClick={() => setSelectedGames(new Set())} className="text-xs text-gray-500 hover:underline">None</button>
            </div>
          </div>
          <div className="flex flex-wrap gap-2">
            {allGames.map((g) => (
              <button
                key={g}
                onClick={() => {
                  const next = new Set(selectedGames);
                  if (next.has(g)) next.delete(g); else next.add(g);
                  setSelectedGames(next);
                }}
                className={`rounded px-2.5 py-1 text-xs font-mono font-medium border transition-colors ${
                  selectedGames.has(g)
                    ? "bg-blue-600 text-white border-blue-600"
                    : "bg-white text-gray-600 border-gray-300 hover:border-blue-400"
                }`}
              >
                {g}
              </button>
            ))}
          </div>
        </div>
      )}

      {/* Optimizer Settings */}
      <div className="rounded-lg border bg-card p-4">
        <h2 className="text-sm font-semibold mb-3">Optimizer Settings</h2>
        <div className="flex flex-wrap gap-4 items-end">
          <div>
            <label className="text-xs text-gray-500 block mb-1">Mode</label>
            <select
              value={mode}
              onChange={(e) => setMode(e.target.value as OptimizerMode)}
              className="rounded border px-2 py-1 text-sm"
            >
              <option value="gpp">GPP (balanced)</option>
              <option value="gpp2">GPP2 (large field)</option>
              {sport === "nba" && <option value="gpp_ls">GPP LineStar (ceiling + calibrated own)</option>}
              <option value="cash">Cash (proj)</option>
            </select>
            {mode === "gpp2" && (
              <p className="mt-1 max-w-44 text-[11px] leading-4 text-muted-foreground">
                Built for top-heavy, large-field contests around 10k+ entries.
              </p>
            )}
            {mode === "gpp_ls" && (
              <p className="mt-1 max-w-44 text-[11px] leading-4 text-muted-foreground">
                High-variance GPP using p90 ceiling edge + LineStar ownership calibrated for team/position bias.
              </p>
            )}
          </div>
          <div>
            <label className="text-xs text-gray-500 block mb-1">Lineups</label>
            <input
              type="number" min={1} max={150} value={nLineups}
              onChange={(e) => setNLineups(parseInt(e.target.value) || 20)}
              className="w-20 rounded border px-2 py-1 text-sm"
            />
          </div>
          {sport === "nba" && (
            <div>
              <label className="text-xs text-gray-500 block mb-1">Team Stacks</label>
              <select
                value={teamStackCount}
                onChange={(e) => setTeamStackCount(parseInt(e.target.value))}
                className="rounded border px-2 py-1 text-sm"
              >
                <option value={1}>1</option>
                <option value={2}>2</option>
                <option value={3}>3</option>
              </select>
            </div>
          )}
          <div>
            <label className="text-xs text-gray-500 block mb-1">{sport === "nba" ? "Stack Size" : "Min Stack"}</label>
            <select
              value={minStack}
              onChange={(e) => setMinStack(parseInt(e.target.value))}
              className="rounded border px-2 py-1 text-sm"
            >
              {sport === "nba" ? (
                <>
                  <option value={2}>2</option>
                  <option value={3}>3</option>
                  <option value={4}>4</option>
                  <option value={5}>5</option>
                </>
              ) : (
                <>
                  <option value={1}>1</option>
                  <option value={2}>2</option>
                  <option value={3}>3</option>
                  <option value={4}>4</option>
                </>
              )}
            </select>
          </div>
          <div>
            <label className="text-xs text-gray-500 block mb-1">Max Exposure</label>
            <select
              value={maxExposure}
              onChange={(e) => setMaxExposure(parseFloat(e.target.value))}
              className="rounded border px-2 py-1 text-sm"
            >
              <option value={0.4}>40%</option>
              <option value={0.5}>50%</option>
              <option value={0.6}>60%</option>
              <option value={0.7}>70%</option>
              <option value={1.0}>100%</option>
            </select>
          </div>
          {sport === "nba" && (
            <>
              <div>
                <label className="text-xs text-gray-500 block mb-1">Opponent Bring-back?</label>
                <select
                  value={bringBackEnabled ? "yes" : "no"}
                  onChange={(e) => setBringBackEnabled(e.target.value === "yes")}
                  className="rounded border px-2 py-1 text-sm"
                >
                  <option value="no">No</option>
                  <option value="yes">Yes</option>
                </select>
              </div>
              <div>
                <label className="text-xs text-gray-500 block mb-1">Bring-back Size</label>
                <select
                  value={bringBackSize}
                  onChange={(e) => setBringBackSize(parseInt(e.target.value))}
                  disabled={!bringBackEnabled}
                  className="rounded border px-2 py-1 text-sm disabled:opacity-50"
                >
                  <option value={1}>1</option>
                  <option value={2}>2</option>
                  <option value={3}>3</option>
                </select>
              </div>
            </>
          )}
          {sport === "mlb" && (
            <div>
              <label className="text-xs text-gray-500 block mb-1">
                Anti-Corr{" "}
                <span className="text-gray-400 font-normal">(SP opp batters)</span>
              </label>
              <select
                value={antiCorrMax}
                onChange={(e) => setAntiCorrMax(parseInt(e.target.value))}
                className="rounded border px-2 py-1 text-sm"
              >
                <option value={0}>Off (0)</option>
                <option value={1}>Max 1</option>
                <option value={2}>Max 2</option>
              </select>
            </div>
          )}
          {sport === "mlb" && (
            <div>
              <label className="text-xs text-gray-500 block mb-1">Pending Lineups</label>
              <select
                value={pendingLineupPolicy}
                onChange={(e) => setPendingLineupPolicy(e.target.value as MlbPendingLineupPolicy)}
                className="rounded border px-2 py-1 text-sm"
              >
                <option value="downgrade">Downgrade</option>
                <option value="ignore">Maintain</option>
                <option value="exclude">Exclude</option>
              </select>
            </div>
          )}
          {sport === "mlb" && (
            <div>
              <label className="text-xs text-gray-500 block mb-1">
                HR Correlation{" "}
                <span
                  className="text-gray-400 font-normal cursor-help"
                  title="Bonus preceding batters (order - 1, order - 2) of high-HR-probability hitters. If batter #3 has a high HR chance, batter #2 (likely on base) gets +5 score and batter #1 gets +2."
                >(?)</span>
              </label>
              <div className="flex items-center gap-2">
                <select
                  value={hrCorrelation ? "on" : "off"}
                  onChange={(e) => setHrCorrelation(e.target.value === "on")}
                  className="rounded border px-2 py-1 text-sm"
                >
                  <option value="off">Off</option>
                  <option value="on">On</option>
                </select>
                {hrCorrelation && (
                  <input
                    type="number"
                    min={0.05}
                    max={0.40}
                    step={0.01}
                    value={hrCorrelationThreshold}
                    onChange={(e) => {
                      const next = Number.parseFloat(e.target.value);
                      if (!Number.isFinite(next)) {
                        setHrCorrelationThreshold(0.12);
                        return;
                      }
                      setHrCorrelationThreshold(Math.min(0.4, Math.max(0.05, next)));
                    }}
                    title="Minimum HR probability (e.g. 0.12 = 12%) to trigger the correlation bonus"
                    className="w-20 rounded border px-2 py-1 text-sm"
                  />
                )}
              </div>
            </div>
          )}
          {sport === "mlb" && (
            <div>
              <label className="text-xs text-gray-500 block mb-1">
                Pitcher Ceiling{" "}
                <span
                  className="text-gray-400 font-normal cursor-help"
                  title="Boost the slate's highest raw-ceiling pitchers in optimizer search using K, outs, ER, opponent team total, projection, and value. This nudges exposure; it does not change pitcher projections."
                >(?)</span>
              </label>
              <div className="flex items-center gap-2">
                <select
                  value={pitcherCeilingBoost ? "on" : "off"}
                  onChange={(e) => setPitcherCeilingBoost(e.target.value === "on")}
                  className="rounded border px-2 py-1 text-sm"
                >
                  <option value="off">Off</option>
                  <option value="on">On</option>
                </select>
                {pitcherCeilingBoost && (
                  <input
                    type="number"
                    min={1}
                    max={5}
                    step={1}
                    value={pitcherCeilingCount}
                    onChange={(e) => {
                      const next = Number.parseInt(e.target.value, 10);
                      if (!Number.isFinite(next)) {
                        setPitcherCeilingCount(3);
                        return;
                      }
                      setPitcherCeilingCount(Math.min(5, Math.max(1, next)));
                    }}
                    title="Number of top ceiling pitchers to boost in search"
                    className="w-16 rounded border px-2 py-1 text-sm"
                  />
                )}
              </div>
            </div>
          )}
          {sport === "nba" && (
            <div>
              <label className="text-xs text-gray-500 block mb-1">
                Ceiling Boost{" "}
                <span
                  className="text-gray-400 font-normal cursor-help"
                  title="Boost the slate's top raw-ceiling NBA players in optimizer search using ceiling, boom rate, points prop, live projection, and value. This nudges exposure; it does not change projections."
                >(?)</span>
              </label>
              <div className="flex items-center gap-2">
                <select
                  value={nbaCeilingBoost ? "on" : "off"}
                  onChange={(e) => setNbaCeilingBoost(e.target.value === "on")}
                  className="rounded border px-2 py-1 text-sm"
                >
                  <option value="off">Off</option>
                  <option value="on">On</option>
                </select>
                {nbaCeilingBoost && (
                  <input
                    type="number"
                    min={1}
                    max={5}
                    step={1}
                    value={nbaCeilingCount}
                    onChange={(e) => {
                      const next = Number.parseInt(e.target.value, 10);
                      if (!Number.isFinite(next)) {
                        setNbaCeilingCount(3);
                        return;
                      }
                      setNbaCeilingCount(Math.min(5, Math.max(1, next)));
                    }}
                    title="Number of top ceiling players to boost in search"
                    className="w-16 rounded border px-2 py-1 text-sm"
                  />
                )}
              </div>
            </div>
          )}
          <div>
            <label className="text-xs text-gray-500 block mb-1">Min Salary</label>
            <input
              type="number" min={3000} max={50000} step={100}
              value={minSalaryFilter}
              onChange={(e) => setMinSalaryFilter(e.target.value)}
              placeholder="e.g. 4500"
              className="w-28 rounded border px-2 py-1 text-sm"
            />
          </div>
          <div>
            <label className="text-xs text-gray-500 block mb-1">Max Salary</label>
            <input
              type="number" min={3000} max={50000} step={100}
              value={maxSalaryFilter}
              onChange={(e) => setMaxSalaryFilter(e.target.value)}
              placeholder="e.g. 9000"
              className="w-28 rounded border px-2 py-1 text-sm"
            />
          </div>
          <button
            onClick={handleOptimize}
            disabled={isOptimizing || filteredPlayers.length === 0}
            className="rounded bg-green-600 px-5 py-1.5 text-sm font-medium text-white hover:bg-green-700 disabled:opacity-50"
          >
            {isOptimizing ? "Optimizing…" : "Optimize"}
          </button>
        </div>
        {optimizeError && <p className="mt-2 text-sm text-red-600 whitespace-pre-wrap">{optimizeError}</p>}
        <OptimizerStatusPanel
          isOptimizing={isOptimizing}
          optimizeStartedAt={optimizeStartedAt}
          builtLineupCount={activeLineups.length}
          lastRequestedLineupCount={lastRequestedLineupCount}
        />
        {optimizeWarning && <p className="mt-2 text-sm text-amber-700">{optimizeWarning}</p>}
        {optimizeDebug && (
          <details className="mt-2 rounded border bg-gray-50 p-3">
            <summary className="cursor-pointer text-sm font-medium text-gray-800">
              Optimizer Debug: {optimizeDebug.builtLineups}/{optimizeDebug.requestedLineups} built in {fmtDuration(debugTotalMs)}
            </summary>
            <div className="mt-3 space-y-3 text-xs text-gray-700">
              <div className="grid gap-2 md:grid-cols-2">
                <div className="rounded border bg-white p-2">
                  <p><strong>Eligible:</strong> {optimizeDebug.eligibleCount}</p>
                  <p><strong>Probe time:</strong> {fmtDuration(optimizeDebug.probeMs)}</p>
                  <p><strong>Termination:</strong> {optimizeDebug.terminationReason}{optimizeDebug.stoppedAtLineup ? ` at lineup ${optimizeDebug.stoppedAtLineup}` : ""}</p>
                  <p><strong>Exposure cap:</strong> {optimizeDebug.maxExposureCount} max uses per player</p>
                </div>
                <div className="rounded border bg-white p-2">
                  {optimizeDebug.sport === "nba" ? (
                    <>
                      <p><strong>Effective team stacks:</strong> {optimizeDebug.effectiveSettings.teamStackCount ?? 1}</p>
                      <p><strong>Effective stack size:</strong> {optimizeDebug.effectiveSettings.minStack}</p>
                      <p><strong>Effective bring-back:</strong> {optimizeDebug.effectiveSettings.bringBackEnabled ? `Yes (${optimizeDebug.effectiveSettings.bringBackSize ?? 1})` : "No"}</p>
                      <p><strong>Ceiling boost:</strong> {optimizeDebug.effectiveSettings.ceilingBoost ? `On (top ${optimizeDebug.effectiveSettings.ceilingCount ?? 3})` : "Off"}</p>
                    </>
                    ) : (
                      <>
                      <p><strong>Effective stack:</strong> {optimizeDebug.effectiveSettings.minStack}</p>
                      <p><strong>Effective bring-back:</strong> {optimizeDebug.effectiveSettings.bringBackThreshold ?? 0}</p>
                      <p><strong>Pending hitters:</strong> {formatPendingLineupPolicy(optimizeDebug.effectiveSettings.pendingLineupPolicy ?? "downgrade")}</p>
                      <p><strong>HR correlation:</strong> {hrCorrelation ? `On (${Math.round(hrCorrelationThreshold * 100)}%)` : "Off"}</p>
                      <p><strong>Pitcher ceiling:</strong> {pitcherCeilingBoost ? `On (top ${pitcherCeilingCount})` : "Off"}</p>
                      </>
                    )}
                  <p><strong>Effective diversity:</strong> {optimizeDebug.effectiveSettings.minChanges}</p>
                  {"antiCorrMax" in optimizeDebug.effectiveSettings && optimizeDebug.effectiveSettings.antiCorrMax != null && (
                    <p><strong>Effective anti-corr:</strong> {optimizeDebug.effectiveSettings.antiCorrMax}</p>
                  )}
                  {optimizeDebug.effectiveSettings.salaryFloor != null && (
                    <p><strong>Salary floor:</strong> ${optimizeDebug.effectiveSettings.salaryFloor.toLocaleString()}</p>
                  )}
                </div>
              </div>
              {optimizeDebug.heuristic && (
                <div className="rounded border bg-white p-2">
                  <p className="mb-1 font-medium text-gray-800">Heuristic Search</p>
                  <div className="grid gap-1 md:grid-cols-2">
                    <p><strong>Pruned candidates:</strong> {optimizeDebug.heuristic.prunedCandidateCount}</p>
                    <p><strong>Template count:</strong> {optimizeDebug.heuristic.templateCount}</p>
                    <p><strong>Templates tried:</strong> {optimizeDebug.heuristic.templatesTried}</p>
                    <p><strong>Repair attempts:</strong> {optimizeDebug.heuristic.repairAttempts}</p>
                  </div>
                  {heuristicRejectSummary.length > 0 && (
                    <div className="mt-2 flex flex-wrap gap-2 text-[11px] text-gray-500">
                      {heuristicRejectSummary.map(([reason, count]) => (
                        <span key={reason} className="rounded bg-gray-100 px-1.5 py-0.5">
                          {reason}: {count}
                        </span>
                      ))}
                    </div>
                  )}
                </div>
              )}
              {optimizeDebug.relaxedConstraints.length > 0 && (
                <p><strong>Relaxed constraints:</strong> {optimizeDebug.relaxedConstraints.join(", ")}</p>
              )}
              {slowestLineup && (
                <p><strong>Slowest lineup:</strong> #{slowestLineup.lineupNumber} in {fmtDuration(slowestLineup.durationMs)}</p>
              )}
              {optimizeDebug.probeSummary.length > 0 && (
                <div>
                  <p className="mb-1 font-medium text-gray-800">Probe Timings</p>
                  <div className="space-y-1 rounded border bg-white p-2">
                    {optimizeDebug.probeSummary.map((probe) => (
                      <div key={`${probe.label}-${probe.durationMs}`} className="flex items-center justify-between gap-3">
                        <span>{probe.label}</span>
                        <span className={probe.success ? "text-green-700" : "text-red-600"}>
                          {probe.success ? "PASS" : "FAIL"} · {fmtDuration(probe.durationMs)}
                        </span>
                      </div>
                    ))}
                  </div>
                </div>
              )}
              {optimizeDebug.lineupSummaries.length > 0 && (
                <div>
                  <p className="mb-1 font-medium text-gray-800">Lineup Attempts</p>
                  <div className="max-h-56 space-y-2 overflow-y-auto rounded border bg-white p-2">
                    {optimizeDebug.lineupSummaries.map((lineup) => (
                      <div key={lineup.lineupNumber} className="rounded border p-2">
                        <div className="flex items-center justify-between gap-3">
                          <span className="font-medium">
                            #{lineup.lineupNumber} {lineup.status === "built" ? "built" : "failed"}
                          </span>
                          <span>{fmtDuration(lineup.durationMs)}{lineup.winningStage ? ` via ${lineup.winningStage}` : ""}</span>
                        </div>
                        <div className="mt-1 flex flex-wrap gap-2 text-[11px] text-gray-500">
                          {lineup.attempts.map((attempt) => (
                            <span key={`${lineup.lineupNumber}-${attempt.stage}`} className="rounded bg-gray-100 px-1.5 py-0.5">
                              {attempt.stage}: {attempt.success ? "ok" : "fail"} ({fmtDuration(attempt.durationMs)})
                              {attempt.templateCount != null ? ` · tpl ${attempt.templateCount}` : ""}
                              {attempt.templatesTried != null ? ` · tried ${attempt.templatesTried}` : ""}
                              {attempt.repairAttempts != null && attempt.repairAttempts > 0 ? ` · repair ${attempt.repairAttempts}` : ""}
                              {!attempt.success && attempt.failureReason ? ` · ${attempt.failureReason}` : ""}
                            </span>
                          ))}
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </div>
          </details>
        )}
      </div>

      {supportsRuleControls && filteredPlayers.length > 0 && (
        showHeavyPanels ? (
          <RuleControlsSection
            sport={sport}
            filteredTeams={filteredTeams}
            blockedTeamSet={blockedTeamSet}
            requiredTeamStackMap={requiredTeamStackMap}
            teamOddsById={teamOddsById}
            hideOutInactivePlayers={hideOutInactivePlayers}
            unavailablePlayerCount={unavailablePlayerCount}
            lockedCount={lockedPlayerIds.length}
            blockedCount={blockedPlayerIds.length + blockedTeamIds.length}
            requiredTeamStackCount={requiredTeamStacks.length}
            onToggleHideOutInactive={toggleHideOutInactivePlayers}
            onClearLocks={clearLocks}
            onClearBlocks={clearBlocks}
            onClearTeamRules={clearTeamRules}
            onToggleTeamBlock={toggleTeamBlock}
            onUpdateTeamStackRule={updateTeamStackRule}
          />
        ) : (
          <div className="rounded-lg border bg-card p-4">
            <div className="h-4 w-40 animate-pulse rounded bg-gray-100" />
            <div className="mt-4 grid gap-2 md:grid-cols-2 xl:grid-cols-4">
              {Array.from({ length: 4 }).map((_, idx) => (
                <div key={idx} className="h-24 animate-pulse rounded border bg-gray-50" />
              ))}
            </div>
          </div>
        )
      )}

      {/* MLB Blowup Candidates */}
      {sport === "mlb" && mlbBlowupCandidates.length > 0 && (
        <div className="rounded-lg border bg-card p-4 text-sm">
          <h2 className="font-semibold mb-2">
            GPP Blowup Candidates
            <span
              className="ml-1.5 text-xs text-gray-400 font-normal cursor-help"
              title="Low-salary batters with high ceiling relative to team total, HR probability, expected HR, and ownership. Excludes SP/RP."
            >(?)</span>
          </h2>
          <div className="overflow-x-auto">
            <table className="w-full text-xs border-collapse">
              <thead>
                <tr className="border-b text-gray-500 text-right">
                  <th className="py-1 text-left">Player</th>
                  <th className="py-1 text-left pl-2">Pos</th>
                  <th className="py-1">Salary</th>
                  <th className="py-1">Proj</th>
                  <th className="py-1">Ceiling</th>
                  <th className="py-1">Value</th>
                  <th className="py-1">HR</th>
                  <th className="py-1">Own</th>
                  <th className="py-1">Team Tot</th>
                  <th className="py-1">Score</th>
                </tr>
              </thead>
              <tbody>
                {mlbBlowupCandidates.map(({ player, blowupScore, teamTotal, proj, ceiling, value, hrProb, projectedOwnership }) => (
                  <tr key={player.id} className="border-b border-gray-50 hover:bg-gray-50">
                    <td className="py-1 font-medium">{player.name}</td>
                    <td className="py-1 pl-2 text-gray-500">{player.eligiblePositions}</td>
                    <td className="py-1 text-right">${(player.salary / 1000).toFixed(1)}k</td>
                    <td className="py-1 text-right">{proj?.toFixed(1) ?? "—"}</td>
                    <td className="py-1 text-right text-blue-700">{ceiling?.toFixed(1) ?? "—"}</td>
                    <td className="py-1 text-right">{value?.toFixed(2) ?? "—"}</td>
                    <td className="py-1 text-right text-rose-700">{hrProb != null ? `${(hrProb * 100).toFixed(1)}%` : "—"}</td>
                    <td className="py-1 text-right">{projectedOwnership != null ? `${projectedOwnership.toFixed(1)}%` : "—"}</td>
                    <td className="py-1 text-right">{teamTotal?.toFixed(1) ?? "—"}</td>
                    <td className="py-1 text-right font-semibold text-green-700">{blowupScore.toFixed(1)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {sport === "mlb" && mlbHrTargets.length > 0 && (
        <div className="rounded-lg border bg-card p-4 space-y-3">
          <div>
            <h2 className="text-sm font-semibold">
              HR Targets
              <span
                className="ml-1.5 text-xs font-normal text-gray-400"
                title="Only hitters meeting the same 1+ HR threshold used for the inline HR badge in the player pool."
              >
                (Badge List)
              </span>
            </h2>
            <p className="mt-1 text-xs text-gray-500">
              Hitters currently qualifying for the DFS page HR badge, sorted by 1+ HR probability.
            </p>
          </div>
          <div className="overflow-x-auto">
            <table className="w-full text-xs border-collapse">
              <thead>
                <tr className="border-b text-gray-500 text-right">
                  <th className="py-1 text-left">Player</th>
                  <th className="py-1 text-left pl-2">Pos</th>
                  <th className="py-1 text-left pl-2">Team</th>
                  <th className="py-1">Order</th>
                  <th className="py-1">Salary</th>
                  <th className="py-1">1+ HR</th>
                  <th className="py-1">Exp HR</th>
                  <th className="py-1">Market</th>
                  <th className="py-1">Edge</th>
                  <th className="py-1">Team Tot</th>
                </tr>
              </thead>
              <tbody>
                {mlbHrTargets.map(({ player, badge, hrPct, expectedHr, marketProb, edgePct, teamTotal }) => (
                  <tr key={player.id} className="border-b border-gray-50 hover:bg-gray-50">
                    <td className="py-1 font-medium">
                      {player.name}
                      <span title={badge.title} className={`ml-2 inline-flex rounded px-1.5 py-0.5 text-[10px] font-medium align-middle ${badge.className}`}>
                        {badge.label}
                      </span>
                    </td>
                    <td className="py-1 pl-2 text-gray-500">{displayPos(player.eligiblePositions, sport)}</td>
                    <td className="py-1 pl-2 text-gray-500">{player.teamAbbrev}</td>
                    <td className="py-1 text-right">{player.dkStartingLineupOrder ?? "—"}</td>
                    <td className="py-1 text-right">{fmtSalary(player.salary)}</td>
                    <td className="py-1 text-right font-semibold text-rose-700">{hrPct != null ? `${hrPct}%` : "—"}</td>
                    <td className="py-1 text-right">{expectedHr != null ? expectedHr.toFixed(2) : "—"}</td>
                    <td className="py-1 text-right">{marketProb != null ? `${(marketProb * 100).toFixed(1)}%` : "—"}</td>
                    <td className={`py-1 text-right ${edgePct == null ? "text-gray-400" : edgePct >= 0 ? "text-emerald-700" : "text-red-500"}`}>
                      {fmtSignedPctPoint(edgePct)}
                    </td>
                    <td className="py-1 text-right">{teamTotal != null ? teamTotal.toFixed(1) : "—"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* Player Pool Table */}
      {filteredPlayers.length > 0 && (
        showHeavyPanels ? (
          <PlayerPoolTable
            sport={sport}
            visiblePlayers={visiblePlayerRows}
            playerPoolSourceCount={playerPoolSourcePlayers.length}
            unavailablePlayerCount={unavailablePlayerCount}
            hideOutInactivePlayers={hideOutInactivePlayers}
            mlbLineupSummary={mlbLineupSummary}
            mlbPlayerPoolFilter={mlbPlayerPoolFilter}
            onChangeMlbPlayerPoolFilter={handleMlbPlayerPoolFilterChange}
            onToggleHideOutInactive={toggleHideOutInactivePlayers}
            supportsRuleControls={supportsRuleControls}
            sortCol={sortCol}
            sortDir={sortDir}
            onToggleSort={toggleSort}
            lockedPlayerSet={lockedPlayerSet}
            blockedPlayerSet={blockedPlayerSet}
            blockedTeamSet={blockedTeamSet}
            requiredTeamStackMap={requiredTeamStackMap}
            nbaTopScorerRanks={nbaTopScorerRanks}
            nbaCeilingBadges={nbaCeilingBadges}
            mlbPitcherDecisionBadges={mlbPitcherDecisionBadges}
            mlbPitcherCeilingBadges={mlbPitcherCeilingBadges}
            manuallyOutSet={manuallyOutSet}
            manuallyOutAdjustments={manuallyOutAdjustments}
            onTogglePlayerLock={togglePlayerLock}
            onTogglePlayerBlock={togglePlayerBlock}
            onToggleManuallyOut={toggleManuallyOut}
          />
        ) : (
          <div className="rounded-lg border bg-card overflow-hidden">
            <div className="border-b px-4 py-3">
              <div className="h-4 w-48 animate-pulse rounded bg-gray-100" />
            </div>
            <div className="space-y-3 p-4">
              {Array.from({ length: 6 }).map((_, idx) => (
                <div key={idx} className="h-10 animate-pulse rounded bg-gray-50" />
              ))}
            </div>
          </div>
        )
      )}
      {/* Legacy inline player table retained temporarily for safe rollback. */}
      {false && filteredPlayers.length > 0 && (
        <div className="rounded-lg border bg-card overflow-hidden">
          <div className="flex items-start justify-between gap-3 px-4 py-3 border-b">
            <div>
              <h2 className="text-sm font-semibold">
                Player Pool - {hideOutInactivePlayers ? `${sortedPlayers.length} of ${playerPoolSourcePlayers.length}` : playerPoolSourcePlayers.length} players
                {unavailablePlayerCount > 0 && (
                  <span className="ml-2 text-xs text-red-500">
                    ({unavailablePlayerCount} OUT)
                  </span>
                )}
              </h2>
              {sport === "mlb" && (
                <div className="mt-2 flex flex-wrap gap-2 text-[11px] text-gray-600">
                  <span className="rounded border border-blue-200 bg-blue-50 px-1.5 py-0.5 text-blue-700">
                    {mlbLineupSummary.confirmedIn} confirmed hitters
                  </span>
                  <span className="rounded border border-amber-200 bg-amber-50 px-1.5 py-0.5 text-amber-700">
                    {mlbLineupSummary.pending} pending
                  </span>
                  <span className="rounded border border-red-200 bg-red-50 px-1.5 py-0.5 text-red-700">
                    {mlbLineupSummary.confirmedOut} out of lineup
                  </span>
                </div>
              )}
            </div>
            <div className="flex flex-col items-end gap-2">
              {sport === "mlb" && (
                <div className="flex flex-wrap justify-end gap-1">
                  {MLB_PLAYER_POOL_FILTER_OPTIONS.map((option) => (
                    <button
                      key={option.value}
                      type="button"
                      onClick={() => setMlbPlayerPoolFilter(option.value)}
                      className={`rounded border px-2 py-1 text-[11px] font-medium ${
                        mlbPlayerPoolFilter === option.value
                          ? "border-slate-300 bg-slate-700 text-white"
                          : "border-gray-300 text-gray-700 hover:bg-gray-50"
                      }`}
                    >
                      {option.label}
                    </button>
                  ))}
                </div>
              )}
              <button
                type="button"
                onClick={() => setHideOutInactivePlayers((current) => !current)}
                disabled={unavailablePlayerCount === 0}
                className={`rounded border px-3 py-1.5 text-xs font-medium ${
                  hideOutInactivePlayers
                    ? "border-blue-300 bg-blue-50 text-blue-700"
                    : "border-gray-300 text-gray-700 hover:bg-gray-50"
                } disabled:cursor-not-allowed disabled:opacity-50`}
              >
                {hideOutInactivePlayers ? "Show Out/Inactive" : "Hide Out/Inactive"}
              </button>
            </div>
          </div>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead className="border-b bg-gray-50">
                <tr>
                  <th className="px-3 py-2 text-left text-xs font-medium text-gray-500">Pos</th>
                  <SortHeader col="name" label="Player" />
                  <th className="px-3 py-2 text-left text-xs font-medium text-gray-500">Team</th>
                  {sport === "mlb" && (
                    <th className="px-3 py-2 text-left text-xs font-medium text-gray-500">Lineup</th>
                  )}
                  {sport === "mlb" && (
                    <th className="px-3 py-2 text-left text-xs font-medium text-gray-500">Odds</th>
                  )}
                  <th className="px-3 py-2 text-left text-xs font-medium text-gray-500">Props</th>
                  {supportsRuleControls && (
                    <th className="px-3 py-2 text-left text-xs font-medium text-gray-500">Rules</th>
                  )}
                  <SortHeader col="salary" label="Salary" />
                  <SortHeader col="avgFptsDk" label="DK Proj" />
                  <SortHeader col="linestarProj" label="LS Proj" />
                  {sport === "nba" && (
                    <>
                      <th className="px-3 py-2 text-left text-xs font-medium text-gray-500">Our Proj</th>
                      <th className="px-3 py-2 text-left text-xs font-medium text-gray-500">Market</th>
                    </>
                  )}
                  <SortHeader col="ourProj" label={sport === "nba" ? "Live Proj" : "Our Proj"} />
                  <SortHeader col="delta" label={sport === "nba" ? "Live Δ" : "Delta"} />
                  <SortHeader col="linestarOwnPct" label="LS Own%" />
                  <SortHeader col="projOwnPct" label="Field Own%" />
                  <SortHeader col="ourOwnPct" label="Our Own%" />
                  {sport === "nba" && <SortHeader col="liveOwnPct" label="Live Own%" />}
                  <SortHeader col="ourLeverage" label={sport === "nba" ? "Live Lev" : "Leverage"} />
                  <SortHeader col="value" label="Value" />
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100">
                {sortedPlayers.length === 0 && (
                  <tr>
                    <td colSpan={18} className="px-3 py-6 text-center text-sm text-gray-500">
                      No active players are visible with the current filter.
                    </td>
                  </tr>
                )}
                {sortedPlayers.slice(0, 200).map((p) => {
                  const ourProjDisplay = sport === "nba" ? (p.modelProj ?? p.ourProj) : p.ourProj;
                  const liveProjDisplay = sport === "nba"
                    ? (p.liveProj ?? p.blendProj ?? p.ourProj)
                    : p.ourProj;
                  const liveOwnDisplay = sport === "nba"
                    ? (p.liveOwnPct ?? p.projOwnPct ?? p.ourOwnPct)
                    : p.ourOwnPct;
                  const leverageDisplay = sport === "nba"
                    ? (p.liveLeverage ?? p.ourLeverage)
                    : p.ourLeverage;
                  const delta = liveProjDisplay != null && p.linestarProj != null
                    ? liveProjDisplay - p.linestarProj : null;
                  const value = liveProjDisplay != null ? liveProjDisplay / (p.salary / 1000) : null;
                  const propTokens = getPlayerPropTokens(p, sport);
                  const odds = getPlayerOddsContext(p);
                  const pos = displayPos(p.eligiblePositions, sport);
                  const mlbLineupBadge = sport === "mlb" ? getMlbLineupBadge(p) : null;
                  const mlbHrBadge = sport === "mlb" ? getMlbHrBadge(p) : null;
                  const mlbOrderBadge = sport === "mlb" ? getMlbOrderBadge(p) : null;
                  const nbaPointsBadge = sport === "nba" ? getNbaPointsBadge(p, nbaTopScorerRanks.get(p.id)) : null;
                  const rowUnavailable = sport === "mlb" ? isMlbRowUnavailable(p) : !!p.isOut;
                  const isLocked = lockedPlayerSet.has(p.id);
                  const isBlocked = blockedPlayerSet.has(p.id);
                  const isTeamBlocked = p.teamId != null && blockedTeamSet.has(p.teamId);
                  const stackSize = p.teamId != null ? requiredTeamStackMap.get(p.teamId) : undefined;
                  return (
                    <tr
                      key={p.id}
                      className={`hover:bg-gray-50 ${
                        rowUnavailable ? "opacity-40 line-through" : ""
                      } ${
                        isLocked ? "bg-blue-50" : isBlocked || isTeamBlocked ? "bg-red-50" : ""
                      }`}
                    >
                      <td className="px-3 py-1.5">
                        <span className={`inline-block rounded px-1.5 py-0.5 text-xs font-medium ${posBadgeColor(p.eligiblePositions, sport)}`}>
                          {pos}
                        </span>
                      </td>
                      <td className="px-3 py-1.5 font-medium">
                        {p.teamLogo && (
                          <img src={p.teamLogo} alt="" className="inline-block mr-1.5 h-4 w-4 align-middle" />
                        )}
                        {p.name}
                        {supportsRuleControls && (
                          <span className="ml-2 inline-flex flex-wrap gap-1 align-middle">
                            {isLocked && <span className="rounded bg-blue-100 px-1.5 py-0.5 text-[10px] font-medium text-blue-700">LOCK</span>}
                            {(isBlocked || isTeamBlocked) && <span className="rounded bg-red-100 px-1.5 py-0.5 text-[10px] font-medium text-red-700">BLOCK</span>}
                            {stackSize != null && <span className="rounded bg-emerald-100 px-1.5 py-0.5 text-[10px] font-medium text-emerald-700">STACK {stackSize}</span>}
                            {nbaPointsBadge && (
                              <span
                                title={nbaPointsBadge.title}
                                className={`rounded px-1.5 py-0.5 text-[10px] font-medium ${nbaPointsBadge.className}`}
                              >
                                {nbaPointsBadge.label}
                              </span>
                            )}
                            {mlbHrBadge && (
                              <span
                                title={mlbHrBadge.title}
                                className={`rounded px-1.5 py-0.5 text-[10px] font-medium ${mlbHrBadge.className}`}
                              >
                                {mlbHrBadge.label}
                              </span>
                            )}
                            {mlbOrderBadge && (
                              <span
                                title={mlbOrderBadge.title}
                                className={`rounded px-1.5 py-0.5 text-[10px] font-medium ${mlbOrderBadge.className}`}
                              >
                                {mlbOrderBadge.label}
                              </span>
                            )}
                          </span>
                        )}
                        {!supportsRuleControls && (
                          <>
                            {nbaPointsBadge && (
                              <span
                                title={nbaPointsBadge.title}
                                className={`ml-2 inline-flex rounded px-1.5 py-0.5 text-[10px] font-medium align-middle ${nbaPointsBadge.className}`}
                              >
                                {nbaPointsBadge.label}
                              </span>
                            )}
                            {mlbHrBadge && (
                              <span
                                title={mlbHrBadge.title}
                                className={`ml-2 inline-flex rounded px-1.5 py-0.5 text-[10px] font-medium align-middle ${mlbHrBadge.className}`}
                              >
                                {mlbHrBadge.label}
                              </span>
                            )}
                            {mlbOrderBadge && (
                              <span
                                title={mlbOrderBadge.title}
                                className={`ml-2 inline-flex rounded px-1.5 py-0.5 text-[10px] font-medium align-middle ${mlbOrderBadge.className}`}
                              >
                                {mlbOrderBadge.label}
                              </span>
                            )}
                          </>
                        )}
                      </td>
                      <td className="px-3 py-1.5 text-xs text-gray-500">{p.teamAbbrev}</td>
                      {sport === "mlb" && (
                        <td className="px-3 py-1.5 text-xs">
                          {mlbLineupBadge && (
                            <span className={`inline-flex rounded border px-1.5 py-0.5 text-[10px] font-medium ${mlbLineupBadge.className}`}>
                              {mlbLineupBadge.label}
                            </span>
                          )}
                        </td>
                      )}
                      {sport === "mlb" && (
                        <td className="px-3 py-1.5 text-[11px] text-gray-500">
                          {(odds.teamTotal != null || odds.vegasTotal != null || odds.moneyline != null) ? (
                            <div className="flex max-w-[200px] flex-wrap gap-1">
                              {odds.teamTotal != null && (
                                <span className="rounded border border-gray-200 bg-gray-50 px-1.5 py-0.5 font-mono text-[10px] text-gray-600">
                                  TT {odds.teamTotal.toFixed(1)}
                                </span>
                              )}
                              {odds.vegasTotal != null && (
                                <span className="rounded border border-gray-200 bg-gray-50 px-1.5 py-0.5 font-mono text-[10px] text-gray-600">
                                  O/U {odds.vegasTotal.toFixed(1)}
                                </span>
                              )}
                              {odds.moneyline != null && (
                                <span className="rounded border border-gray-200 bg-gray-50 px-1.5 py-0.5 font-mono text-[10px] text-gray-600">
                                  ML {fmtAmericanOdds(odds.moneyline)}
                                </span>
                              )}
                            </div>
                          ) : (
                            <span className="text-gray-400">—</span>
                          )}
                        </td>
                      )}
                      <td className="px-3 py-1.5 text-[11px] text-gray-500">
                        {propTokens.length > 0 ? (
                          <div className="flex max-w-[280px] flex-wrap gap-1">
                            {propTokens.map((prop) => (
                              <span
                                key={`${p.id}-${prop.stat}`}
                                className="rounded border border-gray-200 bg-gray-50 px-1.5 py-0.5 font-mono text-[10px] text-gray-600"
                              >
                                {prop.stat} {prop.line.toFixed(1)}
                                {prop.price != null ? ` (${fmtAmericanOdds(prop.price)})` : ""}
                                {prop.book ? ` ${shortBookName(prop.book)}` : ""}
                              </span>
                            ))}
                          </div>
                        ) : (
                          <span className="text-gray-400">—</span>
                        )}
                      </td>
                      {supportsRuleControls && (
                        <td className="px-3 py-1.5 text-xs">
                          <div className="flex flex-wrap gap-1">
                            <button
                              onClick={() => togglePlayerLock(p)}
                              disabled={isTeamBlocked && !isLocked}
                              className={`rounded border px-2 py-0.5 ${
                                isLocked
                                  ? "border-blue-300 bg-blue-100 text-blue-700"
                                  : "text-gray-600 hover:bg-gray-50"
                              } disabled:cursor-not-allowed disabled:opacity-50`}
                            >
                              {isLocked ? "Locked" : "Lock"}
                            </button>
                            <button
                              onClick={() => togglePlayerBlock(p)}
                              className={`rounded border px-2 py-0.5 ${
                                isBlocked
                                  ? "border-red-300 bg-red-100 text-red-700"
                                  : "text-gray-600 hover:bg-gray-50"
                              }`}
                            >
                              {isBlocked ? "Blocked" : "Block"}
                            </button>
                          </div>
                        </td>
                      )}
                      <td className="px-3 py-1.5 font-mono text-xs">{fmtSalary(p.salary)}</td>
                      <td className="px-3 py-1.5 text-xs text-gray-500">{fmt1(p.avgFptsDk)}</td>
                      <td className="px-3 py-1.5 text-xs">{fmt1(p.linestarProj)}</td>
                      {sport === "nba" && (
                        <>
                          <td className="px-3 py-1.5 text-xs">{fmt1(ourProjDisplay)}</td>
                          <td className="px-3 py-1.5 text-xs">{fmt1(p.marketProj)}</td>
                        </>
                      )}
                      <td className="px-3 py-1.5 text-xs font-medium">{fmt1(liveProjDisplay)}</td>
                      <td className={`px-3 py-1.5 text-xs font-medium ${
                        delta == null ? "text-gray-400" : delta >= 2 ? "text-green-600" : delta <= -2 ? "text-red-500" : ""
                      }`}>
                        {delta != null ? (delta >= 0 ? "+" : "") + delta.toFixed(1) : "—"}
                      </td>
                      <td className="px-3 py-1.5 text-xs">{(p.linestarOwnPct ?? p.projOwnPct) != null ? (p.linestarOwnPct ?? p.projOwnPct)!.toFixed(1) + "%" : "—"}</td>
                      <td className="px-3 py-1.5 text-xs">{p.projOwnPct != null ? p.projOwnPct.toFixed(1) + "%" : "—"}</td>
                      <td className="px-3 py-1.5 text-xs">{p.ourOwnPct != null ? p.ourOwnPct.toFixed(1) + "%" : "—"}</td>
                      {sport === "nba" && (
                        <td className="px-3 py-1.5 text-xs">{liveOwnDisplay != null ? liveOwnDisplay.toFixed(1) + "%" : "—"}</td>
                      )}
                      <td className={`px-3 py-1.5 text-xs font-medium ${
                        leverageDisplay == null ? "" :
                        leverageDisplay > 0 ? "text-green-700" : "text-red-400"
                      }`}>{fmt1(leverageDisplay)}</td>
                      <td className="px-3 py-1.5 text-xs text-gray-500">{value != null ? value.toFixed(2) : "—"}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* Generated Lineups */}
      {activeLineups.length > 0 && (
        <GeneratedLineupsSection
          sport={sport}
          activeLineups={activeLineups}
          lastRequestedLineupCount={lastRequestedLineupCount}
          strategy={strategy}
          onStrategyChange={handleStrategyChange}
          onSave={handleSave}
          saveMsg={saveMsg}
          onExport={handleExport}
          isExporting={isExporting}
          exportError={exportError}
        />
      )}
      {/* Legacy inline lineups block retained temporarily for safe rollback. */}
      {false && (((sport === "nba" ? lineups : mlbLineups) ?? []).length > 0) && (() => {
        const activeLineups = (sport === "nba" ? lineups : mlbLineups)!;
        const slotNames = sport === "nba"
          ? ["PG","SG","SF","PF","C","G","F","UTIL"]
          : ["P1","P2","C","1B","2B","3B","SS","OF1","OF2","OF3"];
        return (
        <div className="rounded-lg border bg-card p-4">
          <div className="flex items-center justify-between mb-3">
            <h2 className="text-sm font-semibold">
              {activeLineups.length}{lastRequestedLineupCount != null ? ` / ${lastRequestedLineupCount}` : ""} Lineups Generated
            </h2>
            <div className="flex items-center gap-3">
              <div className="flex items-center gap-2">
                <label className="text-xs text-gray-500">Strategy name</label>
                <input
                  value={strategy}
                  onChange={(e) => setStrategy(e.target.value)}
                  className="w-28 rounded border px-2 py-1 text-xs"
                />
              </div>
              <button
                onClick={handleSave}
                className="rounded border border-blue-600 px-3 py-1 text-xs text-blue-600 hover:bg-blue-50"
              >
                Save Lineups
              </button>
            </div>
          </div>
          {saveMsg && <p className="mb-2 text-xs text-green-600">{saveMsg}</p>}

          {/* Lineup cards */}
          <div className="space-y-2 max-h-[600px] overflow-y-auto pr-1">
            {activeLineups.map((lineup, i) => {
              const slots = lineup.slots as Record<string, typeof lineup.players[0]>;
              return (
              <div key={i} className="rounded border p-2 text-xs">
                <div className="flex items-center gap-4 mb-1 text-gray-500">
                  <span className="font-medium text-gray-800">#{i + 1}</span>
                  <span>Proj: <strong className="text-gray-800">{lineup.projFpts.toFixed(1)}</strong></span>
                  <span>Sal: <strong className="text-gray-800">${lineup.totalSalary.toLocaleString()}</strong></span>
                  <span>Lev: <strong className="text-gray-800">{lineup.leverageScore.toFixed(1)}</strong></span>
                </div>
                <div className="flex flex-wrap gap-1">
                  {slotNames.map((slot) => {
                    const p = slots[slot];
                    return p ? (
                        <span key={slot} className="inline-flex items-center gap-1 rounded bg-gray-100 px-1.5 py-0.5">
                          <span className="text-gray-400">{slot.replace(/\d$/, "")}</span>
                          {p.teamLogo && <img src={p.teamLogo} alt="" className="h-3 w-3" />}
                          <span className="font-medium">{p.name}</span>
                          <span className="font-mono text-gray-500">{fmtSalary(p.salary)}</span>
                          <span className="text-gray-400">{fmt1(p.ourProj)}</span>
                        </span>
                    ) : null;
                  })}
                </div>
              </div>
              );
            })}
          </div>

          {/* Multi-entry export */}
          <div className="mt-4 pt-4 border-t">
            <h3 className="text-xs font-semibold mb-2">Multi-Entry Export</h3>
            <p className="text-xs text-gray-500 mb-2">
              Exports the generated lineups directly as CSV with one row per lineup.
            </p>
            <button
              onClick={handleExport}
              disabled={isExporting}
              className="mt-2 rounded bg-blue-600 px-4 py-1.5 text-sm text-white hover:bg-blue-700 disabled:opacity-50"
            >
              {isExporting ? "Exporting…" : "Export CSV"}
            </button>
            {exportError && <p className="mt-2 text-xs text-red-600">{exportError}</p>}
          </div>
        </div>
        );
      })()}

    </div>
  );
}
