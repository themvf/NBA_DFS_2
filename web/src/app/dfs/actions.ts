"use server";

/**
 * Server actions for the NBA DFS optimizer page.
 *
 * processDkSlate  — parse DK CSV + LineStar CSV, compute projections, save to DB
 * runOptimizer    — run ILP optimizer with given settings, return lineups
 * saveLineups     — persist generated lineups to dk_lineups
 * exportLineups   — build lineup CSV string
 */

import { revalidatePath, revalidateTag } from "next/cache";
import { ANALYTICS_CACHE_TAG } from "@/db/analytics-cache";
import { db } from "@/db";
import { ensureDkPlayerPropColumns, ensureMlbBlowupTrackingTables, ensureMlbHomerunTrackingTables, ensureOddsHistoryTables, ensureOwnershipExperimentTables, ensureProjectionExperimentTables } from "@/db/ensure-schema";
import { teams, nbaTeamStats, nbaPlayerStats, nbaMatchups, dkSlates, dkPlayers, dkLineups, projectionRuns, projectionPlayerSnapshots, ownershipRuns, ownershipPlayerSnapshots, mlbBlowupRuns, mlbBlowupPlayerSnapshots, mlbHomerunRuns, mlbHomerunPlayerSnapshots, gameOddsHistory, playerPropHistory, mlbTeams, mlbTeamStats as mlbTeamStatsTable, mlbMatchups, mlbBatterStats, mlbPitcherStats, mlbParkFactors, type MlbBatterStats, type MlbPitcherStats, type MlbTeamStats, type MlbParkFactors } from "@/db/schema";
import { persistNbaOddsSignalReport } from "@/lib/nba-odds-signal";
import { normalizeDkSlateTiming } from "@/lib/dk-slate-timing";
import { eq, sql, and, desc, inArray } from "drizzle-orm";
import { optimizeLineups, optimizeLineupsWithDebug, buildMultiEntryCSV, probeOptimizerAll } from "./optimizer";
import type { OptimizerPlayer, OptimizerSettings, GeneratedLineup } from "./optimizer";
import { buildMlbBlowupCandidates, MLB_BLOWUP_CANDIDATE_VERSION } from "./mlb-blowup";
import { optimizeMlbLineups, optimizeMlbLineupsWithDebug, buildMlbMultiEntryCSV } from "./mlb-optimizer";
import { isTournamentMode } from "./optimizer-mode";
import { applyMlbPendingLineupPolicy, inferMlbTeamLineupConfirmed, isPositiveMlbLineupOrder } from "./mlb-lineup";
import { validateMlbRuleSelections } from "./mlb-optimizer-rules";
import { updateOptimizerJobLineupActualsForSlate } from "./optimizer-jobs";
import { loadMlbHitterProjectionCalibration } from "./mlb-projection-calibration";
import { applyMlbHitterProjectionCalibration } from "./mlb-projection-utils";
import { applyMlbOwnershipModelV1 } from "./mlb-ownership-model";
import type { OptimizerDebugInfo } from "./optimizer-debug";
import type { MlbOptimizerPlayer, MlbOptimizerSettings, MlbGeneratedLineup } from "./mlb-optimizer";
import type { Sport } from "@/db/queries";

/** Minimal lineup shape accepted by saveLineups — satisfied by both NBA and MLB lineup types. */
type LineupForSave = {
  players: Array<{ id: number; teamAbbrev: string }>;
  totalSalary: number;
  projFpts: number;
  leverageScore: number;
};

type OptimizerRunResult<T> = {
  ok: boolean;
  lineups?: T[];
  error?: string;
  warning?: string;
  debug?: OptimizerDebugInfo;
};

type CsvExportResult = {
  ok: boolean;
  csv?: string;
  error?: string;
};

type NbaPropAuditStat = "pts" | "reb" | "ast" | "blk" | "stl";
type NbaProjectionPropStat = NbaPropAuditStat;
type MlbPropAuditStat = "hits" | "tb" | "runs" | "rbis" | "hr" | "ks" | "outs" | "er";
type MlbProjectionPropStat = MlbPropAuditStat;
type ProjectionRunSource = "load_slate" | "fetch_props" | "recompute";

type PropBookCandidate = {
  bookmakerKey: string;
  bookmakerTitle: string;
  point: number;
  price: number | null;
};

export type NbaPropCoverageAuditBook = {
  bookmakerKey: string;
  bookmakerTitle: string;
  uniquePlayers: number;
  stats: Record<NbaPropAuditStat, number>;
};

export type NbaPropCoverageAuditLeader = {
  stat: NbaPropAuditStat;
  bookmakerKey: string;
  bookmakerTitle: string;
  count: number;
};

export type NbaPropCoverageAuditResult = {
  ok: boolean;
  message: string;
  selectedGames: string[];
  playerPoolCount: number;
  bookmakerCount?: number;
  books?: NbaPropCoverageAuditBook[];
  leaders?: NbaPropCoverageAuditLeader[];
};

export type MlbPropCoverageAuditBook = {
  bookmakerKey: string;
  bookmakerTitle: string;
  uniquePlayers: number;
  stats: Record<MlbPropAuditStat, number>;
};

export type MlbPropCoverageAuditLeader = {
  stat: MlbPropAuditStat;
  bookmakerKey: string;
  bookmakerTitle: string;
  count: number;
};

export type MlbPropCoverageAuditResult = {
  ok: boolean;
  message: string;
  selectedGames: string[];
  playerPoolCount: number;
  bookmakerCount?: number;
  books?: MlbPropCoverageAuditBook[];
  leaders?: MlbPropCoverageAuditLeader[];
};

const LEAGUE_AVG_PACE       = 100.0;
const LEAGUE_AVG_DEF_RTG   = 112.0;
const LEAGUE_AVG_TOTAL      = 228.0;
const LEAGUE_AVG_TEAM_TOTAL = 114.0;
const LEAGUE_AVG_USAGE      = 20.0;
const CURRENT_SEASON        = "2025-26";
const NBA_PROJECTION_MODEL_VERSION = "blend_v1";
const MLB_OWNERSHIP_MODEL_VERSION = "mlb_ownership_v1";
const MLB_HOMERUN_MODEL_VERSION = "mlb_homerun_v3";
const MAIN_LINE_TARGET_PROB = 110 / 210; // -110 hold-adjusted "main" line target
const NBA_PROP_MARKET_TO_STAT: Record<string, NbaPropAuditStat> = {
  player_points: "pts",
  player_rebounds: "reb",
  player_assists: "ast",
  player_blocks: "blk",
  player_steals: "stl",
};
const NBA_PROP_BOOK_PRIORITY: Record<NbaPropAuditStat, string[]> = {
  pts: ["fanduel", "caesars", "betrivers", "draftkings", "betonlineag", "betmgm", "bovada", "fanatics"],
  reb: ["fanduel", "betrivers", "caesars", "draftkings", "betonlineag", "betmgm", "bovada", "fanatics"],
  ast: ["fanduel", "betrivers", "draftkings", "betonlineag", "caesars", "betmgm", "bovada", "fanatics"],
  blk: ["betmgm", "fanduel", "draftkings", "bovada", "caesars", "betrivers", "betonlineag", "fanatics"],
  stl: ["fanduel", "draftkings", "bovada", "caesars", "betonlineag", "betmgm", "betrivers", "fanatics"],
};
const MLB_STATS_STALE_HOURS = 48;
const MLB_PROP_MARKET_TO_STAT: Record<string, MlbPropAuditStat> = {
  batter_hits: "hits",
  batter_total_bases: "tb",
  batter_runs_scored: "runs",
  batter_rbis: "rbis",
  batter_home_runs: "hr",
  pitcher_strikeouts: "ks",
  pitcher_outs: "outs",
  pitcher_earned_runs: "er",
};
const MLB_PROP_BOOK_PRIORITY: Record<MlbProjectionPropStat, string[]> = {
  hits: ["draftkings", "betonlineag", "betmgm", "bovada", "betrivers", "fanatics", "caesars", "fanduel", "mybookieag"],
  tb: ["betonlineag", "betmgm", "mybookieag", "caesars", "betrivers", "draftkings", "bovada", "fanatics", "fanduel"],
  runs: ["draftkings", "betmgm", "bovada", "fanatics", "betonlineag", "betrivers", "caesars", "mybookieag", "fanduel"],
  rbis: ["draftkings", "betmgm", "fanatics", "betonlineag", "betrivers", "caesars", "bovada", "mybookieag", "fanduel"],
  hr: ["betonlineag", "caesars", "betrivers", "draftkings", "betmgm", "bovada", "fanatics", "fanduel", "mybookieag"],
  ks: ["betmgm", "draftkings", "betonlineag", "bovada", "fanatics", "fanduel", "caesars", "betrivers", "mybookieag"],
  outs: ["betmgm", "draftkings", "bovada", "fanduel", "betonlineag", "caesars", "betrivers", "fanatics", "mybookieag"],
  er: ["betmgm", "draftkings", "betonlineag", "bovada", "fanatics", "fanduel", "caesars", "betrivers", "mybookieag"],
};

function finiteOrNull(value: number | null | undefined): number | null {
  return value != null && Number.isFinite(value) ? value : null;
}

function sanitizeProjection(value: number | null | undefined): number | null {
  const finite = finiteOrNull(value);
  return finite == null ? null : Math.max(0, finite);
}

function sanitizeOwnershipPct(value: number | null | undefined): number | null {
  const finite = finiteOrNull(value);
  return finite == null ? null : Math.max(0, Math.min(100, finite));
}

function sanitizeProbability(value: number | null | undefined): number | null {
  const finite = finiteOrNull(value);
  return finite == null ? null : Math.max(0, Math.min(0.9999, finite));
}

function sanitizeLeverage(value: number | null | undefined): number | null {
  return finiteOrNull(value);
}

function parseSlateGameKey(gameInfo: string | null): string {
  return gameInfo?.split(" ")[0] ?? "Unknown";
}

function americanToImpliedProbability(price: number): number {
  return price > 0 ? 100 / (price + 100) : Math.abs(price) / (Math.abs(price) + 100);
}

function normalizeBookPreferenceKey(value: string | null | undefined): string {
  return (value ?? "").toLowerCase().replace(/[^a-z0-9]/g, "");
}

function propBookPriority(
  stat: NbaProjectionPropStat,
  candidate: Pick<PropBookCandidate, "bookmakerKey" | "bookmakerTitle">,
): number {
  const normalizedKey = normalizeBookPreferenceKey(candidate.bookmakerKey);
  const normalizedTitle = normalizeBookPreferenceKey(candidate.bookmakerTitle);
  const idx = NBA_PROP_BOOK_PRIORITY[stat].findIndex((book) => book === normalizedKey || book === normalizedTitle);
  return idx === -1 ? Number.MAX_SAFE_INTEGER : idx;
}

function mainLineDistance(price: number | null): number {
  if (price == null) return Number.POSITIVE_INFINITY;
  return Math.abs(americanToImpliedProbability(price) - MAIN_LINE_TARGET_PROB);
}

function compareMainLineCandidates(a: Pick<PropBookCandidate, "point" | "price">, b: Pick<PropBookCandidate, "point" | "price">): number {
  const distanceDiff = mainLineDistance(a.price) - mainLineDistance(b.price);
  if (distanceDiff !== 0) return distanceDiff;

  const priceAbsDiff = Math.abs(Math.abs(a.price ?? 0) - 110) - Math.abs(Math.abs(b.price ?? 0) - 110);
  if (priceAbsDiff !== 0) return priceAbsDiff;

  return a.point - b.point;
}

function pickPreferredPropLine(stat: NbaProjectionPropStat, candidates: PropBookCandidate[]): PropBookCandidate | null {
  if (candidates.length === 0) return null;

  const bestByBook = new Map<string, PropBookCandidate>();
  for (const candidate of candidates) {
    const bookKey = normalizeBookPreferenceKey(candidate.bookmakerKey) || normalizeBookPreferenceKey(candidate.bookmakerTitle);
    const existing = bestByBook.get(bookKey);
    if (!existing || compareMainLineCandidates(candidate, existing) < 0) {
      bestByBook.set(bookKey, candidate);
    }
  }

  return Array.from(bestByBook.values()).sort((a, b) => {
    const priorityDiff = propBookPriority(stat, a) - propBookPriority(stat, b);
    if (priorityDiff !== 0) return priorityDiff;

    const qualityDiff = compareMainLineCandidates(a, b);
    if (qualityDiff !== 0) return qualityDiff;

    return a.bookmakerTitle.localeCompare(b.bookmakerTitle);
  })[0] ?? null;
}

function mlbPropBookPriority(
  stat: MlbProjectionPropStat,
  candidate: Pick<PropBookCandidate, "bookmakerKey" | "bookmakerTitle">,
): number {
  const normalizedKey = normalizeBookPreferenceKey(candidate.bookmakerKey);
  const normalizedTitle = normalizeBookPreferenceKey(candidate.bookmakerTitle);
  const idx = MLB_PROP_BOOK_PRIORITY[stat].findIndex((book) => book === normalizedKey || book === normalizedTitle);
  return idx === -1 ? Number.MAX_SAFE_INTEGER : idx;
}

function pickPreferredMlbPropLine(stat: MlbProjectionPropStat, candidates: PropBookCandidate[]): PropBookCandidate | null {
  if (candidates.length === 0) return null;

  const bestByBook = new Map<string, PropBookCandidate>();
  for (const candidate of candidates) {
    const bookKey = normalizeBookPreferenceKey(candidate.bookmakerKey) || normalizeBookPreferenceKey(candidate.bookmakerTitle);
    const existing = bestByBook.get(bookKey);
    if (!existing || compareMainLineCandidates(candidate, existing) < 0) {
      bestByBook.set(bookKey, candidate);
    }
  }

  return Array.from(bestByBook.values()).sort((a, b) => {
    const priorityDiff = mlbPropBookPriority(stat, a) - mlbPropBookPriority(stat, b);
    if (priorityDiff !== 0) return priorityDiff;

    const qualityDiff = compareMainLineCandidates(a, b);
    if (qualityDiff !== 0) return qualityDiff;

    return a.bookmakerTitle.localeCompare(b.bookmakerTitle);
  })[0] ?? null;
}

function roundHalf(value: number): number {
  return Math.round(value * 2) / 2;
}

type GameOddsHistoryInput = {
  sport: "nba" | "mlb";
  matchupId: number;
  eventId?: string | null;
  gameDate: string;
  homeTeamId?: number | null;
  awayTeamId?: number | null;
  homeTeamName?: string | null;
  awayTeamName?: string | null;
  bookmakerCount: number;
  homeMl?: number | null;
  awayMl?: number | null;
  homeSpread?: number | null;
  vegasTotal?: number | null;
  homeWinProb?: number | null;
  homeImplied?: number | null;
  awayImplied?: number | null;
};

type PlayerPropHistoryInput = {
  sport: "nba" | "mlb";
  slateId: number | null;
  dkPlayerId: number;
  playerName: string;
  teamId?: number | null;
  eventId?: string | null;
  marketKey: string;
  line?: number | null;
  price?: number | null;
  bookmakerKey?: string | null;
  bookmakerTitle?: string | null;
  bookCount: number;
};

async function recordGameOddsHistory(rows: GameOddsHistoryInput[]): Promise<void> {
  if (rows.length === 0) return;
  await ensureOddsHistoryTables();
  const captureKey = new Date().toISOString();
  await db.insert(gameOddsHistory).values(
    rows.map((row) => ({
      sport: row.sport,
      matchupId: row.matchupId,
      eventId: row.eventId ?? null,
      gameDate: row.gameDate,
      homeTeamId: row.homeTeamId ?? null,
      awayTeamId: row.awayTeamId ?? null,
      homeTeamName: row.homeTeamName ?? null,
      awayTeamName: row.awayTeamName ?? null,
      bookmakerCount: row.bookmakerCount,
      homeMl: row.homeMl ?? null,
      awayMl: row.awayMl ?? null,
      homeSpread: row.homeSpread ?? null,
      vegasTotal: row.vegasTotal ?? null,
      homeWinProb: row.homeWinProb ?? null,
      homeImplied: row.homeImplied ?? null,
      awayImplied: row.awayImplied ?? null,
      captureKey,
    })),
  ).onConflictDoNothing();
}

async function recordPlayerPropHistory(rows: PlayerPropHistoryInput[]): Promise<void> {
  if (rows.length === 0) return;
  await ensureOddsHistoryTables();
  const captureKey = new Date().toISOString();
  await db.insert(playerPropHistory).values(
    rows.map((row) => ({
      sport: row.sport,
      slateId: row.slateId,
      dkPlayerId: row.dkPlayerId,
      playerName: row.playerName,
      teamId: row.teamId ?? null,
      eventId: row.eventId ?? null,
      marketKey: row.marketKey,
      line: row.line ?? null,
      price: row.price ?? null,
      bookmakerKey: row.bookmakerKey ?? null,
      bookmakerTitle: row.bookmakerTitle ?? null,
      bookCount: row.bookCount,
      captureKey,
    })),
  ).onConflictDoNothing();
}

type NbaPlayerOddsMovement = {
  propDeltas: Partial<Record<NbaProjectionPropStat, number>>;
  marketFptsDelta: number;
};

type NbaMatchupOddsMovement = {
  vegasTotalDelta: number;
  homeSpreadDelta: number | null;
};

type NbaOddsMovementContext = {
  playerByDkId: Map<number, NbaPlayerOddsMovement>;
  matchupById: Map<number, NbaMatchupOddsMovement>;
};

const NBA_PROP_HISTORY_MARKETS: Record<NbaProjectionPropStat, string> = {
  pts: "player_points",
  reb: "player_rebounds",
  ast: "player_assists",
  blk: "player_blocks",
  stl: "player_steals",
};

async function buildNbaOddsMovementContext(slateId: number, slateDate: string): Promise<NbaOddsMovementContext> {
  await ensureOddsHistoryTables();

  const [propRows, gameRows] = await Promise.all([
    db.select({
      dkPlayerId: playerPropHistory.dkPlayerId,
      marketKey: playerPropHistory.marketKey,
      line: playerPropHistory.line,
      capturedAt: playerPropHistory.capturedAt,
      id: playerPropHistory.id,
    })
      .from(playerPropHistory)
      .where(and(eq(playerPropHistory.sport, "nba"), eq(playerPropHistory.slateId, slateId))),
    db.select({
      matchupId: gameOddsHistory.matchupId,
      vegasTotal: gameOddsHistory.vegasTotal,
      homeSpread: gameOddsHistory.homeSpread,
      capturedAt: gameOddsHistory.capturedAt,
      id: gameOddsHistory.id,
    })
      .from(gameOddsHistory)
      .where(and(eq(gameOddsHistory.sport, "nba"), eq(gameOddsHistory.gameDate, slateDate))),
  ]);

  const playerByDkId = new Map<number, NbaPlayerOddsMovement>();
  const propRowsByKey = new Map<string, typeof propRows>();
  for (const row of propRows) {
    const key = `${row.dkPlayerId}|${row.marketKey}`;
    const bucket = propRowsByKey.get(key) ?? [];
    bucket.push(row);
    propRowsByKey.set(key, bucket);
  }
  for (const [key, rows] of propRowsByKey) {
    rows.sort((a, b) => {
      const timeDiff = (a.capturedAt?.getTime() ?? 0) - (b.capturedAt?.getTime() ?? 0);
      if (timeDiff !== 0) return timeDiff;
      return a.id - b.id;
    });
    const first = rows[0];
    const last = rows[rows.length - 1];
    if (first?.line == null || last?.line == null || rows.length < 2) continue;
    const delta = Math.round((last.line - first.line) * 100) / 100;
    if (!delta) continue;

    const [dkPlayerIdRaw, marketKey] = key.split("|");
    const dkPlayerId = Number(dkPlayerIdRaw);
    const stat = Object.entries(NBA_PROP_HISTORY_MARKETS).find(([, value]) => value === marketKey)?.[0] as NbaProjectionPropStat | undefined;
    if (!stat) continue;

    const entry = playerByDkId.get(dkPlayerId) ?? { propDeltas: {}, marketFptsDelta: 0 };
    entry.propDeltas[stat] = delta;
    const dkWeight = stat === "pts" ? 1 : stat === "reb" ? 1.25 : stat === "ast" ? 1.5 : 2;
    entry.marketFptsDelta = Math.round((entry.marketFptsDelta + delta * dkWeight) * 100) / 100;
    playerByDkId.set(dkPlayerId, entry);
  }

  const matchupById = new Map<number, NbaMatchupOddsMovement>();
  const gameRowsByMatchup = new Map<number, typeof gameRows>();
  for (const row of gameRows) {
    const bucket = gameRowsByMatchup.get(row.matchupId) ?? [];
    bucket.push(row);
    gameRowsByMatchup.set(row.matchupId, bucket);
  }
  for (const [matchupId, rows] of gameRowsByMatchup) {
    rows.sort((a, b) => {
      const timeDiff = (a.capturedAt?.getTime() ?? 0) - (b.capturedAt?.getTime() ?? 0);
      if (timeDiff !== 0) return timeDiff;
      return a.id - b.id;
    });
    const first = rows[0];
    const last = rows[rows.length - 1];
    if (!first || !last || rows.length < 2) continue;
    const vegasTotalDelta = first.vegasTotal != null && last.vegasTotal != null
      ? Math.round((last.vegasTotal - first.vegasTotal) * 100) / 100
      : 0;
    const homeSpreadDelta = first.homeSpread != null && last.homeSpread != null
      ? Math.round((last.homeSpread - first.homeSpread) * 100) / 100
      : null;
    if (!vegasTotalDelta && !homeSpreadDelta) continue;
    matchupById.set(matchupId, { vegasTotalDelta, homeSpreadDelta });
  }

  return { playerByDkId, matchupById };
}

function extractOverOutcomePlayerName(
  outcome: { name?: string | null; description?: string | null; point?: number | null },
): string | null {
  if (outcome.point == null) return null;
  const overField =
    outcome.name?.toLowerCase() === "over"
      ? "name"
      : outcome.description?.toLowerCase() === "over"
        ? "description"
        : null;
  if (!overField) return null;
  return overField === "name"
    ? outcome.description?.trim() ?? null
    : outcome.name?.trim() ?? null;
}

function buildPartialGenerationWarning<T extends { players: Array<{ id: number; name: string }> }>(
  lineups: T[],
  requested: number,
  maxExposure: number,
  exposureRelaxedHelps: boolean,
): string | undefined {
  if (lineups.length === 0 || lineups.length >= requested) return undefined;

  const base = `Built ${lineups.length} of ${requested} lineups.`;
  if (!exposureRelaxedHelps || maxExposure >= 1) {
    return `${base} Additional lineups were infeasible under the current constraints.`;
  }

  const maxExposureCount = Math.ceil(requested * maxExposure);
  const exposureCounts = new Map<number, { name: string; count: number }>();
  for (const lineup of lineups) {
    for (const player of lineup.players) {
      const current = exposureCounts.get(player.id);
      if (current) current.count += 1;
      else exposureCounts.set(player.id, { name: player.name, count: 1 });
    }
  }

  const capped = Array.from(exposureCounts.values())
    .filter((player) => player.count >= maxExposureCount)
    .sort((a, b) => b.count - a.count || a.name.localeCompare(b.name));
  const summary = capped.length > 0
    ? `${capped.length} players hit the ${maxExposureCount}-lineup cap: ${capped.slice(0, 6).map((player) => player.name).join(", ")}${capped.length > 6 ? ` +${capped.length - 6} more` : ""}.`
    : `One or more players hit the ${maxExposureCount}-lineup cap.`;

  return `${base} Exposure cap (${Math.round(maxExposure * 100)}%) blocked additional lineups. ${summary}`;
}

// ── NBA abbreviation overrides (DK → standard) ──────────────
const DK_OVERRIDES: Record<string, string> = {
  GS: "GSW", SA: "SAS", NO: "NOP", NY: "NYK",
  PHO: "PHX", OKL: "OKC", UTH: "UTA",
};

// ── CSV Parsers ───────────────────────────────────────────────

function parseDkCsv(content: string): DkApiPlayer[] {
  const lines = content.split(/\r?\n/).filter(Boolean);
  if (lines.length < 2) return [];

  const header = lines[0].split(",").map((h) => h.trim().replace(/^"|"$/g, ""));
  const col = (name: string) => header.findIndex((h) => h === name);

  const nameCol      = col("Name");
  const idCol        = col("ID");
  const salaryCol    = col("Salary");
  const rosterPosCol = col("Roster Position");
  const teamCol      = col("TeamAbbrev");
  const gameInfoCol  = col("Game Info");
  const avgCol       = col("AvgPointsPerGame");

  const players = [];
  for (let i = 1; i < lines.length; i++) {
    const cells = lines[i].split(",").map((c) => c.trim().replace(/^"|"$/g, ""));
    const name   = cells[nameCol] ?? "";
    const idStr  = cells[idCol] ?? "";
    if (!name || !idStr) continue;
    const salaryStr = (cells[salaryCol] ?? "0").replace(/[^0-9]/g, "");
    players.push({
      name,
      dkId:              parseInt(idStr, 10),
      teamAbbrev:        (cells[teamCol] ?? "").toUpperCase(),
      eligiblePositions: cells[rosterPosCol] ?? "UTIL",
      salary:            parseInt(salaryStr, 10) || 0,
      gameInfo:          cells[gameInfoCol] ?? "",
      avgFptsDk:         parseFloat(cells[avgCol] ?? "") || null,
      // CSV doesn't carry DK injury status — rely on LineStar for is_out
      dkStatus:    "None",
      isDisabled:  false,
      startingLineupOrder: null,
      inStartingLineup: null,
      probableStarter: null,
      likelyPitcher: null,
      startingPitcher: null,
    });
  }
  return players;
}

type LinestarEntry = { linestarProj: number; projOwnPct: number; isOut: boolean };

function parseLinestarCsv(content: string): Map<string, LinestarEntry> {
  const lines = content.split(/\r?\n/).filter(Boolean);
  const map = new Map<string, LinestarEntry>();
  for (let i = 1; i < lines.length; i++) {
    const cells = lines[i].split(",").map((c) => c.trim());
    if (cells.length < 8) continue;
    const playerName = cells[2] ?? "";
    const salaryStr  = (cells[3] ?? "").replace(/[^0-9]/g, "");
    const projOwnStr = (cells[4] ?? "").replace("%", "");
    const projStr    = cells[7] ?? "";
    if (!playerName) continue;
    const proj    = parseFloat(projStr)    || 0;
    const projOwn = parseFloat(projOwnStr) || 0;
    if (proj === 0 && projOwn === 0) continue;
    const salary = parseInt(salaryStr, 10) || 0;
    const isOut  = proj === 0;
    map.set(`${playerName.toLowerCase()}|${salary}`, { linestarProj: proj, projOwnPct: projOwn, isOut });
  }
  return map;
}

/** Parse tab-separated data pasted directly from the LineStar web table.
 *  Columns: Pos, Team, Player, Salary, projOwn%, actualOwn%, Diff, Proj
 *  Uses the Salary cell ($NNNNN) as an anchor — position-independent. */
/** Normalize a player name for robust matching:
 *  lowercase → strip periods/apostrophes → remove Jr/Sr/II/III → sort tokens.
 *  "De'Aaron Fox" → "aaron dearron fox" (tokens sorted)
 *  "E.J. Harkless" → "ej harkless"
 *  "Nickeil Alexander-Walker" → "alexanderwalker nickeil"
 */
function normalizeName(name: string): string {
  return name
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[.,']/g, "")
    .replace(/\b(jr|sr|ii|iii|iv)\b/g, "")
    .replace(/[-]/g, "")
    .replace(/\s+/g, " ")
    .trim()
    .split(" ")
    .filter(Boolean)
    .sort()
    .join(" ");
}

function canonicalizeName(name: string): string {
  return name
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[.,']/g, "")
    .replace(/\b(jr|sr|ii|iii|iv)\b/g, "")
    .replace(/[-]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function canonicalizeTeamName(name: string): string {
  return name
    .toLowerCase()
    .replace(/[.']/g, "")
    .replace(/[-]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function looksLikeTeamAbbrev(value: string): boolean {
  return /^[A-Z]{2,4}$/.test(value.trim().toUpperCase());
}

type HistoricalTeamCandidate = {
  teamId: number;
  teamAbbrev: string;
  canonicalName: string;
  normalizedName: string;
  firstInitial: string;
  lastToken: string;
  sourceRank: number;
};

type HistoricalTeamResolution = {
  teamId: number | null;
  teamAbbrev: string;
};

type NbaHistoricalTeamResolver = {
  abbrevToId: Map<string, number>;
  byCanonicalName: Map<string, HistoricalTeamCandidate[]>;
  byNormalizedName: Map<string, HistoricalTeamCandidate[]>;
  candidates: HistoricalTeamCandidate[];
};

function buildHistoricalTeamCandidate(
  name: string,
  teamId: number | null,
  teamAbbrev: string | null,
  sourceRank: number,
): HistoricalTeamCandidate | null {
  if (!teamId || !teamAbbrev) return null;
  const canonicalTeamAbbrev = (DK_OVERRIDES[teamAbbrev.toUpperCase()] ?? teamAbbrev.toUpperCase()).trim();
  if (!canonicalTeamAbbrev) return null;
  const canonicalName = canonicalizeName(name);
  const normalizedName = normalizeName(name);
  const tokens = canonicalName.split(" ").filter(Boolean);
  if (!canonicalName || !normalizedName || tokens.length === 0) return null;
  return {
    teamId,
    teamAbbrev: canonicalTeamAbbrev,
    canonicalName,
    normalizedName,
    firstInitial: tokens[0]?.[0] ?? "",
    lastToken: tokens[tokens.length - 1] ?? "",
    sourceRank,
  };
}

function chooseHistoricalTeamCandidate(candidates: HistoricalTeamCandidate[] | undefined): HistoricalTeamCandidate | null {
  if (!candidates || candidates.length === 0) return null;
  const byTeam = new Map<string, HistoricalTeamCandidate>();
  for (const candidate of candidates) {
    const existing = byTeam.get(candidate.teamAbbrev);
    if (!existing || candidate.sourceRank < existing.sourceRank) {
      byTeam.set(candidate.teamAbbrev, candidate);
    }
  }
  const unique = Array.from(byTeam.values()).sort((a, b) => a.sourceRank - b.sourceRank || a.teamAbbrev.localeCompare(b.teamAbbrev));
  return unique.length === 1 ? unique[0] : null;
}

async function buildNbaHistoricalTeamResolver(abbrevToId: Map<string, number>): Promise<NbaHistoricalTeamResolver> {
  const statRows = await db.execute<{ name: string; teamId: number | null; teamAbbrev: string | null }>(sql`
    SELECT DISTINCT ON (lower(nps.name))
      nps.name AS "name",
      nps.team_id AS "teamId",
      t.abbreviation AS "teamAbbrev"
    FROM nba_player_stats nps
    LEFT JOIN teams t ON t.team_id = nps.team_id
    WHERE nps.team_id IS NOT NULL
    ORDER BY lower(nps.name), nps.season DESC, nps.id DESC
  `);

  const recentSlateRows = await db.execute<{ name: string; teamId: number | null; teamAbbrev: string | null }>(sql`
    SELECT DISTINCT ON (lower(dp.name))
      dp.name AS "name",
      dp.team_id AS "teamId",
      dp.team_abbrev AS "teamAbbrev"
    FROM dk_players dp
    JOIN dk_slates ds ON ds.id = dp.slate_id
    WHERE ds.sport = 'nba'
      AND dp.team_id IS NOT NULL
      AND dp.team_abbrev IS NOT NULL
      AND dp.team_abbrev <> 'UNK'
    ORDER BY lower(dp.name), ds.slate_date DESC, dp.id DESC
  `);

  const candidates: HistoricalTeamCandidate[] = [];
  const pushCandidate = (name: string, teamId: number | null, teamAbbrev: string | null, sourceRank: number) => {
    const candidate = buildHistoricalTeamCandidate(name, teamId, teamAbbrev, sourceRank);
    if (candidate) candidates.push(candidate);
  };

  for (const row of recentSlateRows.rows) pushCandidate(row.name, row.teamId, row.teamAbbrev, 0);
  for (const row of statRows.rows) pushCandidate(row.name, row.teamId, row.teamAbbrev, 1);

  const byCanonicalName = new Map<string, HistoricalTeamCandidate[]>();
  const byNormalizedName = new Map<string, HistoricalTeamCandidate[]>();
  for (const candidate of candidates) {
    const canonicalGroup = byCanonicalName.get(candidate.canonicalName) ?? [];
    canonicalGroup.push(candidate);
    byCanonicalName.set(candidate.canonicalName, canonicalGroup);

    const normalizedGroup = byNormalizedName.get(candidate.normalizedName) ?? [];
    normalizedGroup.push(candidate);
    byNormalizedName.set(candidate.normalizedName, normalizedGroup);
  }

  return {
    abbrevToId,
    byCanonicalName,
    byNormalizedName,
    candidates,
  };
}

function resolveHistoricalNbaTeam(
  resolver: NbaHistoricalTeamResolver,
  playerName: string,
  rawTeamAbbrev: string,
): HistoricalTeamResolution {
  const cleanAbbrev = rawTeamAbbrev.toUpperCase().replace(/[^A-Z]/g, "");
  if (cleanAbbrev) {
    const canonicalAbbrev = DK_OVERRIDES[cleanAbbrev] ?? cleanAbbrev;
    const exactTeamId = resolver.abbrevToId.get(canonicalAbbrev);
    if (exactTeamId) {
      return { teamId: exactTeamId, teamAbbrev: canonicalAbbrev };
    }
  }

  const canonicalName = canonicalizeName(playerName);
  const normalizedName = normalizeName(playerName);
  const exactCanonical = chooseHistoricalTeamCandidate(resolver.byCanonicalName.get(canonicalName));
  if (exactCanonical) {
    return { teamId: exactCanonical.teamId, teamAbbrev: exactCanonical.teamAbbrev };
  }

  const exactNormalized = chooseHistoricalTeamCandidate(resolver.byNormalizedName.get(normalizedName));
  if (exactNormalized) {
    return { teamId: exactNormalized.teamId, teamAbbrev: exactNormalized.teamAbbrev };
  }

  const tokens = canonicalName.split(" ").filter(Boolean);
  const firstInitial = tokens[0]?.[0] ?? "";
  const lastToken = tokens[tokens.length - 1] ?? "";
  if (firstInitial && lastToken) {
    let best: HistoricalTeamCandidate | null = null;
    let bestDist = 3;
    let tied = false;
    for (const candidate of resolver.candidates) {
      if (candidate.firstInitial !== firstInitial || candidate.lastToken !== lastToken) continue;
      const dist = levenshtein(canonicalName, candidate.canonicalName);
      if (dist < bestDist) {
        best = candidate;
        bestDist = dist;
        tied = false;
      } else if (best && dist === bestDist && candidate.teamAbbrev !== best.teamAbbrev) {
        tied = true;
      }
    }
    if (best && !tied) {
      return { teamId: best.teamId, teamAbbrev: best.teamAbbrev };
    }
  }

  return { teamId: null, teamAbbrev: cleanAbbrev || "UNK" };
}

async function refreshHistoricalSlateGameCount(slateId: number, sport: Sport): Promise<void> {
  if (sport === "mlb") return;
  const result = await db.execute<{ teamCount: number }>(sql`
    SELECT COUNT(DISTINCT dp.team_id) AS "teamCount"
    FROM dk_players dp
    WHERE dp.slate_id = ${slateId}
      AND dp.team_id IS NOT NULL
  `);
  const teamCount = Number(result.rows[0]?.teamCount ?? 0);
  if (teamCount >= 2) {
    await db.update(dkSlates)
      .set({ gameCount: Math.max(1, Math.round(teamCount / 2)) })
      .where(eq(dkSlates.id, slateId));
  }
}

type PropMatchCandidate = {
  id: number;
  dkPlayerId: number;
  name: string;
  teamId: number | null;
  canonicalName: string;
  normalizedName: string;
  firstInitial: string;
  lastToken: string;
};

function buildPropMatchCandidate(player: { id: number; dkPlayerId: number; name: string; teamId: number | null }): PropMatchCandidate {
  const canonicalName = canonicalizeName(player.name);
  const tokens = canonicalName.split(" ").filter(Boolean);
  return {
    id: player.id,
    dkPlayerId: player.dkPlayerId,
    name: player.name,
    teamId: player.teamId,
    canonicalName,
    normalizedName: normalizeName(player.name),
    firstInitial: tokens[0]?.[0] ?? "",
    lastToken: tokens[tokens.length - 1] ?? "",
  };
}

function matchPropToCandidates(playerName: string, candidates: PropMatchCandidate[]): PropMatchCandidate | null {
  if (candidates.length === 0) return null;

  const canonicalName = canonicalizeName(playerName);
  const normalizedName = normalizeName(playerName);

  const exactCanonical = candidates.find((candidate) => candidate.canonicalName === canonicalName);
  if (exactCanonical) return exactCanonical;

  const exactNormalized = candidates.find((candidate) => candidate.normalizedName === normalizedName);
  if (exactNormalized) return exactNormalized;

  const tokens = canonicalName.split(" ").filter(Boolean);
  const firstInitial = tokens[0]?.[0] ?? "";
  const lastToken = tokens[tokens.length - 1] ?? "";
  if (!firstInitial || !lastToken) return null;

  let best: PropMatchCandidate | null = null;
  let bestDist = 3;
  for (const candidate of candidates) {
    if (candidate.firstInitial !== firstInitial || candidate.lastToken !== lastToken) continue;
    const dist = levenshtein(canonicalName, candidate.canonicalName);
    if (dist < bestDist) {
      bestDist = dist;
      best = candidate;
    }
  }
  return best;
}

function parseLinestarPasteText(text: string): Map<string, LinestarEntry> {
  const lines = text.split(/\r?\n/).filter(Boolean);
  const map = new Map<string, LinestarEntry>();
  for (const line of lines) {
    const cells = line.split("\t").map((c) => c.trim());
    // Anchor on the salary cell — handle both "$4900" and "$4,900" browser clipboard formats
    const salaryIdx = cells.findIndex((c) => /^\$[\d,]{4,7}$/.test(c));
    if (salaryIdx < 1) continue;
    // Handle two LineStar formats:
    //   "Pos | Team | Player | Salary | ..." → cells[salaryIdx-1] = player
    //   "Pos | Player | Team | Salary | ..." → cells[salaryIdx-1] = team abbrev (2-4 caps)
    // If the cell before salary looks like a team abbreviation, step back one more.
    let playerName = cells[salaryIdx - 1];
    if (/^[A-Z]{2,4}$/.test(playerName) && salaryIdx >= 2) {
      playerName = cells[salaryIdx - 2];
    }
    if (!playerName || playerName.toLowerCase() === "player") continue; // skip header
    const salary  = parseInt(cells[salaryIdx].replace(/\D/g, ""), 10);
    if (!salary) continue;
    // projOwn% is the column immediately after salary
    const projOwn = parseFloat((cells[salaryIdx + 1] ?? "").replace("%", "")) || 0;
    // Proj is 4 columns after salary: projOwn%, actualOwn%, Diff, Proj
    const proj    = parseFloat(cells[salaryIdx + 4] ?? "") || 0;
    const isOut   = proj === 0;
    map.set(`${playerName.toLowerCase()}|${salary}`, { linestarProj: proj, projOwnPct: projOwn, isOut });
  }
  return map;
}

// Simple Levenshtein for fuzzy name matching
function levenshtein(a: string, b: string): number {
  const m = a.length, n = b.length;
  const dp: number[][] = Array.from({ length: m + 1 }, (_, i) =>
    Array.from({ length: n + 1 }, (_, j) => (i === 0 ? j : j === 0 ? i : 0))
  );
  for (let i = 1; i <= m; i++) {
    for (let j = 1; j <= n; j++) {
      dp[i][j] = a[i - 1] === b[j - 1]
        ? dp[i - 1][j - 1]
        : 1 + Math.min(dp[i - 1][j], dp[i][j - 1], dp[i - 1][j - 1]);
    }
  }
  return dp[m][n];
}

function findLinestarMatch(name: string, salary: number, map: Map<string, LinestarEntry>): LinestarEntry | null {
  // 1. Exact match (name + salary) — fastest path
  const exact = map.get(`${name.toLowerCase()}|${salary}`);
  if (exact) return exact;

  // 2. Exact normalized name, any salary — handles "$11,500" vs "$11500" parse differences
  const normDk = normalizeName(name);
  for (const [key, val] of map.entries()) {
    const lsName = key.split("|")[0];
    if (normalizeName(lsName) === normDk) return val;
  }

  // 3. Fuzzy normalized name (Levenshtein ≤ 3), same salary — last resort
  let best: LinestarEntry | null = null;
  let bestDist = 4;
  for (const [key, val] of map.entries()) {
    const [lsName, lsSalStr] = key.split("|");
    if (parseInt(lsSalStr, 10) !== salary) continue;
    const dist = levenshtein(normDk, normalizeName(lsName));
    if (dist < bestDist) { bestDist = dist; best = val; }
  }
  return best;
}

// ── Projection helpers ────────────────────────────────────────

function mlToProb(ml: number): number {
  return ml >= 0 ? 100 / (ml + 100) : Math.abs(ml) / (Math.abs(ml) + 100);
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
  // Each 2.5% deviation from 50% ≈ 1 point of spread in NBA
  const impliedSpread = Math.max(-15, Math.min(15, (homeProbClean - 0.5) / 0.025));
  const homeImplied = vegasTotal / 2 + impliedSpread / 2;
  return isHome ? homeImplied : vegasTotal - homeImplied;
}

type NbaProjectionStats = {
  expectedMinutes: number;
  pts: number;
  reb: number;
  ast: number;
  stl: number;
  blk: number;
  tov: number;
  threes: number;
  dd: number;
  fpts: number;
};

type NbaProjectionBlend = {
  modelProj: number | null;
  marketProj: number | null;
  lsProj: number | null;
  finalProj: number | null;
  propCount: number;
  modelConfidence: number;
  marketConfidence: number;
  lsConfidence: number;
  modelWeight: number;
  marketWeight: number;
  lsWeight: number;
  flags: string[];
  modelStats: NbaProjectionStats | null;
  marketStats: NbaProjectionStats | null;
};

type NbaProjectionCalibration = {
  byPos: Map<string, { factor: number; bias: number }>;
};

const NBA_PROJECTION_CALIBRATION_FALLBACK: Record<string, { factor: number; bias: number }> = {
  PG: { factor: 0.9, bias: 1.8 },
  SG: { factor: 0.93, bias: 1.0 },
  SF: { factor: 0.9, bias: 1.6 },
  PF: { factor: 0.9, bias: 1.5 },
  C: { factor: 0.91, bias: 1.4 },
  UTIL: { factor: 0.91, bias: 1.4 },
};

let nbaProjectionCalibrationCache:
  | { loadedAtMs: number; calibration: NbaProjectionCalibration }
  | null = null;

function countProjectionProps(props: {
  propPts?: number | null;
  propReb?: number | null;
  propAst?: number | null;
  propBlk?: number | null;
  propStl?: number | null;
}): number {
  return [props.propPts, props.propReb, props.propAst, props.propBlk, props.propStl].filter((value) => value != null).length;
}

function clamp(value: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, value));
}

function getNbaPrimaryPosition(eligiblePositions: string | null | undefined): string {
  const primary = (eligiblePositions ?? "").split("/")[0]?.trim().toUpperCase();
  if (primary === "PG" || primary === "SG" || primary === "SF" || primary === "PF" || primary === "C") {
    return primary;
  }
  return "UTIL";
}

function safePerMinute(value: number | null | undefined, minutes: number): number {
  if (!Number.isFinite(value ?? null) || value == null || minutes <= 0) return 0;
  return value / minutes;
}

function median(values: number[]): number | null {
  if (values.length === 0) return null;
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 0 ? (sorted[mid - 1] + sorted[mid]) / 2 : sorted[mid];
}

function estimateNbaExpectedMinutes(
  player: {
    avgMinutes: number | null;
    ppg: number | null;
    rpg: number | null;
    apg: number | null;
  },
  teamPace: number,
  oppPace: number,
  vegasTotal: number | null,
  homeMl: number | null,
  awayMl: number | null,
  isHome: boolean,
  props: {
    propPts?: number | null;
    propReb?: number | null;
    propAst?: number | null;
    propBlk?: number | null;
    propStl?: number | null;
  } = {},
): number {
  const avgMinutes = player.avgMinutes ?? 0;
  if (avgMinutes <= 0) return 0;

  const gamePace = (teamPace + oppPace) / 2;
  const paceFactor = gamePace / LEAGUE_AVG_PACE;
  const totalFactor = vegasTotal
    ? computeTeamImpliedTotal(vegasTotal, homeMl, awayMl, isHome) / LEAGUE_AVG_TEAM_TOTAL
    : 1;

  let expectedMinutes = avgMinutes * (1 + (paceFactor - 1) * 0.08 + (totalFactor - 1) * 0.12);

  const anchors: number[] = [];
  const ppmPts = safePerMinute(player.ppg, avgMinutes);
  const ppmReb = safePerMinute(player.rpg, avgMinutes);
  const ppmAst = safePerMinute(player.apg, avgMinutes);
  if (props.propPts != null && ppmPts >= 0.35) anchors.push(props.propPts / ppmPts);
  if (props.propReb != null && ppmReb >= 0.12) anchors.push(props.propReb / ppmReb);
  if (props.propAst != null && ppmAst >= 0.1) anchors.push(props.propAst / ppmAst);
  const marketMinutes = median(anchors);
  if (marketMinutes != null) {
    const clipped = clamp(marketMinutes, avgMinutes * 0.78, Math.min(40, avgMinutes * 1.22 + 1));
    const anchorWeight = anchors.length >= 3 ? 0.42 : anchors.length === 2 ? 0.3 : 0.2;
    expectedMinutes = expectedMinutes * (1 - anchorWeight) + clipped * anchorWeight;
  }

  if (avgMinutes < 18) expectedMinutes *= 0.96;
  return clamp(expectedMinutes, Math.max(10, avgMinutes * 0.75), Math.min(40, avgMinutes * 1.18 + 1.5));
}

function applyNbaProjectionCalibration(
  rawProjection: number | null,
  eligiblePositions: string | null | undefined,
  avgMinutes: number | null | undefined,
  calibration: NbaProjectionCalibration,
): number | null {
  const sanitized = sanitizeProjection(rawProjection);
  if (sanitized == null) return null;
  const primaryPos = getNbaPrimaryPosition(eligiblePositions);
  const base = calibration.byPos.get(primaryPos)
    ?? calibration.byPos.get("UTIL")
    ?? NBA_PROJECTION_CALIBRATION_FALLBACK.UTIL;
  let factor = base.factor;
  let bias = base.bias;
  const minutes = avgMinutes ?? 0;
  if (minutes < 20) {
    factor -= 0.025;
    bias += 0.35;
  } else if (minutes < 26) {
    factor -= 0.015;
    bias += 0.2;
  } else if (minutes >= 34) {
    factor += 0.01;
  }
  const adjusted = sanitized * clamp(factor, 0.82, 1.03) - bias * 0.35;
  return sanitizeProjection(Math.max(0, adjusted));
}

function computeNbaInternalProjection(
  blend: NbaProjectionBlend,
  eligiblePositions: string | null | undefined,
  avgMinutes: number | null | undefined,
  calibration: NbaProjectionCalibration,
): number | null {
  const rawModel = sanitizeProjection(blend.modelProj);
  if (rawModel == null) return null;
  let anchored = rawModel;

  if (blend.marketProj != null) {
    const marketWeight = blend.propCount >= 3 ? 0.22 : blend.propCount === 2 ? 0.15 : 0.08;
    anchored = anchored * (1 - marketWeight) + blend.marketProj * marketWeight;
  }
  if (blend.lsProj != null) {
    const lsWeight = blend.propCount === 0 ? 0.14 : blend.propCount <= 2 ? 0.09 : 0.05;
    anchored = anchored * (1 - lsWeight) + blend.lsProj * lsWeight;
  }

  const externalAnchors = [blend.marketProj, blend.lsProj].filter((value): value is number => value != null && Number.isFinite(value));
  if (externalAnchors.length > 0) {
    const anchorAverage = externalAnchors.reduce((sum, value) => sum + value, 0) / externalAnchors.length;
    const overshoot = anchored - anchorAverage;
    if (overshoot > 4) {
      anchored = anchorAverage + overshoot * 0.55;
    }
  }

  return applyNbaProjectionCalibration(anchored, eligiblePositions, avgMinutes, calibration);
}

async function loadNbaProjectionCalibration(): Promise<NbaProjectionCalibration> {
  const now = Date.now();
  if (nbaProjectionCalibrationCache && now - nbaProjectionCalibrationCache.loadedAtMs < 15 * 60 * 1000) {
    return nbaProjectionCalibrationCache.calibration;
  }

  const rows = await db.execute<{
    primaryPos: string | null;
    n: number;
    avgProj: number | null;
    avgActual: number | null;
    avgBias: number | null;
  }>(sql`
    SELECT
      split_part(dp.eligible_positions, '/', 1) AS "primaryPos",
      COUNT(*)::int AS "n",
      AVG(dp.our_proj) AS "avgProj",
      AVG(dp.actual_fpts) AS "avgActual",
      AVG(dp.our_proj - dp.actual_fpts) AS "avgBias"
    FROM dk_players dp
    JOIN dk_slates ds ON ds.id = dp.slate_id
    WHERE ds.sport = 'nba'
      AND dp.actual_fpts IS NOT NULL
      AND dp.our_proj IS NOT NULL
      AND COALESCE(dp.is_out, false) = false
      AND ds.slate_date >= CURRENT_DATE - INTERVAL '45 days'
    GROUP BY 1
  `);

  const byPos = new Map<string, { factor: number; bias: number }>();
  for (const [pos, fallback] of Object.entries(NBA_PROJECTION_CALIBRATION_FALLBACK)) {
    byPos.set(pos, fallback);
  }
  for (const row of rows.rows) {
    const primaryPos = getNbaPrimaryPosition(row.primaryPos);
    if ((row.n ?? 0) < 25) continue;
    const avgProj = row.avgProj ?? null;
    const avgActual = row.avgActual ?? null;
    const avgBias = row.avgBias ?? null;
    if (avgProj == null || avgActual == null || avgProj <= 0) continue;
    byPos.set(primaryPos, {
      factor: clamp(avgActual / avgProj, 0.84, 1.03),
      bias: clamp(avgBias ?? 0, -1.5, 5),
    });
  }

  const calibration = { byPos };
  nbaProjectionCalibrationCache = { loadedAtMs: now, calibration };
  return calibration;
}

function clamp01(value: number): number {
  return Math.max(0, Math.min(1, value));
}

function normalizeBlendWeights(weights: { model: number; market: number; ls: number }): { model: number; market: number; ls: number } {
  const total = weights.model + weights.market + weights.ls;
  if (total <= 0) return { model: 0, market: 0, ls: 0 };
  return {
    model: weights.model / total,
    market: weights.market / total,
    ls: weights.ls / total,
  };
}

function computeNbaProjectionStats(
  player: {
    avgMinutes: number | null; ppg: number | null; rpg: number | null;
    apg: number | null; spg: number | null; bpg: number | null;
    tovpg: number | null; threefgmPg: number | null;
    usageRate: number | null; ddRate: number | null;
  },
  teamPace: number,
  oppPace: number,
  oppDefRtg: number,
  vegasTotal: number | null = null,
  homeMl: number | null = null,
  awayMl: number | null = null,
  isHome = false,
  props: {
    propPts?: number | null;
    propReb?: number | null;
    propAst?: number | null;
    propBlk?: number | null;
    propStl?: number | null;
  } = {},
): NbaProjectionStats | null {
  const avgMinutes = player.avgMinutes ?? 0;
  if (avgMinutes < 10 && countProjectionProps(props) === 0) return null;

  const ppg      = player.ppg       ?? 0;
  const rpg      = player.rpg       ?? 0;
  const apg      = player.apg       ?? 0;
  const spg      = player.spg       ?? 0;
  const bpg      = player.bpg       ?? 0;
  const tovpg    = player.tovpg     ?? 0;
  const threefgm = player.threefgmPg ?? 0;
  const ddRate   = player.ddRate    ?? 0;
  const usage    = player.usageRate ?? LEAGUE_AVG_USAGE;
  const propCount = countProjectionProps(props);

  // Environment factors
  const gamePace    = (teamPace + oppPace) / 2;
  const paceFactor  = gamePace / LEAGUE_AVG_PACE;

  // Team-specific implied total from moneylines (not raw O/U ÷ 2)
  const totalFactor = vegasTotal
    ? computeTeamImpliedTotal(vegasTotal, homeMl, awayMl, isHome) / LEAGUE_AVG_TEAM_TOTAL
    : 1.0;

  const combinedEnv = 1 + (paceFactor - 1) * 0.35 + (totalFactor - 1) * 0.45;
  const defFactor   = oppDefRtg / LEAGUE_AVG_DEF_RTG;

  // Usage rate as volume multiplier: stars keep slightly more of the pace/total lift,
  // but the old version was too aggressive and created broad upward bias.
  const usageFactor = clamp(usage / LEAGUE_AVG_USAGE, 0.7, 1.35);
  const pointEnv = 1.0 + (combinedEnv - 1.0) * (0.4 + (usageFactor - 1) * 0.25);
  const reboundEnv = 1.0 + (combinedEnv - 1.0) * 0.3;
  const assistEnv = 1.0 + (combinedEnv - 1.0) * 0.28;
  const stockEnv = 1.0 + (combinedEnv - 1.0) * 0.18;
  const turnoverEnv = 1.0 + (combinedEnv - 1.0) * 0.22;

  const expectedMinutes = estimateNbaExpectedMinutes(
    player,
    teamPace,
    oppPace,
    vegasTotal,
    homeMl,
    awayMl,
    isHome,
    props,
  );
  const minuteBase = Math.max(avgMinutes, 1);
  const minuteRatio = clamp(expectedMinutes / minuteBase, 0.72, 1.25);
  const ptsPerMinute = safePerMinute(ppg, minuteBase);
  const rebPerMinute = safePerMinute(rpg, minuteBase);
  const astPerMinute = safePerMinute(apg, minuteBase);
  const stlPerMinute = safePerMinute(spg, minuteBase);
  const blkPerMinute = safePerMinute(bpg, minuteBase);
  const tovPerMinute = safePerMinute(tovpg, minuteBase);
  const threesPerMinute = safePerMinute(threefgm, minuteBase);

  // Per-stat projections — use market prop lines when available (they already
  // bake in matchup, pace, and injury context), fall back to formula otherwise.
  const projPts  = props.propPts  != null ? props.propPts  : ptsPerMinute * expectedMinutes * (1.0 + (defFactor - 1.0) * 0.6) * pointEnv;
  const projReb  = props.propReb  != null ? props.propReb  : rebPerMinute * expectedMinutes * reboundEnv;
  const projAst  = props.propAst  != null ? props.propAst  : astPerMinute * expectedMinutes * (1.0 + (defFactor - 1.0) * 0.35) * assistEnv;
  const projStl  = props.propStl  != null ? props.propStl  : stlPerMinute * expectedMinutes * stockEnv;
  const projBlk  = props.propBlk  != null ? props.propBlk  : blkPerMinute * expectedMinutes * stockEnv;
  const projTov  = tovPerMinute * expectedMinutes * turnoverEnv;
  const projThrees = threesPerMinute * expectedMinutes * pointEnv;
  const projDd   = ddRate * minuteRatio * (1.0 + (combinedEnv - 1.0) * 0.2);

  const fpts = (
    projPts * 1.0
    + projReb * 1.25
    + projAst * 1.5
    + projStl * 2.0
    + projBlk * 2.0
    - projTov * 0.5
    + projThrees * 0.5
    + projDd   * 1.5
  );
  return {
    expectedMinutes: Math.round(expectedMinutes * 100) / 100,
    pts: Math.round(projPts * 100) / 100,
    reb: Math.round(projReb * 100) / 100,
    ast: Math.round(projAst * 100) / 100,
    stl: Math.round(projStl * 100) / 100,
    blk: Math.round(projBlk * 100) / 100,
    tov: Math.round(projTov * 100) / 100,
    threes: Math.round(projThrees * 100) / 100,
    dd: Math.round(projDd * 100) / 100,
    fpts: Math.round(fpts * 100) / 100,
  };
}

function buildNbaProjectionBlend(
  player: {
    avgMinutes: number | null; ppg: number | null; rpg: number | null;
    apg: number | null; spg: number | null; bpg: number | null;
    tovpg: number | null; threefgmPg: number | null;
    usageRate: number | null; ddRate: number | null;
  },
  teamPace: number,
  oppPace: number,
  oppDefRtg: number,
  vegasTotal: number | null = null,
  homeMl: number | null = null,
  awayMl: number | null = null,
  isHome = false,
  linestarProj: number | null = null,
  props: {
    propPts?: number | null;
    propReb?: number | null;
    propAst?: number | null;
    propBlk?: number | null;
    propStl?: number | null;
  } = {},
  movement: {
    propDeltas?: Partial<Record<NbaProjectionPropStat, number>>;
    marketFptsDelta?: number;
    vegasTotalDelta?: number;
    homeSpreadDelta?: number | null;
  } = {},
): NbaProjectionBlend {
  const modelStats = computeNbaProjectionStats(player, teamPace, oppPace, oppDefRtg, vegasTotal, homeMl, awayMl, isHome, {});
  const marketStats = computeNbaProjectionStats(player, teamPace, oppPace, oppDefRtg, vegasTotal, homeMl, awayMl, isHome, props);
  const modelProj = sanitizeProjection(modelStats?.fpts ?? null);
  const marketProj = sanitizeProjection(marketStats?.fpts ?? null);
  const lsProj = sanitizeProjection(linestarProj);
  const propCount = countProjectionProps(props);
  const avgMinutes = player.avgMinutes ?? 0;

  let modelConfidence = avgMinutes >= 30 ? 0.82 : avgMinutes >= 24 ? 0.72 : avgMinutes >= 18 ? 0.58 : 0.42;
  let marketConfidence = propCount >= 4 ? 0.9 : propCount === 3 ? 0.8 : propCount === 2 ? 0.65 : propCount === 1 ? 0.45 : 0;
  let lsConfidence = lsProj != null ? 0.35 : 0;
  const flags: string[] = [];

  if (avgMinutes < 18) flags.push("low_minutes_role");
  if (propCount === 0) flags.push("no_props");
  else if (propCount <= 2) flags.push("sparse_props");
  else flags.push("dense_props");

  const marketGap = modelProj != null && marketProj != null ? Math.abs(modelProj - marketProj) : 0;
  const lsGap = modelProj != null && lsProj != null ? Math.abs(modelProj - lsProj) : 0;
  const marketFptsDelta = Math.abs(movement.marketFptsDelta ?? 0);
  const totalDelta = Math.abs(movement.vegasTotalDelta ?? 0);
  if (marketGap >= 6) flags.push("high_market_disagreement");
  if (lsGap >= 6) flags.push("high_ls_disagreement");
  if (marketFptsDelta >= 1.5 || totalDelta >= 1.5) flags.push("line_movement");

  if (avgMinutes < 18 && lsProj != null && lsGap >= 6) {
    modelConfidence = clamp01(modelConfidence - 0.12);
    lsConfidence = clamp01(lsConfidence + 0.12);
  }
  if (marketGap >= 6 && marketConfidence > 0) {
    modelConfidence = clamp01(modelConfidence - 0.08);
    marketConfidence = clamp01(marketConfidence + 0.05);
  }
  if (marketProj != null && marketConfidence > 0 && (marketFptsDelta > 0 || totalDelta > 0)) {
    const movementBoost = Math.min(0.2, marketFptsDelta * 0.04 + totalDelta * 0.03);
    marketConfidence = clamp01(marketConfidence + movementBoost);
    lsConfidence = clamp01(lsConfidence - movementBoost * 0.75);
  }

  let baseWeights = { model: 0, market: 0, ls: 0 };
  if (marketProj != null && marketConfidence > 0) {
    baseWeights = propCount >= 3
      ? { model: 0.30, market: 0.60, ls: lsProj != null ? 0.10 : 0 }
      : { model: 0.45, market: 0.45, ls: lsProj != null ? 0.10 : 0 };
  } else {
    baseWeights = { model: modelProj != null ? 0.75 : 0, market: 0, ls: lsProj != null ? 0.25 : 0 };
    if (lsProj == null) baseWeights.model = modelProj != null ? 1 : 0;
  }
  if (marketProj != null && (marketFptsDelta > 0 || totalDelta > 0)) {
    const movementShift = Math.min(0.15, marketFptsDelta * 0.03 + totalDelta * 0.025);
    baseWeights.market += movementShift;
    baseWeights.ls = Math.max(0, baseWeights.ls - movementShift);
  }

  const effectiveWeights = normalizeBlendWeights({
    model: modelProj != null ? baseWeights.model * modelConfidence : 0,
    market: marketProj != null ? baseWeights.market * marketConfidence : 0,
    ls: lsProj != null ? baseWeights.ls * lsConfidence : 0,
  });

  const finalProj = sanitizeProjection(
    (modelProj != null ? effectiveWeights.model * modelProj : 0)
    + (marketProj != null ? effectiveWeights.market * marketProj : 0)
    + (lsProj != null ? effectiveWeights.ls * lsProj : 0),
  );

  return {
    modelProj,
    marketProj,
    lsProj,
    finalProj,
    propCount,
    modelConfidence: Math.round(modelConfidence * 1000) / 1000,
    marketConfidence: Math.round(marketConfidence * 1000) / 1000,
    lsConfidence: Math.round(lsConfidence * 1000) / 1000,
    modelWeight: Math.round(effectiveWeights.model * 1000) / 1000,
    marketWeight: Math.round(effectiveWeights.market * 1000) / 1000,
    lsWeight: Math.round(effectiveWeights.ls * 1000) / 1000,
    flags,
    modelStats,
    marketStats,
  };
}

function computeOurProjection(
  player: {
    avgMinutes: number | null; ppg: number | null; rpg: number | null;
    apg: number | null; spg: number | null; bpg: number | null;
    tovpg: number | null; threefgmPg: number | null;
    usageRate: number | null; ddRate: number | null;
  },
  teamPace: number,
  oppPace: number,
  oppDefRtg: number,
  vegasTotal: number | null = null,
  homeMl: number | null = null,
  awayMl: number | null = null,
  isHome = false,
  props: {
    propPts?: number | null;
    propReb?: number | null;
    propAst?: number | null;
    propBlk?: number | null;
    propStl?: number | null;
  } = {},
): number | null {
  return computeNbaProjectionStats(player, teamPace, oppPace, oppDefRtg, vegasTotal, homeMl, awayMl, isHome, props)?.fpts ?? null;
}

async function createProjectionRun(
  slateId: number,
  source: ProjectionRunSource,
  configJson: Record<string, unknown>,
  notes?: string,
): Promise<number> {
  await ensureProjectionExperimentTables();
  const [run] = await db.insert(projectionRuns).values({
    sport: "nba",
    slateId,
    modelVersion: NBA_PROJECTION_MODEL_VERSION,
    source,
    configJson,
    notes: notes ?? null,
  }).returning({ id: projectionRuns.id });
  return run.id;
}

async function recordProjectionSnapshots(
  runId: number,
  snapshots: Array<{
    slateId: number;
    dkPlayerId: number;
    name: string;
    teamId: number | null;
    salary: number;
    isOut: boolean;
    blend: NbaProjectionBlend;
    actualFpts?: number | null;
  }>,
): Promise<void> {
  if (snapshots.length === 0) return;
  await ensureProjectionExperimentTables();

  for (let i = 0; i < snapshots.length; i += 100) {
    const batch = snapshots.slice(i, i + 100).map((snapshot) => ({
      runId,
      slateId: snapshot.slateId,
      dkPlayerId: snapshot.dkPlayerId,
      name: snapshot.name,
      teamId: snapshot.teamId,
      salary: snapshot.salary,
      isOut: snapshot.isOut,
      modelProjFpts: snapshot.blend.modelProj,
      marketProjFpts: snapshot.blend.marketProj,
      linestarProjFpts: snapshot.blend.lsProj,
      finalProjFpts: snapshot.blend.finalProj,
      modelConfidence: snapshot.blend.modelConfidence,
      marketConfidence: snapshot.blend.marketConfidence,
      lsConfidence: snapshot.blend.lsConfidence,
      modelWeight: snapshot.blend.modelWeight,
      marketWeight: snapshot.blend.marketWeight,
      lsWeight: snapshot.blend.lsWeight,
      flagsJson: snapshot.blend.flags,
      modelStatsJson: snapshot.blend.modelStats,
      marketStatsJson: snapshot.blend.marketStats,
      actualFpts: snapshot.actualFpts ?? null,
    }));

    await db.insert(projectionPlayerSnapshots).values(batch).onConflictDoUpdate({
      target: [projectionPlayerSnapshots.runId, projectionPlayerSnapshots.dkPlayerId],
      set: {
        modelProjFpts: sql`EXCLUDED.model_proj_fpts`,
        marketProjFpts: sql`EXCLUDED.market_proj_fpts`,
        linestarProjFpts: sql`EXCLUDED.linestar_proj_fpts`,
        finalProjFpts: sql`EXCLUDED.final_proj_fpts`,
        modelConfidence: sql`EXCLUDED.model_confidence`,
        marketConfidence: sql`EXCLUDED.market_confidence`,
        lsConfidence: sql`EXCLUDED.ls_confidence`,
        modelWeight: sql`EXCLUDED.model_weight`,
        marketWeight: sql`EXCLUDED.market_weight`,
        lsWeight: sql`EXCLUDED.ls_weight`,
        flagsJson: sql`EXCLUDED.flags_json`,
        modelStatsJson: sql`EXCLUDED.model_stats_json`,
        marketStatsJson: sql`EXCLUDED.market_stats_json`,
        actualFpts: sql`EXCLUDED.actual_fpts`,
      },
    });
  }
}

async function syncProjectionSnapshotActualsForSlate(slateId: number): Promise<void> {
  await ensureProjectionExperimentTables();
  await db.execute(sql`
    UPDATE projection_player_snapshots pps
    SET actual_fpts = dp.actual_fpts
    FROM dk_players dp
    WHERE pps.slate_id = dp.slate_id
      AND pps.dk_player_id = dp.dk_player_id
      AND pps.slate_id = ${slateId}
  `);
}

async function createOwnershipRun(
  slateId: number,
  sport: Sport,
  source: string,
  ownershipVersion: string,
  configJson: Record<string, unknown>,
  notes?: string,
): Promise<number> {
  await ensureOwnershipExperimentTables();
  const [run] = await db.insert(ownershipRuns).values({
    sport,
    slateId,
    ownershipVersion,
    source,
    configJson,
    notes: notes ?? null,
  }).returning({ id: ownershipRuns.id });
  return run.id;
}

async function recordOwnershipSnapshots(
  runId: number,
  snapshots: Array<{
    slateId: number;
    dkPlayerId: number;
    name: string;
    teamId: number | null;
    salary: number;
    eligiblePositions: string | null;
    isOut: boolean;
    linestarProjFpts?: number | null;
    ourProjFpts?: number | null;
    liveProjFpts?: number | null;
    linestarOwnPct?: number | null;
    fieldOwnPct?: number | null;
    ourOwnPct?: number | null;
    liveOwnPct?: number | null;
    actualOwnPct?: number | null;
    lineupOrder?: number | null;
    lineupConfirmed?: boolean | null;
  }>,
): Promise<void> {
  if (snapshots.length === 0) return;
  await ensureOwnershipExperimentTables();

  for (let i = 0; i < snapshots.length; i += 100) {
    const batch = snapshots.slice(i, i + 100).map((snapshot) => ({
      runId,
      slateId: snapshot.slateId,
      dkPlayerId: snapshot.dkPlayerId,
      name: snapshot.name,
      teamId: snapshot.teamId,
      salary: snapshot.salary,
      eligiblePositions: snapshot.eligiblePositions ?? null,
      isOut: snapshot.isOut,
      linestarProjFpts: snapshot.linestarProjFpts ?? null,
      ourProjFpts: snapshot.ourProjFpts ?? null,
      liveProjFpts: snapshot.liveProjFpts ?? null,
      linestarOwnPct: snapshot.linestarOwnPct ?? null,
      fieldOwnPct: snapshot.fieldOwnPct ?? null,
      ourOwnPct: snapshot.ourOwnPct ?? null,
      liveOwnPct: snapshot.liveOwnPct ?? null,
      actualOwnPct: snapshot.actualOwnPct ?? null,
      lineupOrder: snapshot.lineupOrder ?? null,
      lineupConfirmed: snapshot.lineupConfirmed ?? null,
    }));

    await db.insert(ownershipPlayerSnapshots).values(batch).onConflictDoUpdate({
      target: [ownershipPlayerSnapshots.runId, ownershipPlayerSnapshots.dkPlayerId],
      set: {
        linestarProjFpts: sql`EXCLUDED.linestar_proj_fpts`,
        ourProjFpts: sql`EXCLUDED.our_proj_fpts`,
        liveProjFpts: sql`EXCLUDED.live_proj_fpts`,
        linestarOwnPct: sql`EXCLUDED.linestar_own_pct`,
        fieldOwnPct: sql`EXCLUDED.field_own_pct`,
        ourOwnPct: sql`EXCLUDED.our_own_pct`,
        liveOwnPct: sql`EXCLUDED.live_own_pct`,
        actualOwnPct: sql`EXCLUDED.actual_own_pct`,
        lineupOrder: sql`EXCLUDED.lineup_order`,
        lineupConfirmed: sql`EXCLUDED.lineup_confirmed`,
      },
    });
  }
}

async function syncOwnershipSnapshotActualsForSlate(slateId: number): Promise<void> {
  await ensureOwnershipExperimentTables();
  await db.execute(sql`
    UPDATE ownership_player_snapshots ops
    SET actual_own_pct = dp.actual_own_pct
    FROM dk_players dp
    WHERE ops.slate_id = dp.slate_id
      AND ops.dk_player_id = dp.dk_player_id
      AND ops.slate_id = ${slateId}
  `);
}

async function createMlbBlowupRun(
  slateId: number,
  source: string,
  analysisVersion: string,
  configJson: Record<string, unknown>,
  notes?: string,
): Promise<number> {
  await ensureMlbBlowupTrackingTables();
  const [run] = await db.insert(mlbBlowupRuns).values({
    slateId,
    analysisVersion,
    source,
    configJson,
    notes: notes ?? null,
  }).returning({ id: mlbBlowupRuns.id });
  return run.id;
}

async function recordMlbBlowupSnapshots(
  runId: number,
  snapshots: Array<{
    slateId: number;
    dkPlayerId: number;
    name: string;
    teamId: number | null;
    teamAbbrev: string | null;
    salary: number;
    eligiblePositions: string | null;
    lineupOrder?: number | null;
    teamTotal?: number | null;
    projectedFpts?: number | null;
    projectedCeiling?: number | null;
    projectedValue?: number | null;
    blowupScore?: number | null;
    candidateRank: number;
    actualFpts?: number | null;
    actualOwnPct?: number | null;
  }>,
): Promise<void> {
  if (snapshots.length === 0) return;
  await ensureMlbBlowupTrackingTables();

  for (let i = 0; i < snapshots.length; i += 100) {
    const batch = snapshots.slice(i, i + 100).map((snapshot) => ({
      runId,
      slateId: snapshot.slateId,
      dkPlayerId: snapshot.dkPlayerId,
      name: snapshot.name,
      teamId: snapshot.teamId,
      teamAbbrev: snapshot.teamAbbrev ?? null,
      salary: snapshot.salary,
      eligiblePositions: snapshot.eligiblePositions ?? null,
      lineupOrder: snapshot.lineupOrder ?? null,
      teamTotal: snapshot.teamTotal ?? null,
      projectedFpts: snapshot.projectedFpts ?? null,
      projectedCeiling: snapshot.projectedCeiling ?? null,
      projectedValue: snapshot.projectedValue ?? null,
      blowupScore: snapshot.blowupScore ?? null,
      candidateRank: snapshot.candidateRank,
      actualFpts: snapshot.actualFpts ?? null,
      actualOwnPct: snapshot.actualOwnPct ?? null,
    }));

    await db.insert(mlbBlowupPlayerSnapshots).values(batch).onConflictDoUpdate({
      target: [mlbBlowupPlayerSnapshots.runId, mlbBlowupPlayerSnapshots.dkPlayerId],
      set: {
        teamTotal: sql`EXCLUDED.team_total`,
        projectedFpts: sql`EXCLUDED.projected_fpts`,
        projectedCeiling: sql`EXCLUDED.projected_ceiling`,
        projectedValue: sql`EXCLUDED.projected_value`,
        blowupScore: sql`EXCLUDED.blowup_score`,
        candidateRank: sql`EXCLUDED.candidate_rank`,
        lineupOrder: sql`EXCLUDED.lineup_order`,
        actualFpts: sql`EXCLUDED.actual_fpts`,
        actualOwnPct: sql`EXCLUDED.actual_own_pct`,
      },
    });
  }
}

async function syncMlbBlowupSnapshotActualsForSlate(slateId: number): Promise<void> {
  await ensureMlbBlowupTrackingTables();
  await db.execute(sql`
    UPDATE mlb_blowup_player_snapshots bps
    SET
      actual_fpts = dp.actual_fpts,
      actual_own_pct = dp.actual_own_pct
    FROM dk_players dp
    WHERE bps.slate_id = dp.slate_id
      AND bps.dk_player_id = dp.dk_player_id
      AND bps.slate_id = ${slateId}
  `);
}

type MlbHomerunSnapshotInput = {
  slateId: number;
  dkPlayerId: number;
  name: string;
  teamId: number | null;
  teamAbbrev: string | null;
  salary: number;
  eligiblePositions: string | null;
  isOut: boolean;
  lineupOrder?: number | null;
  lineupConfirmed?: boolean | null;
  expectedHr?: number | null;
  hrProb1Plus?: number | null;
  hitterHrPg?: number | null;
  hitterIso?: number | null;
  hitterSlug?: number | null;
  hitterPaPg?: number | null;
  hitterWrcPlus?: number | null;
  hitterSplitWrcPlus?: number | null;
  teamTotal?: number | null;
  vegasTotal?: number | null;
  parkHrFactor?: number | null;
  weatherTemp?: number | null;
  windSpeed?: number | null;
  opposingPitcherName?: string | null;
  opposingPitcherHand?: string | null;
  opposingPitcherHrPer9?: number | null;
  opposingPitcherHrFbPct?: number | null;
  opposingPitcherXfip?: number | null;
  opposingPitcherEra?: number | null;
  actualHr?: number | null;
  actualFpts?: number | null;
  actualOwnPct?: number | null;
};

async function createMlbHomerunRun(
  slateId: number,
  source: string,
  analysisVersion: string,
  configJson: Record<string, unknown>,
  notes?: string,
): Promise<number> {
  await ensureMlbHomerunTrackingTables();
  const [run] = await db.insert(mlbHomerunRuns).values({
    slateId,
    analysisVersion,
    source,
    configJson,
    notes: notes ?? null,
  }).returning({ id: mlbHomerunRuns.id });
  return run.id;
}

async function recordMlbHomerunSnapshots(
  runId: number,
  snapshots: MlbHomerunSnapshotInput[],
): Promise<void> {
  if (snapshots.length === 0) return;
  await ensureMlbHomerunTrackingTables();

  for (let i = 0; i < snapshots.length; i += 100) {
    const batch = snapshots.slice(i, i + 100).map((snapshot) => {
      const actualHr = snapshot.actualHr ?? null;
      return {
        runId,
        slateId: snapshot.slateId,
        dkPlayerId: snapshot.dkPlayerId,
        name: snapshot.name,
        teamId: snapshot.teamId,
        teamAbbrev: snapshot.teamAbbrev ?? null,
        salary: snapshot.salary,
        eligiblePositions: snapshot.eligiblePositions ?? null,
        isOut: snapshot.isOut,
        lineupOrder: snapshot.lineupOrder ?? null,
        lineupConfirmed: snapshot.lineupConfirmed ?? null,
        expectedHr: snapshot.expectedHr ?? null,
        hrProb1Plus: snapshot.hrProb1Plus ?? null,
        hitterHrPg: snapshot.hitterHrPg ?? null,
        hitterIso: snapshot.hitterIso ?? null,
        hitterSlug: snapshot.hitterSlug ?? null,
        hitterPaPg: snapshot.hitterPaPg ?? null,
        hitterWrcPlus: snapshot.hitterWrcPlus ?? null,
        hitterSplitWrcPlus: snapshot.hitterSplitWrcPlus ?? null,
        teamTotal: snapshot.teamTotal ?? null,
        vegasTotal: snapshot.vegasTotal ?? null,
        parkHrFactor: snapshot.parkHrFactor ?? null,
        weatherTemp: snapshot.weatherTemp ?? null,
        windSpeed: snapshot.windSpeed ?? null,
        opposingPitcherName: snapshot.opposingPitcherName ?? null,
        opposingPitcherHand: snapshot.opposingPitcherHand ?? null,
        opposingPitcherHrPer9: snapshot.opposingPitcherHrPer9 ?? null,
        opposingPitcherHrFbPct: snapshot.opposingPitcherHrFbPct ?? null,
        opposingPitcherXfip: snapshot.opposingPitcherXfip ?? null,
        opposingPitcherEra: snapshot.opposingPitcherEra ?? null,
        actualHr,
        hitHr1Plus: actualHr == null ? null : actualHr > 0,
        actualFpts: snapshot.actualFpts ?? null,
        actualOwnPct: snapshot.actualOwnPct ?? null,
      };
    });

    await db.insert(mlbHomerunPlayerSnapshots).values(batch).onConflictDoUpdate({
      target: [mlbHomerunPlayerSnapshots.runId, mlbHomerunPlayerSnapshots.dkPlayerId],
      set: {
        teamId: sql`EXCLUDED.team_id`,
        teamAbbrev: sql`EXCLUDED.team_abbrev`,
        salary: sql`EXCLUDED.salary`,
        eligiblePositions: sql`EXCLUDED.eligible_positions`,
        isOut: sql`EXCLUDED.is_out`,
        lineupOrder: sql`EXCLUDED.lineup_order`,
        lineupConfirmed: sql`EXCLUDED.lineup_confirmed`,
        expectedHr: sql`EXCLUDED.expected_hr`,
        hrProb1Plus: sql`EXCLUDED.hr_prob_1plus`,
        hitterHrPg: sql`EXCLUDED.hitter_hr_pg`,
        hitterIso: sql`EXCLUDED.hitter_iso`,
        hitterSlug: sql`EXCLUDED.hitter_slg`,
        hitterPaPg: sql`EXCLUDED.hitter_pa_pg`,
        hitterWrcPlus: sql`EXCLUDED.hitter_wrc_plus`,
        hitterSplitWrcPlus: sql`EXCLUDED.hitter_split_wrc_plus`,
        teamTotal: sql`EXCLUDED.team_total`,
        vegasTotal: sql`EXCLUDED.vegas_total`,
        parkHrFactor: sql`EXCLUDED.park_hr_factor`,
        weatherTemp: sql`EXCLUDED.weather_temp`,
        windSpeed: sql`EXCLUDED.wind_speed`,
        opposingPitcherName: sql`EXCLUDED.opposing_pitcher_name`,
        opposingPitcherHand: sql`EXCLUDED.opposing_pitcher_hand`,
        opposingPitcherHrPer9: sql`EXCLUDED.opposing_pitcher_hr_per_9`,
        opposingPitcherHrFbPct: sql`EXCLUDED.opposing_pitcher_hr_fb_pct`,
        opposingPitcherXfip: sql`EXCLUDED.opposing_pitcher_xfip`,
        opposingPitcherEra: sql`EXCLUDED.opposing_pitcher_era`,
        actualHr: sql`EXCLUDED.actual_hr`,
        hitHr1Plus: sql`EXCLUDED.hit_hr_1plus`,
        actualFpts: sql`EXCLUDED.actual_fpts`,
        actualOwnPct: sql`EXCLUDED.actual_own_pct`,
      },
    });
  }
}

async function syncMlbHomerunSnapshotActualsForSlate(slateId: number): Promise<void> {
  await ensureMlbHomerunTrackingTables();
  await db.execute(sql`
    UPDATE mlb_homerun_player_snapshots hps
    SET
      actual_hr = dp.actual_hr,
      hit_hr_1plus = CASE
        WHEN dp.actual_hr IS NULL THEN hps.hit_hr_1plus
        ELSE dp.actual_hr > 0
      END,
      actual_fpts = dp.actual_fpts,
      actual_own_pct = dp.actual_own_pct
    FROM dk_players dp
    WHERE hps.slate_id = dp.slate_id
      AND hps.dk_player_id = dp.dk_player_id
      AND hps.slate_id = ${slateId}
  `);
}

export async function snapshotMlbHomerunSlateFromStoredRows(
  slateId: number,
  source = "homerun_page",
): Promise<{ ok: boolean; message: string; snapshotCount: number }> {
  if (!Number.isSafeInteger(slateId) || slateId <= 0) {
    return { ok: false, message: "Invalid MLB slate id", snapshotCount: 0 };
  }

  await ensureMlbHomerunTrackingTables();
  const existing = await db.execute<{ rows: number }>(sql`
    SELECT COUNT(*)::int AS "rows"
    FROM mlb_homerun_player_snapshots hps
    JOIN mlb_homerun_runs hr ON hr.id = hps.run_id
    WHERE hr.slate_id = ${slateId}
      AND hr.analysis_version = ${MLB_HOMERUN_MODEL_VERSION}
  `);
  const existingRows = Number(existing.rows[0]?.rows ?? 0);
  if (existingRows > 0) {
    return { ok: true, message: "Homerun slate snapshot already exists", snapshotCount: existingRows };
  }

  const result = await db.execute<{
    slateId: number;
    dkPlayerId: number;
    name: string;
    teamId: number | null;
    teamAbbrev: string | null;
    salary: number;
    eligiblePositions: string | null;
    isOut: boolean | null;
    lineupOrder: number | null;
    lineupConfirmed: boolean | null;
    expectedHr: number | null;
    hrProb1Plus: number | null;
    hitterHrPg: number | null;
    hitterIso: number | null;
    hitterSlug: number | null;
    hitterPaPg: number | null;
    hitterWrcPlus: number | null;
    hitterSplitWrcPlus: number | null;
    teamTotal: number | null;
    vegasTotal: number | null;
    parkHrFactor: number | null;
    weatherTemp: number | null;
    windSpeed: number | null;
    opposingPitcherName: string | null;
    opposingPitcherHand: string | null;
    opposingPitcherHrPer9: number | null;
    opposingPitcherHrFbPct: number | null;
    opposingPitcherXfip: number | null;
    opposingPitcherEra: number | null;
    actualHr: number | null;
    actualFpts: number | null;
    actualOwnPct: number | null;
  }>(sql`
    WITH latest_batter_by_name AS (
      SELECT DISTINCT ON (LOWER(name))
        LOWER(name) AS name_key,
        pa_pg,
        hr_pg,
        iso,
        slg,
        wrc_plus,
        wrc_plus_vs_l,
        wrc_plus_vs_r
      FROM mlb_batter_stats
      ORDER BY LOWER(name), season DESC, fetched_at DESC, id DESC
    ),
    latest_pitcher AS (
      SELECT DISTINCT ON (player_id)
        player_id, name, hand, hr_per_9, hr_fb_pct, xfip, era
      FROM mlb_pitcher_stats
      ORDER BY player_id, season DESC, fetched_at DESC, id DESC
    ),
    latest_pitcher_by_name AS (
      SELECT DISTINCT ON (LOWER(name))
        LOWER(name) AS name_key,
        name,
        hand,
        hr_per_9,
        hr_fb_pct,
        xfip,
        era
      FROM mlb_pitcher_stats
      ORDER BY LOWER(name), season DESC, fetched_at DESC, id DESC
    ),
    latest_park AS (
      SELECT DISTINCT ON (team_id)
        team_id,
        hr_factor
      FROM mlb_park_factors
      ORDER BY team_id, season DESC, id DESC
    )
    SELECT
      dp.slate_id AS "slateId",
      dp.dk_player_id AS "dkPlayerId",
      dp.name,
      dp.mlb_team_id AS "teamId",
      dp.team_abbrev AS "teamAbbrev",
      dp.salary,
      dp.eligible_positions AS "eligiblePositions",
      dp.is_out AS "isOut",
      dp.dk_starting_lineup_order AS "lineupOrder",
      dp.dk_team_lineup_confirmed AS "lineupConfirmed",
      dp.expected_hr AS "expectedHr",
      dp.hr_prob_1plus AS "hrProb1Plus",
      batter.hr_pg AS "hitterHrPg",
      batter.iso AS "hitterIso",
      batter.slg AS "hitterSlug",
      batter.pa_pg AS "hitterPaPg",
      batter.wrc_plus AS "hitterWrcPlus",
      CASE
        WHEN (
          CASE
            WHEN dp.mlb_team_id = mm.home_team_id THEN COALESCE(asp_id.hand, asp_name.hand)
            WHEN dp.mlb_team_id = mm.away_team_id THEN COALESCE(hsp_id.hand, hsp_name.hand)
            ELSE NULL
          END
        ) = 'L' THEN batter.wrc_plus_vs_l
        WHEN (
          CASE
            WHEN dp.mlb_team_id = mm.home_team_id THEN COALESCE(asp_id.hand, asp_name.hand)
            WHEN dp.mlb_team_id = mm.away_team_id THEN COALESCE(hsp_id.hand, hsp_name.hand)
            ELSE NULL
          END
        ) = 'R' THEN batter.wrc_plus_vs_r
        ELSE NULL
      END AS "hitterSplitWrcPlus",
      CASE
        WHEN dp.mlb_team_id = mm.home_team_id THEN mm.home_implied
        WHEN dp.mlb_team_id = mm.away_team_id THEN mm.away_implied
        ELSE NULL
      END AS "teamTotal",
      mm.vegas_total AS "vegasTotal",
      park.hr_factor AS "parkHrFactor",
      mm.weather_temp AS "weatherTemp",
      mm.wind_speed AS "windSpeed",
      CASE
        WHEN dp.mlb_team_id = mm.home_team_id THEN COALESCE(mm.away_sp_name, asp_id.name, asp_name.name)
        WHEN dp.mlb_team_id = mm.away_team_id THEN COALESCE(mm.home_sp_name, hsp_id.name, hsp_name.name)
        ELSE NULL
      END AS "opposingPitcherName",
      CASE
        WHEN dp.mlb_team_id = mm.home_team_id THEN COALESCE(asp_id.hand, asp_name.hand)
        WHEN dp.mlb_team_id = mm.away_team_id THEN COALESCE(hsp_id.hand, hsp_name.hand)
        ELSE NULL
      END AS "opposingPitcherHand",
      CASE
        WHEN dp.mlb_team_id = mm.home_team_id THEN NULLIF(COALESCE(asp_id.hr_per_9, asp_name.hr_per_9), 0)
        WHEN dp.mlb_team_id = mm.away_team_id THEN NULLIF(COALESCE(hsp_id.hr_per_9, hsp_name.hr_per_9), 0)
        ELSE NULL
      END AS "opposingPitcherHrPer9",
      CASE
        WHEN dp.mlb_team_id = mm.home_team_id THEN NULLIF(COALESCE(asp_id.hr_fb_pct, asp_name.hr_fb_pct), 0)
        WHEN dp.mlb_team_id = mm.away_team_id THEN NULLIF(COALESCE(hsp_id.hr_fb_pct, hsp_name.hr_fb_pct), 0)
        ELSE NULL
      END AS "opposingPitcherHrFbPct",
      CASE
        WHEN dp.mlb_team_id = mm.home_team_id THEN NULLIF(COALESCE(asp_id.xfip, asp_name.xfip), 0)
        WHEN dp.mlb_team_id = mm.away_team_id THEN NULLIF(COALESCE(hsp_id.xfip, hsp_name.xfip), 0)
        ELSE NULL
      END AS "opposingPitcherXfip",
      CASE
        WHEN dp.mlb_team_id = mm.home_team_id THEN NULLIF(COALESCE(asp_id.era, asp_name.era), 0)
        WHEN dp.mlb_team_id = mm.away_team_id THEN NULLIF(COALESCE(hsp_id.era, hsp_name.era), 0)
        ELSE NULL
      END AS "opposingPitcherEra",
      dp.actual_hr AS "actualHr",
      dp.actual_fpts AS "actualFpts",
      dp.actual_own_pct AS "actualOwnPct"
    FROM dk_players dp
    JOIN dk_slates ds ON ds.id = dp.slate_id
    LEFT JOIN mlb_matchups mm ON mm.id = dp.matchup_id
    LEFT JOIN latest_batter_by_name batter ON batter.name_key = LOWER(dp.name)
    LEFT JOIN latest_pitcher hsp_id ON hsp_id.player_id = mm.home_sp_id
    LEFT JOIN latest_pitcher asp_id ON asp_id.player_id = mm.away_sp_id
    LEFT JOIN latest_pitcher_by_name hsp_name ON hsp_name.name_key = LOWER(mm.home_sp_name)
    LEFT JOIN latest_pitcher_by_name asp_name ON asp_name.name_key = LOWER(mm.away_sp_name)
    LEFT JOIN latest_park park ON park.team_id = mm.home_team_id
    WHERE ds.sport = 'mlb'
      AND dp.slate_id = ${slateId}
      AND dp.hr_prob_1plus IS NOT NULL
      AND NOT (dp.eligible_positions ILIKE '%SP%' OR dp.eligible_positions ILIKE '%RP%')
  `);

  const snapshots: MlbHomerunSnapshotInput[] = result.rows.map((row) => ({
    slateId: Number(row.slateId),
    dkPlayerId: Number(row.dkPlayerId),
    name: row.name,
    teamId: row.teamId == null ? null : Number(row.teamId),
    teamAbbrev: row.teamAbbrev ?? null,
    salary: Number(row.salary ?? 0),
    eligiblePositions: row.eligiblePositions ?? null,
    isOut: Boolean(row.isOut),
    lineupOrder: row.lineupOrder == null ? null : Number(row.lineupOrder),
    lineupConfirmed: row.lineupConfirmed ?? null,
    expectedHr: finiteOrNull(row.expectedHr),
    hrProb1Plus: finiteOrNull(row.hrProb1Plus),
    hitterHrPg: finiteOrNull(row.hitterHrPg),
    hitterIso: finiteOrNull(row.hitterIso),
    hitterSlug: finiteOrNull(row.hitterSlug),
    hitterPaPg: finiteOrNull(row.hitterPaPg),
    hitterWrcPlus: finiteOrNull(row.hitterWrcPlus),
    hitterSplitWrcPlus: finiteOrNull(row.hitterSplitWrcPlus),
    teamTotal: finiteOrNull(row.teamTotal),
    vegasTotal: finiteOrNull(row.vegasTotal),
    parkHrFactor: finiteOrNull(row.parkHrFactor),
    weatherTemp: finiteOrNull(row.weatherTemp),
    windSpeed: finiteOrNull(row.windSpeed),
    opposingPitcherName: row.opposingPitcherName ?? null,
    opposingPitcherHand: row.opposingPitcherHand ?? null,
    opposingPitcherHrPer9: finiteOrNull(row.opposingPitcherHrPer9),
    opposingPitcherHrFbPct: finiteOrNull(row.opposingPitcherHrFbPct),
    opposingPitcherXfip: finiteOrNull(row.opposingPitcherXfip),
    opposingPitcherEra: finiteOrNull(row.opposingPitcherEra),
    actualHr: row.actualHr == null ? null : Number(row.actualHr),
    actualFpts: finiteOrNull(row.actualFpts),
    actualOwnPct: finiteOrNull(row.actualOwnPct),
  }));

  if (snapshots.length === 0) {
    return { ok: false, message: "No MLB homerun probabilities found for this slate", snapshotCount: 0 };
  }

  const runId = await createMlbHomerunRun(slateId, source, MLB_HOMERUN_MODEL_VERSION, {
    version: MLB_HOMERUN_MODEL_VERSION,
    source,
    snapshotCount: snapshots.length,
    backfill: true,
  });
  await recordMlbHomerunSnapshots(runId, snapshots);
  await syncMlbHomerunSnapshotActualsForSlate(slateId);
  revalidatePath("/homerun");

  return { ok: true, message: `Snapshotted ${snapshots.length} MLB homerun rows`, snapshotCount: snapshots.length };
}

function computeLeverage(
  ourProj: number,
  projOwnPct: number,
  fieldProj: number | null = null,
  spg = 0,
  bpg = 0,
  contrarianFactor = 0.7,
): number {
  // edge = how much more bullish we are than the field's expectation.
  // fieldProj priority: avg_fpts_dk (DK's salary-page projection, which drives
  // most contest ownership) → linestar_proj → fallback to ourProj (old behaviour).
  const edge         = fieldProj != null ? ourProj - fieldProj : ourProj;
  const ownFraction  = Math.max(0, Math.min(1, projOwnPct / 100));
  const ceilingBonus = 1.0 + spg * 0.05 + bpg * 0.04;
  return Math.round(edge * Math.pow(1 - ownFraction, contrarianFactor) * ceilingBonus * 1000) / 1000;
}

/** Compute pool-level ownership estimates based on our projections.
 *  Model: score = ourProj / sqrt(salary/$1K)  → normalize to 800% (8 lineup slots).
 *  Returns a Map of array-index → ownership percentage. */
function computePoolOwnership(
  players: Array<{ projection: number | null; salary: number; isOut: boolean }>,
): Map<number, number> {
  const TOTAL_OWN = 800; // 8 roster slots × 100%

  const scores: { idx: number; score: number }[] = [];
  for (let i = 0; i < players.length; i++) {
    const p = players[i];
    const projection = sanitizeProjection(p.projection);
    if (p.isOut || projection == null || projection <= 0 || p.salary <= 0) continue;
    const score = projection / Math.sqrt(p.salary / 1000);
    if (!Number.isFinite(score) || score <= 0) continue;
    scores.push({ idx: i, score });
  }

  const total = scores.reduce((s, e) => s + e.score, 0);
  if (!Number.isFinite(total) || total <= 0) return new Map();

  const result = new Map<number, number>();
  for (const { idx, score } of scores) {
    const ownPct = Math.round((score / total) * TOTAL_OWN * 10) / 10;
    const sanitized = sanitizeOwnershipPct(ownPct);
    if (sanitized != null) result.set(idx, sanitized);
  }
  return result;
}

// ── NBA Stats API (stats.nba.com) backfill ─────────────────────

type NbaOwnershipModelPlayerLike = {
  dkPlayerId?: number;
  matchupId?: number | null;
  salary: number;
  avgFptsDk: number | null;
  linestarProj: number | null;
  projOwnPct: number | null;
  ourProj: number | null;
  liveProj: number | null;
  ourOwnPct: number | null;
  ourLeverage: number | null;
  liveOwnPct: number | null;
  liveLeverage: number | null;
  isOut: boolean;
  _spg?: number;
  _bpg?: number;
};

function normalizeOwnershipVector(values: Array<number | null>, totalOwnership = 800): Array<number | null> {
  const activeValues = values.filter((value): value is number => value != null && Number.isFinite(value) && value > 0);
  const total = activeValues.reduce((sum, value) => sum + value, 0);
  if (total <= 0) return values.map((value) => (value == null ? null : 0));
  return values.map((value) => {
    if (value == null || !Number.isFinite(value) || value <= 0) return value == null ? null : 0;
    return sanitizeOwnershipPct(Math.round((value / total) * totalOwnership * 10) / 10);
  });
}

function computeNbaLiveProjection(blend: NbaProjectionBlend): number | null {
  return sanitizeProjection(blend.finalProj ?? blend.marketProj ?? blend.lsProj ?? blend.modelProj ?? null);
}

function applyNbaOwnershipModels(
  players: NbaOwnershipModelPlayerLike[],
  movementContext?: NbaOddsMovementContext,
): void {
  const ownMap = computePoolOwnership(
    players.map((player) => ({
      projection: player.ourProj,
      salary: player.salary,
      isOut: player.isOut,
    })),
  );
  const liveModelOwnMap = computePoolOwnership(
    players.map((player) => ({
      projection: player.liveProj,
      salary: player.salary,
      isOut: player.isOut,
    })),
  );

  for (let i = 0; i < players.length; i++) {
    players[i].ourOwnPct = sanitizeOwnershipPct(ownMap.get(i) ?? null);
  }

  const liveFieldRaw = players.map((player, index) => {
    if (player.isOut) return 0;
    const lsOwnPct = sanitizeOwnershipPct(player.projOwnPct ?? null);
    const liveModelOwnPct = sanitizeOwnershipPct(liveModelOwnMap.get(index) ?? null);
    const movement = player.dkPlayerId != null ? movementContext?.playerByDkId.get(player.dkPlayerId) : undefined;
    const matchupMovement = player.matchupId != null ? movementContext?.matchupById.get(player.matchupId) : undefined;
    const movementMagnitude = Math.abs(movement?.marketFptsDelta ?? 0) + Math.abs(matchupMovement?.vegasTotalDelta ?? 0) * 0.6;
    const shift = Math.min(0.4, movementMagnitude * 0.05);

    if (lsOwnPct != null && liveModelOwnPct != null) {
      return lsOwnPct * (1 - shift) + liveModelOwnPct * shift;
    }
    return lsOwnPct ?? liveModelOwnPct ?? sanitizeOwnershipPct(player.ourOwnPct ?? null) ?? null;
  });
  const normalizedLiveField = normalizeOwnershipVector(liveFieldRaw);

  for (let i = 0; i < players.length; i++) {
    const player = players[i];
    const fieldOwnPct = normalizedLiveField[i] ?? sanitizeOwnershipPct(
      player.isOut ? 0 : (player.projOwnPct ?? player.ourOwnPct ?? null),
    );
    const fieldProj = sanitizeProjection(player.avgFptsDk ?? player.linestarProj ?? null);
    const spg = player._spg ?? 0;
    const bpg = player._bpg ?? 0;

    player.liveOwnPct = fieldOwnPct;
    player.ourLeverage = !player.isOut && player.ourProj != null && fieldOwnPct != null
      ? sanitizeLeverage(computeLeverage(player.ourProj, fieldOwnPct, fieldProj, spg, bpg))
      : null;
    player.liveLeverage = !player.isOut && player.liveProj != null && fieldOwnPct != null
      ? sanitizeLeverage(computeLeverage(player.liveProj, fieldOwnPct, fieldProj, spg, bpg))
      : null;
  }
}

const MLB_PITCHER_OWNERSHIP_BUDGET = 200;
const MLB_HITTER_OWNERSHIP_BUDGET = 800;
const MLB_PITCHER_SOFTMAX_K = 2.0;

type MlbOwnershipPlayerLike = {
  eligiblePositions: string;
  salary: number;
  isOut: boolean | null;
  ourProj: number | null;
  linestarProj?: number | null;
  linestarOwnPct?: number | null;
  projOwnPct?: number | null;
  avgFptsDk?: number | null;
  ourOwnPct?: number | null;
  ourLeverage?: number | null;
  dkStartingLineupOrder?: number | null;
  dkTeamLineupConfirmed?: boolean | null;
  teamImplied?: number | null;
  oppImplied?: number | null;
  teamMl?: number | null;
  vegasTotal?: number | null;
  isHome?: boolean | null;
};

function normalizeOwnershipScores(
  scores: Array<{ idx: number; score: number }>,
  budget: number,
): Map<number, number> {
  const valid = scores.filter(({ score }) => Number.isFinite(score) && score > 0);
  const total = valid.reduce((sum, entry) => sum + entry.score, 0);
  if (!Number.isFinite(total) || total <= 0) return new Map();

  const result = new Map<number, number>();
  for (const { idx, score } of valid) {
    const ownPct = Math.round((score / total) * budget * 10) / 10;
    const sanitized = sanitizeOwnershipPct(ownPct);
    if (sanitized != null) result.set(idx, sanitized);
  }
  return result;
}

function getMlbProxyProjection(player: Pick<MlbOwnershipPlayerLike, "ourProj" | "linestarProj">): number | null {
  return sanitizeProjection(player.ourProj ?? player.linestarProj ?? null);
}

function getMlbReferenceProjection(player: Pick<MlbOwnershipPlayerLike, "avgFptsDk" | "ourProj" | "linestarProj">): number | null {
  return sanitizeProjection(player.avgFptsDk ?? player.ourProj ?? player.linestarProj ?? null);
}

function computeMlbBaselineOwnershipScore(refProj: number, poolAvg: number): number {
  return Math.max(1, Math.min(50, (refProj / poolAvg) * 15));
}

function applyMlbOwnershipModels<T extends MlbOwnershipPlayerLike>(players: T[]): void {
  const activePitchers = players
    .map((player, idx) => ({ player, idx }))
    .filter(({ player }) => !player.isOut && isPitcherPos(player.eligiblePositions) && player.salary > 0);
  const activeHitters = players
    .map((player, idx) => ({ player, idx }))
    .filter(({ player }) => !player.isOut && !isPitcherPos(player.eligiblePositions) && player.salary > 0);

  const hitterFallbackRefs = activeHitters
    .filter(({ player }) => sanitizeOwnershipPct(player.linestarOwnPct ?? null) == null)
    .map(({ player }) => getMlbReferenceProjection(player) ?? 0)
    .filter((value) => value > 0);
  const hitterPoolAvg = hitterFallbackRefs.length > 0
    ? hitterFallbackRefs.reduce((sum, value) => sum + value, 0) / hitterFallbackRefs.length
    : 0;

  const pitcherFieldScores = activePitchers.flatMap(({ player, idx }) => {
    const proxyProj = getMlbProxyProjection(player);
    const lsOwn = sanitizeOwnershipPct(player.linestarOwnPct ?? null) ?? 0;
    if (proxyProj != null && proxyProj > 0) {
      const valueScore = proxyProj / (player.salary / 1000);
      const score = Math.exp(valueScore * MLB_PITCHER_SOFTMAX_K) * (1 + lsOwn / 100);
      return Number.isFinite(score) && score > 0 ? [{ idx, score }] : [];
    }
    return lsOwn > 0 ? [{ idx, score: lsOwn }] : [];
  });

  const hitterFieldScores = activeHitters.flatMap(({ player, idx }) => {
    const lsOwn = sanitizeOwnershipPct(player.linestarOwnPct ?? null);
    if (lsOwn != null && lsOwn > 0) return [{ idx, score: lsOwn }];
    const refProj = getMlbReferenceProjection(player);
    if (refProj == null || refProj <= 0 || hitterPoolAvg <= 0) return [];
    return [{ idx, score: computeMlbBaselineOwnershipScore(refProj, hitterPoolAvg) }];
  });

  const ourPitcherScores = activePitchers.flatMap(({ player, idx }) => {
    const proxyProj = getMlbProxyProjection(player);
    if (proxyProj == null || proxyProj <= 0) return [];
    const score = proxyProj / Math.sqrt(player.salary / 1000);
    return Number.isFinite(score) && score > 0 ? [{ idx, score }] : [];
  });

  const ourHitterScores = activeHitters.flatMap(({ player, idx }) => {
    const proxyProj = getMlbProxyProjection(player);
    if (proxyProj == null || proxyProj <= 0) return [];
    const score = proxyProj / Math.sqrt(player.salary / 1000);
    return Number.isFinite(score) && score > 0 ? [{ idx, score }] : [];
  });

  const fieldPitcherMap = normalizeOwnershipScores(pitcherFieldScores, MLB_PITCHER_OWNERSHIP_BUDGET);
  const fieldHitterMap = normalizeOwnershipScores(hitterFieldScores, MLB_HITTER_OWNERSHIP_BUDGET);
  const ourPitcherMap = normalizeOwnershipScores(ourPitcherScores, MLB_PITCHER_OWNERSHIP_BUDGET);
  const ourHitterMap = normalizeOwnershipScores(ourHitterScores, MLB_HITTER_OWNERSHIP_BUDGET);

  for (let i = 0; i < players.length; i++) {
    const player = players[i];
    if (player.isOut) {
      player.projOwnPct = 0;
      player.ourOwnPct = 0;
      player.ourLeverage = null;
      continue;
    }

    const projOwnPct = isPitcherPos(player.eligiblePositions)
      ? fieldPitcherMap.get(i) ?? 0
      : fieldHitterMap.get(i) ?? 0;
    const ourOwnPct = isPitcherPos(player.eligiblePositions)
      ? ourPitcherMap.get(i) ?? 0
      : ourHitterMap.get(i) ?? 0;

    player.projOwnPct = sanitizeOwnershipPct(projOwnPct);
    player.ourOwnPct = sanitizeOwnershipPct(ourOwnPct);
  }

  applyMlbOwnershipModelV1(players);

  for (let i = 0; i < players.length; i++) {
    const player = players[i];
    if (player.isOut) continue;
    const projForLev = getMlbProxyProjection(player);
    if (projForLev != null && projForLev > 0 && player.projOwnPct != null) {
      const fieldProj = sanitizeProjection(player.avgFptsDk ?? player.linestarProj ?? null);
      player.ourLeverage = sanitizeLeverage(computeLeverage(projForLev, player.projOwnPct, fieldProj));
    } else {
      player.ourLeverage = null;
    }
  }
}

const NBA_STATS_HEADERS: Record<string, string> = {
  Referer: "https://stats.nba.com/",
  "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
  Accept: "application/json, text/plain, */*",
  "x-nba-stats-origin": "stats",
  "x-nba-stats-token": "true",
};

/** Parse the stats.nba.com response format: { resultSets: [{ headers, rowSet }] } */
function parseNbaResponse(data: { resultSets: Array<{ headers: string[]; rowSet: unknown[][] }> }) {
  const rs = data.resultSets[0];
  return rs.rowSet.map((row) => {
    const obj: Record<string, unknown> = {};
    for (let i = 0; i < rs.headers.length; i++) obj[rs.headers[i]] = row[i];
    return obj;
  });
}

/** Fetch with retry (stats.nba.com is flaky). */
async function fetchNbaStats(url: string, params: Record<string, string>, retries = 3) {
  const qs = new URLSearchParams(params).toString();
  let lastErr: unknown;
  for (let i = 0; i < retries; i++) {
    try {
      const resp = await fetch(`${url}?${qs}`, {
        headers: NBA_STATS_HEADERS,
        signal: AbortSignal.timeout(60_000),
      });
      if (!resp.ok) throw new Error(`NBA API ${resp.status}: ${resp.statusText}`);
      return await resp.json();
    } catch (e) {
      lastErr = e;
      if (i < retries - 1) await new Promise((r) => setTimeout(r, (i + 1) * 5000));
    }
  }
  throw lastErr;
}

/** NBA numeric team ID → standard 3-letter abbreviation */
const NBA_ID_TO_ABBREV: Record<number, string> = {
  1610612737:"ATL",1610612738:"BOS",1610612751:"BKN",1610612766:"CHA",
  1610612741:"CHI",1610612739:"CLE",1610612742:"DAL",1610612743:"DEN",
  1610612765:"DET",1610612744:"GSW",1610612745:"HOU",1610612754:"IND",
  1610612746:"LAC",1610612747:"LAL",1610612763:"MEM",1610612748:"MIA",
  1610612749:"MIL",1610612750:"MIN",1610612740:"NOP",1610612752:"NYK",
  1610612760:"OKC",1610612753:"ORL",1610612755:"PHI",1610612756:"PHX",
  1610612757:"POR",1610612758:"SAC",1610612759:"SAS",1610612761:"TOR",
  1610612762:"UTA",1610612764:"WAS",
};

export async function backfillTeamStats(): Promise<{ ok: boolean; message: string }> {
  try {
    const data = await fetchNbaStats("https://stats.nba.com/stats/leaguedashteamstats", {
      Season: CURRENT_SEASON,
      SeasonType: "Regular Season",
      MeasureType: "Advanced",
      PerMode: "PerGame",
    });
    const rows = parseNbaResponse(data);

    // Build abbreviation → team_id cache
    const dbTeams = await db.select({ teamId: teams.teamId, abbreviation: teams.abbreviation }).from(teams);
    const abbrevMap = new Map(dbTeams.map((t) => [t.abbreviation.toUpperCase(), t.teamId]));

    // Collect all valid rows, then single batch upsert (avoids 30 serial DB round-trips)
    const batch: Array<typeof nbaTeamStats.$inferInsert> = [];
    for (const row of rows) {
      const nbaId = row.TEAM_ID as number;
      const abbrev = NBA_ID_TO_ABBREV[nbaId];
      const teamId = abbrev ? abbrevMap.get(abbrev) : undefined;
      if (!teamId) continue;
      batch.push({
        teamId,
        season: CURRENT_SEASON,
        pace: (row.PACE as number) ?? null,
        offRtg: (row.OFF_RATING as number) ?? null,
        defRtg: (row.DEF_RATING as number) ?? null,
      });
    }

    if (batch.length > 0) {
      await db.insert(nbaTeamStats).values(batch).onConflictDoUpdate({
        target: [nbaTeamStats.teamId, nbaTeamStats.season],
        set: {
          pace: sql`EXCLUDED.pace`,
          offRtg: sql`EXCLUDED.off_rtg`,
          defRtg: sql`EXCLUDED.def_rtg`,
          fetchedAt: sql`NOW()`,
        },
      });
    }

    revalidatePath("/dfs");
    return { ok: true, message: `Team stats: ${batch.length}/30 teams updated for ${CURRENT_SEASON}` };
  } catch (e) {
    return { ok: false, message: `Team stats failed: ${e instanceof Error ? e.message : String(e)}` };
  }
}

export async function backfillPlayerStats(): Promise<{ ok: boolean; message: string }> {
  try {
    // LeagueDashPlayerStats with LastNGames=10 returns pre-aggregated per-game averages
    // (one row per active player) — far smaller payload than LeagueGameLog which returns
    // one row per player-game and requires client-side grouping + averaging.
    const data = await fetchNbaStats("https://stats.nba.com/stats/leaguedashplayerstats", {
      Season: CURRENT_SEASON,
      SeasonType: "Regular Season",
      PerMode: "PerGame",
      MeasureType: "Base",
      LastNGames: "10",
    });
    const rows = parseNbaResponse(data);
    if (rows.length === 0) return { ok: false, message: "No player stats data returned" };

    // Build abbreviation → team_id cache
    const dbTeams = await db.select({ teamId: teams.teamId, abbreviation: teams.abbreviation }).from(teams);
    const abbrevMap = new Map(dbTeams.map((t) => [t.abbreviation.toUpperCase(), t.teamId]));

    const batch: Array<typeof nbaPlayerStats.$inferInsert> = [];

    for (const row of rows) {
      const playerId = row.PLAYER_ID as number;
      if (!playerId) continue;

      const name = row.PLAYER_NAME as string;
      const teamAbbrev = (row.TEAM_ABBREVIATION as string ?? "").toUpperCase();
      const teamId = abbrevMap.get(teamAbbrev) ?? null;
      const n = Math.max(Number(row.GP) || 1, 1);

      const r = (v: unknown) => Math.round((Number(v) || 0) * 10) / 10;

      // DD2 = double-double count over the last N games; divide by GP for rate
      const dd2 = Number(row.DD2) || 0;
      const ddRate = Math.round((dd2 / n) * 1000) / 1000;

      // USG_PCT is a proper usage rate (0–1 scale) from the endpoint; convert to %
      const usgRaw = Number(row.USG_PCT) || 0;
      const usageRate = Math.round(usgRaw * 1000) / 10; // e.g. 0.254 → 25.4

      batch.push({
        playerId, season: CURRENT_SEASON, teamId, name, position: null, games: n,
        avgMinutes: r(row.MIN),
        ppg: r(row.PTS), rpg: r(row.REB), apg: r(row.AST),
        spg: r(row.STL), bpg: r(row.BLK), tovpg: r(row.TOV),
        threefgmPg: r(row.FG3M),
        usageRate, ddRate,
      });
    }

    // Single batch upsert (Neon handles up to ~500 rows fine)
    for (let i = 0; i < batch.length; i += 100) {
      const chunk = batch.slice(i, i + 100);
      await db.insert(nbaPlayerStats).values(chunk).onConflictDoUpdate({
        target: [nbaPlayerStats.playerId, nbaPlayerStats.season],
        set: {
          teamId: sql`EXCLUDED.team_id`, name: sql`EXCLUDED.name`,
          games: sql`EXCLUDED.games`, avgMinutes: sql`EXCLUDED.avg_minutes`,
          ppg: sql`EXCLUDED.ppg`, rpg: sql`EXCLUDED.rpg`, apg: sql`EXCLUDED.apg`,
          spg: sql`EXCLUDED.spg`, bpg: sql`EXCLUDED.bpg`, tovpg: sql`EXCLUDED.tovpg`,
          threefgmPg: sql`EXCLUDED.threefgm_pg`, usageRate: sql`EXCLUDED.usage_rate`,
          ddRate: sql`EXCLUDED.dd_rate`,
          fptsStd: sql`COALESCE(EXCLUDED.fpts_std, nba_player_stats.fpts_std)`,
          fetchedAt: sql`NOW()`,
        },
      });
    }

    revalidatePath("/dfs");
    return { ok: true, message: `Player stats: ${batch.length} players updated (last 10 games)` };
  } catch (e) {
    return { ok: false, message: `Player stats failed: ${e instanceof Error ? e.message : String(e)}` };
  }
}

// ── Player props (The Odds API) ───────────────────────────────

async function fetchNbaPlayerProps(): Promise<{ ok: boolean; message: string }> {
  try {
    await ensureDkPlayerPropColumns();

    const oddsApiKey = process.env.ODDS_API_KEY;
    if (!oddsApiKey) return { ok: false, message: "ODDS_API_KEY not set in Vercel env vars" };

    // Get current slate
    const [slate] = await db
      .select({ id: dkSlates.id, slateDate: dkSlates.slateDate })
      .from(dkSlates).orderBy(desc(dkSlates.slateDate)).limit(1);
    if (!slate) return { ok: false, message: "No slate loaded — load a slate first" };
    const targetDate = slate.slateDate; // "YYYY-MM-DD"

    // Step 1: Get events for today (36h window to handle ET→UTC offset)
    const eventsResp = await fetch(
      `https://api.the-odds-api.com/v4/sports/basketball_nba/events?apiKey=${oddsApiKey}&dateFormat=iso`,
      { next: { revalidate: 0 } },
    );
    if (!eventsResp.ok) throw new Error(`Odds API events: HTTP ${eventsResp.status}`);
    const allEvents = await eventsResp.json() as Array<{
      id: string;
      commence_time: string;
      home_team?: string;
      away_team?: string;
    }>;

    const windowStart = new Date(`${targetDate}T00:00:00Z`).getTime();
    const windowEnd   = windowStart + 36 * 3_600_000;
    const todayEvents = allEvents.filter((e) => {
      const t = new Date(e.commence_time).getTime();
      return t >= windowStart && t < windowEnd;
    });
    if (todayEvents.length === 0)
      return { ok: false, message: `No NBA events found for ${targetDate}` };

    // Step 2: Get slate players for matching
    const slatePlayers = await db
      .select({ id: dkPlayers.id, dkPlayerId: dkPlayers.dkPlayerId, name: dkPlayers.name, teamId: dkPlayers.teamId })
      .from(dkPlayers).where(eq(dkPlayers.slateId, slate.id));
    const slatePlayerById = new Map(slatePlayers.map((player) => [player.id, player]));
    const slatePlayerCandidates = slatePlayers.map(buildPropMatchCandidate);
    const playersByTeamId = new Map<number, PropMatchCandidate[]>();
    for (const player of slatePlayerCandidates) {
      if (player.teamId == null) continue;
      const bucket = playersByTeamId.get(player.teamId) ?? [];
      bucket.push(player);
      playersByTeamId.set(player.teamId, bucket);
    }
    const teamRows = await db
      .select({ teamId: teams.teamId, name: teams.name, abbreviation: teams.abbreviation })
      .from(teams);
    const teamIdByCanonicalName = new Map<string, number>();
    for (const team of teamRows) {
      teamIdByCanonicalName.set(canonicalizeTeamName(team.name), team.teamId);
      teamIdByCanonicalName.set(canonicalizeTeamName(team.abbreviation), team.teamId);
    }

    // Step 3: Collect props across all events.
    // We do not average alternate lines blindly. For each bookmaker/player/stat,
    // choose the "main" over line by price proximity to a standard -110 market,
    // then choose across books by stat-specific bookmaker priority.
    type SelectedPropLine = {
      point: number;
      price: number | null;
      bookmakerKey: string;
      bookmakerTitle: string;
    };
    type PropSet = Partial<Record<NbaProjectionPropStat, SelectedPropLine>>;
    type PropAccumulator = Partial<Record<NbaProjectionPropStat, PropBookCandidate[]>>;
    const propAccum = new Map<number, PropAccumulator>(); // key = dk_players.id

    for (const event of todayEvents) {
      const eventTeamIds = [event.home_team, event.away_team]
        .map((teamName) => (teamName ? teamIdByCanonicalName.get(canonicalizeTeamName(teamName)) ?? null : null))
        .filter((teamId): teamId is number => teamId != null);
      const eventCandidates = eventTeamIds.flatMap((teamId) => playersByTeamId.get(teamId) ?? []);
      if (eventCandidates.length === 0) continue;

      const qs = new URLSearchParams({
        apiKey: oddsApiKey, regions: "us",
        markets: "player_points,player_rebounds,player_assists,player_blocks,player_steals",
        oddsFormat: "american",
      });
      try {
        const r = await fetch(
          `https://api.the-odds-api.com/v4/sports/basketball_nba/events/${event.id}/odds?${qs}`,
          { next: { revalidate: 0 } },
        );
        if (!r.ok) continue;
        const data = await r.json() as {
          bookmakers: Array<{
            key: string;
            title: string;
            markets: Array<{
              key: string;
              outcomes: Array<{ name: string; description: string; point?: number; price?: number }>;
            }>;
          }>;
        };
        for (const bm of (data.bookmakers ?? [])) {
          for (const market of bm.markets) {
            const statKey = NBA_PROP_MARKET_TO_STAT[market.key];
            if (!statKey) continue;
            for (const o of market.outcomes) {
              const point = o.point;
              if (point == null) continue;
              const playerName = extractOverOutcomePlayerName(o);
              if (!playerName) continue;
              const matchedPlayer = matchPropToCandidates(playerName, eventCandidates);
              if (!matchedPlayer) continue;
              const accum = propAccum.get(matchedPlayer.id) ?? {};
              const candidates = accum[statKey] ?? [];
              candidates.push({
                bookmakerKey: bm.key,
                bookmakerTitle: bm.title,
                point,
                price: finiteOrNull(o.price),
              });
              accum[statKey] = candidates;
              propAccum.set(matchedPlayer.id, accum);
            }
          }
        }
      } catch { /* skip individual event failures */ }
    }

    // Collapse accumulators → preferred line per player/stat
    const propData = new Map<number, PropSet>();
    for (const [playerId, accum] of propAccum) {
      const entry: PropSet = {};
      for (const stat of ["pts", "reb", "ast", "blk", "stl"] as const) {
        const selected = pickPreferredPropLine(stat, accum[stat] ?? []);
        if (!selected) continue;
        entry[stat] = {
          point: Math.round(selected.point * 2) / 2,
          price: selected.price != null ? Math.round(selected.price) : null,
          bookmakerKey: selected.bookmakerKey,
          bookmakerTitle: selected.bookmakerTitle,
        };
      }
      propData.set(playerId, entry);
    }

    if (propData.size === 0)
      return { ok: false, message: "No player props returned by Odds API (check API key / plan)" };

    // Step 4: Match props → slate players (exact then fuzzy)
    // Map name → id for slate players
    let propMatched = 0;
    const updates: Array<{
      id: number;
      pts?: SelectedPropLine;
      reb?: SelectedPropLine;
      ast?: SelectedPropLine;
      blk?: SelectedPropLine;
      stl?: SelectedPropLine;
    }> = [];
    for (const [playerId, props] of propData) {
      updates.push({ id: playerId, ...props });
      propMatched++;
    }

    // Step 5: Store props + recompute ourProj for matched players
    if (updates.length > 0) {
      // Bulk update props
      for (const u of updates) {
        await db.update(dkPlayers)
          .set({
            ...(u.pts != null && { propPts: u.pts.point, propPtsPrice: u.pts.price, propPtsBook: u.pts.bookmakerTitle }),
            ...(u.reb != null && { propReb: u.reb.point, propRebPrice: u.reb.price, propRebBook: u.reb.bookmakerTitle }),
            ...(u.ast != null && { propAst: u.ast.point, propAstPrice: u.ast.price, propAstBook: u.ast.bookmakerTitle }),
            ...(u.blk != null && { propBlk: u.blk.point, propBlkPrice: u.blk.price, propBlkBook: u.blk.bookmakerTitle }),
            ...(u.stl != null && { propStl: u.stl.point, propStlPrice: u.stl.price, propStlBook: u.stl.bookmakerTitle }),
          })
          .where(eq(dkPlayers.id, u.id));
      }
      await recordPlayerPropHistory(
        updates.flatMap((u) => {
          const slatePlayer = slatePlayerById.get(u.id);
          if (!slatePlayer) return [];
          return (["pts", "reb", "ast", "blk", "stl"] as const).flatMap((stat) => {
            const selected = u[stat];
            if (!selected) return [];
            return [{
              sport: "nba" as const,
              slateId: slate.id,
              dkPlayerId: slatePlayer.dkPlayerId,
              playerName: slatePlayer.name,
              teamId: slatePlayer.teamId,
              eventId: null,
              marketKey: NBA_PROP_HISTORY_MARKETS[stat],
              line: selected.point,
              price: selected.price,
              bookmakerKey: selected.bookmakerKey,
              bookmakerTitle: selected.bookmakerTitle,
              bookCount: propAccum.get(u.id)?.[stat]?.length ?? 0,
            }];
          });
        }),
      );

      // Recompute ourProj using props for all matched players
      const updatedIds = new Set(updates.map((u) => u.id));
      const projectionSnapshots: Array<{
        slateId: number;
        dkPlayerId: number;
        name: string;
        teamId: number | null;
        salary: number;
        isOut: boolean;
        blend: NbaProjectionBlend;
      }> = [];
      const pool = await db.execute<{
        id: number; slateId: number; dkPlayerId: number; name: string; salary: number;
        teamId: number | null; matchupId: number | null;
        eligiblePositions: string | null;
        avgFptsDk: number | null; projOwnPct: number | null; linestarProj: number | null;
        isOut: boolean | null; ourProj: number | null; liveProj: number | null;
        propPts: number | null; propReb: number | null; propAst: number | null;
        propBlk: number | null; propStl: number | null;
      }>(sql`
        SELECT id, slate_id AS "slateId", dk_player_id AS "dkPlayerId", name, salary,
               team_id AS "teamId", matchup_id AS "matchupId", eligible_positions AS "eligiblePositions",
               avg_fpts_dk AS "avgFptsDk", proj_own_pct AS "projOwnPct", linestar_proj AS "linestarProj",
               is_out AS "isOut", our_proj AS "ourProj", live_proj AS "liveProj",
               prop_pts AS "propPts", prop_reb AS "propReb", prop_ast AS "propAst",
               prop_blk AS "propBlk", prop_stl AS "propStl"
        FROM dk_players WHERE slate_id = ${slate.id}
      `);

      const teamStatRows = await db.select().from(nbaTeamStats).where(eq(nbaTeamStats.season, CURRENT_SEASON));
      const statsByTeam  = new Map(teamStatRows.map((r) => [r.teamId, r]));

      const matchupRows   = await db.select().from(nbaMatchups).where(eq(nbaMatchups.gameDate, targetDate));
      const matchupByTeam = new Map<number, typeof matchupRows[0]>();
      for (const m of matchupRows) {
        if (m.homeTeamId) matchupByTeam.set(m.homeTeamId, m);
        if (m.awayTeamId) matchupByTeam.set(m.awayTeamId, m);
      }

      const playerStatRows = await db.select().from(nbaPlayerStats).where(eq(nbaPlayerStats.season, CURRENT_SEASON));
      const playersByTeam  = new Map<number, typeof playerStatRows>();
      for (const ps of playerStatRows) {
        if (ps.teamId == null) continue;
        const arr = playersByTeam.get(ps.teamId) ?? [];
        arr.push(ps);
        playersByTeam.set(ps.teamId, arr);
      }
      const oddsMovementContext = await buildNbaOddsMovementContext(slate.id, targetDate);
      const projectionCalibration = await loadNbaProjectionCalibration();

      const enriched: Array<{
        id: number;
        dkPlayerId: number;
        matchupId: number | null;
        salary: number;
        avgFptsDk: number | null;
        linestarProj: number | null;
        projOwnPct: number | null;
        ourProj: number | null;
        liveProj: number | null;
        ourOwnPct: number | null;
        ourLeverage: number | null;
        liveOwnPct: number | null;
        liveLeverage: number | null;
        isOut: boolean;
        _spg: number;
        _bpg: number;
      }> = [];
      for (const p of pool.rows) {
        let ourProj = sanitizeProjection(p.ourProj);
        let liveProj = sanitizeProjection(p.liveProj ?? p.ourProj ?? p.linestarProj);
        let spgForLev = 0;
        let bpgForLev = 0;

        const teamStat = p.teamId ? statsByTeam.get(p.teamId) : null;
        const matchup = p.teamId ? matchupByTeam.get(p.teamId) ?? null : null;
        const oppId = matchup && p.teamId ? (matchup.homeTeamId === p.teamId ? matchup.awayTeamId : matchup.homeTeamId) : null;
        const oppStat = oppId ? statsByTeam.get(oppId) : null;
        const candidates = p.teamId ? (playersByTeam.get(p.teamId) ?? []) : [];
        let bestPlayer: typeof playerStatRows[0] | null = null;
        let bestDist = 4;
        for (const ps of candidates) {
          const d = levenshtein(p.name.toLowerCase(), ps.name.toLowerCase());
          if (d < bestDist) { bestDist = d; bestPlayer = ps; }
        }
        if (bestPlayer) {
          spgForLev = bestPlayer.spg ?? 0;
          bpgForLev = bestPlayer.bpg ?? 0;
        }

        if (updatedIds.has(p.id) && bestPlayer && teamStat && matchup && oppStat) {
          const isHome = matchup.homeTeamId === p.teamId;
          const playerMovement = oddsMovementContext.playerByDkId.get(p.dkPlayerId);
          const matchupMovement = p.matchupId != null ? oddsMovementContext.matchupById.get(p.matchupId) : undefined;
          const projectionBlend = buildNbaProjectionBlend(
            bestPlayer,
            teamStat.pace  ?? LEAGUE_AVG_PACE,
            oppStat.pace   ?? LEAGUE_AVG_PACE,
            oppStat.defRtg ?? LEAGUE_AVG_DEF_RTG,
            matchup.vegasTotal, matchup.homeMl, matchup.awayMl, isHome,
            p.linestarProj,
            { propPts: p.propPts, propReb: p.propReb, propAst: p.propAst, propBlk: p.propBlk, propStl: p.propStl },
            {
              propDeltas: playerMovement?.propDeltas,
              marketFptsDelta: playerMovement?.marketFptsDelta,
              vegasTotalDelta: matchupMovement?.vegasTotalDelta,
              homeSpreadDelta: matchupMovement?.homeSpreadDelta,
            },
          );
          ourProj = computeNbaInternalProjection(
            projectionBlend,
            p.eligiblePositions,
            bestPlayer.avgMinutes,
            projectionCalibration,
          );
          liveProj = computeNbaLiveProjection(projectionBlend);
          projectionSnapshots.push({
            slateId: p.slateId,
            dkPlayerId: p.dkPlayerId,
            name: p.name,
            teamId: p.teamId,
            salary: p.salary,
            isOut: p.isOut ?? false,
            blend: projectionBlend,
          });
        }

        enriched.push({
          id: p.id,
          dkPlayerId: p.dkPlayerId,
          matchupId: p.matchupId,
          salary: p.salary,
          avgFptsDk: sanitizeProjection(p.avgFptsDk),
          linestarProj: sanitizeProjection(p.linestarProj),
          projOwnPct: sanitizeOwnershipPct(p.projOwnPct),
          ourProj,
          liveProj,
          ourOwnPct: null,
          ourLeverage: null,
          liveOwnPct: null,
          liveLeverage: null,
          isOut: p.isOut ?? false,
          _spg: spgForLev,
          _bpg: bpgForLev,
        });
      }

      applyNbaOwnershipModels(enriched, oddsMovementContext);

      for (let i = 0; i < enriched.length; i += 50) {
        const batch = enriched.slice(i, i + 50);
        for (const player of batch) {
          await db.update(dkPlayers)
            .set({
              ourProj: player.ourProj,
              liveProj: player.liveProj,
              ourOwnPct: player.ourOwnPct,
              liveOwnPct: player.liveOwnPct,
              ourLeverage: player.ourLeverage,
              liveLeverage: player.liveLeverage,
            })
            .where(eq(dkPlayers.id, player.id));
        }
      }

      const projectionRunId = await createProjectionRun(slate.id, "fetch_props", {
        version: NBA_PROJECTION_MODEL_VERSION,
        source: "fetch_props",
        updatedPlayers: updatedIds.size,
      });
      await recordProjectionSnapshots(projectionRunId, projectionSnapshots);
    }

    revalidatePath("/dfs");
    return {
      ok: true,
      message: `Player props: ${propMatched}/${slatePlayers.length} players matched across ${todayEvents.length} games`,
    };
  } catch (e) {
    return { ok: false, message: `Props failed: ${e instanceof Error ? e.message : String(e)}` };
  }
}

async function fetchMlbPlayerProps(): Promise<{ ok: boolean; message: string }> {
  try {
    await ensureDkPlayerPropColumns();

    const oddsApiKey = process.env.ODDS_API_KEY;
    if (!oddsApiKey) return { ok: false, message: "ODDS_API_KEY not set in Vercel env vars" };

    const [slate] = await db
      .select({ id: dkSlates.id, slateDate: dkSlates.slateDate })
      .from(dkSlates)
      .where(eq(dkSlates.sport, "mlb"))
      .orderBy(desc(dkSlates.slateDate), desc(dkSlates.id))
      .limit(1);
    if (!slate) return { ok: false, message: "No MLB slate loaded — load a slate first" };

    const eventsResp = await fetch(
      `https://api.the-odds-api.com/v4/sports/baseball_mlb/events?apiKey=${oddsApiKey}&dateFormat=iso`,
      { next: { revalidate: 0 } },
    );
    if (!eventsResp.ok) throw new Error(`Odds API MLB events: HTTP ${eventsResp.status}`);
    const allEvents = await eventsResp.json() as Array<{
      id: string;
      commence_time: string;
      home_team?: string;
      away_team?: string;
    }>;

    const windowStart = new Date(`${slate.slateDate}T00:00:00Z`).getTime();
    const windowEnd   = windowStart + 36 * 3_600_000;
    const todayEvents = allEvents.filter((event) => {
      const t = new Date(event.commence_time).getTime();
      return t >= windowStart && t < windowEnd;
    });
    if (todayEvents.length === 0) {
      return { ok: false, message: `No MLB events found for ${slate.slateDate}` };
    }

    const slatePlayers = await db
      .select({
        id: dkPlayers.id,
        dkPlayerId: dkPlayers.dkPlayerId,
        name: dkPlayers.name,
        eligiblePositions: dkPlayers.eligiblePositions,
        teamId: dkPlayers.mlbTeamId,
      })
      .from(dkPlayers)
      .where(eq(dkPlayers.slateId, slate.id));
    const slatePlayerById = new Map(slatePlayers.map((player) => [player.id, player]));
    const slatePlayerCandidates = slatePlayers.map(buildPropMatchCandidate);
    const playersByTeamId = new Map<number, PropMatchCandidate[]>();
    for (const player of slatePlayerCandidates) {
      if (player.teamId == null) continue;
      const bucket = playersByTeamId.get(player.teamId) ?? [];
      bucket.push(player);
      playersByTeamId.set(player.teamId, bucket);
    }
    const roleByPlayerId = new Map(
      slatePlayers.map((player) => [
        player.id,
        player.eligiblePositions.includes("SP") || player.eligiblePositions.includes("RP") ? "pitcher" : "batter",
      ] as const),
    );

    const teamRows = await db
      .select({ teamId: mlbTeams.teamId, name: mlbTeams.name, abbreviation: mlbTeams.abbreviation, dkAbbrev: mlbTeams.dkAbbrev })
      .from(mlbTeams);
    const teamIdByCanonicalName = new Map<string, number>();
    for (const team of teamRows) {
      teamIdByCanonicalName.set(canonicalizeTeamName(team.name), team.teamId);
      teamIdByCanonicalName.set(canonicalizeTeamName(team.abbreviation), team.teamId);
      if (team.dkAbbrev) teamIdByCanonicalName.set(canonicalizeTeamName(team.dkAbbrev), team.teamId);
    }

    type SelectedPropLine = {
      point: number;
      price: number | null;
      bookmakerKey: string;
      bookmakerTitle: string;
    };
    type PropSet = Partial<Record<MlbProjectionPropStat, SelectedPropLine>>;
    type PropAccumulator = Partial<Record<MlbProjectionPropStat, PropBookCandidate[]>>;
    const propAccum = new Map<number, PropAccumulator>();

    for (const event of todayEvents) {
      const eventTeamIds = [event.home_team, event.away_team]
        .map((teamName) => (teamName ? teamIdByCanonicalName.get(canonicalizeTeamName(teamName)) ?? null : null))
        .filter((teamId): teamId is number => teamId != null);
      const eventCandidates = eventTeamIds.flatMap((teamId) => playersByTeamId.get(teamId) ?? []);
      if (eventCandidates.length === 0) continue;

      const qs = new URLSearchParams({
        apiKey: oddsApiKey,
        regions: "us",
        markets: Object.keys(MLB_PROP_MARKET_TO_STAT).join(","),
        oddsFormat: "american",
      });
      try {
        const response = await fetch(
          `https://api.the-odds-api.com/v4/sports/baseball_mlb/events/${event.id}/odds?${qs}`,
          { next: { revalidate: 0 } },
        );
        if (!response.ok) continue;
        const data = await response.json() as {
          bookmakers: Array<{
            key: string;
            title: string;
            markets: Array<{
              key: string;
              outcomes: Array<{ name?: string; description?: string; point?: number; price?: number }>;
            }>;
          }>;
        };

        for (const bookmaker of data.bookmakers ?? []) {
          for (const market of bookmaker.markets ?? []) {
            const statKey = MLB_PROP_MARKET_TO_STAT[market.key];
            if (!statKey) continue;
            for (const outcome of market.outcomes ?? []) {
              const point = outcome.point;
              if (point == null) continue;
              const playerName = extractOverOutcomePlayerName(outcome);
              if (!playerName) continue;
              const matchedPlayer = matchPropToCandidates(playerName, eventCandidates);
              if (!matchedPlayer) continue;
              const accum = propAccum.get(matchedPlayer.id) ?? {};
              const candidates = accum[statKey] ?? [];
              candidates.push({
                bookmakerKey: bookmaker.key,
                bookmakerTitle: bookmaker.title,
                point,
                price: finiteOrNull(outcome.price),
              });
              accum[statKey] = candidates;
              propAccum.set(matchedPlayer.id, accum);
            }
          }
        }
      } catch {
        continue;
      }
    }

    const propData = new Map<number, PropSet>();
    for (const [playerId, accum] of propAccum) {
      const entry: PropSet = {};
      for (const stat of ["hits", "tb", "runs", "rbis", "hr", "ks", "outs", "er"] as const) {
        const selected = pickPreferredMlbPropLine(stat, accum[stat] ?? []);
        if (!selected) continue;
        entry[stat] = {
          point: Math.round(selected.point * 2) / 2,
          price: selected.price != null ? Math.round(selected.price) : null,
          bookmakerKey: selected.bookmakerKey,
          bookmakerTitle: selected.bookmakerTitle,
        };
      }
      if (Object.keys(entry).length > 0) {
        propData.set(playerId, entry);
      }
    }

    if (propData.size === 0) {
      return { ok: false, message: "No MLB player props returned by Odds API for this slate." };
    }

    let propMatched = 0;
    for (const [playerId, props] of propData) {
      const role = roleByPlayerId.get(playerId) ?? "batter";
      if (role === "pitcher") {
        await db.update(dkPlayers)
          .set({
            propPts: props.ks?.point ?? null,
            propPtsPrice: props.ks?.price ?? null,
            propPtsBook: props.ks?.bookmakerTitle ?? null,
            propReb: props.outs?.point ?? null,
            propRebPrice: props.outs?.price ?? null,
            propRebBook: props.outs?.bookmakerTitle ?? null,
            propAst: props.er?.point ?? null,
            propAstPrice: props.er?.price ?? null,
            propAstBook: props.er?.bookmakerTitle ?? null,
            propBlk: null,
            propBlkPrice: null,
            propBlkBook: null,
            propStl: null,
            propStlPrice: null,
            propStlBook: null,
          })
          .where(eq(dkPlayers.id, playerId));
      } else {
        await db.update(dkPlayers)
          .set({
            propPts: props.hits?.point ?? null,
            propPtsPrice: props.hits?.price ?? null,
            propPtsBook: props.hits?.bookmakerTitle ?? null,
            propReb: props.tb?.point ?? null,
            propRebPrice: props.tb?.price ?? null,
            propRebBook: props.tb?.bookmakerTitle ?? null,
            propAst: props.runs?.point ?? null,
            propAstPrice: props.runs?.price ?? null,
            propAstBook: props.runs?.bookmakerTitle ?? null,
            propBlk: props.rbis?.point ?? null,
            propBlkPrice: props.rbis?.price ?? null,
            propBlkBook: props.rbis?.bookmakerTitle ?? null,
            propStl: props.hr?.point ?? null,
            propStlPrice: props.hr?.price ?? null,
            propStlBook: props.hr?.bookmakerTitle ?? null,
          })
          .where(eq(dkPlayers.id, playerId));
      }
      propMatched++;
    }
    await recordPlayerPropHistory(
      Array.from(propData.entries()).flatMap(([playerId, props]) => {
        const slatePlayer = slatePlayerById.get(playerId);
        if (!slatePlayer) return [];
        return (Object.entries(props) as Array<[MlbProjectionPropStat, SelectedPropLine]>).map(([stat, selected]) => ({
          sport: "mlb" as const,
          slateId: slate.id,
          dkPlayerId: slatePlayer.dkPlayerId,
          playerName: slatePlayer.name,
          teamId: slatePlayer.teamId,
          eventId: null,
          marketKey: Object.entries(MLB_PROP_MARKET_TO_STAT).find(([, value]) => value === stat)?.[0] ?? stat,
          line: selected.point,
          price: selected.price,
          bookmakerKey: selected.bookmakerKey,
          bookmakerTitle: selected.bookmakerTitle,
          bookCount: propAccum.get(playerId)?.[stat]?.length ?? 0,
        }));
      }),
    );

    revalidatePath("/dfs");
    return {
      ok: true,
      message: `MLB player props: ${propMatched}/${slatePlayers.length} players matched across ${todayEvents.length} games`,
    };
  } catch (e) {
    return { ok: false, message: `MLB props failed: ${e instanceof Error ? e.message : String(e)}` };
  }
}

export async function fetchPlayerProps(sport: Sport = "nba"): Promise<{ ok: boolean; message: string }> {
  return sport === "mlb" ? fetchMlbPlayerProps() : fetchNbaPlayerProps();
}

export async function auditNbaPropCoverage(gameKeys: string[]): Promise<NbaPropCoverageAuditResult> {
  try {
    const oddsApiKey = process.env.ODDS_API_KEY;
    if (!oddsApiKey) {
      return {
        ok: false,
        message: "ODDS_API_KEY not set in Vercel env vars",
        selectedGames: gameKeys,
        playerPoolCount: 0,
      };
    }

    const [slate] = await db
      .select({ id: dkSlates.id, slateDate: dkSlates.slateDate })
      .from(dkSlates)
      .orderBy(desc(dkSlates.slateDate), desc(dkSlates.id))
      .limit(1);
    if (!slate) {
      return { ok: false, message: "No slate loaded — load a slate first", selectedGames: gameKeys, playerPoolCount: 0 };
    }

    const selectedGameSet = new Set(gameKeys.filter(Boolean));
    const slatePlayers = await db
      .select({ id: dkPlayers.id, dkPlayerId: dkPlayers.dkPlayerId, name: dkPlayers.name, gameInfo: dkPlayers.gameInfo, teamId: dkPlayers.teamId })
      .from(dkPlayers)
      .where(eq(dkPlayers.slateId, slate.id));
    const selectedPlayers = slatePlayers.filter((player) =>
      selectedGameSet.size === 0 || selectedGameSet.has(parseSlateGameKey(player.gameInfo)),
    );
    if (selectedPlayers.length === 0) {
      return {
        ok: false,
        message: "No players found for the selected games.",
        selectedGames: gameKeys,
        playerPoolCount: 0,
      };
    }

    const selectedCandidates = selectedPlayers.map(buildPropMatchCandidate);
    const playersByTeamId = new Map<number, PropMatchCandidate[]>();
    for (const player of selectedCandidates) {
      if (player.teamId == null) continue;
      const bucket = playersByTeamId.get(player.teamId) ?? [];
      bucket.push(player);
      playersByTeamId.set(player.teamId, bucket);
    }
    const teamRows = await db
      .select({ teamId: teams.teamId, name: teams.name, abbreviation: teams.abbreviation })
      .from(teams);
    const teamIdByCanonicalName = new Map<string, number>();
    for (const team of teamRows) {
      teamIdByCanonicalName.set(canonicalizeTeamName(team.name), team.teamId);
      teamIdByCanonicalName.set(canonicalizeTeamName(team.abbreviation), team.teamId);
    }

    const eventsResp = await fetch(
      `https://api.the-odds-api.com/v4/sports/basketball_nba/events?apiKey=${oddsApiKey}&dateFormat=iso`,
      { next: { revalidate: 0 } },
    );
    if (!eventsResp.ok) throw new Error(`Odds API events: HTTP ${eventsResp.status}`);
    const allEvents = await eventsResp.json() as Array<{
      id: string;
      commence_time: string;
      home_team?: string;
      away_team?: string;
    }>;

    const windowStart = new Date(`${slate.slateDate}T00:00:00Z`).getTime();
    const windowEnd   = windowStart + 36 * 3_600_000;
    const todayEvents = allEvents.filter((event) => {
      const t = new Date(event.commence_time).getTime();
      return t >= windowStart && t < windowEnd;
    });
    if (todayEvents.length === 0) {
      return {
        ok: false,
        message: `No NBA events found for ${slate.slateDate}`,
        selectedGames: gameKeys,
        playerPoolCount: selectedPlayers.length,
      };
    }

    const emptyStats = () => ({
      pts: new Set<string>(),
      reb: new Set<string>(),
      ast: new Set<string>(),
      blk: new Set<string>(),
      stl: new Set<string>(),
    });
    const coverageByBook = new Map<string, {
      bookmakerKey: string;
      bookmakerTitle: string;
      uniquePlayers: Set<string>;
      stats: Record<NbaPropAuditStat, Set<string>>;
    }>();

    for (const event of todayEvents) {
      const eventTeamIds = [event.home_team, event.away_team]
        .map((teamName) => (teamName ? teamIdByCanonicalName.get(canonicalizeTeamName(teamName)) ?? null : null))
        .filter((teamId): teamId is number => teamId != null);
      const eventCandidates = eventTeamIds.flatMap((teamId) => playersByTeamId.get(teamId) ?? []);
      if (eventCandidates.length === 0) continue;

      const qs = new URLSearchParams({
        apiKey: oddsApiKey,
        regions: "us",
        markets: Object.keys(NBA_PROP_MARKET_TO_STAT).join(","),
        oddsFormat: "american",
      });
      try {
        const response = await fetch(
          `https://api.the-odds-api.com/v4/sports/basketball_nba/events/${event.id}/odds?${qs}`,
          { next: { revalidate: 0 } },
        );
        if (!response.ok) continue;
        const data = await response.json() as {
          bookmakers: Array<{
            key: string;
            title: string;
            markets: Array<{
              key: string;
              outcomes: Array<{ name?: string; description?: string; point?: number }>;
            }>;
          }>;
        };

        for (const bookmaker of data.bookmakers ?? []) {
          const existing = coverageByBook.get(bookmaker.key) ?? {
            bookmakerKey: bookmaker.key,
            bookmakerTitle: bookmaker.title,
            uniquePlayers: new Set<string>(),
            stats: emptyStats(),
          };
          for (const market of bookmaker.markets ?? []) {
            const statKey = NBA_PROP_MARKET_TO_STAT[market.key];
            if (!statKey) continue;
            for (const outcome of market.outcomes ?? []) {
              const playerName = extractOverOutcomePlayerName(outcome);
              if (!playerName) continue;
              const matchedPlayer = matchPropToCandidates(playerName, eventCandidates);
              if (!matchedPlayer) continue;
              existing.uniquePlayers.add(matchedPlayer.name);
              existing.stats[statKey].add(matchedPlayer.name);
            }
          }
          coverageByBook.set(bookmaker.key, existing);
        }
      } catch {
        continue;
      }
    }

    const books = Array.from(coverageByBook.values())
      .map((book) => ({
        bookmakerKey: book.bookmakerKey,
        bookmakerTitle: book.bookmakerTitle,
        uniquePlayers: book.uniquePlayers.size,
        stats: {
          pts: book.stats.pts.size,
          reb: book.stats.reb.size,
          ast: book.stats.ast.size,
          blk: book.stats.blk.size,
          stl: book.stats.stl.size,
        },
      }))
      .sort((a, b) =>
        (b.stats.pts + b.stats.reb + b.stats.ast + b.stats.blk + b.stats.stl)
        - (a.stats.pts + a.stats.reb + a.stats.ast + a.stats.blk + a.stats.stl)
        || b.uniquePlayers - a.uniquePlayers
        || a.bookmakerTitle.localeCompare(b.bookmakerTitle),
      );

    const leaders: NbaPropCoverageAuditLeader[] = (["pts", "reb", "ast", "blk", "stl"] as NbaPropAuditStat[])
      .map((stat) => {
        const best = books.reduce<NbaPropCoverageAuditBook | null>((leader, book) => {
          if (!leader || book.stats[stat] > leader.stats[stat]) return book;
          return leader;
        }, null);
        return best
          ? {
              stat,
              bookmakerKey: best.bookmakerKey,
              bookmakerTitle: best.bookmakerTitle,
              count: best.stats[stat],
            }
          : null;
      })
      .filter((leader): leader is NbaPropCoverageAuditLeader => !!leader && leader.count > 0);

    if (books.length === 0) {
      return {
        ok: false,
        message: "No prop coverage found for the selected games.",
        selectedGames: gameKeys,
        playerPoolCount: selectedPlayers.length,
      };
    }

    return {
      ok: true,
      message: `Audited ${books.length} bookmakers for ${selectedPlayers.length} slate players.`,
      selectedGames: gameKeys,
      playerPoolCount: selectedPlayers.length,
      bookmakerCount: books.length,
      books,
      leaders,
    };
  } catch (e) {
    return {
      ok: false,
      message: `Prop coverage audit failed: ${e instanceof Error ? e.message : String(e)}`,
      selectedGames: gameKeys,
      playerPoolCount: 0,
    };
  }
}

// ── DK API fetcher ────────────────────────────────────────────

export async function auditMlbPropCoverage(gameKeys: string[]): Promise<MlbPropCoverageAuditResult> {
  try {
    const oddsApiKey = process.env.ODDS_API_KEY;
    if (!oddsApiKey) {
      return {
        ok: false,
        message: "ODDS_API_KEY not set in Vercel env vars",
        selectedGames: gameKeys,
        playerPoolCount: 0,
      };
    }

    const [slate] = await db
      .select({ id: dkSlates.id, slateDate: dkSlates.slateDate })
      .from(dkSlates)
      .where(eq(dkSlates.sport, "mlb"))
      .orderBy(desc(dkSlates.slateDate), desc(dkSlates.id))
      .limit(1);
    if (!slate) {
      return { ok: false, message: "No MLB slate loaded — load a slate first", selectedGames: gameKeys, playerPoolCount: 0 };
    }

    const selectedGameSet = new Set(gameKeys.filter(Boolean));
    const slatePlayers = await db
      .select({ id: dkPlayers.id, dkPlayerId: dkPlayers.dkPlayerId, name: dkPlayers.name, gameInfo: dkPlayers.gameInfo, teamId: dkPlayers.mlbTeamId })
      .from(dkPlayers)
      .where(eq(dkPlayers.slateId, slate.id));
    const selectedPlayers = slatePlayers.filter((player) =>
      selectedGameSet.size === 0 || selectedGameSet.has(parseSlateGameKey(player.gameInfo)),
    );
    if (selectedPlayers.length === 0) {
      return {
        ok: false,
        message: "No MLB players found for the selected games.",
        selectedGames: gameKeys,
        playerPoolCount: 0,
      };
    }

    const selectedCandidates = selectedPlayers.map(buildPropMatchCandidate);
    const playersByTeamId = new Map<number, PropMatchCandidate[]>();
    for (const player of selectedCandidates) {
      if (player.teamId == null) continue;
      const bucket = playersByTeamId.get(player.teamId) ?? [];
      bucket.push(player);
      playersByTeamId.set(player.teamId, bucket);
    }
    const teamRows = await db
      .select({ teamId: mlbTeams.teamId, name: mlbTeams.name, abbreviation: mlbTeams.abbreviation, dkAbbrev: mlbTeams.dkAbbrev })
      .from(mlbTeams);
    const teamIdByCanonicalName = new Map<string, number>();
    for (const team of teamRows) {
      teamIdByCanonicalName.set(canonicalizeTeamName(team.name), team.teamId);
      teamIdByCanonicalName.set(canonicalizeTeamName(team.abbreviation), team.teamId);
      if (team.dkAbbrev) teamIdByCanonicalName.set(canonicalizeTeamName(team.dkAbbrev), team.teamId);
    }

    const eventsResp = await fetch(
      `https://api.the-odds-api.com/v4/sports/baseball_mlb/events?apiKey=${oddsApiKey}&dateFormat=iso`,
      { next: { revalidate: 0 } },
    );
    if (!eventsResp.ok) throw new Error(`Odds API MLB events: HTTP ${eventsResp.status}`);
    const allEvents = await eventsResp.json() as Array<{
      id: string;
      commence_time: string;
      home_team?: string;
      away_team?: string;
    }>;

    const windowStart = new Date(`${slate.slateDate}T00:00:00Z`).getTime();
    const windowEnd   = windowStart + 36 * 3_600_000;
    const todayEvents = allEvents.filter((event) => {
      const t = new Date(event.commence_time).getTime();
      return t >= windowStart && t < windowEnd;
    });
    if (todayEvents.length === 0) {
      return {
        ok: false,
        message: `No MLB events found for ${slate.slateDate}`,
        selectedGames: gameKeys,
        playerPoolCount: selectedPlayers.length,
      };
    }

    const emptyStats = () => ({
      hits: new Set<string>(),
      tb: new Set<string>(),
      runs: new Set<string>(),
      rbis: new Set<string>(),
      hr: new Set<string>(),
      ks: new Set<string>(),
      outs: new Set<string>(),
      er: new Set<string>(),
    });
    const coverageByBook = new Map<string, {
      bookmakerKey: string;
      bookmakerTitle: string;
      uniquePlayers: Set<string>;
      stats: Record<MlbPropAuditStat, Set<string>>;
    }>();

    for (const event of todayEvents) {
      const eventTeamIds = [event.home_team, event.away_team]
        .map((teamName) => (teamName ? teamIdByCanonicalName.get(canonicalizeTeamName(teamName)) ?? null : null))
        .filter((teamId): teamId is number => teamId != null);
      const eventCandidates = eventTeamIds.flatMap((teamId) => playersByTeamId.get(teamId) ?? []);
      if (eventCandidates.length === 0) continue;

      const qs = new URLSearchParams({
        apiKey: oddsApiKey,
        regions: "us",
        markets: Object.keys(MLB_PROP_MARKET_TO_STAT).join(","),
        oddsFormat: "american",
      });
      try {
        const response = await fetch(
          `https://api.the-odds-api.com/v4/sports/baseball_mlb/events/${event.id}/odds?${qs}`,
          { next: { revalidate: 0 } },
        );
        if (!response.ok) continue;
        const data = await response.json() as {
          bookmakers: Array<{
            key: string;
            title: string;
            markets: Array<{
              key: string;
              outcomes: Array<{ name?: string; description?: string; point?: number }>;
            }>;
          }>;
        };

        for (const bookmaker of data.bookmakers ?? []) {
          const existing = coverageByBook.get(bookmaker.key) ?? {
            bookmakerKey: bookmaker.key,
            bookmakerTitle: bookmaker.title,
            uniquePlayers: new Set<string>(),
            stats: emptyStats(),
          };
          for (const market of bookmaker.markets ?? []) {
            const statKey = MLB_PROP_MARKET_TO_STAT[market.key];
            if (!statKey) continue;
            for (const outcome of market.outcomes ?? []) {
              const playerName = extractOverOutcomePlayerName(outcome);
              if (!playerName) continue;
              const matchedPlayer = matchPropToCandidates(playerName, eventCandidates);
              if (!matchedPlayer) continue;
              existing.uniquePlayers.add(matchedPlayer.name);
              existing.stats[statKey].add(matchedPlayer.name);
            }
          }
          coverageByBook.set(bookmaker.key, existing);
        }
      } catch {
        continue;
      }
    }

    const books = Array.from(coverageByBook.values())
      .map((book) => ({
        bookmakerKey: book.bookmakerKey,
        bookmakerTitle: book.bookmakerTitle,
        uniquePlayers: book.uniquePlayers.size,
        stats: {
          hits: book.stats.hits.size,
          tb: book.stats.tb.size,
          runs: book.stats.runs.size,
          rbis: book.stats.rbis.size,
          hr: book.stats.hr.size,
          ks: book.stats.ks.size,
          outs: book.stats.outs.size,
          er: book.stats.er.size,
        },
      }))
      .sort((a, b) =>
        (b.stats.hits + b.stats.tb + b.stats.runs + b.stats.rbis + b.stats.hr + b.stats.ks + b.stats.outs + b.stats.er)
        - (a.stats.hits + a.stats.tb + a.stats.runs + a.stats.rbis + a.stats.hr + a.stats.ks + a.stats.outs + a.stats.er)
        || b.uniquePlayers - a.uniquePlayers
        || a.bookmakerTitle.localeCompare(b.bookmakerTitle),
      );

    const leaders: MlbPropCoverageAuditLeader[] = (["hits", "tb", "runs", "rbis", "hr", "ks", "outs", "er"] as MlbPropAuditStat[])
      .map((stat) => {
        const best = books.reduce<MlbPropCoverageAuditBook | null>((leader, book) => {
          if (!leader || book.stats[stat] > leader.stats[stat]) return book;
          return leader;
        }, null);
        return best
          ? {
              stat,
              bookmakerKey: best.bookmakerKey,
              bookmakerTitle: best.bookmakerTitle,
              count: best.stats[stat],
            }
          : null;
      })
      .filter((leader): leader is MlbPropCoverageAuditLeader => !!leader && leader.count > 0);

    if (books.length === 0) {
      return {
        ok: false,
        message: "No MLB prop coverage found for the selected games.",
        selectedGames: gameKeys,
        playerPoolCount: selectedPlayers.length,
      };
    }

    return {
      ok: true,
      message: `Audited ${books.length} bookmakers for ${selectedPlayers.length} MLB slate players.`,
      selectedGames: gameKeys,
      playerPoolCount: selectedPlayers.length,
      bookmakerCount: books.length,
      books,
      leaders,
    };
  } catch (e) {
    return {
      ok: false,
      message: `MLB prop coverage audit failed: ${e instanceof Error ? e.message : String(e)}`,
      selectedGames: gameKeys,
      playerPoolCount: 0,
    };
  }
}

const DK_HEADERS = {
  "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
  "Accept": "application/json",
};

const POS_ORDER = ["PG", "SG", "SF", "PF", "C", "G", "F", "UTIL"];

type DkApiPlayer = {
  name: string; dkId: number; teamAbbrev: string;
  eligiblePositions: string; salary: number;
  gameInfo: string; avgFptsDk: number | null;
  /** DK injury status: "None" | "O" | "Q" | "GTD" | "D" | "OUT" */
  dkStatus: string;
  /** True = DK has locked this player out of draftability */
  isDisabled: boolean;
  startingLineupOrder: number | null;
  inStartingLineup: boolean | null;
  probableStarter: boolean | null;
  likelyPitcher: boolean | null;
  startingPitcher: boolean | null;
};

type DkDraftStat = { id: number; abbr?: string; name?: string };

const DK_PGA_STARTING_LINEUP_ORDER = 99;
const DK_PGA_IN_STARTING_LINEUP = 100;
const DK_PGA_PROBABLE_STARTER = 130;
const DK_PGA_LIKELY_PITCHER = 137;
const DK_PGA_STARTING_PITCHER = 110;
const DK_DEFAULT_PROJ_STAT_ID = 279;

function parseDkBoolean(value: unknown): boolean | null {
  if (typeof value === "boolean") return value;
  if (typeof value !== "string") return null;
  const lowered = value.trim().toLowerCase();
  if (lowered === "true") return true;
  if (lowered === "false") return false;
  return null;
}

function parsePositiveInt(value: unknown): number | null {
  const parsed = Number.parseInt(String(value ?? ""), 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : null;
}

function resolveDkProjStatId(draftStats: DkDraftStat[] | undefined): number {
  for (const stat of draftStats ?? []) {
    if (stat.abbr === "FPPG" || stat.name === "Fantasy Points Per Game") {
      return stat.id;
    }
  }
  return DK_DEFAULT_PROJ_STAT_ID;
}

function buildPlayerGameAttrMap(attrs: unknown): Map<number, unknown> {
  const map = new Map<number, unknown>();
  if (!Array.isArray(attrs)) return map;
  for (const attr of attrs as Array<{ id?: unknown; value?: unknown }>) {
    if (typeof attr?.id === "number") map.set(attr.id, attr.value);
  }
  return map;
}

function isLikelyActiveMlbPitcher(player: Pick<DkApiPlayer, "eligiblePositions" | "startingPitcher" | "likelyPitcher" | "probableStarter">): boolean {
  if (!isPitcherPos(player.eligiblePositions)) return true;
  const signals = [player.startingPitcher, player.likelyPitcher, player.probableStarter].filter((v): v is boolean => v != null);
  if (signals.length === 0) return true;
  return signals.some(Boolean);
}

async function fetchDkPlayersFromApi(draftGroupId: number): Promise<DkApiPlayer[]> {
  const url = `https://api.draftkings.com/draftgroups/v1/draftgroups/${draftGroupId}/draftables`;
  const resp = await fetch(url, { headers: DK_HEADERS, next: { revalidate: 0 } });
  if (!resp.ok) throw new Error(`DK API ${resp.status}: ${url}`);
  const data = await resp.json() as { draftables: Record<string, unknown>[]; draftStats?: DkDraftStat[] };
  const { draftables } = data;
  const projStatId = resolveDkProjStatId(data.draftStats);

  // Group by playerId — each player has one entry per eligible roster slot
  const byPlayer = new Map<number, typeof draftables>();
  for (const entry of draftables) {
    const pid = entry.playerId as number;
    if (!byPlayer.has(pid)) byPlayer.set(pid, []);
    byPlayer.get(pid)!.push(entry);
  }

  const players: DkApiPlayer[] = [];

  for (const [, entries] of byPlayer) {
    const sorted = [...entries].sort((a, b) => (a.rosterSlotId as number) - (b.rosterSlotId as number));
    const canonical = sorted[0];

    const allPos = new Set<string>(["UTIL"]);
    for (const e of sorted) {
      const pos = e.position as string;
      if (pos) allPos.add(pos);
    }
    const eligiblePositions = [...allPos]
      .sort((a, b) => {
        const ai = POS_ORDER.indexOf(a), bi = POS_ORDER.indexOf(b);
        return (ai === -1 ? 99 : ai) - (bi === -1 ? 99 : bi);
      })
      .join("/");

    // DK's own FPTS projection (FPPG stat id varies by sport)
    let avgFptsDk: number | null = null;
    for (const attr of (canonical.draftStatAttributes as { id: number; value: string }[] ?? [])) {
      if (attr.id === projStatId) { avgFptsDk = parseFloat(attr.value) || null; break; }
    }

    const playerGameAttrs = buildPlayerGameAttrMap(canonical.playerGameAttributes);

    // DK injury / availability status
    const dkStatus   = (canonical.status as string) || "None";
    const isDisabled = !!(canonical.isDisabled as boolean);

    // Game info string — use Intl to handle EDT/EST automatically
    let gameInfo = "";
    const comp = canonical.competition as Record<string, unknown> | null;
    if (comp) {
      const name  = ((comp.name as string) ?? "").replace(" @ ", "@").replace(/ /g, "");
      const start = comp.startTime as string;
      if (start) {
        try {
          const dt    = new Date(start);
          const parts = new Intl.DateTimeFormat("en-US", {
            timeZone: "America/New_York",
            month: "2-digit", day: "2-digit", year: "numeric",
            hour: "numeric", minute: "2-digit", hour12: true,
          }).formatToParts(dt);
          const p = (t: string) => parts.find((x) => x.type === t)?.value ?? "";
          gameInfo = `${name} ${p("month")}/${p("day")}/${p("year")} ${p("hour")}:${p("minute")}${p("dayPeriod")} ET`;
        } catch {
          gameInfo = name;
        }
      } else {
        gameInfo = name;
      }
    }

    players.push({
      name:              (canonical.displayName as string) ?? "",
      dkId:              canonical.draftableId as number,
      teamAbbrev:        ((canonical.teamAbbreviation as string) ?? "").toUpperCase(),
      eligiblePositions,
      salary:            canonical.salary as number ?? 0,
      gameInfo,
      avgFptsDk,
      dkStatus,
      isDisabled,
      startingLineupOrder: parsePositiveInt(playerGameAttrs.get(DK_PGA_STARTING_LINEUP_ORDER)),
      inStartingLineup: parseDkBoolean(playerGameAttrs.get(DK_PGA_IN_STARTING_LINEUP)),
      probableStarter: parseDkBoolean(playerGameAttrs.get(DK_PGA_PROBABLE_STARTER)),
      likelyPitcher: parseDkBoolean(playerGameAttrs.get(DK_PGA_LIKELY_PITCHER)),
      startingPitcher: parseDkBoolean(playerGameAttrs.get(DK_PGA_STARTING_PITCHER)),
    });
  }
  return players;
}

async function resolveDraftGroupId(contestId: number): Promise<number> {
  const url = `https://api.draftkings.com/contests/v1/contests/${contestId}`;
  const resp = await fetch(url, { headers: DK_HEADERS, next: { revalidate: 0 } });
  if (!resp.ok) throw new Error(`DK API ${resp.status} for contest ${contestId}`);
  const data = await resp.json() as { contestDetail: { draftGroupId: number } };
  return data.contestDetail.draftGroupId;
}

// ── Parse slate date from game_info ──────────────────────────

function parseSlateDate(gameInfo: string): string | null {
  const m = gameInfo.match(/(\d{2}\/\d{2}\/\d{4})/);
  if (!m) return null;
  const [mm, dd, yyyy] = m[1].split("/");
  return `${yyyy}-${mm}-${dd}`;
}

// ── Server Actions ────────────────────────────────────────────

export async function processDkSlate(formData: FormData): Promise<{
  ok: boolean; message: string; playerCount?: number; matchRate?: number;
}> {
  const dkFile        = formData.get("dkFile") as File | null;
  const lsFile        = formData.get("lsFile") as File | null;
  const cashLineStr   = formData.get("cashLine") as string | null;
  const contestType   = normalizeDkSlateTiming(formData.get("contestType") as string | null);
  const fieldSizeStr  = formData.get("fieldSize") as string | null;
  const contestFormat = (formData.get("contestFormat") as string | null) || undefined;
  if (!dkFile) return { ok: false, message: "DK CSV required" };

  const dkPlayers_ = parseDkCsv(await dkFile.text());
  if (dkPlayers_.length === 0) return { ok: false, message: "No players parsed from DK CSV" };

  const lsMap    = lsFile ? parseLinestarCsv(await lsFile.text()) : new Map<string, LinestarEntry>();
  const cashLine = cashLineStr ? parseFloat(cashLineStr) : undefined;
  const fieldSize = fieldSizeStr ? parseInt(fieldSizeStr, 10) : undefined;
  return enrichAndSave(
    dkPlayers_, lsMap,
    isNaN(cashLine!) ? undefined : cashLine,
    undefined,
    contestType,
    fieldSize && !isNaN(fieldSize) ? fieldSize : undefined,
    contestFormat,
  );
}

// ── Auto-populate matchups from DK player pool ───────────────
//
// Called when no nba_matchups rows exist for the slate date — happens when
// the web UI loads a slate before daily_stats.yml has run. Parses games from
// the "away@home" game key in each player's game_info, upserts matchup rows,
// then optionally fills Vegas totals/MLs from The Odds API if ODDS_API_KEY
// is available in the environment.

async function ensureMatchupsForSlate(
  slateDate: string,
  dkPlayers_: Array<{ gameInfo: string | null }>,
  abbrevToId: Map<string, number>,
): Promise<string[]> {
  await ensureOddsHistoryTables();
  const debug: string[] = [];
  const resolve = (abbrev: string): number | null => {
    const canonical = DK_OVERRIDES[abbrev] ?? abbrev;
    return abbrevToId.get(canonical) ?? null;
  };

  // Parse unique game keys like "CHI@OKC" → away=CHI, home=OKC
  const gameSeen = new Set<string>();
  const games: { homeTeamId: number; awayTeamId: number }[] = [];
  for (const p of dkPlayers_) {
    const key = p.gameInfo?.split(" ")[0];
    if (!key || gameSeen.has(key)) continue;
    gameSeen.add(key);
    const [awayAbbr, homeAbbr] = key.split("@");
    const homeTeamId = resolve(homeAbbr ?? "");
    const awayTeamId = resolve(awayAbbr ?? "");
    if (homeTeamId && awayTeamId) {
      games.push({ homeTeamId, awayTeamId });
    } else {
      debug.push(`gameInfo parse failed: "${key}" → home=${homeAbbr}→${homeTeamId ?? "null"} away=${awayAbbr}→${awayTeamId ?? "null"}`);
    }
  }
  debug.push(`games parsed from gameInfo: ${[...gameSeen].join(", ") || "none"}`);

  // Always insert — unique constraint on (game_date, home_team_id, away_team_id)
  // means onConflictDoNothing skips true duplicates but adds missing games.
  if (games.length > 0) {
    await db.insert(nbaMatchups)
      .values(games.map((g) => ({ gameDate: slateDate, ...g })))
      .onConflictDoNothing();
    debug.push(`matchup upsert: ${games.length} games attempted`);
  }

  // Only fetch odds for matchup rows that still have no vegasTotal (avoid wasting quota)
  const needsOdds = await db.select({ id: nbaMatchups.id })
    .from(nbaMatchups)
    .where(and(eq(nbaMatchups.gameDate, slateDate), sql`vegas_total IS NULL`))
    .limit(1);
  debug.push(`rows needing odds: ${needsOdds.length}`);

  const oddsKey = process.env.ODDS_API_KEY;
  debug.push(`ODDS_API_KEY: ${oddsKey ? `set (${oddsKey.slice(0, 6)}…)` : "NOT SET"}`);

  if (oddsKey && needsOdds.length > 0) {
    try {
      const oddsUrl = new URL("https://api.the-odds-api.com/v4/sports/basketball_nba/odds/");
      oddsUrl.searchParams.set("apiKey", oddsKey);
      oddsUrl.searchParams.set("regions", "us");
      oddsUrl.searchParams.set("markets", "h2h,spreads,totals");
      oddsUrl.searchParams.set("oddsFormat", "american");
      const oddsResp = await fetch(oddsUrl.toString(), { next: { revalidate: 0 } });
      debug.push(`Odds API status: ${oddsResp.status} ${oddsResp.statusText}`);
      if (oddsResp.ok) {
        const oddsGames = await oddsResp.json() as Array<{
          home_team: string; away_team: string;
          bookmakers: Array<{ markets: Array<{ key: string; outcomes: Array<{ name: string; price: number; point?: number }> }> }>;
        }>;
        debug.push(`Odds API games returned: ${oddsGames.length} — ${oddsGames.map((g) => `${g.away_team} @ ${g.home_team}`).join(", ") || "none"}`);

        // Build home-name → matchup lookup
        const matchupRows = await db.execute<{ id: number; homeName: string; homeTeamId: number | null; awayTeamId: number | null }>(sql`
          SELECT m.id, t.name AS "homeName", m.home_team_id AS "homeTeamId", m.away_team_id AS "awayTeamId"
          FROM nba_matchups m
          JOIN teams t ON t.team_id = m.home_team_id
          WHERE m.game_date = ${slateDate}
        `);
        const byHome = new Map(matchupRows.rows.map((r) => [r.homeName, r]));
        debug.push(`nba_matchups home names for ${slateDate}: ${[...byHome.keys()].join(", ") || "none"}`);

        let oddsUpdated = 0;
        const historyRows: GameOddsHistoryInput[] = [];
        for (const og of oddsGames) {
          const matchup = byHome.get(og.home_team);
          if (!matchup) { debug.push(`no matchup found for "${og.home_team}"`); continue; }
          const homePrices: number[] = [], awayPrices: number[] = [], totalPoints: number[] = [], homeSpreads: number[] = [];
          for (const bm of og.bookmakers ?? []) {
            for (const market of bm.markets ?? []) {
              if (market.key === "h2h") {
                const ho = market.outcomes.find((o) => o.name === og.home_team);
                const ao = market.outcomes.find((o) => o.name === og.away_team);
                if (ho) homePrices.push(ho.price);
                if (ao) awayPrices.push(ao.price);
              } else if (market.key === "spreads") {
                const homeOutcome = market.outcomes.find((o) => o.name === og.home_team);
                if (homeOutcome?.point != null) homeSpreads.push(homeOutcome.point);
              } else if (market.key === "totals") {
                const over = market.outcomes.find((o) => o.name === "Over");
                if (over?.point != null) totalPoints.push(over.point);
              }
            }
          }
          const homeMl = homePrices.length ? Math.round(homePrices.reduce((a, b) => a + b, 0) / homePrices.length) : null;
          const awayMl = awayPrices.length ? Math.round(awayPrices.reduce((a, b) => a + b, 0) / awayPrices.length) : null;
          const homeSpread = homeSpreads.length ? roundHalf(homeSpreads.reduce((a, b) => a + b, 0) / homeSpreads.length) : null;
          const vegasTotal = totalPoints.length ? roundHalf(totalPoints.reduce((a, b) => a + b, 0) / totalPoints.length) : null;
          const homeWinProb = homeMl != null && awayMl != null ? mlToProb(homeMl) / (mlToProb(homeMl) + mlToProb(awayMl)) : null;
          if (homeMl || awayMl || vegasTotal || homeSpread) {
            await db.execute(sql`
              UPDATE nba_matchups
              SET home_ml = ${homeMl}, away_ml = ${awayMl}, home_spread = ${homeSpread}, vegas_total = ${vegasTotal}, vegas_prob_home = ${homeWinProb}
              WHERE id = ${matchup.id}
            `);
            historyRows.push({
              sport: "nba",
              matchupId: matchup.id,
              eventId: null,
              gameDate: slateDate,
              homeTeamId: matchup.homeTeamId,
              awayTeamId: matchup.awayTeamId,
              homeTeamName: og.home_team,
              awayTeamName: og.away_team,
              bookmakerCount: og.bookmakers?.length ?? 0,
              homeMl,
              awayMl,
              homeSpread,
              vegasTotal,
              homeWinProb,
            });
            oddsUpdated++;
          }
        }
        await recordGameOddsHistory(historyRows);
        debug.push(`odds updated: ${oddsUpdated} matchups`);
      } else {
        const body = await oddsResp.text().catch(() => "");
        debug.push(`Odds API error body: ${body.slice(0, 200)}`);
      }
    } catch (e) {
      debug.push(`Odds API exception: ${e instanceof Error ? e.message : String(e)}`);
    }
  } else if (!oddsKey) {
    debug.push("skipping odds: key not set");
  } else {
    debug.push("skipping odds: all matchups already have vegasTotal");
  }

  return debug;
}

// ── Shared enrichment (used by both CSV and API paths) ───────

async function enrichAndSave(
  dkPlayers_: DkApiPlayer[],
  lsMap: Map<string, LinestarEntry>,
  cashLine?: number,
  draftGroupId?: number,
  contestType?: string,
  fieldSize?: number,
  contestFormat?: string,
): Promise<{ ok: boolean; message: string; playerCount?: number; matchRate?: number }> {
  const normalizedContestType = normalizeDkSlateTiming(contestType);
  let slateDate = "";
  for (const p of dkPlayers_) {
    const d = parseSlateDate(p.gameInfo);
    if (d) { slateDate = d; break; }
  }
  if (!slateDate) slateDate = new Date().toISOString().slice(0, 10);

  const gameCount = new Set(dkPlayers_.map((p) => p.gameInfo.split(" ")[0])).size;

  const slateValues: {
    slateDate: string; gameCount: number; sport: string;
    cashLine?: number; dkDraftGroupId?: number;
    contestType?: string; fieldSize?: number; contestFormat?: string;
  } = { slateDate, gameCount, sport: "nba" };
  if (cashLine != null) slateValues.cashLine = cashLine;
  if (draftGroupId != null) slateValues.dkDraftGroupId = draftGroupId;
  if (normalizedContestType) slateValues.contestType = normalizedContestType;
  if (fieldSize != null) slateValues.fieldSize = fieldSize;
  if (contestFormat) slateValues.contestFormat = contestFormat;

  const conflictSet: Record<string, unknown> = { gameCount };
  if (cashLine != null) conflictSet.cashLine = cashLine;
  // COALESCE: don't overwrite an existing draft group ID with null (CSV re-load)
  if (draftGroupId != null) conflictSet.dkDraftGroupId = draftGroupId;
  if (normalizedContestType) conflictSet.contestType = normalizedContestType;
  if (fieldSize != null) conflictSet.fieldSize = fieldSize;
  if (contestFormat) conflictSet.contestFormat = contestFormat;

  const [slate] = await db
    .insert(dkSlates)
    .values(slateValues)
    .onConflictDoUpdate({
      target: [dkSlates.slateDate, dkSlates.contestType, dkSlates.contestFormat, dkSlates.sport],
      set: conflictSet,
    })
    .returning({ id: dkSlates.id });
  const slateId = slate.id;

  const teamRows = await db.select({ teamId: teams.teamId, abbreviation: teams.abbreviation }).from(teams);
  const abbrevToId = new Map(teamRows.map((t) => [t.abbreviation.toUpperCase(), t.teamId]));

  // Auto-populate matchups from DK player pool if schedule hasn't run yet
  await ensureMatchupsForSlate(slateDate, dkPlayers_, abbrevToId);

  const matchupRows = await db.select().from(nbaMatchups).where(eq(nbaMatchups.gameDate, slateDate));
  const matchupByTeam = new Map<number, typeof matchupRows[0]>();
  for (const m of matchupRows) {
    if (m.homeTeamId) matchupByTeam.set(m.homeTeamId, m);
    if (m.awayTeamId) matchupByTeam.set(m.awayTeamId, m);
  }

  const teamStatRows = await db.select().from(nbaTeamStats).where(eq(nbaTeamStats.season, CURRENT_SEASON));
  const statsByTeam = new Map(teamStatRows.map((r) => [r.teamId, r]));

  const playerStatRows = await db.select().from(nbaPlayerStats).where(eq(nbaPlayerStats.season, CURRENT_SEASON));
  const playersByTeam = new Map<number, typeof playerStatRows>();
  for (const ps of playerStatRows) {
    if (ps.teamId == null) continue;
    const arr = playersByTeam.get(ps.teamId) ?? [];
    arr.push(ps);
    playersByTeam.set(ps.teamId, arr);
  }
  const oddsMovementContext = await buildNbaOddsMovementContext(slateId, slateDate);
  const projectionCalibration = await loadNbaProjectionCalibration();

  let lsMatched = 0;
  let projComputed = 0;
  const insertValues = [];
  const projectionSnapshots: Array<{
    slateId: number;
    dkPlayerId: number;
    name: string;
    teamId: number | null;
    salary: number;
    isOut: boolean;
    blend: NbaProjectionBlend;
  }> = [];

  for (const p of dkPlayers_) {
    const canonical = DK_OVERRIDES[p.teamAbbrev] ?? p.teamAbbrev;
    const teamId    = abbrevToId.get(canonical) ?? null;
    const matchup   = teamId ? matchupByTeam.get(teamId) ?? null : null;
    const matchupId = matchup?.id ?? null;

    const ls = findLinestarMatch(p.name, p.salary, lsMap);
    if (ls) lsMatched++;
    const linestarProj = sanitizeProjection(ls?.linestarProj ?? null);
    const linestarOwnPct = sanitizeOwnershipPct(ls?.projOwnPct ?? null);
    const projOwnPct = linestarOwnPct;

    let ourProj: number | null = null;
    let liveProj: number | null = linestarProj;
    let spgForLev = 0, bpgForLev = 0;
    let projectionBlend: NbaProjectionBlend = {
      modelProj: null,
      marketProj: null,
      lsProj: linestarProj,
      finalProj: null,
      propCount: 0,
      modelConfidence: 0,
      marketConfidence: 0,
      lsConfidence: linestarProj != null ? 0.35 : 0,
      modelWeight: 0,
      marketWeight: 0,
      lsWeight: linestarProj != null ? 1 : 0,
      flags: ["no_model_match"],
      modelStats: null,
      marketStats: null,
    };

    if (teamId && matchup) {
      const teamStat = statsByTeam.get(teamId);
      const oppId    = matchup.homeTeamId === teamId ? matchup.awayTeamId : matchup.homeTeamId;
      const oppStat  = oppId ? statsByTeam.get(oppId) : null;

      const candidates = playersByTeam.get(teamId) ?? [];
      let bestPlayer: typeof playerStatRows[0] | null = null;
      let bestDist = 4;
      for (const ps of candidates) {
        const d = levenshtein(p.name.toLowerCase(), ps.name.toLowerCase());
        if (d < bestDist) { bestDist = d; bestPlayer = ps; }
      }

      if (bestPlayer) {
        const isHome = matchup.homeTeamId === teamId;
        const playerMovement = oddsMovementContext.playerByDkId.get(p.dkId);
        const matchupMovement = matchupId != null ? oddsMovementContext.matchupById.get(matchupId) : undefined;
        projectionBlend = buildNbaProjectionBlend(
          bestPlayer,
          teamStat?.pace    ?? LEAGUE_AVG_PACE,
          oppStat?.pace     ?? LEAGUE_AVG_PACE,
          oppStat?.defRtg   ?? LEAGUE_AVG_DEF_RTG,
          matchup.vegasTotal,
          matchup.homeMl,
          matchup.awayMl,
          isHome,
          linestarProj,
          {},
          {
            propDeltas: playerMovement?.propDeltas,
            marketFptsDelta: playerMovement?.marketFptsDelta,
            vegasTotalDelta: matchupMovement?.vegasTotalDelta,
            homeSpreadDelta: matchupMovement?.homeSpreadDelta,
          },
        );
        ourProj = computeNbaInternalProjection(
          projectionBlend,
          p.eligiblePositions,
          bestPlayer.avgMinutes,
          projectionCalibration,
        );
        liveProj = computeNbaLiveProjection(projectionBlend);
        spgForLev = bestPlayer.spg ?? 0;
        bpgForLev = bestPlayer.bpg ?? 0;
        if (ourProj != null) projComputed++;
      }
    }

    // DK status is authoritative for player availability.
    // DK returns "O", "Out", or "OUT" for scratches — normalise to upper-case.
    const dkIsOut = p.isDisabled || ["O", "OUT"].includes(p.dkStatus.toUpperCase());
    const isOut   = dkIsOut;

    insertValues.push({
      slateId, dkPlayerId: p.dkId, name: p.name,
      teamAbbrev: p.teamAbbrev, teamId, matchupId,
      eligiblePositions: p.eligiblePositions, salary: p.salary,
      gameInfo: p.gameInfo, avgFptsDk: sanitizeProjection(p.avgFptsDk),
      linestarProj, linestarOwnPct, projOwnPct,
      ourProj,
      liveProj,
      ourLeverage: null as number | null,
      ourOwnPct: null as number | null,
      liveLeverage: null as number | null,
      liveOwnPct: null as number | null,
      isOut,
      _spg: spgForLev, _bpg: bpgForLev,  // transient: ceiling bonus for leverage recalc
    });
    projectionSnapshots.push({
      slateId,
      dkPlayerId: p.dkId,
      name: p.name,
      teamId,
      salary: p.salary,
      isOut,
      blend: projectionBlend,
    });
  }

  // Compute internal ownership plus LS-first live ownership/leverage for the slate.
  applyNbaOwnershipModels(insertValues as NbaOwnershipModelPlayerLike[], oddsMovementContext);

  for (let i = 0; i < insertValues.length; i += 50) {
    const batch = insertValues.slice(i, i + 50).map(({ _spg, _bpg, ...rest }) => rest);
    await db.insert(dkPlayers).values(batch).onConflictDoUpdate({
      target: [dkPlayers.slateId, dkPlayers.dkPlayerId],
      set: {
        salary: sql`EXCLUDED.salary`, teamId: sql`EXCLUDED.team_id`,
        matchupId: sql`EXCLUDED.matchup_id`,
        linestarProj: sql`EXCLUDED.linestar_proj`, linestarOwnPct: sql`EXCLUDED.linestar_own_pct`, projOwnPct: sql`EXCLUDED.proj_own_pct`,
        ourProj: sql`EXCLUDED.our_proj`,
        liveProj: sql`EXCLUDED.live_proj`,
        ourLeverage: sql`EXCLUDED.our_leverage`,
        ourOwnPct: sql`EXCLUDED.our_own_pct`,
        liveLeverage: sql`EXCLUDED.live_leverage`,
        liveOwnPct: sql`EXCLUDED.live_own_pct`,
        isOut: sql`EXCLUDED.is_out`, avgFptsDk: sql`EXCLUDED.avg_fpts_dk`,
        eligiblePositions: sql`EXCLUDED.eligible_positions`, gameInfo: sql`EXCLUDED.game_info`,
      },
    });
  }

  const projectionRunId = await createProjectionRun(slateId, "load_slate", {
    version: NBA_PROJECTION_MODEL_VERSION,
    source: "load_slate",
    playerCount: insertValues.length,
    hasLinestar: lsMap.size > 0,
  });
  await recordProjectionSnapshots(projectionRunId, projectionSnapshots);

  revalidatePath("/dfs");
  const matchRate = lsMap.size > 0 ? Math.round((lsMatched / dkPlayers_.length) * 100) : null;
  return {
    ok: true,
    message: `Saved ${insertValues.length} players (${projComputed} with our proj)${matchRate != null ? `, LineStar ${matchRate}% matched` : ""}`,
    playerCount: insertValues.length,
    matchRate: matchRate ?? undefined,
  };
}

// ── LineStar API helpers ──────────────────────────────────────

const LS_BASE    = "https://www.linestarapp.com/DesktopModules/DailyFantasyApi/API/Fantasy";
const LS_HEADERS = {
  "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
  "Accept": "application/json",
  "Referer": "https://www.linestarapp.com/DesktopModules/DailyFantasyApi/",
};

/** Try to fetch LineStar projections + ownership using DNN_COOKIE from env.
 *  Returns an empty map if the cookie is missing, expired, or the API fails. */
function normalizeDnnCookie(raw: string): string {
  // Strip accidental ".DOTNETNUKE=" prefix if the user pasted the full cookie string
  return raw.startsWith(".DOTNETNUKE=") ? raw.slice(".DOTNETNUKE=".length) : raw;
}

async function tryFetchLinestarMap(draftGroupId: number): Promise<Map<string, LinestarEntry>> {
  const raw = process.env.DNN_COOKIE;
  if (!raw) return new Map();
  const cookie = normalizeDnnCookie(raw);
  try {
    const periodId = await resolveLinestarPeriodId(draftGroupId, cookie);
    if (!periodId) return new Map();
    const data = await fetchLinestarSalaries(periodId, cookie);
    return parseLinestarApiResponse(data);
  } catch {
    return new Map();
  }
}

async function resolveLinestarPeriodId(draftGroupId: number, cookie: string): Promise<number | null> {
  try {
    const resp = await fetch(`${LS_BASE}/GetPeriodInformation?site=1&sport=5`, {
      headers: { ...LS_HEADERS, Cookie: `.DOTNETNUKE=${cookie}` },
      next: { revalidate: 0 },
    });
    if (!resp.ok) return null;
    const periods = await resp.json() as Array<{ PeriodId?: number; Id?: number }>;
    const list = Array.isArray(periods) ? periods : (periods as { Periods?: typeof periods }).Periods ?? [];
    for (const period of list.slice(0, 10)) {
      const pid = period.PeriodId ?? period.Id;
      if (!pid) continue;
      // Probe: fetch salaries and see if this period matches our draft group
      const probe = await fetchLinestarSalaries(pid, cookie).catch(() => null);
      if (!probe) continue;
      const slates = (probe as { Slates?: Array<{ DfsSlateId?: number }> }).Slates ?? [];
      if (slates.some((s) => s.DfsSlateId === draftGroupId)) return pid;
    }
    return null;
  } catch {
    return null;
  }
}

async function fetchLinestarSalaries(periodId: number, cookie: string): Promise<unknown> {
  const resp = await fetch(`${LS_BASE}/GetSalariesV5?periodId=${periodId}&site=1&sport=5`, {
    headers: { ...LS_HEADERS, Cookie: `.DOTNETNUKE=${cookie}` },
    next: { revalidate: 0 },
  });
  if (!resp.ok) throw new Error(`LineStar ${resp.status}`);
  return resp.json();
}

function parseLinestarApiResponse(data: unknown): Map<string, LinestarEntry> {
  const d = data as {
    SalaryContainerJson?: string;
    Ownership?: { Projected?: Record<string, Array<{ SalaryId: number; Owned: number }>> };
  };
  const map = new Map<string, LinestarEntry>();
  if (!d.SalaryContainerJson) return map;
  let container: { Salaries?: Array<{ Id: number; Name: string; SAL: number; PP: number; STAT?: number; IS?: number }> };
  try { container = JSON.parse(d.SalaryContainerJson); } catch { return map; }

  // Build salary-id → avg ownership map
  const ownTotals = new Map<number, number>(); const ownCounts = new Map<number, number>();
  for (const entries of Object.values(d.Ownership?.Projected ?? {})) {
    if (!Array.isArray(entries)) continue;
    for (const e of entries) {
      ownTotals.set(e.SalaryId, (ownTotals.get(e.SalaryId) ?? 0) + e.Owned);
      ownCounts.set(e.SalaryId, (ownCounts.get(e.SalaryId) ?? 0) + 1);
    }
  }

  for (const p of container.Salaries ?? []) {
    const isOut = p.STAT === 4 || p.IS === 1;
    const proj  = parseFloat(String(p.PP)) || 0;
    const own   = ownCounts.get(p.Id) ? (ownTotals.get(p.Id)! / ownCounts.get(p.Id)!) : 0;
    const key   = `${p.Name.toLowerCase()}|${p.SAL}`;
    map.set(key, { linestarProj: proj, projOwnPct: own, isOut });
  }
  return map;
}

/** Check if the DNN_COOKIE in Vercel env is valid without fetching full data. */
export async function checkLinestarCookie(): Promise<{ ok: boolean; message: string; status?: number }> {
  const cookie = process.env.DNN_COOKIE;
  if (!cookie) return { ok: false, message: "DNN_COOKIE not set — add it to Vercel env vars" };
  const cookieValue = normalizeDnnCookie(cookie);
  try {
    const resp = await fetch(`${LS_BASE}/GetPeriodInformation?site=1&sport=5`, {
      headers: { ...LS_HEADERS, Cookie: `.DOTNETNUKE=${cookieValue}` },
      next: { revalidate: 0 },
    });
    if (resp.status === 401 || resp.status === 403)
      return { ok: false, message: "Cookie expired — update DNN_COOKIE in Vercel → Settings → Env Vars", status: resp.status };
    if (resp.status === 404)
      return { ok: false, message: "Endpoint not found (HTTP 404) — use manual CSV upload instead", status: 404 };
    if (!resp.ok)
      return { ok: false, message: `LineStar returned HTTP ${resp.status}`, status: resp.status };
    return { ok: true, message: "Cookie is valid" };
  } catch (e) {
    return { ok: false, message: `Connection failed: ${String(e)}` };
  }
}

/** Shared: write a LineStar map into the most-recent slate's player pool. */
async function _applyLinestarMap(
  lsMap: Map<string, LinestarEntry>,
  sport: Sport,
): Promise<{ ok: boolean; message: string; matched: number; total: number }> {
  const slateRows = await db
    .select({ id: dkSlates.id, slateDate: dkSlates.slateDate })
    .from(dkSlates)
    .where(eq(dkSlates.sport, sport))
    .orderBy(desc(dkSlates.slateDate), desc(dkSlates.gameCount), desc(dkSlates.id))
    .limit(1);
  if (!slateRows[0]) return { ok: false, message: `No ${sport.toUpperCase()} slate loaded yet`, matched: 0, total: 0 };
  const slate = slateRows[0];
  const slateId = slate.id;

  const pool = await db.execute<{
    id: number; dkPlayerId: number; matchupId: number | null; name: string; salary: number; teamId: number | null; mlbTeamId: number | null;
    eligiblePositions: string;
    avgFptsDk: number | null; linestarProj: number | null; projOwnPct: number | null;
    ourProj: number | null; liveProj: number | null; isOut: boolean | null;
  }>(sql`
    SELECT id, dk_player_id AS "dkPlayerId", matchup_id AS "matchupId", name, salary, team_id AS "teamId", mlb_team_id AS "mlbTeamId",
           eligible_positions AS "eligiblePositions",
           avg_fpts_dk AS "avgFptsDk", linestar_proj AS "linestarProj", proj_own_pct AS "projOwnPct",
           our_proj AS "ourProj", live_proj AS "liveProj", is_out AS "isOut"
    FROM dk_players WHERE slate_id = ${slateId}
  `);

  // Load player stats for ceiling bonus (spg/bpg)
  const playersByTeam = new Map<number, Array<{ name: string; spg: number | null; bpg: number | null }>>();
  if (sport === "nba") {
    const playerStatRows = await db.select().from(nbaPlayerStats).where(eq(nbaPlayerStats.season, CURRENT_SEASON));
    for (const ps of playerStatRows) {
      if (ps.teamId == null) continue;
      const arr = playersByTeam.get(ps.teamId) ?? [];
      arr.push(ps);
      playersByTeam.set(ps.teamId, arr);
    }
  }
  const oddsMovementContext = sport === "nba"
    ? await buildNbaOddsMovementContext(slate.id, slate.slateDate)
    : undefined;

  let matched = 0;
  for (const p of pool.rows) {
    const ls = findLinestarMatch(p.name, p.salary, lsMap);
    if (!ls) continue;
    matched++;
    const linestarProj = sanitizeProjection(ls.linestarProj);
    const linestarOwnPct = sanitizeOwnershipPct(ls.projOwnPct);
    const projOwnPct = linestarOwnPct;
    // Do NOT touch isOut — DK API status is the authoritative source.
    // LineStar proj=0 does not mean the player is scratched.
    await db.update(dkPlayers)
      .set({ linestarProj, linestarOwnPct, projOwnPct })
      .where(eq(dkPlayers.id, p.id));
  }

  if (sport === "mlb") {
    const refreshed = await db.execute<{
      id: number;
      dkPlayerId: number;
      name: string;
      mlbTeamId: number | null;
      eligiblePositions: string;
      salary: number;
      isOut: boolean | null;
      avgFptsDk: number | null;
      linestarProj: number | null;
      linestarOwnPct: number | null;
      projOwnPct: number | null;
      ourProj: number | null;
      ourOwnPct: number | null;
      ourLeverage: number | null;
      dkStartingLineupOrder: number | null;
      dkTeamLineupConfirmed: boolean | null;
      teamImplied: number | null;
      oppImplied: number | null;
      teamMl: number | null;
      vegasTotal: number | null;
      isHome: boolean | null;
    }>(sql`
      SELECT dp.id AS "id",
             dp.dk_player_id AS "dkPlayerId",
             dp.name AS "name",
             dp.mlb_team_id AS "mlbTeamId",
             dp.eligible_positions AS "eligiblePositions",
             dp.salary,
             dp.is_out AS "isOut",
             dp.avg_fpts_dk AS "avgFptsDk",
             dp.linestar_proj AS "linestarProj",
             dp.linestar_own_pct AS "linestarOwnPct",
             dp.proj_own_pct AS "projOwnPct",
             dp.our_proj AS "ourProj",
             dp.our_own_pct AS "ourOwnPct",
             dp.our_leverage AS "ourLeverage",
             dp.dk_starting_lineup_order AS "dkStartingLineupOrder",
             dp.dk_team_lineup_confirmed AS "dkTeamLineupConfirmed",
             CASE
               WHEN dp.mlb_team_id = mm.home_team_id THEN mm.home_implied
               WHEN dp.mlb_team_id = mm.away_team_id THEN mm.away_implied
               ELSE NULL
             END AS "teamImplied",
             CASE
               WHEN dp.mlb_team_id = mm.home_team_id THEN mm.away_implied
               WHEN dp.mlb_team_id = mm.away_team_id THEN mm.home_implied
               ELSE NULL
             END AS "oppImplied",
             CASE
               WHEN dp.mlb_team_id = mm.home_team_id THEN mm.home_ml
               WHEN dp.mlb_team_id = mm.away_team_id THEN mm.away_ml
               ELSE NULL
             END AS "teamMl",
             mm.vegas_total AS "vegasTotal",
             CASE
               WHEN dp.mlb_team_id = mm.home_team_id THEN TRUE
               WHEN dp.mlb_team_id = mm.away_team_id THEN FALSE
               ELSE NULL
             END AS "isHome"
      FROM dk_players dp
      LEFT JOIN mlb_matchups mm ON mm.id = dp.matchup_id
      WHERE dp.slate_id = ${slateId}
    `);
    const enriched = refreshed.rows.map((player) => ({
      ...player,
      isOut: player.isOut ?? false,
      avgFptsDk: sanitizeProjection(player.avgFptsDk),
      linestarProj: sanitizeProjection(player.linestarProj),
      linestarOwnPct: sanitizeOwnershipPct(player.linestarOwnPct),
      projOwnPct: sanitizeOwnershipPct(player.projOwnPct),
      ourProj: sanitizeProjection(player.ourProj),
      ourOwnPct: sanitizeOwnershipPct(player.ourOwnPct),
      ourLeverage: sanitizeLeverage(player.ourLeverage),
    }));
    applyMlbOwnershipModels(enriched);
    for (const player of enriched) {
      await db.update(dkPlayers)
        .set({
          projOwnPct: player.projOwnPct,
          ourOwnPct: player.ourOwnPct,
          ourLeverage: player.ourLeverage,
        })
        .where(eq(dkPlayers.id, player.id));
    }
    const ownershipRunId = await createOwnershipRun(slateId, "mlb", "linestar_refresh", MLB_OWNERSHIP_MODEL_VERSION, {
      version: MLB_OWNERSHIP_MODEL_VERSION,
      source: "linestar_refresh",
      matchedPlayers: matched,
      slateDate: slate.slateDate,
    });
    await recordOwnershipSnapshots(ownershipRunId, enriched.map((player) => ({
      slateId,
      dkPlayerId: player.dkPlayerId,
      name: player.name,
      teamId: player.mlbTeamId,
      salary: player.salary,
      eligiblePositions: player.eligiblePositions,
      isOut: player.isOut ?? false,
      linestarProjFpts: sanitizeProjection(player.linestarProj),
      ourProjFpts: sanitizeProjection(player.ourProj),
      liveProjFpts: null,
      linestarOwnPct: sanitizeOwnershipPct(player.linestarOwnPct),
      fieldOwnPct: sanitizeOwnershipPct(player.projOwnPct),
      ourOwnPct: sanitizeOwnershipPct(player.ourOwnPct),
      liveOwnPct: null,
      actualOwnPct: null,
      lineupOrder: player.dkStartingLineupOrder,
      lineupConfirmed: player.dkTeamLineupConfirmed,
    })));
  } else {
    const updatedLinestarById = new Map<number, { linestarProj: number | null; projOwnPct: number | null }>();
    for (const player of pool.rows) {
      const ls = findLinestarMatch(player.name, player.salary, lsMap);
      if (!ls) continue;
      updatedLinestarById.set(player.id, {
        linestarProj: sanitizeProjection(ls.linestarProj),
        projOwnPct: sanitizeOwnershipPct(ls.projOwnPct),
      });
    }

      const enriched = pool.rows.map((player) => {
      let spgForLev = 0;
      let bpgForLev = 0;
      if (player.teamId) {
        const candidates = playersByTeam.get(player.teamId) ?? [];
        let bestDist = 4;
        for (const ps of candidates) {
          const d = levenshtein(player.name.toLowerCase(), ps.name.toLowerCase());
          if (d < bestDist) {
            bestDist = d;
            spgForLev = ps.spg ?? 0;
            bpgForLev = ps.bpg ?? 0;
          }
        }
      }
      const lsUpdate = updatedLinestarById.get(player.id);
      return {
        id: player.id,
        dkPlayerId: player.dkPlayerId,
        matchupId: player.matchupId,
        salary: player.salary,
        avgFptsDk: sanitizeProjection(player.avgFptsDk),
        linestarProj: lsUpdate?.linestarProj ?? sanitizeProjection(player.linestarProj),
        projOwnPct: lsUpdate?.projOwnPct ?? sanitizeOwnershipPct(player.projOwnPct),
        ourProj: sanitizeProjection(player.ourProj),
        liveProj: sanitizeProjection(player.liveProj ?? player.ourProj ?? player.linestarProj),
        ourOwnPct: null,
        ourLeverage: null,
        liveOwnPct: null,
        liveLeverage: null,
        isOut: player.isOut ?? false,
        _spg: spgForLev,
        _bpg: bpgForLev,
      };
    });
    applyNbaOwnershipModels(enriched, oddsMovementContext);
    for (const player of enriched) {
      await db.update(dkPlayers)
        .set({
          ourOwnPct: player.ourOwnPct,
          liveOwnPct: player.liveOwnPct,
          ourLeverage: player.ourLeverage,
          liveLeverage: player.liveLeverage,
        })
        .where(eq(dkPlayers.id, player.id));
    }
  }

  revalidatePath("/dfs");
  const pct = pool.rows.length > 0 ? Math.round(matched / pool.rows.length * 100) : 0;
  return {
    ok: true,
    message: `LineStar data applied — ${matched}/${pool.rows.length} players matched (${pct}%)`,
    matched,
    total: pool.rows.length,
  };
}

/** Inject LineStar data from an uploaded CSV file. */
export async function uploadLinestarCsv(formData: FormData, sport: Sport = "nba"): Promise<{
  ok: boolean; message: string; matched: number; total: number;
}> {
  const file = formData.get("lsFile") as File | null;
  if (!file) return { ok: false, message: "No file provided", matched: 0, total: 0 };
  const lsMap = parseLinestarCsv(await file.text());
  if (lsMap.size === 0) return { ok: false, message: "No players parsed from LineStar CSV", matched: 0, total: 0 };
  return _applyLinestarMap(lsMap, sport);
}

/** Inject LineStar data from text pasted directly from the LineStar web table. */
export async function applyLinestarPaste(text: string, sport: Sport = "nba"): Promise<{
  ok: boolean; message: string; matched: number; total: number;
}> {
  if (!text.trim()) return { ok: false, message: "No data provided", matched: 0, total: 0 };
  const lsMap = parseLinestarPasteText(text);
  if (lsMap.size === 0) return { ok: false, message: "No players parsed — expected tab-separated LineStar data", matched: 0, total: 0 };
  return _applyLinestarMap(lsMap, sport);
}

// ── Historical slate import ────────────────────────────────────

type HistoricalEntry = {
  position: string;
  linestarProj: number | null;
  projOwnPct: number | null;
  actualOwnPct: number | null;
  actualFpts: number | null;
  actualHr: number | null;
  teamAbbrev: string;
};

type HistoricalHeaderHints = {
  salaryIdx: number;
  projOwnIdx: number | null;
  actualOwnIdx: number | null;
  projIdx: number | null;
  actualIdx: number | null;
  actualHrIdx: number | null;
};

function normalizeHistoricalHeaderCell(value: string): string {
  return value
    .toLowerCase()
    .replace(/[%_]/g, " ")
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function parseHistoricalNumericCell(value: string | null | undefined): number | null {
  if (value == null) return null;
  const cleaned = value.replace(/[$,%]/g, "").replace(/,/g, "").trim();
  if (!cleaned) return null;
  const parsed = Number.parseFloat(cleaned);
  return Number.isFinite(parsed) ? parsed : null;
}

function parseHistoricalIntegerCell(value: string | null | undefined): number | null {
  const parsed = parseHistoricalNumericCell(value);
  if (parsed == null) return null;
  return Math.max(0, Math.round(parsed));
}

function detectHistoricalHeaderHints(cells: string[]): HistoricalHeaderHints | null {
  const normalized = cells.map(normalizeHistoricalHeaderCell);
  const salaryIdx = normalized.findIndex((cell) => cell === "salary" || cell.includes("salary"));
  if (salaryIdx < 0) return null;

  const findAfterSalary = (matcher: (cell: string) => boolean): number | null => {
    const idx = normalized.findIndex((cell, i) => i > salaryIdx && matcher(cell));
    return idx >= 0 ? idx : null;
  };

  return {
    salaryIdx,
    projOwnIdx: findAfterSalary((cell) => cell.includes("proj") && cell.includes("own")),
    actualOwnIdx: findAfterSalary((cell) => cell.includes("actual") && cell.includes("own")),
    projIdx: findAfterSalary((cell) =>
      (cell === "proj" || cell.includes("projection") || cell.includes("projected"))
      && !cell.includes("own"),
    ),
    actualIdx: findAfterSalary((cell) =>
      (cell.includes("actual") || cell.includes("scored") || cell.includes("score") || cell.includes("fpts"))
      && !cell.includes("own")
      && !cell.includes("hr")
      && !cell.includes("home run"),
    ),
    actualHrIdx: findAfterSalary((cell) =>
      (
        cell === "hr"
        || cell === "hrs"
        || cell === "home run"
        || cell === "home runs"
        || cell === "actual hr"
        || cell === "actual hrs"
        || cell === "actual home run"
        || cell === "actual home runs"
      )
      && !cell.includes("prob")
      && !cell.includes("proj"),
    ),
  };
}

/**
 * Parse LineStar historical paste. Same column anchor as the live parser
 * (salary = $NNNN) but also captures actualOwnPct (+2) and actualFpts (+5).
 *
 * Live format:   Pos | Team | Player | Salary | projOwn% | actualOwn% | Diff | Proj
 * History adds:  ... | Actual (col +5 after salary)
 */
function parseHistoricalPaste(text: string, sport: Sport = "nba"): Map<string, HistoricalEntry> {
  const lines = text.split(/\r?\n/).filter(Boolean);
  const map = new Map<string, HistoricalEntry>();
  const headerCells = lines[0]?.split("\t").map((c) => c.trim()) ?? [];
  const headerHints = detectHistoricalHeaderHints(headerCells);
  const dataLines = headerHints ? lines.slice(1) : lines;

  for (const line of dataLines) {
    const cells = line.split("\t").map((c) => c.trim());
    const salaryIdx = headerHints?.salaryIdx ?? cells.findIndex((c) => /^\$[\d,]{4,7}$/.test(c));
    if (salaryIdx < 1) continue;

    let playerName = cells[salaryIdx - 1];
    let teamAbbrev = "";
    for (let idx = salaryIdx - 1; idx >= 1; idx--) {
      if (looksLikeTeamAbbrev(cells[idx] ?? "")) {
        teamAbbrev = cells[idx] ?? "";
        if (idx > 0) {
          playerName = cells[idx - 1] ?? playerName;
        }
        break;
      }
    }
    if (!teamAbbrev && looksLikeTeamAbbrev(playerName) && salaryIdx >= 2) {
      teamAbbrev = playerName;
      playerName = cells[salaryIdx - 2];
    } else if (!teamAbbrev && salaryIdx >= 2) {
      teamAbbrev = cells[salaryIdx - 2] ?? "";
    }
    if (!playerName || playerName.toLowerCase() === "player") continue;

    const salary     = parseInt(cells[salaryIdx].replace(/\D/g, ""), 10);
    if (!salary) continue;

    // Position is always cells[0] (first column in every LineStar format)
    const posRaw = cells[0]?.trim() ?? "";
    const position = (() => {
      if (sport === "mlb") {
        if (!/^(?:P|SP|RP|C|1B|2B|3B|SS|OF)(?:\/(?:P|SP|RP|C|1B|2B|3B|SS|OF))*$/.test(posRaw)) {
          return "UTIL";
        }
        return posRaw
          .split("/")
          .map((part) => part === "P" ? "SP" : part)
          .join("/");
      }
      return /^(PG|SG|SF|PF|C)(\/(?:PG|SG|SF|PF|C))*$/.test(posRaw) ? posRaw : "UTIL";
    })();

    const actualHrIdx = headerHints?.actualHrIdx ?? null;
    const percentCells = cells
      .map((raw, idx) => ({ idx, raw, value: raw.includes("%") ? parseHistoricalNumericCell(raw) : null }))
      .filter((entry) => entry.idx > salaryIdx && entry.value != null) as Array<{ idx: number; raw: string; value: number }>;
    const numericNonPercentCells = cells
      .map((raw, idx) => ({ idx, raw, value: raw.includes("%") ? null : parseHistoricalNumericCell(raw) }))
      .filter((entry) => entry.idx > salaryIdx && entry.idx !== actualHrIdx && entry.value != null) as Array<{ idx: number; raw: string; value: number }>;

    const projOwnPct = headerHints?.projOwnIdx != null
      ? parseHistoricalNumericCell(cells[headerHints.projOwnIdx] ?? "")
      : (percentCells[0]?.value ?? null);
    const actualOwnPct = headerHints?.actualOwnIdx != null
      ? parseHistoricalNumericCell(cells[headerHints.actualOwnIdx] ?? "")
      : (percentCells[1]?.value ?? null);

    let actualFpts = headerHints?.actualIdx != null
      ? parseHistoricalNumericCell(cells[headerHints.actualIdx] ?? "")
      : null;
    let linestarProj = headerHints?.projIdx != null
      ? parseHistoricalNumericCell(cells[headerHints.projIdx] ?? "")
      : null;
    const actualHr = actualHrIdx != null
      ? parseHistoricalIntegerCell(cells[actualHrIdx] ?? "")
      : null;

    if (actualFpts == null && numericNonPercentCells.length >= 2) {
      actualFpts = numericNonPercentCells[numericNonPercentCells.length - 1]?.value ?? null;
    }

    if (linestarProj == null) {
      if (numericNonPercentCells.length >= 2) {
        linestarProj = numericNonPercentCells[numericNonPercentCells.length - 2]?.value ?? null;
      } else if (numericNonPercentCells.length === 1) {
        linestarProj = numericNonPercentCells[0]?.value ?? null;
      }
    }

    if (linestarProj == null) {
      linestarProj = parseHistoricalNumericCell(cells[salaryIdx + 4] ?? "");
    }

    if (actualFpts == null && numericNonPercentCells.length === 0) {
      actualFpts = parseHistoricalNumericCell(cells[salaryIdx + 5] ?? "");
    }

    map.set(`${playerName.toLowerCase()}|${salary}`, {
      position, linestarProj, projOwnPct, actualOwnPct, actualFpts, actualHr,
      teamAbbrev: teamAbbrev.toUpperCase(),
    });
  }
  return map;
}

/** Deterministic synthetic DK player ID for historical records.
 *  Uses a range > 10 billion to avoid collision with real DK IDs (~20–50M). */
function syntheticDkId(name: string, salary: number): number {
  let h = 5381;
  const s = `${name.toLowerCase()}_${salary}`;
  for (let i = 0; i < s.length; i++) h = ((h * 33) ^ s.charCodeAt(i)) >>> 0;
  return (h % 900_000_000) + 10_000_000_000;
}

/**
 * Save historical LineStar data (past slate results) to Neon.
 *
 * Two modes determined automatically:
 *   - Slate already exists for `date` → update actual_fpts + actual_own_pct
 *     on existing dk_players rows (updates ourProj-based training pairs)
 *   - No slate exists → create dk_slate + dk_players with synthetic IDs
 *     (stores linestarProj + actual for LineStar MAE tracking)
 */
export async function saveHistoricalSlate(
  sport: Sport,
  date: string,
  text: string,
  contestType?: string,
  fieldSize?: number,
  contestFormat?: string,
): Promise<{ ok: boolean; message: string; created: number; updated: number }> {
  if (!date) return { ok: false, message: "Date is required", created: 0, updated: 0 };
  if (!text.trim()) return { ok: false, message: "No data pasted", created: 0, updated: 0 };

  await ensureDkPlayerPropColumns();
  const parsed = parseHistoricalPaste(text, sport);
  if (parsed.size === 0)
    return { ok: false, message: "No players parsed — expected tab-separated LineStar data", created: 0, updated: 0 };
  const parsedActualCount = Array.from(parsed.values()).filter((entry) =>
    entry.actualFpts != null || (sport === "mlb" && entry.actualHr != null)
  ).length;
  if (parsedActualCount === 0) {
    return {
      ok: false,
      message: sport === "mlb"
        ? "No historical actual FPTS or HR results found in the paste. Use the results view, not the live projections table."
        : "No historical actual FPTS found in the paste. Use the LineStar historical/results view, not the live projections table.",
      created: 0,
      updated: 0,
    };
  }

  // Find existing slate to update results into.
  // Priority 1: exact match on date + contestType + contestFormat (user selected the right slate).
  // Priority 2: any loaded slate for the same date that has our_proj populated (contest type
  //   label mismatch between the load and the historical save is common — e.g. loaded as "late"
  //   but historical pasted as "main"). Prefer the slate with the most our_proj coverage.
  const effectiveType   = normalizeDkSlateTiming(contestType) ?? "main";
  const effectiveFormat = contestFormat ?? "gpp";

  const exactMatch = await db
    .select({ id: dkSlates.id })
    .from(dkSlates)
    .where(
      and(
        eq(dkSlates.slateDate, date),
        eq(dkSlates.contestType, effectiveType),
        eq(dkSlates.contestFormat, effectiveFormat),
        eq(dkSlates.sport, sport),
      )
    )
    .limit(1);

  // If no exact match, fall back to the loaded slate with the most our_proj coverage for this date.
  const existingSlate: { id: number }[] = exactMatch[0]
    ? exactMatch
    : (await db.execute<{ id: number }>(sql`
        SELECT ds.id
        FROM dk_slates ds
        WHERE ds.slate_date = ${date}
          AND ds.sport = ${sport}
          AND EXISTS (
            SELECT 1 FROM dk_players dp
            WHERE dp.slate_id = ds.id AND dp.our_proj IS NOT NULL
          )
        ORDER BY (
          SELECT COUNT(*) FROM dk_players dp
          WHERE dp.slate_id = ds.id AND dp.our_proj IS NOT NULL
        ) DESC
        LIMIT 1
      `)).rows;

  const abbrevCache = new Map(
    (await db
      .select({
        teamId: sport === "mlb" ? mlbTeams.teamId : teams.teamId,
        abbreviation: sport === "mlb" ? mlbTeams.abbreviation : teams.abbreviation,
      })
      .from(sport === "mlb" ? mlbTeams : teams))
      .map((t) => [t.abbreviation.toUpperCase(), t.teamId]),
  );
  const nbaHistoricalResolver = sport === "nba"
    ? await buildNbaHistoricalTeamResolver(abbrevCache as Map<string, number>)
    : null;

  // ── Mode 1: slate exists → update actual results on existing rows ──────────
  if (existingSlate[0]) {
    const slateId = existingSlate[0].id;
    const pool = await db.execute<{
      id: number; name: string; salary: number; teamAbbrev: string | null; teamId: number | null;
    }>(sql`
      SELECT id, name, salary, team_abbrev AS "teamAbbrev", team_id AS "teamId"
      FROM dk_players
      WHERE slate_id = ${slateId}
    `);

    let updated = 0;
    for (const p of pool.rows) {
      const entry = parsed.get(`${p.name.toLowerCase()}|${p.salary}`);
      let match = entry;
      if (!match) {
        let bestDist = 4;
        for (const [key, val] of parsed) {
          const [pName, salStr] = key.split("|");
          if (parseInt(salStr, 10) !== p.salary) continue;
          const d = levenshtein(p.name.toLowerCase(), pName);
          if (d < bestDist) { bestDist = d; match = val; }
        }
      }
      if (!match) continue;

      const repairedTeam = sport === "nba" && nbaHistoricalResolver
        ? resolveHistoricalNbaTeam(nbaHistoricalResolver, p.name, match.teamAbbrev)
        : null;
      const updatePayload: Partial<typeof dkPlayers.$inferInsert> = {
        linestarProj: match.linestarProj ?? null,
        linestarOwnPct: match.projOwnPct ?? null,
        ...(sport === "nba" && repairedTeam && (!p.teamId || !p.teamAbbrev || p.teamAbbrev === "UNK")
          ? {
            teamId: repairedTeam.teamId,
            teamAbbrev: repairedTeam.teamAbbrev,
          }
          : {}),
      };
      if (match.actualFpts != null) updatePayload.actualFpts = match.actualFpts;
      if (match.actualOwnPct != null) updatePayload.actualOwnPct = match.actualOwnPct;
      if (sport === "mlb" && match.actualHr != null) updatePayload.actualHr = match.actualHr;
      if (sport === "mlb") {
        const mlbTeamId = abbrevCache.get(match.teamAbbrev) ?? null;
        if (!p.teamId && mlbTeamId) {
          updatePayload.mlbTeamId = mlbTeamId;
          updatePayload.teamAbbrev = match.teamAbbrev;
        }
      }

      await db.update(dkPlayers)
        .set(updatePayload)
        .where(eq(dkPlayers.id, p.id));
      updated++;
    }

    await refreshHistoricalSlateGameCount(slateId, sport);
    try { await syncOwnershipSnapshotActualsForSlate(slateId); } catch { /* non-fatal */ }
    if (sport === "mlb") {
      try { await syncMlbBlowupSnapshotActualsForSlate(slateId); } catch { /* non-fatal */ }
      try { await syncMlbHomerunSnapshotActualsForSlate(slateId); } catch { /* non-fatal */ }
    }

    revalidatePath("/dfs");
    revalidatePath("/homerun");
    revalidatePath("/analytics");
    revalidateTag(ANALYTICS_CACHE_TAG, {});
    return {
      ok: true,
      message: `Updated ${updated}/${pool.rows.length} players with actual results for ${date}`,
      created: 0,
      updated,
    };
  }

  // ── Mode 2: no slate → create dk_slate + dk_players with synthetic IDs ─────
  const slateValues = {
    slateDate: date,
    gameCount: 0,
    sport,
    contestType: effectiveType,
    contestFormat: effectiveFormat,
    ...(fieldSize != null && { fieldSize }),
  };

  const insertedSlateRows = fieldSize != null
    ? await db
      .insert(dkSlates)
      .values(slateValues)
      .onConflictDoUpdate({
        target: [dkSlates.slateDate, dkSlates.contestType, dkSlates.contestFormat, dkSlates.sport],
        set: { fieldSize },
      })
      .returning({ id: dkSlates.id })
    : await db
      .insert(dkSlates)
      .values(slateValues)
      .onConflictDoNothing({
        target: [dkSlates.slateDate, dkSlates.contestType, dkSlates.contestFormat, dkSlates.sport],
      })
      .returning({ id: dkSlates.id });

  const slateId = insertedSlateRows[0]?.id ?? (
    await db
      .select({ id: dkSlates.id })
      .from(dkSlates)
      .where(
        and(
          eq(dkSlates.slateDate, date),
          eq(dkSlates.contestType, effectiveType),
          eq(dkSlates.contestFormat, effectiveFormat),
          eq(dkSlates.sport, sport),
        )
      )
      .limit(1)
  )[0]?.id;

  if (!slateId) {
    throw new Error(`Failed to create or resolve historical ${sport.toUpperCase()} slate for ${date}`);
  }
  let created = 0;

  for (const [key, entry] of parsed) {
    const [playerName, salStr] = key.split("|");
    const salary    = parseInt(salStr, 10);
    const dkPlayerId = syntheticDkId(playerName, salary);
    const repairedTeam = sport === "nba" && nbaHistoricalResolver
      ? resolveHistoricalNbaTeam(nbaHistoricalResolver, playerName, entry.teamAbbrev)
      : {
        teamId: abbrevCache.get(entry.teamAbbrev) ?? null,
        teamAbbrev: entry.teamAbbrev || "UNK",
      };
    const teamId    = repairedTeam.teamId;
    const name      = playerName.replace(/\b\w/g, (c) => c.toUpperCase()); // restore title case

    await db.insert(dkPlayers)
      .values({
        slateId, dkPlayerId, name,
        teamAbbrev: repairedTeam.teamAbbrev || "UNK",
        ...(sport === "mlb" ? { mlbTeamId: teamId } : { teamId }),
        salary,
        eligiblePositions: entry.position || "UTIL",
        linestarProj:  entry.linestarProj ?? null,
        linestarOwnPct: entry.projOwnPct ?? null,
        projOwnPct:    entry.projOwnPct ?? null,
        actualOwnPct:  entry.actualOwnPct ?? null,
        actualFpts:    entry.actualFpts,
        actualHr:      sport === "mlb" ? entry.actualHr : null,
        isOut: false,
      })
      .onConflictDoUpdate({
        target: [dkPlayers.slateId, dkPlayers.dkPlayerId],
        set: {
          teamAbbrev:    sql`COALESCE(NULLIF(EXCLUDED.team_abbrev, 'UNK'), dk_players.team_abbrev)`,
          teamId:        sport === "nba" ? sql`COALESCE(EXCLUDED.team_id, dk_players.team_id)` : sql`dk_players.team_id`,
          mlbTeamId:     sport === "mlb" ? sql`COALESCE(EXCLUDED.mlb_team_id, dk_players.mlb_team_id)` : sql`dk_players.mlb_team_id`,
          linestarProj:  sql`EXCLUDED.linestar_proj`,
          linestarOwnPct: sql`EXCLUDED.linestar_own_pct`,
          projOwnPct:    sql`EXCLUDED.proj_own_pct`,
          actualOwnPct:  sql`EXCLUDED.actual_own_pct`,
          actualFpts:    sql`EXCLUDED.actual_fpts`,
          actualHr:      sql`EXCLUDED.actual_hr`,
        },
      });
    created++;
  }

  await refreshHistoricalSlateGameCount(slateId, sport);
  try { await syncOwnershipSnapshotActualsForSlate(slateId); } catch { /* non-fatal */ }
  if (sport === "mlb") {
    try { await syncMlbBlowupSnapshotActualsForSlate(slateId); } catch { /* non-fatal */ }
    try { await syncMlbHomerunSnapshotActualsForSlate(slateId); } catch { /* non-fatal */ }
  }

  revalidatePath("/dfs");
  revalidatePath("/homerun");
  revalidatePath("/analytics");
  revalidateTag(ANALYTICS_CACHE_TAG, {});
  return {
    ok: true,
    message: `Created historical ${sport.toUpperCase()} slate for ${date} with ${created} players (synthetic IDs — ourProj will be null)`,
    created,
    updated: 0,
  };
}

export async function repairHistoricalNbaTeamMappings(): Promise<{
  ok: boolean;
  updatedRows: number;
  unresolvedRows: number;
  updatedSlates: number;
}> {
  const abbrevCache = new Map(
    (await db
      .select({ teamId: teams.teamId, abbreviation: teams.abbreviation })
      .from(teams))
      .map((t) => [t.abbreviation.toUpperCase(), t.teamId]),
  );
  const resolver = await buildNbaHistoricalTeamResolver(abbrevCache);
  const rows = await db.execute<{
    id: number;
    slateId: number;
    name: string;
    teamAbbrev: string | null;
  }>(sql`
    SELECT
      dp.id,
      dp.slate_id AS "slateId",
      dp.name,
      dp.team_abbrev AS "teamAbbrev"
    FROM dk_players dp
    JOIN dk_slates ds ON ds.id = dp.slate_id
    WHERE ds.sport = 'nba'
      AND dp.actual_fpts IS NOT NULL
      AND (dp.team_id IS NULL OR dp.team_abbrev IS NULL OR dp.team_abbrev = 'UNK')
    ORDER BY ds.slate_date ASC, dp.id ASC
  `);

  let updatedRows = 0;
  let unresolvedRows = 0;
  const touchedSlates = new Set<number>();

  for (const row of rows.rows) {
    const repairedTeam = resolveHistoricalNbaTeam(resolver, row.name, row.teamAbbrev ?? "");
    if (!repairedTeam.teamId || !repairedTeam.teamAbbrev || repairedTeam.teamAbbrev === "UNK") {
      unresolvedRows++;
      continue;
    }
    await db.update(dkPlayers)
      .set({
        teamId: repairedTeam.teamId,
        teamAbbrev: repairedTeam.teamAbbrev,
      })
      .where(eq(dkPlayers.id, row.id));
    updatedRows++;
    touchedSlates.add(row.slateId);
  }

  for (const slateId of touchedSlates) {
    await refreshHistoricalSlateGameCount(slateId, "nba");
  }

  try {
    revalidatePath("/dfs");
    revalidatePath("/analytics");
  } catch {
    // Allow one-off script execution outside a Next request context.
  }
  return {
    ok: true,
    updatedRows,
    unresolvedRows,
    updatedSlates: touchedSlates.size,
  };
}

export async function loadSlateFromContestId(
  contestId: string,
  cashLine?: number,
  contestType?: string,
  fieldSize?: number,
  contestFormat?: string,
): Promise<{
  ok: boolean; message: string; playerCount?: number;
}> {
  try {
    const dgId    = await resolveDraftGroupId(parseInt(contestId, 10));
    const players = await fetchDkPlayersFromApi(dgId);
    if (players.length === 0) return { ok: false, message: "No players returned from DK API" };
    // Auto-fetch LineStar if DNN_COOKIE is available in the server environment
    const lsMap = await tryFetchLinestarMap(dgId);
    const result = await enrichAndSave(players, lsMap, cashLine, dgId, contestType, fieldSize, contestFormat);
    return { ...result, message: `[API] ${result.message}` };
  } catch (e) {
    return { ok: false, message: String(e) };
  }
}

// ── MLB slate loading ─────────────────────────────────────────

const MLB_DK_OVERRIDES: Record<string, string> = {
  CHW: "CWS", ATH: "OAK", KCR: "KC", SFG: "SF", SDP: "SD", TBR: "TB", WAS: "WSH",
};

const MLB_LEAGUE_AVG_TEAM_TOTAL = 4.5;
const MLB_LEAGUE_AVG_XFIP      = 4.20;
const MLB_LEAGUE_AVG_K_PCT     = 0.225;
const MLB_LEAGUE_AVG_ISO       = 0.165;
const MLB_LEAGUE_AVG_HR_PER_9  = 1.10;
const MLB_LEAGUE_AVG_HR_FB     = 0.12;
const MLB_ORDER_PA_FACTOR: Record<number, number> = {
  1: 1.08, 2: 1.12, 3: 1.10, 4: 1.04,
  5: 1.00, 6: 0.96, 7: 0.93, 8: 0.90, 9: 0.88,
};

function mlbCap(v: number, lo: number, hi: number) { return Math.max(lo, Math.min(hi, v)); }

function roundMlbMetric(value: number): number {
  return Math.round(value * 100) / 100;
}

function computeMlbProjectionDistribution(
  projection: number | null,
  pitcherFlag: boolean,
  expectedHr: number | null,
  hrProb1Plus: number | null,
): { projFloor: number | null; projCeiling: number | null; boomRate: number | null } {
  if (projection == null || !Number.isFinite(projection) || projection <= 0) {
    return { projFloor: null, projCeiling: null, boomRate: null };
  }

  if (pitcherFlag) {
    return {
      projFloor: roundMlbMetric(Math.max(0, projection * 0.42)),
      projCeiling: roundMlbMetric(projection * 1.65),
      boomRate: roundMlbMetric(mlbCap(0.04 + (projection / 85), 0.03, 0.42)),
    };
  }

  const hrProb = mlbCap(hrProb1Plus ?? 0, 0, 0.9999);
  const expHr = Math.max(0, expectedHr ?? 0);
  return {
    projFloor: roundMlbMetric(Math.max(0, projection * 0.14)),
    projCeiling: roundMlbMetric((projection * 1.75) + (hrProb * 14) + (expHr * 4)),
    boomRate: roundMlbMetric(mlbCap(0.035 + (projection / 220) + (hrProb * 0.62) + (expHr * 0.12), 0.02, 0.55)),
  };
}

function positiveMlbFloat(value: unknown): number | null {
  const numeric = typeof value === "number" ? value : Number(value);
  return Number.isFinite(numeric) && numeric > 0 ? numeric : null;
}

function mlbRateFraction(value: unknown): number | null {
  const numeric = positiveMlbFloat(value);
  if (numeric == null) return null;
  return numeric > 1 ? numeric / 100 : numeric;
}

const MLB_MAX_CURRENT_SEASON_WEIGHT = 0.9;
const MLB_BATTER_PRIOR_SEASON_PIVOT = 80;
const MLB_BATTER_TEAM_CHANGE_PIVOT = 40;
const MLB_PITCHER_PRIOR_SEASON_PIVOT = 15;
const MLB_PITCHER_TEAM_CHANGE_PIVOT = 8;
const MLB_TEAM_PRIOR_SEASON_PIVOT = 20;
const MLB_BATTER_TEAM_CHANGE_MIN_WEIGHT = 0.35;
const MLB_PITCHER_TEAM_CHANGE_MIN_WEIGHT = 0.4;
const MLB_CONTEXT_WEIGHT_BONUS = 0.05;
const MLB_TEAM_CHANGE_CONTEXT_WEIGHT_BONUS = 0.15;

type MlbSeasonContext = {
  primarySeason: string;
  priorSeason: string | null;
};

type MlbStatsSummary = {
  season: string;
  batters: number;
  pitchers: number;
  teams: number;
  latestFetchedAt: Date | null;
  ready: boolean;
};

function inferMlbSeason(targetDate?: string | null): string {
  if (targetDate) {
    const year = Number(targetDate.slice(0, 4));
    if (Number.isFinite(year) && year >= 2000) return String(year);
  }
  return String(new Date().getUTCFullYear());
}

function inferPriorMlbSeason(season: string): string | null {
  const year = Number(season);
  if (!Number.isFinite(year) || year <= 2000) return null;
  return String(year - 1);
}

function inferMlbSeasonContext(targetDate?: string | null): MlbSeasonContext {
  const primarySeason = inferMlbSeason(targetDate);
  return {
    primarySeason,
    priorSeason: inferPriorMlbSeason(primarySeason),
  };
}

async function loadMlbStatsSummary(season: string): Promise<MlbStatsSummary> {
  const [batterSummary] = await db
    .select({
      count: sql<number>`COUNT(*)`,
      latest: sql<Date | null>`MAX(${mlbBatterStats.fetchedAt})`,
    })
    .from(mlbBatterStats)
    .where(eq(mlbBatterStats.season, season));
  const [pitcherSummary] = await db
    .select({
      count: sql<number>`COUNT(*)`,
      latest: sql<Date | null>`MAX(${mlbPitcherStats.fetchedAt})`,
    })
    .from(mlbPitcherStats)
    .where(eq(mlbPitcherStats.season, season));
  const [teamSummary] = await db
    .select({
      count: sql<number>`COUNT(*)`,
      latest: sql<Date | null>`MAX(${mlbTeamStatsTable.fetchedAt})`,
    })
    .from(mlbTeamStatsTable)
    .where(eq(mlbTeamStatsTable.season, season));

  const latestFetchedAt = [batterSummary?.latest, pitcherSummary?.latest, teamSummary?.latest]
    .map((value) => (value instanceof Date ? value : value ? new Date(value) : null))
    .filter((value): value is Date => value instanceof Date && Number.isFinite(value.getTime()))
    .sort((a, b) => b.getTime() - a.getTime())[0] ?? null;

  const batters = Number(batterSummary?.count ?? 0);
  const pitchers = Number(pitcherSummary?.count ?? 0);
  const teams = Number(teamSummary?.count ?? 0);
  return {
    season,
    batters,
    pitchers,
    teams,
    latestFetchedAt,
    ready: batters > 0 && pitchers > 0 && teams > 0,
  };
}

async function validateMlbStatsReadiness(
  season: string,
): Promise<{ ok: boolean; message?: string }> {
  const primary = await loadMlbStatsSummary(season);
  const priorSeason = inferPriorMlbSeason(season);
  const prior = priorSeason ? await loadMlbStatsSummary(priorSeason) : null;

  if (!primary.ready && !(prior?.ready)) {
    return {
      ok: false,
      message: `MLB stats are missing or incomplete for ${season}${priorSeason ? ` and ${priorSeason}` : ""}. Current counts: batters=${primary.batters}, pitchers=${primary.pitchers}, teams=${primary.teams}. Run the refresh_mlb_stats workflow first.`,
    };
  }

  if (primary.ready && primary.latestFetchedAt) {
    const staleMs = MLB_STATS_STALE_HOURS * 60 * 60 * 1000;
    const ageMs = Date.now() - primary.latestFetchedAt.getTime();
    if (ageMs > staleMs && !(prior?.ready)) {
      const ageHours = Math.round(ageMs / (60 * 60 * 1000));
      return {
        ok: false,
        message: `MLB stats for season ${season} are stale (${ageHours}h old). Run the refresh_mlb_stats workflow before loading the slate.`,
      };
    }
  }

  if (!primary.ready && prior?.ready) {
    return {
      ok: true,
      message: `Season ${season} stats are sparse, so MLB projections will blend against ${priorSeason}.`,
    };
  }
  return { ok: true };
}

type MlbMatchMeta<Row extends { name: string; teamId: number | null }> = {
  row: Row;
  canonicalName: string;
  normalizedName: string;
  firstInitial: string;
  lastToken: string;
  sampleScore: number;
};

type MlbMatchedSeasonRow<Row extends { name: string; teamId: number | null }> = {
  current: Row | null;
  prior: Row | null;
  matchType: "current_only" | "blended" | "prior_only" | "unmatched";
  teamChanged: boolean;
  currentWeight: number;
  contextWeight: number;
};

type MlbCoverageCounter = {
  currentOnly: number;
  blended: number;
  priorOnly: number;
  unmatched: number;
  teamChangeAccelerated: number;
};

function createMlbCoverageCounter(): MlbCoverageCounter {
  return {
    currentOnly: 0,
    blended: 0,
    priorOnly: 0,
    unmatched: 0,
    teamChangeAccelerated: 0,
  };
}

function noteMlbCoverage(counter: MlbCoverageCounter, match: MlbMatchedSeasonRow<{ name: string; teamId: number | null }>) {
  if (match.matchType === "current_only") counter.currentOnly += 1;
  else if (match.matchType === "blended") counter.blended += 1;
  else if (match.matchType === "prior_only") counter.priorOnly += 1;
  else counter.unmatched += 1;
  if (match.teamChanged && match.currentWeight > 0) counter.teamChangeAccelerated += 1;
}

function buildMlbMatchMeta<Row extends { name: string; teamId: number | null }>(
  rows: Row[],
  getSampleScore: (row: Row) => number,
): Array<MlbMatchMeta<Row>> {
  return rows.map((row) => {
    const canonicalName = canonicalizeName(row.name);
    const tokens = canonicalName.split(" ").filter(Boolean);
    return {
      row,
      canonicalName,
      normalizedName: normalizeName(row.name),
      firstInitial: tokens[0]?.[0] ?? "",
      lastToken: tokens[tokens.length - 1] ?? "",
      sampleScore: getSampleScore(row),
    };
  });
}

function preferMlbMetaCandidate<Row extends { name: string; teamId: number | null }>(
  best: MlbMatchMeta<Row> | null,
  candidate: MlbMatchMeta<Row>,
): MlbMatchMeta<Row> {
  if (!best) return candidate;
  if (candidate.sampleScore !== best.sampleScore) {
    return candidate.sampleScore > best.sampleScore ? candidate : best;
  }
  return candidate.canonicalName.length < best.canonicalName.length ? candidate : best;
}

function findBestMlbStatRow<Row extends { name: string; teamId: number | null }>(
  dkName: string,
  dkTeamId: number | null,
  rows: Array<MlbMatchMeta<Row>>,
): Row | null {
  if (rows.length === 0) return null;

  const canonicalName = canonicalizeName(dkName);
  const normalizedName = normalizeName(dkName);

  const exactTeamCanonical = rows
    .filter((row) => dkTeamId != null && row.row.teamId === dkTeamId && row.canonicalName === canonicalName)
    .reduce<MlbMatchMeta<Row> | null>(preferMlbMetaCandidate, null);
  if (exactTeamCanonical) return exactTeamCanonical.row;

  const exactTeamNormalized = rows
    .filter((row) => dkTeamId != null && row.row.teamId === dkTeamId && row.normalizedName === normalizedName)
    .reduce<MlbMatchMeta<Row> | null>(preferMlbMetaCandidate, null);
  if (exactTeamNormalized) return exactTeamNormalized.row;

  const exactCanonical = rows
    .filter((row) => row.canonicalName === canonicalName)
    .reduce<MlbMatchMeta<Row> | null>(preferMlbMetaCandidate, null);
  if (exactCanonical) return exactCanonical.row;

  const exactNormalized = rows
    .filter((row) => row.normalizedName === normalizedName)
    .reduce<MlbMatchMeta<Row> | null>(preferMlbMetaCandidate, null);
  if (exactNormalized) return exactNormalized.row;

  const tokens = canonicalName.split(" ").filter(Boolean);
  const firstInitial = tokens[0]?.[0] ?? "";
  const lastToken = tokens[tokens.length - 1] ?? "";
  if (!firstInitial || !lastToken) return null;

  let best: MlbMatchMeta<Row> | null = null;
  let bestDist = 4;
  for (const candidate of rows) {
    if (candidate.firstInitial !== firstInitial || candidate.lastToken !== lastToken) continue;
    const dist = levenshtein(canonicalName, candidate.canonicalName);
    if (dist > 3) continue;
    if (dist < bestDist) {
      bestDist = dist;
      best = candidate;
      continue;
    }
    if (dist === bestDist) {
      if (dkTeamId != null) {
        const bestTeamMatch = best?.row.teamId === dkTeamId;
        const candidateTeamMatch = candidate.row.teamId === dkTeamId;
        if (candidateTeamMatch !== bestTeamMatch) {
          if (candidateTeamMatch) best = candidate;
          continue;
        }
      }
      best = preferMlbMetaCandidate(best, candidate);
    }
  }
  return best?.row ?? null;
}

function mlbBlendNullableNumber(
  currentValue: number | null | undefined,
  priorValue: number | null | undefined,
  currentWeight: number,
): number | null {
  const current = finiteOrNull(currentValue);
  const prior = finiteOrNull(priorValue);
  if (current == null && prior == null) return null;
  if (current == null) return prior;
  if (prior == null) return current;
  return current * currentWeight + prior * (1 - currentWeight);
}

function mlbBlendNullableInteger(
  currentValue: number | null | undefined,
  priorValue: number | null | undefined,
  currentWeight: number,
): number | null {
  const blended = mlbBlendNullableNumber(currentValue, priorValue, currentWeight);
  return blended == null ? null : Math.round(blended);
}

function mlbPickPreferredScalar<T>(
  currentValue: T | null | undefined,
  priorValue: T | null | undefined,
): T | null {
  return (currentValue ?? priorValue ?? null) as T | null;
}

function getMlbBatterSample(row: MlbBatterStats | null): number {
  if (!row) return 0;
  return Math.max(0, (row.paPg ?? 0) * (row.games ?? 0));
}

function getMlbPitcherSample(row: MlbPitcherStats | null): number {
  if (!row) return 0;
  return Math.max(0, (row.ipPg ?? 0) * (row.games ?? 0));
}

function mlbCurrentSeasonWeight(
  currentSample: number,
  priorPivot: number,
  teamChangePivot: number,
  teamChanged: boolean,
  teamChangeMinWeight: number,
): number {
  if (currentSample <= 0) return 0;
  const pivot = teamChanged ? teamChangePivot : priorPivot;
  let weight = currentSample / (currentSample + pivot);
  if (teamChanged) weight = Math.max(weight, teamChangeMinWeight);
  return mlbCap(weight, 0, MLB_MAX_CURRENT_SEASON_WEIGHT);
}

function mlbContextWeight(currentWeight: number, teamChanged: boolean): number {
  if (currentWeight <= 0) return 0;
  return mlbCap(
    currentWeight + (teamChanged ? MLB_TEAM_CHANGE_CONTEXT_WEIGHT_BONUS : MLB_CONTEXT_WEIGHT_BONUS),
    0,
    0.95,
  );
}

function matchMlbStatsAcrossSeasons<Row extends { name: string; teamId: number | null }>(
  dkName: string,
  dkTeamId: number | null,
  currentRows: Array<MlbMatchMeta<Row>>,
  priorRows: Array<MlbMatchMeta<Row>>,
  getCurrentSample: (row: Row | null) => number,
  priorPivot: number,
  teamChangePivot: number,
  teamChangeMinWeight: number,
): MlbMatchedSeasonRow<Row> {
  const current = findBestMlbStatRow(dkName, dkTeamId, currentRows);
  const prior = findBestMlbStatRow(dkName, dkTeamId, priorRows);
  const teamChanged = Boolean(
    current
    && prior
    && current.teamId != null
    && prior.teamId != null
    && current.teamId !== prior.teamId,
  );
  const currentWeight = mlbCurrentSeasonWeight(
    getCurrentSample(current),
    priorPivot,
    teamChangePivot,
    teamChanged,
    teamChangeMinWeight,
  );
  const contextWeight = mlbContextWeight(currentWeight, teamChanged);
  return {
    current,
    prior,
    matchType: current && prior ? "blended" : current ? "current_only" : prior ? "prior_only" : "unmatched",
    teamChanged,
    currentWeight,
    contextWeight,
  };
}

function blendMlbBatterStats(
  match: MlbMatchedSeasonRow<MlbBatterStats>,
  dkTeamId: number | null,
): MlbBatterStats | null {
  const current = match.current;
  const prior = match.prior;
  if (!current && !prior) return null;
  const currentWeight = match.currentWeight;
  const contextWeight = match.contextWeight;
  return {
    ...(current ?? prior!),
    playerId: current?.playerId ?? prior!.playerId,
    season: current?.season ?? prior!.season,
    teamId: dkTeamId ?? current?.teamId ?? prior!.teamId ?? null,
    name: current?.name ?? prior!.name,
    battingOrder: mlbBlendNullableInteger(current?.battingOrder, prior?.battingOrder, contextWeight),
    games: mlbPickPreferredScalar(current?.games != null && prior?.games != null ? current.games + prior.games : current?.games ?? prior?.games, null),
    paPg: mlbBlendNullableNumber(current?.paPg, prior?.paPg, contextWeight),
    avg: mlbBlendNullableNumber(current?.avg, prior?.avg, currentWeight),
    obp: mlbBlendNullableNumber(current?.obp, prior?.obp, currentWeight),
    slg: mlbBlendNullableNumber(current?.slg, prior?.slg, currentWeight),
    iso: mlbBlendNullableNumber(current?.iso, prior?.iso, currentWeight),
    babip: mlbBlendNullableNumber(current?.babip, prior?.babip, currentWeight),
    wrcPlus: mlbBlendNullableNumber(current?.wrcPlus, prior?.wrcPlus, currentWeight),
    kPct: mlbBlendNullableNumber(current?.kPct, prior?.kPct, currentWeight),
    bbPct: mlbBlendNullableNumber(current?.bbPct, prior?.bbPct, currentWeight),
    hrPg: mlbBlendNullableNumber(current?.hrPg, prior?.hrPg, currentWeight),
    singlesPg: mlbBlendNullableNumber(current?.singlesPg, prior?.singlesPg, currentWeight),
    doublesPg: mlbBlendNullableNumber(current?.doublesPg, prior?.doublesPg, currentWeight),
    triplesPg: mlbBlendNullableNumber(current?.triplesPg, prior?.triplesPg, currentWeight),
    rbiPg: mlbBlendNullableNumber(current?.rbiPg, prior?.rbiPg, contextWeight),
    runsPg: mlbBlendNullableNumber(current?.runsPg, prior?.runsPg, contextWeight),
    sbPg: mlbBlendNullableNumber(current?.sbPg, prior?.sbPg, currentWeight),
    hbpPg: mlbBlendNullableNumber(current?.hbpPg, prior?.hbpPg, currentWeight),
    wrcPlusVsL: mlbBlendNullableNumber(current?.wrcPlusVsL, prior?.wrcPlusVsL, currentWeight),
    wrcPlusVsR: mlbBlendNullableNumber(current?.wrcPlusVsR, prior?.wrcPlusVsR, currentWeight),
    avgFptsPg: mlbBlendNullableNumber(current?.avgFptsPg, prior?.avgFptsPg, contextWeight),
    fptsStd: mlbBlendNullableNumber(current?.fptsStd, prior?.fptsStd, contextWeight),
    fetchedAt: current?.fetchedAt ?? prior!.fetchedAt,
  };
}

function blendMlbPitcherStats(
  match: MlbMatchedSeasonRow<MlbPitcherStats>,
  dkTeamId: number | null,
): MlbPitcherStats | null {
  const current = match.current;
  const prior = match.prior;
  if (!current && !prior) return null;
  const currentWeight = match.currentWeight;
  const contextWeight = match.contextWeight;
  return {
    ...(current ?? prior!),
    playerId: current?.playerId ?? prior!.playerId,
    season: current?.season ?? prior!.season,
    teamId: dkTeamId ?? current?.teamId ?? prior!.teamId ?? null,
    name: current?.name ?? prior!.name,
    hand: mlbPickPreferredScalar(current?.hand, prior?.hand),
    games: mlbPickPreferredScalar(current?.games != null && prior?.games != null ? current.games + prior.games : current?.games ?? prior?.games, null),
    ipPg: mlbBlendNullableNumber(current?.ipPg, prior?.ipPg, contextWeight),
    era: mlbBlendNullableNumber(current?.era, prior?.era, currentWeight),
    fip: mlbBlendNullableNumber(current?.fip, prior?.fip, currentWeight),
    xfip: mlbBlendNullableNumber(current?.xfip, prior?.xfip, currentWeight),
    kPer9: mlbBlendNullableNumber(current?.kPer9, prior?.kPer9, currentWeight),
    bbPer9: mlbBlendNullableNumber(current?.bbPer9, prior?.bbPer9, currentWeight),
    hrPer9: mlbBlendNullableNumber(current?.hrPer9, prior?.hrPer9, currentWeight),
    kPct: mlbBlendNullableNumber(current?.kPct, prior?.kPct, currentWeight),
    bbPct: mlbBlendNullableNumber(current?.bbPct, prior?.bbPct, currentWeight),
    hrFbPct: mlbBlendNullableNumber(current?.hrFbPct, prior?.hrFbPct, currentWeight),
    whip: mlbBlendNullableNumber(current?.whip, prior?.whip, currentWeight),
    avgFptsPg: mlbBlendNullableNumber(current?.avgFptsPg, prior?.avgFptsPg, contextWeight),
    fptsStd: mlbBlendNullableNumber(current?.fptsStd, prior?.fptsStd, contextWeight),
    winPct: mlbBlendNullableNumber(current?.winPct, prior?.winPct, contextWeight),
    qsPct: mlbBlendNullableNumber(current?.qsPct, prior?.qsPct, contextWeight),
    fetchedAt: current?.fetchedAt ?? prior!.fetchedAt,
  };
}

function buildMlbTeamSampleMap(
  batterRows: MlbBatterStats[],
  pitcherRows: MlbPitcherStats[],
): Map<number, number> {
  const samples = new Map<number, number>();
  for (const row of batterRows) {
    if (row.teamId == null) continue;
    samples.set(row.teamId, Math.max(samples.get(row.teamId) ?? 0, row.games ?? 0));
  }
  for (const row of pitcherRows) {
    if (row.teamId == null) continue;
    samples.set(row.teamId, Math.max(samples.get(row.teamId) ?? 0, row.games ?? 0));
  }
  return samples;
}

function blendMlbTeamStats(
  current: MlbTeamStats | null,
  prior: MlbTeamStats | null,
  currentSampleGames: number,
): MlbTeamStats | null {
  if (!current && !prior) return null;
  const currentWeight = mlbCap(
    currentSampleGames > 0 ? currentSampleGames / (currentSampleGames + MLB_TEAM_PRIOR_SEASON_PIVOT) : 0,
    0,
    MLB_MAX_CURRENT_SEASON_WEIGHT,
  );
  return {
    ...(current ?? prior!),
    teamId: current?.teamId ?? prior!.teamId,
    season: current?.season ?? prior!.season,
    teamWrcPlus: mlbBlendNullableNumber(current?.teamWrcPlus, prior?.teamWrcPlus, currentWeight),
    teamKPct: mlbBlendNullableNumber(current?.teamKPct, prior?.teamKPct, currentWeight),
    teamBbPct: mlbBlendNullableNumber(current?.teamBbPct, prior?.teamBbPct, currentWeight),
    teamIso: mlbBlendNullableNumber(current?.teamIso, prior?.teamIso, currentWeight),
    teamOps: mlbBlendNullableNumber(current?.teamOps, prior?.teamOps, currentWeight),
    bullpenEra: mlbBlendNullableNumber(current?.bullpenEra, prior?.bullpenEra, currentWeight),
    bullpenFip: mlbBlendNullableNumber(current?.bullpenFip, prior?.bullpenFip, currentWeight),
    staffKPct: mlbBlendNullableNumber(current?.staffKPct, prior?.staffKPct, currentWeight),
    staffBbPct: mlbBlendNullableNumber(current?.staffBbPct, prior?.staffBbPct, currentWeight),
    fetchedAt: current?.fetchedAt ?? prior!.fetchedAt,
  };
}

function pickMlbParkFactor(
  current: MlbParkFactors | null,
  prior: MlbParkFactors | null,
): MlbParkFactors | null {
  return current ?? prior ?? null;
}

function mlbWinProb(matchup: Record<string, unknown>, isHome: boolean): number {
  const hml = matchup.homeMl as number | null;
  const aml = matchup.awayMl as number | null;
  if (hml != null && aml != null) {
    const rh = hml >= 0 ? 100 / (hml + 100) : Math.abs(hml) / (Math.abs(hml) + 100);
    const ra = aml >= 0 ? 100 / (aml + 100) : Math.abs(aml) / (Math.abs(aml) + 100);
    const tot = rh + ra;
    if (tot > 0) return isHome ? rh / tot : ra / tot;
  }
  const vph = matchup.vegasProbHome as number | null;
  if (vph != null) return isHome ? vph : 1 - vph;
  return 0.5;
}

function dkBatterFpts(s: number, d: number, t: number, hr: number,
  rbi: number, runs: number, bb: number, hbp: number, sb: number) {
  return s * 3 + d * 5 + t * 8 + hr * 10 + rbi * 2 + runs * 2 + bb * 2 + hbp * 2 + sb * 5;
}

function dkPitcherFpts(ip: number, k: number, er: number, h: number, bb: number, wp: number) {
  return ip * 2.25 + k * 2 - er * 2 - h * 0.6 - bb * 0.6 + wp * 4;
}

function computeMlbBatterProj(
  b: Record<string, unknown>,
  matchup: Record<string, unknown>,
  oppSp: Record<string, unknown> | null,
  park: Record<string, unknown> | null,
  isHome: boolean,
  confirmedOrder: number | null,
): number | null {
  if (((b.games as number) || 0) < 3) return null;
  const sPg = (b.singlesPg as number) || 0, dPg = (b.doublesPg as number) || 0;
  const tPg = (b.triplesPg as number) || 0, hrPg = (b.hrPg as number) || 0;
  const rbiPg = (b.rbiPg as number) || 0, runsPg = (b.runsPg as number) || 0;
  const hbpPg = (b.hbpPg as number) || 0, sbPg = (b.sbPg as number) || 0;
  const bbPct = (b.bbPct as number) || 0.085, paPg = (b.paPg as number) || 4.0;
  const bbPg = bbPct * paPg;
  if (sPg + dPg + hrPg + rbiPg + runsPg + bbPg < 0.05) return null;

  const implied = isHome
    ? ((matchup.homeImplied as number) || ((matchup.vegasTotal as number) || 9) / 2)
    : ((matchup.awayImplied as number) || ((matchup.vegasTotal as number) || 9) / 2);
  const envFactor   = mlbCap(implied / MLB_LEAGUE_AVG_TEAM_TOTAL, 0.5, 2.0);
  const runsPf      = mlbCap((park?.runsFactor as number) || 1.0, 0.7, 1.3);
  const hrPf        = mlbCap((park?.hrFactor  as number) || 1.0, 0.7, 1.5);
  const orderFactor = confirmedOrder != null ? (MLB_ORDER_PA_FACTOR[confirmedOrder] || 1.0) : 1.0;
  let xfipFactor = 1.0;
  if (oppSp) {
    const spXfip = (oppSp.xfip as number) || (oppSp.era as number) || MLB_LEAGUE_AVG_XFIP;
    xfipFactor = mlbCap(spXfip / MLB_LEAGUE_AVG_XFIP, 0.6, 1.8);
  }
  let matchupFactor = 1.0;
  const wrcBase = b.wrcPlus as number | null;
  if (oppSp && (oppSp.hand as string) && wrcBase && wrcBase > 0) {
    const hand  = ((oppSp.hand as string) || "").toUpperCase();
    const wrcVs = hand === "L" ? (b.wrcPlusVsL as number | null) : (b.wrcPlusVsR as number | null);
    if (wrcVs) matchupFactor = mlbCap(wrcVs / wrcBase, 0.5, 1.75);
  }
  const hf  = mlbCap(envFactor * runsPf * xfipFactor * orderFactor * matchupFactor, 0.3, 3.0);
  const hrf = mlbCap(envFactor * hrPf   * xfipFactor * orderFactor * matchupFactor, 0.3, 3.0);
  const wf  = mlbCap(envFactor * xfipFactor * orderFactor, 0.3, 3.0);
  const sf  = mlbCap(envFactor * orderFactor, 0.3, 3.0);
  const fpts = dkBatterFpts(sPg*hf, dPg*hf, tPg*hf, hrPg*hrf, rbiPg*hf, runsPg*hf, bbPg*wf, hbpPg*wf, sbPg*sf);
  return fpts > 0 ? Math.round(fpts * 100) / 100 : null;
}

function computeMlbBatterHrSignal(
  b: Record<string, unknown>,
  matchup: Record<string, unknown>,
  oppSp: Record<string, unknown> | null,
  park: Record<string, unknown> | null,
  isHome: boolean,
  confirmedOrder: number | null,
): { expectedHr: number; hrProb1Plus: number } | null {
  if (((b.games as number) || 0) < 3) return null;
  const hrPg = (b.hrPg as number) || 0;
  if (hrPg < 0) return null;

  const implied = isHome
    ? ((matchup.homeImplied as number) || ((matchup.vegasTotal as number) || 9) / 2)
    : ((matchup.awayImplied as number) || ((matchup.vegasTotal as number) || 9) / 2);
  const envFactor = mlbCap(implied / MLB_LEAGUE_AVG_TEAM_TOTAL, 0.5, 2.0);
  const hrPf = mlbCap((park?.hrFactor as number) || 1.0, 0.7, 1.5);
  const orderFactor = confirmedOrder != null ? (MLB_ORDER_PA_FACTOR[confirmedOrder] || 1.0) : 1.0;

  const iso = positiveMlbFloat(b.iso);
  const slg = positiveMlbFloat(b.slg);
  const isoFactor = iso != null ? mlbCap(iso / MLB_LEAGUE_AVG_ISO, 0.7, 1.65) : 1.0;
  const slgFactor = slg != null ? mlbCap(slg / 0.410, 0.8, 1.35) : 1.0;
  const rawPowerFactor = Math.sqrt(isoFactor * slgFactor);
  const powerFactor = 1.0 + (rawPowerFactor - 1.0) * 0.35;

  let xfipFactor = 1.0;
  let pitcherHrFactor = 1.0;
  if (oppSp) {
    const spXfip = positiveMlbFloat(oppSp.xfip) ?? positiveMlbFloat(oppSp.era) ?? MLB_LEAGUE_AVG_XFIP;
    xfipFactor = mlbCap(spXfip / MLB_LEAGUE_AVG_XFIP, 0.6, 1.8);
    const hrPer9 = positiveMlbFloat(oppSp.hrPer9);
    const hrFbPct = mlbRateFraction(oppSp.hrFbPct);
    const hr9Factor = hrPer9 != null ? mlbCap(hrPer9 / MLB_LEAGUE_AVG_HR_PER_9, 0.65, 1.75) : 1.0;
    const hrFbFactor = hrFbPct != null ? mlbCap(hrFbPct / MLB_LEAGUE_AVG_HR_FB, 0.7, 1.6) : 1.0;
    const rawPitcherHrFactor = Math.sqrt(hr9Factor * hrFbFactor);
    pitcherHrFactor = 1.0 + (rawPitcherHrFactor - 1.0) * 0.45;
  }

  let matchupFactor = 1.0;
  const wrcBase = b.wrcPlus as number | null;
  if (oppSp && (oppSp.hand as string) && wrcBase && wrcBase > 0) {
    const hand = ((oppSp.hand as string) || "").toUpperCase();
    const wrcVs = hand === "L" ? (b.wrcPlusVsL as number | null) : (b.wrcPlusVsR as number | null);
    if (wrcVs) matchupFactor = mlbCap(wrcVs / wrcBase, 0.5, 1.75);
  }

  const hrFactorAdj = mlbCap(
    envFactor * hrPf * powerFactor * xfipFactor * pitcherHrFactor * orderFactor * matchupFactor,
    0.3,
    3.0,
  );
  const expectedHr = Math.max(0, hrPg * hrFactorAdj);
  const hrProb1Plus = 1 - Math.exp(-expectedHr);
  return {
    expectedHr: Math.round(expectedHr * 1000) / 1000,
    hrProb1Plus: Math.round(Math.min(0.9999, Math.max(0, hrProb1Plus)) * 10000) / 10000,
  };
}

function computeMlbPitcherProj(
  p: Record<string, unknown>,
  matchup: Record<string, unknown>,
  oppTeam: Record<string, unknown> | null,
  park: Record<string, unknown> | null,
  isHome: boolean,
): number | null {
  if (((p.games as number) || 0) < 2) return null;
  const ipPg = (p.ipPg as number) || 0;
  if (ipPg < 0.5) return null;
  const kPer9  = (p.kPer9 as number)  || 0;
  const bbPer9 = (p.bbPer9 as number) || 0;
  const era    = (p.era as number)   || 4.5;
  const whip   = (p.whip as number)  || 1.3;
  const xfip   = (p.xfip as number)  || era;
  const ip = ipPg, k = kPer9 / 9 * ip, bb = bbPer9 / 9 * ip;
  const er = xfip / 9 * ip, h = Math.max(0, whip * ip - bb);
  const oppWrc  = oppTeam ? ((oppTeam.teamWrcPlus as number) || 100) : 100;
  const oppKPct = oppTeam ? ((oppTeam.teamKPct as number)   || MLB_LEAGUE_AVG_K_PCT) : MLB_LEAGUE_AVG_K_PCT;
  const owf = mlbCap(oppWrc / 100, 0.6, 1.6), okf = mlbCap(oppKPct / MLB_LEAGUE_AVG_K_PCT, 0.6, 1.6);
  const runsPf  = mlbCap((park?.runsFactor as number) || 1.0, 0.7, 1.3);
  const histWin = (p.winPct as number) || 0;
  const teamWin = mlbWinProb(matchup, isHome);
  const effWin  = histWin > 0 ? (histWin + teamWin) / 2 : 0;
  const fpts = dkPitcherFpts(ip, k * okf, er * owf * runsPf, h * owf * runsPf, bb, effWin);
  return fpts > 0 ? Math.round(fpts * 100) / 100 : null;
}

function isPitcherPos(pos: string): boolean {
  return pos.includes("SP") || pos.includes("RP");
}

async function ensureMatchupsForMlbSlate(
  slateDate: string,
  dkPlayers_: DkApiPlayer[],
  abbrevToId: Map<string, number>,
): Promise<void> {
  await ensureOddsHistoryTables();
  const existing = await db.select({ id: mlbMatchups.id })
    .from(mlbMatchups)
    .where(eq(mlbMatchups.gameDate, slateDate));
  if (existing.length > 0) return;

  const resolve = (abbrev: string): number | null => {
    const canon = MLB_DK_OVERRIDES[abbrev] ?? abbrev;
    return abbrevToId.get(canon) ?? null;
  };
  const gameSeen = new Set<string>();
  const games: { homeTeamId: number; awayTeamId: number }[] = [];
  for (const p of dkPlayers_) {
    const key = p.gameInfo.split(" ")[0];
    if (!key || gameSeen.has(key)) continue;
    gameSeen.add(key);
    const [awayAbbr, homeAbbr] = key.split("@");
    const homeTeamId = resolve(homeAbbr ?? "");
    const awayTeamId = resolve(awayAbbr ?? "");
    if (homeTeamId && awayTeamId) games.push({ homeTeamId, awayTeamId });
  }
  if (games.length > 0) {
    await db.insert(mlbMatchups)
      .values(games.map((g) => ({ gameDate: slateDate, ...g })))
      .onConflictDoNothing();
  }
  // Fetch Vegas odds if key available
  const oddsKey = process.env.ODDS_API_KEY;
  if (oddsKey && games.length > 0) {
    try {
      const oddsUrl = new URL("https://api.the-odds-api.com/v4/sports/baseball_mlb/odds/");
      oddsUrl.searchParams.set("apiKey", oddsKey);
      oddsUrl.searchParams.set("regions", "us");
      oddsUrl.searchParams.set("markets", "h2h,totals");
      oddsUrl.searchParams.set("oddsFormat", "american");
      const oddsResp = await fetch(oddsUrl.toString(), { next: { revalidate: 0 } });
      if (oddsResp.ok) {
        const oddsGames = await oddsResp.json() as Array<{
          home_team: string; away_team: string;
          bookmakers: Array<{ markets: Array<{ key: string; outcomes: Array<{ name: string; price: number; point?: number }> }> }>;
        }>;
        const matchupRows = await db.execute<{ id: number; homeName: string; homeTeamId: number | null; awayTeamId: number | null }>(sql`
          SELECT mm.id, mt.name AS "homeName", mm.home_team_id AS "homeTeamId", mm.away_team_id AS "awayTeamId"
          FROM mlb_matchups mm
          JOIN mlb_teams mt ON mt.team_id = mm.home_team_id
          WHERE mm.game_date = ${slateDate}
        `);
        const byHome = new Map(matchupRows.rows.map((r) => [r.homeName, r]));
        const historyRows: GameOddsHistoryInput[] = [];
        for (const og of oddsGames) {
          const matchup = byHome.get(og.home_team);
          if (!matchup) continue;
          const hPs: number[] = [], aPs: number[] = [], tots: number[] = [];
          for (const bm of og.bookmakers ?? []) {
            for (const market of bm.markets ?? []) {
              if (market.key === "h2h") {
                const ho = market.outcomes.find((o) => o.name === og.home_team);
                const ao = market.outcomes.find((o) => o.name === og.away_team);
                if (ho) hPs.push(ho.price);
                if (ao) aPs.push(ao.price);
              } else if (market.key === "totals") {
                const over = market.outcomes.find((o) => o.name === "Over");
                if (over?.point != null) tots.push(over.point);
              }
            }
          }
          const avg = (arr: number[]) => arr.length ? Math.round(arr.reduce((a, b) => a + b) / arr.length) : null;
          const homeMl = avg(hPs), awayMl = avg(aPs);
          const vegasTotal = tots.length ? Math.round(tots.reduce((a, b) => a + b) / tots.length * 2) / 2 : null;
          // Compute implied run totals from moneylines
          let homeImplied: number | null = null, awayImplied: number | null = null;
          if (vegasTotal && homeMl != null && awayMl != null) {
            const rh = homeMl >= 0 ? 100 / (homeMl + 100) : Math.abs(homeMl) / (Math.abs(homeMl) + 100);
            const ra = awayMl >= 0 ? 100 / (awayMl + 100) : Math.abs(awayMl) / (Math.abs(awayMl) + 100);
            const tot = rh + ra;
            const homeWinClean = tot > 0 ? rh / tot : 0.5;
            const spread = Math.max(-10, Math.min(10, (homeWinClean - 0.5) / 0.025)) / 2;
            homeImplied = Math.round((vegasTotal / 2 + spread) * 10) / 10;
            awayImplied = Math.round((vegasTotal - homeImplied) * 10) / 10;
          }
          if (homeMl || awayMl || vegasTotal) {
            await db.execute(sql`
              UPDATE mlb_matchups
              SET home_ml = ${homeMl}, away_ml = ${awayMl}, vegas_total = ${vegasTotal},
                  home_implied = ${homeImplied}, away_implied = ${awayImplied}
              WHERE id = ${matchup.id}
            `);
            historyRows.push({
              sport: "mlb",
              matchupId: matchup.id,
              eventId: null,
              gameDate: slateDate,
              homeTeamId: matchup.homeTeamId,
              awayTeamId: matchup.awayTeamId,
              homeTeamName: og.home_team,
              awayTeamName: og.away_team,
              bookmakerCount: og.bookmakers?.length ?? 0,
              homeMl,
              awayMl,
              vegasTotal,
              homeImplied,
              awayImplied,
            });
          }
        }
        await recordGameOddsHistory(historyRows);
      }
    } catch { /* best-effort */ }
  }
}

async function enrichAndSaveMlb(
  dkPlayers_: DkApiPlayer[],
  lsMap: Map<string, LinestarEntry>,
  cashLine?: number,
  draftGroupId?: number,
  contestType?: string,
  fieldSize?: number,
  contestFormat?: string,
): Promise<{ ok: boolean; message: string; playerCount?: number; matchRate?: number }> {
  const normalizedContestType = normalizeDkSlateTiming(contestType);
  let slateDate = "";
  for (const p of dkPlayers_) {
    const d = parseSlateDate(p.gameInfo);
    if (d) { slateDate = d; break; }
  }
  if (!slateDate) slateDate = new Date().toISOString().slice(0, 10);
  await ensureDkPlayerPropColumns();
  const { primarySeason: mlbSeason, priorSeason: priorMlbSeason } = inferMlbSeasonContext(slateDate);
  const gameCount = new Set(dkPlayers_.map((p) => p.gameInfo.split(" ")[0])).size;

  const slateVals: Record<string, unknown> = { slateDate, gameCount, sport: "mlb" };
  if (cashLine != null)     slateVals.cashLine     = cashLine;
  if (draftGroupId != null) slateVals.dkDraftGroupId = draftGroupId;
  if (normalizedContestType) slateVals.contestType = normalizedContestType;
  if (fieldSize != null)    slateVals.fieldSize    = fieldSize;
  if (contestFormat)        slateVals.contestFormat = contestFormat;

  const conflictVals: Record<string, unknown> = { gameCount };
  if (cashLine != null)     conflictVals.cashLine     = cashLine;
  if (draftGroupId != null) conflictVals.dkDraftGroupId = draftGroupId;
  if (normalizedContestType) conflictVals.contestType = normalizedContestType;
  if (fieldSize != null)    conflictVals.fieldSize    = fieldSize;
  if (contestFormat)        conflictVals.contestFormat = contestFormat;

  const [slate] = await db
    .insert(dkSlates)
    .values(slateVals as typeof dkSlates.$inferInsert)
    .onConflictDoUpdate({
      target: [dkSlates.slateDate, dkSlates.contestType, dkSlates.contestFormat, dkSlates.sport],
      set: conflictVals,
    })
    .returning({ id: dkSlates.id });
  const slateId = slate.id;

  // Build MLB team abbrev → teamId cache
  const mlbTeamRows = await db.select({ teamId: mlbTeams.teamId, abbreviation: mlbTeams.abbreviation }).from(mlbTeams);
  const abbrevToId  = new Map(mlbTeamRows.map((t) => [t.abbreviation.toUpperCase(), t.teamId]));

  await ensureMatchupsForMlbSlate(slateDate, dkPlayers_, abbrevToId);

  const matchupRows = await db.select().from(mlbMatchups).where(eq(mlbMatchups.gameDate, slateDate));
  const matchupByTeam = new Map<number, typeof matchupRows[0]>();
  for (const m of matchupRows) {
    if (m.homeTeamId) matchupByTeam.set(m.homeTeamId, m);
    if (m.awayTeamId) matchupByTeam.set(m.awayTeamId, m);
  }

  const [
    currentBatterRows,
    currentPitcherRows,
    currentTeamStatRows,
    currentParkRows,
    priorBatterRows,
    priorPitcherRows,
    priorTeamStatRows,
    priorParkRows,
  ] = await Promise.all([
    db.select().from(mlbBatterStats).where(eq(mlbBatterStats.season, mlbSeason)),
    db.select().from(mlbPitcherStats).where(eq(mlbPitcherStats.season, mlbSeason)),
    db.select().from(mlbTeamStatsTable).where(eq(mlbTeamStatsTable.season, mlbSeason)),
    db.select().from(mlbParkFactors).where(eq(mlbParkFactors.season, mlbSeason)),
    priorMlbSeason ? db.select().from(mlbBatterStats).where(eq(mlbBatterStats.season, priorMlbSeason)) : Promise.resolve([] as MlbBatterStats[]),
    priorMlbSeason ? db.select().from(mlbPitcherStats).where(eq(mlbPitcherStats.season, priorMlbSeason)) : Promise.resolve([] as MlbPitcherStats[]),
    priorMlbSeason ? db.select().from(mlbTeamStatsTable).where(eq(mlbTeamStatsTable.season, priorMlbSeason)) : Promise.resolve([] as MlbTeamStats[]),
    priorMlbSeason ? db.select().from(mlbParkFactors).where(eq(mlbParkFactors.season, priorMlbSeason)) : Promise.resolve([] as MlbParkFactors[]),
  ]);

  const currentBatterMeta = buildMlbMatchMeta(currentBatterRows, getMlbBatterSample);
  const priorBatterMeta = buildMlbMatchMeta(priorBatterRows, getMlbBatterSample);
  const currentPitcherMeta = buildMlbMatchMeta(currentPitcherRows, getMlbPitcherSample);
  const priorPitcherMeta = buildMlbMatchMeta(priorPitcherRows, getMlbPitcherSample);
  const hitterProjectionCalibration = await loadMlbHitterProjectionCalibration();

  const currentTeamStatsMap = new Map(currentTeamStatRows.map((row) => [row.teamId, row]));
  const priorTeamStatsMap = new Map(priorTeamStatRows.map((row) => [row.teamId, row]));
  const currentParkMap = new Map(currentParkRows.map((row) => [row.teamId, row]));
  const priorParkMap = new Map(priorParkRows.map((row) => [row.teamId, row]));
  const currentTeamSamples = buildMlbTeamSampleMap(currentBatterRows, currentPitcherRows);
  const teamStatsMap = new Map<number, MlbTeamStats>();
  for (const teamId of new Set<number>([
    ...currentTeamStatsMap.keys(),
    ...priorTeamStatsMap.keys(),
  ])) {
    const blended = blendMlbTeamStats(
      currentTeamStatsMap.get(teamId) ?? null,
      priorTeamStatsMap.get(teamId) ?? null,
      currentTeamSamples.get(teamId) ?? 0,
    );
    if (blended) teamStatsMap.set(teamId, blended);
  }
  const parkMap = new Map<number, MlbParkFactors>();
  for (const teamId of new Set<number>([
    ...currentParkMap.keys(),
    ...priorParkMap.keys(),
  ])) {
    const picked = pickMlbParkFactor(currentParkMap.get(teamId) ?? null, priorParkMap.get(teamId) ?? null);
    if (picked) parkMap.set(teamId, picked);
  }
  const lineupConfirmedByTeam = inferMlbTeamLineupConfirmed(dkPlayers_);

  // SP pre-pass: one SP per team
  const spByTeam = new Map<number, MlbPitcherStats>();
  for (const p of dkPlayers_) {
    if (!isPitcherPos(p.eligiblePositions) || !isLikelyActiveMlbPitcher(p)) continue;
    const canon = MLB_DK_OVERRIDES[p.teamAbbrev] ?? p.teamAbbrev;
    const tid = abbrevToId.get(canon);
    if (!tid || spByTeam.has(tid)) continue;
    const match = matchMlbStatsAcrossSeasons(
      p.name,
      tid,
      currentPitcherMeta,
      priorPitcherMeta,
      getMlbPitcherSample,
      MLB_PITCHER_PRIOR_SEASON_PIVOT,
      MLB_PITCHER_TEAM_CHANGE_PIVOT,
      MLB_PITCHER_TEAM_CHANGE_MIN_WEIGHT,
    );
    const blended = blendMlbPitcherStats(match, tid);
    if (blended) spByTeam.set(tid, blended);
  }

  let lsMatched = 0, projComputed = 0;
  const batterCoverage = createMlbCoverageCounter();
  const pitcherCoverage = createMlbCoverageCounter();
  const insertValues: Array<Record<string, unknown>> = [];
  const homerunSnapshots: MlbHomerunSnapshotInput[] = [];

  for (const p of dkPlayers_) {
    const canon = MLB_DK_OVERRIDES[p.teamAbbrev] ?? p.teamAbbrev;
    const mlbTeamId = abbrevToId.get(canon) ?? null;
    const matchup   = mlbTeamId ? matchupByTeam.get(mlbTeamId) ?? null : null;
    const matchupId = matchup?.id ?? null;
    const dkTeamLineupConfirmed = lineupConfirmedByTeam.get((p.teamAbbrev ?? "").toUpperCase()) ?? false;
    const dkStartingLineupOrder = isPositiveMlbLineupOrder(p.startingLineupOrder) ? p.startingLineupOrder : null;
    const confirmedBatterOut =
      !isPitcherPos(p.eligiblePositions)
      && dkTeamLineupConfirmed
      && dkStartingLineupOrder == null
      && p.inStartingLineup !== true;

    const ls = findLinestarMatch(p.name, p.salary, lsMap);
    if (ls) lsMatched++;
    const linestarProj = sanitizeProjection(ls?.linestarProj ?? null);
    const linestarOwnPct = sanitizeOwnershipPct(ls?.projOwnPct ?? null);
    const projOwnPct = linestarOwnPct;

    const isHome = matchup?.homeTeamId === mlbTeamId;
    const park   = matchup ? parkMap.get(matchup.homeTeamId ?? 0) ?? null : null;

    let ourProj: number | null  = null;
    let expectedHr: number | null = null;
    let hrProb1Plus: number | null = null;
    let homerunSnapshot: MlbHomerunSnapshotInput | null = null;
    const pitcherFlag = isPitcherPos(p.eligiblePositions);

    if (pitcherFlag) {
      const pitcherMatch = matchMlbStatsAcrossSeasons(
        p.name,
        mlbTeamId,
        currentPitcherMeta,
        priorPitcherMeta,
        getMlbPitcherSample,
        MLB_PITCHER_PRIOR_SEASON_PIVOT,
        MLB_PITCHER_TEAM_CHANGE_PIVOT,
        MLB_PITCHER_TEAM_CHANGE_MIN_WEIGHT,
      );
      noteMlbCoverage(pitcherCoverage, pitcherMatch);
      const blendedPitcher = blendMlbPitcherStats(pitcherMatch, mlbTeamId);
      if (blendedPitcher && mlbTeamId && matchup) {
        const oppTeamId = isHome ? matchup.awayTeamId : matchup.homeTeamId;
        const oppTeam   = oppTeamId ? teamStatsMap.get(oppTeamId) ?? null : null;
        ourProj = sanitizeProjection(computeMlbPitcherProj(
          blendedPitcher as unknown as Record<string, unknown>,
          matchup as unknown as Record<string, unknown>,
          oppTeam as unknown as Record<string, unknown> | null,
          park as unknown as Record<string, unknown> | null,
          isHome,
        ));
        if (ourProj != null) projComputed++;
      }
    } else {
      const batterMatch = matchMlbStatsAcrossSeasons(
        p.name,
        mlbTeamId,
        currentBatterMeta,
        priorBatterMeta,
        getMlbBatterSample,
        MLB_BATTER_PRIOR_SEASON_PIVOT,
        MLB_BATTER_TEAM_CHANGE_PIVOT,
        MLB_BATTER_TEAM_CHANGE_MIN_WEIGHT,
      );
      noteMlbCoverage(batterCoverage, batterMatch);
      const blendedBatter = blendMlbBatterStats(batterMatch, mlbTeamId);
      if (blendedBatter && mlbTeamId && matchup) {
        const oppTeamId = isHome ? matchup.awayTeamId : matchup.homeTeamId;
        const oppSp     = oppTeamId ? spByTeam.get(oppTeamId) ?? null : null;
        ourProj = sanitizeProjection(computeMlbBatterProj(
          blendedBatter as unknown as Record<string, unknown>,
          matchup as unknown as Record<string, unknown>,
          oppSp as unknown as Record<string, unknown> | null,
          park as unknown as Record<string, unknown> | null,
          isHome,
          dkTeamLineupConfirmed ? dkStartingLineupOrder : null,
        ));
        ourProj = applyMlbHitterProjectionCalibration(
          ourProj,
          dkTeamLineupConfirmed ? dkStartingLineupOrder : null,
          dkTeamLineupConfirmed,
          hitterProjectionCalibration,
        );
        const hrSignal = computeMlbBatterHrSignal(
          blendedBatter as unknown as Record<string, unknown>,
          matchup as unknown as Record<string, unknown>,
          oppSp as unknown as Record<string, unknown> | null,
          park as unknown as Record<string, unknown> | null,
          isHome,
          dkTeamLineupConfirmed ? dkStartingLineupOrder : null,
        );
        expectedHr = hrSignal?.expectedHr ?? null;
        hrProb1Plus = hrSignal?.hrProb1Plus ?? null;
        const oppHand = (oppSp?.hand ?? "").toUpperCase();
        const teamTotal = isHome ? matchup.homeImplied : matchup.awayImplied;
        homerunSnapshot = {
          slateId,
          dkPlayerId: p.dkId,
          name: p.name,
          teamId: mlbTeamId,
          teamAbbrev: p.teamAbbrev,
          salary: p.salary,
          eligiblePositions: p.eligiblePositions,
          isOut: false,
          lineupOrder: dkStartingLineupOrder,
          lineupConfirmed: dkTeamLineupConfirmed,
          expectedHr,
          hrProb1Plus,
          hitterHrPg: finiteOrNull(blendedBatter.hrPg),
          hitterIso: finiteOrNull(blendedBatter.iso),
          hitterSlug: finiteOrNull(blendedBatter.slg),
          hitterPaPg: finiteOrNull(blendedBatter.paPg),
          hitterWrcPlus: finiteOrNull(blendedBatter.wrcPlus),
          hitterSplitWrcPlus: oppHand === "L"
            ? finiteOrNull(blendedBatter.wrcPlusVsL)
            : oppHand === "R"
              ? finiteOrNull(blendedBatter.wrcPlusVsR)
              : null,
          teamTotal: finiteOrNull(teamTotal),
          vegasTotal: finiteOrNull(matchup.vegasTotal),
          parkHrFactor: finiteOrNull(park?.hrFactor),
          weatherTemp: finiteOrNull(matchup.weatherTemp),
          windSpeed: finiteOrNull(matchup.windSpeed),
          opposingPitcherName: oppSp?.name ?? (isHome ? matchup.awaySpName : matchup.homeSpName) ?? null,
          opposingPitcherHand: oppSp?.hand ?? null,
          opposingPitcherHrPer9: finiteOrNull(oppSp?.hrPer9),
          opposingPitcherHrFbPct: finiteOrNull(oppSp?.hrFbPct),
          opposingPitcherXfip: finiteOrNull(oppSp?.xfip),
          opposingPitcherEra: finiteOrNull(oppSp?.era),
        };
        if (ourProj != null) projComputed++;
      }
    }

    const dkIsOut =
      p.isDisabled
      || ["O", "OUT"].includes(p.dkStatus.toUpperCase())
      || !isLikelyActiveMlbPitcher(p);
    const isOut   = dkIsOut || confirmedBatterOut;
    if (homerunSnapshot) {
      homerunSnapshot.isOut = isOut;
      homerunSnapshots.push(homerunSnapshot);
    }

    const distribution = computeMlbProjectionDistribution(
      sanitizeProjection(ourProj ?? linestarProj ?? p.avgFptsDk ?? null),
      pitcherFlag,
      expectedHr,
      hrProb1Plus,
    );

    insertValues.push({
      slateId, dkPlayerId: p.dkId, name: p.name,
      teamAbbrev: p.teamAbbrev, teamId: null, mlbTeamId, matchupId,
      eligiblePositions: p.eligiblePositions, salary: p.salary,
      gameInfo: p.gameInfo, avgFptsDk: sanitizeProjection(p.avgFptsDk),
      linestarProj, linestarOwnPct, projOwnPct,
      dkInStartingLineup: p.inStartingLineup,
      dkStartingLineupOrder,
      dkTeamLineupConfirmed,
      teamImplied: matchup ? (isHome ? matchup.homeImplied : matchup.awayImplied) : null,
      oppImplied: matchup ? (isHome ? matchup.awayImplied : matchup.homeImplied) : null,
      teamMl: matchup ? (isHome ? matchup.homeMl : matchup.awayMl) : null,
      vegasTotal: matchup?.vegasTotal ?? null,
      isHome,
      expectedHr,
      hrProb1Plus,
      projFloor: distribution.projFloor,
      projCeiling: distribution.projCeiling,
      boomRate: distribution.boomRate,
      ourProj, ourLeverage: null as number | null, ourOwnPct: null as number | null, isOut,
    });
  }

  applyMlbOwnershipModels(insertValues as Array<MlbOwnershipPlayerLike>);

  const ownershipSnapshots = insertValues.map((player) => ({
    slateId,
    dkPlayerId: Number(player.dkPlayerId),
    name: String(player.name),
    teamId: (player.mlbTeamId as number | null) ?? null,
    salary: Number(player.salary ?? 0),
    eligiblePositions: (player.eligiblePositions as string | null) ?? null,
    isOut: Boolean(player.isOut),
    linestarProjFpts: sanitizeProjection((player.linestarProj as number | null | undefined) ?? null),
    ourProjFpts: sanitizeProjection((player.ourProj as number | null | undefined) ?? null),
    liveProjFpts: null,
    linestarOwnPct: sanitizeOwnershipPct((player.linestarOwnPct as number | null | undefined) ?? null),
    fieldOwnPct: sanitizeOwnershipPct((player.projOwnPct as number | null | undefined) ?? null),
    ourOwnPct: sanitizeOwnershipPct((player.ourOwnPct as number | null | undefined) ?? null),
    liveOwnPct: null,
    actualOwnPct: null,
    lineupOrder: (player.dkStartingLineupOrder as number | null | undefined) ?? null,
    lineupConfirmed: (player.dkTeamLineupConfirmed as boolean | null | undefined) ?? null,
  }));

  const blowupCandidates = buildMlbBlowupCandidates(
    insertValues.map((player) => ({
      dkPlayerId: Number(player.dkPlayerId),
      name: String(player.name),
      teamId: (player.mlbTeamId as number | null) ?? null,
      teamAbbrev: (player.teamAbbrev as string | null) ?? null,
      eligiblePositions: (player.eligiblePositions as string | null) ?? null,
      salary: Number(player.salary ?? 0),
      isOut: Boolean(player.isOut),
      ourProj: sanitizeProjection((player.ourProj as number | null | undefined) ?? null),
      liveProj: null,
      blendProj: null,
      projCeiling: sanitizeProjection((player.projCeiling as number | null | undefined) ?? null),
      expectedHr: finiteOrNull((player.expectedHr as number | null | undefined) ?? null),
      hrProb1Plus: finiteOrNull((player.hrProb1Plus as number | null | undefined) ?? null),
      projOwnPct: sanitizeOwnershipPct((player.projOwnPct as number | null | undefined) ?? null),
      ourOwnPct: sanitizeOwnershipPct((player.ourOwnPct as number | null | undefined) ?? null),
      teamTotal: finiteOrNull((player.teamImplied as number | null | undefined) ?? null),
      lineupOrder: (player.dkStartingLineupOrder as number | null | undefined) ?? null,
    })),
    12,
  );
  const blowupSnapshots = blowupCandidates.map((candidate, index) => ({
    slateId,
    dkPlayerId: candidate.player.dkPlayerId,
    name: candidate.player.name,
    teamId: candidate.player.teamId,
    teamAbbrev: candidate.player.teamAbbrev,
    salary: candidate.player.salary,
    eligiblePositions: candidate.player.eligiblePositions,
    lineupOrder: candidate.player.lineupOrder,
    teamTotal: Math.round(candidate.teamTotal * 100) / 100,
    projectedFpts: Math.round(candidate.proj * 100) / 100,
    projectedCeiling: Math.round(candidate.ceiling * 100) / 100,
    projectedValue: Math.round(candidate.value * 100) / 100,
    blowupScore: Math.round(candidate.blowupScore * 100) / 100,
    candidateRank: index + 1,
    actualFpts: null,
    actualOwnPct: null,
  }));

  const dbInsertValues = insertValues.map(({ teamImplied, oppImplied, teamMl, vegasTotal, isHome, ...row }) => row);

  for (let i = 0; i < dbInsertValues.length; i += 50) {
    const batch = dbInsertValues.slice(i, i + 50);
    await db.insert(dkPlayers).values(batch as typeof dkPlayers.$inferInsert[]).onConflictDoUpdate({
      target: [dkPlayers.slateId, dkPlayers.dkPlayerId],
      set: {
        salary: sql`EXCLUDED.salary`, mlbTeamId: sql`EXCLUDED.mlb_team_id`,
        matchupId: sql`EXCLUDED.matchup_id`,
        linestarProj: sql`EXCLUDED.linestar_proj`, linestarOwnPct: sql`EXCLUDED.linestar_own_pct`, projOwnPct: sql`EXCLUDED.proj_own_pct`,
        ourProj: sql`EXCLUDED.our_proj`,
        expectedHr: sql`EXCLUDED.expected_hr`,
        hrProb1Plus: sql`EXCLUDED.hr_prob_1plus`,
        projFloor: sql`EXCLUDED.proj_floor`,
        projCeiling: sql`EXCLUDED.proj_ceiling`,
        boomRate: sql`EXCLUDED.boom_rate`,
        ourLeverage: sql`EXCLUDED.our_leverage`,
        ourOwnPct: sql`EXCLUDED.our_own_pct`,
        dkInStartingLineup: sql`EXCLUDED.dk_in_starting_lineup`,
        dkStartingLineupOrder: sql`EXCLUDED.dk_starting_lineup_order`,
        dkTeamLineupConfirmed: sql`EXCLUDED.dk_team_lineup_confirmed`,
        isOut: sql`EXCLUDED.is_out`, avgFptsDk: sql`EXCLUDED.avg_fpts_dk`,
        eligiblePositions: sql`EXCLUDED.eligible_positions`, gameInfo: sql`EXCLUDED.game_info`,
      },
    });
  }

  const ownershipRunId = await createOwnershipRun(slateId, "mlb", "load_slate", MLB_OWNERSHIP_MODEL_VERSION, {
    version: MLB_OWNERSHIP_MODEL_VERSION,
    source: "load_slate",
    playerCount: ownershipSnapshots.length,
    matchedLinestar: lsMatched,
  });
  await recordOwnershipSnapshots(ownershipRunId, ownershipSnapshots);
  const blowupRunId = await createMlbBlowupRun(slateId, "load_slate", MLB_BLOWUP_CANDIDATE_VERSION, {
    version: MLB_BLOWUP_CANDIDATE_VERSION,
    source: "load_slate",
    playerCount: insertValues.length,
    candidateCount: blowupSnapshots.length,
  });
  await recordMlbBlowupSnapshots(blowupRunId, blowupSnapshots);
  const homerunRunId = await createMlbHomerunRun(slateId, "load_slate", MLB_HOMERUN_MODEL_VERSION, {
    version: MLB_HOMERUN_MODEL_VERSION,
    source: "load_slate",
    playerCount: insertValues.length,
    snapshotCount: homerunSnapshots.length,
  });
  await recordMlbHomerunSnapshots(homerunRunId, homerunSnapshots);
  try { await syncMlbHomerunSnapshotActualsForSlate(slateId); } catch { /* non-fatal */ }

  revalidatePath("/dfs");
  revalidatePath("/homerun");
  const matchRate = lsMap.size > 0 ? Math.round((lsMatched / dkPlayers_.length) * 100) : null;
  return {
    ok: true,
    message: `Saved ${insertValues.length} MLB players (${projComputed} with our proj, H ${batterCoverage.currentOnly}/${batterCoverage.blended}/${batterCoverage.priorOnly}, P ${pitcherCoverage.currentOnly}/${pitcherCoverage.blended}/${pitcherCoverage.priorOnly}, movers ${batterCoverage.teamChangeAccelerated + pitcherCoverage.teamChangeAccelerated})${matchRate != null ? `, LineStar ${matchRate}% matched` : ""}`,
    playerCount: insertValues.length,
    matchRate: matchRate ?? undefined,
  };
}

/** Fetch LineStar for MLB (sport=2). Same probe logic as NBA but different sport param. */
async function tryFetchLinestarMapMlb(draftGroupId: number): Promise<Map<string, LinestarEntry>> {
  const raw = process.env.DNN_COOKIE;
  if (!raw) return new Map();
  const cookie = normalizeDnnCookie(raw);
  try {
    // MLB period discovery: same endpoint but sport=2 (NBA uses sport=5)
    const resp = await fetch(`${LS_BASE}/GetPeriodInformation?site=1&sport=2`, {
      headers: { ...LS_HEADERS, Cookie: `.DOTNETNUKE=${cookie}` },
      next: { revalidate: 0 },
    });
    if (!resp.ok) return new Map();
    const periods = await resp.json() as Array<{ PeriodId?: number; Id?: number }>;
    const list = Array.isArray(periods) ? periods : (periods as { Periods?: typeof periods }).Periods ?? [];
    for (const period of list.slice(0, 10)) {
      const pid = period.PeriodId ?? period.Id;
      if (!pid) continue;
      const probe = await fetch(`${LS_BASE}/GetSalariesV5?periodId=${pid}&site=1&sport=2`, {
        headers: { ...LS_HEADERS, Cookie: `.DOTNETNUKE=${cookie}` },
        next: { revalidate: 0 },
      }).then((r) => r.ok ? r.json() : null).catch(() => null);
      if (!probe) continue;
      const slates = (probe as { Slates?: Array<{ DfsSlateId?: number }> }).Slates ?? [];
      if (slates.some((s) => s.DfsSlateId === draftGroupId)) {
        return parseLinestarApiResponse(probe);
      }
    }
  } catch { /* best-effort */ }
  return new Map();
}

export async function loadMlbSlateFromContestId(
  contestId: string,
  cashLine?: number,
  contestType?: string,
  fieldSize?: number,
  contestFormat?: string,
): Promise<{ ok: boolean; message: string; playerCount?: number }> {
  try {
    const dgId    = await resolveDraftGroupId(parseInt(contestId, 10));
    const players = await fetchDkPlayersFromApi(dgId);
    if (players.length === 0) return { ok: false, message: "No players returned from DK API" };
    const slateDate = players.map((player) => parseSlateDate(player.gameInfo)).find((value): value is string => !!value)
      ?? new Date().toISOString().slice(0, 10);
    const mlbSeason = inferMlbSeason(slateDate);
    const statsReady = await validateMlbStatsReadiness(mlbSeason);
    if (!statsReady.ok) {
      return { ok: false, message: statsReady.message ?? `MLB stats are not ready for season ${mlbSeason}` };
    }
    const lsMap = await tryFetchLinestarMapMlb(dgId);
    const result = await enrichAndSaveMlb(players, lsMap, cashLine, dgId, contestType, fieldSize, contestFormat);
    const readinessNote = statsReady.message ? `${statsReady.message} ` : "";
    return { ...result, message: `[MLB API] ${readinessNote}${result.message}`.trim() };
  } catch (e) {
    return { ok: false, message: String(e) };
  }
}

export async function loadMlbSlateFromDraftGroupId(
  draftGroupId: number | string,
  cashLine?: number,
  contestType?: string,
  fieldSize?: number,
  contestFormat?: string,
): Promise<{ ok: boolean; message: string; playerCount?: number }> {
  try {
    const dgId = parsePositiveInt(draftGroupId);
    if (!dgId) return { ok: false, message: "Enter a valid DK draft group ID" };
    const players = await fetchDkPlayersFromApi(dgId);
    if (players.length === 0) return { ok: false, message: "No players returned from DK API" };
    const slateDate = players.map((player) => parseSlateDate(player.gameInfo)).find((value): value is string => !!value)
      ?? new Date().toISOString().slice(0, 10);
    const mlbSeason = inferMlbSeason(slateDate);
    const statsReady = await validateMlbStatsReadiness(mlbSeason);
    if (!statsReady.ok) {
      return { ok: false, message: statsReady.message ?? `MLB stats are not ready for season ${mlbSeason}` };
    }
    const lsMap = await tryFetchLinestarMapMlb(dgId);
    const result = await enrichAndSaveMlb(players, lsMap, cashLine, dgId, contestType, fieldSize, contestFormat);
    const readinessNote = statsReady.message ? `${statsReady.message} ` : "";
    revalidatePath("/homerun");
    return { ...result, message: `[MLB API] ${readinessNote}${result.message}`.trim() };
  } catch (e) {
    return { ok: false, message: String(e) };
  }
}

/**
 * Re-fetch the DK draftables API for the current slate and update is_out for
 * every player whose injury status has changed since the slate was loaded.
 *
 * Requires dk_draft_group_id to be saved on dk_slates (set when loading via
 * Contest ID). Players no longer present in the API response are marked OUT
 * (DK removes confirmed scratches from the draftable list before lock).
 */
export async function refreshPlayerStatus(slateId: number): Promise<{
  ok: boolean; message: string; updated: number;
}> {
  try {
    await ensureDkPlayerPropColumns();
    // Get the draft group ID saved when the slate was loaded
    const slateRows = await db
      .select({ dkDraftGroupId: dkSlates.dkDraftGroupId, sport: dkSlates.sport })
      .from(dkSlates)
      .where(eq(dkSlates.id, slateId));
    const dgId = slateRows[0]?.dkDraftGroupId;
    const sport = slateRows[0]?.sport ?? "nba";
    if (!dgId) {
      return {
        ok: false,
        message: "No draft group ID saved for this slate — reload via Contest ID first",
        updated: 0,
      };
    }

    const livePlayers = await fetchDkPlayersFromApi(dgId);
    const lineupConfirmedByTeam = sport === "mlb" ? inferMlbTeamLineupConfirmed(livePlayers) : new Map<string, boolean>();
    const liveStatus = new Map<number, {
      isOut: boolean;
      dkInStartingLineup: boolean | null;
      dkStartingLineupOrder: number | null;
      dkTeamLineupConfirmed: boolean | null;
    }>();
    for (const player of livePlayers) {
      const dkTeamLineupConfirmed = sport === "mlb"
        ? (lineupConfirmedByTeam.get((player.teamAbbrev ?? "").toUpperCase()) ?? false)
        : null;
      const dkStartingLineupOrder = sport === "mlb" && isPositiveMlbLineupOrder(player.startingLineupOrder)
        ? player.startingLineupOrder
        : null;
      const confirmedBatterOut = sport === "mlb"
        && !isPitcherPos(player.eligiblePositions)
        && dkTeamLineupConfirmed === true
        && dkStartingLineupOrder == null
        && player.inStartingLineup !== true;
      const isOut =
        player.isDisabled
        || ["O", "OUT"].includes(player.dkStatus.toUpperCase())
        || !isLikelyActiveMlbPitcher(player)
        || confirmedBatterOut;
      liveStatus.set(player.dkId, {
        isOut,
        dkInStartingLineup: sport === "mlb" ? player.inStartingLineup : null,
        dkStartingLineupOrder,
        dkTeamLineupConfirmed,
      });
    }

    // Compare against stored players
    const stored = await db
      .select({
        id: dkPlayers.id,
        dkPlayerId: dkPlayers.dkPlayerId,
        isOut: dkPlayers.isOut,
        dkInStartingLineup: dkPlayers.dkInStartingLineup,
        dkStartingLineupOrder: dkPlayers.dkStartingLineupOrder,
        dkTeamLineupConfirmed: dkPlayers.dkTeamLineupConfirmed,
      })
      .from(dkPlayers)
      .where(eq(dkPlayers.slateId, slateId));

    let updated = 0;
    for (const p of stored) {
      const live = liveStatus.get(p.dkPlayerId);
      const next = live ?? {
        isOut: true,
        dkInStartingLineup: null,
        dkStartingLineupOrder: null,
        dkTeamLineupConfirmed: sport === "mlb" ? false : null,
      };
      if (
        next.isOut !== (p.isOut ?? false)
        || next.dkInStartingLineup !== (p.dkInStartingLineup ?? null)
        || next.dkStartingLineupOrder !== (p.dkStartingLineupOrder ?? null)
        || next.dkTeamLineupConfirmed !== (p.dkTeamLineupConfirmed ?? null)
      ) {
        await db.update(dkPlayers).set({
          isOut: next.isOut,
          dkInStartingLineup: next.dkInStartingLineup,
          dkStartingLineupOrder: next.dkStartingLineupOrder,
          dkTeamLineupConfirmed: next.dkTeamLineupConfirmed,
        }).where(eq(dkPlayers.id, p.id));
        updated++;
      }
    }

    revalidatePath("/dfs");
    return {
      ok: true,
      message: updated > 0
        ? `${updated} player status update${updated > 1 ? "s" : ""} applied`
        : "All player statuses are current — no changes",
      updated,
    };
  } catch (e) {
    return { ok: false, message: String(e), updated: 0 };
  }
}

export async function runOptimizer(
  slateId: number,
  gameFilter: number[],
  settings: OptimizerSettings,
): Promise<OptimizerRunResult<GeneratedLineup>> {
  const refreshResult = await refreshPlayerStatus(slateId);
  if (!refreshResult.ok) {
    return { ok: false, error: `Status refresh failed before optimize: ${refreshResult.message}` };
  }

  const rows = await db.execute<OptimizerPlayer & { slateId: number }>(sql`
    SELECT
      dp.id, dp.dk_player_id AS "dkPlayerId", dp.name, dp.team_abbrev AS "teamAbbrev",
      dp.team_id AS "teamId", dp.matchup_id AS "matchupId",
      dp.eligible_positions AS "eligiblePositions", dp.salary,
      COALESCE(dp.live_proj, dp.our_proj, dp.linestar_proj) AS "ourProj",
      COALESCE(dp.live_leverage, dp.our_leverage) AS "ourLeverage",
      dp.linestar_proj AS "linestarProj",
      COALESCE(dp.live_own_pct, dp.proj_own_pct, dp.our_own_pct) AS "projOwnPct",
      dp.proj_ceiling AS "projCeiling",
      dp.boom_rate AS "boomRate",
      dp.prop_pts AS "propPts",
      dp.is_out AS "isOut", dp.game_info AS "gameInfo",
      t.logo_url AS "teamLogo", t.name AS "teamName",
      m.home_team_id AS "homeTeamId"
    FROM dk_players dp
    LEFT JOIN teams t ON t.team_id = dp.team_id
    LEFT JOIN nba_matchups m ON m.id = dp.matchup_id
    WHERE dp.slate_id = ${slateId}
  `);

  const pool: OptimizerPlayer[] = rows.rows
    .map((p) => ({
      ...p,
      ourProj: sanitizeProjection(p.ourProj ?? p.linestarProj ?? null),
      ourLeverage: sanitizeLeverage(p.ourLeverage),
      linestarProj: sanitizeProjection(p.linestarProj),
      projOwnPct: sanitizeOwnershipPct(p.projOwnPct),
    }))
    .filter((p) => gameFilter.length === 0 || (p.matchupId != null && gameFilter.includes(p.matchupId)));

  try {
    const { lineups, debug } = optimizeLineupsWithDebug(pool, settings);
    if (lineups.length === 0) {
      const eligible = pool.filter((p) => {
        if (p.isOut) return false;
        return p.ourProj != null && p.ourProj > 0 && p.salary > 0;
      });
      const guards   = eligible.filter((p) => p.eligiblePositions.includes("PG") || p.eligiblePositions.includes("SG")).length;
      const forwards = eligible.filter((p) => p.eligiblePositions.includes("SF") || p.eligiblePositions.includes("PF")).length;
      const centers  = eligible.filter((p) => p.eligiblePositions.includes("C")).length;
      const withMatchup = eligible.filter((p) => p.matchupId != null).length;
      const withLeverage = isTournamentMode(settings.mode)
        ? eligible.filter((p) => p.ourLeverage != null).length
        : eligible.length;
      const hint = eligible.length < 15
        ? " Pool too small — click Refresh Player Status to reset OUT flags, then re-paste LineStar data."
        : guards < 3
          ? " Not enough guards (need ≥3 PG/SG)."
          : forwards < 3
            ? " Not enough forwards (need ≥3 SF/PF)."
            : centers < 1
              ? " No centers in pool."
              : withMatchup < 8
                ? " Most players missing matchup data — reload slate via Contest ID."
                : withLeverage === 0 && isTournamentMode(settings.mode)
                  ? " No leverage scores — paste LineStar data then re-run Fetch Projections, or switch to Cash mode."
                  : " Try reducing lineup count or switching to Cash mode.";
      const diagLines = probeOptimizerAll(pool, settings);
      return {
        ok: false,
        error: `No lineups — ${eligible.length} eligible: ${guards}G / ${forwards}F / ${centers}C` +
          `, ${withMatchup}/${eligible.length} with matchup data.${hint}\n` +
          diagLines.join(" | "),
        debug,
      };
    }
    let warning: string | undefined;
    if (lineups.length < settings.nLineups && settings.maxExposure < 1) {
      const relaxedCount = optimizeLineups(pool, { ...settings, maxExposure: 1 }).length;
      warning = buildPartialGenerationWarning(
        lineups,
        settings.nLineups,
        settings.maxExposure,
        relaxedCount > lineups.length,
      );
    } else if (lineups.length < settings.nLineups) {
      warning = buildPartialGenerationWarning(lineups, settings.nLineups, settings.maxExposure, false);
    }
    return { ok: true, lineups, warning, debug };
  } catch (e) {
    return { ok: false, error: String(e) };
  }
}

export async function saveLineups(
  slateId: number,
  lineups: LineupForSave[],
  strategy: string,
): Promise<{ ok: boolean; saved: number }> {
  let saved = 0;
  for (let i = 0; i < lineups.length; i++) {
    const l = lineups[i];
    const playerIds = l.players.map((p) => p.id).join(",");
    const stackTeam = (() => {
      const counts = new Map<string, number>();
      for (const p of l.players) counts.set(p.teamAbbrev, (counts.get(p.teamAbbrev) ?? 0) + 1);
      let best = "", bestCount = 0;
      for (const [team, count] of counts) { if (count > bestCount) { bestCount = count; best = team; } }
      return bestCount >= 2 ? best : null;
    })();
    await db
      .insert(dkLineups)
      .values({
        slateId,
        strategy,
        lineupNum:   i + 1,
        playerIds,
        totalSalary: l.totalSalary,
        projFpts:    l.projFpts,
        leverage:    l.leverageScore,
        stackTeam,
      })
      .onConflictDoUpdate({
        target: [dkLineups.slateId, dkLineups.strategy, dkLineups.lineupNum],
        set: {
          playerIds:   sql`EXCLUDED.player_ids`,
          totalSalary: sql`EXCLUDED.total_salary`,
          projFpts:    sql`EXCLUDED.proj_fpts`,
          leverage:    sql`EXCLUDED.leverage`,
          stackTeam:   sql`EXCLUDED.stack_team`,
        },
      });
    saved++;
  }
  await db.delete(dkLineups).where(and(
    eq(dkLineups.slateId, slateId),
    eq(dkLineups.strategy, strategy),
    sql`${dkLineups.lineupNum} > ${lineups.length}`,
  ));
  revalidatePath("/dfs");
  return { ok: true, saved };
}

export async function exportLineups(
  lineups: GeneratedLineup[],
): Promise<CsvExportResult> {
  try {
    return { ok: true, csv: buildMultiEntryCSV(lineups) };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : String(e) };
  }
}

// ── MLB Optimizer ─────────────────────────────────────────────

export async function runMlbOptimizer(
  slateId: number,
  gameFilter: number[],
  settings: MlbOptimizerSettings,
): Promise<OptimizerRunResult<MlbGeneratedLineup>> {
  await ensureDkPlayerPropColumns();
  const refreshResult = await refreshPlayerStatus(slateId);
  if (!refreshResult.ok) {
    return { ok: false, error: `MLB status refresh failed before optimize: ${refreshResult.message}` };
  }
  const hitterProjectionCalibration = await loadMlbHitterProjectionCalibration();

  const rows = await db.execute<MlbOptimizerPlayer & { avgFptsDk: number | null }>(sql`
    SELECT
      dp.id, dp.dk_player_id AS "dkPlayerId", dp.name, dp.team_abbrev AS "teamAbbrev",
      dp.mlb_team_id AS "teamId", dp.matchup_id AS "matchupId",
      dp.eligible_positions AS "eligiblePositions", dp.salary,
      dp.our_proj AS "ourProj", dp.our_leverage AS "ourLeverage",
      dp.linestar_proj AS "linestarProj", dp.proj_own_pct AS "projOwnPct", dp.avg_fpts_dk AS "avgFptsDk",
      dp.proj_ceiling AS "projCeiling", dp.boom_rate AS "boomRate",
      dp.expected_hr AS "expectedHr",
      dp.dk_in_starting_lineup AS "dkInStartingLineup",
      dp.dk_starting_lineup_order AS "dkStartingLineupOrder",
      dp.dk_team_lineup_confirmed AS "dkTeamLineupConfirmed",
      dp.is_out AS "isOut", dp.game_info AS "gameInfo",
      mt.logo_url AS "teamLogo", mt.name AS "teamName",
      mm.home_team_id AS "homeTeamId", mm.away_team_id AS "awayTeamId",
      mm.vegas_total AS "vegasTotal", mm.home_implied AS "homeImplied", mm.away_implied AS "awayImplied",
      dp.hr_prob_1plus AS "hrProb1Plus", dp.prop_pts AS "propPts", dp.prop_reb AS "propReb", dp.prop_ast AS "propAst",
      dp.prop_stl AS "propStl", dp.prop_stl_price AS "propStlPrice", dp.prop_stl_book AS "propStlBook"
    FROM dk_players dp
    LEFT JOIN mlb_teams mt ON mt.team_id = dp.mlb_team_id
    LEFT JOIN mlb_matchups mm ON mm.id = dp.matchup_id
    WHERE dp.slate_id = ${slateId}
  `);

  const pool: MlbOptimizerPlayer[] = rows.rows
    .map((p) => {
      const linestarProj = sanitizeProjection(p.linestarProj);
      const projOwnPct = sanitizeOwnershipPct(p.projOwnPct);
      const calibratedProj = isPitcherPos(p.eligiblePositions)
        ? sanitizeProjection(p.ourProj ?? linestarProj ?? null)
        : applyMlbHitterProjectionCalibration(
            sanitizeProjection(p.ourProj ?? linestarProj ?? null),
            p.dkTeamLineupConfirmed ? p.dkStartingLineupOrder ?? null : null,
            p.dkTeamLineupConfirmed ?? null,
            hitterProjectionCalibration,
          );
      const fieldProj = sanitizeProjection(p.avgFptsDk ?? linestarProj ?? null);
      const calibratedLeverage = !p.isOut && calibratedProj != null && projOwnPct != null
        ? sanitizeLeverage(computeLeverage(calibratedProj, projOwnPct, fieldProj))
        : sanitizeLeverage(p.ourLeverage);
      return {
        ...p,
        ourProj: calibratedProj,
        ourLeverage: calibratedLeverage,
        linestarProj,
        projOwnPct,
        expectedHr: sanitizeProjection(p.expectedHr),
        hrProb1Plus: sanitizeProbability(p.hrProb1Plus),
        propStl: finiteOrNull(p.propStl),
        propStlPrice: p.propStlPrice == null ? null : Math.round(Number(p.propStlPrice)),
        propStlBook: p.propStlBook ?? null,
      };
    })
    .filter((p) => gameFilter.length === 0 || (p.matchupId != null && gameFilter.includes(p.matchupId)));
  const policyAdjustedPool = applyMlbPendingLineupPolicy(pool, settings.pendingLineupPolicy);
  const ruleValidation = validateMlbRuleSelections(policyAdjustedPool, settings);
  if (!ruleValidation.ok) {
    return { ok: false, error: ruleValidation.error };
  }
  try {
    const { lineups, debug } = optimizeMlbLineupsWithDebug(pool, settings);
    if (lineups.length === 0) {
      const eligible = policyAdjustedPool.filter(
        (p) => !p.isOut && p.ourProj != null && p.ourProj > 0 && p.salary > 0,
      );
      const pitchers = eligible.filter((p) =>
        p.eligiblePositions.includes("SP") || p.eligiblePositions.includes("RP"),
      ).length;
      const catchers = eligible.filter(
        (p) => p.eligiblePositions.includes("C") && !p.eligiblePositions.includes("SP"),
      ).length;
      const hint = eligible.length < 20
        ? " Pool too small — upload the DK CSV first."
        : pitchers < 2
          ? " Not enough pitchers (need ≥2 SP/RP)."
          : catchers < 1
            ? " No catchers in pool."
            : " Try reducing lineup count or switching to Cash mode.";
      return {
        ok: false,
        error: `No lineups — ${eligible.length} eligible: ${pitchers} P / ${catchers} C.${hint}`,
        debug,
      };
    }
    let warning: string | undefined;
    if (lineups.length < settings.nLineups && settings.maxExposure < 1) {
      const relaxedCount = optimizeMlbLineups(pool, { ...settings, maxExposure: 1 }).length;
      warning = buildPartialGenerationWarning(
        lineups,
        settings.nLineups,
        settings.maxExposure,
        relaxedCount > lineups.length,
      );
    } else if (lineups.length < settings.nLineups) {
      warning = buildPartialGenerationWarning(lineups, settings.nLineups, settings.maxExposure, false);
    }
    return { ok: true, lineups, warning, debug };
  } catch (e) {
    return { ok: false, error: String(e) };
  }
}

export async function exportMlbLineups(
  lineups: MlbGeneratedLineup[],
): Promise<CsvExportResult> {
  try {
    return { ok: true, csv: buildMlbMultiEntryCSV(lineups) };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : String(e) };
  }
}

// ── Results Upload (Phase 3) ──────────────────────────────────

type ResultPlayer = { name: string; actualFpts: number; actualOwnPct?: number };

/** Split a CSV line respecting double-quoted fields. */
function splitCsvLine(line: string): string[] {
  const cells: string[] = [];
  let cur = "";
  let inQuote = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"') {
      if (inQuote && line[i + 1] === '"') { cur += '"'; i++; }
      else inQuote = !inQuote;
    } else if (ch === "," && !inQuote) {
      cells.push(cur.trim());
      cur = "";
    } else {
      cur += ch;
    }
  }
  cells.push(cur.trim());
  return cells;
}

function parseStandingsCsv(content: string): ResultPlayer[] {
  // DK contest standings: positional columns
  // Common format: Rank,EntryId,EntryName,TimeRemaining,Points,Lineup,TeamName,Player,Roster Position,%Drafted,FPTS
  // Col indices:   0     1       2         3             4      5      6         7      8               9        10
  const lines = content.split(/\r?\n/).filter(Boolean).slice(1); // skip header
  const seen = new Map<string, ResultPlayer>();
  for (const line of lines) {
    const cells = splitCsvLine(line);
    if (cells.length < 10) continue;
    // Try col 7 first (format with TeamName col); fall back to col 6 (format without TeamName)
    const hasFptsAt10 = cells.length >= 11 && !isNaN(parseFloat(cells[10] ?? ""));
    const playerCol  = hasFptsAt10 ? 7 : 6;
    const ownCol     = hasFptsAt10 ? 9 : 8;
    const fptsCol    = hasFptsAt10 ? 10 : 9;
    const name    = cells[playerCol] ?? "";
    const ownStr  = (cells[ownCol] ?? "").replace("%", "");
    const fptsStr = cells[fptsCol] ?? "";
    if (!name) continue;
    const actualFpts    = parseFloat(fptsStr);
    const actualOwnPct  = parseFloat(ownStr);
    if (isNaN(actualFpts)) continue;
    if (!seen.has(name)) {
      seen.set(name, { name, actualFpts, actualOwnPct: isNaN(actualOwnPct) ? undefined : actualOwnPct });
    }
  }
  return Array.from(seen.values());
}

function parseResultsCsv(content: string): ResultPlayer[] {
  // DK results CSV: named columns — Name, Salary, FPTS (or Total Points / ActualFpts)
  const lines = content.split(/\r?\n/).filter(Boolean);
  if (lines.length < 2) return [];
  const header = lines[0].split(",").map((h) => h.trim().replace(/^"|"$/g, ""));
  const col = (name: string) => header.findIndex((h) => h === name);

  const nameCol  = col("Name");
  const fptsCol  = [col("FPTS"), col("Total Points"), col("ActualFpts"), col("Actual FPTS")]
    .find((c) => c !== -1) ?? -1;

  if (nameCol === -1 || fptsCol === -1) return [];

  const players: ResultPlayer[] = [];
  for (let i = 1; i < lines.length; i++) {
    const cells = lines[i].split(",").map((c) => c.trim().replace(/^"|"$/g, ""));
    const name = cells[nameCol] ?? "";
    if (!name) continue;
    const actualFpts = parseFloat(cells[fptsCol] ?? "");
    if (!isNaN(actualFpts)) {
      players.push({ name, actualFpts });
    }
  }
  return players;
}

export async function uploadResults(formData: FormData): Promise<{
  ok: boolean;
  message: string;
  updated?: number;
  total?: number;
  matchRate?: number;
}> {
  try {
  const file = formData.get("resultsFile") as File | null;
  if (!file) return { ok: false, message: "Results CSV required" };

  const content = await file.text();
  const firstLine = content.split("\n")[0] ?? "";
  const isStandings = firstLine.includes("EntryId") || firstLine.includes("%Drafted");

  const resultPlayers = isStandings ? parseStandingsCsv(content) : parseResultsCsv(content);

  if (resultPlayers.length === 0) {
    return { ok: false, message: "No players with FPTS found in the file" };
  }

  // Most recent slate
  const [slate] = await db
    .select({ id: dkSlates.id, slateDate: dkSlates.slateDate, sport: dkSlates.sport })
    .from(dkSlates)
    .orderBy(desc(dkSlates.slateDate))
    .limit(1);

  if (!slate) return { ok: false, message: "No slate found — load a slate first" };

  const pool = await db
    .select({ id: dkPlayers.id, name: dkPlayers.name })
    .from(dkPlayers)
    .where(eq(dkPlayers.slateId, slate.id));

  if (pool.length === 0) {
    return { ok: false, message: `No players in slate ${slate.slateDate}` };
  }

  // Match + update
  let updated = 0;
  for (const rp of resultPlayers) {
    let match = pool.find((p) => p.name === rp.name);
    if (!match) {
      let bestDist = 4;
      for (const p of pool) {
        const d = levenshtein(rp.name.toLowerCase(), p.name.toLowerCase());
        if (d < bestDist) { bestDist = d; match = p; }
      }
    }
    if (match) {
      await db
        .update(dkPlayers)
        .set({ actualFpts: rp.actualFpts, actualOwnPct: rp.actualOwnPct ?? null })
        .where(eq(dkPlayers.id, match.id));
      updated++;
    }
  }

  // Roll up lineup actuals
  const lineupRows = await db
    .select({ id: dkLineups.id, playerIds: dkLineups.playerIds })
    .from(dkLineups)
    .where(eq(dkLineups.slateId, slate.id));

  let lineupsUpdated = 0;
  for (const lineup of lineupRows) {
    const ids = (lineup.playerIds ?? "")
      .split(",")
      .map((s) => parseInt(s.trim(), 10))
      .filter((n) => !isNaN(n));
    if (ids.length === 0) continue;

    const [row] = await db
      .select({
        total: sql<number | null>`SUM(${dkPlayers.actualFpts})`,
        actualCount: sql<number>`COUNT(*) FILTER (WHERE ${dkPlayers.actualFpts} IS NOT NULL)::int`,
      })
      .from(dkPlayers)
      .where(inArray(dkPlayers.id, ids));

    if (row && row.actualCount === ids.length && row.total != null) {
      await db.update(dkLineups).set({ actualFpts: row.total }).where(eq(dkLineups.id, lineup.id));
      lineupsUpdated++;
    } else {
      await db.update(dkLineups).set({ actualFpts: null }).where(eq(dkLineups.id, lineup.id));
    }
  }

  const optimizerLineupActuals = await updateOptimizerJobLineupActualsForSlate(slate.id);

  try { await syncProjectionSnapshotActualsForSlate(slate.id); } catch { /* non-fatal */ }
  try { await syncOwnershipSnapshotActualsForSlate(slate.id); } catch { /* non-fatal */ }
  if (slate.sport === "mlb") {
    try { await syncMlbBlowupSnapshotActualsForSlate(slate.id); } catch { /* non-fatal */ }
  }
  let analysisNote = "";
  if (slate.sport === "nba") {
    try {
      await persistNbaOddsSignalReport(slate.id);
      analysisNote = ", odds signal refreshed";
    } catch {
      analysisNote = ", odds signal refresh skipped";
    }
  }

  revalidatePath("/dfs");
  revalidateTag(ANALYTICS_CACHE_TAG, {}); // bust cached analytics so next /analytics load is fresh

  const matchRate = Math.round((updated / resultPlayers.length) * 100);
  const lineupNote = `${lineupRows.length > 0 ? `, ${lineupsUpdated}/${lineupRows.length} lineup actuals updated` : ""}${optimizerLineupActuals.total > 0 ? `, ${optimizerLineupActuals.updated}/${optimizerLineupActuals.total} optimizer lineup actuals updated` : ""}${analysisNote}`;
  return {
    ok: true,
    message: `${updated}/${resultPlayers.length} players matched (${matchRate}%)${lineupNote} — slate ${slate.slateDate}`,
    updated,
    total: resultPlayers.length,
    matchRate,
  };
  } catch (err) {
    return { ok: false, message: `Upload failed: ${err instanceof Error ? err.message : String(err)}` };
  }
}

// ── Clear Slate ───────────────────────────────────────────────

export async function clearSlate(sport: Sport): Promise<{ ok: boolean; message: string }> {
  try {
    const [slate] = await db
      .select({ id: dkSlates.id, slateDate: dkSlates.slateDate })
      .from(dkSlates)
      .where(eq(dkSlates.sport, sport))
      .orderBy(desc(dkSlates.slateDate), desc(dkSlates.id))
      .limit(1);

    if (!slate) return { ok: false, message: "No slate found to clear" };

    // Delete in FK order — no cascade configured on child tables
    const deletedLineups = await db.delete(dkLineups).where(eq(dkLineups.slateId, slate.id)).returning({ id: dkLineups.id });
    const deletedPlayers = await db.delete(dkPlayers).where(eq(dkPlayers.slateId, slate.id)).returning({ id: dkPlayers.id });
    await db.delete(dkSlates).where(eq(dkSlates.id, slate.id));

    revalidatePath("/dfs");
    return {
      ok: true,
      message: `Cleared slate ${slate.slateDate}: ${deletedPlayers.length} players, ${deletedLineups.length} lineups deleted`,
    };
  } catch (e) {
    return { ok: false, message: `Clear slate failed: ${e instanceof Error ? e.message : String(e)}` };
  }
}

// ── Recompute Projections ─────────────────────────────────────
// Re-runs the NBA projection model against current Neon stats (nba_team_stats,
// nba_player_stats) for the already-loaded slate. No external API calls.
// Use after running refresh_nba_stats to apply updated stats to a loaded slate.

export async function recomputeProjections(): Promise<{ ok: boolean; message: string }> {
  try {
    await ensureDkPlayerPropColumns();

    // Pick the largest slate on the most recent date (gameCount DESC breaks ties
    // when multiple slates share a date, e.g. a 2-game test alongside a 6-game main)
    const [slate] = await db
      .select({ id: dkSlates.id, slateDate: dkSlates.slateDate })
      .from(dkSlates)
      .where(eq(dkSlates.sport, "nba"))
      .orderBy(desc(dkSlates.slateDate), desc(dkSlates.gameCount), desc(dkSlates.id))
      .limit(1);

    if (!slate) return { ok: false, message: "No NBA slate loaded" };

    const currentPlayers = await db.select().from(dkPlayers).where(eq(dkPlayers.slateId, slate.id));
    if (currentPlayers.length === 0) return { ok: false, message: "No players in current slate" };

    // Build abbrev → teamId map first (needed by ensureMatchupsForSlate)
    const teamRows = await db.select({ teamId: teams.teamId, abbreviation: teams.abbreviation }).from(teams);
    const abbrevToId = new Map(teamRows.map((t) => [t.abbreviation.toUpperCase(), t.teamId]));

    // Ensure nba_matchups has rows for today + real Vegas odds from The Odds API.
    // If rows are missing (e.g. load_slate ran without nba_schedule), this creates
    // them from gameInfo strings and fetches live odds before we query below.
    const _matchupDebug = await ensureMatchupsForSlate(slate.slateDate, currentPlayers, abbrevToId);

    // Query matchups — now guaranteed to exist (with real odds when ODDS_API_KEY set)
    const matchupRows = await db.select().from(nbaMatchups).where(eq(nbaMatchups.gameDate, slate.slateDate));
    const matchupById = new Map(matchupRows.map((m) => [m.id, m]));

    // Build teamId → matchup for players whose matchup_id was not stored
    const matchupByTeam = new Map<number, typeof matchupRows[0]>();
    for (const m of matchupRows) {
      if (m.homeTeamId) matchupByTeam.set(m.homeTeamId, m);
      if (m.awayTeamId) matchupByTeam.set(m.awayTeamId, m);
    }

    const teamStatRows = await db.select().from(nbaTeamStats).where(eq(nbaTeamStats.season, CURRENT_SEASON));
    const statsByTeam = new Map(teamStatRows.map((r) => [r.teamId, r]));

    const playerStatRows = await db.select().from(nbaPlayerStats).where(eq(nbaPlayerStats.season, CURRENT_SEASON));
    const playersByTeam = new Map<number, typeof playerStatRows>();
    for (const ps of playerStatRows) {
      if (ps.teamId == null) continue;
      const arr = playersByTeam.get(ps.teamId) ?? [];
      arr.push(ps);
      playersByTeam.set(ps.teamId, arr);
    }
    const oddsMovementContext = await buildNbaOddsMovementContext(slate.id, slate.slateDate);
    const projectionCalibration = await loadNbaProjectionCalibration();

    let projComputed = 0;
    const enriched: Array<{
      slateId: number; dkPlayerId: number; name: string; teamAbbrev: string;
      teamId: number | null; mlbTeamId: number | null; matchupId: number | null;
      eligiblePositions: string; salary: number; gameInfo: string | null;
      avgFptsDk: number | null; linestarProj: number | null; projOwnPct: number | null;
      isOut: boolean; ourProj: number | null; liveProj: number | null;
      ourLeverage: number | null; ourOwnPct: number | null;
      liveLeverage: number | null; liveOwnPct: number | null;
      _spg: number; _bpg: number;
    }> = [];
    const projectionSnapshots: Array<{
      slateId: number;
      dkPlayerId: number;
      name: string;
      teamId: number | null;
      salary: number;
      isOut: boolean;
      blend: NbaProjectionBlend;
      actualFpts?: number | null;
    }> = [];

    for (const p of currentPlayers) {
      let ourProj: number | null = null;
      let liveProj: number | null = sanitizeProjection(p.liveProj ?? p.linestarProj ?? null);
      let spgForLev = 0, bpgForLev = 0;
      let projectionBlend: NbaProjectionBlend = {
        modelProj: null,
        marketProj: null,
        lsProj: sanitizeProjection(p.linestarProj ?? null),
        finalProj: null,
        propCount: 0,
        modelConfidence: 0,
        marketConfidence: 0,
        lsConfidence: p.linestarProj != null ? 0.35 : 0,
        modelWeight: 0,
        marketWeight: 0,
        lsWeight: p.linestarProj != null ? 1 : 0,
        flags: ["no_model_match"],
        modelStats: null,
        marketStats: null,
      };

      // Resolve teamId — stored value may be null; fall back to abbreviation lookup
      const canonical = DK_OVERRIDES[p.teamAbbrev.toUpperCase()] ?? p.teamAbbrev.toUpperCase();
      const resolvedTeamId = p.teamId ?? abbrevToId.get(canonical) ?? null;

      // Resolve matchup — stored matchupId may be null; fall back to teamId map
      const matchup = (p.matchupId ? matchupById.get(p.matchupId) : null)
        ?? (resolvedTeamId ? matchupByTeam.get(resolvedTeamId) : null)
        ?? null;

      if (resolvedTeamId && matchup) {
        const teamStat = statsByTeam.get(resolvedTeamId);
        const oppId    = matchup.homeTeamId === resolvedTeamId ? matchup.awayTeamId : matchup.homeTeamId;
        const oppStat  = oppId ? statsByTeam.get(oppId) : null;

        const candidates = playersByTeam.get(resolvedTeamId) ?? [];
        let bestPlayer: typeof playerStatRows[0] | null = null;
        let bestDist = 4;
        for (const ps of candidates) {
          const d = levenshtein(p.name.toLowerCase(), ps.name.toLowerCase());
          if (d < bestDist) { bestDist = d; bestPlayer = ps; }
        }

        if (bestPlayer) {
          const isHome = matchup.homeTeamId === resolvedTeamId;
          const playerMovement = oddsMovementContext.playerByDkId.get(p.dkPlayerId);
          const matchupMovement = oddsMovementContext.matchupById.get(matchup.id);
          projectionBlend = buildNbaProjectionBlend(
            bestPlayer,
            teamStat?.pace   ?? LEAGUE_AVG_PACE,
            oppStat?.pace    ?? LEAGUE_AVG_PACE,
            oppStat?.defRtg  ?? LEAGUE_AVG_DEF_RTG,
            matchup.vegasTotal,
            matchup.homeMl,
            matchup.awayMl,
            isHome,
            sanitizeProjection(p.linestarProj ?? null),
            {
              propPts: p.propPts,
              propReb: p.propReb,
              propAst: p.propAst,
              propBlk: p.propBlk,
              propStl: p.propStl,
            },
            {
              propDeltas: playerMovement?.propDeltas,
              marketFptsDelta: playerMovement?.marketFptsDelta,
              vegasTotalDelta: matchupMovement?.vegasTotalDelta,
              homeSpreadDelta: matchupMovement?.homeSpreadDelta,
            },
          );
          ourProj = computeNbaInternalProjection(
            projectionBlend,
            p.eligiblePositions,
            bestPlayer.avgMinutes,
            projectionCalibration,
          );
          liveProj = computeNbaLiveProjection(projectionBlend);
          spgForLev = bestPlayer.spg ?? 0;
          bpgForLev = bestPlayer.bpg ?? 0;
          if (ourProj != null) projComputed++;
        }
      }

      const isOut = p.isOut ?? false;
      const linestarProj = sanitizeProjection(p.linestarProj ?? null);
      const projOwnPct = sanitizeOwnershipPct(p.projOwnPct ?? null);

      enriched.push({
        slateId: p.slateId, dkPlayerId: p.dkPlayerId, name: p.name,
        teamAbbrev: p.teamAbbrev, teamId: p.teamId ?? null, mlbTeamId: p.mlbTeamId ?? null,
        matchupId: p.matchupId ?? null, eligiblePositions: p.eligiblePositions,
        salary: p.salary, gameInfo: p.gameInfo ?? null,
        avgFptsDk: sanitizeProjection(p.avgFptsDk ?? null),
        linestarProj,
        projOwnPct, isOut,
        ourProj,
        liveProj,
        ourLeverage: null,
        ourOwnPct: null,
        liveLeverage: null,
        liveOwnPct: null,
        _spg: spgForLev, _bpg: bpgForLev,
      });
      projectionSnapshots.push({
        slateId: p.slateId,
        dkPlayerId: p.dkPlayerId,
        name: p.name,
        teamId: p.teamId ?? null,
        salary: p.salary,
        isOut,
        blend: projectionBlend,
        actualFpts: p.actualFpts ?? null,
      });
    }

    applyNbaOwnershipModels(enriched, oddsMovementContext);

    // Batch upsert — refresh model/live projection and ownership fields together
    for (let i = 0; i < enriched.length; i += 50) {
      const batch = enriched.slice(i, i + 50).map(({ _spg, _bpg, ...rest }) => rest);
      await db.insert(dkPlayers).values(batch).onConflictDoUpdate({
        target: [dkPlayers.slateId, dkPlayers.dkPlayerId],
        set: {
          ourProj: sql`EXCLUDED.our_proj`,
          liveProj: sql`EXCLUDED.live_proj`,
          ourLeverage: sql`EXCLUDED.our_leverage`,
          ourOwnPct: sql`EXCLUDED.our_own_pct`,
          liveLeverage: sql`EXCLUDED.live_leverage`,
          liveOwnPct: sql`EXCLUDED.live_own_pct`,
        },
      });
    }

    const projectionRunId = await createProjectionRun(slate.id, "recompute", {
      version: NBA_PROJECTION_MODEL_VERSION,
      source: "recompute",
      playerCount: enriched.length,
    }, _matchupDebug.join("\n") || undefined);
    await recordProjectionSnapshots(projectionRunId, projectionSnapshots);

    revalidatePath("/dfs");
    const debugSuffix = _matchupDebug.length > 0 ? `\n${_matchupDebug.join("\n")}` : "";
    return { ok: true, message: `Projections updated: ${projComputed}/${currentPlayers.length} players${debugSuffix}` };
  } catch (e) {
    return { ok: false, message: `Failed: ${e instanceof Error ? e.message : String(e)}` };
  }
}

// ── Fetch post-game player stat lines ────────────────────────────────────────
// Uses ESPN's unofficial public API — no key required, works from Vercel.
// Populates actual_pts/reb/ast/stl/blk/tov/3pm for an NBA slate.

export async function fetchPlayerStatsAction(
  slateDate: string,
): Promise<{ ok: boolean; message: string; updated?: number }> {
  "use server";
  try {
    // 1. Get slate + player pool
    const [slate] = await db
      .select({ id: dkSlates.id, slateDate: dkSlates.slateDate })
      .from(dkSlates)
      .where(and(eq(dkSlates.slateDate, slateDate), eq(dkSlates.sport, "nba")))
      .limit(1);

    if (!slate) return { ok: false, message: `No NBA slate found for ${slateDate}` };

    const pool = await db
      .select({ id: dkPlayers.id, name: dkPlayers.name })
      .from(dkPlayers)
      .where(eq(dkPlayers.slateId, slate.id));

    if (pool.length === 0) return { ok: false, message: "No players in slate" };

    const poolByName = new Map(pool.map((p) => [p.name.toLowerCase(), p]));

    // 2. Get ESPN event IDs for the date (format: YYYYMMDD)
    const espnDate = slateDate.replace(/-/g, "");
    const scoreboardRes = await fetch(
      `https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard?dates=${espnDate}`,
      { next: { revalidate: 0 } },
    );
    if (!scoreboardRes.ok) {
      return { ok: false, message: `ESPN scoreboard request failed: ${scoreboardRes.status}` };
    }
    const scoreboard = await scoreboardRes.json();
    const events: { id: string }[] = scoreboard?.events ?? [];
    if (events.length === 0) {
      return { ok: false, message: `No ESPN events found for ${slateDate}` };
    }

    // 3. For each event fetch box score and collect player stats
    const statsByName = new Map<
      string,
      { pts: number; reb: number; ast: number; stl: number; blk: number; tov: number; fg3m: number }
    >();

    for (const event of events) {
      const summaryRes = await fetch(
        `https://site.api.espn.com/apis/site/v2/sports/basketball/nba/summary?event=${event.id}`,
        { next: { revalidate: 0 } },
      );
      if (!summaryRes.ok) continue;
      const summary = await summaryRes.json();

      const boxTeams: { statistics?: { athletes?: { athlete?: { displayName?: string }; stats?: string[]; didNotPlay?: boolean }[]; labels?: string[] }[] }[] =
        summary?.boxscore?.players ?? [];

      for (const teamBox of boxTeams) {
        for (const statGroup of teamBox.statistics ?? []) {
          const labels: string[] = statGroup.labels ?? [];
          const idxPts  = labels.indexOf("PTS");
          const idxReb  = labels.indexOf("REB");
          const idxAst  = labels.indexOf("AST");
          const idxStl  = labels.indexOf("STL");
          const idxBlk  = labels.indexOf("BLK");
          const idxTo   = labels.indexOf("TO");
          const idx3pt  = labels.indexOf("3PT"); // format "made-att"

          for (const athlete of statGroup.athletes ?? []) {
            if (athlete.didNotPlay) continue;
            const name = athlete.athlete?.displayName?.toLowerCase();
            if (!name) continue;
            const s = athlete.stats ?? [];

            const parse = (i: number) => (i >= 0 ? parseFloat(s[i] ?? "0") || 0 : 0);
            const parse3pt = (i: number) => {
              if (i < 0 || !s[i]) return 0;
              return parseInt(s[i].split("-")[0] ?? "0", 10) || 0;
            };

            statsByName.set(name, {
              pts:  parse(idxPts),
              reb:  parse(idxReb),
              ast:  parse(idxAst),
              stl:  parse(idxStl),
              blk:  parse(idxBlk),
              tov:  parse(idxTo),
              fg3m: parse3pt(idx3pt),
            });
          }
        }
      }
    }

    if (statsByName.size === 0) {
      return { ok: false, message: "No player stats in ESPN box scores (game may not be final yet)" };
    }

    // 4. Match to dk_players and write
    let updated = 0;
    for (const [apiName, stats] of statsByName) {
      let match = poolByName.get(apiName);
      if (!match) {
        // Levenshtein ≤ 2 fuzzy fallback
        let bestDist = 3;
        for (const [dkName, dkRow] of poolByName) {
          const d = levenshtein(apiName, dkName);
          if (d < bestDist) { bestDist = d; match = dkRow; }
        }
      }
      if (!match) continue;

      await db
        .update(dkPlayers)
        .set({
          actualPts: stats.pts,
          actualReb: stats.reb,
          actualAst: stats.ast,
          actualStl: stats.stl,
          actualBlk: stats.blk,
          actualTov: stats.tov,
          actual3pm: stats.fg3m,
        })
        .where(eq(dkPlayers.id, match.id));
      updated++;
    }

    revalidatePath("/analytics");
    return {
      ok: true,
      message: `Player stats updated: ${updated}/${pool.length} players for ${slateDate}`,
      updated,
    };
  } catch (err) {
    return {
      ok: false,
      message: `Stat fetch failed: ${err instanceof Error ? err.message : String(err)}`,
    };
  }
}
