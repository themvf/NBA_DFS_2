"use client";

import { useState, useEffect, useMemo, useTransition, useRef } from "react";
import type { DkPlayerRow, DfsAccuracyMetrics, DfsAccuracyRow, LineupStrategyRow, StrategySummaryRow, Sport } from "@/db/queries";
import type { GeneratedLineup, OptimizerSettings } from "./optimizer";
import type { MlbGeneratedLineup, MlbOptimizerSettings } from "./mlb-optimizer";
import type { OptimizerDebugInfo } from "./optimizer-debug";
import type { CreateOptimizerJobResponse, OptimizerJobStatusResponse, PersistedOptimizerJobLineup } from "./optimizer-job-types";
import {
  normalizeNbaRuleSelections,
  validateNbaRuleSelections,
  type NbaTeamStackRule,
} from "./nba-optimizer-rules";
import { processDkSlate, loadSlateFromContestId, loadMlbSlateFromContestId, saveLineups, exportLineups, exportMlbLineups, uploadResults, refreshPlayerStatus, checkLinestarCookie, uploadLinestarCsv, applyLinestarPaste, fetchPlayerProps, clearSlate, recomputeProjections, auditNbaPropCoverage } from "./actions";

type Props = {
  players: DkPlayerRow[];
  slateDate: string | null;
  accuracy: { metrics: DfsAccuracyMetrics; players: DfsAccuracyRow[] } | null;
  comparison: LineupStrategyRow[];
  strategySummary: StrategySummaryRow[];
  sport: Sport;
};

type SortCol = "name" | "salary" | "avgFptsDk" | "linestarProj" | "ourProj" | "delta" | "projOwnPct" | "ourOwnPct" | "ourLeverage" | "value";
type NbaRuleState = {
  playerLocks: number[];
  playerBlocks: number[];
  blockedTeamIds: number[];
  requiredTeamStacks: NbaTeamStackRule[];
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

const OPTIMIZER_CLIENT_TOKEN_KEY = "dfsOptimizerClientToken";
const NBA_RULE_STORAGE_PREFIX = "dfsOptimizerNbaRules";
const NBA_SLOT_NAMES = ["PG","SG","SF","PF","C","G","F","UTIL"] as const;
const MLB_SLOT_NAMES = ["P1","P2","C","1B","2B","3B","SS","OF1","OF2","OF3"] as const;
const EMPTY_NBA_RULE_STATE: NbaRuleState = {
  playerLocks: [],
  playerBlocks: [],
  blockedTeamIds: [],
  requiredTeamStacks: [],
};

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

function optimizerNbaRuleStorageKey(slateId: number): string {
  return `${NBA_RULE_STORAGE_PREFIX}:${slateId}`;
}

function readStoredNbaRuleState(slateId: number): NbaRuleState {
  const raw = window.localStorage.getItem(optimizerNbaRuleStorageKey(slateId));
  if (!raw) return EMPTY_NBA_RULE_STATE;
  try {
    const parsed = JSON.parse(raw) as Partial<NbaRuleState>;
    const normalized = normalizeNbaRuleSelections({
      playerLocks: Array.isArray(parsed.playerLocks) ? parsed.playerLocks : [],
      playerBlocks: Array.isArray(parsed.playerBlocks) ? parsed.playerBlocks : [],
      blockedTeamIds: Array.isArray(parsed.blockedTeamIds) ? parsed.blockedTeamIds : [],
      requiredTeamStacks: Array.isArray(parsed.requiredTeamStacks) ? parsed.requiredTeamStacks : [],
    });
    return normalized;
  } catch {
    return EMPTY_NBA_RULE_STATE;
  }
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
  stat: "PTS" | "REB" | "AST" | "BLK" | "STL";
  line: number;
  price: number | null;
  book: string | null;
};

function getPlayerPropTokens(player: DkPlayerRow): PlayerPropToken[] {
  const tokens: PlayerPropToken[] = [];
  const fields = [
    { stat: "PTS", line: player.propPts, price: player.propPtsPrice, book: player.propPtsBook },
    { stat: "REB", line: player.propReb, price: player.propRebPrice, book: player.propRebBook },
    { stat: "AST", line: player.propAst, price: player.propAstPrice, book: player.propAstBook },
    { stat: "BLK", line: player.propBlk, price: player.propBlkPrice, book: player.propBlkBook },
    { stat: "STL", line: player.propStl, price: player.propStlPrice, book: player.propStlBook },
  ] as const;

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

export default function DfsClient({ players, slateDate, accuracy, comparison, strategySummary, sport }: Props) {
  const [isPending, startTransition] = useTransition();

  // ── Load state ────────────────────────────────────────────
  const [uploadMsg, setUploadMsg] = useState<{ ok: boolean; text: string } | null>(null);
  const dkFileRef = useRef<HTMLInputElement>(null);
  const lsFileRef = useRef<HTMLInputElement>(null);
  const [contestId, setContestId] = useState("");
  const [cashLineInput, setCashLineInput] = useState("");
  const [loadMode, setLoadMode] = useState<"api" | "csv">("api");

  // ── Contest metadata ──────────────────────────────────────
  const [contestTiming, setContestTiming] = useState<"early" | "main" | "late">("main");
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
  const [sortDir, setSortDir] = useState<"asc" | "desc">("desc");

  // ── Optimizer settings ────────────────────────────────────
  const [mode, setMode] = useState<"cash" | "gpp">("gpp");
  const [nLineups, setNLineups] = useState(20);
  const [teamStackCount, setTeamStackCount] = useState(1);
  const [minStack, setMinStack] = useState(() => sport === "nba" ? 2 : 1);
  const [maxExposure, setMaxExposure] = useState(0.6);
  const [mlbBringBackThreshold, setMlbBringBackThreshold] = useState(3);
  const [bringBackEnabled, setBringBackEnabled] = useState(false);
  const [bringBackSize, setBringBackSize] = useState(1);
  const [minSalaryFilter, setMinSalaryFilter] = useState("");
  const [maxSalaryFilter, setMaxSalaryFilter] = useState("");
  const [strategy, setStrategy] = useState("gpp");
  const [lockedPlayerIds, setLockedPlayerIds] = useState<number[]>([]);
  const [blockedPlayerIds, setBlockedPlayerIds] = useState<number[]>([]);
  const [blockedTeamIds, setBlockedTeamIds] = useState<number[]>([]);
  const [requiredTeamStacks, setRequiredTeamStacks] = useState<NbaTeamStackRule[]>([]);

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
  const [optimizeElapsedMs, setOptimizeElapsedMs] = useState(0);
  const [antiCorrMax, setAntiCorrMax] = useState(1); // MLB: max batters facing your own SP
  const [saveMsg, setSaveMsg] = useState<string | null>(null);
  const [lastRequestedLineupCount, setLastRequestedLineupCount] = useState<number | null>(null);

  // ── Export ────────────────────────────────────────────────
  const [entryTemplate, setEntryTemplate] = useState("");
  const [isExporting, setIsExporting] = useState(false);
  const [exportError, setExportError] = useState<string | null>(null);

  // ── Results upload ────────────────────────────────────────
  const resultsFileRef = useRef<HTMLInputElement>(null);
  const [resultsMsg, setResultsMsg] = useState<{ ok: boolean; text: string } | null>(null);
  const [isUploadingResults, setIsUploadingResults] = useState(false);

  // ── Status refresh ────────────────────────────────────────
  const [refreshMsg, setRefreshMsg] = useState<{ ok: boolean; text: string } | null>(null);
  const [isRefreshing, setIsRefreshing] = useState(false);

  // ── Player props ──────────────────────────────────────────
  const [propsMsg, setPropsMsg] = useState<{ ok: boolean; text: string } | null>(null);
  const [isFetchingProps, setIsFetchingProps] = useState(false);
  const [propAudit, setPropAudit] = useState<NbaPropCoverageAuditResult | null>(null);
  const [isAuditingProps, setIsAuditingProps] = useState(false);

  // ── Clear Slate ───────────────────────────────────────────
  const [clearSlateConfirm, setClearSlateConfirm] = useState(false);
  const [isClearingSlate, setIsClearingSlate] = useState(false);

  // ── Props elapsed timer ───────────────────────────────────
  const [propsElapsed, setPropsElapsed] = useState(0);

  // ── Recompute projections ─────────────────────────────────
  const [projMsg, setProjMsg] = useState<{ ok: boolean; text: string } | null>(null);
  const [isRecomputing, setIsRecomputing] = useState(false);

  // Auto-reset clear confirmation after 3s of inactivity
  useEffect(() => {
    if (!clearSlateConfirm) return;
    const id = setTimeout(() => setClearSlateConfirm(false), 3000);
    return () => clearTimeout(id);
  }, [clearSlateConfirm]);

  // Elapsed-time counter for Fetch Player Props
  useEffect(() => {
    if (!isFetchingProps) { setPropsElapsed(0); return; }
    const id = setInterval(() => setPropsElapsed((s) => s + 1), 1000);
    return () => clearInterval(id);
  }, [isFetchingProps]);

  useEffect(() => {
    if (!isOptimizing || optimizeStartedAt == null) {
      setOptimizeElapsedMs(0);
      return;
    }
    setOptimizeElapsedMs(Date.now() - optimizeStartedAt);
    const id = setInterval(() => setOptimizeElapsedMs(Date.now() - optimizeStartedAt), 1000);
    return () => clearInterval(id);
  }, [isOptimizing, optimizeStartedAt]);

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
  const lockedPlayerSet = useMemo(() => new Set(lockedPlayerIds), [lockedPlayerIds]);
  const blockedPlayerSet = useMemo(() => new Set(blockedPlayerIds), [blockedPlayerIds]);
  const blockedTeamSet = useMemo(() => new Set(blockedTeamIds), [blockedTeamIds]);
  const requiredTeamStackMap = useMemo(
    () => new Map(requiredTeamStacks.map((rule) => [rule.teamId, rule.stackSize])),
    [requiredTeamStacks],
  );

  const optimizeStatusText = !isOptimizing
    ? null
    : lastRequestedLineupCount != null && ((sport === "nba" ? lineups : mlbLineups)?.length ?? 0) > 0
      ? `Built ${((sport === "nba" ? lineups : mlbLineups)?.length ?? 0)} of ${lastRequestedLineupCount} lineups so far.`
      : optimizeElapsedMs >= 60_000
        ? "Still solving. This is longer than expected and usually means the slate is highly constrained."
        : optimizeElapsedMs >= 20_000
          ? "Long solve in progress. Exposure, stacking, and diversity constraints can make the solver much slower."
          : "Optimizer job is queued or running on the server."

  const slowestLineup = optimizeDebug?.lineupSummaries.reduce<OptimizerDebugInfo["lineupSummaries"][number] | null>(
    (best, current) => !best || current.durationMs > best.durationMs ? current : best,
    null,
  ) ?? null;
  const debugTotalMs = optimizeDebug
    ? (isOptimizing ? Math.max(optimizeDebug.totalMs, optimizeElapsedMs) : optimizeDebug.totalMs)
    : 0;
  const heuristicRejectSummary = optimizeDebug?.heuristic
    ? Object.entries(optimizeDebug.heuristic.rejectedByReason)
        .filter(([, count]) => count > 0)
        .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
    : [];

  function applyOptimizerJobResult(result: OptimizerJobStatusResponse) {
    setOptimizerJobId(result.job.id);
    window.localStorage.setItem(
      optimizerJobStorageKey(result.job.sport, result.job.slateId),
      String(result.job.id),
    );
    setLastRequestedLineupCount(result.job.requestedLineups);
    setOptimizeDebug(result.debug ?? null);
    setOptimizeWarning(result.job.warning ?? null);
    setOptimizeError(result.job.error ?? null);

    const startedAt = result.job.startedAt ?? result.job.createdAt;
    setOptimizeStartedAt(startedAt ? Date.parse(startedAt) : null);

    if (sport === "mlb") {
      setMlbLineups(buildMlbLineupsFromPersisted(result.lineups, playersById));
      setLineups(null);
    } else {
      setLineups(buildNbaLineupsFromPersisted(result.lineups, playersById));
      setMlbLineups(null);
    }

    setIsOptimizing(result.job.status === "queued" || result.job.status === "running");
  }

  useEffect(() => {
    setOptimizerClientToken(getOrCreateOptimizerClientToken());
  }, []);

  useEffect(() => {
    if (sport !== "nba" || !currentSlateId) {
      setLockedPlayerIds([]);
      setBlockedPlayerIds([]);
      setBlockedTeamIds([]);
      setRequiredTeamStacks([]);
      return;
    }

    const stored = readStoredNbaRuleState(currentSlateId);
    setLockedPlayerIds(stored.playerLocks);
    setBlockedPlayerIds(stored.playerBlocks);
    setBlockedTeamIds(stored.blockedTeamIds);
    setRequiredTeamStacks(stored.requiredTeamStacks);
  }, [currentSlateId, sport]);

  useEffect(() => {
    if (sport !== "nba" || !currentSlateId) return;
    const nextState: NbaRuleState = {
      playerLocks: lockedPlayerIds,
      playerBlocks: blockedPlayerIds,
      blockedTeamIds,
      requiredTeamStacks,
    };
    window.localStorage.setItem(
      optimizerNbaRuleStorageKey(currentSlateId),
      JSON.stringify(nextState),
    );
  }, [blockedPlayerIds, blockedTeamIds, currentSlateId, lockedPlayerIds, requiredTeamStacks, sport]);

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
        timeoutId = setTimeout(() => {
          poll().catch((error) => {
            if (!cancelled) {
              setOptimizeError(error instanceof Error ? error.message : String(error));
              setIsOptimizing(false);
            }
          });
        }, 2500);
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

  const sortedPlayers = useMemo(() => {
    return [...filteredPlayers].sort((a, b) => {
      let av: number, bv: number;
      switch (sortCol) {
        case "salary":      av = a.salary;          bv = b.salary;          break;
        case "avgFptsDk":   av = a.avgFptsDk ?? -99; bv = b.avgFptsDk ?? -99; break;
        case "linestarProj":av = a.linestarProj ?? -99; bv = b.linestarProj ?? -99; break;
        case "ourProj":     av = a.ourProj ?? -99;  bv = b.ourProj ?? -99;  break;
        case "delta":       av = (a.ourProj ?? 0) - (a.linestarProj ?? 0);
                            bv = (b.ourProj ?? 0) - (b.linestarProj ?? 0); break;
        case "projOwnPct":  av = a.projOwnPct ?? -99; bv = b.projOwnPct ?? -99; break;
        case "ourOwnPct":   av = a.ourOwnPct ?? -99; bv = b.ourOwnPct ?? -99; break;
        case "ourLeverage": av = a.ourLeverage ?? -99; bv = b.ourLeverage ?? -99; break;
        case "value":       av = (a.ourProj ?? 0) / (a.salary / 1000);
                            bv = (b.ourProj ?? 0) / (b.salary / 1000);    break;
        default:            return a.name.localeCompare(b.name);
      }
      return sortDir === "desc" ? bv - av : av - bv;
    });
  }, [filteredPlayers, sortCol, sortDir]);

  const filteredTeams = useMemo(() => {
    if (sport !== "nba") return [];
    const byId = new Map<number, { teamId: number; teamAbbrev: string; teamName: string | null; teamLogo: string | null }>();
    for (const player of filteredPlayers) {
      if (player.teamId == null || byId.has(player.teamId)) continue;
      byId.set(player.teamId, {
        teamId: player.teamId,
        teamAbbrev: player.teamAbbrev,
        teamName: player.teamName,
        teamLogo: player.teamLogo,
      });
    }
    return Array.from(byId.values()).sort((a, b) => a.teamAbbrev.localeCompare(b.teamAbbrev));
  }, [filteredPlayers, sport]);

  const teamOddsById = useMemo(() => {
    const byId = new Map<number, { vegasTotal: number | null; teamTotal: number | null; moneyline: number | null }>();
    if (sport !== "nba") return byId;

    for (const player of filteredPlayers) {
      if (player.teamId == null || byId.has(player.teamId)) continue;
      const isHome = player.homeTeamId != null && player.teamId === player.homeTeamId;
      const moneyline = isHome ? player.homeMl : player.awayMl;
      const explicitTeamTotal = isHome ? player.homeImplied : player.awayImplied;
      const derivedTeamTotal = player.vegasTotal != null
        ? computeTeamImpliedTotal(player.vegasTotal, player.homeMl, player.awayMl, isHome)
        : null;
      byId.set(player.teamId, {
        vegasTotal: player.vegasTotal ?? null,
        teamTotal: explicitTeamTotal ?? derivedTeamTotal,
        moneyline: moneyline ?? null,
      });
    }

    return byId;
  }, [filteredPlayers, sport]);

  function toggleSort(col: SortCol) {
    if (sortCol === col) setSortDir((d) => d === "desc" ? "asc" : "desc");
    else { setSortCol(col); setSortDir("desc"); }
  }

  function togglePlayerLock(player: DkPlayerRow) {
    if (sport !== "nba") return;
    setBlockedPlayerIds((current) => current.filter((id) => id !== player.id));
    setLockedPlayerIds((current) =>
      current.includes(player.id)
        ? current.filter((id) => id !== player.id)
        : [...current, player.id],
    );
  }

  function togglePlayerBlock(player: DkPlayerRow) {
    if (sport !== "nba") return;
    setLockedPlayerIds((current) => current.filter((id) => id !== player.id));
    setBlockedPlayerIds((current) =>
      current.includes(player.id)
        ? current.filter((id) => id !== player.id)
        : [...current, player.id],
    );
  }

  function toggleTeamBlock(teamId: number) {
    if (sport !== "nba") return;
    setRequiredTeamStacks((current) => current.filter((rule) => rule.teamId !== teamId));
    setBlockedTeamIds((current) =>
      current.includes(teamId)
        ? current.filter((id) => id !== teamId)
        : [...current, teamId],
    );
  }

  function updateTeamStackRule(teamId: number, value: string) {
    if (sport !== "nba") return;
    const stackSize = Number(value);
    setBlockedTeamIds((current) => current.filter((id) => id !== teamId));
    setRequiredTeamStacks((current) => {
      const remaining = current.filter((rule) => rule.teamId !== teamId);
      if (![2, 3, 4, 5].includes(stackSize)) return remaining;
      return [...remaining, { teamId, stackSize: stackSize as 2 | 3 | 4 | 5 }]
        .sort((a, b) => a.teamId - b.teamId);
    });
  }

  function clearLocks() {
    setLockedPlayerIds([]);
  }

  function clearBlocks() {
    setBlockedPlayerIds([]);
    setBlockedTeamIds([]);
  }

  function clearTeamRules() {
    setRequiredTeamStacks([]);
  }

  function SortHeader({ col, label }: { col: SortCol; label: string }) {
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
    setIsOptimizing(true);
    setOptimizeStartedAt(Date.now());
    setOptimizeElapsedMs(0);
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
            bringBackThreshold: mlbBringBackThreshold, antiCorrMax,
          } satisfies MlbOptimizerSettings
        : {
            mode, nLineups, minStack, teamStackCount, maxExposure, bringBackEnabled, bringBackSize,
            minSalaryFilter: minSalaryFilter ? parseInt(minSalaryFilter, 10) : null,
            maxSalaryFilter: maxSalaryFilter ? parseInt(maxSalaryFilter, 10) : null,
            playerLocks: lockedPlayerIds,
            playerBlocks: blockedPlayerIds,
            blockedTeamIds,
            requiredTeamStacks,
          } satisfies OptimizerSettings;

      if (sport === "nba") {
        const validation = validateNbaRuleSelections(filteredPlayers, settings);
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

  async function handleSave() {
    const activeLineups = sport === "nba" ? lineups : mlbLineups;
    if (!activeLineups || !players[0]?.slateId) return;
    setSaveMsg(null);
    const res = await saveLineups(players[0].slateId, activeLineups, strategy);
    setSaveMsg(res.ok ? `Saved ${res.saved} lineups as "${strategy}"` : "Save failed");
  }

  async function handleExport() {
    const activeLineups = sport === "nba" ? lineups : mlbLineups;
    if (!activeLineups) return;
    setExportError(null);
    setIsExporting(true);
    const result = sport === "mlb"
      ? await exportMlbLineups(mlbLineups!, entryTemplate)
      : await exportLineups(lineups!, entryTemplate);
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
  }

  async function handleUploadResults() {
    const file = resultsFileRef.current?.files?.[0];
    if (!file) { setResultsMsg({ ok: false, text: "Select a results CSV first" }); return; }
    setIsUploadingResults(true);
    setResultsMsg(null);
    const fd = new FormData();
    fd.append("resultsFile", file);
    const res = await uploadResults(fd);
    setIsUploadingResults(false);
    setResultsMsg({ ok: res.ok, text: res.message });
  }

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
    const res = await fetchPlayerProps();
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
    const res = await uploadLinestarCsv(fd);
    setIsUploadingLs(false);
    setLsUploadMsg({ ok: res.ok, text: res.message });
  }

  async function handleApplyPaste() {
    if (!lsPasteText.trim()) { setLsUploadMsg({ ok: false, text: "Paste LineStar data first" }); return; }
    setIsUploadingLs(true);
    setLsUploadMsg(null);
    const res = await applyLinestarPaste(lsPasteText);
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
              {(["early", "main", "late"] as const).map((t, i) => (
                <button
                  key={t}
                  onClick={() => setContestTiming(t)}
                  className={`px-3 py-1 capitalize ${i > 0 ? "border-l" : ""} ${
                    contestTiming === t ? "bg-slate-700 text-white" : "text-gray-500 hover:bg-gray-50"
                  }`}
                >
                  {t}
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
            {isFetchingProps && (
              <div className="flex items-center gap-2 text-sm text-emerald-700">
                <span className="inline-block h-4 w-4 rounded-full border-2 border-emerald-500 border-t-transparent animate-spin" />
                <span>Fetching props… ({propsElapsed}s)</span>
              </div>
            )}
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

      </div>

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
              onChange={(e) => setMode(e.target.value as "cash" | "gpp")}
              className="rounded border px-2 py-1 text-sm"
            >
              <option value="gpp">GPP (leverage)</option>
              <option value="cash">Cash (proj)</option>
            </select>
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
        {isOptimizing && (
          <div className="mt-2 rounded border border-blue-200 bg-blue-50 p-3 text-sm text-blue-900">
            <p className="font-medium">Optimizer running: {fmtDuration(optimizeElapsedMs)}</p>
            {optimizeStatusText && <p className="mt-1 text-xs text-blue-800">{optimizeStatusText}</p>}
          </div>
        )}
        {optimizeError && <p className="mt-2 text-sm text-red-600 whitespace-pre-wrap">{optimizeError}</p>}
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
                    </>
                  ) : (
                    <>
                      <p><strong>Effective stack:</strong> {optimizeDebug.effectiveSettings.minStack}</p>
                      <p><strong>Effective bring-back:</strong> {optimizeDebug.effectiveSettings.bringBackThreshold ?? 0}</p>
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

      {sport === "nba" && filteredPlayers.length > 0 && (
        <div className="rounded-lg border bg-card p-4">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <div>
              <h2 className="text-sm font-semibold">NBA Rule Controls</h2>
              <p className="text-xs text-gray-500">
                Locks persist per slate. Every lineup must satisfy one selected team stack when team stack rules are set.
              </p>
            </div>
            <div className="flex flex-wrap gap-2">
              <button
                onClick={clearLocks}
                disabled={lockedPlayerIds.length === 0}
                className="rounded border px-3 py-1 text-xs text-blue-700 hover:bg-blue-50 disabled:opacity-50"
              >
                Clear Locks
              </button>
              <button
                onClick={clearBlocks}
                disabled={blockedPlayerIds.length === 0 && blockedTeamIds.length === 0}
                className="rounded border px-3 py-1 text-xs text-red-700 hover:bg-red-50 disabled:opacity-50"
              >
                Clear Blocks
              </button>
              <button
                onClick={clearTeamRules}
                disabled={requiredTeamStacks.length === 0}
                className="rounded border px-3 py-1 text-xs text-emerald-700 hover:bg-emerald-50 disabled:opacity-50"
              >
                Clear Team Rules
              </button>
            </div>
          </div>
          <div className="mt-3 flex flex-wrap gap-2 text-xs">
            <span className="rounded-full bg-blue-50 px-2 py-0.5 text-blue-700">{lockedPlayerIds.length} locked</span>
            <span className="rounded-full bg-red-50 px-2 py-0.5 text-red-700">
              {blockedPlayerIds.length + blockedTeamIds.length} blocked rules
            </span>
            <span className="rounded-full bg-emerald-50 px-2 py-0.5 text-emerald-700">
              {requiredTeamStacks.length} team stacks
            </span>
          </div>
          <div className="mt-3 grid gap-2 md:grid-cols-2 xl:grid-cols-4">
            {filteredTeams.map((team) => {
              const isBlocked = blockedTeamSet.has(team.teamId);
              const stackSize = requiredTeamStackMap.get(team.teamId);
              const odds = teamOddsById.get(team.teamId);
              return (
                <div key={team.teamId} className={`rounded border p-3 ${isBlocked ? "border-red-200 bg-red-50" : stackSize ? "border-emerald-200 bg-emerald-50" : "bg-white"}`}>
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
                      onClick={() => toggleTeamBlock(team.teamId)}
                      className={`rounded border px-2 py-1 text-xs ${isBlocked ? "border-red-300 bg-red-100 text-red-700" : "text-gray-600 hover:bg-gray-50"}`}
                    >
                      {isBlocked ? "Blocked" : "Block Team"}
                    </button>
                    <select
                      value={stackSize ?? ""}
                      onChange={(e) => updateTeamStackRule(team.teamId, e.target.value)}
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
      )}

      {/* Player Pool Table */}
      {filteredPlayers.length > 0 && (
        <div className="rounded-lg border bg-card overflow-hidden">
          <div className="px-4 py-3 border-b">
            <h2 className="text-sm font-semibold">
              Player Pool — {filteredPlayers.length} players
              {filteredPlayers.filter((p) => p.isOut).length > 0 && (
                <span className="ml-2 text-xs text-red-500">
                  ({filteredPlayers.filter((p) => p.isOut).length} OUT)
                </span>
              )}
            </h2>
          </div>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead className="border-b bg-gray-50">
                <tr>
                  <th className="px-3 py-2 text-left text-xs font-medium text-gray-500">Pos</th>
                  <SortHeader col="name" label="Player" />
                  <th className="px-3 py-2 text-left text-xs font-medium text-gray-500">Team</th>
                  <th className="px-3 py-2 text-left text-xs font-medium text-gray-500">Props</th>
                  {sport === "nba" && (
                    <th className="px-3 py-2 text-left text-xs font-medium text-gray-500">Rules</th>
                  )}
                  <SortHeader col="salary" label="Salary" />
                  <SortHeader col="avgFptsDk" label="DK Proj" />
                  <SortHeader col="linestarProj" label="LS Proj" />
                  {sport === "nba" && (
                    <>
                      <th className="px-3 py-2 text-left text-xs font-medium text-gray-500">Model</th>
                      <th className="px-3 py-2 text-left text-xs font-medium text-gray-500">Market</th>
                    </>
                  )}
                  <SortHeader col="ourProj" label={sport === "nba" ? "Blend Proj" : "Our Proj"} />
                  <SortHeader col="delta" label={sport === "nba" ? "Blend Δ" : "Delta"} />
                  <SortHeader col="projOwnPct" label="LS Own%" />
                  <SortHeader col="ourOwnPct" label="Our Own%" />
                  <SortHeader col="ourLeverage" label="Leverage" />
                  <SortHeader col="value" label="Value" />
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100">
                {sortedPlayers.slice(0, 200).map((p) => {
                  const blendProj = p.blendProj ?? p.ourProj;
                  const delta = blendProj != null && p.linestarProj != null
                    ? blendProj - p.linestarProj : null;
                  const value = blendProj != null ? blendProj / (p.salary / 1000) : null;
                  const propTokens = getPlayerPropTokens(p);
                  const pos = displayPos(p.eligiblePositions, sport);
                  const isLocked = lockedPlayerSet.has(p.id);
                  const isBlocked = blockedPlayerSet.has(p.id);
                  const isTeamBlocked = p.teamId != null && blockedTeamSet.has(p.teamId);
                  const stackSize = p.teamId != null ? requiredTeamStackMap.get(p.teamId) : undefined;
                  return (
                    <tr
                      key={p.id}
                      className={`hover:bg-gray-50 ${
                        p.isOut ? "opacity-40 line-through" : ""
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
                        {sport === "nba" && (
                          <span className="ml-2 inline-flex flex-wrap gap-1 align-middle">
                            {isLocked && <span className="rounded bg-blue-100 px-1.5 py-0.5 text-[10px] font-medium text-blue-700">LOCK</span>}
                            {(isBlocked || isTeamBlocked) && <span className="rounded bg-red-100 px-1.5 py-0.5 text-[10px] font-medium text-red-700">BLOCK</span>}
                            {stackSize != null && <span className="rounded bg-emerald-100 px-1.5 py-0.5 text-[10px] font-medium text-emerald-700">STACK {stackSize}</span>}
                          </span>
                        )}
                      </td>
                      <td className="px-3 py-1.5 text-xs text-gray-500">{p.teamAbbrev}</td>
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
                      {sport === "nba" && (
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
                          <td className="px-3 py-1.5 text-xs">{fmt1(p.modelProj)}</td>
                          <td className="px-3 py-1.5 text-xs">{fmt1(p.marketProj)}</td>
                        </>
                      )}
                      <td className="px-3 py-1.5 text-xs font-medium">{fmt1(blendProj)}</td>
                      <td className={`px-3 py-1.5 text-xs font-medium ${
                        delta == null ? "text-gray-400" : delta >= 2 ? "text-green-600" : delta <= -2 ? "text-red-500" : ""
                      }`}>
                        {delta != null ? (delta >= 0 ? "+" : "") + delta.toFixed(1) : "—"}
                      </td>
                      <td className="px-3 py-1.5 text-xs">{p.projOwnPct != null ? p.projOwnPct.toFixed(1) + "%" : "—"}</td>
                      <td className="px-3 py-1.5 text-xs">{p.ourOwnPct != null ? p.ourOwnPct.toFixed(1) + "%" : "—"}</td>
                      <td className={`px-3 py-1.5 text-xs font-medium ${
                        p.ourLeverage == null ? "" :
                        p.ourLeverage > 0 ? "text-green-700" : "text-red-400"
                      }`}>{fmt1(p.ourLeverage)}</td>
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
      {((sport === "nba" ? lineups : mlbLineups) ?? []).length > 0 && (() => {
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
              Paste your DK multi-entry template below. Comma-separated CSV and tab-delimited DK paste format are both supported.
            </p>
            <textarea
              value={entryTemplate}
              onChange={(e) => setEntryTemplate(e.target.value)}
              placeholder={sport === "mlb"
                ? "Entry ID,Contest Name,Contest ID,Entry Fee,P,P,C,1B,2B,3B,SS,OF,OF,OF\n12345,MLB..."
                : "Entry ID,Contest Name,Contest ID,Entry Fee,PG,SG,SF,PF,C,G,F,UTIL\n12345,NBA..."
              }
              rows={4}
              className="w-full rounded border px-2 py-1 text-xs font-mono"
            />
            <button
              onClick={handleExport}
              disabled={isExporting || !entryTemplate}
              className="mt-2 rounded bg-blue-600 px-4 py-1.5 text-sm text-white hover:bg-blue-700 disabled:opacity-50"
            >
              {isExporting ? "Exporting…" : "Export CSV"}
            </button>
            {exportError && <p className="mt-2 text-xs text-red-600">{exportError}</p>}
          </div>
        </div>
        );
      })()}

      {/* Accuracy Panel */}
      {accuracy && (
        <div className="rounded-lg border bg-card p-4">
          <h2 className="text-sm font-semibold mb-3">Projection Accuracy — {accuracy.metrics.slateDate}</h2>
          <div className="grid grid-cols-2 gap-4 mb-4">
            <div className="rounded border p-3">
              <p className="text-xs text-gray-500 mb-1">Our Model (n={accuracy.metrics.nOur})</p>
              <p className="text-lg font-bold">{fmt1(accuracy.metrics.ourMAE)} MAE</p>
              <p className="text-xs text-gray-500">Bias: {accuracy.metrics.ourBias != null ? (accuracy.metrics.ourBias >= 0 ? "+" : "") + accuracy.metrics.ourBias.toFixed(2) : "—"}</p>
            </div>
            {accuracy.metrics.nLinestar > 0 && (
              <div className="rounded border p-3">
                <p className="text-xs text-gray-500 mb-1">LineStar (n={accuracy.metrics.nLinestar})</p>
                <p className="text-lg font-bold">{fmt1(accuracy.metrics.linestarMAE)} MAE</p>
                <p className="text-xs text-gray-500">Bias: {accuracy.metrics.linestarBias != null ? (accuracy.metrics.linestarBias >= 0 ? "+" : "") + accuracy.metrics.linestarBias.toFixed(2) : "—"}</p>
              </div>
            )}
          </div>
          {/* Biggest misses table */}
          <div className="overflow-x-auto">
            <table className="w-full text-xs">
              <thead>
                <tr className="text-gray-400 border-b">
                  <th className="py-1 text-left">Player</th>
                  <th className="py-1 text-right">Our</th>
                  <th className="py-1 text-right">LS</th>
                  <th className="py-1 text-right">Actual</th>
                  <th className="py-1 text-right">Err</th>
                </tr>
              </thead>
              <tbody>
                {accuracy.players.slice(0, 15).map((p) => {
                  const err = p.ourProj != null && p.actualFpts != null ? p.ourProj - p.actualFpts : null;
                  return (
                    <tr key={p.id} className="border-b border-gray-50">
                      <td className="py-1 font-medium">{p.name} <span className="text-gray-400">{p.teamAbbrev}</span></td>
                      <td className="py-1 text-right">{fmt1(p.ourProj)}</td>
                      <td className="py-1 text-right text-gray-400">{fmt1(p.linestarProj)}</td>
                      <td className="py-1 text-right font-medium">{fmt1(p.actualFpts)}</td>
                      <td className={`py-1 text-right font-medium ${err == null ? "" : err > 0 ? "text-red-500" : "text-green-600"}`}>
                        {err != null ? (err >= 0 ? "+" : "") + err.toFixed(1) : "—"}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* Strategy Comparison */}
      {comparison.length > 0 && (
        <div className="rounded-lg border bg-card p-4">
          <h2 className="text-sm font-semibold mb-3">Strategy Comparison — Latest Slate</h2>
          <table className="w-full text-xs">
            <thead>
              <tr className="text-gray-400 border-b">
                <th className="py-1 text-left">Strategy</th>
                <th className="py-1 text-right">N</th>
                <th className="py-1 text-right">Avg Proj</th>
                <th className="py-1 text-right">Avg Actual</th>
                <th className="py-1 text-right">Top Stack</th>
              </tr>
            </thead>
            <tbody>
              {comparison.map((row) => (
                <tr key={row.strategy} className="border-b border-gray-50">
                  <td className="py-1 font-medium">{row.strategy}</td>
                  <td className="py-1 text-right">{row.nLineups}</td>
                  <td className="py-1 text-right">{fmt1(row.avgProjFpts)}</td>
                  <td className="py-1 text-right font-medium">{row.avgActualFpts != null ? fmt1(row.avgActualFpts) : "—"}</td>
                  <td className="py-1 text-right text-gray-400">{row.topStack ?? "—"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {/* Cross-slate summary */}
      {strategySummary.length > 0 && (
        <div className="rounded-lg border bg-card p-4">
          <h2 className="text-sm font-semibold mb-3">Strategy Leaderboard — All Slates</h2>
          <table className="w-full text-xs">
            <thead>
              <tr className="text-gray-400 border-b">
                <th className="py-1 text-left">Strategy</th>
                <th className="py-1 text-right">Slates</th>
                <th className="py-1 text-right">Lineups</th>
                <th className="py-1 text-right">Avg FPTS</th>
                <th className="py-1 text-right">Cash%</th>
                <th className="py-1 text-right">Best</th>
              </tr>
            </thead>
            <tbody>
              {strategySummary.map((row) => (
                <tr key={row.strategy} className="border-b border-gray-50">
                  <td className="py-1 font-medium">{row.strategy}</td>
                  <td className="py-1 text-right">{row.nSlates}</td>
                  <td className="py-1 text-right">{row.totalLineups}</td>
                  <td className="py-1 text-right font-medium">{fmt1(row.avgActualFpts)}</td>
                  <td className="py-1 text-right">{row.cashRate != null ? row.cashRate.toFixed(1) + "%" : "—"}</td>
                  <td className="py-1 text-right text-green-600 font-medium">{fmt1(row.bestSingleLineup)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {/* Upload Results */}
      <div className="rounded-lg border bg-card p-4">
        <h2 className="text-sm font-semibold mb-1">Upload Results</h2>
        <p className="text-xs text-gray-500 mb-3">
          Upload a DK results CSV or contest standings CSV to populate actual FPTS and update lineup actuals for the most recent slate.
        </p>
        <div className="flex flex-wrap items-end gap-4">
          <div>
            <label className="text-xs text-gray-500 block mb-1">
              DK Results CSV or Contest Standings CSV
            </label>
            <input ref={resultsFileRef} type="file" accept=".csv" className="text-sm" />
          </div>
          <button
            onClick={handleUploadResults}
            disabled={isUploadingResults}
            className="rounded bg-purple-600 px-4 py-1.5 text-sm text-white hover:bg-purple-700 disabled:opacity-50"
          >
            {isUploadingResults ? "Uploading…" : "Upload & Analyze"}
          </button>
        </div>
        {resultsMsg && (
          <p className={`mt-2 text-sm ${resultsMsg.ok ? "text-green-700" : "text-red-600"}`}>
            {resultsMsg.text}
          </p>
        )}
      </div>
    </div>
  );
}
