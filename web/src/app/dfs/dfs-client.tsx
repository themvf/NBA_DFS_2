"use client";

import { useState, useEffect, useMemo, useTransition, useRef } from "react";
import type { DkPlayerRow, DfsAccuracyMetrics, DfsAccuracyRow, LineupStrategyRow, StrategySummaryRow, Sport } from "@/db/queries";
import type { GeneratedLineup, OptimizerSettings } from "./optimizer";
import type { MlbGeneratedLineup, MlbOptimizerSettings } from "./mlb-optimizer";
import { processDkSlate, loadSlateFromContestId, loadMlbSlateFromContestId, runOptimizer, runMlbOptimizer, saveLineups, exportLineups, exportMlbLineups, uploadResults, refreshPlayerStatus, checkLinestarCookie, uploadLinestarCsv, applyLinestarPaste, fetchPlayerProps, clearSlate, recomputeProjections } from "./actions";

type Props = {
  players: DkPlayerRow[];
  slateDate: string | null;
  accuracy: { metrics: DfsAccuracyMetrics; players: DfsAccuracyRow[] } | null;
  comparison: LineupStrategyRow[];
  strategySummary: StrategySummaryRow[];
  sport: Sport;
};

type SortCol = "name" | "salary" | "avgFptsDk" | "linestarProj" | "ourProj" | "delta" | "projOwnPct" | "ourOwnPct" | "ourLeverage" | "value";

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

  const [selectedGames, setSelectedGames] = useState<Set<string>>(() => new Set(allGames));

  // ── Sort ──────────────────────────────────────────────────
  const [sortCol, setSortCol] = useState<SortCol>("ourLeverage");
  const [sortDir, setSortDir] = useState<"asc" | "desc">("desc");

  // ── Optimizer settings ────────────────────────────────────
  const [mode, setMode] = useState<"cash" | "gpp">("gpp");
  const [nLineups, setNLineups] = useState(20);
  const [minStack, setMinStack] = useState(2);
  const [maxExposure, setMaxExposure] = useState(0.6);
  const [bringBackThreshold, setBringBackThreshold] = useState(3);
  const [strategy, setStrategy] = useState("gpp");

  // ── Lineups ───────────────────────────────────────────────
  const [lineups,    setLineups]    = useState<GeneratedLineup[]    | null>(null);
  const [mlbLineups, setMlbLineups] = useState<MlbGeneratedLineup[] | null>(null);
  const [isOptimizing, setIsOptimizing] = useState(false);
  const [optimizeError, setOptimizeError] = useState<string | null>(null);
  const [antiCorrMax, setAntiCorrMax] = useState(1); // MLB: max batters facing your own SP
  const [saveMsg, setSaveMsg] = useState<string | null>(null);

  // ── Export ────────────────────────────────────────────────
  const [entryTemplate, setEntryTemplate] = useState("");
  const [isExporting, setIsExporting] = useState(false);

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

  // ── LineStar input: CSV file or paste ────────────────────
  const [lsMode, setLsMode] = useState<"csv" | "paste">("paste");
  const lsUploadRef = useRef<HTMLInputElement>(null);
  const [lsUploadMsg, setLsUploadMsg] = useState<{ ok: boolean; text: string } | null>(null);
  const [isUploadingLs, setIsUploadingLs] = useState(false);
  const [lsPasteText, setLsPasteText] = useState("");

  // ── LineStar cookie status ────────────────────────────────
  const [cookieStatus, setCookieStatus] = useState<{ ok: boolean; message: string } | null>(null);
  const [isCheckingCookie, setIsCheckingCookie] = useState(false);

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

  function toggleSort(col: SortCol) {
    if (sortCol === col) setSortDir((d) => d === "desc" ? "asc" : "desc");
    else { setSortCol(col); setSortDir("desc"); }
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
    setIsOptimizing(true);
    setOptimizeError(null);
    setLineups(null);
    setMlbLineups(null);

    // Build game → matchupId map for filtering
    const gameToMatchup = new Map<string, number>();
    for (const p of players) {
      if (p.matchupId != null) gameToMatchup.set(parseGameKey(p.gameInfo), p.matchupId);
    }
    const gameFilter = Array.from(selectedGames)
      .map((g) => gameToMatchup.get(g))
      .filter((id): id is number => id != null);

    if (sport === "mlb") {
      const settings: MlbOptimizerSettings = {
        mode, nLineups, minStack, maxExposure,
        bringBackThreshold, antiCorrMax,
      };
      const res = await runMlbOptimizer(players[0].slateId, gameFilter, settings);
      setIsOptimizing(false);
      if (!res.ok || !res.lineups) { setOptimizeError(res.error ?? "Optimizer failed"); return; }
      setMlbLineups(res.lineups);
    } else {
      const settings: OptimizerSettings = { mode, nLineups, minStack, maxExposure, bringBackThreshold };
      const res = await runOptimizer(players[0].slateId, gameFilter, settings);
      setIsOptimizing(false);
      if (!res.ok || !res.lineups) { setOptimizeError(res.error ?? "Optimizer failed"); return; }
      setLineups(res.lineups);
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
    setIsExporting(true);
    const csvStr = sport === "mlb"
      ? await exportMlbLineups(mlbLineups!, entryTemplate)
      : await exportLineups(lineups!, entryTemplate);
    setIsExporting(false);
    if (!csvStr) return;
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

        {/* Player props from The Odds API — NBA only (pts/reb/ast) */}
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
              <span className="text-xs text-gray-400">
                Pulls pts/reb/ast lines from The Odds API · updates projections (~20s)
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
          <div>
            <label className="text-xs text-gray-500 block mb-1">Min Stack</label>
            <select
              value={minStack}
              onChange={(e) => setMinStack(parseInt(e.target.value))}
              className="rounded border px-2 py-1 text-sm"
            >
              <option value={2}>2</option>
              <option value={3}>3</option>
              <option value={4}>4</option>
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
            <div>
              <label className="text-xs text-gray-500 block mb-1">
                Bring-back{" "}
                <span className="text-gray-400 font-normal">(GPP)</span>
              </label>
              <select
                value={bringBackThreshold}
                onChange={(e) => setBringBackThreshold(parseInt(e.target.value))}
                className="rounded border px-2 py-1 text-sm"
              >
                <option value={0}>Off</option>
                <option value={3}>3+ → 1 back</option>
                <option value={2}>2+ → 1 back</option>
              </select>
            </div>
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
          <button
            onClick={handleOptimize}
            disabled={isOptimizing || filteredPlayers.length === 0}
            className="rounded bg-green-600 px-5 py-1.5 text-sm font-medium text-white hover:bg-green-700 disabled:opacity-50"
          >
            {isOptimizing ? "Optimizing…" : "Optimize"}
          </button>
        </div>
        {optimizeError && <p className="mt-2 text-sm text-red-600 whitespace-pre-wrap">{optimizeError}</p>}
      </div>

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
                  <SortHeader col="salary" label="Salary" />
                  <SortHeader col="avgFptsDk" label="DK Proj" />
                  <SortHeader col="linestarProj" label="LS Proj" />
                  <SortHeader col="ourProj" label="Our Proj" />
                  <SortHeader col="delta" label="Delta" />
                  <SortHeader col="projOwnPct" label="LS Own%" />
                  <SortHeader col="ourOwnPct" label="Our Own%" />
                  <SortHeader col="ourLeverage" label="Leverage" />
                  <SortHeader col="value" label="Value" />
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100">
                {sortedPlayers.slice(0, 200).map((p) => {
                  const delta = p.ourProj != null && p.linestarProj != null
                    ? p.ourProj - p.linestarProj : null;
                  const value = p.ourProj != null ? p.ourProj / (p.salary / 1000) : null;
                  const pos = displayPos(p.eligiblePositions, sport);
                  return (
                    <tr key={p.id} className={`hover:bg-gray-50 ${p.isOut ? "opacity-40 line-through" : ""}`}>
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
                      </td>
                      <td className="px-3 py-1.5 text-xs text-gray-500">{p.teamAbbrev}</td>
                      <td className="px-3 py-1.5 font-mono text-xs">{fmtSalary(p.salary)}</td>
                      <td className="px-3 py-1.5 text-xs text-gray-500">{fmt1(p.avgFptsDk)}</td>
                      <td className="px-3 py-1.5 text-xs">{fmt1(p.linestarProj)}</td>
                      <td className="px-3 py-1.5 text-xs font-medium">{fmt1(p.ourProj)}</td>
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
              {activeLineups.length} Lineups Generated
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
              Paste your DK multi-entry template CSV (with Entry IDs) below, then click Export.
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
