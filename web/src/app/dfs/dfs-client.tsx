"use client";

import { useState, useMemo, useTransition, useRef } from "react";
import type { DkPlayerRow, DfsAccuracyMetrics, DfsAccuracyRow, LineupStrategyRow, StrategySummaryRow } from "@/db/queries";
import type { GeneratedLineup, OptimizerSettings } from "./optimizer";
import { processDkSlate, loadSlateFromContestId, runOptimizer, saveLineups, exportLineups, uploadResults, refreshPlayerStatus, checkLinestarCookie, uploadLinestarCsv } from "./actions";

type Props = {
  players: DkPlayerRow[];
  slateDate: string | null;
  accuracy: { metrics: DfsAccuracyMetrics; players: DfsAccuracyRow[] } | null;
  comparison: LineupStrategyRow[];
  strategySummary: StrategySummaryRow[];
};

type SortCol = "name" | "salary" | "linestarProj" | "ourProj" | "delta" | "projOwnPct" | "ourLeverage" | "value";

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

// NBA position badge color
function posBadgeColor(pos: string): string {
  if (pos.includes("PG")) return "bg-blue-100 text-blue-800";
  if (pos.includes("SG")) return "bg-indigo-100 text-indigo-800";
  if (pos.includes("SF")) return "bg-green-100 text-green-800";
  if (pos.includes("PF")) return "bg-yellow-100 text-yellow-800";
  if (pos.includes("C"))  return "bg-red-100 text-red-800";
  return "bg-gray-100 text-gray-700";
}

// Simplified display position
function displayPos(eligiblePositions: string): string {
  const parts = eligiblePositions.split("/");
  const primary = parts.find((p) => ["PG","SG","SF","PF","C"].includes(p));
  return primary ?? parts[0] ?? "UTIL";
}

export default function DfsClient({ players, slateDate, accuracy, comparison, strategySummary }: Props) {
  const [isPending, startTransition] = useTransition();

  // ── Load state ────────────────────────────────────────────
  const [uploadMsg, setUploadMsg] = useState<{ ok: boolean; text: string } | null>(null);
  const dkFileRef = useRef<HTMLInputElement>(null);
  const lsFileRef = useRef<HTMLInputElement>(null);
  const [contestId, setContestId] = useState("");
  const [cashLineInput, setCashLineInput] = useState("");
  const [loadMode, setLoadMode] = useState<"api" | "csv">("api");

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
  const [lineups, setLineups] = useState<GeneratedLineup[] | null>(null);
  const [isOptimizing, setIsOptimizing] = useState(false);
  const [optimizeError, setOptimizeError] = useState<string | null>(null);
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

  // ── LineStar manual upload ────────────────────────────────
  const lsUploadRef = useRef<HTMLInputElement>(null);
  const [lsUploadMsg, setLsUploadMsg] = useState<{ ok: boolean; text: string } | null>(null);
  const [isUploadingLs, setIsUploadingLs] = useState(false);

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
        case "linestarProj":av = a.linestarProj ?? -99; bv = b.linestarProj ?? -99; break;
        case "ourProj":     av = a.ourProj ?? -99;  bv = b.ourProj ?? -99;  break;
        case "delta":       av = (a.ourProj ?? 0) - (a.linestarProj ?? 0);
                            bv = (b.ourProj ?? 0) - (b.linestarProj ?? 0); break;
        case "projOwnPct":  av = a.projOwnPct ?? -99; bv = b.projOwnPct ?? -99; break;
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

  async function handleLoadApi() {
    if (!contestId.trim()) { setUploadMsg({ ok: false, text: "Enter a DK contest ID" }); return; }
    startTransition(async () => {
      const cashLine = cashLineInput ? parseFloat(cashLineInput) : undefined;
      const res = await loadSlateFromContestId(contestId.trim(), isNaN(cashLine!) ? undefined : cashLine);
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

    // Build game → matchupId map for filtering
    const gameToMatchup = new Map<string, number>();
    for (const p of players) {
      if (p.matchupId != null) gameToMatchup.set(parseGameKey(p.gameInfo), p.matchupId);
    }
    const gameFilter = Array.from(selectedGames)
      .map((g) => gameToMatchup.get(g))
      .filter((id): id is number => id != null);

    const settings: OptimizerSettings = { mode, nLineups, minStack, maxExposure, bringBackThreshold };
    const res = await runOptimizer(players[0].slateId, gameFilter, settings);
    setIsOptimizing(false);
    if (!res.ok || !res.lineups) { setOptimizeError(res.error ?? "Optimizer failed"); return; }
    setLineups(res.lineups);
  }

  async function handleSave() {
    if (!lineups || !players[0]?.slateId) return;
    setSaveMsg(null);
    const res = await saveLineups(players[0].slateId, lineups, strategy);
    setSaveMsg(res.ok ? `Saved ${res.saved} lineups as "${strategy}"` : "Save failed");
  }

  async function handleExport() {
    if (!lineups) return;
    setIsExporting(true);
    const csvStr = await exportLineups(lineups, entryTemplate);
    setIsExporting(false);
    if (!csvStr) return;
    const blob = new Blob([csvStr], { type: "text/csv" });
    const url  = URL.createObjectURL(blob);
    const a    = document.createElement("a");
    a.href     = url;
    a.download = `dk_nba_lineups_${slateDate ?? "export"}.csv`;
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
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold">NBA DFS Optimizer</h1>
          {slateDate && <p className="text-sm text-gray-500">Latest slate: {slateDate} · {players.length} players</p>}
        </div>
      </div>

      {/* Load Slate Panel */}
      <div className="rounded-lg border bg-card p-4">
        <div className="flex items-center justify-between mb-3">
          <h2 className="text-sm font-semibold">Load Slate</h2>
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
                {cookieStatus.ok ? "✓ Valid" : "✗ Expired"} — {cookieStatus.message}
              </span>
            )}
          </div>

          <div className="flex flex-wrap items-end gap-3">
            <div>
              <label className="text-xs text-gray-500 block mb-1">
                LineStar CSV <span className="text-gray-400">(manual upload when cookie expired)</span>
              </label>
              <input ref={lsUploadRef} type="file" accept=".csv" className="text-sm" />
            </div>
            <button
              onClick={handleUploadLinestarCsv}
              disabled={isUploadingLs}
              className="rounded bg-purple-600 px-3 py-1.5 text-sm text-white hover:bg-purple-700 disabled:opacity-50"
            >
              {isUploadingLs ? "Uploading…" : "Upload LineStar CSV"}
            </button>
            {lsUploadMsg && (
              <span className={`text-sm ${lsUploadMsg.ok ? "text-green-700" : "text-red-600"}`}>
                {lsUploadMsg.text}
              </span>
            )}
          </div>
        </div>
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
          <button
            onClick={handleOptimize}
            disabled={isOptimizing || filteredPlayers.length === 0}
            className="rounded bg-green-600 px-5 py-1.5 text-sm font-medium text-white hover:bg-green-700 disabled:opacity-50"
          >
            {isOptimizing ? "Optimizing…" : "Optimize"}
          </button>
        </div>
        {optimizeError && <p className="mt-2 text-sm text-red-600">{optimizeError}</p>}
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
                  <SortHeader col="linestarProj" label="LS Proj" />
                  <SortHeader col="ourProj" label="Our Proj" />
                  <SortHeader col="delta" label="Delta" />
                  <SortHeader col="projOwnPct" label="Own%" />
                  <SortHeader col="ourLeverage" label="Leverage" />
                  <SortHeader col="value" label="Value" />
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100">
                {sortedPlayers.slice(0, 200).map((p) => {
                  const delta = p.ourProj != null && p.linestarProj != null
                    ? p.ourProj - p.linestarProj : null;
                  const value = p.ourProj != null ? p.ourProj / (p.salary / 1000) : null;
                  const pos = displayPos(p.eligiblePositions);
                  return (
                    <tr key={p.id} className={`hover:bg-gray-50 ${p.isOut ? "opacity-40 line-through" : ""}`}>
                      <td className="px-3 py-1.5">
                        <span className={`inline-block rounded px-1.5 py-0.5 text-xs font-medium ${posBadgeColor(p.eligiblePositions)}`}>
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
                      <td className="px-3 py-1.5 text-xs">{fmt1(p.linestarProj)}</td>
                      <td className="px-3 py-1.5 text-xs font-medium">{fmt1(p.ourProj)}</td>
                      <td className={`px-3 py-1.5 text-xs font-medium ${
                        delta == null ? "text-gray-400" : delta >= 2 ? "text-green-600" : delta <= -2 ? "text-red-500" : ""
                      }`}>
                        {delta != null ? (delta >= 0 ? "+" : "") + delta.toFixed(1) : "—"}
                      </td>
                      <td className="px-3 py-1.5 text-xs">{p.projOwnPct != null ? p.projOwnPct.toFixed(1) + "%" : "—"}</td>
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
      {lineups && lineups.length > 0 && (
        <div className="rounded-lg border bg-card p-4">
          <div className="flex items-center justify-between mb-3">
            <h2 className="text-sm font-semibold">
              {lineups.length} Lineups Generated
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
            {lineups.map((lineup, i) => (
              <div key={i} className="rounded border p-2 text-xs">
                <div className="flex items-center gap-4 mb-1 text-gray-500">
                  <span className="font-medium text-gray-800">#{i + 1}</span>
                  <span>Proj: <strong className="text-gray-800">{lineup.projFpts.toFixed(1)}</strong></span>
                  <span>Sal: <strong className="text-gray-800">${lineup.totalSalary.toLocaleString()}</strong></span>
                  <span>Lev: <strong className="text-gray-800">{lineup.leverageScore.toFixed(1)}</strong></span>
                </div>
                <div className="flex flex-wrap gap-1">
                  {(["PG","SG","SF","PF","C","G","F","UTIL"] as const).map((slot) => {
                    const p = lineup.slots[slot];
                    return p ? (
                      <span key={slot} className="inline-flex items-center gap-1 rounded bg-gray-100 px-1.5 py-0.5">
                        <span className="text-gray-400">{slot}</span>
                        {p.teamLogo && <img src={p.teamLogo} alt="" className="h-3 w-3" />}
                        <span className="font-medium">{p.name}</span>
                        <span className="text-gray-400">{fmt1(p.ourProj)}</span>
                      </span>
                    ) : null;
                  })}
                </div>
              </div>
            ))}
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
              placeholder="Entry ID,Contest Name,Contest ID,Entry Fee,PG,SG,SF,PF,C,G,F,UTIL&#10;12345,NBA..."
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
      )}

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
