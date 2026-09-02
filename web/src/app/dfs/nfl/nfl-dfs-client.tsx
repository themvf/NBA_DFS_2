"use client";

import { AlertTriangle, CheckCircle2, FileUp, LockKeyhole, Search, ShieldCheck } from "lucide-react";
import { useMemo, useRef, useState } from "react";
import {
  NflDkCsvError,
  parseNflDkSalaryCsv,
  playablePlayers,
  type NflDkSlate,
  type NflPosition,
} from "@/lib/nfl-dfs/dk-salary-csv";

const POSITION_ORDER: NflPosition[] = ["QB", "RB", "WR", "TE", "K", "DST"];

function dollars(value: number): string {
  return new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", maximumFractionDigits: 0 }).format(value);
}

function number(value: number | null): string {
  return value == null ? "—" : value.toFixed(1);
}

export default function NflDfsClient() {
  const inputRef = useRef<HTMLInputElement>(null);
  const [slate, setSlate] = useState<NflDkSlate | null>(null);
  const [fileName, setFileName] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [query, setQuery] = useState("");
  const [position, setPosition] = useState<"ALL" | NflPosition>("ALL");
  const [showUnavailable, setShowUnavailable] = useState(false);

  const filtered = useMemo(() => {
    const normalized = query.trim().toLowerCase();
    return (slate?.players ?? [])
      .filter((player) => showUnavailable || !player.isOut)
      .filter((player) => position === "ALL" || player.position === position)
      .filter((player) => !normalized || `${player.name} ${player.teamAbbrev} ${player.opponent ?? ""}`.toLowerCase().includes(normalized))
      .sort((a, b) => (b.avgFptsDk ?? -1) - (a.avgFptsDk ?? -1) || b.salary - a.salary);
  }, [position, query, showUnavailable, slate]);

  async function loadFile(file: File | null) {
    if (!file) return;
    setError(null);
    try {
      const parsed = parseNflDkSalaryCsv(await file.text());
      setSlate(parsed);
      setFileName(file.name);
      setQuery("");
      setPosition("ALL");
    } catch (reason) {
      setSlate(null);
      setFileName(null);
      setError(reason instanceof NflDkCsvError || reason instanceof Error ? reason.message : "The salary file could not be parsed.");
    }
  }

  const playable = slate ? playablePlayers(slate.players).length : 0;

  return (
    <div className="mx-auto max-w-7xl space-y-5 p-4 sm:p-6">
      <header className="rounded-2xl border border-slate-200 bg-gradient-to-br from-slate-950 via-blue-950 to-emerald-950 p-6 text-white shadow-lg">
        <div className="flex flex-wrap items-start justify-between gap-5">
          <div>
            <div className="text-xs font-bold uppercase tracking-[0.2em] text-emerald-300">DraftKings · NFL DFS</div>
            <h1 className="mt-2 text-3xl font-black tracking-tight">NFL DFS Workspace</h1>
            <p className="mt-2 max-w-3xl text-sm text-slate-300">Audit a DraftKings Classic or Showdown salary pool. The projection model and optimizer remain locked until their prospective validation gates pass.</p>
          </div>
          <span className="rounded-full border border-amber-300/40 bg-amber-300/10 px-3 py-1.5 text-xs font-bold text-amber-200">INTAKE / READINESS</span>
        </div>
      </header>

      <section className="grid gap-3 md:grid-cols-4">
        <div className="rounded-xl border border-emerald-200 bg-emerald-50 p-4 text-emerald-950"><CheckCircle2 className="h-5 w-5" /><h2 className="mt-2 font-bold">Salary intake</h2><p className="mt-1 text-xs">Classic and Showdown parser verified against real DK exports.</p></div>
        <div className="rounded-xl border border-emerald-200 bg-emerald-50 p-4 text-emerald-950"><ShieldCheck className="h-5 w-5" /><h2 className="mt-2 font-bold">DK scoring</h2><p className="mt-1 text-xs">Offense, DST, kicker, bonuses, and captain rules verified.</p></div>
        <div className="rounded-xl border border-amber-200 bg-amber-50 p-4 text-amber-950"><LockKeyhole className="h-5 w-5" /><h2 className="mt-2 font-bold">Our projections</h2><p className="mt-1 text-xs">Locked pending props, weekly features, and walk-forward accuracy.</p></div>
        <div className="rounded-xl border border-amber-200 bg-amber-50 p-4 text-amber-950"><LockKeyhole className="h-5 w-5" /><h2 className="mt-2 font-bold">Lineup optimizer</h2><p className="mt-1 text-xs">Locked until the model passes position-level validation.</p></div>
      </section>

      <section className="rounded-xl border border-slate-200 bg-white p-5 shadow-sm">
        <div className="flex flex-wrap items-center justify-between gap-4">
          <div><h2 className="font-bold text-slate-950">Load a DK salary CSV</h2><p className="mt-1 text-sm text-slate-600">The file is parsed in your browser and is not saved to the database.</p></div>
          <input ref={inputRef} type="file" accept=".csv,text/csv" className="hidden" onChange={(event) => void loadFile(event.target.files?.[0] ?? null)} />
          <button type="button" onClick={() => inputRef.current?.click()} className="inline-flex min-h-11 items-center gap-2 rounded-lg bg-blue-600 px-4 py-2 text-sm font-bold text-white hover:bg-blue-500"><FileUp className="h-4 w-4" />{slate ? "Replace salary file" : "Upload salary file"}</button>
        </div>
        {fileName ? <p className="mt-3 text-xs font-semibold text-emerald-700">Loaded {fileName}</p> : null}
        {error ? <div role="alert" className="mt-4 flex items-start gap-2 rounded-lg border border-red-200 bg-red-50 p-3 text-sm text-red-900"><AlertTriangle className="mt-0.5 h-4 w-4 shrink-0" />{error}</div> : null}
      </section>

      {slate ? <>
        <section className="grid grid-cols-2 gap-3 md:grid-cols-5">
          {[['Format', slate.format.toUpperCase()], ['Players', String(slate.players.length)], ['Playable', String(playable)], ['Games', String(slate.games.length)], ['Teams', String(slate.teams.length)]].map(([label, value]) => <div key={label} className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm"><div className="text-[10px] font-bold uppercase tracking-wide text-slate-500">{label}</div><div className="mt-1 text-2xl font-black text-slate-950">{value}</div></div>)}
        </section>

        {slate.warnings.length ? <details className="rounded-xl border border-amber-200 bg-amber-50 p-4 text-amber-950"><summary className="cursor-pointer font-bold">Import warnings ({slate.warnings.length})</summary><ul className="mt-3 list-disc space-y-1 pl-5 text-sm">{slate.warnings.map((warning, index) => <li key={`${warning}-${index}`}>{warning}</li>)}</ul></details> : null}

        <section className="rounded-xl border border-slate-200 bg-white shadow-sm">
          <div className="flex flex-wrap items-center gap-2 border-b border-slate-200 p-4">
            <label className="flex min-h-10 min-w-[230px] flex-1 items-center gap-2 rounded-lg border border-slate-200 px-3"><Search className="h-4 w-4 text-slate-400" /><input value={query} onChange={(event) => setQuery(event.target.value)} placeholder="Search player or team" className="w-full bg-transparent text-sm outline-none" /></label>
            <select value={position} onChange={(event) => setPosition(event.target.value as "ALL" | NflPosition)} className="min-h-10 rounded-lg border border-slate-200 bg-white px-3 text-sm font-semibold"><option value="ALL">All positions</option>{POSITION_ORDER.map((item) => <option key={item} value={item}>{item}</option>)}</select>
            <label className="flex min-h-10 items-center gap-2 rounded-lg border border-slate-200 px-3 text-xs font-semibold"><input type="checkbox" checked={showUnavailable} onChange={(event) => setShowUnavailable(event.target.checked)} />Show OUT / IR</label>
          </div>
          <div className="overflow-x-auto"><table className="min-w-[820px] w-full text-xs"><thead className="bg-slate-50 text-left text-[10px] uppercase tracking-wide text-slate-500"><tr><th className="px-3 py-3">Player</th><th className="px-3 py-3">Pos</th><th className="px-3 py-3">Team</th><th className="px-3 py-3">Opponent</th><th className="px-3 py-3">Game</th><th className="px-3 py-3 text-right">Salary</th><th className="px-3 py-3 text-right">DK Avg</th><th className="px-3 py-3">Status</th></tr></thead><tbody>{filtered.map((player) => <tr key={`${player.dkPlayerId}-${player.teamAbbrev}`} className="border-t border-slate-100"><td className="px-3 py-3 font-bold text-slate-950">{player.name}</td><td className="px-3 py-3">{player.position}</td><td className="px-3 py-3 font-semibold">{player.teamAbbrev}</td><td className="px-3 py-3">{player.opponent ?? "—"}</td><td className="px-3 py-3">{player.gameKey ?? "—"}</td><td className="px-3 py-3 text-right tabular-nums">{dollars(player.salary)}</td><td className="px-3 py-3 text-right tabular-nums">{number(player.avgFptsDk)}</td><td className="px-3 py-3"><span className={`rounded-full px-2 py-1 text-[10px] font-bold ${player.isOut ? "bg-red-100 text-red-800" : player.status ? "bg-amber-100 text-amber-800" : "bg-emerald-100 text-emerald-800"}`}>{player.status ?? "ACTIVE"}</span></td></tr>)}</tbody></table>{!filtered.length ? <div className="p-10 text-center text-sm text-slate-500">No players match these filters.</div> : null}</div>
        </section>
      </> : <section className="rounded-xl border border-dashed border-slate-300 bg-slate-50 p-12 text-center"><FileUp className="mx-auto h-8 w-8 text-slate-400" /><h2 className="mt-3 font-bold text-slate-900">No NFL slate loaded</h2><p className="mt-1 text-sm text-slate-600">Download a salary CSV from a DraftKings NFL Classic or Showdown contest, then upload it above.</p></section>}
    </div>
  );
}
