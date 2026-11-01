"use client";

import Link from "next/link";
import { useEffect, useMemo, useState, useTransition } from "react";
import { stringifyCsvLine } from "@/app/dfs/csv";
import { parseCbbSalaries, type CbbMode, type CbbSlate } from "@/lib/cbb-dfs/salary-csv";
import { importCbbEstimates, projectCbbHistory, type CbbEstimates } from "@/lib/cbb-dfs/projection";
import type { CbbLineup } from "@/lib/cbb-dfs/optimizer";
import { buildBasketballLineups } from "./actions";
import { CbbDfsProps } from "../props/props-board";

const STORAGE_KEY = "cbb-dfs-workspace-v1";
const inputClass = "rounded-md border bg-background px-3 py-2 text-sm";
const buttonClass = "rounded-lg border px-4 py-2 text-sm font-medium hover:bg-accent disabled:opacity-40";
function download(name: string, text: string, type = "text/csv") {
  const url = URL.createObjectURL(new Blob([text], { type }));
  const a = document.createElement("a"); a.href = url; a.download = name; a.click();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}
const unavailable = (status: string) => ["O", "OUT", "IR", "SUSP", "SUSPENDED", "INACTIVE"].includes(status.toUpperCase());
const fmt = (n: number | null | undefined, digits = 1) => n == null ? "—" : n.toFixed(digits);

export default function CbbDfsClient() {
  const [mode, setMode] = useState<CbbMode>("classic");
  const [salaryCsv, setSalaryCsv] = useState("");
  const [slate, setSlate] = useState<CbbSlate | null>(null);
  const [fileName, setFileName] = useState("");
  const [estimates, setEstimates] = useState<CbbEstimates>({});
  const [lineups, setLineups] = useState<CbbLineup[]>([]);
  const [count, setCount] = useState(1);
  const [query, setQuery] = useState("");
  const [position, setPosition] = useState("ALL");
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");
  const [ready, setReady] = useState(false);
  const [pending, startTransition] = useTransition();

  useEffect(() => {
    try {
      const raw = localStorage.getItem(STORAGE_KEY);
      if (raw) {
        const saved = JSON.parse(raw);
        if (saved.mode !== "classic" && saved.mode !== "showdown") throw new Error("Unknown saved mode.");
        const parsed = parseCbbSalaries(saved.salaryCsv, saved.mode);
        setMode(saved.mode); setSalaryCsv(saved.salaryCsv); setSlate(parsed);
        setFileName(saved.fileName); setEstimates(saved.estimates ?? {});
      }
    } catch { setMessage("The previous workspace could not be restored. Import a salary file to start again."); }
    setReady(true);
  }, []);
  useEffect(() => {
    if (!ready || !salaryCsv) return;
    try { localStorage.setItem(STORAGE_KEY, JSON.stringify({ mode, salaryCsv, fileName, estimates })); }
    catch { setMessage("Browser storage is unavailable. Download an input snapshot to keep your work."); }
  }, [ready, mode, salaryCsv, fileName, estimates]);

  const players = useMemo(() => (slate?.players ?? []).filter(p => (position === "ALL" || p.positions.includes(position)) && `${p.name} ${p.team}`.toLowerCase().includes(query.toLowerCase())).sort((a, b) => (estimates[b.id]?.projection ?? -1) - (estimates[a.id]?.projection ?? -1) || b.salary - a.salary), [slate, position, query, estimates]);
  const projected = slate?.players.filter(p => estimates[p.id]?.projection != null).length ?? 0;
  const owned = slate?.players.filter(p => estimates[p.id]?.ownership != null).length ?? 0;
  const isLocked = !!slate && Date.parse(slate.firstTipoff) <= Date.now();

  async function upload(file: File | null, kind: "salary" | "projection" | "history") {
    if (!file) return;
    setError(""); setMessage("");
    try {
      if (file.size > 2_000_000) throw new Error("Use a CSV smaller than 2 MB.");
      const text = await file.text();
      if (kind === "salary") {
        const parsed = parseCbbSalaries(text, mode);
        setSlate(parsed); setSalaryCsv(text); setFileName(file.name); setEstimates({}); setLineups([]); setPosition("ALL"); setQuery("");
        setMessage(`Loaded ${parsed.players.length} players. The salary CSV cannot verify the sport: confirm this is a men's college basketball slate.`);
      } else {
        if (!slate) throw new Error("Import salaries first.");
        const next = kind === "projection" ? importCbbEstimates(text, slate) : projectCbbHistory(text, slate);
        setEstimates(next); setLineups([]);
        setMessage(`Loaded ${Object.keys(next).length} player estimates. This replaces previous estimates. Review coverage, availability, and expected minutes before building.`);
      }
    } catch (reason) { setError(reason instanceof Error ? reason.message : "Could not read the file."); }
  }

  function template(kind: "projection" | "history") {
    if (!slate) return;
    const header = kind === "projection" ? ["ID", "Projection", "Minutes", "Fppm", "Bonus", "Ownership", "CaptainOwnership", "Source", "AsOf"] : ["ID", "Team", "GameID", "PlayedAt", "CapturedAt", "Minutes", "PTS", "3PM", "REB", "AST", "STL", "BLK", "TO", "Source"];
    const rows = slate.players.map(p => kind === "projection" ? [p.id, "", "", "", "", "", "", "", ""] : [p.id, p.team, ...Array(12).fill("")]);
    download(`cbb-${kind}-template.csv`, [header, ...rows].map(stringifyCsvLine).join("\r\n"));
  }

  function edit(id: string, field: "projection" | "minutes", raw: string) {
    const e = estimates[id];
    if (!e || !slate || Date.parse(slate.firstTipoff) <= Date.now()) return;
    const value = raw.trim() ? Number(raw) : null;
    if (value !== null && (!Number.isFinite(value) || value < 0 || value > (field === "minutes" ? 40 : 500))) return;
    setEstimates(prev => ({ ...prev, [id]: { ...e, [field]: value,
      projection: field === "minutes" ? value !== null && e.rate !== null ? Math.max(0, value * e.rate + e.bonus) : null : value,
      source: e.source.replace(/; manual edit$/, "") + "; manual edit", asOf: new Date().toISOString() } }));
    setLineups([]);
  }

  function build() {
    setError(""); setMessage(""); setLineups([]);
    startTransition(async () => {
      try {
        const result = await buildBasketballLineups(salaryCsv, mode, estimates, count);
        setLineups(result);
        setMessage(`Built ${result.length} of ${count} requested lineups, ranked by projected fantasy points. Each differs by at least one player. Ownership is displayed for review and does not affect selection.`);
      } catch (reason) { setError(reason instanceof Error ? reason.message : "Could not build lineups."); }
    });
  }

  return <>
    <header className="flex flex-wrap items-start justify-between gap-4">
      <div><Link href="/cbb" className="text-sm text-muted-foreground hover:underline">College Basketball</Link><h1 className="mt-1 text-3xl font-bold">DraftKings CBB DFS</h1><p className="mt-2 text-sm text-muted-foreground">Men’s basketball · Classic & Showdown · Projection workspace</p></div>
      <span className="rounded-full bg-amber-100 px-3 py-1 text-xs font-semibold text-amber-900">Exploratory projections</span>
    </header>
    <CbbDfsProps />
    <section className="rounded-xl border bg-card p-5">
      <h2 className="font-semibold">1. Load your slate</h2>
      <div className="mt-3 flex flex-wrap items-end gap-4">
        <label className="grid gap-1 text-sm">Contest format<select aria-label="Contest format" disabled={pending} value={mode} className={inputClass} onChange={e => { setMode(e.target.value as CbbMode); setSlate(null); setSalaryCsv(""); setFileName(""); setEstimates({}); setLineups([]); setPosition("ALL"); setQuery(""); setError(""); setMessage(""); try { localStorage.removeItem(STORAGE_KEY); } catch { /* optional */ } }}><option value="classic">Classic</option><option value="showdown">Showdown Captain</option></select></label>
        <label className="grid gap-1 text-sm">DraftKings salary CSV<input disabled={pending} type="file" accept=".csv" className={inputClass} onChange={e => { void upload(e.target.files?.[0] ?? null, "salary"); e.target.value = ""; }} /></label>
      </div>
      <p className="mt-3 text-sm text-muted-foreground">{mode === "classic" ? "G · G · G · F · F · F · UTIL · UTIL, at least two games." : "CPT · UTIL · UTIL · UTIL · UTIL · UTIL, both teams from one game. Captain costs and scores 1.5×; both salary rows are required."} $50,000 cap.</p>
      <p className="mt-2 text-xs text-muted-foreground">Your current inputs save in this browser. No shared slate history, live injury feed, or late swap is connected.</p>
    </section>
    {error && <p role="alert" className="rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-800">{error}</p>}
    {message && <p role="status" className="rounded-lg border border-blue-200 bg-blue-50 p-4 text-sm text-blue-900">{message}</p>}
    {slate ? <>
      <div className="grid gap-3 sm:grid-cols-4">{[["Slate", fileName], ["Player projections", `${projected} / ${slate.players.length}`], ["Ownership inputs", `${owned} / ${slate.players.length}`], [isLocked ? "Slate started" : "First tipoff (ET)", new Date(slate.firstTipoff).toLocaleString("en-US", { timeZone: "America/New_York" })]].map(([label, value]) => <div key={label} className="rounded-xl border p-4"><p className="text-xs uppercase text-muted-foreground">{label}</p><p className="mt-2 break-words font-semibold">{value}</p></div>)}</div>
      {isLocked && <p className="rounded-lg bg-amber-50 p-4 text-sm text-amber-900">This slate has started. Inputs can be reviewed, but building and editing projections are disabled. New captures cannot establish pre-lock evidence.</p>}
      <section className="space-y-3 rounded-xl border bg-card p-5">
        <h2 className="font-semibold">2. Add projections</h2>
        <p className="text-sm text-muted-foreground">Import a projection file, or calculate a baseline from mapped box scores. Each import replaces previous estimates. Players without a projection stay out of the builder.</p>
        <div className="grid gap-4 md:grid-cols-2">
          <div className="space-y-2 rounded-lg border p-4"><h3 className="text-sm font-semibold">Projection & ownership CSV</h3><p className="text-xs leading-5 text-muted-foreground">Use Projection, or Minutes × Fppm + Bonus. Ownership and CaptainOwnership are optional percentages (0–100). Fppm excludes double/triple-double bonuses. Source and AsOf are required. Use utility IDs for Showdown; projections are before the Captain multiplier.</p><input aria-label="Import CBB projections" disabled={pending} type="file" accept=".csv" className="block w-full text-sm" onChange={e => { void upload(e.target.files?.[0] ?? null, "projection"); e.target.value = ""; }} /><button className={buttonClass} onClick={() => template("projection")}>Download template</button></div>
          <div className="space-y-2 rounded-lg border p-4"><h3 className="text-sm font-semibold">Box-score baseline</h3><p className="text-xs leading-5 text-muted-foreground">Last 5 played games’ minutes × last 20 games’ base Fppm + average bonus. Uses this season and team only; DNPs are excluded. IDs must be mapped explicitly. PlayedAt and CapturedAt require ISO timestamps with timezone before lock.</p><input aria-label="Import CBB box scores" disabled={pending} type="file" accept=".csv" className="block w-full text-sm" onChange={e => { void upload(e.target.files?.[0] ?? null, "history"); e.target.value = ""; }} /><button className={buttonClass} onClick={() => template("history")}>Download template</button></div>
        </div>
        <p className="text-xs text-amber-800">The baseline has no opponent, pace, availability, or rotation adjustment and has not been validated. No current-season history means no estimate. Imported ownership is an external input, not an internally validated forecast. In Showdown, Ownership means utility share; CaptainOwnership means Captain share.</p>
      </section>
      <section className="overflow-hidden rounded-xl border">
        <div className="flex flex-wrap items-center gap-3 p-4"><h2 className="mr-auto font-semibold">Player pool</h2><input aria-label="Search CBB players" className={inputClass} placeholder="Search player or team" value={query} onChange={e => setQuery(e.target.value)} /><select aria-label="Position filter" className={inputClass} value={position} onChange={e => setPosition(e.target.value)}><option value="ALL">All positions</option>{mode === "classic" && <><option>G</option><option>F</option></>}</select></div>
        <div className="overflow-x-auto"><table className="w-full whitespace-nowrap text-left text-sm"><thead className="border-y bg-muted/40 text-xs text-muted-foreground"><tr>{["Use", "Player / source", "Team", "Pos", "Salary", "DK avg*", "Minutes", "Fppm", "Projection", mode === "showdown" ? "UTIL %" : "Own %", ...(mode === "showdown" ? ["CPT %"] : [])].map(h => <th key={h} className="px-3 py-3 font-medium">{h}</th>)}</tr></thead><tbody>{players.map(p => {
          const e = estimates[p.id]; const out = unavailable(p.status);
          return <tr key={p.id} className="border-b last:border-0"><td className="px-3 py-3"><input type="checkbox" aria-label={`Include ${p.name}`} disabled={!e || out || pending || isLocked} checked={!!e && !e.excluded && !out} onChange={event => { setEstimates(prev => ({ ...prev, [p.id]: { ...prev[p.id], excluded: !event.target.checked } })); setLineups([]); }} /></td>
            <td className="px-3 py-3"><div className="font-medium">{p.name} {p.status && <span className="text-xs text-amber-700">{p.status}</span>}</div><div className="max-w-64 truncate text-xs text-muted-foreground" title={e ? `${e.source} · ${e.asOf}` : "No projection"}>{e ? `${e.source} · ${e.method === "history" ? `${e.games} games` : "import"}` : "No projection"}</div>{e && <div className="text-xs text-muted-foreground">As of {new Date(e.asOf).toLocaleString("en-US", { timeZone: "America/New_York" })} ET</div>}</td>
            <td className="px-3">{p.team} vs {p.opponent}</td><td className="px-3">{p.positions.join("/")}</td><td className="px-3">${p.salary.toLocaleString()}</td><td className="px-3 text-muted-foreground">{fmt(p.dkAverage)}</td>
            <td className="px-3"><input aria-label={`${p.name} expected minutes`} className="w-20 rounded border p-1" type="number" min="0" max="40" step="0.1" disabled={!e || e.rate === null || pending || isLocked} value={e?.minutes ?? ""} onChange={event => edit(p.id, "minutes", event.target.value)} /></td><td className="px-3">{fmt(e?.rate, 2)}</td>
            <td className="px-3"><input aria-label={`${p.name} projection`} className="w-20 rounded border p-1" type="number" min="0" max="500" step="0.1" disabled={!e || pending || isLocked} value={e?.projection == null ? "" : Number(e.projection.toFixed(2))} onChange={event => edit(p.id, "projection", event.target.value)} /></td><td className="px-3">{fmt(e?.ownership)}</td>{mode === "showdown" && <td className="px-3">{fmt(e?.captainOwnership)}</td>}
          </tr>;
        })}</tbody></table></div>
        <p className="p-3 text-xs text-muted-foreground">*DK average is the salary file’s historical average, not our projection. Blank ownership means unknown. Salary-file statuses are not live availability confirmation. Review questionable players manually.</p>
      </section>
      <section className="space-y-4 rounded-xl border p-5"><h2 className="font-semibold">3. Build lineups</h2><div className="flex flex-wrap items-end gap-3"><label className="grid gap-1 text-sm">Lineups (1–20)<input className={`${inputClass} w-24`} type="number" min="1" max="20" value={count} disabled={pending} onChange={e => setCount(Number(e.target.value))} /></label><button onClick={build} disabled={pending || isLocked || projected === 0} className="rounded-lg bg-orange-500 px-5 py-2 font-semibold text-white disabled:opacity-40">{pending ? "Building…" : "Build lineups"}</button><button className={buttonClass} onClick={() => download("cbb-input-snapshot.json", JSON.stringify({ version: "cbb-baseline-v1", exportedAt: new Date().toISOString(), mode, salaryCsv, estimates, lineups }, null, 2), "application/json")}>Download input snapshot</button>
        {lineups.length > 0 && <button className={buttonClass} onClick={() => download(`cbb-${mode}-lineups.csv`, [lineups[0].slots.map(s => s.slot), ...lineups.map(l => l.slots.map(s => `${s.player.name} (${s.id})`))].map(stringifyCsvLine).join("\r\n"))}>Download lineup CSV</button>}</div>
        <p className="text-xs text-muted-foreground">Maximizes supplied fantasy-point projections with at least one different player per lineup. This is not a contest-profit simulation or an exposure-managed portfolio. The CSV contains roster columns only; it does not fill an existing DraftKings entries file.</p>
        <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">{lineups.map((lineup, i) => <article key={i} className="rounded-lg border p-4"><div className="mb-3 flex justify-between text-sm font-semibold"><span>Lineup {i + 1}</span><span>${lineup.salary.toLocaleString()} · {lineup.projection.toFixed(1)} pts</span></div>{lineup.slots.map(s => <div key={s.id} className="flex justify-between gap-3 border-t py-2 text-sm"><span><span className="mr-2 text-xs font-semibold text-orange-700">{s.slot}</span>{s.player.name}</span><span>{s.projection.toFixed(1)}</span></div>)}</article>)}</div>
      </section>
    </> : <div className="rounded-xl border border-dashed p-10 text-center"><p className="text-lg font-semibold">Start with a DraftKings salary file</p><p className="mt-2 text-sm text-muted-foreground">Choose Classic or Showdown above, then import the matching men’s college basketball slate.</p></div>}
    <details className="rounded-xl border p-5"><summary className="cursor-pointer font-semibold">Projection & ownership roadmap</summary><div className="mt-3 space-y-3 text-sm leading-6 text-muted-foreground"><p>Production projections need mapped box scores, complete team rotations, expected minutes totaling 200 per regulation game, current availability, transfers, pace, opponent efficiency, and overtime/blowout context. Candidate source: <a className="text-blue-600 underline" href="https://api.collegebasketballdata.com/api/games" target="_blank" rel="noreferrer">CollegeBasketballData</a>. Live box-score imports are not connected yet.</p><p>Ownership training needs complete contest standings, contest size and entry fee, slate salaries, and frozen pregame projections. Fit and evaluate Classic selection share separately from Showdown utility and Captain share. Until then, ownership is import-only; blank values do not mean low ownership.</p><p>Acceptance requires chronological holdouts: fantasy-point and minutes error against simple baselines, plus ownership percentage error and calibration by slate/contest type. Archive pregame inputs and compare with verified postgame results. Simulation size alone does not validate predictions.</p></div></details>
  </>;
}
