"use client";

import { useEffect, useMemo, useRef, useState, useTransition } from "react";
import {
  exportCfbRun, generateCfbLineups, listCfbSlates, loadCfbRun, loadCfbWorkspace, refreshCfbProjections, uploadCfbSlate,
  type CfbSlateSummary, type CfbWorkspace,
} from "./actions";
import { DEFAULT_CFB_SETTINGS, type CfbLineup } from "@/lib/cfb-dfs/settings";
import { exportCfbDkEntries } from "@/lib/cfb-dfs/entry-export";

const STORAGE_KEY = "cfb-dfs-slate";
const dollars = (n: number) => `$${n.toLocaleString()}`;
const pts = (n: number | null | undefined) => (n == null ? "—" : n.toFixed(1));
const POSITIONS = ["ALL", "QB", "RB", "WR"] as const;
const STATUS_STYLE: Record<string, string> = {
  Q: "bg-amber-100 text-amber-900", D: "bg-red-100 text-red-800", O: "bg-red-100 text-red-800", OUT: "bg-red-100 text-red-800",
};

export default function CfbDfsClient({ initialUploadId }: { initialUploadId: string | null }) {
  const [slates, setSlates] = useState<CfbSlateSummary[]>([]);
  const [uploadId, setUploadId] = useState<string | null>(null);
  const [workspace, setWorkspace] = useState<CfbWorkspace | null>(null);
  const [lineups, setLineups] = useState<CfbLineup[]>([]);
  const [runId, setRunId] = useState<string | null>(null);
  const [message, setMessage] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [pending, startTransition] = useTransition();
  const fileRef = useRef<HTMLInputElement>(null);
  const entryRef = useRef<HTMLInputElement>(null);
  const [entryFile, setEntryFile] = useState<File | null>(null);

  const [settings, setSettings] = useState({ nLineups: DEFAULT_CFB_SETTINGS.nLineups, maxExposurePct: 70, minUnique: 2, randomnessPct: 18, minSalary: 45000,
    requireTwoQbs: false, stackQb: false, bringBack: false });
  const [locked, setLocked] = useState<number[]>([]);
  const [excluded, setExcluded] = useState<number[]>([]);
  const [maxById, setMaxById] = useState<Record<string, number>>({});
  const [position, setPosition] = useState<(typeof POSITIONS)[number]>("ALL");
  const [query, setQuery] = useState("");
  const [showZero, setShowZero] = useState(false);

  function fail(reason: unknown) { setError(reason instanceof Error ? reason.message : "Something went wrong."); }

  function open(id: string) {
    setError(null);
    startTransition(async () => {
      try {
        const next = await loadCfbWorkspace(id);
        setUploadId(id); setWorkspace(next); setLocked([]); setExcluded([]); setMaxById({});
        try { localStorage.setItem(STORAGE_KEY, id); } catch { /* optional */ }
        if (next.runs[0]) { setRunId(next.runs[0].runId); setLineups(await loadCfbRun(id, next.runs[0].runId)); }
        else { setRunId(null); setLineups([]); }
      } catch (reason) { fail(reason); }
    });
  }

  useEffect(() => {
    let live = true;
    listCfbSlates().then((list) => {
      if (!live) return;
      setSlates(list);
      let remembered: string | null = null;
      try { remembered = localStorage.getItem(STORAGE_KEY); } catch { /* use latest */ }
      const pick = initialUploadId ?? list.find((s) => s.uploadId === remembered)?.uploadId ?? list[0]?.uploadId;
      if (pick) open(pick);
    }).catch(fail);
    return () => { live = false; };
    // open() is stable enough for a mount-time load; re-running on its identity would loop.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [initialUploadId]);

  function upload(file: File | null) {
    if (!file) return;
    const form = new FormData(); form.set("file", file);
    setError(null); setMessage(null);
    startTransition(async () => {
      try {
        const { uploadId: id, reused } = await uploadCfbSlate(form);
        setSlates(await listCfbSlates());
        setMessage(reused ? "That file was already uploaded; projections refreshed." : `Uploaded ${file.name} and projected every player.`);
        open(id);
      } catch (reason) { fail(reason); }
      if (fileRef.current) fileRef.current.value = "";
    });
  }

  function refresh() {
    if (!uploadId) return;
    startTransition(async () => {
      try { setWorkspace(await refreshCfbProjections(uploadId)); setMessage("Projections refreshed from the latest box scores and lines."); }
      catch (reason) { fail(reason); }
    });
  }

  function generate() {
    if (!uploadId) return;
    setError(null); setMessage(null);
    startTransition(async () => {
      try {
        const result = await generateCfbLineups(uploadId, {
          nLineups: settings.nLineups, maxExposure: settings.maxExposurePct / 100, minUnique: settings.minUnique,
          randomness: settings.randomnessPct / 100, minSalary: settings.minSalary, lockedIds: locked, excludedIds: excluded, maxExposureById: maxById,
          requireTwoQbs: settings.requireTwoQbs, stackQb: settings.stackQb, bringBack: settings.bringBack,
        });
        setLineups(result.lineups); setRunId(result.runId);
        setWorkspace(await loadCfbWorkspace(uploadId));
        setMessage(result.stoppedEarly ?? `Built and saved ${result.lineups.length} lineups.`);
      } catch (reason) { fail(reason); }
    });
  }

  function download() {
    if (!uploadId || !runId) return;
    startTransition(async () => {
      try {
        const text = await exportCfbRun(uploadId, runId);
        saveText(`cfb-dk-lineups-${runId.slice(0, 8)}.csv`, text);
      } catch (reason) { fail(reason); }
    });
  }

  function saveText(name: string, text: string) {
    const url = URL.createObjectURL(new Blob([text], { type: "text/csv" }));
    const a = document.createElement("a"); a.href = url; a.download = name; a.click();
    URL.revokeObjectURL(url);
  }

  /** Fill DraftKings' Edit Entries file with these lineups, entry by entry. */
  async function exportEntries() {
    if (!entryFile || !lineups.length) return;
    setError(null);
    try {
      const result = exportCfbDkEntries(await entryFile.text(), lineups);
      saveText(`cfb-dk-entries-${runId?.slice(0, 8) ?? "export"}.csv`, result.csv);
      setMessage(`Filled ${result.filled} of ${result.entries} entries. Upload the file on DraftKings' Edit Entries page.`
        + (result.filled < result.entries ? ` ${result.entries - result.filled} entries were left as they were.` : ""));
    } catch (reason) { fail(reason); }
  }

  function openRun(id: string) {
    if (!uploadId) return;
    startTransition(async () => {
      try { setLineups(await loadCfbRun(uploadId, id)); setRunId(id); } catch (reason) { fail(reason); }
    });
  }

  const players = useMemo(() => workspace?.players ?? [], [workspace]);
  const visible = useMemo(() => players
    .filter((p) => position === "ALL" || p.position === position)
    .filter((p) => showZero || (p.proj ?? 0) > 0 || locked.includes(p.dkId))
    .filter((p) => !query.trim() || `${p.name} ${p.team}`.toLowerCase().includes(query.trim().toLowerCase()))
    .sort((a, b) => (b.proj ?? 0) - (a.proj ?? 0) || b.salary - a.salary), [players, position, showZero, query, locked]);
  const hiddenZero = players.filter((p) => (p.proj ?? 0) <= 0).length;

  const exposure = useMemo(() => {
    const counts = new Map<string, number>();
    for (const l of lineups) for (const s of l.slots) counts.set(`${s.player.name} (${s.player.team})`, (counts.get(`${s.player.name} (${s.player.team})`) ?? 0) + 1);
    return [...counts].sort((a, b) => b[1] - a[1]);
  }, [lineups]);

  const teamMap = workspace?.slate.teamMap ?? {};
  const unresolved = Object.entries(teamMap).filter(([, t]) => !t.team);

  return <div className="space-y-4">
    <header className="flex flex-wrap items-end justify-between gap-3">
      <div>
        <p className="text-xs font-bold uppercase tracking-wide text-orange-700">DraftKings · College Football · Classic</p>
        <h1 className="text-2xl font-bold tracking-tight">CFB DFS <span className="ml-2 rounded-full bg-amber-100 px-2 py-0.5 align-middle text-xs font-bold text-amber-900">Unvalidated baseline</span></h1>
        <p className="text-sm text-slate-500">QB, RB, RB, WR, WR, WR, FLEX (RB/WR), SUPER FLEX (QB/RB/WR) · $50,000 · at least 2 games</p>
      </div>
      <div className="flex flex-wrap items-end gap-2">
        <label className="text-xs font-semibold text-slate-600">Slate
          <select className="mt-1 block min-h-10 min-w-80 rounded-lg border bg-white px-3 text-sm" value={uploadId ?? ""} disabled={pending || !slates.length}
            onChange={(e) => open(e.target.value)}>
            {!slates.length ? <option value="">No slates yet</option> : null}
            {slates.map((s) => <option key={s.uploadId} value={s.uploadId}>{s.label}</option>)}
          </select>
        </label>
        <input ref={fileRef} type="file" accept=".csv,text/csv" className="hidden" onChange={(e) => upload(e.target.files?.[0] ?? null)} />
        <button disabled={pending} onClick={() => fileRef.current?.click()} className="min-h-10 rounded-lg bg-orange-600 px-4 text-sm font-bold text-white disabled:opacity-50">Upload salaries</button>
        <button disabled={pending || !uploadId} onClick={refresh} className="min-h-10 rounded-lg border bg-white px-4 text-sm font-semibold disabled:opacity-50">Refresh projections</button>
      </div>
    </header>

    {message ? <p className="rounded-lg border border-emerald-200 bg-emerald-50 p-3 text-sm text-emerald-900">{message}</p> : null}
    {error ? <p role="alert" className="rounded-lg border border-red-300 bg-red-50 p-3 text-sm text-red-900">{error}</p> : null}
    {pending ? <p className="text-sm text-slate-500">Working…</p> : null}

    {!workspace ? <section className="rounded-xl border border-dashed bg-white p-10 text-center text-sm text-slate-600">
      Upload a DraftKings College Football Classic salary CSV to start. NFL files are refused here; use the NFL DFS page for those.
    </section> : <>
      <section className="rounded-xl border bg-white p-4 shadow-sm">
        <div className="flex flex-wrap items-center gap-2 text-xs">
          <span className="font-bold text-slate-600">Teams</span>
          {Object.entries(teamMap).map(([code, t]) => <span key={code} className={`rounded-full border px-2 py-1 ${t.team ? "bg-slate-50" : "border-red-300 bg-red-50 text-red-800"}`}
            title={t.team ? `${t.hits} players matched to ${t.team}'s 2026 box scores` : "Not matched to a school; history by name only, no line adjustment"}>
            {code} → {t.team ?? "unmatched"}{t.implied != null ? ` · ${t.implied.toFixed(1)} implied` : " · no line"}
          </span>)}
        </div>
        <p className="mt-2 text-xs text-slate-500">
          Projections {workspace.slate.projectionVersion ?? "not run"}{workspace.slate.projectedAt ? `, ${new Date(workspace.slate.projectedAt).toLocaleString()}` : ""}.
          Built from 2026 box scores (2025 counts about as much as one 2026 game) and scaled toward each team&apos;s implied total. The weights are judgment, not fitted to results.
          {unresolved.length ? ` ${unresolved.length} team${unresolved.length === 1 ? "" : "s"} could not be matched.` : ""}
        </p>
      </section>

      <section className="grid gap-4 xl:grid-cols-[minmax(0,1fr)_320px]">
        <div className="rounded-xl border bg-white shadow-sm">
          <div className="flex flex-wrap items-end gap-3 border-b p-4">
            <h2 className="mr-auto font-bold">Player pool</h2>
            <input aria-label="Search players" placeholder="Search player or team" value={query} onChange={(e) => setQuery(e.target.value)} className="min-h-10 rounded-lg border px-3 text-sm" />
            <select aria-label="Position" value={position} onChange={(e) => setPosition(e.target.value as (typeof POSITIONS)[number])} className="min-h-10 rounded-lg border bg-white px-3 text-sm">
              {POSITIONS.map((p) => <option key={p} value={p}>{p === "ALL" ? "All positions" : p}</option>)}
            </select>
            <label className="flex items-center gap-1 text-xs"><input type="checkbox" checked={showZero} onChange={(e) => setShowZero(e.target.checked)} />Show {hiddenZero} with no projection</label>
          </div>
          <p className="border-b px-4 py-2 text-xs text-slate-500">{locked.length} locked · {excluded.length} excluded · {Object.keys(maxById).length} exposure caps. Max % blank = the global cap.</p>
          <div className="max-h-[640px] overflow-auto">
            <table className="w-full text-left text-sm">
              <thead className="sticky top-0 bg-slate-100 text-[10px] uppercase text-slate-500"><tr>
                <th className="p-2">Lock / Out</th><th className="p-2">Max %</th><th className="p-2">Player</th><th className="p-2">Pos</th>
                <th className="p-2 text-right">Salary</th><th className="p-2 text-right">Proj</th><th className="p-2 text-right">Pts/$1k</th>
                <th className="p-2 text-right">DK avg</th><th className="p-2 text-right">2026 G</th><th className="p-2">Match</th>
              </tr></thead>
              <tbody>{visible.map((p) => {
                const isLocked = locked.includes(p.dkId), isOut = excluded.includes(p.dkId);
                return <tr key={p.dkId} className={`border-t ${isLocked ? "bg-emerald-50" : isOut ? "bg-slate-100 opacity-60" : ""}`}>
                  <td className="p-2"><div className="flex gap-1">
                    <button aria-pressed={isLocked} onClick={() => { setLocked((v) => isLocked ? v.filter((id) => id !== p.dkId) : [...v, p.dkId]); setExcluded((v) => v.filter((id) => id !== p.dkId)); }}
                      className={`rounded border px-2 py-1 text-xs ${isLocked ? "border-emerald-500 bg-emerald-100 font-bold" : ""}`}>Lock</button>
                    <button aria-pressed={isOut} onClick={() => { setExcluded((v) => isOut ? v.filter((id) => id !== p.dkId) : [...v, p.dkId]); setLocked((v) => v.filter((id) => id !== p.dkId)); }}
                      className={`rounded border px-2 py-1 text-xs ${isOut ? "border-red-400 bg-red-100 font-bold" : ""}`}>Out</button>
                  </div></td>
                  <td className="p-2"><input aria-label={`${p.name} maximum exposure`} type="number" min={0} max={100} placeholder="—" value={maxById[String(p.dkId)] ?? ""}
                    onChange={(e) => setMaxById((cur) => { const next = { ...cur }; if (e.target.value === "") delete next[String(p.dkId)]; else next[String(p.dkId)] = Math.max(0, Math.min(100, Number(e.target.value))); return next; })}
                    className="h-8 w-14 rounded border px-1 text-right text-xs" /></td>
                  <td className="p-2"><span className="font-semibold">{p.name}</span>
                    {p.status ? <span className={`ml-1 rounded px-1 text-[10px] font-bold ${STATUS_STYLE[p.status] ?? "bg-slate-100"}`}>{p.status}</span> : null}
                    <div className="text-[11px] text-slate-500">{p.team} vs {p.opponent}</div></td>
                  <td className="p-2">{p.position}</td>
                  <td className="p-2 text-right">{dollars(p.salary)}</td>
                  <td className="p-2 text-right font-bold">{pts(p.proj)}</td>
                  <td className="p-2 text-right text-slate-600">{p.proj ? (p.proj / (p.salary / 1000)).toFixed(2) : "—"}</td>
                  <td className="p-2 text-right text-slate-600">{pts(p.dkAvg)}</td>
                  <td className="p-2 text-right">{p.games2026 ?? 0}</td>
                  <td className="p-2 text-[11px] text-slate-500">{p.matchMethod === "team+surname" ? "nickname" : p.matchMethod ?? "none"}</td>
                </tr>;
              })}</tbody>
            </table>
          </div>
        </div>

        <aside className="space-y-4">
          <section className="rounded-xl border bg-white p-4 shadow-sm">
            <h2 className="font-bold">Build lineups</h2>
            <p className="mt-1 text-xs text-slate-600">{settings.nLineups} lineups · max {settings.maxExposurePct}% · {locked.length} locked · {excluded.length} out{[
              settings.requireTwoQbs ? "2 QBs" : null, settings.stackQb ? "QB stacks" : null, settings.bringBack ? "bring-backs" : null,
            ].filter(Boolean).map((t) => ` · ${t}`).join("")}</p>
            <button disabled={pending} onClick={generate} className="mt-3 min-h-11 w-full rounded-lg bg-emerald-700 text-sm font-bold text-white disabled:opacity-40">{pending ? "Working…" : "Generate & save"}</button>
            <div className="mt-4 grid grid-cols-2 gap-2 text-xs font-semibold text-slate-600">
              <label>Lineups<input type="number" min={1} max={150} value={settings.nLineups} onChange={(e) => setSettings({ ...settings, nLineups: Number(e.target.value) })} className="mt-1 h-9 w-full rounded border px-2" /></label>
              <label>Max exposure %<input type="number" min={1} max={100} value={settings.maxExposurePct} onChange={(e) => setSettings({ ...settings, maxExposurePct: Number(e.target.value) })} className="mt-1 h-9 w-full rounded border px-2" /></label>
              <label>Min unique<input type="number" min={1} max={7} value={settings.minUnique} onChange={(e) => setSettings({ ...settings, minUnique: Number(e.target.value) })} className="mt-1 h-9 w-full rounded border px-2" /></label>
              <label>Randomness %<input type="number" min={0} max={50} value={settings.randomnessPct} onChange={(e) => setSettings({ ...settings, randomnessPct: Number(e.target.value) })} className="mt-1 h-9 w-full rounded border px-2" /></label>
              <label className="col-span-2">Min salary used<input type="number" step={500} value={settings.minSalary} onChange={(e) => setSettings({ ...settings, minSalary: Number(e.target.value) })} className="mt-1 h-9 w-full rounded border px-2" /></label>
            </div>
            <fieldset className="mt-4 space-y-2 rounded-lg border bg-slate-50 p-3 text-xs">
              <legend className="px-1 font-bold text-slate-700">Tournament rules</legend>
              <label className="flex items-start gap-2"><input type="checkbox" className="mt-0.5" checked={settings.requireTwoQbs} onChange={(e) => setSettings({ ...settings, requireTwoQbs: e.target.checked })} />
                <span><b>Require 2 QBs</b><span className="block text-slate-500">The SUPER FLEX always goes to a second QB.</span></span></label>
              <label className="flex items-start gap-2"><input type="checkbox" className="mt-0.5" checked={settings.stackQb} onChange={(e) => setSettings({ ...settings, stackQb: e.target.checked })} />
                <span><b>Stack each QB</b><span className="block text-slate-500">Every QB comes with at least one of his own WRs or RBs.</span></span></label>
              <label className="flex items-start gap-2"><input type="checkbox" className="mt-0.5" checked={settings.bringBack} onChange={(e) => setSettings({ ...settings, bringBack: e.target.checked })} />
                <span><b>Bring-back</b><span className="block text-slate-500">Every QB also comes with a WR or RB from the team he is playing.</span></span></label>
            </fieldset>
          </section>
          {workspace.runs.length ? <section className="rounded-xl border bg-white p-4 shadow-sm">
            <h2 className="font-bold">Saved lineup sets</h2>
            <select className="mt-2 min-h-10 w-full rounded-lg border bg-white px-2 text-sm" value={runId ?? ""} onChange={(e) => openRun(e.target.value)}>
              {workspace.runs.map((r) => <option key={r.runId} value={r.runId}>{new Date(r.createdAt).toLocaleString()} · {r.lineupCount} lineups</option>)}
            </select>
          </section> : null}
        </aside>
      </section>

      {lineups.length ? <section className="rounded-xl border bg-white p-4 shadow-sm">
        <div className="flex flex-wrap items-center justify-between gap-2">
          <h2 className="font-bold">Lineups ({lineups.length})</h2>
        </div>
        <div className="mt-3 rounded-lg border border-blue-200 bg-blue-50 p-3">
          <h3 className="text-sm font-bold text-blue-950">DraftKings export</h3>
          <p className="mt-1 text-xs text-blue-900">Enter the contest on DraftKings, then download its Edit Entries file. Your entry template supplies the entry IDs; these lineups fill its roster columns in order, one lineup per entry.</p>
          <input ref={entryRef} type="file" accept=".csv,text/csv" className="hidden" onChange={(e) => setEntryFile(e.target.files?.[0] ?? null)} />
          <div className="mt-2 flex flex-wrap gap-2">
            <button onClick={() => entryRef.current?.click()} className="min-h-10 rounded-lg border border-blue-300 bg-white px-4 text-sm font-semibold">{entryFile?.name ?? "Select entry template"}</button>
            <button disabled={!entryFile || !lineups.length} onClick={() => void exportEntries()} className="min-h-10 rounded-lg bg-blue-600 px-4 text-sm font-bold text-white disabled:opacity-40">Export lineups</button>
            <button disabled={pending || !runId} onClick={download} className="min-h-10 rounded-lg border bg-white px-4 text-sm text-slate-700 disabled:opacity-40" title="Player IDs only, in slot order; for DraftKings' upload of new lineups">Plain ID CSV</button>
          </div>
        </div>
        <div className="mt-3 grid gap-4 lg:grid-cols-[minmax(0,1fr)_280px]">
          <div className="max-h-[520px] overflow-auto">
            <table className="w-full text-left text-xs">
              <thead className="sticky top-0 bg-slate-100 uppercase text-slate-500"><tr>
                <th className="p-2">#</th>{["QB", "RB", "RB", "WR", "WR", "WR", "FLEX", "S-FLEX"].map((s, i) => <th key={i} className="p-2">{s}</th>)}
                <th className="p-2 text-right">Salary</th><th className="p-2 text-right">Proj</th>
              </tr></thead>
              <tbody>{lineups.map((l) => <tr key={l.lineupNumber} className="border-t">
                <td className="p-2">{l.lineupNumber}</td>
                {l.slots.map((s, i) => <td key={i} className="p-2"><div className="font-semibold">{s.player.name}</div><div className="text-slate-400">{s.player.team} · {dollars(s.player.salary)}</div></td>)}
                <td className="p-2 text-right">{dollars(l.salary)}</td><td className="p-2 text-right font-bold">{l.projection.toFixed(1)}</td>
              </tr>)}</tbody>
            </table>
          </div>
          <div>
            <h3 className="text-sm font-bold">Exposure</h3>
            <div className="mt-2 max-h-[480px] space-y-1 overflow-auto text-xs">{exposure.map(([name, n]) => <div key={name}>
              <div className="flex justify-between"><span>{name}</span><b>{n}/{lineups.length}</b></div>
              <div className="h-1.5 rounded bg-slate-100"><div className="h-full rounded bg-orange-500" style={{ width: `${(n / lineups.length) * 100}%` }} /></div>
            </div>)}</div>
          </div>
        </div>
      </section> : null}
    </>}
  </div>;
}
