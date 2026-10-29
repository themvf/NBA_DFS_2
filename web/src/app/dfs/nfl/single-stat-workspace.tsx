"use client";

/**
 * The workspace for a DraftKings Single Stat slate (`nfl-single-stat-v1`):
 * three picks, no cap, one stat. It replaces the salary-cap workspace for
 * those slates because nothing about salaries, captains, stacks or the
 * Classic slots applies; what is left is the stat projection, a stated
 * ownership prior, locks and excludes, and a three-pick build.
 */

import { AlertTriangle, CheckCircle2, Download, FileUp, Lock, Search, Unlock, XCircle } from "lucide-react";
import { useEffect, useMemo, useRef, useState, useTransition } from "react";
import { SINGLE_STAT_UNITS, SINGLE_STAT_LABELS, SINGLE_STAT_ROSTER_SIZE } from "@/lib/nfl-dfs/dk-salary-csv";
import { DEFAULT_SINGLE_STAT_SETTINGS, type SingleStatSettings } from "@/lib/nfl-dfs/single-stat";
import { countNflDkEntryRows } from "@/lib/nfl-dfs/entry-export";
import type { NflGeneratedLineup } from "./nfl-optimizer";
import { exportSavedNflSingleStatEntries, exportSavedNflSingleStatLineups, generateNflSingleStatLineups, listSavedNflRuns, loadSavedNflLineups, type NflSingleStatRunSettings, type NflWorkspaceSlate } from "./client-actions";

type SavedRun = Awaited<ReturnType<typeof listSavedNflRuns>>[number];
type SortKey = "projected" | "dkAvg" | "model" | "market" | "ownPct" | "name";

const fmt = (value: number | null | undefined, digits: number) => value == null || !Number.isFinite(value) ? "—" : value.toFixed(digits);
const pct = (value: number | null | undefined) => value == null ? "—" : `${value.toFixed(1)}%`;

function downloadText(name: string, content: string) {
  const url = URL.createObjectURL(new Blob([content], { type: "text/csv;charset=utf-8" }));
  const anchor = document.createElement("a"); anchor.href = url; anchor.download = name; anchor.click(); URL.revokeObjectURL(url);
}

function Metric({ label, value, small = false }: { label: string; value: string; small?: boolean }) {
  return <div className="rounded-xl border bg-white p-4 shadow-sm"><div className="text-[10px] font-bold uppercase text-slate-500">{label}</div><div className={`mt-1 font-black ${small ? "text-sm" : "text-2xl"}`}>{value}</div></div>;
}
function Notice({ children, good = false }: { children: React.ReactNode; good?: boolean }) {
  return <div role={good ? undefined : "alert"} className={`flex items-start gap-2 rounded-lg border p-3 text-sm ${good ? "border-emerald-200 bg-emerald-50 text-emerald-900" : "border-red-200 bg-red-50 text-red-900"}`}>{good ? <CheckCircle2 className="h-4 w-4 shrink-0" /> : <AlertTriangle className="h-4 w-4 shrink-0" />}{children}</div>;
}
function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return <label className="block text-xs font-bold text-slate-700"><span className="mb-1 block">{label}</span>{children}</label>;
}

const SOURCE_CHIP: Record<string, { text: string; className: string }> = {
  market: { text: "books", className: "border-indigo-200 bg-indigo-50 text-indigo-900" },
  model: { text: "model", className: "border-slate-300 bg-slate-50 text-slate-700" },
  dk_avg: { text: "DK avg", className: "border-amber-200 bg-amber-50 text-amber-900" },
  none: { text: "no projection", className: "border-red-200 bg-red-50 text-red-800" },
};

export default function SingleStatWorkspace({ slate, locked: slateLocked }: { slate: NflWorkspaceSlate; locked: boolean }) {
  const meta = slate.singleStat;
  const stat = meta?.stat ?? "yards";
  const unit = SINGLE_STAT_UNITS[stat];
  const digits = stat === "yards" ? 1 : 2;
  const entryRef = useRef<HTMLInputElement>(null);
  const [pending, startTransition] = useTransition();
  const [settings, setSettings] = useState<SingleStatSettings & { projectionSource: "our" | "dk_avg" }>({ ...DEFAULT_SINGLE_STAT_SETTINGS, projectionSource: "our" });
  const [locked, setLocked] = useState<number[]>([]);
  const [excluded, setExcluded] = useState<number[]>([]);
  const [query, setQuery] = useState("");
  const [sort, setSort] = useState<{ key: SortKey; dir: "asc" | "desc" }>({ key: "projected", dir: "desc" });
  const [lineups, setLineups] = useState<NflGeneratedLineup[]>([]);
  const [runId, setRunId] = useState<string | null>(null);
  const [runSettings, setRunSettings] = useState<NflSingleStatRunSettings | null>(null);
  const [warnings, setWarnings] = useState<string[]>([]);
  const [savedRuns, setSavedRuns] = useState<SavedRun[]>([]);
  const [message, setMessage] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [entryFile, setEntryFile] = useState<File | null>(null);
  const [entryRows, setEntryRows] = useState<number | null>(null);

  async function openRun(id: string) {
    const saved = await loadSavedNflLineups(slate.uploadId, id);
    const stored = saved.settings as unknown as Partial<NflSingleStatRunSettings>;
    if (stored.kind !== "single_stat") throw new Error("That saved run is a salary-cap run, not a Single Stat run.");
    setLineups(saved.lineups); setRunId(saved.runId); setRunSettings(stored as NflSingleStatRunSettings);
    setWarnings(saved.warnings ?? []);
    setSettings((current) => ({ ...current, mode: stored.mode ?? current.mode, nLineups: stored.nLineups ?? current.nLineups,
      maxExposure: stored.maxExposure ?? current.maxExposure, minUnique: stored.minUnique ?? current.minUnique,
      leverageExponent: stored.leverageExponent ?? current.leverageExponent, useOwnershipLeverage: stored.useOwnershipLeverage ?? current.useOwnershipLeverage,
      randomness: stored.randomness ?? current.randomness, seed: stored.seed ?? current.seed, projectionSource: stored.projectionSource ?? current.projectionSource }));
    setLocked(stored.lockedPlayerIds ?? []); setExcluded(stored.excludedPlayerIds ?? []);
  }

  // Each slate opens on its newest saved Single Stat run, if it has one.
  const uploadId = slate.uploadId;
  useEffect(() => {
    let cancelled = false;
    setLineups([]); setRunId(null); setRunSettings(null); setWarnings([]); setLocked([]); setExcluded([]); setMessage(null); setError(null);
    listSavedNflRuns(uploadId).then(async (runs) => {
      if (cancelled) return;
      setSavedRuns(runs);
      const newest = runs[0];
      if (!newest) return;
      try { await openRun(newest.runId); if (!cancelled) setMessage(`Restored the newest saved build (${newest.count} lineups).`); }
      catch { /* a salary-cap run on a re-labelled slate; nothing to restore */ }
    }).catch((reason) => { if (!cancelled) setError(reason instanceof Error ? reason.message : "Saved runs could not be listed."); });
    return () => { cancelled = true; };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [uploadId]);

  const rows = useMemo(() => slate.players.map((p) => ({ player: p, s: p.singleStat ?? null })), [slate]);
  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase();
    const value = (row: typeof rows[number]): number | string | null => {
      switch (sort.key) {
        case "name": return row.player.name;
        case "dkAvg": return row.player.avgFptsDk;
        case "model": return row.s?.model ?? null;
        case "market": return row.s?.market ?? null;
        case "ownPct": return row.player.ownPct ?? null;
        default: return row.s?.projected ?? null;
      }
    };
    return rows.filter((row) => !q || `${row.player.name} ${row.player.team} ${row.player.opponent ?? ""} ${row.player.position}`.toLowerCase().includes(q))
      .sort((a, b) => {
        const av = value(a), bv = value(b);
        if (av == null && bv == null) return a.player.name.localeCompare(b.player.name);
        if (av == null) return 1; if (bv == null) return -1;
        const cmp = typeof av === "string" ? av.localeCompare(String(bv)) : (av as number) - (bv as number);
        return sort.dir === "asc" ? cmp : -cmp;
      });
  }, [rows, query, sort]);
  function toggleSort(key: SortKey) { setSort((s) => s.key === key ? { key, dir: s.dir === "desc" ? "asc" : "desc" } : { key, dir: key === "name" ? "asc" : "desc" }); }
  const exposure = useMemo(() => {
    const map = new Map<number, { name: string; n: number }>();
    for (const l of lineups) for (const { player } of l.slots) { const row = map.get(player.dkPlayerId) ?? { name: player.name, n: 0 }; row.n++; map.set(player.dkPlayerId, row); }
    return [...map.values()].sort((a, b) => b.n - a.n || a.name.localeCompare(b.name));
  }, [lineups]);
  const projectedCount = rows.filter((r) => !r.player.isOut && r.s?.projected != null).length;

  function generate() {
    setError(null); setMessage(null);
    const payload = { ...settings, lockedPlayerIds: locked, excludedPlayerIds: excluded };
    startTransition(async () => {
      const response = await generateNflSingleStatLineups(slate.uploadId, payload);
      if (!response.ok) { setError(response.error); return; }
      setLineups(response.lineups); setRunId(response.runId); setRunSettings(response.settings); setWarnings(response.result.warnings);
      try { setSavedRuns(await listSavedNflRuns(slate.uploadId)); } catch { /* the list refreshes on the next open */ }
      setMessage(`Saved build ${response.runId.slice(0, 8)} with ${response.lineups.length}/${settings.nLineups} lineups.${response.result.leverageApplied ? " Ownership leverage applied (uncalibrated prior)." : ""}`);
    });
  }
  function selectEntryFile(file: File | null) {
    setEntryFile(file); setEntryRows(null);
    if (!file) return;
    file.text().then((text) => setEntryRows(countNflDkEntryRows(text)))
      .catch((reason) => setError(reason instanceof Error ? `That entry file can't be used: ${reason.message}` : "That entry file can't be read."));
  }
  function exportLineups() {
    if (!runId) { setError("Build lineups first; export reads the saved run."); return; }
    setError(null);
    startTransition(async () => {
      try { downloadText(`nfl-single-stat-${runId.slice(0, 8)}-rosters.csv`, await exportSavedNflSingleStatLineups(runId)); }
      catch (reason) { setError(reason instanceof Error ? reason.message : "Lineup export failed."); }
    });
  }
  function exportEntries() {
    if (!runId || !entryFile) return;
    setError(null);
    startTransition(async () => {
      try { downloadText(`nfl-single-stat-${runId.slice(0, 8)}.csv`, await exportSavedNflSingleStatEntries(runId, await entryFile.text())); }
      catch (reason) { setError(reason instanceof Error ? reason.message : "Entry export failed."); }
    });
  }
  const sortLabel = (key: SortKey, label: string) => <button type="button" onClick={() => toggleSort(key)} className="font-bold">{label}{sort.key === key ? (sort.dir === "desc" ? " ↓" : " ↑") : ""}</button>;

  return <div className="space-y-4">
    {message ? <Notice good>{message}</Notice> : null}{error ? <Notice>{error}</Notice> : null}
    <section className="grid grid-cols-2 gap-3 md:grid-cols-6">
      <Metric label="Format" value={meta ? SINGLE_STAT_LABELS[meta.stat] : "Single Stat"} small />
      <Metric label="Picks" value={`${SINGLE_STAT_ROSTER_SIZE} · no cap`} small />
      <Metric label="Players" value={String(slate.players.length)} />
      <Metric label="Games" value={String(slate.games.length)} />
      <Metric label="Projected" value={`${projectedCount}/${slate.players.filter((p) => !p.isOut).length}`} />
      <Metric label="As of" value={slate.modelAsOf ? new Date(slate.modelAsOf).toLocaleString() : "No run"} small />
    </section>
    <section className="rounded-xl border border-indigo-200 bg-indigo-50 p-4 text-sm text-indigo-950">
      <p><b>How this slate is projected.</b> DraftKings scores {stat === "yards" ? "combined rushing and receiving yards" : "touchdowns"} and sells no salaries: pick {SINGLE_STAT_ROSTER_SIZE} players, any team, any game. Each player&apos;s {SINGLE_STAT_LABELS[stat].toLowerCase()} comes from the sportsbooks&apos; calibrated lines when they were captured for his game ({meta?.marketSourced ?? 0} players), otherwise from the model&apos;s stat line ({meta?.modelSourced ?? 0}){meta?.missing ? `; ${meta.missing} have neither and cannot be rostered` : ""}. Ranges are the model&apos;s own points shape applied to the stat, an estimate.</p>
      <p className="mt-2"><b>Ownership is a stated prior, not a measurement.</b> With no prices the field drafts on DraftKings&apos; printed average and the names it knows, so ownership is spread by the cube of that average over three slots (cap 60%). No Single Stat contest has been imported to calibrate it; GPP leverage uses it only through the checkbox below and every run says so.</p>
    </section>
    {slate.warnings.length ? <details className="rounded-xl border bg-white p-4" open={!slateLocked}><summary className="cursor-pointer text-sm font-semibold">Slate notes</summary><div className="mt-2 space-y-2">{slate.warnings.map((w) => <Notice key={w}>{w}</Notice>)}</div></details> : null}

    <section className="grid gap-5 xl:grid-cols-[minmax(0,1fr)_320px]">
      <div className="space-y-4">
        <section className="rounded-xl border bg-white shadow-sm">
          <div className="flex flex-wrap items-end gap-3 border-b p-4"><div className="mr-auto"><h2 className="font-bold">Player pool</h2><p className="text-xs text-slate-500">Lock or exclude in the first column. Ruled-out players project 0 and are never rostered.</p></div>
            <label className="flex min-h-10 min-w-56 items-center gap-2 rounded-lg border px-3"><Search className="h-4 w-4 text-slate-400" /><input className="w-full text-sm outline-none" aria-label="Search players or teams" placeholder="Search player or team" value={query} onChange={(e) => setQuery(e.target.value)} /></label></div>
          <div className="max-h-[620px] overflow-auto"><table className="w-full text-sm"><thead className="sticky top-0 bg-slate-50 text-xs"><tr><th className="p-2 text-left">Rules</th><th className="p-2 text-left">{sortLabel("name", "Player")}</th><th className="p-2">Pos</th><th className="p-2">Team</th><th className="p-2">Status</th><th className="p-2 text-right">{sortLabel("dkAvg", `DK avg ${unit}`)}</th><th className="p-2 text-right">{sortLabel("model", `Model ${unit}`)}</th><th className="p-2 text-right">{sortLabel("market", `Books ${unit}`)}</th><th className="p-2 text-right">{sortLabel("projected", `Projected ${unit}`)}</th><th className="p-2 text-right">Range</th><th className="p-2 text-right">{sortLabel("ownPct", "Own%")}</th></tr></thead>
            <tbody>{filtered.length === 0 ? <tr><td colSpan={11} className="p-8 text-center text-slate-500">No players match.</td></tr> : filtered.map(({ player, s }) => {
              const isLocked = locked.includes(player.dkPlayerId), isExcluded = excluded.includes(player.dkPlayerId);
              const chip = SOURCE_CHIP[s?.source ?? "none"];
              return <tr key={player.dkPlayerId} className={`border-t ${player.isOut ? "bg-red-50 opacity-60" : isLocked ? "bg-emerald-50" : isExcluded ? "bg-slate-100 opacity-60" : ""}`}>
                <td className="p-2"><div className="flex gap-1">
                  <button disabled={slateLocked || player.isOut} aria-label={`Lock ${player.name}`} aria-pressed={isLocked} title="Lock" onClick={() => { setLocked((v) => isLocked ? v.filter((id) => id !== player.dkPlayerId) : [...v, player.dkPlayerId]); setExcluded((v) => v.filter((id) => id !== player.dkPlayerId)); }} className="rounded border p-1.5">{isLocked ? <Lock className="h-3.5 w-3.5 text-emerald-700" /> : <Unlock className="h-3.5 w-3.5" />}</button>
                  <button disabled={slateLocked} aria-label={`Exclude ${player.name}`} aria-pressed={isExcluded} title="Exclude" onClick={() => { setExcluded((v) => isExcluded ? v.filter((id) => id !== player.dkPlayerId) : [...v, player.dkPlayerId]); setLocked((v) => v.filter((id) => id !== player.dkPlayerId)); }} className="rounded border p-1.5"><XCircle className={`h-3.5 w-3.5 ${isExcluded ? "text-red-600" : ""}`} /></button></div></td>
                <td className="p-2 font-bold">{player.name}{!player.isOut ? <span title={s?.note ?? ""} className={`ml-2 inline-flex rounded-full border px-2 py-0.5 align-middle text-[10px] font-semibold ${chip.className}`}>{chip.text}</span> : null}{player.marketAnchor && s?.source === "market" ? <div className="text-[10px] font-normal text-slate-500">{player.marketAnchor.book} · captured {new Date(player.marketAnchor.capturedAt).toLocaleString("en-US", { timeZone: "America/New_York", weekday: "short", hour: "numeric", minute: "2-digit" })} ET</div> : null}</td>
                <td className="p-2 text-center">{player.position}</td>
                <td className="p-2 text-center">{player.team}<span className="text-slate-400"> {player.opponent ? `vs ${player.opponent}` : ""}</span></td>
                <td className="p-2 text-center text-xs">{player.isOut ? <span className="font-bold text-red-700">OUT</span> : player.dkStatus ?? ""}</td>
                <td className="p-2 text-right">{fmt(player.avgFptsDk, digits)}</td>
                <td className="p-2 text-right">{fmt(s?.model, digits)}</td>
                <td className="p-2 text-right">{fmt(s?.market, digits)}</td>
                <td className="p-2 text-right font-bold">{fmt(s?.projected, digits)}</td>
                <td className="p-2 text-right text-xs text-slate-500">{s?.p10 != null && s?.p90 != null ? `${fmt(s.p10, digits)}–${fmt(s.p90, digits)}` : "—"}</td>
                <td className="p-2 text-right">{pct(player.ownPct)}{player.ownPct != null ? <div className="text-[9px] text-slate-400">prior · uncalibrated</div> : null}</td>
              </tr>; })}</tbody></table></div>
          <p className="border-t p-3 text-xs text-slate-500">{filtered.length} players · {locked.length} locked · {excluded.length} excluded</p>
        </section>

        {lineups.length ? <section className="rounded-xl border bg-white p-4 shadow-sm">
          <div className="flex flex-wrap items-end justify-between gap-3"><div><h2 className="font-bold">Lineups <span className="text-xs font-normal text-slate-500">saved run {runId?.slice(0, 8)} · {runSettings ? `${runSettings.mode.toUpperCase()} · ${runSettings.projectionSource === "dk_avg" ? "DK average" : "our projection"}` : ""}</span></h2><p className="text-xs text-slate-500">Projected {SINGLE_STAT_LABELS[stat].toLowerCase()} per lineup; Own% sums the three players&apos; prior ownership.</p></div>
            <div className="flex flex-wrap items-end gap-2">
              <button disabled={pending || slateLocked} onClick={exportLineups} className="inline-flex min-h-10 items-center gap-2 rounded-lg border bg-white px-3 text-sm font-semibold"><Download className="h-4 w-4" />Lineups CSV</button>
              <input ref={entryRef} type="file" accept=".csv,text/csv" className="hidden" onChange={(e) => selectEntryFile(e.target.files?.[0] ?? null)} />
              <button disabled={pending || slateLocked} onClick={() => entryRef.current?.click()} className="inline-flex min-h-10 items-center gap-2 rounded-lg border bg-white px-3 text-sm font-semibold"><FileUp className="h-4 w-4" />{entryFile ? `${entryFile.name}${entryRows != null ? ` · ${entryRows} entries` : ""}` : "Choose DK entry file"}</button>
              <button disabled={pending || slateLocked || !entryFile} onClick={exportEntries} className="inline-flex min-h-10 items-center gap-2 rounded-lg bg-blue-600 px-4 text-sm font-bold text-white disabled:opacity-50"><Download className="h-4 w-4" />Write entries</button>
            </div></div>
          {warnings.length ? <div className="mt-3 space-y-2">{warnings.map((w) => <Notice key={w}>{w}</Notice>)}</div> : null}
          <div className="mt-3 grid gap-2 md:grid-cols-2">{lineups.map((l) => <div key={l.lineupNumber} className="rounded-lg border p-3 text-sm"><div className="flex justify-between font-semibold"><span>#{l.lineupNumber}</span><span>{fmt(l.projectedFpts, digits)} {unit}{l.projectedOwnership != null ? <span className="ml-2 font-normal text-slate-500">own {l.projectedOwnership.toFixed(0)}%</span> : null}</span></div><ul className="mt-1 space-y-0.5">{l.slots.map((s) => <li key={s.player.dkPlayerId} className="flex justify-between"><span>{s.player.name} <span className="text-slate-400">{s.player.position} {s.player.team}</span></span><span>{fmt(s.projection, digits)}</span></li>)}</ul></div>)}</div>
          <details className="mt-3 text-xs"><summary className="cursor-pointer font-semibold">Exposure over {lineups.length} lineups</summary><ul className="mt-1 grid gap-x-4 md:grid-cols-3">{exposure.map((e) => <li key={e.name} className="flex justify-between"><span>{e.name}</span><span>{e.n}/{lineups.length}</span></li>)}</ul></details>
        </section> : null}
      </div>

      <aside className="space-y-3 rounded-xl border bg-white p-4 shadow-sm">
        <h2 className="font-bold">Build</h2>
        {savedRuns.length ? <Field label="Saved run"><select className="control" value={runId ?? ""} disabled={pending} onChange={(e) => { if (!e.target.value) return; setError(null); startTransition(async () => { try { await openRun(e.target.value); setMessage("Saved lineups restored with the settings that built them."); } catch (reason) { setError(reason instanceof Error ? reason.message : "Saved lineups could not be loaded."); } }); }}><option value="" disabled>Choose a saved run</option>{savedRuns.map((r) => <option key={r.runId} value={r.runId}>{new Date(r.createdAt).toLocaleString()} · {r.count} lineups · {r.mode.toUpperCase()}</option>)}</select></Field> : null}
        <Field label="Contest type"><select className="control" disabled={slateLocked} value={settings.mode} onChange={(e) => setSettings({ ...settings, mode: e.target.value as "cash" | "gpp" })}><option value="gpp">GPP (leverage, spread)</option><option value="cash">Cash (top three, every lineup)</option></select></Field>
        <Field label="Projection"><select className="control" disabled={slateLocked} value={settings.projectionSource} onChange={(e) => setSettings({ ...settings, projectionSource: e.target.value as "our" | "dk_avg" })}><option value="our">Books + model</option><option value="dk_avg">DraftKings average</option></select></Field>
        <div className="grid grid-cols-2 gap-2">
          <Field label="Lineups"><input type="number" min={1} max={150} className="control" disabled={slateLocked} value={settings.nLineups} onChange={(e) => setSettings({ ...settings, nLineups: Math.max(1, Math.min(150, Number(e.target.value) || 1)) })} /></Field>
          <Field label="Max exposure %"><input type="number" min={1} max={100} className="control" disabled={slateLocked} value={Math.round(settings.maxExposure * 100)} onChange={(e) => setSettings({ ...settings, maxExposure: Math.max(1, Math.min(100, Number(e.target.value) || 1)) / 100 })} /></Field>
          <Field label="Min unique"><select className="control" disabled={slateLocked} value={settings.minUnique} onChange={(e) => setSettings({ ...settings, minUnique: Number(e.target.value) })}>{[1, 2, 3].map((n) => <option key={n} value={n}>{n}</option>)}</select></Field>
          <Field label="Randomness"><input type="number" min={0} max={0.5} step={0.02} className="control" disabled={slateLocked || settings.mode === "cash"} value={settings.randomness} onChange={(e) => setSettings({ ...settings, randomness: Math.max(0, Math.min(0.5, Number(e.target.value) || 0)) })} /></Field>
        </div>
        {settings.mode === "gpp" ? <label className="flex items-start gap-2 rounded-lg border border-amber-200 bg-amber-50 p-3 text-xs text-amber-950"><input type="checkbox" disabled={slateLocked} checked={settings.useOwnershipLeverage} onChange={(e) => setSettings({ ...settings, useOwnershipLeverage: e.target.checked })} /><span><b>Use the uncalibrated ownership prior for leverage</b><span className="mt-1 block">Scores each player as projected × (1 − own)<sup>{settings.leverageExponent}</sup>. Off: lineups rank on projection alone.</span></span></label> : null}
        <button disabled={pending || slateLocked} onClick={generate} className="w-full rounded-lg bg-emerald-600 py-2 text-sm font-bold text-white disabled:opacity-50">{pending ? "Building…" : "Generate & save"}</button>
        {slateLocked ? <p className="text-xs text-slate-500">The games have started, so building and export are closed.</p> : null}
        <p className="text-xs text-slate-500">Every build is saved with its frozen inputs. Nothing here has been graded against a DraftKings Single Stat result yet.</p>
      </aside>
    </section>
  </div>;
}
