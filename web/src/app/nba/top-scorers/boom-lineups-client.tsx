"use client";
import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import { BOOM_POLICY, LINEUPS, buildBoomLineups, type BoomBoard } from "@/lib/nba-boom-lineups";

/**
 * Boom lineups on the Top Scorers page: give a rising-shot-volume player more of
 * the five three-player lineups, see what it costs in projected points, and save
 * the comparison before the first tip so /nba/boom-trios can grade it.
 */
export function BoomLineups({ board }: { board: BoomBoard | null }) {
  const [selected, setSelected] = useState("");
  const [requested, setRequested] = useState(2);
  const [maxDrop, setMaxDrop] = useState(5);
  const [now, setNow] = useState<number | null>(null);
  const [saving, setSaving] = useState(false);
  const [receipt, setReceipt] = useState("");
  useEffect(() => { const update = () => setNow(Date.now()); update(); const timer = setInterval(update, 1000); return () => clearInterval(timer); }, []);
  const result = useMemo(() => {
    if (!board) return null;
    try { return buildBoomLineups(board, selected || null, requested, maxDrop); } catch { return null; }
  }, [board, selected, requested, maxDrop]);
  if (!board || !result) return (
    <section className="rounded-xl border border-slate-700 bg-[#0f1726] p-5 text-slate-200">
      <h2 className="font-semibold text-white">Boom lineups</h2>
      <p className="mt-1 text-sm">No frozen pregame snapshot for this date yet. It is taken at 11:37 AM ET on game days.</p>
    </section>
  );
  const locked = now === null || now >= Date.parse(board.lockAt);
  const players = new Map(board.players.map((p) => [p.id, p]));
  const featured = players.get(selected);
  const normal = selected ? result.baseline.filter((t) => t.ids.includes(selected)).length : 0;
  const candidates = board.players.filter((p) => p.boomRank !== null).sort((a, b) => (a.boomRank as number) - (b.boomRank as number));
  const stamp = (s: string) => new Date(s).toLocaleString("en-US", { timeZone: "America/New_York", month: "short", day: "numeric", hour: "numeric", minute: "2-digit", timeZoneName: "short" });
  const data = { version: BOOM_POLICY, generatedAt: new Date().toISOString(), timing: locked ? "after_lock_what_if" : "unrecorded_preview", board, settings: { selected: selected || null, requested, maxDrop }, ...result };
  const reset = () => setReceipt("");
  const download = () => {
    const url = URL.createObjectURL(new Blob([JSON.stringify(data, null, 2)], { type: "application/json" }));
    const a = document.createElement("a"); a.href = url; a.download = `nba-boom-${board.date}-${Date.now()}.json`; a.click(); setTimeout(() => URL.revokeObjectURL(url), 1000);
  };
  const save = async () => {
    setSaving(true); setReceipt("");
    try {
      const response = await fetch("/api/nba/boom-lineups", { method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ date: board.date, snapshot: board.snapshot, selected: selected || null, requested, maxDrop }) });
      const r = await response.json(); if (!response.ok) throw new Error(r.error || "Could not save");
      setReceipt(`Saved before the first tip: ${String(r.id).slice(0, 12)} · ${stamp(r.createdAt)}. Graded on Boom Trios after the games.`);
    } catch (e) { setReceipt(e instanceof Error ? e.message : "Could not save"); } finally { setSaving(false); }
  };
  return (
    <section className="space-y-4 rounded-xl border border-slate-700 bg-[#0f1726] p-5 text-slate-200" aria-label="Boom lineup builder">
      <div>
        <h2 className="text-xl font-semibold text-white">Boom lineups <span className="text-sm text-amber-300">· experimental</span></h2>
        <p className="mt-1 text-sm">Give a rising-shot-volume player more places in five three-player lineups. Nobody&apos;s projection changes; lineup 1 is always the three highest lines.</p>
        <p className="mt-1 text-xs text-slate-400">Frozen {stamp(board.frozenAt)} · first tip {stamp(board.lockAt)} · {board.players.length} players with a line · {candidates.length} rising-volume players · points = market line</p>
        <p className="mt-2 text-sm text-amber-200">Boom rank = shot attempts per game (free throws × 0.44) over the last 3 games minus the last 20, at least 2. It is a sign of more opportunity, not a probability. Availability is only as current as the freeze. <Link href={`/nba/boom-trios?date=${board.date}`} className="underline">See the tracked boom trios</Link>.</p>
      </div>
      <div className="flex flex-wrap items-end gap-4">
        <label className="min-w-0 flex-1 text-sm">Player to include more often
          <select className="mt-1 block w-full rounded border bg-slate-900 p-2" value={selected} onChange={(e) => { setSelected(e.target.value); reset(); }}>
            <option value="">Choose a player · top lineups only</option>
            {candidates.map((p) => <option key={p.id} value={p.id}>Boom #{p.boomRank} {p.name} · line {p.points.toFixed(1)} (#{p.pointRank}) · {p.shotsLast3?.toFixed(1)} vs {p.shotsLast20?.toFixed(1)} shots</option>)}
          </select>
        </label>
        <label className="text-sm">Minimum inclusion
          <select className="mt-1 block rounded border bg-slate-900 p-2" value={requested} onChange={(e) => { setRequested(Number(e.target.value)); reset(); }}>{[0, 1, 2, 3, 4, 5].map((n) => <option key={n} value={n}>{n} of {LINEUPS} lineups</option>)}</select>
        </label>
        <label className="text-sm">Max points below best trio
          <select className="mt-1 block rounded border bg-slate-900 p-2" value={maxDrop} onChange={(e) => { setMaxDrop(Number(e.target.value)); reset(); }}>{[0, 3, 5, 10, 15, 20].map((n) => <option key={n} value={n}>{n} points</option>)}</select>
        </label>
      </div>
      {featured && <div className="rounded bg-slate-800 p-3 text-sm">
        <strong>{featured.name}: {normal}/{LINEUPS} → {result.achieved}/{LINEUPS}</strong> · asked for at least {requested}/{LINEUPS}. Line {featured.points.toFixed(1)} (#{featured.pointRank} on the slate), boom #{featured.boomRank}.
        <p>Shot attempts a game: {featured.shotsLast3?.toFixed(1) ?? "?"} over his last 3 games, {featured.shotsLast20?.toFixed(1) ?? "?"} over his last 20. Availability: {featured.availability}.</p>
        <button className="mt-2 rounded border px-3 py-1 disabled:opacity-40" disabled={requested >= LINEUPS} onClick={() => { setRequested(Math.min(LINEUPS, Math.max(requested, result.achieved) + 1)); reset(); }}>Include in one more lineup</button>
      </div>}
      {!result.feasible && <p role="status" className="text-amber-300">{result.reason} Raise the points limit or lower the inclusion. No points are added to make it fit.</p>}
      <ol className="space-y-2">{result.adjusted.map((t, i) => <li key={t.key} className="rounded border border-slate-700 p-3">
        <div className="font-semibold">{i + 1}. {t.ids.map((id) => players.get(id)!.name).join(" · ")}</div>
        <div className="text-sm text-slate-400">{t.total.toFixed(1)} projected · {(result.baseline[0].total - t.total).toFixed(1)} below the best trio{selected && t.ids.includes(selected) ? " · includes your player" : ""}</div>
      </li>)}</ol>
      <p className="text-sm">Projected across all five lineups: top five {result.baseline.reduce((s, t) => s + t.total, 0).toFixed(1)} → yours {result.adjusted.reduce((s, t) => s + t.total, 0).toFixed(1)}.</p>
      <div className="flex flex-wrap gap-3">
        <button disabled={locked || saving || !result.feasible} onClick={save} className="rounded bg-sky-600 px-4 py-2 text-white disabled:opacity-40">{saving ? "Saving…" : "Save before the first tip"}</button>
        <button onClick={download} className="rounded border px-4 py-2">Download</button>
      </div>
      {locked && <p className="text-sm text-amber-300">The first tip has passed: preview and download only. A save now would not be a pregame record.</p>}
      <p role="status" className="text-sm">{receipt}</p>
      <details className="text-xs text-slate-400"><summary>The five highest-projected lineups and tracking details</summary>
        <p>Policy {BOOM_POLICY}; snapshot {board.snapshot.slice(0, 12)}. A save keeps both sets; it is your choice, separate from the frozen boom-trio rules. A player who doesn&apos;t play leaves that lineup ungraded, never scored as 0.</p>
        {result.baseline.map((t, i) => <p key={t.key}>{i + 1}. {t.ids.map((id) => players.get(id)!.name).join(" / ")} — {t.total.toFixed(1)}</p>)}
      </details>
    </section>
  );
}
