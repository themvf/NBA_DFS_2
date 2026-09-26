"use client";

import { useState, useTransition } from "react";
import { searchCfbStarterNews } from "./actions";
import type { TeamNews } from "@/lib/cfb-dfs/x-news";

const FLAG_STYLE: Record<string, string> = {
  starter: "bg-violet-100 text-violet-900", out: "bg-red-100 text-red-800",
  "doubtful/GTD": "bg-amber-100 text-amber-900", injury: "bg-orange-100 text-orange-900",
};
const ago = (iso: string) => {
  const m = Math.max(0, Math.round((Date.now() - Date.parse(iso)) / 60_000));
  return m < 60 ? `${m}m ago` : m < 48 * 60 ? `${Math.round(m / 60)}h ago` : `${Math.round(m / 1440)}d ago`;
};
const reach = (n: number | null) => (n == null ? "?" : n >= 1000 ? `${Math.round(n / 1000)}K` : String(n));

/** Starter and injury posts from X, per team. Evidence to read before lock, not a status. */
export default function CfbNewsPanel({ uploadId }: { uploadId: string }) {
  const [teams, setTeams] = useState<TeamNews[] | null>(null);
  const [meta, setMeta] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [flaggedOnly, setFlaggedOnly] = useState(true);
  const [pending, start] = useTransition();

  function search() {
    setError(null);
    start(async () => {
      try {
        const r = await searchCfbStarterNews(uploadId);
        setTeams(r.teams);
        setMeta(`Searched ${new Date(r.searchedAt).toLocaleTimeString([], { hour: "numeric", minute: "2-digit" })} · ${r.postsRead} posts read (≈$${(r.postsRead * 0.00015).toFixed(3)}) · last 72 hours`);
      } catch (reason) { setError(reason instanceof Error ? reason.message : String(reason)); }
    });
  }

  const shown = (teams ?? []).map((t) => ({ ...t, posts: flaggedOnly ? t.posts.filter((p) => p.flags.length) : t.posts }));
  return <section className="rounded-xl border bg-white p-4 shadow-sm">
    <div className="flex flex-wrap items-center gap-2 text-xs">
      <span className="text-sm font-bold text-slate-700">Starter news from X</span>
      <button disabled={pending} onClick={search} className="rounded border bg-white px-2 py-1 font-semibold disabled:opacity-50">
        {pending ? "Searching…" : teams ? "Search again" : "Search X"}</button>
      {teams ? <label className="flex items-center gap-1"><input type="checkbox" checked={flaggedOnly} onChange={(e) => setFlaggedOnly(e.target.checked)} />
        flagged posts only</label> : null}
      {meta ? <span className="text-slate-500">{meta}</span> : null}
    </div>
    <p className="mt-1 text-xs text-slate-500">
      Each team&apos;s QBs and any player DraftKings tags Q/D/O. Posts are flagged by phrases like &quot;will start&quot;, &quot;doubtful&quot;, &quot;ruled out&quot;; nothing here changes a status or a projection.
      Read the newest posts from high-reach reporters, and lock or exclude players yourself. On 2026-09-25 X had Gutierrez starting 90 minutes before lock while DraftKings still showed Woodson Q.
    </p>
    {error ? <p role="alert" className="mt-2 text-sm text-red-800">{error}</p> : null}
    {teams ? <div className="mt-3 grid gap-3 lg:grid-cols-2">
      {shown.map((t) => <div key={t.code} className="rounded-lg border p-2">
        <div className="flex flex-wrap items-baseline gap-2 text-xs">
          <span className="font-bold text-slate-800">{t.code} · {t.school}</span>
          <span className="text-slate-500">{t.players.join(", ") || "no QBs listed"}</span>
          {t.error ? <span className="text-red-700">search failed: {t.error}</span> : null}
        </div>
        {t.posts.length ? <ul className="mt-1 space-y-1">
          {t.posts.map((p) => <li key={p.id} className="text-xs leading-snug">
            <span className="text-slate-500">{ago(p.at)} · </span>
            <a href={p.url ?? `https://x.com/${p.user}`} target="_blank" rel="noreferrer" className={`font-semibold ${(p.followers ?? 0) >= 50_000 ? "text-slate-900" : "text-slate-600"}`}>
              @{p.user}</a>
            <span className="text-slate-500"> ({reach(p.followers)})</span>
            {p.mentions.length ? <span className="ml-1 font-semibold text-slate-700">{p.mentions.join(", ")}</span> : null}
            {p.flags.map((f) => <span key={f} className={`ml-1 rounded px-1 ${FLAG_STYLE[f] ?? "bg-slate-100"}`}>{f}</span>)}
            <div className="text-slate-700">{p.text}</div>
          </li>)}
        </ul> : <p className="mt-1 text-xs text-slate-400">{flaggedOnly ? "No flagged posts." : "No posts found."}</p>}
      </div>)}
    </div> : null}
  </section>;
}
