"use client";

import { useState, useTransition, type ReactNode } from "react";
import type { TeamNews, XNewsResult } from "@/lib/x-news";
import { isXHandle } from "@/lib/x-news-accounts";

const FLAG_STYLE: Record<string, string> = {
  starter: "bg-violet-100 text-violet-900", out: "bg-red-100 text-red-800",
  "doubtful/GTD": "bg-amber-100 text-amber-900", injury: "bg-orange-100 text-orange-900",
};
const ago = (iso: string) => {
  const m = Math.max(0, Math.round((Date.now() - Date.parse(iso)) / 60_000));
  return m < 60 ? `${m}m ago` : m < 48 * 60 ? `${Math.round(m / 60)}h ago` : `${Math.round(m / 1440)}d ago`;
};
const reach = (n: number | null) => (n == null ? "?" : n >= 1000 ? `${Math.round(n / 1000)}K` : String(n));


/**
 * Starter and injury posts from X, per team. Evidence to read before lock, not
 * a status. Shared by the CFB and NFL DFS pages; each passes its own search.
 */
export default function XNewsPanel({ search, intro, storageKey, className }: {
  search: (extraAccounts: string[]) => Promise<XNewsResult>; intro: ReactNode;
  /** localStorage key for this page's extra trusted accounts. */
  storageKey: string; className?: string;
}) {
  // Both pages mount this only after the slate loads in the browser, so reading storage here cannot mismatch the server render.
  const [extra, setExtra] = useState(() => { try { return localStorage.getItem(storageKey) ?? ""; } catch { return ""; } });
  const [trusted, setTrusted] = useState<string[]>([]);
  const extraHandles = extra.split(/[\s,]+/).map((h) => h.replace(/^@/, "")).filter(isXHandle);
  const [teams, setTeams] = useState<TeamNews[] | null>(null);
  const [meta, setMeta] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [flaggedOnly, setFlaggedOnly] = useState(true);
  const [pending, start] = useTransition();

  function run() {
    setError(null);
    start(async () => {
      try {
        try { localStorage.setItem(storageKey, extraHandles.join(" ")); } catch { /* optional */ }
        const r = await search(extraHandles);
        setTrusted(r.trustedAccounts);
        if (r.trustedError) setError(`Trusted-account search failed: ${r.trustedError}`);
        setTeams(r.teams);
        setMeta(`Searched ${new Date(r.searchedAt).toLocaleTimeString([], { hour: "numeric", minute: "2-digit" })} · ${r.postsRead} posts read (≈$${(r.postsRead * 0.00015).toFixed(3)}) · last 72 hours`);
      } catch (reason) { setError(reason instanceof Error ? reason.message : String(reason)); }
    });
  }

  const shown = (teams ?? []).map((t) => ({ ...t, posts: flaggedOnly ? t.posts.filter((p) => p.flags.length) : t.posts }));
  return <section className={className ?? "rounded-xl border bg-white p-4 shadow-sm"}>
    <div className="flex flex-wrap items-center gap-2 text-xs">
      <span className="text-sm font-bold text-slate-700">Starter news from X</span>
      <button disabled={pending} onClick={run} className="rounded border bg-white px-2 py-1 font-semibold disabled:opacity-50">
        {pending ? "Searching…" : teams ? "Search again" : "Search X"}</button>
      {teams ? <label className="flex items-center gap-1"><input type="checkbox" checked={flaggedOnly} onChange={(e) => setFlaggedOnly(e.target.checked)} />
        flagged posts only</label> : null}
      {meta ? <span className="text-slate-500">{meta}</span> : null}
    </div>
    <p className="mt-1 text-xs text-slate-500">{intro}</p>
    <label className="mt-2 flex flex-wrap items-center gap-2 text-xs text-slate-600">
      <span className="font-semibold">Your trusted accounts</span>
      <input value={extra} onChange={(e) => setExtra(e.target.value)} placeholder="@beatwriter @another" className="min-w-[16rem] flex-1 rounded border px-2 py-1" />
      <span className="text-slate-500">added to the built-in list on the next search; saved on this device</span>
    </label>
    {trusted.length ? <p className="mt-1 text-xs text-slate-500">Trusted: {trusted.map((h) => `@${h}`).join(" ")}</p> : null}
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
            {p.trusted ? <span className="ml-1 rounded bg-emerald-100 px-1 font-semibold text-emerald-900">trusted</span> : null}
            {p.mentions.length ? <span className="ml-1 font-semibold text-slate-700">{p.mentions.join(", ")}</span> : null}
            {p.flags.map((f) => <span key={f} className={`ml-1 rounded px-1 ${FLAG_STYLE[f] ?? "bg-slate-100"}`}>{f}</span>)}
            <div className="text-slate-700">{p.text}</div>
          </li>)}
        </ul> : <p className="mt-1 text-xs text-slate-400">{flaggedOnly ? "No flagged posts." : "No posts found."}</p>}
      </div>)}
    </div> : null}
  </section>;
}
