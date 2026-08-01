export const dynamic = "force-dynamic";

import Link from "next/link";
import { getFantasyHomeData } from "@/db/queries-fantasy-football";

export default async function FantasyFootballPage() {
  const { rankingSets, drafts, latestSuccess, dataStale: stale } = await getFantasyHomeData();
  return (
    <div className="space-y-8">
      <section className="overflow-hidden rounded-3xl border bg-gradient-to-br from-slate-950 via-emerald-950 to-slate-900 p-8 text-white shadow-xl">
        <p className="text-xs font-semibold uppercase tracking-[0.3em] text-emerald-300">Season-long intelligence</p>
        <div className="mt-3 grid gap-6 lg:grid-cols-[1fr_auto] lg:items-end">
          <div>
            <h1 className="text-4xl font-black tracking-tight">Fantasy Football Draft Lab</h1>
            <p className="mt-3 max-w-3xl text-slate-300">FantasyPros is the market baseline. Our model combines prior-season opportunity, availability, rookies, team movement, and transparent indicators to form an independent draft opinion.</p>
          </div>
          <div className="flex gap-3">
            <Link href="/fantasy-football/rankings" className="rounded-xl border border-white/20 px-4 py-2 text-sm font-semibold hover:bg-white/10">Explore rankings</Link>
            <Link href="/fantasy-football/draft/new" className="rounded-xl bg-emerald-400 px-4 py-2 text-sm font-bold text-slate-950 hover:bg-emerald-300">Start a draft</Link>
          </div>
        </div>
      </section>

      <section className={`rounded-2xl border p-5 ${stale ? "border-amber-300 bg-amber-50" : "border-emerald-200 bg-emerald-50"}`}>
        <div className="flex flex-wrap items-center justify-between gap-2">
          <div><p className="font-bold">Draft data {stale ? "needs a refresh" : "is ready"}</p><p className="text-sm text-slate-600">Latest successful FantasyPros snapshot: {latestSuccess ? new Date(latestSuccess).toLocaleString() : "none yet"}</p></div>
          <span className={`rounded-full px-3 py-1 text-xs font-bold ${stale ? "bg-amber-200 text-amber-900" : "bg-emerald-200 text-emerald-900"}`}>{stale ? "STALE" : "HEALTHY"}</span>
        </div>
      </section>

      <div className="grid gap-6 lg:grid-cols-2">
        <section className="rounded-2xl border bg-card p-6">
          <h2 className="text-xl font-bold">Latest ranking sets</h2>
          <div className="mt-4 space-y-3">{rankingSets.length ? rankingSets.map((set) => (
            <Link key={set.id} href={`/fantasy-football/rankings?scoring=${set.scoring}`} className="flex items-center justify-between rounded-xl border p-3 hover:bg-muted">
              <div><p className="font-semibold">{set.name}</p><p className="text-xs text-muted-foreground">{new Date(set.createdAt).toLocaleString()}</p></div>
              <div className="text-right"><p className="font-bold">{set.scoring}</p><p className="text-xs text-muted-foreground">{set.playerCount} players</p></div>
            </Link>
          )) : <p className="rounded-xl bg-muted p-4 text-sm text-muted-foreground">No rankings have been ingested. Run the Fantasy Football refresh workflow.</p>}</div>
        </section>
        <section className="rounded-2xl border bg-card p-6">
          <h2 className="text-xl font-bold">Recent drafts</h2>
          <div className="mt-4 space-y-3">{drafts.length ? drafts.map((draft) => (
            <Link key={draft.id} href={`/fantasy-football/draft/${draft.id}`} className="flex items-center justify-between rounded-xl border p-3 hover:bg-muted">
              <div><p className="font-semibold">{draft.name}</p><p className="text-xs text-muted-foreground">{draft.teamCount} teams · Slot {draft.controlledSlot}</p></div>
              <div className="text-right"><p className="text-xs font-bold uppercase">{draft.status}</p><p className="text-xs text-muted-foreground">Pick {Math.min(draft.currentPick, draft.totalPicks)}/{draft.totalPicks}</p></div>
            </Link>
          )) : <p className="rounded-xl bg-muted p-4 text-sm text-muted-foreground">Your saved drafts will appear here.</p>}</div>
        </section>
      </div>
      <p className="text-center text-xs text-muted-foreground">Rankings and projections powered by <a className="underline" href="https://www.fantasypros.com/" target="_blank" rel="noreferrer">FantasyPros</a>. Independent projections and indicators are produced by this application.</p>
    </div>
  );
}
