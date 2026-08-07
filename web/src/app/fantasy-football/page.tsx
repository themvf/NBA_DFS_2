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
            <p className="mt-3 max-w-3xl text-slate-300">Our board uses the current NFL roster, three seasons of production, depth order, availability, rookies, team movement, and transparent indicators. Current ADP is shown for comparison without controlling our projections.</p>
          </div>
          <div className="flex gap-3">
            <Link href="/fantasy-football/rankings" className="rounded-xl border border-white/20 px-4 py-2 text-sm font-semibold hover:bg-white/10">Explore rankings</Link>
            <Link href="/fantasy-football/best-ball" className="rounded-xl border border-blue-300/40 bg-blue-500/20 px-4 py-2 text-sm font-semibold text-blue-100 hover:bg-blue-500/30">NFL Best Ball</Link>
            <Link href="/fantasy-football/redraft" className="rounded-xl border border-emerald-300/40 bg-emerald-500/20 px-4 py-2 text-sm font-semibold text-emerald-100 hover:bg-emerald-500/30">Redraft Mock</Link>
            <Link href="/fantasy-football/draft/new" className="rounded-xl bg-emerald-400 px-4 py-2 text-sm font-bold text-slate-950 hover:bg-emerald-300">Start a draft</Link>
          </div>
        </div>
      </section>

      <section className={`rounded-2xl border p-5 ${stale ? "border-amber-300 bg-amber-50" : "border-emerald-200 bg-emerald-50"}`}>
        <div className="flex flex-wrap items-center justify-between gap-2">
          <div><p className="font-bold">Draft data {stale ? "needs a refresh" : "is ready"}</p><p className="text-sm text-slate-600">Latest successful source refresh: {latestSuccess ? new Date(latestSuccess).toLocaleString() : "none yet"}</p></div>
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
      <p className="text-center text-xs text-muted-foreground">Player and historical data from <a className="underline" href="https://docs.sleeper.com/" target="_blank" rel="noreferrer">Sleeper</a> and <a className="underline" href="https://github.com/nflverse/nflverse-data" target="_blank" rel="noreferrer">nflverse</a>. Current 12-team ADP from <a className="underline" href="https://fantasyfootballcalculator.com/adp/ppr" target="_blank" rel="noreferrer">Fantasy Football Calculator</a>. Rankings and projections are produced by this application.</p>
    </div>
  );
}
