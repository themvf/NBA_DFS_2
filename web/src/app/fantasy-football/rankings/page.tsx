export const dynamic = "force-dynamic";

import Link from "next/link";
import { getFantasyRankings, getLatestRankingSet } from "@/db/queries-fantasy-football";
import RankingsTable from "./rankings-table";

export default async function FantasyRankingsPage({ searchParams }: { searchParams: Promise<{ scoring?: string }> }) {
  const requested = String((await searchParams).scoring || "PPR").toUpperCase();
  const scoring = ["STD", "HALF", "PPR"].includes(requested) ? requested : "PPR";
  const set = await getLatestRankingSet(scoring);
  const rankings = set ? await getFantasyRankings(set.id) : [];
  return <div className="space-y-5">
    <div className="flex flex-wrap items-end justify-between gap-4">
      <div><p className="text-xs font-bold uppercase tracking-widest text-emerald-700">Our board vs the market</p><h1 className="text-3xl font-black">Draft Rankings</h1><p className="text-sm text-muted-foreground">{set ? `${set.name} · ${set.playerCount} players · ${new Date(set.createdAt).toLocaleString()}` : "No ranking snapshot available"}</p></div>
      <div className="flex gap-2">{["STD","HALF","PPR"].map((value) => <Link key={value} href={`/fantasy-football/rankings?scoring=${value}`} className={`rounded-lg px-3 py-2 text-sm font-bold ${value===scoring?"bg-slate-900 text-white":"border hover:bg-muted"}`}>{value}</Link>)}</div>
    </div>
    {!set ? <div className="rounded-2xl border border-amber-300 bg-amber-50 p-6">Run the <code>Refresh Fantasy Football Draft Data</code> GitHub workflow to populate this page.</div> :
    <RankingsTable rankings={rankings} />}
  </div>;
}
