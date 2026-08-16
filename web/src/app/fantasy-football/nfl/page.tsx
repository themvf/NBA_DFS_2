export const dynamic = "force-dynamic";

import Link from "next/link";
import { getFantasyRankings, getLatestRankingSet } from "@/db/queries-fantasy-football";
import NflTeamOverview from "./nfl-team-overview";

const SCORING_PRESETS = ["STD", "HALF", "PPR"] as const;

export default async function FantasyNflPage({
  searchParams,
}: {
  searchParams: Promise<{ scoring?: string }>;
}) {
  const params = await searchParams;
  const requested = String(params.scoring || "PPR").toUpperCase();
  const scoring = SCORING_PRESETS.includes(requested as (typeof SCORING_PRESETS)[number]) ? requested : "PPR";
  const set = await getLatestRankingSet(scoring);
  const rankings = set ? await getFantasyRankings(set.id) : [];

  return <div className="space-y-6">
    <header className="rounded-3xl border bg-gradient-to-br from-slate-950 via-blue-950 to-emerald-950 p-6 text-white shadow-xl sm:p-8">
      <p className="text-xs font-bold uppercase tracking-[0.24em] text-emerald-300">Fantasy Football · NFL</p>
      <div className="mt-2 flex flex-wrap items-end justify-between gap-4">
        <div>
          <h1 className="text-3xl font-black tracking-tight sm:text-4xl">NFL Team Overview</h1>
          <p className="mt-2 max-w-3xl text-sm text-slate-300">Choose a team to review its fantasy depth by position, with projections, prior production, ADP, availability, and role signals kept in separate tables.</p>
          <p className="mt-2 text-xs text-slate-400">{set ? `${set.name} · ${set.playerCount} players · updated ${new Date(set.createdAt).toLocaleString()}` : "No ranking snapshot available"}</p>
        </div>
        <div className="flex gap-2">{SCORING_PRESETS.map((value) => <Link key={value} href={`/fantasy-football/nfl?scoring=${value}`} className={`rounded-lg px-3 py-2 text-sm font-bold ${value === scoring ? "bg-white text-slate-950" : "border border-white/20 hover:bg-white/10"}`}>{value}</Link>)}</div>
      </div>
    </header>

    {!set ? <div className="rounded-2xl border border-amber-300 bg-amber-50 p-6 text-sm text-amber-950">Run the <code>Refresh Fantasy Football Draft Data</code> workflow to populate the NFL team overview.</div> : <NflTeamOverview rankings={rankings} season={set.season} scoring={scoring} />}
  </div>;
}
