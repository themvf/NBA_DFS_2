export const dynamic = "force-dynamic";

import Link from "next/link";
import { getFantasyRankings, getLatestRankingSet } from "@/db/queries-fantasy-football";
import { fantasyBadgeClass } from "@/lib/fantasy-football/badge-style";

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
    <div className="overflow-x-auto rounded-2xl border bg-card"><table className="w-full min-w-[1120px] text-sm"><thead className="bg-muted text-left text-xs uppercase text-muted-foreground"><tr><th className="p-3">Our rank</th><th className="p-3">Player</th><th className="p-3">Role & signals</th><th className="p-3">Current ADP</th><th className="p-3">Value</th><th className="p-3">2025 GP</th><th className="p-3">2025 FPTS</th><th className="p-3">2026 proj</th><th className="p-3">2026 GP</th><th className="p-3">Conf.</th></tr></thead><tbody>{rankings.map((player) => {
      const rank=player.ourRank??player.ecr; const delta=player.adp!==null&&rank!==null?player.adp-rank:null;
      return <tr key={player.playerId} className="border-t align-top hover:bg-muted/40"><td className="p-3 text-lg font-black">{rank??"—"}<span className="block text-xs font-medium text-muted-foreground">{player.position}{player.positionRank??""} · T{player.tier??"—"}</span></td><td className="p-3"><p className="font-bold">{player.name}</p><p className="text-xs text-muted-foreground">{player.position} · {player.team??"FA"} · Bye {player.byeWeek??"—"}</p></td><td className="max-w-[330px] p-3"><div className="flex flex-wrap gap-1">{player.indicators.slice(0,3).map((badge) => <span key={badge.code} title={JSON.stringify(badge.evidence)} className={`rounded-full px-2 py-1 text-[10px] font-bold ring-1 ring-inset ${fantasyBadgeClass(badge)}`}>{badge.label}</span>)}</div></td><td className="p-3">{player.adp?.toFixed(1)??"—"}</td><td className={`p-3 font-bold ${delta===null?"text-muted-foreground":delta>=0?"text-emerald-700":"text-red-700"}`}>{delta===null?"—":`${delta>=0?"+":""}${delta.toFixed(1)}`}</td><td className="p-3">{player.games2025??"—"}</td><td className="p-3 font-semibold">{player.fantasyPoints2025?.toFixed(1)??"—"}</td><td className="p-3 font-semibold">{player.ourProjectedPoints?.toFixed(1)??"—"}</td><td className="p-3">{player.expectedGames?.toFixed(1)??"—"}</td><td className="p-3">{player.confidence!==null?`${Math.round(player.confidence*100)}%`:"—"}</td></tr>})}</tbody></table></div>}
  </div>;
}
