export const dynamic = "force-dynamic";

import Link from "next/link";
import { getFantasyRankings, getLatestRankingSet } from "@/db/queries-fantasy-football";
import { BEST_BALL_POSITIONS } from "@/lib/fantasy-football/best-ball";
import BestBallClient from "./best-ball-client";

export default async function BestBallPage() {
  const set = await getLatestRankingSet("PPR");
  const allRankings = set ? await getFantasyRankings(set.id) : [];
  const rankings = allRankings
    .filter((player) => BEST_BALL_POSITIONS.includes(player.position as "QB" | "RB" | "WR" | "TE"))
    .slice(0, 260);
  return <div className="space-y-6">
    <header className="overflow-hidden rounded-3xl border bg-gradient-to-br from-blue-950 via-slate-950 to-indigo-950 p-7 text-white shadow-xl">
      <div className="flex flex-wrap items-end justify-between gap-5"><div><p className="text-xs font-bold uppercase tracking-[0.28em] text-blue-300">DraftKings · NFL · Season Long</p><h1 className="mt-2 text-4xl font-black">Best Ball Draft Lab</h1><p className="mt-2 max-w-3xl text-slate-300">Build and validate a 20-player roster for automatic weekly optimization and the Weeks 15–17 tournament rounds.</p></div><nav className="flex gap-2"><Link href="/fantasy-football/rankings?scoring=PPR" className="rounded-lg border border-white/20 px-3 py-2 text-sm font-semibold">Redraft rankings</Link><span className="rounded-lg bg-blue-500 px-3 py-2 text-sm font-bold">Best Ball</span></nav></div>
    </header>

    <div className="grid gap-3 md:grid-cols-3">
      <div className="rounded-2xl border bg-card p-4"><p className="text-xs font-bold uppercase text-muted-foreground">Draft</p><p className="mt-1 text-xl font-black">20-player snake</p><p className="text-sm text-muted-foreground">No waivers, trades, lineup setting, K or DST.</p></div>
      <div className="rounded-2xl border bg-card p-4"><p className="text-xs font-bold uppercase text-muted-foreground">Scoring</p><p className="mt-1 text-xl font-black">Full PPR + bonuses</p><p className="text-sm text-muted-foreground">+3 at 300 passing yards and 100 rushing/receiving yards.</p></div>
      <div className="rounded-2xl border bg-card p-4"><p className="text-xs font-bold uppercase text-muted-foreground">Entry action required</p><p className="mt-1 text-xl font-black">Edit, queue, or pick</p><p className="text-sm text-muted-foreground">Take at least one qualifying manual action in DraftKings.</p></div>
    </div>

    <details className="rounded-2xl border bg-card p-5"><summary className="cursor-pointer text-lg font-black">DraftKings scoring and draft rules</summary><div className="mt-4 grid gap-4 text-sm md:grid-cols-2 xl:grid-cols-4">
      <div><h2 className="font-bold">Passing</h2><p className="mt-2">TD +4 · 25 yards +1</p><p>300-yard game +3</p><p>Interception −1</p></div>
      <div><h2 className="font-bold">Rushing</h2><p className="mt-2">TD +6 · 10 yards +1</p><p>100-yard game +3</p><p>Two-point conversion +2</p></div>
      <div><h2 className="font-bold">Receiving</h2><p className="mt-2">Reception +1 · TD +6</p><p>10 yards +1 · 100-yard game +3</p><p>Two-point conversion +2</p></div>
      <div><h2 className="font-bold">Other and clock</h2><p className="mt-2">Return TD +6 · lost fumble −1</p><p>Offensive fumble-recovery TD +6</p><p>Fast pick: 30 sec · slow pick: up to 8 hr</p></div>
    </div></details>

    {!set ? <div className="rounded-2xl border border-amber-300 bg-amber-50 p-6">No PPR ranking snapshot is available. Run the Fantasy Football refresh workflow.</div> : <BestBallClient rankings={rankings} rankingSetId={set.id} />}
  </div>;
}
