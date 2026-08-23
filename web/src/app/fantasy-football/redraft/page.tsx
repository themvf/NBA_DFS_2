export const dynamic = "force-dynamic";

import Link from "next/link";
import { getFantasyProsSourceHealth, getFantasyRankings, getLatestRankingSet } from "@/db/queries-fantasy-football";
import {
  REDRAFT_BENCH_SLOTS,
  REDRAFT_POSITIONS,
  REDRAFT_ROSTER_SIZE,
  REDRAFT_ROUNDS,
  REDRAFT_STARTER_COUNT,
  REDRAFT_TEAM_COUNT,
} from "@/lib/fantasy-football/redraft";
import RedraftClient from "./redraft-client";

// Board size: a full 10-team x 15-round draft is 150 picks. 260 leaves real
// depth on the board at the end rather than forcing the last rounds to pick
// from an empty pool.
const BOARD_SIZE = 260;

export default async function RedraftPage() {
  const set = await getLatestRankingSet("PPR");
  const [allRankings, fantasyProsHealth] = await Promise.all([
    set ? getFantasyRankings(set.id) : Promise.resolve([]),
    getFantasyProsSourceHealth(set?.season ?? 2026),
  ]);
  const rankings = allRankings
    .filter((player) => (REDRAFT_POSITIONS as readonly string[]).includes(player.position))
    .slice(0, BOARD_SIZE);
  const fantasyProsProjectionDataset = fantasyProsHealth.datasets.find((dataset) => dataset.dataset === "projections");

  return <div className="space-y-6">
    <header className="overflow-hidden rounded-3xl border bg-gradient-to-br from-emerald-950 via-slate-950 to-teal-950 p-7 text-white shadow-xl">
      <div className="flex flex-wrap items-end justify-between gap-5">
        <div>
          <p className="text-xs font-bold uppercase tracking-[0.28em] text-emerald-300">Yahoo · NFL · Season Long</p>
          <h1 className="mt-2 text-4xl font-black">Redraft Mock Draft</h1>
          <p className="mt-2 max-w-3xl text-slate-300">Practice a full {REDRAFT_TEAM_COUNT}-team PPR snake draft against yourself, using our independent projections next to the FantasyPros market.</p>
        </div>
        <nav className="flex gap-2">
          <Link href="/fantasy-football/rankings?scoring=PPR" className="rounded-lg border border-white/20 px-3 py-2 text-sm font-semibold">Rankings</Link>
          <Link href="/fantasy-football/redraft/print?scoring=PPR" className="rounded-lg border border-white/20 px-3 py-2 text-sm font-semibold">Print sheet</Link>
          <Link href="/fantasy-football/best-ball" className="rounded-lg border border-white/20 px-3 py-2 text-sm font-semibold">Best Ball</Link>
          <span className="rounded-lg bg-emerald-500 px-3 py-2 text-sm font-bold">Redraft</span>
        </nav>
      </div>
    </header>

    <div className="grid gap-3 md:grid-cols-3">
      <div className="rounded-2xl border bg-card p-4"><p className="text-xs font-bold uppercase text-muted-foreground">League</p><p className="mt-1 text-xl font-black">{REDRAFT_TEAM_COUNT} teams · {REDRAFT_ROUNDS} rounds</p><p className="text-sm text-muted-foreground">Yahoo standard defaults. {REDRAFT_STARTER_COUNT} starters + {REDRAFT_BENCH_SLOTS} bench = {REDRAFT_ROSTER_SIZE} drafted.</p></div>
      <div className="rounded-2xl border bg-card p-4"><p className="text-xs font-bold uppercase text-muted-foreground">Scoring</p><p className="mt-1 text-xl font-black">Full PPR</p><p className="text-sm text-muted-foreground">1 point per reception. DEF uses Yahoo&apos;s standard team-defense scoring.</p></div>
      <div className="rounded-2xl border bg-card p-4"><p className="text-xs font-bold uppercase text-muted-foreground">Starting lineup</p><p className="mt-1 text-xl font-black">QB · 2RB · 2WR · TE</p><p className="text-sm text-muted-foreground">Plus 1 W/R/T flex, 1 K, and 1 DEF.</p></div>
    </div>

    <section className={`rounded-2xl border p-4 ${fantasyProsHealth.connected && !fantasyProsHealth.stale ? "border-emerald-300 bg-emerald-50" : "border-amber-300 bg-amber-50"}`}>
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <p className="text-xs font-bold uppercase tracking-wide text-muted-foreground">FantasyPros market feed</p>
          <p className="mt-1 text-lg font-black">{fantasyProsHealth.connected ? (fantasyProsHealth.stale ? "Connected · stale" : "Connected") : "Incomplete"}</p>
          <p className="text-sm text-muted-foreground">{fantasyProsHealth.availableRequiredDatasets}/{fantasyProsHealth.requiredDatasets} required datasets available. FantasyPros is comparison data only &mdash; the independent nflverse/Sleeper board stays authoritative.</p>
        </div>
        <div className="text-right text-xs text-muted-foreground">
          <p>{fantasyProsProjectionDataset?.rowCount ?? 0} projections · {fantasyProsProjectionDataset?.matchedCount ?? 0} players matched</p>
          <p>{fantasyProsHealth.latestFetchedAt ? `Checked ${new Date(fantasyProsHealth.latestFetchedAt).toLocaleString("en-US", { timeZone: "America/New_York" })} ET` : "No successful snapshot"}</p>
        </div>
      </div>
    </section>

    <section className="rounded-2xl border border-amber-300 bg-amber-50 p-4 text-sm text-amber-950">
      <b>What this board does and does not know.</b> Ranking is our independent PPR projection (nflverse + Sleeper), with ADP shown as comparison context only. <b>Defenses are a known weak spot:</b> they are ordered by 2025 results, but their projected totals are compressed into a narrow band on purpose &mdash; year-over-year defensive signal is very weak (Spearman 0.18), so a confident-looking spread would be fiction. Kickers carry the same caveat. Check the 2025 FPTS column for what a defense actually did.
    </section>

    {!set
      ? <div className="rounded-2xl border border-amber-300 bg-amber-50 p-6">No PPR ranking snapshot is available. Run the Fantasy Football refresh workflow.</div>
      : <RedraftClient rankings={rankings} rankingSetId={Number(set.id)} />}
  </div>;
}
