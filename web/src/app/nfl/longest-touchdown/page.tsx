import Link from "next/link";
import { connection } from "next/server";
import snapshot from "@/data/longest-touchdown.json";

export const dynamic = "force-dynamic";
export const metadata = { title: "NFL Longest Touchdown", description: "Experimental rushing and receiving touchdown-distance estimates from frozen play-by-play evidence." };

function percent(value: number) {
  return value > 0 && value < 0.01 ? "<1%" : `${Math.round(value * 100)}%`;
}
function eastern(value: string) {
  return new Intl.DateTimeFormat("en-US", { timeZone: "America/New_York", month: "short", day: "numeric", hour: "numeric", minute: "2-digit", timeZoneName: "short" }).format(new Date(value));
}

async function requestTime() {
  await connection();
  return Date.now();
}

export default async function LongestTouchdownPage() {
  const started = await requestTime() >= Date.parse(snapshot.game.kickoff);
  return <main className="mx-auto max-w-5xl space-y-6 px-4 py-8">
    <header className="space-y-3">
      <div className="flex flex-wrap items-center gap-3"><h1 className="text-3xl font-bold">Longest touchdown</h1><span className="rounded-full bg-amber-100 px-3 py-1 text-sm font-semibold text-amber-900">Experimental</span></div>
      <p className="text-lg text-slate-700">{snapshot.game.away} at {snapshot.game.home} · Week {snapshot.game.week} · {eastern(snapshot.game.kickoff)}</p>
      <p className="text-sm text-slate-600">Evidence frozen {eastern(snapshot.decisionAt)}. Recalculated with the revised model. This is a saved forecast, not a live feed.</p>
      <p className="rounded-xl border border-amber-200 bg-amber-50 p-4 text-sm text-amber-950">These are model estimates, not validated betting probabilities. No sportsbook odds are used. {started ? "The game has started or finished; these remain pregame estimates. Results have not been graded on this page." : "Later injuries and workload changes may alter the ranking. Final game-day availability is not confirmed."}</p>
    </header>
    <section className="overflow-hidden rounded-xl border bg-white shadow-sm">
      <div className="border-b p-4"><h2 className="text-xl font-semibold">Who could score the longest?</h2><p className="mt-1 text-sm text-slate-600">Rushing and receiving touchdowns in regulation only. Ties divide credit equally per scorer. Quarterbacks get rushing or receiving credit, not credit for throwing a touchdown.</p></div>
      <div className="overflow-x-auto"><table className="w-full text-left text-sm"><thead className="bg-slate-50 text-slate-600"><tr><th className="p-4">Player</th><th className="p-4">Longest TD share</th><th className="p-4">Any TD</th><th className="p-4">40+ yard TD</th></tr></thead><tbody>{snapshot.players.map(player => <tr key={player.identity} className="border-t"><td className="p-4 font-medium">{player.name}</td><td className="p-4"><div className="flex items-center gap-3"><span className="min-w-10 font-semibold">{percent(player.longestShare)}</span><span aria-hidden="true" className="h-2 w-24 rounded bg-slate-100"><span className="block h-2 rounded bg-emerald-600" style={{ width: `${player.longestShare * 100}%` }} /></span></div></td><td className="p-4">{percent(player.anyTd)}</td><td className="p-4">{percent(player.longTd)}</td></tr>)}</tbody></table></div>
      <div className="border-t bg-slate-50 p-4 text-sm text-slate-600"><p>Other or unresolved scorers: {percent(snapshot.residual.reduce((sum, row) => sum + row.longestShare, 0))} combined longest-TD share. No modeled scrimmage touchdown: {percent(snapshot.noTd)}.</p><p className="mt-2">Scoring any touchdown and scoring the longest touchdown are different outcomes. Percentages are rounded; the full field includes the residual scorers above.</p></div>
    </section>
    <section className="rounded-xl border bg-white p-4"><h2 className="text-lg font-semibold">What could change this?</h2><ul className="mt-3 list-disc space-y-2 pl-5 text-sm text-slate-700"><li>Tampa Bay’s quarterback change and backfield split are not separately fitted. Earlier team history still influences this estimate.</li><li>Unexpected players receive a pooled allowance, rather than a reliable named workload forecast.</li><li>Overtime, return touchdowns and defensive touchdowns are excluded. This page does not forecast DraftKings points or select lineups.</li></ul><details className="mt-4"><summary className="cursor-pointer text-sm font-medium">Players without a supported current role</summary><p className="mt-2 text-sm text-slate-600">{snapshot.unresolved.join(", ")}. Missing role evidence does not mean zero scoring chance.</p></details></section>
    <details className="rounded-xl border bg-white p-4"><summary className="cursor-pointer font-semibold">Evidence and model details</summary><div className="mt-3 space-y-2 text-sm text-slate-600"><p>{snapshot.trainingGames} historical games · {snapshot.draws.toLocaleString()} simulated games · {snapshot.modelVersion}. Simulation size is not evidence of accuracy.</p><p>Inputs: canonical schedule, play-by-play and GSIS identities, both Sleeper and FantasyPros depth evidence, and week-matched injury observations. Conflicting availability and missing coverage remain limitations.</p><p>This first release publishes a reviewed snapshot. A new capture, forecast and publication are needed to refresh it; uploading a DFS slate alone does not update this page.</p></div></details>
    <nav className="flex flex-wrap gap-4 text-sm"><Link className="text-emerald-800 underline" href="/nfl/specials">Slate specials</Link><Link className="text-emerald-800 underline" href="/nfl/pbp">Play-by-play</Link><Link className="text-emerald-800 underline" href="/dfs/nfl">DFS workspace</Link></nav>
  </main>;
}
