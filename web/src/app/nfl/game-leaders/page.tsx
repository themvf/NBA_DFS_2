import Link from "next/link";
import { connection } from "next/server";
import saved from "@/data/game-leaders.json";

export const dynamic = "force-dynamic";
export const metadata = { title: "NFL Game Leaders" };
const families = [
  ["rushing_yards", "Most rushing yards"],
  ["receptions", "Most receptions"],
  ["receiving_yards", "Most receiving yards"],
  ["total_yards", "Most total yards (rushing + receiving)"],
] as const;
function pct(value: number) { return value > 0 && value < .01 ? "<1%" : `${Math.round(value * 100)}%`; }
function date(value: string) { return new Intl.DateTimeFormat("en-US", { timeZone: "America/New_York", month: "short", day: "numeric", hour: "numeric", minute: "2-digit", timeZoneName: "short" }).format(new Date(value)); }
async function now() { await connection(); return Date.now(); }
function coverageSummary(game: unknown) {
  const coverage = (game as { history_coverage?: Record<string, { expected: string[] | null; included: string[]; excluded: string[]; excluded_event_games?: string[] }> }).history_coverage;
  if (!coverage) return "Recent-game coverage was not recorded for this older snapshot. Refresh it before using the estimates for a decision.";
  return Object.entries(coverage).map(([team, c]) => `${team}: ${c.included.length} of ${c.expected?.length ?? "unknown"} recent games included; ${c.excluded.length} missing, ${c.excluded_event_games?.length ?? 0} with unresolved yardage events.`).join(" ");
}

export default async function Page({ searchParams }: { searchParams: Promise<{ game?: string }> }) {
  const params = await searchParams;
  const selected = saved.games.find(g => g.game.game_id === params.game) ?? saved.games[0];
  const started = selected && (await now()) >= Date.parse(selected.game.kickoff);
  return <main className="mx-auto max-w-6xl space-y-6 px-4 py-8">
    <header className="space-y-3"><div className="flex flex-wrap items-center gap-3"><h1 className="text-3xl font-bold">Game leaders</h1><span className="rounded-full bg-amber-100 px-3 py-1 text-sm font-semibold text-amber-900">Research model</span></div><p>Rushing yards, receptions, and receiving yards · {saved.season} Week {saved.week}</p><p className="rounded-xl border border-amber-200 bg-amber-50 p-4 text-sm text-amber-950">This model has not consistently beaten a simple recent-average ranking. The probabilities are exploratory, and player availability remains unresolved. No sportsbook odds are used.</p></header>
    <form method="get" className="flex flex-wrap items-end gap-3"><label className="space-y-1 text-sm font-medium"><span className="block">Game</span><select name="game" defaultValue={selected?.game.game_id} className="rounded border bg-white px-3 py-2">{saved.games.map(g => <option key={g.game.game_id} value={g.game.game_id}>{g.game.away} at {g.game.home} · {date(g.game.kickoff)}</option>)}</select></label><button className="rounded bg-emerald-800 px-4 py-2 text-sm text-white" type="submit">Show game</button></form>
    {selected ? <>
      <p className="rounded-xl border border-amber-200 bg-amber-50 p-4 text-sm text-amber-950">{coverageSummary(selected)}</p>
      <section className="rounded-xl border bg-white p-4"><h2 className="text-xl font-semibold">{selected.game.away} at {selected.game.home}</h2><p className="mt-2 text-sm text-slate-600">Evidence frozen {date(selected.decision_at)} · {selected.training_games} historical workload games · {selected.draws.toLocaleString()} simulations.</p><p className="mt-2 text-sm text-amber-900">{started ? "Kickoff has passed. These are saved pregame estimates, not updated live results." : "Both depth providers and this week's injury observations were inspected. Missing coverage and disagreements remain unresolved; this is not final game-day confirmation."}</p><p className="mt-2 text-sm text-slate-600">Full game, including overtime. Total yards means rushing plus receiving; passing and return yards are excluded. Shared carries and targets compete within each team. An average projection is different from the chance of finishing first.</p></section>
      {families.map(([metric, title]) => {
        const family = selected.metrics[metric];
        if (!family) return <section key={metric} className="rounded-xl border bg-white p-4"><h2 className="text-xl font-semibold">{title}</h2><p className="mt-2 text-sm text-slate-600">Not calculated in this older snapshot. Run the updated model to add this category.</p></section>;
        const named = family.players.filter(p => !p.residual);
        const baseline = [...named].sort((a, b) => (b.baseline_mean ?? 0) - (a.baseline_mean ?? 0))[0];
        const display = named.filter(p => p.win_share > 0 || (p.mean ?? 0) > 0);
        return <section key={metric} className="overflow-hidden rounded-xl border bg-white"><div className="border-b p-4"><h2 className="text-xl font-semibold">{title}</h2><p className="mt-1 text-sm text-slate-600">Recent-average baseline leader: <strong>{baseline?.name ?? "Unavailable"}</strong>. Model tie frequency: {pct(family.tie_probability)}.</p></div><div className="overflow-x-auto"><table className="w-full text-left text-sm"><thead className="bg-slate-50"><tr><th className="p-3">Player</th><th className="p-3">Leader share</th><th className="p-3">First or tied</th><th className="p-3">Projected average</th><th className="p-3">Middle 80% range</th><th className="p-3">Recent average</th></tr></thead><tbody>{display.map(p => <tr className="border-t" key={p.identity}><td className="p-3 font-medium">{p.name}<span className="ml-2 font-normal text-slate-500">{p.team}</span></td><td className="p-3 font-semibold">{pct(p.win_share)}</td><td className="p-3">{pct(p.first_or_tied)}</td><td className="p-3">{p.mean?.toFixed(1)}</td><td className="p-3">{p.p10}–{p.p90}</td><td className="p-3">{p.baseline_mean?.toFixed(1)}</td></tr>)}</tbody></table></div><div className="space-y-2 border-t bg-slate-50 p-4 text-sm text-slate-600"><p>Other or unresolved contributors: {pct(family.players.filter(p => p.residual).reduce((sum, p) => sum + p.win_share, 0))} combined leader share.</p><p>Leader share splits ties equally between individuals. First-or-tied counts each tied player in full, so that column can add to more than 100%. Ranges describe simulated outcomes; they are not evidence that the model is accurate.</p></div></section>;
      })}
      <details className="rounded-xl border bg-white p-4"><summary className="cursor-pointer font-semibold">Assumptions and missing factors</summary><ul className="mt-3 list-disc space-y-2 pl-5 text-sm text-slate-600">{selected.limits.map(l => <li key={l}>{l}</li>)}</ul><p className="mt-3 text-sm text-slate-600">This is a saved weekly batch. A fresh source capture, roster review and model run are required to update it. A DraftKings upload is not required.</p></details>
    </> : <p>No eligible saved games.</p>}
    <section className="overflow-hidden rounded-xl border bg-white"><h2 className="p-4 text-xl font-semibold">Historical check: model versus recent average</h2><div className="overflow-x-auto"><table className="w-full text-left text-sm"><thead className="bg-slate-50"><tr><th className="p-3">Season</th><th className="p-3">Games</th><th className="p-3">Category</th><th className="p-3">Model first choice</th><th className="p-3">Average baseline</th></tr></thead><tbody>{saved.evaluation.flatMap(e => families.map(([metric, title]) => { const result = (e.summary as Record<string, { top_choice_credit: number | null; mean_baseline_credit: number | null }>)[metric]; return <tr className="border-t" key={`${e.season}-${metric}`}><td className="p-3">{e.season} · Weeks {e.weeks.join("–")}</td><td className="p-3">{e.games} ({e.skipped} skipped)</td><td className="p-3">{title}</td><td className="p-3">{result?.top_choice_credit != null ? pct(result.top_choice_credit) : "Not evaluated"}</td><td className="p-3">{result?.mean_baseline_credit != null ? pct(result.mean_baseline_credit) : "Not evaluated"}</td></tr>; }))}</tbody></table></div><p className="border-t p-4 text-sm text-slate-600">Ties receive fractional credit. {saved.evaluation_limit} {saved.rejected_training_games} event histories remain quarantined; independently verified workload can still be used.</p></section>
    <nav className="flex gap-4 text-sm"><Link href="/nfl/game-model" className="text-emerald-800 underline">Shared game and DFS model</Link><Link href="/nfl/longest-touchdown" className="text-emerald-800 underline">Longest touchdown</Link><Link href="/nfl/pbp" className="text-emerald-800 underline">Play-by-play</Link></nav>
  </main>;
}
