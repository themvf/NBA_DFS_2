import Link from "next/link";
import { connection } from "next/server";
import saved from "@/data/game-leaders.json";
import { getGameLeadersAvailability } from "@/db/nfl-game-leaders-availability";
import { getGameLeaderMarket, type GameLeaderMarketRow } from "@/db/nfl-game-leaders-market";
import { getAnytimeTouchdowns } from "@/db/nfl-anytime-td";
import { outPlayersInForecast } from "@/lib/nfl/game-leaders-availability";

export const dynamic = "force-dynamic";
export const metadata = { title: "NFL Game Leaders" };
const families = [
  ["rushing_yards", "Most rushing yards"],
  ["receptions", "Most receptions"],
  ["receiving_yards", "Most receiving yards"],
  ["total_yards", "Most total yards (rushing + receiving)"],
] as const;
function pct(value: number) { return value > 0 && value < .001 ? "<0.1%" : `${(value * 100).toFixed(1)}%`; }
const numberFormat = new Intl.NumberFormat("en-US", { maximumFractionDigits: 1 });
function number(value: number | null | undefined) { return value == null ? "Unavailable" : numberFormat.format(value); }
function date(value: string) { return new Intl.DateTimeFormat("en-US", { timeZone: "America/New_York", month: "short", day: "numeric", hour: "numeric", minute: "2-digit", timeZoneName: "short" }).format(new Date(value)); }
async function now() { await connection(); return Date.now(); }
function american(price: number | null) { return price == null ? "" : price > 0 ? `+${price}` : `${price}`.replace("-", "−"); }
const BOOK_NAMES: Record<string, string> = { pinnacle: "Pinnacle", draftkings: "DK", fanduel: "FanDuel", betmgm: "BetMGM", caesars: "Caesars", williamhill_us: "Caesars", espnbet: "ESPN BET", betrivers: "BetRivers", fanatics: "Fanatics" };
function lineLabel(row: GameLeaderMarketRow | undefined) {
  if (!row || row.line == null) return "Not listed";
  const book = row.book && row.book !== "draftkings" ? ` (${BOOK_NAMES[row.book] ?? row.book})` : "";
  return `${numberFormat.format(row.line)} · ${american(row.overPrice)}/${american(row.underPrice)}${book}`;
}
function coverageSummary(game: unknown) {
  const coverage = (game as { history_coverage?: Record<string, { expected: string[] | null; included: string[]; excluded: string[]; excluded_event_games?: string[] }> }).history_coverage;
  if (!coverage) return "Recent-game coverage was not recorded for this older snapshot. Refresh it before using the estimates for a decision.";
  return Object.entries(coverage).map(([team, c]) => `${team}: ${c.included.length} of ${c.expected?.length ?? "unknown"} recent games included; ${c.excluded.length} missing, ${c.excluded_event_games?.length ?? 0} with unresolved yardage events.`).join(" ");
}

export default async function Page({ searchParams }: { searchParams: Promise<{ game?: string }> }) {
  const params = await searchParams;
  const selected = saved.games.find(g => g.game.game_id === params.game) ?? saved.games[0];
  const started = selected && (await now()) >= Date.parse(selected.game.kickoff);
  const availability = selected
    ? await getGameLeadersAvailability(saved.season, saved.week, selected.game.away,
      selected.game.home, selected.game.kickoff).catch((error) => {
        console.error("Game Leaders availability check failed", error);
        return null;
      })
    : null;
  const market = selected
    ? await getGameLeaderMarket(selected.game.game_id, selected.decision_at).catch((error) => {
        console.error("Game Leaders DK pricing read failed", error);
        return null;
      })
    : null;
  const anytime = selected
    ? await getAnytimeTouchdowns(selected.game.home, selected.game.away, selected.game.kickoff).catch((error) => {
        console.error("Game Leaders anytime-TD read failed", error);
        return undefined;
      })
    : null;
  const teamOf = new Map<string, string>();
  for (const family of Object.values(selected?.metrics ?? {}))
    for (const p of (family as { players: { name: string; team: string; residual?: boolean }[] }).players)
      if (!p.residual && p.team) teamOf.set(p.name.toLowerCase(), p.team);
  const marketRows = new Map<string, Map<string, GameLeaderMarketRow>>();
  for (const row of market?.rows ?? []) {
    if (!marketRows.has(row.metric)) marketRows.set(row.metric, new Map());
    marketRows.get(row.metric)!.set(row.identity, row);
  }
  const newlyOut = selected && availability?.complete
    ? outPlayersInForecast(selected.metrics, availability.confirmedOut)
    : [];
  const blockCurrentForecast = newlyOut.length > 0 || (!started && !availability?.complete);
  return <main className="mx-auto max-w-6xl space-y-6 px-4 py-8">
    <header className="space-y-3"><div className="flex flex-wrap items-center gap-3"><h1 className="text-3xl font-bold">Game leaders</h1><span className="rounded-full bg-amber-100 px-3 py-1 text-sm font-semibold text-amber-900">Research model</span></div><p>Rushing yards, receptions, and receiving yards · {saved.season} Week {saved.week}</p><p className="rounded-xl border border-amber-200 bg-amber-50 p-4 text-sm text-amber-950">This model has not consistently beaten a simple recent-average ranking. The probabilities are exploratory, and player availability remains unresolved. The model itself uses no sportsbook odds; DraftKings over/under lines are shown beside it for comparison.</p></header>
    <form method="get" className="flex flex-wrap items-end gap-3"><label className="space-y-1 text-sm font-medium"><span className="block">Game</span><select name="game" defaultValue={selected?.game.game_id} className="rounded border bg-white px-3 py-2">{saved.games.map(g => <option key={g.game.game_id} value={g.game.game_id}>{g.game.away} at {g.game.home} · {date(g.game.kickoff)}</option>)}</select></label><button className="rounded bg-emerald-800 px-4 py-2 text-sm text-white" type="submit">Show game</button></form>
    {selected ? <>
      <p className="rounded-xl border border-amber-200 bg-amber-50 p-4 text-sm text-amber-950">{coverageSummary(selected)}</p>
      <section className="rounded-xl border bg-white p-4"><h2 className="text-xl font-semibold">{selected.game.away} at {selected.game.home}</h2><p className="mt-2 text-sm text-slate-600">Evidence frozen {date(selected.decision_at)} · {selected.training_games} historical workload games · {selected.draws.toLocaleString()} simulations.</p><p className="mt-2 text-sm text-amber-900">{started ? "Kickoff has passed. These are saved pregame estimates, not updated live results." : "Both depth providers and this week's injury observations were inspected. Missing coverage and disagreements remain unresolved; this is not final game-day confirmation."}</p><p className="mt-2 text-sm text-slate-600">Full game, including overtime. Total yards means rushing plus receiving; passing and return yards are excluded. Shared carries and targets compete within each team. An average projection is different from the chance of finishing first.</p></section>
      <section className="rounded-xl border bg-white p-4 space-y-3"><h2 className="text-xl font-semibold">What this saved forecast includes</h2>
        <p className="text-sm text-slate-600">This weekly page uses the original leader model. The expanded joint outcome workflow has a separate research replay; it has not been applied to this saved matchup.</p>
        <div className="overflow-x-auto"><table className="w-full text-left text-sm"><thead><tr><th className="p-2">Factor</th><th className="p-2">Used here</th><th className="p-2">What that means</th></tr></thead><tbody>
          <tr className="border-t"><td className="p-2">Opponent performance</td><td className="p-2">Yes, broadly</td><td className="p-2">Adjusts team workload, catch rate and yards per opportunity using how prior offenses performed against this defense versus their other opponents.</td></tr>
          <tr className="border-t"><td className="p-2">Defensive style</td><td className="p-2">No</td><td className="p-2">Man/zone coverage, blitzes, pressure and player-role matchups are not inputs to these estimates.</td></tr>
          <tr className="border-t"><td className="p-2">Game script</td><td className="p-2">Historical approximation</td><td className="p-2">Both teams borrow workload patterns from the same historical game. This does not simulate the score, remaining clock or changes in play calling while leading or trailing.</td></tr>
          <tr className="border-t"><td className="p-2">Mid-game exits and replacement work</td><td className="p-2">No</td><td className="p-2">The separate research candidate has exit and redistribution logic that needs historical exit labels; it is not enabled here.</td></tr>
          <tr className="border-t"><td className="p-2">Market lines and availability</td><td className="p-2">Shown, not used; availability unresolved</td><td className="p-2">This is an independent saved forecast, with no final game-day participation confirmation. DraftKings lines appear in the tables for comparison only.</td></tr>
        </tbody></table></div>
        <p className="text-sm"><Link href="/nfl/game-model" className="text-emerald-800 underline">View the separate joint outcome research replay</Link></p>
      </section>
      {blockCurrentForecast && <p className="rounded-xl border border-red-300 bg-red-50 p-4 text-sm text-red-950">
        {newlyOut.length > 0
          ? `This saved forecast still includes ${newlyOut.map((p) => `${p.name} (${p.team})`).join(", ")}, now ruled out. The player shares and projected averages are hidden until the full game is rerun.`
          : "Current injury coverage could not be verified. Player shares and projected averages are hidden until availability can be checked."}
      </p>}
      {!blockCurrentForecast && availability?.complete && <p className="text-sm text-slate-600">
        Injury sources checked {date(availability.checkedAt)}. Unresolved player roles remain in this exploratory forecast.
      </p>}
      {!blockCurrentForecast && <p className="rounded-xl border bg-white p-4 text-sm text-slate-600">{market
        ? <>DraftKings over/under lines captured {date(market.propCapturedAt)}. <strong>DK chance over</strong> is the line&apos;s over price with the bookmaker margin removed. <strong>DK-implied leader</strong> turns every listed player&apos;s DraftKings line into a chance of leading the game, using this model&apos;s simulation for players DraftKings does not list. It is a comparison, not DraftKings&apos; own leader price, which our odds feed does not carry. DraftKings has no total-yards market.</>
        : <>DraftKings lines are not available for this game yet. They are captured about 1¾ hours before kickoff.</>}</p>}
      {!blockCurrentForecast && families.map(([metric, title]) => {
        const family = selected.metrics[metric];
        if (!family) return <section key={metric} className="rounded-xl border bg-white p-4"><h2 className="text-xl font-semibold">{title}</h2><p className="mt-2 text-sm text-slate-600">Not calculated in this older snapshot. Run the updated model to add this category.</p></section>;
        const named = family.players.filter(p => !p.residual);
        const baseline = [...named].sort((a, b) => (b.baseline_mean ?? 0) - (a.baseline_mean ?? 0))[0];
        const priced = marketRows.get(metric);
        const display = named.filter(p => p.win_share > 0 || (p.mean ?? 0) > 0 || (priced?.get(p.identity)?.line != null));
        const otherMarket = priced ? [...priced.values()].filter(r => r.identity.startsWith("OTHER:")).reduce((sum, r) => sum + r.marketLeaderShare, 0) : null;
        return <section key={metric} className="overflow-hidden rounded-xl border bg-white"><div className="border-b p-4"><h2 className="text-xl font-semibold">{title}</h2><p className="mt-1 text-sm text-slate-600">Recent-average baseline leader: <strong>{baseline?.name ?? "Unavailable"}</strong>. Model tie frequency: {pct(family.tie_probability)}.</p></div><div className="overflow-x-auto"><table className="w-full text-left text-sm"><thead className="bg-slate-50"><tr><th className="p-3">Player</th><th className="p-3">Leader share</th>{priced && <><th className="p-3">DK line (over/under)</th><th className="p-3">DK chance over</th><th className="p-3">Model chance over</th><th className="p-3">DK-implied leader</th></>}<th className="p-3">First or tied</th><th className="p-3">Projected average</th><th className="p-3">Middle 80% range</th><th className="p-3">Recent average</th></tr></thead><tbody>{display.map(p => <tr className="border-t" key={p.identity}><td className="p-3 font-medium">{p.name}<span className="ml-2 font-normal text-slate-500">{p.team}</span></td><td className="p-3 font-semibold">{pct(p.win_share)}</td>{priced && (() => { const m = priced.get(p.identity); return <><td className="p-3 whitespace-nowrap">{lineLabel(m)}</td><td className="p-3">{m?.fairOver == null ? "" : pct(m.fairOver)}</td><td className="p-3">{m?.modelPOver == null ? "" : pct(m.modelPOver)}</td><td className="p-3 font-semibold">{m ? pct(m.marketLeaderShare) : ""}</td></>; })()}<td className="p-3">{pct(p.first_or_tied)}</td><td className="p-3">{number(p.mean)}</td><td className="p-3">{number(p.p10)}–{number(p.p90)}</td><td className="p-3">{number(p.baseline_mean)}</td></tr>)}</tbody></table></div><div className="space-y-2 border-t bg-slate-50 p-4 text-sm text-slate-600"><p>Other or unresolved contributors: {pct(family.players.filter(p => p.residual).reduce((sum, p) => sum + p.win_share, 0))} combined leader share{otherMarket != null ? `; ${pct(otherMarket)} DK-implied` : ""}.</p><p>Leader share splits ties equally between individuals. First-or-tied counts each tied player in full, so that column can add to more than 100%. Percentages are rounded independently, so displayed shares may not total exactly 100%. A displayed 0.0% means no wins occurred in the saved simulations, not that the outcome is impossible. Ranges describe simulated outcomes; they are not evidence that the model is accurate.</p></div></section>;
      })}
      <section className="overflow-hidden rounded-xl border bg-white"><div className="border-b p-4"><h2 className="text-xl font-semibold">Anytime touchdown</h2><p className="mt-1 text-sm text-slate-600">{anytime === undefined
          ? "The touchdown lines could not be read right now."
          : anytime
            ? <>Sportsbook market, not this model: DraftKings&apos; price to score a rushing or receiving touchdown (passing touchdowns do not count), captured {date(anytime.capturedAt)}. <strong>Fair chance</strong> removes an assumed 8% bookmaker margin from DraftKings&apos; price. <strong>Best price</strong> is the longest price across the books we capture. <strong>1st TD</strong> is DraftKings&apos; first-touchdown-scorer price; its chance scales every outcome of the game, including no touchdown, to 100%.{anytime.firstTdListed ? "" : " First-touchdown prices were not in this capture; they arrive with the next one."}</>
            : "No touchdown lines captured for this game yet. They are captured the day before and 60-75 minutes before kickoff."}</p></div>
        {anytime?.rows.length ? <div className="overflow-x-auto"><table className="w-full text-left text-sm"><thead className="bg-slate-50"><tr><th className="p-3">Player</th><th className="p-3">DK price</th><th className="p-3">DK implied</th><th className="p-3">Fair chance</th><th className="p-3">Best price</th><th className="p-3">2+ TDs (DK)</th><th className="p-3">1st TD (DK)</th><th className="p-3">1st TD chance</th></tr></thead><tbody>{anytime.rows.map(r => <tr className="border-t" key={r.player}><td className="p-3 font-medium">{r.player}<span className="ml-2 font-normal text-slate-500">{teamOf.get(r.player.toLowerCase()) ?? ""}</span></td><td className="p-3">{r.dkPrice == null ? "Not listed" : american(r.dkPrice)}</td><td className="p-3">{r.dkImplied == null ? "" : pct(r.dkImplied)}</td><td className="p-3 font-semibold">{r.fairProb == null ? "" : pct(r.fairProb)}</td><td className="p-3 whitespace-nowrap">{r.bestPrice == null ? "" : `${american(r.bestPrice)} (${BOOK_NAMES[r.bestBook ?? ""] ?? r.bestBook})`}</td><td className="p-3">{r.twoPlusPrice == null ? "" : american(r.twoPlusPrice)}</td><td className="p-3">{r.firstTdPrice == null ? "" : american(r.firstTdPrice)}</td><td className="p-3">{r.firstTdFair == null ? "" : pct(r.firstTdFair)}</td></tr>)}</tbody></table></div> : null}
      </section>
      <details className="rounded-xl border bg-white p-4"><summary className="cursor-pointer font-semibold">Assumptions and missing factors</summary><ul className="mt-3 list-disc space-y-2 pl-5 text-sm text-slate-600">{selected.limits.map(l => <li key={l}>{l}</li>)}</ul><p className="mt-3 text-sm text-slate-600">This is a saved weekly batch. A fresh source capture, roster review and model run are required to update it. A DraftKings upload is not required.</p></details>
    </> : <p>No eligible saved games.</p>}
    <section className="overflow-hidden rounded-xl border bg-white"><h2 className="p-4 text-xl font-semibold">Historical check: model versus recent average</h2><div className="overflow-x-auto"><table className="w-full text-left text-sm"><thead className="bg-slate-50"><tr><th className="p-3">Season</th><th className="p-3">Games</th><th className="p-3">Category</th><th className="p-3">Model first choice</th><th className="p-3">Average baseline</th></tr></thead><tbody>{saved.evaluation.flatMap(e => families.map(([metric, title]) => { const result = (e.summary as Record<string, { top_choice_credit: number | null; mean_baseline_credit: number | null }>)[metric]; return <tr className="border-t" key={`${e.season}-${metric}`}><td className="p-3">{e.season} · Weeks {e.weeks.join("–")}</td><td className="p-3">{e.games} ({e.skipped} skipped)</td><td className="p-3">{title}</td><td className="p-3">{result?.top_choice_credit != null ? pct(result.top_choice_credit) : "Not evaluated"}</td><td className="p-3">{result?.mean_baseline_credit != null ? pct(result.mean_baseline_credit) : "Not evaluated"}</td></tr>; }))}</tbody></table></div><p className="border-t p-4 text-sm text-slate-600">Ties receive fractional credit. {saved.evaluation_limit} {saved.rejected_training_games} event histories remain quarantined; independently verified workload can still be used.</p></section>
    <nav className="flex gap-4 text-sm"><Link href="/nfl/game-model" className="text-emerald-800 underline">Shared game and DFS model</Link><Link href="/nfl/longest-touchdown" className="text-emerald-800 underline">Longest touchdown</Link><Link href="/nfl/pbp" className="text-emerald-800 underline">Play-by-play</Link></nav>
  </main>;
}
