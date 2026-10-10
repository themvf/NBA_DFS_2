import Link from "next/link";
import { connection } from "next/server";
import index from "@/data/longest-touchdown-weeks.json";
import { getGameLeadersAvailability } from "@/db/nfl-game-leaders-availability";

export const dynamic = "force-dynamic";
export const metadata = { title: "NFL Longest Touchdown", description: "Weekly experimental rushing and receiving touchdown-distance estimates." };

type Player = { name: string; identity: string; longestShare: number; anyTd: number; longTd: number };
type PublishedGame = {
  game: { game_id: string; season: number; week: number; kickoff: string; away: string; home: string };
  decisionAt: string;
  draws: number;
  trainingGames: number;
  players: Player[];
  residual: Player[];
  unresolved: string[];
  noTd: number;
  modelVersion: string;
  forecastRosterIds?: string[];
};
type PublishedSeason = { season: number; weeks: Array<{ week: number; games: PublishedGame[] }> };
const saved = index as PublishedSeason & { priorSeasons?: PublishedSeason[] };
const seasons = [...(saved.priorSeasons ?? []), saved].sort((a, b) => a.season - b.season);

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

export default async function LongestTouchdownPage({ searchParams }: { searchParams: Promise<{ season?: string; week?: string; game?: string }> }) {
  const params = await searchParams;
  const now = await requestTime();
  const season = seasons.find((item) => item.season === Number(params.season)) ?? saved;
  const week = season.weeks.find((item) => item.week === Number(params.week)) ?? season.weeks.at(-1);
  const selected = week?.games.find((item) => item.game.game_id === params.game)
    ?? week?.games.find((item) => Date.parse(item.game.kickoff) > now)
    ?? week?.games[0];
  const started = selected ? now >= Date.parse(selected.game.kickoff) : false;
  const availability = selected
    ? await getGameLeadersAvailability(season.season, selected.game.week, selected.game.away,
      selected.game.home, selected.game.kickoff).catch((error) => {
        console.error("Longest Touchdown availability check failed", error);
        return null;
      })
    : null;
  const rosterIds = new Set(selected?.forecastRosterIds
    ?? [...(selected?.players ?? []), ...(selected?.residual ?? [])].map((player) => player.identity));
  const newlyOut = availability?.complete
    ? availability.confirmedOut.filter((player) => rosterIds.has(player.identity))
    : [];
  const blocked = newlyOut.length > 0 || (!started && !availability?.complete);

  return <main className="mx-auto max-w-5xl space-y-6 px-4 py-8">
    <header className="space-y-3">
      <div className="flex flex-wrap items-center gap-3"><h1 className="text-3xl font-bold">Longest touchdown</h1><span className="rounded-full bg-amber-100 px-3 py-1 text-sm font-semibold text-amber-900">Experimental</span></div>
      <p className="text-lg text-slate-700">{season.season} · Week {week?.week ?? "unavailable"} · {week?.games.length ?? 0} games</p>
      <p className="rounded-xl border border-amber-200 bg-amber-50 p-4 text-sm text-amber-950">These saved estimates are not validated betting probabilities. Each game needs a new source capture and forecast to update. A DraftKings upload does not update this page.</p>
    </header>
    {seasons.length > 1 && <nav aria-label="Season" className="flex flex-wrap gap-2">{seasons.map((item) =>
      <Link key={item.season} href={`/nfl/longest-touchdown?season=${item.season}`}
        className={`rounded px-3 py-2 text-sm font-medium ${item.season === season.season ? "bg-slate-800 text-white" : "border bg-white text-slate-800"}`}>{item.season}</Link>)}</nav>}
    <nav aria-label="Week" className="flex flex-wrap gap-2">{season.weeks.map((item) =>
      <Link key={item.week} href={`/nfl/longest-touchdown?season=${season.season}&week=${item.week}`}
        className={`rounded px-3 py-2 text-sm font-medium ${item.week === week?.week ? "bg-emerald-800 text-white" : "border bg-white text-emerald-900"}`}>Week {item.week}</Link>)}</nav>
    {week && selected ? <>
      <form method="get" className="flex flex-wrap items-end gap-3">
        <input type="hidden" name="season" value={season.season} />
        <input type="hidden" name="week" value={week.week} />
        <label className="space-y-1 text-sm font-medium"><span className="block">Game</span>
          <select name="game" defaultValue={selected.game.game_id} className="rounded border bg-white px-3 py-2">{week.games.map((item) =>
            <option key={item.game.game_id} value={item.game.game_id}>{item.game.away} at {item.game.home} · {eastern(item.game.kickoff)}</option>)}</select>
        </label><button className="rounded bg-emerald-800 px-4 py-2 text-sm text-white" type="submit">Show game</button>
      </form>
      <section className="space-y-2 rounded-xl border bg-white p-4">
        <h2 className="text-xl font-semibold">{selected.game.away} at {selected.game.home}</h2>
        <p className="text-sm text-slate-600">Kickoff {eastern(selected.game.kickoff)} · Evidence frozen {eastern(selected.decisionAt)} · {selected.trainingGames} historical games · {selected.draws.toLocaleString()} simulations.</p>
        <p className="text-sm text-slate-700">{started ? "Kickoff has passed. This is the saved pregame forecast, not an updated result." : "Player roles remain conditional. This is not final game-day participation confirmation."}</p>
      </section>
      {blocked ? <p className="rounded-xl border border-red-300 bg-red-50 p-4 text-sm text-red-950">
        {newlyOut.length > 0
          ? `This saved forecast includes ${newlyOut.map((player) => `${player.name} (${player.team})`).join(", ")}, now ruled out. All game probabilities are hidden until the full forecast is rerun.`
          : "Current injury coverage could not be verified. Game probabilities are hidden until availability can be checked."}
      </p> : <>
        {availability?.complete && <p className="text-sm text-slate-600">Injury sources checked {eastern(availability.checkedAt)}. Unresolved player roles remain.</p>}
        <section className="overflow-hidden rounded-xl border bg-white shadow-sm">
          <div className="border-b p-4"><h2 className="text-xl font-semibold">Who could score the longest?</h2><p className="mt-1 text-sm text-slate-600">Rushing and receiving touchdowns in regulation only. Ties divide credit equally. A quarterback gets no credit for throwing a touchdown.</p></div>
          <div className="overflow-x-auto"><table className="w-full text-left text-sm"><thead className="bg-slate-50 text-slate-600"><tr><th className="p-4">Player</th><th className="p-4">Longest TD share</th><th className="p-4">Any TD</th><th className="p-4">40+ yard TD</th></tr></thead><tbody>{selected.players.map((player) =>
            <tr key={player.identity} className="border-t"><td className="p-4 font-medium">{player.name}</td><td className="p-4 font-semibold">{percent(player.longestShare)}</td><td className="p-4">{percent(player.anyTd)}</td><td className="p-4">{percent(player.longTd)}</td></tr>)}</tbody></table></div>
          <div className="border-t bg-slate-50 p-4 text-sm text-slate-600"><p>Other or unresolved scorers: {percent(selected.residual.reduce((sum, player) => sum + player.longestShare, 0))} combined longest-TD share. No modeled scrimmage touchdown: {percent(selected.noTd)}.</p><p className="mt-2">Scoring any touchdown and scoring the longest touchdown are different outcomes. Percentages are rounded; the full field includes other scorers.</p></div>
        </section>
      </>}
      <details className="rounded-xl border bg-white p-4"><summary className="cursor-pointer font-semibold">Evidence and limits</summary><div className="mt-3 space-y-2 text-sm text-slate-600"><p>{selected.modelVersion}. Simulation size is not evidence of accuracy. No sportsbook odds are used.</p><p>Inputs include the canonical schedule, play-by-play, GSIS identities, Sleeper and FantasyPros depth, and week-matched injury observations. Depth rank is not a workload forecast.</p><p>Unresolved availability and replacement work are not confirmed. Overtime, return touchdowns, and defensive touchdowns are excluded.</p><p>Players without a supported current role: {selected.unresolved.length ? selected.unresolved.join(", ") : "none listed"}. No observed role does not prove a zero chance.</p></div></details>
    </> : <p>No saved game forecasts for this week.</p>}
    <nav className="flex flex-wrap gap-4 text-sm"><Link className="text-emerald-800 underline" href="/nfl/game-leaders">Game leaders</Link><Link className="text-emerald-800 underline" href="/nfl/pbp">Play-by-play</Link><Link className="text-emerald-800 underline" href="/dfs/nfl">DFS workspace</Link></nav>
  </main>;
}
