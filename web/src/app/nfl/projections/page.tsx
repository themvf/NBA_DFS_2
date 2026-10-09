import Link from "next/link";
import { getNflRollingProjectionBoard, type RollingPlayer } from "@/db/nfl-rolling-projections";
import RollingRefresh from "./rolling-refresh";

export const dynamic = "force-dynamic";
export const metadata = { title: "NFL Rolling Projections", description: "Salary-independent NFL player projections and quarterback matchup evidence." };

const positions = ["ALL", "QB", "RB", "WR", "TE", "K", "DST"];
const number = (value: number | null, digits = 1) => value == null ? "—" : value.toFixed(digits);
const percent = (value: number | null) => value == null ? "—" : `${(value * 100).toFixed(0)}%`;
const signed = (value: number | null, digits = 1) => value == null ? "—" : `${value > 0 ? "+" : ""}${value.toFixed(digits)}`;
const eastern = (value: string) => new Intl.DateTimeFormat("en-US", {
  timeZone: "America/New_York", month: "short", day: "numeric", hour: "numeric", minute: "2-digit", timeZoneName: "short",
}).format(new Date(value));

function QbCard({ player }: { player: RollingPlayer }) {
  const context = player.matchup;
  return <article className="rounded-xl border border-indigo-200 bg-white p-4 shadow-sm">
    <div className="flex flex-wrap items-start justify-between gap-3"><div><h3 className="text-lg font-bold">{player.name}</h3><p className="text-sm text-slate-600">{player.team} vs {player.opponent} · {eastern(player.kickoff)}</p></div>
      <div className="text-right"><div className="text-2xl font-bold text-indigo-800">{number(player.projected)} <span className="text-sm font-normal">DK pts</span></div><div className="text-xs text-slate-500">{number(player.floor)} floor · {number(player.ceiling)} ceiling</div></div></div>
    <div className="mt-3 grid grid-cols-2 gap-2 text-sm sm:grid-cols-4">
      <div className="rounded-lg bg-slate-50 p-2"><p className="text-xs text-slate-500">Implied points</p><strong>{number(context?.impliedPoints ?? null)}</strong></div>
      <div className="rounded-lg bg-slate-50 p-2"><p className="text-xs text-slate-500">Spread / total</p><strong>{signed(context?.teamSpread ?? null)} / {number(context?.gameTotal ?? null)}</strong></div>
      <div className="rounded-lg bg-slate-50 p-2"><p className="text-xs text-slate-500">Close-game dropbacks</p><strong>{percent(context?.closeDropbackRate ?? null)}</strong><p className="text-[11px] text-slate-500">{context?.closePlays ?? 0} plays</p></div>
      <div className="rounded-lg bg-slate-50 p-2"><p className="text-xs text-slate-500">Opponent pass EPA effect</p><strong>{signed(context?.opponentAdjustedEpa ?? null, 3)}</strong><p className="text-[11px] text-slate-500">{context?.opponentDropbacks ?? 0} dropbacks</p></div>
    </div>
    <p className="mt-3 text-xs text-slate-700">Opponent allowed {context?.opponentCompetitiveDrives ? `${context.opponentTouchdownDrives}/${context.opponentCompetitiveDrives}` : "—"} touchdowns per competitive drives. This team dropped back {percent(context?.trailingDropbackRate ?? null)} when trailing by 8+ and {percent(context?.leadingDropbackRate ?? null)} when leading by 8+.</p>
    <p className="mt-2 text-xs text-slate-500">{context?.status === "ready" ? "Matchup sample complete under the current thresholds." : context?.reason ?? "Matchup evidence unavailable."} {context?.oddsCapturedAt ? `Odds: ${eastern(context.oddsCapturedAt)} from ${context.bookmakerCount ?? 0} books.` : ""}</p>
  </article>;
}

export default async function NflRollingProjectionsPage({ searchParams }: {
  searchParams: Promise<{ week?: string; position?: string; team?: string }>;
}) {
  const params = await searchParams;
  const board = await getNflRollingProjectionBoard(params.week);
  const position = positions.includes(params.position ?? "") ? params.position! : "ALL";
  const teams = [...new Set(board.players.map(player => player.team))].sort();
  const team = teams.includes(params.team ?? "") ? params.team! : "ALL";
  const selected = board.players.filter(player => (position === "ALL" || player.position === position)
    && (team === "ALL" || player.team === team)).sort((a, b) => (b.projected ?? -1) - (a.projected ?? -1));
  const qbs = board.players.filter(player => player.position === "QB" && player.status !== "out" && player.projected != null)
    .sort((a, b) => (b.projected ?? 0) - (a.projected ?? 0));
  return <main className="mx-auto max-w-7xl space-y-6 p-5 text-slate-900">
    <header className="flex flex-wrap items-start justify-between gap-4"><div><p className="text-xs font-semibold uppercase tracking-widest text-indigo-700">NFL / PLAYER RESEARCH</p><h1 className="mt-1 text-3xl font-bold">Rolling projections</h1><p className="mt-2 max-w-3xl text-sm text-slate-600">The scheduled NFL model creates player forecasts without a DraftKings slate. This page reads its newest saved run and adds current pregame market and play-by-play context for quarterbacks.</p></div><div className="flex gap-2"><RollingRefresh /><Link href="/dfs/nfl" className="rounded-lg border border-slate-300 bg-white px-3 py-2 text-sm font-medium">Optimizer</Link></div></header>
    {!board.run ? <section className="rounded-xl border bg-white p-6"><h2 className="font-semibold">No projection run available</h2><p className="mt-2 text-sm text-slate-600">The scheduled projection pipeline has not saved a regular-season run yet.</p></section> : <>
      <section className="grid gap-3 rounded-xl border bg-white p-4 text-sm shadow-sm sm:grid-cols-4"><div><p className="text-xs text-slate-500">Week</p><strong>{board.run.season} · Week {board.run.week}</strong></div><div><p className="text-xs text-slate-500">Projection as of</p><strong>{eastern(board.run.asOf)}</strong></div><div><p className="text-xs text-slate-500">Upcoming games</p><strong>{board.upcomingGames}</strong></div><div><p className="text-xs text-slate-500">Model</p><strong>{board.run.modelVersion}</strong></div></section>
      {board.upcomingGames === 0 && <p role="status" className="rounded-xl border border-amber-200 bg-amber-50 p-4 text-sm">No unstarted games remain in this saved week. The next week will appear when the scheduled pipeline saves its projection run.</p>}
      {board.contextError && <p role="status" className="rounded-xl border border-amber-200 bg-amber-50 p-4 text-sm">{board.contextError}</p>}
      <section><div className="mb-3 flex flex-wrap items-end justify-between gap-2"><div><h2 className="text-xl font-bold">Quarterback assessments</h2><p className="text-sm text-slate-600">Ordered by projected DK points, without salary. Game script and opponent-adjusted rates are descriptive; they do not change the model projection.</p></div><span className="text-xs text-slate-500">Market/PbP read {board.matchupAsOf ? eastern(board.matchupAsOf) : "unavailable"}</span></div>
        {qbs.length ? <div className="grid gap-3 lg:grid-cols-2">{qbs.map(player => <QbCard key={player.id} player={player} />)}</div> : <p className="rounded-xl border bg-white p-4 text-sm text-slate-600">No projected quarterbacks have an unstarted game in this week.</p>}</section>
      <section className="rounded-xl border bg-white p-4 shadow-sm"><div className="flex flex-wrap items-end justify-between gap-3"><div><h2 className="text-xl font-bold">All player projections</h2><p className="text-xs text-slate-500">DK scoring · salary independent · {board.players.length} players with a verified upcoming team/opponent matchup</p></div>
        <form action="/nfl/projections" method="get" className="flex flex-wrap items-end gap-2 text-sm"><label>Week<select name="week" defaultValue={`${board.run.season}-${board.run.week}`} className="ml-2 rounded border p-2">{board.options.map(run => <option key={run.runId} value={`${run.season}-${run.week}`}>{run.season} W{run.week}</option>)}</select></label><label>Position<select name="position" defaultValue={position} className="ml-2 rounded border p-2">{positions.map(value => <option key={value}>{value}</option>)}</select></label><label>Team<select name="team" defaultValue={team} className="ml-2 rounded border p-2"><option>ALL</option>{teams.map(value => <option key={value}>{value}</option>)}</select></label><button type="submit" className="rounded bg-indigo-700 px-3 py-2 font-medium text-white">Apply</button></form></div>
        <div className="mt-4 overflow-x-auto"><table className="w-full min-w-[800px] text-left text-sm"><thead><tr className="border-b text-xs text-slate-600"><th className="p-2">Player</th><th className="p-2">Pos</th><th className="p-2">Matchup</th><th className="p-2 text-right">Projection</th><th className="p-2 text-right">Floor</th><th className="p-2 text-right">Median</th><th className="p-2 text-right">Ceiling</th><th className="p-2 text-right">Confidence</th><th className="p-2 text-right">History</th><th className="p-2">Status</th></tr></thead><tbody>{selected.map(player => <tr key={player.id} className="border-b last:border-0"><td className="p-2 font-medium">{player.name}</td><td className="p-2">{player.position}</td><td className="p-2">{player.team} vs {player.opponent}</td><td className="p-2 text-right font-bold">{number(player.projected)}</td><td className="p-2 text-right">{number(player.floor)}</td><td className="p-2 text-right">{number(player.median)}</td><td className="p-2 text-right">{number(player.ceiling)}</td><td className="p-2 text-right">{percent(player.confidence)}</td><td className="p-2 text-right">{player.historyGames}</td><td className="p-2 text-xs">{player.status.replaceAll("_", " ")}</td></tr>)}</tbody></table></div>
        {!selected.length && <p className="p-4 text-sm text-slate-500">No players match these filters.</p>}
        {board.omittedPlayers > 0 && <p className="mt-3 text-xs text-slate-500">{board.omittedPlayers} saved projection rows are hidden because their game started or the saved team/opponent did not match the schedule.</p>}
      </section>
      <p className="text-xs text-slate-500">The production projection is frozen at its run time; market/PbP evidence refreshes when you open the page or press Refresh. Positive opponent pass EPA effect means opponents passed more efficiently against that defense than in their other games. Competitive drives exclude kneel and clock-expired endings. The page refreshes automatically every five minutes while open.</p>
    </>}
  </main>;
}
