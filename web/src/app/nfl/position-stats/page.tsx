import Link from "next/link";
import { connection } from "next/server";
import { getNflPositionStatsWeek } from "@/db/nfl-position-stats";
import { GROUPS, POSITION_STATS_VERSION, TD_SCALE_CAP, type PlayerRow, type Source, type Stats } from "@/lib/nfl/position-stats";

export const dynamic = "force-dynamic";
export const metadata = { title: "NFL Position Stats", description: "Expected RB, WR and TE stats for the week, by player, team and position." };

const yds = (v: number) => Math.round(v).toLocaleString("en-US");
const one = (v: number) => v.toFixed(1);
const td = (v: number) => v.toFixed(2);
const eastern = (value: string) => new Intl.DateTimeFormat("en-US", {
  timeZone: "America/New_York", weekday: "short", month: "short", day: "numeric", hour: "numeric", minute: "2-digit",
}).format(new Date(value));
const SOURCE_STYLE: Record<Source, string> = {
  vegas: "bg-emerald-100 text-emerald-900", sim: "bg-slate-100 text-slate-700", model: "bg-indigo-100 text-indigo-900", none: "bg-amber-100 text-amber-900",
};
const SOURCE_LABEL: Record<Source, string> = { vegas: "Vegas line", sim: "team simulation", model: "DFS model", none: "no estimate" };

function Cell({ value, source, own }: { value: string; source: Source; own?: string }) {
  const title = own && own !== value ? `${SOURCE_LABEL[source]}: ${own} on its own, ${value} after the team budget` : SOURCE_LABEL[source];
  return <td className="p-2 text-right tabular-nums"><span title={title} className={`rounded px-1.5 py-0.5 ${source === "sim" ? "" : SOURCE_STYLE[source]}`}>{value}</span></td>;
}

function StatCard({ title, stats, teams, emphasis }: { title: string; stats: Stats; teams: number; emphasis?: boolean }) {
  const per = (v: number) => (teams ? v / teams : 0);
  const items: [string, string, string][] = [
    ["Rushing yards", yds(stats.rushYds), yds(per(stats.rushYds))],
    ["Receptions", one(stats.rec), one(per(stats.rec))],
    ["Receiving yards", yds(stats.recYds), yds(per(stats.recYds))],
    ["Touchdowns", one(stats.tds), td(per(stats.tds))],
  ];
  return <div className={`rounded-xl border p-4 shadow-sm ${emphasis ? "border-indigo-300 bg-indigo-50" : "bg-white"}`}>
    <h3 className="text-sm font-bold">{title}</h3>
    <dl className="mt-2 space-y-1 text-sm">{items.map(([label, total, avg]) => <div key={label} className="flex items-baseline justify-between gap-2">
      <dt className="text-slate-600">{label}</dt><dd className="tabular-nums"><strong>{total}</strong> <span className="text-xs text-slate-500">· {avg}/team</span></dd></div>)}</dl>
  </div>;
}

export default async function NflPositionStatsPage({ searchParams }: { searchParams: Promise<{ team?: string; position?: string }> }) {
  await connection();
  const params = await searchParams;
  const week = await getNflPositionStatsWeek();
  const teams = [...week.teams].sort((a, b) => (b.total.rushYds + b.total.recYds) - (a.total.rushYds + a.total.recYds));
  const teamCodes = teams.map((t) => t.team).sort();
  const team = teamCodes.includes(params.team ?? "") ? params.team! : "ALL";
  const position = (GROUPS as readonly string[]).includes(params.position ?? "") ? params.position! : "ALL";
  const players = week.players
    .filter((p) => (team === "ALL" || p.team === team) && (position === "ALL" || p.group === position))
    .filter((p) => p.stats.rushYds + p.stats.recYds >= 1 || p.stats.tds >= 0.02)
    .sort((a, b) => (b.stats.rushYds + b.stats.recYds) - (a.stats.rushYds + a.stats.recYds));
  const vegasCount = week.players.filter((p) => !p.ruledOut && Object.values(p.sources).includes("vegas")).length;
  const counted = week.players.filter((p) => !p.ruledOut).length;
  const ruledOut = week.players.filter((p) => p.ruledOut);

  return <main className="mx-auto max-w-7xl space-y-6 p-5 text-slate-900">
    <header className="flex flex-wrap items-start justify-between gap-4"><div>
      <p className="text-xs font-semibold uppercase tracking-widest text-indigo-700">NFL / WEEKLY RESEARCH</p>
      <h1 className="mt-1 text-3xl font-bold">Expected stats by position</h1>
      <p className="mt-2 max-w-3xl text-sm text-slate-600">What running backs, wide receivers and tight ends should produce this week, team by team and across the league. Each team gets a budget of rushing yards, catches and receiving yards, and its players split it, so a team&apos;s players add up. The budget comes from a game simulation, with receiving yards set by the quarterback&apos;s Vegas passing line once it is posted. Players with a full set of Vegas lines take their share by those lines, the rest by the simulation. Touchdowns come from the anytime-TD line, or the DFS model when there is none.</p>
    </div><nav className="flex gap-2 text-sm"><Link href="/nfl/game-leaders" className="rounded-lg border bg-white px-3 py-2 font-medium">Game leaders</Link><Link href="/nfl/projections" className="rounded-lg border bg-white px-3 py-2 font-medium">Projections</Link></nav></header>

    <section className="grid gap-3 rounded-xl border bg-white p-4 text-sm shadow-sm sm:grid-cols-4">
      <div><p className="text-xs text-slate-500">Week</p><strong>{week.season} · Week {week.week}</strong><p className="text-xs text-slate-500">{week.teamCount} teams, {week.teamCount / 2} games</p></div>
      <div><p className="text-xs text-slate-500">Team simulation</p><strong>{eastern(week.simulatedAt)}</strong></div>
      <div><p className="text-xs text-slate-500">Vegas lines</p><strong>{vegasCount} of {counted} players</strong><p className="text-xs text-slate-500">{week.newestLine ? `newest ${eastern(week.newestLine)}` : "none captured yet"}</p></div>
      <div><p className="text-xs text-slate-500">Injury check</p><strong>{week.availability.checked} of {week.availability.checked + week.availability.incomplete} games</strong><p className="text-xs text-slate-500">{ruledOut.length} ruled-out players left out</p></div>
    </section>

    {week.errors.map((e) => <p key={e} role="status" className="rounded-xl border border-amber-200 bg-amber-50 p-3 text-sm">{e}</p>)}
    {week.availability.incomplete > 0 && <p role="status" className="rounded-xl border border-amber-200 bg-amber-50 p-3 text-sm">Injury sources were not fresh enough to check {week.availability.incomplete} game{week.availability.incomplete === 1 ? "" : "s"}; a player ruled out there may still be counted.</p>}
    {week.skippedGames.length > 0 && <p role="status" className="rounded-xl border border-amber-200 bg-amber-50 p-3 text-sm">Not simulated, so not on this page: {week.skippedGames.map((g) => g.gameId.replace(/^\d{4}_\d{2}_/, "").replace("_", " @ ")).join(", ")}.</p>}

    <section><h2 className="mb-3 text-xl font-bold">League totals</h2>
      <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
        {GROUPS.map((g) => <StatCard key={g} title={`${g}s`} stats={week.league[g]} teams={week.teamCount} />)}
        <StatCard title="RB + WR + TE" stats={week.league.ALL} teams={week.teamCount} emphasis />
      </div>
      <p className="mt-2 text-xs text-slate-500">Sums over every counted player in the simulated games; per team divides by the {week.teamCount} teams playing. Quarterbacks and the simulation&apos;s unnamed &ldquo;other player&rdquo; share are not included.</p>
    </section>

    <section className="rounded-xl border bg-white p-4 shadow-sm"><h2 className="text-xl font-bold">By team</h2>
      <p className="text-xs text-slate-500">Ordered by RB + WR + TE scrimmage yards. Implied points are from the latest pregame spread and total. &ldquo;Rec yds budget&rdquo; says whether the team&apos;s receiving yards come from its quarterback&apos;s passing line or the simulation; &ldquo;lines&rdquo; counts the players whose share comes from Vegas.</p>
      <div className="mt-3 overflow-x-auto"><table className="w-full min-w-[1100px] text-left text-sm">
        <thead><tr className="text-xs text-slate-500"><th colSpan={3}></th><th colSpan={4} className="border-l p-1 text-center">RB</th><th colSpan={3} className="border-l p-1 text-center">WR</th><th colSpan={3} className="border-l p-1 text-center">TE</th><th colSpan={3} className="border-l p-1 text-center">Total</th><th></th></tr>
          <tr className="border-b text-xs text-slate-600"><th className="p-2">Team</th><th className="p-2">Game</th><th className="p-2 text-right">Implied pts</th>
            <th className="border-l p-2 text-right">Rush yds</th><th className="p-2 text-right">Rec</th><th className="p-2 text-right">Rec yds</th><th className="p-2 text-right">TD</th>
            <th className="border-l p-2 text-right">Rec</th><th className="p-2 text-right">Rec yds</th><th className="p-2 text-right">TD</th>
            <th className="border-l p-2 text-right">Rec</th><th className="p-2 text-right">Rec yds</th><th className="p-2 text-right">TD</th>
            <th className="border-l p-2 text-right">Yds</th><th className="p-2 text-right">Rec</th><th className="p-2 text-right">TD</th><th className="p-2">Rec yds budget</th></tr></thead>
        <tbody>{teams.map((t) => <tr key={t.team} className="border-b last:border-0">
          <td className="p-2 font-bold"><Link className="underline decoration-slate-300" href={`/nfl/position-stats?team=${t.team}`}>{t.team}</Link></td>
          <td className="p-2 text-xs text-slate-600">{t.home ? "vs" : "@"} {t.opponent} · {eastern(t.kickoff)}</td>
          <td className="p-2 text-right tabular-nums">{t.impliedPoints == null ? "—" : one(t.impliedPoints)}</td>
          <td className="border-l p-2 text-right tabular-nums">{yds(t.groups.RB.rushYds)}</td><td className="p-2 text-right tabular-nums">{one(t.groups.RB.rec)}</td><td className="p-2 text-right tabular-nums">{yds(t.groups.RB.recYds)}</td><td className="p-2 text-right tabular-nums">{td(t.groups.RB.tds)}</td>
          <td className="border-l p-2 text-right tabular-nums">{one(t.groups.WR.rec)}</td><td className="p-2 text-right tabular-nums">{yds(t.groups.WR.recYds)}</td><td className="p-2 text-right tabular-nums">{td(t.groups.WR.tds)}</td>
          <td className="border-l p-2 text-right tabular-nums">{one(t.groups.TE.rec)}</td><td className="p-2 text-right tabular-nums">{yds(t.groups.TE.recYds)}</td><td className="p-2 text-right tabular-nums">{td(t.groups.TE.tds)}</td>
          <td className="border-l p-2 text-right font-bold tabular-nums">{yds(t.total.rushYds + t.total.recYds)}</td><td className="p-2 text-right tabular-nums">{one(t.total.rec)}</td><td className="p-2 text-right font-bold tabular-nums">{td(t.total.tds)}</td>
          <td className="p-2 text-xs text-slate-600">{t.receivingBudget.source === "vegas" ? <span className={`rounded px-1 ${SOURCE_STYLE.vegas}`}>{t.receivingBudget.quarterback} {yds(t.receivingBudget.passingYards ?? 0)} pass yds</span> : "simulation"}<span className="block">lines: {t.vegasPlayers} of {t.players}</span>{t.ruledOut.length ? <span className="block text-amber-800">Out, share given to teammates: {t.ruledOut.join(", ")}</span> : null}</td>
        </tr>)}</tbody></table></div>
    </section>

    <section className="rounded-xl border bg-white p-4 shadow-sm"><div className="flex flex-wrap items-end justify-between gap-3"><div><h2 className="text-xl font-bold">Players</h2>
      <p className="text-xs text-slate-500">Highlighted numbers come from a <span className={`rounded px-1 ${SOURCE_STYLE.vegas}`}>Vegas line</span> or the <span className={`rounded px-1 ${SOURCE_STYLE.model}`}>DFS model</span>; plain numbers are the team simulation. Every number is after the team budget; hover a highlighted one to see the player&apos;s own number. Players under 1 expected yard are hidden.</p></div>
      <form action="/nfl/position-stats" method="get" className="flex flex-wrap items-end gap-2 text-sm">
        <label>Team<select name="team" defaultValue={team} className="ml-2 rounded border p-2"><option>ALL</option>{teamCodes.map((v) => <option key={v}>{v}</option>)}</select></label>
        <label>Position<select name="position" defaultValue={position} className="ml-2 rounded border p-2"><option>ALL</option>{GROUPS.map((v) => <option key={v}>{v}</option>)}</select></label>
        <button type="submit" className="rounded bg-indigo-700 px-3 py-2 font-medium text-white">Apply</button></form></div>
      <div className="mt-4 overflow-x-auto"><table className="w-full min-w-[760px] text-left text-sm">
        <thead><tr className="border-b text-xs text-slate-600"><th className="p-2">Player</th><th className="p-2">Pos</th><th className="p-2">Team</th><th className="p-2 text-right">Rush yds</th><th className="p-2 text-right">Rec</th><th className="p-2 text-right">Rec yds</th><th className="p-2 text-right">Scrimmage yds</th><th className="p-2 text-right">TD</th></tr></thead>
        <tbody>{players.map((p: PlayerRow) => <tr key={`${p.team}-${p.identity}`} className={`border-b last:border-0 ${p.ruledOut ? "text-slate-400 line-through" : ""}`}>
          <td className="p-2 font-medium">{p.name}{p.ruledOut && <span className="ml-2 text-xs no-underline">ruled out, not counted</span>}</td><td className="p-2">{p.group}</td><td className="p-2">{p.team}</td>
          <Cell value={yds(p.stats.rushYds)} own={yds(p.own.rushYds)} source={p.sources.rushYds} /><Cell value={one(p.stats.rec)} own={one(p.own.rec)} source={p.sources.rec} /><Cell value={yds(p.stats.recYds)} own={yds(p.own.recYds)} source={p.sources.recYds} />
          <td className="p-2 text-right font-bold tabular-nums">{yds(p.stats.rushYds + p.stats.recYds)}</td><Cell value={td(p.stats.tds)} source={p.sources.tds} />
        </tr>)}</tbody></table></div>
      {!players.length && <p className="p-4 text-sm text-slate-500">No players match these filters.</p>}
    </section>

    <footer className="space-y-1 text-xs text-slate-500">
      <p>Method ({POSITION_STATS_VERSION}): each player&apos;s own estimate is the game-leaders simulation&apos;s mean, which splits each team&apos;s sampled carries and targets among its players, or, with a full set of lines (RB: rushing yards, receptions, anytime TD; WR/TE: receptions, receiving yards, anytime TD), the calibrated line for each stat that has one, as the DFS optimizer does. Lines priced one player at a time do not add up to a team, so each team&apos;s RB/WR/TE total is set first (receiving yards: the quarterback&apos;s calibrated passing line times the simulation&apos;s RB/WR/TE share of team receiving yards, when posted; otherwise and for catches and rushing yards: the simulation&apos;s total) and split in proportion to the players&apos; own estimates. Touchdowns without a line are the DFS model&apos;s rushing + receiving TDs, scaled by the share of his model yardage the simulation gives him (at most {TD_SCALE_CAP}x), a stated rule.</p>
      <p>Not validated as a forecast: the simulation has not consistently beaten a recent-average baseline, and team totals are not reconciled with the Vegas implied points. A player ruled out after the simulation ran is removed and his share of the yards and catches goes to his teammates pro rata; his touchdowns are not reassigned. Touchdowns are not budgeted against the implied points.</p>
    </footer>
  </main>;
}
