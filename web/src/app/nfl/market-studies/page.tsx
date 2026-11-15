import { StudyPerformancePanel } from "@/app/sports/market-studies/study-performance-panel";
import { nflObservations } from "@/lib/daily-study-matches";
import { nflSeason, studyDate } from "@/lib/daily-market-studies";
import Link from "next/link";
import { studyHeading } from "@/lib/study-registry";
import type { Metadata } from "next";
import { getNflMarketStudyGames } from "@/db/nfl-market-studies";
import { easternDateString } from "@/lib/eastern-date";
import { summarize, type Result } from "@/lib/nfl/market-studies";

export const dynamic = "force-dynamic";
export const metadata: Metadata = { title: "NFL Market Studies" };

const pct = new Intl.NumberFormat("en-US", { style: "percent", maximumFractionDigits: 1 });
const number = new Intl.NumberFormat("en-US", { maximumFractionDigits: 2 });
const clock = new Intl.DateTimeFormat("en-US", {
  timeZone: "America/New_York", dateStyle: "medium", timeStyle: "short",
});

function complete(row: Result | undefined) {
  if (!row || row.wins + row.losses + row.pushes === 0) return "No graded games";
  const stakes = row.wins + row.losses + row.pushes;
  return <>{row.wins}–{row.losses}–{row.pushes}
    <span className="block text-xs text-muted-foreground">
      {number.format(row.units)} u · {pct.format(row.units / stakes)} return · {stakes} games
    </span>
  </>;
}

function Results({ title, week, overall }: { title: string; week: Result[]; overall: Result[] }) {
  const weekly = new Map(week.map(row => [row.label, row]));
  return <details className="rounded-lg border px-4 py-3">
    <summary className="cursor-pointer font-medium">{title}</summary>
    <div className="mt-3 overflow-x-auto">
      <table className="w-full text-left text-sm">
        <thead><tr><th scope="col">Group</th><th scope="col">Selected week</th><th scope="col">Season through week</th></tr></thead>
        <tbody>{overall.map(row => <tr key={row.label} className="border-t">
          <th scope="row" className="py-2 pr-3 font-medium">{row.label}</th>
          <td className="pr-3">{complete(weekly.get(row.label))}</td><td>{complete(row)}</td>
        </tr>)}</tbody>
      </table>
      {!overall.length && <p className="mt-2 text-muted-foreground">No graded games.</p>}
    </div>
  </details>;
}

function show(value: number | null, digits = 2) {
  return value === null ? "—" : value.toFixed(digits);
}

export default async function NflMarketStudies({ searchParams }: { searchParams: Promise<{ week?: string; date?: string; cohort?: string }> }) {
  const today = easternDateString();
  const { week: requested, date: requestedDate, cohort } = await searchParams;
  const date = studyDate(requestedDate, today);
  const season = nflSeason(date);
  const all = await getNflMarketStudyGames(season);
  const weeks = [...new Set(all.map(row => row.game.week))].sort((a, b) => a - b);
  const currentWeek = all.filter(row => row.game.date !== null && row.game.date <= date).at(-1)?.game.week ?? weeks[0] ?? 1;
  const week = weeks.includes(Number(requested)) ? Number(requested) : currentWeek;
  const selected = all.filter(row => row.game.week === week);
  const daily = summarize(selected);
  const overall = summarize(all.filter(row => row.game.week <= week));

  return <main className="mx-auto max-w-6xl space-y-6 px-4 py-8">
    <header className="space-y-2">
      <h1 className="text-3xl font-bold tracking-tight">NFL Market Studies</h1>
      <p className="max-w-3xl text-sm text-muted-foreground">
        {season} regular season · Week {week}. Exploratory results from archived Pinnacle prices and final full-game scores.
        Returns use one unit per side and refund pushes. Results update when the archive and final scores refresh.
      </p>
    </header>

    <StudyPerformancePanel entries={all.filter(row => row.game.date && row.game.date <= date && (requestedDate || row.game.week <= week)).flatMap(nflObservations)} date={date} initial={cohort} />

    <form action="/nfl/market-studies" className="flex flex-wrap items-center gap-3 text-sm">
      <label htmlFor="week" className="font-medium">Week</label>
      <select id="week" name="week" defaultValue={week} className="rounded-md border bg-background px-3 py-2">
        {weeks.map(value => <option key={value} value={value}>{value}</option>)}
      </select>
      <button type="submit" className="rounded-md border px-3 py-2 hover:bg-muted">Show week</button>
      <Link className="ml-auto underline underline-offset-4" href="/nfl/pickem">Pick’em ledger</Link>
    </form>

    <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
      {([["Final games", "final"], ["Spread quotes", "spread"], ["Total quotes", "total"], ["Moneyline quotes", "moneyline"]] as const)
        .map(([label, key]) => <div key={key} className="rounded-lg border p-3">
          <p className="text-xs font-medium text-muted-foreground">{label}</p>
          <p className="mt-1 text-xl font-semibold tabular-nums">{daily.coverage[key]}/{daily.coverage.games}</p>
          <p className="text-xs text-muted-foreground">{overall.coverage[key]}/{overall.coverage.games} season through week</p>
        </div>)}
    </div>

    <details open className="rounded-lg border px-4 py-3">
      <summary className="cursor-pointer font-medium">Game grades and archived quotes</summary>
      <div className="mt-3 overflow-x-auto">
        <table className="w-full text-left text-sm">
          <thead><tr><th scope="col">Matchup</th><th scope="col">Final · away–home</th><th scope="col">Home spread</th>
            <th scope="col">Total</th><th scope="col">Moneyline · away/home</th></tr></thead>
          <tbody>{selected.map(row => {
            const { game, margin, total, spreadQuotes, totalQuotes, moneylineQuotes } = row;
            const spread = spreadQuotes.last;
            const points = totalQuotes.last;
            const ml = moneylineQuotes.last;
            const cell = (quote: typeof spread, label: string, outcome?: string) => quote ? <>
              {label}{outcome && <span className="block">{outcome}</span>}
              <span className="block text-xs text-muted-foreground">
                {clock.format(new Date(quote.quote.observedAt))} ET · #{quote.quote.id}
                {game.kickoff && <> · {Math.round((Date.parse(game.kickoff) - Date.parse(quote.quote.observedAt)) / 60_000)} min before kickoff</>}
              </span>
            </> : "No eligible quote";
            return <tr key={game.id} className="border-t align-top">
              <th scope="row" className="py-2 pr-3 font-medium">{game.away} at {game.home}
                <span className="block text-xs text-muted-foreground">{game.date}</span></th>
              <td className="py-2 pr-3">{margin === null ? "Awaiting final" : game.awayScore + "–" + game.homeScore}</td>
              <td className="py-2 pr-3">{cell(spread, spread ? spread.line + " (" + spread.prices.join("/") + ")" : "",
                spread && margin !== null ? margin + spread.line === 0 ? "Push" : margin + spread.line > 0 ? "Home covered" : "Away covered" : undefined)}</td>
              <td className="py-2 pr-3">{cell(points, points ? points.line + " (" + points.prices.join("/") + ")" : "",
                points && total !== null ? total === points.line ? "Push" : total > points.line ? "Over" : "Under" : undefined)}</td>
              <td className="py-2">{cell(ml, ml ? ml.prices[1] + " / " + ml.prices[0] : "",
                ml && margin !== null ? margin === 0 ? "Push" : margin > 0 ? "Home won" : "Away won" : undefined)}</td>
            </tr>;
          })}</tbody>
        </table>
        {!selected.length && <p className="mt-2 text-muted-foreground">No scheduled games in this view.</p>}
      </div>
    </details>

    <section aria-label="Market studies" className="space-y-3">
      <Results title={studyHeading("NFL-01")} week={daily.spread} overall={overall.spread} />
      <Results title={studyHeading("NFL-02")} week={daily.rest} overall={overall.rest} />
      <Results title={studyHeading("NFL-03")} week={daily.venue} overall={overall.venue} />
      <Results title={studyHeading("NFL-04")} week={daily.movement} overall={overall.movement} />
      <Results title={studyHeading("NFL-05")} week={daily.totals} overall={overall.totals} />
      <Results title={studyHeading("NFL-06")} week={daily.teams} overall={overall.teams} />
      <details className="rounded-lg border px-4 py-3">
        <summary className="cursor-pointer font-medium">{studyHeading("NFL-07")}</summary>
        <p className="mt-2 text-sm text-muted-foreground">Mean absolute error in points on identical games with two distinct quotes.</p>
        {overall.timing.map((row, i) => <p key={row.market} className="mt-2 text-sm">
          <strong className="capitalize">{row.market}</strong> · Week {show(daily.timing[i].first)} → {show(daily.timing[i].last)}
          {" "}({daily.timing[i].n} games) · Season {show(row.first)} → {show(row.last)} ({row.n} games)
        </p>)}
      </details>
      <details className="rounded-lg border px-4 py-3">
        <summary className="cursor-pointer font-medium">{studyHeading("NFL-08")}</summary>
        <p className="mt-2 text-sm text-muted-foreground">Sides with a better price in the same capture, strictly before 60 minutes to each scheduled kickoff.</p>
        {(["spread", "total", "moneyline"] as const).map(market => <p key={market} className="mt-2 text-sm">
          <strong className="capitalize">{market}</strong> · Week {daily.shopping[market].better}/{daily.shopping[market].compared}
          {" "}· Season {overall.shopping[market].better}/{overall.shopping[market].compared}
        </p>)}
      </details>
      <Results title={studyHeading("NFL-10")} week={daily.moneyline} overall={overall.moneyline} />
      <details className="rounded-lg border px-4 py-3">
        <summary className="cursor-pointer font-medium">NFL-10 · Moneyline calibration</summary>
        <p className="mt-2 text-sm text-muted-foreground">
          Home win probability on games with a winner. The Brier comparison uses identical games and removes vig within each book
          before averaging the other books in Pinnacle’s selected capture.
        </p>
        {([[daily.calibration, "Week"], [overall.calibration, "Season"]] as const).map(([c, label]) =>
          <p key={label} className="mt-2 text-sm">{label} · {c.n} games · Paired Brier Pinnacle {show(c.pinnacleBrier, 4)}
            {" "}· consensus {show(c.consensusBrier, 4)} ({c.paired} games)</p>)}
        <div className="mt-3 overflow-x-auto">
          <table className="w-full text-left text-sm">
            <thead><tr><th scope="col">Pinnacle no-vig bin</th><th scope="col">Games</th><th scope="col">Mean forecast</th><th scope="col">Actual home wins</th></tr></thead>
            <tbody>{overall.calibration.bins.map(bin => <tr key={bin.label} className="border-t">
              <th scope="row" className="py-2">{bin.label}</th><td>{bin.n}</td>
              <td>{bin.probability === null ? "—" : pct.format(bin.probability)}</td>
              <td>{bin.actual === null ? "—" : pct.format(bin.actual)}</td>
            </tr>)}</tbody>
          </table>
        </div>
      </details>
      <Results title={studyHeading("NFL-11")} week={daily.restEdge} overall={overall.restEdge} />
    </section>

    <section className="rounded-lg border p-4">
      <h2 className="font-semibold">Studies awaiting evidence</h2>
      <ul className="mt-2 space-y-1 text-sm text-muted-foreground">
        <li>NFL-09 · Totals model needs original pregame game-total forecasts.</li>
        <li>NFL-12 · Injury and repricing needs timestamped official observations matched to before-and-after prices.</li>
        <li>NFL-13 · First-half markets need archived first-half prices and verified halftime scores.</li>
      </ul>
      <p className="mt-3 text-sm text-muted-foreground">
        First and last refer to eligible archive captures. The T−60 comparison is per game. Any future pick’em comparison must use
        its contest’s actual lock. There is no active forward paper rule on this dashboard.
      </p>
    </section>
  </main>;
}
