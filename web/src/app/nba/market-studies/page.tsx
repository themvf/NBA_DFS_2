import Link from "next/link";
import { studyHeading } from "@/lib/study-registry";
import type { Metadata } from "next";
import { getNbaMarketStudyGames } from "@/db/nba-market-studies";
import { easternDateString } from "@/lib/eastern-date";
import { PAPER_START, summarize } from "@/lib/nba-market-studies";

export const dynamic = "force-dynamic";
export const metadata: Metadata = { title: "NBA Market Studies" };

const number = new Intl.NumberFormat("en-US", { maximumFractionDigits: 2, signDisplay: "exceptZero" });
const percent = new Intl.NumberFormat("en-US", { style: "percent", maximumFractionDigits: 1 });
const stamp = new Intl.DateTimeFormat("en-US", {
  timeZone: "America/New_York", hour: "numeric", minute: "2-digit", timeZoneName: "short",
});
type Summary = ReturnType<typeof summarize>;
type Group = Summary["totals"][number] & { outright?: number };

function record(row: Group) {
  return row.games
    ? `${row.win}–${row.loss}–${row.push}${row.win + row.loss ? ` · ${percent.format(row.win / (row.win + row.loss))}` : ""}`
    : "Awaiting results";
}

function resultTable(label: string, daily: Group[], overall: Group[], prices = false) {
  return <details className="rounded-lg border px-4 py-3">
    <summary className="cursor-pointer font-medium">{label}</summary>
    <div className="mt-3 overflow-x-auto">
      <table className="w-full text-left text-sm">
        <thead className="text-muted-foreground">
          <tr><th scope="col">Group</th><th scope="col">Selected slate</th><th scope="col">Overall through slate</th></tr>
        </thead>
        <tbody>{overall.map((row, index) => {
          const day = daily[index];
          const cell = (value: Group) => <>
            {record(value)}
            {value.outright !== undefined && value.games > 0 && <span className="block text-muted-foreground">{value.outright}/{value.games} outright</span>}
            {prices && value.priced > 0 && <span className="block text-muted-foreground">{number.format(value.units)} units · {value.priced} priced</span>}
          </>;
          return <tr key={row.label} className="border-t"><th scope="row" className="py-2 pr-3 font-medium">{row.label}</th><td className="pr-3">{cell(day)}</td><td>{cell(row)}</td></tr>;
        })}</tbody>
      </table>
    </div>
  </details>;
}

export default async function NbaMarketStudies({ searchParams }: { searchParams: Promise<{ date?: string }> }) {
  const today = easternDateString();
  const { date: requested } = await searchParams;
  const all = await getNbaMarketStudyGames(today);
  const dates = [...new Set([today, ...all.map(row => row.game.date)])].sort().reverse();
  const date = requested && dates.includes(requested) ? requested : today;
  const day = all.filter(row => row.game.date === date);
  const daily = summarize(day);
  const overall = summarize(all.filter(row => row.game.date <= date));
  const dayPaper = daily.paper;
  const paper = overall.paper;
  const roi = paper.result.priced ? percent.format(paper.result.units / paper.result.priced) : null;
  const latestQuotes = day.map(row => ({
    row, quote: row.spreadQuotes.at(-1),
  }));

  return <main className="mx-auto max-w-6xl space-y-6 px-4 py-6">
    <header className="space-y-3">
      <div>
        <Link href="/nba/top-scorers" className="text-sm text-muted-foreground underline">NBA</Link>
        <h1 className="mt-1 text-3xl font-semibold tracking-tight">Market Studies</h1>
      </div>
      <p className="max-w-3xl text-sm text-muted-foreground">Daily and cumulative NBA research from archived Pinnacle quotes and verified final scores. The study records are exploratory. Returns assume one unit per priced observation.</p>
      <form action="/nba/market-studies" className="flex flex-wrap items-end gap-2">
        <label className="flex flex-col gap-1 text-sm">Slate date
          <select name="date" defaultValue={date} className="rounded-md border bg-background px-3 py-2">
            {dates.map(value => <option key={value} value={value}>{value}{value === today ? " · today" : ""}</option>)}
          </select>
        </label>
        <button className="rounded-md border px-3 py-2 text-sm">View slate</button>
      </form>
    </header>

    <section className="space-y-3 rounded-xl border p-4" aria-labelledby="paper-total">
      <div className="space-y-1">
        <h2 id="paper-total" className="text-lg font-semibold">NBA-05 · Over 235+ paper test</h2>
        <p className="text-sm text-muted-foreground">From Nov. 15, 2026. Latest valid Pinnacle total before 30 minutes to first scheduled tip. Outcomes use archived pregame quotes and full-game scores.</p>
      </div>
      <div className="grid gap-3 sm:grid-cols-2">
        <div><h3 className="text-sm font-medium">Selected slate</h3>
          <p className="mt-1 text-xl font-semibold tabular-nums">{date < PAPER_START ? "Before trial" : record(dayPaper.result)}</p>
          <p className="text-sm text-muted-foreground">{dayPaper.games} slate games · {dayPaper.pending} qualifying awaiting final · {dayPaper.missing} without an eligible quote</p>
        </div>
        <div><h3 className="text-sm font-medium">Overall through slate</h3>
          <p className="mt-1 text-xl font-semibold tabular-nums">{record(paper.result)}</p>
          <p className="text-sm text-muted-foreground">{paper.result.priced ? `${number.format(paper.result.units)} units · ${roi} ROI · ` : ""}{paper.games} slate games · {paper.missing} without an eligible quote</p>
        </div>
      </div>
    </section>

    <section className="space-y-2" aria-labelledby="daily-games">
      <h2 id="daily-games" className="text-lg font-semibold">Slate results</h2>
      <p className="text-sm text-muted-foreground">{daily.spreadGraded}/{daily.games} graded spreads on this slate · {overall.spreadGraded}/{overall.games} overall. Times are Eastern. Displayed lines are archived pre-tip observations.</p>
      {day.length === 0 ? <p className="rounded border p-3 text-sm">Awaiting verified schedule or game results for {date}.</p> :
        <div className="overflow-x-auto">
          <table className="w-full text-left text-sm">
            <thead className="text-muted-foreground"><tr>
              <th scope="col">Game</th><th scope="col">Pinnacle spread</th><th scope="col">Pinnacle total</th>
              <th scope="col">Final score</th><th scope="col">Over 235+ cutoff</th>
            </tr></thead>
            <tbody>{latestQuotes.map(({ row, quote }) => {
              const { game } = row;
              const favorite = row.spread === 0 ? "Pick’em" : row.spread === undefined ? null
                : `${row.homeFavorite ? game.home : game.away} −${Math.abs(row.spread)}`;
              return <tr key={game.id} className="border-t">
                <th scope="row" className="py-2 pr-3 font-medium">{game.away} at {game.home}</th>
                <td className="pr-3">{favorite ?? "Unavailable"}{quote && <span className="block text-muted-foreground">{stamp.format(new Date(quote.observedAt))}</span>}
                  {row.coverMargin !== null && <span className="block text-muted-foreground">{row.coverMargin > 0 ? "Covered" : row.coverMargin < 0 ? "Missed" : "Push"} · {number.format(row.coverMargin)} pts</span>}</td>
                <td className="pr-3">{row.total ?? "Unavailable"}{row.actualTotal !== null && row.total !== undefined &&
                  <span className="block text-muted-foreground">{row.actualTotal > row.total ? "Over" : row.actualTotal < row.total ? "Under" : "Push"}</span>}</td>
                <td className="pr-3">{row.actualTotal === null ? "Pending" : `${game.awayScore}–${game.homeScore}`}</td>
                <td>{date < PAPER_START ? "Before trial" : !row.hasPaperQuote ? "No eligible quote" :
                  row.paperPlay ? `Over ${row.paperQuote?.total} at ${number.format(row.paperQuote!.over_price!)}` : "Below 235"}</td>
              </tr>;
            })}</tbody>
          </table>
        </div>}
    </section>

    <section className="space-y-2" aria-labelledby="research-results">
      <h2 id="research-results" className="text-lg font-semibold">Study records</h2>
      <p className="text-sm text-muted-foreground">Records show wins, losses and pushes. Percentages exclude pushes. Overall results include the available Oct. 25 and November archive.</p>
      {resultTable(studyHeading("NBA-01"), daily.spread, overall.spread)}
      {resultTable(studyHeading("NBA-02"), daily.rest, overall.rest)}
      {resultTable(studyHeading("NBA-03"), daily.venue, overall.venue)}
      {resultTable(studyHeading("NBA-04"), daily.movement, overall.movement)}
      {resultTable(studyHeading("NBA-05"), daily.totals, overall.totals, true)}
      <details className="rounded-lg border px-4 py-3">
        <summary className="cursor-pointer font-medium">{studyHeading("NBA-06")} and {studyHeading("NBA-07")}</summary>
        <div className="mt-3 overflow-x-auto">
          <table className="w-full text-left text-sm">
            <thead className="text-muted-foreground"><tr><th scope="col">Team and role</th><th scope="col">Selected slate</th><th scope="col">Overall first / last quote</th><th scope="col">Overall median / trimmed mean</th></tr></thead>
            <tbody>{overall.teams.map(team => {
              const row = daily.teams.find(value => value.label === team.label);
              return <tr key={team.label} className="border-t">
                <th scope="row" className="py-2 pr-3 font-medium">{team.label}</th>
                <td className="pr-3">{row ? record(row.last) : "—"}</td>
                <td className="pr-3">{record(team.first)} / {record(team.last)}</td>
                <td>{number.format(team.median)} / {team.trimmedAverage === null ? "—" : number.format(team.trimmedAverage)} pts</td>
              </tr>;
            })}</tbody>
          </table>
        </div>
      </details>
      <section className="rounded-lg border px-4 py-3 text-sm">
        <h3 className="font-medium">{studyHeading("NBA-08")}</h3>
        <p className="text-muted-foreground">{daily.shopping.improvements}/{daily.shopping.comparisons} comparable sides had a better price on the selected slate · {overall.shopping.improvements}/{overall.shopping.comparisons} overall at the common cutoff.</p>
      </section>
      {resultTable(studyHeading("NBA-11"), daily.travel, overall.travel)}
      <dl className="rounded-lg border px-4 py-3 text-sm">
        <dt className="font-medium">{studyHeading("NBA-09")}</dt><dd className="mb-2 text-muted-foreground">Awaiting original point-in-time forecasts.</dd>
        <dt className="font-medium">{studyHeading("NBA-10")}</dt><dd className="mb-2 text-muted-foreground">Awaiting consistent two-sided book-level moneyline coverage.</dd>
        <dt className="font-medium">{studyHeading("NBA-12")}</dt><dd className="mb-2 text-muted-foreground">Awaiting matched official observations and before/after quotes.</dd>
        <dt className="font-medium">{studyHeading("NBA-13")}</dt><dd className="text-muted-foreground">Awaiting comparable first-half prices and verified halftime scores.</dd>
      </dl>
    </section>
  </main>;
}
