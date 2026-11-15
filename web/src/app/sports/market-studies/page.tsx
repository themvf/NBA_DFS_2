import Link from "next/link";
import type { Metadata } from "next";

import type { ReactNode } from "react";
import { getCfbStudySelections } from "@/db/cfb-study-matches";
import { getLatestPatternWatchDataset } from "@/db/cfb-pattern-watch";
import { getNbaMarketStudyGames } from "@/db/nba-market-studies";
import { getNflMarketStudyGames } from "@/db/nfl-market-studies";

import { cfbDay, DAILY_STUDIES_PATH, nbaDay, nflDay, nflSeason, shiftDate, studyDate } from "@/lib/daily-market-studies";
import { cfbObservations, nbaObservations, nflObservations, qualifyingStudyMatches } from "@/lib/daily-study-matches";
import { easternDateString } from "@/lib/eastern-date";
import { PAPER_START } from "@/lib/nba-market-studies";
import { AllStudies } from "./all-studies";
import { StudyMatches } from "./study-matches";

export const dynamic = "force-dynamic";
export const metadata: Metadata = { title: "Daily Market Studies", robots: { index: false, follow: false } };

const dateLabel = new Intl.DateTimeFormat("en-US", {
  weekday: "long", month: "long", day: "numeric", year: "numeric", timeZone: "UTC",
});
const shortDate = new Intl.DateTimeFormat("en-US", { month: "short", day: "numeric", timeZone: "UTC" });
const easternStamp = new Intl.DateTimeFormat("en-US", {
  month: "short", day: "numeric", hour: "numeric", minute: "2-digit", timeZone: "America/New_York", timeZoneName: "short",
});
const units = new Intl.NumberFormat("en-US", { maximumFractionDigits: 2, signDisplay: "exceptZero" });
const button = "rounded-md border px-3 py-2 text-sm font-medium hover:bg-muted";
const href = (date: string) => DAILY_STUDIES_PATH + "?date=" + encodeURIComponent(date);
const WLP = (wins: number, losses: number, pushes: number) =>
  wins + losses + pushes ? [wins, losses, pushes].join("–") : "—";
const count = (n: number, noun: string) => n + " " + noun + (n === 1 ? "" : "s");

function Card({ title, label, children, link }: { title: string; label: string; children: ReactNode; link?: string }) {
  return <article className="flex min-w-0 flex-col rounded-xl border bg-card p-5">
    <header className="mb-4 border-b pb-3">
      <h2 className="text-xl font-semibold">{title}</h2>
      <p className="text-sm text-muted-foreground">{label}</p>
    </header>
    <div className="flex-1 space-y-4 text-sm">{children}</div>
    {link && <Link href={link} className="mt-5 w-fit text-sm font-medium text-primary underline underline-offset-4">Open {title} studies</Link>}
  </article>;
}

function Stat({ label, value, detail }: { label: string; value: string; detail?: string }) {
  return <div className="flex items-baseline justify-between gap-3">
    <dt>{label}{detail && <span className="block text-xs text-muted-foreground">{detail}</span>}</dt>
    <dd className="shrink-0 font-semibold tabular-nums">{value}</dd>
  </div>;
}

export default async function DailyMarketStudies({
  searchParams,
}: {
  searchParams: Promise<{ date?: string }>;
}) {
  const today = easternDateString();
  const params = await searchParams;
  const date = studyDate(params.date, today);
  const from = shiftDate(date, -6);

  const [nbaData, nflData, cfbData, cfbSelectionData] = await Promise.allSettled([
    getNbaMarketStudyGames(date),
    getNflMarketStudyGames(nflSeason(date)),
    getLatestPatternWatchDataset(),
    getCfbStudySelections(date),
  ]);
  const nbaRows = nbaData.status === "fulfilled" ? nbaData.value : null;
  const nflRows = nflData.status === "fulfilled" ? nflData.value : null;
  const cfb = cfbData.status === "fulfilled" ? cfbData.value : null;
  const nba = nbaRows && nbaDay(nbaRows.filter(row => row.game.date === date));
  const nbaThrough = nbaRows && nbaDay(nbaRows);
  const nfl = nflRows && nflDay(nflRows.filter(row => row.game.date === date));
  const nflThrough = nflRows && nflDay(nflRows.filter(row => row.game.date && row.game.date <= date));
  const cfbGrades = cfb?.results.filter(row => row.Date === date);
  const cfbRecent = cfbGrades && cfbDay(cfbGrades);
  const cfbThrough = cfb && new Map(cfbDay(cfb.results.filter(row => row.Date <= date)));
  const days = Array.from({ length: 7 }, (_, i) => shiftDate(date, -i));
  const nbaEntries = nbaRows?.flatMap(nbaObservations) ?? null;
  const nflEntries = nflRows?.flatMap(nflObservations) ?? null;
  const cfbEntries = cfbSelectionData.status === "fulfilled" && cfbSelectionData.value
    ? cfbObservations(cfbSelectionData.value) : null;
  const assessed = (entries: typeof nbaEntries) => entries &&
    new Set(entries.filter(row => row.game.date === date).map(row => row.game.id)).size;
  const matches = qualifyingStudyMatches([...(nbaEntries ?? []), ...(nflEntries ?? []), ...(cfbEntries ?? [])], date);

  return <main className="mx-auto max-w-7xl space-y-6 px-4 py-8 sm:px-6">
    <header className="space-y-2">
      <h1 className="text-3xl font-bold tracking-tight sm:text-4xl">Daily Market Studies</h1>
      <p className="text-sm text-muted-foreground">NBA, NFL and CFB results for {dateLabel.format(new Date(date))} · Eastern game date</p>
      <a href="#all-studies" className="inline-block text-sm font-medium underline underline-offset-4">Jump to all study results</a>
    </header>

    <div className="flex flex-wrap items-center justify-between gap-3 rounded-xl border bg-card p-3">
      <form action={DAILY_STUDIES_PATH} className="flex flex-wrap items-center gap-2">
        <label htmlFor="game-date" className="text-sm font-medium">Game date</label>
        <input id="game-date" name="date" type="date" defaultValue={date} max={today} required className="rounded-md border bg-background px-2 py-2 text-sm" />
        <button type="submit" className={button}>View date</button>
      </form>
      <nav aria-label="Choose game date" className="flex flex-wrap items-center gap-2">
        <Link className={button} href={href(shiftDate(date, -1))}>Previous day</Link>
        {date < today && <Link className={button} href={href(shiftDate(date, 1))}>Next day</Link>}
        <Link className={button} href={href(today)}>Today</Link>
      </nav>
    </div>

    <StudyMatches matches={matches} date={date} coverage={{
      nba: assessed(nbaEntries), nfl: assessed(nflEntries), cfb: assessed(cfbEntries),
    }} />

    <p className="text-xs text-muted-foreground">Records show wins–losses–pushes. Cumulative results run through the selected game date. Each sport uses its own study and grading rules.</p>
    <section aria-label="Sport summaries" className="grid gap-4 lg:grid-cols-3">
      <Card title="NBA" label="Archived Pinnacle grades and NBA-05" link={"/nba/market-studies?date=" + date}>
        {nba && nbaThrough ? <>
          <dl className="space-y-2">
            <Stat label="Final games" value={nba.finals + "/" + nba.games} detail={nba.games ? undefined : "No slate in the study feed"} />
            <Stat label="Spread grades" value={String(nba.ats)} detail={count(nbaThrough.ats, "grade") + " through date"} />
            <Stat label="Over record" value={WLP(nba.totals.win, nba.totals.loss, nba.totals.push)} detail={WLP(nbaThrough.totals.win, nbaThrough.totals.loss, nbaThrough.totals.push) + " through date"} />
          </dl>
          <div className="border-t pt-3">
            <h3 className="font-medium">NBA-05 · Over 235+</h3>
            {date < PAPER_START ? <p className="text-muted-foreground">Before trial</p> : <dl className="mt-2 space-y-2">
              <Stat label="Paper record" value={WLP(nba.paper.result.win, nba.paper.result.loss, nba.paper.result.push)} detail={WLP(nbaThrough.paper.result.win, nbaThrough.paper.result.loss, nbaThrough.paper.result.push) +
                (nbaThrough.paper.result.priced ? " · " + units.format(nbaThrough.paper.result.units) + " u" : "") + " through date"} />
              <Stat label="Pending picks" value={String(nba.paper.pending)} />
              <Stat label="Games without an entry quote" value={String(nba.paper.missing)} />
            </dl>}
          </div>
        </> : <p role="status">NBA study data is temporarily unavailable.</p>}
      </Card>

      <Card title="NFL" label="Archived Pinnacle game grades" link={"/nfl/market-studies" + (nfl?.weeks.length === 1 ? "?week=" + nfl.weeks[0] : "")}>
        {nfl && nflThrough ? <dl className="space-y-2">
          <Stat label="Final games" value={nfl.final + "/" + nfl.games} detail={nfl.games ? undefined : "No games in the study feed"} />
          <Stat label="Favorite ATS" value={WLP(nfl.ats.wins, nfl.ats.losses, nfl.ats.pushes)} detail={WLP(nflThrough.ats.wins, nflThrough.ats.losses, nflThrough.ats.pushes) +
            (nflThrough.ats.wins + nflThrough.ats.losses + nflThrough.ats.pushes ? " · " + units.format(nflThrough.ats.units) + " u" : "") + " season through date"} />
          <Stat label="Over record" value={WLP(nfl.over.wins, nfl.over.losses, nfl.over.pushes)} detail={WLP(nflThrough.over.wins, nflThrough.over.losses, nflThrough.over.pushes) + " season through date"} />
          <Stat label="Moneyline favorite" value={WLP(nfl.moneylineFavorite.wins, nfl.moneylineFavorite.losses, nfl.moneylineFavorite.pushes)} detail={WLP(nflThrough.moneylineFavorite.wins, nflThrough.moneylineFavorite.losses, nflThrough.moneylineFavorite.pushes) + " season through date"} />
          <Stat label="Games with spread / total / ML prices" value={[nfl.spread, nfl.total, nfl.moneyline].join(" / ")} />
        </dl> : <p role="status">NFL study data is temporarily unavailable.</p>}
      </Card>

      <Card title="CFB" label="Pattern Watch · T1–T6" link="/cfb/pattern-watch#patterns">
        {cfb && cfbRecent && cfbThrough ? <>
          <p className="text-xs text-muted-foreground">Last published {easternStamp.format(new Date(cfb.generatedAt))}</p>
          {!cfbRecent.length && <p className="text-muted-foreground">No closing grades published for this date.</p>}
          <dl className="space-y-2">
            {cfb.patterns.map(pattern => {
              const day = cfbRecent.find(([id]) => id === pattern.Pattern)?.[1];
              const overall = cfbThrough.get(pattern.Pattern);
              return <Stat key={pattern.Pattern} label={pattern.Pattern}
                value={day ? WLP(day.wins, day.losses, day.pushes) : "—"}
                detail={overall ? WLP(overall.wins, overall.losses, overall.pushes) + " · " + units.format(overall.units) + " u through date" : undefined} />;
            })}
          </dl>
          <p className="text-xs text-muted-foreground">Trigger grades can share a game. {cfb.verdictAt ? "Registered verdict recorded." : "Registered verdict pending."}</p>
        </> : <p role="status">{cfbData.status === "rejected" ? "CFB study data is temporarily unavailable." : "Awaiting a published CFB dataset."}</p>}
      </Card>
    </section>

    <AllStudies nba={nbaRows} nfl={nflRows} cfb={cfb} cfbUnavailable={cfbData.status === "rejected"} date={date} />

    <section className="rounded-xl border bg-card p-5" aria-labelledby="recent-grades">
      <h2 id="recent-grades" className="text-lg font-semibold">Seven days of grades</h2>
      <p className="mb-3 text-xs text-muted-foreground">NBA and NFL columns count graded games. CFB counts individual closing trigger grades.</p>
      <div className="overflow-x-auto">
        <table className="w-full min-w-[28rem] text-left text-sm">
          <thead><tr className="border-b text-muted-foreground">
            <th scope="col" className="py-2">Game date</th><th scope="col">NBA spread / total</th><th scope="col">NFL spread / total</th><th scope="col">CFB closing</th>
          </tr></thead>
          <tbody>{days.map(day => {
            const n = nbaRows && nbaDay(nbaRows.filter(row => row.game.date === day));
            const f = nflRows && nflSeason(day) === nflSeason(date) ? nflDay(nflRows.filter(row => row.game.date === day)) : null;
            return <tr key={day} className="border-b last:border-0">
              <th scope="row" className="py-2 font-medium"><Link href={href(day)} aria-current={day === date ? "date" : undefined} className="underline underline-offset-4">{shortDate.format(new Date(day))}</Link></th>
              <td className="tabular-nums">{n ? n.ats + " / " + n.totals.games : "—"}</td>
              <td className="tabular-nums">{f ? (f.ats.wins + f.ats.losses + f.ats.pushes) + " / " + (f.over.wins + f.over.losses + f.over.pushes) : "—"}</td>
              <td className="tabular-nums">{cfb ? cfb.results.filter(row => row.Date === day).length : "—"}</td>
            </tr>;
          })}</tbody>
        </table>
      </div>
      <p className="mt-3 text-xs text-muted-foreground">Window begins {shortDate.format(new Date(from))}. Grades reflect the latest available outcomes and can update as games settle.</p>
    </section>
  </main>;
}
