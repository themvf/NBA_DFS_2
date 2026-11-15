import Link from "next/link";
import { STUDY_MATCH_PERCENT, type StudyMatch } from "@/lib/daily-study-matches";
import { studyRegistry } from "@/lib/study-registry";

const easternTime = new Intl.DateTimeFormat("en-US", {
  hour: "numeric", minute: "2-digit", timeZone: "America/New_York", timeZoneName: "short",
});
const observedTime = new Intl.DateTimeFormat("en-US", {
  month: "short", day: "numeric", hour: "numeric", minute: "2-digit", timeZone: "America/New_York",
});
const percent = new Intl.NumberFormat("en-US", { maximumFractionDigits: 1, style: "percent" });
const href = { NBA: "/nba/market-studies", NFL: "/nfl/market-studies", CFB: "/cfb/pattern-watch#patterns" };

export function StudyMatches({
  matches, coverage, date,
}: {
  matches: StudyMatch[];
  coverage: { nba: number | null; nfl: number | null; cfb: number | null; cfbUnlocked: boolean };
  date: string;
}) {
  const groups = new Map<string, StudyMatch[]>();
  for (const match of matches) {
    const key = match.game.sport + ":" + match.game.id;
    const group = groups.get(key) ?? [];
    group.push(match);
    groups.set(key, group);
  }
  const coverageLabel = (n: number | null) => n === null ? "Unavailable" : n ? n + " assessed" : "No eligible entries";

  return <section aria-labelledby="study-matches" className="space-y-4 rounded-xl border bg-card p-4 sm:p-5">
    <header className="space-y-2">
      <div className="flex flex-wrap items-baseline justify-between gap-2">
        <h2 id="study-matches" className="text-xl font-semibold">Games matching a {STUDY_MATCH_PERCENT}%+ study</h2>
        <span className="text-sm font-medium tabular-nums">{groups.size} {groups.size === 1 ? "game" : "games"} · {matches.length} {matches.length === 1 ? "match" : "matches"}</span>
      </div>
      <p className="text-sm text-muted-foreground">
        Historical win rate on earlier game dates with finals observed before the matchup. Pushes are listed separately.
        Small samples can produce high rates. These are historical records and do not estimate tonight’s win probability.
      </p>
      <p className="text-xs text-muted-foreground">
        NBA {coverageLabel(coverage.nba)} · NFL {coverageLabel(coverage.nfl)} · CFB {coverage.cfbUnlocked ? coverageLabel(coverage.cfb) : "Locked"}
        {coverage.cfbUnlocked ? " · CFB uses saved board entries." : " · Unlock Pattern Watch below to include CFB."}
      </p>
    </header>
    {groups.size === 0 ? <p className="border-t pt-3 text-sm text-muted-foreground">No qualifying matches in the available evidence for this date.</p> : (
      <div className="divide-y border-t">
        {[...groups.entries()].map(([key, gameMatches]) => {
          const { game } = gameMatches[0];
          return <article key={key} className="space-y-2 py-3">
            <div className="flex flex-wrap items-center justify-between gap-2">
              <h3 className="font-semibold"><span className="mr-2 text-xs text-muted-foreground">{game.sport}</span>{game.away} at {game.home}</h3>
              <span className="text-xs text-muted-foreground">{easternTime.format(new Date(game.start))} · <Link className="underline underline-offset-4"
                href={game.sport === "NBA" ? href.NBA + "?date=" + date : href[game.sport]}>Study details</Link></span>
            </div>
            <div className="overflow-x-auto">
              <table className="w-full min-w-[42rem] text-left text-sm">
                <thead className="text-xs text-muted-foreground"><tr>
                  <th scope="col" className="py-1">Study and cohort</th>
                  <th scope="col" className="py-1">Matched side</th>
                  <th scope="col" className="py-1">Past rate</th>
                  <th scope="col" className="py-1">Wins–losses–pushes</th>
                </tr></thead>
                <tbody>{gameMatches.map(match => (
                  <tr key={match.id + match.market + match.cohort} className="border-t align-top">
                    <th scope="row" className="py-2 pr-3 font-medium">
                      {match.id} · {studyRegistry[match.id].title}
                      <span className="block text-xs font-normal text-muted-foreground">{match.cohort}</span>
                    </th>
                    <td className="py-2 pr-3">{match.side}
                      <span className="block text-xs text-muted-foreground">{match.market}{match.observedAt && " · " + observedTime.format(new Date(match.observedAt)) + " ET"}</span>
                    </td>
                    <td className="py-2 pr-3 font-semibold tabular-nums">{percent.format(match.rate)}</td>
                    <td className="py-2 tabular-nums">{match.record.wins}–{match.record.losses}–{match.record.pushes}
                      <span className="block text-xs text-muted-foreground">{match.decided} decided{match.decided < 20 && " · Small sample"}</span>
                    </td>
                  </tr>
                ))}</tbody>
              </table>
            </div>
          </article>;
        })}
      </div>
    )}
    <details className="border-t pt-2 text-xs">
      <summary className="cursor-pointer font-medium">Study coverage and rules</summary>
      <p className="mt-2 text-muted-foreground">
        Spread and total matches use the archived pregame observations from the sport studies. NBA-05 paper entries keep their original cutoff.
        Quote-timing errors and price-shopping diagnostics have no game win rate. Studies awaiting suitable data show no match.
        Earlier games with unavailable final-observation times are excluded.
      </p>
      <ul className="mt-2 grid gap-1 sm:grid-cols-2 lg:grid-cols-3">
        {Object.entries(studyRegistry).filter(([id]) => coverage.cfbUnlocked || !id.startsWith("T")).map(([id, definition]) =>
          <li key={id}>{id} · {definition.title} <span className="text-muted-foreground">· {definition.mode === "match" ? "Game matches" : definition.mode === "awaiting" ? "Awaiting evidence" : "Diagnostic"}</span></li>)}
      </ul>
    </details>
  </section>;
}
