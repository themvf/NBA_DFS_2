export const dynamic = "force-dynamic";

import Link from "next/link";
import { unstable_cache } from "next/cache";
import { getPickemLiveCard, getPickemLiveWeeks } from "@/db/queries";
import {
  buildBoardGames,
  groupByWindow,
  outcomeOf,
  parseEspnScoreboard,
  summarize,
} from "@/lib/nfl/pickem-live-board";
import { resolveNflSeason } from "@/lib/nfl/season";
import PickemTabs from "../pickem-tabs";
import AutoRefresh from "./auto-refresh";
import { BoardCard, kickoffLabel } from "./board-card";

export const metadata = {
  title: "Pick'em Live Board",
  description:
    "The frozen pick'em card against the live scoreboard: our picks, their pregame win chances and line trails, and where we flipped off the favourite.",
};

/**
 * Both reads are cached for five minutes across requests, so a page view (or
 * the five-minute auto refresh) is a cache hit. The card and its pregame line
 * trails only change when a card is refrozen; ESPN is the only moving part.
 */
const REVALIDATE = 300;

const cachedWeeks = unstable_cache(
  (season: number) => getPickemLiveWeeks(season),
  ["pickem-live-weeks"],
  { revalidate: REVALIDATE },
);

const cachedCard = unstable_cache(
  (season: number, week: number) => getPickemLiveCard(season, week),
  ["pickem-live-card"],
  { revalidate: REVALIDATE },
);

/** Throws on failure, so a bad ESPN response is never cached. */
const cachedScoreboard = unstable_cache(
  async (season: number, week: number) => {
    const url = `https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard?seasontype=2&week=${week}&dates=${season}`;
    const res = await fetch(url, { signal: AbortSignal.timeout(10_000) });
    if (!res.ok) throw new Error(`ESPN scoreboard HTTP ${res.status}`);
    return { games: parseEspnScoreboard(await res.json()), fetchedAt: new Date().toISOString() };
  },
  ["pickem-live-espn"],
  { revalidate: REVALIDATE },
);

/** The request's clock, read once so every card agrees on "now". */
function requestTime(): number {
  return Date.now();
}

function Tile({ value, label, small }: { value: string; label: string; small?: boolean }) {
  return (
    <div className="flex min-w-0 flex-col gap-0.5 rounded-lg border bg-card px-3.5 py-3">
      <b className={`font-bold tabular-nums leading-tight ${small ? "text-lg" : "text-3xl"}`}>{value}</b>
      <span className="text-xs text-muted-foreground">{label}</span>
    </div>
  );
}

export default async function PickemLivePage({
  searchParams,
}: {
  searchParams: Promise<{ season?: string; week?: string }>;
}) {
  const { season: rawSeason, week: rawWeek } = await searchParams;
  const season = resolveNflSeason(rawSeason);
  const nowMs = requestTime();

  const weeks = await cachedWeeks(season);
  const parsedWeek = Number(rawWeek);
  // Default to the first card week still being played (last kickoff within
  // four hours), else the latest card.
  const current =
    weeks.find((w) => w.lastKickoff && Date.parse(w.lastKickoff) + 4 * 3600_000 > nowMs)?.week ??
    weeks[weeks.length - 1]?.week;
  const week = weeks.some((w) => w.week === parsedWeek) ? parsedWeek : current;

  const header = (
    <>
      <header className="space-y-2">
        <h1 className="text-2xl font-bold tracking-tight">Pick&apos;em Pools</h1>
        <p className="max-w-3xl text-sm text-muted-foreground">
          The frozen card for the week against the live scoreboard. Picks and frozen chances come
          from the ledger; settlement is unchanged and still happens on the Board tab.
        </p>
      </header>
      <PickemTabs active="live" />
    </>
  );

  if (week == null) {
    return (
      <div className="mx-auto max-w-[1100px] space-y-4 p-4">
        {header}
        <div className="rounded-xl border border-dashed bg-card p-7 text-center text-sm text-muted-foreground">
          No frozen pick&apos;em card for the {season} season yet. Freeze one on the Board tab.
        </div>
      </div>
    );
  }

  const card = await cachedCard(season, week);
  let scoreboard: Awaited<ReturnType<typeof cachedScoreboard>> | null = null;
  let scoreError: string | null = null;
  try {
    scoreboard = await cachedScoreboard(season, week);
  } catch (error) {
    scoreError = error instanceof Error ? error.message : String(error);
  }

  const weekNav = (
    <nav className="flex flex-wrap items-center gap-1.5 text-xs" aria-label="Week">
      <span className="text-muted-foreground">Week</span>
      {weeks.map((w) => (
        <Link
          key={w.week}
          href={`/nfl/pickem/live?season=${season}&week=${w.week}`}
          className={`rounded border px-2 py-1 tabular-nums ${w.week === week
            ? "border-foreground bg-foreground font-semibold text-background"
            : "text-muted-foreground hover:text-foreground"}`}
        >
          {w.week}
        </Link>
      ))}
    </nav>
  );

  if (!card) {
    return (
      <div className="mx-auto max-w-[1100px] space-y-4 p-4">
        {header}
        {weekNav}
        <div className="rounded-xl border border-dashed bg-card p-7 text-center text-sm text-muted-foreground">
          No unsuperseded card for week {week}.
        </div>
      </div>
    );
  }

  const games = buildBoardGames(card, scoreboard?.games ?? null);
  const s = summarize(games);
  const left = games.length - s.finals;
  const vegas = s.vegasFlips.join(", ") || "None";

  return (
    <div className="mx-auto max-w-[1100px] space-y-5 p-4">
      {header}
      <AutoRefresh active={left > 0} seconds={REVALIDATE} />

      <div className="space-y-2">
        <div className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">
          NFL · {season} season · {card.format === "straight" ? "straight picks" : "confidence"}
        </div>
        <h2 className="text-3xl font-bold tracking-tight">Week {week} live board</h2>
        <p className="text-xs text-muted-foreground">
          {scoreboard ? `Scores updated ${kickoffLabel(scoreboard.fetchedAt)}` : "Scores unavailable"}
          {" · "}{left} game{left === 1 ? "" : "s"} left · card #{card.id} frozen {kickoffLabel(card.frozenAt)}
        </p>
        {weekNav}
      </div>

      {scoreError && (
        <p className="rounded-md border border-amber-500/40 bg-amber-500/10 px-3 py-2 text-xs text-amber-700 dark:text-amber-400">
          ESPN scoreboard did not load ({scoreError}). Finals come from the season table; live
          scores will appear on the next refresh.
        </p>
      )}

      <div className="grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-5">
        <Tile value={`${s.won}–${s.lost}`} label="Record so far" />
        <Tile value={s.expectedCorrect.toFixed(1)} label={`Expected correct of ${games.length}`} />
        <Tile value={s.live ? `${s.liveAhead} of ${s.live}` : "0"} label={s.live ? "Live picks ahead" : "Games live now"} />
        <Tile value={s.ourFlips.join(", ") || "None"} label="Our flips" small={s.ourFlips.join(", ").length > 12} />
        <Tile value={vegas} label="Vegas flips" small={vegas.length > 12} />
      </div>

      <div className="flex flex-wrap gap-x-4 gap-y-1.5 text-xs text-muted-foreground">
        <span className="flex items-center gap-1.5"><i className="inline-block h-0.5 w-4 bg-sky-600 dark:bg-sky-400" />Our pick&apos;s win chance, first line to kickoff</span>
        <span className="flex items-center gap-1.5"><i className="inline-block w-4 border-t-2 border-dashed border-muted-foreground" />Chance frozen in the ledger</span>
        <span className="flex items-center gap-1.5"><i className="inline-block h-3 w-3 rounded-sm ring-2 ring-inset ring-orange-600 dark:ring-orange-400" />Our flip: we picked the market underdog</span>
        <span className="flex items-center gap-1.5"><i className="inline-block h-3 w-3 rounded-sm bg-violet-500/15 ring-1 ring-inset ring-violet-600 dark:ring-violet-400" />Vegas flip: the favourite changed since the first line</span>
      </div>

      {groupByWindow(games).map(({ window, games: gs }) => {
        const done = gs.filter((g) => g.state === "post");
        const won = done.filter((g) => outcomeOf(g) === "won").length;
        return (
          <section key={window} className="space-y-2.5">
            <h3 className="flex items-baseline gap-2.5 text-sm font-semibold uppercase tracking-wider text-muted-foreground">
              {window}
              <small className="text-xs font-normal normal-case tracking-normal">
                {done.length ? `${won}–${done.length - won}` : `${gs.length} game${gs.length > 1 ? "s" : ""}`}
              </small>
            </h3>
            <div className="grid grid-cols-[repeat(auto-fill,minmax(min(100%,300px),1fr))] gap-3">
              {gs.map((g) => <BoardCard key={g.gameId} g={g} nowMs={nowMs} />)}
            </div>
          </section>
        );
      })}

      <p className="border-t pt-3 text-xs text-muted-foreground">
        Picks and frozen chances: pick&apos;em ledger (card #{card.id}). Lines: our odds captures,
        vig-free consensus, the 10 days before kickoff. Scores: ESPN scoreboard. Database and
        scores are cached for five minutes.
      </p>
    </div>
  );
}
