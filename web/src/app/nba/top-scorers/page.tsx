export const dynamic = "force-dynamic";

import Link from "next/link";
import type { Metadata } from "next";
import { easternDateString } from "@/lib/eastern-date";
import { buildBoard, type ScorerRow, type GameSummary } from "@/lib/nba-top-scorers";
import {
  getNbaPointsPropDates,
  getNbaPointsProps,
  getNbaPulledLines,
  getNbaSeasonStats,
} from "@/db/nba-top-scorers";

export const metadata: Metadata = {
  title: "NBA Top Scorers",
  description: "Who the sportsbooks expect to score the most points tonight.",
};

// Last completed season: the comparison column, until this season has enough games.
const LAST_SEASON = "2025-26";
const CHART_ROWS = 30;

const BOOK_NAMES: Record<string, string> = {
  pinnacle: "Pinnacle",
  draftkings: "DK",
  fanduel: "FanDuel",
  betmgm: "BetMGM",
  williamhill_us: "Caesars",
  fanatics: "Fanatics",
};

const one = (v: number) => v.toFixed(1);
const pct = (v: number | null) => (v == null ? "-" : `${Math.round(v * 100)}%`);
const american = (p: number | null) => (p == null ? "-" : p > 0 ? `+${p}` : `${p}`);
const signed = (v: number | null) => (v == null ? "-" : `${v >= 0 ? "+" : ""}${v.toFixed(1)}`);

function tip(value: string) {
  return new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York",
    hour: "numeric",
    minute: "2-digit",
  }).format(new Date(value));
}

function stamp(value: string) {
  return new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York",
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
    timeZoneName: "short",
  }).format(new Date(value));
}

function longDate(d: string) {
  return new Date(`${d}T12:00:00`).toLocaleDateString("en-US", {
    weekday: "long",
    month: "long",
    day: "numeric",
  });
}

function shortGame(g: GameSummary) {
  const last = (name: string) => name.split(" ").slice(-1)[0];
  return `${last(g.awayTeam)} @ ${last(g.homeTeam)}`;
}

function href(date: string, game?: string, all?: boolean) {
  const q = new URLSearchParams({ date });
  if (game) q.set("game", game);
  if (all) q.set("all", "1");
  return `/nba/top-scorers?${q.toString()}`;
}

/** Ranked dot plot: dot = consensus line, bar = range across books, ring = last season's PPG. */
function RankChart({ rows, scaleMax }: { rows: ScorerRow[]; scaleMax: number }) {
  const x = (v: number) => `${Math.max(0, Math.min(100, (v / scaleMax) * 100))}%`;
  const ticks: number[] = [];
  for (let t = 0; t <= scaleMax; t += 10) ticks.push(t);
  return (
    <div className="viz-root">
      <div className="mb-3 flex flex-wrap items-center gap-x-5 gap-y-1 text-xs text-muted-foreground">
        <span className="flex items-center gap-1.5">
          <span className="inline-block h-2.5 w-2.5 rounded-full bg-[#2a78d6] dark:bg-[#3987e5]" />
          Consensus points line
        </span>
        <span className="flex items-center gap-1.5">
          <span className="inline-block h-1 w-5 rounded bg-slate-300 dark:bg-slate-600" />
          Range across books
        </span>
        <span className="flex items-center gap-1.5">
          <span className="inline-block h-2.5 w-2.5 rounded-full border-2 border-slate-700 dark:border-slate-300" />
          {LAST_SEASON} points per game
        </span>
      </div>
      <div className="grid grid-cols-[1.25rem_6.5rem_minmax(0,1fr)_2.25rem] items-center gap-x-2 text-sm sm:grid-cols-[2rem_minmax(9rem,15rem)_minmax(0,1fr)_3.5rem]">
        <div />
        <div />
        <div className="relative h-5 text-[11px] text-muted-foreground">
          {ticks.map((t) => (
            <span key={t} className="absolute -translate-x-1/2" style={{ left: x(t) }}>
              {t}
            </span>
          ))}
        </div>
        <div className="text-right text-[11px] text-muted-foreground">Line</div>
        {rows.map((r) => {
          const title = [
            `${r.player}${r.teamAbbrev ? ` (${r.teamAbbrev})` : ""}, ${r.game}`,
            `Line ${one(r.line)} · P(over) ${pct(r.pOver)}`,
            `Books ${one(r.lineLow)}-${one(r.lineHigh)} (${r.books.length})`,
            r.lastSeasonPpg != null ? `${LAST_SEASON}: ${one(r.lastSeasonPpg)} ppg` : `${LAST_SEASON}: no match`,
          ].join("\n");
          return (
            <div key={`${r.eventId}-${r.player}`} className="contents" title={title}>
              <div className="py-1.5 text-right tabular-nums text-muted-foreground">{r.rank}</div>
              <div className="truncate py-1.5">
                <span className="font-medium">{r.player}</span>
                <span className="ml-1.5 hidden text-xs text-muted-foreground sm:inline">{r.teamAbbrev ?? ""}</span>
              </div>
              <div className="relative h-6">
                {ticks.map((t) => (
                  <span key={t} className="absolute top-0 h-full w-px bg-slate-100 dark:bg-slate-800" style={{ left: x(t) }} />
                ))}
                <span
                  className="absolute top-1/2 h-1 -translate-y-1/2 rounded bg-slate-300 dark:bg-slate-600"
                  style={{ left: x(r.lineLow), width: `calc(${x(r.lineHigh)} - ${x(r.lineLow)} + 4px)`, marginLeft: -2 }}
                />
                {r.lastSeasonPpg != null && (
                  <span
                    className="absolute top-1/2 h-3 w-3 -translate-x-1/2 -translate-y-1/2 rounded-full border-2 border-slate-700 bg-transparent dark:border-slate-300"
                    style={{ left: x(r.lastSeasonPpg) }}
                  />
                )}
                <span
                  className="absolute top-1/2 h-3 w-3 -translate-x-1/2 -translate-y-1/2 rounded-full bg-[#2a78d6] ring-2 ring-white dark:bg-[#3987e5] dark:ring-slate-950"
                  style={{ left: x(r.line) }}
                />
              </div>
              <div className="py-1.5 text-right font-semibold tabular-nums">{one(r.line)}</div>
            </div>
          );
        })}
      </div>
    </div>
  );
}

function GameCard({ game, scorers }: { game: GameSummary; scorers: ScorerRow[] }) {
  const side = (team: string) => scorers.filter((s) => s.team === team).slice(0, 3);
  const unknown = scorers.filter((s) => s.team == null).slice(0, 3);
  const block = (label: string, list: ScorerRow[]) => (
    <div>
      <div className="mb-1 text-xs font-medium text-muted-foreground">{label}</div>
      {list.length === 0 ? (
        <div className="text-xs text-muted-foreground">No lines</div>
      ) : (
        list.map((s) => (
          <div key={s.player} className="flex justify-between gap-2 text-sm">
            <span className="truncate">{s.player}</span>
            <span className="tabular-nums font-medium">{one(s.line)}</span>
          </div>
        ))
      )}
    </div>
  );
  return (
    <div className="rounded-lg border p-3">
      <div className="mb-2 flex items-baseline justify-between gap-2">
        <span className="font-semibold">{shortGame(game)}</span>
        <span className="text-xs text-muted-foreground">{tip(game.commenceTime)} ET</span>
      </div>
      <div className="grid grid-cols-2 gap-3">
        {block(game.awayTeam, side(game.awayTeam))}
        {block(game.homeTeam, side(game.homeTeam))}
      </div>
      {unknown.length > 0 && <div className="mt-2">{block("Team not matched", unknown)}</div>}
      <div className="mt-2 text-xs text-muted-foreground">
        {game.players} players listed · lines sum to {one(game.lineSum)}
      </div>
    </div>
  );
}

export default async function TopScorersPage({
  searchParams,
}: {
  searchParams: Promise<{ date?: string; game?: string; all?: string }>;
}) {
  const params = await searchParams;
  const today = easternDateString();
  const dates = await getNbaPointsPropDates();
  const requested = params.date && /^\d{4}-\d{2}-\d{2}$/.test(params.date) ? params.date : null;
  const date = requested ?? (dates.includes(today) ? today : dates.find((d) => d > today) ?? dates[0] ?? today);

  const [rows, pulled, lastSeason] = await Promise.all([
    getNbaPointsProps(date),
    getNbaPulledLines(date),
    getNbaSeasonStats(LAST_SEASON),
  ]);
  const { scorers, games } = buildBoard(rows, lastSeason);
  const selectedGame = games.find((g) => g.eventId === params.game) ?? null;
  const shown = selectedGame ? scorers.filter((s) => s.eventId === selectedGame.eventId) : scorers;
  const showAll = params.all === "1" || selectedGame != null;
  const chartRows = showAll ? shown : shown.slice(0, CHART_ROWS);
  const scaleMax = Math.max(40, Math.ceil((Math.max(0, ...shown.map((s) => Math.max(s.lineHigh, s.lastSeasonPpg ?? 0))) + 2) / 10) * 10);
  const lastCapture = games.reduce<string | null>((m, g) => (m == null || g.capturedAt > m ? g.capturedAt : m), null);
  const gameName = new Map(games.map((g) => [g.eventId, shortGame(g)]));

  return (
    <main className="mx-auto max-w-7xl space-y-6 px-4 py-6">
      <header className="space-y-1">
        <h1 className="text-2xl font-bold tracking-tight">NBA Top Scorers</h1>
        <p className="text-sm text-muted-foreground">
          {longDate(date)} · ranked by the sportsbooks&apos; player-points lines
          {lastCapture ? ` · lines as of ${stamp(lastCapture)}` : ""}
        </p>
      </header>

      {dates.length > 0 && (
        <nav className="flex flex-wrap gap-1.5 text-sm" aria-label="Dates">
          {[...dates].sort().map((d) => (
            <Link
              key={d}
              href={href(d)}
              className={`rounded px-2.5 py-1 ${d === date ? "bg-blue-600 text-white" : "border hover:bg-accent"}`}
            >
              {d.slice(5)}
            </Link>
          ))}
        </nav>
      )}

      {scorers.length === 0 ? (
        <section className="rounded-lg border p-6 text-sm">
          <p className="font-medium">No points lines stored for {longDate(date)}.</p>
          <p className="mt-1 text-muted-foreground">
            Lines come from <code>python -m ingest.nba_points_props --date {date} --apply --max-credits 20</code>.
            Books usually post NBA player props the morning of the game, so an early capture can come back empty.
          </p>
        </section>
      ) : (
        <>
          <section className="grid grid-cols-2 gap-3 sm:grid-cols-4">
            <div className="rounded-lg border p-3">
              <div className="text-xs text-muted-foreground">Projected top scorer</div>
              <div className="mt-1 font-semibold">{scorers[0].player}</div>
              <div className="text-sm tabular-nums">{one(scorers[0].line)} pts line</div>
            </div>
            <div className="rounded-lg border p-3">
              <div className="text-xs text-muted-foreground">Lines of 25+</div>
              <div className="mt-1 text-2xl font-semibold tabular-nums">{scorers.filter((s) => s.line >= 25).length}</div>
            </div>
            <div className="rounded-lg border p-3">
              <div className="text-xs text-muted-foreground">Games with lines</div>
              <div className="mt-1 text-2xl font-semibold tabular-nums">{games.length}</div>
            </div>
            <div className="rounded-lg border p-3">
              <div className="text-xs text-muted-foreground">Players listed</div>
              <div className="mt-1 text-2xl font-semibold tabular-nums">{scorers.length}</div>
            </div>
          </section>

          <nav className="flex flex-wrap gap-1.5 text-sm" aria-label="Games">
            <Link href={href(date)} className={`rounded px-2.5 py-1 ${!selectedGame ? "bg-slate-900 text-white dark:bg-slate-100 dark:text-slate-900" : "border hover:bg-accent"}`}>
              All games
            </Link>
            {games.map((g) => (
              <Link
                key={g.eventId}
                href={href(date, g.eventId)}
                className={`rounded px-2.5 py-1 ${selectedGame?.eventId === g.eventId ? "bg-slate-900 text-white dark:bg-slate-100 dark:text-slate-900" : "border hover:bg-accent"}`}
              >
                {shortGame(g)} <span className="text-xs opacity-70">{tip(g.commenceTime)}</span>
              </Link>
            ))}
          </nav>

          <section className="rounded-lg border p-4">
            <div className="mb-3 flex flex-wrap items-baseline justify-between gap-2">
              <h2 className="font-semibold">
                {selectedGame ? `Every listed player, ${selectedGame.game}` : showAll ? "Every listed player" : `Top ${chartRows.length} by points line`}
              </h2>
              {!selectedGame && shown.length > CHART_ROWS && (
                <Link href={href(date, undefined, !showAll)} className="text-sm text-blue-600 hover:underline">
                  {showAll ? `Show top ${CHART_ROWS}` : `Show all ${shown.length}`}
                </Link>
              )}
            </div>
            <RankChart rows={chartRows} scaleMax={scaleMax} />
          </section>

          {!selectedGame && (
            <section className="space-y-3">
              <h2 className="font-semibold">Leading scorers by game</h2>
              <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
                {games.map((g) => (
                  <GameCard key={g.eventId} game={g} scorers={scorers.filter((s) => s.eventId === g.eventId)} />
                ))}
              </div>
            </section>
          )}

          <section className="rounded-lg border">
            <h2 className="border-b px-4 py-3 font-semibold">Lines by book</h2>
            <div className="overflow-x-auto">
              <table className="w-full min-w-[56rem] text-sm">
                <thead className="bg-muted/40 text-left text-xs text-muted-foreground">
                  <tr>
                    <th className="px-3 py-2 text-right">#</th>
                    <th className="px-3 py-2">Player</th>
                    <th className="px-3 py-2">Game</th>
                    <th className="px-3 py-2 text-right">Line</th>
                    <th className="px-3 py-2 text-right">P(over)</th>
                    <th className="px-3 py-2">Books (line, over/under)</th>
                    <th className="px-3 py-2 text-right">{LAST_SEASON} PPG</th>
                    <th className="px-3 py-2 text-right">Line vs PPG</th>
                  </tr>
                </thead>
                <tbody>
                  {shown.map((s) => (
                    <tr key={`${s.eventId}-${s.player}`} className="border-t">
                      <td className="px-3 py-1.5 text-right tabular-nums text-muted-foreground">{s.rank}</td>
                      <td className="whitespace-nowrap px-3 py-1.5 font-medium">
                        {s.player}
                        {s.teamAbbrev && <span className="ml-1.5 text-xs text-muted-foreground">{s.teamAbbrev}</span>}
                      </td>
                      <td className="whitespace-nowrap px-3 py-1.5 text-muted-foreground">
                        {gameName.get(s.eventId)} · {tip(s.commenceTime)}
                      </td>
                      <td className="px-3 py-1.5 text-right font-semibold tabular-nums">{one(s.line)}</td>
                      <td className="px-3 py-1.5 text-right tabular-nums">{pct(s.pOver)}</td>
                      <td className="px-3 py-1.5 text-xs text-muted-foreground">
                        {s.books.map((b) => `${BOOK_NAMES[b.book] ?? b.book} ${one(b.line)} ${american(b.over)}/${american(b.under)}`).join(" · ")}
                      </td>
                      <td className="px-3 py-1.5 text-right tabular-nums">
                        {s.lastSeasonPpg == null ? "-" : `${one(s.lastSeasonPpg)}`}
                        {s.lastSeasonGames != null && <span className="ml-1 text-xs text-muted-foreground">({s.lastSeasonGames}g)</span>}
                      </td>
                      <td className="px-3 py-1.5 text-right tabular-nums">{signed(s.vsLastSeason)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </section>

          {pulled.length > 0 && (
            <section className="rounded-lg border p-4 text-sm">
              <h2 className="font-semibold">Lines pulled since an earlier capture</h2>
              <p className="mt-1 text-muted-foreground">
                Quoted earlier today but missing from the game&apos;s latest capture. Books usually pull a line on
                injury or rest news.
              </p>
              <ul className="mt-2 space-y-0.5">
                {pulled.map((p) => (
                  <li key={`${p.eventId}-${p.player}`}>
                    {p.player} <span className="text-muted-foreground">({gameName.get(p.eventId) ?? "game"}, last seen {stamp(p.lastSeenAt)})</span>
                  </li>
                ))}
              </ul>
            </section>
          )}
        </>
      )}

      <section className="space-y-1 text-xs text-muted-foreground">
        <p>
          Line is the points line most books quote. A points line is the books&apos; median outcome, so a player scores
          above it about half the time; P(over) is that chance with the vig removed, from the books quoting that line.
        </p>
        <p>
          This is the market&apos;s number, not a model of ours, and it has not been checked against results here.
          Team and {LAST_SEASON} PPG come from last season&apos;s stats and can be stale for a player who changed teams.
        </p>
      </section>
    </main>
  );
}
