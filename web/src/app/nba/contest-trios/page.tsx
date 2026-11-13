export const dynamic = "force-dynamic";

import Link from "next/link";
import type { Metadata } from "next";
import { easternDateString } from "@/lib/eastern-date";
import {
  getContestDates, getContestGrades, getContestPortfolio,
  type ContestEntry, type ContestGrade, type ContestPoolPlayer,
} from "@/db/nba-contest-trios";
import { PRIMARY_N, REVIEW_AFTER_NIGHTS, sharePct, tallyContest } from "@/lib/nba-contest-trios";
import { getNbaCurrentTeams, getNbaPointsProps, getNbaSeasonStats, getNbaTeams } from "@/db/nba-top-scorers";
import { buildBoard, playerKey, teamKey } from "@/lib/nba-top-scorers";
import type { ContestPlayer } from "@/lib/nba-contest-optimizer";
import { ContestOptimizer, type OptimizerGame, type OptimizerInjuryReport } from "./contest-optimizer";
import { getNbaInjuryReport } from "@/db/nba-availability";
import { playerInjuries } from "@/lib/nba-contest-injury";
import { getContestRunFinals, getContestRuns, type SavedRun } from "@/db/nba-contest-runs";
import { scoreRun, type BoxGame, type EntryResult, type RunEntry, type RunPoolPlayer } from "@/lib/nba-contest-runs";

// Last completed season, for the board's team fallback (same as the Top Scorers page).
const LAST_SEASON = "2025-26";

/** Tonight's latest lines as optimizer players: the same board the Top Scorers page builds. */
async function liveSlate(date: string): Promise<{
  players: ContestPlayer[]; games: OptimizerGame[]; capturedAt: string | null; linesAgeMinutes: number | null; injuryReport: OptimizerInjuryReport | null;
}> {
  const [rows, ladderRows, lastSeason, teams, currentTeams, report] = await Promise.all([
    getNbaPointsProps(date), getNbaPointsProps(date, "player_points_alternate"), getNbaSeasonStats(LAST_SEASON),
    getNbaTeams(), getNbaCurrentTeams(date), getNbaInjuryReport(date),
  ]);
  const { scorers, games } = buildBoard(rows, lastSeason, teams, ladderRows, currentTeams);
  // The official injury report, matched to each quoted player in his own game (nba-points-sit-injury-v1).
  const injuries = playerInjuries(date, report, scorers.map((s) => ({ key: playerKey(s.player), eventId: s.eventId })), games);
  const now = Date.now();
  const capturedAt = games.reduce<string | null>((m, g) => (m == null || g.capturedAt > m ? g.capturedAt : m), null);
  const open = games.some((g) => new Date(g.commenceTime).getTime() > now);
  return {
    players: scorers.map((s) => ({
      key: playerKey(s.player), player: s.player, eventId: s.eventId, team: s.team ? teamKey(s.team) : null,
      line: s.line, pOver: s.pOver, books: s.books.length,
      curve: (s.distribution?.curve ?? []).map((c) => ({ point: c.point, p: c.p })),
      // How long before his own tip these lines were read: sets his sitting chance (nba-points-sit-lead-v1).
      leadHours: (new Date(s.commenceTime).getTime() - new Date(s.capturedAt).getTime()) / 3_600_000,
      injury: injuries.get(playerKey(s.player))?.status ?? null,
      teamsSubmitted: injuries.get(playerKey(s.player))?.teamsSubmitted ?? false,
    })),
    games: [...games].sort((a, b) => a.commenceTime.localeCompare(b.commenceTime)).map((g) => ({
      eventId: g.eventId, game: g.game, commenceTime: g.commenceTime, players: g.players,
      started: new Date(g.commenceTime).getTime() <= now,
    })),
    capturedAt,
    // Decided on the server, like `started`, so server and browser render the same warning.
    linesAgeMinutes: capturedAt && open ? Math.max(0, Math.round((now - new Date(capturedAt).getTime()) / 60_000)) : null,
    injuryReport: report ? { publishedAt: report.publishedAt, observedAt: report.observedAt, sourceUrl: report.sourceUrl } : null,
  };
}

export const metadata: Metadata = {
  title: "NBA Contest Trios",
  description: "Entries for a winner-take-all top-three scorer contest, chosen to beat a crowded field rather than to score the most on average.",
};

const stamp = (s: string) => new Intl.DateTimeFormat("en-US", {
  timeZone: "America/New_York", month: "short", day: "numeric", hour: "numeric", minute: "2-digit", timeZoneName: "short",
}).format(new Date(s));
const longDate = (d: string) => new Date(`${d}T12:00:00`).toLocaleDateString("en-US", { weekday: "long", month: "long", day: "numeric" });
const pct = (v: number | null | undefined, digits = 0) => (v == null ? "-" : `${(v * 100).toFixed(digits)}%`);
const one = (v: number | null | undefined) => (v == null ? "-" : v.toFixed(1));

function PlayerChip({ p }: { p: ContestPoolPlayer | undefined; }) {
  if (!p) return null;
  return (
    <span className="whitespace-nowrap">
      {p.player} <span className="font-mono text-xs text-slate-500">{p.line.toFixed(1)}</span>
      {p.books === 1 && <span className="ml-1 rounded border border-orange-400 px-1 font-mono text-[10px] text-orange-700" title="Only one book quotes this line; such players sat about 35% of the time.">1 BOOK</span>}
    </span>
  );
}

function EntryRows({ entries, pool, actual, start }: {
  entries: ContestEntry[]; pool: Map<string, ContestPoolPlayer>; actual: number[] | null; start: number;
}) {
  return (
    <>
      {entries.map((e, i) => (
        <tr key={e.players.join("|")} className={`border-t ${e.rank <= PRIMARY_N ? "" : "text-slate-600"}`}>
          <td className="py-2 pr-2 font-mono text-xs">{e.rank}</td>
          <td className="py-2 pr-3">
            <div className="flex flex-wrap gap-x-3 gap-y-1">{e.players.map((k) => <PlayerChip key={k} p={pool.get(k)} />)}</div>
          </td>
          <td className="pr-3 text-right font-mono">{e.lineSum.toFixed(1)}</td>
          <td className="pr-3 text-right font-mono" title="Chance this entry alone wins (ties split), against the modeled field">{sharePct(e.standaloneShare)}</td>
          <td className="pr-3 text-right font-mono" title="Prize share this entry adds to the entries above it">{sharePct(e.marginalShare)}</td>
          <td className="text-right font-mono font-semibold">{actual ? actual[start + i]?.toFixed(0) ?? "-" : "-"}</td>
        </tr>
      ))}
    </>
  );
}

function RunEntryRows({ entries, results, pool }: { entries: RunEntry[]; results: EntryResult[]; pool: Map<string, RunPoolPlayer> }) {
  return (
    <>
      {entries.map((e, i) => (
        <tr key={e.players.join("|")} className="border-t">
          <td className="py-2 pr-2 font-mono text-xs">{e.rank}</td>
          <td className="py-2 pr-3">
            <div className="flex flex-wrap gap-x-3 gap-y-1">{e.players.map((k, j) => {
              const p = pool.get(k), r = results[i]?.players[j];
              return (
                <span key={k} className="whitespace-nowrap">
                  {p?.player ?? k} <span className="font-mono text-xs text-slate-500">{p?.line.toFixed(1)}</span>
                  {r?.points != null && <span className="ml-1 font-mono text-xs font-semibold">{r.dnp ? "0 (did not play)" : r.points.toFixed(0)}</span>}
                </span>
              );
            })}</div>
          </td>
          <td className="pr-3 text-right font-mono">{e.lineSum.toFixed(1)}</td>
          <td className="pr-3 text-right font-mono">{sharePct(e.standaloneShare)}</td>
          <td className="text-right font-mono font-semibold">{results[i]?.total != null ? results[i].total.toFixed(0) : "-"}</td>
        </tr>
      ))}
    </>
  );
}

/**
 * Every run of the builder for this date. These are the entries that can actually be
 * submitted, so they are kept and scored from the box scores; the contest's own result
 * (winning score, field, finish) is added when a results file is imported.
 */
function SavedRuns({ runs, finals, date }: { runs: SavedRun[]; finals: BoxGame[]; date: string }) {
  return (
    <section className="space-y-3 rounded-xl border bg-white p-4 shadow-sm">
      <div>
        <h2 className="font-bold">Your saved runs</h2>
        <p className="text-xs text-muted-foreground">
          Every Optimize run for {longDate(date)}, newest first, with the time it was saved. Points come from the final box scores the
          next morning; a player who did not play scores 0, as in the contest. The winning score, the field and where your entries
          finished are not here yet: they come from the contest&apos;s results file, and no file has been imported.
        </p>
      </div>
      {runs.length === 0 ? <p className="text-sm text-slate-600">No runs saved for this date. Press Optimize above and the run is saved.</p> : runs.map((run, i) => {
        const p = run.payload, score = scoreRun(p, finals);
        const pool = new Map(p.pool.map((x) => [x.key, x]));
        return (
          <details key={run.id} open={i === 0} className="rounded-lg border p-3">
            <summary className="cursor-pointer text-sm">
              <b>{stamp(run.createdAt)}</b> · {p.games.length} game{p.games.length === 1 ? "" : "s"} · field of {p.fieldSize} · {p.entries.length} entries
              {run.late
                ? <span className="ml-2 rounded border border-amber-500 px-1 text-xs text-amber-700">saved after the first tip ({stamp(run.firstTip)})</span>
                : <span className="ml-2 rounded border border-emerald-500 px-1 text-xs text-emerald-700">before the first tip</span>}
              {score.best != null && <span className="ml-2 font-semibold">best entry {score.best.toFixed(0)}{score.baselineBest != null ? ` · biggest lines ${score.baselineBest.toFixed(0)}` : ""}</span>}
            </summary>
            <p className="mt-2 text-xs text-slate-600">
              {p.games.map((g) => `${g.awayTeam} @ ${g.homeTeam}`).join(" · ")}
              {p.linesCapturedAt ? ` · lines captured ${stamp(p.linesCapturedAt)}` : ""} · model winning score {p.winningScore.median.toFixed(0)}
            </p>
            <div className="mt-2 overflow-x-auto">
              <table className="w-full text-sm">
                <thead><tr className="text-left text-[10px] uppercase text-slate-500">
                  <th>#</th><th>Players (line, points)</th><th className="text-right">Line sum</th><th className="text-right">Wins alone</th><th className="text-right">Actual</th>
                </tr></thead>
                <tbody><RunEntryRows entries={p.entries} results={score.entries} pool={pool} /></tbody>
              </table>
            </div>
            {!score.graded && <p className="mt-2 text-xs text-muted-foreground">Not scored yet: at least one of these games has no final box score.</p>}
          </details>
        );
      })}
    </section>
  );
}

function TrackRecord({ grades }: { grades: ContestGrade[] }) {
  const t = tallyContest(grades);
  return (
    <section className="space-y-2 rounded-xl border bg-white p-4 shadow-sm">
      <h2 className="font-bold">Track record: first {PRIMARY_N} entries vs the {PRIMARY_N} biggest-line trios</h2>
      <p className="text-sm text-slate-700">
        {t.nights === 0 ? "No graded nights yet." : <>
          {t.nights} of {REVIEW_AFTER_NIGHTS} nights graded. Prizes won against the modeled field with the actual points: these
          entries <b>{t.oursPrizes.toFixed(2)}</b>, biggest lines <b>{t.baselinePrizes.toFixed(2)}</b> (ahead on {t.oursAhead} nights,
          behind on {t.baselineAhead}). Best entry&apos;s rank among every pool trio: {pct(t.oursPercentile, 1)} vs {pct(t.baselinePercentile, 1)};
          in the top 1% on {t.oursContention} vs {t.baselineContention} nights.
        </>}
      </p>
      <p className="text-xs text-muted-foreground">
        The prize count uses the same assumed field the entries were chosen against, so it partly grades them on their own assumption;
        the rank among pool trios needs no field and is the check on that. Reviewed once at {REVIEW_AFTER_NIGHTS} nights with a paired
        interval (docs/nba-wta-trios.md). Your real contest results outrank both.
      </p>
      {grades.length > 0 && (
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead><tr className="text-left text-[10px] uppercase text-slate-500">
              <th>Night</th><th className="text-right">Best possible</th><th className="text-right">Our best</th><th className="text-right">Biggest lines best</th>
              <th className="text-right">Our prize share</th><th className="text-right">Biggest lines share</th>
            </tr></thead>
            <tbody>{[...grades].reverse().map((g) => {
              const c = g.byN[String(PRIMARY_N)];
              return (
                <tr key={g.gameDate} className="border-t">
                  <td className="py-1"><Link className="text-blue-700 underline" href={`/nba/contest-trios?date=${g.gameDate}`}>{g.gameDate}</Link></td>
                  <td className="text-right font-mono">{g.bestPoolTrio.toFixed(0)}</td>
                  <td className="text-right font-mono">{c?.ours.best.toFixed(0) ?? "-"}</td>
                  <td className="text-right font-mono">{c?.baseline.best.toFixed(0) ?? "-"}</td>
                  <td className="text-right font-mono">{c ? sharePct(c.ours.realizedShare) : "-"}</td>
                  <td className="text-right font-mono">{c ? sharePct(c.baseline.realizedShare) : "-"}</td>
                </tr>
              );
            })}</tbody>
          </table>
        </div>
      )}
    </section>
  );
}

export default async function ContestTriosPage({ searchParams }: { searchParams: Promise<{ date?: string }> }) {
  const params = await searchParams;
  const today = easternDateString();
  const date = params.date && /^\d{4}-\d{2}-\d{2}$/.test(params.date) ? params.date : today;
  const [portfolio, dates, grades, slate, runs] = await Promise.all([
    getContestPortfolio(date), getContestDates(), getContestGrades(), liveSlate(date), getContestRuns(date),
  ]);
  const finals = runs.length > 0 ? await getContestRunFinals(date) : [];
  const grade = grades.find((g) => g.gameDate === date) ?? null;
  const p = portfolio?.payload;
  const pool = new Map((p?.pool ?? []).map((x) => [x.key, x]));
  const main = p?.entries.slice(0, PRIMARY_N) ?? [];
  const more = p?.entries.slice(PRIMARY_N) ?? [];
  const ours5 = main.at(-1)?.portfolioShare ?? null;
  const base5 = p?.baseline[PRIMARY_N - 1]?.portfolioShare ?? null;
  const cell = grade?.byN[String(PRIMARY_N)];

  return (
    <main className="mx-auto max-w-6xl space-y-5 p-4">
      <header className="space-y-1">
        <h1 className="text-2xl font-black">NBA Contest Trios <span className="text-base font-normal text-amber-700">Forward test</span></h1>
        <p className="text-sm text-slate-700">
          Entries for a winner-take-all contest: pick three players, score their combined points, highest total takes everything, ties split.
          In a big field the three biggest lines are the most likely trio but also the most copied, so each entry here is chosen for its chance to
          finish first and not share it, given the entries above it.
        </p>
        <p className="text-xs text-muted-foreground">
          Rules frozen 2026-11-06 (nba-wta-trios-v1, docs/nba-wta-trios.md). Unvalidated: on the 16 earlier nights these entries were no better
          than the biggest lines at scoring, and all of their edge comes from avoiding a crowd we have not measured.
        </p>
      </header>

      <nav className="flex flex-wrap gap-2 text-sm" aria-label="Contest dates">
        {[...new Set([today, ...dates])].sort().reverse().map((d) => (
          <Link key={d} href={`/nba/contest-trios?date=${d}`} className={`rounded border px-2 py-1 ${d === date ? "bg-slate-900 text-white" : "hover:bg-slate-100"}`}>
            {d.slice(5)}{d === today ? " · today" : ""}
          </Link>
        ))}
      </nav>

      <ContestOptimizer date={date} players={slate.players} games={slate.games} capturedAt={slate.capturedAt} linesAgeMinutes={slate.linesAgeMinutes} injuryReport={slate.injuryReport} />

      <SavedRuns runs={runs} finals={finals} date={date} />

      {!p || !portfolio ? (
        <p className="rounded-lg border p-4 text-sm">
          No entries for {longDate(date)}. They are built after the 11:07 AM and 6:07 PM ET line captures, only while every game is still to tip.
        </p>
      ) : (
        <>
          <p className="text-sm">
            <b>{longDate(date)}</b> · built {stamp(portfolio.createdAt)} from lines captured {stamp(p.inputs.capturedAt)} ·
            first tip {stamp(p.inputs.firstTip)} · {p.inputs.games} games, top {p.pool.length} lines
          </p>

          <section className="grid gap-3 sm:grid-cols-3">
            <div className="rounded-xl border bg-white p-4 shadow-sm">
              <p className="text-xs uppercase text-slate-500">Model: prize share, first {PRIMARY_N} entries</p>
              <p className="text-2xl font-black">{ours5 == null ? "-" : sharePct(ours5)}</p>
              <p className="text-xs text-slate-600">vs {base5 == null ? "-" : sharePct(base5)} for the {PRIMARY_N} biggest-line trios</p>
            </div>
            <div className="rounded-xl border bg-white p-4 shadow-sm">
              <p className="text-xs uppercase text-slate-500">Model: likely winning score</p>
              <p className="text-2xl font-black">{p.simulatedWinningScore.median.toFixed(0)}</p>
              <p className="text-xs text-slate-600">middle half {p.simulatedWinningScore.p25.toFixed(0)}-{p.simulatedWinningScore.p75.toFixed(0)} in a field of {p.assumptions.fieldSize}</p>
            </div>
            <div className="rounded-xl border bg-white p-4 shadow-sm">
              <p className="text-xs uppercase text-slate-500">Result</p>
              {cell && grade ? (
                <>
                  <p className="text-2xl font-black">{cell.ours.best.toFixed(0)} <span className="text-base font-normal text-slate-600">best entry</span></p>
                  <p className="text-xs text-slate-600">biggest lines {cell.baseline.best.toFixed(0)} · best possible {grade.bestPoolTrio.toFixed(0)} ·
                    prize share {sharePct(cell.ours.realizedShare)} vs {sharePct(cell.baseline.realizedShare)}</p>
                </>
              ) : <p className="text-sm text-slate-600">Graded the next morning, after the box scores.</p>}
            </div>
          </section>

          <section className="rounded-xl border bg-white p-4 shadow-sm">
            <h2 className="font-bold">Tracked entries (all games)</h2>
            <p className="mb-2 text-xs text-muted-foreground">
              In order: the first {PRIMARY_N} are the best {PRIMARY_N}-entry set, and each further entry adds the most to the ones above it. Enter as
              many as you like from the top down; skipping one breaks that guarantee.
            </p>
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead><tr className="text-left text-[10px] uppercase text-slate-500">
                  <th>#</th><th>Players (line)</th><th className="text-right">Line sum</th><th className="text-right">Wins alone</th>
                  <th className="text-right">Adds</th><th className="text-right">Actual</th>
                </tr></thead>
                <tbody>
                  <EntryRows entries={main} pool={pool} actual={grade?.ours ?? null} start={0} />
                  {more.length > 0 && <tr><td colSpan={6} className="pt-3 text-[10px] uppercase text-slate-500">More entries</td></tr>}
                  <EntryRows entries={more} pool={pool} actual={grade?.ours ?? null} start={PRIMARY_N} />
                </tbody>
              </table>
            </div>
          </section>

          <section className="rounded-xl border bg-white p-4 shadow-sm">
            <h2 className="font-bold">The pool</h2>
            <p className="mb-2 text-xs text-muted-foreground">
              Field share is the assumed fraction of entries holding the player (a guess: there is no data on this contest&apos;s entrants).
              Sits is the measured share of players with this many books quoting them who did not play.
            </p>
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead><tr className="text-left text-[10px] uppercase text-slate-500">
                  <th>Player</th><th>Game</th><th className="text-right">Line</th><th className="text-right">Ceiling</th><th className="text-right">30+</th>
                  <th className="text-right">Field share</th><th className="text-right">Sits</th>
                </tr></thead>
                <tbody>{p.pool.map((x) => (
                  <tr key={x.key} className="border-t">
                    <td className="py-1"><PlayerChip p={x} /></td>
                    <td className="text-xs text-slate-600">{x.awayTeam} @ {x.homeTeam}</td>
                    <td className="text-right font-mono">{x.line.toFixed(1)}</td>
                    <td className="text-right font-mono">{one(x.ceiling)}</td>
                    <td className="text-right font-mono">{pct(x.p30)}</td>
                    <td className="text-right font-mono">{pct(x.fieldOwnership)}</td>
                    <td className="text-right font-mono">{pct(x.dnpProb, 1)}</td>
                  </tr>
                ))}</tbody>
              </table>
            </div>
          </section>

          <details className="rounded-xl border bg-white p-4 text-sm shadow-sm">
            <summary className="font-semibold">Assumptions</summary>
            <ul className="mt-2 list-disc space-y-1 pl-5 text-slate-700">
              <li>Each player&apos;s points follow the books&apos; own ladder of alternate lines, extended past the last rung; {p.sims.toLocaleString()} simulated nights.</li>
              <li>Same-game scoring moves together a little: teammates {p.assumptions.teammateRho}, opponents +{p.assumptions.opponentRho} (measured Oct 20 - Nov 5).</li>
              <li>Field of {p.assumptions.fieldSize}: {pct(p.assumptions.chalkShare)} enter the three biggest lines exactly; the rest favour big lines (weight e^(line/{p.assumptions.popularityTau})). A guess.</li>
              <li>A player who does not play scores 0. The books&apos; 30+ and 40+ chances have read high this season, which flatters high-ceiling players.</li>
            </ul>
          </details>
        </>
      )}

      <TrackRecord grades={grades} />

      <p className="text-sm">
        <Link href={`/nba/top-scorers?date=${date}`} className="underline">Top Scorers</Link> ·{" "}
        <Link href={`/nba/boom-trios?date=${date}`} className="underline">Boom Trios</Link> ·{" "}
        <Link href={`/nba/trio-lab?date=${date}`} className="underline">Trio Lab</Link>
      </p>
    </main>
  );
}
