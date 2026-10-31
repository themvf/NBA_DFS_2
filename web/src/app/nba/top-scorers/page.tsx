export const dynamic = "force-dynamic";

import Link from "next/link";
import type { Metadata } from "next";
import { easternDateString } from "@/lib/eastern-date";
import { buildBoard } from "@/lib/nba-top-scorers";
import {
  getNbaPointsPropDates,
  getNbaPointsProps,
  getNbaPulledLines,
  getNbaSeasonStats,
  getNbaTeams,
} from "@/db/nba-top-scorers";
import { getNbaPointsCalibration, getNbaPointsCalibrationRows, type CalibrationSummary } from "@/db/nba-points-calibration";
import {
  coverageData,
  reliabilitySeries,
  scatterData,
  shortMinutesSummary,
  skillTable,
  tailTrend,
  type CoverageData,
  type ReliabilitySeries,
  type ScatterData,
  type SkillRow,
  type TailSeries,
} from "@/lib/nba-points-calibration-charts";
import { CalibrationCharts } from "./calibration-charts";
import { TopScorersClient } from "./top-scorers-client";

export const metadata: Metadata = {
  title: "NBA Top Scorers",
  description: "Who the sportsbooks expect to score the most points tonight.",
};

// Last completed season: the comparison column, until this season has enough games.
const LAST_SEASON = "2025-26";

// The app-wide `* { border-color: var(--border) }` rule beats border utilities.
const DARK_BORDER = { "--border": "#1e293b" } as React.CSSProperties;

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
  return new Date(`${d}T12:00:00`).toLocaleDateString("en-US", { weekday: "long", month: "long", day: "numeric" });
}

export default async function TopScorersPage({ searchParams }: { searchParams: Promise<{ date?: string }> }) {
  const params = await searchParams;
  const today = easternDateString();
  const dates = await getNbaPointsPropDates();
  const requested = params.date && /^\d{4}-\d{2}-\d{2}$/.test(params.date) ? params.date : null;
  const date = requested ?? (dates.includes(today) ? today : dates.find((d) => d > today) ?? dates[0] ?? today);

  const [rows, ladderRows, pulled, lastSeason, teams, calibration, nightCalibration, gradedRows] = await Promise.all([
    getNbaPointsProps(date),
    getNbaPointsProps(date, "player_points_alternate"),
    getNbaPulledLines(date),
    getNbaSeasonStats(LAST_SEASON),
    getNbaTeams(),
    getNbaPointsCalibration(),
    getNbaPointsCalibration(date),
    getNbaPointsCalibrationRows(),
  ]);
  const reliability = reliabilitySeries(gradedRows);
  const scatter = scatterData(gradedRows);
  const coverage = coverageData(gradedRows);
  const tails = tailTrend(gradedRows);
  const skill = skillTable(gradedRows);
  const shortNights = shortMinutesSummary(gradedRows);
  const { scorers, games } = buildBoard(rows, lastSeason, teams, ladderRows);
  const lastCapture = games.reduce<string | null>((m, g) => (m == null || g.capturedAt > m ? g.capturedAt : m), null);
  const bookCount = new Set(scorers.flatMap((s) => s.books.map((b) => b.book))).size;

  return (
    <main className="mx-auto max-w-7xl space-y-4 px-4 py-6">
      <header style={DARK_BORDER} className="rounded-xl border border-slate-800 bg-[#0f1726] px-5 py-4 text-slate-200">
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div>
            <div className="font-mono text-xs text-slate-400">NBA · Player points · Market lines</div>
            <h1 className="mt-1 text-2xl font-bold tracking-tight text-white">Top Scorers · {longDate(date)}</h1>
            <div className="mt-1 font-mono text-sm text-slate-400">
              <span className="text-emerald-400">{games.length} games</span> · {scorers.length} players · {bookCount} books
              {lastCapture ? ` · lines as of ${stamp(lastCapture)}` : ""}
            </div>
          </div>
          <span className="rounded-full border border-emerald-500/60! px-3 py-1 font-mono text-xs text-emerald-300">
            Market number · unvalidated
          </span>
        </div>
        {dates.length > 0 && (
          <nav className="mt-3 flex flex-wrap gap-1.5 font-mono text-xs" aria-label="Dates">
            {[...dates].sort().map((d) => (
              <Link
                key={d}
                href={`/nba/top-scorers?date=${d}`}
                className={`rounded px-2.5 py-1 ${d === date ? "bg-sky-500 text-white" : "border border-slate-700 text-slate-300 hover:bg-slate-800"}`}
              >
                {d.slice(5)}
              </Link>
            ))}
          </nav>
        )}
      </header>

      {scorers.length === 0 ? (
        <section style={DARK_BORDER} className="rounded-xl border border-slate-800 bg-[#0f1726] p-6 text-sm text-slate-300">
          <p className="font-medium text-white">No points lines stored for {longDate(date)}.</p>
          <p className="mt-1 text-slate-400">
            Lines come from{" "}
            <code className="text-slate-200">python -m ingest.nba_points_props --date {date} --apply --max-credits 20</code>.
            Books usually post NBA player props the morning of the game, so an early capture can come back empty.
          </p>
        </section>
      ) : (
        <TopScorersClient scorers={scorers} games={games} pulled={pulled} lastSeason={LAST_SEASON} />
      )}

      <CalibrationPanel
        all={calibration}
        night={nightCalibration}
        date={date}
        reliability={reliability}
        scatter={scatter}
        coverage={coverage}
        tails={tails}
        skill={skill}
        shortNights={shortNights}
      />

      <section className="space-y-1 text-xs text-muted-foreground">
        <p>
          Line is the points line most books quote. A points line is the books&apos; median outcome, so a player scores above
          it about half the time. Floor, ceiling, 25+ and 30+ come from the books&apos; alternate points ladder (&quot;20+ points&quot;
          and so on): floor is the score he beats 90% of the time, ceiling the score he beats 10% of the time. Over-only rungs
          are de-vigged with each book&apos;s main-line margin, which does not establish fair long-shot probabilities. High-end chances may be overstated. Without a ladder the bar shows the spread of main lines across books instead.
        </p>
        <p>
          This is the market&apos;s number, not a model of ours; the calibration panel above is its record against results.
          {LAST_SEASON} PPG is the full regular-season arithmetic average across recorded appearances, excluding playoffs.
        </p>
      </section>
    </main>
  );
}

const pct = (v: number | null | undefined, digits = 0) => (v == null ? "n/a" : `${(v * 100).toFixed(digits)}%`);

/**
 * The nightly grade of these numbers against the box score: what the books'
 * last pre-tip capture said, beside how often it happened. Pooled over every
 * graded night, with the viewed night beside it when it has been graded.
 */
function CalibrationPanel({
  all,
  night,
  date,
  reliability,
  scatter,
  coverage,
  tails,
  skill,
  shortNights,
}: {
  all: CalibrationSummary | null;
  night: CalibrationSummary | null;
  date: string;
  reliability: ReliabilitySeries[];
  scatter: ScatterData;
  coverage: CoverageData;
  tails: TailSeries[];
  skill: SkillRow[];
  shortNights: { n: number; short: number };
}) {
  if (!all) {
    return (
      <section style={DARK_BORDER} className="rounded-xl border border-slate-800 bg-[#0f1726] px-5 py-4 text-sm text-slate-300">
        <h2 className="font-semibold text-white">Calibration · not graded yet</h2>
        <p className="mt-1 text-slate-400">
          The nightly grade (<code className="text-slate-200">grade_nba_points_props.yml</code>, 7:07 AM ET) scores each
          game&apos;s last pre-tip capture against the ESPN box score. Nothing has been graded so far.
        </p>
      </section>
    );
  }
  const span = all.firstDate === all.lastDate ? all.lastDate : `${all.firstDate} to ${all.lastDate}`;
  const nightCells = new Map((night?.cells ?? []).map((c) => [c.key, c]));
  const nightLabel = date.slice(5);
  const rows = [
    ...all.cells.map((c) => {
      const n = nightCells.get(c.key);
      return {
        key: c.key,
        label: c.key === "over" ? "Over the line" : c.key,
        n: c.n,
        said: c.n ? pct(c.predicted, 1) : "n/a",
        happened: c.n ? pct(c.observed, 1) : "n/a",
        brier: c.n ? c.brier.toFixed(3) : "n/a",
        nightN: n?.n ?? 0,
        nightSaid: n?.n ? pct(n.predicted, 1) : "n/a",
        nightHappened: n?.n ? pct(n.observed, 1) : "n/a",
      };
    }),
    {
      key: "floor",
      label: "Beat the floor (target 90%)",
      n: all.floorN,
      said: "90%",
      happened: all.floorN ? pct(all.floorBeatRate, 1) : "n/a",
      brier: "n/a",
      nightN: night?.floorN ?? 0,
      nightSaid: "90%",
      nightHappened: night?.floorN ? pct(night.floorBeatRate, 1) : "n/a",
    },
    {
      key: "ceiling",
      label: "Beat the ceiling (target 10%)",
      n: all.ceilingN,
      said: "10%",
      happened: all.ceilingN ? pct(all.ceilingBeatRate, 1) : "n/a",
      brier: "n/a",
      nightN: night?.ceilingN ?? 0,
      nightSaid: "10%",
      nightHappened: night?.ceilingN ? pct(night.ceilingBeatRate, 1) : "n/a",
    },
  ];
  return (
    <section style={DARK_BORDER} className="rounded-xl border border-slate-800 bg-[#0f1726] px-5 py-4 text-slate-200">
      <div className="flex flex-wrap items-baseline justify-between gap-2">
        <h2 className="font-semibold text-white">Calibration · {all.nights} {all.nights === 1 ? "night" : "nights"} graded</h2>
        <div className="font-mono text-xs text-slate-400">
          {span} · {all.games} games · {all.graded} player-games graded
          {all.dnp ? ` · ${all.dnp} did not play` : ""}
          {all.unmatched ? ` · ${all.unmatched} unmatched` : ""}
          {all.noBoxScore ? ` · ${all.noBoxScore} without a box score` : ""}
          {all.meanMinutesBeforeTip != null ? ` · graded capture ${Math.round(all.meanMinutesBeforeTip)} min before tip` : ""}
          {all.lastGradedAt ? ` · graded ${stamp(all.lastGradedAt)}` : ""}
        </div>
      </div>
      <div className="mt-3 overflow-x-auto">
        <table className="w-full font-mono text-xs">
          <thead className="text-slate-400">
            <tr className="text-left">
              <th className="py-1 pr-3 font-normal">Reading</th>
              <th className="py-1 pr-3 text-right font-normal">n</th>
              <th className="py-1 pr-3 text-right font-normal">Books said</th>
              <th className="py-1 pr-3 text-right font-normal">Happened</th>
              <th className="py-1 pr-3 text-right font-normal">Brier</th>
              <th className="py-1 pr-3 text-right font-normal">{nightLabel} n</th>
              <th className="py-1 pr-3 text-right font-normal">{nightLabel} said</th>
              <th className="py-1 text-right font-normal">{nightLabel} happened</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((r) => (
              <tr key={r.key} className="border-t border-slate-800">
                <td className="py-1 pr-3 text-slate-200">{r.label}</td>
                <td className="py-1 pr-3 text-right">{r.n}</td>
                <td className="py-1 pr-3 text-right">{r.said}</td>
                <td className="py-1 pr-3 text-right text-white">{r.happened}</td>
                <td className="py-1 pr-3 text-right">{r.brier}</td>
                <td className="py-1 pr-3 text-right text-slate-400">{r.nightN}</td>
                <td className="py-1 pr-3 text-right text-slate-400">{r.nightSaid}</td>
                <td className="py-1 text-right text-slate-400">{r.nightHappened}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <p className="mt-2 text-xs text-slate-400">
        Each row grades the books&apos; last capture before tip-off, read exactly as the table above reads it, against the
        ESPN box score. A player who did not play is counted, not graded (the books void his line). Brier is the mean
        squared gap between the stated chance and what happened; lower is better, and a coin flip on a 50% line scores
        0.25. Under a few hundred player-games the night-to-night swing is noise; the pooled columns are the ones to read.
      </p>
      <CalibrationCharts
        reliability={reliability}
        scatter={scatter}
        coverage={coverage}
        tails={tails}
        skill={skill}
        shortNights={shortNights}
      />
    </section>
  );
}
