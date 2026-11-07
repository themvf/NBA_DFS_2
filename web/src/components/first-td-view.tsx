import type { ReactNode } from "react";
import type { FirstTdChip, FirstTdGame, FirstTdGrades } from "@/db/cfb-first-td";

/** The First TD page body, shared by the CFB and NFL tabs; each tab supplies its data and wording. */

const ET = "America/New_York";
export function pct(value: number) { return `${(value * 100).toFixed(1)}%`; }
function american(price: number) { return price > 0 ? `+${price}` : `${price}`.replace("-", "−"); }
function time(value: string) {
  return new Intl.DateTimeFormat("en-US", { timeZone: ET, month: "short", day: "numeric", hour: "numeric", minute: "2-digit", timeZoneName: "short" }).format(new Date(value));
}
export function etDate(ms: number) { return new Intl.DateTimeFormat("en-CA", { timeZone: ET }).format(new Date(ms)); }
function dateLabel(d: string) {
  return new Intl.DateTimeFormat("en-US", { timeZone: "UTC", weekday: "short", month: "short", day: "numeric" }).format(new Date(`${d}T12:00:00Z`));
}
/** The scoring play, cut before the extra point and the clock note. */
function shortPlay(text: string) {
  return text.replace(/^\(\d{1,2}:\d{2}\)\s*/, "").split(/,\s*clock|\(.*kick/i)[0].trim();
}

const CHIP_STYLE: Record<string, string> = {
  GOAL_LINE: "bg-emerald-100 text-emerald-900",
  INSIDE_TEN: "bg-teal-100 text-teal-900",
  HOT: "bg-orange-100 text-orange-900",
  NEW_ROLE: "bg-sky-100 text-sky-900",
  RUNNING_QB: "bg-violet-100 text-violet-900",
  NOT_LISTED: "bg-rose-100 text-rose-900",
  BIG_FAVOURITE: "bg-amber-100 text-amber-900",
  SCORED_FIRST: "bg-slate-100 text-slate-700",
};

function Chip({ chip }: { chip: FirstTdChip }) {
  return <span className={`mr-1 mt-1 inline-block rounded-full px-2 py-0.5 text-xs font-medium ${CHIP_STYLE[chip.code] ?? "bg-slate-100 text-slate-700"}`}>{chip.label}</span>;
}

function playerChips(p: FirstTdGame["players"][number]): FirstTdChip[] {
  const chips = [...(p.chips ?? [])];
  if (p.listed === false) chips.push({ code: "NOT_LISTED", label: "Not on DK list" });
  return chips;
}

function GameCard({ g, started, volumeLabel, actualWait }: { g: FirstTdGame; started: boolean; volumeLabel: string; actualWait: string }) {
  const pAway = 1 - g.pHomeFirst;
  return <section className="overflow-hidden rounded-xl border bg-white">
    <div className="border-b p-4">
      <div className="flex flex-wrap items-baseline justify-between gap-2">
        <h2 className="text-lg font-semibold">{g.away} at {g.home}</h2>
        <span className="text-sm text-slate-600">{time(g.kickoff)}{started ? " · started" : ""}</span>
      </div>
      <p className="mt-1 text-sm text-slate-600">
        Implied points {g.away} {g.awayImplied.toFixed(1)}, {g.home} {g.homeImplied.toFixed(1)} ·
        Scores first: <strong>{g.away} {pct(pAway * (1 - g.nonOffense))}</strong>, <strong>{g.home} {pct(g.pHomeFirst * (1 - g.nonOffense))}</strong>,
        defense or special teams {pct(g.nonOffense)}
      </p>
      {(g.teamChips.away.length > 0 || g.teamChips.home.length > 0) && <div className="mt-1 grid gap-1 text-xs sm:grid-cols-2">
        {(["away", "home"] as const).map((side) => <div key={side}>
          <span className="mr-1 font-semibold text-slate-700">{g[side]}</span>
          {g.teamChips[side].map((c) => <Chip key={c.code} chip={c} />)}
        </div>)}
      </div>}
      <div className="mt-2 flex h-2 overflow-hidden rounded bg-slate-100" aria-hidden>
        <div className="bg-slate-500" style={{ width: `${pAway * 100}%` }} />
        <div className="bg-emerald-700" style={{ width: `${g.pHomeFirst * 100}%` }} />
      </div>
    </div>
    <div className="overflow-x-auto"><table className="w-full text-left text-sm">
      <thead className="bg-slate-50"><tr>
        <th className="p-3">Player</th><th className="p-3">Chance</th><th className="p-3">Fair price</th>
        <th className="p-3">TDs this season</th><th className="p-3">{volumeLabel}</th>
      </tr></thead>
      <tbody>{g.players.map((p) => <tr className="border-t" key={`${p.team}-${p.name}`}>
        <td className="p-3 font-medium">{p.name}<span className="ml-2 font-normal text-slate-500">{p.team}</span>
          {playerChips(p).length > 0 && <div>{playerChips(p).map((c) => <Chip key={c.code} chip={c} />)}</div>}
        </td>
        <td className="p-3 font-semibold">{pct(p.prob)}</td>
        <td className="p-3">{american(p.fair)}</td>
        <td className="p-3">{p.td}</td>
        <td className="p-3">{p.touches}</td>
      </tr>)}</tbody>
    </table></div>
    <div className="space-y-1 border-t bg-slate-50 p-4 text-sm text-slate-600">
      <p>Everyone else on offense: {pct(g.otherOffense)} combined. Saved {time(g.savedAt)}.</p>
      <AvailabilityNote a={g.availability} />
      {g.actual
        ? <p className="text-slate-900"><strong>First touchdown:</strong> {g.actual.playType} · {shortPlay(g.actual.text)}</p>
        : started ? <p>{actualWait}</p> : null}
    </div>
  </section>;
}

function AvailabilityNote({ a }: { a: FirstTdGame["availability"] }) {
  if (!a) return null;
  if (a.status === "no_lines") return <p>No DraftKings touchdown list for this game yet, so nobody was removed.</p>;
  if (a.dropped.length === 0) return <p>Everyone in our player list is on DraftKings&apos; anytime-touchdown list.</p>;
  return <p>
    <strong className="text-slate-900">Left out (not on DraftKings&apos; anytime-touchdown list):</strong>{" "}
    {a.dropped.map((d) => `${d.name} (${d.team})`).join(", ")}. Their share went to their teammates.
  </p>;
}

function GradePanel({ grades, gradeWait, enough }: { grades: FirstTdGrades | null; gradeWait: string; enough: number }) {
  return <section className="rounded-xl border bg-white p-4 text-sm">
    <h2 className="text-base font-semibold">Does removing players without a line help?</h2>
    {!grades
      ? <p className="mt-2 text-slate-600">No graded games yet. {gradeWait}</p>
      : <>
        <table className="mt-3 w-full max-w-xl text-left">
          <thead><tr className="text-slate-600"><th className="py-1">Version</th><th className="py-1">Log loss (lower is better)</th><th className="py-1">Top pick scored first</th></tr></thead>
          <tbody>
            <tr className="border-t"><td className="py-1">Our model alone</td><td className="py-1">{grades.baseLogLoss.toFixed(3)}</td><td className="py-1">{pct(grades.baseTopPick)}</td></tr>
            <tr className="border-t"><td className="py-1">With players without a line removed</td><td className="py-1">{grades.linesLogLoss.toFixed(3)}</td><td className="py-1">{pct(grades.linesTopPick)}</td></tr>
          </tbody>
        </table>
        <p className="mt-2 text-slate-600">
          {grades.games} games graded; the filter changed the forecast in {grades.changed}. Difference{" "}
          {(grades.linesLogLoss - grades.baseLogLoss >= 0 ? "+" : "") + (grades.linesLogLoss - grades.baseLogLoss).toFixed(3)}.
          {grades.games < enough ? " Too few games to call it yet." : ""}
        </p>
      </>}
  </section>;
}

export type FirstTdViewProps = {
  subtitle: string;
  banner: ReactNode;
  dates: string[];
  date: string | undefined;
  today: string;
  nowMs: number;
  games: FirstTdGame[];
  grades: FirstTdGrades | null;
  emptyNote: string;
  volumeLabel: string;
  actualWait: string;
  gradeWait: string;
  enoughGames: number;
  notes: ReactNode;
};

export function FirstTdView(props: FirstTdViewProps) {
  const { dates, date, today, nowMs, games } = props;
  return <main className="mx-auto max-w-6xl space-y-6 px-4 py-8">
    <header className="space-y-3">
      <div className="flex flex-wrap items-center gap-3">
        <h1 className="text-3xl font-bold">First touchdown scorer</h1>
        <span className="rounded-full bg-amber-100 px-3 py-1 text-sm font-semibold text-amber-900">Research model</span>
      </div>
      <p>{props.subtitle}</p>
      <div className="rounded-xl border border-amber-200 bg-amber-50 p-4 text-sm text-amber-950">{props.banner}</div>
    </header>
    {dates.length > 0 && <form method="get" className="flex flex-wrap items-end gap-3">
      <label className="space-y-1 text-sm font-medium"><span className="block">Date</span>
        <select name="date" defaultValue={date} className="rounded border bg-white px-3 py-2">
          {dates.map((d) => <option key={d} value={d}>{dateLabel(d)}{d === today ? " (today)" : ""}</option>)}
        </select>
      </label>
      <button className="rounded bg-emerald-800 px-4 py-2 text-sm text-white" type="submit">Show</button>
    </form>}
    {games.length === 0
      ? <p className="rounded-xl border bg-white p-4 text-sm text-slate-600">No saved forecasts{date ? ` for ${dateLabel(date)}` : " yet"}. {props.emptyNote}</p>
      : games.map((g) => <GameCard key={g.gameKey} g={g} started={nowMs >= Date.parse(g.kickoff)}
                                   volumeLabel={props.volumeLabel} actualWait={props.actualWait} />)}
    <GradePanel grades={props.grades} gradeWait={props.gradeWait} enough={props.enoughGames} />
    <details className="rounded-xl border bg-white p-4"><summary className="cursor-pointer font-semibold">How it works</summary>
      <ul className="mt-3 list-disc space-y-2 pl-5 text-sm text-slate-600">{props.notes}</ul>
    </details>
  </main>;
}

/** Pick the date to show: the one asked for, else the next date with forecasts, else the latest. */
export function pickDate(dates: string[], today: string, asked: string | undefined): string | undefined {
  if (asked && /^\d{4}-\d{2}-\d{2}$/.test(asked)) return asked;
  return [...dates].reverse().find((d) => d >= today) ?? dates[0];
}
