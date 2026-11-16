import type { ReactNode } from "react";
import type { PatternWatchDataset } from "@/db/cfb-pattern-watch";
import { cfbDay } from "@/lib/daily-market-studies";
import { isNumber, PAPER_START, summarize as summarizeNba } from "@/lib/nba-market-studies";
import { summarize as summarizeNfl } from "@/lib/nfl/market-studies";
import { studyHeading, studyRegistry, type StudyId } from "@/lib/study-registry";

type NbaRows = Parameters<typeof summarizeNba>[0];
type NflRows = Parameters<typeof summarizeNfl>[0];
type NbaId = Extract<StudyId, `NBA-${string}`>;
type NflId = Extract<StudyId, `NFL-${string}`>;
type CfbId = Extract<StudyId, `T${string}`>;
type Cells = { label: string; day?: string; prior?: string };
type Study = { rows?: Cells[]; note?: string };
type Record = { wins: number; losses: number; pushes: number };
const pct = new Intl.NumberFormat("en-US", { style: "percent", maximumFractionDigits: 1 });
const units = new Intl.NumberFormat("en-US", { maximumFractionDigits: 2, signDisplay: "exceptZero" });
const empty = "No grades";
const waiting = (note: string): Study => ({ note });
const value = (x: number | null, digits = 2) => x === null ? "—" : x.toFixed(digits);
const record = (r: Record, pnl?: number) => {
  const decided = r.wins + r.losses;
  if (!decided && !r.pushes) return empty;
  return r.wins + "–" + r.losses + "–" + r.pushes +
    (decided ? " · " + pct.format(r.wins / decided) : "") +
    (pnl !== undefined ? " · " + units.format(pnl) + " u" : "");
};

function compare<T extends { label: string }>(day: T[], through: T[], cell: (row: T) => string): Cells[] {
  const a = new Map(day.map(row => [row.label, cell(row)]));
  const b = new Map(through.map(row => [row.label, cell(row)]));
  const labels = new Set([...a.keys(), ...b.keys()]);
  return [...labels].map(label => ({ label, day: a.get(label), prior: b.get(label) }));
}

function Group({ sport, studies, available }: {
  sport: string; studies: Partial<{ [Id in StudyId]: Study }>; available: boolean;
}) {
  const entries = Object.keys(studyRegistry).filter((id): id is StudyId =>
    sport === "CFB" ? id.startsWith("T") : id.startsWith(sport + "-"));
  return <section id={"all-studies-" + sport.toLowerCase()} className="scroll-mt-20 space-y-3">
    <h3 className="text-xl font-semibold">{sport} studies</h3>
    {entries.map(id => {
      const detail = studies[id];
      const rows = available ? detail?.rows : undefined;
      return <article key={id} className="rounded-lg border bg-card p-4">
        <h4 className="font-medium">{studyHeading(id)}</h4>
        <p className="mt-1 text-xs text-muted-foreground">
          {detail?.note ?? (!available ? "Source unavailable." : !rows?.length ? "No grades published for this view." : null)}
        </p>
        {!!rows?.length && <div className="mt-2 max-h-96 overflow-auto">
          <table className="w-full min-w-[30rem] text-left text-sm">
            <thead className="sticky top-0 bg-card text-xs text-muted-foreground">
              <tr><th scope="col" className="py-1 pr-4">Measure</th><th scope="col" className="pr-4">Selected date</th><th scope="col">Through date</th></tr>
            </thead>
            <tbody>{rows.map(row => <tr key={row.label} className="border-t align-top">
              <th scope="row" className="py-1 pr-4 font-medium">{row.label}</th>
              <td className="pr-4 tabular-nums">{row.day ?? empty}</td>
              <td className="tabular-nums">{row.prior ?? empty}</td>
            </tr>)}</tbody>
          </table>
        </div>}
      </article>;
    })}
  </section>;
}

function nbaTiming(rows: NbaRows) {
  function paired(label: string, pairs: [number, number][]) {
    if (!pairs.length) return { label, value: "No paired games" };
    const first = pairs.reduce((sum, pair) => sum + pair[0], 0) / pairs.length;
    const last = pairs.reduce((sum, pair) => sum + pair[1], 0) / pairs.length;
    return { label, value: value(first) + " → " + value(last) + " pts · " + pairs.length + " games" };
  }
  return [
    paired("Home spread", rows.flatMap(row => row.margin !== null && row.spreadQuotes.length > 1 &&
      isNumber(row.firstSpread) && isNumber(row.spread)
      ? [[Math.abs(row.margin + row.firstSpread), Math.abs(row.margin + row.spread)]] : [])),
    paired("Game total", rows.flatMap(row => {
      const first = row.firstTotal?.books.pinnacle.total;
      const last = row.lastTotal?.books.pinnacle.total;
      return row.actualTotal !== null && row.firstTotal !== row.lastTotal && isNumber(first) && isNumber(last)
        ? [[Math.abs(row.actualTotal - first), Math.abs(row.actualTotal - last)]] : [];
    })),
  ];
}

function nbaStudies(rows: NbaRows, date: string): { [Id in NbaId]: Study } {
  const today = rows.filter(row => row.game.date === date);
  const a = summarizeNba(today);
  const b = summarizeNba(rows);
  const priceRecord = (r: typeof b.totals[number]) =>
    record({ wins: r.win, losses: r.loss, pushes: r.push }, r.priced ? r.units : undefined) +
    (r.priced > 0 && r.priced < r.games ? " · " + r.priced + " priced" : "");
  const teamRows = (summary: typeof b) => summary.teams.flatMap(team => [
    { ...team.first, label: team.label + " · first quote" },
    { ...team.last, label: team.label + " · last quote" },
  ]);
  const shopping = (v: typeof b.shopping) => v.comparisons ? v.improvements + "/" + v.comparisons + " sides" : "No exact-line comparisons";
  return {
    "NBA-01": { rows: compare(a.spread, b.spread, r => priceRecord(r) + (r.games ? " · " + r.outright + "/" + r.games + " outright" : "")), note: "Favorite covers by spread size, with outright wins shown separately." },
    "NBA-02": { rows: compare(a.rest, b.rest, priceRecord) },
    "NBA-03": { rows: compare(a.venue, b.venue, priceRecord) },
    "NBA-04": { rows: compare(a.movement, b.movement, priceRecord) },
    "NBA-05": { rows: [
      { label: "Over 235+ · paper trial", day: date < PAPER_START ? "Before trial" : priceRecord(a.paper.result) + " · " + a.paper.pending + " pending",
        prior: date < PAPER_START ? "Before trial" : priceRecord(b.paper.result) + " · " + b.paper.missing + " games missing entry quote" },
      ...compare(a.totals, b.totals, priceRecord),
    ] },
    "NBA-06": { rows: compare(teamRows(a), teamRows(b), priceRecord), note: "Team ATS by market role and quote timing." },
    "NBA-07": { rows: compare(nbaTiming(today), nbaTiming(rows), row => row.value), note: "First to last archived quote mean absolute error on paired games." },
    "NBA-08": { rows: [{ label: "Better same-capture price than Pinnacle", day: shopping(a.shopping), prior: shopping(b.shopping) }], note: "Exact-line quote comparisons." },
    "NBA-09": waiting("Awaiting original point-in-time totals forecasts."),
    "NBA-10": waiting("Awaiting consistent two-sided book-level moneyline coverage."),
    "NBA-11": { rows: compare(a.travel, b.travel, priceRecord) },
    "NBA-12": waiting("Awaiting official observations matched to before-and-after quotes."),
    "NBA-13": waiting("Awaiting comparable first-half prices and verified halftime scores."),
  };
}

function nflStudies(rows: NflRows, date: string): { [Id in NflId]: Study } {
  const a = summarizeNfl(rows.filter(row => row.game.date === date));
  const b = summarizeNfl(rows.filter(row => row.game.date && row.game.date <= date));
  const priced = (r: typeof b.totals[number]) => record(r, r.units);
  const timing = (r: typeof b.timing[number]) => r.n ? value(r.first) + " → " + value(r.last) + " pts · " + r.n + " games" : "No paired games";
  const calibration = (c: typeof b.calibration) => [
    { label: "Paired Brier · Pinnacle / consensus", text: c.paired
      ? value(c.pinnacleBrier, 4) + " / " + value(c.consensusBrier, 4) + " · " + c.paired + " games" : "No paired games" },
    ...c.bins.map(bin => ({ label: "Home win calibration · " + bin.label,
      text: bin.probability === null || bin.actual === null ? "No games" : pct.format(bin.probability) + " forecast / " + pct.format(bin.actual) + " actual · " + bin.n + " games" })),
  ];
  const shopping = (x: { compared: number; better: number }) => x.compared ? x.better + "/" + x.compared + " sides" : "No exact-line comparisons";
  return {
    "NFL-01": { rows: compare(a.spread, b.spread, priced) },
    "NFL-02": { rows: compare(a.rest, b.rest, priced) },
    "NFL-03": { rows: compare(a.venue, b.venue, priced) },
    "NFL-04": { rows: compare(a.movement, b.movement, priced) },
    "NFL-05": { rows: compare(a.totals, b.totals, priced) },
    "NFL-06": { rows: compare(a.teams, b.teams, priced) },
    "NFL-07": { rows: compare(a.timing.map(r => ({ ...r, label: r.market })), b.timing.map(r => ({ ...r, label: r.market })), timing),
      note: "First to last archived quote mean absolute error on paired games." },
    "NFL-08": { rows: (["spread", "total", "moneyline"] as const).map(market => ({
      label: market, day: shopping(a.shopping[market]), prior: shopping(b.shopping[market]),
    })), note: "Exact-line price comparisons in the same capture before the study’s 60-minute cutoff." },
    "NFL-09": waiting("Awaiting original pregame totals forecasts."),
    "NFL-10": { rows: [...compare(a.moneyline, b.moneyline, priced), ...compare(calibration(a.calibration), calibration(b.calibration), r => r.text)] },
    "NFL-11": { rows: compare(a.restEdge, b.restEdge, priced) },
    "NFL-12": waiting("Awaiting timestamped official observations matched to before-and-after prices."),
    "NFL-13": waiting("Awaiting archived first-half prices and verified halftime scores."),
  };
}

function cfbStudies(dataset: PatternWatchDataset | null, date: string, unavailable: boolean): { [Id in CfbId]: Study } {
  const daily = new Map(cfbDay(dataset?.results.filter(row => row.Date === date) ?? []));
  const through = new Map(cfbDay(dataset?.results.filter(row => row.Date <= date) ?? []));
  const study = (id: CfbId): Study => {
    if (!dataset) return waiting(unavailable ? "CFB study data is temporarily unavailable." : "Awaiting a published CFB dataset.");
    const pattern = dataset.patterns.find(row => row.Pattern === id);
    const d = daily.get(id);
    const s = through.get(id);
    return { note: pattern?.Rule, rows: [
      { label: "Closing record", day: d && record(d, d.units), prior: s && record(s, s.units) },
      ...(pattern ? [{ label: "Registered sample floor", prior: (s ? s.wins + s.losses + s.pushes : 0) + "/" + pattern["Sample floor"] }] : []),
    ] };
  };
  return { T1: study("T1"), T2: study("T2"), T3: study("T3"), T4: study("T4"), T5: study("T5"), T6: study("T6"), T7: study("T7") };
}

export function AllStudies({ nba, nfl, cfb, cfbUnavailable, date }: {
  nba: NbaRows | null; nfl: NflRows | null; cfb: PatternWatchDataset | null; cfbUnavailable: boolean; date: string;
}): ReactNode {
  return <section id="all-studies" aria-labelledby="all-studies-title" className="scroll-mt-20 space-y-5">
    <header>
      <h2 id="all-studies-title" className="text-2xl font-semibold">All study results</h2>
      <p className="text-sm text-muted-foreground">Daily and cumulative records for every registered study, including studies below the 65% threshold.</p>
      <nav aria-label="Jump to sport studies" className="mt-2 flex gap-4 text-sm">
        {["NBA", "NFL", "CFB"].map(sport => <a key={sport} href={"#all-studies-" + sport.toLowerCase()} className="underline underline-offset-4">{sport}</a>)}
      </nav>
    </header>
    <Group sport="NBA" available={nba !== null} studies={nbaStudies(nba ?? [], date)} />
    <Group sport="NFL" available={nfl !== null} studies={nflStudies(nfl ?? [], date)} />
    <Group sport="CFB" available={cfb !== null} studies={cfbStudies(cfb, date, cfbUnavailable)} />
  </section>;
}
