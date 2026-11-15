"use client";

import { useMemo, useState } from "react";
import Link from "next/link";
import { CartesianGrid, Line, LineChart, ReferenceLine, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import type { Observation } from "@/lib/daily-study-matches";
import { shiftDate } from "@/lib/daily-market-studies";
import { studyEvidence, studyPerformance, studySeries, studyUnits } from "@/lib/study-performance";
import { studyHeading } from "@/lib/study-registry";

const percent = new Intl.NumberFormat("en-US", { style: "percent", maximumFractionDigits: 1 });
const pct = (value: number | null) => value === null ? "—" : percent.format(value);
const number = (value: number | null) => value === null ? "—" : value.toFixed(2);
const field = "rounded-md border bg-background p-2 text-sm";
const stamp = new Intl.DateTimeFormat("en-US", { timeZone: "America/New_York", dateStyle: "short", timeStyle: "short" });

type Series = ReturnType<typeof studySeries>[number];
type Stats = ReturnType<typeof studyPerformance>;

function record(stats: Stats) {
  return [stats.wins, stats.losses, stats.pushes].join("–");
}

function StudyTrend({ stats }: { stats: Stats }) {
  if (!stats.priced) return <p className="text-sm text-muted-foreground">No settled selections have a verified price at the graded line.</p>;
  return <div className="space-y-2">
    <p className="text-sm">Cumulative return <b>{number(stats.units)} u</b> · Maximum daily drawdown <b>{number(stats.drawdown)} u</b></p>
    <div className="h-56 w-full min-w-0" role="img" aria-label="Cumulative flat stake return by game date">
      <ResponsiveContainer width="100%" height="100%">
        <LineChart data={stats.trend} margin={{ top: 10, right: 18, bottom: 0, left: 0 }}>
          <CartesianGrid vertical={false} stroke="var(--border)" />
          <XAxis dataKey="date" tickFormatter={date => date.slice(5)} minTickGap={25} tick={{ fontSize: 11 }} />
          <YAxis width={48} tick={{ fontSize: 11 }} domain={([low, high]: [number, number]) => [Math.min(0, low), Math.max(0, high)]} />
          <ReferenceLine y={0} stroke="var(--muted-foreground)" strokeDasharray="4 4" />
          <Tooltip contentStyle={{ background: "var(--card)", color: "var(--foreground)" }} />
          <Line dataKey="units" name="Cumulative units" stroke="var(--primary)" strokeWidth={2} dot={{ r: 2 }} isAnimationActive={false} />
        </LineChart>
      </ResponsiveContainer>
    </div>
  </div>;
}

function Ledger({ rows }: { rows: Observation[] }) {
  const [limit, setLimit] = useState(25);
  const sorted = [...rows].sort((a, b) => b.game.start.localeCompare(a.game.start));
  if (!sorted.length) return <p className="text-sm text-muted-foreground">No observations match these filters.</p>;
  return <>
    <div className="overflow-auto">
      <table className="w-full min-w-[42rem] text-left text-sm">
        <thead><tr>{["Game date", "Matchup and selection", "Price", "Quote / freeze (ET)", "Result", "Units"].map(label => <th key={label} scope="col" className="p-2">{label}</th>)}</tr></thead>
        <tbody>{sorted.slice(0, limit).map(row => <tr key={row.game.id} className="border-t">
          <td className="p-2">{row.game.date}</td>
          <td className="p-2">{row.game.away} @ {row.game.home}<span className="block text-muted-foreground">{row.side}</span></td>
          <td className="p-2 tabular-nums">{row.price == null || Math.abs(row.price) < 100 ? "—" : row.price > 0 ? "+" + row.price : row.price}</td>
          <td className="p-2">{row.observedAt && stamp.format(new Date(row.observedAt))}
            <span className="block text-xs text-muted-foreground">{row.quoteId ? "Pinnacle · " + row.quoteId : row.game.sport === "CFB" ? "Board entry" : "Pinnacle archive"}</span></td>
          <td className="p-2 whitespace-nowrap">{row.margin === null ? "Pending" : row.margin > 0 ? "Win" : row.margin < 0 ? "Loss" : "Push"}</td>
          <td className="p-2 tabular-nums">{number(studyUnits(row))}</td>
        </tr>)}</tbody>
      </table>
    </div>
    {sorted.length > limit && <button className={field} onClick={() => setLimit(limit + 50)}>Show more games ({sorted.length - limit} remaining)</button>}
  </>;
}

function Drilldown({ series, peers, date, shared }: { series: Series; peers: Series[]; date: string; shared: boolean }) {
  const [team, setTeam] = useState("");
  const [from, setFrom] = useState("");
  const [priceBand, setPriceBand] = useState("");
  const [timing, setTiming] = useState("");
  const [minLine, setMinLine] = useState("");
  const [maxLine, setMaxLine] = useState("");
  const teams = [...new Set(series.rows.flatMap(row => [row.game.home, row.game.away]))].sort();
  const rows = series.rows.filter(row => {
    const minutes = (Date.parse(row.game.start) - Date.parse(row.observedAt!)) / 60_000;
    const validPrice = row.price != null && Math.abs(row.price) >= 100;
    return (!team || row.game.home === team || row.game.away === team)
      && (!from || row.game.date >= from)
      && (!minLine || (row.line != null && row.line >= Number(minLine)))
      && (!maxLine || (row.line != null && row.line <= Number(maxLine)))
      && (!priceBand || (priceBand === "missing" ? !validPrice : validPrice && (priceBand === "plus" ? row.price! >= 100 : row.price! <= -100)))
      && (!timing || (timing === "late" ? minutes <= 60 : minutes > 60));
  });
  const parent = studyPerformance(series.rows);
  const subset = studyPerformance(rows);
  const months = new Map<string, Observation[]>();
  for (const row of rows) {
    const month = row.game.date.slice(0, 7);
    const existing = months.get(month) ?? [];
    existing.push(row);
    months.set(month, existing);
  }
  const games = new Set(rows.map(row => row.game.id));
  const overlap = peers.filter(peer => peer.id !== series.id && peer.rows[0]?.game.sport === series.rows[0].game.sport)
    .map(peer => ({ peer, count: peer.rows.filter(row => games.has(row.game.id)).length }))
    .filter(entry => entry.count > 0).sort((a, b) => b.count - a.count || a.peer.key.localeCompare(b.peer.key)).slice(0, 5);
  const href = series.id.startsWith("NBA") ? "/nba/market-studies" : series.id.startsWith("NFL") ? "/nfl/market-studies" : "/cfb/pattern-watch";
  return <section className="min-w-0 space-y-4 border-t pt-4" aria-label="Study drilldown">
    <div>
      <h3 className="font-semibold">{studyHeading(series.id)} · {series.cohort}</h3>
      {shared && <Link className="text-sm underline" href={href + "?" + new URLSearchParams({ date, cohort: series.key }) + "#study-performance"}>Open sport drilldown</Link>}
    </div>
    <div className="flex flex-wrap gap-3">
      <label className="grid gap-1 text-xs">Team or opponent<select className={field} value={team} onChange={event => setTeam(event.target.value)}>
        <option value="">All teams</option>{teams.map(name => <option key={name}>{name}</option>)}
      </select></label>
      <label className="grid gap-1 text-xs">Starting date<input type="date" className={field} value={from} max={date} onChange={event => setFrom(event.target.value)} /></label>
      {series.market !== "Outright win" && <>
        <label className="grid gap-1 text-xs">Minimum {series.market === "ATS" ? "spread size" : "total"}
          <input type="number" min="0" step="0.5" className={field + " w-28"} value={minLine} onChange={event => setMinLine(event.target.value)} />
        </label>
        <label className="grid gap-1 text-xs">Maximum {series.market === "ATS" ? "spread size" : "total"}
          <input type="number" min="0" step="0.5" className={field + " w-28"} value={maxLine} onChange={event => setMaxLine(event.target.value)} />
        </label>
      </>}
      <label className="grid gap-1 text-xs">Price range<select className={field} value={priceBand} onChange={event => setPriceBand(event.target.value)}>
        <option value="">All prices</option><option value="minus">Negative odds</option><option value="plus">Positive odds</option><option value="missing">Missing price</option>
      </select></label>
      <label className="grid gap-1 text-xs">{series.id.startsWith("T") ? "Board freeze timing" : "Quote timing"}<select className={field} value={timing} onChange={event => setTiming(event.target.value)}>
        <option value="">Any time</option><option value="late">Within 60 min of start</option><option value="early">More than 60 min before start</option>
      </select></label>
    </div>
    <p className="text-sm"><b>Filtered</b> {record(subset)} · {pct(subset.hitRate)} hit · {pct(subset.roi)} ROI · {subset.priced} priced.
      {" "}<b>Full cohort</b> {record(parent)} · {pct(parent.hitRate)} hit · {pct(parent.roi)} ROI · {parent.priced} priced.
      {" "}{series.rows.length - rows.length} observations excluded.</p>
    <StudyTrend stats={subset} />
    {!!months.size && <div>
      <h4 className="mb-2 text-sm font-medium">Monthly performance</h4>
      <ul className="flex flex-wrap gap-2">{[...months.keys()].sort().map(month => {
        const stat = studyPerformance(months.get(month)!);
        const tone = stat.roi === null || stat.roi === 0 ? "bg-muted" : stat.roi > 0 ? "bg-emerald-500/10" : "bg-rose-500/10";
        return <li key={month} className={tone + " rounded border px-3 py-2 text-xs"}>{month}<b className="block">{pct(stat.roi)} ROI</b>
          {record(stat)} · {stat.priced} priced</li>;
      })}</ul>
    </div>}
    {!!overlap.length && <details className="text-sm">
      <summary>Games shared with other study cohorts</summary>
      <ul className="mt-2 space-y-1">{overlap.map(({ peer, count }) => <li key={peer.key}>{peer.id} · {peer.cohort} · {count} of {games.size} games</li>)}</ul>
    </details>}
    <details className="space-y-2"><summary className="text-sm font-medium">Game ledger ({rows.length})</summary><Ledger rows={rows} /></details>
  </section>;
}

function ReturnRange({ stats, extent }: { stats: Stats; extent: number }) {
  if (stats.roi === null) return <span>—</span>;
  const coordinate = (value: number) => 50 + 50 * value / extent + "%";
  return <div className="space-y-1">
    <span>{pct(stats.roi)}{stats.range && <span className="text-xs text-muted-foreground"> ({pct(stats.range[0])} to {pct(stats.range[1])})</span>}</span>
    <div className="relative h-3 w-32 rounded bg-muted" aria-hidden="true">
      <div className="absolute inset-y-0 left-1/2 border-l border-muted-foreground" />
      {stats.range && <div className="absolute top-[5px] border-t-2 border-primary" style={{ left: coordinate(stats.range[0]), right: coordinate(-stats.range[1]) }} />}
      <div className="absolute top-[3px] size-1.5 -translate-x-1/2 rounded-full bg-primary" style={{ left: coordinate(stats.roi) }} />
    </div>
  </div>;
}

export function StudyPerformancePanel({ entries, date, initial, shared = false }: { entries: Observation[]; date: string; initial?: string; shared?: boolean }) {
  const groups = useMemo(() => studySeries(entries, date), [entries, date]);
  const comparison = useMemo(() => groups.map(group => ({ group, stats: studyPerformance(group.rows), evidence: studyEvidence(group.cohort) })), [groups]);
  const [selected, setSelected] = useState(initial ?? "");
  const [market, setMarket] = useState(groups.find(group => group.key === initial)?.market ?? groups[0]?.market ?? "ATS");
  const [sport, setSport] = useState("");
  const [evidence, setEvidence] = useState("");
  const [sort, setSort] = useState("Coverage");
  const [min, setMin] = useState(0);
  const available = comparison.filter(row => (!sport || row.group.rows[0].game.sport === sport)
    && (!evidence || row.evidence === evidence) && row.group.market === market
    && row.stats.wins + row.stats.losses >= min)
    .sort((a, b) => sort === "Return"
      ? (b.stats.roi ?? -Infinity) - (a.stats.roi ?? -Infinity) || b.stats.priced - a.stats.priced
      : (b.stats.wins + b.stats.losses) - (a.stats.wins + a.stats.losses));
  const focused = groups.find(group => group.key === selected) ?? available[0]?.group;
  const extent = Math.max(0.1, ...available.flatMap(({ stats }) => (stats.range ?? [stats.roi ?? 0]).map(Math.abs)));
  const recent = shiftDate(date, -6);
  return <section id="study-performance" className="min-w-0 scroll-mt-16 space-y-4 rounded-xl border bg-card p-4 sm:p-5">
    <div>
      <h2 className="text-xl font-semibold">Study performance</h2>
      <p className="text-sm text-muted-foreground">Through {date} · Prices at the graded line · One unit per priced selection. Pushes count in ROI and are excluded from hit rates.</p>
    </div>
    {!groups.length ? <p className="text-sm text-muted-foreground">No eligible study observations through this date.</p> : <>
      <div className="flex flex-wrap gap-3">
        {shared && <label className="grid gap-1 text-xs">Sport<select className={field} value={sport} onChange={e => { setSport(e.target.value); setSelected(""); }}>
          <option value="">All sports</option>{["NBA", "NFL", "CFB"].map(value => <option key={value}>{value}</option>)}
        </select></label>}
        <label className="grid gap-1 text-xs">Market<select className={field} value={market} onChange={e => { setMarket(e.target.value as Series["market"]); setSelected(""); }}>
          {["ATS", "Total", "Outright win"].map(value => <option key={value}>{value}</option>)}
        </select></label>
        <label className="grid gap-1 text-xs">Evidence<select className={field} value={evidence} onChange={e => { setEvidence(e.target.value); setSelected(""); }}>
          <option value="">All evidence</option>{["Exploratory archive", "NBA paper tracking", "Frozen CFB entries"].map(value => <option key={value}>{value}</option>)}
        </select></label>
        <label className="grid gap-1 text-xs">Sort by<select className={field} value={sort} onChange={e => setSort(e.target.value)}>
          <option>Coverage</option><option>Return</option>
        </select></label>
        <label className="grid gap-1 text-xs">Minimum decisions<select className={field} value={min} onChange={e => { setMin(Number(e.target.value)); setSelected(""); }}>
          {[0, 10, 25, 50, 100].map(value => <option key={value}>{value}</option>)}
        </select></label>
      </div>
      <p className="text-xs text-muted-foreground">Return ranges are descriptive 95% date-clustered approximations shown with at least 20 priced dates and measurable variation. Cohorts can share games.</p>
      <div className="max-h-[25rem] overflow-auto">
        <table className="w-full min-w-[54rem] text-left text-sm">
          <thead className="sticky top-0 z-10 bg-card"><tr>{["Study cohort", "Evidence", "W–L–P / hit", "Priced hit / avg. break-even", "Priced / unpriced / pending", "7-day decisions", "Return / range"].map(label => <th key={label} scope="col" className="p-2">{label}</th>)}</tr></thead>
          <tbody>{available.map(({ group, stats, evidence: mode }) => <tr key={group.key} className="border-t align-top">
            <th scope="row" className="p-2"><button className="text-left underline underline-offset-2" aria-pressed={focused?.key === group.key}
              onClick={() => setSelected(group.key)}>{group.id} · {group.cohort}</button></th>
            <td className="p-2">{mode}</td><td className="p-2 tabular-nums">{record(stats)} / {pct(stats.hitRate)}</td>
            <td className="p-2 tabular-nums">{pct(stats.pricedHitRate)} / {pct(stats.averageBreakEven)}</td>
            <td className="p-2 tabular-nums">{stats.priced} / {stats.missingPrice} / {stats.pending}<span className="block text-xs text-muted-foreground">{stats.priceDates} priced dates</span></td>
            <td className="p-2 tabular-nums">{group.rows.filter(row => row.game.date >= recent && row.margin !== null && row.margin !== 0).length}</td>
            <td className="p-2 tabular-nums"><ReturnRange stats={stats} extent={extent} /></td>
          </tr>)}</tbody>
        </table>
        {!available.length && <p className="p-2 text-sm text-muted-foreground">No cohorts match these comparison filters.</p>}
      </div>
      {focused && <Drilldown key={focused.key} series={focused} peers={groups} date={date} shared={shared} />}
    </>}
  </section>;
}

