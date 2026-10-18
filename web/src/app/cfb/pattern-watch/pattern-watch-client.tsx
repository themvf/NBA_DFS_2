"use client";

import { useMemo, useState } from "react";
import {
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import type { PatternWatchDataset } from "@/db/cfb-pattern-watch";
import MarketBreakdown from "./market-breakdown";

// dataviz reference palette, categorical slots in fixed order (never cycled).
const SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6", "#e34948"];

const signed = (v: number | null | undefined, digits = 2) =>
  v == null ? "—" : `${v > 0 ? "+" : ""}${v.toFixed(digits)}`;
const pct = (v: number | null | undefined) => (v == null ? "—" : `${v.toFixed(1)}%`);
const tone = (v: number | null | undefined) => (v == null || v === 0 ? "" : v > 0 ? "text-emerald-700" : "text-rose-700");

function shortDay(d: string): string {
  const x = new Date(`${d}T12:00:00Z`);
  return x.toLocaleDateString("en-US", { month: "short", day: "numeric", timeZone: "UTC" });
}

function Tile({ label, value, sub, className = "" }: { label: string; value: string; sub?: string; className?: string }) {
  return (
    <div className="rounded-lg border bg-card p-4">
      <div className="text-[11px] uppercase tracking-wide text-gray-500">{label}</div>
      <div className={`mt-1 text-2xl font-semibold tabular-nums ${className}`}>{value}</div>
      {sub && <div className="text-xs text-gray-500">{sub}</div>}
    </div>
  );
}

function Pill({ result }: { result: string }) {
  const cls =
    result === "won" ? "bg-emerald-50 text-emerald-700" : result === "lost" ? "bg-rose-50 text-rose-700" : "bg-slate-100 text-slate-600";
  return <span className={`rounded-full px-2 py-0.5 text-[11px] font-medium uppercase ${cls}`}>{result}</span>;
}

export default function PatternWatchClient({ dataset, error }: { dataset: PatternWatchDataset | null; error: string | null }) {
  const [patternFilter, setPatternFilter] = useState("all");

  const codes = useMemo(() => (dataset?.patterns ?? []).map((p) => p.Pattern), [dataset]);
  const totals = useMemo(() => {
    const p = dataset?.patterns ?? [];
    return {
      graded: p.reduce((s, x) => s + x.Graded, 0),
      wins: p.reduce((s, x) => s + x.Wins, 0),
      losses: p.reduce((s, x) => s + x.Losses, 0),
      pushes: p.reduce((s, x) => s + x.Pushes, 0),
      units: p.reduce((s, x) => s + x.Units, 0),
    };
  }, [dataset]);
  const results = useMemo(() => {
    const r = [...(dataset?.results ?? [])].reverse();
    return patternFilter === "all" ? r : r.filter((x) => x.Pattern === patternFilter);
  }, [dataset, patternFilter]);

  if (error) {
    return <main className="mx-auto max-w-6xl p-6"><h1 className="text-2xl font-semibold">CFB Pattern Watch</h1><p className="mt-4 text-rose-700">{error}</p></main>;
  }
  if (!dataset) {
    return (
      <main className="mx-auto max-w-6xl p-6">
        <h1 className="text-2xl font-semibold">CFB Pattern Watch</h1>
        <p className="mt-4 text-gray-600">No data stored yet. The next pattern watch run writes it.</p>
      </main>
    );
  }

  const cumulative = dataset.cumulative.map((r) => ({ ...r, label: shortDay(String(r.Date)) }));
  const weekly = dataset.weekly.filter((w) => w["Favorites won (%)"] != null && w["Price implied (%)"] != null);

  return (
    <main className="mx-auto max-w-6xl space-y-6 p-4 sm:p-6">
      <header>
        <div className="text-xs uppercase tracking-wide text-gray-500">
          Private · updated {new Date(dataset.generatedAt).toLocaleString("en-US", { timeZone: "America/New_York", dateStyle: "medium", timeStyle: "short" })} ET
        </div>
        <h1 className="text-3xl font-semibold">College Football Pattern Watch</h1>
      </header>

      <section className="grid grid-cols-2 gap-3 sm:grid-cols-4">
        <Tile label="Graded picks" value={String(totals.graded)} />
        <Tile label="Record" value={`${totals.wins}-${totals.losses}`} sub={`${totals.pushes} push`} />
        <Tile label="Units, 1 per pick" value={signed(totals.units)} className={tone(totals.units)} />
        <Tile label="Verdict" value="None" sub="not before Dec 7" />
      </section>

      <section className="rounded-lg border bg-card p-4">
        <h2 className="text-sm font-semibold">Running units by pattern</h2>
        <p className="mb-3 text-xs text-gray-500">One unit per qualifying game, graded at the closing line.</p>
        <div className="h-72">
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={cumulative} margin={{ top: 8, right: 16, bottom: 4, left: 0 }}>
              <CartesianGrid stroke="#ecebe7" vertical={false} />
              <XAxis dataKey="label" tick={{ fontSize: 11 }} tickLine={false} />
              <YAxis tick={{ fontSize: 11 }} tickLine={false} axisLine={false} width={36} />
              <ReferenceLine y={0} stroke="#52514e" />
              <Tooltip formatter={(v) => signed(Number(v))} />
              <Legend wrapperStyle={{ fontSize: 12 }} />
              {codes.map((c, i) => (
                <Line key={c} dataKey={c} name={c} stroke={SERIES[i]} strokeWidth={2} dot={{ r: 3 }} isAnimationActive={false} />
              ))}
            </LineChart>
          </ResponsiveContainer>
        </div>
      </section>

      <section className="rounded-lg border bg-card p-4">
        <h2 className="mb-3 text-sm font-semibold">Patterns</h2>
        <div className="overflow-x-auto">
          <table className="min-w-full text-xs">
            <thead>
              <tr className="border-b text-left text-gray-500">
                <th className="py-1 pr-3">#</th><th className="py-1 pr-3">Rule</th><th className="py-1 pr-3">Bet</th>
                <th className="py-1 pr-3 text-right">Graded</th><th className="py-1 pr-3 text-right">Record</th>
                <th className="py-1 pr-3 text-right">Units</th><th className="py-1 pr-3 text-right">ROI</th>
                <th className="py-1 pr-3 text-right">To floor</th><th className="py-1 text-right">Discovery</th>
              </tr>
            </thead>
            <tbody>
              {dataset.patterns.map((p) => (
                <tr key={p.Pattern} className="border-b border-gray-50">
                  <td className="py-1 pr-3 font-semibold">{p.Pattern}</td>
                  <td className="py-1 pr-3">{p.Rule}</td>
                  <td className="py-1 pr-3">{p.Bet}</td>
                  <td className="py-1 pr-3 text-right tabular-nums">{p.Graded}</td>
                  <td className="py-1 pr-3 text-right tabular-nums">{p.Record}</td>
                  <td className={`py-1 pr-3 text-right tabular-nums ${tone(p.Units)}`}>{signed(p.Units)}</td>
                  <td className={`py-1 pr-3 text-right tabular-nums ${tone(p["ROI (%)"])}`}>{p["ROI (%)"] == null ? "—" : `${signed(p["ROI (%)"], 1)}%`}</td>
                  <td className="py-1 pr-3 text-right tabular-nums">{p.Graded}/{p["Sample floor"]}</td>
                  <td className="py-1 text-right tabular-nums text-gray-500">{p["Discovery record"]} · {p["Discovery ROI"]}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      {dataset.marketBreakdown.length > 0 && <MarketBreakdown rows={dataset.marketBreakdown} />}

      {/* Older datasets carry only the overlapping splits; newer ones render the ordered breakdown above. */}
      {dataset.marketBreakdown.length === 0 && dataset.marketSplits.length > 0 && (
        <section className="rounded-lg border bg-card p-4">
          <h2 className="text-sm font-semibold">Moneyline favorites and underdogs, season to date</h2>
          <p className="mb-3 text-xs text-gray-500">
            All FBS games with a closing line. One unit on each side at its closing moneyline: the favorite, then the underdog in the
            same games. Descriptive only: these
            splits are not study patterns and are not graded for a verdict.
          </p>
          <div className="overflow-x-auto">
            <table className="min-w-full text-xs">
              <thead>
                <tr className="border-b text-gray-500">
                  <th className="py-1 pr-3 text-left">Split</th><th className="py-1 pr-3 text-right">Games</th>
                  <th className="py-1 pr-3 text-right">Record</th><th className="py-1 pr-3 text-right">Won</th>
                  <th className="py-1 pr-3 text-right">Implied</th><th className="py-1 pr-3 text-right">Difference</th>
                  <th className="py-1 pr-3 text-right">Units</th><th className="py-1 pr-3 text-right">ROI</th>
                  <th className="py-1 pr-3 text-right">Median price</th>
                  <th className="border-l py-1 pl-3 pr-3 text-right">Dog record</th><th className="py-1 pr-3 text-right">Dog units</th>
                  <th className="py-1 pr-3 text-right">Dog ROI</th><th className="py-1 text-right">Dog median price</th>
                </tr>
              </thead>
              <tbody>
                {dataset.marketSplits.map((m) => (
                  <tr key={m.Split} className="border-b border-gray-50 tabular-nums">
                    <td className="py-1 pr-3">{m.Split}</td>
                    <td className="py-1 pr-3 text-right">{m.Games}</td>
                    <td className="py-1 pr-3 text-right">{m.Record}</td>
                    <td className="py-1 pr-3 text-right">{pct(m["Favorites won (%)"])}</td>
                    <td className="py-1 pr-3 text-right">{pct(m["Price implied (%)"])}</td>
                    <td className={`py-1 pr-3 text-right ${tone(m["Difference (pts)"])}`}>{m["Difference (pts)"] == null ? "—" : `${signed(m["Difference (pts)"], 1)} pts`}</td>
                    <td className={`py-1 pr-3 text-right ${tone(m.Units)}`}>{signed(m.Units)}</td>
                    <td className={`py-1 pr-3 text-right ${tone(m["ROI (%)"])}`}>{m["ROI (%)"] == null ? "—" : `${signed(m["ROI (%)"], 1)}%`}</td>
                    <td className="py-1 pr-3 text-right">{m["Median price"] ?? "—"}</td>
                    <td className="border-l py-1 pl-3 pr-3 text-right">{m["Underdog record"] ?? "—"}</td>
                    <td className={`py-1 pr-3 text-right ${tone(m["Underdog units"])}`}>{signed(m["Underdog units"])}</td>
                    <td className={`py-1 pr-3 text-right ${tone(m["Underdog ROI (%)"])}`}>{m["Underdog ROI (%)"] == null ? "—" : `${signed(m["Underdog ROI (%)"], 1)}%`}</td>
                    <td className="py-1 text-right">{m["Underdog median price"] ?? "—"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <p className="mt-2 text-[11px] text-gray-500">Splits overlap: a game can appear in several rows. Small samples (a handful of games) mean very little.</p>
        </section>
      )}

      {weekly.length > 0 && (
        <section className="rounded-lg border bg-card p-4">
          <h2 className="text-sm font-semibold">Favorites&apos; win rate against price-implied, by week</h2>
          <p className="mb-3 text-xs text-gray-500">All FBS games with a closing line. Implied removes the bookmaker&apos;s margin. CFBD week numbering.</p>
          <div className="h-64">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={weekly} margin={{ top: 8, right: 16, bottom: 4, left: 0 }}>
                <CartesianGrid stroke="#ecebe7" vertical={false} />
                <XAxis dataKey="Week" tickFormatter={(w) => `Wk ${w}`} tick={{ fontSize: 11 }} tickLine={false} />
                <YAxis domain={["dataMin - 3", "dataMax + 3"]} tickFormatter={(v) => `${Math.round(Number(v))}%`} tick={{ fontSize: 11 }} tickLine={false} axisLine={false} width={40} />
                <Tooltip formatter={(v) => pct(Number(v))} labelFormatter={(w) => `Week ${w}`} />
                <Legend wrapperStyle={{ fontSize: 12 }} />
                <Line dataKey="Favorites won (%)" name="Favorites won" stroke={SERIES[0]} strokeWidth={2} dot={{ r: 3 }} isAnimationActive={false} />
                <Line dataKey="Price implied (%)" name="Price implied" stroke={SERIES[1]} strokeWidth={2} dot={{ r: 3 }} isAnimationActive={false} />
              </LineChart>
            </ResponsiveContainer>
          </div>
          <div className="mt-3 overflow-x-auto">
            <table className="min-w-full text-xs">
              <thead>
                <tr className="border-b text-gray-500">
                  <th className="py-1 text-left">Week</th><th className="py-1 text-right">Games</th><th className="py-1 text-right">Fav won</th>
                  <th className="py-1 text-right">Implied</th><th className="py-1 text-right">Difference</th><th className="py-1 text-right">Fav covered</th>
                  <th className="py-1 text-right">Overs</th><th className="py-1 text-right">By 7 or less</th>
                </tr>
              </thead>
              <tbody>
                {dataset.weekly.map((w) => (
                  <tr key={w.Week} className="border-b border-gray-50 tabular-nums">
                    <td className="py-1">{w.Week}</td><td className="py-1 text-right">{w.Games}</td>
                    <td className="py-1 text-right">{pct(w["Favorites won (%)"])}</td><td className="py-1 text-right">{pct(w["Price implied (%)"])}</td>
                    <td className={`py-1 text-right ${tone(w["Difference (pts)"])}`}>{w["Difference (pts)"] == null ? "—" : `${signed(w["Difference (pts)"], 1)} pts`}</td>
                    <td className="py-1 text-right">{pct(w["Favorites covered (%)"])}</td><td className="py-1 text-right">{pct(w["Overs (%)"])}</td>
                    <td className="py-1 text-right">{pct(w["Decided by 7 or less (%)"])}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      )}

      <section className="rounded-lg border bg-card p-4">
        <div className="mb-3 flex flex-wrap items-center justify-between gap-2">
          <h2 className="text-sm font-semibold">Results</h2>
          <label className="text-xs text-gray-600">
            Pattern{" "}
            <select className="rounded border px-2 py-1 text-xs" value={patternFilter} onChange={(e) => setPatternFilter(e.target.value)}>
              <option value="all">All</option>
              {codes.map((c) => <option key={c} value={c}>{c}</option>)}
            </select>
          </label>
        </div>
        <div className="overflow-x-auto">
          <table className="min-w-full text-xs">
            <thead>
              <tr className="border-b text-left text-gray-500">
                <th className="py-1 pr-3">Date</th><th className="py-1 pr-3">Game</th><th className="py-1 pr-3">#</th><th className="py-1 pr-3">Pick</th>
                <th className="py-1 pr-3 text-right">Price</th><th className="py-1 pr-3 text-right">Final</th><th className="py-1 pr-3">Result</th><th className="py-1 text-right">Units</th>
              </tr>
            </thead>
            <tbody>
              {results.map((r, i) => (
                <tr key={`${r.Date}-${r.Game}-${r.Pattern}-${i}`} className="border-b border-gray-50">
                  <td className="py-1 pr-3 whitespace-nowrap">{shortDay(r.Date)}</td><td className="py-1 pr-3">{r.Game}</td>
                  <td className="py-1 pr-3">{r.Pattern}</td><td className="py-1 pr-3">{r.Pick}</td>
                  <td className="py-1 pr-3 text-right tabular-nums">{r.Price}</td><td className="py-1 pr-3 text-right tabular-nums">{r.Final}</td>
                  <td className="py-1 pr-3"><Pill result={r.Result} /></td>
                  <td className={`py-1 text-right tabular-nums ${tone(r.Units)}`}>{signed(r.Units)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      {dataset.frozenVsClose.length > 0 && (
        <section className="rounded-lg border bg-card p-4">
          <h2 className="text-sm font-semibold">Frozen line vs close</h2>
          <p className="mb-3 text-xs text-gray-500">Lines are the home spread or the total. Positive points vs close means the frozen line was better for the pick.</p>
          <div className="overflow-x-auto">
            <table className="min-w-full text-xs">
              <thead>
                <tr className="border-b text-left text-gray-500">
                  <th className="py-1 pr-3">Game</th><th className="py-1 pr-3">#</th><th className="py-1 pr-3">Pick</th>
                  <th className="py-1 pr-3 text-right">Frozen</th><th className="py-1 pr-3 text-right">Close</th><th className="py-1 pr-3 text-right">Pts vs close</th>
                  <th className="py-1 pr-3">At close</th><th className="py-1 pr-3">At frozen</th><th className="py-1 text-right">Best retail units</th>
                </tr>
              </thead>
              <tbody>
                {dataset.frozenVsClose.map((r, i) => (
                  <tr key={`${r.Game}-${r.Pattern}-${i}`} className="border-b border-gray-50">
                    <td className="py-1 pr-3">{r.Game}</td><td className="py-1 pr-3">{r.Pattern}</td><td className="py-1 pr-3">{r.Pick}</td>
                    <td className="py-1 pr-3 text-right tabular-nums">{r["Frozen home spread or total"] ?? "—"}</td>
                    <td className="py-1 pr-3 text-right tabular-nums">{r["Closing home spread or total"] ?? "—"}</td>
                    <td className={`py-1 pr-3 text-right tabular-nums ${tone(r["Points vs close"])}`}>{signed(r["Points vs close"], 1)}</td>
                    <td className="py-1 pr-3"><Pill result={r["Result at close"]} /></td><td className="py-1 pr-3"><Pill result={r["Result at frozen line"]} /></td>
                    <td className={`py-1 text-right tabular-nums ${tone(r["Units at best retail"])}`}>{signed(r["Units at best retail"])}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      )}

      <footer className="border-t pt-3 text-xs text-gray-500">
        One unit per pick at the verified closing consensus: moneylines at the closing price, spreads and totals at -110. {dataset.studyVersion}
      </footer>
    </main>
  );
}
