"use client";

import { Fragment, useState } from "react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  LabelList,
  Legend,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import type { MarketBreakdownRow } from "@/db/cfb-pattern-watch";

/**
 * Moneyline favorites and underdogs, from the simplest cut to the most detailed:
 *   1. every game   2. by spread   3. by total   4. spread and total together.
 * Rows come pre-computed from model/cfb_pattern_csv.py (market_breakdown); this
 * file only arranges them. Descriptive season-to-date numbers, not study patterns.
 */

const FAV = "#2a78d6";
const DOG = "#eb6834";
// Below this many games a cell is shown but muted: a few games say very little.
const THIN = 20;

const signed = (v: number | null | undefined, digits = 1) =>
  v == null ? "—" : `${v > 0 ? "+" : ""}${v.toFixed(digits)}`;
const pct = (v: number | null | undefined) => (v == null ? "—" : `${v.toFixed(1)}%`);
const roi = (v: number | null | undefined) => (v == null ? "—" : `${signed(v)}%`);
const tone = (v: number | null | undefined) => (v == null || v === 0 ? "" : v > 0 ? "text-emerald-700" : "text-rose-700");
// Pad both ends past zero so value labels on short or negative bars never sit on the axis labels.
const PADDED: [(m: number) => number, (m: number) => number] = [
  (m) => Math.floor(Math.min(0, m) * 1.25 - 1),
  (m) => Math.ceil(Math.max(0, m) * 1.25 + 1),
];
const shortBand = (s: string | null) => (s ?? "").replace(/^Spread /, "").replace(/^Total /, "");

function Step({ n, title, note, children }: { n: number; title: string; note: string; children: React.ReactNode }) {
  return (
    <div className="space-y-3 border-t pt-4 first:border-t-0 first:pt-0">
      <div className="flex items-baseline gap-2">
        <span className="flex h-5 w-5 shrink-0 items-center justify-center rounded-full bg-slate-800 text-[11px] font-semibold text-white">{n}</span>
        <div>
          <h3 className="text-sm font-semibold">{title}</h3>
          <p className="text-xs text-gray-500">{note}</p>
        </div>
      </div>
      {children}
    </div>
  );
}

function SideCard({ label, color, record, units, roiPct, price, sub }: {
  label: string; color: string; record: string; units: number | null | undefined; roiPct: number | null | undefined;
  price: string | null | undefined; sub: string;
}) {
  return (
    <div className="rounded-lg border p-4" style={{ borderLeft: `4px solid ${color}` }}>
      <div className="text-[11px] uppercase tracking-wide text-gray-500">{label}</div>
      <div className="mt-1 flex items-baseline gap-3">
        <span className={`text-2xl font-semibold tabular-nums ${tone(roiPct)}`}>{roi(roiPct)}</span>
        <span className="text-sm tabular-nums text-gray-700">{record}</span>
      </div>
      <div className="text-xs text-gray-500 tabular-nums">
        {signed(units, 2)} units · median price {price ?? "—"} · {sub}
      </div>
    </div>
  );
}

function RoiBars({ rows, label }: { rows: MarketBreakdownRow[]; label: (r: MarketBreakdownRow) => string }) {
  const data = rows.map((r) => ({
    name: label(r),
    games: r.Games,
    Favorites: r["ROI (%)"] ?? 0,
    Underdogs: r["Underdog ROI (%)"] ?? 0,
  }));
  return (
    <div className="h-64">
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={data} margin={{ top: 18, right: 8, bottom: 4, left: 0 }}>
          <CartesianGrid stroke="#ecebe7" vertical={false} />
          <XAxis dataKey="name" tick={{ fontSize: 10 }} tickLine={false} interval={0} />
          <YAxis domain={PADDED} tickFormatter={(v) => `${v}%`} tick={{ fontSize: 11 }} tickLine={false} axisLine={false} width={44} />
          <ReferenceLine y={0} stroke="#52514e" />
          <Tooltip
            formatter={(v) => roi(Number(v))}
            labelFormatter={(l, items) => `${l}: ${(items?.[0]?.payload as { games?: number } | undefined)?.games ?? 0} games`}
          />
          <Legend wrapperStyle={{ fontSize: 12 }} />
          <Bar dataKey="Favorites" fill={FAV} isAnimationActive={false}>
            <LabelList dataKey="Favorites" position="top" formatter={(v) => roi(Number(v))} style={{ fontSize: 10 }} />
          </Bar>
          <Bar dataKey="Underdogs" fill={DOG} isAnimationActive={false}>
            <LabelList dataKey="Underdogs" position="top" formatter={(v) => roi(Number(v))} style={{ fontSize: 10 }} />
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}

function WinVsImplied({ rows, label }: { rows: MarketBreakdownRow[]; label: (r: MarketBreakdownRow) => string }) {
  const data = rows.map((r) => ({
    name: label(r),
    gap: r["Difference (pts)"] ?? 0,
    won: r["Favorites won (%)"],
    implied: r["Price implied (%)"],
  }));
  return (
    <div className="h-48">
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={data} margin={{ top: 18, right: 8, bottom: 4, left: 0 }}>
          <CartesianGrid stroke="#ecebe7" vertical={false} />
          <XAxis dataKey="name" tick={{ fontSize: 10 }} tickLine={false} interval={0} />
          <YAxis domain={PADDED} tickFormatter={(v) => `${v}`} tick={{ fontSize: 11 }} tickLine={false} axisLine={false} width={44} />
          <ReferenceLine y={0} stroke="#52514e" />
          <Tooltip
            formatter={(v, _n, item) => {
              const p = item?.payload as { won: number | null; implied: number | null } | undefined;
              return [`${signed(Number(v))} pts (won ${pct(p?.won)} vs implied ${pct(p?.implied)})`, "Favorites vs price"];
            }}
          />
          <Bar dataKey="gap" isAnimationActive={false}>
            {data.map((d) => <Cell key={d.name} fill={d.gap >= 0 ? "#1baf7a" : "#e34948"} />)}
            <LabelList dataKey="gap" position="top" formatter={(v) => `${signed(Number(v))} pts`} style={{ fontSize: 10 }} />
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}

function BandTable({ rows, first }: { rows: MarketBreakdownRow[]; first: string }) {
  return (
    <div className="overflow-x-auto">
      <table className="min-w-full text-xs">
        <thead>
          <tr className="border-b text-gray-500">
            <th className="py-1 pr-3 text-left">{first}</th><th className="py-1 pr-3 text-right">Games</th>
            <th className="py-1 pr-3 text-right">Fav record</th><th className="py-1 pr-3 text-right">Won</th>
            <th className="py-1 pr-3 text-right">Implied</th><th className="py-1 pr-3 text-right">Difference</th>
            <th className="py-1 pr-3 text-right">Fav ROI</th><th className="py-1 pr-3 text-right">Fav median</th>
            <th className="border-l py-1 pl-3 pr-3 text-right">Dog record</th><th className="py-1 pr-3 text-right">Dog units</th>
            <th className="py-1 pr-3 text-right">Dog ROI</th><th className="py-1 text-right">Dog median</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((m) => (
            <tr key={m.Split} className={`border-b border-gray-50 tabular-nums ${m.Games < THIN ? "text-gray-400" : ""}`}>
              <td className="py-1 pr-3">{m.Split}</td>
              <td className="py-1 pr-3 text-right">{m.Games}</td>
              <td className="py-1 pr-3 text-right">{m.Record}</td>
              <td className="py-1 pr-3 text-right">{pct(m["Favorites won (%)"])}</td>
              <td className="py-1 pr-3 text-right">{pct(m["Price implied (%)"])}</td>
              <td className={`py-1 pr-3 text-right ${tone(m["Difference (pts)"])}`}>{m["Difference (pts)"] == null ? "—" : `${signed(m["Difference (pts)"])} pts`}</td>
              <td className={`py-1 pr-3 text-right ${tone(m["ROI (%)"])}`}>{roi(m["ROI (%)"])}</td>
              <td className="py-1 pr-3 text-right">{m["Median price"] ?? "—"}</td>
              <td className="border-l py-1 pl-3 pr-3 text-right">{m["Underdog record"] ?? "—"}</td>
              <td className={`py-1 pr-3 text-right ${tone(m["Underdog units"])}`}>{signed(m["Underdog units"], 2)}</td>
              <td className={`py-1 pr-3 text-right ${tone(m["Underdog ROI (%)"])}`}>{roi(m["Underdog ROI (%)"])}</td>
              <td className="py-1 text-right">{m["Underdog median price"] ?? "—"}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

// Diverging fill for an ROI cell: green above zero, red below, stronger with size, capped at +-40%.
function heat(v: number | null | undefined, games: number): React.CSSProperties {
  if (v == null || games === 0) return { background: "#f6f6f4" };
  const a = Math.min(Math.abs(v), 40) / 40;
  const alpha = (games < THIN ? 0.35 : 1) * (0.12 + 0.6 * a);
  return { background: v >= 0 ? `rgba(27,175,122,${alpha})` : `rgba(227,73,72,${alpha})` };
}

function Grid({ rows }: { rows: MarketBreakdownRow[] }) {
  const [side, setSide] = useState<"fav" | "dog">("fav");
  const spreads = [...new Set(rows.map((r) => r.Spread ?? ""))];
  const totals = [...new Set(rows.map((r) => r.Total ?? ""))];
  const cell = (s: string, t: string) => rows.find((r) => r.Spread === s && r.Total === t);
  return (
    <div className="space-y-2">
      <div className="flex gap-1 text-xs">
        {(["fav", "dog"] as const).map((k) => (
          <button
            key={k}
            onClick={() => setSide(k)}
            className={`rounded border px-2 py-1 ${side === k ? "border-slate-800 bg-slate-800 text-white" : "bg-white text-gray-700"}`}
          >
            {k === "fav" ? "Favorites" : "Underdogs"}
          </button>
        ))}
      </div>
      <div className="overflow-x-auto">
        <table className="w-full min-w-[520px] border-separate border-spacing-1 text-xs">
          <thead>
            <tr>
              <th className="p-1 text-left font-normal text-gray-500">Spread ↓ · Total →</th>
              {totals.map((t) => <th key={t} className="p-1 text-center font-medium">{shortBand(t)}</th>)}
            </tr>
          </thead>
          <tbody>
            {spreads.map((s) => (
              <tr key={s}>
                <th className="p-1 text-left font-medium">{shortBand(s)}</th>
                {totals.map((t) => {
                  const c = cell(s, t);
                  const games = c?.Games ?? 0;
                  const v = side === "fav" ? c?.["ROI (%)"] : c?.["Underdog ROI (%)"];
                  const rec = side === "fav" ? c?.Record : c?.["Underdog record"];
                  return (
                    <td key={t} className="rounded p-2 text-center tabular-nums" style={heat(v, games)}>
                      {games === 0 ? (
                        <span className="text-gray-400">no games</span>
                      ) : (
                        <Fragment>
                          <div className={`text-sm font-semibold ${games < THIN ? "text-gray-500" : ""}`}>{roi(v)}</div>
                          <div className="text-[10px] text-gray-600">{rec} · {games} g</div>
                        </Fragment>
                      )}
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <p className="text-[11px] text-gray-500">
        Each cell is one unit per game on the {side === "fav" ? "favorite" : "underdog"} at its closing moneyline. Faded cells
        have fewer than {THIN} games.
      </p>
    </div>
  );
}

export default function MarketBreakdown({ rows }: { rows: MarketBreakdownRow[] }) {
  const [showCells, setShowCells] = useState(false);
  const overall = rows.find((r) => r.Level === 1);
  const bySpread = rows.filter((r) => r.Level === 2);
  const byTotal = rows.filter((r) => r.Level === 3);
  const cross = rows.filter((r) => r.Level === 4);
  if (!overall) return null;
  const noTotal = overall.Games - byTotal.reduce((s, r) => s + r.Games, 0);

  return (
    <section className="space-y-4 rounded-lg border bg-card p-4">
      <div>
        <h2 className="text-sm font-semibold">Moneyline favorites and underdogs, season to date</h2>
        <p className="text-xs text-gray-500">
          All FBS-vs-FBS regular-season games with a closing spread and moneylines. One unit on each side at its closing moneyline.
          From the whole slate down to spread and total together. Descriptive only: not study patterns and not graded for a verdict.
        </p>
      </div>

      <Step n={1} title="Every game" note={`${overall.Games} games. The starting point every cut below adds up to.`}>
        <div className="grid gap-3 sm:grid-cols-2">
          <SideCard label="Favorites" color={FAV} record={overall.Record} units={overall.Units} roiPct={overall["ROI (%)"]}
            price={overall["Median price"]}
            sub={`won ${pct(overall["Favorites won (%)"])} vs ${pct(overall["Price implied (%)"])} implied`} />
          <SideCard label="Underdogs" color={DOG} record={overall["Underdog record"] ?? "—"} units={overall["Underdog units"]}
            roiPct={overall["Underdog ROI (%)"]} price={overall["Underdog median price"]}
            sub={`won ${pct(overall["Favorites won (%)"] == null ? null : 100 - overall["Favorites won (%)"]!)}`} />
        </div>
      </Step>

      <Step n={2} title="By spread" note="Split by the size of the closing spread. Every game falls in exactly one band.">
        <RoiBars rows={bySpread} label={(r) => shortBand(r.Spread)} />
        <div>
          <div className="mb-1 text-xs font-medium text-gray-600">Favorites&apos; win rate minus price-implied, percentage points</div>
          <WinVsImplied rows={bySpread} label={(r) => shortBand(r.Spread)} />
        </div>
        <BandTable rows={bySpread} first="Spread" />
      </Step>

      <Step n={3} title="By total" note={`Split by the closing over/under.${noTotal > 0 ? ` ${noTotal} game${noTotal === 1 ? "" : "s"} without a total ${noTotal === 1 ? "is" : "are"} left out.` : ""}`}>
        <RoiBars rows={byTotal} label={(r) => shortBand(r.Total)} />
        <div>
          <div className="mb-1 text-xs font-medium text-gray-600">Favorites&apos; win rate minus price-implied, percentage points</div>
          <WinVsImplied rows={byTotal} label={(r) => shortBand(r.Total)} />
        </div>
        <BandTable rows={byTotal} first="Total" />
      </Step>

      <Step n={4} title="Spread and total together" note="Every spread band crossed with every total band. The most detailed view, and the smallest samples.">
        <Grid rows={cross} />
        <button className="text-xs text-blue-700 underline" onClick={() => setShowCells((v) => !v)}>
          {showCells ? "Hide" : "Show"} all {cross.length} cells as a table
        </button>
        {showCells && <BandTable rows={cross} first="Spread and total" />}
      </Step>
    </section>
  );
}
