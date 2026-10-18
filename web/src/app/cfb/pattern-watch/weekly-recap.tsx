"use client";

import { useMemo, useState } from "react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  LabelList,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

/**
 * Weekly recap of the season at the closing line: what happened each CFBD week.
 * Rows come pre-computed from model/cfb_weekly_recap.py (build_tables); this file
 * only arranges them. Descriptive, with sample sizes visible: not a study pattern
 * and not a betting edge.
 */

export type RecapWeekRow = {
  Week: number;
  Games: number;
  "Fav SU record": string;
  "Fav SU (%)": number | null;
  "Implied (%)": number | null;
  "Fav ATS record": string;
  "Fav ATS (%)": number | null;
  "Overs record": string;
  "Overs (%)": number | null;
  Upsets: number;
  "FBS vs FCS games": number;
  "FCS upsets": number;
  "Priced games": number;
  "Biggest upset": string | null;
};

export type RecapUpsetRow = {
  Week: number;
  Date: string | null;
  Winner: string;
  Loser: string;
  "Winner venue": string;
  Final: string;
  "Winner moneyline": number;
  "Winner price": string;
  "Winner implied (%)": number;
  "Winner spread": number | null;
  Matchup: string;
  matchup_id?: number | null;
};

export type RecapHookRow = {
  Week: number;
  Date: string | null;
  Game: string;
  Matchup: string;
  matchup_id?: number | null;
  Bet: "Spread" | "Total" | string;
  Line: string;
  Result: string;
  Miss: number;
  "Went to": string;
};

export type RecapTotalMissRow = {
  Week: number;
  Date: string | null;
  Game: string;
  "Closing total": number;
  "Actual total": number;
  Miss: number;
  Direction: string;
  Matchup: string;
  matchup_id?: number | null;
  Rank: number;
};

const BLUE = "#2a78d6";
const ORANGE = "#eb6834";

const pct = (v: number | null | undefined) => (v == null ? "—" : `${v.toFixed(1)}%`);
const signed = (v: number | null | undefined, digits = 1) =>
  v == null ? "—" : `${v > 0 ? "+" : ""}${v.toFixed(digits)}`;
const tone = (v: number | null | undefined) => (v == null || v === 0 ? "" : v > 0 ? "text-emerald-700" : "text-rose-700");
const spreadText = (v: number | null | undefined) => {
  if (v == null) return "—";
  if (v === 0) return "PK";
  const s = signed(v);
  return s.endsWith(".0") ? s.slice(0, -2) : s;
};
const day = (d: string | null) => {
  if (!d) return "—";
  const [y, m, dd] = d.split("-").map(Number);
  return new Date(Date.UTC(y, m - 1, dd)).toLocaleDateString("en-US", { month: "short", day: "numeric", timeZone: "UTC" });
};
const fcsTag = (m: string) =>
  m === "FBS vs FCS" ? <span className="ml-1 rounded bg-orange-50 px-1 text-[10px] text-orange-700">FCS</span> : null;

function Tile({ label, value, sub, valueTone = "" }: { label: string; value: string; sub: string; valueTone?: string }) {
  return (
    <div className="rounded-lg border p-3">
      <div className="text-[11px] uppercase tracking-wide text-gray-500">{label}</div>
      <div className={`mt-1 text-xl font-semibold tabular-nums ${valueTone}`}>{value}</div>
      <div className="text-xs tabular-nums text-gray-500">{sub}</div>
    </div>
  );
}

function UpsetsPerWeek({ weeks, selected, onSelect }: { weeks: RecapWeekRow[]; selected: number; onSelect: (w: number) => void }) {
  const data = weeks.map((w) => ({
    name: `Wk ${w.Week}`,
    week: w.Week,
    fbs: w.Upsets,
    fcs: w["FCS upsets"],
    total: w.Upsets + w["FCS upsets"],
    priced: w["Priced games"],
  }));
  return (
    <div>
      <div className="mb-1 flex flex-wrap items-center gap-x-3 text-xs font-medium text-gray-600">
        <span>Upsets per week</span>
        <span className="flex items-center gap-1 font-normal"><span className="h-2 w-2 rounded-sm" style={{ background: BLUE }} />FBS vs FBS</span>
        <span className="flex items-center gap-1 font-normal"><span className="h-2 w-2 rounded-sm" style={{ background: ORANGE }} />FCS beat FBS</span>
        <span className="font-normal text-gray-400">Click a bar to open that week.</span>
      </div>
      <div className="h-48">
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={data} margin={{ top: 18, right: 8, bottom: 4, left: 0 }}>
            <CartesianGrid stroke="#ecebe7" vertical={false} />
            <XAxis dataKey="name" tick={{ fontSize: 10 }} tickLine={false} interval={0} />
            <YAxis allowDecimals={false} tick={{ fontSize: 11 }} tickLine={false} axisLine={false} width={32} />
            <Tooltip
              formatter={(v, n) => [Number(v), n === "fbs" ? "FBS vs FBS" : "FCS beat FBS"]}
              labelFormatter={(l, items) => {
                const p = items?.[0]?.payload as { total?: number; priced?: number } | undefined;
                return `${l}: ${p?.total ?? 0} upsets of ${p?.priced ?? 0} priced games`;
              }}
            />
            <Bar dataKey="fbs" stackId="u" fill={BLUE} isAnimationActive={false} onClick={(d) => onSelect((d as unknown as { week: number }).week)}>
              {data.map((d) => <Cell key={d.week} cursor="pointer" fillOpacity={d.week === selected ? 1 : 0.55} />)}
            </Bar>
            <Bar dataKey="fcs" stackId="u" fill={ORANGE} isAnimationActive={false} onClick={(d) => onSelect((d as unknown as { week: number }).week)}>
              {data.map((d) => <Cell key={d.week} cursor="pointer" fillOpacity={d.week === selected ? 1 : 0.55} />)}
              <LabelList dataKey="total" position="top" style={{ fontSize: 10 }} />
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}

function Empty({ children }: { children: React.ReactNode }) {
  return <p className="text-xs text-gray-500">{children}</p>;
}

export default function WeeklyRecap({
  weeks,
  upsets,
  hooks,
  totalMisses,
}: {
  weeks: RecapWeekRow[];
  upsets: RecapUpsetRow[];
  hooks: RecapHookRow[];
  totalMisses: RecapTotalMissRow[];
}) {
  const sortedWeeks = useMemo(() => [...weeks].sort((a, b) => a.Week - b.Week), [weeks]);
  const latest = sortedWeeks.length ? sortedWeeks[sortedWeeks.length - 1].Week : null;
  const [picked, setPicked] = useState<number | null>(null);
  const selected = picked ?? latest;

  if (selected == null) {
    return (
      <section className="rounded-lg border bg-card p-4">
        <h2 className="text-sm font-semibold">Weekly recap</h2>
        <Empty>No completed weeks with closing lines yet.</Empty>
      </section>
    );
  }

  const wk = sortedWeeks.find((w) => w.Week === selected);
  const wkUpsets = upsets.filter((u) => u.Week === selected);
  const wkHooks = hooks.filter((h) => h.Week === selected);
  const wkMisses = totalMisses.filter((m) => m.Week === selected).sort((a, b) => a.Rank - b.Rank);
  const suGap = wk && wk["Fav SU (%)"] != null && wk["Implied (%)"] != null ? wk["Fav SU (%)"] - wk["Implied (%)"] : null;
  const hookCounts = wkHooks.reduce(
    (acc, h) => {
      if (h["Went to"] === "push") acc.push += 1;
      else acc.decided += 1;
      return acc;
    },
    { decided: 0, push: 0 },
  );

  return (
    <section className="space-y-4 rounded-lg border bg-card p-4">
      <div>
        <h2 className="text-sm font-semibold">Weekly recap</h2>
        <p className="text-xs text-gray-500">
          What happened each week at the closing line, regular season. Rates use FBS-vs-FBS games; FBS-vs-FCS games are listed
          and tagged but kept out of the rates. Descriptive only: one week is a small sample, and none of this is a pattern or an edge.
        </p>
      </div>

      <div className="flex flex-wrap items-center gap-1">
        <span className="mr-1 text-xs text-gray-500">Week</span>
        {sortedWeeks.map((w) => (
          <button
            key={w.Week}
            onClick={() => setPicked(w.Week)}
            className={`rounded border px-2.5 py-1 text-xs tabular-nums ${
              w.Week === selected ? "border-slate-800 bg-slate-800 text-white" : "bg-white text-gray-700"
            }`}
          >
            {w.Week}
          </button>
        ))}
      </div>

      {wk && (
        <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
          <Tile
            label="Favorites straight up"
            value={pct(wk["Fav SU (%)"])}
            valueTone={tone(suGap)}
            sub={`${wk["Fav SU record"]} · implied ${pct(wk["Implied (%)"])}${suGap == null ? "" : ` (${signed(suGap)} pts)`}`}
          />
          <Tile label="Favorites ATS" value={pct(wk["Fav ATS (%)"])} sub={`${wk["Fav ATS record"]} against the closing spread`} />
          <Tile label="Overs" value={pct(wk["Overs (%)"])} sub={`${wk["Overs record"]} against the closing total`} />
          <Tile
            label="Upsets"
            value={String(wk.Upsets + wk["FCS upsets"])}
            sub={`of ${wk["Priced games"]} priced games${wk["FCS upsets"] ? ` · ${wk["FCS upsets"]} by FCS teams` : ""}`}
          />
        </div>
      )}
      {wk && (
        <p className="text-[11px] text-gray-500">
          {wk.Games} FBS-vs-FBS games{wk["FBS vs FCS games"] ? ` and ${wk["FBS vs FCS games"]} FBS-vs-FCS games` : ""} this week.
          {wk["Biggest upset"] ? ` Biggest upset: ${wk["Biggest upset"]}.` : " No outright upsets."}
        </p>
      )}

      {sortedWeeks.length > 0 && <UpsetsPerWeek weeks={sortedWeeks} selected={selected} onSelect={setPicked} />}

      <div className="space-y-2">
        <h3 className="text-xs font-semibold text-gray-700">Upsets by price, week {selected}</h3>
        {wkUpsets.length === 0 ? (
          <Empty>No moneyline underdog won outright this week.</Empty>
        ) : (
          <div className="overflow-x-auto">
            <table className="min-w-full text-xs">
              <thead>
                <tr className="border-b text-gray-500">
                  <th className="py-1 pr-3 text-left">Date</th>
                  <th className="py-1 pr-3 text-left">Winner</th>
                  <th className="py-1 pr-3 text-left">Beat</th>
                  <th className="py-1 pr-3 text-right">Final</th>
                  <th className="py-1 pr-3 text-right">Price</th>
                  <th className="py-1 pr-3 text-right">Implied</th>
                  <th className="py-1 pr-3 text-right">Spread</th>
                  <th className="py-1 text-left">Where</th>
                </tr>
              </thead>
              <tbody>
                {wkUpsets.map((u, i) => (
                  <tr key={`${u.matchup_id ?? i}-${u.Winner}`} className="border-b border-gray-50 tabular-nums">
                    <td className="py-1 pr-3 text-gray-500">{day(u.Date)}</td>
                    <td className="py-1 pr-3 font-medium">{u.Winner}{fcsTag(u.Matchup)}</td>
                    <td className="py-1 pr-3">{u.Loser}</td>
                    <td className="py-1 pr-3 text-right">{u.Final}</td>
                    <td className="py-1 pr-3 text-right text-emerald-700">{u["Winner price"]}</td>
                    <td className="py-1 pr-3 text-right">
                      <span className="inline-flex items-center gap-1">
                        <span className="inline-block h-1.5 rounded-sm" style={{ width: `${Math.max(2, u["Winner implied (%)"] * 0.6)}px`, background: BLUE }} />
                        {pct(u["Winner implied (%)"])}
                      </span>
                    </td>
                    <td className="py-1 pr-3 text-right">{spreadText(u["Winner spread"])}</td>
                    <td className="py-1 text-gray-500">{u["Winner venue"]}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>

      <div className="space-y-2">
        <h3 className="text-xs font-semibold text-gray-700">
          Decided by the hook, week {selected}
          {wkHooks.length > 0 && (
            <span className="ml-2 font-normal text-gray-500">{hookCounts.decided} by half a point, {hookCounts.push} pushes</span>
          )}
        </h3>
        <p className="text-[11px] text-gray-500">
          The final landed within half a point of the closing spread or total. A game whose spread and total both came down to the hook
          appears twice.
        </p>
        {wkHooks.length === 0 ? (
          <Empty>No spread or total landed within half a point of the close this week.</Empty>
        ) : (
          <div className="overflow-x-auto">
            <table className="min-w-full text-xs">
              <thead>
                <tr className="border-b text-gray-500">
                  <th className="py-1 pr-3 text-left">Date</th>
                  <th className="py-1 pr-3 text-left">Game</th>
                  <th className="py-1 pr-3 text-left">Bet</th>
                  <th className="py-1 pr-3 text-left">Closing line</th>
                  <th className="py-1 pr-3 text-left">Result</th>
                  <th className="py-1 pr-3 text-right">Margin</th>
                  <th className="py-1 text-left">Went to</th>
                </tr>
              </thead>
              <tbody>
                {wkHooks.map((h, i) => (
                  <tr key={`${h.matchup_id ?? i}-${h.Bet}`} className="border-b border-gray-50 tabular-nums">
                    <td className="py-1 pr-3 text-gray-500">{day(h.Date)}</td>
                    <td className="py-1 pr-3">{h.Game}{fcsTag(h.Matchup)}</td>
                    <td className="py-1 pr-3">{h.Bet}</td>
                    <td className="py-1 pr-3">{h.Line}</td>
                    <td className="py-1 pr-3">{h.Result}</td>
                    <td className="py-1 pr-3 text-right">{h.Miss === 0 ? "0" : signed(h.Miss)}</td>
                    <td className={`py-1 ${h["Went to"] === "push" ? "text-gray-500" : "font-medium"}`}>{h["Went to"]}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>

      <div className="space-y-2">
        <h3 className="text-xs font-semibold text-gray-700">Totals that missed most, week {selected}</h3>
        {wkMisses.length === 0 ? (
          <Empty>No closing totals this week.</Empty>
        ) : (
          <div className="overflow-x-auto">
            <table className="min-w-full text-xs">
              <thead>
                <tr className="border-b text-gray-500">
                  <th className="py-1 pr-3 text-right">#</th>
                  <th className="py-1 pr-3 text-left">Date</th>
                  <th className="py-1 pr-3 text-left">Game</th>
                  <th className="py-1 pr-3 text-right">Closing total</th>
                  <th className="py-1 pr-3 text-right">Actual</th>
                  <th className="py-1 pr-3 text-right">Miss</th>
                  <th className="py-1 text-left">Side</th>
                </tr>
              </thead>
              <tbody>
                {wkMisses.map((m, i) => (
                  <tr key={`${m.matchup_id ?? i}-${m.Rank}`} className="border-b border-gray-50 tabular-nums">
                    <td className="py-1 pr-3 text-right text-gray-500">{m.Rank}</td>
                    <td className="py-1 pr-3 text-gray-500">{day(m.Date)}</td>
                    <td className="py-1 pr-3">{m.Game}{fcsTag(m.Matchup)}</td>
                    <td className="py-1 pr-3 text-right">{m["Closing total"]}</td>
                    <td className="py-1 pr-3 text-right">{m["Actual total"]}</td>
                    <td className={`py-1 pr-3 text-right ${tone(m.Miss)}`}>{signed(m.Miss)}</td>
                    <td className="py-1">{m.Direction}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </section>
  );
}
