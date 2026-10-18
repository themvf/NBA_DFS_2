"use client";

import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Legend,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

/**
 * Key numbers: how often college football games land on each final margin,
 * with the NFL beside it where its scores exist, and how 2026 games landed
 * against their closing spread and total. Rows come pre-computed from
 * model/cfb_key_numbers.py (build_tables); this file only arranges them.
 * Descriptive, not a betting signal.
 */

export type KeyMarginRow = {
  Margin: string; // "1".."40", "41+"
  "CFB games": number | null;
  "CFB share (%)": number | null;
  "CFB cumulative (%)": number | null;
  "NFL games": number | null;
  "NFL share (%)": number | null;
  "NFL cumulative (%)": number | null;
};

export type KeyNumberRow = {
  Margin: number;
  "CFB rank": number | null;
  "CFB games": number;
  "CFB share (%)": number | null;
  "NFL rank": number | null;
  "NFL games": number | null;
  "NFL share (%)": number | null;
  "CFB vs NFL": number | null;
};

export type SpreadLandingRow = {
  Measure: string;
  Line: string;
  Games: number;
  Count: number;
  "Share (%)": number | null;
};

export type TotalLandingRow = {
  "Miss (points)": string;
  Games: number;
  "Share (%)": number | null;
  "Cumulative (%)": number | null;
};

const CFB_COLOR = "#2a78d6";
const NFL_COLOR = "#eb6834";
const KEY_COLOR = "#1baf7a";
const KEY_MARGINS = new Set([3, 7, 10, 14, 17, 21]);
const CHART_MAX = 30;
// Below this many games a share says very little; shown, but muted.
const THIN = 20;

const pct = (v: number | null | undefined, d = 1) => (v == null ? "—" : `${v.toFixed(d)}%`);
const num = (v: number | null | undefined) => (v == null ? "—" : v.toLocaleString());

function Card({ title, note, children }: { title: string; note?: string; children: React.ReactNode }) {
  return (
    <div className="rounded-lg border bg-card p-4">
      <h3 className="text-sm font-semibold">{title}</h3>
      {note && <p className="mb-3 mt-1 text-xs text-gray-500">{note}</p>}
      {children}
    </div>
  );
}

function MarginChart({ margins }: { margins: KeyMarginRow[] }) {
  const data = margins
    .filter((r) => /^\d+$/.test(r.Margin) && Number(r.Margin) <= CHART_MAX)
    .map((r) => ({ margin: Number(r.Margin), cfb: r["CFB share (%)"], nfl: r["NFL share (%)"] }));
  const hasNfl = margins.some((r) => (r["NFL games"] ?? 0) > 0);
  const cfbGames = margins.reduce((s, r) => s + (r["CFB games"] ?? 0), 0);
  const nflGames = margins.reduce((s, r) => s + (r["NFL games"] ?? 0), 0);
  return (
    <Card
      title="Share of games by final margin"
      note={`College: ${cfbGames.toLocaleString()} FBS-vs-FBS regular-season games, overtime included. Key numbers in green.${
        hasNfl ? ` NFL: ${nflGames.toLocaleString()} regular-season games (fewer seasons than college).` : ""
      }`}
    >
      <div className="h-72 w-full">
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={data} margin={{ top: 8, right: 8, bottom: 4, left: -12 }} barGap={1}>
            <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#e4e3df" />
            <XAxis dataKey="margin" tick={{ fontSize: 10 }} interval={0} />
            <YAxis tick={{ fontSize: 10 }} unit="%" />
            <Tooltip
              formatter={(v, name) => [pct(typeof v === "number" ? v : null, 2), name === "cfb" ? "College" : "NFL"]}
              labelFormatter={(m) => `Won by ${m}`}
            />
            {hasNfl && (
              <Legend
                wrapperStyle={{ fontSize: 11 }}
                formatter={(v) => (v === "cfb" ? "College" : "NFL")}
              />
            )}
            <Bar dataKey="cfb" fill={CFB_COLOR} isAnimationActive={false}>
              {data.map((d) => (
                <Cell key={d.margin} fill={KEY_MARGINS.has(d.margin) ? KEY_COLOR : CFB_COLOR} />
              ))}
            </Bar>
            {hasNfl && <Bar dataKey="nfl" fill={NFL_COLOR} fillOpacity={0.55} isAnimationActive={false} />}
          </BarChart>
        </ResponsiveContainer>
      </div>
    </Card>
  );
}

function KeyNumberTable({ rows }: { rows: KeyNumberRow[] }) {
  const hasNfl = rows.some((r) => r["NFL games"] != null);
  return (
    <Card
      title="The most common margins"
      note={
        hasNfl
          ? "Each league's most common margins, ranked by college share. Ratio above 1 means the margin is more common in college."
          : "Ranked by college share."
      }
    >
      <div className="overflow-x-auto">
        <table className="w-full min-w-[420px] text-xs tabular-nums">
          <thead>
            <tr className="border-b text-left text-gray-500">
              <th className="py-1 pr-3 font-medium">Margin</th>
              <th className="py-1 pr-3 text-right font-medium">College rank</th>
              <th className="py-1 pr-3 text-right font-medium">Games</th>
              <th className="py-1 pr-3 text-right font-medium">College share</th>
              {hasNfl && <th className="border-l py-1 pl-3 pr-3 text-right font-medium">NFL rank</th>}
              {hasNfl && <th className="py-1 pr-3 text-right font-medium">NFL share</th>}
              {hasNfl && <th className="py-1 text-right font-medium">College ÷ NFL</th>}
            </tr>
          </thead>
          <tbody>
            {rows.map((r) => (
              <tr key={r.Margin} className="border-b last:border-0">
                <td className={`py-1 pr-3 font-medium ${KEY_MARGINS.has(r.Margin) ? "text-emerald-700" : ""}`}>{r.Margin}</td>
                <td className="py-1 pr-3 text-right">{r["CFB rank"] ?? "—"}</td>
                <td className="py-1 pr-3 text-right">{num(r["CFB games"])}</td>
                <td className="py-1 pr-3 text-right">{pct(r["CFB share (%)"])}</td>
                {hasNfl && <td className="border-l py-1 pl-3 pr-3 text-right">{r["NFL rank"] ?? "—"}</td>}
                {hasNfl && <td className="py-1 pr-3 text-right">{pct(r["NFL share (%)"])}</td>}
                {hasNfl && (
                  <td
                    className={`py-1 text-right ${
                      r["CFB vs NFL"] == null ? "" : r["CFB vs NFL"] >= 1 ? "text-emerald-700" : "text-rose-700"
                    }`}
                  >
                    {r["CFB vs NFL"] == null ? "—" : `${r["CFB vs NFL"].toFixed(2)}×`}
                  </td>
                )}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </Card>
  );
}

function SpreadLandingTable({ rows }: { rows: SpreadLandingRow[] }) {
  return (
    <Card
      title="2026 games against the closing spread"
      note="2026 FBS-vs-FBS regular-season games with a closing spread. Push rates are at integer lines only; most lines here have few games."
    >
      <div className="overflow-x-auto">
        <table className="w-full min-w-[420px] text-xs tabular-nums">
          <thead>
            <tr className="border-b text-left text-gray-500">
              <th className="py-1 pr-3 font-medium">Measure</th>
              <th className="py-1 pr-3 font-medium">Line</th>
              <th className="py-1 pr-3 text-right font-medium">Games</th>
              <th className="py-1 pr-3 text-right font-medium">Count</th>
              <th className="py-1 text-right font-medium">Share</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((r) => (
              <tr key={`${r.Measure}|${r.Line}`} className={`border-b last:border-0 ${r.Games < THIN ? "text-gray-400" : ""}`}>
                <td className="py-1 pr-3">{r.Measure}</td>
                <td className="py-1 pr-3">{r.Line}</td>
                <td className="py-1 pr-3 text-right">{num(r.Games)}</td>
                <td className="py-1 pr-3 text-right">{num(r.Count)}</td>
                <td className="py-1 text-right">{pct(r["Share (%)"])}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <p className="mt-2 text-[11px] text-gray-500">Grey rows have fewer than {THIN} games.</p>
    </Card>
  );
}

function TotalLandingTable({ rows }: { rows: TotalLandingRow[] }) {
  const games = rows.reduce((s, r) => s + r.Games, 0);
  return (
    <Card
      title="2026 games against the closing total"
      note={`Combined score minus the closing total, ${games.toLocaleString()} games. Each bin includes its lower edge.`}
    >
      <div className="overflow-x-auto">
        <table className="w-full min-w-[320px] text-xs tabular-nums">
          <thead>
            <tr className="border-b text-left text-gray-500">
              <th className="py-1 pr-3 font-medium">Miss (points)</th>
              <th className="py-1 pr-3 text-right font-medium">Games</th>
              <th className="py-1 pr-3 text-right font-medium">Share</th>
              <th className="py-1 text-right font-medium">Cumulative</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((r) => (
              <tr key={r["Miss (points)"]} className="border-b last:border-0">
                <td className="py-1 pr-3">{r["Miss (points)"]}</td>
                <td className="py-1 pr-3 text-right">{num(r.Games)}</td>
                <td className="py-1 pr-3 text-right">{pct(r["Share (%)"])}</td>
                <td className="py-1 text-right">{pct(r["Cumulative (%)"])}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </Card>
  );
}

export default function KeyNumbers({
  margins,
  keyNumbers,
  spreadLanding,
  totalLanding,
}: {
  margins: KeyMarginRow[];
  keyNumbers: KeyNumberRow[];
  spreadLanding: SpreadLandingRow[];
  totalLanding?: TotalLandingRow[];
}) {
  if (!margins.length && !keyNumbers.length && !spreadLanding.length && !totalLanding?.length) {
    return (
      <div className="rounded-lg border bg-card p-4 text-xs text-gray-500">
        No key-number tables yet. Run model/cfb_key_numbers.py to build them.
      </div>
    );
  }
  return (
    <div className="space-y-4">
      {margins.length > 0 && <MarginChart margins={margins} />}
      <div className="grid gap-4 lg:grid-cols-2">
        {keyNumbers.length > 0 && <KeyNumberTable rows={keyNumbers} />}
        {spreadLanding.length > 0 && <SpreadLandingTable rows={spreadLanding} />}
      </div>
      {totalLanding && totalLanding.length > 0 && <TotalLandingTable rows={totalLanding} />}
    </div>
  );
}
