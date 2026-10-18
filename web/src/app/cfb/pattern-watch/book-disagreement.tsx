"use client";

import { useMemo, useState } from "react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Legend,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

/**
 * Where the sportsbooks disagreed and how the lines moved, 2026 CFB.
 * Rows come pre-computed from model/cfb_book_disagreement.py (build_tables);
 * this file only arranges them. Descriptive, not a betting signal.
 */

export type BookGapRow = {
  Week: number | null;
  Matchup: string;
  Books: number;
  "Hours before kickoff": number | null;
  "Spread range (pts)": number | null;
  "Home spread low": number | null;
  "Low spread books": string | null;
  "Home spread high": number | null;
  "High spread books": string | null;
  "Total range (pts)": number | null;
  "Total low": number | null;
  "Low total books": string | null;
  "Total high": number | null;
  "High total books": string | null;
  "Home win prob range (pts)": number | null;
  "Home win prob low (%)": number | null;
  "Low prob books": string | null;
  "Home win prob high (%)": number | null;
  "High prob books": string | null;
};

export type LineMoveRow = {
  Week: number | null;
  Matchup: string;
  "Favorite (close)": string | null;
  "Open hours before kickoff": number | null;
  Captures: number;
  "Home spread open": number | null;
  "Home spread close": number | null;
  "Spread move (pts)": number | null;
  "Fav spread open": number | null;
  "Fav spread close": number | null;
  "Moved toward": string | null;
  "Total open": number | null;
  "Total close": number | null;
  "Total move (pts)": number | null;
  "Fav win prob open (%)": number | null;
  "Fav win prob close (%)": number | null;
  "Fav prob move (pts)": number | null;
  Final: string | null;
  "Home margin": number | null;
  "Margin error at open": number | null;
  "Margin error at close": number | null;
  "Spread closer": "Close" | "Open" | "Tie" | null;
  "Total points": number | null;
  "Total error at open": number | null;
  "Total error at close": number | null;
  "Total closer": "Close" | "Open" | "Tie" | null;
};

export type MoveSummaryRow = {
  Scope: string;
  Week: number | null;
  Games: number;
  "Games with open": number;
  "Median spread move (pts)": number | null;
  "Spread moved 2+ (%)": number | null;
  "Spread decided games": number;
  "Spread ties": number;
  "Close beat open, spread (%)": number | null;
  "Median total move (pts)": number | null;
  "Total moved 2+ (%)": number | null;
  "Total decided games": number;
  "Total ties": number;
  "Close beat open, total (%)": number | null;
};

const A_COLOR = "#2a78d6";
const B_COLOR = "#eb6834";
const TOP_N = 15;

const num = (v: number | null | undefined, digits = 1) => (v == null ? "—" : v.toFixed(digits));
const pct = (v: number | null | undefined) => (v == null ? "—" : `${v.toFixed(1)}%`);
const line = (v: number | null | undefined) => (v == null ? "—" : v === 0 ? "PK" : `${v > 0 ? "+" : ""}${v}`);
const signed = (v: number | null | undefined) => (v == null ? "—" : `${v > 0 ? "+" : ""}${v.toFixed(1)}`);
const closerTone = (v: string | null) => (v === "Close" ? "text-emerald-700" : v === "Open" ? "text-rose-700" : "text-gray-500");
const vsHalf = (v: number | null | undefined) => (v == null || v === 50 ? "" : v > 50 ? "text-emerald-700" : "text-rose-700");

function Tile({ label, value, sub, tone = "" }: { label: string; value: string; sub: string; tone?: string }) {
  return (
    <div className="rounded-lg border bg-card p-4">
      <div className="text-[11px] uppercase tracking-wide text-gray-500">{label}</div>
      <div className={`mt-1 text-2xl font-semibold tabular-nums ${tone}`}>{value}</div>
      <div className="text-xs text-gray-500">{sub}</div>
    </div>
  );
}

function Books({ names }: { names: string | null }) {
  if (!names) return <span className="text-gray-400">—</span>;
  const list = names.split(", ");
  const shown = list.length > 2 ? `${list.slice(0, 2).join(", ")} +${list.length - 2}` : names;
  return <span title={names} className="text-gray-500">{shown}</span>;
}

export default function BookDisagreement({ gaps, moves, summary }: {
  gaps: BookGapRow[];
  moves: LineMoveRow[];
  summary: MoveSummaryRow[];
}) {
  const [week, setWeek] = useState<number | "all">("all");
  const weeks = useMemo(
    () => [...new Set(gaps.map((g) => g.Week).filter((w): w is number => w != null))].sort((a, b) => a - b),
    [gaps],
  );
  const shownGaps = useMemo(
    () => (week === "all" ? gaps : gaps.filter((g) => g.Week === week)).slice(0, TOP_N),
    [gaps, week],
  );
  const bigMoves = useMemo(
    () => moves
      .filter((m) => m["Spread move (pts)"] != null)
      .sort((a, b) => Math.abs(b["Spread move (pts)"]!) - Math.abs(a["Spread move (pts)"]!))
      .slice(0, TOP_N),
    [moves],
  );
  const season = summary.find((s) => s.Week == null);
  const byWeek = summary
    .filter((s) => s.Week != null && (s["Spread decided games"] > 0 || s["Total decided games"] > 0))
    .map((s) => ({
      name: `Wk ${s.Week}`,
      spread: s["Close beat open, spread (%)"],
      total: s["Close beat open, total (%)"],
      ns: s["Spread decided games"],
      nt: s["Total decided games"],
    }));

  if (!gaps.length && !moves.length) {
    return (
      <div className="rounded-lg border bg-card p-4 text-sm text-gray-500">
        No pre-kickoff sportsbook captures for this season yet.
      </div>
    );
  }

  return (
    <div className="space-y-4">
      {season && (
        <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
          <Tile
            label="Median spread move"
            value={`${num(season["Median spread move (pts)"])} pts`}
            sub={`open to close, ${season["Games with open"]} games with an open 12h+ out`}
          />
          <Tile
            label="Spread moved 2+ points"
            value={pct(season["Spread moved 2+ (%)"])}
            sub={`median total move ${num(season["Median total move (pts)"])} pts`}
          />
          <Tile
            label="Close beat open, spread"
            value={pct(season["Close beat open, spread (%)"])}
            tone={vsHalf(season["Close beat open, spread (%)"])}
            sub={`${season["Spread decided games"]} finished games, ${season["Spread ties"]} ties left out`}
          />
          <Tile
            label="Close beat open, total"
            value={pct(season["Close beat open, total (%)"])}
            tone={vsHalf(season["Close beat open, total (%)"])}
            sub={`${season["Total decided games"]} finished games, ${season["Total ties"]} ties left out`}
          />
        </div>
      )}

      <div className="rounded-lg border bg-card p-4">
        <div className="flex flex-wrap items-baseline justify-between gap-2">
          <div>
            <h3 className="text-sm font-semibold">Widest disagreements between books</h3>
            <p className="text-xs text-gray-500">
              At each game&apos;s last capture before kickoff. Exchanges left out. Win probability is each book&apos;s
              moneyline with the vig removed.
            </p>
          </div>
          <label className="text-xs text-gray-600">
            Week{" "}
            <select
              className="rounded border px-1 py-0.5 text-xs"
              value={week}
              onChange={(e) => setWeek(e.target.value === "all" ? "all" : Number(e.target.value))}
            >
              <option value="all">All</option>
              {weeks.map((w) => <option key={w} value={w}>{w}</option>)}
            </select>
          </label>
        </div>
        <div className="mt-2 overflow-x-auto">
          <table className="min-w-full text-xs">
            <thead>
              <tr className="border-b text-gray-500">
                <th className="py-1 pr-3 text-left">Wk</th>
                <th className="py-1 pr-3 text-left">Matchup</th>
                <th className="py-1 pr-3 text-right">Books</th>
                <th className="py-1 pr-3 text-right">Spread range</th>
                <th className="py-1 pr-3 text-left">Home spread, low → high</th>
                <th className="py-1 pr-3 text-right">Total range</th>
                <th className="py-1 pr-3 text-left">Total, low → high</th>
                <th className="py-1 pr-3 text-right">Win prob range</th>
              </tr>
            </thead>
            <tbody>
              {shownGaps.map((g) => (
                <tr key={`${g.Week}-${g.Matchup}`} className="border-b border-gray-50 tabular-nums">
                  <td className="py-1 pr-3">{g.Week ?? "—"}</td>
                  <td className="py-1 pr-3 whitespace-nowrap">{g.Matchup}</td>
                  <td className="py-1 pr-3 text-right">{g.Books}</td>
                  <td className="py-1 pr-3 text-right font-semibold">{num(g["Spread range (pts)"])}</td>
                  <td className="py-1 pr-3 whitespace-nowrap">
                    {line(g["Home spread low"])} <Books names={g["Low spread books"]} /> → {line(g["Home spread high"])}{" "}
                    <Books names={g["High spread books"]} />
                  </td>
                  <td className="py-1 pr-3 text-right">{num(g["Total range (pts)"])}</td>
                  <td className="py-1 pr-3 whitespace-nowrap">
                    {num(g["Total low"])} <Books names={g["Low total books"]} /> → {num(g["Total high"])}{" "}
                    <Books names={g["High total books"]} />
                  </td>
                  <td className="py-1 pr-3 text-right">{num(g["Home win prob range (pts)"])} pts</td>
                </tr>
              ))}
              {!shownGaps.length && (
                <tr><td colSpan={8} className="py-2 text-gray-500">No games with two or more books this week.</td></tr>
              )}
            </tbody>
          </table>
        </div>
      </div>

      <div className="rounded-lg border bg-card p-4">
        <h3 className="text-sm font-semibold">Biggest spread moves</h3>
        <p className="text-xs text-gray-500">
          Consensus spread at the open (earliest capture 12+ hours before kickoff) and the close (last capture
          before kickoff), from the closing favorite&apos;s side. &quot;Nearer&quot; says whether the close or the open
          sat closer to the final margin.
        </p>
        <div className="mt-2 overflow-x-auto">
          <table className="min-w-full text-xs">
            <thead>
              <tr className="border-b text-gray-500">
                <th className="py-1 pr-3 text-left">Wk</th>
                <th className="py-1 pr-3 text-left">Matchup</th>
                <th className="py-1 pr-3 text-left">Favorite</th>
                <th className="py-1 pr-3 text-right">Open</th>
                <th className="py-1 pr-3 text-right">Close</th>
                <th className="py-1 pr-3 text-right">Move</th>
                <th className="py-1 pr-3 text-left">Toward</th>
                <th className="py-1 pr-3 text-right">Fav prob</th>
                <th className="py-1 pr-3 text-left">Final</th>
                <th className="py-1 pr-3 text-right">Error open / close</th>
                <th className="py-1 text-left">Nearer</th>
              </tr>
            </thead>
            <tbody>
              {bigMoves.map((m) => (
                <tr key={`${m.Week}-${m.Matchup}`} className="border-b border-gray-50 tabular-nums">
                  <td className="py-1 pr-3">{m.Week ?? "—"}</td>
                  <td className="py-1 pr-3 whitespace-nowrap">{m.Matchup}</td>
                  <td className="py-1 pr-3 whitespace-nowrap">{m["Favorite (close)"] ?? "—"}</td>
                  <td className="py-1 pr-3 text-right">{line(m["Fav spread open"])}</td>
                  <td className="py-1 pr-3 text-right">{line(m["Fav spread close"])}</td>
                  <td className="py-1 pr-3 text-right font-semibold">{num(Math.abs(m["Spread move (pts)"] ?? 0))}</td>
                  <td className="py-1 pr-3 whitespace-nowrap">{m["Moved toward"] ?? "—"}</td>
                  <td className="py-1 pr-3 text-right whitespace-nowrap">
                    {pct(m["Fav win prob open (%)"])} → {pct(m["Fav win prob close (%)"])}
                  </td>
                  <td className="py-1 pr-3 whitespace-nowrap">{m.Final ?? "not played"}</td>
                  <td className="py-1 pr-3 text-right">
                    {m["Margin error at open"] == null ? "—" : `${num(m["Margin error at open"])} / ${num(m["Margin error at close"])}`}
                  </td>
                  <td className={`py-1 ${closerTone(m["Spread closer"])}`}>{m["Spread closer"] ?? "—"}</td>
                </tr>
              ))}
              {!bigMoves.length && (
                <tr><td colSpan={11} className="py-2 text-gray-500">No games with both an open and a close.</td></tr>
              )}
            </tbody>
          </table>
        </div>
      </div>

      <div className="rounded-lg border bg-card p-4">
        <h3 className="text-sm font-semibold">Did the close beat the open?</h3>
        <p className="text-xs text-gray-500">
          Share of finished games where the closing line sat nearer the final than the open, by week. Ties (no move,
          or equal misses) left out; hover for the number of games.
        </p>
        {byWeek.length ? (
          <div className="mt-2 h-60">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={byWeek} margin={{ top: 12, right: 8, bottom: 4, left: 0 }}>
                <CartesianGrid stroke="#ecebe7" vertical={false} />
                <XAxis dataKey="name" tick={{ fontSize: 11 }} tickLine={false} />
                <YAxis domain={[0, 100]} tickFormatter={(v) => `${v}%`} tick={{ fontSize: 11 }} tickLine={false} axisLine={false} width={44} />
                <ReferenceLine y={50} stroke="#52514e" strokeDasharray="4 3" />
                <Tooltip
                  formatter={(v, name, item) => {
                    const p = item?.payload as { ns: number; nt: number };
                    const n = name === "Spread" ? p.ns : p.nt;
                    return [`${pct(v == null ? null : Number(v))} of ${n} games`, String(name)];
                  }}
                />
                <Legend wrapperStyle={{ fontSize: 11 }} />
                <Bar dataKey="spread" name="Spread" fill={A_COLOR} isAnimationActive={false} />
                <Bar dataKey="total" name="Total" fill={B_COLOR} isAnimationActive={false} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        ) : (
          <p className="mt-2 text-xs text-gray-500">No finished games with an open yet.</p>
        )}
        {season && (
          <p className="mt-1 text-[11px] text-gray-500">
            Season: spread {pct(season["Close beat open, spread (%)"])} ({signed((season["Close beat open, spread (%)"] ?? 50) - 50)} pts vs
            a coin flip), total {pct(season["Close beat open, total (%)"])}.
          </p>
        )}
      </div>
    </div>
  );
}
