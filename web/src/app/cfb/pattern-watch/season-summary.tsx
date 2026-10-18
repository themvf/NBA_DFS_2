"use client";

import { Bar, BarChart, CartesianGrid, Legend, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import type { SeasonSummaryRow } from "@/db/cfb-pattern-watch";

/**
 * This season against the three before it: the same FBS-vs-FBS population and
 * closing-line grading as the breakdown below (model/cfb_pattern_history.py).
 */

const pct = (v: number | null | undefined) => (v == null ? "—" : `${v.toFixed(1)}%`);
const signed = (v: number | null | undefined) => (v == null ? "—" : `${v > 0 ? "+" : ""}${v.toFixed(1)}`);
const tone = (v: number | null | undefined) => (v == null || v === 0 ? "" : v > 0 ? "text-emerald-700" : "text-rose-700");

export default function SeasonSummary({ rows }: { rows: SeasonSummaryRow[] }) {
  const data = rows.map((r) => ({
    name: String(r.Season).replace(" (to date)", "*"),
    won: r["Favorites won (%)"],
    implied: r["Price implied (%)"],
    covered: r["Favorites covered (%)"],
  }));
  return (
    <section className="space-y-3 rounded-lg border bg-card p-4">
      <div>
        <h2 className="text-sm font-semibold">This season against the last three</h2>
        <p className="text-xs text-gray-500">
          Every FBS-vs-FBS regular-season game with closing lines. Prior seasons use one sportsbook&apos;s closing line per game
          (mostly DraftKings), with spreads and totals at -110. * season to date.
        </p>
      </div>
      <div className="h-56">
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={data} margin={{ top: 8, right: 8, bottom: 4, left: 0 }}>
            <CartesianGrid stroke="#ecebe7" vertical={false} />
            <XAxis dataKey="name" tick={{ fontSize: 11 }} tickLine={false} />
            <YAxis domain={[40, 85]} tickFormatter={(v) => `${v}%`} tick={{ fontSize: 11 }} tickLine={false} axisLine={false} width={40} />
            <Tooltip formatter={(v) => pct(Number(v))} />
            <Legend wrapperStyle={{ fontSize: 12 }} />
            <Bar dataKey="won" name="Favorites won" fill="#2a78d6" isAnimationActive={false} />
            <Bar dataKey="implied" name="Price implied" fill="#eb6834" isAnimationActive={false} />
            <Bar dataKey="covered" name="Favorites covered" fill="#1baf7a" isAnimationActive={false} />
          </BarChart>
        </ResponsiveContainer>
      </div>
      <div className="overflow-x-auto">
        <table className="min-w-full text-xs">
          <thead>
            <tr className="border-b text-gray-500">
              <th className="py-1 pr-3 text-left">Season</th><th className="py-1 pr-3 text-right">Games</th>
              <th className="py-1 pr-3 text-right">Favorites won</th><th className="py-1 pr-3 text-right">Implied</th>
              <th className="py-1 pr-3 text-right">Gap</th><th className="py-1 pr-3 text-right">Favorite ROI</th>
              <th className="py-1 pr-3 text-right">Favorites covered</th><th className="py-1 text-right">Overs</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((r) => (
              <tr key={String(r.Season)} className="border-b border-gray-50 tabular-nums">
                <td className="py-1 pr-3">{r.Season}</td>
                <td className="py-1 pr-3 text-right">{r["Games with lines"]}</td>
                <td className="py-1 pr-3 text-right">{pct(r["Favorites won (%)"])}</td>
                <td className="py-1 pr-3 text-right">{pct(r["Price implied (%)"])}</td>
                <td className={`py-1 pr-3 text-right ${tone(r["Difference (pts)"])}`}>{signed(r["Difference (pts)"])} pts</td>
                <td className={`py-1 pr-3 text-right ${tone(r["Favorite ROI (%)"])}`}>{signed(r["Favorite ROI (%)"])}%</td>
                <td className="py-1 pr-3 text-right">{pct(r["Favorites covered (%)"])}</td>
                <td className="py-1 text-right">{pct(r["Overs (%)"])}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}
