"use client";

import type { MlbLineMovementRow } from "@/db/queries";

// Shared open→close movement panel (MLB / soccer / tennis vegas views).
// Rows come from getLineMovement(sport) over the game_odds_history capture
// trail; per-book columns (Pin gap) populate from 2026-07-02 when the books
// JSONB capture began.
export default function LineMovementPanel({
  rows,
  cadenceNote,
}: {
  rows: MlbLineMovementRow[];
  cadenceNote: string;
}) {
  const pp = (v: number) => `${v >= 0 ? "+" : ""}${v.toFixed(1)}pp`;
  const pct = (v: number) => `${(v * 100).toFixed(1)}%`;
  const trusted = rows.filter((r) => r.postFix);
  const shown = (trusted.length >= 5 ? trusted : rows).slice(0, 20);
  return (
    <div className="rounded-lg border bg-white p-4">
      <h3 className="text-sm font-semibold text-gray-700 uppercase tracking-wide mb-1">
        Line Movement — open → close (last 7 days)
      </h3>
      <p className="text-xs text-gray-500 mb-2">
        Per-game consensus movement from {cadenceNote}. <span className="font-medium">Pin gap</span> =
        Pinnacle&rsquo;s vig-free P(home) minus retail consensus at the close — the sharp book sitting
        off retail marks the sharp side. <span className="font-medium">Max jump</span> = largest single-interval
        move (fast synchronized moves are steam; slow drift is position balancing).
      </p>
      {shown.length === 0 ? (
        <div className="rounded bg-amber-50/60 border border-amber-200 px-3 py-2 text-xs text-amber-700">
          Accruing — needs ≥2 pre-game captures per game. Populates as the refresh cron runs.
        </div>
      ) : (
        <table className="w-full text-xs border-collapse">
          <thead>
            <tr className="border-b text-gray-500">
              <th className="py-1 text-left">Date</th>
              <th className="py-1 text-left">Game</th>
              <th className="py-1 text-right">Caps</th>
              <th className="py-1 text-right">P(home) open→close</th>
              <th className="py-1 text-right">Move</th>
              <th className="py-1 text-right">Max jump</th>
              <th className="py-1 text-right">Total open/Δ</th>
              <th className="py-1 text-right">Pin gap</th>
            </tr>
          </thead>
          <tbody>
            {shown.map((r, i) => {
              const move = (r.closeProb - r.openProb) * 100;
              return (
                <tr
                  key={`${r.gameDate}-${r.matchup}-${i}`}
                  className={`border-b border-gray-50 ${!r.postFix ? "opacity-50" : ""} ${
                    Math.abs(move) >= 3 && r.postFix ? "bg-amber-50/50" : ""
                  }`}
                >
                  <td className="py-1 text-gray-500 whitespace-nowrap">{r.gameDate.slice(5)}</td>
                  <td className="py-1">{r.matchup}{!r.postFix ? " *" : ""}</td>
                  <td className="py-1 text-right text-gray-400">{r.captures}</td>
                  <td className="py-1 text-right tabular-nums">{pct(r.openProb)} → {pct(r.closeProb)}</td>
                  <td className={`py-1 text-right tabular-nums font-medium ${
                    move > 0 ? "text-emerald-600" : move < 0 ? "text-red-500" : "text-gray-400"
                  }`}>{pp(move)}</td>
                  <td className="py-1 text-right tabular-nums text-gray-500">{pp(r.maxJumpPp)}</td>
                  <td className="py-1 text-right tabular-nums text-gray-500">
                    {r.openTotal != null
                      ? `${r.openTotal.toFixed(1)}${r.totalMove ? ` ${r.totalMove > 0 ? "+" : ""}${r.totalMove.toFixed(2)}` : ""}`
                      : "—"}
                  </td>
                  <td className={`py-1 text-right tabular-nums ${
                    r.pinGapPp != null && Math.abs(r.pinGapPp) >= 1 ? "font-medium text-indigo-600" : "text-gray-500"
                  }`}>
                    {r.pinGapPp != null ? pp(r.pinGapPp) : "—"}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      )}
      {shown.some((r) => !r.postFix) && (
        <p className="text-[11px] text-gray-400 mt-1">
          * pre-2026-07-02 capture history — movement includes odds-averaging noise
        </p>
      )}
    </div>
  );
}
