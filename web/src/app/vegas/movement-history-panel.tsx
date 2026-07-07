"use client";

import { useState } from "react";
import type { LineMovementHistoryRow } from "@/db/queries";

const PAGE_SIZE = 50;

function fmtDate(d: string): string {
  return d.slice(5); // MM-DD
}

export default function MovementHistoryPanel({
  rows,
  cadenceNote = "the 3-hourly odds captures",
}: {
  rows: LineMovementHistoryRow[];
  cadenceNote?: string;
}) {
  const [page, setPage] = useState(1);
  if (rows.length === 0) return null;

  const totalPages = Math.max(1, Math.ceil(rows.length / PAGE_SIZE));
  const safePage = Math.min(page, totalPages);
  const pageRows = rows.slice((safePage - 1) * PAGE_SIZE, safePage * PAGE_SIZE);
  const fullTotal = rows[0]?.total ?? rows.length;

  // Decided, non-flat games: did the side the line moved toward actually win?
  const decided = rows.filter((r) => r.movedToward != null && r.movedSideWon != null);
  const hits = decided.filter((r) => r.movedSideWon).length;
  const hitRate = decided.length > 0 ? hits / decided.length : null;

  const pp = (open: number, close: number) => {
    const d = (close - open) * 100;
    return `${d >= 0 ? "+" : ""}${d.toFixed(1)}pp`;
  };

  return (
    <div className="rounded-lg border bg-white p-4">
      <h3 className="text-sm font-semibold text-gray-700 uppercase tracking-wide mb-1">
        Movement History
      </h3>
      <p className="text-xs text-gray-500 mb-2">
        Past games&rsquo; open&rarr;close consensus movement from {cadenceNote}, with the result.
        &ldquo;Moved&rarr;&rdquo; is the side the line drifted toward; &ldquo;Hit?&rdquo; is whether
        that side actually won.{" "}
        {hitRate != null && (
          <span className="font-medium text-gray-700">
            Moved-side won {hits}/{decided.length} ({(hitRate * 100).toFixed(0)}%) over decided games.
          </span>
        )}
      </p>

      <div className="overflow-x-auto">
        <table className="w-full text-xs border-collapse">
          <thead>
            <tr className="border-b text-gray-500 text-left">
              <th className="py-1 pr-2">Date</th>
              <th className="py-1 pr-2">Game</th>
              <th className="py-1 pr-2 text-right">P(home) o&rarr;c</th>
              <th className="py-1 pr-2 text-right">Move</th>
              <th className="py-1 pr-2 text-right">Tot &Delta;</th>
              <th className="py-1 pr-2">Result</th>
              <th className="py-1 pr-2">Moved&rarr;</th>
              <th className="py-1 pr-2 text-center">Hit?</th>
            </tr>
          </thead>
          <tbody>
            {pageRows.map((r) => {
              const movedLabel =
                r.movedToward == null ? "flat"
                : r.movedToward === "home" ? "home" : "away";
              const hit =
                r.movedToward == null ? { t: "—", c: "text-gray-300" }
                : r.movedSideWon == null ? { t: "pending", c: "text-gray-400" }
                : r.movedSideWon ? { t: "✓", c: "text-emerald-600 font-semibold" }
                : { t: "✗", c: "text-red-500 font-semibold" };
              return (
                <tr key={r.matchupId} className={`border-b border-gray-50 ${r.postFix ? "" : "opacity-50"}`}>
                  <td className="py-1 pr-2 text-gray-500 tabular-nums">{fmtDate(r.gameDate)}</td>
                  <td className="py-1 pr-2">{r.matchup}</td>
                  <td className="py-1 pr-2 text-right tabular-nums">
                    {(r.openProb * 100).toFixed(0)}%&rarr;{(r.closeProb * 100).toFixed(0)}%
                  </td>
                  <td className={`py-1 pr-2 text-right tabular-nums ${
                    Math.abs(r.closeProb - r.openProb) < 0.005 ? "text-gray-400" : "text-gray-700"
                  }`}>
                    {pp(r.openProb, r.closeProb)}
                  </td>
                  <td className="py-1 pr-2 text-right tabular-nums text-gray-500">
                    {r.totalMove != null ? `${r.totalMove >= 0 ? "+" : ""}${r.totalMove.toFixed(2)}` : "—"}
                  </td>
                  <td className="py-1 pr-2 tabular-nums text-gray-600">
                    {r.score ?? (r.winner ? r.winner : "—")}
                    {r.winner === "draw" && <span className="text-gray-400"> (draw)</span>}
                  </td>
                  <td className="py-1 pr-2 text-gray-600">{movedLabel}</td>
                  <td className={`py-1 pr-2 text-center ${hit.c}`}>{hit.t}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      <div className="flex items-center justify-between pt-2 text-xs text-gray-500">
        <span>
          Page {safePage} of {totalPages}
          {fullTotal > rows.length ? ` · showing most recent ${rows.length} of ${fullTotal}` : ` · ${rows.length} games`}
        </span>
        <div className="flex gap-2">
          <button
            onClick={() => setPage((p) => Math.max(1, p - 1))}
            disabled={safePage <= 1}
            className="rounded border px-2 py-1 disabled:opacity-40"
          >
            Prev
          </button>
          <button
            onClick={() => setPage((p) => Math.min(totalPages, p + 1))}
            disabled={safePage >= totalPages}
            className="rounded border px-2 py-1 disabled:opacity-40"
          >
            Next
          </button>
        </div>
      </div>
    </div>
  );
}
