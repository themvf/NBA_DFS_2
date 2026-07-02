"use client";

import type { MlbLineMovementRow } from "@/db/queries";

// Shared open→close movement panel (MLB / soccer / tennis vegas views).
// Rows come from getLineMovement(sport) over the game_odds_history capture
// trail. Signals are translated into named-team chips with the action spelled
// out — the honest framing: Pin gap is a PRICE rule you can use today; move /
// jump are WATCH signals until the alert audit shows they carry CLV.

const teams = (matchup: string): { away: string; home: string } => {
  const [away, home] = matchup.split(" @ ");
  return { away: away ?? "away", home: home ?? "home" };
};

function SignalChips({ r }: { r: MlbLineMovementRow }) {
  const { away, home } = teams(r.matchup);
  const movePp = (r.closeProb - r.openProb) * 100;
  const chips: { label: string; cls: string; tip: string }[] = [];

  if (r.pinGapPp != null && Math.abs(r.pinGapPp) >= 2) {
    const side = r.pinGapPp > 0 ? home : away;
    chips.push({
      label: `Sharp side: ${side}`,
      cls: "bg-indigo-100 text-indigo-700",
      tip: `Pinnacle (the sharp book) prices ${side} ${Math.abs(r.pinGapPp).toFixed(1)}pp higher than retail books do. ` +
           `Usable today as a PRICE rule: if you bet this game at all, only take ${side} — retail is offering it ` +
           `cheaper than the sharp fair value — and never take the other side at a retail price Pinnacle says is too short.`,
    });
  }
  if (Math.abs(movePp) >= 2) {
    const side = movePp > 0 ? home : away;
    chips.push({
      label: `Walking → ${side}`,
      cls: movePp > 0 ? "bg-blue-100 text-blue-700" : "bg-rose-100 text-rose-700",
      tip: `The consensus has drifted ${Math.abs(movePp).toFixed(1)}pp toward ${side} since our first capture — ` +
           `money is arriving on ${side}. WATCH signal: whether following it is profitable is what the alert audit below measures.`,
    });
  }
  if (r.maxJumpPp >= 2 && Math.abs(movePp) < 2) {
    chips.push({
      label: "Jump",
      cls: "bg-amber-100 text-amber-700",
      tip: `A single capture interval moved ${r.maxJumpPp.toFixed(1)}pp — a fast move that later settled back. ` +
           `Fast synchronized moves are the steam signature; see the Sharp Line Alerts feed for confirmed multi-book steam.`,
    });
  }
  if (chips.length === 0) {
    return <span className="text-[10px] text-gray-300">quiet</span>;
  }
  return (
    <div className="flex flex-wrap gap-1 justify-end">
      {chips.slice(0, 2).map((c) => (
        <span key={c.label} title={c.tip}
              className={`cursor-help rounded-full px-2 py-0.5 text-[10px] font-semibold whitespace-nowrap ${c.cls}`}>
          {c.label}
        </span>
      ))}
    </div>
  );
}

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
        Per-game consensus movement from {cadenceNote}. Hover any chip for what to do with it.
      </p>
      <div className="rounded bg-gray-50 border px-3 py-2 text-[11px] text-gray-600 mb-3 space-y-0.5">
        <div><span className="rounded-full bg-indigo-100 text-indigo-700 px-1.5 py-0.5 font-semibold">Sharp side</span>{" "}
          = Pinnacle prices that team ≥2pp above retail. <span className="font-medium">Usable now:</span> only ever
          bet that side of this game, and shop the best retail price — never take the other side.</div>
        <div><span className="rounded-full bg-blue-100 text-blue-700 px-1.5 py-0.5 font-semibold">Walking</span>{" "}
          = the market has drifted ≥2pp toward that team since open. <span className="font-medium">Watch signal</span> —
          the alert audit below is measuring whether following it pays.</div>
        <div><span className="rounded-full bg-amber-100 text-amber-700 px-1.5 py-0.5 font-semibold">Jump</span>{" "}
          = a fast single-interval move (steam signature). Confirmed multi-book steam lands in Sharp Line Alerts.</div>
      </div>
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
              <th className="py-1 text-right">P(home) open→close</th>
              <th className="py-1 text-right">Move</th>
              <th className="py-1 text-right">Total open/Δ</th>
              <th className="py-1 text-right">Pin gap</th>
              <th className="py-1 text-right">Signal</th>
            </tr>
          </thead>
          <tbody>
            {shown.map((r, i) => {
              const move = (r.closeProb - r.openProb) * 100;
              return (
                <tr key={`${r.gameDate}-${r.matchup}-${i}`}
                    className={`border-b border-gray-50 ${!r.postFix ? "opacity-50" : ""}`}>
                  <td className="py-1 text-gray-500 whitespace-nowrap">{r.gameDate.slice(5)}</td>
                  <td className="py-1" title={`${r.captures} captures; max single-interval jump ${pp(r.maxJumpPp)}`}>
                    {r.matchup}{!r.postFix ? " *" : ""}
                  </td>
                  <td className="py-1 text-right tabular-nums">{pct(r.openProb)} → {pct(r.closeProb)}</td>
                  <td className={`py-1 text-right tabular-nums font-medium ${
                    move > 0 ? "text-emerald-600" : move < 0 ? "text-red-500" : "text-gray-400"
                  }`}>{pp(move)}</td>
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
                  <td className="py-1 text-right"><SignalChips r={r} /></td>
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
