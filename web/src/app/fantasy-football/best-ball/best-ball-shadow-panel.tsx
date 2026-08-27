"use client";

import type { ShadowBestBallSimulation } from "@/lib/fantasy-football/best-ball-simulation";

type Props = {
  simulation: ShadowBestBallSimulation | null;
  canDraft: boolean;
  nextUserPick: number | null;
  followingUserPick: number | null;
  onDraft: (playerId: number) => void;
};

function signed(value: number): string {
  return `${value >= 0 ? "+" : ""}${value.toFixed(1)}`;
}

const YAHOO_SIGNAL = {
  "major-discount": { label: "MAJOR DISCOUNT", className: "bg-emerald-100 text-emerald-800" },
  discount: { label: "YAHOO DISCOUNT", className: "bg-green-100 text-green-800" },
  fair: { label: "NEAR MARKET", className: "bg-slate-100 text-slate-700" },
  premium: { label: "YAHOO PREMIUM", className: "bg-rose-100 text-rose-800" },
  unavailable: { label: "NO YAHOO MATCH", className: "bg-slate-100 text-slate-500" },
} as const;

const YAHOO_ACTION = {
  wait: { label: "WAIT", className: "bg-blue-100 text-blue-800" },
  "target-soon": { label: "TARGET SOON", className: "bg-amber-100 text-amber-900" },
  "take-now": { label: "TAKE NOW", className: "bg-emerald-100 text-emerald-800" },
  "pass-at-price": { label: "PASS AT THIS PRICE", className: "bg-rose-100 text-rose-800" },
  "no-market-data": { label: "NO MARKET DATA", className: "bg-slate-100 text-slate-500" },
} as const;

export function BestBallShadowPanel({ simulation, canDraft, nextUserPick, followingUserPick, onDraft }: Props) {
  if (!simulation?.candidates.length) return null;
  return <section className="rounded-3xl border border-violet-200 bg-violet-50 p-5 shadow-sm">
    <div className="flex flex-wrap items-start justify-between gap-3">
      <div>
        <p className="text-xs font-black uppercase tracking-[0.22em] text-violet-700">Shadow Best Ball simulation</p>
        <h2 className="mt-1 text-2xl font-black">Roster upside and Yahoo draft-room leverage</h2>
        <p className="mt-1 max-w-3xl text-sm text-violet-950/75">Our rank measures value. Yahoo XRank and ADP estimate acquisition cost. Shadow tells you whether to wait or act before the room reaches that player.</p>
      </div>
      <div className="text-right"><span className="rounded-full bg-violet-100 px-3 py-1 text-xs font-black text-violet-800">SHADOW · DOES NOT REORDER V1.6</span><p className="mt-2 text-xs font-bold text-violet-950/65">Next picks: #{nextUserPick ?? "—"}{followingUserPick !== null ? ` → #${followingUserPick}` : ""}</p></div>
    </div>

    <div className="mt-5 overflow-x-auto rounded-2xl border border-violet-200 bg-white">
      <table className="w-full min-w-[1040px] text-left text-sm">
        <thead className="bg-violet-100/70 text-xs uppercase tracking-wide text-violet-950/70"><tr>
          <th className="p-3">Candidate</th>
          <th className="p-3 text-right">Marginal counted pts</th>
          <th className="p-3 text-right">Expected counted pts</th>
          <th className="p-3 text-right">Counted weeks</th>
          <th className="p-3 text-right">Roster P90 Δ</th>
          <th className="p-3 text-right" title="Our projection-driven overall rank and projected PPR points.">Our value</th>
          <th className="p-3 text-right" title="Yahoo pre-draft XRank and Yahoo ADP.">Yahoo market</th>
          <th className="p-3" title="Pick-aware recommendation using the earlier of Yahoo XRank and ADP, with a half-round safety buffer.">Draft timing</th>
          <th className="p-3"></th>
        </tr></thead>
        <tbody>{simulation.candidates.map((candidate, index) => <tr key={candidate.playerId} className="border-t border-violet-100">
          <td className="p-3"><span className="mr-2 text-xs font-black text-violet-600">#{index + 1}</span><b>{candidate.name}</b><span className="ml-2 text-xs text-muted-foreground">{candidate.position}</span></td>
          <td className="p-3 text-right font-black text-emerald-700">{signed(candidate.marginalCountedPoints)}</td>
          <td className="p-3 text-right font-semibold">{candidate.expectedCountedPoints.toFixed(1)}</td>
          <td className="p-3 text-right">{candidate.expectedCountedWeeks.toFixed(1)}</td>
          <td className="p-3 text-right font-semibold text-violet-700">{signed(candidate.p90RosterDelta)}</td>
          <td className="p-3 text-right"><p className="font-black text-slate-900">#{candidate.ourRank?.toFixed(1) ?? "—"}</p><p className="text-[10px] text-muted-foreground">{candidate.projectedPoints?.toFixed(1) ?? "—"} PPR pts</p></td>
          <td className="p-3 text-right">{candidate.yahooXRank !== null ? <><p className="font-black text-purple-700">X {candidate.yahooXRank.toFixed(1)}</p><p className="text-[10px] text-muted-foreground">ADP {candidate.yahooAdp?.toFixed(1) ?? "—"}</p></> : <span className="text-muted-foreground">—</span>}</td>
          <td className="p-3"><span className={`inline-flex rounded-full px-2 py-1 text-[10px] font-black ${YAHOO_ACTION[candidate.yahooDraftAction].className}`}>{YAHOO_ACTION[candidate.yahooDraftAction].label}</span><p className="mt-1 text-xs font-bold text-slate-700">{candidate.yahooTargetPick !== null ? `Target by ~#${candidate.yahooTargetPick}` : "Timing unavailable"}</p><p className="text-[10px] text-muted-foreground">{candidate.yahooMarketPick !== null ? `Pressure ~#${candidate.yahooMarketPick.toFixed(1)}` : YAHOO_SIGNAL[candidate.yahooMarketSignal].label}{candidate.yahooRankGap !== null ? ` · XRank gap ${signed(candidate.yahooRankGap)}` : ""}</p></td>
          <td className="p-3 text-right"><button disabled={!canDraft} onClick={() => onDraft(candidate.playerId)} className="rounded-lg border border-violet-300 px-3 py-1.5 text-xs font-bold text-violet-800 disabled:opacity-35">{canDraft ? "Draft" : "Waiting"}</button></td>
        </tr>)}</tbody>
      </table>
    </div>

    <p className="mt-3 text-xs text-violet-950/70"><b>Draft timing:</b> market pressure is the earlier of Yahoo XRank and Yahoo ADP; the target is approximately half a 12-team round earlier. <b>Wait</b> means another user turn remains before that window. <b>Target Soon</b> means the following turn enters it. <b>Take Now</b> means passing risks losing the player before your next turn. <b>Pass at This Price</b> means Yahoo is pushing the player earlier than our valuation. These are timing estimates, not availability guarantees.</p>
  </section>;
}
