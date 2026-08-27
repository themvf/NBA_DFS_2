"use client";

import type { YahooRedraftStrategy } from "@/lib/fantasy-football/yahoo-redraft-strategy";

type Props = {
  strategy: YahooRedraftStrategy | null;
  canDraft: boolean;
  onDraft: (playerId: number) => void;
};

const STRATEGY_LABEL = {
  "best-path": { label: "BEST 2-PICK PATH", className: "bg-emerald-700 text-white" },
  "position-can-wait": { label: "POSITION CAN WAIT", className: "bg-blue-100 text-blue-800" },
  "tier-drop": { label: "TIER DROP", className: "bg-amber-100 text-amber-900" },
  alternative: { label: "ALTERNATIVE", className: "bg-slate-100 text-slate-700" },
} as const;

const TIMING_LABEL = {
  "take-now": { label: "TAKE NOW", className: "bg-emerald-100 text-emerald-800" },
  "target-soon": { label: "TARGET SOON", className: "bg-amber-100 text-amber-900" },
  wait: { label: "WAIT", className: "bg-blue-100 text-blue-800" },
  "pass-at-price": { label: "PASS AT THIS PRICE", className: "bg-rose-100 text-rose-800" },
  "no-market-data": { label: "NO YAHOO MATCH", className: "bg-slate-100 text-slate-500" },
} as const;

function signed(value: number): string {
  return `${value >= 0 ? "+" : ""}${value.toFixed(1)}`;
}

export default function YahooRedraftStrategyPanel({ strategy, canDraft, onDraft }: Props) {
  if (!strategy?.candidates.length) return null;
  return <section className="rounded-3xl border border-emerald-200 bg-emerald-50 p-5 shadow-sm">
    <div className="flex flex-wrap items-start justify-between gap-3">
      <div>
        <p className="text-xs font-black uppercase tracking-[0.22em] text-emerald-700">Yahoo Redraft Strategy</p>
        <h2 className="mt-1 text-2xl font-black">Two-pick opportunity cost and Yahoo room timing</h2>
        <p className="mt-1 max-w-3xl text-sm text-emerald-950/75">This compares each pick with the best player likely to remain at your following Yahoo turn. It values starters, flex depth, and later positional replacements instead of automatically filling an empty QB or TE slot.</p>
      </div>
      <div className="text-right"><span className="rounded-full bg-emerald-100 px-3 py-1 text-xs font-black text-emerald-800">YAHOO · TWO-PICK V1</span><p className="mt-2 text-xs font-bold text-emerald-950/65">Picks: #{strategy.nextPick ?? "—"}{strategy.followingPick !== null ? ` → #${strategy.followingPick}` : ""}</p></div>
    </div>

    {strategy.recommendation ? <div className="mt-4 rounded-2xl border border-emerald-300 bg-white p-4">
      <p className="text-[10px] font-black uppercase tracking-[0.2em] text-emerald-700">Recommended path</p>
      <p className="mt-1 text-lg font-black text-slate-950">{strategy.recommendation.headline}</p>
      <p className="mt-1 text-sm text-slate-700">{strategy.recommendation.explanation}</p>
    </div> : null}

    <div className="mt-5 overflow-x-auto rounded-2xl border border-emerald-200 bg-white">
      <table className="w-full min-w-[1120px] text-left text-sm">
        <thead className="bg-emerald-100/70 text-xs uppercase tracking-wide text-emerald-950/70"><tr>
          <th className="p-3">Candidate</th>
          <th className="p-3 text-right" title="Projected roster improvement from this player plus the best likely player at your following turn.">Two-pick roster Δ</th>
          <th className="p-3 text-right" title="Projected roster improvement from this pick by itself.">One-pick Δ</th>
          <th className="p-3 text-right">Our value</th>
          <th className="p-3 text-right">Yahoo market</th>
          <th className="p-3">Strategy</th>
          <th className="p-3">Yahoo timing</th>
          <th className="p-3"></th>
        </tr></thead>
        <tbody>{strategy.candidates.map((candidate, index) => <tr key={candidate.playerId} className={`border-t border-emerald-100 ${index === 0 ? "bg-emerald-50/60" : ""}`}>
          <td className="p-3"><span className="mr-2 text-xs font-black text-emerald-700">#{index + 1}</span><b>{candidate.name}</b><span className="ml-2 text-xs text-muted-foreground">{candidate.position === "DST" ? "DEF" : candidate.position}</span></td>
          <td className="p-3 text-right"><p className="font-black text-emerald-700">{signed(candidate.twoPickValueAdded)}</p><p className="text-[10px] text-muted-foreground">{candidate.futureTargetName ? `with ${candidate.futureTargetName}` : "no later target"}</p></td>
          <td className="p-3 text-right font-semibold">{signed(candidate.onePickValueAdded)}</td>
          <td className="p-3 text-right"><p className="font-black">#{candidate.ourRank?.toFixed(1) ?? "—"}</p><p className="text-[10px] text-muted-foreground">{candidate.projectedPoints?.toFixed(1) ?? "—"} PPR pts</p></td>
          <td className="p-3 text-right"><p className="font-black text-purple-700">X {candidate.yahooXRank?.toFixed(1) ?? "—"}</p><p className="text-[10px] text-muted-foreground">ADP {candidate.yahooAdp?.toFixed(1) ?? "—"}{candidate.yahooRankGap !== null ? ` · gap ${signed(candidate.yahooRankGap)}` : ""}</p></td>
          <td className="max-w-[310px] p-3"><span className={`inline-flex rounded-full px-2 py-1 text-[10px] font-black ${STRATEGY_LABEL[candidate.strategyLabel].className}`}>{STRATEGY_LABEL[candidate.strategyLabel].label}</span><p className="mt-1 text-xs leading-relaxed text-slate-700">{candidate.strategyExplanation}</p></td>
          <td className="p-3"><span className={`inline-flex rounded-full px-2 py-1 text-[10px] font-black ${TIMING_LABEL[candidate.yahooTimingAction].className}`}>{TIMING_LABEL[candidate.yahooTimingAction].label}</span><p className="mt-1 text-xs font-bold text-slate-700">{candidate.yahooTargetPick !== null ? `Target by ~#${candidate.yahooTargetPick}` : "Timing unavailable"}</p><p className="text-[10px] text-muted-foreground">{candidate.yahooMarketPick !== null ? `Yahoo pressure ~#${candidate.yahooMarketPick.toFixed(1)}` : "No matched XRank/ADP"}</p></td>
          <td className="p-3 text-right"><button disabled={!canDraft} onClick={() => onDraft(candidate.playerId)} className="rounded-lg border border-emerald-300 px-3 py-1.5 text-xs font-bold text-emerald-800 disabled:opacity-35">{canDraft ? "Draft" : "Waiting"}</button></td>
        </tr>)}</tbody>
      </table>
    </div>

    <p className="mt-3 text-xs text-emerald-950/70"><b>Strategy order:</b> candidates are ranked by this pick plus the best plausible option at the following turn. Yahoo XRank and ADP estimate availability; our projections determine football value. Kicker and defense are held out of early-round recommendations and enter the comparison in Round 12.</p>
  </section>;
}
