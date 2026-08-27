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

const MARKET_SIGNAL = {
  "major-discount": { label: "MAJOR DISCOUNT", className: "bg-emerald-100 text-emerald-800" },
  discount: { label: "DK DISCOUNT", className: "bg-green-100 text-green-800" },
  fair: { label: "NEAR MARKET", className: "bg-slate-100 text-slate-700" },
  premium: { label: "DK PREMIUM", className: "bg-rose-100 text-rose-800" },
  unavailable: { label: "NO DK MATCH", className: "bg-slate-100 text-slate-500" },
} as const;

const DK_ACTION = {
  wait: { label: "WAIT", className: "bg-blue-100 text-blue-800" },
  "target-soon": { label: "TARGET SOON", className: "bg-amber-100 text-amber-900" },
  "take-now": { label: "TAKE NOW", className: "bg-emerald-100 text-emerald-800" },
  "pass-at-price": { label: "PASS AT THIS PRICE", className: "bg-rose-100 text-rose-800" },
  "no-market-data": { label: "NO MARKET DATA", className: "bg-slate-100 text-slate-500" },
} as const;

const STRATEGY_LABEL = {
  "best-path": { label: "BEST 2-PICK PATH", className: "bg-violet-700 text-white" },
  "position-can-wait": { label: "POSITION CAN WAIT", className: "bg-blue-100 text-blue-800" },
  "tier-drop": { label: "TIER DROP", className: "bg-amber-100 text-amber-900" },
  alternative: { label: "ALTERNATIVE", className: "bg-slate-100 text-slate-700" },
} as const;

export function BestBallShadowPanel({ simulation, canDraft, nextUserPick, followingUserPick, onDraft }: Props) {
  if (!simulation?.candidates.length) return null;
  return <section className="rounded-3xl border border-violet-200 bg-violet-50 p-5 shadow-sm">
    <div className="flex flex-wrap items-start justify-between gap-3">
      <div>
        <p className="text-xs font-black uppercase tracking-[0.22em] text-violet-700">DraftKings Best Ball Shadow</p>
        <h2 className="mt-1 text-2xl font-black">Two-pick strategy and DraftKings room leverage</h2>
        <p className="mt-1 max-w-3xl text-sm text-violet-950/75">Shadow compares what happens if you draft each player now and then take the best likely option at your following pick. This prices in later QB and TE alternatives instead of rewarding an empty roster slot.</p>
      </div>
      <div className="text-right"><span className="rounded-full bg-violet-100 px-3 py-1 text-xs font-black text-violet-800">SHADOW · TWO-PICK V1.7</span><p className="mt-2 text-xs font-bold text-violet-950/65">Next picks: #{nextUserPick ?? "—"}{followingUserPick !== null ? ` → #${followingUserPick}` : ""}</p></div>
    </div>

    {simulation.recommendation ? <div className="mt-4 rounded-2xl border border-violet-300 bg-white p-4">
      <p className="text-[10px] font-black uppercase tracking-[0.2em] text-violet-600">Recommended path</p>
      <p className="mt-1 text-lg font-black text-slate-950">{simulation.recommendation.headline}</p>
      <p className="mt-1 text-sm text-slate-700">{simulation.recommendation.explanation}</p>
    </div> : null}

    <div className="mt-5 overflow-x-auto rounded-2xl border border-violet-200 bg-white">
      <table className="w-full min-w-[1260px] text-left text-sm">
        <thead className="bg-violet-100/70 text-xs uppercase tracking-wide text-violet-950/70"><tr>
          <th className="p-3">Candidate</th>
          <th className="p-3 text-right" title="Expected roster improvement after this pick plus the best likely player at your following turn.">Two-pick roster Δ</th>
          <th className="p-3 text-right" title="The candidate's standalone contribution to the current roster; this is no longer the ordering signal.">One-pick contribution</th>
          <th className="p-3 text-right">Counted weeks</th>
          <th className="p-3 text-right">Two-pick P90 Δ</th>
          <th className="p-3 text-right" title="Our projection-driven overall rank and projected PPR points.">Our value</th>
          <th className="p-3 text-right" title="Current DraftKings Best Ball pre-draft rank and DraftKings ADP.">DK market</th>
          <th className="p-3" title="Replacement-aware football decision using the likely player pool at your following pick.">Strategy</th>
          <th className="p-3" title="Pick-aware recommendation using the earlier of DraftKings Rank and ADP, with a half-round safety buffer.">DK Shadow</th>
          <th className="p-3"></th>
        </tr></thead>
        <tbody>{simulation.candidates.map((candidate, index) => <tr key={candidate.playerId} className={`border-t border-violet-100 ${index === 0 ? "bg-violet-50/60" : ""}`}>
          <td className="p-3"><span className="mr-2 text-xs font-black text-violet-600">#{index + 1}</span><b>{candidate.name}</b><span className="ml-2 text-xs text-muted-foreground">{candidate.position}</span></td>
          <td className="p-3 text-right"><p className="font-black text-emerald-700">{signed(candidate.twoPickMarginalPoints)}</p><p className="text-[10px] text-muted-foreground">{candidate.futureTargetName ? `with ${candidate.futureTargetName}` : "no later target"}</p></td>
          <td className="p-3 text-right"><p className="font-semibold">{signed(candidate.marginalCountedPoints)}</p><p className="text-[10px] text-muted-foreground">{candidate.expectedCountedPoints.toFixed(1)} counted pts</p></td>
          <td className="p-3 text-right">{candidate.expectedCountedWeeks.toFixed(1)}</td>
          <td className="p-3 text-right font-semibold text-violet-700">{signed(candidate.twoPickP90Delta)}</td>
          <td className="p-3 text-right"><p className="font-black text-slate-900">#{candidate.ourRank?.toFixed(1) ?? "—"}</p><p className="text-[10px] text-muted-foreground">{candidate.projectedPoints?.toFixed(1) ?? "—"} PPR pts</p></td>
          <td className="p-3 text-right">{candidate.dkBestBallRank !== null ? <><p className="font-black text-blue-700">R {candidate.dkBestBallRank}</p><p className="text-[10px] text-muted-foreground">ADP {candidate.dkBestBallAdp?.toFixed(1) ?? "—"}</p></> : <span className="text-muted-foreground">—</span>}</td>
          <td className="max-w-[300px] p-3"><span className={`inline-flex rounded-full px-2 py-1 text-[10px] font-black ${STRATEGY_LABEL[candidate.strategyLabel].className}`}>{STRATEGY_LABEL[candidate.strategyLabel].label}</span><p className="mt-1 text-xs leading-relaxed text-slate-700">{candidate.strategyExplanation}</p></td>
          <td className="p-3"><span className={`inline-flex rounded-full px-2 py-1 text-[10px] font-black ${DK_ACTION[candidate.dkDraftAction].className}`}>{DK_ACTION[candidate.dkDraftAction].label}</span><p className="mt-1 text-xs font-bold text-slate-700">{candidate.dkTargetPick !== null ? `Target by ~#${candidate.dkTargetPick}` : "Timing unavailable"}</p><p className="text-[10px] text-muted-foreground">{candidate.dkMarketPick !== null ? `DK pressure ~#${candidate.dkMarketPick.toFixed(1)}` : MARKET_SIGNAL[candidate.dkMarketSignal].label}{candidate.dkRankGap !== null ? ` · Rank gap ${signed(candidate.dkRankGap)}` : ""}</p></td>
          <td className="p-3 text-right"><button disabled={!canDraft} onClick={() => onDraft(candidate.playerId)} className="rounded-lg border border-violet-300 px-3 py-1.5 text-xs font-bold text-violet-800 disabled:opacity-35">{canDraft ? "Draft" : "Waiting"}</button></td>
        </tr>)}</tbody>
      </table>
    </div>

    <p className="mt-3 text-xs text-violet-950/70"><b>Strategy order:</b> candidates are ranked by the simulated value of this pick plus the best plausible player at your following turn. A later player is considered plausible when DraftKings Rank/ADP has not crossed the half-round safety window. <b>DK Shadow timing</b> then tells you whether the room is likely to force the current decision now.</p>
  </section>;
}
