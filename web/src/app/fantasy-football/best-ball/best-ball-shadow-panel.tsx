"use client";

import type { ShadowBestBallSimulation } from "@/lib/fantasy-football/best-ball-simulation";

type Props = {
  simulation: ShadowBestBallSimulation | null;
  canDraft: boolean;
  onDraft: (playerId: number) => void;
};

function signed(value: number): string {
  return `${value >= 0 ? "+" : ""}${value.toFixed(1)}`;
}

export function BestBallShadowPanel({ simulation, canDraft, onDraft }: Props) {
  if (!simulation?.candidates.length) return null;
  return <section className="rounded-3xl border border-violet-200 bg-violet-50 p-5 shadow-sm">
    <div className="flex flex-wrap items-start justify-between gap-3">
      <div>
        <p className="text-xs font-black uppercase tracking-[0.22em] text-violet-700">Shadow Best Ball simulation</p>
        <h2 className="mt-1 text-2xl font-black">How the current choices fit your weekly lineup</h2>
        <p className="mt-1 max-w-3xl text-sm text-violet-950/75">Paired {simulation.iterations}-scenario simulation of the Decision Desk choices. It applies weekly availability, byes, player variance, and automatic 1 QB · 2 RB · 3 WR · 1 TE · 1 FLEX selection.</p>
      </div>
      <span className="rounded-full bg-violet-100 px-3 py-1 text-xs font-black text-violet-800">SHADOW · DOES NOT REORDER V1.6</span>
    </div>

    <div className="mt-5 overflow-x-auto rounded-2xl border border-violet-200 bg-white">
      <table className="w-full min-w-[720px] text-left text-sm">
        <thead className="bg-violet-100/70 text-xs uppercase tracking-wide text-violet-950/70"><tr>
          <th className="p-3">Candidate</th>
          <th className="p-3 text-right">Marginal counted pts</th>
          <th className="p-3 text-right">Expected counted pts</th>
          <th className="p-3 text-right">Counted weeks</th>
          <th className="p-3 text-right">Roster P90 Δ</th>
          <th className="p-3"></th>
        </tr></thead>
        <tbody>{simulation.candidates.map((candidate, index) => <tr key={candidate.playerId} className="border-t border-violet-100">
          <td className="p-3"><span className="mr-2 text-xs font-black text-violet-600">#{index + 1}</span><b>{candidate.name}</b><span className="ml-2 text-xs text-muted-foreground">{candidate.position}</span></td>
          <td className="p-3 text-right font-black text-emerald-700">{signed(candidate.marginalCountedPoints)}</td>
          <td className="p-3 text-right font-semibold">{candidate.expectedCountedPoints.toFixed(1)}</td>
          <td className="p-3 text-right">{candidate.expectedCountedWeeks.toFixed(1)}</td>
          <td className="p-3 text-right font-semibold text-violet-700">{signed(candidate.p90RosterDelta)}</td>
          <td className="p-3 text-right"><button disabled={!canDraft} onClick={() => onDraft(candidate.playerId)} className="rounded-lg border border-violet-300 px-3 py-1.5 text-xs font-bold text-violet-800 disabled:opacity-35">{canDraft ? "Draft" : "Waiting"}</button></td>
        </tr>)}</tbody>
      </table>
    </div>

    <p className="mt-3 text-xs text-violet-950/70"><b>Interpretation:</b> marginal points are the extra automatically counted points versus your roster as currently drafted—not a completed-roster forecast. This first shadow version derives weekly outcomes from V1.6 PPR totals and ranges. The exact DraftKings stat-line scorer is implemented, but yardage bonuses remain excluded here until weekly passing/rushing/receiving projections are connected.</p>
  </section>;
}
