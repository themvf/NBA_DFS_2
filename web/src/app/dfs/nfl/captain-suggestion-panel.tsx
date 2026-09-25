"use client";

import type { CaptainRecommendation } from "@/lib/nfl-dfs/captain-recommendation";
import type { CaptainTarget } from "@/lib/nfl-dfs/generation-settings";

/**
 * A captain-range suggestion shown as a PREVIEW. Nothing in the CPT fields
 * changes until the user presses Apply -- the suggestion is advice, and the
 * fields are the user's instruction to the optimizer.
 */
export default function CaptainSuggestionPanel({ suggestion, current, nLineups, onApply, onDismiss }: {
  suggestion: CaptainRecommendation;
  current: Record<string, CaptainTarget>;
  nLineups: number;
  onApply: () => void;
  onDismiss: () => void;
}) {
  const count = (pct: number) => Math.round((pct / 100) * nLineups);
  const range = (t: CaptainTarget | undefined) =>
    !t || (t.min == null && t.max == null) ? "not set" : `${t.min ?? 0}–${t.max ?? 100}%`;
  const overwrites = suggestion.rows.filter((r) => current[String(r.dkPlayerId)]);
  return <div role="region" aria-label="Suggested captain ranges" className="border-b border-violet-200 bg-violet-50 px-4 py-3 text-xs">
    <div className="flex flex-wrap items-start justify-between gap-2">
      <div>
        <p className="font-bold text-violet-900">Suggested captain ranges</p>
        <p className="mt-0.5 text-violet-800">
          Top {suggestion.rows.length} captain-eligible players by {suggestion.basis}; share proportional to projection²,
          ±8 points, rounded to 5. Skips OUT, Questionable and Doubtful players. A starting point — not validated.
        </p>
      </div>
      <div className="flex gap-2">
        <button type="button" onClick={onApply}
          className="rounded bg-violet-700 px-3 py-1.5 font-bold text-white hover:bg-violet-600">Apply ranges</button>
        <button type="button" onClick={onDismiss}
          className="rounded border border-violet-300 bg-white px-3 py-1.5 text-violet-800 hover:bg-violet-100">Dismiss</button>
      </div>
    </div>
    <table className="mt-2 w-full max-w-lg text-left">
      <thead className="text-[10px] uppercase text-violet-700">
        <tr><th className="py-1">Player</th><th>Share</th><th>Suggested CPT</th><th>Lineups of {nLineups}</th><th>Current</th></tr>
      </thead>
      <tbody>{suggestion.rows.map((r) => <tr key={r.dkPlayerId} className="border-t border-violet-200">
        <td className="py-1 font-semibold">{r.name}</td>
        <td>{r.sharePct.toFixed(0)}%</td>
        <td className="font-bold">{r.min}–{r.max}%</td>
        <td>{count(r.min)}–{count(r.max)}</td>
        <td className="text-slate-500">{range(current[String(r.dkPlayerId)])}</td>
      </tr>)}</tbody>
    </table>
    {overwrites.length ? <p className="mt-2 text-amber-800">
      Applying replaces your current range for {overwrites.map((r) => r.name).join(", ")}. Ranges on other players are kept.
    </p> : <p className="mt-2 text-violet-700">Applying fills these fields only; ranges on other players are kept.</p>}
  </div>;
}
