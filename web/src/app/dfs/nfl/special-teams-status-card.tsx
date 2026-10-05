"use client";

import type { specialTeamsStatus } from '@/lib/nfl-dfs/special-teams-status';

export default function SpecialTeamsStatusCard({ status, pending, onAction }: {
  status: ReturnType<typeof specialTeamsStatus>; pending: boolean;
  onAction: (action: 'refresh_projections' | 'update_data') => void;
}) {
  if (!status) return null;
  return <section aria-label="Special teams forecasts" className="rounded-lg border border-slate-200 bg-slate-50 p-3 text-xs text-slate-700">
    <div role="status" aria-live="polite"><h3 className="font-bold">{status.title}</h3><p className="mt-1">{status.text}</p></div>
    {status.action ? <button type="button" disabled={pending} onClick={() => onAction(status.action!)} className="mt-2 min-h-11 rounded-lg border bg-white px-3 font-semibold disabled:opacity-50">{status.action === 'update_data' ? 'Update data' : 'Refresh projections'}</button> : null}
    <details className="mt-2"><summary className="min-h-11 cursor-pointer content-center font-semibold">Forecast details</summary>
      <p>Matchup forecasts use recent sack and turnover history plus opponent scoring context for defenses, and team scoring context for kickers. Tournament benefit has not been established.</p>
      {status.missing.length ? <ul className="mt-2 space-y-1">{status.missing.map((player, index) => <li key={`${player.name}-${player.position}-${index}`} className="break-words"><b>{player.name}:</b> {player.specialTeamsReason ?? 'No matchup forecast was saved.'}</li>)}</ul> : null}
    </details>
  </section>;
}
