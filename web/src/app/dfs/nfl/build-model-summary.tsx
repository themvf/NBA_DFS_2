/** Explain the active build behavior without adding model/research switches. */
export default function BuildModelSummary({mode,historical,heuristicLeverage,absenceTeams}: {
  mode:'cash'|'gpp';historical:boolean;heuristicLeverage:boolean;absenceTeams:string[];
}) {
  return <section aria-label="How this build scores" className="mt-3 rounded-lg border border-slate-200 bg-slate-50 p-3 text-xs text-slate-700">
    <h3 className="font-semibold text-slate-900">How this build scores</h3>
    <dl className="mt-2 space-y-2">
      <div><dt className="font-semibold">{mode==='cash'?'Downside':'Upside'}</dt><dd>{historical
        ? `Search uses individual player ${mode==='cash'?'floors':'ceilings'}. Lineup totals are not joint ${mode==='cash'?'floor':'ceiling'} forecasts.`
        : 'Search uses the selected projections and heuristic risk/upside estimates.'}</dd></div>
      <div><dt className="font-semibold">Ownership</dt><dd>{mode==='gpp' && heuristicLeverage
        ? 'Uncalibrated ownership penalty requested. Review which players it fades after building.'
        : 'No ownership penalty requested. High projected ownership does not lower a player’s score.'}</dd></div>
      <div><dt className="font-semibold">Injuries and roles</dt><dd>Verified unavailable players are excluded. Active status does not guarantee a full workload.
        {absenceTeams.length ? ` ${[...new Set(absenceTeams)].join(', ')} teammates retain baseline workloads until absence adjustments qualify.` : ''}</dd></div>
    </dl>
  </section>;
}
