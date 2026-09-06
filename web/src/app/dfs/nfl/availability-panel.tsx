import type { NflWorkspaceSlate } from './actions';

export default function AvailabilityPanel({ slate }: { slate: NflWorkspaceSlate }) {
  const players = slate.players;
  const coverage=slate.injuryCoverage;
  return <section className="rounded-xl border border-amber-200 bg-white p-5">
    <h2 className="font-bold">Game-week availability and roles</h2>
    <p className="my-2 text-sm">Evidence is refreshed when you resume the slate and again when generating lineups. Each saved optimizer run freezes the observations, timestamps, warnings and exclusions it used. Injury status does not automatically change projected targets or carries.</p>
    <div className="my-3 grid grid-cols-2 gap-3 md:grid-cols-4">{[
      ['Players', players.length], ['Excluded', players.filter(p => p.isOut).length],
      ['Fresh FantasyPros evidence', `${players.filter(p => p.availability?.freshFantasyPros).length} / ${players.filter(p => p.availability?.evidence?.some(e => e.source === 'fantasypros')).length} observed`],
      ['Official confirmations', players.filter(p => p.availability?.officialConfirmed).length],
    ].map(([label,value]) => <div key={label} className="rounded bg-slate-50 p-3"><strong className="text-xl">{value}</strong><p className="text-xs">{label}</p></div>)}</div>
    <p className="text-sm text-amber-900">Official inactive lists require a source-linked manual import; there is no automatic official feed. Near kickoff, roster and injury evidence older than two hours needs review. A fresh retrieval alone does not prove that the provider updated its report.</p>
    {coverage ? <div className="mt-4 rounded-lg bg-slate-50 p-4">
      <h3 className="font-semibold">Whole-feed identity audit · snapshot {coverage.snapshotId}</h3>
      <p className="my-2 text-xs">Captured {coverage.capturedAt}. These counts cover the provider feed, not just this salary slate. The API documents year/week parameters and a live probe returned different datasets, but responses do not echo the requested week. Injury-date timezone remains unverified. Provider limited-data flag: {coverage.limited === null ? 'unknown' : String(coverage.limited)}.</p>
      <div className="flex flex-wrap gap-3">{Object.entries(coverage.counts).map(([label,count])=><span className="rounded border bg-white px-3 py-2 text-sm" key={label}>{label.replaceAll('_',' ')}: <strong>{count}</strong></span>)}</div>
      <details className="mt-3"><summary className="cursor-pointer text-sm font-semibold">Which records were not matched?</summary>
        <ul className="mt-2 space-y-1 text-xs">{coverage.unresolved.map(row=><li key={`${row.name}-${row.team}-${row.position}`}>{row.name} · {row.team} · {row.position}: {row.category.replaceAll('_',' ')}</li>)}</ul>
        <p className="mt-2 text-xs">Provider nonteam means the feed lists no current NFL team; it is not independently verified retirement. Position conflicts and missing identities stay unresolved.</p>
      </details>
    </div> : <p className="mt-3 text-sm">Whole-feed identity audit is unavailable for this game week.</p>}
    <details className="mt-3"><summary className="cursor-pointer font-semibold">Inspect player evidence and conflicts</summary>
      <div className="mt-2 max-h-96 overflow-auto"><table className="w-full text-left text-xs"><thead><tr>{['Player / role','Reported status / practice','Evidence / decision time','Review needed'].map(h=><th className="p-2" key={h}>{h}</th>)}</tr></thead>
      <tbody>{players.map(p=><tr key={p.dkPlayerId} className="border-t"><td className="p-2">{p.name}<p>{p.availability?.role ?? 'Unresolved'}</p></td>
        <td className="p-2">{p.availability?.status ?? 'Unknown'}{p.availability?.evidence?.map(e=><p key={e.id}>{e.source}: {e.status}; practice: {e.practice ?? 'not reported'}</p>)}</td>
        <td className="p-2">{p.availability?.evaluatedAt}{p.availability?.evidence?.map(e=><p key={e.id}>#{e.id} captured {e.observedAt}; provider update {e.updatedAt ?? (e.unverifiedUpdate ? `${e.unverifiedUpdate} (timezone unverified)` : 'unknown')}</p>)}</td>
        <td className="p-2">{p.availability?.warnings?.join(' ')}</td></tr>)}</tbody></table></div>
    </details>
  </section>;
}
