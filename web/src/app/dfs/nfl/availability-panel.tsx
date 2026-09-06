import type { NflWorkspaceSlate } from './actions';

export default function AvailabilityPanel({ slate }: { slate: NflWorkspaceSlate }) {
  const players = slate.players;
  return <section className="rounded-xl border border-amber-200 bg-white p-5">
    <h2 className="font-bold">Game-week availability and roles</h2>
    <p className="my-2 text-sm">Evidence is refreshed when you resume the slate and again when generating lineups. Each saved optimizer run freezes the observations, timestamps, warnings and exclusions it used. Injury status does not automatically change projected targets or carries.</p>
    <div className="my-3 grid grid-cols-2 gap-3 md:grid-cols-4">{[
      ['Players', players.length], ['Excluded', players.filter(p => p.isOut).length],
      ['Fresh FantasyPros evidence', `${players.filter(p => p.availability?.freshFantasyPros).length} / ${players.filter(p => p.availability?.evidence?.some(e => e.source === 'fantasypros')).length} observed`],
      ['Official confirmations', players.filter(p => p.availability?.officialConfirmed).length],
    ].map(([label,value]) => <div key={label} className="rounded bg-slate-50 p-3"><strong className="text-xl">{value}</strong><p className="text-xs">{label}</p></div>)}</div>
    <p className="text-sm text-amber-900">Official inactive lists require a source-linked manual import; there is no automatic official feed. Near kickoff, roster and injury evidence older than two hours needs review. A fresh retrieval alone does not prove that the provider updated its report.</p>
    <details className="mt-3"><summary className="cursor-pointer font-semibold">Inspect player evidence and conflicts</summary>
      <div className="mt-2 max-h-96 overflow-auto"><table className="w-full text-left text-xs"><thead><tr>{['Player / role','Reported status / practice','Evidence / decision time','Review needed'].map(h=><th className="p-2" key={h}>{h}</th>)}</tr></thead>
      <tbody>{players.map(p=><tr key={p.dkPlayerId} className="border-t"><td className="p-2">{p.name}<p>{p.availability?.role ?? 'Unresolved'}</p></td>
        <td className="p-2">{p.availability?.status ?? 'Unknown'}{p.availability?.evidence?.map(e=><p key={e.id}>{e.source}: {e.status}; practice: {e.practice ?? 'not reported'}</p>)}</td>
        <td className="p-2">{p.availability?.evaluatedAt}{p.availability?.evidence?.map(e=><p key={e.id}>#{e.id} captured {e.observedAt}; provider update {e.updatedAt ?? (e.unverifiedUpdate ? `${e.unverifiedUpdate} (timezone unverified)` : 'unknown')}</p>)}</td>
        <td className="p-2">{p.availability?.warnings?.join(' ')}</td></tr>)}</tbody></table></div>
    </details>
  </section>;
}
