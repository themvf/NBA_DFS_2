import Link from 'next/link';
import saved from '@/data/shared-game-model.json';
import jointSaved from '@/data/joint-outcomes.json';
import { readJointPageReport } from '@/lib/nfl-dfs/joint-report';

export const metadata = { title: 'Shared NFL Game Model' };
const pct = (v: number) => `${(100 * v).toFixed(1)}%`;
const number = (v: number) => v.toFixed(1);
const families = [['rushing_yards', 'Rushing yards'], ['receptions', 'Receptions'],
  ['receiving_yards', 'Receiving yards'], ['total_yards', 'Total yards']] as const;

export default function Page() {
  const joint = readJointPageReport(jointSaved);
  const pairs = [...saved.correlations].filter(p => p.correlation !== null)
    .sort((a, b) => Math.abs(b.correlation ?? 0) - Math.abs(a.correlation ?? 0)).slice(0, 10);
  return <main className="mx-auto max-w-6xl space-y-6 px-4 py-8">
    <header className="space-y-3"><h1 className="text-3xl font-bold">Shared game model</h1>
      <p>{saved.game.away} at {saved.game.home} · Local research example</p>
      <p className="text-sm text-slate-600">Original evidence cutoff: {saved.decisionAt}. Replayed with expanded workload logic; this is not refreshed availability or a live forecast.</p>
    </header>
    <section className="rounded-xl border bg-white p-4 space-y-3"><h2 className="text-xl font-semibold">Joint outcome research</h2>
      {joint.status === 'not_generated' ? <p>No new joint forecast has been published locally.</p> : <>
        <p>{joint.game!.away} at {joint.game!.home} · {joint.branch} · {joint.draws!.toLocaleString()} shared scenarios</p>
        <p className="text-sm text-slate-600">Frozen evidence cutoff: {joint.decision_at}. This is an exploratory replay, not a live forecast or evidence of a profitable edge.</p>
        <p>Current-game availability: {joint.source_manifest?.availability_verified ? 'provider evidence verified for the saved cutoff' : 'unresolved; this replay cannot confirm game-day participation'}. Mid-game exit fit: {joint.exit_model_enabled ? 'enabled' : 'disabled; requires adjudicated historical exit labels'}.</p>
        {joint.market_reweighting && <p>Market adjustment: {joint.market_reweighting.accepted ? 'accepted mechanically; predictive validation still required' : `rejected; original weights retained (${joint.market_reweighting.reason})`}.</p>}
        {families.map(([key, label]) => <details key={key}><summary className="cursor-pointer font-semibold">{label}: player ranges and exact top-three sets</summary>
          <div className="overflow-x-auto mt-3"><table className="w-full text-left text-sm"><thead><tr><th className="p-2">Player</th><th className="p-2">Average</th><th className="p-2">Median</th><th className="p-2">Middle 80%</th><th className="p-2">Leader share</th></tr></thead><tbody>
            {joint.metrics![key].players.filter(p => !p.residual).slice(0, 12).map(p => <tr key={p.identity}><td className="p-2">{p.name}</td><td className="p-2">{number(p.mean)}</td><td className="p-2">{number(p.p50)}</td><td className="p-2">{number(p.p10)}–{number(p.p90)}</td><td className="p-2">{pct(p.leader_share)}</td></tr>)}
          </tbody></table></div>
          <p className="text-sm my-2">These sets use all modeled individuals. Boundary ties receive equal shares; they are not conventional DFS lineups.</p>
          <ul className="space-y-1">{joint.exact_top_three![key].sets.slice(0, 5).map(s => <li key={s.identities.join(':')}>{s.names.join(' + ')}: {pct(s.probability)}</li>)}</ul>
          <p className="text-sm text-slate-600 mt-2">Sets involving unresolved contributors: {pct(joint.exact_top_three![key].unresolved_identity_mass)}. Unenumerated crowded ties: {pct(joint.exact_top_three![key].enumeration_overflow_mass)}.</p>
        </details>)}
      </>}
    </section>
    <section className="rounded-xl border bg-white p-4 space-y-2"><h2 className="text-xl font-semibold">Earlier baseline example</h2>
      <p>The sections below retain the earlier baseline replay. They use a separate scenario bank from the joint outcome research above.</p>
      <p>Within this baseline bank, the same simulated carries, targets, catches and yards produce both game-leader chances and the DFS production points below.</p>
      <p>Workload variability is estimated from prior team seasons. Replacement workloads require an explicit scenario with recorded evidence.</p>
      <p className="text-sm text-slate-600">{saved.draws.toLocaleString()} shared simulations. Simulated relationships and ranges still need historical and forward validation.</p>
    </section>
    <section className="overflow-x-auto rounded-xl border bg-white"><h2 className="p-4 text-xl font-semibold">Leader model</h2>
      <table className="w-full text-left text-sm"><thead className="bg-slate-50"><tr><th className="p-3">Category</th><th className="p-3">First choice</th><th className="p-3">Leader share</th><th className="p-3">Average</th><th className="p-3">Middle 80% range</th></tr></thead>
        <tbody>{families.map(([key, label]) => { const p = saved.leaderMetrics[key].players.find(p => !p.residual)!;
          return <tr key={key} className="border-t"><td className="p-3">{label}</td><td className="p-3">{p.name}</td><td className="p-3">{pct(p.win_share)}</td><td className="p-3">{number(p.mean!)}</td><td className="p-3">{number(p.p10!)}–{number(p.p90!)}</td></tr>; })}</tbody></table>
    </section>
    <section className="overflow-x-auto rounded-xl border bg-white"><h2 className="p-4 text-xl font-semibold">DFS production component</h2>
      <p className="px-4 pb-4">Includes receptions, rushing and receiving yards, and their separate DraftKings bonuses. Excludes passing, touchdowns and turnovers. These are not full fantasy projections, salary-value recommendations, or a minimum score.</p>
      <table className="w-full text-left text-sm"><thead className="bg-slate-50"><tr><th className="p-3">Player</th><th className="p-3">Average component points</th><th className="p-3">Middle 80% range</th><th className="p-3">100 rushing yards</th><th className="p-3">100 receiving yards</th></tr></thead>
        <tbody>{saved.players.filter(p => p.productionPoints.mean > 0).slice(0, 20).map(p => <tr key={p.identity} className="border-t"><td className="p-3">{p.name} · {p.team}</td><td className="p-3">{number(p.productionPoints.mean)}</td><td className="p-3">{number(p.productionPoints.p10)}–{number(p.productionPoints.p90)}</td><td className="p-3">{pct(p.rushBonusProbability)}</td><td className="p-3">{pct(p.receivingBonusProbability)}</td></tr>)}</tbody></table>
    </section>
    <section className="rounded-xl border bg-white p-4 space-y-3"><h2 className="text-xl font-semibold">Players moving together</h2>
      <p>Positive values mean the simulated production components tend to rise together; negative values mean they tend to move in opposite directions. These describe the model, not a measured real-world relationship.</p>
      <ul className="space-y-2">{pairs.map(p => <li key={`${p.first}-${p.second}`}>{p.first} / {p.second}: <strong>{p.correlation?.toFixed(2)}</strong> · {p.sameTeam ? 'teammates' : 'opponents'}</li>)}</ul>
    </section>
    <section className="rounded-xl border bg-white p-4 space-y-3"><h2 className="text-xl font-semibold">Full DFS path and remaining inputs</h2>
      <p>A separate consumer now scores complete banks from the existing DFS event simulator, including passing, touchdowns, turnovers, kickers and defense. It can compare legal lineups with captain scoring and salary-based targets while retaining shared draw order. No complete DFS forecast has been generated for this example.</p>
      <p>Routes and snaps are not captured here. The new research workflow supports role-conditioned gains, exit fitting and remaining-work redistribution when the required historical evidence is supplied. Complete DFS banks can also use the fitted exit allocation while preserving their event accounting. Detailed defensive coverage and sequential game scripts still need development and validation. The optimizer is not using this candidate.</p>
      <details><summary className="cursor-pointer font-semibold">Range check on examined games</summary><p className="mt-2 text-sm text-slate-600">{saved.developmentScope}</p><ul className="mt-2 space-y-1">{families.map(([key, label]) => <li key={key}>{label}: {pct(saved.developmentReview[key].coverage_80pct)} observed coverage of the advertised 80% interval.</li>)}</ul></details>
    </section>
    <nav className="flex gap-4"><Link className="text-emerald-800 underline" href="/nfl/game-leaders">Game leaders</Link><Link className="text-emerald-800 underline" href="/nfl/projections">Existing projections</Link></nav>
  </main>;
}
