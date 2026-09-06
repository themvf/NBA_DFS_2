"use client";

import { useState } from 'react';
import report from '@/data/nfl-receiving-components.json';

const names = { production: 'Existing model', role: 'Previous role experiment', candidate: 'Receiving components' };

export default function ReceivingComponents({ team }: { team: string }) {
  const examples = report.examples.filter(row => row.team === team);
  const [identity, setIdentity] = useState(examples[0]?.identity ?? '');
  const selected = examples.find(row => row.identity === identity);
  const contributions = selected ? [
    ['Catches', selected.components.points.receptions],
    ['Receiving yards', selected.components.points.receiving_yards],
    ['Receiving touchdowns', selected.components.points.receiving_tds],
    ['Bonuses and other scoring', selected.components.points.bonuses_and_other],
    ['Prior-error correction', selected.calibration_offset],
  ] as const : [];
  const scale = Math.max(1, ...contributions.map(([, value]) => Math.abs(value)));

  return <section className="rounded-2xl border border-slate-200 bg-white p-5">
    <div className="flex flex-wrap items-center justify-between gap-3">
      <h2 className="text-xl font-bold">From targets to catches, yards and touchdowns</h2>
      <span className="rounded-full bg-amber-100 px-3 py-1 text-sm font-semibold text-amber-950">
        {report.passes_screen ? 'Passed screen · not activated' : report.improves_role ? 'Improved experiment · not activated' : 'Experimental · not activated'}
      </span>
    </div>
    <p className="my-3 text-sm">Projected targets are identical to the previous role experiment. We now estimate catches, yards and touchdowns separately, with small samples pulled toward earlier league rates. Lower scores below are better.</p>
    <div className="overflow-x-auto">
      <table className="w-full text-left text-sm">
        <thead><tr>{['Season / paired games', 'Model', 'Point error', 'Floor/ceiling score', '25-point probability error'].map(label => <th key={label} className="p-2">{label}</th>)}</tr></thead>
        <tbody>{Object.entries(report.seasons).flatMap(([season, result]) =>
          (['production', 'role', 'candidate'] as const).map(model => <tr key={`${season}-${model}`} className={`border-t ${model === 'candidate' ? 'bg-teal-50' : ''}`}>
            <td className="p-2">{season} / {result.n.toLocaleString()}</td>
            <td className="p-2">{names[model]}</td>
            <td className="p-2">{result.models[model].mae.toFixed(3)}</td>
            <td className="p-2">{result.models[model].interval80.toFixed(3)}</td>
            <td className="p-2">{result.models[model].brier25.toFixed(5)}</td>
          </tr>))}</tbody>
      </table>
    </div>
    <p className="my-3 text-sm">The interval score penalizes overly wide ranges and outcomes outside the floor/ceiling. Existing model means a production-algorithm replay with market inputs disabled, not archived live projections. These seasons were previously inspected.</p>
    <details className="my-4 rounded-xl bg-slate-50 p-3">
      <summary className="font-semibold">How certain is the comparison?</summary>
      <p className="my-2 text-sm">The ranges below show 95% bootstrap intervals for candidate minus existing model, resampling whole weeks. Negative favors the candidate; crossing zero means the direction is uncertain. This does not correct for repeated research on these seasons.</p>
      {Object.entries(report.seasons).map(([year, result]) => <p key={year} className="my-2 text-sm">
        {year}: point error [{result.candidate_minus_production.mae.lower95.toFixed(3)}, {result.candidate_minus_production.mae.upper95.toFixed(3)}]; floor/ceiling score [{result.candidate_minus_production.interval80.lower95.toFixed(3)}, {result.candidate_minus_production.interval80.upper95.toFixed(3)}]; 25-point error [{result.candidate_minus_production.brier25.lower95.toFixed(4)}, {result.candidate_minus_production.brier25.upper95.toFixed(4)}].
      </p>)}
    </details>
    <h3 className="mt-5 font-bold">Historical player example · {team}</h3>
    <p className="my-2 text-sm">Examples come from the latest evaluated week for this team, selected by projected targets rather than actual performance. They are not current-slate recommendations.</p>
    {selected ? <>
      <label className="text-sm">Historical receiver
        <select className="ml-3 rounded border p-2" value={identity} onChange={event => setIdentity(event.target.value)}>
          {examples.map(row => <option key={row.identity} value={row.identity}>{row.name} · {row.season} W{row.week}</option>)}
        </select>
      </label>
      <div className="my-4 grid grid-cols-2 gap-3 lg:grid-cols-4">
        {([
          ['Targets', selected.components.targets], ['Catches', selected.components.receptions],
          ['Receiving yards', selected.components.receiving_yards], ['Receiving TDs', selected.components.receiving_tds],
        ] as const).map(([label, value]) => <div key={label} className="rounded-xl bg-slate-50 p-3">
          <strong className="text-xl">{value.toFixed(2)}</strong><p className="text-sm">{label}</p>
        </div>)}
      </div>
      <p className="text-xs">Touchdowns are an expected count, not the probability of scoring. Catch and efficiency evidence: {selected.components.observed_targets} prior targets and {selected.components.observed_receptions} prior catches.</p>
      <div className="my-4 space-y-3">{contributions.map(([label, value]) => <div key={label}>
        <div className="flex justify-between text-sm"><span>{label}</span><span>{value.toFixed(2)} DK points</span></div>
        <div className="mt-1 h-2 overflow-hidden rounded bg-slate-100"><div className={`h-full rounded ${value < 0 ? 'bg-rose-500' : 'bg-teal-600'}`} style={{ width: `${Math.abs(value) / scale * 100}%` }} /></div>
      </div>)}</div>
      <div className="grid grid-cols-2 gap-3 rounded-xl bg-teal-950 p-4 text-white lg:grid-cols-4">
        {([
          ['Projected mean', selected.candidate.mean], ['Floor · P10', selected.candidate.p10],
          ['Median · P50', selected.candidate.p50], ['Ceiling · P90', selected.candidate.p90],
        ] as const).map(([label, value]) => <div key={label}><strong className="text-xl">{value.toFixed(1)}</strong><p className="text-sm">{label}</p></div>)}
      </div>
      <p className="mt-3 text-xs">Ranges use {selected.residual_rows} earlier prediction errors, not independent player percentile sums. Bonuses use historical averages; crossing 100 projected yards does not automatically award a bonus. The separate correction reconciles component points to the final mean.</p>
    </> : <p className="text-sm">No qualifying historical example for this team.</p>}
    <p className="mt-4 text-sm text-amber-950">The candidate remains outside the optimizer. This test does not establish current rookie roles, injury replacements, salary-multiple hit rates or tournament winnings.</p>
  </section>;
}
