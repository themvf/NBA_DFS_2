import benchmark from '@/data/nfl-volume-benchmark.json';

export default function Benchmark() {
  return <section className="rounded-2xl border border-slate-200 bg-white p-5">
    <h2 className="text-xl font-bold">Does it beat the optimizer’s historical model?</h2>
    <p className="my-2 text-sm">Same player, game and actual score. Production algorithm replay with market inputs disabled; not archived live forecasts.</p>
    <p className="rounded-lg bg-amber-50 p-3 font-semibold text-amber-900">Not qualified: average predictions improved, but floor and ceiling ranges worsened in both seasons.</p>
    <div className="my-4 grid gap-4 md:grid-cols-2">{Object.entries(benchmark.seasons).map(([year, r]) => <article key={year} className="rounded-xl bg-slate-50 p-4">
      <h3 className="font-bold">{year} · {r.n.toLocaleString()} matched WR games</h3>
      {(['mae','interval80'] as const).map(metric => <div key={metric} className="mt-3"><p className="text-sm font-semibold">{metric === 'mae' ? 'Average scoring error' : 'Floor/ceiling interval score'} · lower is better</p>
        {(['production','candidate'] as const).map(source => <div key={source} className="mt-2"><div className="flex justify-between text-sm"><span>{source === 'production' ? 'Historical model' : 'Volume/share'}</span><span>{r[source][metric].toFixed(2)}</span></div><div className="h-2 rounded bg-slate-200"><div className={`h-2 rounded ${source === 'production' ? 'bg-slate-600' : 'bg-teal-600'}`} style={{width:`${r[source][metric]/Math.max(r.production[metric], r.candidate[metric])*100}%`}} /></div></div>)}
      </div>)}
    </article>)}</div>
    <details><summary className="cursor-pointer font-semibold">Where the ranges fail: prior workload</summary><p className="my-2 text-sm">A calibrated P10 has about 10% of outcomes below it; a P90 has about 10% above it. Groups use earlier target history.</p>
      <div className="overflow-x-auto"><table className="w-full text-left text-sm"><thead><tr>{['Season / prior targets','Games','Below P10: historical → new','Above P90: historical → new'].map(h => <th key={h} className="p-2">{h}</th>)}</tr></thead><tbody>{Object.entries(benchmark.tiers).flatMap(([year, tiers]) => Object.entries(tiers).map(([tier,r]) => <tr key={`${year}-${tier}`} className="border-t"><td className="p-2">{year} / {tier}</td><td>{r.n}</td>{(['below_p10','above_p90'] as const).map(metric => <td key={metric}>{(r.production[metric]*100).toFixed(1)}% → {(r.candidate[metric]*100).toFixed(1)}%</td>)}</tr>))}</tbody></table></div>
    </details>
    <p className="mt-3 text-xs text-slate-500">{benchmark.unmatched.production_unmatched} historical-model records lack a matching volume/share forecast and are excluded from both sides. Previously inspected seasons; recorded games only, excluding missing/DNP observations. No contest-profitability claim.</p>
  </section>;
}
