export const dynamic='force-dynamic';
import Link from 'next/link';
import type {Metadata} from 'next';
import {easternDateString} from '@/lib/eastern-date';
import {getMathResearch,getMathResearchDates,getMathResearchCounts,getMathOutcomes,getTrioPoolAudit,getTrioPoolSummary,type MathPortfolio} from '@/db/nba-trio-math';
import {PoolAudit} from './pool-audit';
import registration from '@/data/nba-trio-math-registration-v2.json';
import {trioPortfolioResult} from '@/lib/nba-trio-results';
import {getNbaAvailability} from '@/db/nba-availability';
export const metadata:Metadata={title:'NBA Trio Lab',description:'Prospective comparisons of five-trio selection methods, with separate best-entry and combined results.'};
const stamp=(s:string)=>new Intl.DateTimeFormat('en-US',{timeZone:'America/New_York',month:'short',day:'numeric',hour:'numeric',minute:'2-digit',timeZoneName:'short'}).format(new Date(s));
const labels:Record<string,string>={market_baseline:'Market baseline',joint_best_of_five:'Joint best-of-five',robust_best_of_five:'Robust best-of-five'};
export default async function TrioLab({searchParams}:{searchParams:Promise<{date?:string}>}){
 const params=await searchParams,today=easternDateString(),date=params.date&&/^\d{4}-\d{2}-\d{2}$/.test(params.date)?params.date:today;
 const [record,dates,availability,counts,poolSummary]=await Promise.all([getMathResearch(date),getMathResearchDates(),getNbaAvailability(date),getMathResearchCounts(),getTrioPoolSummary()]);
 const [outcomes,poolAudit]=record?await Promise.all([getMathOutcomes(record),getTrioPoolAudit(record.sha256)]):[[],null];
 const players=new Map(record?.players.map(p=>[p.id,p])??[]);
 const gradeRows=outcomes.map(o=>({event_id:o.id.split(':')[0],player_key:o.id.split(':').slice(1).join(':'),status:o.status==='verified'?'graded':o.status,actual_points:o.points}));
 return <main className="mx-auto max-w-6xl space-y-5 px-4 py-6">
  <header className="space-y-2"><h1 className="text-2xl font-bold">NBA Trio Lab <span className="text-base font-normal text-amber-700">Experimental</span></h1>
   <p>Compare five trios selected for different goals. Best-of-five is your highest-scoring entry; combined score adds all five entries.</p>
   <p className="text-sm text-muted-foreground">These methods have no demonstrated prospective improvement. Models remain conditional on playing; unlisted availability is unverified.</p>
   <p className="text-sm">Prospective trial: {counts.recorded} recorded dates · {counts.graded} graded · {counts.complete}/30 completely paired. Historical calibration: {registration.historical_calibration_n} appearances, separate from this trial.</p>
   <nav className="flex flex-wrap gap-2" aria-label="Research dates">{[...new Set([today,...dates])].sort().map(d=><Link key={d} href={`/nba/trio-lab?date=${d}`} className="rounded border px-3 py-1 text-sm">{d}{d===today?' · today':''}</Link>)}</nav>
  </header>
  {!record?<section className="space-y-2 rounded-lg border p-4"><h2 className="font-semibold">Awaiting a prospective record for {date}</h2><p className="text-sm">A record is created only on a scheduled NBA slate, before the first game, from a genuine original v3 forecast. Prior dates are never rebuilt into prospective evidence.</p><p className="text-sm text-muted-foreground">Registered November 3; enrollment begins November 4. No result means pending, not zero performance.</p></section>:<>
   <p className="text-sm">Original forecast {stamp(record.source_frozen_at)} · math record {stamp(record.created_at)} · cutoff {stamp(record.lock_at)} · {record.pool_n} modeled candidates selected from {record.modeled_pool_n} modeled / {record.quoted_pool_n} quoted players.</p>
   <p className="text-sm text-amber-700">Latest official availability check: {availability?stamp(availability.observedAt):'unverified'}. These are original frozen research selections. Later status changes are displayed below and never rewrite them.</p>
   <div className="grid gap-4 lg:grid-cols-3">{Object.entries(labels).map(([key,label])=>{
    const portfolio=record.portfolios[key] as MathPortfolio,actual=trioPortfolioResult(portfolio.trios,gradeRows);
    const reportedOut=[...new Set(portfolio.trios.flat())].filter(id=>availability?.states[id]?.status.toLowerCase()==='out');
    return <section key={key} className="space-y-3 rounded-lg border p-4"><h2 className="font-semibold">{label}</h2><p className="text-sm text-muted-foreground">{key==='market_baseline'?'Five highest frozen market-line sums.':key==='joint_best_of_five'?'Greedy selection to improve the best entry across original joint v3 outcomes.':'Selection tested against original v3, compound joint and compound independent scenarios.'}</p>
     {reportedOut.length>0&&<p className="text-sm font-medium text-red-700">Later reported Out: {reportedOut.map(id=>players.get(id)?.player).join(', ')}. This frozen comparison is not a current eligible lineup recommendation.</p>}
     <ol className="space-y-2">{portfolio.trios.map((ids,i)=><li key={ids.join('|')} className="border-t pt-2 text-sm"><b>{i+1}.</b> {ids.map(id=>players.get(id)?.player??id).join(' · ')}<div className="text-xs text-muted-foreground">Frozen line sum {portfolio.market_line_sums[i].toFixed(1)}{actual.totals[i]!=null?` · actual ${actual.totals[i]}`:''}</div></li>)}</ol>
     <p className="font-medium">Actual best entry: {actual.bestOfFive??'Pending'}</p><p className="text-sm">Actual combined five: {actual.allFive??'Pending'}</p>
     {actual.status!=='resolved'&&<p className="text-xs text-muted-foreground">Any selected DNP or unresolved outcome keeps the comparison incomplete. Players are never replaced or scored zero.</p>}
     <details className="text-sm"><summary>Modeled scores by scenario</summary>{Object.entries(portfolio.scenario_expected_best).map(([s,v])=><p key={s}>{s}: expected best entry {v.toFixed(1)} · combined {portfolio.scenario_expected_all_five[s].toFixed(1)}</p>)}<p>Simulation summaries, not calibrated probabilities or proven improvements.</p></details>
    </section>})}</div>
   <PoolAudit audit={poolAudit} />
   <section className="space-y-2"><h2 className="font-semibold">Player estimates and opportunity changes</h2><div className="overflow-x-auto"><table className="w-full text-sm"><thead><tr className="text-left"><th>Player</th><th>Line</th><th>V3 mean</th><th>Compound mean</th><th>Raw / adjusted P90</th><th>Frozen / latest status</th></tr></thead><tbody>{record.players.map(p=><tr key={p.id} className="border-t"><td className="py-2">{p.player}</td><td>{p.market_line.toFixed(1)}</td><td>{p.v3_mean.toFixed(1)}</td><td>{p.compound_mean.toFixed(1)}</td><td>{p.raw_p90.toFixed(1)} / {p.adjusted_p90?.toFixed(1)??'Pending'}</td><td>{p.availability.status??'unverified'} / {availability?.states[p.id]?.status??'unverified'}</td></tr>)}</tbody></table></div><p className="text-xs text-muted-foreground">P90 adjustment: {record.calibration.source}. Historical residual adjustment does not establish current-season conditional coverage.</p></section>
   <details className="text-sm"><summary>Evidence and limits</summary><p>Protocol {record.protocol_sha256.slice(0,12)} · model {record.model_sha256.slice(0,12)} · original {record.sha256.slice(0,12)}</p><ul className="list-disc pl-5">{record.limitations.map(s=><li key={s}>{s}</li>)}</ul></details>
  </>}
  <section className="space-y-2 border-t pt-4"><h2 className="font-semibold">Player-pool restriction audit</h2>
   <p className="text-sm">Complete coverage: {poolSummary.complete.positive}/{poolSummary.complete.n} dates lost points at the top-12 cutoff · mean gap {poolSummary.complete.mean?.toFixed(1)??'Pending'}.</p>
   <p className="text-sm">Provisional coverage: {poolSummary.provisional.positive}/{poolSummary.provisional.n} dates show a verified-so-far pool gap · mean {poolSummary.provisional.mean?.toFixed(1)??'Pending'}. Missing outcomes can change these benchmarks.</p>
   <p className="text-sm">Latest matching audit pending: {poolSummary.pending??'Unavailable'}.</p>
   <p className="text-sm text-muted-foreground">Only each original&apos;s latest matching grade and audit count. This diagnoses the cutoff; it does not select new players or change the registered top-12 trial. An expanded-pool challenger requires a separate preregistration.</p>
  </section>
  <section className="space-y-2 border-t pt-4"><h2 className="font-semibold">What is being tested?</h2><ul className="list-disc space-y-1 pl-5 text-sm"><li>Bayesian detection of changing minutes and shot intensity.</li><li>Integer scoring from uncertain attempts and makes.</li><li>Shrunk historical residual dependence across scoring roles.</li><li>Five-entry selection across multiple model scenarios.</li><li>Separate historical and prior-trial residual P90 calibration.</li></ul><p className="text-sm">Review after 30 completely paired prospective slate dates, followed by a separate confirmation period. Existing forecasts and registered trials retain their original versions.</p></section>
  <Link href={`/nba/top-scorers?date=${date}`} className="text-sm underline">Return to Top Scorers</Link>
 </main>
}
