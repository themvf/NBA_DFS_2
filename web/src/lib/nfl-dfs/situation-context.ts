import 'server-only';
import { getNflWorkload } from '@/db/nfl-dfs-workload';
import { getNflEfficiency } from '@/db/nfl-dfs-efficiency';
import { benchmarkTeam } from './competitor-benchmark';
import { resolveGameAvailability, type RosterEvidence } from './availability';
import type { SituationTeam, SituationEvidence } from './projection-audit';
import type { PlayerContext } from './player-context';

/** Read existing evidence; no narrative is converted into an invented numerical effect. */
export async function loadSituationContext(season:number,week:number,roster:Map<number,RosterEvidence>,now:number) {
  const [{default:context},{default:rawHistory}]=await Promise.all([import('@/data/nfl-team-context.json'),import('@/data/nfl-player-context-2025.json')]);
  const history=rawHistory as unknown as PlayerContext;
  const teams:SituationTeam[]=context.teams.map(t=>{
    const team=benchmarkTeam(t.team);
    const members=t.players.map(p=>{const e=roster.get(Number(p.id));return {...p,availability:resolveGameAvailability(e,team,p.position,now,week,e?.kickoff??null)};});
    const qbs=members.filter(p=>p.position==='QB'&&p.availability.role==='Expected starter · QB1'&&p.availability.fresh&&!p.availability.blockedReason);
    const games=Object.entries(history.games).filter(([,g])=>benchmarkTeam(g.team)===team).sort((a,b)=>b[1].week-a[1].week).slice(0,4);
    const prior=games.map(([key])=>history.rows.filter(r=>r.gameKey===key&&(r.attempts??0)>0).sort((a,b)=>(b.attempts??0)-(a.attempts??0))[0]?.playerId);
    const valid=context.season===season&&context.historical_season<season;
    return {team,capturedAt:context.as_of,recipeDigest:context.recipe_digest,rosterDigest:context.roster_digest,coachingDigest:context.coaching_digest,continuity:t.continuity,coaching:t.coaching,profiles:t.profiles,members,currentQb:qbs.length===1?qbs[0].identity:null,historicalQb:prior.length===4&&prior.every(q=>q&&q===prior[0])?prior[0]!:null,reason:valid?null:'Team context season or historical cutoff does not match this slate.'};
  });
  let saved:Awaited<ReturnType<typeof getNflEfficiency>>=null;
  let failure='No dated efficiency forecast matched to the workload snapshot.';
  try {
    const workload=await getNflWorkload();
    if(workload&&workload.report.season===season&&workload.report.week===week) {
      saved=await getNflEfficiency(workload.digest);
      if(saved&&(saved.report.workload_run_digest!==workload.digest||saved.report.workload_dataset_digest!==workload.report.dataset_digest)) {saved=null;failure='Efficiency provenance does not match its workload dataset.';}
    }
  }catch{failure='Efficiency evidence could not be read; no situation point adjustments.';}
  const rates=new Map<string,SituationEvidence>();
  if(saved&&saved.report.season===season&&saved.report.week===week)for(const t of saved.report.forecasts)for(const p of t.players) {
    if(!p.identity)continue;
    const team=teams.find(c=>c.team===benchmarkTeam(t.team));
    const member=team?.members.find(m=>m.identity===p.identity);
    const matches=member&&Date.parse(member.availability.kickoff??'')===Date.parse(t.kickoff);
    const key=`${benchmarkTeam(t.team)}:${p.identity}:${p.position}`;
    const duplicate=rates.has(key);
    rates.set(key,{team:null,rates:matches&&!duplicate?p.rates:null,ratesDigest:saved.digest,ratesAsOf:saved.report.as_of_at,reason:duplicate?'Ambiguous efficiency identity.':matches?null:'Efficiency game no longer matches current roster and kickoff.'});
  }
  return {teams,rates,failure};
}
