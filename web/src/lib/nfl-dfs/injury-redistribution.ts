import type {Availability} from './availability';
import type {RoleMember} from './role-allocation';

export type InjuryRole = RoleMember & {availability:Availability};
/** Frozen hypothesis: half of removed known share, proportional to supported remaining roles.
 * Unknown roles and the other half remain reserved. This is not calibrated redistribution.
 */
export function redistributeInjuryTargets(members:InjuryRole[],targetBudget:number,currentQb:string|null,historicalQb:string|null,now:number) {
  if(!Number.isFinite(targetBudget)||targetBudget<0)throw new Error('Invalid target budget.');
  if(new Set(members.map(p=>p.id)).size!==members.length)throw new Error('Duplicate roster player.');
  const absent=members.filter(p=>['WR','TE'].includes(p.position)&&p.availability.officialConfirmed&&p.availability.status==='INACTIVE');
  if(absent.length!==1)throw new Error('Requires exactly one verified inactive WR/TE on the full roster.');
  if(!currentQb||!historicalQb||currentQb!==historicalQb)throw new Error('Current QB differs from the historical reference or is unresolved; a new team passing budget is required.');
  const fresh=(p:InjuryRole)=>{
    const a=p.availability,stamp=Date.parse(a.evaluatedAt??''),kickoff=Date.parse(a.kickoff??'');
    return a.fresh&&Number.isFinite(stamp)&&stamp<=now&&now-stamp<=60000&&kickoff>now;
  };
  if(!fresh(absent[0]))throw new Error('Inactive player roster or game evidence is stale.');
  const reference=(p:InjuryRole)=>!p.new_team&&!p.rookie&&p.prior_target_share!==null;
  for(const p of members)if(p.prior_target_share!==null&&(!Number.isFinite(p.prior_target_share)||p.prior_target_share<0||p.prior_target_share>1))throw new Error('Invalid prior target share.');
  if(!reference(absent[0]))throw new Error('Absent player has no same-team workload reference.');
  const refs=members.filter(reference);const total=refs.reduce((s,p)=>s+p.prior_target_share!,0);
  if(total>1+1e-9)throw new Error('Historical shares exceed the team budget.');
  const eligible=(p:InjuryRole)=>p.id!==absent[0].id&&fresh(p)&&!p.availability.blockedReason&&!p.out&&reference(p)&&['RB','FB','WR','TE'].includes(p.position)&&p.availability.kickoff===absent[0].availability.kickoff;
  const recipients=members.filter(eligible),remaining=recipients.reduce((s,p)=>s+p.prior_target_share!,0);
  const removed=absent[0].prior_target_share!,redistributed=remaining>0?removed*.5:0;
  const rows=members.map(p=>{const base=reference(p)?p.prior_target_share!:0;const gain=eligible(p)&&remaining>0?redistributed*base/remaining:0;const share=eligible(p)?base+gain:0;
    return {id:p.id,name:p.name,position:p.position,eligible:eligible(p),referenceTargets:base*targetBudget,targets:share*targetBudget,gain:gain*targetBudget,share};});
  return {version:'nfl-injury-targets-v1',optimizerEnabled:false as const,absentId:absent[0].id,currentQb,historicalQb,targetBudget,removedTargets:removed*targetBudget,redistributedTargets:redistributed*targetBudget,
    reservedTargets:Math.max(0,1-rows.reduce((s,p)=>s+p.share,0))*targetBudget,rows,members,
    limits:['50% proportional redistribution is a fixed research hypothesis, not a learned injury effect.','Historical team play mix is a reference, not a verified current scheme forecast.','Rookies, arrivals and unresolved roles remain unallocated. No automatic point or ceiling uplift.']};
}
