export type RoleMember = {id:string;identity?:string|null;name:string;position:string;role?:string;evidence_current:boolean;out:boolean;new_team:boolean;rookie:boolean;captured_at:string|null;prior_target_share:number|null;prior_carry_share:number|null};
export type RoleOverride = {targets?:number;carries?:number};
export type RoleSettings = Record<string,RoleOverride>;
const finiteShare=(x:number)=>Number.isFinite(x)&&x>=0&&x<=1;

/** Conservation, not a validated injury or depth-chart forecasting model. */
export function allocateRoles(members:RoleMember[], overrides:RoleSettings, targetBudget:number, carryBudget:number, now:number) {
  if (![targetBudget,carryBudget].every(x=>Number.isFinite(x)&&x>=0)) throw new Error('Invalid team opportunity budget');
  if (new Set(members.map(p=>p.id)).size!==members.length) throw new Error('Duplicate roster identity');
  const gsis=members.map(p=>p.identity).filter(Boolean);
  if(new Set(gsis).size!==gsis.length) throw new Error('Duplicate GSIS roster identity');
  const ids=new Set(members.map(p=>p.id));
  for(const id of Object.keys(overrides)) if(!ids.has(id)) throw new Error('Saved role no longer matches this roster');
  const rows=members.map(p=>{
    const captured=Date.parse(p.captured_at??'');
    const eligible=p.evidence_current&&!p.out&&Number.isFinite(captured)&&captured<=now&&now-captured<=72*3600000;
    const entered=overrides[p.id];
    for(const value of Object.values(entered??{})) if(value!==undefined&&!finiteShare(value)) throw new Error(`Invalid share for ${p.name}`);
    if(!eligible&&entered&&Object.values(entered).some(v=>v!==undefined&&v>0)) throw new Error(`${p.name}: unavailable or roster evidence unresolved`);
    const reference=eligible&&!p.new_team&&!p.rookie&&(p.position!=='QB'||p.role==='Listed QB1');
    const targets=eligible?(entered?.targets??(reference?p.prior_target_share??0:0)):0;
    const carries=eligible?(entered?.carries??(reference?p.prior_carry_share??0:0)):0;
    if(!finiteShare(targets)||!finiteShare(carries)) throw new Error('Invalid historical role share');
    return {id:p.id,name:p.name,eligible,targetShare:targets,carryShare:carries,targets:targets*targetBudget,carries:carries*carryBudget,
      targetSource:entered?.targets!==undefined?'Manual assumption':reference&&p.prior_target_share!==null?'Prior team reference':'Unassigned',
      carrySource:entered?.carries!==undefined?'Manual assumption':reference&&p.prior_carry_share!==null?'Prior team reference':'Unassigned'};
  });
  const targetShare=rows.reduce((s,r)=>s+r.targetShare,0),carryShare=rows.reduce((s,r)=>s+r.carryShare,0);
  if(targetShare>1+1e-9||carryShare>1+1e-9) throw new Error(`Shares exceed team budget. Reduce targets by ${Math.max(0,(targetShare-1)*100).toFixed(2)} percentage points and carries by ${Math.max(0,(carryShare-1)*100).toFixed(2)} percentage points.`);
  return {rows,targetShare,carryShare,unassignedTargets:Math.max(0,1-targetShare)*targetBudget,unassignedCarries:Math.max(0,1-carryShare)*carryBudget};
}
