import type { DefensiveProfile } from './defensive-projection';

export type DefensiveActivation = {
  id:string;state:'active'|'pending'|'revoked';consumer:'nfl-dfs-optimizer';
  profile:DefensiveProfile;baselineModelVersion:string;configurationHash:string;
  scoringVersion:string;implementationHash:string;qualificationId:string;
  effectiveAt:string;expiresAt:string;
};
export type DefensiveQualification = {
  id:string;verdict:'PASS'|'FAIL'|'NO_VERDICT';profile:DefensiveProfile;
  baselineModelVersion:string;configurationHash:string;scoringVersion:string;
  implementationHash:string;expiresAt:string;
};
export type DefensivePolicyContext = {
  profile:DefensiveProfile;baselineModelVersion:string;configurationHash:string;
  scoringVersion:string;implementationHash:string;
};

export function evaluateDefensiveActivation(policy:DefensiveActivation|null,
  qualification:DefensiveQualification|null,context:DefensivePolicyContext,now:Date):
  {active:boolean;reason:string;activationId:string|null;qualificationId:string|null} {
  const fail=(reason:string)=>({active:false,reason,activationId:policy?.id??null,qualificationId:qualification?.id??null});
  if(!policy)return fail('no_active_policy');
  if(policy.state!=='active')return fail(policy.state==='revoked'?'policy_revoked':'policy_pending');
  if(policy.consumer!=='nfl-dfs-optimizer')return fail('wrong_consumer');
  if(!Number.isFinite(Date.parse(policy.effectiveAt))||!Number.isFinite(Date.parse(policy.expiresAt))
    ||Date.parse(policy.effectiveAt)>now.getTime()||Date.parse(policy.expiresAt)<=now.getTime())return fail('policy_not_effective');
  if(!qualification||qualification.id!==policy.qualificationId)return fail('qualification_missing');
  if(qualification.verdict!=='PASS')return fail(`qualification_${qualification.verdict.toLowerCase()}`);
  if(!Number.isFinite(Date.parse(qualification.expiresAt))||Date.parse(qualification.expiresAt)<=now.getTime())return fail('qualification_expired');
  for(const field of ['profile','baselineModelVersion','configurationHash','scoringVersion','implementationHash'] as const) {
    if(policy[field]!==context[field]||qualification[field]!==context[field])return fail(`${field}_mismatch`);
  }
  return {active:true,reason:'qualified_exact_configuration',activationId:policy.id,qualificationId:qualification.id};
}

/** Server environment is the activation control plane; clients cannot send verdicts. */
export function readServerDefensivePolicy(context:DefensivePolicyContext,now=new Date()) {
  const parse=<T>(raw:string|undefined):T|null=>{
    if(!raw)return null;
    try {return JSON.parse(raw) as T;} catch {return null;}
  };
  if(process.env.NFL_DEFENSIVE_REVOKE==='true')return evaluateDefensiveActivation(null,null,context,now);
  return evaluateDefensiveActivation(parse<DefensiveActivation>(process.env.NFL_DEFENSIVE_ACTIVATION),
    parse<DefensiveQualification>(process.env.NFL_DEFENSIVE_QUALIFICATION),context,now);
}
