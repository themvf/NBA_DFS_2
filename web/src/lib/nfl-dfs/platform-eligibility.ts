import { createHash } from 'node:crypto';

export const PLATFORM_ELIGIBILITY_VERSION='platform-slate-eligibility-v1';

export type PlatformEligibilityDecision={
  version:typeof PLATFORM_ELIGIBILITY_VERSION;
  platform:'draftkings';
  slateId:string;
  decisionAt:string;
  availableAt:string;
  state:'ELIGIBLE'|'INELIGIBLE';
  status:string|null;
  reason:string;
  sourceRecordId:number;
  manifestDigest:string;
};

export function buildDraftKingsEligibilityManifest(input:{slateId:string;decisionAt:string;fileDigest:string;
  players:readonly {dkPlayerId:number;status:string|null;isOut:boolean}[]}) {
  const decisions=input.players.map<Omit<PlatformEligibilityDecision,'manifestDigest'>>(player=>({version:PLATFORM_ELIGIBILITY_VERSION,platform:'draftkings',
    slateId:input.slateId,decisionAt:input.decisionAt,availableAt:input.decisionAt,
    state:player.isOut?'INELIGIBLE':'ELIGIBLE',status:player.status,
    reason:player.isOut?`DraftKings salary file reports ${player.status||'OUT'}.`:'DraftKings salary file does not mark this entry OUT.',
    sourceRecordId:player.dkPlayerId})).sort((a,b)=>a.sourceRecordId-b.sourceRecordId);
  const body={version:PLATFORM_ELIGIBILITY_VERSION,platform:'draftkings' as const,slateId:input.slateId,
    decisionAt:input.decisionAt,fileDigest:input.fileDigest,decisions};
  const digest=createHash('sha256').update(JSON.stringify(body)).digest('hex');
  return {digest,decisions:decisions.map(decision=>({...decision,manifestDigest:digest})) as PlatformEligibilityDecision[]};
}
