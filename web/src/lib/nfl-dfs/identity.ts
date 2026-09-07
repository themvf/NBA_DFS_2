/** Salary IDs identify roster entries, not verified permanent player identities. */
export function nflIdentityName(value: string): string {
  return value.normalize('NFKD').replace(/[\u0300-\u036f]/g, '').toLowerCase()
    .replace(/\b(jr|sr|ii|iii|iv)\b/g, '').replace(/[^a-z0-9]/g, '');
}

export function nflIdentityTeam(value: string | null): string {
  const team = (value ?? '').trim().toUpperCase();
  return ({LA:'LAR', WAS:'WSH', JAC:'JAX', AZ:'ARI'} as Record<string,string>)[team] ?? team;
}

export function nflIdentityPosition(value: string): string {
  const position = value.trim().toUpperCase();
  return ({FB:'RB', HB:'RB', PK:'K', DEF:'DST', 'D/ST':'DST'} as Record<string,string>)[position] ?? position;
}

type Identity = {name: string; team: string | null; position: string; gsisId?: string | null};
export type NflRosterIdentity = Identity & {aliases:string[]; registryStatus:'resolved'|'conflict'|'unregistered';
  localPlayerId?:number; fetchedAt?:string; claimDigests?:string[]; sourceGsisId?:string|null};
export type IdentityMethod = 'gsis_id' | 'exact_name_position_team' | 'team_position_dst'
  | 'team_conflict' | 'position_conflict' | 'ambiguous' | 'unmatched' | 'missing_team' | 'identifier_conflict';

export function nflIdentityLabel(method: string): string {
  return ({gsis_id:'Matched by permanent NFL ID', exact_name_position_team:'Matched by name, team and position',
    team_position_dst:'Defense matched by team', team_conflict:'Match blocked: different team',
    position_conflict:'Match blocked: different position', ambiguous:'Match blocked: multiple candidates',
    unmatched:'No matching player', missing_team:'Match blocked: missing team', identifier_conflict:'Match blocked: conflicting ID',
    exact_name_position:'Legacy match without team verification — reload salary CSV'} as Record<string,string>)[method]
    ?? 'Matching evidence unavailable — reload salary CSV';
}

/** Resolve aliases only through current-season roster candidates, never old teams. */
export function resolveNflRosterIdentity(incoming:Identity, candidates:readonly NflRosterIdentity[]): {
  gsisId:string|null; method:IdentityMethod;
} {
  const name=nflIdentityName(incoming.name);
  if(!nflIdentityTeam(incoming.team)) return {gsisId:null,method:'missing_team'};
  const named=candidates.filter(p=>name && [p.name,...p.aliases].some(n=>nflIdentityName(n)===name));
  if(!named.length)return {gsisId:null,method:'unmatched'};
  const onTeam=named.filter(p=>nflIdentityTeam(p.team)===nflIdentityTeam(incoming.team));
  if(!onTeam.length)return {gsisId:null,method:'team_conflict'};
  const compatible=onTeam.filter(p=>nflIdentityPosition(p.position)===nflIdentityPosition(incoming.position));
  if(!compatible.length)return {gsisId:null,method:'position_conflict'};
  if(compatible.some(p=>p.registryStatus!=='resolved'||!p.gsisId))return {gsisId:null,method:'identifier_conflict'};
  const ids=new Set(compatible.map(p=>p.gsisId!));
  return ids.size===1?{gsisId:[...ids][0],method:'gsis_id'}:{gsisId:null,method:'ambiguous'};
}

/** Duplicate local rows may share evidence only for the same permanent ID/team/position. */
export function nflIdentityLocalLinks(candidates:readonly NflRosterIdentity[]):Map<number,number[]> {
  const groups=new Map<string,Set<number>>();
  for(const p of candidates) {
    if(p.registryStatus!=='resolved'||!p.gsisId||p.localPlayerId==null||!nflIdentityTeam(p.team))continue;
    const key=`${p.gsisId}|${nflIdentityTeam(p.team)}|${nflIdentityPosition(p.position)}`;
    if(!groups.has(key))groups.set(key,new Set());
    groups.get(key)!.add(p.localPlayerId);
  }
  const links=new Map<number,number[]>();
  for(const ids of groups.values())for(const id of ids)links.set(id,[...ids].sort((a,b)=>a-b));
  return links;
}

export function assertUniqueNflSalaryIdentities(rows:readonly {name:string;gsisId?:string|null;localPlayerId?:number|null}[]):void {
  const seen=new Map<string,string>();
  for(const row of rows){
    const key=row.gsisId?`gsis:${row.gsisId}`:row.localPlayerId!=null?`local:${row.localPlayerId}`:null;
    if(!key)continue;
    if(seen.has(key))throw new Error(`Salary entries for ${seen.get(key)} and ${row.name} resolve to the same player. Correct the duplicate entries before importing.`);
    seen.set(key,row.name);
  }
}

/** Candidates must already be scoped to the intended season/run. Never pick first. */
export function matchNflIdentity<T extends Identity>(incoming: Identity, candidates: readonly T[]): {
  match: T | null; method: IdentityMethod;
} {
  const team = nflIdentityTeam(incoming.team);
  const position = nflIdentityPosition(incoming.position);
  if (!team) return {match:null, method:'missing_team'};
  const byId = incoming.gsisId ? candidates.filter(p=>p.gsisId===incoming.gsisId) : [];
  const named = position==='DST' ? candidates.filter(p=>nflIdentityPosition(p.position)==='DST')
    : candidates.filter(p=>nflIdentityName(incoming.name)!=='' && nflIdentityName(p.name)===nflIdentityName(incoming.name));
  if (incoming.gsisId && !byId.length) return {match:null, method:'identifier_conflict'};
  const selected = incoming.gsisId ? byId : named;
  if (!selected.length) return {match:null, method:'unmatched'};
  const onTeam = selected.filter(p=>nflIdentityTeam(p.team)===team);
  if (!onTeam.length) return {match:null, method:'team_conflict'};
  const compatible = onTeam.filter(p=>nflIdentityPosition(p.position)===position);
  if (!compatible.length) return {match:null, method:'position_conflict'};
  if (compatible.length!==1 || (incoming.gsisId && byId.length!==1)) return {match:null, method:'ambiguous'};
  return {match:compatible[0],method:incoming.gsisId ? 'gsis_id' : position==='DST' ? 'team_position_dst' : 'exact_name_position_team'};
}
