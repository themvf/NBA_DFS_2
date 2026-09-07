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
