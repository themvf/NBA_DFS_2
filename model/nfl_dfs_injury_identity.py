"""Conservative injury identity reconciliation; no fuzzy matching or roster mutation."""
import re
import unicodedata
from collections import Counter

ALIASES={'WAS':'WSH','JAC':'JAX','LA':'LAR','AZ':'ARI'}
NFL_TEAMS=set('ARI ATL BAL BUF CAR CHI CIN CLE DAL DEN DET GB HOU IND JAX KC LAC LAR LV MIA MIN NE NO NYG NYJ PHI PIT SEA SF TB TEN WSH'.split())


def team(value):
    value=str(value or '').upper()
    return ALIASES.get(value,value)


def name(value):
    value=unicodedata.normalize('NFKD',value or '').encode('ascii','ignore').decode()
    value=re.sub(r'\b(jr|sr|ii|iii|iv)\.?\b','',value,flags=re.I)
    return re.sub('[^a-z0-9]','',value.lower())


def reconcile(row, players):
    base={'source_id':row.get('player_id'),'name':row.get('name',row.get('player_name')),
          'team':row.get('team_id'),'position':row.get('position_id'),'source':row}
    def result(category,candidate=None,method=None):
        return {**base,'category':category,'player_id':candidate['id'] if candidate else None,
                'canonical_name':candidate.get('canonical_name') if candidate else None,'method':method}
    if row.get('position_id') not in {'QB','RB','WR','TE','K'}:
        return result('outside_skill_pool')
    if team(row.get('team_id')) not in NFL_TEAMS:
        return result('provider_nonteam')
    selected=[]
    method=None
    for field,key,label in [('fantasypros_player_id','player_id','fantasypros_id'),('yahoo_id','yahoo_id','yahoo_id')]:
        value=row.get(key)
        matches=[p for p in players if value is not None and str(value) not in {'','0'} and str(p.get(field))==str(value)]
        if matches:
            selected=matches;method=label;break
    if not selected:
        selected=[p for p in players if name(p.get('canonical_name') or p.get('normalized_name'))==name(base['name']) and name(base['name'])]
        method='exact_name_team_position'
    if not selected:
        return result('missing_identity')
    matching=[p for p in selected if team(p.get('team_abbrev'))==team(row.get('team_id')) and p.get('position')==row.get('position_id')]
    if len(matching)>1 or (method!='exact_name_team_position' and len(selected)>1):
        return result('ambiguous')
    if not matching:
        return result('position_conflict' if any(team(p.get('team_abbrev'))==team(row.get('team_id')) for p in selected) else 'team_conflict')
    candidate=matching[0]
    if method!='fantasypros_id' and candidate.get('fantasypros_player_id') is not None and str(candidate['fantasypros_player_id'])!=str(row.get('player_id')):
        return result('identifier_conflict')
    return result('matched',candidate,method)


def audit(rows,players):
    ids=[str(r.get('player_id')) for r in rows]
    if len(ids)!=len(set(ids)):
        raise ValueError('Duplicate provider player identity')
    decisions=[reconcile(r,players) for r in rows]
    matched=[r['player_id'] for r in decisions if r['category']=='matched']
    if len(matched)!=len(set(matched)):
        raise ValueError('Multiple provider identities resolve to one canonical player')
    return {'counts':dict(Counter(r['category'] for r in decisions)), 'decisions':decisions}
