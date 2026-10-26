"""Verified postgame observations; none of these fields is a prediction input."""
from collections import Counter,defaultdict
from datetime import datetime,timezone
from zoneinfo import ZoneInfo

from model.nba_points_ladder import player_key,team_key
from research.nba_leader_evaluation import finite,identity


def number(value):
    try:return float(value) if finite(value) else None
    except (ValueError,TypeError):return None


def split_shots(value):
    try:
        made,attempts=str(value).split('-')
        return float(made),float(attempts)
    except (TypeError,ValueError):return None,None


def parse_summary(payload,event_id):
    header=payload.get('header',{});comps=header.get('competitions',[])
    if len(comps)!=1:raise ValueError('summary does not identify exactly one game')
    comp=comps[0]
    identifiers=[str(v) for v in [header.get('id'),comp.get('id')] if v is not None]
    if not identifiers or any(v!=str(event_id) for v in identifiers):raise ValueError('summary event identity mismatch')
    if not comp.get('status',{}).get('type',{}).get('completed'):raise ValueError('postgame diagnostics require final game')
    sides={r['homeAway']:r for r in comp['competitors']}
    home,away=sides['home']['team']['displayName'],sides['away']['team']['displayName']
    day=datetime.fromisoformat(comp['date'].replace('Z','+00:00')).astimezone(ZoneInfo('America/New_York')).date().isoformat()
    players=[];seen=set();team_scores=defaultdict(float)
    for block in payload.get('boxscore',{}).get('players',[]):
        team=block['team']['displayName']
        if team_key(team) not in [team_key(home),team_key(away)]:raise ValueError('summary box team mismatch')
        groups=block.get('statistics',[])
        if not groups:raise ValueError('missing box statistics')
        group=groups[0]
        for raw in group.get('athletes',[]):
            athlete=raw['athlete'];aid=str(athlete['id'])
            if aid in seen:raise ValueError('duplicate summary athlete')
            seen.add(aid)
            stats=dict(zip(group['labels'],raw.get('stats',[])))
            points=number(stats.get('PTS'))
            played=not raw.get('didNotPlay',False) and points is not None
            fgm,fga=split_shots(stats.get('FG'));fg3m,fg3a=split_shots(stats.get('3PT'));ftm,fta=split_shots(stats.get('FT'))
            minute_text=str(stats.get('MIN',''))
            if ':' in minute_text:
                try:m,s=minute_text.split(':');minutes=float(m)+float(s)/60
                except ValueError:minutes=None
            else:minutes=number(minute_text)
            mix=None
            if played and all(v is not None for v in [fgm,fga,fg3m,fg3a,ftm,fta]):
                if (not (0<=fg3m<=fgm<=fga and 0<=fg3m<=fg3a<=fga and 0<=ftm<=fta)
                    or abs(2*fgm+fg3m+ftm-points)>1e-6):raise ValueError('box shot/point accounting mismatch')
                mix={'two_point':2*(fgm-fg3m),'three_point':3*fg3m,'free_throw':ftm}
            if played:team_scores[team_key(team)]+=points
            players.append({'athlete_id':aid,'player':athlete['displayName'],'player_key':player_key(athlete['displayName']),
                            'team':team,'played':played,'points':points,'minutes':minutes,'minute_source':minute_text,
                            'fgm':fgm,'fga':fga,'fg3m':fg3m,'fg3a':fg3a,'ftm':ftm,'fta':fta,
                            'fouls':number(stats.get('PF')),'scoring_components':mix})
    for side in ['home','away']:
        expected=number(sides[side].get('score'));team=sides[side]['team']['displayName']
        if expected is None or abs(team_scores[team_key(team)]-expected)>1e-6:raise ValueError('final team/player scores do not reconcile')
    periods=defaultdict(lambda:defaultdict(float));plays=payload.get('plays',[]);ids=set();valid_plays=bool(plays)
    for play in plays:
        pid=play.get('id',play.get('sequenceNumber'))
        if pid is None or str(pid) in ids:valid_plays=False
        ids.add(str(pid))
        if not play.get('scoringPlay'):continue
        participants=play.get('participants',[]);value=number(play.get('scoreValue'));period=play.get('period',{}).get('number')
        if not participants or value is None or period is None or int(period)<1:valid_plays=False;continue
        aid=str(participants[0]['athlete']['id'])
        if aid not in seen:valid_plays=False;continue
        periods[aid][int(period)]+=value
    # A partial feed must not quietly turn unobserved scoring into zero OT points.
    if any(p['played'] and abs(sum(periods[p['athlete_id']].values())-p['points'])>1e-6 for p in players):valid_plays=False
    for p in players:
        p['period_status']='reconciled' if valid_plays and p['played'] else 'unavailable_or_unreconciled'
        p['regulation_points']=sum(v for k,v in periods[p['athlete_id']].items() if k<=4) if valid_plays and p['played'] else None
        p['overtime_points']=sum(v for k,v in periods[p['athlete_id']].items() if k>4) if valid_plays and p['played'] else None
    return {'espn_event_id':str(event_id),'game_date':day,'home_team':home,'away_team':away,'players':players,
            'period_data_reconciled':valid_plays,'source_url':'https://site.api.espn.com/apis/site/v2/sports/basketball/nba/summary?event='+str(event_id)}


def assemble(snapshot,grade,summaries,captured_at=None):
    by_teams=defaultdict(list)
    for event,payload in summaries.items():
        game=parse_summary(payload,event)
        if game['game_date']!=snapshot['game_date']:raise ValueError('diagnostic game date differs from forecast slate')
        by_teams[(team_key(game['home_team']),team_key(game['away_team']))].append(game)
    outcomes={identity(r):r for r in grade['rows']};rows=[];counts=Counter()
    for c in snapshot['candidates']:
        outcome=outcomes[identity(c)];matches=by_teams[(team_key(c['home_team']),team_key(c['away_team']))]
        row={'event_id':c['event_id'],'player_key':c['player_key'],'player':c['player'],'status':'missing_summary'}
        if len(matches)==1:
            game=matches[0];hits=[p for p in game['players'] if p['player_key']==c['player_key']]
            if len(hits)==1:
                p=hits[0]
                if finite(outcome.get('actual_points')) and (not p['played'] or p['points']!=outcome['actual_points']):raise ValueError('diagnostic points disagree with the graded outcome')
                row.update(status='verified' if p['played'] else 'dnp',source_url=game['source_url'],espn_event_id=game['espn_event_id'],
                           actual=p,opportunity_forecasts=c.get('opportunity_forecasts',{}),
                           frozen_scoring_components=c.get('opportunity_scoring_components'),
                           foul_limit_reached=(p['fouls']>=6) if p['fouls'] is not None else None)
                row['opportunity_differences']={key:p[key]-value for key,value in c.get('opportunity_forecasts',{}).items() if key in p and finite(p[key]) and finite(value)}
                rates=c.get('opportunity_conversion_rates');forecast=c['predictions'].get('opportunity_points')
                if p['played'] and rates and finite(forecast) and all(finite(p[k]) for k in ['fga','fg3a','fta']):
                    conditional=2*(p['fga']-p['fg3a'])*rates['two']+3*p['fg3a']*rates['three']+p['fta']*rates['free']
                    row['postgame_decomposition']={'points_at_actual_attempts_frozen_rates':conditional,'attempt_mix_difference':conditional-forecast,
                                                   'conversion_residual':p['points']-conditional,'total_forecast_error':p['points']-forecast}
                else:row['postgame_decomposition']=None
            else:row['status']='not_listed_in_final_box' if not hits else 'ambiguous_player'
        elif len(matches)>1:row['status']='ambiguous_game'
        counts[row['status']]+=1;rows.append(row)
    return {'version':'nba-points-diagnostics-v1','captured_at':captured_at or datetime.now(timezone.utc).isoformat(),
            'snapshot_sha256':snapshot['sha256'],'game_date':snapshot['game_date'],'rows':rows,'coverage':dict(counts),
            'limitation':'Postgame observations and arithmetic, not causal explanations or pregame signals. OT/foul notes overlap with opportunity; do not add them as independent effects.'}
