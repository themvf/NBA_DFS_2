"""Freeze experimental player-points forecasts and grade paired market comparisons.

Read-only warehouse access. Immutable local artifacts; no production forecasts change.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
import json
import math
from pathlib import Path

from research.nba_points_research_grade import Capture, grade_captures, load_box_scores
from model.nba_points_ladder import player_key
from research.nba_points_forward import VERSION as MARKET_VERSION, aware, digest, write_new

VERSION = 'nba-points-ghost-v1'
MODELS = {
    'history20': 'Mean of up to 20 prior regular-season appearances; minimum 10; across seasons and trades.',
    'blend90_10': 'Market line + clip(0.10 * (history20 - market line), -2, +2). Fixed exploratory weights, not fitted.',
}


def verify(snapshot, version):
    if snapshot.get('version') != version or snapshot.get('sha256') != digest({k:v for k,v in snapshot.items() if k!='sha256'}):
        raise ValueError('snapshot version or digest mismatch')


def freeze(market, history, created):
    verify(market, MARKET_VERSION)
    created = aware(created)
    if created < aware(market['created_at']):
        raise ValueError('ghost creation precedes market snapshot')
    starts=[aware(g['start_time']) for g in market.get('espn_scheduled_starts',[]) if g.get('start_time')]
    if starts and aware(market['lock_at'])>min(starts):
        raise ValueError('common lock is later than earliest known scheduled start')
    cutoff = min(created, aware(market['lock_at']))
    # Snapshot observations must have been in the warehouse by this decision time.
    groups = defaultdict(list)
    seen = set()
    for r in history:
        if str(r['game_date']) >= market['game_date'] or aware(r['fetched_at']) >= cutoff:
            continue
        if r['season_type'] != 'Regular Season' or not r.get('minutes') or r['minutes'] <= 0 or r.get('points') is None:
            continue
        if not math.isfinite(float(r['points'])):
            raise ValueError('nonfinite history points')
        identity = (r['player_id'], str(r['game_id']))
        if identity in seen:
            raise ValueError('duplicate player-game history')
        seen.add(identity)
        groups[player_key(r['name'])].append(dict(r))
    candidates=[]
    for c in market['candidates']:
        history_rows=groups[c['player_key']]
        ids={r['player_id'] for r in history_rows}
        status='ambiguous_history' if len(ids)>1 else 'insufficient_history'
        chosen=sorted(history_rows,key=lambda r:(str(r['game_date']),str(r['game_id'])))[-20:] if len(ids)==1 else []
        value=sum(float(r['points']) for r in chosen)/len(chosen) if len(chosen)>=10 else None
        predictions={'market_line':c['line'], 'history20':value, 'blend90_10':None}
        if value is not None:
            status='ready'
            predictions['blend90_10']=c['line']+max(-2,min(2,.10*(value-c['line'])))
        candidates.append({**c,'predictions':predictions,'history_status':status,'history':chosen})
    result={'version':VERSION,'created_at':created.isoformat(),'game_date':market['game_date'],
            'lock_at':market['lock_at'],'market_as_of':market['as_of'],'history_as_of':cutoff.isoformat(),
            'mode':'prospective' if created<aware(market['lock_at']) and market['mode']=='prospective' else 'retrospective_reconstruction',
            'market_snapshot_sha256':market['sha256'],'models':MODELS,'candidates':candidates,
            'history_source':'nba_player_game_logs only; no box-score history supplementation',
            'latest_history_game_date':max((str(r['game_date']) for c in candidates for r in c['history']),default=None),
            'limitations':['Experimental forecasts; no demonstrated improvement over market lines.',
                           'Historical inputs are available-by-decision warehouse records, not original historical pregame captures.',
                           'History crosses offseasons; current role, availability and expected minutes are unverified.',
                           'Market line is a benchmark, not an estimated mean. DNPs excluded and reported separately.',
                           'Observed market pool can omit players. Primary cohort is frozen market line >=20.']}
    result['sha256']=digest(result)
    return result


def metrics(rows):
    result={}
    for model in MODELS:
        paired=[r for r in rows if r['status'] in ('graded','push') and r['actual_points'] is not None and r['predictions'].get(model) is not None]
        def score(key):
            errors=[r['predictions'][key]-r['actual_points'] for r in paired]
            return {'n':len(errors),'mae':sum(abs(e) for e in errors)/len(errors) if errors else None,
                    'rmse':math.sqrt(sum(e*e for e in errors)/len(errors)) if errors else None,
                    'bias':sum(errors)/len(errors) if errors else None}
        ghost,baseline=score(model),score('market_line')
        result[model]={'ghost':ghost,'paired_market':baseline,
                      'mae_change':ghost['mae']-baseline['mae'] if paired else None}
    return result


def grade(snapshot, games):
    verify(snapshot,VERSION)
    if games and any(g.start_time and g.start_time<aware(snapshot['lock_at']) for g in games):
        raise ValueError('common lock is later than a verified scheduled start; timing review required')
    rows=[]
    for c in snapshot['candidates']:
        cap=Capture(c['event_id'],aware(c['commence_time']),aware(c['captured_at']),c['home_team'],c['away_team'],c['player'],c['main_books'])
        outcome=grade_captures(date.fromisoformat(snapshot['game_date']),[cap],{},games)[0]
        rows.append({'event_id':c['event_id'],'player_key':c['player_key'],'player':c['player'],
                     'status':outcome.status,'actual_points':outcome.actual_points,'actual_minutes':outcome.minutes,
                     'predictions':c['predictions'],'history_status':c['history_status']})
    from research.nba_leader_evaluation import evaluate
    return {'version':VERSION,'game_date':snapshot['game_date'],'snapshot_sha256':snapshot['sha256'],
            'snapshot_mode':snapshot['mode'],'graded_at':datetime.now(timezone.utc).isoformat(),
            'models':snapshot['models'],'rows':rows,'by_status':dict(Counter(r['status'] for r in rows)),
            'leaderboards':evaluate(snapshot,rows),
            'all_players':metrics(rows),'market_line_at_least20':metrics([r for r in rows if r['predictions']['market_line']>=20])}


def report(grades):
    from research.nba_leader_evaluation import aggregate
    days=set(); rows=[]; excluded=[]
    for g in grades:
        if g.get('version')!=VERSION or g.get('models')!=MODELS:
            raise ValueError('incompatible model version')
        if g['snapshot_mode']!='prospective':
            excluded.append(g['game_date']); continue
        if g['game_date'] in days:
            raise ValueError('multiple snapshots for one date; choose one before comparing')
        days.add(g['game_date']); rows.extend(g['rows'])
    return {'dates':sorted(days),'retrospective_dates_excluded':excluded,'by_status':dict(Counter(r['status'] for r in rows)),
            'leaderboards':aggregate(grades),
            'all_players':metrics(rows),'market_line_at_least20':metrics([r for r in rows if r['predictions']['market_line']>=20]),
            'interpretation':'Descriptive paired errors only; games and dates are clustered. No promotion or validated edge implied.'}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    sub=parser.add_subparsers(dest='action',required=True)
    for action in ['freeze','grade']:
        p=sub.add_parser(action);p.add_argument('--snapshot',type=Path,required=True);p.add_argument('--output',type=Path,required=True)
    p=sub.add_parser('report');p.add_argument('--grades',type=Path,nargs='+',required=True);p.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    if args.action=='report':
        result=report([json.loads(p.read_text(encoding='utf-8')) for p in args.grades])
    else:
        import psycopg2
        from psycopg2.extras import RealDictCursor
        from config import load_config
        from db.database import DatabaseManager
        snapshot=json.loads(args.snapshot.read_text(encoding='utf-8'))
        db=DatabaseManager(load_config().database_url,initialize_schema=False)
        with psycopg2.connect(db.database_url,cursor_factory=RealDictCursor) as conn:
            conn.set_session(readonly=True);db._shared_connection=conn
            try:
                if args.action=='freeze':
                    created=datetime.now(timezone.utc)
                    with conn.cursor() as cur:
                        cur.execute('''SELECT player_id,name,game_id,game_date,season,season_type,team_id,minutes,points,fga,fta,fetched_at
                            FROM nba_player_game_logs WHERE game_date < %s AND season_type='Regular Season'
                            AND minutes>0 AND points IS NOT NULL AND fetched_at < %s''',
                                    (snapshot['game_date'],min(created,aware(snapshot['lock_at']))))
                        history=[dict(r) for r in cur.fetchall()]
                    result=freeze(snapshot,history,created)
                else:
                    result=grade(snapshot,load_box_scores(db,date.fromisoformat(snapshot['game_date'])))
            finally: db._shared_connection=None
    # Serialize dates identically for the content digest and subsequent readers.
    result=json.loads(json.dumps(result,default=str))
    write_new(args.output,result)
    print(json.dumps({'output':str(args.output),'mode':result.get('mode',result.get('snapshot_mode')),
                      'players':len(result.get('candidates',result.get('rows',[]))),
                      'history_status':dict(Counter(c['history_status'] for c in result.get('candidates',[])))},indent=2))


if __name__=='__main__': main()
