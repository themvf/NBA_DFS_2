"""Separate immutable market observations; archived pre-tip quotes are not certified closes."""
from collections import Counter,defaultdict
from datetime import datetime,timezone
from statistics import mean
import json

from model.nba_points_ladder import player_key,team_key,read_player
from research.nba_points_forward import aware,digest
from research.nba_leader_evaluation import finite,identity

VERSION='nba-points-market-benchmarks-v1'
BASELINES=('frozen_line','contest_lock_proxy','game_close_proxy')

def normalize(value):return json.loads(json.dumps(value,default=str))

def teams(row):return (team_key(row['home_team']),team_key(row['away_team']))

def choose(records,cutoff,official_start):
    audit=Counter();valid=[]
    for row in records:
        try:
            captured=aware(row['captured_at']);start=min(official_start,aware(row['commence_time']))
        except (ValueError,TypeError,KeyError):audit['missing_or_invalid_timestamp']+=1;continue
        if captured>=min(cutoff,start):audit['at_or_after_cutoff']+=1;continue
        books=row.get('books') or {};allowed={};missing_update=[]
        for key,book in books.items():
            if not isinstance(book,dict):audit['invalid_book']+=1;continue
            stamp=book.get('lastUpdate')
            if stamp:
                try:
                    if aware(stamp)>captured: audit['provider_update_after_capture']+=1;continue
                except (ValueError,TypeError):audit['invalid_provider_timestamp']+=1;continue
            else:missing_update.append(key)
            allowed[key]=book
        reading=read_player(allowed,{})
        if reading is None:audit['no_readable_main_line']+=1;continue
        valid.append((captured,row,allowed,reading,missing_update))
    if not valid:return {'status':'missing_pretip_quote','line':None,'rejected':dict(audit)}
    latest=max(v[0] for v in valid);tied=[v for v in valid if v[0]==latest]
    signatures={digest(v[2]) for v in tied}
    if len(signatures)>1:return {'status':'ambiguous_latest_capture','line':None,'rejected':dict(audit)}
    captured,row,books,reading,missing=tied[0]
    updates=[aware(b['lastUpdate']) for b in books.values() if b.get('lastUpdate')]
    return {'status':'last_archived_pretip','line':reading.line,'captured_at':captured.isoformat(),
            'selection_cutoff':cutoff.isoformat(),'minutes_before_scheduled_tip':(official_start-captured).total_seconds()/60,
            'minutes_before_cutoff':(cutoff-captured).total_seconds()/60,
            'within_15_minutes_of_tip':0<(official_start-captured).total_seconds()<=900,
            'verified_closing_line':False,'source_row_ids':[v[1].get('id') for v in tied],
            'capture_key':row.get('capture_key'),'market_commence_time':str(row['commence_time']),
            'book_provider_timestamps_missing':missing,'earliest_provider_update':min(updates).isoformat() if updates else None,
            'latest_provider_update':max(updates).isoformat() if updates else None,'books':books,'rejected':dict(audit)}

def statistics(rows,key):
    paired=[r for r in rows if r['outcome_status'] in ('graded','push') and finite(r['actual_points']) and finite(r[key].get('line'))]
    counts=Counter('over' if r['actual_points']>r[key]['line'] else 'under' if r['actual_points']<r[key]['line'] else 'push' for r in paired)
    nonpush=counts['over']+counts['under'];ages=[r[key]['minutes_before_scheduled_tip'] for r in paired if 'minutes_before_scheduled_tip' in r[key]]
    return {'n':len(paired),'over':counts['over'],'under':counts['under'],'push':counts['push'],
            'over_rate_all_graded':counts['over']/len(paired) if paired else None,
            'over_rate_excluding_pushes':counts['over']/nonpush if nonpush else None,
            'minutes_before_scheduled_tip_min':min(ages) if ages else None,'minutes_before_scheduled_tip_max':max(ages) if ages else None,
            'within_15_minutes_of_tip':sum(r[key].get('within_15_minutes_of_tip',False) for r in paired),
            'verified_closing_line_count':sum(r[key].get('verified_closing_line',False) for r in paired)}

def build(snapshot,grade,history,games):
    candidates=snapshot['candidates'];counts=Counter(c['player_key'] for c in candidates)
    outcomes={identity(r):r for r in grade['rows']}
    if len(outcomes)!=len(grade['rows']):raise ValueError('duplicate outcome identity')
    if grade['snapshot_sha256']!=snapshot['sha256']:raise ValueError('grade references a different frozen snapshot')
    indexed=defaultdict(list);source_audit=Counter()
    for raw in history:
        r=normalize(raw)
        if r.get('sport')!='nba' or r.get('market')!='player_points':source_audit['wrong_sport_or_market']+=1;continue
        if r.get('game_date')!=snapshot['game_date']:source_audit['different_date']+=1;continue
        indexed[(str(r['event_id']),player_key(r['player']))].append(r)
    schedule=defaultdict(dict)
    for g in games:
        if g.start_time:schedule[(team_key(g.home_team),team_key(g.away_team))].setdefault(str(g.espn_event_id),[]).append(aware(g.start_time))
    for g in snapshot.get('slate_schedule',[]):
        if g.get('start_time'):schedule[teams(g)].setdefault(str(g['event_id']),[]).append(aware(g['start_time']))
    result=[]
    for c in candidates:
        r=outcomes[identity(c)];games_for_teams=schedule[teams(c)]
        row={k:c[k] for k in ['event_id','player_key','player']}
        row.update(outcome_status=r['status'],actual_points=r['actual_points'],predictions=c['predictions'],
                   frozen_line={'line':c['predictions']['market_line'],'captured_at':c['captured_at'],'snapshot_created_at':snapshot['created_at']})
        reason='ambiguous_player' if counts[c['player_key']]!=1 else 'missing_verified_schedule' if not games_for_teams else 'ambiguous_game' if len(games_for_teams)!=1 else None
        if reason:
            row.update({key:{'status':reason,'line':None} for key in BASELINES[1:]});result.append(row);continue
        espn_id,starts=next(iter(games_for_teams.items()));start=min(starts);records=[]
        for source in indexed[identity(c)]:
            if (team_key(source.get('home_team_name','')),team_key(source.get('away_team_name','')))!=teams(c):source_audit['event_team_mismatch']+=1;continue
            records.append(source)
        row.update(espn_event_id=espn_id,scheduled_start=start.isoformat(),
                   contest_lock_proxy=choose(records,min(start,aware(snapshot['lock_at'])),start),game_close_proxy=choose(records,start,start))
        result.append(row)
    paired=[r for r in result if all(finite(r[k].get('line')) for k in BASELINES)]
    model_errors={}
    for model in snapshot['models']:
        model_errors[model]={}
        for cohort in ['all','frozen_market_line_at_least20']:
            pool=[r for r in result if r['outcome_status'] in ('graded','push') and finite(r['actual_points']) and finite(r['predictions'].get(model)) and finite(r['game_close_proxy'].get('line')) and (cohort=='all' or r['frozen_line']['line']>=20)]
            error=lambda method:mean(abs((r['predictions'][model] if method=='model' else r[method]['line'])-r['actual_points']) for r in pool) if pool else None
            model_errors[model][cohort]={'n':len(pool),'model_mae':error('model'),'frozen_line_mae':error('frozen_line'),'game_close_proxy_mae':error('game_close_proxy')}
    output={'version':VERSION,'game_date':snapshot['game_date'],'created_at':datetime.now(timezone.utc).isoformat(),'snapshot_sha256':snapshot['sha256'],
            'timing_basis':'earliest verified scheduled start and row market commence time; capture strictly before cutoff',
            'interpretation':'Last archived pre-tip observations, not certified closing lines. Fifteen minutes is a descriptive freshness flag, not closing-line certification. Later-game observations can be after first-game contest lock.',
            'rows':result,'source_audit':dict(source_audit),'coverage':{key:dict(Counter(r[key].get('status','frozen') for r in result)) for key in BASELINES},
            'summary':{key:statistics(result,key) for key in BASELINES},'paired_same_players':{key:statistics(paired,key) for key in BASELINES},
            'paired_model_errors':model_errors,'outcome_exclusions':dict(Counter(r['outcome_status'] for r in result if r['outcome_status'] not in ('graded','push')))}
    output['sha256']=digest(output);return output

def markdown(report):
    labels={'frozen_line':'Original frozen line','contest_lock_proxy':'Last archived before contest lock','game_close_proxy':'Last archived before each game'}
    lines=['## Market line timing comparisons','','Archived quotes are pre-tip proxies, not verified closing lines. Game-specific quotes may contain information unavailable at the first-game contest lock.','',
           '| Baseline | Graded | Over | Under | Push | Over % | Quote age before tip (minutes) |','|---|---:|---:|---:|---:|---:|---|']
    for key,r in report['summary'].items():
        rate=f"{100*r['over_rate_all_graded']:.1f}%" if r['n'] else 'Unavailable'
        age=f"{r['minutes_before_scheduled_tip_min']:.1f}–{r['minutes_before_scheduled_tip_max']:.1f}" if r['minutes_before_scheduled_tip_min'] is not None else 'Original snapshot'
        lines.append(f"| {labels[key]} | {r['n']} | {r['over']} | {r['under']} | {r['push']} | {rate} | {age} |")
    lines+=['','Over % includes pushes in its denominator; no push counts as an over. DNP/unmatched results are excluded. Matching-cohort rates, per-player/per-book evidence and paired model errors are in market-benchmarks.json. No profitability or market-efficiency claim is implied.','']
    return '\n'.join(lines)
