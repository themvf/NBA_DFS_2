"""Read-only NHL audit by default; explicit --repair appends corrected grades.

No scans, notifications, paid API calls, schema initialization or entry edits.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import random
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

from config import load_config
from db.database import DatabaseManager
from model.nhl_signal_audit import GRADING_VERSION, load_grade, utc, decimal, pair_probability

PROTOCOL_PATH=Path(__file__).resolve().parents[1]/"docs/nhl-forward-validation-v1.json"
ENTRY_FIELDS=("id","created_at","sport","matchup_id","game_date","matchup","commence_time","alert_type","side",
              "capture_key","alert_prob","sharp_prob","details_json","signal_version","origin",
              "trigger_history_id","previous_history_id","opening_history_id","dedupe_key")

def digest(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,default=str,separators=(",",":")).encode()).hexdigest()

def entries(rows):
    return {r['id']:digest({k:r.get(k) for k in ENTRY_FIELDS}) for r in rows}

def steam_interval(minutes):
    if minutes is None or minutes <= 0:return 'unknown'
    return '<=30m' if minutes <= 30 else '>30-180m' if minutes <= 180 else '>180m'

def snapshot(db):
    return {'captured_at':datetime.now(timezone.utc).isoformat(),
            'alerts':db.execute("SELECT * FROM line_alerts WHERE sport='nhl' ORDER BY id"),
            'grades':db.execute("SELECT g.* FROM alert_grades g JOIN line_alerts a ON a.id=g.alert_id WHERE a.sport='nhl' ORDER BY g.id"),
            'games':db.execute("SELECT * FROM nhl_matchups ORDER BY id"),
            'closes':db.execute("SELECT * FROM event_closing_lines WHERE sport='nhl' ORDER BY id")}

def verify_preservation(before,after):
    old,new=entries(before['alerts']),entries(after['alerts'])
    if any(new.get(k)!=v for k,v in old.items()):
        raise RuntimeError('Original NHL alert entry changed or disappeared')
    new_grades={g['id']:g for g in after['grades']}
    for grade in before['grades']:
        fields={k:v for k,v in grade.items() if k!='is_current'}
        latest=new_grades.get(grade['id'])
        if latest is None or digest(fields)!=digest({k:v for k,v in latest.items() if k!='is_current'}):
            raise RuntimeError('Prior NHL grade changed or disappeared')
    counts=Counter(g['alert_id'] for g in after['grades'] if g['is_current'])
    if any(n!=1 for n in counts.values()):
        raise RuntimeError('Multiple current NHL grades')
    return {'original_entries_preserved':len(old),'prior_grades_preserved':len(before['grades']),
            'new_grades':len(after['grades'])-len(before['grades'])}

def forward_report(db, snap, protocol_path=PROTOCOL_PATH):
    protocol=json.loads(protocol_path.read_text(encoding='utf-8'))
    from model import line_alerts
    thresholds={'pinnacle_gap_pp':line_alerts._PIN_GAP_MIN_PP,'dk_ev':line_alerts._DK_VALUE_MIN_EV,
                'steam_books':line_alerts._STEAM_MIN_BOOKS,'steam_move_pp':line_alerts._STEAM_MIN_MOVE_PP,
                'walking_pp':line_alerts._WALK_MIN_PP,'dk_max_decimal':line_alerts._DK_VALUE_MAX_DECIMAL}
    if thresholds!=protocol['thresholds']:
        raise RuntimeError('NHL detector thresholds drifted from the registered protocol')
    start=utc(protocol['starts_at']); games={g['id']:g for g in snap['games']}
    eligible=[g for g in games.values() if utc(g['commence_time']) and utc(g['commence_time'])>=start
              and g['game_type'] in (2,3) and g['schedule_state']=='OK']
    alerts=[a for a in snap['alerts'] if a['origin']=='prospective' and utc(a['created_at'])>=start
            and a['matchup_id'] in {g['id'] for g in eligible} and a['alert_type'] in protocol['signal_types']]
    selected={}
    for a in sorted(alerts,key=lambda a:(a['created_at'],a['id'])):
        key=(a['alert_type'],a['matchup_id'])
        if key not in selected:selected[key]=a
    result={'protocol':protocol,'protocol_sha256':digest(protocol),'status':'collecting',
            'eligible_games':len(eligible),'completed_eligible_games':sum(g['completed'] for g in eligible),
            'all_forward_alerts':len(alerts),'families':{},'forecast_basis':'market reference; no signal probability model',
            'goalies':'not_captured','injuries':'not_captured','promotion':'manual review required; no recommendations enabled'}
    for kind in protocol['signal_types']:
        rows=[]
        for (family,gid),a in selected.items():
            if family!=kind:continue
            g=games[gid]; grade=load_grade(db,a); meta=grade['grading_json']
            trigger=db.execute_one("SELECT * FROM game_odds_history WHERE id=%s AND sport='nhl'",(a['trigger_history_id'],))
            b=((trigger or {}).get('books') or {}).get((a['details_json'] or {}).get('exec_book')) or {}
            item={'alert_id':a['id'],'matchup_id':gid,'side':a['side'],'created_at':str(a['created_at']),
                  'quote_status':meta['timing']['quote_status'],'entry_verified':meta['entry_verified'],
                  'signal_version':a.get('signal_version'), 'game_date':str(g['game_date']),
                  'interval_minutes':(a['details_json'] or {}).get('interval_minutes'),
                  'price_clv_pct':grade['dk_clv_pct'],'close_history_id':meta['close_history_id'],
                  'outcome':None,'paper_units':None,'favorite_benchmark_units':None,
                  'selected_market_probability':pair_probability(b,a['side']),'market_brier':None}
            if g['completed'] and g['home_score'] is not None and g['away_score'] is not None and g['home_score']!=g['away_score']:
                won=(g['home_score']>g['away_score'])==(a['side']=='home')
                item['outcome']='won' if won else 'lost'
                if meta['entry_verified']:
                    item['paper_units']=meta['entry_decimal']-1 if won else -1
                p=pair_probability(b,'home')
                if p is not None:
                    favorite='home' if p>=.5 else 'away'; price=decimal(b.get('ml_'+favorite))
                    fw=(g['home_score']>g['away_score'])==(favorite=='home')
                    item['favorite_benchmark_units']=price-1 if fw else -1
                    item['market_brier']=(item['selected_market_probability']-int(won))**2
            rows.append(item)
        priced=[r for r in rows if r['paper_units'] is not None]
        primary=[r for r in priced if r['quote_status']=='fresh' and r['signal_version']==protocol['signal_version']]
        paired=[r for r in primary if r['favorite_benchmark_units'] is not None]
        clv=[r for r in primary if r['price_clv_pct'] is not None]
        def interval(population,key):
            if len(population)<2:return None
            rng=random.Random(20261012)
            values=sorted(sum(rng.choice(population)[key] for _ in population)/len(population) for _ in range(2000))
            return [values[50],values[1950]]
        differences=[{**r,'difference':r['paper_units']-r['favorite_benchmark_units']} for r in paired]
        dates=len({r['game_date'] for r in primary})
        floor=(len(primary)>=protocol['review_floor']['settled_independent_games_per_family']
               and dates>=protocol['review_floor']['distinct_game_dates'])
        result['families'][kind]={'unique_games':len(rows),'settled':sum(r['outcome'] is not None for r in rows),
                                 'priced_settled':len(priced),'paper_units':sum(r['paper_units'] for r in priced),
                                 'quote_status':dict(Counter(r['quote_status'] for r in rows)),
                                 'price_clv_n':sum(r['price_clv_pct'] is not None for r in rows),
                                 'primary_fresh_settled_games':len(primary),'primary_game_dates':dates,
                                 'primary_paper_roi':sum(r['paper_units'] for r in primary)/len(primary) if primary else None,
                                 'primary_roi_interval':interval(primary,'paper_units'),
                                 'primary_price_clv_n':len(clv),
                                 'primary_price_clv_mean':sum(r['price_clv_pct'] for r in clv)/len(clv) if clv else None,
                                 'primary_price_clv_interval':interval(clv,'price_clv_pct'),
                                 'paired_benchmark_games':len(paired),
                                 'paired_difference_interval':interval(differences,'difference'),
                                 'review_floor_met':floor,'status':'review_required' if floor else 'collecting',
                                 'steam_subsets':{label:[r['alert_id'] for r in rows if steam_interval(r['interval_minutes'])==label]
                                    for label in protocol['steam_intervals']} if kind=='steam' else None,
                                 'rows':rows}
    # All eligible games are listed, including those with no alert or no close.
    closeids={c['matchup_id']:c['history_id'] for c in snap['closes']}
    result['game_coverage']=[{'matchup_id':g['id'],'nhl_game_id':g['nhl_game_id'],'commence_time':str(g['commence_time']),
                            'completed':g['completed'],'close_history_id':closeids.get(g['id']),
                            'pre_registration_alert_count':sum(a['matchup_id']==g['id'] and utc(a['created_at'])<start for a in snap['alerts']),
                            'alert_count':sum(a['matchup_id']==g['id'] for a in alerts)} for g in eligible]
    return result

def run(db, output, *, repair=False):
    output=Path(output);output.parent.mkdir(parents=True,exist_ok=True)
    before=snapshot(db)
    report={'as_of':before['captured_at'],'grading_version':GRADING_VERSION,'mode':'repair' if repair else 'read_only',
            'dry_run':[]}
    for a in before['alerts']:
        if utc(a['commence_time'])<=utc(before['captured_at']):
            g=load_grade(db,a);report['dry_run'].append({'id':a['id'],'old_price_clv':a['dk_clv_pct'],
                'new_price_clv':g['dk_clv_pct'],'grade':g})
    if repair:
        backup=output.with_name(output.stem+'-before.json')
        with backup.open('x',encoding='utf-8') as f:json.dump(before,f,default=str,indent=2)
        from model.line_alerts import settle
        report['processed']=settle(db,'nhl',regrade_nhl=True)
        after=snapshot(db);report['preservation']=verify_preservation(before,after)
        # Repeat once to verify that unchanged source evidence does not append grades.
        settle(db,'nhl',regrade_nhl=True)
        second=snapshot(db);verify_preservation(after,second)
        stable_ids={a['id'] for a in after['alerts'] if a['outcome'] is not None}
        old_counts=Counter(g['alert_id'] for g in after['grades'] if g['alert_id'] in stable_ids)
        new_counts=Counter(g['alert_id'] for g in second['grades'] if g['alert_id'] in stable_ids)
        if old_counts!=new_counts:raise RuntimeError('Settled-grade repeat was not idempotent')
        report['idempotent_settled_grades']=True
        report['after']=second; before=second
    report['forward']=forward_report(db,before)
    output.write_text(json.dumps(report,default=str,indent=2),encoding='utf-8')
    return report

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--repair',action='store_true')
    args=parser.parse_args()
    db=DatabaseManager(load_config().database_url,initialize_schema=False)
    with db.reuse_connection():
        if not args.repair: db._shared_connection.set_session(readonly=True)
        result=run(db,args.output,repair=args.repair)
    print(json.dumps({k:v for k,v in result.items() if k not in ('dry_run','after','forward')},default=str))
    print('NHL forward protocol:',result['forward']['status'],'eligible games:',result['forward']['eligible_games'])

if __name__=='__main__':main()
