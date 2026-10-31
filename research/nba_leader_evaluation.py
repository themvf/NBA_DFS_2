"""Deterministic scoring-leader diagnostics from unchanged frozen forecasts.

Three distinct observed market players; no salary, payout or contest eligibility
is implied. Missing results never turn into zero-point scores or replacement picks.
"""
from collections import Counter,defaultdict
import math

EVALUATION_VERSION='nba-leader-evaluation-v1'
TIE_RULE='predicted points descending, market line descending, canonical player key, event ID'


def finite(value):
    try:return value is not None and not isinstance(value,bool) and math.isfinite(float(value))
    except (ValueError,TypeError):return False


def identity(row):return (str(row['event_id']),row['player_key'])


def eligible(candidates,method):
    counts=Counter(c['player_key'] for c in candidates)
    return [c for c in candidates if counts[c['player_key']]==1 and finite(c['predictions'].get(method))
            and str(c.get('availability',{}).get('status','')).lower()!='out']


def ranked(candidates,method):
    return sorted(candidates,key=lambda c:(-c['predictions'][method],-c['predictions']['market_line'],c['player_key'],str(c['event_id'])))


def freeze_selections(candidates,methods):
    result={}
    for method in methods:
        pool=eligible(candidates,method);order=ranked(pool,method);picks=order[:3] if len(order)>=3 else []
        result[method]={'selected':[{'event_id':c['event_id'],'player_key':c['player_key'],'player':c['player']} for c in picks],
                        'eligible_count':len(pool),'boundary_tied_count':sum(c['predictions'][method]==picks[-1]['predictions'][method] for c in pool) if picks else 0}
    return {'version':EVALUATION_VERSION,'tie_rule':TIE_RULE,'scope':'three distinct quoted players; contest rules unverified','strategies':result}


def evaluate_pool(pool,method,outcomes):
    selected=ranked(pool,method)[:3] if len(pool)>=3 else []
    resolved=[c for c in pool if identity(c) in outcomes and outcomes[identity(c)]['status'] in ('graded','push') and finite(outcomes[identity(c)].get('actual_points'))]
    missing=[c for c in pool if c not in resolved and outcomes.get(identity(c),{}).get('status')!='dnp']
    actual=lambda c:outcomes[identity(c)]['actual_points']
    best=sorted(resolved,key=lambda c:(-actual(c),c['player_key'],str(c['event_id'])))[:3]
    best=best if len(best)==3 else []
    complete=len(selected)==3 and all(c in resolved for c in selected)
    total=sum(actual(c) for c in selected) if complete else None
    best_total=sum(actual(c) for c in best) if best else None
    cutoff=actual(best[-1]) if best else None
    credit=None
    if complete and cutoff is not None:
        above=sum(actual(c)>cutoff for c in resolved);tied=sum(actual(c)==cutoff for c in resolved)
        credit=sum(1 if actual(c)>cutoff else ((3-above)/tied if actual(c)==cutoff else 0) for c in selected)
    def item(c):
        r=outcomes.get(identity(c),{})
        return {'event_id':c['event_id'],'player_key':c['player_key'],'player':c['player'],
                'forecast':c['predictions'][method],'actual_points':r.get('actual_points'),
                'status':r.get('status','missing'),'forecast_rank':1+sum(x['predictions'][method]>c['predictions'][method] for x in pool),
                'actual_rank':1+sum(actual(x)>r['actual_points'] for x in resolved) if c in resolved else None}
    return {'pool_count':len(pool),'resolved_pool_count':len(resolved),'unresolved_pool_count':len(missing),
            'dnp_pool_count':sum(outcomes.get(identity(c),{}).get('status')=='dnp' for c in pool),
            'selected':[item(c) for c in selected],'selected_forecast_total':sum(c['predictions'][method] for c in selected) if len(selected)==3 else None,
            'selected_actual_total':total,'selected_complete':complete,
            'best_verified_trio':[item(c) for c in best],'best_verified_trio_points':best_total,
            'gap_to_best_verified':best_total-total if best_total is not None and total is not None else None,
            'captured_top3_slots':credit,'top3_capture_fraction':credit/3 if credit is not None else None,
            'leaderboard_provisional':bool(missing),'benchmark_scope':'resolved members of this frozen eligible pool; unresolved players excluded explicitly',
            'point_leaderboard':[item(c) for c in sorted(resolved,key=lambda c:(-actual(c),c['player_key']))]}


def evaluate(snapshot,rows):
    methods=['market_line',*snapshot['models']]
    candidates=snapshot['candidates'];outcomes={identity(r):r for r in rows}
    if len(outcomes)!=len(rows):raise ValueError('duplicate outcome identity')
    policy=freeze_selections(candidates,methods)
    recorded=snapshot.get('leader_selections')
    if recorded and recorded!=policy:raise ValueError('frozen selection policy does not match recorded forecasts')
    strategies={}
    for method in methods:
        pool=eligible(candidates,method)
        strategies[method]=evaluate_pool(pool,method,outcomes)
        if method!='market_line':strategies[method]['paired_market']=evaluate_pool(pool,'market_line',outcomes)
    return {'version':EVALUATION_VERSION,'selection_provenance':'recorded_in_snapshot' if recorded else 'reconstructed_from_frozen_predictions',
            'tie_rule':TIE_RULE,'actual_boundary_ties':'fractional top-three slot credit; deterministic best-trio names do not break scoring ties',
            'strategies':strategies,'limitation':'Observed quoted pool only; three-player totals do not establish contest legality, finish position or profitability.'}


def aggregate(grades):
    methods=defaultdict(list);missing=[]
    for g in grades:
        if g.get('snapshot_mode')!='prospective':continue
        if 'leaderboards' not in g:missing.append(g['game_date']);continue
        for method,result in g['leaderboards']['strategies'].items():methods[method].append((g['game_date'],result))
    result={}
    for method,days in methods.items():
        complete=[r for _,r in days if r['selected_complete']]
        credit=[r['captured_top3_slots'] for r in complete if r['captured_top3_slots'] is not None]
        gaps=[r['gap_to_best_verified'] for r in complete if r['gap_to_best_verified'] is not None]
        result[method]={'dates_evaluated':len(days),'dates_with_complete_selected_trio':len(complete),
                        'provisional_benchmark_dates':sum(r['leaderboard_provisional'] for _,r in days),
                        'top3_capture_fraction':sum(credit)/(3*len(credit)) if credit else None,
                        'mean_gap_to_best_verified':sum(gaps)/len(gaps) if gaps else None,
                        'by_date':[{'date':d,**{k:r[k] for k in ['selected_actual_total','selected_forecast_total','best_verified_trio_points','gap_to_best_verified','top3_capture_fraction','unresolved_pool_count']}} for d,r in days]}
    return {'version':EVALUATION_VERSION,'missing_metric_dates':missing,'strategies':result}
