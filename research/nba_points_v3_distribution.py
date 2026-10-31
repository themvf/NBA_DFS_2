"""Shared historical residual blocks; probabilities are experimental, conditional on playing."""
from collections import defaultdict
from itertools import combinations
import math

import numpy as np

from research.nba_points_v3_model import band, fair_odds, probability, residual_pool


def joint_draws(model, candidates, draws=5000, seed=20261028):
    if draws < 100 or draws > 100000:
        raise ValueError('draw count must be between 100 and 100000')
    rng = np.random.default_rng(seed)
    blocks = defaultdict(lambda:{True:[],False:[]})
    for r in model['calibration_rows']:
        blocks[r['event']][r['home']].append(r)
    blocks = [b for b in blocks.values() if all(len(b[s]) >= 5 for s in [True,False])]
    if not blocks:
        raise ValueError('no calibration games with both sides covered')
    for b in blocks:
        for side in [True,False]:
            b[side].sort(key=lambda r:-r['forecast']['points'])
    values = np.empty((draws,len(candidates)),dtype=int)
    games = defaultdict(list)
    for i,c in enumerate(candidates):
        games[str(c['event_id'])].append(i)
    fallback = 0
    for indices in games.values():
        ranks = {}
        for side in [True,False]:
            ordered = sorted([i for i in indices if candidates[i]['home'] is side],key=lambda i:-candidates[i]['prediction']['points'])
            ranks.update({i:n for n,i in enumerate(ordered)})
        # Each draw chooses one entire observed game for this future game.
        chosen = rng.integers(len(blocks),size=draws)
        for i in indices:
            c = candidates[i]; p = c['prediction']['points']; residuals = []
            pool = residual_pool(model,p)
            for block_index in chosen:
                side = blocks[block_index][c['home']]; rank = ranks[i]
                if rank < len(side) and band(side[rank]['forecast']['points']) == band(p):
                    residuals.append(side[rank]['residuals']['points'])
                else:
                    residuals.append(pool[int(rng.integers(len(pool)))]['residuals']['points']);fallback += 1
            values[:,i] = np.maximum(0,np.rint(p+np.asarray(residuals))).astype(int)
    return values, {'draws':draws,'seed':seed,'calibration_game_blocks':len(blocks),
        'independent_fallback_cells':fallback,'total_player_draw_cells':draws*len(candidates),
        'dependence':'Empirical home/away scoring-rank residual blocks; approximate, not validated current-season correlations.',
        'roster_coverage':'Quoted eligible historical pool only; unknown unquoted participants excluded.'}


def fractional_top_slots(values, slots):
    a = np.asarray(values)
    if a.shape[1] == 0:
        return np.zeros_like(a,dtype=float)
    slots = min(slots,a.shape[1])
    boundary = np.sort(a,axis=1)[:,-slots][:,None]
    above = a > boundary; tied = a == boundary
    remaining = slots-above.sum(axis=1)
    return above.astype(float)+tied*(remaining/tied.sum(axis=1))[:,None]


def summarize(model, candidates, values, audit):
    leader = fractional_top_slots(values,1).mean(axis=0)
    top3 = fractional_top_slots(values,3).mean(axis=0)
    rows = []
    for i,c in enumerate(candidates):
        a = values[:,i]; p = c['prediction']['points']; line = c['market_line']
        targets = {}
        for target in sorted({20,30,40,math.ceil(p)}):
            chance = probability(a,target)
            targets[str(target)] = {'event':f'points >= {target}', 'probability':chance,'fair_american_odds':fair_odds(chance)}
        rows.append({**c,'distribution':{'mean':float(a.mean()),'p10':float(np.quantile(a,.1)),
            'median':float(np.quantile(a,.5)),'p90':float(np.quantile(a,.9)),
            'thresholds':targets,'over_market_probability':probability(a,line,'over'),
            'market_push_probability':probability(a,line,'push'),
            'opportunity_target':math.ceil(p),'leader_share':float(leader[i]),'top3_slot_share':float(top3[i]),
            'independent_fallback_pool_n':len(residual_pool(model,p)),
            'status':'exploratory; conditional on appearance; not calibrated for current season'}})
    order = np.argsort(-values.mean(axis=0))
    selected = order[:3].tolist()
    trios = []
    for combo in combinations(order[:min(10,len(order))].tolist(),3):
        totals = values[:,combo].sum(axis=1)
        trios.append({'players':[candidates[i]['player'] for i in combo],
            'expected_total':float(totals.mean()),'p10_total':float(np.quantile(totals,.1)),
            'p90_total':float(np.quantile(totals,.9))})
    trios.sort(key=lambda x:-x['expected_total'])
    return {'rows':sorted(rows,key=lambda r:-r['distribution']['mean']), 'simulation':audit,
        'expected_points_trio': [candidates[i]['player'] for i in selected],
        'five_highest_mean_trios':trios[:5],
        'selection_scope':'Maximize expected combined full-game points within this quoted modeled pool; no salary/payout optimization.',
        'tie_policy':'Fractional leader and top-three boundary ties; shares sum to 1 and min(3,pool size).'}
