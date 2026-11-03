"""Independent experimental trio mathematics. No changes to v1/v2/v3 forecasts."""
from __future__ import annotations
from itertools import combinations
from math import exp, lgamma, log, pi
import numpy as np

VERSION = 'nba-trio-math-v1'
SETTINGS = {'hazard': .05, 'history': 20, 'prior_strength': 1., 'conversion_strength': 20.,
            'pool': 12, 'entries': 5, 'draws': 2000, 'calibration_draws': 500,
            'seed': 20261103, 'development_end': '2025-12-31', 'dependence_end': '2026-01-15',
            'calibration_start': '2026-01-16', 'calibration_end': '2026-01-31',
            'minimum_covariance_games': 40, 'minimum_calibration': 100}

def checked_values(history, target):
    """Reject future, duplicate, impossible shot totals; preserve zero-point appearances."""
    if not history: raise ValueError('no history')
    seen=set()
    for h in history:
        identity=(str(h['game_date']),str(h.get('source_game_id',h.get('game_id',''))))
        if identity in seen or str(h['game_date'])>=str(target): raise ValueError('duplicate/future history')
        seen.add(identity)
        vals=[float(h[k]) for k in ['minutes','fga','fg3a','fgm','fg3m','fta','ftm','points']]
        if not all(np.isfinite(vals)) or min(vals)<0 or vals[0]<=0: raise ValueError('invalid played history')
        if h['fg3m']>h['fg3a'] or h['fg3a']>h['fga'] or h['fgm']>h['fga'] or h['fg3m']>h['fgm'] or h['ftm']>h['fta']:raise ValueError('invalid shot history')
        if h['fgm']-h['fg3m']>h['fga']-h['fg3a'] or abs(2*h['fgm']+h['fg3m']+h['ftm']-h['points'])>.01:raise ValueError('unreconciled shot history')
    return history[-SETTINGS['history']:]

def posterior(state,x):
    mu,k,a,b=state;kn=k+1
    return (mu+(x-mu)/kn,kn,a+.5,b+.5*k*(x-mu)**2/kn)

def log_predictive(state,x):
    mu,k,a,b=state;df=2*a;s2=b*(k+1)/(a*k)
    return lgamma((df+1)/2)-lgamma(df/2)-.5*log(df*pi*s2)-(df+1)/2*log(1+(x-mu)**2/(df*s2))

def regime(values,prior,hazard=SETTINGS['hazard']):
    """Normal-inverse-gamma filter with change before the current observation."""
    if not 0<hazard<1:raise ValueError('invalid hazard')
    states=[tuple(prior)];weights=np.ones(1)
    for x in values:
        if not np.isfinite(x):raise ValueError('invalid regime observation')
        logs=np.array([log(hazard)+log_predictive(prior,x)]+[log(1-hazard)+log(max(w,1e-300))+log_predictive(s,x) for w,s in zip(weights,states)])
        weights=np.exp(logs-logs.max());weights/=weights.sum()
        states=[posterior(prior,x)]+[posterior(s,x) for s in states]
    means=np.array([s[0] for s in states]);avg=float(weights@means)
    return {'weights':weights,'states':np.array(states),'mean':avg,
            'recent_regime_mass':float(weights[:3].sum()),'run_length_mean':float(weights@np.arange(len(weights)))}

def regime_draws(fit,rng,n,z=None):
    index=rng.choice(len(fit['weights']),n,p=fit['weights']);mu,k,a,b=fit['states'][index].T
    if z is None:t=rng.standard_t(2*a)
    else:
        from scipy.special import ndtr
        from scipy.stats import t as student_t
        t=student_t.ppf(np.clip(ndtr(z),1e-8,1-1e-8),2*a)
    return mu+np.sqrt(b*(k+1)/(a*k))*t

def compound(history,target,artifact,rng,n,z=None):
    h=checked_values(history,target)
    # An observed team change resets this workload segment; never blend a previous team's role silently.
    side=lambda r:r.get('team',r.get('team_id'))
    if side(h[-1]) is not None:
        last=side(h[-1]);start=len(h)-1
        while start>0 and side(h[start-1])==last:start-=1
        h=h[start:]
    features={'minutes':[r['minutes'] for r in h],
              'fga_rate':[r['fga']/r['minutes'] for r in h],
              'fta_rate':[r['fta']/r['minutes'] for r in h]}
    fits={k:regime(v,artifact['priors'][k]) for k,v in features.items()}
    # Full-game minutes include OT. No invented DNP mixture or 48-minute truncation.
    m=np.maximum(1.,regime_draws(fits['minutes'],rng,n,None if z is None else z[0]))
    fg=np.maximum(0.,regime_draws(fits['fga_rate'],rng,n,None if z is None else z[1]))*m
    ft=np.maximum(0.,regime_draws(fits['fta_rate'],rng,n,None if z is None else z[2]))*m
    def attempts(mu,key):
        shape=artifact['count_shapes'][key]
        return rng.negative_binomial(shape,shape/(shape+mu))
    fga=attempts(fg,'fga');fta=attempts(ft,'fta')
    total=lambda k:sum(r[k] for r in h)
    share=rng.beta(1+total('fg3a'),1+total('fga')-total('fg3a'),n)
    fg3a=rng.binomial(fga,share)
    makes=[]
    for key,trials,success,miss in [
        ('two',fga-fg3a,total('fgm')-total('fg3m'),total('fga')-total('fg3a')-total('fgm')+total('fg3m')),
        ('three',fg3a,total('fg3m'),total('fg3a')-total('fg3m')),
        ('free',fta,total('ftm'),total('fta')-total('ftm'))]:
        rate=artifact['league_rates'][key];s=SETTINGS['conversion_strength']
        p=rng.beta(s*rate+success,s*(1-rate)+miss,n);makes.append(rng.binomial(trials,p))
    values=2*makes[0]+3*makes[1]+makes[2]
    return values,{'minutes':float(m.mean()),'fga':float(fga.mean()),'fta':float(fta.mean()),'fg3a':float(fg3a.mean()),
                   'regime':{k:{j:f[j] for j in ['mean','recent_regime_mass','run_length_mean']} for k,f in fits.items()}}

def fit_dependence(calibration_rows):
    from collections import defaultdict
    from sklearn.covariance import LedoitWolf
    groups=defaultdict(lambda:{False:[],True:[]})
    for r in calibration_rows:
        if r['date']<=SETTINGS['dependence_end']:groups[r['event']][r['home']].append(r)
    blocks=[]
    for sides in groups.values():
        if min(map(len,sides.values()))<5:continue
        blocks.append([r for side in [False,True] for r in sorted(sides[side],key=lambda r:-r['forecast']['points'])[:5]])
    out={'games':len(blocks),'channels':{},'method':'Ledoit-Wolf on standardized historical forecast residuals by side/scoring rank; not current-player correlation estimates'}
    for key in ['minutes','fga','fta']:
        if len(blocks)<SETTINGS['minimum_covariance_games']:
            out['channels'][key]={'status':'insufficient_history_independent','correlation':np.eye(10).tolist()};continue
        x=np.array([[r['residuals'][key] for r in b] for b in blocks]);std=np.maximum(x.std(axis=0),1e-6)
        fitted=LedoitWolf().fit((x-x.mean(axis=0))/std);cov=fitted.covariance_
        if not np.isfinite(cov).all() or np.min(np.diag(cov))<=1e-12:
            out['channels'][key]={'status':'degenerate_history_independent','correlation':np.eye(10).tolist()};continue
        sd=np.sqrt(np.diag(cov));corr=cov/np.outer(sd,sd)
        out['channels'][key]={'status':'historical_rank_shrinkage','shrinkage':float(fitted.shrinkage_),'correlation':corr.tolist()}
    return out

def compound_slate(candidates,artifact,n,seed,dependent=True):
    rng=np.random.default_rng(seed);channels={};groups={}
    for i,c in enumerate(candidates):groups.setdefault(c['event_id'],[]).append(i)
    if dependent:
        for ids in groups.values():
            slots={}
            for side in [False,True]:
                ordered=sorted([i for i in ids if candidates[i]['home'] is side],key=lambda i:-candidates[i]['prediction']['points'])
                slots.update({i:(5 if side else 0)+rank for rank,i in enumerate(ordered[:5])})
            normals=[rng.multivariate_normal(np.zeros(10),artifact['dependence']['channels'][key]['correlation'],n) for key in ['minutes','fga','fta']]
            for i,slot in slots.items():channels[i]=[z[:,slot] for z in normals]
    values=[];audit=[]
    for i,c in enumerate(candidates):
        sample,info=compound(c['history'],c['target'],artifact,rng,n,channels.get(i));values.append(sample);audit.append(info)
    return np.array(values,dtype=float).T,audit

def quantile_adjustment(scores,level=.9):
    if not scores:return None
    n=len(scores);index=int(np.ceil((n+1)*level))-1
    if index>=n:return None
    return float(np.sort(scores)[index])

def select_portfolios(scenarios,lines,ids,entries=5):
    """Exact candidate enumeration; deterministic greedy + one-swap local improvement."""
    if len(ids)<3 or len(ids)!=len(set(ids)) or len(lines)!=len(ids):raise ValueError('invalid pool')
    if any(np.asarray(a).ndim!=2 or a.shape[1]!=len(ids) or not np.isfinite(a).all() or np.min(a)<0 for a in scenarios.values()):raise ValueError('invalid scenarios')
    combos=list(combinations(range(len(ids)),3))
    if len(combos)<entries:raise ValueError('fewer than five distinct trios')
    scores={k:np.stack([v[:,c].sum(axis=1) for c in combos],axis=1) for k,v in scenarios.items()}
    sums=np.array([sum(lines[i] for i in c) for c in combos])
    order=sorted(range(len(combos)),key=lambda j:(-sums[j],sum(i+1 for i in combos[j]),tuple(ids[i] for i in combos[j])))
    baseline=order[:entries]
    references={k:float(v[:,baseline].max(axis=1).mean()) for k,v in scores.items()}
    def objective(chosen,robust):
        improvement={k:float(v[:,chosen].max(axis=1).mean())-references[k] for k,v in scores.items()}
        return min(improvement.values()) if robust else improvement['v3']
    def solve(robust):
        chosen=[order[0]]
        while len(chosen)<entries:
            chosen.append(max((j for j in order if j not in chosen),key=lambda j:(objective(chosen+[j],robust),-order.index(j))))
        # Preserve the market baseline trio. Fixed single pass, no claimed global optimum.
        for slot in range(1,entries):
            best=chosen[slot];value=objective(chosen,robust)
            for j in order:
                if j in chosen:continue
                trial=chosen.copy();trial[slot]=j;score=objective(trial,robust)
                if score>value+1e-9:best,value=j,score
            chosen[slot]=best
        if objective(chosen,robust)<-1e-9:chosen=baseline.copy()
        return chosen
    def result(chosen):
        return {'trios':[[ids[i] for i in combos[j]] for j in chosen],
                'market_line_sums':[float(sums[j]) for j in chosen],
                'scenario_expected_best':{k:float(v[:,chosen].max(axis=1).mean()) for k,v in scores.items()},
                'scenario_expected_all_five':{k:float(v[:,chosen].sum(axis=1).mean()) for k,v in scores.items()}}
    return {'market_baseline':result(baseline),'joint_best_of_five':result(solve(False)),
            'robust_best_of_five':result(solve(True)),
            'candidate_trios':len(combos),'algorithm':'greedy + one fixed local-swap pass; baseline trio retained; not globally optimal'}

def grade_portfolio(trios,outcomes):
    if len(trios)!=SETTINGS['entries'] or len({tuple(sorted(t)) for t in trios})!=len(trios):raise ValueError('five unique trios required')
    totals=[];unresolved=[]
    for trio in trios:
        if len(trio)!=3 or len(set(trio))!=3:raise ValueError('invalid stored trio')
        rows=[outcomes.get(i) for i in trio]
        missing=[i for i,r in zip(trio,rows) if not r or r.get('status')!='verified' or r.get('points') is None]
        unresolved.extend(missing);totals.append(None if missing else sum(r['points'] for r in rows))
    complete=bool(totals) and not unresolved
    return {'status':'resolved' if complete else 'incomplete','trio_totals':totals,
            'best_of_five':max(totals) if complete else None,'all_five_total':sum(totals) if complete else None,
            'unresolved_ids':sorted(set(unresolved))}
