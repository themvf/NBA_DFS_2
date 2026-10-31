"""Portable fixed-specification opportunity challenger and residual calibration."""
from collections import defaultdict
from statistics import mean
import math

import numpy as np

from research.nba_points_forward import digest
from research.nba_points_ghost_v2 import linear_predict, quantile
from research.nba_points_v3_data import FEATURES, HISTORY_FEATURES, TARGETS

MODEL_VERSION = 'nba-points-v3-opportunity-model-v1'


def rates(history, league):
    total = lambda k: sum(float(r[k]) for r in history)
    return {'two': (total('fgm')-total('fg3m')+20*league['two'])/(total('fga')-total('fg3a')+20),
            'three': (total('fg3m')+20*league['three'])/(total('fg3a')+20),
            'free': (total('ftm')+20*league['free'])/(total('fta')+20)}


def predict(model, f, history, variant='context'):
    specs = model['variants'][variant]
    m = min(48., max(0., linear_predict(specs['minutes'], f)))
    intensity = max(0., linear_predict(specs['fga_per_min'], f))
    fraction = min(1., max(0., linear_predict(specs['fg3_fraction'], f)))
    fga = m*intensity; fg3a = fga*fraction
    fta = m*max(0., linear_predict(specs['fta_per_min'], f))
    r = rates(history, model['league_rates'])
    components = {'two': 2*(fga-fg3a)*r['two'], 'three': 3*fg3a*r['three'], 'free': fta*r['free']}
    return {'points': sum(components.values()), 'minutes': m, 'fga': fga, 'fg3a': fg3a, 'fta': fta,
            'conversion_rates': r, 'scoring_components': components}


def band(points):
    return 'ge20' if points >= 20 else ('10to20' if points >= 10 else 'lt10')


def residual_pool(model, points):
    pool = [r for r in model['calibration_rows'] if band(r['forecast']['points']) == band(points)]
    return pool if len(pool) >= 30 else model['calibration_rows']


def support(model, points):
    # Integer nonnegative full-game outcomes; threshold events are exact.
    return np.maximum(0, np.rint(points+np.array([r['residuals']['points'] for r in residual_pool(model, points)]))).astype(int)


def probability(values, threshold, event='at_least'):
    a = np.asarray(values)
    if not len(a):
        raise ValueError('empty distribution')
    if event == 'at_least':
        return float(np.mean(a >= math.ceil(threshold)))
    if event == 'over':
        return float(np.mean(a > threshold))
    if event == 'push':
        return float(np.mean(a == threshold))
    raise ValueError('unknown threshold event')


def fair_odds(p):
    if not 0 <= p <= 1:
        raise ValueError('invalid probability')
    if p in (0, 1):
        return None  # No finite odds; empirical zero is not impossibility.
    return round(-100*p/(1-p) if p > .5 else 100*(1-p)/p)


def crps(values, actual):
    a = np.sort(np.asarray(values, dtype=float)); n = len(a)
    pair_half = np.dot(2*np.arange(1,n+1)-n-1, a)/(n*n)
    return float(np.mean(np.abs(a-actual))-pair_half)


def distribution_metrics(model, examples, variant):
    rows = []
    for e in examples:
        p = predict(model, e['features'], e['rates_history'], variant)
        if variant != 'context':
            # Each challenger gets its own calibration residuals, not context's.
            pool = model['variant_residuals'][variant]
            chosen = [r for r in pool if band(r['forecast']['points']) == band(p['points'])]
            chosen = chosen if len(chosen) >= 30 else pool
            a = np.maximum(0, np.rint(p['points']+np.array([r['residuals']['points'] for r in chosen]))).astype(int)
        else:
            a = support(model, p['points'])
        q = float(np.quantile(a,.9)); error = e['points']-q
        reference_error = e['points']-e['features']['reference_q90']
        rows.append({'date':e['date'],'event':e['event'],'actual':e['points'], 'prediction':p['points'],
            'history': e['features']['p20'], 'primary': e['features']['p10'] >= 20,
            'errors':{k:abs(p[k]-e[k]) for k in ['points','minutes','fga','fg3a','fta']},
            'crps':crps(a,e['points']), 'q90_loss':max(.9*error,-.1*error), 'q90_hit':e['points']>q,
            'history_crps':crps(e['points_history'],e['points']),
            'history_q90_loss':max(.9*reference_error,-.1*reference_error),
            'thresholds':{str(t):{'p':probability(a,t),'hit':e['points']>=t} for t in [20,30,40]}})
    def summarize(rs):
        if not rs:
            return {'n':0}
        thresholds = {}
        for t in ['20','30','40']:
            pairs = [r['thresholds'][t] for r in rs]
            bins = []
            for lo in [0,.2,.4,.6,.8]:
                group = [x for x in pairs if lo <= x['p'] < lo+.2 or (lo == .8 and x['p'] == 1)]
                bins.append({'from':lo,'through':lo+.2,'n':len(group),
                             'mean_probability':mean(x['p'] for x in group) if group else None,
                             'observed_rate':mean(x['hit'] for x in group) if group else None})
            thresholds[t] = {'n':len(pairs),'hits':sum(x['hit'] for x in pairs),
                'brier':mean((x['p']-x['hit'])**2 for x in pairs),'reliability':bins}
        # Cluster by date; this is uncertainty in paired point errors, not a promotion test.
        groups = defaultdict(list)
        for r in rs:
            groups[r['date']].append(abs(r['prediction']-r['actual'])-abs(r['history']-r['actual']))
        dates = list(groups); rng = np.random.default_rng(20261028)
        boot = []
        for _ in range(1000):
            selected = rng.choice(dates,len(dates),replace=True)
            boot.append(mean(v for d in selected for v in groups[d]))
        return {'n':len(rs),'dates':len(dates), 'mae':{k:mean(r['errors'][k] for r in rs) for k in rs[0]['errors']},
            'history20_points_mae':mean(abs(r['history']-r['actual']) for r in rs),
            'paired_mae_delta_vs_history20':mean(abs(r['prediction']-r['actual'])-abs(r['history']-r['actual']) for r in rs),
            'date_clustered_delta_interval_95':np.quantile(boot,[.025,.975]).tolist(),
            'crps':mean(r['crps'] for r in rs),'q90_loss':mean(r['q90_loss'] for r in rs),
            'history_empirical_crps':mean(r['history_crps'] for r in rs),
            'history_q90_loss':mean(r['history_q90_loss'] for r in rs),
            'q90_exceedances':sum(r['q90_hit'] for r in rs),'threshold_audit':thresholds}
    return {'all':summarize(rows),'prior10_points_ge20':summarize([r for r in rows if r['primary']])}


def fit(examples, players, source_sha, created, audit):
    from sklearn.impute import SimpleImputer
    from sklearn.preprocessing import StandardScaler
    from sklearn.linear_model import Ridge
    from sklearn.pipeline import make_pipeline
    dev = [e for e in examples if e['date'] < '2026-01-01']
    cal = [e for e in examples if '2026-01-01' <= e['date'] < '2026-02-01']
    evaluation = [e for e in examples if '2026-02-01' <= e['date'] < '2026-07-01']
    if min(len(dev),len(cal),len(evaluation)) < 100:
        raise ValueError('insufficient chronological coverage')
    total = lambda k:sum(float(r[k]) for r in players if r['game_date'] < '2026-01-01')
    league = {'two':(total('fgm')-total('fg3m'))/(total('fga')-total('fg3a')),
              'three':total('fg3m')/total('fg3a'),'free':total('ftm')/total('fta')}
    variants = {}
    for name, fs in [('history_only', HISTORY_FEATURES),('context', FEATURES)]:
        x = np.array([[e['features'].get(f) if e['features'].get(f) is not None else np.nan for f in fs] for e in dev])
        variants[name] = {}
        for target in TARGETS:
            pipeline = make_pipeline(SimpleImputer(strategy='median',keep_empty_features=True),StandardScaler(),Ridge(alpha=100))
            pipeline.fit(x,[e['targets'][target] for e in dev])
            imputer, scaler, learner = pipeline
            spec = {'features':fs,'impute':imputer.statistics_.tolist(),'mean':scaler.mean_.tolist(),
                'scale':scaler.scale_.tolist(),'coef':learner.coef_.tolist(),'intercept':float(learner.intercept_)}
            portable = [linear_predict(spec,e['features']) for e in dev[:50]]
            if not np.allclose(portable,pipeline.predict(x[:50]),atol=1e-8):
                raise AssertionError('portable inference mismatch')
            variants[name][target] = spec
    model = {'version':MODEL_VERSION,'created_at':created,'source_sha256':source_sha,
        'development_end':'2025-12-31','calibration_end':'2026-01-31','evaluation_end':'2026-06-30',
        'variants':variants,'league_rates':league,'source_audit':audit,
        'split_counts':{'development':len(dev),'calibration':len(cal),'evaluation':len(evaluation)},
        'settings':{'ridge_alpha':100,'conversion_shrinkage_attempts':20,'minimum_history':10,
                    'distribution':'calibration-game residual block by home/away predicted scoring rank'},
        'limitations':['Experimental, uncalibrated current-season probabilities; no demonstrated edge.',
            'Date-ordered late-ingested historical replay is not verified pregame evidence.',
            'Previously inspected season, not pristine holdout. No minute restriction or lineup-stint inputs.',
            'Independent minute/rate expectations omit covariance. Point residual calibration is separate.',
            'Historical shared residuals approximate dependence; no possession or full-roster simulator.']}
    residuals = {}
    for variant in variants:
        rows = []
        for e in cal:
            p = predict(model,e['features'],e['rates_history'],variant)
            rows.append({k:e[k] for k in ['date','event','player_id','home']})
            rows[-1].update(forecast={k:p[k] for k in ['points','minutes','fga','fg3a','fta']},
                residuals={k:e[k]-p[k] for k in ['points','minutes','fga','fg3a','fta']})
        residuals[variant] = rows
    model['variant_residuals'] = residuals
    model['calibration_rows'] = residuals['context']
    model['evaluation'] = {v:distribution_metrics(model,evaluation,v) for v in variants}
    model['sha256'] = digest(model)
    return model


def evaluate_joint(model, examples, draws=1000, seed=20261028):
    """Audit the actual joint sampler separately from marginal residual scores."""
    from research.nba_points_v3_distribution import joint_draws, fractional_top_slots
    selected = [e for e in examples if model['calibration_end'] < e['date'] <= model['evaluation_end']]
    by_date = defaultdict(list)
    for e in selected:
        by_date[e['date']].append(e)
    rows = []; dates = []; fallback = 0; cells = 0
    for offset,(day, es) in enumerate(sorted(by_date.items())):
        candidates = [{'event_id':e['event'],'home':e['home'],
                       'prediction':predict(model,e['features'],e['rates_history'])} for e in es]
        values,audit = joint_draws(model,candidates,draws,seed+offset)
        fallback += audit['independent_fallback_cells']; cells += audit['total_player_draw_cells']
        means = values.mean(axis=0); pred_leader = fractional_top_slots(values,1).mean(axis=0)
        actual = np.array([e['points'] for e in es]); actual_leader = fractional_top_slots(actual[None,:],1)[0]
        chosen = np.argsort(-means)[:3]; best = np.sort(actual)[-3:]
        dates.append({'date':day,'eligible_appearances':len(es),
            'selected_trio':[es[i]['player'] for i in chosen],
            'actual_trio_total':float(actual[chosen].sum()),'best_trio_total':float(best.sum()),
            'hindsight_gap':float(best.sum()-actual[chosen].sum()),
            'leader_brier_sum':float(((pred_leader-actual_leader)**2).sum())})
        for i,e in enumerate(es):
            a = values[:,i]; q = float(np.quantile(a,.9)); err = e['points']-q
            rows.append({'primary':e['features']['p10']>=20,'crps':crps(a,e['points']),
                'mean_abs_error':abs(means[i]-e['points']),'q90_loss':max(.9*err,-.1*err),
                'history_crps':crps(e['points_history'],e['points']),
                'history_abs_error':abs(e['features']['p20']-e['points']),
                'history_q90_loss':max(.9*(e['points']-e['features']['reference_q90']),-.1*(e['points']-e['features']['reference_q90'])),
                'q90_hit':e['points']>q,
                'thresholds':{str(t):{'p':probability(a,t),'hit':e['points']>=t} for t in [20,30,40]}})
    def summary(rs):
        if not rs:return {'n':0}
        return {'n':len(rs),'crps':mean(r['crps'] for r in rs),
            'distribution_mean_mae':mean(r['mean_abs_error'] for r in rs),
            'history20_mae':mean(r['history_abs_error'] for r in rs),
            'history_empirical_crps':mean(r['history_crps'] for r in rs),
            'history_q90_loss':mean(r['history_q90_loss'] for r in rs),
            'q90_loss':mean(r['q90_loss'] for r in rs),'q90_exceedances':sum(r['q90_hit'] for r in rs),
            'thresholds':{t:{'n':len(rs),'hits':sum(r['thresholds'][t]['hit'] for r in rs),
                'mean_probability':mean(r['thresholds'][t]['p'] for r in rs),
                'brier':mean((r['thresholds'][t]['p']-r['thresholds'][t]['hit'])**2 for r in rs)} for t in ['20','30','40']}}
    return {'model_sha256':model['sha256'],'evaluation_kind':'Actual joint sampler, retrospective appearance-conditioned eligible pool',
        'draws_per_date':draws,'seed':seed,'date_n':len(dates),'all':summary(rows),
        'prior10_points_ge20':summary([r for r in rows if r['primary']]),
        'joint_sampling_fallback_cells':fallback,'joint_sampling_total_cells':cells,'daily_selection_audit':dates,
        'limitations':['Actual participants define this historical conditional pool; no pregame availability validation.',
            'Previously inspected historical season. No contemporaneous market benchmark available.',
            'Joint simulation diagnostics do not establish calibrated slate-leader probabilities.']}
