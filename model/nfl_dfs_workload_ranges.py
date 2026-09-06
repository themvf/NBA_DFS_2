"""Prior-only workload-conditioned residual experiment; never activates production."""
from collections import defaultdict
import numpy as np

CONFIG = {'minimum_tier_rows': 100, 'maximum_tier_rows': 2000, 'target_boundaries': [4, 7], 'thresholds': [10, 15, 20, 25]}


def tier(targets):
    return 0 if targets < 4 else 1 if targets < 7 else 2


def distribution(mean, residuals):
    draws = mean + np.asarray(residuals[-CONFIG['maximum_tier_rows']:], dtype=float)
    if len(draws) < CONFIG['minimum_tier_rows'] or not np.isfinite(draws).all():
        raise ValueError('Insufficient or invalid residual evidence')
    return draws


def summarize(draws):
    return {'mean': float(np.mean(draws)), 'p10': float(np.quantile(draws,.1)), 'p50': float(np.quantile(draws,.5)), 'p90': float(np.quantile(draws,.9))}


def probability(draws, threshold):
    if not np.isfinite(threshold) or len(draws) == 0 or not np.isfinite(draws).all():
        raise ValueError('Invalid threshold or draws')
    return float(np.mean(np.asarray(draws) >= threshold))


def salary_probabilities(draws, salary):
    if not np.isfinite(salary) or salary <= 0:
        raise ValueError('Salary must be positive')
    return {str(x): probability(draws, x*salary/1000) for x in [2,3,4]}


def replay(rows):
    weeks = defaultdict(list)
    for r in rows:
        weeks[(r['season'],r['week'])].append(r)
    errors = defaultdict(list)
    output = []
    for _, current in sorted(weeks.items()):
        for r in current:
            group = tier(r['targets_baseline'])
            if len(errors[group]) < CONFIG['minimum_tier_rows']:
                continue
            draws = distribution(r['candidate']['mean'], errors[group])
            output.append({**r, 'workload': summarize(draws), 'probabilities': {str(t): probability(draws,t) for t in CONFIG['thresholds']},
                           'above_projection_probability': float(np.mean(draws > np.mean(draws))), 'residual_rows': min(len(errors[group]), CONFIG['maximum_tier_rows'])})
        # No player in a week may borrow another player's result from that week.
        for r in sorted(current,key=lambda r:(r['game_id'],r['identity'])):
            errors[tier(r['targets_baseline'])].append(r['actual']-r['candidate']['mean'])
    return output, errors


def calibration(rows, threshold):
    pairs = [(r['probabilities'][str(threshold)], r['actual'] >= threshold) for r in rows]
    bins = []
    for i in range(5):
        values = [(p,y) for p,y in pairs if min(4,int(p*5)) == i]
        if values:
            bins.append({'lower':i/5,'upper':(i+1)/5,'n':len(values),'predicted':float(np.mean([p for p,_ in values])),'observed':float(np.mean([y for _,y in values]))})
    return {'brier':float(np.mean([(p-y)**2 for p,y in pairs])), 'bins':bins}
