"""Prior-game method-of-moments role variance; research, not calibration."""
from collections import defaultdict
import numpy as np


def draw_role_shares(rng, probabilities, concentration, size):
    p = np.asarray(probabilities, float)
    if not np.isfinite(p).all() or np.any(p < 0) or abs(p.sum() - 1) > 1e-8 or not np.isfinite(concentration) or concentration <= 0:
        raise ValueError('Invalid role distribution')
    result = np.zeros((size, len(p)))
    positive = p > 0
    result[:, positive] = rng.dirichlet(p[positive] * concentration, size)
    return result


def estimate_dispersion(history, action, minimum_groups=8):
    if action not in ('carries', 'targets'):
        raise ValueError('Unsupported opportunity')
    grouped = defaultdict(list)
    for h in history:
        for team in (h['game']['away'], h['game']['home']):
            boxes = [b for b in h['boxes'] if b['team'] == team]
            total = sum(b[action] for b in boxes)
            if total > 1:
                grouped[h['game']['season'], team].append((total, {b['identity']: b[action] for b in boxes}))
    estimates = []
    observations = 0
    for rows in grouped.values():
        if len(rows) < 4:
            continue
        ids = sorted(set().union(*(set(r) for _, r in rows)))
        n = np.array([n for n, _ in rows], float)
        shares = np.array([[r.get(i, 0) / total for i in ids] for total, r in rows])
        mean = shares.mean(axis=0)
        diversity = float(np.sum(mean * (1 - mean)))
        if diversity <= 0:
            continue
        # Trace of share covariance, less the finite-count multinomial noise.
        observed = float(np.var(shares, axis=0, ddof=1).sum()) / diversity
        noise = float(np.mean(1 / n))
        rho = (observed - noise) / (1 - noise)
        estimates.append((rho, len(rows) - 1))
        observations += len(rows)
    fallback = 40. if action == 'targets' else 55.
    report = {'method': 'pooled_team_season_moments', 'groups': len(estimates),
              'game_team_observations': observations, 'fallback': False,
              'limits': ['Role changes and injuries contribute to observed variance.',
                         'Concentration bounds 2 to 1000 are numerical safeguards, not calibration.']}
    if len(estimates) < minimum_groups:
        return {**report, 'concentration': fallback, 'fallback': True, 'reason': 'insufficient_team_season_groups'}
    rho = sum(v * w for v, w in estimates) / sum(w for _, w in estimates)
    concentration = 1000. if rho <= 0 else float(np.clip(1 / rho - 1, 2, 1000))
    return {**report, 'concentration': concentration, 'intraclass_variation': float(rho)}


def interval_diagnostic(actual, lower, upper, alpha=.2):
    if not (0 < alpha < 1) or not np.isfinite([actual, lower, upper]).all() or lower > upper:
        raise ValueError('Invalid interval')
    return {'covered': lower <= actual <= upper, 'width': upper - lower,
            'interval_score': upper - lower + 2 / alpha * (max(lower - actual, 0) + max(actual - upper, 0))}
