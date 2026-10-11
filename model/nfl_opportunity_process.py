from __future__ import annotations

from collections import defaultdict

import numpy as np

from model.nfl_joint_contracts import digest, require_evidence
from model.nfl_longest_touchdown import timestamp
from model.nfl_midgame_exits import redistributed_shares
from model.nfl_role_dispersion import draw_role_shares


def fit_market_volume(rows, cutoff, ridge=10.):
    if not np.isfinite(ridge) or ridge <= 0 or len(rows) < 20:
        raise ValueError('At least 20 paired market/volume observations required')
    keys = set()
    for row in rows:
        require_evidence(row['market_evidence'], row['decision_at'])
        if timestamp(row['decision_at']) >= timestamp(row['kickoff']) or timestamp(row['ended_at']) >= timestamp(cutoff):
            raise ValueError('Market volume training boundary violation')
        if row['game_id'] in keys:
            raise ValueError('Duplicate market volume game')
        keys.add(row['game_id'])
    features = np.array([[r['total'], r['home_spread']] for r in rows], float)
    outcomes = np.array([[r[f'{side}_{action}'] for side in ('away', 'home') for action in ('targets', 'carries')] for r in rows], float)
    if not np.isfinite(features).all() or not np.isfinite(outcomes).all() or (outcomes < 0).any():
        raise ValueError('Invalid volume observations')
    center, scale = features.mean(axis=0), features.std(axis=0)
    scale[scale == 0] = 1
    design = np.column_stack([np.ones(len(rows)), (features - center) / scale])
    regularizer = np.diag([0., ridge, ridge])
    coefficients = np.linalg.solve(design.T @ design + regularizer, design.T @ np.log1p(outcomes))
    return {'method': 'ridge_log_volume', 'training_cutoff': cutoff, 'rows': len(rows),
            'center': center.tolist(), 'scale': scale.tolist(), 'coefficients': coefficients.tolist(),
            'mean_volumes': outcomes.mean(axis=0).tolist(), 'source_sha256': digest(rows),
            'limits': ['Aggregate counts condition paired historical scenarios; no sequential score simulation.']}


def conditioned_volumes(bank, fit, market):
    require_evidence(market['evidence'], bank['decision_at'])
    if timestamp(fit['training_cutoff']) > timestamp(bank['decision_at']):
        raise ValueError('Volume fit after decision')
    feature = (np.array([market['total'], market['home_spread']]) - fit['center']) / fit['scale']
    if not np.isfinite(feature).all():
        raise ValueError('Invalid game market features')
    predicted = np.maximum(0, np.expm1(np.r_[1., feature] @ np.asarray(fit['coefficients'])))
    result = {}
    for side, team in enumerate((bank['game']['away'], bank['game']['home'])):
        players = [p for p in bank['players'] if p['team'] == team]
        for action_index, action in enumerate(('targets', 'carries')):
            parent = np.sum([p['draws'][action] for p in players], axis=0)
            ratio = predicted[side * 2 + action_index] / max(float(parent.mean()), 1.)
            result[(team, action)] = np.rint(parent * np.clip(ratio, .5, 1.5)).astype(int)
    return result


def allocate_segments(rng, players, totals, shares, concentration, participation, exit_fit=None, action='targets'):
    totals = np.asarray(totals)
    if totals.ndim != 1 or (totals < 0).any() or not np.equal(totals, np.rint(totals)).all():
        raise ValueError('Invalid opportunity totals')
    segments = participation.shape[1]
    if participation.shape != (len(totals), segments, len(players)):
        raise ValueError('Misaligned participation')
    role_draws = draw_role_shares(rng, shares, concentration, len(totals))
    allocations = np.zeros(participation.shape, dtype=int)
    for s, total in enumerate(totals):
        counts = rng.multinomial(int(total), np.full(segments, 1 / segments))
        for segment, count in enumerate(counts):
            available = participation[s, segment]
            if not available.all():
                if exit_fit is None:
                    raise ValueError('Unavailable participant requires redistribution fit')
                probabilities = redistributed_shares(role_draws[s], available, players, exit_fit, action)
            else:
                probabilities = role_draws[s]
            allocations[s, segment] = rng.multinomial(int(count), probabilities)
    if not np.array_equal(allocations.sum(axis=(1, 2)), totals) or np.any(allocations[~participation]):
        raise AssertionError('Opportunity conservation failure')
    return allocations
