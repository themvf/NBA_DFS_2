from __future__ import annotations

import numpy as np
from scipy.optimize import minimize
from scipy.special import expit

from model.nfl_joint_contracts import digest, require_evidence
from model.nfl_longest_touchdown import timestamp

FEATURES = ('salary', 'projected_points', 'projected_value')


def fit_ownership(rows, cutoff, ridge=10.):
    if len(rows) < 20 or ridge <= 0:
        raise ValueError('At least 20 sourced ownership observations required')
    keys = set()
    for row in rows:
        key = (row['contest_id'], row['identity'], row['slot'])
        if key in keys:
            raise ValueError('Duplicate ownership label')
        keys.add(key)
        require_evidence(row['projection_evidence'], row['decision_at'])
        if timestamp(row['contest_ended_at']) >= timestamp(cutoff) or not row.get('ownership_source_ref'):
            raise ValueError('Ownership label boundary/source violation')
        if row['slot'] not in ('CPT', 'FLEX', 'CLASSIC'):
            raise ValueError('Unsupported ownership slot')
    features = np.array([[r[k] for k in FEATURES] + [float(r['slot'] == 'CPT'), float(r['slot'] == 'CLASSIC')] for r in rows], float)
    owned = np.array([r['ownership'] for r in rows], float)
    if not np.isfinite(features).all() or not np.isfinite(owned).all() or ((owned < 0) | (owned > 1)).any():
        raise ValueError('Invalid ownership inputs')
    center, scale = features.mean(axis=0), features.std(axis=0)
    scale[scale == 0] = 1
    design = np.column_stack([np.ones(len(rows)), (features - center) / scale])
    def objective(beta):
        logits = design @ beta
        loss = np.sum(np.logaddexp(0, logits) - owned * logits) + ridge / 2 * (beta[1:] @ beta[1:])
        gradient = design.T @ (expit(logits) - owned)
        gradient[1:] += ridge * beta[1:]
        return loss, gradient
    solved = minimize(objective, np.zeros(design.shape[1]), jac=True, method='L-BFGS-B')
    if not solved.success:
        raise ValueError('Ownership fitting failed')
    return {'version': 'nfl-ownership-v1', 'training_cutoff': cutoff, 'rows': len(rows),
            'center': center.tolist(), 'scale': scale.tolist(), 'coefficients': solved.x.tolist(),
            'source_sha256': digest(rows), 'authority': 'exploratory_marginal_ownership_not_field_generator'}


def predict_ownership(fit, players, decision_at):
    if timestamp(fit['training_cutoff']) > timestamp(decision_at):
        raise ValueError('Ownership fit after decision')
    output = []
    for p in players:
        require_evidence(p['projection_evidence'], decision_at)
        features = np.array([p[k] for k in FEATURES] + [float(p['slot'] == 'CPT'), float(p['slot'] == 'CLASSIC')])
        if not np.isfinite(features).all():
            raise ValueError('Missing ownership predictor')
        probability = expit(np.r_[1., (features - fit['center']) / fit['scale']] @ fit['coefficients'])
        output.append({'identity': p['identity'], 'slot': p['slot'], 'ownership': float(probability)})
    return {'players': output, 'authority': fit['authority'],
            'limits': ['Marginal ownership does not identify legal lineup or trio selection dependence.']}
