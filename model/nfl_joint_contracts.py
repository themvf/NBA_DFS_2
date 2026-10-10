from __future__ import annotations

from copy import deepcopy
from hashlib import sha256
import json

import numpy as np

from model.nfl_longest_touchdown import timestamp

METRICS = {'rushing_yards': 'rushYds', 'receiving_yards': 'recYds', 'receptions': 'receptions', 'total_yards': None}


def digest(value):
    return sha256(json.dumps(value, sort_keys=True, allow_nan=False, separators=(',', ':')).encode()).hexdigest()


def normalized_weights(values, size):
    weights = np.ones(size) if values is None else np.asarray(values, dtype=float)
    if weights.shape != (size,) or not np.isfinite(weights).all() or (weights < 0).any() or weights.sum() <= 0:
        raise ValueError('Invalid scenario weights')
    return weights / weights.sum()


def require_evidence(evidence, decision_at):
    if not isinstance(evidence, dict) or not evidence.get('source_ref') or not evidence.get('captured_at'):
        raise ValueError('Timestamped evidence required')
    if timestamp(evidence['captured_at']) > timestamp(decision_at):
        raise ValueError('Evidence captured after decision boundary')
    if evidence.get('published_at') and timestamp(evidence['published_at']) > timestamp(decision_at):
        raise ValueError('Evidence published after decision boundary')


def validate_bank(bank):
    if bank.get('schema_version') != 1 or bank.get('scope') != 'partial_offense_not_full_dfs':
        raise ValueError('Unsupported joint production bank')
    game = bank['game']
    if not game.get('game_id') or game['home'] == game['away'] or timestamp(bank['decision_at']) >= timestamp(game['kickoff']):
        raise ValueError('Invalid game/decision boundary')
    ids = bank['scenario_ids']
    if len(ids) < 2 or len(set(ids)) != len(ids) or any(not i for i in ids):
        raise ValueError('Invalid scenario identities')
    players = bank['players']
    if not players or len({p['identity'] for p in players}) != len(players):
        raise ValueError('Invalid player identities')
    fields = ('rushYds', 'recYds', 'receptions', 'targets', 'carries')
    if not set(fields).issubset(bank['modeled_fields']) or not bank.get('missing_fields'):
        raise ValueError('Partial coverage must be explicit')
    for player in players:
        if player['team'] not in (game['home'], game['away']) or player['identity'].startswith('OTHER:'):
            raise ValueError('Invalid individual field identity')
        for field in fields:
            values = np.asarray(player['draws'][field], dtype=float)
            if values.shape != (len(ids),) or not np.isfinite(values).all() or not np.equal(values, np.rint(values)).all():
                raise ValueError(f'Invalid {field} draws')
            if field in ('receptions', 'targets', 'carries') and (values < 0).any():
                raise ValueError('Negative opportunity count')
        if np.any(np.asarray(player['draws']['receptions']) > np.asarray(player['draws']['targets'])):
            raise ValueError('Receptions exceed targets')
        if player.get('status') == 'out' and any(np.any(player['draws'][k]) for k in fields):
            raise ValueError('Out player has production')
    return normalized_weights(bank.get('weights'), len(ids))


def metric_matrix(bank, metric):
    if metric not in METRICS:
        raise ValueError('Unsupported outcome')
    validate_bank(bank)
    if metric == 'total_yards':
        return np.array([np.asarray(p['draws']['rushYds']) + np.asarray(p['draws']['recYds']) for p in bank['players']]).T
    return np.array([p['draws'][METRICS[metric]] for p in bank['players']]).T


def weighted_quantile(values, weights, quantiles):
    values = np.asarray(values, dtype=float)
    weights = normalized_weights(weights, len(values))
    order = np.argsort(values, kind='stable')
    positive = weights[order] > 0
    ordered = values[order][positive]
    cumulative = np.cumsum(weights[order][positive])
    levels = np.asarray(quantiles, dtype=float)
    if ((levels < 0) | (levels > 1)).any():
        raise ValueError('Invalid quantile')
    return ordered[np.minimum(np.searchsorted(cumulative, levels), len(ordered) - 1)]


def clone_bank(bank, branch):
    validate_bank(bank)
    result = deepcopy(bank)
    result['parent_sha256'] = digest(bank)
    result['branch'] = branch
    return result
