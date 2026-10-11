from __future__ import annotations

from collections import defaultdict
from itertools import combinations
from math import comb

import numpy as np

from model.nfl_joint_contracts import metric_matrix, normalized_weights, validate_bank, weighted_quantile


def set_credit(values, identities, selected, size=3):
    if len(set(selected)) != size or not set(selected).issubset(identities) or len(identities) < size:
        raise ValueError('Invalid exact set')
    values = np.asarray(values)
    boundary = np.partition(values, len(values) - size)[len(values) - size]
    above = {identities[j] for j in np.flatnonzero(values > boundary)}
    tied = {identities[j] for j in np.flatnonzero(values == boundary)}
    chosen = set(selected)
    return 1 / comb(len(tied), size - len(above)) if above.issubset(chosen) and chosen.issubset(above | tied) else 0.


def exact_set_probability(bank, metric, selected):
    weights = validate_bank(bank)
    identities = [p['identity'] for p in bank['players']]
    matrix = metric_matrix(bank, metric)
    credit = np.array([set_credit(row, identities, selected) for row in matrix])
    return float(weights @ credit)


def exact_sets(bank, metric, display_limit=20, maximum_tie_sets=10000):
    if display_limit < 1 or maximum_tie_sets < 1:
        raise ValueError('Positive display and tie budgets required')
    weights = validate_bank(bank)
    players = bank['players']
    if len(players) < 3:
        raise ValueError('At least three individuals required')
    identities = [p['identity'] for p in players]
    unresolved = {p['identity'] for p in players if p.get('residual', False)}
    probabilities = defaultdict(float)
    overflow = 0.
    for row, weight in zip(metric_matrix(bank, metric), weights):
        boundary = np.partition(row, len(row) - 3)[len(row) - 3]
        above = tuple(identities[j] for j in np.flatnonzero(row > boundary))
        tied = tuple(identities[j] for j in np.flatnonzero(row == boundary))
        choose = 3 - len(above)
        count = comb(len(tied), choose)
        if count > maximum_tie_sets:
            overflow += float(weight)
            continue
        for subset in combinations(tied, choose):
            probabilities[tuple(sorted(above + subset))] += float(weight / count)
    rows = sorted(probabilities.items(), key=lambda item: (-item[1], item[0]))
    unresolved_mass = sum(p for ids, p in rows if set(ids) & unresolved)
    return {'metric': metric, 'tie_rule': 'uniform_random_boundary_tiebreak',
            'field_scope': bank.get('field_scope', 'full_modeled_individual_field'), 'authority': 'exploratory',
            'sets': [{'identities': list(ids), 'probability': p} for ids, p in rows[:display_limit]],
            'displayed_mass': sum(p for _, p in rows[:display_limit]), 'enumerated_mass': sum(probabilities.values()),
            'unresolved_identity_mass': unresolved_mass, 'enumeration_overflow_mass': overflow,
            'full_distribution': [{'identities': list(ids), 'probability': p} for ids, p in rows]}


def summarize(bank):
    weights = validate_bank(bank)
    output = {}
    for metric in ('rushing_yards', 'receiving_yards', 'receptions', 'total_yards'):
        matrix = metric_matrix(bank, metric)
        winning = matrix == matrix.max(axis=1)[:, None]
        credits = winning / winning.sum(axis=1)[:, None]
        rows = []
        for j, player in enumerate(bank['players']):
            values = matrix[:, j]
            quantiles = weighted_quantile(values, weights, [.1, .5, .8, .9, .95])
            row = {k: player.get(k) for k in ('identity', 'name', 'team', 'residual')}
            row.update(mean=float(weights @ values), leader_share=float(weights @ credits[:, j]),
                       **{k: float(v) for k, v in zip(('p10', 'p50', 'p80', 'p90', 'p95'), quantiles)})
            if metric == 'receptions':
                row['count_probabilities'] = {str(int(v)): float(weights[values == v].sum()) for v in np.unique(values)}
            rows.append(row)
        output[metric] = {'players': sorted(rows, key=lambda r: -r['leader_share']),
                          'tie_probability': float(weights @ (winning.sum(axis=1) > 1))}
    return output


def portfolio_returns(bank, wagers):
    weights = validate_bank(bank)
    returns = np.zeros(len(weights))
    stake = 0.
    for wager in wagers:
        if wager['stake'] < 0 or wager['decimal_odds'] <= 1 or not np.isfinite([wager['stake'], wager['decimal_odds']]).all():
            raise ValueError('Invalid wager')
        stake += wager['stake']
        matrix = metric_matrix(bank, wager['metric'])
        identities = [p['identity'] for p in bank['players']]
        if wager['identity'] not in identities:
            raise ValueError('Wager player outside field')
        j = identities.index(wager['identity'])
        winning = matrix == matrix.max(axis=1)[:, None]
        credit = winning[:, j] / winning.sum(axis=1)
        returns += wager['stake'] * (wager['decimal_odds'] * credit - 1)
    return {'settlement': 'single_leader_dead_heat_stake_split', 'stake': stake,
            'expected_net': float(weights @ returns), 'loss_probability': float(weights @ (returns < 0)),
            'p10_p50_p90_net': weighted_quantile(returns, weights, [.1, .5, .9]).tolist(), 'net_draws': returns.tolist()}
