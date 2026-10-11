from __future__ import annotations

from dataclasses import dataclass

import numpy as np
from scipy.optimize import linprog, minimize
from scipy.special import logsumexp

from model.nfl_joint_contracts import clone_bank, digest, metric_matrix, require_evidence, validate_bank


def implied_probability(price, price_format='american'):
    if not np.isfinite(price):
        raise ValueError('Nonfinite price')
    if price_format == 'decimal':
        if price <= 1:
            raise ValueError('Invalid decimal odds')
        return 1 / price
    if price_format != 'american' or abs(price) < 100:
        raise ValueError('Invalid American odds')
    return 100 / (price + 100) if price > 0 else -price / (100 - price)


def paired_probability(over, under, method='power', price_format='american'):
    raw = np.array([implied_probability(over, price_format), implied_probability(under, price_format)])
    if method == 'normalize':
        return float(raw[0] / raw.sum())
    if method != 'power':
        raise ValueError('Unsupported margin method')
    from scipy.optimize import brentq
    exponent = brentq(lambda power: np.power(raw, power).sum() - 1, .0001, 10000.)
    return float(raw[0] ** exponent)


def quote_constraints(quotes, metric_map, decision_at, method='power', tolerance=.02):
    groups = {}
    for quote in quotes:
        require_evidence({'source_ref': quote['payload_digest'], 'captured_at': quote['observed_at'],
                          'published_at': quote.get('published_at')}, decision_at)
        if not quote.get('eligible_pregame') or not quote.get('identity') or not quote.get('game_id') or quote['market'] not in metric_map:
            continue
        key = (quote['game_id'], quote['identity'], quote['book'], quote['market'], quote['line'], quote['observed_at'])
        sides = groups.setdefault(key, {})
        if quote['side'] in sides:
            raise ValueError('Duplicate side in quote pair')
        sides[quote['side']] = quote
    constraints, unpaired = [], []
    for key, sides in groups.items():
        if set(sides) != {'over', 'under'}:
            unpaired.append({'identity': key[1], 'book': key[2], 'market': key[3], 'line': key[4]})
            continue
        over, under = sides['over'], sides['under']
        if over['price_format'] != under['price_format'] or over['comparator'] != 'gt' or under['comparator'] != 'lt':
            raise ValueError('Incompatible paired quote settlement')
        probability = paired_probability(over['price'], under['price'], method, over['price_format'])
        constraints.append({'id': digest(key)[:24], 'identity': key[1], 'metric': metric_map[key[3]],
                            'game_id': key[0], 'book': key[2], 'line': key[4], 'comparator': 'gt',
                            'probability': probability, 'tolerance': tolerance, 'basis': 'paired_quotes',
                            'margin_method': method, 'conditional_on_no_push': float(key[4]).is_integer(),
                            'evidence': {'source_ref': over['payload_digest'], 'captured_at': over['observed_at'],
                                         'published_at': over.get('published_at')}})
    return {'constraints': constraints, 'unpaired': unpaired, 'decision_at': decision_at,
            'limits': ['Matched paired quotes only; one-sided lines retained without invented fair probabilities.']}


@dataclass(frozen=True)
class ReweightSettings:
    method: str = 'soft'
    penalty: float = 1000.
    minimum_ess_fraction: float = .10
    warning_ess_fraction: float = .25
    maximum_iterations: int = 1000

    def __post_init__(self):
        if self.method not in ('soft', 'hard') or not np.isfinite(self.penalty) or self.penalty <= 0:
            raise ValueError('Invalid reweight settings')
        if not 0 < self.minimum_ess_fraction <= self.warning_ess_fraction <= 1 or self.maximum_iterations < 1:
            raise ValueError('Invalid ESS/iteration settings')


def constraint_matrix(bank, constraints):
    if not constraints or len({r['id'] for r in constraints}) != len(constraints):
        raise ValueError('Nonempty unique constraints required')
    identities = [p['identity'] for p in bank['players']]
    columns, targets, tolerances = [], [], []
    for row in constraints:
        require_evidence(row['evidence'], bank['decision_at'])
        if (row.get('game_id') or row.get('basis') == 'paired_quotes') and row.get('game_id') != bank['game']['game_id']:
            raise ValueError('Constraint event does not match forecast')
        if row.get('basis') not in ('paired_quotes', 'assumed_margin', 'research_probability'):
            raise ValueError('Constraint probability basis required')
        if row['basis'] == 'assumed_margin' and not row.get('margin_assumption'):
            raise ValueError('One-sided margin assumption must be explicit')
        if row['identity'] not in identities:
            raise ValueError('Constraint player outside modeled field')
        values = metric_matrix(bank, row['metric'])[:, identities.index(row['identity'])]
        line = row['line']
        if not np.isfinite(line):
            raise ValueError('Invalid threshold')
        comparator = row['comparator']
        if comparator not in ('gt', 'ge', 'lt', 'le', 'eq'):
            raise ValueError('Explicit threshold comparator required')
        event = {'gt': values > line, 'ge': values >= line, 'lt': values < line,
                 'le': values <= line, 'eq': values == line}[comparator]
        probability, tolerance = float(row['probability']), float(row.get('tolerance', .02))
        if not np.isfinite([probability, tolerance]).all() or not 0 <= probability <= 1 or not 0 <= tolerance < 1:
            raise ValueError('Invalid probability interval')
        if row.get('conditional_on_no_push'):
            if comparator not in ('gt', 'lt'):
                raise ValueError('No-push conditional quote requires strict comparator')
            column = event.astype(float) - probability * (values != line)
            targets.append(0.)
        else:
            column = event.astype(float)
            targets.append(probability)
        columns.append(column)
        tolerances.append(tolerance)
    return np.column_stack(columns), np.array(targets), np.array(tolerances)


def reweight(bank, constraints, settings=ReweightSettings()):
    parent = validate_bank(bank)
    matrix, targets, tolerances = constraint_matrix(bank, constraints)
    support = parent > 0
    a, q = matrix[support], parent[support]
    q /= q.sum()
    result = clone_bank(bank, 'player-market-reweighted')
    feasible = True
    if settings.method == 'hard':
        feasible = linprog(np.zeros(len(q)), A_eq=np.vstack([np.ones(len(q)), a.T]),
                           b_eq=np.r_[1., targets], bounds=(0, None), method='highs').success
    if not feasible:
        result.update(weights=parent.tolist(), branch=bank.get('branch', 'independent'))
        result['market_reweighting'] = {'accepted': False, 'reason': 'infeasible_constraints', 'constraints_sha256': digest(constraints)}
        return result
    logq = np.log(q)
    def objective(parameters):
        logits = logq + a @ parameters
        logz = logsumexp(logits)
        weights = np.exp(logits - logz)
        regularizer = .5 * np.sum(parameters ** 2) / settings.penalty if settings.method == 'soft' else 0.
        gradient = a.T @ weights - targets
        if settings.method == 'soft':
            gradient += parameters / settings.penalty
        return logz - targets @ parameters + regularizer, gradient
    solved = minimize(objective, np.zeros(len(targets)), jac=True, method='L-BFGS-B',
                      options={'maxiter': settings.maximum_iterations, 'gtol': 1e-10, 'ftol': 1e-13})
    logits = logq + a @ solved.x
    fitted = np.exp(logits - logsumexp(logits))
    weights = np.zeros_like(parent)
    weights[support] = fitted
    residual = matrix.T @ weights - targets
    ess = 1 / float(weights @ weights)
    acceptable = bool(solved.success and ess / len(weights) >= settings.minimum_ess_fraction and np.all(np.abs(residual) <= (1e-6 if settings.method == 'hard' else tolerances)))
    diagnostics = {'accepted': acceptable, 'solver_success': bool(solved.success), 'message': str(solved.message),
                   'method': settings.method, 'penalty': settings.penalty, 'ess': ess, 'ess_fraction': ess / len(weights),
                   'ess_warning': ess / len(weights) < settings.warning_ess_fraction,
                   'max_weight': float(weights.max()), 'kl_divergence': float(fitted @ (np.log(fitted.clip(1e-300)) - logq)),
                   'constraints_sha256': digest(constraints), 'constraints': [],
                   'reason': None if acceptable else 'solver_residual_or_ess_guard',
                   'limits': ['Marginal constraints do not identify tails or teammate dependence.', 'Reweighting cannot add outcomes absent from parent support.']}
    for j, row in enumerate(constraints):
        values = metric_matrix(bank, row['metric'])[:, [p['identity'] for p in bank['players']].index(row['identity'])]
        push_mass = float(weights @ (values == row['line']))
        achieved = float(matrix[:, j] @ weights)
        if row.get('conditional_on_no_push'):
            achieved = None if push_mass >= 1 - 1e-12 else float((achieved + row['probability'] * (1 - push_mass)) / (1 - push_mass))
            if achieved is None or abs(achieved - row['probability']) > row.get('tolerance', .02):
                acceptable = False
        diagnostics['constraints'].append({'id': row['id'], 'target': row['probability'], 'achieved': achieved,
                                           'moment_residual': float(residual[j]), 'push_mass': push_mass,
                                           'tolerance': float(tolerances[j]), 'basis': row['basis']})
    diagnostics['accepted'] = acceptable
    if not acceptable and diagnostics['reason'] is None:
        diagnostics['reason'] = 'undefined_or_inaccurate_conditional_probability'
    result['weights'] = (weights if acceptable else parent).tolist()
    if not acceptable:
        result['branch'] = bank.get('branch', 'independent')
    result['market_reweighting'] = diagnostics
    result['market_constraints'] = constraints
    return result
