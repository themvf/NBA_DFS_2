from __future__ import annotations

from itertools import combinations
from math import comb

import numpy as np

from model.nfl_joint_contracts import digest, metric_matrix, validate_bank, weighted_quantile
from model.nfl_joint_decisions import exact_set_probability, exact_sets
from model.nfl_longest_touchdown import timestamp
from model.nfl_role_dispersion import interval_diagnostic


def crps(values, actual, weights=None):
    values = np.asarray(values, float)
    from model.nfl_joint_contracts import normalized_weights
    weights = normalized_weights(weights, len(values))
    if not np.isfinite(values).all() or not np.isfinite(actual):
        raise ValueError('Invalid distribution labels')
    order = np.argsort(values)
    x, w = values[order], weights[order]
    previous_w = np.cumsum(w) - w
    previous_wx = np.cumsum(w * x) - w * x
    return float(weights @ np.abs(values - actual) - np.sum(w * (x * previous_w - previous_wx)))


def grade_bank(bank, boxes, floor=1e-12):
    if not 0 < floor < 1:
        raise ValueError('Invalid reporting floor')
    weights = validate_bank(bank)
    if len({b['identity'] for b in boxes}) != len(boxes):
        raise ValueError('Duplicate actual identities')
    if any(b.get('game_id', bank['game']['game_id']) != bank['game']['game_id'] for b in boxes):
        raise ValueError('Actuals outside forecast game')
    actual_by_id = {b['identity']: b for b in boxes}
    identities = [p['identity'] for p in bank['players']]
    metrics = {}
    for metric in ('rushing_yards', 'receiving_yards', 'receptions', 'total_yards'):
        def observed(box):
            return box['rushing_yards'] + box['receiving_yards'] if metric == 'total_yards' else box[metric]
        if not boxes or len(boxes) < 3:
            raise ValueError('Full-field actual labels required')
        actual_values = np.array([observed(b) for b in boxes], float)
        if not np.isfinite(actual_values).all():
            raise ValueError('Nonfinite actual labels')
        boundary = np.partition(actual_values, len(boxes) - 3)[len(boxes) - 3]
        zero_field_unresolved = boundary == 0 and bool(set(identities) - set(actual_by_id))
        above = tuple(b['identity'] for b in boxes if observed(b) > boundary)
        tied = tuple(b['identity'] for b in boxes if observed(b) == boundary)
        count = comb(len(tied), 3 - len(above))
        scores, missing, actual_sets = [], [], []
        for subset in combinations(tied, 3 - len(above)):
            selected = tuple(sorted(above + subset))
            probability = exact_set_probability(bank, metric, selected) if set(selected).issubset(identities) else 0.
            scores.append(-np.log(max(probability, floor)) / count)
            if not set(selected).issubset(identities):
                missing.append(selected)
            actual_sets.append({'identities': list(selected), 'probability': probability, 'actual_credit': 1 / count})
        distribution = exact_sets(bank, metric)
        probability_sq = sum(row['probability'] ** 2 for row in distribution['full_distribution'])
        brier = None if distribution['enumeration_overflow_mass'] > 1e-12 else probability_sq + 1 / count - 2 * sum(row['probability'] / count for row in actual_sets)
        player_scores = []
        matrix = metric_matrix(bank, metric)
        simulated_winners = matrix == matrix.max(axis=1)[:, None]
        predicted_winner_credit = weights @ (simulated_winners / simulated_winners.sum(axis=1)[:, None])
        actual_winners = {b['identity'] for b in boxes if observed(b) == actual_values.max()}
        actual_credit = 1 / len(actual_winners)
        leader_brier = float(predicted_winner_credit @ predicted_winner_credit + actual_credit -
                             2 * sum(predicted_winner_credit[j] * actual_credit for j, identity in enumerate(identities) if identity in actual_winners))
        leader_log = -sum(np.log(max(predicted_winner_credit[identities.index(i)] if i in identities else 0, floor)) * actual_credit for i in actual_winners)
        for j, player in enumerate(bank['players']):
            if player.get('residual') or player['identity'] not in actual_by_id:
                continue
            actual = observed(actual_by_id[player['identity']])
            lower, median, upper = weighted_quantile(matrix[:, j], weights, [.1, .5, .9])
            player_scores.append({'identity': player['identity'], 'actual': actual, 'crps': crps(matrix[:, j], actual, weights),
                'p10': float(lower), 'p50': float(median), 'p90': float(upper), 'exceeds_p90': bool(actual > upper),
                **interval_diagnostic(actual, lower, upper)})
        metrics[metric] = {'exact_set_log_loss': None if zero_field_unresolved else float(sum(scores)), 'exact_set_brier': None if brier is None or zero_field_unresolved else float(brier),
                           'actual_sets': actual_sets, 'unmodeled_actual_sets': [list(s) for s in missing],
                           'player_scores': player_scores, 'enumeration_overflow_mass': distribution['enumeration_overflow_mass'],
                           'leader_brier': leader_brier, 'leader_log_loss': float(leader_log),
                           'first_choice_credit': actual_credit if identities[int(np.argmax(predicted_winner_credit))] in actual_winners else 0.,
                           'mean_crps': float(np.mean([p['crps'] for p in player_scores])) if player_scores else None,
                           'interval_80_coverage': float(np.mean([p['covered'] for p in player_scores])) if player_scores else None,
                           'p90_exceedance': float(np.mean([p['exceeds_p90'] for p in player_scores])) if player_scores else None,
                           'status': 'zero_boundary_eligible_actual_field_unresolved' if zero_field_unresolved else 'graded'}
    return {'game_id': bank['game']['game_id'], 'forecast_sha256': digest(bank), 'actuals_sha256': digest(boxes),
            'metrics': metrics, 'log_reporting_floor': floor, 'authority': 'research_grade_not_validation',
            'limits': ['Boundary ties graded with random-tiebreak fractional targets.',
                       'Zero-sample probabilities use a numerical reporting floor, not a probability estimate.']}


def validate_registration(study):
    required = ('study_id', 'registered_at', 'variants', 'primary_metric', 'metrics', 'populations',
                'development_games', 'selection_games', 'locked_games', 'exposed_games', 'gates',
                'draws', 'seeds', 'multiplicity', 'tie_rule', 'source_policy', 'status')
    if any(k not in study for k in required):
        raise ValueError('Incomplete study registration')
    timestamp(study['registered_at'])
    if study['primary_metric'] != 'exact_set_log_loss' or study['tie_rule'] != 'uniform_random_boundary_tiebreak':
        raise ValueError('Unsupported primary endpoint/tie contract')
    if study['status'] not in ('development', 'locked', 'opened') or study['draws'] < 2 or not study['seeds']:
        raise ValueError('Invalid study execution state')
    populations = [set(study[k]) for k in ('development_games', 'selection_games', 'locked_games')]
    if any(populations[i] & populations[j] for i in range(3) for j in range(i)):
        raise ValueError('Overlapping study populations')
    if populations[2] & set(study['exposed_games']):
        raise ValueError('Locked games were previously examined')
    if len(set(study['variants'])) != len(study['variants']) or not study['variants']:
        raise ValueError('Unique model variants required')
    if study['status'] == 'locked':
        if not study['locked_games'] or study.get('exposure_registry_status') != 'audited' or not study.get('exposure_registry_sha256'):
            raise ValueError('Locked study requires an audited exposure registry and populated holdout')
        if not isinstance(study['gates'].get('minimum_locked_games'), int) or len(study['locked_games']) < study['gates']['minimum_locked_games']:
            raise ValueError('Locked sample does not meet registered precision plan')
    return digest(study)


def paired_bootstrap(candidate, baseline, seed=1, samples=2000):
    if set(candidate) != set(baseline) or not candidate or samples < 10:
        raise ValueError('Matched game scope and bootstrap sample required')
    differences = np.array([candidate[k] - baseline[k] for k in sorted(candidate)], float)
    if not np.isfinite(differences).all():
        raise ValueError('Nonfinite paired scores')
    rng = np.random.default_rng(seed)
    draws = np.mean(rng.choice(differences, (samples, len(differences))), axis=1)
    return {'games': len(differences), 'mean_difference': float(differences.mean()),
            'ci95': np.quantile(draws, [.025, .975]).tolist(), 'unit': 'paired_whole_games',
            'passed_unadjusted_improvement': bool(np.quantile(draws, .975) < 0),
            'authority': 'diagnostic_requires_registered_multiplicity_gate'}
