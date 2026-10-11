import json

import numpy as np
import pytest

from model.nfl_game_leaders_roles import blend_prior
from research.nfl_game_leaders_calibration import (calibration_bins, evaluate, fit, game_log_loss, softmax_baseline,
                                                   tempered)


def test_temperature_one_is_identity_and_sharpening_concentrates_mass():
    rows = [['a', False, .6, 50.], ['b', False, .3, 40.], ['OTHER:X', True, .1, None]]
    assert tempered(rows, 1.) == pytest.approx({'a': .6, 'b': .3, 'OTHER:X': .1})
    assert tempered(rows, .5)['a'] > .6
    assert sum(tempered(rows, 1.7).values()) == pytest.approx(1)


def test_softmax_baseline_orders_by_recent_average_and_reserves_unnamed_mass():
    rows = [['a', False, .2, 80.], ['b', False, .7, 40.], ['OTHER:X', True, .1, None]]
    p = softmax_baseline(rows, 10., .03)
    assert p['a'] > p['b']
    assert p['OTHER:X'] == pytest.approx(.03)
    assert sum(p.values()) == pytest.approx(1)


def test_log_loss_splits_tied_leaders():
    assert game_log_loss({'a': .5, 'b': .25}, {'a': .5, 'b': .5}) == pytest.approx(-(.5 * np.log(.5) + .5 * np.log(.25)))


def test_prior_blend_is_off_by_default_and_fills_a_player_without_current_usage():
    rates = {'a': {'games_played': 2, 'carries': 10., 'targets': 2.}}
    prior = {'a': {'carries': 20., 'targets': 4.}, 'b': {'carries': 0., 'targets': 6.}}
    assert blend_prior(rates, prior, 0.) is rates
    out = blend_prior(rates, prior, 2.)
    assert out['a']['carries'] == pytest.approx((2 * 10 + 2 * 20) / 4)
    assert out['b']['targets'] == pytest.approx(6.)


def _record(season, gid, shares, target, baseline_means=(60., 30.)):
    rows = [['a', False, shares[0], baseline_means[0]], ['b', False, shares[1], baseline_means[1]], ['OTHER:X', True, shares[2], None]]
    metric = {'rows': rows, 'target': target, 'log_loss': game_log_loss(dict(zip(['a', 'b', 'OTHER:X'], shares)), target),
              'brier': 0., 'top_choice_credit': target.get('a', 0.), 'baseline_credit': target.get('a', 0.)}
    variant = {m: metric for m in ('rushing_yards', 'receiving_yards', 'receptions', 'total_yards')}
    return {'game_id': gid, 'season': season, 'week': 5, 'variants': {v: variant for v in ('A', 'B', 'C2', 'C4', 'C8')}}


def test_fit_and_evaluate_run_end_to_end(tmp_path):
    rng = np.random.default_rng(0)
    records = []
    for j in range(60):
        season = 2023 + j % 3
        winner = 'a' if rng.random() < .7 else 'b'
        records.append(_record(season, f'g{j}', (.5, .45, .05), {winner: 1.}))
    records.append({'game_id': 'skip', 'season': 2024, 'week': 1, 'skipped': 'reason'})
    path = tmp_path / 'dev.jsonl'
    path.write_text('\n'.join(json.dumps(r) for r in records))
    fitted = fit(path)
    assert fitted['variant_choice']['chosen'] in ('B', 'C2', 'C4', 'C8')
    assert fitted['skipped'] == 1
    # 'a' wins 70% while the raw forecast says 50%: the fitted temperature should sharpen (T < 1).
    assert fitted['temperature']['rushing_yards'] < 1
    report = evaluate(records, fitted)
    assert report['games'] == 60
    market = report['markets']['rushing_yards']
    assert market['primary']['ci95'][0] <= market['primary']['mean_difference'] <= market['primary']['ci95'][1]
    assert calibration_bins([r for r in records if 'variants' in r], 'B', 'receptions', 1.)['expected_calibration_error'] >= 0
