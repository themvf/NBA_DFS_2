from copy import deepcopy

import pytest

from ingest.nfl_dfs_receiving_components import unique_rows, paired_week_intervals
from model.nfl_dfs_receiving_components import component_forecast, population_prior, replay, validate_history


def row(week=1, identity='a', **changes):
    return {'season': 2023, 'week': week, 'game_id': str(week), 'identity': identity, 'name': identity,
            'position': 'WR', 'team': 'SF', 'targets': 10., 'receptions': 6., 'receiving_yards': 60.,
            'receiving_tds': 1., 'fpts': 18., **changes}


PRIOR = {'catch_rate': .6, 'yards_per_reception': 10., 'td_per_target': .1, 'targets': 1000}


def test_components_score_separately_and_do_not_scale_other_scoring_with_targets():
    history = [row(fpts=21.)]*4  # Three points from other observed scoring.
    result = component_forecast(history, 10., PRIOR)
    assert result['receptions'] == 6
    assert result['receiving_yards'] == 60
    assert result['receiving_tds'] == 1
    assert result['mean'] == 21
    more = component_forecast(history, 20., PRIOR)
    assert more['points']['bonuses_and_other'] == 3
    assert more['mean'] == 39  # No automatic bonus at projected 120 yards.
    assert sum(more['points'].values()) == more['mean']


def test_small_samples_shrink_and_missing_team_games_only_affect_other_scoring():
    small = [row(targets=1., receptions=1., receiving_yards=20., receiving_tds=0., fpts=3.)]
    large = small*100
    a, b = component_forecast(small, 5., PRIOR), component_forecast(large, 5., PRIOR)
    assert .6 < a['catch_rate'] < b['catch_rate'] < 1
    assert 10 < a['yards_per_reception'] < b['yards_per_reception'] < 20
    assert component_forecast([None, *small], 5., PRIOR)['catch_rate'] == a['catch_rate']
    for targets in [-1., float('nan'), float('inf')]:
        with pytest.raises(ValueError):
            component_forecast(small, targets, PRIOR)


def test_population_cannot_see_target_week_and_lateral_td_records_are_preserved():
    prior_rows = [row(identity=str(i)) for i in range(40)]
    expected = population_prior(prior_rows, (2023, 2))
    assert population_prior(prior_rows+[row(week=2, receiving_yards=10000)], (2023, 2)) == expected
    validate_history([row(position='QB', targets=0., receptions=0., receiving_yards=7., receiving_tds=1.)])
    with pytest.raises(ValueError):
        validate_history([row(), row()])
    with pytest.raises(ValueError):
        validate_history([row(receptions=11.)])
    with pytest.raises(ValueError):
        validate_history([row(receiving_yards=float('nan'))])


def test_replay_target_week_and_order_do_not_leak_into_any_prediction():
    history, teams = [], []
    for week in range(1, 18):
        teams.append({'season': 2023, 'week': week, 'game_id': str(week), 'team': 'SF', 'targets': 120., 'primary_qb': 'qb'})
        for i in range(12):
            history.append(row(week, str(i), receiving_yards=float(50+i), fpts=17+i/10))
    before, _ = replay(history, teams)
    changed = deepcopy(history)
    for r in changed:
        if r['week'] == 17:
            r.update(receptions=0., receiving_yards=0., receiving_tds=0., fpts=0.)
    after, _ = replay(changed, teams)
    reversed_rows, _ = replay(list(reversed(history)), list(reversed(teams)))
    assert before and len(before) == len(after)
    for a, b, c in zip(before, after, reversed_rows):
        assert a['candidate'] == b['candidate'] == c['candidate']
        assert a['components'] == b['components'] == c['components']
        assert sum(a['components']['points'].values())+a['calibration_offset'] == pytest.approx(a['candidate']['mean'])
        assert a['candidate']['p10'] <= a['candidate']['p50'] <= a['candidate']['p90']


def test_duplicate_comparisons_fail_and_week_bootstrap_is_reproducible():
    with pytest.raises(ValueError):
        unique_rows([row(), row()])
    rows = []
    for week in range(1, 5):
        rows.append({'week': week, 'actual': 10., 'production': {'mean': 12., 'p10': 0., 'p90': 20.},
                     'candidate': {'mean': 11., 'p10': 2., 'p90': 18.},
                     'candidate_boom_probability': .1, 'production_boom_probability': .2})
    a = paired_week_intervals(rows)
    assert a == paired_week_intervals(rows)
    assert a['mae']['lower95'] == a['mae']['upper95'] == -1
