import pytest

from model.nfl_game_leaders_roles import RB_DEPTH_SPLIT, RESERVE, attach, team_scenario, usage_rates
from research.nfl_game_leaders_publish import MAX_UNASSIGNED_SHARE, publish

GAMES = ['g1', 'g2', 'g3', 'g4']


def box(identity, carries=0, targets=0, name=None):
    return {'identity': identity, 'name': name or identity, 'carries': carries, 'targets': targets}


def player(identity, position, status='unresolved', name=None):
    return {'identity': identity, 'name': name or identity, 'team': 'T', 'position': position, 'status': status}


def depth(**ranks):
    return {'captured_at': '2026-10-10T10:00:00+00:00',
            'comparisons': [{'name': n, 'position': p, 'fantasypros': {'rank': r}, 'sleeper': {}} for n, (p, r) in ranks.items()],
            'sleeper_only': []}


def test_missed_weeks_are_absences_and_two_games_are_shrunk():
    boxes = {'g1': [box('te', targets=6)], 'g2': [box('te', targets=6)], 'g3': [], 'g4': []}
    rate = usage_rates(boxes, GAMES)['te']
    assert rate['games_played'] == 2
    # Played rate 6, all-games rate 3: shrunk between them, nearer the played rate.
    assert 3 < rate['targets'] < 6
    assert rate['targets'] == pytest.approx((2 * 6 + 1 * 3) / 3)


def test_vacated_rb_carries_follow_depth_and_field_is_complete():
    boxes = {g: [box('star', carries=16, targets=2), box('rb2', carries=4, targets=1), box('qb', carries=4)] for g in GAMES}
    players = [player('star', 'RB', 'out'), player('rb2', 'RB'), player('rb3', 'RB'), player('qb', 'QB')]
    scen, note = team_scenario('T', players, depth(rb2=('RB', 1), rb3=('RB', 2), star=('RB', 3)), boxes, GAMES)
    carries = scen['carries']
    assert set(carries) == {'rb2', 'rb3', 'qb', 'NEW:T:0', 'NEW:T:1', 'NEW:T:2'}
    assert sum(carries.values()) == pytest.approx(1)
    assert carries['NEW:T:0'] * 3 == pytest.approx(RESERVE['carries'])
    # rb2 keeps 4 a game and inherits the first depth share of the star's 16; rb3 the second.
    split = RB_DEPTH_SPLIT[:2]
    expected_rb2 = 4 + 16 * split[0] / sum(split)
    expected_rb3 = 16 * split[1] / sum(split)
    total = expected_rb2 + expected_rb3 + 4
    assert carries['rb2'] == pytest.approx(expected_rb2 / total * (1 - RESERVE['carries']))
    assert carries['rb3'] == pytest.approx(expected_rb3 / total * (1 - RESERVE['carries']))
    assert note['vacated_rb_carries'] == 16


def test_out_quarterback_carries_go_to_the_backup():
    boxes = {g: [box('qb1', carries=6), box('rb', carries=15, targets=3)] for g in GAMES}
    players = [player('qb1', 'QB', 'out'), player('qb2', 'QB'), player('rb', 'RB')]
    scen, note = team_scenario('T', players, depth(qb2=('QB', 1), rb=('RB', 1)), boxes, GAMES)
    assert scen['carries']['qb2'] > 0
    assert note['qb_receiving_carries'] == 'qb2'


def test_departed_players_are_named_at_zero_and_newcomers_get_a_placeholder():
    boxes = {g: [box('wr1', targets=8), box('gone', carries=2, targets=3), box('rb', carries=12)] for g in GAMES}
    players = [player('wr1', 'WR'), player('rookie', 'WR'), player('deep', 'WR'), player('rb', 'RB')]
    scen, _ = team_scenario('T', players, depth(wr1=('WR', 1), rookie=('WR', 2), deep=('WR', 6)), boxes, GAMES)
    assert scen['targets']['gone'] == 0
    assert scen['targets']['rookie'] > 0
    assert scen['targets']['deep'] == 0


def test_attach_keeps_an_explicit_scenario_and_refuses_late_evidence():
    req = {'game': {'away': 'T', 'home': 'U'}, 'decision_at': '2026-10-10T09:00:00+00:00',
           'role_scenarios': {'T': {}}, 'players': []}
    assert attach(req, {}, {}) is req
    late = {**req, 'role_scenarios': None,
            'players': [player('a', 'RB'), {**player('b', 'RB'), 'team': 'U'}],
            'dual_depth': {'T': depth(a=('RB', 1)), 'U': depth(b=('RB', 1))}}
    boxes = {(g, t): [box(i, carries=10, targets=2)] for g in GAMES for t, i in (('T', 'a'), ('U', 'b'))}
    with pytest.raises(ValueError, match='after the decision'):
        attach(late, boxes, {'T': GAMES, 'U': GAMES})


def _forecast(residual_share):
    rows = [{'name': 'A', 'residual': False, 'win_share': 1 - residual_share},
            {'name': 'OTHER:T', 'residual': True, 'win_share': residual_share}]
    metrics = {m: {'players': rows} for m in ('receptions', 'receiving_yards', 'rushing_yards', 'total_yards')}
    return {'game': {'game_id': 'x'}, 'decision_at': 'd', 'availability_verified': False, 'metrics': metrics,
            'history_coverage': {}, 'training_game_ids': [], 'settings': {'draws': 1}, 'limits': [],
            'recent_history_verified': True, 'market_inputs_used': False, 'scope': 'full_game_including_overtime'}


def test_publish_refuses_unassigned_mass_above_the_limit():
    from model.nfl_game_leaders import VERSION
    batch = {'version': VERSION, 'authority': 'exploratory_not_calibrated', 'market_inputs_used': False,
             'season': 2026, 'week': 5, 'decision_at': 'd', 'skipped': [], 'reconciliation_rejections': [],
             'source_sha256': 's', 'forecasts': [_forecast(MAX_UNASSIGNED_SHARE + .01)]}
    with pytest.raises(ValueError, match='unassigned leader share'):
        publish(batch, [])
    batch['forecasts'] = [_forecast(.01)]
    assert publish(batch, [])['games'][0]['unassigned_share']['rushing_yards'] == pytest.approx(.01)


def test_sleeper_reserve_list_alone_counts_as_out_but_a_weekly_out_needs_agreement():
    from research.nfl_game_leaders import availability_status
    for reserve in ('IR', 'PUP', 'NFI', 'SUSPENDED'):
        assert availability_status(reserve, 'UNKNOWN', 'UNKNOWN') == 'out'
    assert availability_status('OUT', 'UNKNOWN', 'UNKNOWN') == 'unresolved'
    assert availability_status('OUT', 'OUT', 'UNKNOWN') == 'out'
    assert availability_status('QUESTIONABLE', 'IR', 'UNKNOWN') == 'unresolved'
    assert availability_status('', 'UNKNOWN', 'INACTIVE') == 'out'
    assert availability_status('ACTIVE', 'HEALTHY', 'UNKNOWN') == 'unresolved'


def test_roles_v2_blends_prior_targets_and_keeps_carries():
    from model.nfl_game_leaders_roles import PRIOR_TARGET_GAMES
    boxes = {'g1': [box('wr', carries=1, targets=3)], 'g2': [box('wr', carries=1, targets=3)], 'g3': [], 'g4': []}
    v1 = usage_rates(boxes, GAMES)['wr']
    v2 = usage_rates(boxes, GAMES, {'wr': 10.})['wr']
    assert v2['targets'] == pytest.approx((2 * v1['targets'] + PRIOR_TARGET_GAMES * 10.) / (2 + PRIOR_TARGET_GAMES))
    assert v2['carries'] == v1['carries']


def test_roles_v2_returner_gets_fraction_of_prior_and_v1_is_unchanged_without_prior():
    from model.nfl_game_leaders_roles import RETURNER_TARGET_FRACTION, VERSION, VERSION_V2
    boxes = {g: [box('wr1', carries=1, targets=8)] for g in GAMES}
    players = [player('wr1', 'WR'), player('back', 'WR')]
    v1, _ = team_scenario('T', players, depth(), boxes, GAMES)
    v2, _ = team_scenario('T', players, depth(), boxes, GAMES, {'back': 10.})
    assert v1['targets']['back'] == 0.
    expected_ratio = RETURNER_TARGET_FRACTION * 10. / usage_rates(boxes, GAMES)['wr1']['targets']
    assert v2['targets']['back'] / v2['targets']['wr1'] == pytest.approx(expected_ratio)
    req = {'game': {'away': 'T', 'home': 'U'}, 'decision_at': '2026-10-11T12:00:00+00:00',
           'players': players + [{'identity': 'u1', 'name': 'u1', 'team': 'U', 'position': 'WR', 'status': 'unresolved'}],
           'dual_depth': {'T': depth(), 'U': depth()}}
    index = {(g, 'T'): boxes[g] for g in GAMES} | {(g, 'U'): [box('u1', carries=1, targets=5)] for g in GAMES}
    assert attach(req, index, {'T': GAMES, 'U': GAMES})['scenario_evidence']['method'] == VERSION
    assert attach(req, index, {'T': GAMES, 'U': GAMES}, {})['scenario_evidence']['method'] == VERSION_V2


def test_prior_season_targets_requires_four_games_in_previous_season():
    from model.nfl_game_leaders_roles import prior_season_targets
    games = [{'game_id': f'p{i}', 'season': 2024, 'completed': True} for i in range(5)] + [
        {'game_id': 'c1', 'season': 2025, 'completed': True}]
    snap = {'games': games, 'box_verified_game_ids': [g['game_id'] for g in games],
            'boxes': [box('a', targets=6) | {'game_id': f'p{i}'} for i in range(4)]
                     + [box('b', targets=9) | {'game_id': f'p{i}'} for i in range(3)]
                     + [box('a', targets=20) | {'game_id': 'c1'}]}
    assert prior_season_targets(snap, 2025) == {'a': 6.}
