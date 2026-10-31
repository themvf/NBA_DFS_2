"""nfl-leaders-regime-roles-v1: the roles-v3 estimator reduces to v2, the grid, floors, scoring and the one look."""
import json
import math
from datetime import datetime, timezone
from itertools import product
from pathlib import Path

import numpy as np
import pytest

from model.nfl_game_leaders_roles import RB_DEPTH_SPLIT, RESERVE, attach, team_scenario, usage_rates
from research import nfl_game_leaders_regime_roles as rr
from research.nfl_prop_under_study import pins_hold

GAMES = ['2025_01_T_A', '2025_02_T_B', '2025_03_T_C', '2025_04_T_D', '2025_05_T_E']
DEPTH_AT = '2025_10_01T10:00:00+00:00'


def box(identity, carries=0, targets=0):
    return {'identity': identity, 'name': identity, 'carries': carries, 'targets': targets}


def player(identity, position, status='unresolved', sleeper_status='ACTIVE'):
    return {'identity': identity, 'name': identity, 'team': 'T', 'position': position, 'status': status,
            'sleeper_status': sleeper_status}


def depth(**ranks):
    return {'captured_at': '2025-10-01T10:00:00+00:00',
            'comparisons': [{'name': n, 'position': p, 'fantasypros': {'rank': r}, 'sleeper': {}} for n, (p, r) in ranks.items()],
            'sleeper_only': []}


def hall_fixture():
    """A lead back (hall) who played the first three window games and missed the last two; allen took over."""
    boxes = {GAMES[0]: [box('hall', 18, 3), box('allen', 4, 1), box('qb', 3), box('wr1', 0, 8), box('te1', 0, 5)],
             GAMES[1]: [box('hall', 20, 2), box('allen', 3, 2), box('qb', 4), box('wr1', 0, 9), box('te1', 0, 4)],
             GAMES[2]: [box('hall', 15, 4), box('allen', 6, 0), box('qb', 2), box('wr1', 0, 7), box('te1', 0, 6), box('gone', 2, 0)],
             GAMES[3]: [box('allen', 22, 3), box('davis', 4, 1), box('qb', 5), box('wr1', 0, 10), box('te1', 0, 3)],
             GAMES[4]: [box('allen', 24, 2), box('davis', 3, 0), box('qb', 3), box('wr1', 0, 8), box('te1', 0, 5)]}
    players = [player('hall', 'RB', 'out'), player('allen', 'RB'), player('davis', 'RB'), player('qb', 'QB'),
               player('wr1', 'WR'), player('te1', 'TE'), player('wr2', 'WR')]
    ranks = depth(allen=('RB', 1), davis=('RB', 2), hall=('RB', 3), qb=('QB', 1), wr1=('WR', 1), wr2=('WR', 2), te1=('TE', 1))
    prior = {'hall': {'carries': 16., 'targets': 3.}, 'allen': {'carries': 2., 'targets': 1.},
             'wr1': {'carries': 0., 'targets': 7.}, 'wr2': {'carries': 0., 'targets': 4.}}
    return boxes, players, ranks, prior


def same(a, b):
    return set(a) == set(b) and all(a[i] == pytest.approx(b[i], abs=1e-12) for i in a)


def test_pinned_game_leaders_files_are_untouched():
    """v3 lives in its own research module; the prop-under pins must still hold."""
    registration = json.loads(Path('docs/nfl-prop-under-study-v2.json').read_text())
    held = pins_hold(registration)
    assert all(held.values()), [k for k, v in held.items() if not v]


def test_registered_constants():
    assert rr.STUDY == 'nfl-leaders-regime-roles-v1' and rr.ESTIMATOR == 'nfl-game-leaders-roles-v3'
    assert rr.K_GRID == (0.5, 1.0, 2.0, 4.0) and rr.H_GRID == (math.inf, 4.0, 3.0, 2.0, 1.0)
    assert len(rr.grid_cells()) == 20
    assert rr.DEVELOPMENT == {2023: (4, 18), 2024: (1, 18), 2025: (1, 18)}
    assert rr.CONFIRMATION == {'season': 2026, 'first_week': 7, 'last_week': 18}
    assert rr.FLOORS == {'affected_team_games': 60, 'game_weeks': 8}
    assert rr.BOOTSTRAP == {'resamples': 2000, 'seed': 20261018, 'cluster': 'season_week', 'interval': 0.95}
    assert rr.GUARD_UPPER_BOUND == 0.005
    assert rr.parse_cell(rr.cell_key(0.5, math.inf)) == (0.5, math.inf)
    assert rr.parse_cell(rr.cell_key(4, 3)) == (4.0, 3.0)


def test_equal_weights_at_infinite_half_life_and_halving_per_game():
    assert rr.window_weights(5, math.inf) == [1.0] * 5
    w = rr.window_weights(5, 1.0)
    assert w[-1] == 1 and w[-2] == pytest.approx(.5) and w[0] == pytest.approx(1 / 16)
    assert rr.window_weights(3, 2.0)[0] == pytest.approx(.5)


def test_weighted_usage_rates_match_v2_at_equal_weight():
    boxes, _, _, _ = hall_fixture()
    assert rr.weighted_usage_rates(boxes, GAMES, math.inf) == usage_rates(boxes, GAMES)
    # The shuffled window gives the same answer: order comes from the game ids.
    assert rr.weighted_usage_rates(boxes, list(reversed(GAMES)), math.inf) == usage_rates(boxes, GAMES)


def test_v3_at_its_defaults_is_v2_exactly_including_prior_newcomer_departed_and_out_qb():
    boxes, players, ranks, prior = hall_fixture()
    players = players + [player('qb', 'QB', 'out')][:0]
    v2, _ = team_scenario('T', players, ranks, boxes, GAMES, prior, prior_weight=2.)
    v3, note = rr.team_scenario_v3('T', players, ranks, boxes, GAMES, prior, prior_weight=2.)
    assert same(v2['carries'], v3['carries']) and same(v2['targets'], v3['targets'])
    assert note['affected'] and note['affected_by'][0]['name'] == 'hall' and note['regime']['RB']['games'] == GAMES[3:]
    # K = inf with regime games, and finite K with none, are both v2.
    inf, _ = rr.team_scenario_v3('T', players, ranks, boxes, GAMES, prior, 2., K=math.inf)
    assert same(v2['carries'], inf['carries'])
    played_all = {g: [box('hall', 18, 3), box('allen', 4, 1), box('qb', 3), box('wr1', 0, 8)] for g in GAMES}
    v2b, _ = team_scenario('T', players, ranks, played_all, GAMES, prior, 2.)
    v3b, note_b = rr.team_scenario_v3('T', players, ranks, played_all, GAMES, prior, 2., K=1.)
    assert same(v2b['carries'], v3b['carries']) and not note_b['affected'] and note_b['regime']['RB']['games'] == []


def test_attach_v3_reduces_to_the_pinned_attach_and_marks_itself_shadow():
    boxes, players, ranks, prior = hall_fixture()
    index = {(g, 'T'): rows for g, rows in boxes.items()}
    index.update({(g, 'O'): [box('orb', 15, 2), box('owr', 0, 9)] for g in GAMES})
    opp = [player('orb', 'RB') | {'team': 'O'}, player('owr', 'WR') | {'team': 'O'}]
    request = {'game': {'game_id': '2025_06_T_O', 'away': 'T', 'home': 'O', 'season': 2025, 'week': 6},
               'decision_at': '2025-10-02T10:00:00+00:00', 'players': players + opp,
               'dual_depth': {'T': ranks, 'O': depth(orb=('RB', 1), owr=('WR', 1))}}
    prior_games = {'T': GAMES, 'O': GAMES}
    v2 = attach(request, index, prior_games, prior, 2.)
    v3 = rr.attach_v3(request, index, prior_games, prior, 2.)
    assert not rr.scenarios_differ(v2['role_scenarios'], v3['role_scenarios'])
    assert v3['scenario_evidence']['method'] == rr.ESTIMATOR and 'Shadow only' in v3['scenario_evidence']['description']
    fitted = rr.attach_v3(request, index, prior_games, prior, 2., K=1., h=math.inf, K_targets=2., h_targets=3.)
    assert rr.scenarios_differ(v2['role_scenarios'], fitted['role_scenarios'])
    assert fitted['scenario_evidence']['parameters'] == {'carries': {'K': 1., 'h': 'inf'}, 'targets': {'K': 2., 'h': 3.}}


def test_regime_conditioning_moves_the_takeover_back_toward_his_regime_usage():
    boxes, players, ranks, prior = hall_fixture()
    v2, _ = team_scenario('T', players, ranks, boxes, GAMES, prior, 2.)
    shares = {K: rr.team_scenario_v3('T', players, ranks, boxes, GAMES, prior, 2., K=K)[0]['carries']['allen'] for K in (0.5, 1., 4.)}
    # Allen took 22 and 24 carries without hall against 4 inherited under v2's 55/35/10 split.
    assert v2['carries']['allen'] < shares[4.] < shares[1.] < shares[.5]
    # Davis, the other active back, loses share the same way; qb and receivers keep their v2 share
    # relative to each other (only the RB group is re-estimated; the field renormalises).
    v3, note = rr.team_scenario_v3('T', players, ranks, boxes, GAMES, prior, 2., K=1.)
    assert note['K'] == 1. and note['h'] == 'inf'
    assert v3['carries']['wr1'] == 0 and v3['targets']['wr1'] == pytest.approx(v2['targets']['wr1'], rel=.2)
    assert sum(v3['carries'].values()) == pytest.approx(1) and sum(v3['targets'].values()) == pytest.approx(1)
    assert v3['carries']['NEW:T:0'] * 3 == pytest.approx(RESERVE['carries'])


def test_posterior_rate_is_the_registered_formula():
    """rate = (sum w c + K v2) / (sum w + K), teammate absences dropped, prior = own blended rate + depth split."""
    boxes, players, ranks, prior = hall_fixture()
    K = 2.
    rates = rr.weighted_usage_rates(boxes, GAMES, math.inf)
    from model.nfl_game_leaders_roles import blend_prior
    blended = blend_prior(rates, prior, 2.)
    split = RB_DEPTH_SPLIT[:2]
    v2_allen = blended['allen']['carries'] + blended['hall']['carries'] * split[0] / sum(split)
    v2_davis = blended['davis']['carries'] + blended['hall']['carries'] * split[1] / sum(split)
    allen = (22 + 24 + K * v2_allen) / (2 + K)
    davis = (4 + 3 + K * v2_davis) / (2 + K)
    qb = blended['qb']['carries']
    total = allen + davis + qb
    v3, _ = rr.team_scenario_v3('T', players, ranks, boxes, GAMES, prior, 2., K=K)
    assert v3['carries']['allen'] == pytest.approx(allen / total * (1 - RESERVE['carries']))
    assert v3['carries']['davis'] == pytest.approx(davis / total * (1 - RESERVE['carries']))


def test_half_life_weights_recent_games_and_the_prior_blend_uses_effective_games():
    boxes, players, ranks, prior = hall_fixture()
    recent, _ = rr.team_scenario_v3('T', players, ranks, boxes, GAMES, prior, 2., K=None, h=1.)
    flat, _ = rr.team_scenario_v3('T', players, ranks, boxes, GAMES, prior, 2., K=None, h=math.inf)
    # wr1's targets rise through the window (8, 9, 7, 10, 8 -> latest 8); hall's carries fell to zero latest.
    rates = rr.weighted_usage_rates(boxes, GAMES, 1.)
    w = rr.window_weights(5, 1.)
    expected = sum(wi * c for wi, c in zip(w, (8, 9, 7, 10, 8))) / sum(w)
    assert rates['wr1']['targets'] == pytest.approx((sum(w) * expected + 1 * expected) / (sum(w) + 1))
    assert rates['wr1']['games_played'] == pytest.approx(sum(w))
    assert recent['carries']['allen'] != pytest.approx(flat['carries']['allen'])


def test_dm_log_score_is_a_proper_distribution_and_routes_unknown_players_to_the_reserve():
    shares = {'a': .5, 'b': .3, 'NEW:T:0': .1, 'NEW:T:1': .05, 'NEW:T:2': .05}
    total = 0.
    for counts in product(range(4), repeat=3):
        if sum(counts) == 3:
            total += math.exp(rr.dm_log_score(shares, {'a': counts[0], 'b': counts[1], 'NEW:T:0': counts[2]}, 20.))
    assert total == pytest.approx(1.)
    unknown = rr.dm_log_score(shares, {'a': 2, 'zzz': 1}, 20.)
    assert unknown == pytest.approx(rr.dm_log_score(shares, {'a': 2, 'NEW:T:2': 1}, 20.))
    # A zero share is floored, never -inf, and the better-matched field scores higher.
    assert math.isfinite(rr.dm_log_score({'a': 1., 'b': 0.}, {'b': 5}, 20.))
    assert rr.dm_log_score({'a': .8, 'b': .2}, {'a': 8, 'b': 2}, 20.) > rr.dm_log_score({'a': .2, 'b': .8}, {'a': 8, 'b': 2}, 20.)


def test_score_team_reports_loss_and_named_mae():
    scenario = {'carries': {'a': .6, 'b': .4}, 'targets': {'a': .5, 'b': .5}}
    boxes = [box('a', 12, 4), box('b', 8, 6)]
    out = rr.score_team(scenario, boxes, ['a', 'b'], {'carries': 55., 'targets': 40.})
    assert out['carries'] == pytest.approx(-rr.dm_log_score(scenario['carries'], {'a': 12, 'b': 8}, 55.))
    assert out['mae_carries'] == pytest.approx(0.) and out['mae_targets'] == pytest.approx(1.)


def _record(season, week, gid, affected, cells):
    team = {'affected': affected, 'affected_by': [], 'regime': {}, 'out_with_usage': [], 'named': 3,
            'totals': {'carries': 20, 'targets': 30}, 'cells': cells}
    return {'study': rr.STUDY, 'game_id': gid, 'season': season, 'week': week, 'teams': {'T': team}}


def _cells(best):
    """A loss per cell; `best` names the cell that beats v2 on the affected subset."""
    cells = {'v2': {'carries': 10., 'targets': 12., 'mae_carries': 2., 'mae_targets': 2.}}
    for K, h in rr.grid_cells():
        key = rr.cell_key(K, h)
        delta = -1. if key == best else .5
        cells[key] = {'carries': 10. + delta, 'targets': 12. + delta, 'mae_carries': 2. + delta / 10, 'mae_targets': 2.}
    return cells


def test_fit_chooses_the_lowest_affected_loss_and_counts_the_subsets(tmp_path):
    best = rr.cell_key(2., 3.)
    records = []
    for j in range(30):
        season, week = 2023 + j % 3, 4 + j % 10
        records.append(_record(season, week, f'g{j}', affected=j % 2 == 0, cells=_cells(best)))
    records.append({'study': rr.STUDY, 'game_id': 'skip', 'season': 2024, 'week': 2, 'skipped': 'reason'})
    path = tmp_path / 'dev.jsonl'
    path.write_text('\n'.join(json.dumps(r) for r in records))
    fitted = rr.fit(path)
    assert fitted['chosen'] == {'carries': {'K': 2., 'h': 3., 'cell': best}, 'targets': {'K': 2., 'h': 3., 'cell': best}}
    assert fitted['subset']['affected_team_games'] == 15 and fitted['subset']['unaffected_team_games'] == 15
    assert fitted['subset']['skipped'] == 1 and fitted['subset']['games'] == 30
    assert fitted['affected_grid']['carries'][best]['mean_dm_log_loss_difference'] == pytest.approx(-1.)
    assert fitted['development_in_sample_ci']['carries']['ci95'] == pytest.approx([-1., -1.])
    assert fitted['grid'] == {'K': [.5, 1., 2., 4.], 'h': ['inf', 4, 3, 2, 1]}


def test_cluster_bootstrap_resamples_weeks_not_rows():
    values = [1.] * 10 + [-1.] * 10
    clusters = [(2026, 7)] * 10 + [(2026, 8)] * 10
    out = rr.cluster_bootstrap(values, clusters)
    assert out['mean'] == 0 and out['clusters'] == 2 and out['n'] == 20
    assert out['ci95'][0] == -1. and out['ci95'][1] == 1.
    assert rr.cluster_bootstrap([], [])['mean'] is None


def test_guard_summary_holds_only_when_every_market_upper_bound_is_within_bound():
    good = [{'season': 2024, 'week': w, 'identical': False, 'difference': {m: -.01 for m in rr.METRICS}} for w in range(1, 9)]
    assert rr.guard_summary(good)['held']
    bad = [{**r, 'difference': {**r['difference'], 'receptions': .05}} for r in good]
    summary = rr.guard_summary(bad)
    assert not summary['held'] and not summary['markets']['receptions']['held'] and summary['markets']['rushing_yards']['held']
    assert rr.guard_summary(good + [{'season': 2024, 'week': 9, 'skipped': 'x'}])['skipped'] == 1


def _shadow_entry(gid, week, affected, v3_better, pregame=True):
    kickoff = f'2026-11-{week:02d}T18:00:00+00:00'
    v2 = {'carries': {'a': .6, 'b': .3, 'NEW:T:0': .1}, 'targets': {'a': .5, 'b': .4, 'NEW:T:0': .1}}
    v3 = {'carries': {'a': .8 if v3_better else .3, 'b': .1 if v3_better else .6, 'NEW:T:0': .1},
          'targets': {'a': .7 if v3_better else .2, 'b': .2 if v3_better else .7, 'NEW:T:0': .1}}
    return {'game': {'game_id': gid, 'season': 2026, 'week': week, 'away': 'T', 'home': 'O', 'kickoff': kickoff},
            'decision_at': '2026-11-01T10:00:00+00:00', 'shadow_at': '2026-11-01T11:00:00+00:00', 'pregame': pregame,
            'teams': {'T': {'affected': affected, 'named': ['a', 'b'], 'v2': v2, 'v3': v3}},
            'forecasts': {'v2': {m: [['a', False, .5], ['b', False, .4], ['OTHER:T', True, .1]] for m in rr.METRICS},
                          'v3': {m: [['a', False, .7], ['b', False, .2], ['OTHER:T', True, .1]] for m in rr.METRICS}}}


def _capture(gids):
    games = [{'game_id': g, 'season': 2026, 'week': int(g.split('_')[1]), 'away': 'T', 'home': 'O', 'completed': True} for g in gids]
    boxes = [b for g in gids for b in ({**box('a', 16, 7), 'game_id': g, 'team': 'T', 'rushing_yards': 80., 'receiving_yards': 50., 'receptions': 6},
                                       {**box('b', 4, 3), 'game_id': g, 'team': 'T', 'rushing_yards': 20., 'receiving_yards': 10., 'receptions': 2},
                                       {**box('o', 10, 5), 'game_id': g, 'team': 'O', 'rushing_yards': 30., 'receiving_yards': 20., 'receptions': 4})]
    return {'games': games, 'boxes': boxes, 'game_coverage': {g: {'regulation_end_observed': True} for g in gids}}


def test_grade_is_blinded_before_the_floors_and_writes_the_verdict_once(tmp_path, monkeypatch):
    monkeypatch.setattr(rr, 'SHADOW_DIR', tmp_path / 'shadow')
    monkeypatch.setattr(rr, 'GRADE_DIR', tmp_path / 'runs')
    monkeypatch.setattr(rr, 'VERDICT_PATH', tmp_path / 'verdict.json')
    rr.SHADOW_DIR.mkdir()
    # Nine weeks of ten affected team-games, every one favouring v3 (upper bound below zero), one week short of nothing.
    gids = []
    for week in range(7, 16):
        games = {}
        for j in range(10):
            gid = f'2026_{week:02d}_T{j}_O'
            games[gid] = _shadow_entry(gid, week, affected=True, v3_better=True)
            gids.append(gid)
        (rr.SHADOW_DIR / f'2026-w{week:02d}.json').write_text(json.dumps({'games': games}))
    capture = _capture(gids)
    captures_path = tmp_path / 'cap' / 'capture.json.gz'
    captures_path.parent.mkdir()
    from research.nfl_longest_touchdown import write
    write(captures_path, capture)
    pattern = str(tmp_path / 'cap' / '*.json.gz')
    # Only three weeks complete: floors unmet, no effect reported.
    partial = {**capture, 'games': [g for g in capture['games'] if g['week'] <= 9],
               'boxes': [b for b in capture['boxes'] if int(b['game_id'].split('_')[1]) <= 9]}
    captures_path.unlink()
    write(captures_path, partial)
    first = rr.grade(2026, pattern, now=datetime(2026, 11, 10, tzinfo=timezone.utc))
    assert first['status'].startswith('blinded') and not rr.VERDICT_PATH.exists()
    assert first['floors']['affected_team_games'] == 30 and first['floors']['game_weeks'] == 3
    assert 'verdict' not in first and len(first['pending_games']) == 60
    # All nine weeks complete: one look, verdict written, second grade refuses to rewrite it.
    captures_path.unlink()
    write(captures_path, capture)
    second = rr.grade(2026, pattern, now=datetime(2026, 11, 20, tzinfo=timezone.utc))
    assert second['status'] == 'verdict written' and second['floors']['met']
    v = second['verdict']
    assert v['primary_dm_log_loss_v3_minus_v2']['carries']['ci95'][1] < 0 and v['passes']
    rr.VERDICT_PATH.write_text(json.dumps({**v, 'marker': 'frozen'}))
    third = rr.grade(2026, pattern, now=datetime(2026, 11, 21, tzinfo=timezone.utc))
    assert third['status'].startswith('verdict already written') and third['verdict']['marker'] == 'frozen'
    assert len(list(rr.GRADE_DIR.glob('*.json'))) == 3


def test_grade_rows_drop_post_kickoff_shadows_and_use_the_dm_loss_sign():
    entry = _shadow_entry('2026_07_T_O', 7, affected=True, v3_better=True)
    late = _shadow_entry('2026_07_T2_O', 7, affected=True, v3_better=True, pregame=False)
    rows, pending, excluded = rr.grade_rows([{'games': {'2026_07_T_O': entry, '2026_07_T2_O': late}}],
                                            [_capture(['2026_07_T_O'])], {'carries': 55., 'targets': 40.})
    assert len(rows) == 1 and rows[0]['dm']['carries'] < 0 and rows[0]['leader']['rushing_yards'] < 0
    assert pending == [] and excluded[0]['game_id'] == '2026_07_T2_O'
    assert rr.floors_met(rows) == {'affected_team_games': 1, 'game_weeks': 1, 'met': False}


def test_shadow_skips_weeks_outside_the_confirmation_window(tmp_path):
    page = tmp_path / 'page.json'
    page.write_text(json.dumps({'season': 2026, 'week': 6, 'games': []}))
    fit = tmp_path / 'fit.json'
    fit.write_text(json.dumps({'chosen': {'carries': {'K': 1., 'h': 'inf'}, 'targets': {'K': 1., 'h': 'inf'}}}))
    assert 'outside the confirmation window' in rr.shadow(page, fit)
