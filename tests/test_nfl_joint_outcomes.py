from copy import deepcopy
from datetime import timedelta

import numpy as np
import pytest

from model.nfl_game_leaders import Settings, prepare
from model.nfl_gain_distribution import sample_draws
from model.nfl_joint_contracts import digest, metric_matrix, validate_bank
from model.nfl_joint_decisions import exact_set_probability, exact_sets, portfolio_returns, summarize
from model.nfl_joint_evaluation import crps, grade_bank, paired_bootstrap, validate_registration
from model.nfl_joint_outcomes import enrich_history, fit, generate, implementation_digest
from model.nfl_longest_touchdown import timestamp
from model.nfl_market_reweighting import ReweightSettings, implied_probability, paired_probability, quote_constraints, reweight
from model.nfl_midgame_exits import fit_exits, participation, redistributed_shares
from model.nfl_opportunity_process import allocate_segments, conditioned_volumes, fit_market_volume
from research.nfl_alt_capture import execute, normalize, plan
from research.nfl_joint_store import verify_bundle, write_bundle
from model.nfl_ownership_model import fit_ownership, predict_ownership
from model.nfl_joint_full_dfs import complete_report, reweight_complete
from tests.test_nfl_game_leaders import fixture, request


def bank():
    history, _ = prepare(fixture(), request()['decision_at'])
    return generate(history, request(), Settings(draws=100, seed=7))['shared_draws']


def simple_bank():
    values = [[10, 8, 5, 1], [10, 8, 5, 5], [1, 2, 3, 10], [0, 0, 0, 0]]
    result = bank()
    result['scenario_ids'] = ['s0', 's1', 's2', 's3']
    result['players'] = [deepcopy(p) for p in result['players'] if not p['residual']]
    result['weights'] = [.25] * 4
    for j, p in enumerate(result['players']):
        p['draws'] = {field: [row[j] for row in values] for field in ('rushYds', 'recYds', 'receptions', 'targets', 'carries')}
    return result


def constraint(probability=.7, line=4, tolerance=.01):
    return {'id': 'c1', 'identity': 'A0', 'metric': 'receiving_yards', 'line': line, 'comparator': 'gt',
            'probability': probability, 'tolerance': tolerance, 'basis': 'research_probability',
            'evidence': {'source_ref': 'test', 'captured_at': '2025-10-01T00:00:00Z'}}


def exit_rows():
    return [{'game_id': 'past', 'identity': 'p1', 'role': 'WR', 'game_ended_at': '2025-09-01T22:00:00Z',
             'source_ref': 'verified', 'confidence': 'adjudicated', 'at_risk_segments': 1, 'reason': 'injury',
             'exit_segment': 0, 'return_segment': 2, 'replacement_counts': {'targets': {'RB': 5}}},
            {'game_id': 'past', 'identity': 'p2', 'role': 'RB', 'game_ended_at': '2025-09-01T22:00:00Z',
             'source_ref': 'verified', 'confidence': 'adjudicated', 'at_risk_segments': 8, 'reason': 'none'}]


def registration():
    return {'study_id': 'test', 'registered_at': '2025-01-01T00:00:00Z', 'variants': ['baseline', 'role-gains'],
            'primary_metric': 'exact_set_log_loss', 'metrics': ['receptions'], 'populations': ['full'],
            'development_games': ['g1'], 'selection_games': ['g2'], 'locked_games': ['g3'], 'exposed_games': [],
            'gates': {}, 'draws': 100, 'seeds': [1], 'multiplicity': 'hierarchical',
            'tie_rule': 'uniform_random_boundary_tiebreak', 'source_policy': 'retrospective', 'status': 'development'}


def test_joint_baseline_conserves_and_weighted_summary_uses_joint_total():
    b = simple_bank()
    validate_bank(b)
    assert np.array_equal(metric_matrix(b, 'total_yards'), 2 * metric_matrix(b, 'receiving_yards'))
    output = summarize(b)
    assert sum(p['leader_share'] for p in output['receptions']['players']) == pytest.approx(1)
    b['weights'] = [1, 0, 0, 0]
    assert summarize(b)['receiving_yards']['players'][0]['mean'] == 10
    b['players'][0]['draws']['targets'][0] = 0
    with pytest.raises(ValueError, match='exceed'):
        validate_bank(b)


def test_exact_sets_boundary_ties_normalize_without_display_renormalization():
    b = simple_bank()
    result = exact_sets(b, 'receiving_yards', display_limit=1)
    assert result['enumerated_mass'] == pytest.approx(1)
    assert result['displayed_mass'] < 1
    assert exact_set_probability(b, 'receiving_yards', ['A0', 'A1', 'B0']) == pytest.approx(.25 + .125 + .0625)
    assert sum(row['probability'] for row in result['full_distribution']) == pytest.approx(1)
    result = exact_sets(b, 'receiving_yards', maximum_tie_sets=1)
    assert result['enumerated_mass'] + result['enumeration_overflow_mass'] == pytest.approx(1)


def test_legacy_bank_without_weights_and_separate_latents_work():
    b = bank()
    b.pop('weights')
    assert sum(validate_bank(b)) == pytest.approx(1)
    assert exact_sets(b, 'receptions')['unresolved_identity_mass'] >= 0
    b['players'][0]['identity'] = 'OTHER:A'
    with pytest.raises(ValueError, match='individual'):
        validate_bank(b)


def test_reweight_hard_and_soft_reproduce_supported_probability():
    b = simple_bank()
    before = deepcopy(b)
    for method in ('hard', 'soft'):
        result = reweight(b, [constraint()], ReweightSettings(method=method))
        assert result['market_reweighting']['accepted']
        assert sum(result['weights']) == pytest.approx(1)
        achieved = sum(w for w, v in zip(result['weights'], b['players'][0]['draws']['recYds']) if v > 4)
        assert achieved == pytest.approx(.7, abs=.01)
    assert b == before


def test_reweight_support_and_ess_guards_fall_back_without_fake_tail():
    b = simple_bank()
    result = reweight(b, [constraint(.9, 100)], ReweightSettings(method='hard'))
    assert result['market_reweighting']['reason'] == 'infeasible_constraints'
    assert result['weights'] == b['weights']
    result = reweight(b, [constraint(.999, 0)], ReweightSettings(method='hard', minimum_ess_fraction=.95, warning_ess_fraction=.95))
    assert not result['market_reweighting']['accepted']
    assert result['weights'] == b['weights']


def test_reweight_handles_conditional_integer_push_mass_and_late_quotes():
    b = simple_bank()
    c = constraint(.6, 1, .02)
    c['conditional_on_no_push'] = True
    result = reweight(b, [c])
    assert result['market_reweighting']['accepted']
    assert result['market_reweighting']['constraints'][0]['achieved'] == pytest.approx(.6, abs=.02)
    c['evidence']['captured_at'] = '2025-10-02T01:00:00Z'
    with pytest.raises(ValueError, match='boundary'):
        reweight(b, [c])


def test_price_methods_require_appropriate_prices():
    assert implied_probability(-115) == pytest.approx(115 / 215)
    assert paired_probability(-110, -110) == pytest.approx(.5)
    assert paired_probability(-110, -110, 'normalize') == pytest.approx(.5)
    with pytest.raises(ValueError):
        implied_probability(0)
    with pytest.raises(ValueError):
        implied_probability(1, 'decimal')


def test_exit_fit_exposure_timing_and_remaining_share_redistribution():
    fitted = fit_exits(exit_rows(), '2025-10-01T00:00:00Z')
    players = [{'identity': 'a', 'position': 'WR'}, {'identity': 'b', 'position': 'RB'},
               {'identity': 'u', 'position': 'WR', 'residual': True}]
    result = redistributed_shares([.6, .3, .1], [False, True, True], players, fitted, 'targets')
    assert result == pytest.approx([0, .9, .1])
    fitted['roles']['WR']['hazard'] = 1.
    presence, exits = participation(np.random.default_rng(2), players, 3, fitted, '2025-10-02T00:00:00Z')
    assert not presence[:, 0, 0].any() and presence[:, 2, 0].all()
    assert exits['a'] == 3
    bad = deepcopy(exit_rows())
    bad[0]['exit_segment'] = 8
    with pytest.raises(ValueError, match='timing'):
        fit_exits(bad, '2025-10-01T00:00:00Z')


def test_segment_allocation_conserves_and_preserves_pre_exit_touches():
    fitted = fit_exits(exit_rows(), '2025-10-01T00:00:00Z')
    players = [{'identity': 'a', 'position': 'WR'}, {'identity': 'b', 'position': 'RB'}]
    presence = np.ones((20, 8, 2), bool)
    presence[:, 4:, 0] = False
    allocations = allocate_segments(np.random.default_rng(1), players, np.full(20, 80), [.8, .2], 1000, presence, fitted)
    assert np.array_equal(allocations.sum(axis=(1, 2)), np.full(20, 80))
    assert not allocations[:, 4:, 0].any()
    assert allocations[:, :4, 0].sum() > 0


def test_role_gain_candidate_uses_measured_depth_and_preserves_baseline():
    source = fixture()
    h, rejected = prepare(source, request()['decision_at'])
    assert not rejected
    enriched = enrich_history(h, source)
    assert any(e.get('air_yards') is not None for e in enriched[0]['events'])
    fitted = fit(enriched, request()['decision_at'])
    original = deepcopy(fitted)
    result = generate(enriched, request(), Settings(draws=100, seed=11), fitted)
    assert fitted == original
    assert all(d['max_budget_error'] == 0 for t in result['shared_draws']['candidate_diagnostics'].values() for d in t['actions'].values())
    validate_bank(result['shared_draws'])
    fitted['gains']['prior_events'] = 999
    with pytest.raises(ValueError, match='digest'):
        generate(enriched, request(), Settings(draws=20), fitted)


def test_missing_or_ambiguous_depth_does_not_invent_features():
    source = fixture()
    h, _ = prepare(source, request()['decision_at'])
    source['plays'].append({**source['plays'][0], 'play_id': 999})
    enriched = enrich_history(h, source)
    first = enriched[0]['events'][0]
    assert first.get('depth_join_unresolved') and first.get('air_yards') is None


def test_crps_matches_brute_force_and_grading_scores_exact_decision():
    values = np.array([0., 2., 5.])
    weights = np.array([.2, .3, .5])
    expected = weights @ abs(values - 1) - .5 * np.sum(np.outer(weights, weights) * abs(values[:, None] - values))
    assert crps(values, 1, weights) == pytest.approx(expected)
    b = simple_bank()
    boxes = [{'identity': p['identity'], 'rushing_yards': 10 - j, 'receiving_yards': 10 - j, 'receptions': 10 - j} for j, p in enumerate(b['players'])]
    grade = grade_bank(b, boxes)
    assert grade['metrics']['receiving_yards']['exact_set_log_loss'] == pytest.approx(-np.log(.4375))
    assert grade['metrics']['receiving_yards']['player_scores']
    assert paired_bootstrap({'a': 1, 'b': 2}, {'a': 2, 'b': 3})['ci95'] == pytest.approx([-1, -1])


def test_study_rejects_reused_holdout_and_population_overlap():
    study = registration()
    assert validate_registration(study)
    study['exposed_games'] = ['g3']
    with pytest.raises(ValueError, match='examined'):
        validate_registration(study)
    study['exposed_games'] = []
    study['development_games'].append('g2')
    with pytest.raises(ValueError, match='Overlapping'):
        validate_registration(study)


def test_portfolio_uses_same_scenarios_and_dead_heat_credit():
    b = simple_bank()
    output = portfolio_returns(b, [{'identity': 'A0', 'metric': 'receiving_yards', 'stake': 5, 'decimal_odds': 2},
                                   {'identity': 'B1', 'metric': 'receiving_yards', 'stake': 5, 'decimal_odds': 4}])
    assert output['stake'] == 10
    assert min(output['net_draws']) >= -10
    assert output['expected_net'] == pytest.approx(np.mean(output['net_draws']))


def prop_payload():
    return {'id': 'event', 'commence_time': '2025-10-03T17:00:00Z', 'home_team': 'B', 'away_team': 'A',
            'bookmakers': [{'key': 'book', 'markets': [{'key': 'alt', 'last_update': '2025-10-01T00:00:00Z',
              'outcomes': [{'name': side, 'description': 'Player', 'point': line, 'price': -110}
                           for line in (15.5, 25.5, 31.5) for side in ('Over', 'Under')]}]}]}


def test_alt_capture_preserves_each_rung_and_paired_side():
    result = normalize(prop_payload(), '2025-10-02T00:00:00Z', identity_map={'Player': 'gsis'})
    assert len(result['quotes']) == 6 and result['paired_lines'] == 3
    assert len({q['line'] for q in result['quotes']}) == 3
    assert all(q['eligible_pregame'] and q['identity'] == 'gsis' for q in result['quotes'])
    planned = plan([{'id': 'event', 'commence_time': '2025-10-03T17:00:00Z'}], ['alt'], ['book'], '2025-10-02T00:00:00Z')
    assert planned['estimated_credits'] == 1 and not planned['paid_capture_permitted']
    with pytest.raises(ValueError, match='budget'):
        execute(planned, 'key', 'unused')


def test_paid_capture_records_quota_raw_first_and_never_reuses_output(tmp_path):
    class Response:
        status_code = 200
        headers = {'x-requests-last': '1', 'x-requests-used': '20', 'x-requests-remaining': '9000'}
        def json(self):
            return prop_payload()
    class Session:
        def get(self, *args, **kwargs):
            return Response()
    planned = plan([{'id': 'event', 'commence_time': '2025-10-03T17:00:00Z'}], ['alt'], ['book'], '2025-10-02T00:00:00Z', 1)
    output = tmp_path / 'capture'
    result = execute(planned, 'key', output, session=Session(), now=lambda: '2025-10-02T00:00:00Z')
    assert result['spent_credits'] == 1 and list(output.glob('*/raw.json'))
    with pytest.raises(FileExistsError):
        execute(planned, 'key', output, session=Session())


def test_matched_ladder_constraints_preserve_book_line_time_and_event():
    canonical = {'provider_event_id': 'event', 'game_id': '2025_05_A_B', 'kickoff': '2025-10-03T17:00:00Z',
                 'provider_home': 'B', 'provider_away': 'A'}
    quotes = normalize(prop_payload(), '2025-10-01T12:00:00Z', canonical, {'Player': 'A0'})['quotes']
    output = quote_constraints(quotes, {'alt': 'receiving_yards'}, '2025-10-02T00:00:00Z')
    assert len(output['constraints']) == 3 and not output['unpaired']
    assert all(c['probability'] == pytest.approx(.5) and not c['conditional_on_no_push'] for c in output['constraints'])
    with pytest.raises(ValueError, match='event'):
        c = constraint()
        c['game_id'] = 'other'
        reweight(simple_bank(), [c])


def test_capture_network_error_preserves_run_and_does_not_expose_credentials(tmp_path):
    import requests
    class Session:
        def get(self, *args, **kwargs):
            raise requests.Timeout('request contained secret-key')
    planned = plan([{'id': 'event', 'commence_time': '2025-10-03T17:00:00Z'}], ['alt'], ['book'], '2025-10-02T00:00:00Z', 1)
    report = execute(planned, 'secret-key', tmp_path / 'failure', session=Session(), now=lambda: '2025-10-02T00:00:00Z')
    assert report['unknown_charge_requests'] == 1
    assert 'secret-key' not in str(report)
    assert (tmp_path / 'failure' / 'report.json').exists()


def test_capture_normalization_failure_preserves_raw_payload_and_quota(tmp_path):
    class Response:
        status_code = 200
        headers = {'x-requests-last': '1', 'x-requests-remaining': '2000'}
        def json(self):
            return {'id': 'event', 'commence_time': '2025-10-03T17:00:00Z',
                    'bookmakers': [{'key': 'book', 'markets': [{'key': 'alt', 'outcomes': None}]}]}
    class Session:
        def get(self, *args, **kwargs):
            return Response()
    planned = plan([{'id': 'event', 'commence_time': '2025-10-03T17:00:00Z'}], ['alt'], ['book'], '2025-10-02T00:00:00Z', 1)
    output = tmp_path / 'invalid'
    report = execute(planned, 'key', output, session=Session(), now=lambda: '2025-10-02T00:00:00Z')
    assert report['spent_credits'] == 1
    assert report['captures'][0]['status'] == 'normalization_error'
    assert list(output.glob('*/raw.json')) and (output / 'report.json').exists()


def test_market_volume_fit_is_prior_only_and_conditions_both_teams():
    rows = [{'game_id': str(j), 'decision_at': '2025-09-01T00:00:00Z', 'kickoff': '2025-09-02T00:00:00Z',
             'ended_at': '2025-09-02T05:00:00Z', 'total': 35 + j, 'home_spread': -5 + j / 2,
             'away_targets': 20 + j, 'home_targets': 25 + j, 'away_carries': 30 - j, 'home_carries': 35 - j,
             'market_evidence': {'source_ref': 'historical', 'captured_at': '2025-08-31T22:00:00Z'}} for j in range(20)]
    fitted = fit_market_volume(rows, '2025-09-10T00:00:00Z')
    volumes = conditioned_volumes(simple_bank(), fitted, {'total': 55, 'home_spread': -3,
                                 'evidence': {'source_ref': 'market', 'captured_at': '2025-10-01T00:00:00Z'}})
    assert set(volumes) == {('A', 'targets'), ('A', 'carries'), ('B', 'targets'), ('B', 'carries')}
    rows[0]['market_evidence']['captured_at'] = '2025-09-02T00:00:00Z'
    with pytest.raises(ValueError, match='boundary'):
        fit_market_volume(rows, '2025-09-10T00:00:00Z')


def test_ownership_fit_keeps_future_observations_out_and_reports_marginal_scope():
    evidence = {'source_ref': 'projection', 'captured_at': '2025-09-01T00:00:00Z'}
    rows = [{'contest_id': 'one', 'identity': str(j), 'slot': 'FLEX', 'salary': 4000 + j * 100,
             'projected_points': j, 'projected_value': j / 5, 'ownership': .05 + j / 30,
             'projection_evidence': evidence, 'decision_at': '2025-09-02T00:00:00Z',
             'contest_ended_at': '2025-09-03T00:00:00Z', 'ownership_source_ref': 'field'} for j in range(20)]
    fitted = fit_ownership(rows, '2025-09-04T00:00:00Z')
    prediction = predict_ownership(fitted, rows[:2], '2025-09-05T00:00:00Z')
    assert prediction['authority'].endswith('not_field_generator')
    assert all(0 < p['ownership'] < 1 for p in prediction['players'])
    rows[0]['contest_ended_at'] = '2026-01-01T00:00:00Z'
    with pytest.raises(ValueError, match='boundary'):
        fit_ownership(rows, '2025-09-04T00:00:00Z')


def test_bundle_detects_tampering_and_rejects_path_escape(tmp_path):
    manifest = write_bundle(tmp_path, 'study', 'run', {'forecast.json': {'x': 1}, 'draws.json.gz': {'weights': [.5, .5]}})
    assert verify_bundle(tmp_path / 'study' / 'run') == manifest
    (tmp_path / 'study' / 'run' / 'forecast.json').write_text('{"x":2}', encoding='utf-8')
    with pytest.raises(ValueError, match='digest'):
        verify_bundle(tmp_path / 'study' / 'run')
    with pytest.raises(ValueError, match='identity'):
        write_bundle(tmp_path, '../escape', 'run', {'forecast.json': {}})
    with pytest.raises(ValueError, match='filenames'):
        write_bundle(tmp_path, 'study', 'invalid', {'../escaped.json': {}})
    assert not (tmp_path / 'study' / 'invalid').exists()


def test_complete_dfs_bridge_derives_leaders_from_identical_event_scenarios():
    from tests.test_nfl_matchup_scenarios import inputs
    kwargs = inputs()
    game_id = str(kwargs['forecasts'][0]['game_id'])
    teams = [f['team'] for f in kwargs['forecasts']]
    games = {game_id: {'game_id': game_id, 'home': teams[0], 'away': teams[1], 'kickoff': '2026-10-01T00:00:00Z'}}
    with pytest.raises(ValueError, match='evidence'):
        complete_report(kwargs, games)
    kwargs['source_manifest']['captured_at'] = '2026-09-27T13:00:00Z'
    report = complete_report(kwargs, games)
    production = report['games'][game_id]['evaluation']['shared_draws']
    complete = report['complete_dfs']['evaluation']
    assert production['scenario_ids'] == [s['id'] for s in complete['scenarios']]
    assert report['games'][game_id]['evaluation']['exact_top_three']['receiving_yards']['field_scope'].endswith('incomplete_game_field')
    for p in production['players']:
        dk_id = kwargs['identities'][p['identity']]
        assert p['draws']['recYds'] == [s['stats'][str(dk_id)]['recYds'] for s in complete['scenarios']]


def test_complete_exit_branch_preserves_ledger_and_never_assigns_post_exit_work():
    from tests.test_nfl_matchup_scenarios import inputs
    kwargs = inputs()
    kwargs['source_manifest']['captured_at'] = '2026-09-27T13:00:00Z'
    gid = str(kwargs['forecasts'][0]['game_id'])
    teams = [f['team'] for f in kwargs['forecasts']]
    games = {gid: {'game_id': gid, 'home': teams[0], 'away': teams[1], 'kickoff': '2026-10-01T00:00:00Z'}}
    fitted = {'training_cutoff': '2026-09-26T00:00:00Z', 'fit_sha256': 'synthetic', 'role_excluded_game_ids': ['injury-game'],
              'dispersion': {a: {'method': 'synthetic', 'concentration': 1000} for a in ('targets', 'carries')},
              'roles': {}, 'exit_fit': {'training_cutoff': '2026-09-26T00:00:00Z', 'segments': 8, 'pooled_hazard': 0,
                'roles': {'QB': {'hazard': 0, 'durations': [8], 'replacement': {}},
                          'WR': {'hazard': 1, 'durations': [8], 'replacement': {}}}}}
    fitted['training_game_ids'] = ['injury-game']
    fitted['implementation_sha256'] = implementation_digest()
    fitted['fit_sha256'] = digest({k: v for k, v in fitted.items() if k != 'fit_sha256'})
    report = complete_report(kwargs, games, fitted)
    for p in report['games'][gid]['evaluation']['shared_draws']['players']:
        if p['identity'].endswith('WR'):
            assert not any(p['draws']['targets']) and not any(p['draws']['receptions'])
    for ledger in report['complete_dfs']['diagnostics'][1]['event_ledgers']:
        for team in ledger[0]['teams']:
            assert team['passing_yards'] == sum(p.get('receiving_yards', 0) for p in team['players'].values()) + team['unallocated']['receiving_yards']


def test_empirical_full_dfs_gains_keep_signed_credit_and_share_qb_receiving_totals():
    from tests.test_nfl_matchup_scenarios import inputs
    kwargs = inputs()
    kwargs['source_manifest']['captured_at'] = '2026-09-27T13:00:00Z'
    h, _ = prepare(fixture(), request()['decision_at'])
    fitted = fit(enrich_history(h, fixture()), request()['decision_at'])
    gid = str(kwargs['forecasts'][0]['game_id'])
    teams = [f['team'] for f in kwargs['forecasts']]
    games = {gid: {'game_id': gid, 'home': teams[0], 'away': teams[1], 'kickoff': '2026-10-01T00:00:00Z'}}
    future = deepcopy(kwargs)
    future['decision_at'] = '2025-10-02T00:00:00Z'
    future['source_manifest']['captured_at'] = '2025-10-01T00:00:00Z'
    future['history'][0]['season'] = 2026
    with pytest.raises(ValueError, match='future-season'):
        complete_report(future, games, fitted)
    report = complete_report(kwargs, games, fitted)
    assert report['complete_dfs']['version'].endswith('empirical-gains')
    assert any(v < 0 for p in report['games'][gid]['evaluation']['shared_draws']['players'] for v in p['draws']['rushYds'])
    for ledger in report['complete_dfs']['diagnostics'][1]['event_ledgers']:
        for team in ledger[0]['teams']:
            assert team['passing_yards'] == sum(p.get('receiving_yards', 0) for p in team['players'].values()) + team['unallocated']['receiving_yards']
    tampered = deepcopy(fitted)
    tampered['gains']['prior_events'] += 1
    with pytest.raises(ValueError, match='digest'):
        complete_report(kwargs, games, tampered)
    target_leak = deepcopy(fitted)
    target_leak['training_game_ids'].append(gid)
    target_leak['fit_sha256'] = digest({k: v for k, v in target_leak.items() if k != 'fit_sha256'})
    with pytest.raises(ValueError, match='leaked'):
        complete_report(kwargs, games, target_leak)
