from copy import deepcopy

import numpy as np
import pytest

from model.nfl_game_leaders import Settings, allocate, forecast, grade, opponent_effect, prepare
from research.nfl_game_leaders import batch
from research.nfl_game_leaders_publish import publish


def fixture():
    games, boxes, plays, coverage = [], [], [], {}
    for week in range(1,5):
        gid=f'2025_{week:02}_A_B'
        g={'game_id':gid,'season':2025,'week':week,'away':'A','home':'B',
            'kickoff':f'2025-09-{week*7:02}T17:00:00Z','completed':True,
            'home_score':7,'away_score':7,'source_captured_at':'2025-10-01T00:00:00Z'}
        games.append(g)
        coverage[gid]={'regulation_end_observed':True}
        pid=0
        for team,opponent in [('A','B'),('B','A')]:
            for player in range(2):
                identity=f'{team}{player}'
                boxes.append({'game_id':gid,'identity':identity,'name':identity,'team':team,
                    'position':'RB' if player==0 else 'WR','fetched_at':'2025-10-01T00:00:00Z',
                    'carries':2 if player==0 else 0,'targets':2,'receptions':1,
                    'rushing_yards':-2 if player==0 else 0,'receiving_yards':20 if player==0 else 50})
                for action,count in [('run',2 if player==0 else 0),('pass',2)]:
                    for k in range(count):
                        pid+=1
                        caught=action=='pass' and k==0
                        yards=-1 if action=='run' else (20 if player==0 else 50) if caught else 0
                        plays.append({'game_id':gid,'play_id':pid,'season':2025,'week':week,
                            'posteam':team,'defteam':opponent,'home_team':'B','away_team':'A',
                            'play_type':action,'quarter':5 if week==4 else 4,
                            'labelled_at':'2025-10-01T00:00:00Z','description':'run' if action=='run' else 'pass',
                            'yards_gained':yards,'air_yards':yards if caught else 10,
                            'yards_after_catch':0 if caught else None,'had_sack':False,
                            'actors':[{'role':'rusher' if action=='run' else 'receiver','player_id':identity,'position':'RB' if player==0 else 'WR'}]})
    return {'games':games,'boxes':boxes,'plays':plays,'game_coverage':coverage}


def request():
    return {'decision_at':'2025-10-02T00:00:00Z','game':{'game_id':'2025_05_A_B',
        'season':2025,'week':5,'away':'A','home':'B','kickoff':'2025-10-03T17:00:00Z'},
        'players':[{'identity':f'{t}{j}','name':f'{t}{j}','team':t,'position':'RB' if j==0 else 'WR','status':'unresolved'} for t in ('A','B') for j in range(2)]}


def test_reconciled_all_periods_and_negative_yards():
    history,rejected=prepare(fixture(),'2025-10-02T00:00:00Z')
    assert len(history)==4 and not rejected
    assert any(e['yards']<0 for e in history[-1]['events'])
    assert len(history[-1]['events'])==12  # OT retained


def test_count_only_reconciliation_preserves_games_and_blocks_yardage_use():
    s = fixture()
    s['boxes'][0]['receiving_yards'] += 19
    full, rejected = prepare(s, '2025-10-02T00:00:00Z')
    assert len(full) == 3 and rejected[0]['reasons'] == ['pbp_box_mismatch']
    counts, rejected = prepare(s, '2025-10-02T00:00:00Z', reconciliation_fields=('targets', 'receptions'))
    assert len(counts) == 4 and not rejected
    with pytest.raises(ValueError, match='requested outcome'):
        forecast(counts, request(), Settings(draws=100))
    result = forecast(counts, request(), Settings(draws=500), outcomes=('receptions',))
    assert set(result['metrics']) == {'receptions'}
    for r in result['metrics']['receptions']['players']:
        if r['residual']:
            continue
        pmf = r['count_probabilities']
        assert sum(pmf.values()) == pytest.approx(1)
        assert sum(int(k) * p for k, p in pmf.items()) == pytest.approx(r['mean'])
        tails = [sum(p for k, p in pmf.items() if int(k) >= n) for n in range(20)]
        assert all(a >= b for a, b in zip(tails, tails[1:]))
    s['boxes'][0]['targets'] += 1
    counts, rejected = prepare(s, '2025-10-02T00:00:00Z', reconciliation_fields=('targets', 'receptions'))
    assert len(counts) == 3 and rejected[0]['reasons'] == ['pbp_box_mismatch']


def test_reception_market_comparison_uses_tie_credit_not_full_tie_hit():
    from research.nfl_receptions_analysis import analyze
    p = {'game': {}, 'decision_at': '2026-10-08T22:00:00Z', 'settings': {'draws': 100},
         'source_sha256': 'source', 'implementation_sha256': 'implementation',
         'reconciliation_fields': ['targets', 'receptions'], 'availability_verified': False,
         'metrics': {'receptions': {'tie_probability': .2, 'players': [{
             'name': 'CeeDee Lamb', 'residual': False, 'win_share': .5, 'first_or_tied': .6,
             'mean': 6., 'count_probabilities': {'5': .5, '7': .5}}]}}}
    r = analyze(p)['rows'][0]
    assert r['at_least']['7'] == .5 and sum(r['bins'].values()) == 1
    assert r['break_even_credit'] == pytest.approx(108 / 208)
    assert r['model_expected_net_per_unit'] == pytest.approx(.5 * (1 + 100 / 108) - 1)
    p['metrics']['receptions']['players'][0]['count_probabilities']['5'] = .6
    with pytest.raises(ValueError, match='sum to one'):
        analyze(p)


def test_official_fumble_credit_and_lateral_without_extra_catch():
    s = fixture()
    play = next(p for p in s['plays'] if p['game_id'] == '2025_01_A_B' and p['play_type'] == 'pass' and p['yards_after_catch'] is not None)
    identity = play['actors'][0]['player_id']
    box = next(b for b in s['boxes'] if b['game_id'] == play['game_id'] and b['identity'] == identity)
    play['description'] += ' FUMBLES.'
    play['stat_credit'] = {'receiver_player_id': identity, 'complete_pass': 1, 'receiving_yards': 19}
    play['stat_credit_captured_at'] = '2025-10-01T00:00:00Z'
    box['receiving_yards'] = 19
    h, rejected = prepare(s, '2025-10-02T00:00:00Z')
    assert len(h) == 4 and not rejected
    assert next(e for e in h[0]['events'] if e['identity'] == identity and e['caught'])['yards'] == 19
    play['description'] += ' Lateral to new for 7 yards.'
    play['stat_credit'].update(lateral_receiver_player_id='new', lateral_receiving_yards=7)
    s['boxes'].append({'game_id': play['game_id'], 'identity': 'new', 'name': 'new', 'team': 'A', 'position': 'RB',
                      'carries': 0, 'targets': 0, 'receptions': 0, 'rushing_yards': 0, 'receiving_yards': 7,
                      'fetched_at': '2025-10-01T00:00:00Z'})
    h, rejected = prepare(s, '2025-10-02T00:00:00Z')
    assert len(h) == 4 and not rejected
    lateral = next(e for e in h[0]['events'] if e['identity'] == 'new')
    assert lateral['action'] == 'lateral_receiving' and not lateral['caught'] and lateral['yards'] == 7
    play['stat_credit_captured_at'] = '2025-10-03T00:00:00Z'
    h, rejected = prepare(s, '2025-10-02T00:00:00Z')
    assert len(h) == 3 and 'later_stat_credit_capture' in rejected[0]['reasons']


def test_verified_workload_survives_event_failure_but_yardage_is_blocked():
    s = fixture(); s['boxes'][0]['receiving_yards'] += 1
    s['box_verified_game_ids'] = [g['game_id'] for g in s['games']]
    s['box_verification'] = {'sources': [{'captured_at': '2025-10-01T00:00:00Z'}]}
    h, rejected = prepare(s, '2025-10-02T00:00:00Z')
    assert len(h) == 4 and len(rejected) == 1
    assert h[0]['events'] == [] and not h[0]['event_reconciled']
    assert rejected[0]['discrepancies'][0]['field'] == 'receiving_yards'
    req = request(); req['expected_prior_game_ids'] = {t: [g['game_id'] for g in s['games']] for t in ('A', 'B')}
    with pytest.raises(ValueError, match='Unresolved recent yardage events'):
        forecast(h, req, Settings(draws=100))
    result = forecast(h, req, Settings(draws=100), outcomes=('receptions',))
    assert result['recent_history_verified'] and result['workload_only_game_ids'] == ['2025_01_A_B']
    with pytest.raises(ValueError, match='Incomplete recent history'):
        forecast(h[1:], req, Settings(draws=100), outcomes=('receptions',))


def test_final_status_preserves_partial_gain_after_nullified_touchdown():
    s = fixture()
    play = next(p for p in s['plays'] if p['play_type'] == 'pass' and p['yards_after_catch'] is not None)
    play['description'] += ' TOUCHDOWN NULLIFIED by penalty.'
    play['stat_credit'] = {'receiver_player_id': play['actors'][0]['player_id'], 'no_play': 0,
                          'pass_attempt': 1, 'complete_pass': 1, 'receiving_yards': play['yards_gained']}
    play['stat_credit_captured_at'] = '2025-10-01T00:00:00Z'
    h, rejected = prepare(s, '2025-10-02T00:00:00Z')
    assert len(h) == 4 and not rejected


def test_primary_capture_canonical_identity_and_duplicate_checks():
    import pandas as pd
    from research.nfl_game_leaders_source import enrich
    s = fixture(); p = s['plays'][0]
    raw = {'game_id': p['game_id'], 'play_id': p['play_id'], 'season': 2025, 'week': 1,
           'home_team': 'B', 'away_team': 'A', 'desc': p['description'], 'play_type': 'run',
           'rusher_player_id': p['actors'][0]['player_id'], 'rusher_player_name': 'A0', 'rushing_yards': -1,
           'rush_attempt': 1, 'pass_attempt': 0}
    sources = [{'season': 2025, 'captured_at': '2025-10-01T00:00:00Z'}]
    output = enrich(s, {2025: pd.DataFrame([raw])}, sources)
    assert output['plays'][0]['stat_credit']['rushing_yards'] == -1
    assert output['plays'][0]['stat_credit']['no_play'] == 0
    assert 'stat_credit' not in s['plays'][0]
    with pytest.raises(ValueError, match='Duplicate primary'):
        enrich(s, {2025: pd.DataFrame([raw, raw])}, sources)
    raw['home_team'] = 'WRONG'
    with pytest.raises(ValueError, match='canonical'):
        enrich(s, {2025: pd.DataFrame([raw])}, sources)


def test_temporal_boundary_and_mismatch_quarantine():
    s=fixture()
    s['plays'][0]['labelled_at']='2025-10-04T00:00:00Z'
    h,rejected=prepare(s,'2025-10-02T00:00:00Z')
    assert len(h)==3
    assert 'later_labels' in rejected[0]['reasons']
    h,_=prepare(s,'2025-10-02T00:00:00Z',True)
    assert len(h)==4
    s['boxes'][0]['rushing_yards']=100
    h,rejected=prepare(s,'2025-10-02T00:00:00Z',True)
    assert len(h)==3 and 'pbp_box_mismatch' in rejected[0]['reasons']


def test_duplicates_and_bad_joins_fail():
    s=fixture()
    s['plays'].append(s['plays'][0])
    with pytest.raises(ValueError,match='Duplicate PBP'):
        prepare(s,'2025-10-02T00:00:00Z')
    s=fixture()
    s['plays'][0]['home_team']='X'
    with pytest.raises(ValueError,match='schedule mismatch'):
        prepare(s,'2025-10-02T00:00:00Z')


def test_allocation_conserves_budgets_and_zero_share():
    totals=np.arange(100)
    counts=allocate(np.random.default_rng(1),totals,[.2,0,.8],12)
    assert np.array_equal(counts.sum(axis=1),totals)
    assert not counts[:,1].any()
    with pytest.raises(ValueError):
        allocate(np.random.default_rng(1),totals,[.2,.2],12)


def test_joint_forecast_reproducible_and_probabilities_accounted():
    history,_=prepare(fixture(),'2025-10-02T00:00:00Z')
    cfg=Settings(draws=150,seed=4)
    a=forecast(history,request(),cfg)
    b=forecast(history,request(),cfg)
    assert a==b
    for metric,family in a['metrics'].items():
        assert sum(p['win_share'] for p in family['players'])==pytest.approx(1)
        assert all(0<=p['sole_first']<=p['first_or_tied']<=1 for p in family['players'])
        assert all(p['mean'] is None for p in family['players'] if p['residual'])
    assert all(d['max_budget_mismatch']==0 for t in a['diagnostics'].values() for d in t['actions'].values())
    assert a['scope']=='full_game_including_overtime' and not a['market_inputs_used']


def test_future_target_rejected_and_out_not_candidate():
    history,_=prepare(fixture(),'2025-10-02T00:00:00Z')
    r=request()
    r['players'][0]['status']='out'
    result=forecast(history,r,Settings(draws=50))
    assert all(p['identity']!='A0' for p in result['metrics']['rushing_yards']['players'])
    history.append({'game':r['game'],'boxes':[],'events':[]})
    with pytest.raises(ValueError,match='Future/target'):
        forecast(history,r)


def test_opponent_adjusted_against_same_offenses_other_games_only():
    rows=[{'season':2025,'game_id':'1','team':'A','defense':'D','receptions':10,'targets':20},
          {'season':2025,'game_id':'2','team':'A','defense':'X','receptions':5,'targets':20},
          {'season':2024,'game_id':'3','team':'A','defense':'D','receptions':20,'targets':20}]
    result=opponent_effect(rows,'D',2025,'receptions','targets',20)
    assert result['adjustment']==pytest.approx(.125)
    assert result['comparisons'][0]['other_games']==['2']


def test_individual_tie_credit_before_residual_grouping():
    s=fixture()
    game=s['games'][-1]
    s['boxes']=[{'game_id':game['game_id'],'identity':i,'team':t,
        'rushing_yards':10,'receptions':3,'receiving_yards':20} for i,t in [('a','A'),('x','A'),('y','B')]]
    rows=[{'identity':i,'win_share':p,'residual':i.startswith('OTHER:'),'mean':0 if i=='a' else None,'baseline_mean':0 if i=='a' else None}
        for i,p in [('a',1/3),('OTHER:A',1/3),('OTHER:B',1/3)]]
    p={'game':game,'metrics':{m:{'players':deepcopy(rows)} for m in ('rushing_yards','receptions','receiving_yards')}}
    result=grade(p,s)
    assert result['metrics']['receptions']['brier']==pytest.approx(0)
    assert result['metrics']['receptions']['top_choice_credit']==pytest.approx(1/3)
    s['games'][-1]['completed']=False
    assert grade(p,s)['status']=='outcome_unknown'


def test_invalid_settings_and_unsupported_role_override():
    with pytest.raises(ValueError):
        Settings(draws=0)
    h,_=prepare(fixture(),'2025-10-02T00:00:00Z')
    r=request()
    r['role_scenarios']={'A':{'carries':{'A0':1}}}
    with pytest.raises(ValueError,match='scenario evidence'):
        forecast(h,r,Settings(draws=30))


def test_kneels_are_official_carries_and_stale_verified_evidence_fails():
    s=fixture()
    s['plays'][0]['play_type']='qb_kneel'
    h,rejected=prepare(s,'2025-10-02T00:00:00Z')
    assert len(h)==4 and not rejected
    r=request()
    r['availability_verified']=True
    with pytest.raises(ValueError,match='dual-provider evidence'):
        forecast(h,r,Settings(draws=20))
    r['availability_evidence']={t:{provider:{'season':2025,'team':t,'source_ref':'frozen-source',
        'captured_at':'2025-09-01T00:00:00Z','week':5} for provider in ('sleeper','fantasypros_depth','fantasypros_injuries')} for t in ('A','B')}
    with pytest.raises(ValueError,match='stale'):
        forecast(h,r,Settings(draws=20))


def test_batch_supports_each_game_and_preserves_capture_failure():
    s=fixture()
    r=request()
    s['games'].append(r['game'])
    s['games'].append({**r['game'],'game_id':'second','kickoff':'2025-10-04T17:00:00Z'})
    requests={r['game']['game_id']:r,'second':{'capture_error':'Provider unavailable'}}
    result=batch(s,2025,5,r['decision_at'],Settings(draws=30),requests)
    assert len(result['forecasts'])==1 and result['selected_games']==2
    assert result['skipped'][0]['reason']=='Provider unavailable'
    result['forecasts'][0]['metrics']['receptions']['players'][0]['win_share']=float('nan')
    with pytest.raises(ValueError,match='probability'):
        publish(result,[])


def test_publisher_preserves_scope_provenance_and_rejects_odds():
    h,_=prepare(fixture(),'2025-10-02T00:00:00Z')
    req=request(); req['expected_prior_game_ids']={t:[r['game']['game_id'] for r in h] for t in ('A','B')}
    p=forecast(h,req,Settings(draws=30))
    payload={'version':p['version'],'authority':p['authority'],'market_inputs_used':False,
        'season':2025,'week':5,'decision_at':p['decision_at'],'source_sha256':'frozen-source',
        'forecasts':[p],'skipped':[],'reconciliation_rejections':[]}
    result=publish(payload,[])
    assert result['games'][0]['metrics']==p['metrics']
    assert result['games'][0]['source_sha256']=='frozen-source'
    p['recent_history_verified']=False
    with pytest.raises(ValueError,match='recent-game coverage'):
        publish(payload,[])
    p['recent_history_verified']=True
    payload['market_inputs_used']=True
    with pytest.raises(ValueError,match='market-free'):
        publish(payload,[])


def test_training_order_does_not_change_forecast():
    h,_=prepare(fixture(),'2025-10-02T00:00:00Z')
    cfg=Settings(draws=30)
    assert forecast(h,request(),cfg)==forecast(list(reversed(h)),request(),cfg)


def test_player_transferred_between_opponents_cannot_be_overwritten_by_old_team():
    h,_=prepare(fixture(),'2025-10-02T00:00:00Z')
    r=request()
    r['players'][0]['team']='B'  # A0 is now on B; its old carries are vacant on A
    result=forecast(h,r,Settings(draws=30))
    for family in result['metrics'].values():
        moved=next(p for p in family['players'] if p['identity']=='A0')
        assert moved['team']=='B' and not moved['residual']


def test_total_yards_uses_joint_draws_not_component_leaders_or_quantiles():
    from model.nfl_game_leaders import simulated_values, observed_value
    # A leads rushing, B leads receiving, C leads total in both draws.
    stats = {'a': np.array([[90, 0, 0], [10, 0, 0]]),
             'b': np.array([[0, 1, 90], [0, 1, 10]]),
             'c': np.array([[60, 1, 60], [6, 1, 6]])}
    ids = list(stats)
    totals = simulated_values(stats, ids, 'total_yards')
    assert totals.tolist() == [[90, 90, 120], [10, 10, 12]]
    assert np.argmax(totals, axis=1).tolist() == [2, 2]
    # Negative official yardage is retained; passing/returns do not enter.
    assert observed_value({'rushing_yards': -3, 'receiving_yards': 10,
                           'passing_yards': 300, 'return_yards': 100}, 'total_yards') == 7


def test_total_forecast_mean_baseline_and_single_outcome_match_joint():
    h, _ = prepare(fixture(), '2025-10-02T00:00:00Z')
    cfg = Settings(draws=300)
    all_outcomes = forecast(h, request(), cfg)
    total = forecast(h, request(), cfg, outcomes=('total_yards',))
    assert total['metrics']['total_yards'] == all_outcomes['metrics']['total_yards']
    families = all_outcomes['metrics']
    for row in families['total_yards']['players']:
        if row['residual']:
            continue
        rush = next(p for p in families['rushing_yards']['players'] if p['identity'] == row['identity'])
        rec = next(p for p in families['receiving_yards']['players'] if p['identity'] == row['identity'])
        assert row['mean'] == pytest.approx(rush['mean'] + rec['mean'])
        assert row['baseline_mean'] == pytest.approx(rush['baseline_mean'] + rec['baseline_mean'])
    counts, _ = prepare(fixture(), '2025-10-02T00:00:00Z', reconciliation_fields=('targets', 'receptions'))
    with pytest.raises(ValueError, match='requested outcome'):
        forecast(counts, request(), cfg, outcomes=('total_yards',))
    h[0]['event_reconciled'] = False
    with pytest.raises(ValueError, match='Unresolved recent yardage'):
        forecast(h, request(), cfg, outcomes=('total_yards',))


def test_total_grading_combines_official_components_and_splits_ties():
    s = fixture(); g = s['games'][-1]
    s['boxes'] = [{'game_id': g['game_id'], 'identity': i, 'team': t,
                   'rushing_yards': rush, 'receiving_yards': rec}
                  for i, t, rush, rec in [('a', 'A', 90, 0), ('b', 'B', 0, 90),
                                          ('c', 'A', 60, 60), ('d', 'B', 70, 50)]]
    rows = [{'identity': i, 'win_share': .5 if i in ('c', 'd') else 0,
             'residual': False, 'mean': 120 if i in ('c', 'd') else 90,
             'baseline_mean': 120 if i in ('c', 'd') else 90} for i in ('c', 'd', 'a', 'b')]
    result = grade({'game': g, 'metrics': {'total_yards': {'players': rows}}}, s)
    total = result['metrics']['total_yards']
    assert total['winner_ids'] == ['c', 'd'] and total['winning_stat'] == 120
    assert total['top_choice_credit'] == .5 and total['brier'] == 0
