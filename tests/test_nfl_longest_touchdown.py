import numpy as np
import pytest

from model.nfl_longest_touchdown import Settings, TouchdownModel, evaluate, field_bin, prepare, simulate
from research.nfl_longest_touchdown import write


def play(i=1, *, action="run", field=60, yards=4, td=False, team="ATL", actor="rb", position="RB", **extra):
    row = {"game_id":"2025_01_ATL_NO","play_id":i,"season":2025,"week":1,
           "season_type":"REG","game_type":"REG","posteam":team,"defteam":"NO" if team=="ATL" else "ATL",
           "home_team":"NO","away_team":"ATL","canonical_home":"NO","canonical_away":"ATL",
           "canonical_season":2025,"canonical_week":1,"kickoff":"2025-09-01T00:00:00Z",
           "labelled_at":"2025-09-02T00:00:00Z","play_type":action,"yardline_100":field,
           "yards_gained":field if td else yards,"down":1,"ydstogo":10,"game_seconds_remaining":3500-i,
           "score_differential":0,"turnover_type":None,"had_sack":False,"drive":1,"drive_plays":4,"quarter":1,
           "elapsed_seconds":30,"description":f"Run for {field if td else yards} yards"+(" TOUCHDOWN" if td else ""),
           "actors":[{"role":"rusher" if action=="run" else "receiver","player_id":actor,"name":actor,"position":position}]}
    row.update(extra)
    return row


def fixture():
    rows=[]
    for team,actor in (("ATL","a"),("NO","n")):
        for down in range(1,5):
            for field in (3,8,15,30,50,75):
                for action in ("run","pass"):
                    for td in (True,False,False,False):
                        rows.append(play(len(rows)+1,action=action,team=team,actor=actor,field=field,td=td,
                                         down=down,position="RB" if action=="run" else "WR"))
    return {"plays":rows}, {"decision_at":"2026-10-05T22:00:00Z",
          "game":{"game_id":"2026_04_ATL_NO","kickoff":"2026-10-06T00:15:00Z","season":2026,"week":4,"away":"ATL","home":"NO"},
          "players":[{"identity":"a","name":"a","team":"ATL","status":"active"},
                     {"identity":"n","name":"n","team":"NO","status":"active"}]}


def test_td_requires_verified_scoring_description_and_geometry():
    snapshot={"plays":[play(1,field=60,yards=55),play(2,field=55,td=True),
        play(3,field=60,td=True,yards_gained=8),
        play(4,field=60,td=True,description="Run for 60 yards TOUCHDOWN. No Play"),
        play(5,td=True,turnover_type="interception",description="INTERCEPTED returned for 60 yards TOUCHDOWN"),
        play(6,td=True,description="Run TOUCHDOWN. FUMBLES recovered in end zone"),
        play(7,td=True,actors=[])]}
    rows,audit=prepare(snapshot,"2026-10-05T22:00:00Z")
    assert [r["play_id"] for r in rows if r["td"]]==[2]
    assert not rows[0]["td"]
    assert audit["unverified_offensive_td_excluded"]==2


def test_time_boundary_later_labels_and_duplicate_or_bad_joins_rejected():
    source={"plays":[play(1),play(2,labelled_at="2026-10-06T00:00:00Z"),play(3,kickoff="2026-10-06T00:00:00Z")]}
    rows,audit=prepare(source,"2026-10-05T22:00:00Z")
    assert len(rows)==1 and audit["future_game_excluded"]==1 and audit["later_label_excluded"]==1
    assert len(prepare(source,"2026-10-05T22:00:00Z",retrospective=True)[0])==2
    with pytest.raises(ValueError,match="Duplicate"):
        prepare({"plays":[play(),play()]},"2026-10-05T22:00:00Z")
    with pytest.raises(ValueError,match="schedule mismatch"):
        prepare({"plays":[play(canonical_home="BUF")]},"2026-10-05T22:00:00Z")


def test_rare_td_prior_is_positive_despite_no_player_long_td():
    source,request=fixture()
    source['plays'].append(play(900,field=75,actor='never_scored'))
    rows,_=prepare(source,request['decision_at'])
    model=TouchdownModel(rows,2026,4,Settings(draws=10))
    p,evidence=model.td_probability('never_scored','RB','run',75,'NO')
    assert 0 < p < 1 and evidence['weighted_player_td']==0
    assert field_bin(5)==0 and field_bin(40)==4
    with pytest.raises(ValueError):
        field_bin(0)


def test_scenario_paths_geometry_score_changes_and_mass_conservation():
    source,request=fixture()
    result=simulate(source,request,Settings(draws=100,seed=23))
    assert result['market_inputs_used'] is False
    assert abs(sum(p['longest_td_win_share'] for p in result['players'])+result['no_scrimmage_td_probability']-1)<1e-9
    saw_td=False
    for ledger in result['sample_ledgers']:
        for event in ledger:
            if event['result']=='touchdown':
                saw_td=True
                assert event['td_distance']==event['field_before']
            assert event['field_before']>0
        assert any(sum(e['score_after'])>0 for e in ledger)
    assert saw_td
    second=simulate(source,request,Settings(draws=100,seed=23))
    assert result['players']==second['players']


def test_longest_td_ties_and_no_td_are_distinct_outcomes():
    prediction={'game':{'game_id':'2025_01_ATL_NO'},'players':[{'identity':'a','longest_td_win_share':.4},
                 {'identity':'n','longest_td_win_share':.4}], 'no_scrimmage_td_probability':.2}
    rows,_=prepare({'plays':[play(1,actor='a',td=True,field=40),play(2,actor='n',team='NO',td=True,field=40)]},'2026-10-05T22:00:00Z')
    grade=evaluate(prediction,rows)
    assert grade['winners']==['a','n']
    assert grade['brier_score']==pytest.approx(.06)
    non_scoring,_=prepare({'plays':[play(1)]},'2026-10-05T22:00:00Z')
    empty=evaluate(prediction,non_scoring)
    assert empty['winners']==[] and empty['brier_score']==pytest.approx(.96)
    with pytest.raises(ValueError,match='Missing actual'):
        evaluate(prediction,[])


def test_target_leak_and_postkickoff_request_are_rejected_and_files_immutable(tmp_path):
    source,request=fixture()
    request['decision_at']=request['game']['kickoff']
    with pytest.raises(ValueError,match='precede'):
        simulate(source,request,Settings(draws=1))
    request['decision_at']='2026-10-05T22:00:00Z'
    source['plays'][0]['game_id']=request['game']['game_id']
    with pytest.raises(ValueError,match='leaked'):
        simulate(source,request,Settings(draws=1))
    path=tmp_path/'frozen.json.gz'
    write(path,{'a':1})
    with pytest.raises(FileExistsError):
        write(path,{'a':2})


def test_score_state_changes_pass_selection_without_assuming_deeper_throws():
    rows=[]
    for i in range(200):
        trailing=i>=100
        rows.append(play(i+1,action='pass' if trailing else 'run',score_differential=-14 if trailing else 14))
    history,_=prepare({'plays':rows},'2026-10-05T22:00:00Z')
    model=TouchdownModel(history,2026,4,Settings(draws=1))
    rng=np.random.default_rng(77)
    lead=model.action_pool('ATL',1,60,14,2000,10)
    trail=model.action_pool('ATL',1,60,-14,2000,10)
    assert sum(model.pick(rng,trail)['action']=='pass' for _ in range(100))>90
    assert sum(model.pick(rng,lead)['action']=='pass' for _ in range(100))<10


def test_unknown_roster_players_are_unresolved_and_target_schedule_verified():
    source,request=fixture()
    request['players'].append({'identity':'new','name':'new','team':'ATL','status':'active'})
    result=simulate(source,request,Settings(draws=3))
    assert result['unresolved_players'][0]['identity']=='new'
    assert all(p['identity']!='new' for p in result['players'])
    source['games']=[{**request['game'],'home':'BUF'}]
    with pytest.raises(ValueError,match='canonical schedule'):
        simulate(source,request,Settings(draws=3))


def test_no_active_player_gets_inactive_opportunities():
    source,request=fixture()
    request['players'].append({'identity':'inactive','name':'inactive','team':'ATL','status':'out'})
    source['plays'].append(play(999,actor='inactive',td=True,field=75))
    result=simulate(source,request,Settings(draws=3))
    assert all(p['identity']!='inactive' for p in result['players'])


def settled_snapshot(rows):
    game_id = rows[0]['game_id']
    end = play(999999, game_id=game_id, action='no_play', quarter=4,
               game_seconds_remaining=0, description='END GAME', actors=[])
    return {'plays': rows+[end],
            'games':[{'game_id':game_id,'completed':True,'home_score':7,'away_score':0}],
            'game_coverage':{game_id:{'play_count':len(rows)+1,'regulation_end_observed':True}}}


def _forward(snapshot_rows, forecast_players, no_td=0.0):
    from research.nfl_longest_touchdown import grade
    forecast = {"game": {"game_id": "2025_01_ATL_NO"}, "decision_at": "2025-08-31T00:00:00Z",
                "players": forecast_players, "no_scrimmage_td_probability": no_td}
    return grade(settled_snapshot(snapshot_rows), forecast)


def test_grade_missing_target_game_is_unknown_not_no_td():
    other = play(1, game_id="2025_02_ATL_NO")
    result = _forward([other], [{"identity": "rb", "name": "rb", "longest_td_win_share": 1.0}])
    assert result["status"] == "outcome_unknown"


def test_grade_fails_closed_when_a_touchdown_play_was_quarantined():
    rows = [play(1, field=20, td=True),
            play(2, field=70, td=True, description="Run for 70 yards TOUCHDOWN. LATERAL to wr")]
    result = _forward(rows, [{"identity": "rb", "name": "rb", "longest_td_win_share": 1.0}])
    assert result["status"] == "needs_review"
    assert [q["play_id"] for q in result["quarantined_td_plays"]] == [2]
    assert "model" not in result


def test_grade_scores_verified_longest_td_and_flags_unmodeled_winner():
    rows = [play(1, field=20, td=True), play(2, field=45, td=True, actor="x", position="WR")]
    players = [{"identity": "rb", "name": "Back", "longest_td_win_share": 0.6},
               {"identity": "OTHER:ATL", "name": "OTHER:ATL", "longest_td_win_share": 0.0}]
    result = _forward(rows, players, no_td=0.4)
    assert result["status"] == "graded"
    assert result["model"]["longest_td_yards"] == 45
    assert result["model"]["winners"] == ["OTHER:ATL"] and result["model"]["winner_was_unmodeled"]
    assert not result["model"]["top_choice_hit"]


def test_grade_rejects_baseline_frozen_at_a_different_decision_time():
    from research.nfl_longest_touchdown import grade
    forecast = {"game": {"game_id": "2025_01_ATL_NO"}, "decision_at": "2025-08-31T00:00:00Z",
                "players": [{"identity": "rb", "name": "rb", "longest_td_win_share": 1.0}],
                "no_scrimmage_td_probability": 0.0}
    baseline = {**forecast, "decision_at": "2025-09-01T00:00:00Z"}
    with pytest.raises(ValueError, match="decision time"):
        grade(settled_snapshot([play(1, field=20, td=True)]), forecast, baseline)


def test_newcomer_share_counts_players_absent_from_prior_three_team_games():
    from model.nfl_longest_touchdown import newcomer_share
    rows = []
    for g in range(5):
        for k, actor in enumerate(["a", "a", "a", "new" if g == 4 else "a"]):
            rows.append({"game_id": f"G{g}", "kickoff": f"2025-09-0{g+1}T00:00:00Z", "season": 2025,
                         "team": "ATL", "action": "run", "actor": actor})
    share = newcomer_share(rows)
    # Games 3 and 4 have three prior games; one of their eight carries is new.
    assert share["run"] == 1/8 and share["pass"] == 0.


def test_newcomer_reserve_gives_unlisted_scorers_nonzero_mass_and_keeps_accounting():
    snapshot, request = fixture()
    extra = [play(10_000+i, team="ATL", actor=f"depth{i}", field=50, td=(i % 5 == 0),
                  game_id=f"2025_0{2+i//10}_ATL_NO", kickoff=f"2025-09-{10+i//10}T00:00:00Z",
                  canonical_week=2+i//10, week=2+i//10)
             for i in range(40)]
    snapshot = {"plays": snapshot["plays"] + extra}
    off = simulate(snapshot, request, Settings(draws=400, newcomer_reserve=False))
    on = simulate(snapshot, request, Settings(draws=400, newcomer_reserve=True))
    share = {p["identity"]: p for p in on["players"]}
    assert {p["identity"]: p for p in off["players"]}["OTHER:ATL"]["mean_opportunities"] == 0
    assert share["OTHER:ATL"]["mean_opportunities"] > 0
    total = sum(p["longest_td_win_share"] for p in on["players"]) + on["no_scrimmage_td_probability"]
    assert abs(total - 1) < 1e-9


def test_partial_or_unfinalized_game_cannot_be_graded():
    from research.nfl_longest_touchdown import grade
    forecast={'game':{'game_id':'2025_01_ATL_NO'},'decision_at':'2025-08-31T00:00:00Z',
              'players':[{'identity':'rb','name':'Back','longest_td_win_share':1.}], 'no_scrimmage_td_probability':0.}
    snapshot=settled_snapshot([play(1,td=True)])
    assert grade(snapshot,forecast)['status']=='graded'
    snapshot['plays'].pop()
    assert grade(snapshot,forecast)['status']=='outcome_unknown'
    snapshot=settled_snapshot([play(1,td=True)])
    snapshot['games'][0]['completed']=False
    assert grade(snapshot,forecast)['status']=='outcome_unknown'


def test_unknown_scorer_ties_preserve_individual_fractional_credit():
    rows,_=prepare({'plays':[play(i+1,actor=a,field=40,td=True) for i,a in enumerate(['rb','x','y'])]},'2026-10-05T22:00:00Z')
    forecast={'game':{'game_id':'2025_01_ATL_NO'},'players':[
        {'identity':'rb','longest_td_win_share':1/3},{'identity':'OTHER:ATL','longest_td_win_share':2/3}],
        'no_scrimmage_td_probability':0.}
    result=evaluate(forecast,rows)
    assert result['individual_winners']==['rb','x','y']
    assert result['brier_score']==pytest.approx(0.)


def test_regulation_training_and_grading_exclude_overtime():
    rows,audit=prepare({'plays':[play(1,td=True,field=20,quarter=4),play(2,td=True,field=80,actor='ot',quarter=5)]},'2026-10-05T22:00:00Z')
    assert audit['overtime_excluded']==1
    forecast={'game':{'game_id':'2025_01_ATL_NO'},'players':[{'identity':'rb','name':'Back','longest_td_win_share':1.}], 'no_scrimmage_td_probability':0.}
    assert evaluate(forecast,rows)['longest_td_yards']==20
    with pytest.raises(ValueError,match='Quarter coverage'):
        prepare({'plays':[play(3,quarter=None)]},'2026-10-05T22:00:00Z')


def test_unattributed_passes_do_not_create_named_or_residual_targets(monkeypatch):
    source,request=fixture()
    template=prepare({'plays':[play(999,action='pass',actors=[],yards=0)]},request['decision_at'])[0][0]
    monkeypatch.setattr(TouchdownModel,'action_pool',lambda *args:([template],np.array([1.])))
    result=simulate(source,request,Settings(draws=5,max_snaps=8))
    assert result['no_scrimmage_td_probability']==1.
    assert all(p['mean_opportunities']==0 for p in result['players'])
    assert result['diagnostics']['unattributed_pass_without_target']==40


def test_kneel_requires_sufficient_downs_after_opponent_timeouts():
    from model.nfl_longest_touchdown import can_exhaust_clock
    assert not can_exhaust_clock(90,1,3)
    assert not can_exhaust_clock(90,3,0)
    assert not can_exhaust_clock(20,4,0)
    assert can_exhaust_clock(90,1,0)
    assert can_exhaust_clock(30,3,0)


def test_halftime_receiving_team_does_not_depend_on_current_possession(monkeypatch):
    source,request=fixture()
    template=prepare({'plays':[play(999,action='run',yards=0,elapsed_seconds=60)]},request['decision_at'])[0][0]
    monkeypatch.setattr(TouchdownModel,'action_pool',lambda *args:([template],np.array([1.])))
    monkeypatch.setattr(TouchdownModel,'td_probability',lambda *args:(0.,{}))
    monkeypatch.setattr(TouchdownModel,'gain_pool',lambda *args:([template],np.array([1.])))
    result=simulate(source,request,Settings(draws=1,max_snaps=40))
    ledger=result['sample_ledgers'][0]
    assert ledger[30]['clock_before']==1800
    assert ledger[30]['team'] != ledger[0]['team']


def test_ot_end_marker_proves_regulation_finished_without_grading_ot_td():
    from research.nfl_longest_touchdown import grade, regulation_finished
    rows=[play(1,td=True,field=20,quarter=4),play(2,td=True,field=80,actor='ot',quarter=5)]
    snapshot=settled_snapshot(rows)
    snapshot['plays'][-1].update(quarter=5,game_seconds_remaining=525)
    assert regulation_finished(snapshot['plays'])
    f={'game':{'game_id':'2025_01_ATL_NO'},'decision_at':'2025-08-31T00:00:00Z',
       'players':[{'identity':'rb','name':'Back','longest_td_win_share':1.}], 'no_scrimmage_td_probability':0.}
    assert grade(snapshot,f)['model']['longest_td_yards']==20


def test_comparison_baseline_reserves_probability_for_unlisted_scorers():
    from research.nfl_longest_touchdown import simple_baseline
    source,request=fixture()
    for p in request['players']:p['position']='RB'
    for week in range(2,6):
        source['plays'] += [play(10000+week*100+i,actor=f'depth{week}',field=50,td=i%3==0,
             game_id=f'2025_0{week}_ATL_NO',week=week,canonical_week=week,kickoff=f'2025-09-{week+10}T00:00:00Z') for i in range(12)]
    history,_=prepare(source,request['decision_at'])
    result=simple_baseline(history,request,Settings(draws=500,newcomer_reserve=True))
    assert result['newcomer_reserve']['run']>0
    assert next(p for p in result['players'] if p['identity']=='OTHER:ATL')['longest_td_win_share']>0
    assert sum(p['longest_td_win_share'] for p in result['players'])+result['no_scrimmage_td_probability']==pytest.approx(1.)


def test_grade_rejects_baseline_with_different_frozen_source():
    from research.nfl_longest_touchdown import grade
    f={'game':{'game_id':'2025_01_ATL_NO'},'decision_at':'2025-08-31T00:00:00Z',
       'source_sha256':'one','request_sha256':'same',
       'players':[{'identity':'rb','name':'Back','longest_td_win_share':1.}], 'no_scrimmage_td_probability':0.}
    with pytest.raises(ValueError,match='frozen forecast'):
        grade(settled_snapshot([play(1,td=True)]),f,{**f,'source_sha256':'two'})


def test_development_comparison_pairs_by_game_and_reports_unmatched_games():
    from research.nfl_longest_touchdown_comparison import paired_summary
    a=[{'game_id':'G1','log_loss':2.,'brier_score':.8}, {'game_id':'G2','log_loss':3.,'brier_score':.9}]
    b=[{'game_id':'G2','log_loss':2.,'brier_score':.7}]
    result=paired_summary(a,b)
    assert result['paired_games']==1 and result['left_only']==['G1']
    assert result['log_loss']['mean_left_minus_right']==1.
    assert result['log_loss']['game_bootstrap_95_percent_interval']==[1.,1.]


def test_known_voided_and_defensive_scores_do_not_block_scrimmage_grading():
    rows=[play(1,td=True,field=20),
          play(2,td=True,field=80,description='Run TOUCHDOWN NULLIFIED by penalty. No Play'),
          play(3,td=True,field=90,turnover_type='interception',actors=[],
               description='INTERCEPTED returned for 90 yards TOUCHDOWN')]
    result=_forward(rows,[{'identity':'rb','name':'Back','longest_td_win_share':1.}])
    assert result['status']=='graded' and result['model']['longest_td_yards']==20
