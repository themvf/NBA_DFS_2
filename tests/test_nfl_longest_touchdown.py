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
           "score_differential":0,"turnover_type":None,"had_sack":False,"drive":1,"drive_plays":4,
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


def _forward(snapshot_rows, forecast_players, no_td=0.0):
    from research.nfl_longest_touchdown import grade
    forecast = {"game": {"game_id": "2025_01_ATL_NO"}, "decision_at": "2025-08-31T00:00:00Z",
                "players": forecast_players, "no_scrimmage_td_probability": no_td}
    return grade({"plays": snapshot_rows}, forecast)


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
        grade({"plays": [play(1, field=20, td=True)]}, forecast, baseline)
