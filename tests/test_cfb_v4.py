from datetime import datetime,timedelta,timezone
import numpy as np
import pytest
from research.cfb_v4_features import offensive_points,is_snap,audit_games,profile,replay
from research.cfb_v4_calibration import proposition,fit_market,date_split,predict
from research.cfb_forecast_v4 import market_quote
from ingest.cfb_v4_context import validate_team,context_status

NOW=datetime(2026,10,17,16,tzinfo=timezone.utc)


def test_returns_and_bad_score_deltas_never_credit_offense():
    drive={"drive_result":"INT TD","start_offense_score":7,"end_offense_score":7}
    assert offensive_points(drive,[{"play_type":"Interception Return Touchdown"}])==0
    drive.update(drive_result="TD",end_offense_score=14)
    assert offensive_points(drive,[{"play_type":"Passing Touchdown"}])==7
    assert offensive_points(drive,[{"play_type":"Interception Return Touchdown"}]) is None
    drive.update(drive_result="FG",end_offense_score=13)
    assert offensive_points(drive,[]) is None


def test_kneels_special_teams_and_timeout_are_not_scrimmage():
    assert not is_snap({"play_type":"Rush","play_text":"Quarterback kneels"})
    for kind in ("Kickoff","Punt","Timeout","End Period"):
        assert not is_snap({"play_type":kind})
    assert is_snap({"play_type":"Rush","play_text":"Smith rushes for 7"})


def test_discrete_cover_push_ev_and_opposite_side():
    errors=[-1,0,1,2]*5
    home=proposition(3,errors,-3,-110,"home","spread")
    away=proposition(3,errors,-3,-110,"away","spread")
    assert home["p_push"]==.25
    assert home["p_win"]==.5 and away["p_win"]==.25
    assert home["p_win"]+home["p_push"]+home["p_loss"]==1
    assert home["ev"]==pytest.approx(.5*(1+100/110)+.25-1)
    assert proposition(3,errors,-3.5,-110,"home","spread")["p_push"]==0
    assert proposition(3,errors[:5],-3,-110,"home","spread")["p_win"] is None


def dataset():
    rows=[]
    for day in range(12):
        for n in range(10):
            # Football correction is systematically in the wrong direction: fitting must allow zero weight.
            actual=(n%3)-1
            rows.append({"date":(NOW+timedelta(days=day)).date().isoformat(),"game_id":day*10+n,
                         "market":{"margin":0.,"total":50.,"home_prob":.5},
                         "raw":{"margin":10.,"total":60.},"actual":{"margin":actual,"total":50+actual}})
    return rows


def test_date_splits_do_not_leak_and_weights_can_be_zero():
    rows=dataset();train,cal,test,_=date_split(rows)
    assert max(r["date"] for r in train)<min(r["date"] for r in cal)<min(r["date"] for r in test)
    fit=fit_market(rows)
    assert fit["margin"]["weight"]==0 and fit["total"]["weight"]==0
    before=fit["margin"]["weight"],fit["margin"]["residuals"],fit["moneyline"]
    for r in test:r["actual"]={"margin":100,"total":200}
    after=fit_market(rows)
    assert before==(after["margin"]["weight"],after["margin"]["residuals"],after["moneyline"])


def test_latest_quote_refuses_missing_book_stale_and_future():
    q={"spread_home":-3.,"spread_away":3.,"spread_home_price":-110,"spread_away_price":-110,
       "total_line":50.,"over":-110,"under":-110,"ml_home":-150,"ml_away":130,"last_update":NOW.isoformat()}
    h={"id":1,"captured_at":NOW,"books":{"draftkings":q}}
    assert market_quote([h],NOW)["margin"]==3
    assert market_quote([h],NOW-timedelta(seconds=1)) is None
    assert market_quote([h],NOW+timedelta(minutes=16)) is None
    newer={"id":2,"captured_at":NOW+timedelta(seconds=1),"books":{}}
    assert market_quote([h,newer],NOW+timedelta(seconds=2)) is None


def test_context_rejects_wrong_team_future_source_and_postkickoff():
    game={"id":1,"home_team_id":1,"away_team_id":2,"commence_time":NOW+timedelta(hours=3)}
    p={"team_id":1,"source_url":"https://school.edu/report","source_published_at":NOW.isoformat(),
       "qb":{"name":"Player","source_player_id":"cfbd-123","status":"confirmed"}}
    validate_team(p,game,NOW)
    with pytest.raises(ValueError):validate_team({**p,"team_id":3},game,NOW)
    with pytest.raises(ValueError):validate_team(p,game,NOW+timedelta(hours=4))
    with pytest.raises(ValueError):validate_team({**p,"source_published_at":(NOW+timedelta(hours=1)).isoformat()},game,NOW)
    assert not context_status([],game,NOW)["complete"]
    capture={"game_id":1,"team_id":1,"kind":"team","captured_at":NOW+timedelta(hours=1),
             "source_published_at":NOW,"payload_json":p}
    assert context_status([capture],game,NOW)["captures"]=={}


def test_prior_replacement_and_recency():
    def entry(season,value):
        return {"season":season,"off":{k:value for k in ("ppd","ppa","success","explosive","field","seconds","drives","other")},
                "def":{},"game_id":value,"audit":{"ppa_plays":40}}
    es=[entry(2025,10),entry(2026,1),entry(2026,2)]
    assert profile(es,2026,0,3)["current_weight"]==1
    assert profile(es,2026,4,3)["current_weight"]==pytest.approx(1/3)
    assert profile(es,2026,0,3)["off"]["ppd"]>profile(es,2026,0,1000)["off"]["ppd"]


def test_regulation_overtime_and_garbage_audit_stay_separate():
    game={"id":1,"season":2026,"commence_time":NOW,"completed":True,"home_score":14,"away_score":0,
          "home_team_id":1,"away_team_id":2,"home_name":"H","away_name":"A"}
    drives=[];plays=[]
    for team in (1,2):
        drives.append({"game_id":1,"cfbd_drive_id":team,"offense_team_id":team,"drive_result":"TD" if team==1 else "PUNT",
          "start_offense_score":0,"end_offense_score":7 if team==1 else 0,"start_period":4,"end_period":4,
          "start_yards_to_goal":70,"start_seconds_remaining":800,"end_seconds_remaining":400})
        for i in range(35):
            plays.append({"game_id":1,"cfbd_drive_id":team,"play_type":"Rushing Touchdown" if team==1 and i==34 else "Rush",
                          "period":4,"offense_score":0,"defense_score":0,"ppa":.1,"yards_gained":2})
    drives.append({"game_id":1,"cfbd_drive_id":3,"offense_team_id":1,"drive_result":"TD","start_offense_score":7,
                   "end_offense_score":14,"start_period":5,"end_period":5})
    plays.append({"game_id":1,"cfbd_drive_id":3,"play_type":"Passing Touchdown","period":5})
    drives.append({"game_id":1,"cfbd_drive_id":4,"offense_team_id":1,"drive_result":"END OF GAME","start_offense_score":14,
                   "end_offense_score":14,"start_period":4,"end_period":4})
    plays.append({"game_id":1,"cfbd_drive_id":4,"play_type":"Rush","play_text":"kneel","period":4})
    stats,audits=audit_games([game],drives,plays)
    home=stats[(1,1)]["audit"]
    assert home["regulation_offensive_points"]==7 and home["overtime_offensive_points"]==7
    assert home["meaningful_drives"]==1 and home["excluded_drives"]==1
    assert home["nonoffensive_or_unattributed_points"]==0
    assert audits[0]["usable"]


def test_replay_cannot_use_same_day_or_future_games():
    from copy import deepcopy
    games=[];stats={}
    for i,days in enumerate((14,7,0),1):
        g={"id":i,"season":2026,"commence_time":NOW-timedelta(days=days),"completed":True,
           "home_score":14,"away_score":7,"home_team_id":1,"away_team_id":2,"neutral_site":False}
        games.append(g)
        for t,o in ((1,2),(2,1)):
            value={"ppd":2.,"ppa":.1,"success":.5,"explosive":.1,"field":70.,"seconds":27.,"drives":11.,"other":0.}
            stats[(i,t)]={"game_id":i,"season":2026,"team":t,"opponent":o,"kickoff":g["commence_time"],
                          "usable":True,"off":dict(value),"def":dict(value),"audit":{"ppa_plays":50}}
    games.append({**games[-1],"id":4,"commence_time":NOW+timedelta(hours=3),"completed":False})
    before=replay(games,stats,2,6)[-1]
    stats[(3,1)]["off"]["ppd"]=1000
    after=replay(games,stats,2,6)[-1]
    assert before["x_home"]==after["x_home"]
    assert 3 not in before["profiles"][1]["source_games"]


def test_forward_grading_uses_frozen_price_push_and_registration():
    from research.cfb_v4_evaluation import grade_market,evaluate
    e={"context":{"complete":False},"raw":{"margin":3,"total":50},
       "forecast":{"margin":3,"total":50,"home_win_probability":.6},
       "market":{"margin":3,"total":50,"book":"draftkings"},
       "propositions":{"spread_home":{"line":-3,"price":-115,"p_win":.6,"p_push":.1,"p_loss":.3,"ev":.1}}}
    assert grade_market(e,3,50)["spread"]["profit"]==0
    assert grade_market(e,4,50)["spread"]["profit"]==pytest.approx(100/115)
    row={"game_id":1,"frozen_at":NOW,"kickoff":NOW+timedelta(hours=3),"commence_time":NOW+timedelta(hours=3),
         "evidence_json":e,"completed":True,"home_score":24,"away_score":20,"game_date":"2026-10-17"}
    r=evaluate([row],{"registered_at":NOW-timedelta(minutes=1)})
    assert r["settled_games"]==1 and r["betting_permission"] is False
    assert r["matched_metrics"]["margin"]["delta_ci95"] is None
    assert r["matched_legacy_benchmarks"]=={}
    row["matched_legacy"]={"cfb-score-context-v1":{"forecast":{"home_points":30,"away_points":20}}}
    paired=evaluate([row],{"registered_at":NOW-timedelta(minutes=1)})["matched_legacy_benchmarks"]["cfb-score-context-v1"]
    assert paired["n"]==1 and paired["margin_error"]==6 and paired["v4_margin_error"]==1
    assert evaluate([row],{"registered_at":NOW+timedelta(minutes=1)})["excluded"]


def test_explicit_conversion_text_recovers_postevent_score_but_never_assumes_pat():
    d={"drive_result":"TD","start_offense_score":7,"end_offense_score":7}
    assert offensive_points(d,[{"play_type":"Passing Touchdown","play_text":"Smith 20 yd pass (Jones Kick)"}])==7
    assert offensive_points(d,[{"play_type":"Passing Touchdown","play_text":"Smith pass for TD"}]) is None
    assert offensive_points(d,[{"play_type":"Passing Touchdown","play_text":"Smith TD (Jones Kick Failed)"}])==6
    assert offensive_points(d,[{"play_type":"Passing Touchdown","play_text":"Smith TD (two-point conversion good)"}])==8
    d["drive_result"]="FUMBLE TD"
    assert offensive_points(d,[{"play_type":"Fumble Return Touchdown"}])==0


def test_source_revision_updates_old_score_even_when_hash_already_matches():
    # Original importer updated the payload hash without refreshing these fields.
    from ingest.cfb_plays import _DRIVE_INSERT,_PLAY_INSERT
    for field in ("start_offense_score","end_offense_score","start_seconds_remaining"):
        assert f"{field} = EXCLUDED.{field}" in _DRIVE_INSERT
        assert f"cfb_drives.{field} IS DISTINCT FROM EXCLUDED.{field}" in _DRIVE_INSERT
    for field in ("offense_score","defense_score","game_seconds_remaining"):
        assert f"{field} = EXCLUDED.{field}" in _PLAY_INSERT


def test_error_decomposition_reconciles_without_calling_turnovers_predictable():
    from research.cfb_v4_diagnostics import decompose
    r=decompose(10,2.5,1,12,24,31)
    assert r["possession_contribution"]==5
    assert r["efficiency_contribution"]==-6
    assert r["other_scoring_contribution"]==6
    assert r["actual_minus_predicted"]==5 and abs(r["identity_error"])<1e-9


def test_linked_made_field_goal_can_repair_stale_drive_score():
    d={"drive_result":"FG","start_offense_score":3,"end_offense_score":3}
    assert offensive_points(d,[{"play_type":"Field Goal Good"}])==3
    assert offensive_points(d,[{"play_type":"Field Goal Missed"}]) is None


def test_context_effects_remain_unfitted_without_real_evidence():
    from research.cfb_v4_context_model import vector,fit_context
    assert vector({"context":{"captures":{}}}) is None
    report,model=fit_context([])
    assert report["status"]=="unfitted_context_effects" and model is None
    assert report["applied_to_primary"] is False


def test_possessions_are_fitted_and_used_in_scoring():
    from research.cfb_v4_calibration import fit_football,football_score
    rows=[]
    for i in range(320):
        fast=i%2==0
        p=[11.,11.,20. if fast else 35.,20. if fast else 35.,.4,.4]
        rate={"off":{"ppd":2.,"drives":14. if fast else 8.}}
        rows.append({"x_home":[float(i%7)]*13,"x_away":[float(i%7)]*13,"pace_features":[p,p],
                     "actual":[rate,rate]})
    model=fit_football(rows)
    def row(seconds):
        return {"x_home":[1.]*13,"x_away":[1.]*13,"pace_features":[[11,11,seconds,seconds,.4,.4]]*2,
                "profiles":{1:{"off":{"other":1.}},2:{"off":{"other":1.}}},"home_team_id":1,"away_team_id":2}
    fast,slow=football_score(row(20),model),football_score(row(35),model)
    assert fast["expected_drives"][0]>slow["expected_drives"][0]
    assert fast["home_points"]==pytest.approx(fast["rates"][0]*fast["expected_drives"][0]+1)


def test_physical_score_guards_and_nonnegative_total_distribution():
    from research.cfb_v4_calibration import centers
    f={"margin":{"weight":1},"total":{"weight":0}}
    assert centers({"raw":{"margin":40,"total":60},"market":{"margin":3,"total":20}},f)["total"]==40
    p=proposition(1,[-10]*20,0,-110,"under","total")
    assert p["interval80"]==[0.,0.] and p["p_push"]==1


@pytest.mark.parametrize("module_name,args", [
    ("research.cfb_forecast_v1",["--publish"]),
    ("research.cfb_forecast_v2",["--publish"]),
    ("research.cfb_forecast_v3",["--publish"]),
    ("research.cfb_prospective_evaluation",["--publish"]),
    ("ingest.cfb_schedule",["--health"]),
    ("ingest.cfb_plays",["--current-season"]),
])
def test_scheduled_commands_check_schema_without_migration_locks(monkeypatch,module_name,args):
    import importlib
    import sys
    from types import SimpleNamespace
    module=importlib.import_module(module_name)
    class CheckedTables(Exception):pass
    class ReadOnlySchema:
        def __init__(self,url,initialize_schema=True):
            assert initialize_schema is False, "Scheduled job attempted application-wide DDL"
        def require_tables(self,names):
            assert "cfb_matchups" in names
            raise CheckedTables()
    monkeypatch.setattr(module,"DatabaseManager",ReadOnlySchema)
    monkeypatch.setattr(module,"load_config",lambda:SimpleNamespace(database_url="unused"))
    monkeypatch.setattr(sys,"argv",[module_name,*args])
    with pytest.raises(CheckedTables):module.main()


def test_weather_venue_coordinates_join_provider_identity_and_reject_invalid_values(monkeypatch):
    from types import SimpleNamespace
    from ingest.cfb_v4_context import refresh_venue_coordinates
    import ingest.cfb_v4_context as module
    response=SimpleNamespace(content=b"venue fixture",raise_for_status=lambda:None,json=lambda:[
        {"id":50,"latitude":40.,"longitude":-75.},
        {"id":51,"latitude":999.,"longitude":-75.},
        {"id":52,"latitude":None,"longitude":None}])
    monkeypatch.setattr(module.requests,"get",lambda *args,**kwargs:response)
    calls=[]
    db=SimpleNamespace(execute_many=lambda sql,rows:calls.append((sql,rows)))
    reference=refresh_venue_coordinates(db,"fixture-key")
    assert calls[0][1]==[(40.,-75.,50)] and "WHERE cfbd_venue_id=%s" in calls[0][0]
    assert reference["valid_source_venues"]==1 and reference["response_sha256"]
    assert refresh_venue_coordinates(db,"")["status"]=="unavailable"


def test_real_source_alternate_touchdown_labels_and_explicit_kick_attempts():
    stale={"drive_result":"TD","start_offense_score":7,"end_offense_score":7}
    p={"play_type":"Pass Reception","scoring":True,"play_text":"Pass complete for 45 yards TOUCHDOWN, clock 09:32. Kicker kick attempt good (H: Holder)"}
    assert offensive_points(stale,[p])==7
    assert offensive_points(stale,[{**p,"scoring":False}]) is None
    assert offensive_points(stale,[{**p,"play_text":p["play_text"]+" overturned, no play"}]) is None
    assert offensive_points(stale,[{**p,"play_type":"Rush"}])==7
    assert offensive_points(stale,[{"play_type":"Blocked Punt Touchdown","scoring":True}])==0
    assert offensive_points(stale,[{**p,"play_type":"Passing Touchdown","play_text":"pass TOUCHDOWN; kick attempt failed"}])==6
