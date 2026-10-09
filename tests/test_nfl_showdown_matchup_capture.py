from copy import deepcopy
from datetime import datetime,timezone
import pytest
from research.nfl_showdown_matchup_capture import validate_capture,select_upcoming_upload

NOW=datetime(2026,9,27,15,tzinfo=timezone.utc)
def fixture():
    salary=[];players=[]
    for i,(team,opp) in enumerate((("TB","MIN"),("MIN","TB")),1):
        s={"dk_player_id":i,"captain_dk_player_id":i+100,"captain_salary":9000,"ff_player_id":i,"position":"QB","team":team,"opponent":opp,
           "game_key":"MIN@TB","identity_method":"gsis_id","identity_evidence":{"gsisId":f"gsis-{i}"},"created_at":"2026-09-27T14:00:00Z"}
        p={"player_id":i,"position":"QB","team":team,"opponent":opp,"player_gsis_id":f"gsis-{i}","run_id":"baseline",
           "created_at":"2026-09-27T14:00:00Z","source_evidence":{"game_id":"2026_03_MIN_TB"}}
        salary.append(s);players.append({"dk_player_id":i,"player_id":i,"team":team,"game_id":"2026_03_MIN_TB","kickoff":"2026-09-27T17:00:00Z","baseline":p,
                                        "shadow":{"ledger":[],"delta":0}})
    return {"format":"showdown","baseline_version":"nfl-dfs-historical-v5","baseline_run_id":"baseline",
            "baseline_as_of_at":"2026-09-27T14:00:00Z","as_of_at":"2026-09-27T14:30:00Z","salary_snapshot":salary,"players":players}

def test_pregame_same_identity_baseline_is_preserved():
    value=fixture();before=deepcopy(value)
    assert validate_capture(value,as_of=NOW)==before

@pytest.mark.parametrize("mutation,match",[
    (lambda d:d.update(format="classic"),"Showdown"),
    (lambda d:d["players"][0].update(kickoff="2026-09-27T14:59:00Z"),"kickoff"),
    (lambda d:d["players"][0]["baseline"].update(position="RB"),"position/team"),
    (lambda d:d["players"][0]["baseline"].update(team="ATL"),"position/team"),
    (lambda d:d["players"][0]["baseline"].update(player_gsis_id="other"),"GSIS"),
    (lambda d:d["players"][0]["baseline"].update(created_at="2026-09-27T15:01:00Z"),"after cutoff"),
    (lambda d:d.update(forecast_state="retrospective_development"),"Retrospective"),
    (lambda d:d["salary_snapshot"][0].update(captain_dk_player_id=None),"Captain"),
    (lambda d:d["players"][0]["shadow"].update(delta=1),"invent"),
])
def test_identity_and_cutoff_fail_closed(mutation,match):
    value=fixture();mutation(value)
    with pytest.raises(ValueError,match=match):validate_capture(value,as_of=NOW)

def test_no_live_showdown_is_pending_without_synthetic_replacement():
    class Db:
        def execute(self,query,params):
            assert "g.kickoff<=%s" in query and "u.player_count" in query
            return []
    assert select_upcoming_upload(Db(),season=2026,week=3,as_of=NOW) is None

def test_live_adapter_retains_unmatched_salary_population(monkeypatch):
    import research.nfl_showdown_matchup_capture as module
    value=fixture();value["skipped"]=[{"dk_player_id":3,"reason":"baseline_or_game_missing"}]
    all_salary=deepcopy(value["salary_snapshot"])
    all_salary.append({**all_salary[0],"dk_player_id":3,"captain_dk_player_id":103,"ff_player_id":None,"identity_method":"unmatched"})
    class Db:
        def execute_one(self,query,params):
            if "slate_uploads" in query:return {"format":"showdown","projection_run_id":"baseline","created_at":"2026-09-27T13:00:00Z","player_count":3}
            return {"season":2026,"week":3,"created_at":"2026-09-27T13:00:00Z","as_of_at":"2026-09-27T13:00:00Z"}
        def execute(self,query,params):
            if "slate_players" in query:return all_salary
            return [{"game_id":"2026_03_MIN_TB","away":"MIN","home":"TB","kickoff":"2026-09-27T17:00:00Z","completed":False}]
    monkeypatch.setattr(module,"capture_comparison",lambda *a,**k:deepcopy(value))
    result=module.capture(Db(),upload_id="upload",as_of=NOW)
    assert len(result["salary_snapshot"])==3
    assert len(result["players"])==2
    assert result["skipped"]==value["skipped"]
    assert result["capture_contract"]["numerical_matchup_adjustment"] is False
