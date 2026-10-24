from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
import json

import pytest
from research import nba_points_forward as f
from model.nba_points_calibration import BoxGame, BoxPlayer

LOCK = datetime(2026,10,23,20,tzinfo=timezone.utc)
DAY = date(2026,10,23)


def source(player, line, when, market="player_points"):
    return dict(event_id="e1",game_date=DAY,commence_time=LOCK,captured_at=when,
                home_team_name="A",away_team_name="B",player=player,market=market,
                books={"dk":{"line":line,"over":-110,"under":-110}})


def raw():
    return [source(n,line,LOCK-timedelta(hours=1)) for n,line in [("A",30.5),("B",25.5),("C",25.5),("D",20.5)]]


def test_cutoff_excludes_late_quotes_and_future_capture_at_creation():
    rows = raw()+[source(n,line,LOCK-timedelta(minutes=10)) for n,line in [("A",30.5),("B",25.5),("C",25.5),("D",40.5)]]
    snapshot = f.build_snapshot(rows,DAY,LOCK,LOCK-timedelta(minutes=30))
    assert snapshot["mode"] == "prospective"
    assert [p["player"] for p in snapshot["selections"]["highest_lines"]["picks"]] == ["A","B","C"]
    assert snapshot["selections"]["highest_ceilings"]["picks"] == []
    assert snapshot["availability_verified"] is False
    assert f.build_snapshot(rows,DAY,LOCK,LOCK+timedelta(hours=1))["mode"] == "retrospective_reconstruction"


def test_frozen_grades_do_not_rerank_using_results_and_dnp_is_unresolved():
    snap = json.loads(json.dumps(f.build_snapshot(raw(),DAY,LOCK,LOCK-timedelta(minutes=30))))
    players = [BoxPlayer(n,n.lower(),"A",True,True,pts,30) for n,pts in [("A",10),("B",20),("C",30),("D",50)]]
    game = BoxGame("espn","A","B",players,LOCK)
    grade = f.grade_snapshot(snap,[game])
    assert grade["selections"]["highest_lines"]["points"] == 60
    assert grade["payout"] is None and grade["snapshot_mode"] == "prospective"
    players[0].played=False;players[0].points=None
    assert f.grade_snapshot(snap,[game])["selections"]["highest_lines"]["points"] is None
    tampered = deepcopy(snap);tampered["candidates"][0]["line"] = 999
    with pytest.raises(ValueError,match="digest"):
        f.grade_snapshot(tampered,[game])


def test_removed_quotes_not_revived_and_existing_artifacts_not_overwritten(tmp_path):
    rows = raw()+[source(n,line,LOCK-timedelta(minutes=15)) for n,line in [("B",25.5),("C",25.5),("D",20.5)]]
    snap = f.build_snapshot(rows,DAY,LOCK,LOCK+timedelta(hours=1))
    assert {r["player"] for r in snap["candidates"]} == {"B","C","D"}
    p = tmp_path/"snapshot.json"
    f.write_new(p,snap)
    with pytest.raises(FileExistsError):f.write_new(p,{})
    assert json.loads(p.read_text())["sha256"] == snap["sha256"]


def test_reject_naive_timestamps_late_all_day_cutoff_and_alias_collision():
    with pytest.raises(ValueError,match="UTC offset"):f.aware("2026-10-23T20:00:00")
    with pytest.raises(ValueError,match="earliest market start"):
        f.build_snapshot(raw(),DAY,LOCK+timedelta(minutes=10),LOCK+timedelta(hours=1))
    with pytest.raises(ValueError,match="ambiguous"):
        f.build_snapshot(raw()+[source(n,10.5,LOCK-timedelta(hours=1)) for n in ["Herb Jones","Herbert Jones"]],DAY,LOCK,LOCK)
