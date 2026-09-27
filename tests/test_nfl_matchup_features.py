from datetime import datetime, timezone, timedelta
from copy import deepcopy

import pytest

from model.nfl_matchup_features import build_matchup, normalized_rows, participant_manifests, SCHEMA

NOW = datetime(2026,9,27,14,tzinfo=timezone.utc)
GAME = {"game_id":"2026_03_MIN_TB","home":"TB","away":"MIN","kickoff":NOW+timedelta(hours=3)}


def sources():
    games=[]; snaps=[]
    for i,(home,away) in enumerate((("TB","ATL"),("MIN","GB")),1):
        gid=f"2026_01_{away}_{home}"
        games.append({"game_id":gid,"home":home,"away":away,"kickoff":NOW-timedelta(days=7),"completed":True})
        payload={"game_id":gid,"stats_schema":SCHEMA,"parser_version":"nflverse-pfr-v1",
                 "captured_at":(NOW-timedelta(hours=1)).isoformat(),"identity_manifest":{"digest":"fixed"},
                 "rows":[{"section":"passing_advanced","team":home,"stats":{"times_pressured_pct":20,"times_sacked":0}},
                         {"section":"passing_advanced","team":away,"stats":{"times_pressured_pct":30,"times_sacked":3}},
                         {"section":"rushing_advanced","team":home,"position":"RB","identity_status":"resolved",
                          "stats":{"carries":10,"rushing_yards_before_contact":20,"rushing_yards_after_contact":30}}]}
        for j,row in enumerate(payload["rows"]):
            row.update(gsis_id=f"{gid}-{j}", identity_status="resolved")
        expected=[{"team":r["team"],"gsis_id":r["gsis_id"],"position":r.get("position","QB"),
                   "passer":r["section"]=="passing_advanced","rusher":r["section"]=="rushing_advanced",
                   "carries":r["stats"].get("carries",0)} for r in payload["rows"]]
        snaps.append({"snapshot_id":i,"game_id":gid,"captured_at":NOW-timedelta(hours=1),"recorded_at":NOW-timedelta(minutes=59),"payload":payload,
                      "participant_manifest":{"manifest_hash":f"participants-{i}","game_id":gid,"rows":expected,"available_at":(NOW-timedelta(minutes=30)).isoformat()}})
    return games,snaps


def test_later_revision_cannot_change_frozen_features():
    games,snaps=sources()
    first=build_matchup(game=GAME,prior_games=games,snapshots=snaps,as_of=NOW)
    later=deepcopy(snaps[0]);later["snapshot_id"]=99;later["recorded_at"]=NOW+timedelta(seconds=1)
    later["payload"]["rows"][0]["stats"]["times_pressured_pct"]=99
    assert build_matchup(game=GAME,prior_games=games,snapshots=snaps+[later],as_of=NOW)==first
    own=first["teams"]["TB"]["offense"]
    assert own["pressure_exact_denominator"] is None
    assert own["charted_sacks"] == 0
    assert own["rb_contact_yards_per_carry"] == 5


def test_legacy_identity_and_missing_contact_are_not_zero():
    games,snaps=sources();snaps[0]["payload"].pop("identity_manifest")
    out=build_matchup(game=GAME,prior_games=games,snapshots=snaps,as_of=NOW)
    assert out["teams"]["TB"]["offense"]["rb_contact_yards_per_carry"] is None
    assert out["teams"]["TB"]["offense"]["unresolved_rushing_rows"] == 1


def test_multi_qb_rate_cannot_be_pooled_without_denominator():
    games,snaps=sources();snaps[0]["payload"]["rows"].append(deepcopy(snaps[0]["payload"]["rows"][0]))
    out=build_matchup(game=GAME,prior_games=games,snapshots=snaps,as_of=NOW)
    assert out["teams"]["TB"]["offense"]["pressure_pct"] is None


def test_html_and_csv_units_match_and_future_freeze_rejected():
    html={"parser_version":"pfr-boxscore-v1","rows":[{"team":"WSH","section":"passing_advanced","stats":{"pass_pressured_pct":20}}]}
    csv={"stats_schema":SCHEMA,"rows":[{"team":"WAS","section":"passing_advanced","stats":{"times_pressured_pct":20}}]}
    assert normalized_rows(html)==normalized_rows(csv)
    games,snaps=sources()
    with pytest.raises(ValueError,match="precede"):
        build_matchup(game=GAME,prior_games=games,snapshots=snaps,as_of=GAME["kickoff"])


def test_missing_backup_and_missing_rb_cannot_create_complete_rates():
    games,snaps=sources()
    expected=snaps[0]["participant_manifest"]["rows"]
    expected.append({"team":"TB","gsis_id":"backup","position":"QB","passer":True,"rusher":False,"carries":0})
    expected.append({"team":"TB","gsis_id":"second-rb","position":"RB","passer":False,"rusher":True,"carries":3})
    own=build_matchup(game=GAME,prior_games=games,snapshots=snaps,as_of=NOW)["teams"]["TB"]["offense"]
    assert own["pressure_pct"] is None and own["rb_contact_yards_per_carry"] is None
    assert own["pressure_coverage_complete"] is False and own["contact_coverage_complete"] is False
    assert "participant_set_mismatch" in own["participant_coverage"][0]["pressure"]["reasons"]


def test_missing_contact_field_or_wrong_carry_total_withholds_whole_game():
    for mutation in ({"rushing_yards_after_contact":None},{"carries":9}):
        games,snaps=sources();snaps[0]["payload"]["rows"][2]["stats"].update(mutation)
        own=build_matchup(game=GAME,prior_games=games,snapshots=snaps,as_of=NOW)["teams"]["TB"]["offense"]
        assert own["rb_carries"] is None and own["contact_coverage_complete"] is False


def test_weekly_manifest_retains_raw_identity_and_rejects_later_revisions():
    games,_=sources();gid=games[0]["game_id"]
    row={"id":1,"team":"TB","source":"nflverse","fetched_at":NOW-timedelta(minutes=1),
         "source_row":{"game_id":gid,"team":"TB","player_id":"00-rb","position":"RB","carries":10,"attempts":0}}
    first=participant_manifests([row],games,NOW)
    later=deepcopy(row);later["id"]=2;later["fetched_at"]=NOW+timedelta(seconds=1);later["source_row"]["carries"]=50
    assert participant_manifests([row,later],games,NOW)==first
    assert first[gid]["source_kind"]=="nflverse_weekly_player_stats"
    assert first[gid]["pbp_participant_ids_available"] is False
    assert first[gid]["rows"][0]["gsis_id"]=="00-rb"


def test_no_participant_manifest_is_explicitly_incomplete():
    games,snaps=sources();snaps[0].pop("participant_manifest")
    own=build_matchup(game=GAME,prior_games=games,snapshots=snaps,as_of=NOW)["teams"]["TB"]["offense"]
    assert own["pressure_coverage_complete"] is False
    assert "participant_source_unavailable" in own["participant_coverage"][0]["pressure"]["reasons"]


def test_future_participant_manifest_is_not_eligible_at_cutoff():
    games,snaps=sources();snaps[0]["participant_manifest"]["available_at"]=(NOW+timedelta(seconds=1)).isoformat()
    result=build_matchup(game=GAME,prior_games=games,snapshots=snaps,as_of=NOW)
    assert result["teams"]["TB"]["missing_game_ids"] == [games[0]["game_id"]]
    assert result["teams"]["TB"]["offense"]["pressure_coverage_complete"] is False
