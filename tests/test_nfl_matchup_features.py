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


def multi_qb_sources():
    games, snaps = sources()
    gid = games[0]["game_id"]
    qb = snaps[0]["payload"]["rows"][0]
    qb["stats"].update(times_pressured=7, times_pressured_pct=28)
    backup = deepcopy(qb)
    backup.update(gsis_id="backup")
    backup["stats"].update(times_pressured=1, times_pressured_pct=25)
    snaps[0]["payload"]["rows"].append(backup)
    weekly = [{"id": i, "source": "nflverse", "fetched_at": NOW-timedelta(minutes=10),
        "source_row": {"game_id": gid, "team": "TB", "player_id": player,
            "player_name": name, "position": "QB", "attempts": attempts,
            "sacks_suffered": 0, "carries": carries}}
        for i, (player, name, attempts, carries) in enumerate(
            [(qb["gsis_id"], "M.Stafford", 25, 2), ("backup", "S.Bennett", 3, 1)], 1)]
    plays = [{"game_id": gid, "play_id": i, "team": "TB", "qb_name": name,
        "scramble": scramble, "sack": False, "available_at": (NOW-timedelta(minutes=5)).isoformat()}
        for i, (name, scramble) in enumerate([("M.Stafford", False)]*25
            + [("S.Bennett", False)]*3 + [("S.Bennett", True)], 1)]
    manifest = participant_manifests(weekly, games, NOW, plays)[gid]
    # Retain the fixture's other participants so coverage checks remain intact.
    originals = snaps[0]["participant_manifest"]["rows"]
    manifest["rows"] += [r for r in originals if r["gsis_id"] != qb["gsis_id"]]
    snaps[0]["participant_manifest"] = manifest
    return games, snaps, weekly, plays


def test_real_stafford_bennett_example_pools_counts_for_offense_and_defense():
    games, snaps, _, _ = multi_qb_sources()
    out = build_matchup(game=GAME, prior_games=games, snapshots=snaps, as_of=NOW)
    own = out["teams"]["TB"]["offense"]
    assert own["pressure_coverage_complete"] is True
    assert own["pressure_pct"] == pytest.approx(100 * 8 / 29)
    assert own["pressure_pct"] != pytest.approx((28 + 25) / 2)
    assert own["pressure_game_observations"][0]["dropbacks"] == 29
    # The same source rows define pressure created by the opponent's defense.
    opponent_game = {**GAME, "away": "ATL"}
    defense = build_matchup(game=opponent_game, prior_games=games, snapshots=snaps,
        as_of=NOW)["teams"]["ATL"]["defense"]
    assert defense["pressure_pct"] == own["pressure_pct"]


def test_multi_qb_inputs_reach_combined_pickem_calculation():
    from research.nfl_pickem_matchup import matchup_values, paired_probabilities
    games, snaps, _, _ = multi_qb_sources()
    for game, snap in zip(list(games), list(snaps)):
        away_rb = {"section": "rushing_advanced", "team": game["away"], "position": "RB",
            "gsis_id": game["away"]+"-rb", "identity_status": "resolved",
            "stats": {"carries": 10, "rushing_yards_before_contact": 20, "rushing_yards_after_contact": 30}}
        snap["payload"]["rows"].append(away_rb)
        snap["participant_manifest"]["rows"].append({**away_rb, "passer": False, "rusher": True, "carries": 10})
        older_game = {**game, "game_id": game["game_id"]+"-older", "kickoff": game["kickoff"]-timedelta(days=7)}
        older_snap = deepcopy(snap)
        older_snap.update(snapshot_id=snap["snapshot_id"]+10, game_id=older_game["game_id"])
        games.append(older_game); snaps.append(older_snap)
    matchup = build_matchup(game=GAME, prior_games=games, snapshots=snaps, as_of=NOW)
    values = matchup_values(matchup)
    assert values is not None
    assert values["own_pressure"] == pytest.approx(100 * 8 / 29 - 20)
    baseline, adjusted = paired_probabilities(.49, .003, values["own_pressure"] * .01)
    assert adjusted["home"] > baseline["home"]


def test_passing_play_manifest_is_json_serializable_and_preserves_observation_time():
    import json
    games, _, weekly, plays = multi_qb_sources()
    for p in plays:
        p["available_at"] = NOW-timedelta(minutes=5)
    result = participant_manifests(weekly, games, NOW, plays)
    json.dumps(result)
    participant = result[games[0]["game_id"]]["rows"][0]
    assert participant["available_at"] == (NOW-timedelta(minutes=5)).isoformat()
    assert participant["pressure_play_source"]["plays"][0]["available_at"] == participant["available_at"]


@pytest.mark.parametrize("mutation", ["missing_play", "future_play", "wrong_name", "duplicate_play", "wrong_sack", "wrong_rate"])
def test_multi_qb_requires_reconciled_timely_exposures(mutation):
    games, snaps, weekly, plays = multi_qb_sources()
    if mutation == "missing_play":
        plays.pop(0)
    elif mutation == "future_play":
        plays[0]["available_at"] = (NOW+timedelta(seconds=1)).isoformat()
    elif mutation == "wrong_name":
        plays[0]["qb_name"] = "different-quarterback"
    elif mutation == "duplicate_play":
        plays.append(deepcopy(plays[0]))
    elif mutation == "wrong_sack":
        plays[0]["sack"] = True
    else:
        snaps[0]["payload"]["rows"][0]["stats"]["times_pressured_pct"] = 35
    rows = participant_manifests(weekly, games, NOW, plays)[games[0]["game_id"]]["rows"]
    originals = snaps[0]["participant_manifest"]["rows"]
    snaps[0]["participant_manifest"]["rows"] = rows + [r for r in originals if r["position"] != "QB" or r["team"] != "TB"]
    own = build_matchup(game=GAME, prior_games=games, snapshots=snaps, as_of=NOW)["teams"]["TB"]["offense"]
    assert own["pressure_coverage_complete"] is False
    assert own["pressure_pct"] is None
    assert "qb_pressure_denominator_missing_or_mismatched" in own["participant_coverage"][0]["pressure"]["reasons"]


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
