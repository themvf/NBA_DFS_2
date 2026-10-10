from copy import deepcopy
import pytest
from model.nfl_workload_risk import fit, predict
from research.nfl_workload_risk import build_report, evaluate


def row(i):
    return {"player_id":str(i),"game_id":f"game-{i}","position":"RB","designation":"QUESTIONABLE",
            "features_available_at":"2026-09-19T10:00:00Z","decision_at":"2026-09-20T10:00:00Z",
            "kickoff":"2026-09-20T17:00:00Z","labels_available_at":"2026-09-21T00:00:00Z",
            "settled_at":"2026-09-20T23:00:00Z","snapshot_id":f'snapshot-{i}',
            "observation_ids":[i+1],"baseline_source_digest":"a"*64,
            "baseline_opportunities":20,"depth_order":1,"days_since_last_active":7,
            "played":i%3!=0,"actual_opportunities":0 if i%3==0 else 8 if i%3==1 else 22}


def current():
    return {**row(1000),"features_available_at":"2026-10-05T22:00:00Z","decision_at":"2026-10-05T23:00:00Z",
            "kickoff":"2026-10-06T00:15:00Z","settled_at":"2026-10-06T04:00:00Z"}


def test_learned_three_states_are_not_fixed_medical_priors():
    cases=[row(i) for i in range(60)];before=deepcopy(cases)
    model=fit(cases,"2026-10-05T22:00:00Z");result=predict(model,current())
    assert cases==before
    assert sum(result["probabilities"].values())==pytest.approx(1)
    assert all(0<v<1 for v in result["probabilities"].values())
    assert result["limited_workload_factor"]==pytest.approx(.4)
    assert result["optimizer_enabled"] is False
    assert predict(model,{**current(),"played":False,"actual_opportunities":999})==result,"Current outcomes are not model features"


def test_missing_training_means_unknown_not_a_guessed_weight():
    model=fit([],"2026-10-05T22:00:00Z")
    assert predict(model,current())["probabilities"] is None


def test_future_labels_are_excluded_and_target_games_cannot_leak():
    cases=[row(i) for i in range(60)]
    future={**row(100),"labels_available_at":"2026-10-07T00:00:00Z"}
    assert fit(cases+[future],"2026-10-05T22:00:00Z")==fit(cases,"2026-10-05T22:00:00Z")
    with pytest.raises(ValueError,match="Target game"):predict(fit(cases,"2026-10-05T22:00:00Z"),{**current(),"game_id":"game-1"})
    with pytest.raises(ValueError,match="not available"):predict(fit(cases,"2026-10-06T01:00:00Z"),current())


def test_invalid_time_role_label_and_duplicate_cases_rejected():
    cases=[row(i) for i in range(60)]
    with pytest.raises(ValueError):fit(cases+[row(0)],"2026-10-05T22:00:00Z")
    for bad in [{**row(0),"features_available_at":"2026-09-20T11:00:00Z"},
                {**row(0),"actual_opportunities":float("nan")},{**row(0),"depth_order":-1},
                {**row(0),"played":False,"actual_opportunities":20}]:
        with pytest.raises(ValueError):fit([bad],"2026-10-05T22:00:00Z")
    with pytest.raises(ValueError,match='provenance'):fit([{**row(0),'observation_ids':[]}],"2026-10-05T22:00:00Z")


def test_walk_forward_never_trains_with_same_game_or_later_labels():
    cases=[row(i) for i in range(60)]
    later={**current(),'labels_available_at':'2026-10-07T00:00:00Z'}
    report=evaluate(cases+[later])
    assert report['held_out_player_games']==1
    assert report['records'][0]['training_cases']==60
    assert report['metrics']['brier']>=0
    assert report['qualification']=='withheld'
    forecast=build_report({'cases':cases,'as_of':'2026-10-05T22:00:00Z','forecast_cases':[current()]})
    assert forecast['production_changed'] is False
    assert forecast['forecasts'][0]['probabilities'] is not None
