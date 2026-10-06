from copy import deepcopy
import pytest
from model.nfl_availability_scenarios import condition_workloads, workload_scenarios

DECISION = "2026-10-05T22:00:00Z"


def inputs():
    players = [{"identity": key, "name": key, "position": pos, "efficiency": .7,
                "components": {field: {"share": value} for field, value in shares.items()}}
               for key, pos, shares in [("qb", "QB", {"attempts": 1, "carries": .1}),
                                       ("rb1", "RB", {"carries": .5, "targets": .1}),
                                       ("rb2", "RB", {"carries": .2, "targets": .05}),
                                       ("wr1", "WR", {"targets": .3}), ("wr2", "WR", {"targets": .2})]]
    forecasts = [{"game_id": "target", "team": "NO", "players": players,
                  "budgets": {"attempts": {"mean": 32}, "carries": {"mean": 24}, "targets": {"mean": 30}}}]
    evidence = [{"game_id": "target", "team": "NO", "identity": p["identity"],
                 "state": "OUT_CONFIRMED" if p["identity"] == "rb1" else "EXPECTED_ACTIVE",
                 "expected_role": "QB1" if p["position"] == "QB" else "RB_COMMITTEE" if p["position"] == "RB" else "WR_STARTER",
                 "available_at": "2026-10-05T21:00:00Z", "kickoff": "2026-10-06T00:15:00Z", "observation_ids": [p["identity"]]}
                for p in players]
    history = [{"game_id": f"old-{i}", "identity": p["identity"], "team": "NO", "field": field,
                "opportunities": (share + .1 if i < 3 else share) * 30, "team_budget": 30,
                "inactive_ids": ["rb1"] if i < 3 else [], "available_at": "2026-10-04T00:00:00Z",
                "absence_coverage_complete":True,"source_snapshot_id":f'history-{i}'}
               for p in players if p["identity"] != "rb1" for field, share in
               ((field, v["share"]) for field, v in p["components"].items() if field != "attempts") for i in range(6)]
    return forecasts, evidence, history, DECISION


def test_absence_changes_work_not_efficiency_and_keeps_unallocated_budget():
    args = inputs()
    original = deepcopy(args)
    result = condition_workloads(*args)
    assert args == original
    assert result["production_changed"] is False
    assert result["probability"] is None
    players = {p["identity"]: p for p in result["forecasts"][0]["players"]}
    assert players["rb1"]["components"] == {}
    assert players["rb2"]["components"]["carries"]["share"] > .2
    assert players["wr1"]["components"]["targets"]["share"] > .3
    assert all(p["efficiency"] == .7 for p in players.values())
    for audit in result["audit"]:
        assert audit["allocated_share"] <= audit["baseline_allocated_share"]
        assert audit["allocated_share"] + audit["unallocated_share"] == pytest.approx(1)
    assert result["forecasts"][0]["budgets"]["carries"]["mean"] == 24


@pytest.mark.parametrize("change", ["future", "late_kickoff", "wrong_team", "missing_provenance", "stale", "duplicate"])
def test_bad_current_evidence_fails_closed(change):
    forecasts, evidence, history, at = inputs()
    if change == "future": evidence[0]["available_at"] = "2026-10-05T23:00:00Z"
    if change == "late_kickoff": evidence[0]["kickoff"] = at
    if change == "wrong_team": evidence[0]["team"] = "ATL"
    if change == "missing_provenance": evidence[0]["observation_ids"] = []
    if change == "stale": evidence[0]["available_at"] = "2026-10-01T23:00:00Z"
    if change == "duplicate": evidence.append(deepcopy(evidence[0]))
    with pytest.raises(ValueError): condition_workloads(forecasts,evidence,history,at)


@pytest.mark.parametrize("change", ["future", "target_game", "duplicate", "missing_coverage", "over_budget", "nan"])
def test_invalid_history_is_not_silently_dropped(change):
    forecasts, evidence, history, at = inputs()
    if change == "future": history[0]["available_at"] = "2026-10-06T00:00:00Z"
    if change == "target_game": history[0]["game_id"] = "target"
    if change == "duplicate": history.append(deepcopy(history[0]))
    if change == "missing_coverage": del history[0]["inactive_ids"]
    if change == "over_budget": history[0]["opportunities"] = 40
    if change == "nan": history[0]["opportunities"] = float("nan")
    with pytest.raises(ValueError): condition_workloads(forecasts,evidence,history,at)


def test_unresolved_role_or_coverage_withholds_transfers():
    forecasts, evidence, history, at = inputs()
    evidence[2]["expected_role"] = "RB_DEPTH"
    result = condition_workloads(forecasts,evidence,history,at)
    assert result["forecasts"][0]["players"][2]["components"]["carries"]["share"] == .2
    evidence[2]["state"] = "UNKNOWN"
    result = condition_workloads(forecasts,evidence,history,at)
    assert all(not a["role_coverage_complete"] for a in result["audit"])
    assert result["forecasts"][0]["players"][3]["components"]["targets"]["share"] == .3


def test_simultaneous_absence_requires_matching_joint_history_not_sum_of_boosts():
    forecasts, evidence, history, at = inputs()
    evidence[3]["state"] = "OUT_CONFIRMED"
    result = condition_workloads(forecasts,evidence,history,at)
    assert result["forecasts"][0]["players"][4]["components"]["targets"]["share"] == .2


def test_questionable_states_are_unweighted_sensitivities_and_do_not_clear_official_out():
    forecasts, evidence, history, at = inputs()
    evidence[2]["state"] = "QUESTIONABLE"
    evidence[3]["state"] = "DOUBTFUL"
    result = workload_scenarios(forecasts,evidence,history,at)
    assert len(result) == 7
    assert all(s["probability"] is None and s["authority"] == "shadow_only" for s in result)
    assert all(s["forecasts"][0]["players"][1]["components"] == {} for s in result)
    inactive = next(s for s in result if s["id"] == "rb2:inactive")
    assert inactive["forecasts"][0]["players"][2]["components"] == {}
    limited = next(s for s in result if s["id"] == "rb2:limited")
    normal = result[0]["forecasts"][0]["players"][2]["components"]["carries"]["share"]
    assert limited["forecasts"][0]["players"][2]["components"]["carries"]["share"] == pytest.approx(normal/2)


def test_bad_factors_and_overallocated_baseline_rejected():
    args=inputs()
    with pytest.raises(ValueError): condition_workloads(*args,workload_factors={"rb2":1.1})
    with pytest.raises(ValueError): condition_workloads(*args,workload_factors={"foreign":.5})
    args[0][0]["players"][2]["components"]["carries"]["share"] = .9
    with pytest.raises(ValueError): condition_workloads(*args)
    args=inputs();args[2][0]['absence_coverage_complete']=False
    with pytest.raises(ValueError,match='coverage'):condition_workloads(*args)


def test_rb_share_normalization_does_not_transfer_qb_rushing():
    forecasts,evidence,history,at=inputs()
    template=next(r for r in history if r['identity']=='rb2' and r['field']=='carries')
    history=[r for r in history if not (r['identity']=='rb2' and r['field']=='carries')]
    history.extend({**template,'game_id':f'large-{i}','opportunities':30,'inactive_ids':['rb1']} for i in range(100))
    result=condition_workloads(forecasts,evidence,history,at)
    assert result['forecasts'][0]['players'][0]['components']['carries']['share']==.1
    assert next(a for a in result['audit'] if a['field']=='carries')['normalization']<1


def test_learned_limited_state_uses_matching_pregame_risk_inputs_without_promoting_weights():
    from research.nfl_availability_scenarios import build_report
    from tests.test_nfl_workload_risk import row,current
    forecasts,evidence,history,at=inputs()
    evidence[2]['state']='QUESTIONABLE'
    case={**current(),'player_id':'rb2','game_id':'target','decision_at':at,'features_available_at':'2026-10-05T21:00:00Z'}
    payload={'forecasts':forecasts,'evidence':evidence,'history':history,'decision_at':at,
             'workload_risk_input':{'cases':[row(i) for i in range(60)],'as_of':at,'forecast_cases':[case]}}
    report=build_report(payload)
    normal=report['scenarios'][0]['forecasts'][0]['players'][2]['components']['carries']['share']
    limited=next(s for s in report['scenarios'] if s['id']=='rb2:limited')
    assert limited['forecasts'][0]['players'][2]['components']['carries']['share']==pytest.approx(normal*.4)
    assert limited['limited_factor_source']=='learned_shadow'
    assert limited['probability'] is None
    assert report['optimizer_enabled'] is False
    payload['workload_risk_input']['forecast_cases'][0]['decision_at']='2026-10-05T23:00:00Z'
    with pytest.raises(ValueError,match='cutoffs'):build_report(payload)


def test_conditional_coherent_banks_preserve_shared_events_and_separate_streams():
    from tests.test_nfl_matchup_scenarios import inputs as coherent_inputs
    from research.nfl_availability_scenarios import build_report
    coherent=coherent_inputs()
    evidence=[{'game_id':f['game_id'],'team':f['team'],'identity':p['identity'],
               'state':'QUESTIONABLE' if p['position']=='WR' and f['team']=='AAA' else 'EXPECTED_ACTIVE',
               'expected_role':'WR_STARTER' if p['position']=='WR' else 'QB1',
               'available_at':'2026-09-27T13:00:00Z','kickoff':'2026-09-27T17:00:00Z','observation_ids':[p['identity']]}
              for f in coherent['forecasts'] for p in f['players']]
    payload={'forecasts':coherent['forecasts'],'evidence':evidence,'history':[],
             'decision_at':coherent['decision_at'],'coherent_input':coherent}
    before=deepcopy(payload)
    result=build_report(payload)
    assert payload==before
    assert len(result['scenarios'])==3
    for scenario in result['scenarios']:
        banks=scenario['joint_banks']
        assert banks['selection']['seed']!=banks['evaluation']['seed']
        assert not {s['id'] for s in banks['selection']['scenarios']} & {s['id'] for s in banks['evaluation']['scenarios']}
        for draw,games in zip(banks['selection']['scenarios'],banks['diagnostics'][0]['event_ledgers']):
            own,opp=games[0]['teams']
            assert draw['stats']['102']['pointsAllowed']==opp['final_points']-6*opp['defensive_tds']-2*opp['safeties']-2*opp['two_point_returns']
            assert own['passing_yards']==sum(p.get('receiving_yards',0) for p in own['players'].values())+own['unallocated']['receiving_yards']
        assert scenario['probability'] is None
