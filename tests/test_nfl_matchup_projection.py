from copy import deepcopy

import pytest

from model.nfl_dfs_historical import HistoricalWeek, ProjectionContext, project_player, MODEL_CONFIG
from model.nfl_matchup_projection import sample_baseline_draws,summarize_draws,shadow_projection,fit_family,feature_vector


def fixture():
    history=[HistoricalWeek(1,"id","Test RB","RB",2025,w,"TB","MIN",{
        "carries":10+w,"rushing_yards":30+5*w,"rushing_tds":w%2,"receptions":3,"receiving_yards":25}) for w in range(1,10)]
    cfg={**MODEL_CONFIG,"draws":400}
    p=project_player(player_id=1,player_gsis_id="id",player_name="Test RB",position="RB",historical_rows=history,
                     cutoff_season=2026,cutoff_week=3,context=ProjectionContext(team_implied_total=25),seed=123,config=cfg).as_dict()
    p["team"]="TB"
    side={"pressure_games":2,"pressure_pct":25,"rb_carries":40,"rb_before_contact_per_carry":2.,"rb_after_contact_per_carry":3.,
          "pressure_coverage_complete":True,"contact_coverage_complete":True}
    m={"manifest_hash":"frozen","home":"TB","away":"MIN","teams":{t:{"offense":side,"defense":side} for t in ("TB","MIN")}}
    return p,history,cfg,m


def test_sampler_matches_protected_v5_draw_distribution():
    p,history,cfg,_=fixture()
    summary=summarize_draws("RB",sample_baseline_draws(p,history,cfg,123))
    assert round(summary["mean"],4)==p["model_proj_fpts"]
    assert round(summary["p90"],4)==p["ceiling_fpts"]
    assert {k:round(v,4) for k,v in summary["stat_means"].items()}==p["stat_means"]


def test_contact_changes_only_rushing_yards_and_never_active_projection():
    p,history,cfg,m=fixture();before=deepcopy(p)
    artifact={"status":"research_fitted","artifact_hash":"a","features":["own_ybc","opp_ybc","own_yac","opp_yac"],
              "center":[0,0,0,0],"scale":[1,1,1,1],"coefficients":[1,1,1,1]}
    s=shadow_projection(p,sample_baseline_draws(p,history,cfg,123),m,{"contact":artifact})
    assert s["status"]=="under_evaluation" and s["delta"]>0 and s["active_delta"]==0
    assert p==before
    assert s["ledger"][0]["factor"]==pytest.approx(1.1)
    for k,v in s["baseline"]["stat_means"].items():
        if k!="rushing_yards": assert s["candidate"]["stat_means"][k]==v
    assert sum(step["points_delta"] for step in s["ledger"])==pytest.approx(s["delta"])


def test_unreproduced_availability_or_missing_model_withholds_shadow():
    p,history,cfg,m=fixture();draws=sample_baseline_draws(p,history,cfg,123)
    assert shadow_projection(p,draws,m,{})["reason"]=="no_fitted_research_model"
    p["is_out"]=True
    assert shadow_projection(p,draws,m,{})["reason"]=="ineligible_or_out"


def test_out_fallback_retains_saved_zero_and_excludes_unreproduced_scores():
    p,history,cfg,m=fixture();draws=sample_baseline_draws(p,history,cfg,123)
    p.update(is_out=True, model_proj_fpts=0, floor_fpts=0, median_fpts=0, ceiling_fpts=0, boom_rate=0)
    s=shadow_projection(p,draws,m,{},include_scores=True)
    assert s["baseline"]["mean"]==s["candidate"]["mean"]==0
    assert not s["distribution_available"] and "scores" not in s


def test_equal_mean_with_different_tail_is_not_a_distribution_reproduction():
    p,history,cfg,m=fixture();draws=sample_baseline_draws(p,history,cfg,123)
    p["ceiling_fpts"]+=2
    artifact={"status":"research_fitted","artifact_hash":"a","features":["own_ybc","opp_ybc","own_yac","opp_yac"],
              "center":[0,0,0,0],"scale":[1,1,1,1],"coefficients":[1,1,1,1]}
    s=shadow_projection(p,draws,m,{"contact":artifact},include_scores=True)
    assert s["reason"]=="saved_baseline_not_reproduced_or_availability_adjusted"
    assert s["candidate"]["p90"]==p["ceiling_fpts"] and "scores" not in s


def test_reported_rates_without_proven_participant_completeness_are_withheld():
    _,_,_,m=fixture()
    assert feature_vector(m,"TB","contact") is not None
    m["teams"]["MIN"]["defense"]["contact_coverage_complete"]=False
    assert feature_vector(m,"TB","contact") is None
    m["teams"]["TB"]["offense"].pop("pressure_coverage_complete")
    assert feature_vector(m,"TB","pressure") is None


def test_training_is_deterministic_no_automatic_qualification():
    rows=[{"family":"pressure","own_pressure":i%40,"opp_pressure":i%35,"residual":(i%40)*-.02} for i in range(120)]
    a=fit_family("pressure",rows,{"availability":"retrospective_development_only"})
    assert a==fit_family("pressure",rows,{"availability":"retrospective_development_only"})
    assert a["production_qualified"] is False and a["forward_weeks"]==0
