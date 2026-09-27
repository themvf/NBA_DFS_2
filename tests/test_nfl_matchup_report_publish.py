from copy import deepcopy
import pytest
from model.nfl_context_engine import stable_digest
from research.nfl_matchup_report_publish import build_report, persist_report

BASE="00000000-0000-0000-0000-000000000001"
UPLOAD="00000000-0000-0000-0000-000000000002"

def inputs():
    projection={"baseline_run_id":BASE,"upload_id":UPLOAD,"production_changed":False,
        "as_of_at":"2026-09-27T14:55:00Z","baseline_version":"v5","salary_rows":1,
        "players":[{"name":"Runner","position":"RB","team":"TB","salary":5000,
        "baseline":{"model_proj_fpts":10,"large_history":[1]*1000},
        "shadow":{"candidate":{"mean":11},"delta":1,"status":"under_evaluation","ledger":[]}}]}
    coherent={"productionChanged":False,"audit":{"productionRunId":BASE,"uploadId":UPLOAD},
        "manifest":{"version":"coherent-v1","sources":{"comparison_digest":stable_digest(projection),"comparison_file_sha256":"raw-hash"}}}
    portfolio={"productionChanged":False,"inputAudit":{"productionRunId":BASE},"uploadId":UPLOAD,"comparisonDigest":"raw-hash","evaluationModel":"coherent-v1"}
    return projection,coherent,portfolio

def test_exact_comparison_required_even_for_same_baseline_and_upload():
    p,c,f=inputs();p["as_of_at"]="2026-09-27T14:56:00Z"
    with pytest.raises(ValueError,match="exact comparison"):
        build_report(p,comparison_bytes_digest="raw-hash",coherent=c,portfolios=f)
    p,c,f=inputs();f["comparisonDigest"]="old-file"
    with pytest.raises(ValueError,match="exact comparison"):
        build_report(p,comparison_bytes_digest="raw-hash",coherent=c,portfolios=f)

def test_postlock_data_is_separate_and_large_raw_inputs_are_omitted():
    p,c,f=inputs()
    archive={"authority":"postlock_evaluation_only","forecast_inputs_allowed":False,
        "contests":[{"contest_id":"past","score_curve":[1]*100,"slot_actuals":[2]*100,"saved_portfolio_grades":[{"best_points":100}]}]}
    report=build_report(p,comparison_bytes_digest="raw-hash",coherent=c,portfolios=f,archived=archive)
    assert report["players"][0]["baseline"]==10
    assert "large_history" not in str(report)
    assert "score_curve" not in report["archived"]["contests"][0]
    assert report["archived"]["forecast_inputs_allowed"] is False
    assert report==build_report(p,comparison_bytes_digest="raw-hash",coherent=c,portfolios=f,archived=deepcopy(archive))
    archive["forecast_inputs_allowed"]=True
    with pytest.raises(ValueError,match="evaluation-only"):
        build_report(p,comparison_bytes_digest="raw-hash",archived=archive)

def test_optional_reports_are_explicit_missing_not_invented():
    p,_,_=inputs();r=build_report(p,comparison_bytes_digest="raw-hash")
    assert len(r["missing"])==2 and r["coherent"] is None and r["portfolios"] is None

def test_publication_after_kickoff_is_rejected_before_database_access():
    p,_,_=inputs();r=build_report(p,comparison_bytes_digest="raw-hash")
    r["firstKickoff"]="2020-01-01T00:00:00Z"
    with pytest.raises(ValueError,match="cutoff"):
        persist_report(None,r)

def test_same_comparison_cannot_mix_scenario_model_versions_or_seed_manifests():
    p,c,f=inputs();f["evaluationModel"]="coherent-v2"
    with pytest.raises(ValueError,match="versions differ"):
        build_report(p,comparison_bytes_digest="raw-hash",coherent=c,portfolios=f)
    f["evaluationModel"]="coherent-v1";f["scenarioManifest"]={**c["manifest"],"seed":999}
    with pytest.raises(ValueError,match="manifests differ"):
        build_report(p,comparison_bytes_digest="raw-hash",coherent=c,portfolios=f)
