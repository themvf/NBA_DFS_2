from copy import deepcopy
from datetime import datetime, timedelta, timezone

from model.nfl_coherent_study import distribution_metrics, grade_coherent
from model.nfl_matchup_study import digest

NOW=datetime(2026,12,2,tzinfo=timezone.utc)


def manifest():
    return {"study_id":"coherent-test","registered_at":"2026-09-27T15:00:00+00:00",
        "evaluation_end_at":"2026-12-01T00:00:00+00:00","positions":["QB","RB"],
        "seed":20260927,"bootstrap_draws":2000,"draws":300,"scoring_version":"nfl-dk-realized-v2",
        "implementation_hashes":{"model.py":"a"*64},"model_version":"coherent-v2",
        "forward_start":{"season":2026,"first_full_week":4},"boom_thresholds":{"QB":30,"RB":25},"baseline_config_hash":"f"*64}


def records(m,weeks=8):
    rows=[]
    for week in range(4,4+weeks):
        kickoff=datetime(2026,10,4,17,tzinfo=timezone.utc)+timedelta(weeks=week-4)
        for position in m["positions"]:
            for player in range(30):
                rows.append({"forecast_id":f"{week}:{position}:{player}","player_id":f"{position}:{player}",
                    "game_id":f"{week}:{player}","season":2026,"week":week,"position":position,"kickoff":kickoff,
                    "captured_at":kickoff-timedelta(hours=2),"available_at":kickoff-timedelta(hours=3),
                    "published_at":kickoff-timedelta(hours=1),"model_version":m["model_version"],"registration_hash":digest(m),
                    "implementation_hashes":m["implementation_hashes"],"draws":300,"scoring_version":m["scoring_version"],"baseline_config_hash":m["baseline_config_hash"],
                    "input_manifest_hash":"b"*64,"baseline_run_id":"run","baseline_reproduced":True,
                    "baseline":{"mean":10,"p10":0,"p25":5,"median":10,"p75":15,"p90":20,"boom_probability":0,"boom_threshold":m["boom_thresholds"][position]},
                    "candidate":{"mean":12,"p10":10,"p25":11,"median":12,"p75":13,"p90":14,"boom_probability":0,"boom_threshold":m["boom_thresholds"][position]},
                    "actual":12,"scoring_status":"exact","result_id":f"result:{week}:{position}:{player}","result_digest":"c"*64,
                    "result_at":kickoff+timedelta(hours=4)})
    return rows


def grade(m,rows,now=NOW):
    return grade_coherent(m,rows,now=now,complete_weeks=[(2026,w) for w in range(4,12)])


def test_standard_wis_uses_both_frozen_intervals():
    sample=records(manifest())[0]
    assert distribution_metrics(sample["baseline"],12,30)["wis"] == 2.2
    assert distribution_metrics(sample["candidate"],12,30)["wis"] == .36


def test_holm_position_gate_passes_only_after_fixed_endpoint_and_floors():
    m=manifest()
    result=grade(m,records(m))
    assert result["verdict"] == "PASS" and result["production_promotion"] is False
    assert all(p["holm_reject"] for p in result["positions"].values())
    assert grade(m,records(m,7))["verdict"] == "NO_VERDICT"
    assert grade(m,records(m),NOW-timedelta(days=3))["verdict"] == "NO_VERDICT"


def test_missing_quantiles_are_not_interpolated_or_replaced_by_old_rows():
    m=manifest();rows=records(m)
    latest=deepcopy(rows[0]); latest["captured_at"]+=timedelta(minutes=1)
    latest["baseline_reproduced"]=False; latest["baseline"]=None
    result=grade(m,rows+[latest])
    assert result["frozen_rows"]==480 and result["scored_rows"]==479
    assert result["rejected"]["exact baseline distribution was not reproduced"]==1
    rows[1]["baseline"].pop("p25")
    assert grade(m,rows)["rejected"]["missing exact paired quantiles or mean/boom forecast"]==1


def test_postlock_retrospective_code_changes_and_harm_never_pass():
    m=manifest();rows=records(m)
    rows[0]["published_at"]=rows[0]["kickoff"]
    rows[1]["retrospective"]=True
    rows[2]["implementation_hashes"]={"other":"d"*64}
    result=grade(m,rows)
    assert result["frozen_rows"]==477
    for row in rows:
        if row["position"]=="RB":
            row["candidate"]["mean"]=2
    assert grade(m,rows)["verdict"]=="FAIL"


def test_coherent_format_cohorts_are_independent_and_cannot_share_a_pass():
    m=manifest();m["cohorts"]={"classic":{"positions":["QB","RB"]},"showdown":{"positions":["QB","RB","K"]}}
    rows=records(m)
    for row in rows:
        row["format"]="classic"
    complete=[(2026,w) for w in range(4,12)]
    assert grade_coherent(m,rows,now=NOW,complete_weeks=complete,cohort="classic")["verdict"]=="PASS"
    assert grade_coherent(m,rows,now=NOW,complete_weeks=complete,cohort="showdown")["verdict"]=="NO_VERDICT"
    rows[0]["baseline_config_hash"]="changed"
    rows[1]["input_manifest_hash"]="not-a-hash"
    result=grade_coherent(m,rows,now=NOW,complete_weeks=complete,cohort="classic")
    assert result["frozen_rows"]==478


def test_scenario_and_exact_outcome_scoring_contracts_are_explicit():
    m=manifest();m.update(scoring_version="nfl-dk-scenario-v1",outcome_scoring_version="nfl-dk-realized-v2")
    rows=records(m)
    for row in rows:
        row["scoring_version"]="nfl-dk-realized-v2"
    assert grade(m,rows)["verdict"]=="PASS"
    rows[0]["scoring_version"]="unregistered-scoring"
    assert grade(m,rows)["frozen_rows"]==479


def test_current_coherent_registration_matches_code_and_preserves_prior_contract():
    import json
    from pathlib import Path
    from hashlib import sha256
    from model.nfl_matchup_scenarios import VERSION
    from research.nfl_coherent_study import registered_manifests

    current_path = Path("research/nfl_coherent_scenario_study.json")
    current = json.loads(current_path.read_text())
    previous_path = Path("research/nfl_coherent_scenario_study_v3.json")
    previous = json.loads(previous_path.read_text())
    assert current["model_version"] == VERSION == "nfl-coherent-matchup-research-v4"
    assert current["previous_registration_sha256_lf"] == sha256(previous_path.read_bytes().replace(b"\r\n", b"\n")).hexdigest()
    assert current_path.read_bytes() == Path("research/nfl_coherent_scenario_study_v4.json").read_bytes()
    assert current["registered_at"] > previous["registered_at"]
    for name, expected in current["implementation_hashes"].items():
        assert sha256(Path(name).read_bytes().replace(b"\r\n", b"\n")).hexdigest() == expected, name
    for key in ("baseline_config_hash", "forecast_gate", "portfolio_gate", "forward_start",
                "evaluation_end_at", "scoring_version", "outcome_scoring_version", "seed",
                "draws", "bootstrap_draws", "cohorts", "production_authority", "protected_studies"):
        assert current[key] == previous[key], key
    manifests = registered_manifests()
    assert any(digest(m) == digest(previous) for m in manifests)
    assert any(digest(m) == digest(current) for m in manifests)


def test_forward_registration_never_inherits_previous_version_rows():
    old = manifest()
    new = deepcopy(old)
    new.update(study_id="new-cohort", model_version="coherent-new", registered_at="2026-09-28T12:00:00+00:00")
    historical = records(old)
    result = grade(new, historical)
    assert result["frozen_rows"] == 0
    assert result["rejected"]["different coherent registration/model"] == len(historical)
    assert result["verdict"] == "NO_VERDICT"
    assert grade(old, historical)["verdict"] == "PASS"
