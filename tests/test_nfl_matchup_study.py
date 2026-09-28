from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path

from model.nfl_matchup_study import evaluate_study, validate_manifest

NOW = datetime(2026, 12, 2, tzinfo=timezone.utc)


def manifest(kind="dfs_mean"):
    source = json.loads(Path("docs/nfl-matchup-studies.json").read_text())["studies"]
    m = deepcopy(next(s for s in source if s["kind"] == kind and s["components"] == ["pressure"]))
    m.update(status="registered", registered_at="2026-09-27T15:00:00+00:00",
             holdout_start_at="2026-10-01T00:00:00+00:00",
             baseline_config_hash="a"*64, candidate_config_hash="b"*64, bootstrap_draws=2000)
    return m


def rows(m, weeks=8):
    out = []
    for week in range(weeks):
        for player in range(30):
            kick = datetime(2026, 10, 4, 17, tzinfo=timezone.utc)+timedelta(weeks=week)
            baseline = {"mean": 10., "median": 11., "p10": 0., "p90": 20., "boom_probability": .1}
            candidate = {**baseline, "mean": 12.}
            if m["kind"] == "pickem":
                baseline = {"probabilities": {"home": .49, "away": .50, "tie": .01}}
                candidate = {"probabilities": {"home": .80, "away": .19, "tie": .01}}
            out.append({"study_id": m["study_id"], "forecast_id": f"{week}:{player}", "game_id": f"{week}:{player}",
                        "player_id": player, "season": 2026, "week": week+4, "position": "QB",
                        "captured_at": kick-timedelta(hours=1), "decision_cutoff": kick-timedelta(hours=2),
                        "available_at": kick-timedelta(hours=3), "kickoff": kick,
                        "input_manifest_hash": "c"*64, "baseline_config_hash": m["baseline_config_hash"],
                        "candidate_config_hash": m["candidate_config_hash"], "scoring_version": m["scoring_version"],
                        "baseline": baseline, "candidate": candidate, "covered": True,
                        "result_at": kick+timedelta(hours=5), "result_digest": "d"*64, "result_id": f"{week}:{player}",
                        "actual": "home" if m["kind"] == "pickem" else 12., "scoring_status": "exact"})
    return out


def evaluate(m, data, **kw):
    return evaluate_study(m, data, now=NOW, complete_weeks=[(2026, i) for i in range(4, 12)], **kw)


def test_registration_requires_real_hashes_and_prospective_boundaries():
    m = manifest()
    assert validate_manifest(m) == []
    m["registered_at"] = "2026-11-01T00:00:00+00:00"
    assert validate_manifest(m)
    m = manifest()
    m["candidate_config_hash"] = None
    assert evaluate(m, [])["verdict"] == "NO_VERDICT"


def test_mean_gate_and_sample_floor_do_not_promote_production():
    m = manifest()
    assert evaluate(m, rows(m))["verdict"] == "PASS"
    assert evaluate(m, rows(m, 7))["verdict"] == "NO_VERDICT"
    assert evaluate(m, rows(m))["production_promotion"] is False


def test_paired_dedup_after_lock_and_mutable_config_rejected():
    m = manifest()
    data = rows(m)
    data += [deepcopy(data[0]), {**data[1], "candidate_config_hash": "z"*64},
             {**data[2], "captured_at": data[2]["kickoff"]}]
    r = evaluate(m, data)
    assert r["n"] == 240 and sum(r["rejected"].values()) == 2


def test_missing_coverage_cannot_change_baseline_or_inflate_cohort():
    m = manifest()
    data = rows(m)
    data[0]["covered"] = False
    assert evaluate(m, data)["n"] == 239
    data[0]["candidate"] = deepcopy(data[0]["baseline"])
    r = evaluate(m, data)
    assert r["n"] == 240 and r["covered_n"] == 239


def test_pickem_three_class_ties_not_dropped():
    m = manifest("pickem")
    data = rows(m)
    data[0]["actual"] = "tie"
    r = evaluate(m, data)
    assert r["n"] == 240 and r["verdict"] == "PASS"
    data[0]["candidate"]["probabilities"] = {"home": .79, "away": .19, "tie": .02}
    assert evaluate(m, data)["n"] == 239


def test_fixed_endpoint_does_not_allow_optional_stopping():
    m = manifest()
    r = evaluate_study(m, rows(m), now=NOW-timedelta(days=5), complete_weeks=[(2026, i) for i in range(4, 12)])
    assert r["verdict"] == "NO_VERDICT" and "endpoint" in r["reason"]


def test_rollback_is_separate_and_detects_harm():
    m = manifest()
    m.update(promoted_at="2026-10-01T00:00:00+00:00", rollback_end_at="2026-12-01T00:00:00+00:00")
    data = rows(m)
    for row in data:
        row["candidate"]["mean"] = 5.
    assert evaluate(m, data, phase="rollback")["verdict"] == "ROLLBACK"


def test_pickem_adapter_ignores_ui_metadata_without_changing_any_probability():
    from research.nfl_matchup_study import probability_arm
    p = {"home":.54,"away":.45,"tie":.01}
    assert probability_arm(p) == probability_arm({**p,"homeConditional":.54/.99})


def test_implementation_change_requires_new_forward_pin():
    m = manifest()
    m.update(implementation_hashes={"model.py":"e"*64},implementation_pinned_at="2026-10-01T00:00:00+00:00")
    data = rows(m)
    assert evaluate(m,data)["frozen_rows"] == 0
    for row in data:
        row["implementation_hashes"] = m["implementation_hashes"]
    assert evaluate(m,data)["verdict"] == "PASS"


def test_manifest_cannot_weaken_registered_materiality_or_harm_margins():
    m = manifest("pickem")
    m["gates"]["brier_harm"] = .2
    assert "brier_harm cannot relax the specification" in validate_manifest(m)
    m = manifest()
    m["gates"]["primary_materiality"] = -.5
    assert "primary materiality cannot relax the specification" in validate_manifest(m)


def test_implementation_pin_survives_checkout_newlines_but_not_code_changes():
    from model.nfl_matchup_study import implementation_digest
    lf = b"def project():\n    return 1\n"
    assert implementation_digest(lf) == implementation_digest(lf.replace(b"\n",b"\r\n"))
    assert implementation_digest(lf) != implementation_digest(lf.replace(b"return 1",b"return 2"))


def test_distribution_gate_requires_final_distribution_improvement_not_a_mean_claim():
    m = manifest()
    m.update(kind="dfs_distribution")
    m["gates"].update(primary_metric="weighted_interval_score",primary_materiality=.01)
    data=rows(m)
    for row in data:
        row["baseline"].update(mean=12.,median=12.,p10=-10.,p90=34.)
        row["candidate"].update(mean=12.,median=12.,p10=11.,p90=13.)
    result=evaluate(m,data)
    assert result["verdict"] == "PASS"
    assert result["metrics"]["mae"]["delta"] == 0


def test_imprecise_guardrail_is_no_verdict_without_relaxing_pass_rule():
    m = manifest()
    data = rows(m)
    for row in data:
        row["candidate"]["p90"] = 13. if row["week"] % 2 else 31.
    result = evaluate(m, data)
    assert result["metrics"]["primary"]["ci"][1] < 0
    assert result["metrics"]["p90_pinball"]["ci"][0] < .05 < result["metrics"]["p90_pinball"]["ci"][1]
    assert result["verdict"] == "NO_VERDICT"


def test_grade_records_exact_forecast_and_outcome_revision_population():
    m = manifest()
    data = rows(m)
    first = evaluate(m, data)
    assert len(first["forecast_evidence"]) == 240
    assert first["forecast_evidence"][0]["forecast_id"] == data[0]["forecast_id"]
    assert first["forecast_evidence"][0]["result_digest"] == "d"*64
    assert all(row["scored_in_report"] for row in first["forecast_evidence"])
    data[0]["result_id"] = "corrected-revision"
    data[0]["result_digest"] = "e"*64
    assert evaluate(m, data)["population_digest"] != first["population_digest"]


def test_outcome_amendment_is_forward_only_and_has_an_exact_scorer_pin(tmp_path):
    from model.nfl_matchup_study import digest, implementation_digest
    from research.nfl_matchup_study import outcome_implementation_errors, resolve_registration
    original = manifest()
    original["scoring_version"] = "nfl-dk-realized-v2"
    source = tmp_path / "scorer.py"
    source.write_bytes(b"SCORING_VERSION='v3'\n")
    registration = tmp_path / "registered.json"
    registration.write_text(json.dumps(original))
    amended_at = "2026-10-02T00:00:00+00:00"
    amendment = {"registration_manifest_hash": digest(original), "registered_at": amended_at,
                 "previous_scoring_version": original["scoring_version"], "scoring_version": "nfl-dk-realized-v3",
                 "previous_amendment_hash": None, "hash_algorithm": "sha256_lf_normalized",
                 "implementation_hashes": {str(source): implementation_digest(source.read_bytes())}}
    amendment_path = tmp_path / "outcome-amendment.json"
    amendment_path.write_text(json.dumps(amendment))
    index = {"registration_file": str(registration), "outcome_amendment_files": [str(amendment_path)]}
    result = resolve_registration(index)
    assert result["registered_at"] == result["holdout_start_at"] == amended_at
    assert result["scoring_version"] == "nfl-dk-realized-v3"
    assert original["scoring_version"] == "nfl-dk-realized-v2"
    assert outcome_implementation_errors(result) == []
    first_pin = {"registration_manifest_hash": digest(original), "pinned_at": "2026-10-01T00:00:00+00:00",
                 "hashes": {"forecast.py": "a"*64}}
    later_pin = {**first_pin, "pinned_at": "2026-10-03T00:00:00+00:00",
                 "previous_pin_hash": digest(first_pin), "hashes": {"forecast.py": "b"*64}}
    pin_paths = [tmp_path / "first-pin.json", tmp_path / "later-pin.json"]
    for pin_path, pin in zip(pin_paths, [first_pin, later_pin]):
        pin_path.write_text(json.dumps(pin))
    index["implementation_pin_files"] = [str(p) for p in pin_paths]
    result = resolve_registration(index)
    assert result["scoring_version"] == "nfl-dk-realized-v3"
    assert result["implementation_pinned_at"] == later_pin["pinned_at"]
    assert result["registered_at"] == amended_at
    assert result["implementation_hashes"] == later_pin["hashes"]
    source.write_bytes(b"SCORING_VERSION='v3'\nchanged=True\n")
    assert outcome_implementation_errors(result) == [str(source)]
    amendment["registered_at"] = "2026-09-01T00:00:00+00:00"
    amendment_path.write_text(json.dumps(amendment))
    import pytest
    with pytest.raises(ValueError, match="backdated"):
        resolve_registration(index)


def test_prospective_adapter_reports_foreign_envelopes_without_grading_them(monkeypatch):
    from model.nfl_matchup_study import digest
    from research import nfl_matchup_study as adapter
    m = manifest()
    m["baseline_config_hash"] = digest({"model_version": "nfl-dfs-historical-v5", "model_config": {}})
    monkeypatch.setattr(adapter, "resolve_registration", lambda entry: deepcopy(entry))
    row = rows(m)[0]
    distribution = {"mean": 10., "p50": 11., "p10": 0., "p90": 20., "boom": .1}
    valid = {"forecast_model_version": "nfl-matchup-shadow-v1", "player_id": "qb", "game_id": row["game_id"],
        "season": row["season"], "week": row["week"], "kickoff": row["kickoff"], "run_id": "pfr-run",
        "created_at": row["captured_at"], "as_of_at": row["decision_cutoff"], "baseline_created_at": row["available_at"],
        "baseline_version": "nfl-dfs-historical-v5", "baseline_config": {},
        "manifest": {"model_hashes": {"pressure": m["candidate_config_hash"]}},
        "projection": {"position": "QB", "baseline": {"id": "saved-qb"}, "shadow": {
            "status": "under_evaluation", "baseline": distribution, "candidate": dict(distribution),
            "matchup_manifest_hash": "c"*64}}}
    foreign = {"forecast_model_version": "nfl-allowed-rushing-volume-v1", "run_id": "allowed-run",
               "projection": {"baseline": {"position": "RB"}, "shadow": {}}, "manifest": {}}
    assert "position" not in foreign["projection"]
    class Database:
        def __init__(self):
            self.calls = 0
        def execute(self, sql, params):
            self.calls += 1
            if self.calls == 1:
                assert "f.model_version forecast_model_version" in sql
                return [foreign, valid]
            return []
    report = adapter.prospective_reports(Database(), 2026, NOW, {"studies": [m]})
    assert report["source_forecast_rows"] == 2
    assert report["supported_forecast_rows"] == 1
    assert report["ignored_foreign_forecast_rows"] == 1
    assert report["ignored_foreign_model_versions"] == {"nfl-allowed-rushing-volume-v1": 1}
    assert report["studies"][0]["frozen_rows"] == 1
    assert report["studies"][0]["rejected"] == {"unscored": 1}
    assert report["studies"][0]["registration_errors"] == []
