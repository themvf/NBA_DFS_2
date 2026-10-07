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


# --- Registered capture scope (enforced from the capture's recorded format) ---

CLASSIC_SCOPE = {"consumer": "nfl_dfs_projection", "usage": "predictive_shadow_only",
                 "platform": "DraftKings", "capture": "saved_current_week_Classic_salary_pool"}


def test_a_showdown_capture_never_enters_a_classic_scoped_study():
    """Since #295 Showdown captures share the Classic tables. A later
    Showdown capture of the same player/game must not displace the Classic
    one, and Showdown-only rows must not be graded at all."""
    m = manifest()
    m["qualification_scope"] = CLASSIC_SCOPE
    classic = rows(m)
    for row in classic:
        row["capture_format"] = "classic"
    assert evaluate(m, classic)["verdict"] == "PASS"
    showdown = deepcopy(classic)
    for row in showdown:
        row.update(capture_format="showdown", forecast_id="sd:" + row["forecast_id"],
                   captured_at=row["captured_at"] + timedelta(minutes=30))
        row["candidate"]["mean"] = 5.  # would flip the verdict if it were pooled
    mixed = evaluate(m, classic + showdown)
    assert mixed["verdict"] == "PASS"
    assert mixed["frozen_rows"] == len(classic)
    assert mixed["rejected"]["capture outside the registered slate format"] == len(showdown)
    assert mixed["registered_capture_format"] == "classic"
    only_showdown = evaluate(m, showdown)
    assert only_showdown["frozen_rows"] == 0 and only_showdown["verdict"] == "NO_VERDICT"


def test_a_capture_with_no_recorded_format_is_not_assumed_classic():
    m = manifest()
    m["qualification_scope"] = CLASSIC_SCOPE
    result = evaluate(m, rows(m))
    assert result["frozen_rows"] == 0
    assert result["rejected"]["capture outside the registered slate format"] == 240


def test_an_unrecognized_capture_scope_is_refused_not_guessed():
    m = manifest()
    m["qualification_scope"] = {**CLASSIC_SCOPE, "capture": "saved_current_week_any_salary_pool"}
    data = rows(m)
    for row in data:
        row["capture_format"] = "classic"
    result = evaluate(m, data)
    assert result["frozen_rows"] == 0
    assert result["rejected"] == {"unrecognized registered capture scope: saved_current_week_any_salary_pool": 240}


def test_a_scope_that_is_not_a_salary_slate_imposes_no_format():
    m = manifest("pickem")
    m["qualification_scope"] = {"consumer": "nfl_pickem_probability", "usage": "predictive_shadow_only",
                                "capture": "eligible_upcoming_regular_season_games"}
    assert evaluate(m, rows(m))["verdict"] == "PASS"


def test_the_prospective_adapter_carries_the_capture_format(monkeypatch):
    from model.nfl_matchup_study import digest
    from research import nfl_matchup_study as adapter
    m = manifest()
    m["baseline_config_hash"] = digest({"model_version": "nfl-dfs-historical-v5", "model_config": {}})
    m["qualification_scope"] = CLASSIC_SCOPE
    monkeypatch.setattr(adapter, "resolve_registration", lambda entry: deepcopy(entry))
    row = rows(m)[0]
    distribution = {"mean": 10., "p50": 11., "p10": 0., "p90": 20., "boom": .1}
    def record(fmt, run_id, created):
        return {"forecast_model_version": "nfl-matchup-shadow-v1", "player_id": "qb", "game_id": row["game_id"],
            "season": row["season"], "week": row["week"], "kickoff": row["kickoff"], "run_id": run_id,
            "created_at": created, "as_of_at": row["decision_cutoff"], "baseline_created_at": row["available_at"],
            "baseline_version": "nfl-dfs-historical-v5", "baseline_config": {},
            "manifest": {"model_hashes": {"pressure": m["candidate_config_hash"]}, "format": fmt},
            "projection": {"position": "QB", "baseline": {"id": "saved-qb"}, "shadow": {
                "status": "under_evaluation", "baseline": distribution, "candidate": dict(distribution),
                "matchup_manifest_hash": "c"*64}}}
    class Database:
        def __init__(self):
            self.calls = 0
        def execute(self, sql, params):
            self.calls += 1
            if self.calls == 1:
                assert "'format',f.manifest->'format'" in sql
                return [record("classic", "classic-run", row["captured_at"]),
                        record("showdown", "showdown-run", row["captured_at"] + timedelta(minutes=5))]
            return []
    study = adapter.prospective_reports(Database(), 2026, NOW, {"studies": [m]})["studies"][0]
    assert study["frozen_rows"] == 1
    assert study["rejected"] == {"capture outside the registered slate format": 1, "unscored": 1}
    assert [f["forecast_id"] for f in study["forecast_evidence"]] == ["classic-run:qb"]


# --- The checked-in registrations agree with the checked-out code ------------

def _registry():
    return json.loads(Path("docs/nfl-matchup-studies.json").read_text())["studies"]


def test_dfs_captures_record_exactly_the_files_their_latest_pin_binds():
    """Pin 4 bound nine files while captures recorded six, so no capture could
    ever match (n=0). Captures and the latest pin must name the same set, at
    the checked-out hashes."""
    from research.nfl_matchup_implementation import IMPLEMENTATION_FILES, implementation_hashes
    from research.nfl_matchup_study import resolve_registration
    dfs = [s for s in _registry() if s.get("status") == "registered" and s["kind"] == "dfs_mean"]
    assert len(dfs) == 3
    assert "research/nfl_saved_upload_selection.py" in IMPLEMENTATION_FILES
    for entry in dfs:
        resolved = resolve_registration(entry)
        assert set(resolved["implementation_hashes"]) == set(IMPLEMENTATION_FILES), entry["study_id"]
        assert resolved["implementation_hashes"] == implementation_hashes(), entry["study_id"]
        assert resolved["qualification_scope"]["capture"] == "saved_current_week_Classic_salary_pool"


def test_dfs_studies_grade_the_current_realized_version_with_its_pinned_scorer():
    from ingest.nfl_dfs_results import SCORING_VERSION
    from model.nfl_matchup_study import validate_manifest
    from research.nfl_matchup_study import outcome_implementation_errors, resolve_registration
    for entry in (s for s in _registry() if s.get("status") == "registered" and s["kind"] == "dfs_mean"):
        resolved = resolve_registration(entry)
        assert resolved["scoring_version"] == entry["scoring_version"] == SCORING_VERSION
        assert outcome_implementation_errors(resolved) == []
        assert validate_manifest(resolved) == []
        # The outcome amendment restarts the holdout; the pin gates captures.
        assert resolved["holdout_start_at"] == resolved["registered_at"]
        assert resolved["implementation_pinned_at"] <= resolved["holdout_start_at"]


def test_pickem_freezes_pass_their_latest_implementation_pin():
    """freeze-pickem refuses to capture when the pinned files drift; every
    registered pickem artifact must resolve against the checked-out code."""
    from datetime import datetime, timezone
    from research import nfl_pickem_matchup as pickem
    model = json.loads(pickem.MODEL_PATH.read_text())
    families = [json.loads(p.read_text(encoding="utf-8")) for p in pickem.FAMILY_MODEL_PATHS.values() if p.exists()]
    resolved = [pickem.registered_model(m, datetime.now(timezone.utc), True) for m in [model, *families]]
    assert {r["study_id"] for r in resolved} == {
        "nfl-matchup-pickem-combined-v1", "nfl-matchup-pickem-pressure-v2", "nfl-matchup-pickem-contact-v2"}


def test_context_compatibility_declares_the_current_realized_version():
    from ingest.nfl_dfs_results import SCORING_VERSION
    from research.nfl_matchup_study import outcome_implementation_errors
    compat = json.loads(Path("research/nfl_dfs_context_scoring_compatibility.json").read_text())
    rule = compat["versions"][SCORING_VERSION]
    assert rule["positions"] == ["QB", "RB", "WR", "TE"], "DST stays excluded"
    assert outcome_implementation_errors({"outcome_implementation_hashes": rule["implementation_hashes"]}) == []
    assert all(proof["equal_ast"] for proof in rule["equivalence_proof"].values())
    assert rule["scoring_fields_equal_ast"] is True
    # The skill-position scorer is the one the v3 declaration proved.
    assert rule["equivalence_proof"]["ingest/nfl_dfs_results.py:score_source_row"]["ast_sha256"] == \
        compat["equivalence_proof"]["ingest/nfl_dfs_results.py:score_source_row"]["ast_sha256"]
    for earlier in ("nfl-dk-realized-v2", "nfl-dk-realized-v3"):
        assert earlier in compat["versions"], "earlier declarations are kept"


def test_pickem_input_digest_is_stored_once_and_matches_inline_digest():
    """Grading reads a stored digest; it must equal digest(payload.input) exactly."""
    from model.nfl_matchup_study import digest
    from research import nfl_matchup_study as adapter
    inputs = {f"f{i}": {"gameId": f"g{i}", "baseline": {"homeConditional": .5 + i / 100}} for i in range(5)}
    stored = {}

    class Cursor:
        def __enter__(self): return self
        def __exit__(self, *_): return False
        def execute(self, sql, params):
            assert sql.startswith("UPDATE nfl_pickem_matchup_forecasts SET input_digest")
            stored.setdefault(params[1], params[0])

    class Connection:
        def __enter__(self): return self
        def __exit__(self, *_): return False
        def cursor(self): return Cursor()

    class Database:
        def execute(self, sql, params=None):
            if sql.startswith("ALTER"):
                return []
            pending = [k for k in sorted(inputs) if k not in stored][:params[0]]
            return [{"forecast_id": k, "input": inputs[k]} for k in pending]
        def connect(self): return Connection()

    assert adapter.fill_pickem_input_digests(Database(), batch=2) == 5
    assert stored == {k: digest(v) for k, v in inputs.items()}
    assert adapter.fill_pickem_input_digests(Database(), batch=2) == 0, "a filled row is never read again"
