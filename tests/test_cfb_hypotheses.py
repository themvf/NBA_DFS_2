from datetime import date

import pytest

from model.cfb_historical_signals import DEFINITION_VERSION, MARKET_BREAK_EVEN_110
from research.cfb_hypotheses import (
    HYPOTHESIS_DEFINITIONS,
    _require_evaluable,
    advance,
    allowed_transition,
    definition_drift,
    definition_for,
    is_evaluable,
    readiness_report,
    settle_prospective,
)


def test_hypothesis_state_machine_is_forward_only() -> None:
    assert allowed_transition("PROPOSED", "PREREGISTERED")
    assert allowed_transition("BACKTESTED", "HOLDOUT_PASSED")
    assert allowed_transition("PROSPECTIVE_SHADOW", "VALIDATED_SIGNAL")
    assert not allowed_transition("PREREGISTERED", "HOLDOUT_PASSED")
    assert not allowed_transition("VALIDATED_SIGNAL", "BACKTESTED")


def test_any_active_state_can_be_retired_but_retired_is_terminal() -> None:
    assert allowed_transition("BACKTESTED", "RETIRED")
    assert not allowed_transition("RETIRED", "RETIRED")


class _SettlementDb:
    def __init__(self) -> None:
        self.inserts = []

    def execute_one(self, query, params=None):
        return {"id": 7, "hypothesis_key": "CFB-H001", "version": "v1"}

    def execute(self, query, params=None):
        if "FROM cfb_game_signal_snapshots" in query:
            return [
                {"id": 1, "inputs_json": {"spread": -14.5}, "game_id": 10,
                 "game_date": date(2026, 9, 1), "home_score": 31, "away_score": 14,
                 "close_home_spread": -16.0, "close_quality": "A"},
                {"id": 2, "inputs_json": {"spread": -15.5}, "game_id": 11,
                 "game_date": date(2026, 9, 2), "home_score": 21, "away_score": 10,
                 "close_home_spread": None, "close_quality": None},
            ]
        self.inserts.append((query, params))
        return []


def test_prospective_settlement_grades_frozen_line_and_only_verified_clv() -> None:
    db = _SettlementDb()
    result = settle_prospective(db)
    assert result["summary"] == pytest.approx({
        "n": 2, "wins": 1, "losses": 1, "pushes": 0,
        "decision_rate": 0.5, "ci_low": 0.09453120573423074,
        "ci_high": 0.9054687942657693,
    })
    assert result["verified_clv"] == {"n": 1, "average_points": 1.5}
    assert len(db.inserts) == 1


class _AdvanceDb:
    def __init__(self, result) -> None:
        self.result = result
        self.updated = False

    def execute_one(self, query, params=None):
        if "SELECT * FROM cfb_hypotheses" in query:
            return {"id": 9, "status": "PROSPECTIVE_SHADOW",
                    "min_sample_json": {"prospective_n": 100}}
        return self.result

    def execute(self, query, params=None):
        self.updated = query.startswith("UPDATE cfb_hypotheses")


def test_validation_requires_sample_uncertainty_and_positive_verified_clv() -> None:
    db = _AdvanceDb({"n": 100, "ci_low": 0.54, "avg_clv": 0.25})
    advance(db, "CFB-H001", "v1", "VALIDATED_SIGNAL")
    assert db.updated

    with pytest.raises(ValueError, match="CLV"):
        advance(_AdvanceDb({"n": 100, "ci_low": 0.54, "avg_clv": None}),
                "CFB-H001", "v1", "VALIDATED_SIGNAL")


def test_h003_and_h005_are_registered_with_frozen_definitions() -> None:
    for key in ("CFB-H003", "CFB-H005"):
        definition = definition_for(key, "v1")
        # Section 10.1 preregistration fields must all be present.
        for field in ("name", "claim", "outcome", "population", "features",
                      "minimums", "split", "test", "family", "promotion"):
            assert definition.get(field) is not None, f"{key} missing {field}"
        assert definition["test"]["alpha"] == 0.05
        assert definition["test"]["direction"] in {"greater", "less"}
        assert definition["minimums"]["prospective_n"] >= 100
        assert definition["split"]["method"] == "expanding_walk_forward"
        assert definition["split"]["holdout"] == 2025


def test_the_two_new_hypotheses_declare_their_own_test_families() -> None:
    """A distinct family keeps them out of H001's multiple-testing pool."""
    families = {definition_for(key, "v1")["family"]
                for key in ("CFB-H001", "CFB-H003", "CFB-H005")}
    assert families == {"spread-buckets-v1", "roster-continuity-v1", "coaching-regime-v1"}


def test_h005_predicts_under_performance_not_over_performance() -> None:
    """'Overvalued' means covering BELOW break-even, so direction is 'less'."""
    test_plan = definition_for("CFB-H005", "v1")["test"]
    assert test_plan["direction"] == "less"
    assert test_plan["baseline"] == pytest.approx(MARKET_BREAK_EVEN_110)


def test_blocked_hypotheses_name_the_exact_missing_input() -> None:
    h003 = definition_for("CFB-H003", "v1")["data_readiness"]
    assert h003["state"] == "BLOCKED_MISSING_FEATURE"
    assert h003["missing_inputs"] == ["returning_defensive_production_pct"]
    # The market side of H003 exists; only the feature is missing.
    assert any("total" in item for item in h003["available_inputs"])
    assert h003["enabler"]

    h005 = definition_for("CFB-H005", "v1")["data_readiness"]
    assert h005["missing_inputs"] == ["offensive_coordinator_regime"]
    assert h005["enabler"]


def test_only_h001_is_evaluable_today() -> None:
    assert is_evaluable("CFB-H001", "v1")
    assert not is_evaluable("CFB-H003", "v1")
    assert not is_evaluable("CFB-H005", "v1")


def test_evaluating_a_blocked_hypothesis_fails_closed() -> None:
    """The walk-forward routine scores H001's hardcoded cohort.

    Without this guard, `evaluate CFB-H003` would store H001's cohort results
    under H003's hypothesis id and silently manufacture a backtest.
    """
    for key in ("CFB-H003", "CFB-H005"):
        with pytest.raises(ValueError) as error:
            _require_evaluable(key, "v1")
        message = str(error.value)
        assert "no implemented evaluator" in message
        assert "BLOCKED_MISSING_FEATURE" in message


def test_an_unregistered_hypothesis_is_rejected_rather_than_assumed() -> None:
    with pytest.raises(ValueError):
        definition_for("CFB-H999", "v1")
    with pytest.raises(ValueError):
        is_evaluable("CFB-H001", "v99")


def test_readiness_report_covers_every_registered_definition() -> None:
    report = readiness_report()
    assert {row["hypothesis"] for row in report} == {
        f"{key}-{version}" for key, version in HYPOTHESIS_DEFINITIONS
    }
    assert sum(1 for row in report if row["evaluable"]) == 1


def _stored_row(key: str) -> dict:
    definition = definition_for(key, "v1")
    return {
        "name": definition["name"], "claim": definition["claim"],
        "outcome_definition_json": definition["outcome"],
        "population_filter_json": definition["population"],
        "bucket_definition_json": definition["buckets"],
        "min_sample_json": definition["minimums"],
        "split_plan_json": definition["split"],
        "test_plan_json": definition["test"],
        "promotion_rules_json": definition["promotion"],
        "multiple_test_family": definition["family"],
        "feature_definition_json": {
            **definition["features"], "data_readiness": {"state": "BLOCKED_MISSING_FEATURE"},
            "evaluator": definition["evaluator"],
        },
    }


def test_an_unchanged_definition_reports_no_drift() -> None:
    for key in ("CFB-H001", "CFB-H003", "CFB-H005"):
        assert definition_drift(definition_for(key, "v1"), _stored_row(key)) == []


def test_moving_a_frozen_threshold_after_registration_is_detected() -> None:
    definition = definition_for("CFB-H005", "v1")
    flipped = dict(_stored_row("CFB-H005"),
                   test_plan_json={**definition["test"], "direction": "greater"})
    assert definition_drift(definition, flipped) == ["test"]
    lowered = dict(_stored_row("CFB-H003"), min_sample_json={"holdout_n": 1, "prospective_n": 1})
    assert definition_drift(definition_for("CFB-H003", "v1"), lowered) == ["minimums"]


def test_readiness_may_change_without_counting_as_drift() -> None:
    """Whether an enabler has shipped is an operational fact, not the claim."""
    definition = definition_for("CFB-H003", "v1")
    shipped = dict(_stored_row("CFB-H003"))
    shipped["feature_definition_json"] = {
        **definition["features"], "data_readiness": {"state": "READY", "missing_inputs": []},
        "evaluator": "opponent_total_error",
    }
    assert definition_drift(definition, shipped) == []


def test_h001_catalog_still_matches_what_was_originally_frozen() -> None:
    """H001 is already registered in production.

    Editing its catalog entry would make `register` raise on the next
    scheduled run. These values reproduce the original `register_default`
    payload exactly; changing one means registering CFB-H001 v2.
    """
    definition = definition_for("CFB-H001", "v1")
    assert definition["name"] == "Non-neutral home favorites 14.0-16.5"
    assert definition["claim"] == (
        "The registered cohort covers above the -110 market break-even rate."
    )
    assert definition["outcome"] == {
        "market": "full_game_spread", "perspective": "home", "overtime": "included",
    }
    assert definition["population"] == {
        "home_spread_min": -16.5, "home_spread_max": -14.0,
        "neutral_site": False, "home_classification": "fbs",
        "away_classification": "fbs", "line_designation": "historical_reference",
    }
    assert definition["buckets"] == {
        "version": DEFINITION_VERSION, "favorite_low": 14.0, "favorite_high": 16.5,
    }
    assert definition["minimums"] == {"holdout_n": 40, "prospective_n": 100}
    assert definition["split"] == {
        "method": "expanding_walk_forward", "start": 2016, "holdout": 2025,
    }
    assert definition["test"] == {
        "alpha": 0.05, "direction": "greater", "baseline": MARKET_BREAK_EVEN_110,
    }
    assert definition["promotion"] == {
        "requires_positive_prospective_clv": True, "manual_review": True,
    }
    assert definition["family"] == "spread-buckets-v1"
    # The original row left feature_definition_json at its column default.
    assert definition["features"] == {}


def test_registering_an_already_frozen_h001_would_not_raise() -> None:
    """Simulates the production row, which carries no feature definition."""
    stored = _stored_row("CFB-H001")
    stored["feature_definition_json"] = {}
    assert definition_drift(definition_for("CFB-H001", "v1"), stored) == []
