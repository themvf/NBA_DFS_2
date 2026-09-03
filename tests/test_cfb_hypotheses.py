from datetime import date

import pytest

from research.cfb_hypotheses import advance, allowed_transition, settle_prospective


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
