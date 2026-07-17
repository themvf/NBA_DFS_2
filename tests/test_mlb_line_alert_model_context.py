from datetime import datetime, timezone

from model.line_alerts import _mlb_model_signal_context


class _FakeDb:
    def __init__(self, probability: float | None):
        self.probability = probability

    def execute_one(self, query, params):
        assert "mlb_game_prediction_snapshots" in query
        assert params[0] == 42
        if self.probability is None:
            return None
        return {
            "raw_prediction": self.probability,
            "created_at": datetime(2026, 7, 17, 14, 0, tzinfo=timezone.utc),
            "model_version": "mlb-ml-v1",
        }


def _capture():
    return {
        "matchup_id": 42,
        "captured_at": datetime(2026, 7, 17, 14, 30, tzinfo=timezone.utc),
    }


def test_freezes_home_model_agreement_with_alert_details() -> None:
    context = _mlb_model_signal_context(_FakeDb(0.57), _capture(), "home", 0.54)
    assert context["model_probability"] == 0.57
    assert context["model_gap_pp"] == 3.0
    assert context["model_agreement"] == "agree"
    assert context["model_kind"] == "raw_market_anchored"


def test_flips_home_probability_for_away_alert() -> None:
    context = _mlb_model_signal_context(_FakeDb(0.57), _capture(), "away", 0.46)
    assert context["model_probability"] == 0.43
    assert context["model_gap_pp"] == -3.0
    assert context["model_agreement"] == "disagree"


def test_returns_empty_context_without_a_prediction() -> None:
    assert _mlb_model_signal_context(_FakeDb(None), _capture(), "home", 0.54) == {}


def test_marks_extreme_live_prediction_unavailable() -> None:
    context = _mlb_model_signal_context(_FakeDb(0.999), _capture(), "home", 0.54)
    assert context["model_probability"] == 0.999
    assert context["model_agreement"] == "unavailable_extreme"
    assert "model_gap_pp" not in context


def test_marks_implausible_market_gap_unavailable() -> None:
    context = _mlb_model_signal_context(_FakeDb(0.80), _capture(), "home", 0.50)
    assert context["model_probability"] == 0.80
    assert context["model_agreement"] == "unavailable_extreme_gap"
    assert "model_gap_pp" not in context
