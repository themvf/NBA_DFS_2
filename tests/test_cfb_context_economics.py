from decimal import Decimal

from model.cfb_context_economics import resolve_legacy_economics, summarize_resolutions


def _alert(**overrides):
    row = {
        "id": 1,
        "alert_type": "dk_value",
        "signal_version": "cfb-lines-v1",
        "outcome": "lost",
        "pnl_units": None,
        "details_json": {"exec_decimal": 8.5, "dk_decimal": 8.5},
    }
    row.update(overrides)
    return row


def test_legacy_moneyline_loss_is_recomputed_when_pnl_column_is_null():
    result = resolve_legacy_economics(_alert(), [{
        "id": 8, "outcome": "lost", "pnl_units": None, "grading_json": {},
    }])
    assert result.result_state == "settled"
    assert result.pnl_units == Decimal("-1")
    assert result.roi_stake_units == Decimal("1")
    assert result.pnl_source == "recomputed"


def test_win_requires_a_frozen_entry_price():
    result = resolve_legacy_economics(_alert(
        outcome="won", details_json={},
    ))
    assert result.result_state == "missing_entry"
    assert result.pnl_units is None


def test_push_is_zero_profit_but_remains_in_roi_denominator():
    result = resolve_legacy_economics(_alert(outcome="push"))
    assert result.result_state == "settled"
    assert result.pnl_units == 0
    assert result.roi_stake_units == 1


def test_legacy_football_void_is_recovered_as_a_push():
    alert = _alert(
        alert_type="key_cross", outcome="void",
        details_json={"exec_decimal": 1.95, "market": "spread", "entry_home_line": -10},
    )
    result = resolve_legacy_economics(alert, [{
        "id": 3, "outcome": "void", "pnl_units": 0,
        "grading_json": {"market": "spread", "home_score": 30, "away_score": 20,
                         "entry_home_line": -10, "pnl_units": 0},
    }])
    assert result.result_state == "settled"
    assert result.outcome == "push"
    assert result.pnl_units == 0
    assert result.roi_stake_units == 1


def test_conflicting_current_grades_are_quarantined():
    result = resolve_legacy_economics(_alert(), [
        {"id": 1, "outcome": "lost", "grading_json": {}},
        {"id": 2, "outcome": "lost", "grading_json": {}},
    ])
    assert result.result_state == "conflict"
    assert result.reason_codes == ("multiple_current_grades",)


def test_stored_pnl_mismatch_is_not_silently_overridden():
    result = resolve_legacy_economics(_alert(pnl_units=0.5))
    assert result.result_state == "conflict"
    assert result.reason_codes == ("pnl_mismatch:line_alerts.pnl_units",)


def test_summary_uses_resolved_stake_not_observation_count():
    settled = resolve_legacy_economics(_alert())
    pending = resolve_legacy_economics(_alert(id=2, outcome=None))
    summary = summarize_resolutions([(_alert(), settled), (_alert(id=2), pending)])[0]
    assert summary["observations"] == 2
    assert summary["pending"] == 1
    assert Decimal(summary["pnl_units"]) == -1
    assert Decimal(summary["roi"]) == -1
