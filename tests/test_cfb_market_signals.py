from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from model import line_alerts


BOOKS = ("draftkings", "fanduel", "betmgm", "betrivers")


def _books(spread: float, total: float = 50.0, *, home_price: int = -110,
           away_price: int = -110, pinnacle_spread: float | None = None) -> dict:
    result = {
        key: {
            "spread_home": spread,
            "spread_home_price": home_price,
            "spread_away": -spread,
            "spread_away_price": away_price,
            "total_line": total,
            "over": -110,
            "under": -110,
        }
        for key in BOOKS
    }
    result["pinnacle"] = {
        "spread_home": spread if pinnacle_spread is None else pinnacle_spread,
        "spread_home_price": -108,
        "spread_away": -(spread if pinnacle_spread is None else pinnacle_spread),
        "spread_away_price": -108,
        "total_line": total,
        "over": -108,
        "under": -108,
    }
    return result


def _history(lines: list[tuple[float, float]], *, minutes: int = 10) -> list[dict]:
    start = datetime(2026, 9, 5, 16, tzinfo=timezone.utc)
    return [
        {
            "history_id": index + 1,
            "captured_at": start + timedelta(minutes=index * minutes),
            "capture_key": f"capture-{index + 1}",
            "books": _books(spread, total),
        }
        for index, (spread, total) in enumerate(lines)
    ]


def test_cfb_detects_supported_spread_and_total_steam() -> None:
    signals = line_alerts._cfb_market_signals(_history([(-2.0, 50.0), (-3.0, 51.5)]))
    keyed = {(signal["alert_type"], signal["side"]): signal for signal in signals}
    assert ("spread_steam", "home") in keyed
    assert ("total_steam", "over") in keyed
    detail = keyed[("spread_steam", "home")]["details"]
    assert detail["signal_version"] == "cfb-lines-v1"
    assert detail["trigger_history_id"] == 2
    assert detail["consensus_support"] >= 4


def test_cfb_detects_key_cross_and_reversal_with_evidence_ids() -> None:
    history = _history([(-2.5, 50.0), (-4.0, 50.0), (-3.0, 50.0)])
    signals = line_alerts._cfb_market_signals(history)
    reversal = next(signal for signal in signals if signal["alert_type"] == "reversal")
    assert reversal["side"] == "away"
    assert reversal["details"]["pivot_history_id"] == 2
    assert reversal["details"]["trigger_history_id"] == 3
    key_signals = line_alerts._cfb_market_signals(_history([(-2.5, 50.0), (-4.0, 50.0)]))
    assert any(signal["alert_type"] == "key_cross" for signal in key_signals)


def test_cfb_price_pressure_requires_same_line_and_four_comparable_books() -> None:
    history = _history([(-3.0, 50.0), (-3.0, 50.0)])
    history[0]["books"] = _books(-3.0, home_price=110, away_price=-130)
    history[1]["books"] = _books(-3.0, home_price=-130, away_price=110)
    signals = line_alerts._cfb_market_signals(history)
    pressure = next(signal for signal in signals if signal["alert_type"] == "price_pressure")
    assert pressure["side"] == "home"
    assert pressure["details"]["comparable_books"] >= 4
    assert pressure["details"]["price_move_pp"] >= 4


def test_cfb_reference_led_requires_reference_then_retail_follow() -> None:
    history = _history([(-2.0, 50.0), (-2.0, 50.0), (-3.0, 50.0)])
    history[0]["books"] = _books(-2.0, pinnacle_spread=-2.0)
    history[1]["books"] = _books(-2.0, pinnacle_spread=-3.0)
    history[2]["books"] = _books(-3.0, pinnacle_spread=-3.0)
    signals = line_alerts._cfb_market_signals(history)
    led = next(signal for signal in signals if signal["alert_type"] == "reference_led")
    assert led["side"] == "home"
    assert led["details"]["reference_book"] == "pinnacle"


class SettlementDb:
    def __init__(self) -> None:
        self.updates = []

    def execute(self, sql, params=None):
        if "SELECT a.*, m.home_score" in sql:
            return [{
                "id": 41, "matchup_id": 9, "commence_time": datetime(2026, 9, 5, 16, tzinfo=timezone.utc),
                "home_score": 24, "away_score": 20, "side": "home",
                "details_json": {"market": "spread", "trigger_line": -3.0,
                                 "entry_home_line": -3.0, "exec_line": -3.0,
                                 "exec_book": "draftkings", "exec_decimal": 1.91,
                                 "signal_version": "cfb-lines-v1"},
            }]
        self.updates.append((sql, params))
        return []

    def execute_one(self, sql, params=None):
        assert "verified_clv_closes" in sql
        return {"history_id": 99, "books": _books(-4.0)}


def test_cfb_settlement_uses_verified_close_and_saves_roi(monkeypatch) -> None:
    db = SettlementDb()
    grades = []
    monkeypatch.setattr(line_alerts, "_append_grade_history", lambda _db, alert_id, grade, outcome=None: grades.append((alert_id, grade, outcome)))
    assert line_alerts._settle_football_line_alerts(db, "cfb") == 1
    update_params = db.updates[-1][1]
    assert update_params[0] == "won"
    grading = __import__("json").loads(update_params[1])
    assert grading["close_history_id"] == 99
    assert grading["line_clv"] == pytest.approx(1.0)
    assert grading["pnl_units"] == pytest.approx(0.91)
    assert grades[0][2] == "won"


def test_nfl_line_settlement_also_requires_verified_close(monkeypatch) -> None:
    db = SettlementDb()
    grades = []
    monkeypatch.setattr(
        line_alerts, "_append_grade_history",
        lambda _db, alert_id, grade, outcome=None: grades.append((alert_id, grade, outcome)),
    )

    assert line_alerts._settle_football_line_alerts(db, "nfl") == 1
    grading = __import__("json").loads(db.updates[-1][1][1])
    assert grading["close_source"] == "verified_clv_closes"
    assert grading["close_history_id"] == 99
    assert grades[0][2] == "won"
