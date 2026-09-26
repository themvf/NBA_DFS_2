"""Verified-close grading for the CFB moneyline pilot."""

import pytest

from model.line_alerts import _cfb_moneyline_close_grade
from model import line_alerts


def _alert(book: str = "fanduel", side: str = "away") -> dict:
    return {
        "side": side,
        "details_json": {"market": "moneyline", "exec_book": book, "exec_decimal": 1.8333},
    }


def test_uses_selected_book_and_selection_at_verified_close() -> None:
    close = {
        "history_id": 59937,
        "books": {
            "fanduel": {"ml_away": -138, "ml_home": 115},
            "draftkings": {"ml_away": -142, "ml_home": 120},
        },
    }
    grade = _cfb_moneyline_close_grade(_alert(), close)
    assert grade["close_history_id"] == 59937
    assert grade["close_decimal"] == 1 + 100 / 138
    assert grade["price_clv_pct"] == 6.301
    assert grade["price_comparison_status"] == "SAME_BOOK_SELECTION_RULE_UNVERIFIED"
    assert grade["settlement_rule_status"] == "UNVERIFIED_LEGACY_QUOTES"


def test_missing_execution_book_remains_missing() -> None:
    close = {"history_id": 59937, "books": {"williamhill": {"ml_away": -141}}}
    grade = _cfb_moneyline_close_grade(_alert("williamhill_us"), close)
    assert grade["close_history_id"] == 59937
    assert grade["price_clv_pct"] is None
    assert grade["close_decimal"] is None
    assert grade["price_comparison_status"] == "CLOSE_UNAVAILABLE"


def test_missing_selection_price_remains_missing() -> None:
    close = {"history_id": 59827, "books": {"draftkings": {"ml_away": -250}}}
    grade = _cfb_moneyline_close_grade(_alert("draftkings", "home"), close)
    assert grade["price_clv_pct"] is None


def test_settlement_persists_verified_price_and_regrades_existing_result(monkeypatch) -> None:
    class Cursor:
        def __init__(self):
            self.writes = []

        def execute(self, sql, params=None):
            self.writes.append((sql, params))

    class Connection:
        def __init__(self):
            self.cursor_instance = Cursor()

        def __enter__(self):
            return self

        def __exit__(self, *_):
            return False

        def cursor(self):
            return self.cursor_instance

    class Db:
        database_url = None

        def __init__(self):
            self.connection = Connection()

        def execute(self, sql, params=None):
            assert "close_history_id IS NULL" in sql
            return [{
                "id": 93465, "matchup_id": 10, "side": "away", "sport": "cfb",
                "alert_type": "steam", "alert_prob": 0.5,
                "details_json": _alert()["details_json"],
            }]

        def execute_one(self, sql, params=None):
            return {"hs": 10, "as_": 24}

        def connect(self):
            return self.connection

    db = Db()
    monkeypatch.setattr(line_alerts, "_verified_close", lambda *_args, **_kwargs: {
        "history_id": 59937, "books": {"fanduel": {"ml_away": -138}},
    })
    monkeypatch.setattr(line_alerts, "_retail_fair_side", lambda *_: 0.51)
    monkeypatch.setattr(line_alerts, "_grade_alert_prices", lambda *_: {
        "dk_close_decimal": None, "dk_clv_pct": None, "pin_close_prob": None,
        "convergence": None, "dk_survival_min": None, "grading_json": {},
        "comparison_status": "NO_REFERENCE", "grading_version": "convergence_v2",
    })
    monkeypatch.setattr(line_alerts, "_append_grade_history_cur", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(line_alerts, "_settle_football_line_alerts", lambda *_args: 0)
    assert line_alerts.settle(db, "cfb") == 1
    sql, params = db.connection.cursor_instance.writes[0]
    assert "COALESCE(settled_at, NOW())" in sql
    assert params[-4] == 59937
    assert params[-3] == pytest.approx(0.8333)
    assert params[-2] == "won"
    assert __import__("json").loads(params[8])["price_clv_pct"] == 6.301
