from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from model import line_alerts


class _Cursor:
    def __init__(self):
        self.writes: list = []

    def execute(self, sql, params=None):
        self.writes.append((sql, params))


class _Connection:
    def __init__(self):
        self.cursor_instance = _Cursor()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def cursor(self):
        return self.cursor_instance


class _Db:
    def __init__(self, scores: dict | None):
        self.scores = scores
        self.connection = _Connection()

    def execute(self, sql, params=None):
        if "FROM line_alerts" in sql and "alert_type IN ('pinnacle_divergence'" in sql:
            return [{"id": 9, "matchup_id": 3, "side": "away", "sport": "nfl", "alert_type": "walking",
                     "alert_prob": 0.40, "commence_time": datetime(2026, 9, 27, 17, tzinfo=timezone.utc),
                     "details_json": {"market": "moneyline"}}]
        return []

    def execute_one(self, sql, params=None):
        assert "FROM nfl_matchups" in sql
        return self.scores

    def connect(self):
        return self.connection


def _settle(monkeypatch, scores: dict | None) -> list:
    monkeypatch.setattr(line_alerts, "_verified_close", lambda *_a, **_k: None)
    monkeypatch.setattr(line_alerts, "_grade_alert_prices",
                        lambda *_: pytest.fail("no close means no price grade"))
    monkeypatch.setattr(line_alerts, "_append_grade_history_cur", lambda *_a, **_k: None)
    db = _Db(scores)
    line_alerts.settle(db, "nfl")
    return db.connection.cursor_instance.writes


def test_a_final_without_a_verified_close_settles_on_the_score(monkeypatch) -> None:
    writes = _settle(monkeypatch, {"hs": 16, "as_": 23})
    assert len(writes) == 1
    params = writes[0][1]
    assert params[0] is None and params[1] is None  # no close, no CLV
    assert params[2] == "won"
    assert params[9] == "NO_CLOSE"
    assert json.loads(params[8]) == {"close_source": "unavailable"}


def test_an_unscored_game_without_a_verified_close_stays_open(monkeypatch) -> None:
    assert _settle(monkeypatch, {"hs": None, "as_": None}) == []
