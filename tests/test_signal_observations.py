from datetime import datetime, timedelta, timezone
import json

import pytest

from model.signal_observations import lifecycle, record_observations
from model.line_alerts import _cfb_market_signals
from tests.test_cfb_market_signals import _books, _history


@pytest.mark.parametrize("line,state", [(-3, "held"), (-4, "strengthened"),
                                       (-2.5, "weakened"), (-2, "faded"), (-1, "reversed")])
def test_spread_lifecycle(line, state):
    status, evidence = lifecycle(_books(-2), _books(-3), _books(line), "spread", "home")
    assert status == state
    assert evidence["comparable_books"] == 4  # excludes reference book
    assert evidence["retention_fraction"] == pytest.approx(-line - 2)


def test_missing_books_and_nonfinite_quotes_are_not_failed_moves():
    status, _ = lifecycle(_books(-2), _books(-3), {"draftkings": {"spread_home": -1}}, "spread", "home")
    assert status == "unavailable"
    status, _ = lifecycle(_books(-2), _books(-3), _books(float("nan")), "spread", "home")
    assert status == "unavailable"


def test_price_pressure_does_not_compare_changed_proposition():
    status, _ = lifecycle(_books(-3), _books(-3, home_price=-150, away_price=130),
                          _books(-3.5, home_price=-150, away_price=130), "spread", "home", True)
    assert status == "unavailable"


def test_nfl_has_separate_version_and_key_numbers():
    signals = _cfb_market_signals(_history([(-2.5, 50), (-3.5, 50)]), sport="nfl")
    crossing = next(s for s in signals if s["alert_type"] == "key_cross")
    assert crossing["details"]["signal_version"] == "nfl-structure-v1"
    assert crossing["details"]["key_number"] == 3
    assert not any(s["alert_type"] == "key_cross" for s in
                   _cfb_market_signals(_history([(-9.5, 50), (-10.5, 50)]), sport="nfl"))


class ObservationDb:
    def __init__(self):
        self.rows = []
    def execute(self, sql, params):
        if "SELECT DISTINCT ON" in sql:
            return self.rows[:1]
        assert "INSERT INTO market_signal_observations" in sql
        assert "ON CONFLICT DO NOTHING" in sql
        keys = ("sport", "matchup_id", "market", "alert_type", "side", "detector_version",
                "history_id", "trigger_history_id", "baseline_history_id", "observed_at", "state", "details_json")
        item = dict(zip(keys, params))
        item["details_json"] = json.loads(item["details_json"])
        item.update(trigger_books=_books(-3), baseline_books=_books(-2))
        if not any(r["history_id"] == item["history_id"] for r in self.rows):
            self.rows.append(item)
        return []
    def execute_one(self, sql, params):
        return {"id": 1}


def test_repeat_capture_is_idempotent_and_absent_detector_can_hold():
    now = datetime.now(timezone.utc)
    row = dict(matchup_id=4, history_id=2, captured_at=now - timedelta(minutes=5),
               commence_time=now + timedelta(hours=1), books=_books(-3),
               movement_candidates=[dict(alert_type="spread_steam", side="home", details={"market": "spread"})])
    db = ObservationDb()
    record_observations(db, "nfl", row)
    record_observations(db, "nfl", row)
    assert len(db.rows) == 1
    assert db.rows[0]["state"] == "triggered"
    row.update(history_id=3, captured_at=now, movement_candidates=[])
    record_observations(db, "nfl", row)
    record_observations(db, "nfl", row)
    assert len(db.rows) == 2
    assert db.rows[1]["state"] == "held"
    row.update(history_id=4, captured_at=now - timedelta(hours=1))
    record_observations(db, "nfl", row)
    assert len(db.rows) == 2
