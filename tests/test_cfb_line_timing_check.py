from collections import Counter
from datetime import datetime, timedelta, timezone

from research import cfb_line_timing_check as t

KICK = datetime(2026, 10, 3, 19, 30, tzinfo=timezone.utc)


def _series(*values):
    return [(KICK - timedelta(hours=48 - i), v) for i, v in enumerate(values)]


def test_classify_outcomes():
    s = _series(-3.0, -3.5, -4.5)
    assert t.classify(-4.5, s) == "close_only"
    assert t.classify(-3.0, s) == "open_only"
    assert t.classify(-3.5, s) == "neither"
    assert t.classify(-4.5, _series(-4.5, -4.5)) == "no_move"
    assert t.classify(None, s) is None
    assert t.classify(-4.5, []) is None


def test_latest_match_hours_takes_the_latest_equal_capture():
    s = _series(-3.0, -4.5, -3.0, -4.0)
    assert t.latest_match_hours(-3.0, s, KICK) == 46.0
    assert t.latest_match_hours(-7.0, s, KICK) is None


def test_verdict_thresholds():
    assert t.verdict(Counter(close_only=70, open_only=15, neither=15))["label"] == "CLOSE_LIKE"
    assert t.verdict(Counter(close_only=69, open_only=10, neither=21))["label"] == "INDETERMINATE"
    assert t.verdict(Counter(close_only=70, open_only=16, neither=14))["label"] == "INDETERMINATE"
    assert t.verdict(Counter(open_only=80, close_only=5, neither=15))["label"] == "OPEN_LIKE"
    assert t.verdict(Counter(no_move=40))["label"] == "INDETERMINATE"


def test_median_series_is_built_per_capture():
    rows = [
        {"matchup_id": 1, "captured_at": KICK - timedelta(hours=5), "book": "draftkings", "spread": -3.0, "total": 50.0},
        {"matchup_id": 1, "captured_at": KICK - timedelta(hours=5), "book": "fanduel", "spread": -4.0, "total": None},
        {"matchup_id": 1, "captured_at": KICK - timedelta(hours=5), "book": "betmgm", "spread": -3.5, "total": 51.0},
    ]
    series = t._series_by_book(rows)
    assert series[(1, "__median__", "spread")] == [(KICK - timedelta(hours=5), -3.5)]
    assert series[(1, "__median__", "total")] == [(KICK - timedelta(hours=5), 50.5)]
    assert series[(1, "draftkings", "spread")] == [(KICK - timedelta(hours=5), -3.0)]


class FakeDB:
    def __init__(self, games, rows):
        self.games, self.rows = games, rows

    def execute(self, sql, params=None):
        return self.rows if "jsonb_each" in sql else self.games


def test_run_reaches_a_close_like_decision_on_draftkings():
    games = [{"id": i, "cfbd_game_id": 1000 + i, "commence_time": KICK} for i in range(60)]
    rows, lines = [], []
    for i in range(60):
        for hours, spread in ((40, -3.0), (2, -4.5)):
            rows.append({"matchup_id": i, "captured_at": KICK - timedelta(hours=hours),
                         "book": "draftkings", "spread": spread, "total": 50.0})
        lines.append({"id": 1000 + i, "lines": [{"provider": "DraftKings", "spread": -4.5,
                                                   "spreadOpen": -3.0, "overUnder": 50.0}]})
    report = t.run(FakeDB(games, rows), lines, KICK + timedelta(days=5))
    assert report["decision_basis"] == "draftkings"
    assert report["decision"] == "CLOSE_LIKE"
    assert report["table"]["draftkings|spread_open"]["label"] == "OPEN_LIKE"
    assert report["table"]["draftkings|total_reference"]["counts"] == {"no_move": 60}
    assert report["spread_sign_check_vs_close"] == {"same": 60}
