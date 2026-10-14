"""The alert price trail fetches only the books grading reads.

A capture carries every quoted book (1.3-2.9 KB). Settlement re-reads the full
trail of every open alert on every run, about 13,700 trail reads a day by
2026-10-11, so the query now returns only the selection book, Pinnacle and
DraftKings. A book grading read but the query left out would come back as "no
quote" without any error; these tests fail instead.
"""
from datetime import datetime, timedelta, timezone

import pytest

from model import line_alerts

TRIGGER = datetime(2026, 10, 1, 17, tzinfo=timezone.utc)
KICKOFF = TRIGGER + timedelta(hours=3)


class RecordingBooks(dict):
    def __init__(self, data, seen):
        super().__init__(data)
        self.seen = seen

    def get(self, key, default=None):
        self.seen.add(key)
        return super().get(key, default)

    def __getitem__(self, key):
        self.seen.add(key)
        return super().__getitem__(key)

    def __iter__(self):
        raise AssertionError("grading read every book")

    def items(self):
        raise AssertionError("grading read every book")


class TrailDatabase:
    def __init__(self, books_by_capture):
        self.books_by_capture = books_by_capture
        self.trail = None
        self.seen: set[str] = set()

    def execute_one(self, sql, params=None):
        return None

    def execute(self, sql, params=None):
        self.trail = (" ".join(sql.split()), params)
        return [{"captured_at": TRIGGER + timedelta(minutes=30 * i), "books": RecordingBooks(books, self.seen)}
                for i, books in enumerate(self.books_by_capture)]


def moneyline(home, away):
    return {"ml_home": home, "ml_away": away}


def prop(line, over, under):
    return {"line": line, "over": over, "under": under}


GAME_CAPTURES = [
    {"draftkings": moneyline(120, -140), "pinnacle": moneyline(110, -125),
     "betrivers": moneyline(125, -150), "fanduel": moneyline(118, -138)},
    {"draftkings": moneyline(105, -125), "pinnacle": moneyline(102, -118),
     "betrivers": moneyline(110, -130), "fanduel": moneyline(100, -120)},
]
PROP_CAPTURES = [
    {"draftkings": prop(5.5, -105, -115), "pinnacle": prop(5.5, -120, 100), "betrivers": prop(5.5, 130, -160)},
    {"draftkings": prop(5.5, -125, 105), "pinnacle": prop(5.5, -130, 110), "betrivers": prop(5.5, 120, -150)},
]


def alert(alert_type, details, side="home"):
    return {"id": 1, "alert_type": alert_type, "sport": "mlb", "matchup_id": 7, "side": side,
            "details_json": details, "created_at": TRIGGER, "commence_time": KICKOFF, "sharp_prob": 0.47}


CASES = {
    "v1 game alert at DraftKings": (alert("dk_value", {"dk_decimal": 2.2}), GAME_CAPTURES),
    "v2 alert at its execution book": (alert("dk_value", {"dk_decimal": 2.25, "exec_book": "betrivers"}), GAME_CAPTURES),
    "v3 alert at its selection book": (
        alert("dk_value", {"dk_decimal": 2.2, "clv_book": "draftkings", "exec_book": "betrivers"}), GAME_CAPTURES),
    "player prop": (
        alert("dk_prop_value", {"dk_decimal": 1.95, "market": "pitcher_strikeouts", "line": 5.5, "bet": "Over"},
              side="Someone K O5.5"), PROP_CAPTURES),
}


@pytest.mark.parametrize("name", CASES)
def test_grading_reads_only_books_the_trail_query_fetched(name):
    a, captures = CASES[name]
    db = TrailDatabase(captures)
    grade = line_alerts._grade_alert_prices(db, a)
    sql, params = db.trail
    assert "jsonb_each(books)" in sql
    assert db.seen and db.seen <= set(params[0]), db.seen - set(params[0])
    assert grade["dk_close_decimal"] is not None


def test_graded_books_follow_the_selection_book_across_generations():
    assert line_alerts._graded_books(alert("dk_value", {})) == ["draftkings", "pinnacle"]
    assert line_alerts._graded_books(alert("dk_value", {"exec_book": "betrivers"})) == [
        "betrivers", "draftkings", "pinnacle"]
    assert line_alerts._graded_books(alert("dk_value", {"clv_book": "fanduel", "exec_book": "betrivers"})) == [
        "draftkings", "fanduel", "pinnacle"]
