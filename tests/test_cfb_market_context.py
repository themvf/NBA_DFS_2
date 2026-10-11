from datetime import datetime, timedelta, timezone

from model.cfb_market_context import lower_median, measure_movement


T0 = datetime(2026, 9, 5, 16, tzinfo=timezone.utc)
BOOKS = ("draftkings", "fanduel", "betmgm", "betrivers", "pinnacle")


def capture(identifier, minutes, lines, *, ages=None, revision="rev-1"):
    observed = T0 + timedelta(minutes=minutes)
    ages = ages or {}
    books = {}
    for book, line in zip(BOOKS, lines):
        books[book] = {
            "spread_home": line, "spread_away": -line,
            "spread_home_price": -110, "spread_away_price": -110,
            "total_line": 50 - line, "over": -110, "under": -110,
            "last_update": (observed - timedelta(seconds=ages.get(book, 0))).isoformat(),
        }
    return {"capture_id": identifier, "observed_at": observed, "schedule_revision_id": revision,
            "pregame_state": "pregame", "books": books}


def run(rows, endpoint="end", market="spread"):
    return measure_movement(rows, endpoint_capture_id=endpoint, market=market, allowlist=BOOKS,
                            event_key="cfb:event:1", config_digest="abc", scheduled_kickoff=T0 + timedelta(hours=2))


def test_lower_median_for_four_and_five_books():
    assert lower_median([4, 1, 3, 2]) == 2
    assert lower_median([5, 1, 4, 2, 3]) == 3


def test_endpoint_selection_signs_and_membership_safe_median():
    rows = [capture("old", 0, [-1] * 5), capture("start", 10, [-3, -3, -3, -3, -3]),
            capture("end", 30, [-4, -4, -4, -4, -4])]
    result = run(rows)
    assert result.status == "accepted"
    assert result.payload["start_capture_id"] == "start"
    assert result.payload["scalar_value"] == 1
    assert result.payload["direction"] == "toward_home"
    total = run(rows, market="total")
    assert total.payload["scalar_value"] == 1
    assert total.payload["under_scalar_value"] == -1


def test_freshness_300_included_301_excluded_and_four_book_floor():
    start = capture("start", 0, [-3] * 5, ages={"pinnacle": 300})
    end = capture("end", 15, [-4] * 5, ages={"pinnacle": 301})
    accepted = run([start, end])
    assert accepted.status == "accepted"
    assert accepted.payload["common_book_count"] == 4
    end["books"]["betrivers"]["last_update"] = (T0 + timedelta(minutes=15, seconds=-301)).isoformat()
    rejected = run([start, end])
    assert rejected.reason == "insufficient_book_intersection"


def test_exact_15_and_30_boundaries_and_tie_capture_id():
    rows = [capture("z", 0, [-1] * 5), capture("a", 0, [-3] * 5),
            capture("latest", 15, [-3.5] * 5), capture("end", 30, [-4] * 5)]
    result = run(rows)
    assert result.payload["start_capture_id"] == "latest"
    boundary = run([capture("a", 0, [-3] * 5), capture("end", 30, [-4] * 5)])
    assert boundary.status == "accepted"
    tie = run([capture("z", 15, [-1] * 5), capture("a", 15, [-3] * 5), capture("end", 30, [-4] * 5)])
    assert tie.payload["start_capture_id"] == "a"


def test_no_start_disappeared_book_and_unsupported_market():
    assert run([capture("end", 30, [-4] * 5)]).reason == "no_start_capture"
    start, end = capture("start", 0, [-3] * 5), capture("end", 15, [-4] * 5)
    del end["books"]["pinnacle"]
    result = run([start, end])
    assert result.status == "accepted"
    assert result.payload["membership_only_books"] == ["pinnacle"]
    assert run([start, end], market="moneyline").reason == "unsupported_market"
