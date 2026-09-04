from datetime import datetime, timedelta, timezone

from ingest.cfb_historical_replay import request_times, replay, resolve_candidates

KICK = datetime(2026, 8, 29, 16, tzinfo=timezone.utc)


def game():
    return {"id": 1, "label": "Away @ Home", "commence_time": KICK,
            "completed": True, "home_score": 30, "away_score": 20}


def row(i, minutes, line):
    stamp = KICK-timedelta(minutes=minutes)
    return {"history_id": str(i), "captured_at": stamp, "capture_key": str(i),
            "books": {b: {"spread_home": line, "spread_away": -line,
                           "spread_home_price": -110, "spread_away_price": -110,
                           "last_update": stamp.isoformat()}
                      for b in ("draftkings", "fanduel", "betmgm", "betrivers")}}


def test_requests_bounded_before_kickoff():
    times = request_times(game())
    assert len(times) == 75
    assert max(times) == KICK-timedelta(seconds=1)
    assert min(times) == KICK-timedelta(hours=48)


def test_missing_history_is_explicit():
    assert replay(game(), [])["captures"] == 0


def test_replay_preserves_origin_grades_away_and_measures_same_book_clv():
    result = replay(game(), [row(1, 25, -14.5), row(2, 15, -13), row(3, 3, -12)])
    signal = next(s for s in result["signals"] if s["type"] == "spread_steam")
    assert signal["side"] == "away"
    assert signal["outcome"] == "won"  # Away +13 loses game by 10.
    assert signal["same_book_line_clv_points"] == 1
    assert signal["details"]["origin"] == "historical_backtest"


def test_stale_close_and_stale_book_cannot_generate_clv():
    rows = [row(1, 45, -14.5), row(2, 25, -13)]
    assert all(s["same_book_line_clv_points"] is None for s in replay(game(), rows)["signals"])
    rows.append(row(3, 3, -12))
    for q in rows[-1]["books"].values():
        q["last_update"] = (KICK-timedelta(hours=1)).isoformat()
    assert all(s["same_book_line_clv_points"] is None for s in replay(game(), rows)["signals"])


def test_same_price_duplicates_use_older_timestamp_and_conflicts_fail():
    import copy
    import pytest
    a = {"id": "event", "home_team": "Home", "away_team": "Away", "bookmakers": [
        {"key": "dk", "last_update": "2026-08-29T15:55:00Z", "markets": [
            {"key": "h2h", "outcomes": [{"name": "Home", "price": -110}]}]}]}
    b = copy.deepcopy(a)
    b["bookmakers"][0]["last_update"] = "2026-08-29T15:56:00Z"
    assert resolve_candidates([b,a])["bookmakers"][0]["last_update"] == "2026-08-29T15:55:00Z"
    b["bookmakers"][0]["markets"][0]["outcomes"][0]["price"] = -115
    with pytest.raises(ValueError, match="Conflicting"):
        resolve_candidates([a,b])
