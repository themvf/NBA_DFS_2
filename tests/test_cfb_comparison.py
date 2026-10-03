from datetime import datetime, timedelta, timezone

from research.cfb_comparison import comparison


def test_comparison_is_market_specific_and_point_in_time():
    frozen = datetime(2026, 10, 3, 16, 0, tzinfo=timezone.utc)
    captured = frozen - timedelta(minutes=5)
    quote = {
        "spread_home": -3, "spread_away": 3,
        "spread_home_price": -110, "spread_away_price": -110,
        "total_line": 48.5, "over": -110, "under": -110,
        "last_update": (captured - timedelta(minutes=1)).isoformat(),
    }
    row = comparison(
        {"generated_at": frozen, "kickoff": frozen + timedelta(hours=2),
         "home_points": 27.5, "away_points": 24.0, "home_win_probability": 0.61},
        {"captured_at": captured, "books": {
            "draftkings": quote, "fanduel": quote, "betmgm": quote},
         "home_spread": -3, "vegas_total": 48.5,
         "home_ml": None, "away_ml": None},
        {"home_adjusted_off_ppa": 0.1},
    )
    assert row["markets"]["spread"]["eligible"]
    assert row["markets"]["total"]["eligible"]
    assert not row["markets"]["moneyline"]["eligible"]
    assert "missing_market_value" in row["markets"]["moneyline"]["reasons"]
    assert row["market_age_minutes"] == 5
    assert row["feature"]["home_adjusted_off_ppa"] == 0.1
    assert row["forecast"]["home_points"] == 27.5


def test_old_capture_is_excluded_even_with_three_books():
    frozen = datetime(2026, 10, 3, 16, 0, tzinfo=timezone.utc)
    captured = frozen - timedelta(hours=3)
    quote = {
        "ml_home": -150, "ml_away": 130,
        "last_update": captured.isoformat(),
    }
    row = comparison(
        {"generated_at": frozen, "kickoff": frozen + timedelta(hours=2)},
        {"captured_at": captured, "books": {
            "draftkings": quote, "fanduel": quote, "betmgm": quote},
         "home_spread": None, "vegas_total": None,
         "home_ml": -150, "away_ml": 130},
        None,
    )
    assert row["markets"]["moneyline"]["books"] == 3
    assert not row["markets"]["moneyline"]["eligible"]
    assert row["markets"]["moneyline"]["reasons"] == ["capture_too_old"]
