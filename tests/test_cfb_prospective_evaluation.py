from datetime import date, datetime, timedelta, timezone

from research.cfb_prospective_evaluation import FORECAST_VERSIONS, evaluate


NOW = datetime(2026, 10, 4, 12, tzinfo=timezone.utc)


def _row(version: str, *, source: int = 101, completed: bool = True) -> dict:
    freeze = datetime(2026, 10, 3, 12, tzinfo=timezone.utc)
    kickoff = freeze + timedelta(hours=6)
    evidence = {
        "definition": "cfb-comparison-v1",
        "forecast": {"home_points": 27, "away_points": 23, "home_win_probability": 0.65},
        "markets": {
            "spread": {"eligible": True, "value": 3, "reasons": []},
            "total": {"eligible": True, "value": 48, "reasons": []},
            "moneyline": {"eligible": True, "value": 0.6, "reasons": []},
        },
    }
    if version == FORECAST_VERSIONS[2]:
        evidence["anchor"] = {
            "version": "cfb-market-anchor-v1",
            "markets": {"spread": 3.25, "total": 48.5, "moneyline": 0.6125},
        }
    return {
        "game_id": 1, "game_date": date(2026, 10, 3),
        "commence_time": kickoff, "forecast_at": freeze,
        "home_name": "Home", "away_name": "Away",
        "version": version, "odds_history_id": source,
        "evidence_json": evidence, "completed": completed,
        "home_score": 28 if completed else None,
        "away_score": 21 if completed else None,
        "close_id": 7, "close_at": freeze + timedelta(hours=5),
        "close_history_id": 202, "close_home_spread": -4,
        "close_total": 49, "close_home_ml": -175, "close_away_ml": 145,
    }


def test_grades_market_model_and_anchor_on_same_capture():
    report = evaluate([_row(version) for version in FORECAST_VERSIONS], 2026, NOW)
    v3 = report["market_comparison"][FORECAST_VERSIONS[2]]
    assert v3["spread"]["n"] == 1
    assert v3["spread"]["model_error"] == 3
    assert v3["spread"]["market_error"] == 4
    assert v3["spread"]["anchor_error"] == 3.75
    assert v3["spread"]["directional_close_move"] == 1
    assert v3["moneyline"]["anchor_n"] == 1
    assert report["strict_same_capture"]["spread"]["n"] == 1
    assert report["coverage"][FORECAST_VERSIONS[2]]["verified_close"] == 1


def test_missing_final_or_mismatched_capture_stays_out_of_strict_cohort():
    rows = [_row(FORECAST_VERSIONS[0]), _row(FORECAST_VERSIONS[1]),
            _row(FORECAST_VERSIONS[2], source=102)]
    report = evaluate(rows, 2026, NOW)
    assert report["market_comparison"][FORECAST_VERSIONS[2]]["spread"]["n"] == 1
    assert report["strict_same_capture"]["spread"]["n"] == 0
    rows = [_row(version, completed=False) for version in FORECAST_VERSIONS]
    report = evaluate(rows, 2026, NOW)
    assert report["market_comparison"][FORECAST_VERSIONS[2]]["spread"]["n"] == 0
    assert report["coverage"][FORECAST_VERSIONS[2]]["awaiting_final"] == 1
