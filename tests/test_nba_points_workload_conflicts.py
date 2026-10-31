import json
from pathlib import Path

import pytest

from research.nba_points_workload_conflicts import evaluate_candidate, evaluate_snapshot, grade, headline

FIXTURE = Path(__file__).parent / "fixtures" / "nba_workload_conflict_barnes_2026_10_30.json"


def _cand(fgas, forecast=10.0, minutes=30.0, prior=None, key="p1", line=15.5, team="TOR"):
    hist = [{"game_date": f"2026-03-{i + 1:02d}", "source_game_id": f"old{i}", "fga": f, "minutes": 30, "team": team, "played": True}
            for i, f in enumerate(prior or [])]
    hist += [{"game_date": f"2026-10-{20 + i:02d}", "source_game_id": f"g{i}", "fga": f, "minutes": minutes, "team": team, "played": True}
             for i, f in enumerate(fgas)]
    return {"player_key": key, "player": key, "predictions": {"market_line": line},
            "opportunity_forecasts": {"fga": forecast, "minutes": 30.0}, "history": hist}


def test_barnes_replay_flags_review():
    fx = json.loads(FIXTURE.read_text())
    r = evaluate_candidate(fx["candidate"], fx["game_date"])
    assert r["status"] == "review_required"
    assert r["window_size"] == 4
    assert r["recent_fga"] == pytest.approx(18.5)
    assert r["forecast_fga"] == pytest.approx(13.32, abs=0.01)
    assert r["recent_workload_rising"] is True
    assert "Review role assumptions" in headline(r)


def test_rule_boundaries():
    assert evaluate_candidate(_cand([13, 13, 13], forecast=10.0), "2026-10-30")["status"] == "review_required"
    assert evaluate_candidate(_cand([12.9, 12.9, 12.9], forecast=10.0), "2026-10-30")["status"] == "no_conflict"
    # gap 3 but ratio 30/33 > 0.85: not a conflict for a heavy shooter
    assert evaluate_candidate(_cand([33, 33, 33], forecast=30.0), "2026-10-30")["status"] == "no_conflict"


def test_window_is_latest_five_current_season():
    r = evaluate_candidate(_cand([2, 20, 20, 20, 20, 20], forecast=10.0), "2026-10-30")
    assert r["window_size"] == 5 and r["recent_fga"] == 20


def test_statuses():
    assert evaluate_candidate(_cand([20, 20]), "2026-10-30")["status"] == "insufficient_history"
    c = _cand([20, 20, 20]); c["opportunity_forecasts"] = {}
    assert evaluate_candidate(c, "2026-10-30")["status"] == "missing_forecast"
    c = _cand([20, 20, 20]); c["history"][-1]["fga"] = None
    assert evaluate_candidate(c, "2026-10-30")["status"] == "incomplete_window"
    c = _cand([20, 20, 20]); c["history"].append(dict(c["history"][-1]))
    assert evaluate_candidate(c, "2026-10-30")["status"] == "ambiguous_identity"
    c = _cand([20, 20, 20]); c["history_status"] = "blocked"
    assert evaluate_candidate(c, "2026-10-30")["status"] == "ambiguous_identity"


def test_future_games_never_enter_the_window():
    c = _cand([20, 20, 20])
    c["history"].append({"game_date": "2026-10-30", "source_game_id": "tonight", "fga": 0, "minutes": 30, "played": True})
    assert evaluate_candidate(c, "2026-10-30")["recent_fga"] == 20


def test_rising_needs_ten_prior_games():
    assert evaluate_candidate(_cand([20, 20, 20], prior=[10] * 9), "2026-10-30")["recent_workload_rising"] is None
    assert evaluate_candidate(_cand([20, 20, 20], prior=[10] * 10), "2026-10-30")["recent_workload_rising"] is True
    assert evaluate_candidate(_cand([20, 20, 20], prior=[19] * 10), "2026-10-30")["recent_workload_rising"] is False


def test_snapshot_order_and_failed_alert():
    snap = {"game_date": "2026-10-30", "sha256": "x", "candidates": [
        _cand([14, 14, 14], key="b", line=10.5), _cand([16, 16, 16], key="a", line=12.5), _cand([10, 10, 10], key="c")]}
    rec = evaluate_snapshot(snap)
    assert [r["player_key"] for r in rec["conflicts"]] == ["a", "b"]
    assert rec["counts"] == {"review_required": 2, "no_conflict": 1}
    g = grade(rec, {"a": {"fga": 9, "played": True}, "b": {"played": False}})
    assert [o["outcome"] for o in g["outcomes"]] == ["at_or_below_forecast", "unresolved"]
