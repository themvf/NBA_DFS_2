import json
from pathlib import Path

import pytest

from research import nba_points_role_challenger as rc

FIXTURE = Path(__file__).parent / "fixtures" / "nba_workload_conflict_barnes_2026_10_30.json"
RATES = {"two": 0.5, "three": 0.35, "free": 0.8}


def cand(rows, fc=None, key="p1"):
    fc = fc or {"minutes": 30.0, "fga": 12.0, "fg3a": 4.0, "fta": 3.0}
    pts = 2 * (fc["fga"] - fc["fg3a"]) * RATES["two"] + 3 * fc["fg3a"] * RATES["three"] + fc["fta"] * RATES["free"]
    hist = [{"game_date": f"2026-10-{20 + i:02d}", "source_game_id": f"g{i}", "team": "TOR", "played": True, **r} for i, r in enumerate(rows)]
    return {"player_key": key, "player": key, "opportunity_forecasts": fc, "opportunity_conversion_rates": RATES,
            "predictions": {"opportunity_points": pts}, "history": hist}


ROW = {"minutes": 30, "fga": 18, "fg3a": 6, "fta": 5}
ROSTER = {"p1": {"team": "TOR", "observed_at": "2026-10-30T14:00:00+00:00"}}
FREEZE = "2026-10-30T15:37:00+00:00"


def test_formula_and_baseline():
    r = rc.challenge(cand([ROW] * 3), "2026-10-30", base_model="v3", freeze_at=FREEZE, roster=ROSTER)
    assert r["status"] == "eligible"
    assert r["candidate"]["fga"] == pytest.approx(15.0) and r["candidate"]["fta"] == pytest.approx(4.0)
    assert r["candidate"]["fg3a"] == pytest.approx(15.0 * (0.5 * 4 / 12 + 0.5 * 6 / 18))
    assert r["candidate"]["minutes"] == 30.0 and r["recent_rate_baseline"]["fga"] == pytest.approx(18.0)
    assert r["probabilities"] == "not_produced"


def test_skips_are_explicit():
    assert rc.challenge(cand([ROW] * 3), "2026-10-30", base_model="v3", freeze_at=FREEZE)["reason"] == "team_identity_unverified"
    late = {"p1": {"team": "TOR", "observed_at": "2026-10-30T16:00:00+00:00"}}
    assert rc.challenge(cand([ROW] * 3), "2026-10-30", base_model="v3", freeze_at=FREEZE, roster=late)["reason"] == "roster_observed_after_freeze"
    moved = {"p1": {"team": "BOS", "observed_at": "2026-10-30T14:00:00+00:00"}}
    assert rc.challenge(cand([ROW] * 3), "2026-10-30", base_model="v3", freeze_at=FREEZE, roster=moved)["reason"] == "team_changed"
    c = cand([ROW] * 3); c["predictions"]["opportunity_points"] += 0.5
    assert rc.challenge(c, "2026-10-30", base_model="v3", freeze_at=FREEZE, roster=ROSTER)["reason"] == "base_arithmetic_mismatch"
    bad = {**ROW, "minutes": 0}
    r = rc.challenge(cand([ROW, ROW, bad]), "2026-10-30", base_model="v3", freeze_at=FREEZE, roster=ROSTER)
    assert r["reason"] == "insufficient_history" and r["data_errors"] == ["attempts_with_zero_minutes:g2"]


def test_zero_attempt_games_stay_and_unregistered_base_is_labelled():
    zero = {"minutes": 20, "fga": 0, "fg3a": 0, "fta": 0}
    r = rc.challenge(cand([ROW, ROW, zero]), "2026-10-30", base_model="v2", freeze_at=FREEZE, roster=ROSTER)
    assert r["status"] == "eligible_unregistered_base" and r["recent_window"]["games"] == 3


def test_barnes_replay_arithmetic_on_v2_base():
    fx = json.loads(FIXTURE.read_text())
    c = fx["candidate"]
    if "opportunity_conversion_rates" not in c:
        pytest.skip("fixture predates conversion rates")
    snap = {"game_date": fx["game_date"], "created_at": FREEZE, "candidates": [c]}
    out = rc.challenge_snapshot(snap, base_model="v2")
    assert out["activated"] is False and out["players"][0]["reason"] == "team_identity_unverified"
    assert out["players"][0]["candidate"]["fga"] > out["players"][0]["original"]["fga"]
