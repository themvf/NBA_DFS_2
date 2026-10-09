"""Frozen-rule tests for the CFB early-season pattern watch (cfb-pattern-watch-v1).

These pin the registered trigger definitions and grading so a quiet edit to a
threshold shows up as a red test rather than a silently changed cohort.
"""

from datetime import datetime, timezone

import pytest

from model import cfb_pattern_watch as pw


def _game(**over):
    base = {
        "home_spread": -10.0, "home_ml": -400, "away_ml": 320, "vegas_total": 52.5,
        "hclass": "fbs", "aclass": "fbs", "hconf": "Sun Belt", "aconf": "Conference USA",
        "home": "Home U", "away": "Away St", "home_score": 31, "away_score": 17,
        # Saturday 2026-10-17 7:30 pm ET == 23:30 UTC
        "commence_time": datetime(2026, 10, 17, 23, 30, tzinfo=timezone.utc),
    }
    base.update(over)
    return base


def test_registration_constants_are_frozen():
    assert pw.STUDY_VERSION == "cfb-pattern-watch-v1"
    assert pw.REGISTERED_AT == datetime(2026, 10, 9, 22, 0, tzinfo=timezone.utc)
    assert pw.VERDICT_NOT_BEFORE == datetime(2026, 12, 7, tzinfo=timezone.utc)
    assert {k: v[1] for k, v in pw.TRIGGERS.items()} == {
        "T1_g5_mid_fav_ml": 40, "T2_fcs_fav_spread": 25, "T3_small_fav_dog_spread": 80,
        "T4_big_fav_over": 50, "T5_sat_evening_g5_fav_ml": 60,
    }
    assert pw.G5_TARGET_DOGS == {"Conference USA", "Sun Belt", "Mid-American"}


def test_t1_requires_g5_both_mid_spread_and_target_dog_conference():
    assert "T1_g5_mid_fav_ml" in pw.classify(_game())
    assert "T1_g5_mid_fav_ml" not in pw.classify(_game(aconf="American Athletic"))
    assert "T1_g5_mid_fav_ml" not in pw.classify(_game(hconf="SEC"))
    assert "T1_g5_mid_fav_ml" not in pw.classify(_game(home_spread=-6.5))
    assert "T1_g5_mid_fav_ml" not in pw.classify(_game(home_spread=-14.0))
    # boundaries are inclusive
    assert "T1_g5_mid_fav_ml" in pw.classify(_game(home_spread=-7.0))
    assert "T1_g5_mid_fav_ml" in pw.classify(_game(home_spread=-13.5))
    # away favorite with the home team as the target dog also qualifies
    g = _game(home_spread=10.0, home_ml=320, away_ml=-400, hconf="Mid-American", aconf="Mountain West")
    assert "T1_g5_mid_fav_ml" in pw.classify(g)


def test_t2_is_fbs_favorite_against_non_fbs_only():
    assert "T2_fcs_fav_spread" in pw.classify(_game(aclass="fcs", home_spread=-30.0))
    assert "T2_fcs_fav_spread" not in pw.classify(_game())
    # an FCS team favored over an FBS team is NOT the trigger
    assert "T2_fcs_fav_spread" not in pw.classify(_game(aclass="fcs", home_spread=3.0, home_ml=130, away_ml=-150))


def test_t3_and_t4_spread_bands():
    assert "T3_small_fav_dog_spread" in pw.classify(_game(home_spread=-3.0))
    assert "T3_small_fav_dog_spread" in pw.classify(_game(home_spread=6.5, home_ml=200, away_ml=-240))
    assert "T3_small_fav_dog_spread" not in pw.classify(_game(home_spread=-2.5))
    assert "T3_small_fav_dog_spread" not in pw.classify(_game(home_spread=-7.0))
    assert "T4_big_fav_over" in pw.classify(_game(home_spread=-14.0))
    assert "T4_big_fav_over" in pw.classify(_game(home_spread=-20.5))
    assert "T4_big_fav_over" not in pw.classify(_game(home_spread=-21.0))
    assert "T4_big_fav_over" not in pw.classify(_game(home_spread=-17.0, vegas_total=None))
    assert "T3_small_fav_dog_spread" not in pw.classify(_game(home_spread=-4.0, aclass="fcs"))


def test_t5_saturday_evening_window_and_g5_requirement():
    assert "T5_sat_evening_g5_fav_ml" in pw.classify(_game())  # 7:30 pm ET Saturday
    assert "T5_sat_evening_g5_fav_ml" in pw.classify(_game(commence_time=datetime(2026, 10, 17, 21, 0, tzinfo=timezone.utc)))   # 5:00 pm
    assert "T5_sat_evening_g5_fav_ml" not in pw.classify(_game(commence_time=datetime(2026, 10, 17, 20, 30, tzinfo=timezone.utc)))  # 4:30 pm
    assert "T5_sat_evening_g5_fav_ml" not in pw.classify(_game(commence_time=datetime(2026, 10, 18, 0, 30, tzinfo=timezone.utc)))  # 8:30 pm
    assert "T5_sat_evening_g5_fav_ml" not in pw.classify(_game(commence_time=datetime(2026, 10, 16, 23, 30, tzinfo=timezone.utc)))  # Friday
    assert "T5_sat_evening_g5_fav_ml" not in pw.classify(_game(hconf="SEC", aconf="Big Ten"))
    assert "T5_sat_evening_g5_fav_ml" in pw.classify(_game(hconf="SEC"))  # one G5 team is enough


def test_grading_moneyline_uses_consensus_price_and_spread_uses_minus_110():
    g = _game(home_score=31, away_score=17)  # home -10 favorite wins by 14
    ml = pw.grade("T1_g5_mid_fav_ml", g)
    assert ml["result"] == "won" and ml["price"] == 1.25 and ml["pnl_units"] == 0.25
    assert ml["selection"] == "Home U ML -400"
    dog = pw.grade("T3_small_fav_dog_spread", _game(home_spread=-4.5, home_score=31, away_score=17))
    assert dog["result"] == "lost" and dog["pnl_units"] == -1.0
    push = pw.grade("T3_small_fav_dog_spread", _game(home_spread=-4.0, home_score=21, away_score=17))
    assert push["result"] == "push" and push["pnl_units"] == 0.0
    over = pw.grade("T4_big_fav_over", _game(home_spread=-17.0, vegas_total=48.0, home_score=31, away_score=17))
    assert over["result"] == "push"
    fcs = pw.grade("T2_fcs_fav_spread", _game(aclass="fcs", home_spread=-30.5, home_score=45, away_score=10))
    assert fcs["result"] == "won" and fcs["pnl_units"] == pytest.approx(100 / 110, abs=1e-4)
    upset = pw.grade("T5_sat_evening_g5_fav_ml", _game(home_score=17, away_score=20))
    assert upset["result"] == "lost" and upset["pnl_units"] == -1.0


def test_ledger_rows_are_written_once_per_trigger_game(tmp_path):
    g = dict(_game(), matchup_id=7, week=8, game_date="2026-10-17", price_source="verified_close:A")
    rows = pw.build_rows([g], "prospective", set())
    assert {r["trigger"] for r in rows} == {"T1_g5_mid_fav_ml", "T5_sat_evening_g5_fav_ml"}
    path = tmp_path / "ledger.jsonl"
    pw.append_rows(path, rows)
    keys = {(r["trigger"], r["matchup_id"]) for r in pw.read_ledger(path)}
    assert pw.build_rows([g], "prospective", keys) == []
    assert len(pw.read_ledger(path)) == 2


def test_summary_withholds_verdict_below_floor():
    rows = pw.build_rows([dict(_game(), matchup_id=1, week=8, game_date="2026-10-17", price_source="x")], "prospective", set())
    text = "\n".join(pw.summarize(rows, "t"))
    assert "descriptive-only" in text
    assert "verdict eligible" not in text
