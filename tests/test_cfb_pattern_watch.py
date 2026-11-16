"""Frozen-rule tests for the CFB early-season pattern watch (cfb-pattern-watch-v2).

These pin the registered trigger definitions and grading so a quiet edit to a
threshold shows up as a red test rather than a silently changed cohort.
"""

from datetime import datetime, timedelta, timezone

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
    assert pw.STUDY_VERSION == "cfb-pattern-watch-v2"
    assert pw.REGISTERED_AT == datetime(2026, 10, 9, 22, 0, tzinfo=timezone.utc)
    assert pw.T6_REGISTERED_AT == datetime(2026, 10, 10, 13, 30, tzinfo=timezone.utc)
    assert pw.trigger_registered_at("T6_mid_fav_spread") == pw.T6_REGISTERED_AT
    assert pw.trigger_registered_at("T1_g5_mid_fav_ml") == pw.REGISTERED_AT
    assert pw.VERDICT_NOT_BEFORE == datetime(2026, 12, 7, tzinfo=timezone.utc)
    assert {k: v[1] for k, v in pw.TRIGGERS.items()} == {
        "T1_g5_mid_fav_ml": 40, "T2_fcs_fav_spread": 25, "T3_small_fav_dog_spread": 80,
        "T4_big_fav_over": 50, "T5_sat_evening_g5_fav_ml": 60, "T6_mid_fav_spread": 80,
        "T7_turnover_margin_fade": 150,
    }
    assert pw.FAMILY_SIZE == 6 and pw.T7 not in pw.V2_FAMILY          # T7 never widens the v2 family
    assert pw.T7_REGISTERED_AT == datetime(2026, 11, 16, 6, 0, tzinfo=timezone.utc)
    assert pw.T7_VERDICT_NOT_BEFORE == datetime(2027, 1, 21, tzinfo=timezone.utc)
    assert (pw.T7_MIN_PRIOR_GAMES, pw.T7_GIVEAWAYS) == (3, ("INT", "FUMBLE", "INT TD", "FUMBLE RETURN TD"))
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


def test_t5_uses_eastern_standard_time():
    eligible = _game(commence_time=datetime(2026, 11, 8, 1, tzinfo=timezone.utc))
    early = dict(eligible, commence_time=datetime(2026, 11, 7, 21, 30, tzinfo=timezone.utc))
    assert pw.T5 in pw.classify(eligible)
    assert pw.T5 not in pw.classify(early)
    assert not pw.corrected_t5_window(dict(early, trigger=pw.T5, commence_time=early["commence_time"].isoformat()))


def test_registered_verdict_uses_date_and_team_gates():
    rows = [
        dict(trigger="T3_small_fav_dog_spread", game_date=f"2026-10-{10 + i % 5}",
             home=f"Home {i}", away=f"Away {i}", result="won", pnl_units=0.9, implied_prob=0.5)
        for i in range(80)
    ]
    def metric(as_of, picks):
        return pw.evaluate_patterns(picks, as_of)[2]

    before = pw.VERDICT_NOT_BEFORE - timedelta(seconds=1)
    assert metric(before, rows)["status"].startswith("Awaiting")
    assert metric(pw.VERDICT_NOT_BEFORE, rows)["status"].startswith("Pass")
    assert metric(pw.VERDICT_NOT_BEFORE, rows)["family_interval"] == pytest.approx((0.9, 0.9))
    assert metric(pw.VERDICT_NOT_BEFORE, rows[:-1])["status"].startswith("Not decidable")
    concentrated = [dict(row, home="Same team") for row in rows]
    assert metric(pw.VERDICT_NOT_BEFORE, concentrated)["status"].startswith("No pass")
    losses = [dict(row, pnl_units=-1, result="lost") for row in rows]
    assert metric(pw.VERDICT_NOT_BEFORE, losses)["status"].startswith("Dead")


def test_t6_mid_favorite_spread_band_and_grade():
    assert "T6_mid_fav_spread" in pw.classify(_game(home_spread=-7.0))
    assert "T6_mid_fav_spread" in pw.classify(_game(home_spread=-13.5))
    assert "T6_mid_fav_spread" in pw.classify(_game(home_spread=10.0, home_ml=320, away_ml=-400, hconf="SEC", aconf="Big Ten"))
    assert "T6_mid_fav_spread" not in pw.classify(_game(home_spread=-6.5))
    assert "T6_mid_fav_spread" not in pw.classify(_game(home_spread=-14.0))
    assert "T6_mid_fav_spread" not in pw.classify(_game(home_spread=-10.0, aclass="fcs"))
    won = pw.grade("T6_mid_fav_spread", _game(home_spread=-10.0, home_score=31, away_score=17))
    assert won["result"] == "won" and won["selection"] == "Home U -10" and won["pnl_units"] == pytest.approx(100 / 110, abs=1e-4)
    assert pw.grade("T6_mid_fav_spread", _game(home_spread=-10.0, home_score=27, away_score=17))["result"] == "push"
    assert pw.grade("T6_mid_fav_spread", _game(home_spread=-10.0, home_score=20, away_score=17))["result"] == "lost"


def test_t6_prospective_rows_start_at_its_own_registration_not_v1s():
    before_t6 = dict(_game(home_spread=-10.0, commence_time=datetime(2026, 10, 10, 0, 0, tzinfo=timezone.utc)),
                     matchup_id=5, week=7, game_date="2026-10-09", price_source="x")
    rows = pw.build_rows([before_t6], "prospective", set())
    trigs = {r["trigger"] for r in rows}
    assert "T1_g5_mid_fav_ml" in trigs            # registered 10-09 22:00, this game is after that
    assert "T6_mid_fav_spread" not in trigs        # registered 10-10 13:30, this game is before that
    disc = {r["trigger"] for r in pw.build_rows([before_t6], "discovery", set())}
    assert "T6_mid_fav_spread" in disc and "T1_g5_mid_fav_ml" not in disc


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
    assert {r["trigger"] for r in rows} == {"T1_g5_mid_fav_ml", "T5_sat_evening_g5_fav_ml", "T6_mid_fav_spread"}
    path = tmp_path / "ledger.jsonl"
    pw.append_rows(path, rows)
    keys = {(r["trigger"], r["matchup_id"]) for r in pw.read_ledger(path)}
    assert pw.build_rows([g], "prospective", keys) == []
    assert len(pw.read_ledger(path)) == 3


def test_summary_withholds_verdict_below_floor():
    rows = pw.build_rows([dict(_game(), matchup_id=1, week=8, game_date="2026-10-17", price_source="x")], "prospective", set())
    text = "\n".join(pw.summarize(rows, "t"))
    assert "descriptive-only" in text
    assert "verdict eligible" not in text


def test_t7_takes_the_team_with_the_worse_turnover_margin():
    assert pw.T7 not in pw.classify(_game())                         # no turnover data: no pick
    assert pw.T7 not in pw.classify(_game(to_edge=0.0))              # level margins: no pick
    assert pw.T7 not in pw.classify(_game(to_edge=0.5, aclass="fcs"))
    away_side = _game(to_edge=0.5, home_spread=-10.0, home_score=27, away_score=20)   # home is better: take away +10
    assert pw.T7 in pw.classify(away_side)
    g = pw.grade(pw.T7, away_side)
    assert g["result"] == "won" and g["selection"].endswith("+10") and g["price"] == pytest.approx(1.9091, abs=1e-4)
    home_side = _game(to_edge=-0.5, home_spread=-10.0, home_score=27, away_score=20)  # home is worse: take home -10
    assert pw.grade(pw.T7, home_side)["result"] == "lost"
    assert pw.grade(pw.T7, dict(home_side, home_score=30))["result"] == "push"


def test_turnover_edges_use_only_earlier_weeks_and_fail_closed():
    games = [dict(matchup_id=w, home_team_id=1, away_team_id=10 + w, order=w, completed=True) for w in (1, 2, 3)]
    games += [dict(matchup_id=20 + w, home_team_id=2, away_team_id=30 + w, order=w, completed=True) for w in (1, 2, 3)]
    games += [dict(matchup_id=99, home_team_id=1, away_team_id=2, order=4, completed=False)]
    give = {}
    for w in (1, 2, 3):
        give[(w, 1)], give[(w, 10 + w)] = 0, 2            # team 1: +2 a game
        give[(20 + w, 2)], give[(20 + w, 30 + w)] = 1, 0  # team 2: -1 a game
    edges = pw.turnover_edges(games, give)
    assert edges[99] == pytest.approx(3.0)
    assert edges[3] is None                               # only two earlier games in week 3
    give[(99, 1)], give[(99, 2)] = 5, 0                   # the game's own turnovers never count
    assert pw.turnover_edges(games, give)[99] == pytest.approx(3.0)
    del give[(2, 1)]                                      # an earlier game without drives: no edge
    assert pw.turnover_edges(games, give)[99] is None


def test_t7_has_its_own_verdict_date_and_is_left_out_of_the_december_verdict():
    rows = [dict(trigger=pw.T7, game_date=f"2026-11-{17 + i % 9}", home=f"H{i}", away=f"A{i}",
                 result="won", pnl_units=0.9, implied_prob=0.5) for i in range(150)]
    assert [p["trigger"] for p in pw.evaluate_patterns(rows, pw.VERDICT_NOT_BEFORE, pw.V2_FAMILY)] == list(pw.V2_FAMILY)
    t7 = lambda as_of: pw.evaluate_patterns(rows, as_of, (pw.T7,))[0]
    assert t7(pw.VERDICT_NOT_BEFORE)["status"].startswith("Awaiting Jan. 21")
    assert t7(pw.T7_VERDICT_NOT_BEFORE)["status"].startswith("Pass")
