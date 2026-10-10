"""Rules for the CFB pattern live board (cfb-pattern-board-v1)."""

from datetime import datetime, timedelta, timezone

from model import cfb_pattern_board as board
from model import cfb_pattern_watch as pw

KICK = datetime(2026, 10, 17, 23, 30, tzinfo=timezone.utc)  # Sat 7:30 pm ET


def _game(**over):
    base = {"matchup_id": 7, "week": 8, "commence_time": KICK, "completed": False, "odds_fetched_at": None,
            "home_spread": -10.0, "vegas_total": 52.5, "home_ml": -400, "away_ml": 320,
            "hclass": "fbs", "aclass": "fbs", "hconf": "Sun Belt", "aconf": "Conference USA",
            "home": "Home U", "away": "Away St"}
    base.update(over)
    return base


def _cap(at, **over):
    row = {"matchup_id": 7, "captured_at": at, "home_spread": -10.0, "vegas_total": 52.5, "home_ml": -400, "away_ml": 320,
           "books": {"draftkings": {"spread_home": -10.0, "spread_home_price": -108, "spread_away": 10.0, "spread_away_price": -112,
                                    "ml_home": -400, "ml_away": 330, "total_line": 52.5, "over": -110, "under": -110}}}
    row.update(over)
    return row


def test_stale_limit_tightens_near_kickoff():
    far = KICK - timedelta(hours=20)
    near = KICK - timedelta(hours=2)
    assert board.stale_limit_minutes(KICK, far) == board.STALE_OUTSIDE_MIN
    assert board.stale_limit_minutes(KICK, near) == board.STALE_INSIDE_MIN
    assert board.is_stale(near - timedelta(minutes=61), KICK, near)
    assert not board.is_stale(near - timedelta(minutes=59), KICK, near)
    assert board.is_stale(None, KICK, near)


def test_stale_quote_never_freezes_a_selection():
    now = KICK - timedelta(hours=2)
    hist = {7: [_cap(now - timedelta(hours=3))]}  # 3h old inside the 6h window
    cards, new = board.build_board([_game()], hist, {}, now)
    assert new == []
    assert cards and all(c["status"] == "stale" for c in cards)
    assert all(c["frozen"] is None for c in cards)


def test_fresh_quote_freezes_once_and_is_immutable_across_runs():
    now = KICK - timedelta(hours=5)
    hist = {7: [_cap(now - timedelta(minutes=10))]}
    frozen = {}
    cards, new = board.build_board([_game()], hist, frozen, now)
    assert {r["trigger"] for r in new} == {"T1_g5_mid_fav_ml", "T5_sat_evening_g5_fav_ml", "T6_mid_fav_spread"}
    t6 = next(r for r in new if r["trigger"] == "T6_mid_fav_spread")
    assert t6["selection"] == "Home U -10" and t6["best_retail"]["book"] == "draftkings"
    # line moves, later run: nothing new is frozen and the original selection is unchanged
    later = now + timedelta(hours=1)
    hist[7].append(_cap(later - timedelta(minutes=5), home_spread=-11.5))
    cards2, new2 = board.build_board([_game()], hist, frozen, later)
    assert new2 == []
    t6c = next(c for c in cards2 if c["trigger"] == "T6_mid_fav_spread")
    assert t6c["frozen"]["home_spread"] == -10.0 and t6c["drift"]["spread"] == -1.5


def test_late_move_flags_watch_and_band_exit_flags_dropped():
    now = KICK - timedelta(minutes=30)
    ref = _cap(KICK - timedelta(hours=2, minutes=5))                       # the T-2h reference
    moved = _cap(now - timedelta(minutes=5), home_spread=-12.0)            # +2 toward home: late move
    frozen = {("T6_mid_fav_spread", 7): {"trigger": "T6_mid_fav_spread", "matchup_id": 7, "home_spread": -10.0,
                                         "vegas_total": 52.5, "home_ml": -400, "away_ml": 320, "selection": "Home U -10", "best_retail": None}}
    cards, _ = board.build_board([_game()], {7: [ref, moved]}, dict(frozen), now)
    t6 = next(c for c in cards if c["trigger"] == "T6_mid_fav_spread")
    assert t6["status"] == "watch" and t6["late_reasons"] and "spread" in t6["late_reasons"][0]
    # spread leaves the 7-13.5 band: trigger no longer fires -> dropped, selection still shown
    out = _cap(now - timedelta(minutes=4), home_spread=-14.0)
    cards, _ = board.build_board([_game()], {7: [ref, moved, out]}, dict(frozen), now)
    t6 = next(c for c in cards if c["trigger"] == "T6_mid_fav_spread")
    assert t6["status"] == "dropped" and t6["frozen"]["selection"] == "Home U -10"


def test_inside_cutoff_is_final_and_started_games_are_started():
    now = KICK - timedelta(minutes=45)
    hist = {7: [_cap(now - timedelta(minutes=5))]}
    cards, new = board.build_board([_game()], hist, {}, now)
    assert new and all(c["status"] == "final" for c in cards)
    after = KICK + timedelta(minutes=1)
    cards, new2 = board.build_board([_game()], {7: [_cap(after - timedelta(minutes=5))]}, {}, after)
    assert new2 == [] and all(c["status"] == "started" for c in cards)


def test_best_retail_prefers_more_points_for_dog_and_fewer_for_favorite():
    books = {"draftkings": {"spread_home": -10.0, "spread_home_price": -105, "spread_away": 10.0, "spread_away_price": -115},
             "fanduel": {"spread_home": -9.5, "spread_home_price": -115, "spread_away": 9.5, "spread_away_price": -105},
             "pinnacle": {"spread_home": -9.0, "spread_home_price": -110, "spread_away": 9.0, "spread_away_price": -110}}
    g = _game()
    assert board.best_retail("T6_mid_fav_spread", g, books)["book"] == "fanduel"      # -9.5 beats -10 even at a worse price
    assert board.best_retail("T3_small_fav_dog_spread", dict(g, home_spread=-4.0), books)["book"] == "draftkings"  # +10 most points
    assert board.best_retail("T6_mid_fav_spread", g, {"pinnacle": books["pinnacle"]}) is None  # not retail
